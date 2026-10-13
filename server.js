import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import express from 'express';
import cors from 'cors';
import pino from 'pino';
import ffmpegPath from 'ffmpeg-static';
import sharp from 'sharp';
import pairRouter, { setPairingController, updatePairingStatus } from './pair.js';
import { Boom } from '@hapi/boom';
import {
  default as makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  prepareWAMessageMedia,
  generateWAMessageFromContent,
  downloadMediaMessage,
  proto,
  Browsers
} from '@whiskeysockets/baileys';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number(process.env.PORT || 3000);
const VERSION = '1.0.0';
let PREFIX = '.';
const BOT_SETTINGS_FILE = path.join(path.dirname(fileURLToPath(import.meta.url)), 'bot-settings.json');
try { const savedBotSettings = JSON.parse(fs.readFileSync(BOT_SETTINGS_FILE, 'utf8')); if (typeof savedBotSettings.prefix === 'string' && savedBotSettings.prefix.length === 1) PREFIX = savedBotSettings.prefix; } catch {}
function saveBotSettings(value) { fs.writeFileSync(BOT_SETTINGS_FILE, JSON.stringify(value, null, 2)); }
const STARTED_AT = Date.now();
const OWNER_NUMBER = String(process.env.MADARA_OWNER_NUMBER || '').replace(/\D/g, '');
const AUTH_DIR = path.join(__dirname, 'auth', 'linked');
const MEDIA_DIR = __dirname; // Media files live beside server.js in the repository root.
const logger = pino({ level: process.env.LOG_LEVEL || 'silent' });
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const app = express();
const server = http.createServer(app);
app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Accept'] }));
app.use(express.json({ limit: '1mb' }));
// Serve only the public pairing page. Never expose the source tree or auth/ credentials through static hosting.
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));
// Keep public pairing endpoints from being abused to repeatedly request codes for arbitrary numbers.
app.set('trust proxy', 1);
const pairRateLimits = new Map();
const pairStatusRateLimits = new Map();
app.use('/api/pair', (req, res, next) => {
  if (req.method !== 'GET') return next();
  const now = Date.now();
  const ip = String(req.ip || req.socket?.remoteAddress || 'unknown');
  if (req.path === '/' || req.path === '') {
    const recent = (pairRateLimits.get(ip) || []).filter(ts => now - ts < 15 * 60 * 1000);
    if (recent.length >= 8) return res.status(429).json({ ok: false, error: 'Too many pairing requests from this address. Wait 15 minutes and try again.' });
    recent.push(now); pairRateLimits.set(ip, recent);
    if (pairRateLimits.size > 1000) pairRateLimits.delete(pairRateLimits.keys().next().value);
  } else if (req.path === '/status' || req.path === '/cancel') {
    const recent = (pairStatusRateLimits.get(ip) || []).filter(ts => now - ts < 60 * 1000);
    if (recent.length >= 100) return res.status(429).json({ ok: false, error: 'Too many pairing-status requests. Slow down and retry.' });
    recent.push(now); pairStatusRateLimits.set(ip, recent);
    if (pairStatusRateLimits.size > 1000) pairStatusRateLimits.delete(pairStatusRateLimits.keys().next().value);
  }
  next();
});
app.use(pairRouter);

const socketSessions = new Map();
const pairingTasks = new Map();
const messages = new Map();
const seenMessageIds = new Map();
const groupConfig = new Map();
const settingsFile = path.join(__dirname, 'group-settings.json');
let waVersionPromise = null;

try {
  const saved = JSON.parse(fs.readFileSync(settingsFile, 'utf8'));
  for (const [jid, value] of Object.entries(saved || {})) groupConfig.set(jid, value);
} catch {}

function normalizeJid(jid = '') {
  return String(jid || '').replace(/:\d+(?=@)/, '');
}
function normalNumber(jid = '') {
  return normalizeJid(jid).split('@')[0].split(':')[0].replace(/\D/g, '');
}
function chatJidOf(msg) {
  return normalizeJid(msg?.key?.remoteJid || '');
}
function senderJidOf(msg) {
  const sender = normalizeJid(msg?.key?.participant || msg?.key?.remoteJid || '');
  if (sender.endsWith('@lid')) {
    const alt = normalizeJid(msg?.key?.senderPn || msg?.key?.participantPn || msg?.key?.remoteJidAlt || '');
    if (alt.endsWith('@s.whatsapp.net')) return alt;
  }
  return sender;
}
function storeKey(sessionId, jid, id) {
  return `${sessionId}|${normalizeJid(jid)}|${id}`;
}
function storeMessage(msg, sessionId) {
  if (!msg?.key?.id || !msg?.key?.remoteJid) return;
  const key = storeKey(sessionId, msg.key.remoteJid, msg.key.id);
  messages.set(key, msg);
  if (messages.size > 3500) messages.delete(messages.keys().next().value);
}
function rememberOutgoing(msg, session) {
  if (!msg?.key?.id) return;
  storeMessage(msg, session.id);
  session.ownMessageIds.add(msg.key.id);
  if (session.ownMessageIds.size > 2500) session.ownMessageIds.delete(session.ownMessageIds.values().next().value);
}
function unwrapMessage(raw = {}) {
  let current = raw;
  for (let i = 0; i < 10; i++) {
    const next = current?.ephemeralMessage?.message ||
      current?.viewOnceMessage?.message || current?.viewOnceMessageV2?.message ||
      current?.viewOnceMessageV2Extension?.message || current?.documentWithCaptionMessage?.message ||
      current?.editedMessage?.message;
    if (!next) break;
    current = next;
  }
  return current || {};
}
function getText(msg) {
  const content = unwrapMessage(msg?.message || {});
  return String(content.conversation || content.extendedTextMessage?.text ||
    content.imageMessage?.caption || content.videoMessage?.caption ||
    content.documentMessage?.caption || content.buttonsResponseMessage?.selectedDisplayText ||
    content.templateButtonReplyMessage?.selectedDisplayText || '').trim();
}
function findContextInfo(value, depth = 0) {
  if (!value || typeof value !== 'object' || depth > 12) return null;
  if (value.contextInfo) return value.contextInfo;
  for (const [key, child] of Object.entries(value)) {
    if (key === 'contextInfo') continue;
    if (child && typeof child === 'object') {
      const found = findContextInfo(child, depth + 1);
      if (found) return found;
    }
  }
  return null;
}
function quotedMediaMessage(msg) {
  const context = findContextInfo(unwrapMessage(msg?.message || {}));
  if (!context?.quotedMessage || !context?.stanzaId) return null;
  return {
    key: {
      remoteJid: chatJidOf(msg),
      id: context.stanzaId,
      participant: context.participant || undefined,
      fromMe: false
    },
    message: context.quotedMessage
  };
}
function getButtonId(msg) {
  const allowed = new Set(['allmenu', 'legendaryspeech']);
  const found = [];
  function visit(value, depth = 0) {
    if (value == null || depth > 10) return;
    if (typeof value === 'string') {
      const text = value.trim();
      if (!text) return;
      try { visit(JSON.parse(text), depth + 1); }
      catch { if (allowed.has(text.toLowerCase())) found.push(text.toLowerCase()); }
      return;
    }
    if (typeof value !== 'object') return;
    for (const id of [value.id, value.selectedId, value.rowId, value.selectedButtonId, value.selectedRowId]) {
      if (typeof id === 'string' && allowed.has(id.trim().toLowerCase())) found.push(id.trim().toLowerCase());
    }
    if (value.paramsJson) visit(value.paramsJson, depth + 1);
    for (const [key, child] of Object.entries(value)) {
      if (key === 'contextInfo') continue;
      if (child && typeof child === 'object') visit(child, depth + 1);
    }
  }
  visit(msg?.message || {});
  visit(unwrapMessage(msg?.message || {}));
  return found[0] || '';
}

function isGroup(jid) { return String(jid || '').endsWith('@g.us'); }
function isAccountOperator(msg) {
  if (msg?.key?.fromMe === true) return true;
  return Boolean(OWNER_NUMBER && normalNumber(senderJidOf(msg)) === OWNER_NUMBER);
}
function getGroupConfig(jid) {
  if (!groupConfig.has(jid)) groupConfig.set(jid, { antilink: false });
  return groupConfig.get(jid);
}
function saveGroupConfig() {
  const data = Object.fromEntries(groupConfig.entries());
  const temp = `${settingsFile}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(data, null, 2));
  fs.renameSync(temp, settingsFile);
}
function formatUptime(ms = Date.now() - STARTED_AT) {
  const seconds = Math.floor(ms / 1000);
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return [d ? `${d}d` : '', h ? `${h}h` : '', m ? `${m}m` : '', `${s}s`].filter(Boolean).join(' ');
}
function isInvisiblePrefix(text) { return /^(?:\u200b|\u200c|\u200d|\ufeff)+/.test(text); }
function normalizeCommandText(text) {
  const trimmed = String(text || '').replace(/^\uFEFF/, '').trim();
  if (isInvisiblePrefix(trimmed)) return '';
  return trimmed;
}
function toCommand(text) {
  const normalized = normalizeCommandText(text);
  if (!normalized.startsWith(PREFIX)) return null;
  const parts = normalized.slice(PREFIX.length).trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return null;
  return { command: parts.shift().toLowerCase(), args: parts, raw: normalized };
}
function sharinganArt(tomoe = 3, stage = 0) {
  const marks = {
    1: ['        ◉      ', '          ●      ', '                 '],
    2: ['    勾玉       ', '        ●       ', '          勾玉   '],
    3: ['   勾玉     勾玉', '        ●       ', '       勾玉      ']
  };
  const lines = marks[tomoe] || marks[3];
  const titles = ['CHAKRA TRACE', 'TOMOE ROTATION', 'SHARINGAN LOCK'];
  const title = titles[Math.min(stage, titles.length - 1)];
  return `╭━━━━━━━━━━━━━━━━━━╮\n│  🔴 うちは・写輪眼  🔴 │\n│   ${title.padEnd(14, ' ')} │\n╰━━━━━━━━━━━━━━━━━━╯\n       .-────────-.\n     .'  ◜       ◝  '.\n    /   ${lines[0]}   \\\n   |    ${lines[1]}    |\n   |    ${lines[2]}    |\n    \\   ◟         ◞   /\n     '.  ───────  .'\n       '────────'\n\n    うちはマダラ • EYE SYSTEM`;
}
const PROVERBS = [
  'Wake up to reality.',
  'Every bright path casts a shadow; wisdom is knowing which one to follow.',
  'Power begins as an idea and becomes real through deliberate action.',
  'Growth asks for effort, patience, and the courage to release old habits.',
  'Reality can be harsh, but your response is still yours to shape.',
  'The world is a cruel place, but discipline decides what you do with that truth.',
  'A calm mind can see the opening that anger hides.',
  'Do not confuse a quiet warrior with a powerless one.',
  'A plan is only powerful when the will to finish it is stronger.',
  'The eye sees the battlefield; wisdom sees the reason for the battle.',
  'A shadow cannot erase the moon. It can only make its light more visible.',
  'The strongest move is sometimes the one you refuse to make.',
  'Learn the pattern before you try to break it.',
  'Fear speaks loudly. Preparation answers without a word.',
  'A throne built on noise shakes; a legacy built on discipline lasts.',
  'If you cannot command your emotions, your opponent will command your decisions.',
  'Every defeat leaves data behind. Only the careless leave it unread.',
  'A blade is dangerous; a mind that knows when not to draw it is wiser.',
  'The night is not empty. It is full of things the careless never notice.',
  'Your next move should make your last mistake impossible to repeat.'
];
const TRUTHS = [
  'What is one goal you keep postponing?', 'What is a skill you wish you had started learning earlier?',
  'What is one thing people often misunderstand about you?', 'What is the bravest decision you have made?',
  'Which habit would you like to improve this month?', 'What is one promise you want to keep to yourself?'
];
const DARES = [
  'Give someone in the group a genuine compliment.', 'Describe your current mood using three emojis.',
  'Send a sentence that sounds like a legendary anime entrance.', 'Share one useful thing you learned this week.',
  'Write your best Madara-style proverb in one line.', 'Let the group choose a harmless nickname for you for five minutes.'
];
const RIDDLES = [
  ['I speak without a mouth and hear without ears. What am I?', 'An echo.'],
  ['The more you take, the more you leave behind. What am I?', 'Footsteps.'],
  ['I have keys but no locks, and space but no room. What am I?', 'A keyboard.'],
  ['What has one eye but cannot see?', 'A needle.'],
  ['What gets wetter as it dries?', 'A towel.']
];
const TRIVIA = [
  ['What is the name of the Uchiha clan eye technique?', 'Sharingan'],
  ['What is the name of Naruto’s village?', 'Konohagakure'],
  ['Which element is associated with lightning-style techniques?', 'Lightning'],
  ['What does “chakra” represent in Naruto’s ninja system?', 'Energy']
];
function pickRandom(items, avoid) {
  if (!items.length) return null;
  let chosen = items[Math.floor(Math.random() * items.length)];
  if (items.length > 1 && chosen === avoid) chosen = items[(items.indexOf(chosen) + 1) % items.length];
  return chosen;
}
const lastProverbByChat = new Map();
const pendingAnswers = new Map();

async function waitForOpen(sock, timeoutMs = 12000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (sock && [...socketSessions.values()].some(s => s.socket === sock && s.connection === 'open')) return sock;
    await sleep(200);
  }
  throw new Error('WhatsApp connection is not open.');
}
async function sendMessage(sock, jid, content, options = {}, tries = 2) {
  let lastError;
  for (let attempt = 0; attempt <= tries; attempt++) {
    try {
      await waitForOpen(sock, 12000);
      const sent = await sock.sendMessage(jid, content, options);
      const session = [...socketSessions.values()].find(s => s.socket === sock);
      if (session) rememberOutgoing(sent, session);
      return sent;
    } catch (error) {
      lastError = error;
      const message = String(error?.message || '');
      const status = error?.output?.statusCode;
      if (attempt >= tries || !(status === 428 || status === 503 || status === 408 || /connection closed|stream errored|timed out/i.test(message))) break;
      await sleep(700 * (attempt + 1));
    }
  }
  throw lastError || new Error('Could not send WhatsApp message.');
}
function buildNativeMenuNode() {
  return {
    tag: 'biz', attrs: { actual_actors: '2', host_storage: '2', privacy_mode_ts: String(Math.floor(Date.now() / 1000) - 77980457) },
    content: [
      { tag: 'interactive', attrs: { type: 'native_flow', v: '1' }, content: [{ tag: 'native_flow', attrs: { v: '9', name: 'mixed' } }] },
      { tag: 'quality_control', attrs: { source_type: 'third_party' } }
    ]
  };
}
async function sendInteractiveButtons(sock, jid, videoBuffer = null, bodyText = 'Choose your next move, shinobi.') {
  let preparedMedia = {};
  if (videoBuffer) {
    preparedMedia = await prepareWAMessageMedia(
      { video: videoBuffer, mimetype: 'video/mp4' },
      { upload: sock.waUploadToServer }
    );
  }
  const interactiveMessage = proto.Message.InteractiveMessage.create({
    body: proto.Message.InteractiveMessage.Body.create({ text: bodyText }),
    footer: proto.Message.InteractiveMessage.Footer.create({ text: '🔴 MADARA UCHIHA • ETERNAL EYE SYSTEM' }),
    header: proto.Message.InteractiveMessage.Header.create({
      title: videoBuffer ? 'MADARA UCHIHA • MENU' : 'UCHIHA CONTROL PANEL',
      subtitle: videoBuffer ? 'Tomoe sequence completed' : 'Native quick actions',
      hasMediaAttachment: Boolean(videoBuffer),
      ...preparedMedia
    }),
    nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
      buttons: [
        proto.Message.InteractiveMessage.NativeFlowButton.create({
          name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: 'ALL MENU', id: 'allmenu' })
        }),
        proto.Message.InteractiveMessage.NativeFlowButton.create({
          name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: 'MADARA LEGENDARY SPEECH', id: 'legendaryspeech' })
        })
      ], messageParamsJson: '{}', messageVersion: 1
    })
  });
  const generated = generateWAMessageFromContent(jid, { interactiveMessage }, { userJid: normalizeJid(sock.user?.id || '') });
  const session = [...socketSessions.values()].find(s => s.socket === sock);
  // Save the exact generated WAMessage before relaying, matching the King Bug MD retry/decryption pattern.
  if (session) rememberOutgoing(generated, session);
  await sock.relayMessage(jid, generated.message, { messageId: generated.key.id, additionalNodes: [buildNativeMenuNode()] });
}

const MICHIKO_IMPORTED_COMMANDS = ["/g) || []).length\n    return numupper\n}\n\n//================= { base } =========================================\nmodule.exports = mrshefzy = async (mrshefzy, rawmessage, chatupdate, store) =>\n{\ntry {\n\n        if (!moderation && mrshefzy) {\n            moderation = new moderationfeatures(mrshefzy);\n        }\n        \nconst m = await smsg(mrshefzy, rawmessage, store);\nif (!m) return m;\n\n        // --- standalone variables (for convenience) ---\nconst from = m.from\nconst sender = m.sender\nconst pushname = m.pushname || ", "audio", "voice", "doc", "video", "repo", "repository", "github", "readmore", "kickall", "cancelkick", "menu", "help", "mich", "wcg", "joinwcg", "endwcg", "clear", "setprefix", "addpack", "addsticker", "delsticker", "listpacks", "sendpack", "delpack", "autoreact", "shortlink", "shorturl", "gitclone", "toaudio", "tovoicenote", "toimage", "tovideo", "device", "tourl", "mediafire", "toptt", "bass", "blown", "earrape", "deep", "robot", "reverse", "tomp3", "google", "ai", "gimage", "image", "ytsearch", "take", "wm", "steal", "mine", "s", "sticker", "stiker", "play", "play2", "ytmp3", "ytmp4", "gen", "apk", "app", "spotify", "igdl", "instagram", "ig", "facebook", "fb", "fbdl", "fbvideo", "tiktok", "tt", "ttdl", "tiktokdl", "tiktokmp3", "ttmp3", "twitter", "tw", "lyrics", "lyric", "lyrics2", "lyric2", "gemini-vision", "flux", "screenshot", "imgur", "pinterest", "weather", "ssweb", "removebg", "imdb", "say", "tts", "gtts", "animekill", "animesmile", "animeslap", "animedance", "animekiss", "animeyeet", "satorugojo", "sukuna", "emojimix", "emix", "catfact", "simi", "meme", "neko", "waifu", "shinobu", "megumin", "slap", "kill", "kiss", "yeet", "hug", "cry", "riddle", "ship", "whois", "truth", "dare", "pickupline", "quote", "insult", "trivia", "ephotomenu", "alive", "checkidch", "idch", "pay", "payme", "testapi", "ping", "addsudo", "delsudo", "listsudo", "gcbroadcast", "block", "setpp", "setbotpp", "jid", "shefzy", "runtime", "uptime", "owner", "self", "public", "addprem", "delprem", "vv", "pinchat", "unpinchat", "alwaysonline", "autostatusview", "savestatus", "deljunk", "disk", "gcaddprivacy", "groupid", "hostip", "join", "lastseen", "leave", "listbadword", "listblocked", "listignorelist", "modestatus", "online", "setstickercmd", "delstickercmd", "tostatus", "dlvo", "vv2", "toviewonce", "unblock", "unblockall", "warn", "ppprivacy", "react", "readreceipts", "restart", "setbio", "kick", "remove", "add", "getpp", "allow", "delallowed", "listallowed", "announcements", "addcode", "delcode", "listcode", "listactive", "listinactive", "kickinactive", "welcome", "goodbye", "approveall", "opengc", "closegc", "delppgroup", "disapproveall", "getgrouppp", "editsettings", "grouplink", "hidetag", "everyone", "invite", "listonline", "listrequests", "mediatag", "closetime", "autorecord", "autoread", "autotyping", "opentime", "poll", "promote", "demote", "resetlink", "setdesc", "setgroupname", "setppgroup", "tagadmin", "tagall", "tag", "totalmembers", "userid", "groupvcf", "vcf", "listgc", "delete", "del", "creategc", "yt", "mute-user", "unmute-user", "mutee", "unmutee", "antilink", "antispam", "antibot", "antipromote", "antidemote", "antibadword", "antibadwords", "antigm", "antigroupmention", "modstatus", "modsettings", "resetwarns", "glitchtext", "pixelglitch", "neonglitch", "glitch2", "cyberpunk", "advancedglow", "neontext", "neonlight", "neonmetal", "writetext", "fogtext", "sandwriting", "graffiti", "typographytext", "flagtext", "goldflag", "3dstone", "3dmetal", "3dchrome", "3dgold", "gaminglogo", "cartoontext", "flame", "smoke", "ice"];
const MICHIKO_API_OR_EXTERNAL_COMMANDS = new Set([
  'ai','simi','flux','gemini-vision','gen','gimage','image','google','imgur','ssweb','screenshot',
  'ig','igdl','instagram','tiktokdl','twitter','tw','fbvideo','yt','play','play2','video','ytmp3','ytmp4','ytsearch',
  'spotify','mediafire','apk','gitclone','shortlink','shorturl','tourl','removebg','weather','imdb','lyrics','lyrics2','lyric','lyric2',
  'gtts','tts','ephotomenu','glitchtext','pixelglitch','neonglitch','glitch2','cyberpunk','advancedglow','neontext','neonlight','neonmetal',
  'writetext','fogtext','sandwriting','graffiti','typographytext','flagtext','goldflag','3dstone','3dmetal','3dchrome','3dgold','gaminglogo','cartoontext','flame','smoke','ice',
  'pinterest','gimage','meme','neko','waifu','shinobu','megumin','satorugojo','sukuna','animedance','animekill','animekiss','animeslap','animesmile','animeyeet',
  'bass','blown','deep','earrape','robot','volaudio','tovoicenote','toptt','tovideo','toviewonce','vv','vv2','dlvo','take','steal','mine','wm','voice',
  'public','self','restart','disk','hostip','testapi','pay','payme','gcbroadcast','tostatus','savestatus','savestatus','join','leave','pinchat','unpinchat',
  'addsudo','delsudo','listsudo','addprem','delprem','block','unblock','unblockall','listblocked','warn','resetwarns','allow','delallowed','listallowed',
  'creategc','addcode','delcode','listcode','approveall','disapproveall','listrequests','antibot','antidemote','antipromote','antibadword','antibadwords','antispam','antigm','antigroupmention','antitag','antitagadmin','modsettings','modstatus','modestatus',
  'wcg','joinwcg','endwcg','addpack','addsticker','delsticker','listpacks','sendpack','delpack','setstickercmd','delstickercmd','vcf','groupvcf','autoreact','autoread','autorecord','autotyping','alwaysonline','autostatusview','readreceipts','ppprivacy','gcaddprivacy','setbio','setpp','setbotpp','setppgroup','delppgroup','setdesc','setgroupname','editsettings','announcements','opentime','closetime','kickinactive','listinactive','listactive','listonline','listgc','listignorelist','deljunk','checkidch','idch','device','whois','lastseen','app'
]);

function mainMenuText() {
  return `╔══════════════════════════════════╗\n      ༺𓆩 𝐌𝐀𝐃𝐀𝐑𝐀 𝐔𝐂𝐇𝐈𝐇𝐀 𓆪༻\n╚══════════════════════════════════╝\n\n🔴 うちはマダラ | THE GHOST OF THE UCHIHA\n\n『 Wake up to reality. 』\n⚔️ 𝑻𝒉𝒆 𝒘𝒐𝒓𝒍𝒅 𝒐𝒃𝒆𝒚𝒔 𝒐𝒏𝒍𝒚 𝒑𝒐𝒘𝒆𝒓.\n\n╭━━〔 🔴 SHARINGAN SYSTEM 〕━━╮\n┃ ⚡ .LG             Legendary proverb\n┃ 👁️ .sharingan      Awaken the red eye\n┃ 🌀 .mangeko-sh     Space-time eye protocol\n┃ 🛡️ .susano         Activate spectral armor\n┃ 📋 .allmenu        Every available command\n┃ 👥 .groupmenu      Group tools only\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n༒ THE ETERNAL EYE IS WATCHING ༒\n『 You choose your path. Reality judges the result. 』\n\n🔴 MADARA UCHIHA • VERSION ${VERSION}`;
}
function allMenuText() {
  const groups = {
    'MADARA CORE': ['menu','allmenu','groupmenu','lg','madaraspeech','sharingan','mangeko-sh','susano','ping','alive','runtime','uptime','owner','repo','github','help'],
    'TEXT & FUN — LOCAL': ['say','quote','reverse','readmore','coinflip','dice','8ball','ship','truth','dare','riddle','trivia','answer','catfact','insult','pickupline','emix','emojimix','hug','slap','kiss','kill','cry','yeet'],
    'MEDIA — LOCAL TOOLS': ['sticker','s','stiker','toimage','toaudio','tomp3','react','delete','del','poll','take','toviewonce','vv','vv2','dlvo'],
    'GROUP MENU — .GROUPMENU': ['tag','tagall','everyone','hidetag','tagadmin','listonline','listgc','getpp','grouplink','invite','totalmembers','userid','add','kick','promote','demote','setgroupname','setdesc','setppgroup','delppgroup','resetlink','mediatag','opengc','closegc','open','close','welcome','goodbye','antilink','antibot','antispam','listadmin','groupinfo','groupid','poll'],
    'OWNER & SETTINGS': ['setprefix','public','self','restart','disk','hostip','modestatus','online','lastseen','addsudo','delsudo','listsudo','block','unblock','unblockall','listblocked','warn','testapi','payme','setbio','ppprivacy','gcaddprivacy','readreceipts','autoreact','clear','alwaysonline','autostatusview','setpp','setprofilepic','autotyping','autorecord','autoread','join','leave','pinchat','unpinchat','jid','idch','setstickercmd','delstickercmd'],
    'DOWNLOADS & ONLINE SERVICES — NEED EXTERNAL SERVICES': ['play','video','ytmp3','ytmp4','instagram','ig','tiktok','tt','twitter','tw','ttmp3','fb','spotify','lyrics','lyrics2','mediafire','gitclone','apk','tourl','shorturl','removebg','ssweb','screenshot','weather','imdb','google','image','imgur','gen','flux','gemini-vision','ytsearch','pinterest'],
    'STICKER PACKS & EFFECTS': ['addpack','addsticker','delsticker','listpacks','sendpack','delpack','glitchtext','pixelglitch','neonglitch','cyberpunk','advancedglow','neontext','neonlight','neonmetal','writetext','fogtext','sandwriting','graffiti','typographytext','flagtext','goldflag','3dstone','3dmetal','3dchrome','3dgold','gaminglogo','cartoontext','flame','smoke','ice'],
    'OTHER MICHIKO FEATURES — MIGRATION STATUS VARIES': ['ai','simi','catfact','meme','neko','waifu','shinobu','megumin','satorugojo','sukuna','wcg','joinwcg','endwcg','vcf','groupvcf','listactive','listinactive','kickinactive','creategc','addcode','delcode','listcode','approveall','disapproveall','listrequests','welcome','goodbye','editsettings','announcements']
  };
  const lines = ['╭━━━ ◈ 𝐌𝐀𝐃𝐀𝐑𝐀 𝐔𝐂𝐇𝐈𝐇𝐀 ◈ ━━━╮','             𝐀𝐋𝐋 𝐌𝐄𝐍𝐔','╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯'];
  for (const [title, commands] of Object.entries(groups)) {
    lines.push('', `╭─〔 ${title} 〕`);
    lines.push(...[...new Set(commands)].map(c => `│ ${PREFIX}${c}`));
    lines.push('╰────────────────────');
  }
  lines.push('', '╭─〔 COMPLETE MICHIKO SOURCE COMMAND INDEX 〕');
  for (let i = 0; i < MICHIKO_IMPORTED_COMMANDS.length; i += 7) {
    lines.push(`│ ${MICHIKO_IMPORTED_COMMANDS.slice(i, i + 7).map(c => PREFIX + c).join(' • ')}`);
  }
  lines.push('╰────────────────────');
  lines.push('', 'ℹ️ External-service commands are listed for migration tracking. If not implemented in this build, Madara will say so instead of pretending the command worked.', '🔴 Adult-content commands were intentionally not migrated.');
  return lines.join('\n');
}
function groupMenuText() {
  return `╭━━━ ◈ 𝐔𝐂𝐇𝐈𝐇𝐀 𝐆𝐑𝐎𝐔𝐏 𝐀𝐑𝐒𝐄𝐍𝐀𝐋 ◈ ━━━╮\n                .GROUPMENU\n╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n🛡️ ADMIN-ONLY (BOT MUST BE GROUP ADMIN WHEN NEEDED)\n> .tagall [message] — mention every member\n> .hidetag [message] — hidden mention blast\n> .tagadmin [message] — mention admins only\n> .kick @user / reply — remove a member\n> .promote @user / reply — make admin\n> .demote @user / reply — remove admin\n> .open — allow all members to send\n> .close — admins-only messages\n> .setgroupname [name] — change group subject\n> .setdesc [text] — change group description\n> .listadmin — list group admins\n> .antilink on/off — delete ordinary links\n> .welcome on/off • .goodbye on/off\n> .setppgroup (reply to image) — set group picture\n> .getgrouppp — show group picture\n> .groupinfo • .groupid • .grouplink • .totalmembers\n> .poll question|option 1|option 2 — create a poll\n> .delete (reply to message) — revoke your/admin-removable message\n\n⚠️ Group admin checks are enabled for moderation commands.`;
}
async function animateSharingan(sock, jid) {
  const frames = [sharinganArt(1, 0), sharinganArt(2, 1), sharinganArt(3, 2)];
  let latest = await sendMessage(sock, jid, { text: frames[0] });
  for (let i = 1; i < frames.length; i++) {
    await sleep(550);
    try {
      const edited = await sock.sendMessage(jid, { text: frames[i], edit: latest.key });
      if (edited?.key) latest = edited;
    } catch {
      // Older clients may reject edits. Replace the previous animation frame instead of spamming the group.
      const replacement = await sendMessage(sock, jid, { text: frames[i] });
      try { await sock.sendMessage(jid, { delete: latest.key }); } catch {}
      latest = replacement;
    }
  }
  await sleep(400);
  try { await sock.sendMessage(jid, { delete: latest.key }); } catch {}
}
async function sendMenu(sock, jid) {
  await animateSharingan(sock, jid);
  const videoPath = path.join(MEDIA_DIR, 'menu.mp4');
  const caption = mainMenuText();
  const hasVideo = fs.existsSync(videoPath);
  const videoBuffer = hasVideo ? fs.readFileSync(videoPath) : null;
  try {
    // Preferred path: video, menu caption/body, and exactly two native quick replies in one message.
    await sendInteractiveButtons(sock, jid, videoBuffer, hasVideo ? caption : `${caption}\n\n📹 Add menu.mp4 beside server.js to enable the video header.`);
    return;
  } catch (error) {
    console.warn('Interactive menu delivery failed; using standard media fallback:', error?.message || error);
  }
  if (videoBuffer) {
    try { await sendMessage(sock, jid, { video: videoBuffer, mimetype: 'video/mp4', caption }); }
    catch (error) { console.warn('Menu video send failed:', error?.message || error); await sendMessage(sock, jid, { text: caption }); }
  } else {
    await sendMessage(sock, jid, { text: `${caption}\n\n📹 Add menu.mp4 beside server.js to enable the video menu.` });
  }
  try { await sendInteractiveButtons(sock, jid, null, 'Choose your next move, shinobi.'); }
  catch (error) {
    console.warn('Native-flow menu buttons unavailable on this session:', error?.message || error);
    await sendMessage(sock, jid, { text: '⌁ QUICK ACTIONS\n• .allmenu\n• .madaraspeech' });
  }
}

async function sendEyeAsset(sock, jid, command) {
  const options = {
    sharingan: {
      file: 'sharingan.jpg', title: '🔴 𝐒𝐇𝐀𝐑𝐈𝐍𝐆𝐀𝐍 — TOMOE SYNC COMPLETE',
      caption: '⟦ EYE PROTOCOL 01 ⟧\n◉ CHAKRA SIGNATURE: DETECTED\n◉ THREE-TOMOE TRACKING: ONLINE\n◉ GENJUTSU INTERFACE: ARMED\n\n「The eye sees what the careless mind misses.」'
    },
    'mangeko-sh': {
      file: 'mangeko-sh.png', title: '🌀 𝐌𝐀𝐍𝐆𝐄𝐊𝐘𝐎 — SPACE/TIME VECTOR ONLINE',
      caption: '⟦ EYE PROTOCOL 02 ⟧\n▰ TARGET VECTOR: LOCKED\n▰ SPACE-TIME DISTORTION: STAGED\n▰ REALITY ANCHOR: STABLE\n\n「Even distance bends when resolve becomes absolute.」'
    },
    susano: {
      file: 'susano.png', title: '🛡️ 𝐒𝐔𝐒𝐀𝐍𝐎 — AVATAR ARMOR MANIFESTED',
      caption: '⟦ AVATAR PROTOCOL 03 ⟧\n▓ CHAKRA FRAME: FORMED\n▓ ARMOR LAYERS: SYNCHRONIZED\n▓ DEFENSE GRID: ACTIVE\n\n「A will that protects becomes a force of its own.」'
    }
  };
  const item = options[command];
  const filePath = path.join(MEDIA_DIR, item.file);
  // A short telemetry sequence makes each skill feel distinct before its media is sent.
  const sequence = command === 'sharingan'
    ? ['⟦ EYE SCAN 01/03 ⟧ retinal signature detected…', '⟦ EYE SCAN 02/03 ⟧ tomoe alignment: 66%', '⟦ EYE SCAN 03/03 ⟧ SHARINGAN AWAKENED 🔴']
    : command === 'mangeko-sh'
      ? ['⟦ VECTOR 01/03 ⟧ locating space-time coordinates…', '⟦ VECTOR 02/03 ⟧ stabilizing dimensional seam…', '⟦ VECTOR 03/03 ⟧ KAMUI-CLASS VECTOR LOCKED 🌀']
      : ['⟦ ARMOR 01/03 ⟧ gathering chakra…', '⟦ ARMOR 02/03 ⟧ skeletal frame materializing…', '⟦ ARMOR 03/03 ⟧ SUSANO MANIFESTED 🛡️'];
  let statusMessage = await sendMessage(sock, jid, { text: sequence[0] });
  for (let i = 1; i < sequence.length; i++) {
    await sleep(450);
    try {
      const changed = await sock.sendMessage(jid, { text: sequence[i], edit: statusMessage.key });
      if (changed?.key) statusMessage = changed;
    } catch {
      const next = await sendMessage(sock, jid, { text: sequence[i] });
      try { await sock.sendMessage(jid, { delete: statusMessage.key }); } catch {}
      statusMessage = next;
    }
  }
  if (fs.existsSync(filePath)) {
    await sendMessage(sock, jid, { image: fs.readFileSync(filePath), caption: `${item.title}\n\n${item.caption}` });
  } else {
    await sendMessage(sock, jid, { text: `${item.title}\n\n${item.caption}\n\n🖼️ Add ${item.file} beside server.js to enable the image response.` });
  }
}
async function getGroupMetadata(sock, jid) {
  if (!isGroup(jid)) throw new Error('This command works inside a WhatsApp group.');
  return await sock.groupMetadata(jid);
}
function sameJid(a, b) {
  const aa = normalizeJid(a), bb = normalizeJid(b);
  if (!aa || !bb) return false;
  if (aa === bb) return true;
  const na = normalNumber(aa), nb = normalNumber(bb);
  return Boolean(na && nb && na === nb);
}
async function senderIsAdmin(sock, msg) {
  const jid = chatJidOf(msg);
  if (!isGroup(jid)) return false;
  const meta = await getGroupMetadata(sock, jid);
  const sender = senderJidOf(msg);
  return meta.participants?.some(p => sameJid(p.id || p.jid, sender) && ['admin', 'superadmin'].includes(p.admin));
}
async function botIsAdmin(sock, jid) {
  const meta = await getGroupMetadata(sock, jid);
  const bot = normalizeJid(sock.user?.id || '');
  return meta.participants?.some(p => sameJid(p.id || p.jid, bot) && ['admin', 'superadmin'].includes(p.admin));
}
async function requireGroupAdmin(sock, msg, reply, needsBotAdmin = false) {
  const jid = chatJidOf(msg);
  if (!isGroup(jid)) { await reply('⚠️ This command only works in a group chat.'); return false; }
  try {
    if (!await senderIsAdmin(sock, msg)) { await reply('🛡️ Only group admins can use this command.'); return false; }
    if (needsBotAdmin && !await botIsAdmin(sock, jid)) { await reply('🤖 Promote Madara to group admin first.'); return false; }
    return true;
  } catch (error) {
    await reply(`⚠️ Could not verify group admin status: ${error?.message || error}`);
    return false;
  }
}
function getMentionTargets(msg) {
  const body = getText(msg);
  const mentions = [];
  const context = findContextInfo(unwrapMessage(msg?.message || {}));
  if (context?.mentionedJid?.length) mentions.push(...context.mentionedJid);
  for (const match of body.matchAll(/@([0-9]{7,15})/g)) mentions.push(`${match[1]}@s.whatsapp.net`);
  const unique = [...new Set(mentions.map(normalizeJid).filter(Boolean))];
  return unique;
}
async function getTargetJids(sock, msg) {
  const direct = getMentionTargets(msg);
  if (direct.length) return direct;
  const quoted = quotedMediaMessage(msg);
  if (quoted?.key?.participant) return [normalizeJid(quoted.key.participant)];
  const text = getText(msg);
  const number = text.match(/(?:^|\s)(\+?\d{7,15})(?:\s|$)/)?.[1]?.replace(/\D/g, '');
  return number ? [`${number}@s.whatsapp.net`] : [];
}
async function getMediaBuffer(sock, msg, preferred = null) {
  const source = preferred || msg;
  const content = unwrapMessage(source?.message || {});
  if (!content.imageMessage && !content.videoMessage && !content.stickerMessage && !content.documentMessage && !content.audioMessage) {
    throw new Error('Attach or reply to a supported media message first.');
  }
  return await downloadMediaMessage(source, 'buffer', {}, { logger, reuploadRequest: sock.updateMediaMessage });
}
async function convertBufferWithFfmpeg(input, args) {
  return await new Promise((resolve, reject) => {
    const child = spawn(ffmpegPath, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    const stdout = [];
    const stderr = [];
    child.stdout.on('data', chunk => stdout.push(chunk));
    child.stderr.on('data', chunk => stderr.push(chunk));
    child.on('error', reject);
    child.on('close', code => {
      if (code === 0 && stdout.length) resolve(Buffer.concat(stdout));
      else reject(new Error(Buffer.concat(stderr).toString('utf8').slice(-500) || `FFmpeg exited with ${code}`));
    });
    child.stdin.end(input);
  });
}
async function commandHandler(sock, msg, session) {
  const jid = chatJidOf(msg);
  if (!jid || !msg?.message) return;
  const rawText = getText(msg);
  const buttonId = getButtonId(msg);
  const command = buttonId || toCommand(rawText)?.command;
  const args = buttonId ? [] : (toCommand(rawText)?.args || []);
  if (!command) return;
  const argText = args.join(' ');
  const reply = async text => sendMessage(sock, jid, { text: String(text) });

  if (command === 'pair') {
    if (isGroup(jid)) return reply('⚠️ For privacy, use .pair in a private chat with the bot.');
    const requester = msg.key?.fromMe ? normalNumber(sock.user?.id || '') : normalNumber(senderJidOf(msg));
    if (!OWNER_NUMBER || requester !== OWNER_NUMBER) return reply('⛔ Only the configured bot owner can create additional pairing sessions. Set MADARA_OWNER_NUMBER in Render to your owner number, including country code.');
    const number = String(args[0] || '').replace(/\D/g, '');
    if (number.length < 10 || number.length > 15) return reply('Usage: .pair 234xxxxxxxxx\nInclude the country code and digits only. The owner of that number must enter the code on their own WhatsApp device to approve linking.');
    try {
      const response = await fetch(`http://127.0.0.1:${PORT}/api/pair?number=${encodeURIComponent(number)}`, { headers: { Accept: 'application/json' } });
      const result = await response.json();
      if (!response.ok || !result.ok || !result.code) return reply(`⚠️ Pairing could not start: ${result.error || `HTTP ${response.status}`}`);
      return reply(`╭━━〔 🔴 MADARA MULTI-SESSION 〕━━╮\n📱 Number: +${number}\n🔑 Pairing code: *${result.code}*\n🆔 Session: ${result.pairingId}\n\nOn that number's WhatsApp: Settings → Linked devices → Link a device → Link with phone number, then enter this code.\n\n⚠️ Share this code only with the person who controls that WhatsApp account. It expires; use .pair again if needed.\n╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯`);
    } catch (error) {
      return reply(`⚠️ Could not reach the pairing service: ${String(error?.message || error).slice(0, 180)}`);
    }
  }

  if (command === 'allmenu') return reply(allMenuText());
  if (['legendaryspeech', 'madaraspeech', 'lg'].includes(command)) {
    const last = lastProverbByChat.get(jid);
    const proverb = pickRandom(PROVERBS, last);
    lastProverbByChat.set(jid, proverb);
    return reply(`╭━━━〔 🔴 𝐌𝐀𝐃𝐀𝐑𝐀'𝐒 𝐋𝐄𝐆𝐄𝐍𝐃𝐀𝐑𝐘 𝐒𝐏𝐄𝐄𝐂𝐇 〕━━━╮\n\n❝ ${proverb} ❞\n\n╰━━━ ◈ 𝐔𝐂𝐇𝐈𝐇𝐀 𝐖𝐈𝐒𝐃𝐎𝐌 ◈ ━━━╯\n\n🔻 Type .LG to hear another proverb.`);
  }
  if (['menu', 'help', 'start'].includes(command)) return sendMenu(sock, jid);
  if (command === 'groupmenu') return reply(groupMenuText());
  if (['sharingan', 'mangeko-sh', 'mangekyo-sh', 'susano'].includes(command)) {
    const normalized = command === 'mangekyo-sh' ? 'mangeko-sh' : command;
    return sendEyeAsset(sock, jid, normalized);
  }
  if (command === 'ping') {
    const started = Date.now();
    const sent = await reply('🔴 Measuring chakra latency…');
    const ms = Date.now() - started;
    try { await sock.sendMessage(jid, { text: `⚡ 𝐑𝐄𝐅𝐋𝐄𝐗: ${ms}ms\n🔴 𝐒𝐘𝐒𝐓𝐄𝐌: ONLINE\n༒ SHARINGAN ROUTE: STABLE`, edit: sent.key }); }
    catch { await reply(`⚡ 𝐑𝐄𝐅𝐋𝐄𝐗: ${ms}ms\n🔴 SYSTEM: ONLINE`); }
    return;
  }
  if (command === 'alive') return reply(`🔴 𝐌𝐀𝐃𝐀𝐑𝐀 𝐌𝐃 𝐈𝐒 𝐀𝐖𝐀𝐊𝐄\n╭─ ◈ STATUS: ONLINE\n├─ ◈ VERSION: ${VERSION}\n├─ ◈ UPTIME: ${formatUptime()}\n╰─ ◈ MODE: NO-API CORE\n\n「The eternal eye does not sleep.」`);
  if (['runtime', 'uptime'].includes(command)) return reply(`⏱️ 𝐒𝐘𝐒𝐓𝐄𝐌 𝐔𝐏𝐓𝐈𝐌𝐄\n▰ ${formatUptime()}\n▰ Version: ${VERSION}\n▰ Sessions: ${[...socketSessions.values()].filter(s => s.connection === 'open').length} connected`);
  if (command === 'owner') return reply(`👑 𝐂𝐑𝐄𝐀𝐓𝐎𝐑 / 𝐎𝐖𝐍𝐄𝐑\n${OWNER_NUMBER ? `+${OWNER_NUMBER}` : 'The linked operator'}\n\n「A legend is defined by the will behind the name.」`);
  if (command === 'jid') return reply(`⌁ CHAT JID: ${jid}\n⌁ SENDER JID: ${senderJidOf(msg)}`);
  if (['groupid', 'group-id'].includes(command)) return reply(isGroup(jid) ? `👥 GROUP ID\n${jid}` : '⚠️ Use .groupid inside a group.');
  if (command === 'groupinfo') {
    if (!isGroup(jid)) return reply('⚠️ Use .groupinfo inside a group.');
    const meta = await getGroupMetadata(sock, jid);
    return reply(`╭━━━〔 👥 GROUP SCAN 〕━━━╮\nName: ${meta.subject || 'Unknown'}\nMembers: ${meta.participants?.length || 0}\nOwner: ${meta.owner || 'Not exposed'}\nCreated: ${meta.creation ? new Date(meta.creation * 1000).toLocaleString() : 'Not exposed'}\nDescription: ${(meta.desc || 'No description').slice(0, 500)}\n╰━━━━━━━━━━━━━━━━━━━━╯`);
  }
  if (['totalmembers', 'members'].includes(command)) {
    if (!isGroup(jid)) return reply('⚠️ Use .totalmembers inside a group.');
    const meta = await getGroupMetadata(sock, jid);
    return reply(`👥 CURRENT GROUP MEMBERS: ${meta.participants?.length || 0}`);
  }
  if (['grouplink', 'linkgc'].includes(command)) {
    if (!isGroup(jid)) return reply('⚠️ Use .grouplink inside a group.');
    if (!await requireGroupAdmin(sock, msg, reply, false)) return;
    try { const code = await sock.groupInviteCode(jid); return reply(`🔗 ${metaSafeGroupName(await getGroupMetadata(sock, jid))}\nhttps://chat.whatsapp.com/${code}`); }
    catch { return reply('⚠️ Could not retrieve the invite link.'); }
  }
  if (command === 'say') {
    if (!argText) return reply('Usage: .say your text');
    return reply(argText.slice(0, 3000));
  }
  if (command === 'quote') {
    const proverb = pickRandom(PROVERBS);
    return reply(`❝ ${proverb} ❞\n\n— 𝐌𝐀𝐃𝐀𝐑𝐀 𝐔𝐂𝐇𝐈𝐇𝐀`);
  }
  if (command === 'reverse') return reply(argText ? Array.from(argText).reverse().join('') : 'Usage: .reverse some text');
  if (command === 'readmore') return reply(argText ? `${argText}\n\n━━━━━━━━━━ ◈ 𝐂𝐎𝐍𝐓𝐈𝐍𝐔𝐄 ◈ ━━━━━━━━━━\n${'› '.repeat(12)}[more below]` : 'Usage: .readmore your text');
  if (['coinflip', 'flip'].includes(command)) return reply(`🪙 RESULT: ${Math.random() < 0.5 ? 'HEADS' : 'TAILS'}\n「Chance favors the prepared mind.」`);
  if (command === 'dice') return reply(`🎲 ROLL: ${1 + Math.floor(Math.random() * 6)}`);
  if (['8ball', '8-ball'].includes(command)) return reply(`🔮 THE ETERNAL EYE ANSWERS: ${pickRandom(['The signs align.', 'Not yet.', 'Focus and try again.', 'The outcome is uncertain.', 'A hidden opportunity approaches.', 'Your plan needs more preparation.'])}`);
  if (command === 'ship') {
    const targets = getMentionTargets(msg);
    const first = targets[0] || senderJidOf(msg);
    const second = targets[1] || `${String(args[0] || 'mystery').replace(/[^a-z0-9]/gi, '').slice(0, 18) || 'mystery'}@s.whatsapp.net`;
    const score = Math.floor(Math.random() * 101);
    return reply(`💞 𝐂𝐇𝐀𝐊𝐑𝐀 𝐂𝐎𝐌𝐏𝐀𝐓𝐈𝐁𝐈𝐋𝐈𝐓𝐘\n@${normalNumber(first)}  ×  @${normalNumber(second)}\n▰ ${'█'.repeat(Math.round(score / 10))}${'░'.repeat(10 - Math.round(score / 10))} ${score}%\n\nJust for fun — no destiny is decided by a bot.`);
  }
  if (command === 'truth') return reply(`🎭 𝐓𝐑𝐔𝐓𝐇 𝐓𝐄𝐒𝐓\n${pickRandom(TRUTHS)}`);
  if (command === 'dare') return reply(`⚔️ 𝐃𝐀𝐑𝐄 𝐏𝐑𝐎𝐓𝐎𝐂𝐎𝐋\n${pickRandom(DARES)}`);
  if (command === 'riddle') {
    const item = pickRandom(RIDDLES);
    pendingAnswers.set(jid, { answer: item[1], kind: 'riddle', askedAt: Date.now() });
    return reply(`🧩 𝐒𝐇𝐈𝐍𝐎𝐁𝐈 𝐑𝐈𝐃𝐃𝐋𝐄\n${item[0]}\n\nUse .answer to reveal it. Ask .riddle again for a new challenge.`);
  }
  if (command === 'trivia') {
    const item = pickRandom(TRIVIA);
    pendingAnswers.set(jid, { answer: item[1], kind: 'trivia', askedAt: Date.now() });
    return reply(`📜 𝐀𝐍𝐈𝐌𝐄 𝐓𝐑𝐈𝐕𝐈𝐀\n${item[0]}\n\nLock in your answer, then type .answer to reveal.`);
  }
  if (command === 'answer') {
    const pending = pendingAnswers.get(jid);
    if (!pending) return reply('🧩 No answer is waiting. Start a .riddle or .trivia challenge first.');
    pendingAnswers.delete(jid);
    return reply(`🔓 𝐀𝐍𝐒𝐖𝐄𝐑 𝐔𝐍𝐋𝐎𝐂𝐊𝐄𝐃\n${pending.answer}\n
「Knowledge is power only after it is understood.」`);
  }
  if (command === 'react') {
    const context = findContextInfo(unwrapMessage(msg?.message || {}));
    const quoted = context?.stanzaId;
    const emoji = argText || '🔴';
    if (!quoted) return reply('Reply to a message and use .react 🔥');
    try { await sock.sendMessage(jid, { react: { text: emoji.slice(0, 4), key: { remoteJid: jid, id: quoted, participant: context.participant || undefined, fromMe: false } } }); return; }
    catch { return reply('⚠️ Could not react to that message.'); }
  }
  if (command === 'delete' || command === 'del') {
    const context = findContextInfo(unwrapMessage(msg?.message || {}));
    if (!context?.stanzaId) return reply('Reply to a message and use .delete.');
    if (isGroup(jid) && !await requireGroupAdmin(sock, msg, reply, true)) return;
    try {
      await sock.sendMessage(jid, { delete: { remoteJid: jid, id: context.stanzaId, participant: context.participant || undefined, fromMe: false } });
      return;
    } catch { return reply('⚠️ Could not delete that message. WhatsApp permissions may block it.'); }
  }
  if (command === 'poll') {
    if (!isGroup(jid)) return reply('⚠️ Polls are available in groups.');
    const [question, ...options] = argText.split('|').map(v => v.trim()).filter(Boolean);
    if (!question || options.length < 2 || options.length > 12) return reply('Usage: .poll Your question|Option 1|Option 2 (up to 12 options)');
    await sendMessage(sock, jid, { poll: { name: question.slice(0, 250), values: options.map(o => o.slice(0, 100)), selectableCount: 1 } });
    return;
  }
  if (['sticker', 's', 'stiker'].includes(command)) {
    try {
      const source = quotedMediaMessage(msg) || msg;
      const buffer = await getMediaBuffer(sock, source);
      const sticker = await sharp(buffer, { animated: false }).rotate().resize(512, 512, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } }).webp({ quality: 82 }).toBuffer();
      return sendMessage(sock, jid, { sticker });
    } catch (error) { return reply(`⚠️ Sticker conversion failed: ${error?.message || error}\nReply to an image and type .sticker`); }
  }
  if (['toimage', 'toimg'].includes(command)) {
    try {
      const source = quotedMediaMessage(msg) || msg;
      const buffer = await getMediaBuffer(sock, source);
      const image = await sharp(buffer).png().toBuffer();
      return sendMessage(sock, jid, { image, caption: '🔴 𝐌𝐄𝐃𝐈𝐀 𝐑𝐄𝐅𝐎𝐑𝐆𝐄 — image conversion complete.' });
    } catch (error) { return reply(`⚠️ Conversion failed: ${error?.message || error}\nReply to a sticker/image and type .toimage`); }
  }
  if (['toaudio', 'tomp3'].includes(command)) {
    try {
      const source = quotedMediaMessage(msg) || msg;
      const content = unwrapMessage(source.message || {});
      if (!content.videoMessage && !content.audioMessage && !content.documentMessage) return reply('Reply to a video/audio file and type .toaudio.');
      const buffer = await getMediaBuffer(sock, source);
      const audio = await convertBufferWithFfmpeg(buffer, ['-hide_banner', '-loglevel', 'error', '-i', 'pipe:0', '-vn', '-ac', '2', '-b:a', '128k', '-f', 'mp3', 'pipe:1']);
      return sendMessage(sock, jid, { audio, mimetype: 'audio/mpeg', fileName: 'madara-audio.mp3' });
    } catch (error) { return reply(`⚠️ Audio conversion failed: ${String(error?.message || error).slice(0, 300)}`); }
  }
  if (command === 'tag' || command === 'tagall' || command === 'everyone' || command === 'hidetag' || command === 'tagadmin') {
    if (!await requireGroupAdmin(sock, msg, reply, false)) return;
    const meta = await getGroupMetadata(sock, jid);
    let recipients = meta.participants || [];
    if (command === 'tagadmin') recipients = recipients.filter(p => ['admin', 'superadmin'].includes(p.admin));
    const mentions = recipients.map(p => normalizeJid(p.id || p.jid)).filter(Boolean);
    const phrase = argText || (command === 'hidetag' ? '🔴 Attention, shinobi.' : '🔴 The Uchiha summons the squad.');
    if (command === 'hidetag') return sendMessage(sock, jid, { text: phrase, mentions });
    const lines = recipients.map(p => `◈ @${normalNumber(p.id || p.jid)}`);
    return sendMessage(sock, jid, { text: `╭━━〔 🔴 ${command === 'tagadmin' ? 'ADMIN TRANSMISSION' : 'GROUP SUMMON'} 〕━━╮\n${phrase}\n\n${lines.join('\n')}\n╰━━━━━━━━━━━━━━━━━━━━╯`, mentions });
  }
  if (['kick', 'remove', 'promote', 'demote'].includes(command)) {
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    const targets = await getTargetJids(sock, msg);
    if (!targets.length) return reply(`Usage: .${command} @user (or reply to their message)`);
    const action = command === 'kick' || command === 'remove' ? 'remove' : command;
    try {
      await sock.groupParticipantsUpdate(jid, targets, action);
      return reply(`✅ UCHIHA GROUP PROTOCOL\nAction: ${action.toUpperCase()}\nTargets: ${targets.map(normalNumber).map(n => `@${n}`).join(', ')}`);
    } catch (error) { return reply(`⚠️ Group action failed: ${error?.message || error}`); }
  }
  if (command === 'open' || command === 'close' || command === 'opengc' || command === 'closegc') {
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    try {
      const closing = command === 'close' || command === 'closegc';
      await sock.groupSettingUpdate(jid, closing ? 'announcement' : 'not_announcement');
      return reply(closing ? '🔒 GROUP LOCKED — only admins can send messages.' : '🔓 GROUP OPEN — all members can send messages.');
    } catch (error) { return reply(`⚠️ Could not update group setting: ${error?.message || error}`); }
  }
  if (command === 'setgroupname' || command === 'setname') {
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    if (!argText) return reply('Usage: .setgroupname New group name');
    try { await sock.groupUpdateSubject(jid, argText.slice(0, 100)); return reply('✅ Group name updated.'); }
    catch (error) { return reply(`⚠️ Rename failed: ${error?.message || error}`); }
  }
  if (command === 'setdesc') {
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    if (!argText) return reply('Usage: .setdesc New group description');
    try { await sock.groupUpdateDescription(jid, argText.slice(0, 2048)); return reply('✅ Group description updated.'); }
    catch (error) { return reply(`⚠️ Description update failed: ${error?.message || error}`); }
  }
  if (command === 'listadmin') {
    if (!isGroup(jid)) return reply('⚠️ Use .listadmin in a group.');
    const meta = await getGroupMetadata(sock, jid);
    const admins = (meta.participants || []).filter(p => ['admin', 'superadmin'].includes(p.admin));
    return sendMessage(sock, jid, { text: `╭━━━〔 👑 GROUP ADMINS 〕━━━╮\n${admins.map((p, i) => `${i + 1}. @${normalNumber(p.id || p.jid)}${p.admin === 'superadmin' ? ' (owner)' : ''}`).join('\n') || 'No admin metadata found.'}\n╰━━━━━━━━━━━━━━━━━━━━╯`, mentions: admins.map(p => normalizeJid(p.id || p.jid)) });
  }
  if (command === 'antilink') {
    if (!await requireGroupAdmin(sock, msg, reply, false)) return;
    const mode = (args[0] || '').toLowerCase();
    if (!['on', 'off'].includes(mode)) return reply('Usage: .antilink on/off');
    const config = getGroupConfig(jid); config.antilink = mode === 'on'; saveGroupConfig();
    return reply(`🛡️ ANTILINK ${mode.toUpperCase()} — ordinary group links ${mode === 'on' ? 'will be removed for non-admin members' : 'are allowed'}.`);
  }
  if (command === 'setppgroup') {
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    try {
      const source = quotedMediaMessage(msg) || msg;
      const buffer = await getMediaBuffer(sock, source);
      await sock.updateProfilePicture(jid, buffer);
      return reply('✅ Group profile picture updated.');
    } catch (error) { return reply(`⚠️ Set picture failed: ${error?.message || error}\nReply to an image with .setppgroup`); }
  }
  if (command === 'welcome' || command === 'goodbye') {
    if (!await requireGroupAdmin(sock, msg, reply, false)) return;
    const mode = (args[0] || '').toLowerCase();
    if (!['on', 'off'].includes(mode)) return reply(`Usage: .${command} on/off`);
    const config = getGroupConfig(jid); config[command] = mode === 'on'; saveGroupConfig();
    return reply(`👁️ ${command.toUpperCase()} PROTOCOL ${mode.toUpperCase()} — ${command === 'welcome' ? 'new-member greeting' : 'member departure notice'} ${mode === 'on' ? 'enabled' : 'disabled'}.`);
  }
  if (command === 'setbio') {
    if (!isAccountOperator(msg)) return reply('👑 Only the linked account operator can change the bot profile status.');
    if (!argText) return reply('Usage: .setbio Your WhatsApp profile status');
    try { await sock.updateProfileStatus(argText.slice(0, 139)); return reply('✅ WhatsApp profile status updated.'); }
    catch (error) { return reply(`⚠️ Could not update profile status: ${error?.message || error}`); }
  }
  if (['setpp', 'setbotpp', 'setprofilepic'].includes(command)) {
    if (!isAccountOperator(msg)) return reply('👑 Only the linked account operator can change the bot profile picture.');
    try {
      const source = quotedMediaMessage(msg) || msg;
      const buffer = await getMediaBuffer(sock, source);
      await sock.updateProfilePicture(normalizeJid(sock.user?.id || ''), buffer);
      return reply('✅ Madara profile picture updated.');
    } catch (error) { return reply(`⚠️ Profile picture update failed: ${error?.message || error}\nReply to an image and use .setpp`); }
  }
  if (command === 'getpp' || command === 'getgrouppp') {
    if (command === 'getgrouppp' && !isGroup(jid)) return reply('⚠️ Use .getgrouppp inside a group.');
    const quotedContext = findContextInfo(unwrapMessage(msg?.message || {}));
    const target = command === 'getgrouppp'
      ? jid
      : (getMentionTargets(msg)[0] || normalizeJid(quotedContext?.participant || senderJidOf(msg)));
    try {
      const url = await sock.profilePictureUrl(target, 'image');
      return sendMessage(sock, jid, { image: { url }, caption: `◈ PROFILE IMAGE SCAN\nTarget: ${target}` });
    } catch { return reply('⚠️ Profile picture is unavailable or hidden by that account’s privacy settings.'); }
  }
  if (['repo','repository','github'].includes(command)) return reply('🔴 MADARA UCHIHA MD\nRepository: publish this project to your own GitHub repository.\nRuntime: Node.js 20 • Baileys • local command core.');
  if (['device','app'].includes(command)) return reply(`📱 LINKED DEVICE REPORT\nAccount: ${sock.user?.id || 'Not available'}\nPlatform: WhatsApp linked device\nConnection: ${session.connection || 'unknown'}\nRuntime: Node.js ${process.version}`);
  if (['whois','userid','mine'].includes(command)) return reply(`👁️ SHINOBI IDENTITY\nName: ${msg.pushName || 'Unknown'}\nJID: ${senderJidOf(msg)}\nChat: ${jid}\nType: ${isGroup(jid) ? 'Group' : 'Private'}`);
  if (['catfact'].includes(command)) return reply(`🐈 RANDOM CAT FACT\n${pickRandom(['Cats can rotate their ears independently.','A cat’s nose print is unique, much like a human fingerprint.','Cats use whiskers to help judge nearby spaces.','Many cats use their tails to signal mood and balance.'])}`);
  if (command === 'insult') return reply(`😈 FRIENDLY ROAST PROTOCOL\n${pickRandom(['Your strategy has the loading icon permanently enabled.','Even a shadow needs better directions than that.','Your brain has 99 tabs open and every one is buffering.','You bring side-quest energy to a final-boss meeting.'])}\nKeep it playful—no personal attacks.`);
  if (command === 'pickupline') return reply(`💬 SHINOBI CHARM MODULE\n${pickRandom(['Are you a Sharingan? Because you caught my attention in one glance.','You must be a rare scroll, because everyone wants to know your story.','If confidence were chakra, you would light up the whole village.'])}`);
  if (['emix','emojimix'].includes(command)) {
    const emojis = argText.split(/[+\s]+/).filter(Boolean);
    if (emojis.length < 2) return reply('Usage: .emojimix 😎 🔥\nThis local build can create a text fusion, not a custom image.');
    return reply(`🧬 EMOJI FUSION\n${emojis[0]} × ${emojis[1]} = ${emojis[0]}‍${emojis[1]}\nFusion engine: text-only mode.`);
  }
  if (['hug','slap','kiss','kill','cry','yeet','animedance','animekill','animekiss','animeslap','animesmile','animeyeet','satorugojo','sukuna','shinobu','megumin','neko','waifu','meme'].includes(command)) {
    const target = getMentionTargets(msg)[0] || senderJidOf(msg);
    const scenes = {
      hug: `🤗 @${normalNumber(target)} receives a respectful shinobi hug.`, slap: `🫲 Anime-style slap scene for @${normalNumber(target)} — fictional and playful.`,
      kiss: `💫 Anime romance reaction sent toward @${normalNumber(target)}.`, kill: `🎭 Fictional anime battle scene triggered for @${normalNumber(target)} — no real-world harm.`,
      cry: '🥹 The Uchiha mask slips for one moment…', yeet: `🚀 @${normalNumber(target)} has been launched into a harmless anime gag.`,
      animedance: '🕺 Madara has entered the forbidden dance phase.', animekill: '⚔️ Fictional anime battle animation requested; image/GIF provider is not configured.', animekiss: '💫 Anime kiss reaction requested; GIF provider is not configured.', animeslap: '🫲 Anime slap reaction requested; GIF provider is not configured.', animesmile: '😊 Anime smile reaction requested; GIF provider is not configured.', animeyeet: '🚀 Anime yeet reaction requested; GIF provider is not configured.', satorugojo: '🔵 GOJO CHARACTER MODE — image provider not configured.', sukuna: '🔴 SUKUNA CHARACTER MODE — image provider not configured.', shinobu: '🦋 SHINOBU IMAGE PROVIDER NOT CONFIGURED.', megumin: '💥 MEGUMIN IMAGE PROVIDER NOT CONFIGURED.', neko: '🐈 NEKO IMAGE PROVIDER NOT CONFIGURED.', waifu: '🌸 WAIFU IMAGE PROVIDER NOT CONFIGURED.', meme: '😂 MEME PROVIDER NOT CONFIGURED.'
    };
    return sendMessage(sock, jid, { text: scenes[command], ...(getMentionTargets(msg).length ? { mentions: [target] } : {}) });
  }
  if (['setprefix'].includes(command)) {
    if (!isAccountOperator(msg)) return reply('👑 Only the linked account operator can change the command prefix.');
    if (!argText || argText.length !== 1 || /\s/.test(argText)) return reply(`Usage: ${PREFIX}setprefix ! (exactly one character)`);
    PREFIX = argText;
    saveBotSettings({ prefix: PREFIX });
    return reply(`✅ Prefix changed to ${PREFIX}\nNew example: ${PREFIX}menu`);
  }
  if (['listonline','listgc','listactive','listinactive','listignorelist','listrequests','listsudo','listblocked','listallowed','listcode','listpacks','modstatus','modestatus','modsettings','modstatus','disk','hostip','online','lastseen','checkidch','idch','groupvcf','vcf'].includes(command)) {
    if (['listgc','listonline','listactive','listinactive','listignorelist','listrequests','listsudo','listblocked','listallowed','listcode','listpacks','modstatus','modestatus','modsettings','modstatus','lastseen','groupvcf','vcf'].includes(command)) {
      return reply(`⌁ ${command.toUpperCase()}\nThis command is recognized from the Michiko source. Its original storage/provider module is not present in the Madara package yet, so it has not been falsely simulated. Use .allmenu to review migration coverage.`);
    }
    if (command === 'disk') return reply(`💾 RUNTIME MEMORY\nRSS: ${(process.memoryUsage().rss / 1024 / 1024).toFixed(1)} MB\nHeap used: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1)} MB`);
    if (command === 'hostip') return reply('🌐 Host IP lookup is not enabled in this privacy-conscious build.');
    if (command === 'online') return reply(`🟢 Session connection: ${session.connection || 'unknown'}\nUptime: ${formatUptime()}`);
    return reply(`🔎 ${command.toUpperCase()}\nCurrent chat: ${jid}\nSender: ${senderJidOf(msg)}`);
  }
  if (MICHIKO_IMPORTED_COMMANDS.includes(command) && MICHIKO_API_OR_EXTERNAL_COMMANDS.has(command)) {
    return reply(`⚙️ 𝐌𝐈𝐆𝐑𝐀𝐓𝐈𝐎𝐍 𝐒𝐓𝐀𝐓𝐔𝐒\n.${command} is recognized from the Michiko source, but its original API/downloader/provider module is not included in this no-API Madara build. It has not been run.\n\nUse .allmenu to see the command groups. Add the relevant provider/dependency to enable this feature.`);
  }
  if (MICHIKO_IMPORTED_COMMANDS.includes(command)) {
    return reply(`⌁ .${command} is recognized from the Michiko command list, but its original handler has not yet been ported into Madara. The command is recorded for migration rather than silently doing nothing.`);
  }
  if (command === 'help') return reply('Type .menu for the Madara video menu, .allmenu for the full Michiko migration catalog, or .groupmenu for group commands.');
}
function metaSafeGroupName(meta) { return meta?.subject || 'Group invite'; }
async function enforceAntilink(sock, msg) {
  const jid = chatJidOf(msg);
  if (!isGroup(jid) || !getGroupConfig(jid).antilink) return false;
  const body = getText(msg);
  if (!/(https?:\/\/|chat\.whatsapp\.com\/|www\.)/i.test(body)) return false;
  try {
    const meta = await sock.groupMetadata(jid);
    const sender = senderJidOf(msg);
    const isAdmin = meta.participants?.some(p => sameJid(p.id || p.jid, sender) && ['admin', 'superadmin'].includes(p.admin));
    const botAdmin = meta.participants?.some(p => sameJid(p.id || p.jid, sock.user?.id) && ['admin', 'superadmin'].includes(p.admin));
    if (isAdmin || msg.key?.fromMe) return false;
    if (!botAdmin) return false;
    await sock.sendMessage(jid, { delete: { remoteJid: jid, id: msg.key.id, participant: msg.key.participant || undefined, fromMe: false } });
    await sendMessage(sock, jid, { text: `🛡️ @${normalNumber(sender)} — link removed by the group protection protocol.`, mentions: [sender] });
    return true;
  } catch (error) {
    console.warn('Antilink check failed:', error?.message || error);
    return false;
  }
}

function waitForReady(session, timeoutMs = 15000) {
  if (session.pairingReady) return Promise.resolve(true);
  let timer;
  return Promise.race([
    session.readyPromise,
    new Promise(resolve => { timer = setTimeout(() => resolve(false), timeoutMs); })
  ]).finally(() => clearTimeout(timer));
}
async function getCurrentVersion() {
  const pinned = String(process.env.WA_VERSION || '').split(/[.,]/).map(Number);
  if (pinned.length === 3 && pinned.every(Number.isInteger)) return pinned;
  try {
    const found = await fetchLatestBaileysVersion();
    if (Array.isArray(found?.version) && found.version.length === 3) return found.version;
  } catch (error) { console.warn('Could not discover latest supported WhatsApp version:', error?.message || error); }
  return null;
}
function markLinked(task) {
  if (!task || task.linked || task.cancelled || task.finished) return;
  task.linked = true;
  updatePairingStatus('linking', {}, task.id);
  clearTimeout(task.linkTimer);
  task.linkTimer = setTimeout(() => {
    if (task.finished || task.session?.connection === 'open') return;
    failPairing(task, 'WhatsApp accepted the code but the connection did not finish. Generate a new code.');
    disposeSession(task.session, true).catch(() => {});
  }, 3 * 60 * 1000);
  task.linkTimer.unref?.();
}
function finishTask(task) {
  clearTimeout(task.linkTimer);
  pairingTasks.delete(task.id);
}
function failPairing(task, reason) {
  if (!task || task.finished) return;
  task.finished = true;
  task.cancelled = true;
  updatePairingStatus('failed', { error: reason }, task.id);
  finishTask(task);
}
async function disposeSession(session, wipe = false) {
  if (!session) return;
  session.disposed = true;
  clearTimeout(session.reconnectTimer);
  session.resolveReady?.(false);
  const sock = session.socket;
  session.socket = null;
  session.connection = 'close';
  if (socketSessions.get(session.id) === session) socketSessions.delete(session.id);
  try { sock?.end?.(new Error('Pairing socket disposed')); } catch {}
  if (wipe) await fs.promises.rm(session.authDir, { recursive: true, force: true }).catch(() => {});
}
async function requestCodeForSession(session, task) {
  const ready = await waitForReady(session, 15000);
  if (!ready || session.disposed || !session.socket) throw new Error('WhatsApp pairing socket did not become ready. Retry the request.');
  let lastError;
  for (let attempt = 0; attempt < 4; attempt++) {
    if (task.cancelled) throw new Error('Pairing cancelled.');
    if (session.disposed || task.session !== session) throw new Error('Pairing socket was replaced.');
    try {
      if (session.state.creds.registered) throw new Error('This number already has a registered WhatsApp session.');
      const code = String(await session.socket.requestPairingCode(task.number) || '').trim().toUpperCase();
      if (!code) throw new Error('WhatsApp returned an empty pairing code.');
      task.codeIssuedAt = Date.now();
      updatePairingStatus('code_ready', { code }, task.id);
      return code;
    } catch (error) {
      lastError = error;
      if (/already.*registered|cancelled|replaced/i.test(error?.message || '')) throw error;
      await sleep(1000 + attempt * 900);
    }
  }
  throw lastError || new Error('Failed to generate pairing code.');
}
async function openPairingSocket(task) {
  await disposeSession(task.session, false);
  task.session = null;
  await fs.promises.rm(task.authDir, { recursive: true, force: true }).catch(() => {});
  await fs.promises.mkdir(task.authDir, { recursive: true });
  const session = await launchSocketSession({ sessionId: task.sessionId, authDir: task.authDir, number: task.number, pairingId: task.id, task });
  task.session = session;
  return requestCodeForSession(session, task);
}
async function regeneratePairingCode(task, why = 'connection closed') {
  if (task.regenerating || task.cancelled || task.linked || task.finished) return;
  task.regenerating = true;
  try {
    for (let attempt = 0; attempt < 6 && !task.cancelled && !task.linked && !task.finished; attempt++) {
      updatePairingStatus('restarting', { message: 'Refreshing expired pairing code…' }, task.id);
      await sleep(Math.min(1000 + attempt * 1400, 8000));
      try { await openPairingSocket(task); return; }
      catch (error) { why = error?.message || String(error); console.warn('Pairing code refresh failed:', why); }
    }
    failPairing(task, `Could not refresh pairing code: ${why}`);
    await disposeSession(task.session, true);
  } finally { task.regenerating = false; }
}
async function cancelPairingSession(pairingId, reason = 'cancelled') {
  const task = pairingTasks.get(pairingId);
  if (!task) return;
  task.cancelled = true;
  task.finished = true;
  finishTask(task);
  const session = task.session;
  if (session && session.connection !== 'open' && !session.state.creds.registered) await disposeSession(session, true);
  console.log(`Pairing ${pairingId} ${reason}.`);
}
function activeSessionCount() {
  const activeNumbers = new Set();
  for (const session of socketSessions.values()) {
    if (session.state?.creds?.registered || session.connection === 'open' || (session.task && !session.task.finished && !session.task.cancelled)) {
      const number = session.number || normalNumber(session.socket?.user?.id || '');
      if (number) activeNumbers.add(number);
      else activeNumbers.add(session.id);
    }
  }
  for (const task of pairingTasks.values()) {
    if (!task.finished && !task.cancelled) activeNumbers.add(task.number);
  }
  return activeNumbers.size;
}

async function startPairingSession(number, pairingId) {
  const sessionId = `linked:${number}`;
  const authDir = path.join(AUTH_DIR, number);
  const alreadyActive = socketSessions.has(sessionId) || [...pairingTasks.values()].some(t => t.number === number && !t.finished && !t.cancelled);
  const maxSessions = Math.max(1, Math.min(5, Number(process.env.MAX_LINKED_SESSIONS || 5)));
  if (!alreadyActive && activeSessionCount() >= maxSessions) throw new Error(`Session limit reached (${maxSessions}). Disconnect a linked account before adding another.`);
  if (socketSessions.get(sessionId)?.state?.creds?.registered) throw new Error('This number is already paired with the bot.');
  try {
    const onDisk = JSON.parse(await fs.promises.readFile(path.join(authDir, 'creds.json'), 'utf8'));
    if (onDisk?.registered) throw new Error('This number is already paired with the bot.');
  } catch (error) {
    if (/already paired/.test(error?.message || '')) throw error;
  }
  for (const old of [...pairingTasks.values()]) if (old.number === number) await cancelPairingSession(old.id, 'replaced');
  const task = { id: pairingId, number, sessionId, authDir, session: null, linked: false, cancelled: false, finished: false, firstCodeDelivered: false, regenerating: false, codeIssuedAt: 0, linkTimer: null };
  pairingTasks.set(pairingId, task);
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const code = await openPairingSocket(task);
      task.firstCodeDelivered = true;
      return code;
    } catch (error) {
      lastError = error;
      if (task.cancelled || /already.*registered/i.test(error?.message || '')) break;
      await sleep(1000 + attempt * 700);
    }
  }
  task.cancelled = true;
  task.finished = true;
  finishTask(task);
  await disposeSession(task.session, true);
  throw lastError || new Error('Could not generate a pairing code.');
}
async function launchSocketSession({ sessionId, authDir, number = '', pairingId = null, task = null, authBundle = null }) {
  const existing = socketSessions.get(sessionId);
  if (existing?.socket && existing.connection !== 'close') return existing;
  const { state, saveCreds } = authBundle || await useMultiFileAuthState(authDir);
  const version = await (waVersionPromise ||= getCurrentVersion());
  let resolveReady;
  const session = {
    id: sessionId, authDir, number, pairingId, task, state, saveCreds, socket: null,
    disposed: false, connection: 'connecting', pairingReady: false, reconnectTimer: null,
    readyPromise: new Promise(resolve => { resolveReady = resolve; }), resolveReady,
    ownMessageIds: new Set(), handledIds: new Set(), openedAt: 0
  };
  const sock = makeWASocket({
    ...(version ? { version } : {}), logger, printQRInTerminal: false,
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
    markOnlineOnConnect: !task || task.linked,
    syncFullHistory: false, browser: Browsers.ubuntu('Chrome'),
    defaultQueryTimeoutMs: 60000, connectTimeoutMs: 60000, keepAliveIntervalMs: 15000,
    emitOwnEvents: true,
    getMessage: async key => messages.get(storeKey(sessionId, key?.remoteJid, key?.id))?.message || undefined
  });
  session.socket = sock;
  socketSessions.set(sessionId, session);
  sock.ev.on('creds.update', saveCreds);
  sock.ev.on('connection.update', async update => {
    if (session.disposed) return;
    const { connection, lastDisconnect, qr, isNewLogin } = update;
    if (qr && !state.creds.registered) {
      session.pairingReady = true;
      session.resolveReady?.(true);
      session.resolveReady = null;
    }
    if (isNewLogin && task) markLinked(task);
    if (connection === 'connecting') session.connection = 'connecting';
    if (connection === 'open') {
      session.connection = 'open';
      session.openedAt = Date.now();
      const actualNumber = number || normalNumber(sock.user?.id || '');
      console.log(`✅ Madara session ${sessionId} connected.`);
      if (task && !task.finished) {
        task.finished = true;
        task.linked = true;
        clearTimeout(task.linkTimer);
        updatePairingStatus('open', {}, task.id);
        finishTask(task);
        session.task = null;
        setTimeout(async () => {
          try {
            const ownJid = normalizeJid(sock.user?.id || `${actualNumber}@s.whatsapp.net`);
            await sendMessage(sock, ownJid, { text: `╔═══ ◈ 𝐌𝐀𝐃𝐀𝐑𝐀 𝐌𝐃 ◈ ═══╗\n> 🔴 STATUS: LINKED SUCCESSFULLY\n> ⚡ VERSION: ${VERSION}\n> ⏱️ UPTIME: ${formatUptime()}\n╚══════════════════════════╝\n\nType .menu to awaken the Sharingan.` });
          } catch (error) { console.warn('Linked confirmation failed:', error?.message || error); }
        }, 1800);
      }
    }
    if (connection === 'close') {
      session.connection = 'close';
      session.resolveReady?.(false);
      session.resolveReady = null;
      const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
      const loggedOut = statusCode === DisconnectReason.loggedOut;
      const registered = Boolean(state.creds.registered);
      console.warn(`🔌 Session ${sessionId} closed (${statusCode ?? 'unknown'}): ${lastDisconnect?.error?.message || 'unknown reason'}`);
      if (task && !task.linked && !registered) {
        if (task.firstCodeDelivered && !task.cancelled && !task.finished) {
          regeneratePairingCode(task, `${statusCode ?? 'closed'}`).catch(err => console.error('Pairing refresh exception:', err?.message || err));
        }
        return;
      }
      if (loggedOut) {
        socketSessions.delete(sessionId);
        if (!sessionId.startsWith('primary')) await fs.promises.rm(authDir, { recursive: true, force: true }).catch(() => {});
        if (task && !task.finished) failPairing(task, 'WhatsApp logged out this linked device. Pair again.');
        return;
      }
      clearTimeout(session.reconnectTimer);
      session.reconnectTimer = setTimeout(() => {
        if (session.disposed) return;
        launchSocketSession({ sessionId, authDir, number, pairingId, task: task?.finished ? null : task, authBundle: { state, saveCreds } })
          .catch(error => console.error(`Reconnect failed for ${sessionId}:`, error?.stack || error));
      }, statusCode === DisconnectReason.restartRequired ? 300 : 1800);
    }
  });
  sock.ev.on('messages.upsert', async ({ messages: incoming = [] }) => {
    for (const msg of incoming) {
      try {
        if (!msg?.key?.id || !msg?.key?.remoteJid || !msg.message) continue;
        storeMessage(msg, sessionId);
        if (session.ownMessageIds.has(msg.key.id)) continue;
        if (session.handledIds.has(msg.key.id)) continue;
        session.handledIds.add(msg.key.id);
        if (session.handledIds.size > 2500) session.handledIds.delete(session.handledIds.values().next().value);
        const wasAntilink = await enforceAntilink(sock, msg);
        if (wasAntilink) continue;
        // Sequential processing per linked socket keeps Signal's ratchet in event order.
        await commandHandler(sock, msg, session);
      } catch (error) {
        console.error('Message handler error:', error?.stack || error);
        try { await sendMessage(sock, chatJidOf(msg), { text: `⚠️ Madara command error: ${String(error?.message || error).slice(0, 250)}` }, {}, 0); } catch {}
      }
    }
  });
  sock.ev.on('groups.update', async updates => {
    for (const update of updates || []) {
      if (!update?.id) continue;
      try { await sock.groupMetadata(update.id); } catch {}
    }
  });
  sock.ev.on('group-participants.update', async event => {
    try {
      const groupId = event?.id || '';
      const action = event?.action;
      const config = getGroupConfig(groupId);
      if (!((action === 'add' && config.welcome) || (action === 'remove' && config.goodbye))) return;
      const meta = await sock.groupMetadata(groupId);
      const participants = (event.participants || []).map(normalizeJid).filter(Boolean);
      if (!participants.length) return;
      const names = participants.map(p => `@${normalNumber(p)}`).join(', ');
      const greeting = action === 'add'
        ? `Welcome to ${meta.subject || 'the group'}, ${names}.\n\nRead the group rules, respect the members, and choose your path wisely.`
        : `${names} has left ${meta.subject || 'the group'}.\n\nThe shinobi records have been updated.`;
      await sendMessage(sock, groupId, { text: `╭━━〔 🔴 UCHIHA ${action === 'add' ? 'ARRIVAL' : 'DEPARTURE'} SCAN 〕━━╮\n${greeting}\n╰━━━━━━━━━━━━━━━━━━━━╯`, mentions: participants });
    } catch (error) { console.warn('Welcome protocol failed:', error?.message || error); }
  });
  return session;
}

app.get('/health', (_req, res) => {
  const sessions = [...socketSessions.values()];
  res.json({ ok: true, bot: 'Madara Uchiha MD', version: VERSION, uptimeSeconds: Math.floor((Date.now() - STARTED_AT) / 1000), whatsapp: sessions.some(s => s.connection === 'open') ? 'open' : 'close', connectedSessions: sessions.filter(s => s.connection === 'open').length, totalSessions: sessions.length, maxLinkedSessions: Math.max(1, Math.min(5, Number(process.env.MAX_LINKED_SESSIONS || 5))) });
});
app.get('/api/status', (_req, res) => {
  const sessions = [...socketSessions.values()];
  res.set('Cache-Control', 'no-store').json({ ok: true, version: VERSION, uptime: formatUptime(), connected: sessions.filter(s => s.connection === 'open').length, sessions: sessions.map(s => { const n = s.number || normalNumber(s.socket?.user?.id || ''); return { number: n ? `${n.slice(0, 3)}••••${n.slice(-3)}` : 'linked device', status: s.connection }; }) });
});

setPairingController({ startPairing: startPairingSession, cancelPairing: cancelPairingSession });

async function restoreSavedSessions() {
  await fs.promises.mkdir(AUTH_DIR, { recursive: true });
  const entries = await fs.promises.readdir(AUTH_DIR, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d{10,15}$/.test(entry.name)) continue;
    const authDir = path.join(AUTH_DIR, entry.name);
    try {
      const auth = await useMultiFileAuthState(authDir);
      if (!auth.state.creds.registered) continue;
      await launchSocketSession({ sessionId: `linked:${entry.name}`, authDir, number: entry.name, authBundle: auth });
    } catch (error) { console.warn(`Could not restore saved session ${entry.name}:`, error?.message || error); }
  }
}

await restoreSavedSessions();
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🔴 Madara Uchiha MD v${VERSION} listening on ${PORT}`);
  console.log('Pairing panel: /  |  Health: /health');
});

process.on('SIGTERM', () => {
  for (const session of socketSessions.values()) {
    try { session.disposed = true; clearTimeout(session.reconnectTimer); session.socket?.end?.(new Error('Server shutting down')); } catch {}
  }
  server.close(() => process.exit(0));
});
process.on('SIGINT', () => {
  for (const session of socketSessions.values()) {
    try { session.disposed = true; clearTimeout(session.reconnectTimer); session.socket?.end?.(new Error('Server shutting down')); } catch {}
  }
  server.close(() => process.exit(0));
});
