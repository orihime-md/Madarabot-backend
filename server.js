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
  downloadContentFromMessage,
  getAggregateVotesInPollMessage,
  proto,
  Browsers
} from '@whiskeysockets/baileys';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number(process.env.PORT || 3000);
const VERSION = '1.0.1';
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
const groupMetadataCache = new Map();
const lidToPn = new Map();
const trackedPolls = new Map();
const chatTaskQueues = new Map();
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
  const remote = normalizeJid(msg?.key?.remoteJid || '');
  // WhatsApp may identify a private chat with a LID while exposing the phone JID as an alternate.
  if (remote.endsWith('@lid') && !remote.endsWith('@g.us')) {
    const phoneJid = normalizeJid(msg?.key?.remoteJidAlt || msg?.key?.senderPn || msg?.key?.participantPn || '');
    if (phoneJid.endsWith('@s.whatsapp.net')) return phoneJid;
  }
  return remote;
}
function rememberJidPair(first, second) {
  const a = normalizeJid(first);
  const b = normalizeJid(second);
  if (!a || !b || a === b) return;
  if (a.endsWith('@lid') && (b.endsWith('@s.whatsapp.net') || b.endsWith('@c.us'))) lidToPn.set(a, b);
  if (b.endsWith('@lid') && (a.endsWith('@s.whatsapp.net') || a.endsWith('@c.us'))) lidToPn.set(b, a);
  if (lidToPn.size > 10000) lidToPn.delete(lidToPn.keys().next().value);
}
function rememberJidAliases(key = {}) {
  const pairs = [
    [key.remoteJid, key.remoteJidAlt],
    [key.participant, key.participantAlt],
    [key.senderLid, key.senderPn],
    [key.participantLid, key.participantPn]
  ];
  for (const [a, b] of pairs) rememberJidPair(a, b);
}
function rememberParticipantAliases(participants = []) {
  for (const participant of participants || []) {
    const rawIds = [participant?.id, participant?.jid, participant?.lid, participant?.pn,
      participant?.phoneNumber, participant?.participantPn, participant?.participantLid]
      .filter(Boolean).map(value => {
        const id = normalizeJid(value);
        return /^\+?\d{7,16}$/.test(id) ? `${id.replace(/\D/g, '')}@s.whatsapp.net` : id;
      });
    for (const first of rawIds) {
      if (!first.endsWith('@lid')) continue;
      for (const second of rawIds) rememberJidPair(first, second);
    }
  }
}
function rememberAccountAliases(sock) {
  rememberJidPair(sock?.user?.id, sock?.user?.lid);
  rememberJidPair(sock?.user?.id, sock?.user?.phoneNumber);
}
function jidAliases(jid) {
  const value = normalizeJid(jid);
  const values = new Set(value ? [value] : []);
  const mapped = lidToPn.get(value);
  if (mapped) values.add(normalizeJid(mapped));
  if (value.endsWith('@s.whatsapp.net') || value.endsWith('@c.us')) {
    for (const [lid, pn] of lidToPn.entries()) {
      if (normalizeJid(pn) === value) values.add(lid);
    }
  }
  return values;
}
function senderJidOf(msg, sock = null) {
  // Messages written by the linked account's own phone are marked fromMe.
  if (msg?.key?.fromMe === true && sock?.user?.id) {
    rememberAccountAliases(sock);
    return normalizeJid(sock.user.id);
  }
  rememberJidAliases(msg?.key || {});
  const sender = normalizeJid(msg?.key?.participant || msg?.key?.remoteJid || '');
  if (sender.endsWith('@lid')) {
    const alt = normalizeJid(msg?.key?.senderPn || msg?.key?.participantPn || msg?.key?.remoteJidAlt || lidToPn.get(sender) || '');
    if (alt.endsWith('@s.whatsapp.net') || alt.endsWith('@c.us')) return alt;
  }
  return sender;
}
function storeKey(sessionId, jid, id) {
  return `${sessionId}|${normalizeJid(jid)}|${id}`;
}
function storeMessage(msg, sessionId) {
  if (!msg?.key?.id || !msg?.key?.remoteJid) return;
  rememberJidAliases(msg.key);
  const originalJid = normalizeJid(msg.key.remoteJid);
  const canonicalJid = chatJidOf(msg);
  // Keep retry lookup indexed under every known PN/LID alias. WhatsApp can
  // return a retry receipt with a different identifier for the same chat.
  const chatIds = new Set([originalJid, canonicalJid]);
  for (const jid of [...chatIds]) {
    for (const alias of jidAliases(jid)) chatIds.add(normalizeJid(alias));
  }
  for (const jid of chatIds) {
    if (jid) messages.set(storeKey(sessionId, jid, msg.key.id), msg);
  }
  while (messages.size > 4000) messages.delete(messages.keys().next().value);
}

function getStoredMessageForRetry(sessionId, key = {}) {
  if (!key?.id) return undefined;
  const requestedJid = normalizeJid(key.remoteJid || '');
  const direct = messages.get(storeKey(sessionId, requestedJid, key.id));
  if (direct?.message) return direct.message;

  // Fallback for retry receipts where WhatsApp reports the same conversation
  // using its LID instead of PN (or vice versa). Match within this session only.
  const sessionPrefix = `${sessionId}|`;
  for (const [storedKey, msg] of messages.entries()) {
    if (!storedKey.startsWith(sessionPrefix)) continue;
    if (!msg?.message || msg?.key?.id !== key.id) continue;
    const originalJid = normalizeJid(msg?.key?.remoteJid || '');
    if (originalJid && requestedJid && sameJid(originalJid, requestedJid)) {
      return msg.message;
    }
    const canonicalJid = chatJidOf(msg);
    if (canonicalJid && requestedJid && sameJid(canonicalJid, requestedJid)) {
      return msg.message;
    }
  }
  return undefined;
}
function rememberOutgoing(msg, session) {
  if (!msg?.key?.id) return;
  storeMessage(msg, session.id);
  session.ownMessageIds.add(storeKey(session.id, msg.key.remoteJid || '', msg.key.id));
  if (session.ownMessageIds.size > 2500) session.ownMessageIds.delete(session.ownMessageIds.values().next().value);
}
function unwrapMessage(raw = {}) {
  let current = raw;
  for (let i = 0; i < 10; i++) {
    const next = current?.deviceSentMessage?.message ||
      current?.ephemeralMessage?.message ||
      current?.viewOnceMessage?.message || current?.viewOnceMessageV2?.message ||
      current?.viewOnceMessageV2Extension?.message || current?.documentWithCaptionMessage?.message ||
      current?.editedMessage?.message || current?.groupMentionedMessage?.message;
    if (!next) break;
    current = next;
  }
  return current || {};
}
function unwrapWithViewOnceInfo(message = {}) {
  let current = message || {};
  let viewOnce = false;
  for (let depth = 0; depth < 12; depth++) {
    if (current.viewOnceMessage || current.viewOnceMessageV2 || current.viewOnceMessageV2Extension) {
      viewOnce = true;
    }
    const mediaNode = current.imageMessage || current.videoMessage || current.audioMessage;
    if (current.viewOnce === true || current.isViewOnce === true ||
        mediaNode?.viewOnce === true || mediaNode?.isViewOnce === true) {
      viewOnce = true;
    }
    const next = current.deviceSentMessage?.message ||
      current.ephemeralMessage?.message ||
      current.viewOnceMessage?.message ||
      current.viewOnceMessageV2?.message ||
      current.viewOnceMessageV2Extension?.message ||
      current.documentWithCaptionMessage?.message ||
      current.editedMessage?.message ||
      current.groupMentionedMessage?.message;
    if (!next) break;
    current = next;
  }
  const context = findContextInfo(current);
  if (context?.isViewOnce === true || context?.isViewOnceMessage === true) viewOnce = true;
  return { content: current || {}, viewOnce };
}

function getText(msg) {
  const content = unwrapMessage(msg?.message || {});
  const nativeParams = content.interactiveResponseMessage?.nativeFlowResponseMessage?.paramsJson;
  let nativeText = '';
  if (typeof nativeParams === 'string') {
    try { const parsed = JSON.parse(nativeParams); nativeText = parsed.id || parsed.selectedId || parsed.rowId || parsed.display_text || ''; } catch {}
  }
  return String(content.conversation || content.extendedTextMessage?.text ||
    content.imageMessage?.caption || content.videoMessage?.caption ||
    content.documentMessage?.caption || content.buttonsResponseMessage?.selectedButtonId ||
    content.buttonsResponseMessage?.selectedDisplayText ||
    content.templateButtonReplyMessage?.selectedId || content.templateButtonReplyMessage?.selectedDisplayText ||
    content.listResponseMessage?.singleSelectReply?.selectedRowId || nativeText || '').trim();
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
      remoteJid: normalizeJid(msg?.key?.remoteJid || chatJidOf(msg)),
      id: context.stanzaId,
      participant: context.participant || undefined,
      fromMe: false
    },
    message: context.quotedMessage
  };
}
function getButtonId(msg) {
  // Inspect only response-message fields. Walking every property of ordinary media
  // messages can traverse thousands of thumbnail/Buffer entries and slow busy groups.
  const roots = [msg?.message || {}, unwrapMessage(msg?.message || {})];
  const allowed = new Set(['allmenu', 'legendaryspeech', 'groupmenu']);
  const candidates = [];
  for (const root of roots) {
    candidates.push(
      root.buttonsResponseMessage?.selectedButtonId,
      root.buttonsResponseMessage?.selectedDisplayText,
      root.templateButtonReplyMessage?.selectedId,
      root.templateButtonReplyMessage?.selectedDisplayText,
      root.listResponseMessage?.singleSelectReply?.selectedRowId,
      root.interactiveResponseMessage?.nativeFlowResponseMessage?.paramsJson
    );
  }
  for (const candidate of candidates) {
    if (typeof candidate !== 'string' || !candidate.trim()) continue;
    const value = candidate.trim();
    if (allowed.has(value.toLowerCase())) return value.toLowerCase();
    try {
      const parsed = JSON.parse(value);
      for (const key of ['id', 'selectedId', 'rowId', 'selectedButtonId']) {
        const id = String(parsed?.[key] || '').trim().toLowerCase();
        if (allowed.has(id)) return id;
      }
    } catch {}
  }
  return '';
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
const planetaryTimers = new Map();

async function waitForOpen(sock, timeoutMs = 12000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (sock && [...socketSessions.values()].some(s => s.socket === sock && s.connection === 'open')) return sock;
    await sleep(200);
  }
  throw new Error('WhatsApp connection is not open.');
}
function uchihaStyle(text) {
  const value = String(text ?? '');
  if (!value.trim()) return value;
  if (/^\s*(?:╭|╔|╰|╚|༺|🔴\s*𝐌𝐀𝐃𝐀𝐑𝐀)/u.test(value) ||
      /𝐔𝐂𝐇𝐈𝐇𝐀|MADARA UCHIHA|うちはマダラ|𝐔𝐂𝐇𝐈𝐇𝐀 𝐒𝐂𝐑𝐎𝐋𝐋/i.test(value.slice(0, 260))) {
    return value;
  }
  return `╭━━〔 🔴 𝐔𝐂𝐇𝐈𝐇𝐀 𝐒𝐂𝐑𝐎𝐋𝐋 〕━━╮\n${value}\n╰━━ ◈ 𝐌𝐀𝐃𝐀𝐑𝐀 𝐔𝐂𝐇𝐈𝐇𝐀 ◈ ━━╯`;
}

async function sendMessage(sock, jid, content, options = {}, tries = 2) {
  let lastError;
  for (let attempt = 0; attempt <= tries; attempt++) {
    try {
      await waitForOpen(sock, 12000);
      const outgoing = content && typeof content === 'object' ? { ...content } : content;
      if (outgoing && typeof outgoing.text === 'string') outgoing.text = uchihaStyle(outgoing.text);
      if (outgoing && typeof outgoing.caption === 'string') outgoing.caption = uchihaStyle(outgoing.caption);
      const sent = await sock.sendMessage(jid, outgoing, options);
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
async function sendInteractiveButtons(sock, jid, headerVideoBuffer = null, bodyText = mainMenuText()) {
  // Native-flow menu with the Madara video in the same message card as its menu text/buttons.
  await waitForOpen(sock, 12000);
  let preparedMedia = {};
  if (headerVideoBuffer) {
    preparedMedia = await prepareWAMessageMedia(
      { video: headerVideoBuffer, mimetype: 'video/mp4' },
      { upload: sock.waUploadToServer }
    );
  }
  const interactiveMessage = proto.Message.InteractiveMessage.create({
    body: proto.Message.InteractiveMessage.Body.create({ text: bodyText }),
    footer: proto.Message.InteractiveMessage.Footer.create({
      text: '「🔴 MADARA UCHIHA • ETERNAL EYE SYSTEM」'
    }),
    header: proto.Message.InteractiveMessage.Header.create({
      title: 'MADARA UCHIHA • MENU SCROLL',
      hasMediaAttachment: Boolean(headerVideoBuffer),
      ...preparedMedia
    }),
    nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
      buttons: [
        proto.Message.InteractiveMessage.NativeFlowMessage.NativeFlowButton.create({
          name: 'quick_reply',
          buttonParamsJson: JSON.stringify({ display_text: 'ALL MENU', id: 'allmenu' })
        }),
        proto.Message.InteractiveMessage.NativeFlowMessage.NativeFlowButton.create({
          name: 'quick_reply',
          buttonParamsJson: JSON.stringify({ display_text: 'MADARA LEGENDARY SPEECH', id: 'legendaryspeech' })
        })
      ],
      messageParamsJson: '{}',
      messageVersion: 1
    })
  });

  const generated = generateWAMessageFromContent(
    jid,
    { interactiveMessage },
    { userJid: normalizeJid(sock.user?.id || '') }
  );
  const session = [...socketSessions.values()].find(s => s.socket === sock);
  if (session) rememberOutgoing(generated, session);
  await sock.relayMessage(jid, generated.message, {
    messageId: generated.key.id,
    additionalNodes: [buildNativeMenuNode()]
  });
}

function mainMenuText() {
  return `╔══════════════════════════════════╗
        ༺𓆩 𝐌𝐀𝐃𝐀𝐑𝐀 𝐔𝐂𝐇𝐈𝐇𝐀 𓆪༻
╚══════════════════════════════════╝

🔴 うちはマダラ | THE GHOST OF THE UCHIHA
『 Wake up to reality. 』
⚔️ Power without purpose is merely noise.

╭━━〔 🔴 ETERNAL EYE SYSTEM 〕━━╮
│ .allmenu — open the complete working command scroll
│ .groupmenu — view group techniques
│ .LG — receive a random Madara proverb
│ .sharingan — activate the three-tomoe group scan
│ .mangeko-sh — open the dimensional poll
│ .susano — manifest Susano with its video and audio
╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯

「The world shall know the will of the Uchiha.」`;
}

function allMenuText() {
  const groups = [
    ['CORE / UCHIHA TECHNIQUES', [
      'menu', 'allmenu', 'groupmenu', 'lg', 'madaraspeech',
      'sharingan', 'mangeko-sh', 'susano', 'susano-video',
      'ping', 'alive', 'runtime', 'uptime', 'owner', 'repo', 'device', 'jid'
    ]],
    ['LOCAL TEXT / TRAINING', [
      'say', 'quote', 'reverse', 'readmore', 'coinflip', 'dice', '8ball',
      'ship', 'truth', 'dare', 'riddle', 'trivia', 'answer',
      'catfact', 'insult', 'pickupline', 'emix', 'emojimix',
      'hug', 'slap', 'kiss', 'kill', 'cry', 'yeet', 'animedance',
      'animekill', 'animekiss', 'animeslap', 'animesmile', 'animeyeet',
      'satorugojo', 'sukuna', 'shinobu', 'megumin', 'neko', 'waifu', 'meme'
    ]],
    ['MEDIA / LOCAL CONVERTERS', [
      'sticker', 's', 'stiker', 'toimage', 'toimg', 'toaudio', 'tomp3',
      'release', 'release2', 'react', 'delete', 'del', 'poll'
    ]],
    ['GROUP / ADMIN TECHNIQUES', [
      'tag', 'tagall', 'everyone', 'hidetag', 'tagadmin', 'groupinfo',
      'groupid', 'totalmembers', 'grouplink', 'listadmin', 'getpp',
      'getgrouppp', 'kick', 'remove', 'promote', 'demote',
      'open', 'close', 'opengc', 'closegc', 'setgroupname', 'setname',
      'setdesc', 'setppgroup', 'antilink', 'welcome', 'goodbye',
      'planetary-devestation', 'amatarasu', 'substitution', 'substitution2'
    ]],
    ['OWNER / SESSION SETTINGS', [
      'pair', 'setprefix', 'setbio', 'setpp', 'setbotpp',
      'setprofilepic', 'disk', 'online'
    ]]
  ];
  const out = [
    '╭━━━ ◈ 𝐌𝐀𝐃𝐀𝐑𝐀 𝐔𝐂𝐇𝐈𝐇𝐀 ◈ ━━━╮',
    '             𝐀𝐋𝐋 𝐌𝐄𝐍𝐔',
    '╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯',
    '',
    '「Only commands implemented in this build are listed below.」'
  ];
  for (const [title, commands] of groups) {
    out.push('', `╭─〔 ${title} 〕`);
    out.push(...commands.map(name => `│ ${PREFIX}${name}`));
    out.push('╰────────────────────────────');
  }
  out.push(
    '',
  );
  return out.join('\n');
}

function groupMenuText() {
  return `╭━━━ ◈ 𝐔𝐂𝐇𝐈𝐇𝐀 𝐆𝐑𝐎𝐔𝐏 𝐀𝐑𝐒𝐄𝐍𝐀𝐋 ◈ ━━━╮

🔴 SCAN
│ .sharingan — scan members/admin count
│ .mangeko-sh — launch the dimensional technique poll
│ .susano — manifest the group shield
│ .groupinfo / .groupid / .totalmembers
│ .listadmin / .grouplink

⚔️ SUMMON & MODERATION
│ .tagall [message] / .tagadmin / .hidetag
│ .kick @user / .amatarasu @user / .promote @user / .demote @user
│ .open / .close
│ .setgroupname [name] / .setdesc [description]
│ .setppgroup (reply to media)
│ .antilink on/off
│ .welcome on/off / .goodbye on/off
│ .planetary-devestation confirm — temporary group seal, no members removed
│ .substitution / .substitution2 — owner-only promotion

╰━━ ◈ Bot must be a group admin for actions that need it. ◈ ━━╯`;
}

let cachedMenuSticker = null;
async function videoToAnimatedSticker(videoBuffer) {
  return await new Promise((resolve, reject) => {
    const args = [
      '-hide_banner', '-loglevel', 'error', '-i', 'pipe:0',
      '-t', '2', '-an',
      '-vf', 'fps=8,scale=512:512:force_original_aspect_ratio=decrease,pad=512:512:(ow-iw)/2:(oh-ih)/2:color=0x00000000',
      '-loop', '0', '-vsync', '0', '-c:v', 'libwebp_anim',
      '-pix_fmt', 'yuva420p', '-q:v', '42', '-compression_level', '6', '-preset', 'picture',
      '-f', 'webp', 'pipe:1'
    ];
    const child = spawn(ffmpegPath, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    const stdout = [], stderr = [];
    child.stdout.on('data', chunk => stdout.push(chunk));
    child.stderr.on('data', chunk => stderr.push(chunk));
    child.on('error', reject);
    child.on('close', code => {
      const buffer = Buffer.concat(stdout);
      if (code === 0 && buffer.length > 0 && buffer.length <= 500 * 1024 &&
          buffer.subarray(0, 4).toString('ascii') === 'RIFF' && buffer.subarray(8, 12).toString('ascii') === 'WEBP') {
        resolve(buffer);
      } else {
        reject(new Error(code !== 0
          ? (Buffer.concat(stderr).toString('utf8').slice(-350) || `FFmpeg exited with ${code}`)
          : `Animated sticker output is invalid or exceeds 500 KB (${buffer.length} bytes).`));
      }
    });
    child.stdin.on('error', () => {});
    child.stdin.end(videoBuffer);
  });
}

async function sendMenu(sock, jid) {
  const caption = mainMenuText();
  const stickerPath = path.join(MEDIA_DIR, 'menu-sticker.webp');
  const videoPath = path.join(MEDIA_DIR, 'menu.mp4');
  const speechPath = path.join(MEDIA_DIR, 'madara-speech.m4a');

  // 1) Sticker first. Cache the compact animated WebP so it is not transcoded per request.
  let stickerBuffer = cachedMenuSticker;
  if (!stickerBuffer) {
    try {
      if (fs.existsSync(stickerPath)) {
        const candidate = await fs.promises.readFile(stickerPath);
        if (candidate.length >= 16 && candidate.length <= 500 * 1024 &&
            candidate.subarray(0, 4).toString('ascii') === 'RIFF' &&
            candidate.subarray(8, 12).toString('ascii') === 'WEBP') {
          stickerBuffer = candidate;
        }
      }
      if (!stickerBuffer && fs.existsSync(videoPath)) {
        const sourceVideo = await fs.promises.readFile(videoPath);
        stickerBuffer = await videoToAnimatedSticker(sourceVideo);
      }
      if (stickerBuffer && stickerBuffer.length <= 500 * 1024) cachedMenuSticker = stickerBuffer;
    } catch (error) {
      console.warn('Madara sticker preparation failed:', error?.message || error);
    }
  }

  if (stickerBuffer?.length) {
    try {
      // Do not set the nonstandard isAnimated property; the animated WebP payload
      // itself carries the animation frames and WhatsApp detects them from the file.
      await sendMessage(sock, jid, { sticker: stickerBuffer, mimetype: 'image/webp' }, {}, 0);
    } catch (error) {
      console.warn('Madara sticker send failed:', error?.message || error);
    }
  } else {
    console.warn('Madara animated sticker unavailable; continuing with the video menu.');
  }

  // 2) Keep the video, menu copy and interactive buttons together in one native-flow card.
  let videoBuffer = null;
  if (fs.existsSync(videoPath)) {
    try {
      videoBuffer = await fs.promises.readFile(videoPath);
      await sendInteractiveButtons(sock, jid, videoBuffer, caption);
    } catch (error) {
      console.warn('Interactive video menu failed:', error?.message || error);
      // Fallback keeps the video and menu text together rather than sending a separate text menu.
      if (videoBuffer) {
        try {
          await sendMessage(sock, jid, { video: videoBuffer, mimetype: 'video/mp4', caption }, {}, 0);
        } catch (videoError) {
          console.warn('Fallback menu video send failed:', videoError?.message || videoError);
          await sendMessage(sock, jid, { text: caption }, {}, 0).catch(() => {});
        }
      } else {
        await sendMessage(sock, jid, { text: caption }, {}, 0).catch(() => {});
      }
    }
  } else {
    console.warn('Madara menu video is missing: menu.mp4');
    await sendMessage(sock, jid, { text: caption }, {}, 0).catch(() => {});
  }

  // 3) The speech is deliberately sent last, after the sticker and combined video/menu card.
  if (fs.existsSync(speechPath)) {
    try {
      const speech = await fs.promises.readFile(speechPath);
      await sendMessage(sock, jid, {
        audio: speech, mimetype: 'audio/mp4', fileName: 'madara-speech.m4a', ptt: false
      }, {}, 0);
    } catch (error) {
      console.warn('Madara speech send failed:', error?.message || error);
    }
  }
}
async function sendEyeAsset(sock, jid, command, msg = null, session = null) {
  const options = {
    sharingan: {
      file: 'sharingan.jpg',
      title: '🔴 𝐒𝐇𝐀𝐑𝐈𝐍𝐆𝐀𝐍 — TOMOE SYNC COMPLETE',
      caption: '⟦ EYE PROTOCOL 01 ⟧\n◉ CHAKRA SIGNATURE: DETECTED\n◉ THREE-TOMOE TRACKING: ONLINE\n◉ GENJUTSU INTERFACE: ARMED\n\n「The eye sees what the careless mind misses.」'
    },
    'mangeko-sh': {
      file: 'mangeko-sh.png',
      title: '🌀 𝐌𝐀𝐍𝐆𝐄𝐊𝐘𝐎 — SPACE/TIME VECTOR ONLINE',
      caption: '⟦ EYE PROTOCOL 02 ⟧\n▰ TARGET VECTOR: LOCKED\n▰ SPACE-TIME DISTORTION: STAGED\n▰ REALITY ANCHOR: STABLE\n\n「Even distance bends when resolve becomes absolute.」'
    },
    susano: {
      file: 'susano.png',
      title: '🛡️ 𝐒𝐔𝐒𝐀𝐍𝐎 — AVATAR ARMOR MANIFESTED',
      caption: '⟦ AVATAR PROTOCOL 03 ⟧\n▓ CHAKRA FRAME: FORMED\n▓ ARMOR LAYERS: SYNCHRONIZED\n▓ DEFENSE GRID: ACTIVE\n\n「A will that protects becomes a force of its own.」'
    }
  };
  const item = options[command];
  const filePath = path.join(MEDIA_DIR, item.file);
  const inGroup = isGroup(jid);
  let groupMeta = null;
  if (inGroup) {
    try { groupMeta = await getGroupMetadata(sock, jid); } catch {}
  }
  const groupAction = inGroup && groupMeta
    ? `\n\n⟦ GROUP SCAN ⟧\n▰ ${groupMeta.subject || 'Shinobi group'}\n▰ Members: ${groupMeta.participants?.length || 0}\n▰ Admins: ${(groupMeta.participants || []).filter(p => ['admin', 'superadmin'].includes(p.admin)).length}\n▰ Chakra surveillance: ACTIVE`
    : '';
  if (fs.existsSync(filePath)) {
    await sendMessage(sock, jid, {
      image: fs.readFileSync(filePath),
      caption: `${item.title}\n\n${item.caption}${command === 'sharingan' ? groupAction : ''}`
    });
  } else {
    await sendMessage(sock, jid, { text: `${item.title}\n\n${item.caption}\n\n🖼️ Missing media file: ${item.file}` });
  }

  if (command === 'mangeko-sh' && inGroup) {
    try {
      const pollMessage = await sendMessage(sock, jid, {
        poll: {
          name: '🌀 𝐊𝐀𝐌𝐔𝐈 𝐃𝐈𝐌𝐄𝐍𝐒𝐈𝐎𝐍𝐀𝐋 𝐂𝐇𝐀𝐋𝐋𝐄𝐍𝐆𝐄',
          values: ['Kamui', 'Tsukuyomi', 'Amaterasu'],
          selectableCount: 1
        }
      });
      if (session && pollMessage?.key?.id) {
        trackedPolls.set(storeKey(session.id, jid, pollMessage.key.id), {
          sessionId: session.id, jid, createdAt: Date.now(), lastFingerprint: '', lastCounts: new Map(), pollUpdates: []
        });
        if (trackedPolls.size > 200) trackedPolls.delete(trackedPolls.keys().next().value);
      }
    } catch (error) {
      await sendMessage(sock, jid, { text: '🌀 The dimensional poll could not open in this group; the Mangekyō image has still been manifested.' });
    }
  }

  if (command === 'susano') {
    const videoPath = path.join(MEDIA_DIR, 'susano.mp4');
    const audioPath = path.join(MEDIA_DIR, 'susano-audio.m4a');
    if (fs.existsSync(videoPath)) {
      await sendMessage(sock, jid, {
        video: fs.readFileSync(videoPath),
        mimetype: 'video/mp4',
        caption: `╭━━〔 🛡️ 𝐒𝐔𝐒𝐀𝐍𝐎 𝐀𝐕𝐀𝐓𝐀𝐑 〕━━╮\n> Chakra armor is now manifested.${groupAction}\n╰━━ ◈ 𝐔𝐂𝐇𝐈𝐇𝐀 ◈ ━━╯`
      });
    }
    if (fs.existsSync(audioPath)) {
      await sendMessage(sock, jid, {
        audio: fs.readFileSync(audioPath),
        mimetype: 'audio/mp4',
        fileName: 'susano-audio.m4a',
        ptt: false
      });
    }
  }
}
async function getGroupMetadata(sock, jid, forceRefresh = false) {
  if (!isGroup(jid)) throw new Error('This command works inside a WhatsApp group.');
  rememberAccountAliases(sock);
  const key = normalizeJid(jid);
  const cached = groupMetadataCache.get(key);
  if (!forceRefresh && cached && Date.now() - cached.at < 8000) {
    rememberParticipantAliases(cached.value?.participants || []);
    return cached.value;
  }
  const value = await sock.groupMetadata(key);
  rememberParticipantAliases(value?.participants || []);
  groupMetadataCache.set(key, { at: Date.now(), value });
  if (groupMetadataCache.size > 400) groupMetadataCache.delete(groupMetadataCache.keys().next().value);
  return value;
}
function invalidateGroupMetadata(jid) {
  if (jid) groupMetadataCache.delete(normalizeJid(jid));
  else groupMetadataCache.clear();
}
function sameJid(a, b) {
  const aa = jidAliases(a), bb = jidAliases(b);
  for (const value of aa) if (bb.has(value)) return true;
  // Compare digits only when both IDs are phone-number JIDs. LID values are
  // opaque identifiers and must not be mistaken for phone numbers.
  const phoneA = [...aa].find(value => value.endsWith('@s.whatsapp.net') || value.endsWith('@c.us'));
  const phoneB = [...bb].find(value => value.endsWith('@s.whatsapp.net') || value.endsWith('@c.us'));
  return Boolean(phoneA && phoneB && normalNumber(phoneA) && normalNumber(phoneA) === normalNumber(phoneB));
}
function participantIdentifiers(participant = {}) {
  return [participant.id, participant.jid, participant.lid, participant.pn,
    participant.phoneNumber, participant.participantPn, participant.participantLid]
    .filter(Boolean).map(value => {
      const id = normalizeJid(value);
      return /^\+?\d{7,16}$/.test(id) ? `${id.replace(/\D/g, '')}@s.whatsapp.net` : id;
    });
}
function participantIsAdmin(participant = {}) {
  return ['admin', 'superadmin'].includes(String(participant.admin || participant.role || '').toLowerCase());
}
function participantMatchesJid(participant, jid) {
  return participantIdentifiers(participant).some(id => sameJid(id, jid));
}
async function senderIsAdmin(sock, msg, meta = null) {
  const jid = chatJidOf(msg);
  if (!isGroup(jid)) return false;
  const groupMeta = meta || await getGroupMetadata(sock, jid, true);
  const sender = senderJidOf(msg, sock);
  return (groupMeta.participants || []).some(p => participantIsAdmin(p) && participantMatchesJid(p, sender));
}
async function botIsAdmin(sock, jid, meta = null) {
  rememberAccountAliases(sock);
  const groupMeta = meta || await getGroupMetadata(sock, jid, true);
  const botIds = [sock?.user?.id, sock?.user?.lid, sock?.user?.phoneNumber].filter(Boolean);
  return (groupMeta.participants || []).some(p => participantIsAdmin(p) && botIds.some(bot => participantMatchesJid(p, bot)));
}
async function requireGroupAdmin(sock, msg, reply, needsBotAdmin = false) {
  const jid = chatJidOf(msg);
  if (!isGroup(jid)) { await reply('⚠️ This command only works in a group chat.'); return false; }
  try {
    // Force-refresh once per authorization decision so recently granted admin
    // roles and WhatsApp's LID/PN participant aliases are not hidden by cache.
    const meta = await getGroupMetadata(sock, jid, true);
    if (!await senderIsAdmin(sock, msg, meta)) { await reply('🛡️ Only group admins can use this command.'); return false; }
    if (needsBotAdmin && !await botIsAdmin(sock, jid, meta)) { await reply('🤖 Promote Madara to group admin first.'); return false; }
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
async function downloadMediaNodeBuffer(mediaNode, mediaType) {
  const stream = await downloadContentFromMessage(mediaNode, mediaType);
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}
async function getQuotedMediaBuffer(sock, quotedMessage, mediaNode, mediaType) {
  let directError = null;
  try {
    const buffer = await downloadMediaNodeBuffer(mediaNode, mediaType);
    if (buffer?.length) return buffer;
    directError = new Error('Direct media stream returned no data.');
  } catch (error) {
    directError = error;
  }
  // This fallback can ask WhatsApp to re-upload media whose CDN URL has expired.
  try {
    const buffer = await getMediaBuffer(sock, quotedMessage);
    if (buffer?.length) return buffer;
  } catch (fallbackError) {
    if (!directError) directError = fallbackError;
  }
  throw directError || new Error('Could not download the quoted media.');
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
async function commandHandler(sock, msg, session, preParsed = null) {
  const jid = chatJidOf(msg);
  if (!jid || !msg?.message) return;
  const rawText = preParsed?.rawText ?? getText(msg);
  const buttonId = preParsed?.buttonId ?? getButtonId(msg);
  const parsedCommand = preParsed ? preParsed.parsedCommand : toCommand(rawText);
  const command = buttonId || parsedCommand?.command;
  const args = buttonId ? [] : (parsedCommand?.args || []);
  if (!command) return;
  const argText = args.join(' ');
  const reply = async (text, options = {}) => sendMessage(sock, jid, { text: String(text), ...options });

  if (['substitution', 'substitution2'].includes(command)) {
    const senderNumber = normalNumber(senderJidOf(msg, sock));
    const linkedNumber = normalNumber(sock.user?.id || '');
    const isConfiguredOwner = Boolean(OWNER_NUMBER && (senderNumber === OWNER_NUMBER || (msg.key?.fromMe && linkedNumber === OWNER_NUMBER)));
    if (!isConfiguredOwner) return reply('⛔ This command is reserved for the owner configured in Render as MADARA_OWNER_NUMBER.');

    // In a group, operate on that group. In a private chat, require an explicit group JID
    // so the owner cannot accidentally run the command against an unintended group.
    const targetGroup = isGroup(jid) ? jid : String(args[0] || '').trim();
    if (!isGroup(targetGroup)) {
      return reply(`Usage in private chat: ${PREFIX}${command} 1234567890-1234567890@g.us\nIn a group, use ${PREFIX}${command} directly.`);
    }

    try {
      const meta = await getGroupMetadata(sock, targetGroup, true);
      if (!await botIsAdmin(sock, targetGroup, meta)) return reply('🤖 Madara must already be an admin in the target group.');
      const participants = meta.participants || [];
      const ownerParticipant = participants.find(person => participantMatchesJid(person, `${OWNER_NUMBER}@s.whatsapp.net`));
      if (!ownerParticipant) return reply('⚠️ Your configured owner number is not a participant in that group. Add that number to the group first.');
      if (participantIsAdmin(ownerParticipant)) {
        if (command === 'substitution2') {
          await sock.groupLeave(targetGroup);
          return reply('✅ Your owner number is already an admin. Madara has now left the target group.');
        }
        return reply('✅ Your owner number is already an admin in that group.');
      }

      await sock.groupParticipantsUpdate(targetGroup, [normalizeJid(ownerParticipant.id || ownerParticipant.jid)], 'promote');
      // Re-read metadata to verify the promotion before an optional leave.
      const refreshed = await getGroupMetadata(sock, targetGroup);
      const promoted = (refreshed.participants || []).some(person =>
        participantMatchesJid(person, `${OWNER_NUMBER}@s.whatsapp.net`) && participantIsAdmin(person)
      );
      if (!promoted) return reply('⚠️ WhatsApp did not confirm the promotion. Madara will stay in the group.');

      if (command === 'substitution2') {
        await sock.groupLeave(targetGroup);
        return reply('✅ Owner number promoted to admin and verified. Madara has left the target group.');
      }
      return reply('✅ Owner number promoted to group admin successfully. No other admins were changed.');
    } catch (error) {
      return reply(`⚠️ Substitution failed: ${String(error?.message || error).slice(0, 250)}`);
    }
  }

  if (command === 'pair') {
    if (isGroup(jid)) return reply('⚠️ For privacy, use .pair in a private chat with the bot.');
    const requester = msg.key?.fromMe ? normalNumber(sock.user?.id || '') : normalNumber(senderJidOf(msg, sock));
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
  if (command === 'lg') {
    const last = lastProverbByChat.get(jid);
    const proverb = pickRandom(PROVERBS, last);
    lastProverbByChat.set(jid, proverb);
    return reply(`╭━━━〔 🔴 𝐌𝐀𝐃𝐀𝐑𝐀'𝐒 𝐖𝐈𝐒𝐃𝐎𝐌 〕━━━╮\n\n❝ ${proverb} ❞\n\n╰━━ ◈ 𝐔𝐂𝐇𝐈𝐇𝐀 𝐏𝐑𝐎𝐕𝐄𝐑𝐁 ◈ ━━╯`);
  }
  if (['legendaryspeech', 'madaraspeech'].includes(command)) {
    const speechPath = path.join(MEDIA_DIR, 'madara-speech.m4a');
    if (fs.existsSync(speechPath)) {
      await sendMessage(sock, jid, {
        audio: fs.readFileSync(speechPath),
        mimetype: 'audio/mp4',
        fileName: 'madara-speech.m4a',
        ptt: false
      });
    }
    const proverb = pickRandom(PROVERBS, lastProverbByChat.get(jid));
    lastProverbByChat.set(jid, proverb);
    return reply(`╭━━━〔 🔴 𝐌𝐀𝐃𝐀𝐑𝐀'𝐒 𝐋𝐄𝐆𝐄𝐍𝐃𝐀𝐑𝐘 𝐒𝐏𝐄𝐄𝐂𝐇 〕━━━╮\n\n❝ ${proverb} ❞\n\n╰━━ ◈ 𝐖𝐀𝐊𝐄 𝐔𝐏 𝐓𝐎 𝐑𝐄𝐀𝐋𝐈𝐓𝐘 ◈ ━━╯`);
  }
  if (['menu', 'help', 'start'].includes(command)) return sendMenu(sock, jid);
  if (command === 'groupmenu') return reply(groupMenuText());
  if (['sharingan', 'mangeko-sh', 'mangekyo-sh', 'susano'].includes(command)) {
    const normalized = command === 'mangekyo-sh' ? 'mangeko-sh' : command;
    return sendEyeAsset(sock, jid, normalized, msg, session);
  }
  if (['susano-video', 'susano-mp4', 'susano-clip'].includes(command)) {
    if (isGroup(jid)) return reply('🔒 This private scroll is only available in a direct chat with Madara.');
    const videoPath = path.join(MEDIA_DIR, 'susano.mp4');
    if (!fs.existsSync(videoPath)) return reply('⚠️ The Susano video file is missing beside server.js.');
    return sendMessage(sock, jid, {
      video: fs.readFileSync(videoPath),
      mimetype: 'video/mp4',
      caption: '╭━━〔 🛡️ 𝐒𝐔𝐒𝐀𝐍𝐎 𝐕𝐈𝐃𝐄𝐎 〕━━╮\n> The avatar of chakra manifests.\n╰━━ ◈ 𝐔𝐂𝐇𝐈𝐇𝐀 ◈ ━━╯'
    });
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
  if (command === 'jid') return reply(`⌁ CHAT JID: ${jid}\n⌁ SENDER JID: ${senderJidOf(msg, sock)}`);
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
    const first = targets[0] || senderJidOf(msg, sock);
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
  if (['release', 'release2'].includes(command)) {
    const context = findContextInfo(unwrapMessage(msg?.message || {}));
    if (!context?.quotedMessage || !context?.stanzaId) {
      return reply('Reply to media with .release or .release2 to invoke the Uchiha media scroll.');
    }
    const quoted = quotedMediaMessage(msg);
    const detected = unwrapWithViewOnceInfo(context.quotedMessage);
    const content = detected.content || {};
    const kind = [
      ['image', 'imageMessage'],
      ['video', 'videoMessage'],
      ['audio', 'audioMessage'],
      ['sticker', 'stickerMessage'],
      ['document', 'documentMessage']
    ].find(([, key]) => content[key]);
    if (!kind) return reply('That scroll contains an unsupported media type. Use an image, video, audio, sticker, or document.');
    try {
      const [type, key] = kind;
      const node = content[key] || {};
      const buffer = await getQuotedMediaBuffer(sock, quoted, node, type);
      if (!buffer?.length) return reply('The media scroll is empty or has expired.');
      const caption = '> Genjutsu of that level doesn’t work on me.';
      const mimetype = node.mimetype || ({
        image: 'image/jpeg', video: 'video/mp4', audio: 'audio/ogg; codecs=opus',
        sticker: 'image/webp', document: 'application/octet-stream'
      }[type]);
      if (command === 'release2') {
        const ext = ({ image: 'jpg', video: 'mp4', audio: 'm4a', sticker: 'webp', document: 'bin' })[type];
        await sendMessage(sock, jid, {
          document: buffer,
          mimetype,
          fileName: `uchiha-release.${ext}`,
          caption
        });
      } else if (type === 'image') {
        await sendMessage(sock, jid, { image: buffer, caption });
      } else if (type === 'video') {
        await sendMessage(sock, jid, { video: buffer, mimetype, caption });
      } else if (type === 'audio') {
        await sendMessage(sock, jid, { audio: buffer, mimetype, ptt: Boolean(node.ptt), fileName: `uchiha-release.${mimetype.includes('mpeg') ? 'mp3' : 'm4a'}` });
        await reply(caption);
      } else if (type === 'sticker') {
        await sendMessage(sock, jid, { sticker: buffer, mimetype: 'image/webp' });
        await reply(caption);
      } else {
        await sendMessage(sock, jid, { document: buffer, mimetype, fileName: node.fileName || 'uchiha-release.bin', caption });
      }
    } catch (error) {
      console.error(`.${command} failed:`, error?.message || error);
      return reply('The media scroll could not be restored. It may have expired or WhatsApp may have rejected the download.');
    }
    return;
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
      const content = unwrapMessage(source?.message || {});
      const videoNode = content.videoMessage || (content.documentMessage?.mimetype || '').startsWith('video/') ? (content.videoMessage || content.documentMessage) : null;
      const buffer = await getMediaBuffer(sock, source);
      let sticker;
      if (videoNode) {
        sticker = await videoToAnimatedSticker(buffer);
      } else if (content.imageMessage || content.stickerMessage || (content.documentMessage?.mimetype || '').startsWith('image/')) {
        sticker = await sharp(buffer, { animated: false })
          .rotate()
          .resize(512, 512, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
          .webp({ quality: 82, alphaQuality: 90, effort: 6 })
          .toBuffer();
      } else {
        return reply('Reply to an image or video and type .sticker. Videos become a short animated WebP sticker.');
      }
      if (!sticker?.length || sticker.length > 500 * 1024) return reply('⚠️ The generated sticker is empty or exceeds WhatsApp’s 500 KB animated-sticker limit. Try a shorter clip.');
      return sendMessage(sock, jid, { sticker, mimetype: 'image/webp', isAnimated: Boolean(videoNode) });
    } catch (error) { return reply(`⚠️ Sticker conversion failed: ${error?.message || error}\nReply to an image/video and type .sticker`); }
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
  if (['planetary-devestation', 'plenetary-devestation'].includes(command)) {
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    const mode = String(args[0] || '').toLowerCase();
    if (mode !== 'confirm') {
      return reply('╭━━〔 ☄️ 𝐏𝐋𝐀𝐍𝐄𝐓𝐀𝐑𝐘 𝐃𝐄𝐕𝐀𝐒𝐓𝐀𝐓𝐈𝐎𝐍 〕━━╮\nThis technique temporarily seals group messaging for 45 seconds; it does not remove members.\nConfirm with .planetary-devestation confirm\n╰━━ ◈ 𝐔𝐂𝐇𝐈𝐇𝐀 ◈ ━━╯');
    }
    try {
      await sock.groupSettingUpdate(jid, 'announcement');
      const prior = planetaryTimers.get(jid);
      if (prior) clearTimeout(prior);
      await reply('☄️ PLANETARY SEAL ACTIVATED. Only admins can send messages for 45 seconds; no members were removed.');
      const timer = setTimeout(async () => {
        planetaryTimers.delete(jid);
        try {
          await sock.groupSettingUpdate(jid, 'not_announcement');
          await sendMessage(sock, jid, { text: '🌌 The planetary seal has faded. Group messaging is restored.' });
        } catch (error) {
          console.warn('Planetary seal auto-release failed:', error?.message || error);
        }
      }, 45_000);
      timer.unref?.();
      planetaryTimers.set(jid, timer);
      return;
    } catch (error) {
      return reply(`The planetary seal could not activate: ${error?.message || error}`);
    }
  }
  if (command === 'open' || command === 'close' || command === 'opengc' || command === 'closegc') {
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    try {
      const closing = command === 'close' || command === 'closegc';
      if (!closing && planetaryTimers.has(jid)) {
        clearTimeout(planetaryTimers.get(jid));
        planetaryTimers.delete(jid);
      }
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
  if (command === 'amatarasu') {
    if (!isGroup(jid)) return reply('⚠️ .amatarasu @user can only be used in a group.');
    if (!await requireGroupAdmin(sock, msg, reply, true)) return;
    const targets = await getTargetJids(sock, msg);
    if (!targets.length) return reply('Usage: reply to one member or mention one member with .amatarasu @user.');
    const target = normalizeJid(targets[0]);
    const targetNumber = normalNumber(target);
    if (!target || !targetNumber) return reply('⚠️ I could not resolve that member. Mention them directly and try again.');
    if (sameJid(target, sock.user?.id || '') || targetNumber === OWNER_NUMBER) return reply('🛡️ Madara will not remove itself or the configured bot owner.');
    try {
      const meta = await getGroupMetadata(sock, jid);
      const participant = (meta.participants || []).find(person => sameJid(person.id || person.jid, target));
      if (!participant) return reply('⚠️ That account is not a member of this group.');
      if (participant.admin === 'superadmin') return reply('🛡️ The group owner cannot be removed by this command.');
      if (participant.admin === 'admin') return reply('🛡️ .amatarasu only removes ordinary members; it will not remove other group admins.');
      await sock.groupParticipantsUpdate(jid, [target], 'remove');
      return reply(`🔥 AMATERASU COMPLETE — @${targetNumber} was removed from the group.\n\nWhatsApp does not provide a supported bot action to erase every historical message sent by a member, so their past messages have not been bulk-deleted.`, { mentions: [target] });
    } catch (error) {
      return reply(`⚠️ Amaterasu failed: ${error?.message || error}`);
    }
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
      : (getMentionTargets(msg)[0] || normalizeJid(quotedContext?.participant || senderJidOf(msg, sock)));
    try {
      const url = await sock.profilePictureUrl(target, 'image');
      return sendMessage(sock, jid, { image: { url }, caption: `◈ PROFILE IMAGE SCAN\nTarget: ${target}` });
    } catch { return reply('⚠️ Profile picture is unavailable or hidden by that account’s privacy settings.'); }
  }
  if (['repo','repository','github'].includes(command)) return reply('🔴 MADARA UCHIHA MD\nRepository: publish this project to your own GitHub repository.\nRuntime: Node.js 20 • Baileys • local command core.');
  if (['device','app'].includes(command)) return reply(`📱 LINKED DEVICE REPORT\nAccount: ${sock.user?.id || 'Not available'}\nPlatform: WhatsApp linked device\nConnection: ${session.connection || 'unknown'}\nRuntime: Node.js ${process.version}`);
  if (['whois','userid','mine'].includes(command)) return reply(`👁️ SHINOBI IDENTITY\nName: ${msg.pushName || 'Unknown'}\nJID: ${senderJidOf(msg, sock)}\nChat: ${jid}\nType: ${isGroup(jid) ? 'Group' : 'Private'}`);
  if (['catfact'].includes(command)) return reply(`🐈 RANDOM CAT FACT\n${pickRandom(['Cats can rotate their ears independently.','A cat’s nose print is unique, much like a human fingerprint.','Cats use whiskers to help judge nearby spaces.','Many cats use their tails to signal mood and balance.'])}`);
  if (command === 'insult') return reply(`😈 FRIENDLY ROAST PROTOCOL\n${pickRandom(['Your strategy has the loading icon permanently enabled.','Even a shadow needs better directions than that.','Your brain has 99 tabs open and every one is buffering.','You bring side-quest energy to a final-boss meeting.'])}\nKeep it playful—no personal attacks.`);
  if (command === 'pickupline') return reply(`💬 SHINOBI CHARM MODULE\n${pickRandom(['Are you a Sharingan? Because you caught my attention in one glance.','You must be a rare scroll, because everyone wants to know your story.','If confidence were chakra, you would light up the whole village.'])}`);
  if (['emix','emojimix'].includes(command)) {
    const emojis = argText.split(/[+\s]+/).filter(Boolean);
    if (emojis.length < 2) return reply('Usage: .emojimix 😎 🔥\nThis local build can create a text fusion, not a custom image.');
    return reply(`🧬 EMOJI FUSION\n${emojis[0]} × ${emojis[1]} = ${emojis[0]}‍${emojis[1]}\nFusion engine: text-only mode.`);
  }
  if ([
    'hug','slap','kiss','kill','cry','yeet','animedance',
    'animekill','animekiss','animeslap','animesmile','animeyeet',
    'satorugojo','sukuna','shinobu','megumin','neko','waifu','meme'
  ].includes(command)) {
    const mentions = getMentionTargets(msg);
    const target = mentions[0] || senderJidOf(msg, sock);
    const actions = {
      hug: `🤝 @${normalNumber(target)} receives an alliance seal from the Uchiha.`,
      slap: `🫲 Anime sparring scene: @${normalNumber(target)} has been challenged to a friendly duel.`,
      kiss: `💮 A harmless anime affection reaction is sent toward @${normalNumber(target)}.`,
      kill: `⚔️ A fictional anime battle scene is declared for @${normalNumber(target)} — role-play only.`,
      cry: '🥀 Even the strongest shinobi has a moment of sorrow.',
      yeet: `🌪️ @${normalNumber(target)} has been launched into a harmless anime gag.`,
      animedance: '🕺 Madara performs the forbidden shinobi dance.',
      animekill: `⚔️ Anime battle challenge locked on @${normalNumber(target)}. Fictional role-play only.`,
      animekiss: `🌸 Anime affection reaction selected for @${normalNumber(target)}.`,
      animeslap: `🫲 Friendly sparring reaction selected for @${normalNumber(target)}.`,
      animesmile: `✨ @${normalNumber(target)} receives a rare shinobi smile.`,
      animeyeet: `🌪️ Space-time toss simulation launched for @${normalNumber(target)}.`,
      satorugojo: '🔵 INFINITY: Even a Uchiha must respect a worthy rival.',
      sukuna: '🔴 CURSED REALITY: Power without control is merely chaos.',
      shinobu: '🦋 A calm smile can conceal a precise strategy.',
      megumin: '💥 EXPLOSION STYLE: The battlefield trembles in this text-only role-play.',
      neko: '🐈 NYA — the Uchiha cat-clan has entered the battlefield.',
      waifu: '🌸 Shinobi charm scroll: loyalty, kindness, and confidence are the true rare traits.',
      meme: '😂 Uchiha meme scroll: “When you activate the Sharingan but still forget the group rules.”'
    };
    return sendMessage(sock, jid, {
      text: `╭━━〔 🔴 𝐔𝐂𝐇𝐈𝐇𝐀 𝐑𝐄𝐀𝐂𝐓𝐈𝐎𝐍 〕━━╮\n${actions[command]}\n╰━━ ◈ MADARA • LOCAL NO-API MODE ◈ ━━╯`,
      ...(mentions.length ? { mentions: [target] } : {})
    });
  }
  if (['setprefix'].includes(command)) {
    if (!isAccountOperator(msg)) return reply('👑 Only the linked account operator can change the command prefix.');
    if (!argText || argText.length !== 1 || /\s/.test(argText)) return reply(`Usage: ${PREFIX}setprefix ! (exactly one character)`);
    PREFIX = argText;
    saveBotSettings({ prefix: PREFIX });
    return reply(`✅ Prefix changed to ${PREFIX}\nNew example: ${PREFIX}menu`);
  }
  if (command === 'disk') return reply(`💾 𝐂𝐇𝐀𝐊𝐑𝐀 𝐑𝐄𝐒𝐎𝐔𝐑𝐂𝐄 𝐒𝐂𝐀𝐍\nRSS: ${(process.memoryUsage().rss / 1024 / 1024).toFixed(1)} MB\nHeap: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1)} MB`);
  if (command === 'online') return reply(`🟢 𝐒𝐄𝐒𝐒𝐈𝐎𝐍 𝐒𝐂𝐀𝐍\nConnection: ${session.connection || 'unknown'}\nUptime: ${formatUptime()}\nLinked sessions: ${[...socketSessions.values()].filter(s => s.connection === 'open').length}`);
  if (command === 'help') return reply('Use .menu to open the Uchiha menu, .allmenu for working commands, or .groupmenu for group techniques.');

}
function metaSafeGroupName(meta) { return meta?.subject || 'Group invite'; }
async function enforceAntilink(sock, msg) {
  const jid = chatJidOf(msg);
  if (!isGroup(jid) || !getGroupConfig(jid).antilink) return false;
  const body = getText(msg);
  if (!/(https?:\/\/|chat\.whatsapp\.com\/|www\.)/i.test(body)) return false;
  try {
    const meta = await getGroupMetadata(sock, jid, true);
    const sender = senderJidOf(msg, sock);
    const isAdmin = (meta.participants || []).some(p => participantIsAdmin(p) && participantMatchesJid(p, sender));
    const botAdmin = await botIsAdmin(sock, jid, meta);
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
    getMessage: async key => getStoredMessageForRetry(sessionId, key)
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
  sock.ev.on('messages.upsert', ({ messages: incoming = [] }) => {
    for (const msg of incoming) {
      try {
        if (!msg?.key?.id || !msg?.key?.remoteJid || !msg.message) continue;
        rememberJidAliases(msg.key);
        rememberAccountAliases(sock);
        storeMessage(msg, sessionId);
        const messageIdentity = storeKey(sessionId, msg.key.remoteJid, msg.key.id);
        if (session.ownMessageIds.has(messageIdentity)) continue;
        if (session.handledIds.has(messageIdentity)) continue;
        session.handledIds.add(messageIdentity);
        if (session.handledIds.size > 5000) session.handledIds.delete(session.handledIds.values().next().value);

        const jid = chatJidOf(msg);
        const rawText = getText(msg);
        const buttonId = getButtonId(msg);
        const parsedCommand = buttonId ? null : toCommand(rawText);
        const hasCommand = Boolean(buttonId || parsedCommand);
        const needsAntilink = isGroup(jid) && Boolean(getGroupConfig(jid).antilink);
        // Messages that are neither commands nor subject to enabled anti-link checks
        // are discarded immediately rather than awaiting work in large group chats.
        if (!hasCommand && !needsAntilink) continue;

        const handleError = async error => {
          console.error('Message handler error:', error?.stack || error);
          try {
            await sendMessage(sock, jid, {
              text: `⚠️ Madara command error: ${String(error?.message || error).slice(0, 250)}`
            }, {}, 0);
          } catch {}
        };

        if (hasCommand) {
          // Commands take the fast path immediately instead of waiting for a prior
          // .menu/.susano/media upload or another slow command in this group.
          // Only actual anti-link messages can reach the metadata/deletion path.
          void (async () => {
            if (needsAntilink && await enforceAntilink(sock, msg)) return;
            await commandHandler(sock, msg, session, { rawText, buttonId, parsedCommand });
          })().catch(handleError);
        } else if (needsAntilink) {
          // Serialize only non-command anti-link moderation. It no longer blocks commands.
          const queueKey = `${sessionId}|${jid}`;
          const previous = chatTaskQueues.get(queueKey) || Promise.resolve();
          const task = previous.catch(() => {}).then(() => enforceAntilink(sock, msg));
          const handled = task.catch(handleError).finally(() => {
            if (chatTaskQueues.get(queueKey) === handled) chatTaskQueues.delete(queueKey);
          });
          chatTaskQueues.set(queueKey, handled);
        }
      } catch (error) {
        console.error('Message dispatch error:', error?.stack || error);
      }
    }
  });
  sock.ev.on('messages.update', async updates => {
    for (const entry of updates || []) {
      try {
        const key = entry?.key;
        const update = entry?.update;
        if (!key?.id || !key?.remoteJid || !update?.pollUpdates?.length) continue;
        const pollKey = storeKey(sessionId, key.remoteJid, key.id);
        const tracked = trackedPolls.get(pollKey);
        if (!tracked || Date.now() - tracked.createdAt > 24 * 60 * 60 * 1000) {
          if (tracked) trackedPolls.delete(pollKey);
          continue;
        }
        const pollMessage = messages.get(pollKey);
        if (!pollMessage?.message) continue;
        for (const pollUpdate of update.pollUpdates) {
          const voteKey = pollUpdate?.pollUpdateMessageKey || {};
          const voter = normalizeJid(voteKey.participant || voteKey.participantPn || voteKey.remoteJidAlt || voteKey.remoteJid || '');
          if (voter) tracked.pollUpdates = tracked.pollUpdates.filter(oldVote => {
            const oldKey = oldVote?.pollUpdateMessageKey || {};
            const oldVoter = normalizeJid(oldKey.participant || oldKey.participantPn || oldKey.remoteJidAlt || oldKey.remoteJid || '');
            return oldVoter !== voter;
          });
          if (pollUpdate?.vote?.selectedOptions?.length) tracked.pollUpdates.push(pollUpdate);
        }
        const aggregated = getAggregateVotesInPollMessage({ message: pollMessage, pollUpdates: tracked.pollUpdates });
        const counts = (aggregated || []).map(option => ({ name: option.name || 'Unknown', count: (option.voters || []).length }));
        const fingerprint = counts.map(option => `${option.name}:${option.count}`).join('|');
        if (!counts.length || fingerprint === tracked.lastFingerprint) continue;
        const previousCounts = tracked.lastCounts;
        const newlyChosen = counts.filter(option => option.count > (previousCounts.get(option.name) || 0));
        tracked.lastCounts = new Map(counts.map(option => [option.name, option.count]));
        tracked.lastFingerprint = fingerprint;
        const totalVotes = counts.reduce((sum, option) => sum + option.count, 0);
        if (!totalVotes) continue;
        const latest = newlyChosen.length ? newlyChosen.map(option => option.name).join(', ') : '';
        const tally = counts.map((option, index) => `${index + 1}. ${option.name}: ${option.count}`).join('\n');
        const line = latest ? `Latest detected choice: ${latest}\n\n` : '';
        await sendMessage(sock, tracked.jid, {
          text: `🌀 𝐌𝐀𝐍𝐆𝐄𝐊𝐘𝐎 𝐏𝐎𝐋𝐋 𝐑𝐄𝐀𝐃\n${line}Current vote tally (${totalVotes} vote${totalVotes === 1 ? '' : 's'}):\n${tally}\n\n「The eye records every choice.」`
        }, {}, 0);
      } catch (error) {
        console.warn('Mangekyo poll update could not be read:', error?.message || error);
      }
    }
  });

  sock.ev.on('groups.update', async updates => {
    for (const update of updates || []) {
      if (!update?.id) continue;
      invalidateGroupMetadata(update.id);
      try { await getGroupMetadata(sock, update.id); } catch {}
    }
  });
  sock.ev.on('group-participants.update', async event => {
    try {
      const groupId = event?.id || '';
      invalidateGroupMetadata(groupId);
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

let keepAliveTimer = null;
function startKeepAlive() {
  clearInterval(keepAliveTimer);
  const target = process.env.RENDER_EXTERNAL_URL || process.env.KEEP_ALIVE_URL || '';
  if (!target) {
    console.log('🫀 Keep-alive: no public URL configured; Baileys WebSocket keepalive remains enabled.');
    return;
  }
  const healthUrl = target.endsWith('/health') ? target : `${target.replace(/\/$/, '')}/health`;
  keepAliveTimer = setInterval(async () => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const response = await fetch(`${healthUrl}?keepalive=1&t=${Date.now()}`, {
        method: 'GET',
        headers: { 'x-madara-keepalive': '1' },
        signal: controller.signal
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      console.log('🫀 Uchiha keep-alive heartbeat OK.');
    } catch (error) {
      console.warn('⚠️ Uchiha keep-alive heartbeat failed:', error?.message || error);
    } finally {
      clearTimeout(timeout);
    }
  }, 5 * 60 * 1000);
  keepAliveTimer.unref?.();
  console.log(`🫀 Keep-alive enabled → ${healthUrl}`);
}

await restoreSavedSessions();
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🔴 Madara Uchiha MD v${VERSION} listening on ${PORT}`);
  console.log('Pairing panel: /  |  Health: /health');
  startKeepAlive();
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