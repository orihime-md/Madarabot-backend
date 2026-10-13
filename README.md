# 🔴 Madara Uchiha MD — WhatsApp Bot

A separate Baileys multi-device bot inspired by the pairing/session lifecycle and native-flow button examples in the supplied King Bug MD source, plus the message/command patterns in the supplied Michiko MD source. It does **not** replace or alter either original project.

## Features

- Phone-number pairing code panel at `/` with status polling and cancellation.
- Saved multi-device credentials in `auth/linked/<phone-number>` and reconnect handling.
- `.menu` runs a tomoe-changing Sharingan text animation, sends `menu.mp4` from the repository root as the menu video/caption, and sends native quick replies for **ALL MENU** and **MADARA LEGENDARY SPEECH**.
- `.LG` returns a random Uchiha-style proverb, avoiding the previous proverb in the same chat where possible.
- `.sharingan`, `.mangeko-sh` (also `.mangekyo-sh`) and `.susano` each run a distinct short telemetry animation and send their corresponding artwork with a tech-style caption.
- Main and group command menus are separated: `.allmenu` versus `.groupmenu`.
- No external AI/downloader APIs or API keys are required for the commands included here.
- Group administration commands check group-admin permissions; commands that modify group membership/settings also check that Madara is a group admin.
- Sequential per-session incoming-message processing and an outgoing-message ID guard reduce duplicate handling and accidental response loops.

## Deploy to Render

1. Push this folder as its own GitHub repository. Do not place it inside either source project's existing repository.
2. Create a Render **Web Service** from the new repository. Build command: `npm install`; start command: `npm start`; Node 20 is specified in `package.json`/`render.yaml`.
3. Set `MADARA_OWNER_NUMBER` if you want `.owner` to display a particular owner number. Use digits with country code only. It is optional.
4. Open the service URL, enter the service URL in **Backend URL** if it did not autofill, enter the WhatsApp number with country code, and generate the code.
5. On the phone, open **WhatsApp → Settings → Linked devices → Link a device → Link with phone number instead**. Enter the pairing code shown on the panel.
6. Wait for the panel to display that the device is linked, then test `.alive` and `.menu` in a private chat.

### Session persistence warning

The bot writes auth credentials under `auth/linked`. A host's **ephemeral filesystem** can lose those credentials on redeploy/restart, which means WhatsApp may need to be paired again. Use a host/disk configuration with persistent storage mounted at the project's `auth` directory for reliable persistence. Never publish the `auth` folder or share its contents; it grants access to the linked account.

## Media files in the repository root

The supplied media files are included individually in the same root folder as `server.js` (no `assets/` folder is needed):

| File | Used by |
|---|---|
| `menu.mp4` | `.menu` video; the bot puts the menu text in its caption |
| `sharingan.jpg` | `.sharingan` |
| `mangeko-sh.png` | `.mangeko-sh` / `.mangekyo-sh` |
| `susano.png` | `.susano` |

Keep these exact filenames in the repository root beside `server.js`. The bot reads them directly from that location.

## Command index

### Main / private-friendly commands

- `.menu` — animated Sharingan scan, then menu video/caption and two quick-reply buttons.
- `.allmenu` — full command list.
- `.groupmenu` — group-only command guide.
- `.LG` / `.madaraspeech` — random legendary proverb/speech.
- `.sharingan`, `.mangeko-sh`, `.susano` — eye/armor activation + artwork.
- `.ping`, `.alive`, `.runtime`, `.owner`, `.jid`, `.groupid`, `.groupinfo`, `.grouplink`, `.totalmembers`, `.listadmin`.
- `.quote`, `.say text`, `.reverse text`, `.readmore text`, `.coinflip`, `.dice`, `.8ball`, `.ship`, `.truth`, `.dare`, `.riddle`, `.trivia`, `.answer`.
- `.sticker` / `.s` — reply to an image or send it with the command.
- `.toimage` — convert a replied sticker/image to PNG.
- `.toaudio` / `.tomp3` — convert replied audio/video media to MP3 using bundled FFmpeg.
- `.react 🔥` — reply to a message and react.
- `.poll Question|Option 1|Option 2` — create a group poll.

### Group commands

- `.tagall [message]`, `.hidetag [message]`, `.tagadmin [message]`
- `.kick @user`, `.promote @user`, `.demote @user` (also accepts a reply)
- `.open`, `.close`, `.setgroupname Name`, `.setdesc Description`
- `.antilink on/off`, `.setppgroup` (reply to an image)
- `.groupinfo`, `.groupid`, `.grouplink`, `.totalmembers`, `.listadmin`, `.delete` (reply to a message)
- `.welcome on/off`, `.goodbye on/off` — enable/disable join/departure notices.
- `.setbio text`, `.setpp` (reply to image), `.getpp`, `.getgrouppp` — profile utilities.

Commands requiring a group only work in groups. Moderation actions require an admin sender, and actions that need the bot to perform moderation require Madara to be a group admin. `.antilink` deletes ordinary links only when it is enabled and the bot has permission to remove the message.

## API endpoints

- `GET /health` — service/WhatsApp health summary.
- `GET /api/status` — connected session summary.
- `GET /api/pair?number=23480...` — generate or resume a pairing request.
- `GET /api/pair/status?id=...` — poll pairing state/code.
- `GET /api/pair/cancel?id=...` — cancel a pairing request.

## Notes

- This is an unofficial WhatsApp automation project using Baileys; it is not affiliated with WhatsApp/Meta. Follow WhatsApp's terms and get consent before automating group actions.
- The project intentionally does not port view-once bypass/download commands, API-dependent downloaders, AI API handlers, mass-removal features, or commands that rely on embedded API credentials.
- Keep `auth/`, any generated logs, and real phone numbers private.


## Michiko command migration

The command catalog was extracted from the supplied `DevShefzy.js` source. Existing Madara handlers remain first-class. Commands requiring external APIs, downloaders, or Michiko-only modules return an explicit migration notice instead of claiming success. Adult-content commands are intentionally excluded. Run `.allmenu` to inspect the categorized catalog and use `.groupmenu` for group-specific commands.

## Multi-session pairing
- The backend supports up to five linked WhatsApp accounts at once by default. Set `MAX_LINKED_SESSIONS` in Render to a value from `1` to `5` if you want a lower limit; the cap is intentionally clamped at five.
- Set `MADARA_OWNER_NUMBER` to the owner phone number with country code (digits only). The owner can then send `.pair 234xxxxxxxxx` to the bot in a private chat to request a pairing code for that number.
- The target account holder must personally enter the code in WhatsApp under **Settings → Linked devices → Link a device → Link with phone number**. Do not share the code with anyone else.
- The five-session cap applies to active linked accounts and pending pairing attempts. Saved sessions are restored on restart only if the deployment has persistent storage configured; an ephemeral Render filesystem can lose authentication after a restart/redeploy.


## Uchiha media + command update
All runtime media assets live beside `server.js` in the repository root:
- `menu-sticker.mp4` — converted to a non-deleting animated sticker for `.menu`
- `madara-speech.m4a` — audio sent with `.menu` and `.madaraspeech`
- `susano.mp4` — clip sent with `.susano`; `.susano-video` sends it in a private chat
- `susano-audio.m4a` — audio paired with `.susano`
- `sharingan.jpg`, `mangeko-sh.png`, `susano.png` — standalone command images

Menu buttons now use the King Bug MD native-flow method: a generated message is stored before relay.
The HTTP keep-alive heartbeat follows the King Bug MD pattern (every five minutes); platform restarts and free-plan suspension are still controlled by the host.
`.allmenu` now lists implemented local commands only. API/provider-only and incomplete migration handlers were removed from the catalogue and their fake migration replies were removed.
`.release` and `.release2` use local media download only for normal media or one-time media sent by the command user; the bot does not defeat another person's view-once privacy.
`.planetary-devestation confirm` temporarily limits group posting to admins for 45 seconds and automatically reopens the group; it does not remove members.
