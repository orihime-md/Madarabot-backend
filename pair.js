import express from 'express';

const router = express.Router();

// ─────────────────────────────────────────────────────────────────────────────
// Pairing registry.
//
// A pairing is a *long-lived session* that stays alive until it reaches a
// terminal state:  open (success) | failed | cancelled | expired.
//
// Everything else is "in flight" and keeps the session alive, including
// `restarting` (WhatsApp dropped the pre-auth socket, the runtime is fetching a
// fresh code) and `linking` (the phone accepted the code, WhatsApp is finishing
// the handshake and will close the socket once with 515 before it opens).
// ─────────────────────────────────────────────────────────────────────────────

const TERMINAL = new Set(['open', 'failed', 'cancelled', 'expired']);

const FIRST_CODE_TIMEOUT_MS = Number(process.env.PAIRING_FIRST_CODE_TIMEOUT_MS || 90_000);
// Sliding window: any sign of life from the WhatsApp socket pushes this out.
const IDLE_TTL_MS = Number(process.env.PAIRING_IDLE_TTL_MS || 15 * 60_000);
// Absolute ceiling so a stuck session can never live forever.
const HARD_TTL_MS = Number(process.env.PAIRING_HARD_TTL_MS || 45 * 60_000);
const KEEP_FINISHED_MS = 30 * 60_000;
const MAX_RECORDS = 100;

let controller = null;
const pairings = new Map();       // id -> pairing
const activeByNumber = new Map(); // number -> id (only non-terminal)

const withTimeout = (promise, ms, message) => {
    let timer;
    return Promise.race([
        promise,
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(message)), ms); })
    ]).finally(() => clearTimeout(timer));
};

export function setPairingController(nextController) {
    controller = nextController;
}

function normalizeNumber(value) {
    return String(value || '').replace(/\D/g, '');
}

function newPairingId() {
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function isTerminal(pairing) {
    return TERMINAL.has(pairing.status);
}

function touch(pairing) {
    const now = Date.now();
    pairing.updatedAt = now;
    pairing.expiresAt = Math.min(now + IDLE_TTL_MS, pairing.createdAt + HARD_TTL_MS);
}

function finish(pairing, status, error = null) {
    pairing.status = status;
    pairing.updatedAt = Date.now();
    pairing.finishedAt = Date.now();
    if (error) pairing.error = error;
    if (activeByNumber.get(pairing.number) === pairing.id) activeByNumber.delete(pairing.number);
}

function publicView(pairing) {
    const now = Date.now();
    return {
        ok: true,
        pairingId: pairing.id,
        status: pairing.status,
        terminal: isTerminal(pairing),
        code: pairing.code,
        codeVersion: pairing.codeVersion,   // bumps every time a NEW code is issued
        codeIssuedAt: pairing.codeIssuedAt,
        refreshes: pairing.refreshes,       // how many times the code had to be regenerated
        message: pairing.message,
        error: pairing.error,
        ageSeconds: Math.floor((now - pairing.createdAt) / 1000),
        updatedSecondsAgo: Math.floor((now - pairing.updatedAt) / 1000),
        expiresInSeconds: isTerminal(pairing) ? 0 : Math.max(0, Math.floor((pairing.expiresAt - now) / 1000)),
        statusUrl: `/api/pair/status?id=${encodeURIComponent(pairing.id)}`
    };
}

function prune() {
    const now = Date.now();
    for (const [id, pairing] of pairings) {
        if (isTerminal(pairing) && now - (pairing.finishedAt || pairing.updatedAt) > KEEP_FINISHED_MS) {
            pairings.delete(id);
        }
    }
    if (pairings.size > MAX_RECORDS) {
        for (const [id, pairing] of pairings) {
            if (pairings.size <= MAX_RECORDS) break;
            if (isTerminal(pairing)) pairings.delete(id);
        }
    }
}

async function cancelInController(pairing, reason) {
    try {
        await controller?.cancelPairing?.(pairing.id, reason);
    } catch (error) {
        console.warn(`⚠️ cancelPairing(${pairing.id}) failed:`, error?.message || error);
    }
}

// Expire sessions only after a long stretch of silence (see IDLE_TTL_MS).
const sweeper = setInterval(() => {
    const now = Date.now();
    for (const pairing of pairings.values()) {
        if (!isTerminal(pairing) && now > pairing.expiresAt) {
            console.log(`⌛ Pairing session ${pairing.id} expired after inactivity.`);
            finish(pairing, 'expired', 'Pairing session expired. Generate a new code.');
            cancelInController(pairing, 'expired');
        }
    }
    prune();
}, 15_000);
sweeper.unref?.();

router.get('/api/pair', async (req, res) => {
    const number = normalizeNumber(req.query.number);

    if (!number) {
        return res.status(400).json({ ok: false, error: 'Phone number is required.' });
    }
    if (number.length < 10 || number.length > 15) {
        return res.status(400).json({
            ok: false,
            error: 'Invalid phone number. Use country code and digits only.'
        });
    }
    if (typeof controller?.startPairing !== 'function') {
        return res.status(503).json({ ok: false, error: 'WhatsApp connection manager is not ready yet.' });
    }

    // Idempotent: re-requesting while a session is alive RESUMES it instead of
    // killing it or returning a conflict. (Page refresh, double click, retry.)
    const existingId = activeByNumber.get(number);
    const existing = existingId ? pairings.get(existingId) : null;
    if (existing && !isTerminal(existing)) {
        if (existing.code) {
            touch(existing);
            console.log(`♻️ Resuming pairing session ${existing.id} for ${number}`);
            return res.json({ ...publicView(existing), resumed: true });
        }
        return res.status(409).json({
            ok: false,
            error: 'A pairing session for this number is still starting. Wait a few seconds and try again.',
            pairingId: existing.id
        });
    }

    const id = newPairingId();
    const now = Date.now();
    const pairing = {
        id,
        number,
        status: 'requesting',
        code: null,
        codeVersion: 0,
        codeIssuedAt: null,
        refreshes: 0,
        message: 'Starting a WhatsApp connection…',
        error: null,
        createdAt: now,
        updatedAt: now,
        finishedAt: null,
        expiresAt: now + IDLE_TTL_MS
    };
    pairings.set(id, pairing);
    activeByNumber.set(number, id);
    prune();
    console.log(`🟡 Pairing session ${id} created for ${number}`);

    try {
        // The controller resolves with the FIRST code. After that the session
        // lives on in the background and keeps itself valid by issuing fresh
        // codes (reported through updatePairingStatus) until WhatsApp links.
        const firstCode = await withTimeout(
            controller.startPairing(number, id),
            FIRST_CODE_TIMEOUT_MS,
            'Pairing code request timed out. Please try again.'
        );

        if (isTerminal(pairing)) {
            return res.status(409).json({ ok: false, error: pairing.error || 'Pairing request was cancelled.' });
        }

        const code = String(firstCode || '').trim().toUpperCase();
        if (!code) throw new Error('WhatsApp did not return a pairing code.');
        applyCode(pairing, code);
        if (pairing.status === 'requesting') pairing.status = 'code_ready';
        touch(pairing);

        console.log(`🔑 Pairing code generated for session ${id}.`);
        return res.json(publicView(pairing));
    } catch (error) {
        console.error('❌ Pairing error:', error?.stack || error);
        if (!isTerminal(pairing)) finish(pairing, 'failed', error?.message || 'Pairing failed.');
        await cancelInController(pairing, 'failed');
        return res.status(500).json({ ok: false, error: pairing.error });
    }
});

router.get('/api/pair/status', (req, res) => {
    const id = String(req.query.id || '');
    const pairing = pairings.get(id);
    if (!id || !pairing) {
        return res.status(404).json({ ok: false, error: 'Pairing session not found or expired.' });
    }
    // Polling the status is itself proof the user is still on the page.
    if (!isTerminal(pairing)) {
        pairing.expiresAt = Math.min(Date.now() + IDLE_TTL_MS, pairing.createdAt + HARD_TTL_MS);
    }
    res.set('Cache-Control', 'no-store');
    return res.json(publicView(pairing));
});

router.get('/api/pair/cancel', async (req, res) => {
    const pairing = pairings.get(String(req.query.id || ''));
    if (!pairing) return res.status(404).json({ ok: false, error: 'Pairing session not found.' });
    if (!isTerminal(pairing)) {
        finish(pairing, 'cancelled', 'Pairing cancelled.');
        await cancelInController(pairing, 'cancelled');
    }
    return res.json(publicView(pairing));
});

function applyCode(pairing, code) {
    if (pairing.code && pairing.code !== code) pairing.refreshes += 1;
    if (pairing.code !== code) {
        pairing.code = code;
        pairing.codeVersion += 1;
        pairing.codeIssuedAt = Date.now();
    }
}

const DEFAULT_MESSAGES = {
    requesting: 'Starting a WhatsApp connection…',
    ready: 'WhatsApp connection is ready…',
    connecting: 'Connecting to WhatsApp…',
    code_ready: 'Pairing code active. Enter it on WhatsApp → Linked devices → Link with phone number instead.',
    restarting: 'WhatsApp reset the connection. Getting a fresh code — keep this page open…',
    linking: 'Code accepted. Finishing the link with WhatsApp…',
    open: 'Device paired successfully.'
};

// Called by sever.js whenever the WhatsApp socket behind a pairing changes.
export function updatePairingStatus(status, details = {}, pairingId = '') {
    if (!pairingId) return;
    const pairing = pairings.get(pairingId);
    if (!pairing) return;
    // A finished session never comes back to life through a late socket event.
    if (isTerminal(pairing)) return;

    if (details.code) applyCode(pairing, String(details.code).trim().toUpperCase());
    if (details.error) pairing.error = details.error;
    pairing.message = details.message || DEFAULT_MESSAGES[status] || pairing.message;

    if (TERMINAL.has(status)) {
        finish(pairing, status, details.error || null);
        console.log(status === 'open'
            ? `🟢 Pairing session ${pairing.id} completed.`
            : `🔴 Pairing session ${pairing.id}: ${status}${details.error ? ` (${details.error})` : ''}`);
        return;
    }

    // Everything non-terminal keeps the session alive.
    pairing.status = status;
    touch(pairing);
    if (status === 'restarting') pairing.error = null; // transient, not an error
}

export function getActivePairing(id) {
    return pairings.get(String(id || '')) || null;
}

export default router;
