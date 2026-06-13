'use strict';

/**
 * nte-matchmaker-core / server.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Combined:  Express API Gateway  +  WebSocket Lobby Server
 *         +  Matchmaker  +  AI Toxicity & Safety Filter
 * Memory target: optimised for machines with 32 GB RAM (no external DB needed)
 * ─────────────────────────────────────────────────────────────────────────────
 */

const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 1 — IN-MEMORY DATABASE  (simulated PostgreSQL / MongoDB layer)
// ═════════════════════════════════════════════════════════════════════════════

/**
 * @typedef {Object} Player
 * @property {string} id
 * @property {string} username
 * @property {number} mmr
 * @property {'offline'|'lobby'|'queue'|'ingame'|'restricted'|'banned'} status
 * @property {number} createdAt
 * @property {number} violationScore   — cumulative AI toxicity score
 * @property {number} violationCount   — total infractions recorded
 */

/**
 * @typedef {Object} ChatLog
 * @property {string} id               — unique message UUID
 * @property {string} playerId
 * @property {string} username
 * @property {string} message          — original raw text
 * @property {number} violationScore   — 0–100 score returned by AI filter
 * @property {boolean} flagged         — true if score >= VIOLATION_THRESHOLD
 * @property {string[]} triggeredTerms — which toxic keywords matched
 * @property {number} timestamp
 */

const db = (() => {
    /** @type {Map<string, Player>} username → player */
    const byUsername = new Map();
    /** @type {Map<string, Player>} id → player */
    const byId = new Map();
    /** @type {Map<string, string>} token → playerId */
    const sessions = new Map();
    /** @type {ChatLog[]} append-only chat history ring buffer (max 500 entries) */
    const chatLogs = [];
    const CHAT_LOG_MAX = 500;

    function _insert(username, mmr) {
        /** @type {Player} */
        const player = {
            id: uuidv4(),
            username,
            mmr: Math.max(0, Math.min(9999, Math.round(mmr))),
            status: 'offline',
            createdAt: Date.now(),
            violationScore: 0,
            violationCount: 0,
        };
        byUsername.set(username, player);
        byId.set(player.id, player);
        console.log(`[DB]  INSERT  player="${username}"  mmr=${player.mmr}  id=${player.id}`);
        return player;
    }

    function seed() {
        const fixtures = [
            { username: 'ArenaKing', mmr: 2850 },
            { username: 'NightBlade', mmr: 2640 },
            { username: 'StormWarden', mmr: 2510 },
            { username: 'IronClad', mmr: 2390 },
            { username: 'VoidHunter', mmr: 2200 },
            { username: 'CryptoMage', mmr: 2100 },
            { username: 'LancerX', mmr: 1980 },
            { username: 'PhoenixRise', mmr: 1850 },
            { username: 'ShadowStep', mmr: 1720 },
            { username: 'FrostEdge', mmr: 1600 },
        ];
        fixtures.forEach(f => _insert(f.username, f.mmr));
        console.log(`[DB]  Seed complete — ${fixtures.length} players ready.\n`);
    }

    async function findOrCreate(username, mmr = 1000) {
        await _delay(15);
        if (byUsername.has(username)) return byUsername.get(username);
        return _insert(username, mmr);
    }

    async function setStatus(playerId, newStatus) {
        await _delay(5);
        const p = byId.get(playerId);
        if (!p) {
            console.warn(`[DB]  setStatus — unknown id ${playerId}`);
            return null;
        }
        const prev = p.status;
        p.status = newStatus;
        console.log(`[DB]  STATUS   player="${p.username}"  ${prev} → ${newStatus}`);
        return p;
    }

    async function leaderboard(limit = 20) {
        await _delay(10);
        return [...byUsername.values()]
            .sort((a, b) => b.mmr - a.mmr)
            .slice(0, Math.min(limit, 100));
    }

    function getQueue() {
        return [...byUsername.values()].filter(p => p.status === 'queue');
    }

    async function setStatusBatch(playerIds, newStatus) {
        for (const id of playerIds) await setStatus(id, newStatus);
    }

    /**
     * Append a resolved chat log entry. Enforces ring-buffer max size.
     * @param {ChatLog} entry
     */
    function appendChatLog(entry) {
        chatLogs.push(entry);
        if (chatLogs.length > CHAT_LOG_MAX) chatLogs.shift();
        console.log(
            `[CHAT_LOG]  id=${entry.id}  player="${entry.username}"  ` +
            `flagged=${entry.flagged}  score=${entry.violationScore}  ` +
            `msg="${entry.message.slice(0, 60)}"`
        );
    }

    /** Return a shallow copy of recent chat history. */
    function getChatLogs(limit = 50) {
        return chatLogs.slice(-Math.min(limit, CHAT_LOG_MAX));
    }

    /**
     * Increment a player's running violation score and count.
     * @param {string} playerId
     * @param {number} score  — score of this single infraction (0–100)
     */
    function recordViolation(playerId, score) {
        const p = byId.get(playerId);
        if (!p) return;
        p.violationScore += score;
        p.violationCount += 1;
        console.log(
            `[DB]  VIOLATION  player="${p.username}"  ` +
            `infractions=${p.violationCount}  totalScore=${p.violationScore}`
        );
    }

    function createSession(playerId) {
        const token = `nteJWT.${crypto.randomBytes(28).toString('hex')}`;
        sessions.set(token, playerId);
        return token;
    }

    function resolveSession(token) {
        const id = sessions.get(token);
        return id ? byId.get(id) : null;
    }

    const getById = id => byId.get(id) ?? null;
    const getByUsername = username => byUsername.get(username) ?? null;
    const playerCount = () => byUsername.size;

    return {
        seed,
        findOrCreate,
        setStatus,
        setStatusBatch,
        leaderboard,
        getQueue,
        appendChatLog,
        getChatLogs,
        recordViolation,
        createSession,
        resolveSession,
        getById,
        getByUsername,
        playerCount,
    };
})();

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 2 — UTILITY HELPERS
// ═════════════════════════════════════════════════════════════════════════════

function _delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function toJSON(obj) {
    try { return JSON.stringify(obj); } catch { return '{}'; }
}

function fromJSON(raw) {
    try { return JSON.parse(raw); } catch { return null; }
}

function sendPacket(ws, payload) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(toJSON(payload));
    }
}

/** Broadcast a packet to every connected, authenticated socket. */
function broadcastAll(payload, excludeWs = null) {
    const data = toJSON(payload);
    for (const client of wss.clients) {
        if (
            client.readyState === WebSocket.OPEN &&
            client._playerId !== null &&
            client !== excludeWs
        ) {
            client.send(data);
        }
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 3 — AI TOXICITY & SAFETY FILTER
// ═════════════════════════════════════════════════════════════════════════════

/**
 * Toxic keyword registry.
 *
 * Each entry carries:
 *   pattern  — RegExp (word-boundary aware, case-insensitive)
 *   score    — violation severity contribution (0–100)
 *   category — human-readable infraction class
 *
 * The final violationScore for a message is the MAX of all matched scores
 * (not the sum), so a single hard-banned term always yields score 100
 * regardless of how many soft-warning terms also appear.
 */
const TOXIC_REGISTRY = [
    // ── Hard bans (instant score 100) ──────────────────────────────────────────
    { pattern: /\bcheat(s|ing|er|ers)?\b/i, score: 100, category: 'CHEAT_ACCUSATION' },
    { pattern: /\bhack(s|ing|er|ers)?\b/i, score: 100, category: 'HACK_ACCUSATION' },
    { pattern: /\baimbot(s)?\b/i, score: 100, category: 'CHEAT_TOOL' },
    { pattern: /\bexploit(s|ing|ed)?\b/i, score: 100, category: 'EXPLOIT_ABUSE' },
    { pattern: /\bwallhack(s)?\b/i, score: 100, category: 'CHEAT_TOOL' },
    { pattern: /\bspeedhack(s)?\b/i, score: 100, category: 'CHEAT_TOOL' },
    { pattern: /\btriggerbot(s)?\b/i, score: 100, category: 'CHEAT_TOOL' },
    { pattern: /\bduplication glitch\b/i, score: 100, category: 'EXPLOIT_ABUSE' },
    { pattern: /\bddos(ing)?\b/i, score: 100, category: 'ATTACK_THREAT' },
    { pattern: /\bip.?grab(ber)?\b/i, score: 100, category: 'ATTACK_THREAT' },
    // ── Severe toxicity (score 80) ─────────────────────────────────────────────
    { pattern: /\btoxic\b/i, score: 80, category: 'TOXIC_BEHAVIOUR' },
    { pattern: /\bragehack\b/i, score: 80, category: 'TOXIC_BEHAVIOUR' },
    { pattern: /\bintentional.?feed(ing)?\b/i, score: 80, category: 'GRIEFING' },
    { pattern: /\bgrief(ing|er|ers)?\b/i, score: 80, category: 'GRIEFING' },
    { pattern: /\btroll(ing|er|ers)?\b/i, score: 60, category: 'DISRUPTIVE' },
    // ── Moderate warnings (score 40–60) ──────────────────────────────────────
    { pattern: /\bscript.?kiddi(e|es)?\b/i, score: 60, category: 'CHEAT_REFERENCE' },
    { pattern: /\bmod.?menu\b/i, score: 60, category: 'CHEAT_REFERENCE' },
    { pattern: /\bno.?recoil\b/i, score: 60, category: 'CHEAT_REFERENCE' },
    { pattern: /\bauto.?aim\b/i, score: 60, category: 'CHEAT_REFERENCE' },
    { pattern: /\bstompk(ing)?\b/i, score: 40, category: 'POOR_SPORTSMANSHIP' },
    { pattern: /\bnoob.?stomp(ing)?\b/i, score: 40, category: 'POOR_SPORTSMANSHIP' },
];

/** Violation score threshold above which action is taken. */
const VIOLATION_THRESHOLD = 40;

/**
 * analyzePlayerMessage — simulated AI Safety Filter pipeline.
 *
 * Mimics the latency of a real LLM/classifier call (30–80 ms) while
 * performing deterministic keyword matching against TOXIC_REGISTRY.
 *
 * @param {string} message       — raw chat string from client
 * @returns {Promise<AnalysisResult>}
 *
 * @typedef {Object} AnalysisResult
 * @property {boolean}  flagged
 * @property {number}   violationScore  — 0–100
 * @property {string[]} triggeredTerms
 * @property {string[]} categories
 * @property {number}   processingMs
 */
async function analyzePlayerMessage(message) {
    const pipelineStart = Date.now();

    // — Stage 1: Normalise input ───────────────────────────────────────────────
    const normalised = message
        .toLowerCase()
        .replace(/[^\w\s]/g, ' ')   // strip punctuation
        .replace(/\s+/g, ' ')
        .trim();

    console.log(`[AI_FILTER]  ── Pipeline START ─────────────────────────────`);
    console.log(`[AI_FILTER]  Raw input     : "${message}"`);
    console.log(`[AI_FILTER]  Normalised    : "${normalised}"`);

    // Simulate tokenisation latency (Stage 1 overhead)
    await _delay(10 + Math.random() * 15);
    console.log(`[AI_FILTER]  Stage 1 ✓  Tokenisation complete`);

    // — Stage 2: Keyword pattern scan ─────────────────────────────────────────
    let maxScore = 0;
    const matchedTerms = [];
    const categories = [];

    for (const entry of TOXIC_REGISTRY) {
        const match = normalised.match(entry.pattern);
        if (match) {
            matchedTerms.push(match[0]);
            if (!categories.includes(entry.category)) categories.push(entry.category);
            if (entry.score > maxScore) maxScore = entry.score;
            console.log(
                `[AI_FILTER]    HIT  term="${match[0]}"  ` +
                `category=${entry.category}  score=${entry.score}`
            );
        }
    }

    await _delay(8 + Math.random() * 12);
    console.log(
        `[AI_FILTER]  Stage 2 ✓  Pattern scan — ` +
        `${matchedTerms.length} hit(s)  maxScore=${maxScore}`
    );

    // — Stage 3: Contextual severity resolution ────────────────────────────────
    // In a real pipeline this would call an LLM for context-aware evaluation.
    // Here we apply a slight boost if multiple distinct categories were matched,
    // simulating the model noticing compound toxicity.
    let finalScore = maxScore;
    if (categories.length > 1 && finalScore < 100) {
        finalScore = Math.min(100, finalScore + categories.length * 5);
        console.log(
            `[AI_FILTER]  Stage 3  Compound boost applied: ` +
            `${maxScore} → ${finalScore}  (${categories.length} categories)`
        );
    }

    await _delay(12 + Math.random() * 20);
    console.log(`[AI_FILTER]  Stage 3 ✓  Severity resolved  finalScore=${finalScore}`);

    const flagged = finalScore >= VIOLATION_THRESHOLD;
    const processingMs = Date.now() - pipelineStart;

    console.log(
        `[AI_FILTER]  ── Pipeline END  flagged=${flagged}  ` +
        `score=${finalScore}  elapsed=${processingMs}ms ──────────────`
    );

    return { flagged, violationScore: finalScore, triggeredTerms: matchedTerms, categories, processingMs };
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 4 — EXPRESS APP  (API Gateway)
// ═════════════════════════════════════════════════════════════════════════════

const app = express();
app.use(express.json());

app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// ── POST /api/auth/login ─────────────────────────────────────────────────────
app.post('/api/auth/login', async (req, res) => {
    try {
        const rawUsername = (req.body.username ?? '').toString().trim();
        const rawMmr = req.body.mmr;

        if (rawUsername.length < 2 || rawUsername.length > 32) {
            return res.status(400).json({ ok: false, error: 'Username must be 2–32 characters.' });
        }
        if (!/^[a-zA-Z0-9_\-]+$/.test(rawUsername)) {
            return res.status(400).json({ ok: false, error: 'Username contains invalid characters.' });
        }

        const mmr = Number.isFinite(Number(rawMmr)) ? Number(rawMmr) : 1000;
        const player = await db.findOrCreate(rawUsername, mmr);
        await db.setStatus(player.id, 'lobby');
        const token = db.createSession(player.id);

        console.log(`[AUTH]  LOGIN   player="${player.username}"  mmr=${player.mmr}  → token issued`);

        return res.status(200).json({
            ok: true,
            token,
            player: { id: player.id, username: player.username, mmr: player.mmr, status: player.status },
        });
    } catch (err) {
        console.error('[AUTH]  Login handler error:', err);
        return res.status(500).json({ ok: false, error: 'Internal server error.' });
    }
});

// ── GET /api/players/leaderboard ─────────────────────────────────────────────
app.get('/api/players/leaderboard', async (req, res) => {
    try {
        const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 100);
        const rows = await db.leaderboard(limit);

        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.setHeader('Transfer-Encoding', 'chunked');
        res.write('[');
        for (let i = 0; i < rows.length; i++) {
            const { id, username, mmr, status } = rows[i];
            res.write(toJSON({ rank: i + 1, id, username, mmr, status }));
            if (i < rows.length - 1) res.write(',');
            await _delay(2);
        }
        res.write(']');
        res.end();
        console.log(`[LEADERBOARD]  Streamed ${rows.length} players`);
    } catch (err) {
        console.error('[LEADERBOARD]  Error:', err);
        if (!res.headersSent) res.status(500).json({ ok: false, error: 'Internal server error.' });
    }
});

// ── GET /api/health ───────────────────────────────────────────────────────────
app.get('/api/health', (_req, res) => {
    res.json({
        ok: true,
        uptime: Math.round(process.uptime()),
        players: db.playerCount(),
        queueSize: db.getQueue().length,
        activeSockets: wss ? wss.clients.size : 0,
        chatMessages: db.getChatLogs(500).length,
        memoryMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
    });
});

// ── GET /api/chat/logs ────────────────────────────────────────────────────────
app.get('/api/chat/logs', (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 50, 500);
    res.json({ ok: true, logs: db.getChatLogs(limit) });
});

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 5 — HTTP SERVER (shared with WebSocket server)
// ═════════════════════════════════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;
const server = http.createServer(app);

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 6 — WEBSOCKET LOBBY SERVER
// ═════════════════════════════════════════════════════════════════════════════

/**
 * Per-socket metadata stored directly on the ws object:
 *   ws._playerId    {string|null}
 *   ws._username    {string|null}
 *   ws._isAlive     {boolean}
 *   ws._missedPings {number}
 */

const wss = new WebSocketServer({ server });

const HEARTBEAT_INTERVAL_MS = 10_000;
const MAX_MISSED_PINGS = 2;

// ── Heartbeat loop ────────────────────────────────────────────────────────────
const heartbeatTimer = setInterval(async () => {
    for (const ws of wss.clients) {
        if (ws.readyState !== WebSocket.OPEN) continue;

        if (!ws._isAlive) {
            ws._missedPings = (ws._missedPings ?? 0) + 1;
            console.warn(
                `[WS]  HEARTBEAT MISS  player="${ws._username ?? 'unknown'}"  ` +
                `missed=${ws._missedPings}/${MAX_MISSED_PINGS}`
            );
            if (ws._missedPings >= MAX_MISSED_PINGS) {
                console.warn(`[WS]  DROP (silent disconnect)  player="${ws._username ?? 'unknown'}"`);
                await _handleDisconnect(ws);
                ws.terminate();
                continue;
            }
        }

        ws._isAlive = false;
        ws._missedPings = ws._missedPings ?? 0;
        ws.ping();
    }
}, HEARTBEAT_INTERVAL_MS);

heartbeatTimer.unref();

// ── Connection handler ────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
    ws._playerId = null;
    ws._username = null;
    ws._isAlive = true;
    ws._missedPings = 0;

    const remoteIp = req.socket.remoteAddress ?? 'unknown';
    console.log(`[WS]  OPEN    ip=${remoteIp}  total=${wss.clients.size}`);

    sendPacket(ws, { event: 'CONNECTED', message: 'Socket open. Send JOIN_LOBBY to authenticate.' });

    ws.on('pong', () => {
        ws._isAlive = true;
        ws._missedPings = 0;
    });

    ws.on('message', async (rawData) => {
        const packet = fromJSON(rawData.toString());

        if (!packet || typeof packet.action !== 'string') {
            sendPacket(ws, { event: 'ERROR', message: 'Malformed packet. Expected { action, ... }.' });
            return;
        }

        const { action, token } = packet;

        switch (action) {

            // ── JOIN_LOBBY ─────────────────────────────────────────────────────────
            case 'JOIN_LOBBY': {
                if (!token) {
                    sendPacket(ws, { event: 'ERROR', message: 'JOIN_LOBBY requires a token field.' });
                    return;
                }
                const player = db.resolveSession(token);
                if (!player) {
                    sendPacket(ws, { event: 'AUTH_FAIL', message: 'Invalid or expired session token.' });
                    return;
                }

                ws._playerId = player.id;
                ws._username = player.username;

                await db.setStatus(player.id, 'lobby');

                console.log(`[WS]  JOIN_LOBBY  player="${player.username}"  mmr=${player.mmr}`);

                sendPacket(ws, {
                    event: 'LOBBY_JOINED',
                    message: `Welcome to the lobby, ${player.username}!`,
                    player: { id: player.id, username: player.username, mmr: player.mmr, status: 'lobby' },
                });
                break;
            }

            // ── ENTER_QUEUE ───────────────────────────────────────────────────────
            case 'ENTER_QUEUE': {
                const player = _requireAuth(ws);
                if (!player) return;

                if (player.status === 'restricted' || player.status === 'banned') {
                    console.log(`[WS]  ENTER_QUEUE blocked — player="${player.username}"  status=${player.status}`);
                    sendPacket(ws, {
                        event: 'QUEUE_BLOCKED',
                        message: `Queue access is ${player.status}. Chat violations prevent matchmaking.`,
                    });
                    return;
                }
                if (player.status === 'queue') {
                    sendPacket(ws, { event: 'NOTICE', message: 'You are already in queue.' });
                    return;
                }
                if (player.status === 'ingame') {
                    sendPacket(ws, { event: 'NOTICE', message: 'You are already in a match.' });
                    return;
                }

                await db.setStatus(player.id, 'queue');
                console.log(`[WS]  ENTER_QUEUE  player="${player.username}"  mmr=${player.mmr}`);

                sendPacket(ws, {
                    event: 'QUEUE_ENTERED',
                    message: 'You have entered the matchmaking queue.',
                    queueSize: db.getQueue().length,
                });
                break;
            }

            // ── LEAVE_QUEUE ───────────────────────────────────────────────────────
            case 'LEAVE_QUEUE': {
                const player = _requireAuth(ws);
                if (!player) return;

                if (player.status !== 'queue') {
                    sendPacket(ws, { event: 'NOTICE', message: 'You are not currently in queue.' });
                    return;
                }

                await db.setStatus(player.id, 'lobby');
                console.log(`[WS]  LEAVE_QUEUE  player="${player.username}"`);
                sendPacket(ws, { event: 'QUEUE_LEFT', message: 'You have left the queue.' });
                break;
            }

            // ── SEND_MESSAGE ──────────────────────────────────────────────────────
            case 'SEND_MESSAGE': {
                const player = _requireAuth(ws);
                if (!player) return;

                const rawMessage = (packet.message ?? '').toString().trim();

                if (rawMessage.length === 0) {
                    sendPacket(ws, { event: 'ERROR', message: 'Message cannot be empty.' });
                    return;
                }
                if (rawMessage.length > 300) {
                    sendPacket(ws, { event: 'ERROR', message: 'Message exceeds 300-character limit.' });
                    return;
                }

                // Gate: already restricted/banned players cannot send chat.
                if (player.status === 'restricted' || player.status === 'banned') {
                    console.log(
                        `[CHAT]  BLOCKED  player="${player.username}"  ` +
                        `status=${player.status}  msg="${rawMessage.slice(0, 40)}"`
                    );
                    sendPacket(ws, {
                        event: 'SYSTEM_WARNING',
                        message: 'You are restricted from sending chat messages due to prior violations.',
                    });
                    return;
                }

                console.log(
                    `[CHAT]  RECEIVED  player="${player.username}"  ` +
                    `msg="${rawMessage.slice(0, 60)}"  — routing to AI Safety Filter…`
                );

                // ── AI Safety Filter pipeline ──────────────────────────────────────
                let analysis;
                try {
                    analysis = await analyzePlayerMessage(rawMessage);
                } catch (filterErr) {
                    console.error('[AI_FILTER]  Pipeline threw unexpectedly:', filterErr);
                    // Fail-open: allow message but log the failure.
                    analysis = { flagged: false, violationScore: 0, triggeredTerms: [], categories: [], processingMs: 0 };
                }

                // ── Build and persist chat log entry ──────────────────────────────
                /** @type {ChatLog} */
                const logEntry = {
                    id: uuidv4(),
                    playerId: player.id,
                    username: player.username,
                    message: rawMessage,
                    violationScore: analysis.violationScore,
                    flagged: analysis.flagged,
                    triggeredTerms: analysis.triggeredTerms,
                    timestamp: Date.now(),
                };
                db.appendChatLog(logEntry);

                // ── Branch: flagged ───────────────────────────────────────────────
                if (analysis.flagged) {
                    db.recordViolation(player.id, analysis.violationScore);

                    // Determine penalty tier based on cumulative violation count.
                    const p = db.getById(player.id);
                    const newStatus = p.violationCount >= 3 ? 'banned' : 'restricted';

                    await db.setStatus(player.id, newStatus);

                    console.log(
                        `[AI_FILTER]  ACTION  player="${player.username}"  ` +
                        `newStatus=${newStatus}  score=${analysis.violationScore}  ` +
                        `terms=[${analysis.triggeredTerms.join(', ')}]`
                    );

                    // Notify the offending player.
                    sendPacket(ws, {
                        event: 'SYSTEM_WARNING',
                        message: 'Chat violation detected. Your queue access is temporarily restricted.',
                        violationScore: analysis.violationScore,
                        triggeredTerms: analysis.triggeredTerms,
                        categories: analysis.categories,
                        newStatus,
                        processingMs: analysis.processingMs,
                    });

                    // Broadcast a sanitised moderation notice to all other lobby players.
                    broadcastAll({
                        event: 'MODERATION_ACTION',
                        message: `[SYSTEM] Player "${player.username}" has been ${newStatus} for chat violations.`,
                        username: player.username,
                        newStatus,
                    }, ws);

                    return; // do NOT broadcast the original message
                }

                // ── Branch: clean — broadcast to all lobby clients ────────────────
                console.log(
                    `[CHAT]  CLEAN  player="${player.username}"  ` +
                    `score=${analysis.violationScore}  broadcasting to lobby…`
                );

                const chatPayload = {
                    event: 'LOBBY_CHAT',
                    messageId: logEntry.id,
                    username: player.username,
                    message: rawMessage,
                    timestamp: logEntry.timestamp,
                    processingMs: analysis.processingMs,
                };

                // Send to the sender first so they see their own message immediately.
                sendPacket(ws, chatPayload);
                // Then broadcast to everyone else in the lobby.
                broadcastAll(chatPayload, ws);
                break;
            }

            default: {
                sendPacket(ws, { event: 'ERROR', message: `Unknown action: "${action}".` });
            }
        }
    });

    ws.on('close', async (code, reason) => {
        console.log(
            `[WS]  CLOSE   player="${ws._username ?? 'unauthenticated'}"  ` +
            `code=${code}  reason="${reason?.toString() ?? ''}"`
        );
        await _handleDisconnect(ws);
    });

    ws.on('error', (err) => {
        console.error(`[WS]  ERROR   player="${ws._username ?? 'unknown'}"  ${err.message}`);
    });
});

async function _handleDisconnect(ws) {
    if (ws._playerId) {
        const player = db.getById(ws._playerId);
        // Do NOT clear restricted/banned status on disconnect — penalty persists.
        if (player && player.status !== 'offline' && player.status !== 'restricted' && player.status !== 'banned') {
            await db.setStatus(ws._playerId, 'offline');
        } else if (player && (player.status === 'restricted' || player.status === 'banned')) {
            console.log(`[WS]  DISCONNECT  player="${player.username}"  penalty status "${player.status}" preserved`);
        }
    }
}

function _requireAuth(ws) {
    if (!ws._playerId) {
        sendPacket(ws, { event: 'AUTH_FAIL', message: 'Send JOIN_LOBBY with a valid token first.' });
        return null;
    }
    const player = db.getById(ws._playerId);
    if (!player) {
        sendPacket(ws, { event: 'ERROR', message: 'Player record not found.' });
        return null;
    }
    return player;
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 7 — MATCHMAKER LOOP
// ═════════════════════════════════════════════════════════════════════════════

const MATCH_SIZE = 4;
const MATCHMAKER_TICK = 2_000;

function _buildSocketIndex() {
    /** @type {Map<string, WebSocket>} */
    const index = new Map();
    for (const ws of wss.clients) {
        if (ws._playerId && ws.readyState === WebSocket.OPEN) {
            index.set(ws._playerId, ws);
        }
    }
    return index;
}

const matchmakerTimer = setInterval(async () => {
    try {
        const queue = db.getQueue();
        if (queue.length < MATCH_SIZE) return;

        queue.sort((a, b) => a.mmr - b.mmr);
        const socketIndex = _buildSocketIndex();

        let i = 0;
        while (i + MATCH_SIZE <= queue.length) {
            const group = queue.slice(i, i + MATCH_SIZE);
            const matchId = `MATCH_${uuidv4()}`;
            const playerIds = group.map(p => p.id);
            const playerNames = group.map(p => p.username);
            const avgMmr = Math.round(group.reduce((s, p) => s + p.mmr, 0) / MATCH_SIZE);

            await db.setStatusBatch(playerIds, 'ingame');

            console.log(
                `[MATCHMAKER]  MATCH_FOUND  id=${matchId}  ` +
                `players=[${playerNames.join(', ')}]  avgMmr=${avgMmr}`
            );

            const payload = {
                event: 'MATCH_FOUND',
                matchId,
                players: group.map(p => ({ id: p.id, username: p.username, mmr: p.mmr })),
                avgMmr,
                message: `Match found! Game ID: ${matchId}`,
            };

            for (const id of playerIds) {
                const ws = socketIndex.get(id);
                if (ws) {
                    sendPacket(ws, payload);
                } else {
                    console.warn(`[MATCHMAKER]  No open socket for player id=${id} — skipping broadcast`);
                }
            }

            i += MATCH_SIZE;
        }
    } catch (err) {
        console.error('[MATCHMAKER]  Tick error:', err);
    }
}, MATCHMAKER_TICK);

matchmakerTimer.unref();

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 8 — START
// ═════════════════════════════════════════════════════════════════════════════

db.seed();

server.listen(PORT, () => {
    console.log('╔══════════════════════════════════════════════════════╗');
    console.log(`║  NTE Matchmaker Core — listening on port ${PORT}         ║`);
    console.log('║  Dashboard  →  http://localhost:3000                 ║');
    console.log('║  Health     →  http://localhost:3000/api/health      ║');
    console.log('║  WS         →  ws://localhost:3000                   ║');
    console.log('╚══════════════════════════════════════════════════════╝');
});

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function shutdown(signal) {
    console.log(`\n[SERVER]  ${signal} received — shutting down gracefully…`);
    clearInterval(heartbeatTimer);
    clearInterval(matchmakerTimer);
    for (const ws of wss.clients) ws.close(1001, 'Server shutting down');
    wss.close(() => {
        server.close(() => {
            console.log('[SERVER]  HTTP server closed. Goodbye.');
            process.exit(0);
        });
    });
    setTimeout(() => { console.error('[SERVER]  Forced exit after timeout.'); process.exit(1); }, 5_000).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', err => console.error('[UNCAUGHT]', err));
process.on('unhandledRejection', (r) => console.error('[UNHANDLED]', r));