/**
 * Crimson Harbor — game server.
 *
 * Responsibilities (all with Node built-ins only, zero npm deps):
 *   1. Static file hosting for the game client (index.html, js/, css/).
 *      - ES-module MIME types so the browser can load the game directly.
 *      - Directory-traversal protection + safe path resolution.
 *   2. Multiplayer matchmaking + relay over raw WebSocket (RFC 6455 subset
 *      implemented by hand in src/net/ws-socket.js):
 *      - Rooms with 6-char friend codes (no external service required).
 *      - Server-authoritative validation: rate limits, position sanity,
 *        name filtering, coop-mission state arbitration.
 *
 * Run:  npm start   →  http://localhost:8080
 */

import http from 'node:http';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { WsSocket } from './src/net/ws-socket.js';
import { validateMove } from './src/net/validation.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 8080);
const VERBOSE = process.argv.includes('--verbose');

/* ------------------------------------------------------------------ */
/* Static file serving                                                 */
/* ------------------------------------------------------------------ */

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.md': 'text/markdown; charset=utf-8',
  '.webm': 'video/webm',
  '.woff2': 'font/woff2',
};

const ROOT = __dirname; // serve project root (index.html + /src + vendored three.js)

/** Resolve a request path safely inside /public, guarding against traversal. */
function safePath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split('?')[0]);
  const rel = decoded === '/' ? '/index.html' : decoded;
  const abs = path.normalize(path.join(ROOT, rel));
  if (!abs.startsWith(ROOT)) return null; // e.g. "..\\..\\secret"
  return abs;
}

async function serveStatic(req, res) {
  try {
    const file = safePath(req.url);
    if (!file) {
      res.writeHead(403).end('Forbidden');
      return;
    }
    let data;
    try {
      data = await fs.readFile(file);
    } catch {
      // SPA-friendly fallback: unknown non-asset routes get index.html
      if (!path.extname(file)) {
        data = await fs.readFile(path.join(ROOT, 'index.html'));
      } else {
        res.writeHead(404).end('Not found');
        return;
      }
    }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=60',
    });
    res.end(data);
  } catch (err) {
    console.error('[static] error serving', req.url, err.message);
    try { res.writeHead(500).end('Server error'); } catch { /* already sent */ }
  }
}

/* ------------------------------------------------------------------ */
/* Multiplayer rooms                                                   */
/* ------------------------------------------------------------------ */

/**
 * A room = one shared open world. Host-authoritative for coop missions,
 * server-authoritative for everything that touches integrity.
 */
class Room {
  constructor(code, hostName) {
    this.code = code;
    this.createdAt = Date.now();
    this.clients = new Map(); // clientId -> { socket, name, state, lastMsgAt, tokens }
    this.hostClientId = null; // assigned when the host registers
    this.hostName = hostName || 'Host';
    this.missionState = {
      active: false,
      missionId: null,
      objective: null,
      progress: {},
      ready: {},
    };
  }

  broadcast(type, data, exceptId = null) {
    const packet = JSON.stringify({ type, ...data });
    for (const [id, c] of this.clients) {
      if (id === exceptId) continue;
      try { c.socket.sendText(packet); } catch { /* dead socket cleaned elsewhere */ }
    }
  }

  sendTo(id, type, data) {
    const c = this.clients.get(id);
    if (!c) return;
    try { c.socket.sendText(JSON.stringify({ type, ...data })); } catch { /* ignore */ }
  }

  roster() {
    const list = [];
    for (const [id, c] of this.clients) {
      list.push({ id, name: c.name, score: c.state?.score ?? 0, isHost: id === this.hostClientId });
    }
    return list;
  }

  isEmpty() { return this.clients.size === 0; }
}

const rooms = new Map(); // code -> Room
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I — human-friendly codes

function makeRoomCode() {
  for (let attempt = 0; attempt < 20; attempt++) {
    let code = '';
    for (let i = 0; i < 6; i++) code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(code)) return code;
  }
  return 'R' + Date.now().toString(36).toUpperCase().slice(-5);
}

setInterval(() => { // garbage-collect rooms idle > 15 min
  const now = Date.now();
  for (const [code, room] of rooms) {
    let newest = 0;
    for (const c of room.clients.values()) newest = Math.max(newest, c.lastMsgAt);
    if (room.isEmpty() || now - newest > 15 * 60 * 1000) {
      rooms.delete(code);
      if (VERBOSE) console.log('[rooms] reaped', code);
    }
  }
}, 60_000);

/* ------------------------------------------------------------------ */
/* WebSocket upgrade handling                                          */
/* ------------------------------------------------------------------ */

const httpServer = http.createServer(serveStatic);

httpServer.on('upgrade', (req, socket, head) => {
  try {
    if (String(req.headers.upgrade || '').toLowerCase() !== 'websocket') {
      socket.destroy();
      return;
    }
    WsSocket.accept(req, socket, head, (ws) => handleSocket(ws));
  } catch (err) {
    console.error('[ws] upgrade failed:', err.message);
    try { socket.destroy(); } catch { /* ignore */ }
  }
});

const MAX_PACKET = 4096;                       // bytes — anything larger is abuse
const MOVE_RATE = 20;                          // move updates / second allowed

function handleSocket(ws) {
  let clientId = null;
  let room = null;
  let moveBudget = MOVE_RATE;
  let lastRefill = Date.now();
  let joined = false;
  let name = 'Player';
  let charState = null; // last validated { x, z, heading }

  const close = (why) => {
    try { ws.close(); } catch { /* ignore */ }
    if (room && clientId) {
      room.clients.delete(clientId);
      room.broadcast('playerLeft', { id: clientId, reason: why });
      room.broadcast('roster', { players: room.roster() });
      if (VERBOSE) console.log(`[ws] ${clientId} left ${room.code} (${why})`);
    }
  };

  ws.on('message', (buf) => {
    if (buf.length > MAX_PACKET) return close('packet too large');
    let msg;
    try { msg = JSON.parse(buf.toString('utf8')); } catch { return close('bad json'); }
    if (typeof msg !== 'object' || msg === null || typeof msg.type !== 'string') return close('bad packet');

    /* --- handshake: join or create a room ------------------------- */
    if (msg.type === 'join' && !joined) {
      name = String(msg.name || 'Player').trim().slice(0, 16) || 'Player';
      const action = msg.action === 'create' ? 'create' : 'join';
      const code = String(msg.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);

      if (action === 'create') {
        room = new Room(makeRoomCode(), name);
        rooms.set(room.code, room);
      } else {
        if (code.length !== 6) { ws.sendText(JSON.stringify({ type: 'joinError', error: 'Enter a 6-character code.' })); return; }
        room = rooms.get(code);
        if (!room) { ws.sendText(JSON.stringify({ type: 'joinError', error: 'Room not found — check the code.' })); return; }
        if (room.clients.size >= 8) { ws.sendText(JSON.stringify({ type: 'joinError', error: 'Room is full (8 players max).' })); return; }
      }
      joined = true;
      // id is assigned by the server, never trusted from the client
      clientId = 'p' + Math.random().toString(36).slice(2, 9);
      room.clients.set(clientId, { socket: ws, name, state: { x: 0, z: 0 }, lastMsgAt: Date.now(), tokens: moveBudget });
      if (room.hostClientId === null) room.hostClientId = clientId;

      ws.sendText(JSON.stringify({
        type: 'joined', id: clientId, code: room.code, isHost: clientId === room.hostClientId,
        missionState: room.missionState, roster: room.roster(),
      }));
      room.broadcast('playerJoined', { id: clientId, name }, clientId);
      room.broadcast('roster', { players: room.roster() });
      if (VERBOSE) console.log(`[ws] ${name}(${clientId}) joined room ${room.code}`);
      return;
    }
    if (!joined) return close('join first');

    /* --- game messages (rate + validity checked) ------------------- */
    room.clients.get(clientId).lastMsgAt = Date.now();

    if (msg.type === 'move') {
      // token-bucket rate limit: 20 updates/s with small burst allowance
      const now = Date.now();
      moveBudget = Math.min(MOVE_RATE, moveBudget + (now - lastRefill) * (MOVE_RATE / 1000));
      lastRefill = now;
      if (moveBudget < 1) return; // silently drop, client slows itself down
      moveBudget -= 1;

      const v = validateMove(msg.p);
      if (!v.ok) return; // invalid positions are dropped, never relayed
      charState = { x: v.x, z: v.z, heading: v.heading, anim: v.anim, vehicle: v.vehicle };
      room.clients.get(clientId).state = charState;
      room.broadcast('state', { id: clientId, p: charState }, clientId);
      return;
    }

    if (msg.type === 'chat') {
      const text = String(msg.text || '').slice(0, 140);
      if (!text) return;
      room.broadcast('chat', { id: clientId, name, text });
      return;
    }

    if (msg.type === 'vehicle') {
      // claim / release a vehicle seat so two players can't drive one car
      room.broadcast('vehicleUse', { id: clientId, vehicleId: String(msg.vehicleId || '').slice(0, 40), seated: !!msg.seated });
      return;
    }

    /* --- coop mission flow: host arbitrates, everyone follows ------ */
    if (msg.type === 'coopStart') {
      if (clientId !== room.hostClientId) return;
      room.missionState = {
        active: true, missionId: String(msg.missionId || 'coop_sweep').slice(0, 32),
        objective: 'Co-op objective in progress', progress: {}, ready: {},
      };
      room.broadcast('coopStarted', { missionId: room.missionState.missionId });
      return;
    }
    if (msg.type === 'coopEvent') {
      if (!room.missionState.active) return;
      room.missionState.progress[clientId] = { event: String(msg.event || '').slice(0, 24), at: Date.now() };
      room.broadcast('coopEvent', { id: clientId, event: String(msg.event || '').slice(0, 24) });
      return;
    }
    if (msg.type === 'coopDone') {
      if (clientId !== room.hostClientId) return;
      room.missionState = { active: false, missionId: null, objective: null, progress: {}, ready: {} };
      room.broadcast('coopFinished', {});
      return;
    }

    if (msg.type === 'ping') { ws.sendText(JSON.stringify({ type: 'pong' })); return; }
    // unknown types are ignored, not fatal — forward compatibility
  });

  ws.on('close', () => close('disconnect'));
  ws.on('error', () => close('socket error'));
}

httpServer.listen(PORT, () => {
  console.log('');
  console.log('  ╔══════════════════════════════════════════════════╗');
  console.log('  ║   CRIMSON HARBOR — open-world action adventure   ║');
  console.log('  ╠══════════════════════════════════════════════════╣');
  console.log(`  ║   Game:      http://localhost:${String(PORT).padEnd(21)}║`);
  console.log('  ║   Multiplayer: built-in, no keys or services     ║');
  console.log('  ║   Stop with Ctrl+C                                 ║');
  console.log('  ╚══════════════════════════════════════════════════╝');
  console.log('');
});
