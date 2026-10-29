// Сервер только знакомит устройства друг с другом (signaling).
// Сами файлы идут напрямую между устройствами по WebRTC и через сервер не проходят.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'public', 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.end('ok'); return; }
  fs.readFile(INDEX, (err, data) => {
    if (err) { res.writeHead(500); res.end('index.html not found'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

// ---- «Одна сеть» = один внешний IP (или один локальный адрес) ----
const PRIVATE_V4 = /^(10\.|127\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|169\.254\.)/;

function v6prefix(ip) {
  const [h, t] = ip.split('::');
  const a = h ? h.split(':') : [];
  const b = t ? t.split(':') : [];
  const fill = t === undefined ? 0 : 8 - a.length - b.length;
  const full = [...a, ...Array(fill).fill('0'), ...b];
  return full.slice(0, 4).map(x => x.padStart(4, '0')).join(':');
}

function roomOf(req) {
  // Комната по коду (QR): работает между любыми сетями
  const code = new URL(req.url, 'http://x').searchParams.get('room');
  if (code && /^[A-Z0-9]{6}$/i.test(code)) return 'code:' + code.toUpperCase();

  let ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim()
    || req.socket.remoteAddress || '';
  ip = ip.replace(/^::ffff:/, '');
  if (PRIVATE_V4.test(ip) || ip === '::1' || /^f[cde]/i.test(ip)) return 'local';
  if (ip.includes(':')) return v6prefix(ip); // IPv6: одна домашняя сеть = один /64
  return ip;
}

// STUN/TURN. Для связи между разными сетями иногда нужен TURN-сервер:
// задай переменную окружения ICE_SERVERS, например
// [{"urls":"stun:stun.l.google.com:19302"},{"urls":"turn:host:3478","username":"u","credential":"p"}]
let ICE = [{ urls: 'stun:stun.l.google.com:19302' }];
try { if (process.env.ICE_SERVERS) ICE = JSON.parse(process.env.ICE_SERVERS); } catch {}

// ---- Имена устройств ----
const ANIMALS = [
  ['лис', '🦊', 'm'], ['кот', '🐱', 'm'], ['пингвин', '🐧', 'm'], ['енот', '🦝', 'm'],
  ['жираф', '🦒', 'm'], ['панда', '🐼', 'f'], ['сова', '🦉', 'f'], ['лама', '🦙', 'f'],
];
const ADJ = {
  m: ['Быстрый', 'Весёлый', 'Смелый', 'Тихий', 'Ловкий'],
  f: ['Быстрая', 'Весёлая', 'Смелая', 'Тихая', 'Ловкая'],
};
const pick = arr => arr[Math.floor(Math.random() * arr.length)];
function randomIdentity() {
  const [noun, emoji, g] = pick(ANIMALS);
  return { id: crypto.randomUUID().slice(0, 8), name: `${pick(ADJ[g])} ${noun}`, emoji };
}

// ---- Комнаты ----
const rooms = new Map(); // room -> Map(id -> {ws, name, emoji})

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const roomKey = roomOf(req);
  if (!rooms.has(roomKey)) rooms.set(roomKey, new Map());
  const room = rooms.get(roomKey);
  const me = randomIdentity();

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  const others = [...room.entries()].map(([id, p]) => ({ id, name: p.name, emoji: p.emoji }));
  room.set(me.id, { ws, name: me.name, emoji: me.emoji });
  send(ws, { type: 'init', me, peers: others, iceServers: ICE });
  for (const [id, p] of room) if (id !== me.id) send(p.ws, { type: 'peer-joined', peer: me });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'signal' && typeof msg.to === 'string') {
      const target = room.get(msg.to);
      if (target) send(target.ws, { type: 'signal', from: me.id, data: msg.data });
    }
  });

  ws.on('close', () => {
    room.delete(me.id);
    for (const p of room.values()) send(p.ws, { type: 'peer-left', id: me.id });
    if (room.size === 0) rooms.delete(roomKey);
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log(`Рядом запущен: http://localhost:${PORT}`));
