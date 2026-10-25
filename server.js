const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const page = path.join(__dirname, 'public', 'index.html');
const srv = http.createServer((q, r) => {
  r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  fs.createReadStream(page).pipe(r);
});
const wss = new WebSocketServer({ server: srv, maxPayload: 1e6 });
const rooms = {}, MAX = 10, WAIT = 30000;
const rnd = () => Math.random().toString(36).slice(2, 8).toUpperCase();
const send = (p, o) => p.ws && p.ws.readyState === 1 && p.ws.send(JSON.stringify(o));

function view(r, p) {
  const i = r.players.indexOf(p);
  const v = {
    code: r.code, phase: r.phase, step: r.step || 0, steps: r.steps, you: p.id, host: p.host,
    left: Math.max(0, (r.deadline || 0) - Date.now()),
    players: r.players.map(x => ({ n: x.name, on: !x.off, d: !!(r.done && r.done[x.id]) })),
  };
  if (r.phase === 'play') {
    const it = r.chains[(i + r.step + 1) % r.n][r.step];
    v.task = { t: it.t, v: it.v };
  }
  if (r.phase === 'album') v.album = r.chains;
  return v;
}
const push = r => r.players.forEach(p => send(p, view(r, p)));

function begin(r, phase, ms) { r.phase = phase; r.sub = {}; r.done = {}; r.deadline = ms ? Date.now() + ms : 0; }
function adv(r) {
  if (r.phase === 'words') {
    r.n = r.players.length;
    r.chains = r.players.map(p => [{ by: p.name, t: 'word', v: r.sub[p.id] || '❓' }]);
    r.steps = Math.min(r.n - 1, 6); if (r.steps % 2) r.steps--;
    r.step = 0; begin(r, 'play', 60000);
  } else if (r.phase === 'play') {
    r.players.forEach((p, i) => r.chains[(i + r.step + 1) % r.n].push(
      { by: p.name, t: r.step % 2 ? 'guess' : 'draw', v: r.sub[p.id] || null }));
    r.step++;
    if (r.step >= r.steps) begin(r, 'album', 0); else begin(r, 'play', r.step % 2 ? 30000 : 60000);
  }
  push(r);
}
const ready = r => r.players.every(p => r.done[p.id] || (p.off && Date.now() - p.off >= WAIT));

setInterval(() => {
  for (const r of Object.values(rooms)) {
    if (r.phase === 'lobby') {
      const before = r.players.length;
      r.players = r.players.filter(p => p.host || !p.off || Date.now() - p.off < 20000);
      if (r.players.length !== before) push(r);
    } else if (r.phase === 'words' || r.phase === 'play') {
      if (Date.now() > r.deadline + 3000 || ready(r)) adv(r);
    }
    if (r.players.every(p => p.off && Date.now() - p.off > 3600000)) delete rooms[r.code];
  }
}, 1000);

wss.on('connection', ws => {
  let r, me;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const err = e => ws.send(JSON.stringify({ err: e }));
    if (m.a === 'create' || m.a === 'join') {
      const name = String(m.name || '').trim().slice(0, 14) || 'Խաղացող';
      if (m.a === 'create') {
        let c; do c = rnd(); while (rooms[c]);
        r = rooms[c] = { code: c, players: [], phase: 'lobby', step: 0 };
      } else {
        r = rooms[String(m.code || '').toUpperCase()];
        if (!r) return err('Այս խաղը չկա կամ արդեն ավարտվել է։');
        me = m.tok && r.players.find(p => p.tok === m.tok);
        if (!me) {
          if (r.phase !== 'lobby') return err('Խաղը արդեն սկսվել է, կեսից մտնել չի թույլատրվում։');
          if (r.players.length >= MAX) return err('Սենյակը լիքն է (առավելագույնը 10 հոգի)։');
        }
      }
      if (!me) {
        me = { id: rnd(), tok: rnd() + rnd() + rnd(), name, host: m.a === 'create' };
        r.players.push(me);
      }
      me.ws = ws; me.off = null;
      ws.send(JSON.stringify({ wel: 1, code: r.code, tok: me.tok, name: me.name }));
      return push(r);
    }
    if (!r || !me) return;
    if (m.a === 'start' && me.host && r.phase === 'lobby' && r.players.length >= 3) { begin(r, 'words', 45000); push(r); }
    else if (m.a === 'skip' && me.host && (r.phase === 'words' || r.phase === 'play')) adv(r);
    else if (m.a === 'again' && me.host && r.phase === 'album') { begin(r, 'lobby', 0); r.step = 0; push(r); }
    else if (m.a === 'sub' && (r.phase === 'words' || r.phase === 'play') && !r.done[me.id]) {
      let v = String(m.v || '');
      if (r.phase === 'play' && r.step % 2 === 0) {
        if (!v.startsWith('data:image/jpeg;base64,') || v.length > 600000) v = null;
      } else v = v.trim().slice(0, 40);
      r.sub[me.id] = v; r.done[me.id] = 1;
      push(r);
      if (ready(r)) adv(r);
    }
  });
  ws.on('close', () => { if (me && me.ws === ws) { me.off = Date.now(); if (r) push(r); } });
});
srv.listen(process.env.PORT || 3000, () => console.log('Ready'));
