// Dielman HQ — texts and marketing emails to Dielman Moving & Storage's past customers.
// Railway variables (Billy sets these; nothing here is hard-coded):
//   APP_PASSWORD                 password to log in (Charles + Billy)
//   UPSTASH_REDIS_REST_URL       \ Dielman's own Upstash database
//   UPSTASH_REDIS_REST_TOKEN     /
//   TEXTMAGIC_USERNAME           \ Dielman's own TextMagic account
//   TEXTMAGIC_API_KEY            /
//   PUBLIC_BASE_URL              this app's address, e.g. https://dielman-hq.up.railway.app
// Without Upstash it stores data in ./data/store.json (local testing only).
//
// How it works:
//   - An Apps Script in info@dielmanmoving.com (get it from Setup) sends HQ every estimate and confirmation
//     email, hourly, including all old ones. HQ reads the fields and builds the customer list.
//   - Texts go out through TextMagic the moment you press Send.
//   - Emails wait in a queue; the same Apps Script sends them from his Gmail every hour (max EMAIL_PER_DAY a day).

const express = require('express');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const CFG = {
  PASSWORD: (process.env.APP_PASSWORD || '').trim(),
  REDIS_URL: (process.env.UPSTASH_REDIS_REST_URL || '').trim().replace(/\/$/, ''),
  REDIS_TOKEN: (process.env.UPSTASH_REDIS_REST_TOKEN || '').trim(),
  TM_USER: (process.env.TEXTMAGIC_USERNAME || '').trim(),
  TM_KEY: (process.env.TEXTMAGIC_API_KEY || '').trim(),
  BASE: (process.env.PUBLIC_BASE_URL || 'http://localhost:' + (process.env.PORT || 3000)).trim().replace(/\/$/, ''),
};
const BIZ = { name: 'Dielman Moving & Storage', addr: '8730 Big Bend Blvd, Unit F-3, Webster Groves, MO 63119', phone: '314-428-6683', email: 'info@dielmanmoving.com' };
// Dielman's own numbers appear in email headers/footers ('Phone: … Fax: …'); never treat them as a customer's phone.
const BIZ_PHONES = new Set(['13144286683', '13142797255']);
const EMAIL_PER_DAY = 300;        // marketing emails per day from his Gmail (Workspace allows ~1,500 total; leaves room for his normal email)
const EMAIL_HOURS = [8, 19];      // emails only go out 8am–7pm Central
const TEXT_PRICE = 0.05;          // rough TextMagic cost per text segment, for the estimate on the Send button
const JOURNEY_WINDOW_DAYS = 3;    // an automatic step only reaches customers whose step date was in the last 3 days

const app = express();
app.set('trust proxy', true);
app.use('/public/dielman/ingest', express.json({ limit: '4mb' }));
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: false }));
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('X-Frame-Options', 'DENY');
  next();
});

// ===================== STORAGE (Upstash Redis, or a local file) =====================
const FILE = path.join(__dirname, 'data', 'store.json');
let LOCAL = null;
const localLoad = () => { if (!LOCAL) { try { LOCAL = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch { LOCAL = {}; } } return LOCAL; };
const localSave = () => { fs.mkdirSync(path.dirname(FILE), { recursive: true }); fs.writeFileSync(FILE, JSON.stringify(LOCAL)); };
async function redis(cmd) {
  const r = await fetch(CFG.REDIS_URL, { method: 'POST', headers: { Authorization: `Bearer ${CFG.REDIS_TOKEN}` }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (j.error) throw new Error('Redis: ' + j.error);
  return j.result;
}
const db = {
  async get(k, dflt) {
    if (CFG.REDIS_URL) { const v = await redis(['GET', k]); return v ? JSON.parse(v) : dflt; }
    const v = localLoad()[k]; return v === undefined ? dflt : v;
  },
  async set(k, v) {
    if (CFG.REDIS_URL) return redis(['SET', k, JSON.stringify(v)]);
    localLoad()[k] = v; localSave();
  },
  async hall(k) {   // whole hash as { field: parsedValue }
    if (CFG.REDIS_URL) { const a = (await redis(['HGETALL', k])) || [], o = {}; for (let i = 0; i < a.length; i += 2) o[a[i]] = JSON.parse(a[i + 1]); return o; }
    return { ...(localLoad()[k] || {}) };
  },
  async hset(k, obj) {   // { field: value, ... }
    const ents = Object.entries(obj); if (!ents.length) return;
    if (CFG.REDIS_URL) { for (let i = 0; i < ents.length; i += 200) await redis(['HSET', k, ...ents.slice(i, i + 200).flatMap(([f, v]) => [f, JSON.stringify(v)])]); return; }
    const h = localLoad()[k] = localLoad()[k] || {}; ents.forEach(([f, v]) => { h[f] = v; }); localSave();
  },
  async hdel(k, fields) {
    if (!fields.length) return;
    if (CFG.REDIS_URL) return redis(['HDEL', k, ...fields]);
    const h = localLoad()[k] || {}; fields.forEach(f => delete h[f]); localSave();
  },
};
const K = {
  sess: 'dm:sess', key: 'dm:scriptKey', secret: 'dm:secret', rec: 'dm:rec', optout: 'dm:optout', settings: 'dm:settings',
  paired: 'dm:paired', hook: 'dm:hook', camps: 'dm:camps', out: 'dm:out', jlog: 'dm:jlog', sync: 'dm:sync', mailday: 'dm:mailday',
};
const crKey = id => 'dm:cr:' + id;   // one text campaign's recipients + status
async function rand(key) { let v = await db.get(key, ''); if (!v) { v = crypto.randomBytes(18).toString('hex'); await db.set(key, v); } return v; }

// ===================== TIME (Central) =====================
const TZ = 'America/Chicago';
const ymdCT = (d = new Date()) => d.toLocaleDateString('en-CA', { timeZone: TZ });
const hourCT = () => Number(new Date().toLocaleString('en-US', { timeZone: TZ, hour: 'numeric', hour12: false })) % 24;
const ymd = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
const addDays = (s, n) => { const d = new Date(s + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
const daysBetween = (a, b) => Math.round((Date.parse(b + 'T12:00:00Z') - Date.parse(a + 'T12:00:00Z')) / 864e5);
const nice = s => s ? new Date(s + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }) : 'date TBD';

// ===================== LOGIN (one password, 30-day sessions, lockout after 5 wrong tries) =====================
const SESS_DAYS = 30;
let SESS = null;
const loadSess = async () => { if (!SESS) SESS = (await db.get(K.sess, {})) || {}; return SESS; };
const cookieTok = req => { const c = (req.headers.cookie || '').split(';').map(x => x.trim()).find(x => x.startsWith('dhq=')); return c ? c.slice(4) : ''; };
const sameStr = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
async function authed(req) { const t = cookieTok(req), s = t && (await loadSess())[t]; return !!CFG.PASSWORD && !!s && s.exp > Date.now(); }
const saveSess = () => { const now = Date.now(); Object.keys(SESS).forEach(k => { if (!(SESS[k].exp > now)) delete SESS[k]; }); return db.set(K.sess, SESS); };
const clientIp = req => String(req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
const loginFails = new Map();
app.post('/login', async (req, res) => {
  const ip = clientIp(req), now = Date.now();
  const fails = (loginFails.get(ip) || []).filter(t => now - t < 15 * 60 * 1000);
  if (fails.length >= 5) return res.redirect('/?bad=locked');
  if (!CFG.PASSWORD || !sameStr(req.body.password || '', CFG.PASSWORD)) { fails.push(now); loginFails.set(ip, fails); return res.redirect('/?bad=' + (fails.length >= 5 ? 'locked' : '1')); }
  loginFails.delete(ip); await loadSess();
  const tok = crypto.randomBytes(32).toString('hex');
  SESS[tok] = { exp: now + SESS_DAYS * 864e5, created: now, ip };
  try { await saveSess(); } catch (e) { console.log('session save failed', e.message); }
  const secure = CFG.BASE.startsWith('https') ? '; Secure' : '';
  res.setHeader('Set-Cookie', `dhq=${tok}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${SESS_DAYS * 86400}${secure}`);
  res.redirect('/');
});
app.get('/logout', async (req, res) => {
  const t = cookieTok(req); await loadSess(); if (t && SESS[t]) { delete SESS[t]; try { await saveSess(); } catch {} }
  res.setHeader('Set-Cookie', 'dhq=; Path=/; Max-Age=0'); res.redirect('/');
});
app.get('/', async (req, res) => res.set('Cache-Control', 'no-cache').sendFile(path.join(__dirname, (await authed(req)) ? 'dielman.html' : 'login.html')));
app.get('/healthz', (req, res) => res.send('ok'));
app.use('/api', async (req, res, next) => (await authed(req)) ? next() : res.status(401).json({ error: 'login' }));

// ===================== READING HIS EMAILS =====================
const clean = (v, n = 200) => String(v == null ? '' : v).replace(/\s+/g, ' ').trim().slice(0, n);
const field = (body, re) => { const m = String(body).match(re); return m ? clean(m[1], 160) : ''; };
const normPhone = v => { const d = String(v || '').replace(/\D/g, ''); return d.length === 10 ? '1' + d : d.length === 11 && d[0] === '1' ? d : ''; };
const normEmail = v => { const m = String(v || '').match(/[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/); return m ? m[0].toLowerCase() : ''; };
function parseMoveDate(s, fallbackYear) {
  s = String(s || '').replace(/\(.*?\)/g, ' ').replace(/\b(mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)[a-z]*\.?,?\s*/gi, ' ').replace(/(\d)(st|nd|rd|th)\b/gi, '$1').replace(/\s+/g, ' ').trim();
  if (!s || /tbd|tba/i.test(s)) return '';
  let m = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (m) return ymd(new Date(+(m[3].length === 2 ? '20' + m[3] : m[3]), +m[1] - 1, +m[2]));
  m = s.match(/([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:,?\s*(\d{4}))?/);
  if (!m) return '';
  const d = new Date(`${m[1]} ${m[2]}, ${m[3] || fallbackYear || new Date().getFullYear()}`);
  return isNaN(d) ? '' : ymd(d);
}
// One estimate or confirmation email → the fields HQ keeps (the email body itself isn't stored).
function readEmail(it) {
  const body = String(it.body || ''), type = it.type === 'confirm' ? 'confirm' : 'estimate';
  const sent = it.date ? ymd(new Date(it.date)) : '';
  const hello = field(body, /(?:Hello|Hi|Dear|Good (?:morning|afternoon|evening))\s+([^,\n]{1,40}),/i);
  let phone = '';   // first customer phone that isn't one of Dielman's own numbers
  for (const m of body.matchAll(/(?:PHONE(?: NUMBER)?|CELL(?: PHONE)?|MOBILE)\s*(?:#|NUMBER)?\s*:[ \t]*([^\n]+)/gi)) { const p = normPhone(m[1].split(/fax|ext/i)[0]); if (p && !BIZ_PHONES.has(p)) { phone = p; break; } }
  const name = field(body, /CUSTOMER NAME:[ \t]*([^\n]+)/i) || hello;
  return {
    id: clean(it.threadId, 40) + ':' + type, type, threadId: clean(it.threadId, 40),
    email: normEmail(it.to), phone,
    name, first: clean((hello || name).split(/\s+/)[0], 30).replace(/^\w/, c => c.toUpperCase()),
    moveDate: parseMoveDate(field(body, /(?:MOVE DATE|MOVING DATE|DATE OF (?:THE )?MOVE|MOVE DAY)\s*:[ \t]*([^\n]+)/i) || field(body, /^[ \t*]*DATE\s*:[ \t]*([^\n]+)/im), (sent || '').slice(0, 4)),
    pickup: field(body, /PICK[ -]?UP\s*:[ \t]*([^\n]+)/i), delivery: field(body, /DELIVERY\s*:[ \t]*([^\n]+)/i),
    crew: field(body, /(?:CREW SIZE & RATES|Rates\/Crew Size|STAFF)\s*:[ \t]*([^\n]+)/i),
    sent, replied: !!it.replied, at: Date.now(),
  };
}
const shortPlace = a => { const m = String(a || '').match(/([A-Za-z .'-]+),?\s+MO\b/i); return m ? clean(m[1].split(/[.,]/).pop(), 30) : clean(a, 30); };

// ===================== BOOKED RULE — the ONE place that decides what counts as a real move =====================
// 1) Every confirmation email he sends (subject contains "Confirmation", not "Onsite") = a booked move (on its move date).
// 2) An estimate the customer replied to also counts (his estimate says the crew can't be dispatched until they reply),
//    unless a confirmation for the same customer already covers it (move dates within 45 days).
// Change this function if Charles says otherwise; everything else (counts, lists, automatic steps) follows it.
function bookedMoves(recs, personOf) {
  const conf = recs.filter(r => r.type === 'confirm'), moves = [];
  const seen = new Set();
  for (const r of conf) {   // the same confirmation re-sent = one move
    const k = personOf(r) + '|' + (r.moveDate || r.sent); if (seen.has(k)) continue; seen.add(k);
    moves.push({ ...r, date: r.moveDate || r.sent, src: 'confirmation' });
  }
  for (const r of recs.filter(x => x.type === 'estimate' && x.replied)) {
    const date = r.moveDate; if (!date) continue;
    const covered = moves.some(m => personOf(m) === personOf(r) && Math.abs(daysBetween(m.date, date)) <= 45);
    if (!covered) moves.push({ ...r, date, src: 'estimate reply' });
  }
  return moves;
}

// People = records grouped by email or phone. Cached until new emails come in.
let PEOPLE = null;
async function people() {
  if (PEOPLE) return PEOPLE;
  const recs = Object.values(await db.hall(K.rec)).filter(r => r.email || r.phone);
  // A phone that shows up with 4+ different customer emails is an office/business number, not a customer's.
  const phoneEmails = {}; recs.forEach(r => { if (r.phone && r.email) (phoneEmails[r.phone] = phoneEmails[r.phone] || new Set()).add(r.email); });
  recs.forEach(r => { if (r.phone && (BIZ_PHONES.has(r.phone) || (phoneEmails[r.phone] && phoneEmails[r.phone].size >= 4))) r.phone = ''; });
  // union-find on email/phone so one customer with two emails or a new phone stays one person
  const parent = {}; const find = x => (parent[x] === undefined ? (parent[x] = x) : parent[x] === x ? x : (parent[x] = find(parent[x])));
  const union = (a, b) => { parent[find(a)] = find(b); };
  recs.forEach(r => { const ks = [r.email && 'e:' + r.email, r.phone && 'p:' + r.phone].filter(Boolean); ks.forEach(k => find(k)); if (ks.length === 2) union(ks[0], ks[1]); });
  const personOf = r => find(r.email ? 'e:' + r.email : 'p:' + r.phone);
  const moves = bookedMoves(recs, personOf), today = ymdCT(), map = {};
  const get = pid => map[pid] = map[pid] || { id: pid, name: '', first: '', emails: new Set(), phones: new Set(), moves: [], estimates: 0, lastEstimate: '' };
  recs.sort((a, b) => (a.sent || '').localeCompare(b.sent || '')).forEach(r => {
    const p = get(personOf(r));
    if (r.email) p.emails.add(r.email); if (r.phone) p.phones.add(r.phone);
    if (r.name) p.name = r.name; if (r.first && (r.type === 'estimate' || !p.first)) p.first = r.first;
    if (r.type === 'estimate') { p.estimates++; if ((r.sent || '') > p.lastEstimate) p.lastEstimate = r.sent; }
  });
  moves.forEach(m => get(personOf(m)).moves.push({ date: m.date, from: shortPlace(m.pickup), to: shortPlace(m.delivery), src: m.src, key: personOf(m) + '|' + m.date }));
  const list = Object.values(map).map(p => {
    p.moves.sort((a, b) => a.date.localeCompare(b.date));
    const past = p.moves.filter(m => m.date <= today), dates = [];
    past.forEach(m => { if (!dates.length || daysBetween(dates[dates.length - 1], m.date) > 30) dates.push(m.date); });
    return { id: p.id, name: p.name, first: p.first || 'there', email: [...p.emails].pop() || '', phone: [...p.phones].pop() || '',
      moves: p.moves, pastMoves: dates.length, lastMove: dates[dates.length - 1] || '', repeat: dates.length >= 2,
      estimates: p.estimates, lastEstimate: p.lastEstimate, booked: p.moves.length > 0 };
  });
  return (PEOPLE = { list, moves, recs: recs.length, at: Date.now() });
}

// ===================== AUDIENCES =====================
const AUDIENCES = [
  { id: 'all', name: 'All past customers', test: p => p.pastMoves > 0 },
  { id: 'y1', name: 'Moved 1+ year ago', test: (p, t) => p.lastMove && daysBetween(p.lastMove, t) >= 365 },
  { id: 'd90', name: 'Moved in last 90 days', test: (p, t) => p.lastMove && daysBetween(p.lastMove, t) <= 90 },
  { id: 'repeat', name: 'Repeat customers', test: p => p.repeat },
  { id: 'nobook', name: "Got estimate, didn't book", test: (p, t) => !p.booked && p.lastEstimate && daysBetween(p.lastEstimate, t) >= 14 && daysBetween(p.lastEstimate, t) <= 730 },
];
async function optouts() { return new Set(Object.keys(await db.hall(K.optout))); }
const reachable = (p, ch, oo) => ch === 'text' ? p.phone && !oo.has('p:' + p.phone) : p.email && !oo.has('e:' + p.email);
async function audience(aud, picks, ch) {
  const { list } = await people(), t = ymdCT(), oo = await optouts();
  const a = AUDIENCES.find(x => x.id === aud);
  const chosen = aud === 'pick' ? list.filter(p => (picks || []).includes(p.id)) : a ? list.filter(p => a.test(p, t)) : [];
  const seen = new Set();
  return chosen.filter(p => reachable(p, ch, oo)).filter(p => { const k = ch === 'text' ? p.phone : p.email; if (seen.has(k)) return false; seen.add(k); return true; });
}

// ===================== MESSAGES =====================
const TEMPLATES = [
  { id: 'storage', name: 'Need storage or junk hauled?', subject: 'Need storage or junk hauled?',
    body: "Hi {first}, it's Charles at Dielman Moving. Need storage or a junk haul after your move? Reply here and we'll take care of it." },
  { id: 'season', name: 'Moving season is coming — book early', subject: 'Moving this spring? Lock in your date',
    body: "Hi {first}, it's Charles at Dielman Moving. Spring and summer dates fill up fast. If you or anyone you know is moving this year, reply here or call 314-428-6683 to hold a date." },
  { id: 'referral', name: 'Know someone moving? (referral)', subject: 'Know someone who is moving?',
    body: "Hi {first}, it's Charles at Dielman Moving. Most of our work comes from customers like you. If a friend, family member or neighbor is moving, have them call 314-428-6683 and mention your name." },
  { id: 'holiday', name: 'Happy holidays from Dielman', subject: 'Happy holidays from Dielman Moving',
    body: "Hi {first}, happy holidays from all of us at Dielman Moving & Storage. Thank you for trusting us with your move. We hope you're settled in and enjoying the season!" },
];
const merge = (s, p, st) => String(s || '').replace(/\{first\}/g, (p && p.first) || 'there').replace(/\{review\}/g, (st && st.reviewLink) || '');
const withStop = s => /\bSTOP\b/.test(s) ? s : s.trim() + '\nReply STOP to opt out.';
const segments = s => { const n = withStop(s).length; return n <= 160 ? 1 : Math.ceil(n / 153); };
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
async function unsubUrl(email) { const s = await rand(K.secret), e = Buffer.from(email).toString('base64url'); return `${CFG.BASE}/u/${e}.${crypto.createHmac('sha256', s).update(email).digest('hex').slice(0, 20)}`; }
async function emailBody(text, email) {
  const u = await unsubUrl(email);
  const html = `<div style="font-family:Arial,sans-serif;font-size:15px;line-height:1.5;color:#1a1a1a;max-width:560px">${esc(text).replace(/\n/g, '<br>')}
<br><br>Charles Price<br>${esc(BIZ.name)}<br>${esc(BIZ.phone)}
<hr style="border:0;border-top:1px solid #ddd;margin:24px 0 10px"><div style="font-size:12px;color:#777">${esc(BIZ.name)} · ${esc(BIZ.addr)}<br>You're getting this because you moved with us. <a href="${u}" style="color:#777">Unsubscribe</a></div></div>`;
  return { html, text: `${text}\n\nCharles Price\n${BIZ.name}\n${BIZ.phone}\n\n${BIZ.addr}\nUnsubscribe: ${u}` };
}

// ---------- TextMagic ----------
async function tm(method, p, body) {
  if (!CFG.TM_USER || !CFG.TM_KEY) throw new Error('Texts are off until TEXTMAGIC_USERNAME and TEXTMAGIC_API_KEY are added in Railway');
  const r = await fetch('https://rest.textmagic.com' + p, { method, headers: { 'X-TM-Username': CFG.TM_USER, 'X-TM-Key': CFG.TM_KEY, Accept: 'application/json', ...(body ? { 'Content-Type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error('TextMagic: ' + (j.message || r.status));
  return j;
}
const sendText = (phone, text) => tm('POST', '/api/v2/messages', { text: withStop(text), phones: phone });
// People who replied STOP are kept by TextMagic; copy them into HQ's opt-out list before every send.
async function syncTextOptouts() {
  if (!CFG.TM_USER || !CFG.TM_KEY) return;
  const add = {};
  for (let page = 1; page <= 20; page++) {
    const j = await tm('GET', `/api/v2/unsubscribers?page=${page}&limit=100`);
    (j.resources || []).forEach(u => { const p = normPhone(u.phone); if (p) add['p:' + p] = { at: Date.now(), via: 'STOP' }; });
    if (!j.pageCount || page >= j.pageCount) break;
  }
  await db.hset(K.optout, add);
}

// ---------- email queue (the Gmail script sends these) ----------
async function queueEmail(to, subject, text, camp, notBefore) {
  const id = crypto.randomBytes(8).toString('hex'), b = await emailBody(text, to);
  await db.hset(K.out, { [id]: { id, to, subject, html: b.html, text: b.text, camp, notBefore: notBefore || 0, st: 'q', at: Date.now() } });
}

// Instant sending: the Gmail script also runs as a private web app. Calling it makes Gmail send what's waiting
// right away (the hourly timer stays as a backup). Returns how many it sent, or -1 if it couldn't be reached.
async function kickGmail() {
  const url = await db.get(K.hook, ''), key = await db.get(K.key, '');
  if (!url || !key) return -1;
  try {
    const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key }), redirect: 'follow', signal: AbortSignal.timeout(90000) });
    const j = await r.json().catch(() => ({}));
    return j.ok ? Number(j.sent) || 0 : -1;
  } catch (e) { console.log('instant send failed', e.message); return -1; }
}

// ===================== CAMPAIGNS (Send tab) =====================
const running = new Set();
async function runTextCampaign(id) {
  if (running.has(id)) return; running.add(id);
  try {
    const camps = await db.hall(K.camps), c = camps[id]; if (!c || c.status === 'canceled') return;
    c.status = 'sending'; await db.hset(K.camps, { [id]: c });
    try { await syncTextOptouts(); } catch (e) { console.log('opt-out sync failed', e.message); }
    const oo = await optouts(), recips = await db.hall(crKey(id));
    for (const [k, r] of Object.entries(recips)) {
      if (r.st !== 'q') continue;
      if (oo.has('p:' + r.to)) { await db.hset(crKey(id), { [k]: { ...r, st: 'skip' } }); continue; }
      await db.hset(crKey(id), { [k]: { ...r, st: 'sending' } });   // marked first so a restart never double-texts
      try { await sendText(r.to, r.msg); await db.hset(crKey(id), { [k]: { ...r, st: 's', at: Date.now() } }); }
      catch (e) { await db.hset(crKey(id), { [k]: { ...r, st: 'f', err: e.message.slice(0, 120) } }); if (/TEXTMAGIC_|401|403/.test(e.message)) break; }
    }
    c.status = 'done'; c.doneAt = Date.now(); await db.hset(K.camps, { [id]: c });
  } finally { running.delete(id); }
}
app.post('/api/send', async (req, res) => {
  try {
    const b = req.body || {}, ch = b.channel === 'email' ? 'email' : 'text', body = String(b.body || '').trim(), subject = clean(b.subject, 150);
    if (!body) return res.status(400).json({ error: 'Write a message first' });
    if (ch === 'email' && !subject) return res.status(400).json({ error: 'Add a subject' });
    const st = await db.get(K.settings, {});
    if (ch === 'text' && !(CFG.TM_USER && CFG.TM_KEY)) return res.status(400).json({ error: 'Texts are off until TEXTMAGIC_USERNAME and TEXTMAGIC_API_KEY are added in Railway' });
    if (b.test) {   // test goes only to Billy/Charles's own phone or email from Setup
      const me = { first: 'Billy' };
      if (ch === 'text') { const p = normPhone(st.testPhone); if (!p) return res.status(400).json({ error: 'Add your phone in Setup first' }); await sendText(p, '[TEST] ' + merge(body, me, st)); return res.json({ ok: true, msg: 'Test text sent to ' + st.testPhone }); }
      const e = normEmail(st.testEmail); if (!e) return res.status(400).json({ error: 'Add your email in Setup first' });
      await queueEmail(e, '[TEST] ' + merge(subject, me, st), merge(body, me, st), 'test', 0);
      const sent = await kickGmail();
      return res.json({ ok: true, msg: sent > 0 ? 'Test email sent to ' + e : 'Test email queued to ' + e + '. It goes out on the Gmail script\'s next run.' });
    }
    const list = await audience(b.audience, b.picks, ch);
    if (!list.length) return res.status(400).json({ error: 'Nobody in that group can get a ' + ch });
    if (Number(b.expect) !== list.length) return res.status(409).json({ error: `The count changed to ${list.length}. Check and press Send again.`, count: list.length });
    const sendAt = b.sendAt ? Date.parse(b.sendAt) : 0;
    if (b.sendAt && !(sendAt > Date.now())) return res.status(400).json({ error: 'Pick a time in the future' });
    const id = Date.now().toString(36) + crypto.randomBytes(3).toString('hex');
    const a = AUDIENCES.find(x => x.id === b.audience);
    const c = { id, ch, subject, body, audience: a ? a.name : `${list.length} picked`, count: list.length, createdAt: Date.now(), sendAt: sendAt || Date.now(), status: sendAt ? 'scheduled' : 'queued' };
    await db.hset(K.camps, { [id]: c });
    if (ch === 'text') {
      const r = {}; list.forEach((p, i) => { r[i] = { to: p.phone, msg: merge(body, p, st), st: 'q' }; });
      await db.hset(crKey(id), r);
      if (!sendAt) runTextCampaign(id).catch(e => console.log('text campaign failed', e.message));
    } else {
      for (const p of list) await queueEmail(p.email, merge(subject, p, st), merge(body, p, st), id, sendAt || 0);
      if (!sendAt) kickGmail().catch(() => {});
    }
    res.json({ ok: true, id, count: list.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/api/campaign/cancel', async (req, res) => {
  try {
    const camps = await db.hall(K.camps), c = camps[req.body.id]; if (!c) return res.status(404).json({ error: 'not found' });
    if (c.ch === 'text' && c.status !== 'scheduled') return res.status(400).json({ error: 'Already sending' });
    c.status = 'canceled'; await db.hset(K.camps, { [c.id]: c });
    if (c.ch === 'email') { const out = await db.hall(K.out); await db.hdel(K.out, Object.values(out).filter(o => o.camp === c.id && o.st === 'q').map(o => o.id)); }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
// Scheduled text campaigns start on time (checked every minute).
setInterval(async () => {
  try { const camps = await db.hall(K.camps); for (const c of Object.values(camps)) if (c.ch === 'text' && c.status === 'scheduled' && c.sendAt <= Date.now()) runTextCampaign(c.id).catch(() => {}); }
  catch (e) { console.log('schedule check failed', e.message); }
}, 60 * 1000);

// ===================== AUTOMATIC STEPS (after each booked move) =====================
const STEPS = [
  { id: 'moveday', when: 'Move day', title: 'Thank you', ch: ['text'], off: 0, fromHour: 16,
    text: "Thanks for trusting Dielman with your move today, {first}! If anything wasn't right, reply here. — Charles" },
  { id: 'review', when: 'Day 2', title: 'Google review ask', ch: ['text'], off: 2, needsReview: true,
    text: "Hi {first}, it's Charles at Dielman Moving. If our crew did a good job, would you leave us a quick Google review? It really helps a small family business: {review}" },
  { id: 'card7', when: 'Day 7', title: 'Thank-you card', postcard: true },
  { id: 'day30', when: 'Day 30', title: 'Need storage or junk hauled?', ch: ['email'], off: 30,
    subject: 'Need storage or junk hauled?', body: "Hi {first}, it's Charles at Dielman Moving. Now that you've had a month to settle in, do you need storage for anything that didn't fit, or a junk haul for what you don't want? Reply here and we'll take care of it." },
  { id: 'year1', when: '1 year', title: 'Happy 1 year in your new place', ch: ['email', 'text'], off: 365,
    subject: 'Happy 1 year in your new place!', body: "Hi {first}, it's been one year since we moved you. Happy anniversary in your new place! If you or anyone you know is moving this year, we'd love to help. Call 314-428-6683.",
    text: "Hi {first}, it's Charles at Dielman Moving. Happy 1 year in your new place! If you or anyone you know is moving, we'd love to help: 314-428-6683" },
  { id: 'holiday', when: 'December', title: 'Holiday card to everyone', postcard: true },
];
async function stepsWithSettings() {
  const st = await db.get(K.settings, {}), s = st.steps || {};
  return STEPS.map(x => ({ ...x, ...(s[x.id] || {}), on: !!(s[x.id] || {}).on && !x.postcard }));
}
let journeyBusy = false;
async function runJourney() {
  if (journeyBusy) return; journeyBusy = true;
  try {
    const steps = (await stepsWithSettings()).filter(s => s.on), h = hourCT();
    if (!steps.length || h < 10 || h >= 19) return;
    const st = await db.get(K.settings, {}), today = ymdCT(), { list } = await people(), log = await db.hall(K.jlog);
    if (steps.some(s => s.ch.includes('text'))) { try { await syncTextOptouts(); } catch (e) { console.log('opt-out sync failed', e.message); } }
    const oo = await optouts();
    for (const s of steps) {
      if (s.needsReview && !st.reviewLink) continue;
      if (s.fromHour && h < s.fromHour) continue;
      for (const p of list) for (const m of p.moves) {
        const due = addDays(m.date, s.off), age = daysBetween(due, today);
        if (age < 0 || age > JOURNEY_WINDOW_DAYS) continue;                       // never the backlog
        if (s.onAt && due < ymd(new Date(s.onAt - JOURNEY_WINDOW_DAYS * 864e5))) continue;
        for (const ch of s.ch) {
          const lk = `${s.id}|${ch}|${m.key}`; if (log[lk]) continue;
          if (!reachable(p, ch, oo)) { log[lk] = { at: Date.now(), skip: true }; await db.hset(K.jlog, { [lk]: log[lk] }); continue; }
          log[lk] = { at: Date.now() }; await db.hset(K.jlog, { [lk]: log[lk] });   // logged first: never twice
          try {
            if (ch === 'text') await sendText(p.phone, merge(s.text, p, st));
            else await queueEmail(p.email, merge(s.subject, p, st), merge(s.body, p, st), 'auto:' + s.id, 0);
          } catch (e) { await db.hset(K.jlog, { [lk]: { at: Date.now(), err: e.message.slice(0, 120) } }); console.log('step failed', s.id, e.message); }
        }
      }
    }
  } finally { journeyBusy = false; }
}
setInterval(() => runJourney().catch(e => console.log('journey failed', e.message)), 15 * 60 * 1000);
app.post('/api/step', async (req, res) => {
  try {
    const b = req.body || {}, def = STEPS.find(x => x.id === b.id); if (!def) return res.status(404).json({ error: 'unknown step' });
    if (def.postcard) return res.status(400).json({ error: 'Postcards are not connected yet' });
    const st = await db.get(K.settings, {}); st.steps = st.steps || {}; const cur = st.steps[def.id] || {};
    if (b.on === true && def.needsReview && !st.reviewLink) return res.status(400).json({ error: 'Add the Google review link in Setup first' });
    if (b.on === true && !cur.on) cur.onAt = Date.now();
    if (typeof b.on === 'boolean') cur.on = b.on;
    ['text', 'subject', 'body'].forEach(k => { if (typeof b[k] === 'string' && def[k] !== undefined) cur[k] = b[k].trim() || def[k]; });
    st.steps[def.id] = cur; await db.set(K.settings, st);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/api/settings', async (req, res) => {
  try {
    const b = req.body || {}, st = await db.get(K.settings, {});
    if (b.reviewLink !== undefined) { const v = clean(b.reviewLink, 300); if (v && !/^https:\/\//.test(v)) return res.status(400).json({ error: 'Review link should start with https://' }); st.reviewLink = v; }
    if (b.testPhone !== undefined) st.testPhone = clean(b.testPhone, 20);
    if (b.testEmail !== undefined) st.testEmail = clean(b.testEmail, 120);
    await db.set(K.settings, st); res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===================== PAGE DATA =====================
app.get('/api/state', async (req, res) => {
  try {
    const P = await people(), t = ymdCT(), oo = await optouts(), st = await db.get(K.settings, {});
    const counts = {};
    AUDIENCES.forEach(a => { const g = P.list.filter(p => a.test(p, t)); counts[a.id] = { total: g.length, text: g.filter(p => reachable(p, 'text', oo)).length, email: g.filter(p => reachable(p, 'email', oo)).length }; });
    const camps = Object.values(await db.hall(K.camps)).sort((a, b) => b.createdAt - a.createdAt).slice(0, 25);
    const out = Object.values(await db.hall(K.out));
    for (const c of camps) {
      const rs = c.ch === 'text' ? Object.values(await db.hall(crKey(c.id))) : out.filter(o => o.camp === c.id);
      c.sent = rs.filter(r => r.st === 's').length; c.failed = rs.filter(r => r.st === 'f').length; c.waiting = rs.filter(r => r.st === 'q' || r.st === 'c' || r.st === 'sending').length;
      if (c.ch === 'email' && c.status !== 'canceled') c.status = c.waiting ? (c.sendAt > Date.now() ? 'scheduled' : 'sending') : 'done';
      c.preview = clean(c.body, 60); delete c.body;
    }
    const sync = await db.get(K.sync, {});
    res.json({
      audiences: AUDIENCES.map(a => ({ id: a.id, name: a.name, ...counts[a.id] })), templates: TEMPLATES, campaigns: camps,
      steps: await stepsWithSettings(), settings: { reviewLink: st.reviewLink || '', testPhone: st.testPhone || '', testEmail: st.testEmail || '' },
      status: { gmailLast: sync.at || 0, gmailTotal: P.recs, people: P.list.length, customers: counts.all.total, repeat: counts.repeat.total,
        textsReady: !!(CFG.TM_USER && CFG.TM_KEY), gmailPaired: !!(await db.get(K.paired, false)), instant: !!(await db.get(K.hook, '')), baseOk: !!process.env.PUBLIC_BASE_URL, emailQueue: out.filter(o => o.st === 'q').length, optouts: oo.size },
      textPrice: TEXT_PRICE, emailPerDay: EMAIL_PER_DAY,
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/people', async (req, res) => {
  try {
    const q = String(req.query.q || '').toLowerCase().trim(), qd = q.replace(/\D/g, ''); if (q.length < 2) return res.json({ people: [] });
    const { list } = await people();
    res.json({ people: list.filter(p => p.name.toLowerCase().includes(q) || p.email.includes(q) || (qd.length >= 3 && p.phone.includes(qd))).slice(0, 15)
      .map(p => ({ id: p.id, name: p.name || p.email, email: p.email, phone: p.phone, last: p.lastMove ? nice(p.lastMove) : p.lastEstimate ? 'estimate ' + nice(p.lastEstimate) : '' })) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===================== GMAIL SCRIPT ENDPOINTS =====================
async function keyOk(k) { const real = await db.get(K.key, ''); return !!k && !!real && sameStr(k, real); }
app.post('/public/dielman/ingest', async (req, res) => {
  try {
    const b = req.body || {}; if (!(await keyOk(b.key))) return res.status(403).json({ error: 'bad key' });
    const add = {};
    (Array.isArray(b.items) ? b.items : []).slice(0, 200).forEach(it => { const r = readEmail(it); if (r.threadId && (r.email || r.phone)) add[r.id] = r; });
    await db.hset(K.rec, add); PEOPLE = null;
    const s = await db.get(K.sync, {}); await db.set(K.sync, { ...s, at: Date.now(), last: Object.keys(add).length });
    res.json({ ok: true, saved: Object.keys(add).length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
// Hands the script the next emails to send (respects the daily cap and sending hours).
let outboxBusy = Promise.resolve();   // one hand-out at a time, so the timer and an instant call never get the same email
app.get('/public/dielman/outbox', (req, res) => { outboxBusy = outboxBusy.then(() => outbox(req, res)).catch(() => {}); });
async function outbox(req, res) {
  try {
    if (!(await keyOk(req.query.key))) return res.status(403).json({ error: 'bad key' });
    const s = await db.get(K.sync, {}); await db.set(K.sync, { ...s, at: Date.now() });
    const h = hourCT(), awake = h >= EMAIL_HOURS[0] && h < EMAIL_HOURS[1];   // tests to Billy/Charles go out any hour
    const day = ymdCT(), md = await db.get(K.mailday, {}), used = md.d === day ? md.n : 0;
    const max = Math.max(0, Math.min(Number(req.query.max) || 30, 40, EMAIL_PER_DAY - used));
    if (!max) return res.json({ items: [] });
    const oo = await optouts(), now = Date.now(), all = Object.values(await db.hall(K.out));
    const camps = await db.hall(K.camps);
    const skip = all.filter(o => o.st === 'q' && (oo.has('e:' + o.to) || (camps[o.camp] || {}).status === 'canceled'));
    if (skip.length) await db.hset(K.out, Object.fromEntries(skip.map(o => [o.id, { ...o, st: 'skip' }])));
    const items = all.filter(o => o.st === 'q' && !skip.includes(o) && (o.notBefore || 0) <= now && (awake || o.camp === 'test')).sort((a, b) => (a.camp === 'test' ? -1 : 0) - (b.camp === 'test' ? -1 : 0) || a.at - b.at).slice(0, max);
    if (!items.length) return res.json({ items: [] });
    await db.hset(K.out, Object.fromEntries(items.map(o => [o.id, { ...o, st: 'c', claimedAt: now }])));   // handed out once, never re-sent
    await db.set(K.mailday, { d: day, n: used + items.length });
    res.json({ items: items.map(o => ({ id: o.id, to: o.to, subject: o.subject, html: o.html, text: o.text })) });
  } catch (e) { res.status(500).json({ error: e.message }); }
}
// The script tells HQ its web-app address so HQ can ask it to send right away.
app.post('/public/dielman/hook', async (req, res) => {
  try {
    const b = req.body || {}; if (!(await keyOk(b.key))) return res.status(403).json({ error: 'bad key' });
    const url = String(b.url || '');
    if (!/^https:\/\/script\.google\.com\/(a\/[^/]+\/)?macros\/s\/[\w-]+\/exec$/.test(url)) return res.status(400).json({ error: 'not a web app address' });
    await db.set(K.hook, url); res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/public/dielman/sent', async (req, res) => {
  try {
    const b = req.body || {}; if (!(await keyOk(b.key))) return res.status(403).json({ error: 'bad key' });
    const all = await db.hall(K.out), upd = {};
    (b.done || []).forEach(d => { const o = all[d.id]; if (o) upd[o.id] = { ...o, st: d.ok ? 's' : 'f', err: d.ok ? undefined : clean(d.err, 120), sentAt: Date.now() }; });
    await db.hset(K.out, upd);
    // keep the queue small: drop finished emails older than 60 days
    const old = Object.values(all).filter(o => o.st !== 'q' && o.st !== 'c' && o.at < Date.now() - 60 * 864e5).map(o => o.id);
    if (old.length) await db.hdel(K.out, old);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
// Gmail script pairing: the script carries no key. On its first run it asks HQ for one (only once, only as
// info@dielmanmoving.com) and keeps it in the script's own storage. Setup → "Reset Gmail connection" allows a new pairing.
app.post('/public/dielman/pair', async (req, res) => {
  try {
    if (await db.get(K.paired, false)) return res.status(409).json({ error: 'Already connected. Use Reset Gmail connection in Dielman HQ Setup first.' });
    if (normEmail((req.body || {}).email) !== BIZ.email) return res.status(403).json({ error: 'Run this from ' + BIZ.email });
    const k = crypto.randomBytes(18).toString('hex');
    await db.set(K.key, k); await db.set(K.paired, { at: Date.now() });
    res.json({ ok: true, key: k });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/api/gmail/reset', async (req, res) => {
  try { await db.set(K.key, crypto.randomBytes(18).toString('hex')); await db.set(K.paired, false); await db.set(K.hook, ''); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
// Unsubscribe link in every email. Two steps (page + button) so email scanners can't unsubscribe people by accident.
async function unsubEmail(tok) {
  const [e, sig] = String(tok || '').split('.'); let email = '';
  try { email = Buffer.from(e, 'base64url').toString(); } catch { return ''; }
  const good = crypto.createHmac('sha256', await rand(K.secret)).update(email).digest('hex').slice(0, 20);
  return sig && sameStr(sig, good) ? email : '';
}
const unsubPage = msg => `<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Dielman Moving</title><body style="font-family:Arial,sans-serif;max-width:420px;margin:60px auto;padding:0 20px;text-align:center;color:#17201c"><h2 style="color:#00593D">Dielman Moving &amp; Storage</h2>${msg}</body>`;
app.get('/u/:tok', async (req, res) => {
  const email = await unsubEmail(req.params.tok); if (!email) return res.status(400).send(unsubPage('<p>This link is not valid.</p>'));
  res.send(unsubPage(`<p>Stop marketing emails to <b>${esc(email)}</b>?</p><form method="post"><button style="background:#00593D;color:#fff;border:0;border-radius:8px;padding:12px 20px;font-size:16px">Unsubscribe</button></form>`));
});
app.post('/u/:tok', async (req, res) => {
  const email = await unsubEmail(req.params.tok); if (!email) return res.status(400).send(unsubPage('<p>This link is not valid.</p>'));
  await db.hset(K.optout, { ['e:' + email]: { at: Date.now(), via: 'link' } });
  res.send(unsubPage('<p>Done. You won\'t get marketing emails from us anymore.</p>'));
});

// The Gmail script (no key inside; it pairs itself on first run). Setup → "Open the Gmail script".
app.get('/api/script', (req, res) => res.type('text/plain').send(gmailScript()));
function gmailScript() {
  return `// Dielman HQ: reads estimate + confirmation emails and sends HQ's queued emails from this Gmail.
// Setup (once, logged into ${BIZ.email}): script.google.com > New project > paste this > Save >
// pick "setup" in the top bar > Run > Allow. It then runs every hour by itself.
const HQ = '${CFG.BASE}';
// Each email type has its own label so an estimate and a confirmation in the same thread are both read.
const FINDS = [
  { type: 'estimate', label: 'Dielman-HQ', q: 'in:sent "Here is your estimate for your move"', phrase: 'Here is your estimate for your move' },
  { type: 'confirm', label: 'Dielman-HQ-Conf', q: 'in:sent subject:confirmation -subject:onsite', phrase: '' },
];
function setup() {
  ScriptApp.getProjectTriggers().forEach(function (t) { ScriptApp.deleteTrigger(t); });
  key();
  ScriptApp.newTrigger('run').timeBased().everyHours(1).create();
  run();
}
function run() {
  const start = Date.now();
  reportWebApp();
  sendQueued();
  readEmails(start);
}
// Instant sending: after Deploy > Web app, HQ calls this the moment you press Send.
function doPost(e) {
  let b = {};
  try { b = JSON.parse(e.postData.contents); } catch (x) {}
  const out = function (o) { return ContentService.createTextOutput(JSON.stringify(o)).setMimeType(ContentService.MimeType.JSON); };
  if (!b.key || b.key !== PropertiesService.getScriptProperties().getProperty('KEY')) return out({ ok: false, error: 'bad key' });
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(60000)) return out({ ok: false, error: 'busy' });
  try { return out({ ok: true, sent: sendQueued() }); } finally { lock.releaseLock(); }
}
function reportWebApp() {
  const url = ScriptApp.getService().getUrl();
  if (url && /\/exec$/.test(url)) { try { post('/public/dielman/hook', { url: url }); } catch (e) {} }
}
// Pairs with HQ the first time and remembers the key in this script's own storage.
function key() {
  const props = PropertiesService.getScriptProperties();
  let k = props.getProperty('KEY');
  if (!k) {
    const r = UrlFetchApp.fetch(HQ + '/public/dielman/pair', { method: 'post', contentType: 'application/json', muteHttpExceptions: true, payload: JSON.stringify({ email: me() }) });
    if (r.getResponseCode() !== 200) throw new Error('Could not connect to Dielman HQ: ' + r.getContentText());
    k = JSON.parse(r.getContentText()).key;
    props.setProperty('KEY', k);
  }
  return k;
}
function post(path, obj) {
  obj.key = key();
  const r = UrlFetchApp.fetch(HQ + path, { method: 'post', contentType: 'application/json', muteHttpExceptions: true, payload: JSON.stringify(obj) });
  if (r.getResponseCode() !== 200) throw new Error('HQ said ' + r.getResponseCode() + ': ' + r.getContentText().slice(0, 200));
  return JSON.parse(r.getContentText());
}
function me() { return Session.getEffectiveUser().getEmail().toLowerCase(); }
function fromMe(m) { return m.getFrom().toLowerCase().indexOf(me()) >= 0; }
function item(type, th, phrase) {
  const msgs = th.getMessages();
  const mine = msgs.filter(function (m) { return !m.isDraft() && fromMe(m) && (!phrase || m.getPlainBody().indexOf(phrase) >= 0); });
  if (!mine.length) return null;
  const last = mine[mine.length - 1];
  const replied = msgs.some(function (m) { return !m.isDraft() && !fromMe(m) && m.getDate() > mine[0].getDate(); });
  return { type: type, threadId: th.getId(), to: last.getTo(), date: last.getDate().toISOString(), replied: replied, body: last.getPlainBody().slice(0, 5000) };
}
// New emails first, then old ones in batches (labels each thread once HQ has it), until ~4.5 minutes are used.
function readEmails(start) {
  // re-check recent estimates for customer replies
  sendThreads('estimate', GmailApp.search('label:Dielman-HQ "Here is your estimate for your move" newer_than:4d', 0, 100), null, FINDS[0].phrase);
  FINDS.forEach(function (f) {
    const label = GmailApp.getUserLabelByName(f.label) || GmailApp.createLabel(f.label);
    while (Date.now() - start < 270000) {
      const threads = GmailApp.search(f.q + ' -label:' + f.label, 0, 25);
      if (!threads.length) break;
      sendThreads(f.type, threads, label, f.phrase);
    }
  });
}
function sendThreads(type, threads, label, phrase) {
  const items = [];
  threads.forEach(function (th) { const it = item(type, th, phrase); if (it) items.push(it); });
  if (items.length) post('/public/dielman/ingest', { items: items });
  if (label) label.addToThreads(threads);
}
// Sends emails waiting in HQ (HQ decides how many per hour and per day).
function sendQueued() {
  const left = MailApp.getRemainingDailyQuota();
  if (left < 100) return 0;
  const r = UrlFetchApp.fetch(HQ + '/public/dielman/outbox?key=' + key() + '&max=' + Math.min(40, left - 100), { muteHttpExceptions: true });
  if (r.getResponseCode() !== 200) return 0;
  const done = [];
  (JSON.parse(r.getContentText()).items || []).forEach(function (it) {
    try { GmailApp.sendEmail(it.to, it.subject, it.text, { htmlBody: it.html, name: '${BIZ.name}' }); done.push({ id: it.id, ok: true }); }
    catch (e) { done.push({ id: it.id, ok: false, err: String(e) }); }
  });
  if (done.length) post('/public/dielman/sent', { done: done });
  return done.filter(function (d) { return d.ok; }).length;
}
`;
}

// Public SMS terms + privacy page (needed for the 10DLC text-message registration).
app.get(['/sms-terms', '/privacy'], (req, res) => res.set('Cache-Control', 'public, max-age=3600').send(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SMS Terms &amp; Privacy — ${esc(BIZ.name)}, Inc.</title>
<style>body{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;max-width:720px;margin:0 auto;padding:28px 20px 60px;color:#17201c;line-height:1.55}h1{color:#00593D;font-size:26px}h2{color:#00593D;font-size:18px;margin-top:28px}</style></head><body>
<h1>${esc(BIZ.name)}, Inc. — SMS Terms &amp; Privacy Policy</h1>
<p>Last updated: November 23, 2026</p>
<h2>Text message program</h2>
<p>${esc(BIZ.name)}, Inc. ("Dielman Moving") sends customer-care text messages to customers who give us their mobile number when they request a moving estimate or book a move, on our website estimate form or by phone. Messages are about the customer's own move: a thank-you on move day, a request to review our service, a follow-up about storage or junk removal, and a check-in on the anniversary of the move.</p>
<p>Message frequency varies (about 4 messages per move). Message and data rates may apply. Consent to receive texts is not a condition of any purchase.</p>
<h2>How to opt out or get help</h2>
<p>Reply <b>STOP</b> to any message to stop receiving texts. You will get one confirmation message and no further texts. Reply <b>HELP</b> for help, or call us at ${esc(BIZ.phone)} or email ${esc(BIZ.email)}.</p>
<p>Carriers are not liable for delayed or undelivered messages.</p>
<h2>Privacy</h2>
<p>We use your name, phone number, email address and move details only to provide our moving and storage services and to send the messages described above.</p>
<p><b>No mobile information will be shared with third parties or affiliates for marketing or promotional purposes.</b> Text messaging originator opt-in data and consent will not be shared with any third parties, except with the service providers that deliver our messages (such as our SMS provider), who may use it only to send our messages.</p>
<p>We do not sell your personal information. You can ask us to stop contacting you or to delete your information at any time by emailing ${esc(BIZ.email)} or calling ${esc(BIZ.phone)}.</p>
<h2>Contact</h2>
<p>${esc(BIZ.name)}, Inc.<br>${esc(BIZ.addr)}<br>${esc(BIZ.phone)} · ${esc(BIZ.email)}</p>
</body></html>`));

app.use((req, res) => res.status(404).send('Not found'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Dielman HQ on ' + PORT + (CFG.REDIS_URL ? ' (Upstash)' : ' (local file storage)')));
module.exports = { readEmail, parseMoveDate, bookedMoves, gmailScript };
