// Dungeon Quest Reborn Tracker - Node.js (khong can cai thu vien) - BAN MULTI-USER
// Chay: dung start.bat (sua cac dong "set ..." trong do).
//
// Che do 1 (nhu cu, 1 nguoi dung): chi can API_KEY.
// Che do 2 (multi-user): them DISCORD_CLIENT_ID, DISCORD_CLIENT_SECRET, PUBLIC_URL, ADMIN_DISCORD_IDS.
//   - Nguoi khac vao web -> dang nhap Discord -> nhap KEY ban cap -> dung tracker voi du lieu rieng.
//   - Ban quan ly key o trang /admin.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { spawn } = require('child_process'), os = require('os');
// BUILD_ID: ma hoa tu chinh noi dung file index.js dang chay -> MOI LAN sua file (kieu sua cua Claude), ma nay tu doi,
// khong can nho tang tay. Dung de web TU PHAT HIEN co ban index.js moi (server vua restart voi code khac) va tu tai lai
// trang, khong can nguoi dung bam Ctrl+F5 nua (xem BUILD_WATCH_JS).
const BUILD_ID = (() => {
  try { return crypto.createHash('md5').update(fs.readFileSync(__filename)).digest('hex').slice(0, 10); }
  catch { return String(Date.now()); }
})();
// SSE (Server-Sent Events): bao dashboard dang mo biet NGAY khi co account vua bao cao xong, khong can doi
// toi vong poll dinh ky (truoc gio la 0.5s) nua -> giam do tre tu "acc gui du lieu" den "web hien" gan nhu ve 0.
const sseClients = new Map();   // owner -> Set<res>
function notifyOwner(owner) {
  const set = sseClients.get(owner);
  if (!set || !set.size) return;
  for (const res of set) { try { res.write('data: u\n\n'); } catch {} }
}
// [GOC] const PORT = +process.env.PORT || 3000, KEY = process.env.API_KEY || '';
// [GOC] if (!KEY) { console.error('Thieu API_KEY. Sua start.bat hoac chay: API_KEY=matkhau node server.js'); process.exit(1); }
const PORT = +process.env.PORT || 3000; let KEY = process.env.API_KEY || '';
const HOST = process.env.HOST || '127.0.0.1';   // chi may nay truy cap duoc. Muon cho may khac trong mang LAN dung: set HOST=0.0.0.0
if (!KEY) { KEY = 'local-key'; console.log('[local] Chua dat API_KEY -> dung key mac dinh: local-key (dat API_KEY=... de doi)'); }
// ---- Che do "local agent": ban rieng cua index.js chay NGAM tren may nguoi choi (trong app Windows), KHONG dung Redis/Discord,
// CHI phuc vu /api/rejoin,/api/perf,/api/config (dieu khien truc tiep Roblox tren chinh may do). Trang web chinh (Vercel) goi sang
// day qua 127.0.0.1 de lam viec ma server tren mang (cloud) khong bao gio lam duoc: mo/dong Roblox, gioi han CPU, doc/ghi config local.
// Bat bang env LOCAL_AGENT=1 (xem desktop-client). Vi HOST mac dinh da la 127.0.0.1 (chi may nay goi duoc) nen chi can mo CORS
// cho dung 1 domain cua trang web la an toan - trang khac khong doc duoc phan hoi (trinh duyet tu chan).
const LOCAL_AGENT = process.env.LOCAL_AGENT === '1';
const LOCAL_AGENT_ORIGIN = process.env.LOCAL_AGENT_ORIGIN || 'https://trackerdqrtbao.online';

// ---- Cau hinh multi-user ----
const DISCORD_ID = process.env.DISCORD_CLIENT_ID || '', DISCORD_SECRET = process.env.DISCORD_CLIENT_SECRET || '';
const PUBLIC_URL = (process.env.PUBLIC_URL || (process.env.VERCEL_PROJECT_PRODUCTION_URL ? 'https://' + process.env.VERCEL_PROJECT_PRODUCTION_URL : '')).trim().replace(/\/+$/, '');
const ADMIN_IDS = new Set(String(process.env.ADMIN_DISCORD_IDS || '').split(',').map(x => x.trim()).filter(Boolean));
// Tu dong xoa user KHONG CO KEY sau N giay (mac dinh 60 giay; dat PURGE_NOKEY_SECONDS=0 de tat, =1 la 1 giay)
const NOKEY_GRACE_MS = (() => { const r = process.env.PURGE_NOKEY_SECONDS; if (r === undefined || r === '') return 60e3; const n = Number(r); return Number.isFinite(n) && n >= 0 ? n * 1000 : 60e3; })();
const DISCORD_API = process.env.DISCORD_API || 'https://discord.com/api';
const DISCORD_AUTHORIZE = process.env.DISCORD_AUTHORIZE || 'https://discord.com/oauth2/authorize';
const DISCORD_INVITE = 'https://discord.gg/5VAGQ6Bvfr';
const MULTI = !!(DISCORD_ID && DISCORD_SECRET && PUBLIC_URL);
if ((DISCORD_ID || DISCORD_SECRET) && !MULTI) console.error('[!] Thieu DISCORD_CLIENT_ID / DISCORD_CLIENT_SECRET / PUBLIC_URL -> multi-user DANG TAT.');
if (MULTI && !ADMIN_IDS.size) console.error('[!] Chua dat ADMIN_DISCORD_IDS -> chi vao /admin duoc tu localhost (may chay server).');
const SESSION_MS = 30 * 864e5;
const INV_MAX = Math.max(300, +process.env.INV_MAX || 1000);   // so dong item toi da luu cho moi account (khong cat bot kho do)

function cleanItemStats(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const stats = {};
  for (const field of ['physicalDamage', 'physicalPower', 'spellPower', 'health', 'upgrades', 'maxUpgrades', 'req']) {
    const stat = value[field];
    if (typeof stat === 'number' && Number.isFinite(stat)) stats[field] = stat;
  }
  return Object.keys(stats).length ? stats : null;
}

// ---- LUU TRU: Upstash Redis (REST) thay cho data.json / auth.json (Vercel khong co o dia ghi lau dai) ----
// dq:users, dq:licenses, dq:sessions : hash nho, nap moi request
// dq:acc:<owner>  : hash { <ten acc>: JSON } - chi nap kho cua nguoi dang goi
// dq:counts       : hash { <owner>: so acc }
// dq:img          : cache anh wiki
const REDIS_URL = (process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '';
// [GOC] if (!REDIS_URL || !REDIS_TOKEN) console.error('[!] Thieu KV_REST_API_URL / KV_REST_API_TOKEN (them Upstash Redis trong Vercel > Storage).');
const USE_REMOTE = !!(REDIS_URL && REDIS_TOKEN);   // co Upstash -> dung nhu cu; khong co -> luu vao file data.json canh index.js
async function redisRemote(cmds) {
  const r = await fetch(REDIS_URL + '/pipeline', {
    method: 'POST', headers: { authorization: 'Bearer ' + REDIS_TOKEN, 'content-type': 'application/json' }, body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new Error('redis HTTP ' + r.status);
  return (await r.json()).map(x => { if (x && x.error) throw new Error('redis: ' + x.error); return x.result; });
}
// ---- LUU TRU LOCAL: gia lap cac lenh Redis dang dung (HGETALL/HGET/HSET/HDEL/GET/SET), ghi ra data.json ----
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const localDb = { h: Object.create(null), k: Object.create(null) };
if (!USE_REMOTE) {
  try {
    const j = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    if (j && j.h) Object.assign(localDb.h, j.h);
    if (j && j.k) Object.assign(localDb.k, j.k);
    console.log('[local] Da nap du lieu tu ' + DATA_FILE);
  } catch (e) { console.log('[local] Chua co ' + DATA_FILE + ' -> se tao moi khi co du lieu.'); }
}
let localTimer = null;
function localFlush() {
  localTimer = null;
  try {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ h: localDb.h, k: localDb.k }));
    fs.renameSync(tmp, DATA_FILE);
  } catch (e) { console.error('[local] Loi ghi file:', e.message); }
}
const localSave = () => { if (!localTimer) localTimer = setTimeout(localFlush, 2000); };
function redisLocal(cmds) {
  let changed = false;
  const out = cmds.map(c => {
    const op = String(c[0]).toUpperCase(), key = c[1];
    if (op === 'HGETALL') { const h = localDb.h[key] || {}; const a = []; for (const f of Object.keys(h)) a.push(f, h[f]); return a; }
    if (op === 'HGET') { const h = localDb.h[key]; return h && f0(h, c[2]) ? h[c[2]] : null; }
    if (op === 'HSET') { const h = localDb.h[key] || (localDb.h[key] = Object.create(null)); let n = 0; for (let i = 2; i + 1 < c.length; i += 2) { if (!(c[i] in h)) n++; h[c[i]] = String(c[i + 1]); } changed = true; return n; }
    if (op === 'HDEL') { const h = localDb.h[key]; let n = 0; if (h) for (let i = 2; i < c.length; i++) if (c[i] in h) { delete h[c[i]]; n++; } if (h && !Object.keys(h).length) delete localDb.h[key]; changed = true; return n; }
    if (op === 'GET') {
      if (localDb.exp && localDb.exp[key] && Date.now() > localDb.exp[key]) { delete localDb.k[key]; delete localDb.exp[key]; return null; }
      return key in localDb.k ? localDb.k[key] : null;
    }
    if (op === 'SET') {   // ho tro them NX/PX (dung cho runsJobMarkClaim - chong dem trung Total Run qua nhieu may chu)
      let nx = false, px = null;
      for (let i = 3; i < c.length; i++) { const t = String(c[i]).toUpperCase(); if (t === 'NX') nx = true; else if (t === 'PX' && c[i + 1] != null) { px = +c[i + 1]; i++; } }
      if (nx) { const exp = localDb.exp && localDb.exp[key]; const live = key in localDb.k && !(exp && Date.now() > exp); if (live) return null; }
      localDb.k[key] = String(c[2]);
      if (px) { (localDb.exp || (localDb.exp = {}))[key] = Date.now() + px; } else if (localDb.exp) delete localDb.exp[key];
      changed = true; return 'OK';
    }
    throw new Error('redis local: chua ho tro lenh ' + op);
  });
  if (changed) localSave();
  return out;
}
const f0 = (h, f) => Object.prototype.hasOwnProperty.call(h, f);
const redis = cmds => USE_REMOTE ? redisRemote(cmds) : Promise.resolve().then(() => redisLocal(cmds));
process.on('exit', () => { if (!USE_REMOTE && localTimer) localFlush(); });
process.on('SIGINT', () => process.exit(0));
const pairs = arr => { const raw = {}; if (Array.isArray(arr)) for (let i = 0; i + 1 < arr.length; i += 2) raw[arr[i]] = arr[i + 1]; return raw; };
const parseAll = raw => { const o = {}; for (const [k, v] of Object.entries(raw)) { try { o[k] = JSON.parse(v); } catch {} } return o; };

let owners = {}, dirty = false;                 // owners chi chua kho cua owner da nap trong request nay
const auth = { users: {}, licenses: {}, sessions: {} };
let authDirty = false;
const saveAuth = () => { authDirty = true; };
const tokenIdx = new Map();
let counts = {};
let trade = { owner: '', jobs: {}, loaded: false };   // lenh trade cua owner dang goi (dq:trade:<owner>)
let snap = { users: {}, licenses: {}, sessions: {}, acc: {}, trade: {} };   // chuoi JSON luc nap, de so sanh khi luu
const loadedOwners = new Set();
const bucket = o => owners[o] || (owners[o] = {});
const accCount = o => owners[o] ? Object.keys(owners[o]).length : (+counts[o] || 0);
// Lenh "Force lobby" (nut trong tab Config > Parties & Runs): owner+'\u0001'+ten(thuong) -> thoi diem bam nut.
// /api/report tra {lobby:true} DUNG 1 LAN cho dung account roi tu xoa o day; script Lua doc duoc la teleport ve Lobby ngay.
const forceLobbyCmd = new Map();
const FORCE_LOBBY_TTL = 5 * 60 * 1000;   // lenh qua cu (acc khong bao cao lai) -> coi nhu het han, don rac
// Khi lenh Force lobby THAT SU duoc giao cho script (takeLobbyCmd tra true), acc se teleport ve Lobby trong vai giay
// toi - danh dau "dang cho" o day de Total Run biet lan ve lobby SAP TOI cua acc nay la DO BI BAT VE (khong phai thua
// that trong game) va BO QUA, khong tinh vao thua. Xem cho dung o runsTrack().
const forceLobbyPending = new Map();
const FORCE_LOBBY_PENDING_TTL = 60 * 1000;   // du rong cho teleport xong (thuong vai giay) + 1-2 nhip bao cao
function takeLobbyCmd(owner, name) {   // doc + xoa lenh "Force lobby" cua 1 account (1 lan duy nhat)
  const key = owner + '\u0001' + String(name).toLowerCase(), at = forceLobbyCmd.get(key);
  if (at === undefined) return false;
  forceLobbyCmd.delete(key);
  if (Date.now() - at >= FORCE_LOBBY_TTL) return false;
  forceLobbyPending.set(key, Date.now());
  return true;
}
function takeForceLobbyPending(owner, name) {   // runsTrack goi khi thay acc vua ve lobby: co phai do Force lobby khong?
  const key = owner + '\u0001' + String(name).toLowerCase(), at = forceLobbyPending.get(key);
  if (at === undefined) return false;
  forceLobbyPending.delete(key);
  return Date.now() - at < FORCE_LOBBY_PENDING_TTL;
}

async function loadOwner(o) {
  if (!o || loadedOwners.has(o)) return;
  const [h] = await redis([['HGETALL', 'dq:acc:' + o]]);
  const raw = pairs(h); snap.acc[o] = raw; owners[o] = parseAll(raw); loadedOwners.add(o);
}
let has = { licenses: false, sessions: false };
async function loadState(req) {
  owners = {}; dirty = false; authDirty = false; loadedOwners.clear(); trade = { owner: '', jobs: {}, loaded: false, hist: {}, histNew: {}, histDel: [] }; snap = { users: {}, licenses: {}, sessions: {}, acc: {}, trade: {} }; sellStore.clear(); buyStore.clear(); RUNS.boxes.clear();
  auth.users = {}; auth.licenses = {}; auth.sessions = {}; counts = {};
  const pth = String(req.url || '').split('?')[0];
  const needAll = pth === '/' || pth.startsWith('/admin') || pth.startsWith('/auth/') || pth === '/api/me' || pth === '/api/me/token' || pth === '/api/redeem';
  const needSess = needAll || /(?:^|;\s*)dq_sid=/.test(String(req.headers.cookie || ''));
  has = { licenses: needAll, sessions: needSess };
  const cmds = [['HGETALL', 'dq:users']];
  if (needAll) cmds.push(['HGETALL', 'dq:licenses'], ['HGETALL', 'dq:counts']);
  if (needSess) cmds.push(['HGETALL', 'dq:sessions']);
  const out = await redis(cmds); let k = 0;
  snap.users = pairs(out[k++]); auth.users = parseAll(snap.users);
  if (needAll) { snap.licenses = pairs(out[k++]); auth.licenses = parseAll(snap.licenses); counts = pairs(out[k++]); }
  if (needSess) { snap.sessions = pairs(out[k++]); auth.sessions = parseAll(snap.sessions); }
  tokenIdx.clear();
  for (const x of Object.values(auth.users)) if (x && x.token) tokenIdx.set(x.token, x.id);
  if (needAll && !pth.startsWith('/auth/')) purgeNoKeyUsers(Date.now());
  // chi nap kho du lieu cua nguoi dang goi API
  const a = authenticate(req);
  if (a && a.owner) {
    const lightTrade = pth === '/api/trade/poll' || pth === '/api/trade/wait' || pth === '/api/trade/status' || pth === '/api/trade/list' || pth === '/api/trade/history' || pth === '/api/trade/history/clear' || pth === '/api/trade/sell/status' || pth === '/api/trade/sell/list' || pth === '/api/trade/buy/status' || pth === '/api/trade/buy/list' || pth === '/api/trade/buy/clear';   // khong nap kho do -> nhe, goi thuong xuyen duoc
    if (!lightTrade) await loadOwner(a.owner);
    if (lightTrade || pth.startsWith('/api/trade/') || pth === '/api/report' || pth === '/api/accounts') {
      await loadTrade(a.owner, pth === '/api/trade/history' || pth === '/api/trade/history/clear');
      await loadSellBox(a.owner); await loadBuyBox(a.owner);   // can cho CA /api/trade/wait + /api/trade/poll (tradeInfoFor -> sellFor/buyFor), khong chi /api/trade/sell|buy/*
    }
    if (pth === '/api/report' || pth.startsWith('/api/runs')) await loadRunsBox(a.owner);   // Total Run (thang/thua + thoi gian) - xem ghi chu o loadRunsBox
  }
}
function diffCmds(key, cur, old, cmds) {
  const set = [];
  for (const [f, v] of Object.entries(cur)) { const j = JSON.stringify(v); if (old[f] !== j) set.push(f, j); }
  if (set.length) cmds.push(['HSET', key, ...set]);
  const del = Object.keys(old).filter(f => !(f in cur));
  if (del.length) cmds.push(['HDEL', key, ...del]);
}
async function saveState() {
  const cmds = [];
  if (authDirty) {
    diffCmds('dq:users', auth.users, snap.users, cmds);
    if (has.licenses) diffCmds('dq:licenses', auth.licenses, snap.licenses, cmds);
    if (has.sessions) diffCmds('dq:sessions', auth.sessions, snap.sessions, cmds);
  }
  for (const o of loadedOwners) {
    const before = cmds.length;
    diffCmds('dq:acc:' + o, owners[o] || {}, snap.acc[o] || {}, cmds);
    const n = Object.keys(owners[o] || {}).length;
    if (n !== Object.keys(snap.acc[o] || {}).length) cmds.push(['HSET', 'dq:counts', o, String(n)]);
  }
  if (trade.loaded) diffCmds('dq:trade:' + trade.owner, trade.jobs, snap.trade || {}, cmds);
  if (trade.loaded && trade.histNew && Object.keys(trade.histNew).length) {   // lich su trade: ghi them cac lenh vua ket thuc
    const f = []; for (const [id, e] of Object.entries(trade.histNew)) f.push(id, JSON.stringify(e));
    cmds.push(['HSET', 'dq:tradehist:' + trade.owner, ...f]);
  }
  if (trade.loaded && trade.histDel && trade.histDel.length) cmds.push(['HDEL', 'dq:tradehist:' + trade.owner, ...trade.histDel]);
  if (trade.loaded && trade.goldDirty) cmds.push(['SET', 'dq:goldev:' + trade.owner, JSON.stringify(trade.goldEv || [])]);
  if (trade.loaded && trade.ver) cmds.push(['SET', 'dq:tradever:' + trade.owner, trade.ver]);   // bao cho cac acc dang cho: co lenh moi
  for (const [o, b] of sellStore) if (b.loaded && b.dirty) { cmds.push(['SET', 'dq:sell:' + o, JSON.stringify(b.jobs)]); b.dirty = false; }
  for (const [o, b] of buyStore) if (b.loaded && b.dirty) { cmds.push(['SET', 'dq:buy:' + o, JSON.stringify(b.jobs)]); b.dirty = false; }
  for (const [o, b] of RUNS.boxes) if (b.loaded && b.dirty) { cmds.push(['SET', 'dq:runs:' + o, JSON.stringify(b.data)]); b.dirty = false; }
  if (cmds.length) await redis(cmds);
}

const HTML = "<!DOCTYPE html>\n<html lang=\"vi\"><head><meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">\n<title>NGUYEN PHAN TRI BAO</title>\n<style>\n:root{--bg:#08090b;--pn:#0e1013;--cd:#12151a;--bd:#1d2128;--tx:#e8eaed;--mu:#8a919c;--ok:#4ade80;--cy:#5eead4;--am:#fbbf24;--pu:#c4b5fd;--pk:#f472b6;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}\n@media (prefers-color-scheme:light){:root:not([data-theme=\"dark\"]){--bg:#f4f5f7;--pn:#fff;--cd:#f6f7f9;--bd:#e3e6ea;--tx:#15181d;--mu:#6b7280;--ok:#16a34a;--cy:#0d9488;--am:#d97706;--pu:#7c3aed;--pk:#db2777}}\n:root[data-theme=\"light\"]{--bg:#f4f5f7;--pn:#fff;--cd:#f6f7f9;--bd:#e3e6ea;--tx:#15181d;--mu:#6b7280;--ok:#16a34a;--cy:#0d9488;--am:#d97706;--pu:#7c3aed;--pk:#db2777}\nhtml{scroll-padding-top:env(safe-area-inset-top,0px)}\nbody{margin:0;background:var(--bg);color:var(--tx);font:13px/1.45 system-ui,-apple-system,\"Segoe UI\",sans-serif}\n.w{max-width:1280px;margin:0 auto;padding:14px}\nbutton,input,select,textarea{font:inherit;color:var(--tx)}\n.mu{color:var(--mu)}.rw{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-top:12px}\n.pn{background:var(--pn);border:1px solid var(--bd);border-radius:16px;padding:14px}\n.top{display:grid;grid-template-columns:minmax(200px,280px) 1fr;gap:12px;margin-top:12px}\n@media(max-width:720px){.top{grid-template-columns:1fr}}\n.sg{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}\n.st{background:var(--cd);border:1px solid var(--bd);border-radius:12px;padding:14px}\n.st small{color:var(--mu);font-size:12px}.st b{display:block;font-size:26px;margin-top:8px}\n.sub{margin-top:6px;font-size:13px;font-weight:600;min-height:1em}\n.bt,select,#q,#iq,.pop input{background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:8px 12px}\n.bt{cursor:pointer}.bt:hover{border-color:var(--mu)}.bt.on{background:var(--tx);color:var(--bg)}\n#q{flex:1;min-width:200px}\n.rd{display:flex;gap:14px}.rd label{cursor:pointer}\n.wr{position:relative}.pop{display:none;position:absolute;top:110%;left:0;z-index:5;background:var(--pn);border:1px solid var(--bd);border-radius:12px;padding:10px;min-width:180px;flex-direction:column;gap:8px}\n.pop.s{display:flex}.pop input[type=number]{width:100px;padding:5px 8px}.pop select{padding:4px 8px}\n.mr{display:flex;align-items:center;gap:6px}\n.sw{display:inline-block;width:18px;height:18px;border-radius:5px}\n.bg{display:inline-block;padding:2px 9px;border-radius:999px;background:var(--cd);border:1px solid var(--bd);font-size:12px}\n.bg.on{color:var(--ok)}.gp{display:flex;gap:4px;flex-wrap:wrap;min-width:170px}.gp .bg{padding:2px 6px;font-size:11px}\n.gp .gp-vip.on{color:#fbbf24;background:#fbbf2422;border-color:#fbbf24}.gp .gp-xp2.on{color:#c084fc;background:#c084fc22;border-color:#c084fc}.gp .gp-gold2.on{color:#fbbf24;background:#fbbf2422;border-color:#fbbf24}.bg.up{color:#fbbf24;background:#fbbf2422;border-color:#fbbf24}.bg.up0{color:var(--mu)}.bg.ban{color:#fff;background:#be123c;border-color:#fb7185;font-weight:600;margin-left:4px}.own{display:inline-block;margin-top:5px;padding:1px 8px;border-radius:6px;background:rgba(0,0,0,.4);color:#fff;font-size:12px;font-weight:600;word-break:break-all}.bg.cap{color:#f43f5e;background:#f43f5e22;border-color:#f43f5e;font-weight:600;margin-left:4px}.bg.eq{color:#fff;background:#60a5fa33;border-color:#60a5fa;font-weight:600}.gp .gp-drop.on{color:#60a5fa;background:#60a5fa22;border-color:#60a5fa}.gp .gp-freeResets.on{color:#4ade80;background:#4ade8022;border-color:#4ade80}\n.tb{display:inline-block;min-width:26px;text-align:center;padding:2px 10px;border-radius:999px;font-weight:700;border:1px solid}\n.tw{overflow-x:auto;background:var(--pn);border:1px solid var(--bd);border-radius:16px;margin-top:12px}\ntable{border-collapse:collapse;width:100%;min-width:820px}\nth,td{text-align:left;padding:11px 14px;border-bottom:1px solid var(--bd);white-space:nowrap}\nth{color:var(--mu);font-size:12px;font-weight:600;cursor:pointer;user-select:none}\ntr.r{cursor:pointer}tr.r:hover td{background:var(--cd)}\nth.ck,td.ck{width:34px;padding-right:0;cursor:default}.cb{width:16px;height:16px;cursor:pointer;accent-color:#60a5fa;vertical-align:middle}.bt.dg{border-color:#f43f5e;color:#f43f5e}.bt.dg:disabled{opacity:.4;cursor:not-allowed}.bt.dg:not(:disabled):hover{background:#f43f5e22}\n.empty{padding:40px;text-align:center;color:var(--mu)}\n.ov{position:fixed;inset:0;background:rgba(0,0,0,.65);display:none;align-items:center;justify-content:center;padding:16px;z-index:9}.ov.s{display:flex}\n.am{width:min(900px,100%);max-height:88vh;overflow:auto;background:var(--pn);border:1px solid var(--bd);border-radius:16px;padding:18px}\n.th{margin:18px 0 8px;font-weight:700;letter-spacing:.06em}\n.ig{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:10px}\n.ic{display:flex;gap:10px;align-items:center;padding:8px 10px;border-radius:12px;border:1px solid var(--bd);background:var(--cd)}\n.im{width:46px;height:46px;flex:none;border-radius:10px;background:var(--bg);display:flex;align-items:center;justify-content:center;font-weight:700;color:var(--mu);overflow:hidden}.im img{width:100%;height:100%;object-fit:contain}\ntextarea{width:100%;min-height:220px;box-sizing:border-box;background:var(--bg);border:1px solid var(--bd);border-radius:10px;padding:10px;font:12px/1.4 ui-monospace,Menlo,monospace}\n#t{position:fixed;left:50%;transform:translateX(-50%);bottom:calc(20px + env(safe-area-inset-bottom,0px));background:var(--tx);color:var(--bg);padding:8px 14px;border-radius:10px;display:none;z-index:10}\n.discord-button{grid-column:span 2;display:flex;width:100%;min-height:118px;box-sizing:border-box;align-items:center;justify-content:center;gap:10px;margin-top:0;padding:12px 16px;border:1px solid #7983f5;border-radius:14px;background:#5865f2;color:#fff;font:600 15px/1.2 system-ui,-apple-system,\"Segoe UI\",sans-serif;cursor:pointer;transition:background .15s,transform .15s}.discord-button:hover{background:#4752c4}.discord-button:active{transform:scale(.99)}.discord-button svg{width:26px;height:26px;flex:none}@media(max-width:520px){.discord-button{gap:10px;min-height:118px}}@media(max-width:390px){.discord-button{grid-column:1/-1;min-height:88px}}.discord-button{flex-direction:row;text-decoration:none}.discord-button:hover{text-decoration:none}.discord-button .discord-title{font-size:18px}.discord-button .discord-click{font-size:42px;font-weight:900;letter-spacing:.04em;text-shadow:0 2px 8px #252e9c}@media(max-width:520px){.discord-button{gap:10px}.discord-button .discord-title{font-size:16px}.discord-button .discord-click{font-size:34px}}.dr{display:flex;gap:8px;align-items:center;padding:6px 2px;border-bottom:1px solid var(--bd)}.dr .bt{padding:0 8px;margin-left:4px}#df{overflow:visible}#dfl{scrollbar-width:auto;scrollbar-color:var(--mu) var(--cd)}#dfl::-webkit-scrollbar{width:10px}#dfl::-webkit-scrollbar-track{background:var(--cd);border-radius:8px}#dfl::-webkit-scrollbar-thumb{background:var(--mu);border-radius:8px}#dhid:empty{display:none}#dmin{background:var(--pn);border:1px solid var(--bd);border-radius:10px}\n</style></head><body><div class=\"w\">\n<div class=\"rw\" style=\"margin-top:0;justify-content:space-between\"><b style=\"font-size:17px\">⚔️ NGUYEN PHAN TRI BAO</b><button class=\"bt\" id=\"th\">🌓</button></div>\n<div class=\"top\">\n<div class=\"pn\"><small class=\"mu\">▣ Online / Total</small><div style=\"margin-top:14px;font-size:34px;font-weight:700\"><span id=\"on\" style=\"color:var(--ok)\"></span> <span id=\"tt\" class=\"mu\" style=\"font-size:18px\"></span></div></div>\n<div class=\"pn\"><div class=\"sg\" id=\"sg\"></div></div>\n</div>\n<div class=\"pn\" style=\"margin-top:12px\"><div class=\"rw\" style=\"margin:0 0 6px;justify-content:space-between\"><b>🔥 ULT/Legend DROP!!!</b><span class=\"rw\" style=\"margin:0;gap:8px\"><span class=\"mu\" id=\"dn\" style=\"font-size:12px\"></span></span></div><div id=\"df\"></div></div>\n<div class=\"rw\"><input id=\"q\" placeholder=\"Tìm username, ghi chú, item...\">\n<div class=\"rd\"><label><input type=\"radio\" name=\"f\" value=\"all\" checked> Tất cả</label><label><input type=\"radio\" name=\"f\" value=\"on\"> Online</label><label><input type=\"radio\" name=\"f\" value=\"off\"> Offline</label><label><input type=\"radio\" name=\"f\" value=\"cap\"> Captcha/Ban</label></div></div>\n<div class=\"rw\">\n<div class=\"wr\"><button class=\"bt\" data-p=\"pc\">☰ Cột</button><div class=\"pop\" id=\"pc\"></div></div>\n<div class=\"wr\"><button class=\"bt\" data-p=\"pf\">⏷ Lọc chỉ số</button><div class=\"pop\" id=\"pf\"><label>Level từ <input id=\"fa\" type=\"number\"></label><label>Level đến <input id=\"fb\" type=\"number\"></label><label>Gold tối thiểu <input id=\"fg\" type=\"number\"></label></div></div>\n<div class=\"wr\"><button class=\"bt\" id=\"iall\" type=\"button\">📦 Tất cả item</button></div>\n<div class=\"wr\"><button class=\"bt\" data-p=\"px\">⎘ Xuất</button><div class=\"pop\" id=\"px\"><button class=\"bt\" data-x=\"json\">Copy JSON</button><button class=\"bt\" data-x=\"csv\">Copy CSV</button></div></div>\n<div class=\"wr\"><button class=\"bt\" data-p=\"pm\" title=\"M?u vi?n/n?n c?a item bi?u th? ?? hi?m; ch?n c?p t??ng ?ng cho t?ng m?u.\">🎨 Màu độ hiếm</button><div class=\"pop\" id=\"pm\" style=\"min-width:300px\"></div></div>\n<span style=\"margin-left:auto\"></span><span class=\"mu\" id=\"lv\"></span><button class=\"bt\" id=\"rf\">⟳ Làm mới</button></div>\n<div class=\"rw\"><span class=\"bg\" id=\"ic\"></span><button class=\"bt\" id=\"ha\" type=\"button\" aria-pressed=\"false\">Hide Name</button><button class=\"bt dg\" id=\"del\" type=\"button\" disabled>Xoá account (0)</button><span class=\"mu\" style=\"margin-left:6px\">Gắn cờ:</span><button class=\"bt\" data-fl=\"rejoin\" disabled>Captcha rejoin</button><button class=\"bt\" data-fl=\"ban\" disabled>Ban</button><button class=\"bt\" data-fl=\"\" disabled>Bỏ cờ</button><span style=\"margin-left:auto\"></span>\n<select id=\"sz\"><option>25</option><option selected>50</option><option>100</option></select><span class=\"bg\" id=\"pgl\"></span><button class=\"bt\" id=\"pv\">‹</button><button class=\"bt\" id=\"nx\">›</button></div>\n<div class=\"tw\"><table><thead><tr id=\"hd\"></tr></thead><tbody id=\"tb\"></tbody></table><div class=\"empty\" id=\"em\" style=\"display:none\">Không có tài khoản nào</div></div>\n</div>\n<div class=\"ov\" id=\"am\"><div class=\"am\"><div class=\"rw\" style=\"margin-top:0;justify-content:space-between;align-items:flex-start\"><div id=\"ah\"></div><button class=\"bt\" id=\"ax\">✕</button></div><div id=\"ab\"></div></div></div>\n<div class=\"ov\" id=\"im\"><div class=\"am\" style=\"width:min(1100px,100%)\"><div class=\"rw\" style=\"margin-top:0;justify-content:space-between\"><b style=\"font-size:17px\">📦 Tất cả item <span class=\"mu\" id=\"icnt\"></span></b><button class=\"bt\" id=\"ix\">✕</button></div><div class=\"rw\"><input id=\"iq\" placeholder=\"Tìm tên item hoặc tên account...\" style=\"flex:1;min-width:200px\"><select id=\"ir\"><option value=\"\">Tất cả độ hiếm</option><option value=\"6\">Ult</option><option value=\"5\">Legend</option><option value=\"4\">Epic</option><option value=\"3\">Rare</option><option value=\"2\">Uncommon</option><option value=\"1\">Common</option><option value=\"0\">Chưa rõ</option></select><select id=\"it\"><option value=\"\">Tất cả Item</option><option value=\"armor\">Armor</option><option value=\"weapon\">Weapon</option><option value=\"skill\">Skill</option></select><button class=\"bt\" id=\"ieq\" type=\"button\">Ẩn đồ đang mang</button></div><div id=\"ib\"></div></div></div>\n<div class=\"ov\" id=\"m\"><div class=\"am\" style=\"width:min(640px,100%)\"><b>Dữ liệu xuất</b><p class=\"mu\">Không copy tự động được — hãy chọn và copy thủ công.</p><textarea id=\"ta\"></textarea><div class=\"rw\" style=\"justify-content:flex-end\"><button class=\"bt\" id=\"x\">Đóng</button></div></div></div>\n<div id=\"t\"></div>\n<script>\nconst HK='dq_hist_v2',TIER={6:{n:'Ult',c:'#f43f5e'},5:{n:'Legend',c:'#f59e0b'},4:{n:'Epic',c:'#a78bfa'},3:{n:'Rare',c:'#60a5fa'},2:{n:'Uncommon',c:'#4ade80'},1:{n:'Common',c:'#9ca3af'}};\nconst $=id=>document.getElementById(id);\nfunction ld(k,f){try{const s=localStorage.getItem(k);if(s)return JSON.parse(s)}catch(e){}return f}\nfunction sv(k,v){try{localStorage.setItem(k,JSON.stringify(v))}catch(e){}}\nlet RMAP=ld('dq_rmap',{})||{},IMG=ld('dq_img',{})||{},pend=new Set(),failed=new Set(),mAcc='',pmSig='',data=[],flt='all',qs='',sort={k:'name',d:1},hid=new Set(),page=1,size=50,RG={a:'',b:'',g:''},hideAccounts=ld('dq_hide_accounts',false)===true;const displayName=a=>hideAccounts?'HIDE':a.name;\nconst esc=s=>String(s??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));\nconst sh=n=>{n=+n||0;const a=Math.abs(n);for(const[v,u]of[[1e12,'T'],[1e9,'B'],[1e6,'M'],[1e3,'K']])if(a>=v){const x=n/v;return(x>=100?x.toFixed(0):x>=10?x.toFixed(1):x.toFixed(2)).replace(/(\\.\\d*?)0+$/,'$1').replace(/\\.$/,'')+u}return String(n)};\nconst capBadge=a=>(a.flag==='ban'||a.flag==='rejoin')?`<span class=\"bg ${a.flag==='ban'?'ban':'cap'}\" title=\"${esc(a.err||(a.manual?'Gắn thủ công':''))}\">${a.flag==='ban'?'Ban':a.flag==='rejoin'?'Captcha rejoin':'Captcha lock'}</span>`:'';\nconst fmt=n=>(+n||0).toLocaleString('en-US'),inv=a=>a.inv||[],isOn=a=>!!a._t&&Date.now()-a._t<12000;\nconst LOBBY_PLACE='77649408247578';\nconst where=a=>{if(!isOn(a))return '';const q=String(a.placeId||'');return !q||q===LOBBY_PLACE?'lobby':'dungeon'};\nconst placeLabel=a=>!isOn(a)?'Offline':where(a)==='lobby'?'Lobby':'Dungeon';\nconst placeColor=a=>!isOn(a)?'#8a919c':where(a)==='lobby'?'#60a5fa':'#a78bfa';\nconst rn=i=>{\n  const explicit=String(i.r||'').trim();\n  if(explicit)return explicit;\n  const c=cc(i);\n  return c && Object.prototype.hasOwnProperty.call(RMAP,c)?RMAP[c]:'';\n};\nconst cc=i=>/^#[0-9a-f]{6}$/i.test(i.c||'')?i.c:'';\nconst normalizeRarity=v=>String(v||'').toLowerCase().replace(/[^a-z]/g,'');\nconst tier=i=>{\n  const direct=normalizeRarity(rn(i));\n  if(!direct)return 0;\n  if(direct.startsWith('ult')||direct.includes('ultimate'))return 6;\n  if(direct.startsWith('legend'))return 5;\n  if(direct.startsWith('epic'))return 4;\n  if(direct.startsWith('rare'))return 3;\n  if(direct.startsWith('uncommon'))return 2;\n  if(direct.startsWith('common'))return 1;\n  return 0;\n};\nconst enchantCode=i=>{const value=String(i.e||'').toLowerCase().replace(/[^a-z]/g,'');if(value==='eir'||value==='enhancedinnerrage')return'EIR';if(value==='eif'||value==='enhancedinnerfocus')return'EIF';if(i.t==='ability'){const name=String(i.n||'').toLowerCase().replace(/[^a-z]/g,'');if(name==='enhancedinnerrage')return'EIR';if(name==='enhancedinnerfocus')return'EIF'}return''};\nconst cnt=(a,f)=>inv(a).filter(f).reduce((s,i)=>s+(+i.q||1),0),tcn=(a,t)=>cnt(a,i=>tier(i)===t);\nconst tbd=(n,t)=>n?`<span class=\"tb\" style=\"color:${TIER[t].c};border-color:${TIER[t].c}\">${n}</span>`:'<span class=\"mu\">—</span>';\nfunction boostTime(s){const m=Math.floor(s/60),h=Math.floor(m/60);return h?h+'h '+m%60+'m':m+'m'}\nfunction passBadges(a){const p=a.gpass||{},boost=Math.max(0,Math.floor(+a.xpBoostSeconds||0)),defs=[['vip','VIP'],['xp2','XP'],['gold2','x2 Gold'],['drop','Drop'],['freeResets','RESET']],labels=defs.filter(([k])=>p[k]||(k==='xp2'&&boost>0));return labels.length?`<div class=\"gp\">${labels.map(([k,n])=>{const active=!!p[k]||(k==='xp2'&&boost>0),title=k==='xp2'&&boost>0?'XP boost active - '+boostTime(boost)+' remaining':(active?'Owned':'Not owned');return `<span class=\"bg gp-${k} ${active?'on':''}\" title=\"${title}\">${n}</span>`}).join('')}</div>`:'<span class=\"mu\">-</span>'}\nfunction ago(t){if(!t)return'';const s=(Date.now()-t)/1e3;return s<60?'':s<3600?Math.floor(s/60)+'p trước':s<86400?Math.floor(s/3600)+'h trước':Math.floor(s/86400)+'d trước'}\nfunction fmtCountdown(ms){if(ms<=0)return 'READY?';const s=Math.floor(ms/1000),h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;return h>0?(h+'h '+m+'m'):(m+'m '+sc+'s')}\nfunction dailyCell(a){\n if(a.dailyReward==='READY')return `<span class=\"bg on\" style=\"color:var(--ok);border-color:var(--ok)\">READY</span>`;\n if(a.dailyResetAt)return `<span class=\"bg\" data-reset=\"${a.dailyResetAt}\" style=\"color:var(--am);border-color:var(--bd)\" title=\"Đọc đúng từ game (PlayerGui.DailyRewardCountdown)\">${fmtCountdown(a.dailyResetAt-Date.now())}</span>`;\n if(a.dailyClaimedAt){const resetAt=a.dailyClaimedAt+864e5;return `<span class=\"bg\" data-reset=\"${resetAt}\" style=\"color:var(--mu);border-color:var(--bd)\" title=\"Chưa đọc được giờ thật, ước tính 24h từ lúc nhận (${new Date(a.dailyClaimedAt).toLocaleString()})\">~${fmtCountdown(resetAt-Date.now())}</span>`}\n if(a.dailyReward)return `<span class=\"mu\" title=\"Chưa rõ mốc đã nhận, hiện chữ thô từ game\">${esc(a.dailyReward)}</span>`;\n return `<span class=\"bg\" style=\"color:var(--mu);border-color:var(--bd)\" title=\"Chưa có dữ liệu daily (acc offline hoặc chưa chạy script mới) — giả định chưa nhận\">READY</span>`;\n}\nconst COLS=[\n['name','Account',a=>a.name,a=>`<b>${esc(displayName(a))}</b>`],\n['gpass','GPass',a=>Object.values(a.gpass||{}).filter(Boolean).length+((+a.xpBoostSeconds||0)>0&&!(a.gpass||{}).xp2?1:0),a=>passBadges(a)],\n['st','Status',a=>!isOn(a)?0:where(a)==='lobby'?3:2,a=>`<span class=\"bg on\" style=\"color:${placeColor(a)};border-color:${placeColor(a)}\">${placeLabel(a)}</span> <span class=\"mu\">${ago(a.updated)}</span>${capBadge(a)}`],\n['daily','Daily',a=>a.dailyReward==='READY'?1e15:-(a.dailyResetAt||(a.dailyClaimedAt?a.dailyClaimedAt+864e5:0)),a=>dailyCell(a)],\n['level','Level',a=>+a.level||0,a=>fmt(a.level)],\n['gold','Gold',a=>+a.gold||0,a=>`<span title=\"${fmt(a.gold)}\" style=\"color:var(--cy)\">${sh(a.gold)}</span>`],\n['u','Ult',a=>tcn(a,6),a=>tbd(tcn(a,6),6)],\n['l','Legend',a=>tcn(a,5),a=>tbd(tcn(a,5),5)],\n['note','Note',a=>a.note||'',a=>esc(a.note)||'<span class=\"mu\">—</span>']];\n$('pc').innerHTML=COLS.slice(1).map(c=>`<label><input type=\"checkbox\" data-c=\"${c[0]}\" checked> ${c[1]}</label>`).join('');\nconst sel=new Set();let lastRows=[];\nfunction rows(){const k=qs.toLowerCase();return data.filter(a=>{const on=isOn(a),L=+a.level||0,G=+a.gold||0;\n if(flt==='on'&&!on||flt==='off'&&on||flt==='cap'&&!a.flag)return false;\n if(RG.a!==''&&L<+RG.a||RG.b!==''&&L>+RG.b||RG.g!==''&&G<+RG.g)return false;\n return!k||[a.name,a.note,...inv(a).map(i=>i.n)].join(' ').toLowerCase().includes(k)})}\nfunction stats(){const n=data.length,tg=data.reduce((s,a)=>s+(+a.gold||0),0),all=f=>data.reduce((s,a)=>s+cnt(a,f),0),ct=t=>all(i=>tier(i)===t),en=k=>all(i=>enchantCode(i)===k);\n const nl=data.filter(a=>isOn(a)&&where(a)==='lobby').length,nd=data.filter(a=>isOn(a)&&where(a)==='dungeon').length;$('on').textContent=data.filter(isOn).length;$('on').title='Lobby '+nl+' · Dungeon '+nd;$('tt').textContent='/ '+n;\n const S=[['ALL GOLD',sh(tg),'--cy',`<span class=\"mu\">TB ${sh(n?tg/n:0)}/acc</span>`],['Best LV',fmt(Math.max(0,...data.map(a=>+a.level||0))),'--pk',''],\n ['Best item',ct(6)+ct(5),'--am',`<span style=\"color:${TIER[6].c}\">Ult ${ct(6)}</span> · <span style=\"color:${TIER[5].c}\">Legend ${ct(5)}</span>`],\n ['CAPTCHA / BAN',data.filter(a=>a.flag==='ban'||a.flag==='rejoin').length,'--pk',`<span style=\"color:#fbbf24\" title=\"Bị captcha rejoin (Error Code 773)\">Rejoin ${data.filter(a=>a.flag==='rejoin').length}</span> · <span style=\"color:#fb7185\" title=\"Bị ban (Error Code 600)\">Ban ${data.filter(a=>a.flag==='ban').length}</span>`],\n ['ENCHANT LEGEND',en('EIR')+en('EIF'),'--ok',`<span title=\"Enhanced Inner Rage\" style=\"color:var(--ok)\">EIR ${en('EIR')}</span> · <span title=\"Enhanced Inner Focus\" style=\"color:var(--pk)\">EIF ${en('EIF')}</span>`]];\n $('sg').innerHTML=S.map(s=>`<div class=\"st\"><small>${s[0]}</small><b style=\"color:var(${s[2]})\">${s[1]}</b><div class=\"sub\">${s[3]}</div></div>`).join('')}\nfunction chart(){const h=ld(HK,[]),W=1000,H=110,v=h.length>1?h.map(p=>p[1]):Array.from({length:24},(_,i)=>50+Math.sin(i/3.2)*10+i*1.6),mn=Math.min(...v),sp=(Math.max(...v)-mn)||1;\n const d=v.map((y,i)=>(i?'L':'M')+(i/(v.length-1)*W).toFixed(1)+' '+(H-8-(y-mn)/sp*(H-24)).toFixed(1)).join('');\n $('ch').innerHTML=`<path d=\"${d}L${W} ${H}L0 ${H}Z\" fill=\"var(--cy)\" opacity=\".12\"/><path d=\"${d}\" fill=\"none\" stroke=\"var(--cy)\" stroke-width=\"2\" vector-effect=\"non-scaling-stroke\"/>`;\n $('cn').textContent=h.length>1?h.length+' điểm dữ liệu':'Chưa có lịch sử dữ liệu'}\nfunction pushH(){const h=ld(HK,[]),n=data.length,l=h[h.length-1];if(!l||Date.now()-l[0]>6e4){h.push([Date.now(),n?data.reduce((s,a)=>s+(+a.gold||0),0)/n:0]);sv(HK,h.slice(-200))}}\nfunction colorsPanel(){const cols={};data.forEach(a=>inv(a).forEach(i=>{const c=cc(i);if(c)cols[c]=(cols[c]||0)+(+i.q||1)}));\n const e=Object.entries(cols).sort((x,y)=>y[1]-x[1]),sig=JSON.stringify([e,RMAP]);if(sig===pmSig)return;pmSig=sig;\n $('pm').innerHTML='<div class=\"mu\" style=\"max-width:300px\">M\\u00e0u vi\\u1ec1n v\\u00e0 n\\u1ec1n c\\u1ee7a m\\u1ed7i item bi\\u1ec3u th\\u1ecb \\u0111\\u1ed9 hi\\u1ebfm trong game. Ch\\u1ecdn c\\u1ea5p \\u0111\\u1ed9 kh\\u1edbp v\\u1edbi m\\u00e0u \\u0111\\u00f3; thi\\u1ebft l\\u1eadp ch\\u1ec9 \\u0111\\u01b0\\u1ee3c l\\u01b0u tr\\u00ean tr\\u00ecnh duy\\u1ec7t n\\u00e0y.</div>'+(e.length?e.map(([c,n])=>`<div class=\"mr\"><span class=\"sw\" style=\"background:${c}\"></span>${fmt(n)} item<select data-col=\"${c}\" style=\"margin-left:auto\"><option value=\"\">—</option>${[6,5,4,3,2,1].map(t=>`<option ${RMAP[c]===TIER[t].n?'selected':''}>${TIER[t].n}</option>`).join('')}</select></div>`).join(''):'<div class=\"mu\">Chưa có màu nào được ghi nhận</div>')}\nfunction itemStatLine(i){const F=[['physicalDamage','Phys'],['physicalPower','Phys'],['spellPower','Spell'],['health','HP']],s=i.s||{},m=i.m||{},fields=F.filter(([k])=>Number.isFinite(m[k])||Number.isFinite(s[k]));return fields.length?`<div class='mu' style='font-size:11px;line-height:1.5;margin-top:3px'>${fields.map(([k,n])=>{const v=Number.isFinite(m[k])?m[k]:s[k];return `<div title='${n}: ${fmt(v)}'>${n} ${sh(v)}</div>`}).join('')}</div>`:''} function card(i){const c=cc(i)||(TIER[tier(i)]||{}).c||'',u=IMG[ik(i)],t=tier(i),L=esc(i.n.slice(0,1).toUpperCase()),e=enchantCode(i);\n return `<div class=\"ic\"${c?` style=\"border-color:${c};background:${c}22\"`:''}><div class=\"im\">${/^https:\\/\\/[^\\s\"'<>]+$/.test(u||'')?`<img src=\"${esc(u)}\" alt=\"${L}\" loading=\"lazy\" referrerpolicy=\"no-referrer\" onerror=\"this.replaceWith(this.alt)\">`:L}</div><div><div style=\"font-weight:600\">${esc(i.n)}</div><div class=\"mu\" style=\"font-size:12px\">×${fmt(i.q)}${t?' · '+TIER[t].n:''}</div>${itemStatLine(i)}${i.eq?'<span class=\"bg eq\" style=\"margin-right:4px\">⚔ Đang mang</span>':''}${e?`<span class=\"bg on\" title=\"${e==='EIR'?'Enhanced Inner Rage':'Enhanced Inner Focus'}\">${e}</span>`:''}${i.own?`<div class=\"own\" title=\"Account đang sở hữu\">👤 ${esc(i.own)}</div>`:''}</div></div>`}\nfunction itemStatGroup(i){const s0=i.s||{},m0=i.m||{},s={};for(const k of ['physicalDamage','physicalPower','spellPower'])if(Number.isFinite(m0[k])||Number.isFinite(s0[k]))s[k]=Number.isFinite(m0[k])?m0[k]:s0[k];const dmg=Number.isFinite(s.physicalDamage)?s.physicalDamage:s.physicalPower,spell=s.spellPower;if(Number.isFinite(dmg)&&Number.isFinite(spell))return dmg>=spell?'dmg':'mage';if(Number.isFinite(dmg))return'dmg';if(Number.isFinite(spell))return'mage';const n=String(i.n||'').toLowerCase();return/mage|spell|sceptre|scepter|staff|wand/.test(n)?'mage':'dmg'}\nfunction renderModal(){const a=data.find(x=>x.name===mAcc);if(!a){$('am').classList.remove('s');return}\n const its=inv(a).slice().sort((x,y)=>tier(y)-tier(x)||dstat(y)-dstat(x)||x.n.localeCompare(y.n));\n $('ah').innerHTML=`<b style=\"font-size:18px\">${esc(displayName(a))}</b> <span class=\"bg on\" style=\"color:${placeColor(a)};border-color:${placeColor(a)}\">${placeLabel(a)}</span><div class=\"mu\" style=\"margin:6px 0\">Level ${fmt(a.level)} · Gold ${sh(a.gold)} · ${fmt(cnt(a,()=>true))} item</div><div>${[6,5].map(t=>`<span class=\"tb\" style=\"color:${TIER[t].c};border-color:${TIER[t].c};margin-right:6px\">${TIER[t].n} ${tcn(a,t)}</span>`).join('')}</div>`;\n const eqs=its.filter(i=>i.eq);\n $('ab').innerHTML=(eqs.length?'<div class=\"th\" style=\"color:#60a5fa\">⚔ ĐANG MANG <span class=\"mu\">'+eqs.length+' item</span></div><div class=\"ig\">'+eqs.map(card).join('')+'</div>':'')+[6,5,4,3,2,1,0].map(t=>{const rarityItems=its.filter(i=>tier(i)===t);if(!rarityItems.length)return'';const T=TIER[t]||{n:'Chưa rõ độ hiếm',c:'#8a919c'},groups=[['skill','SKILLS',rarityItems.filter(i=>String(i.t||'').toLowerCase()==='ability')],['mage','MAGE',rarityItems.filter(i=>String(i.t||'').toLowerCase()!=='ability'&&itemStatGroup(i)==='mage')],['dmg','WAR',rarityItems.filter(i=>String(i.t||'').toLowerCase()!=='ability'&&itemStatGroup(i)==='dmg')]].filter(([,label,items])=>items.length);return `<div class=\"th\" style=\"color:${T.c}\">${T.n.toUpperCase()} <span class=\"mu\">${fmt(rarityItems.reduce((s,i)=>s+(+i.q||1),0))} item</span></div>${groups.map(([key,label,items])=>`<div class=\"th\" style=\"font-size:12px;margin:8px 0 6px;color:${key==='mage'?'#a78bfa':key==='dmg'?'#f59e0b':'var(--mu)'}\">${label} <span class=\"mu\">${fmt(items.reduce((s,i)=>s+(+i.q||1),0))} item</span></div><div class=\"ig\">${items.map(card).join('')}</div>`).join('')}`}).join('')||'<div class=\"empty\">Chưa ghi nhận item nào</div>';\n loadImgs(its)}\nconst ik=i=>i.im?'rbx:'+i.im:i.n;\nasync function loadImgs(its){const m=new Map();its.forEach(i=>{const k=ik(i);if(!m.has(k))m.set(k,i)});\n const todo=[...m.keys()].filter(k=>!IMG[k]&&!pend.has(k)&&!failed.has(k));if(!todo.length)return;\n for(let i=0;i<todo.length;i+=4)await Promise.all(todo.slice(i,i+4).map(async k=>{pend.add(k);const it=m.get(k);\n  try{const r=await fetch('/api/img?name='+encodeURIComponent(it.n)+(it.im?'&id='+it.im:''),{headers:{'x-api-key':KEYV}}),j=await r.json();if(j.retry||!j.url)failed.add(k);else IMG[k]=j.url}catch(e){failed.add(k)}pend.delete(k)}));\n sv('dq_img',IMG);if(mAcc)renderModal();if($('im').classList.contains('s'))renderAll();feed()}\nfunction render(){stats();feed();colorsPanel();\n const vis=COLS.filter(c=>!hid.has(c[0]));const names=new Set(data.map(a=>a.name));[...sel].forEach(n=>{if(!names.has(n))sel.delete(n)});\n $('hd').innerHTML='<th class=\"ck\"><input type=\"checkbox\" class=\"cb\" id=\"sa\" title=\"Chọn tất cả\"></th>'+vis.map(c=>`<th data-k=\"${c[0]}\">${c[1]} ↕</th>`).join('');\n const r=rows(),sc=COLS.find(x=>x[0]===sort.k)||COLS[0];\n r.sort((a,b)=>{const x=sc[2](a),y=sc[2](b);const c=(typeof x==='string'?x.localeCompare(y):x-y)*sort.d;return c||((!isOn(a)?2:(where(a)==='lobby'?0:1))-(!isOn(b)?2:(where(b)==='lobby'?0:1)))||a.name.localeCompare(b.name)});\n lastRows=r;const pgs=Math.max(1,Math.ceil(r.length/size));page=Math.min(page,pgs);\n $('ic').textContent=r.length+(r.length===1?' Account':' Accounts');$('pgl').textContent=page+' / '+pgs+' trang';\n $('tb').innerHTML=r.slice((page-1)*size,page*size).map(a=>`<tr class=\"r\" data-ai=\"${data.indexOf(a)}\"><td class=\"ck\"><input type=\"checkbox\" class=\"cb sc\" data-n=\"${esc(a.name)}\" ${sel.has(a.name)?'checked':''}></td>${vis.map(c=>`<td>${c[3](a)}</td>`).join('')}</tr>`).join('');\n const sa=$('sa');if(sa){const n=r.filter(a=>sel.has(a.name)).length;sa.checked=!!r.length&&n===r.length;sa.indeterminate=n>0&&n<r.length}\n $('del').disabled=!sel.size;document.querySelectorAll('[data-fl]').forEach(b=>b.disabled=!sel.size);$('del').textContent='Xoá account ('+sel.size+')';\n $('em').style.display=r.length?'none':'block';if(mAcc)renderModal();if($('im').classList.contains('s'))renderAll()}\nfunction toast(m){const t=$('t');t.textContent=m;t.style.display='block';setTimeout(()=>t.style.display='none',2200)}\nfunction closeM(){mAcc='';$('am').classList.remove('s')}\n$('hd').onclick=e=>{const k=e.target.dataset.k;if(!k)return;sort=sort.k===k?{k,d:-sort.d}:{k,d:1};render()};\n$('tb').onclick=e=>{if(e.target.closest('.ck'))return;const r=e.target.closest('tr.r');if(!r)return;mAcc=data[+r.dataset.ai]?.name||'';if(!mAcc)return;$('am').classList.add('s');renderModal()};\n$('tb').onchange=e=>{const c=e.target.closest('.sc');if(!c)return;c.checked?sel.add(c.dataset.n):sel.delete(c.dataset.n);render()};\n$('hd').onchange=e=>{if(e.target.id!=='sa')return;lastRows.forEach(a=>e.target.checked?sel.add(a.name):sel.delete(a.name));render()};\n$('del').onclick=async()=>{const ns=[...sel];if(!ns.length)return;\n if(!confirm('Xoá '+ns.length+' account khỏi danh sách?\\nNếu script Lua vẫn đang chạy trên acc đó thì acc sẽ tự xuất hiện lại ở lần gửi dữ liệu tiếp theo.'))return;\n $('del').disabled=true;let ok=0;\n for(const n of ns){try{const r=await fetch('/api/accounts?name='+encodeURIComponent(n),{method:'DELETE',headers:{'x-api-key':KEYV,'x-requested-with':'dq'}});if(r.ok){ok++;sel.delete(n);data=data.filter(a=>a.name!==n)}}catch(x){}}\n if(mAcc&&!data.some(a=>a.name===mAcc))closeM();\n render();toast(ok===ns.length?'Đã xoá '+ok+' account':'Xoá được '+ok+'/'+ns.length+' account')};\ndocument.querySelectorAll('[data-fl]').forEach(b=>b.onclick=async()=>{const ns=[...sel];if(!ns.length)return;const fl=b.dataset.fl;\n try{const r=await fetch('/api/flag',{method:'POST',headers:{'x-api-key':KEYV,'x-requested-with':'dq','content-type':'application/json'},body:JSON.stringify({names:ns,flag:fl})});\n  if(r.ok){data.forEach(a=>{if(sel.has(a.name)){if(fl){a.flag=fl;a.manual=true}else{delete a.flag;delete a.manual;delete a.err}}});sel.clear();render();toast(fl?'Đã gắn cờ '+ns.length+' account':'Đã bỏ cờ '+ns.length+' account')}else toast('Lỗi gắn cờ')}catch(e){toast('Lỗi kết nối')}});\nlet iShow=300,hideEq=ld('dq_hide_eq',false)===true;\nfunction eqBtn(){const b=$('ieq');b.textContent=hideEq?'Hiện đồ đang mang':'Ẩn đồ đang mang';b.classList.toggle('on',hideEq)}\n$('ieq').onclick=()=>{hideEq=!hideEq;sv('dq_hide_eq',hideEq);eqBtn();iShow=300;renderAll()};eqBtn();\nfunction renderAll(){const k=($('iq').value||'').toLowerCase().trim(),rf=$('ir').value,tf=$('it').value,box=$('im').firstElementChild,st=box.scrollTop;\n let its=data.flatMap(a=>inv(a).map(i=>({...i,own:displayName(a)})));\n its=its.filter(i=>(!hideEq||!i.eq)&&(!k||(i.n+' '+i.own).toLowerCase().includes(k))&&(rf===''||tier(i)===+rf)&&(tf===''||(t=>t==='ability'?'skill':t==='weapon'?'weapon':t==='chest'||t==='helmet'?'armor':t)(String(i.t||'').toLowerCase())===tf));\n const gk=i=>String(i.t||'').toLowerCase()==='ability'?'skill':itemStatGroup(i),GO={skill:0,mage:1,dmg:2},GL={skill:'SKILLS',mage:'MAGE',dmg:'WAR'},GC={skill:'var(--mu)',mage:'#a78bfa',dmg:'#f59e0b'};its.sort((x,y)=>tier(y)-tier(x)||GO[gk(x)]-GO[gk(y)]||dstat(y)-dstat(x)||x.n.localeCompare(y.n)||x.own.localeCompare(y.own));\n const total=its.length,shown=its.slice(0,iShow),cnt={},cg={};its.forEach(i=>{const t=tier(i),q=t+'|'+gk(i);cnt[t]=(cnt[t]||0)+(+i.q||1);cg[q]=(cg[q]||0)+(+i.q||1)});\n let html='',cur=-1,curG='',open=false;\n shown.forEach(i=>{const t=tier(i),g=gk(i);if(t!==cur){if(open)html+='</div>';open=false;cur=t;curG='';const T=TIER[t]||{n:'Chưa rõ độ hiếm',c:'#8a919c'};html+=`<div class='th' style='color:${T.c}'>${T.n.toUpperCase()} <span class='mu'>${fmt(cnt[t])} item</span></div>`}if(g!==curG){if(open)html+='</div>';curG=g;html+=`<div class='th' style='font-size:12px;margin:8px 0 6px;color:${GC[g]}'>${GL[g]} <span class='mu'>${fmt(cg[t+'|'+g])} item</span></div><div class='ig'>`;open=true}html+=card(i)});\n if(open)html+='</div>';\n $('ib').innerHTML=(html||'<div class=\"empty\">Không có item</div>')+(total>shown.length?`<div class=\"rw\" style=\"justify-content:center\"><button class=\"bt\" id=\"imore\">Hiện thêm (${total-shown.length} còn lại)</button></div>`:'');\n $('icnt').textContent='('+fmt(total)+' dòng)';box.scrollTop=st;loadImgs(shown)}\n$('iall').onclick=()=>{iShow=300;$('im').classList.add('s');renderAll()};\n$('ix').onclick=()=>$('im').classList.remove('s');$('im').onclick=e=>{if(e.target.id==='im')$('im').classList.remove('s')};\n$('iq').oninput=()=>{iShow=300;renderAll()};$('ir').onchange=()=>{iShow=300;renderAll()};$('it').onchange=()=>{iShow=300;renderAll()};\n$('ib').onclick=e=>{if(e.target.id==='imore'){iShow+=300;renderAll()}};\ndocument.addEventListener('keydown',e=>{if(e.key==='Escape')$('im').classList.remove('s')});\n$('ax').onclick=closeM;$('am').onclick=e=>{if(e.target.id==='am')closeM()};\ndocument.addEventListener('keydown',e=>{if(e.key==='Escape')closeM()});\n$('q').oninput=e=>{qs=e.target.value;page=1;render()};\ndocument.querySelectorAll('[name=f]').forEach(r=>r.onchange=()=>{flt=r.value;page=1;render()});\n$('sz').onchange=e=>{size=+e.target.value;page=1;render()};\n$('pv').onclick=()=>{page=Math.max(1,page-1);render()};$('nx').onclick=()=>{page++;render()};\n\n['fa','fb','fg'].forEach((id,i)=>$(id).oninput=e=>{RG['abg'[i]]=e.target.value;page=1;render()});\n$('pc').onchange=e=>{const c=e.target.dataset.c;e.target.checked?hid.delete(c):hid.add(c);render()};\n$('pm').onchange=e=>{const c=e.target.dataset.col;if(!c)return;if(e.target.value)RMAP[c]=e.target.value;else delete RMAP[c];sv('dq_rmap',RMAP);render()};\ndocument.addEventListener('click',e=>{const b=e.target.closest('[data-p]');document.querySelectorAll('.pop').forEach(p=>{if(b&&p.id===b.dataset.p)p.classList.toggle('s');else if(!p.contains(e.target))p.classList.remove('s')})});\n$('px').onclick=async e=>{const f=e.target.dataset.x;if(!f)return;\n const txt=f==='json'?JSON.stringify(hideAccounts?data.map(a=>({...a,name:displayName(a)})):data,null,2):'name,level,gold,online,ult,legend,epic,note\\n'+data.map(a=>[displayName(a),a.level,a.gold,isOn(a),tcn(a,6),tcn(a,5),tcn(a,4),a.note||''].map(v=>'\"'+String(v??'').replace(/\"/g,'\"\"')+'\"').join(',')).join('\\n');\n try{await navigator.clipboard.writeText(txt);toast('Đã copy '+f.toUpperCase())}catch(x){$('ta').value=txt;$('m').classList.add('s')}};\n$('x').onclick=()=>$('m').classList.remove('s');\n$('th').onclick=()=>{const r=document.documentElement;r.dataset.theme=getComputedStyle(r).getPropertyValue('--bg').trim()==='#08090b'?'light':'dark'};\n\nconst agoD=t=>{const s=Math.max(0,Math.floor((Date.now()-t)/1e3));return s<60?s+'s trước':s<3600?Math.floor(s/60)+' phút trước':s<86400?Math.floor(s/3600)+' tiếng trước':Math.floor(s/86400)+' ngày trước'};\nconst dstat=i=>{const s=i.s||{},m=i.m||{};return Math.max(0,...['physicalDamage','physicalPower','spellPower'].map(k=>Number.isFinite(m[k])?m[k]:Number.isFinite(s[k])?s[k]:0))};\nconst FINAL_ITEMS=['mjolnir','gungnir','laevateinn','valhalla','enhanced inner'];const isFinal=d=>!FINAL_ITEMS.length||FINAL_ITEMS.some(k=>String(d.n||'').toLowerCase().includes(k));function dropStats(d){const F=[['physicalDamage','Phys'],['physicalPower','Phys'],['spellPower','Spell'],['health','HP']],s=d.s||{},m=d.m||{};return F.filter(([k])=>Number.isFinite(m[k])||Number.isFinite(s[k])).map(([k,n])=>n+' '+sh(Number.isFinite(m[k])?m[k]:s[k])).join(' · ')}function dropCard(d,a){const c={Ult:TIER[6].c,Legend:TIER[5].c}[d.r]||'#8a919c',u=IMG[ik(d)],L=esc(String(d.n||'?').slice(0,1).toUpperCase()),img=String(u||'').startsWith('https://')?`<img src='${esc(u)}' alt='${L}' loading='lazy' referrerpolicy='no-referrer' onerror='this.replaceWith(this.alt)'>`:L,F=[['physicalDamage','Phys'],['physicalPower','Phys'],['spellPower','Spell'],['health','HP']],s=d.s||{},m=d.m||{},chips=F.filter(([k])=>Number.isFinite(m[k])||Number.isFinite(s[k])).map(([k,n])=>`<span class='mu' style='white-space:nowrap'>${n} <b style='color:var(--tx);font-weight:600'>${sh(Number.isFinite(m[k])?m[k]:s[k])}</b></span>`).join(''); return `<div class='ic' style='border-color:${c};background:${c}22;position:relative;min-width:0;flex-wrap:wrap;gap:8px 14px;padding-right:12px'><div class='im'>${img}</div><div style='min-width:120px'><div style='font-weight:600'>${esc(d.n)}${d.q>1?' ×'+d.q:''}</div><div class='mu' style='font-size:12px'>${esc(d.r)}${d.e?' · '+esc(d.e):''}</div></div><div style='display:flex;flex-direction:column;gap:2px;flex:1;min-width:100px;font-size:13px'>${chips||(d.e?'':`<span class='mu'>Skill</span>`)}</div><div style='text-align:right;margin-left:auto;max-width:140px;flex-shrink:0'><div class='own' style='margin:0;max-width:100%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;word-break:normal;vertical-align:bottom' title='${esc(displayName(a))} · Account nhận được item (rớt từ dungeon, không tính trade)'>👤 ${esc(displayName(a))}</div><div class='mu' style='font-size:12px;margin-top:3px'><span data-at='${d.at}'>${agoD(d.at)}</span></div></div></div>`}function feed(){const dsc=$('dfl'),dst=dsc?dsc.scrollTop:0;const all=[]; data.forEach(a=>(a.drops||[]).forEach(d=>{if(!isFinal(d))return;all.push({d,a})})); all.sort((x,y)=>y.d.at-x.d.at); const top=all.slice(0,10); {const ca=f=>top.reduce((n,x)=>n+(f(x.d)?(+x.d.q||1):0),0),u=ca(d=>d.r==='Ult'),l=ca(d=>d.r==='Legend'),en=ca(d=>d.e==='EIR'||d.e==='EIF');$('dn').innerHTML=`<span style=\"color:${TIER[6].c}\">Ultimate ${u}</span> · <span style=\"color:${TIER[5].c}\">Legend ${l}</span> · <span style=\"color:var(--ok)\">Enchant ${en}</span>`} $('df').innerHTML=top.length?`<div id='dfl' style='display:flex;flex-direction:column;gap:8px;max-height:232px;overflow-y:scroll;padding-right:6px'>${top.map(({d,a})=>dropCard(d,a)).join('')}</div>`:`<div class='empty' style='padding:18px'>Chưa có đồ Ult/Legend nào rớt từ map cuối (cần ít nhất 2 lần cập nhật để so sánh)</div>`; {const n=$('dfl');if(n)n.scrollTop=dst}loadImgs(top.map(x=>x.d))}setInterval(()=>document.querySelectorAll('#df [data-at]').forEach(e=>{e.textContent=agoD(+e.dataset.at)}),1000);\nsetInterval(()=>document.querySelectorAll('[data-reset]').forEach(e=>{e.textContent=fmtCountdown(+e.dataset.reset-Date.now())}),1000);\nlet KEYV=ld('dq_key',''),asked=false,polling=false;\nvar lastTxt='',lastRender=0;async function poll(manual){\n if(!KEYV&&(!asked||manual===true)){asked=true;KEYV=(prompt('Nhập API key của tracker:')||'').trim();sv('dq_key',KEYV);connectStream()}\n if(!KEYV){$('lv').textContent='Chưa nhập key';return}\n if(polling)return;polling=true;\n try{const r=await fetch('/api/accounts',{headers:{'x-api-key':KEYV}});\n  if(r.status===401){KEYV='';sv('dq_key','');$('lv').textContent='Sai key - bấm Làm mới để nhập lại';return}\n  const txt=await r.text();if(manual===true||txt!==lastTxt||Date.now()-lastRender>15000){lastTxt=txt;lastRender=Date.now();const _j=JSON.parse(txt);data=_j.accounts||[];if(typeof window.__checkBuild==='function')window.__checkBuild(_j.build);if(Date.now()-(window._cs||0)>5000){window._cs=Date.now();try{if(txt.length<4e6)localStorage.setItem('dq_acc_cache2',txt)}catch(e){}}const _sn=+r.headers.get(`x-now`)||0,_k=_sn?Date.now()-_sn:0;data.forEach(a=>{a._t=(a.updated||0)+_k});checkFlagAlerts(data);window._os=data.map(a=>isOn(a)?1:0).join(``);render()}else{const _s=data.map(a=>isOn(a)?1:0).join(``);if(_s!==window._os){window._os=_s;render()}}$('lv').textContent='Cập nhật '+new Date().toLocaleTimeString();try{window.dispatchEvent(new Event('dq-update'))}catch(e){}}\n catch(e){$('lv').textContent='Mất kết nối server'}finally{polling=false}}\n$('rf').onclick=()=>poll(true);\n// SSE: nhan tin ngay khi co account bao cao xong, khong doi vong poll 0.5s. EventSource tu dong ket noi lai neu rot.\nlet _es=null;function connectStream(){if(!KEYV||typeof EventSource==='undefined')return;try{if(_es)_es.close()}catch(e){}try{_es=new EventSource('/api/stream?key='+encodeURIComponent(KEYV));_es.onmessage=()=>poll(true)}catch(e){}}\nconnectStream();\n$('ha').onclick=()=>{hideAccounts=!hideAccounts;sv('dq_hide_accounts',hideAccounts);$('ha').textContent=hideAccounts?'View Name':'Hide Name';$('ha').setAttribute('aria-pressed',String(hideAccounts));render()};\n$('ha').textContent=hideAccounts?'View Name':'Hide Name';$('ha').setAttribute('aria-pressed',String(hideAccounts));\ntry{localStorage.removeItem(`dq_acc_cache`)}catch(e){}render();poll();document.addEventListener('visibilitychange',function(){if(!document.hidden)poll()});setInterval(function(){if(!document.hidden)poll()},500);\n</script></body></html>\n";
const LUA = "-- ===== DUNGEON QUEST REBORN TRACKER CLIENT =====\n-- Đọc thông tin tài khoản hiện tại và gửi lên server tracker của BẠN.\n-- Chỉ đọc dữ liệu trong game, không đụng tới cookie/token.\n\nif _G.__GPO_TRACKER then return end\n_G.__GPO_TRACKER = true\n\n-- ===== CẤU HÌNH (sửa ở đây) =====\nlocal DEFAULT = {\n    Endpoint = \"__ENDPOINT__\", -- URL backend của bạn\n    ApiKey   = \"doi-thanh-key-bi-mat-cua-ban\",               -- phải khớp với key trên backend\n    Note     = \"PC\",                                          -- ghi chú máy/acc\n    Interval = 1,                                            -- giay giua moi lan gui dinh ky (1s)\n    Debug    = false,                                          -- true: in log + in cây dữ liệu để tìm đường dẫn\n\n    -- Tên các giá trị cần đọc (không phân biệt hoa/thường, bỏ qua khoảng trắng/gạch dưới).\n    -- Nếu game dùng tên khác, thêm vào đây.\n    Aliases = {\n        level = { \"Level\", \"Lvl\" },\n        gold  = { \"Gold\", \"Coins\", \"Money\", \"Cash\" },\n        class = { \"Class\", \"Role\" },\n    },\n    -- Thư mục chứa inventory (dưới Player). Tool trong Backpack/Character luôn được quét.\n    -- Inventory is read from the game's reloadInvy RemoteFunction.\n}\n\nlocal C = _G.Config or {}\nfor k, v in pairs(DEFAULT) do\n    if C[k] == nil then C[k] = v end\nend\n\n-- ===== DỊCH VỤ =====\nif not game:IsLoaded() then game.Loaded:Wait() end\nlocal Players     = game:GetService(\"Players\")\nlocal HttpService = game:GetService(\"HttpService\")\nlocal player      = Players.LocalPlayer\n\nlocal httpRequest = (syn and syn.request) or (http and http.request) or http_request or request\n\nlocal function log(...)\n    if C.Debug then print(\"[GPO-Tracker]\", ...) end\nend\n\n-- khai bao truoc o day (dinh nghia that su o duoi, gan buildPayload/post): mot vai noi phia tren (gamepass/xp-boost\n-- refresh...) can goi lai sendWithRetry truoc khi no duoc dinh nghia day du - phai khai bao local o DAY (truoc moi\n-- noi su dung) de cac closure ben duoi deu tham chieu dung 1 local nay, khong bi roi ve global (luon la nil).\nlocal sendWithRetry\n\nlocal function norm(s)\n    return (tostring(s):lower():gsub(\"[%s_]\", \"\"))\nend\n\n-- ===== ĐỌC DỮ LIỆU =====\nlocal cache = {}\n\nlocal function findValue(key)\n    local cached = cache[key]\n    if cached and cached.Parent then return cached.Value end\n\n    local wanted = {}\n    for _, a in ipairs(C.Aliases[key] or {}) do wanted[norm(a)] = true end\n\n    for _, d in ipairs(player:GetDescendants()) do\n        if d:IsA(\"ValueBase\") and wanted[norm(d.Name)] then\n            cache[key] = d\n            return d.Value\n        end\n    end\n    for a in pairs(wanted) do\n        for name, val in pairs(player:GetAttributes()) do\n            if norm(name) == a then return val end\n        end\n    end\n    return nil\nend\n\n-- ===== ĐỌC ITEM TỪ GIAO DIỆN =====\n-- ===== READ INVENTORY FROM THE GAME =====\nlocal ReplicatedStorage = game:GetService(\"ReplicatedStorage\")\nlocal ITEM_CATEGORIES = {\n    { key = \"weapons\", kind = \"weapon\", catalog = \"GetWeapons\" },\n    { key = \"chests\", kind = \"chest\", catalog = \"GetChests\" },\n    { key = \"helmets\", kind = \"helmet\", catalog = \"GetHelmets\" },\n    { key = \"abilities\", kind = \"ability\" },\n}\nlocal itemNamesByImage = {}\nlocal catalogRetryAt = 0\n\nlocal function imageKey(value)\n    if value == nil then return nil end\n    local text = tostring(value):lower()\n    return text:match(\"%d+\") or text\nend\n\nlocal function indexCatalog(category, catalog)\n    for catalogName, item in pairs(catalog) do\n        if type(item) == \"table\" and item.imageId ~= nil then\n            local id = imageKey(item.imageId)\n            local name = tostring(item.name or catalogName)\n            if id then\n                local key = category .. \"|\" .. id\n                local existing = itemNamesByImage[key]\n                if existing == nil then\n                    itemNamesByImage[key] = name\n                elseif existing ~= name then\n                    itemNamesByImage[key] = false\n                end\n            end\n        end\n    end\nend\n\nlocal function refreshItemCatalogs()\n    local now = os.clock()\n    if now < catalogRetryAt then return end\n    catalogRetryAt = now + 300\n\n    local remotes = ReplicatedStorage:FindFirstChild(\"remotes\")\n    local getData = remotes and remotes:FindFirstChild(\"getData\")\n    if not getData or not getData:IsA(\"RemoteFunction\") then\n        log(\"Missing game getData RemoteFunction; item names will use exact inventory identifiers\")\n        return\n    end\n\n    for _, category in ipairs(ITEM_CATEGORIES) do\n        if category.catalog then\n            local ok, catalog = pcall(function()\n                return getData:InvokeServer(category.catalog)\n            end)\n            if ok and type(catalog) == \"table\" then\n                indexCatalog(category.key, catalog)\n            elseif ok then\n                log(\"Invalid in-game item catalog:\", category.catalog)\n            else\n                log(\"Failed to read in-game item catalog:\", category.catalog, catalog)\n            end\n        end\n    end\nend\n\nlocal function directItemName(item)\n    for _, field in ipairs({ \"displayName\", \"itemName\", \"name\", \"Name\" }) do\n        local value = item[field]\n        if type(value) == \"string\" and value ~= \"\" then return value end\n    end\nend\n\nlocal function enchantCodeFromName(name)\n    local normalized = string.lower(tostring(name or \"\")):gsub(\"[^%a]\", \"\")\n    if normalized == \"enhancedinnerrage\" then return \"EIR\" end\n    if normalized == \"enhancedinnerfocus\" then return \"EIF\" end\nend\n\n-- Moi lan nang cap game cong +10 vao Physical/Spell (da kiem chung: 5,939,111 + 134,974*10 = 7,288,851)\nlocal UPGRADE_STEP = 10\nlocal dbgSamples = {}   -- 1 item mau (toan bo truong) cho moi loai|do hiem, gui len dashboard de chan doan\nlocal invCache = nil\nlocal invCacheAt = 0\nlocal function getInventory()\n    -- cache 2.5s: reloadInvy:InvokeServer() la RPC lam server game xu ly, goi lien tuc moi tick (1s) gay giat lag.\n    -- Cac noi inventory thuc su thay doi (reloadInventory event, thang dungeon, GPO_SEND_NOW) da tu dat invCacheAt=-10 de buoc doc lai ngay.\n    if invCache and (os.clock() - invCacheAt) < 2.5 then return invCache end\n    local remotes = ReplicatedStorage:FindFirstChild(\"remotes\")\n    local reloadInvy = remotes and remotes:FindFirstChild(\"reloadInvy\")\n    if not reloadInvy or not reloadInvy:IsA(\"RemoteFunction\") then\n        if invCache then return invCache end\n        error(\"Missing ReplicatedStorage.remotes.reloadInvy\")\n    end\n    local ok, ownedItems = pcall(function()\n        return reloadInvy:InvokeServer()\n    end)\n    if not ok then\n        if invCache then return invCache end\n        error(\"Could not read inventory from reloadInvy: \" .. tostring(ownedItems))\n    end\n    if type(ownedItems) ~= \"table\" then\n        if invCache then return invCache end\n        error(\"reloadInvy returned invalid inventory data\")\n    end\n\n        local counts, order, meta = {}, {}, {}\n    local foundCategory = false\n\n    local debugged = {}\n    -- Equipped detection: item-level flags OR a top-level \"equip*\" table in reloadInvy\n    local equippedIds, equippedTop = {}, {}\n    local function collectIds(v, depth)\n        if depth > 3 then return end\n        local tv = type(v)\n        if tv == \"table\" then\n            for k, x in pairs(v) do\n                if type(x) == \"table\" then\n                    collectIds(x, depth + 1)\n                    if x.uniqueItemNum ~= nil then equippedIds[tostring(x.uniqueItemNum)] = true end\n                    if type(k) == \"string\" then equippedIds[k] = true end\n                elseif type(x) == \"string\" or type(x) == \"number\" then\n                    equippedIds[tostring(x)] = true\n                elseif x == true and type(k) == \"string\" then\n                    equippedIds[k] = true\n                end\n            end\n        elseif tv == \"string\" or tv == \"number\" then\n            equippedIds[tostring(v)] = true\n        end\n    end\n    for topKey, topVal in pairs(ownedItems) do\n        if type(topKey) == \"string\" and string.find(string.lower(topKey), \"equip\", 1, true) then\n            table.insert(equippedTop, topKey)\n            collectIds(topVal, 0)\n        end\n    end\n    local function isEquipped(itemKey, item)\n        -- ability: equipped la bang { q, e, q2, e2 } (o hotkey nao dang gan)\n        local eqt = item.equipped\n        if type(eqt) == \"table\" then\n            for _, v in pairs(eqt) do\n                if v == true or v == 1 or v == \"true\" or (type(v) == \"string\" and v ~= \"\" and v ~= \"false\") then return true end\n            end\n        elseif type(eqt) == \"string\" and eqt ~= \"\" and eqt ~= \"false\" then\n            return true\n        end\n        for _, f in ipairs({ \"equipped\", \"Equipped\", \"isEquipped\", \"equip\", \"equiped\", \"worn\", \"inUse\" }) do\n            local v = item[f]\n            if v == true or v == 1 or v == \"true\" then return true end\n        end\n        if equippedIds[tostring(itemKey)] then return true end\n        if item.uniqueItemNum ~= nil and equippedIds[tostring(item.uniqueItemNum)] then return true end\n        return false\n    end\n    local function itemStats(item)\n        local stats = {}\n        for _, field in ipairs({ \"physicalDamage\", \"physicalPower\", \"spellPower\", \"health\", \"upgrades\" }) do\n            local value = tonumber(item[field])\n            if value ~= nil and value == value and value ~= math.huge and value ~= -math.huge then\n                stats[field] = value\n            end\n        end\n        -- Upgrade state: game field names are not known for sure, try the common ones\n        local function pick(names)\n            for _, f in ipairs(names) do\n                local v = tonumber(item[f])\n                if v ~= nil and v == v and v ~= math.huge and v ~= -math.huge then return v end\n            end\n        end\n        if stats.upgrades == nil then\n            stats.upgrades = pick({ \"upgradeLevel\", \"upgradeCount\", \"upgradeLvl\", \"timesUpgraded\", \"upgradesDone\", \"upgrade\" })\n        end\n        stats.maxUpgrades = pick({ \"maxUpgrades\", \"maxUpgrade\", \"upgradeMax\", \"upgradeCap\", \"maxUpgradeLevel\", \"upgradesMax\" })\n        -- REQ level (level yeu cau de dung vu khi/giap/mu): thu ten truong pho bien, neu khong co thi quet theo ten\n        stats.req = pick({ \"reqLevel\", \"reqLvl\", \"levelReq\", \"lvlReq\", \"requiredLevel\", \"requiredLvl\", \"levelRequirement\", \"lvlRequirement\", \"minLevel\", \"minLvl\", \"levelRequired\", \"level\", \"lvl\", \"Level\", \"Lvl\" })\n        if stats.req ~= nil and stats.req <= 0 then stats.req = nil end\n        if stats.req == nil then\n            local function scanReq(t, depth)\n                for fk, fv in pairs(t) do\n                    if stats.req ~= nil then return end\n                    if type(fk) == \"string\" then\n                        local l = string.lower(fk)\n                        if (string.find(l, \"req\", 1, true) or string.find(l, \"minlev\", 1, true) or string.find(l, \"minlvl\", 1, true)) and (string.find(l, \"lev\", 1, true) or string.find(l, \"lvl\", 1, true)) and not string.find(l, \"upgrad\", 1, true) then\n                            local n = tonumber(fv)\n                            if n and n == n and n > 0 and n ~= math.huge then stats.req = n end\n                        end\n                    end\n                    if depth < 1 and type(fv) == \"table\" then scanReq(fv, depth + 1) end\n                end\n            end\n            scanReq(item, 0)\n        end\n        -- Fuzzy: neu ten truong khac, quet moi truong co \"upgrad\" (item + bang long nhau 1 cap)\n        if stats.upgrades == nil or stats.maxUpgrades == nil then\n            local function scanUp(t, depth)\n                for fk, fv in pairs(t) do\n                    if type(fk) == \"string\" then\n                        local l = string.lower(fk)\n                        if string.find(l, \"upgrad\", 1, true) then\n                            local n = tonumber(fv)\n                            if n and n == n and n ~= math.huge and n ~= -math.huge then\n                                if string.find(l, \"max\", 1, true) or string.find(l, \"cap\", 1, true) or string.find(l, \"limit\", 1, true) or string.find(l, \"total\", 1, true) then\n                                    if stats.maxUpgrades == nil then stats.maxUpgrades = n end\n                                elseif stats.upgrades == nil then\n                                    stats.upgrades = n\n                                end\n                            end\n                        end\n                    end\n                    if depth < 1 and type(fv) == \"table\" then scanUp(fv, depth + 1) end\n                end\n            end\n            scanUp(item, 0)\n        end\n        -- Pot max: stats when fully upgraded. Try \"maxX / xMax / potentialX / potX\" on the item and in nested tables.\n        local pot = {}\n        local nested = {}\n        for _, nk in ipairs({ \"maxStats\", \"maxStat\", \"potential\", \"potentialStats\", \"pot\", \"max\", \"maxed\", \"fullStats\" }) do\n            if type(item[nk]) == \"table\" then table.insert(nested, item[nk]) end\n        end\n        for _, field in ipairs({ \"physicalDamage\", \"physicalPower\", \"spellPower\", \"health\" }) do\n            local cap = field:sub(1, 1):upper() .. field:sub(2)\n            local names = { \"max\" .. cap, field .. \"Max\", \"maxPot\" .. cap, \"potential\" .. cap, \"pot\" .. cap, field .. \"Potential\", field .. \"Pot\", \"maxed\" .. cap, \"full\" .. cap }\n            local found\n            for _, nm in ipairs(names) do\n                local v = tonumber(item[nm])\n                if v ~= nil and v == v and v ~= math.huge and v ~= -math.huge then found = v break end\n            end\n            if found == nil then\n                for _, t in ipairs(nested) do\n                    local v = tonumber(t[field]) or tonumber(t[cap])\n                    if v ~= nil and v == v and v ~= math.huge and v ~= -math.huge then found = v break end\n                end\n            end\n            if found ~= nil then pot[field] = found end\n        end\n        -- Fuzzy pass: any numeric field whose name has max/pot/full + a stat word (spell, health/hp, dmg/damage, phys/power/attack)\n        local function statFromKey(k)\n            local l = string.lower(k)\n            if not (string.find(l, \"max\", 1, true) or string.find(l, \"pot\", 1, true) or string.find(l, \"full\", 1, true)) then return nil end\n            if string.find(l, \"upgrade\", 1, true) or string.find(l, \"level\", 1, true) then return nil end\n            if string.find(l, \"spell\", 1, true) or string.find(l, \"magic\", 1, true) then return \"spellPower\" end\n            if string.find(l, \"health\", 1, true) or string.find(l, \"hp\", 1, true) then return \"health\" end\n            if string.find(l, \"dmg\", 1, true) or string.find(l, \"damage\", 1, true) then return \"physicalDamage\" end\n            if string.find(l, \"phys\", 1, true) or string.find(l, \"power\", 1, true) or string.find(l, \"attack\", 1, true) then return \"physicalPower\" end\n            return nil\n        end\n        local function fuzzy(t, depth)\n            for fk, fv in pairs(t) do\n                if type(fk) == \"string\" then\n                    local f = statFromKey(fk)\n                    local n = f and tonumber(fv)\n                    if f and n and n == n and n ~= math.huge and n ~= -math.huge and pot[f] == nil then pot[f] = n end\n                end\n                if depth < 2 and type(fv) == \"table\" then fuzzy(fv, depth + 1) end\n            end\n        end\n        fuzzy(item, 0)\n        -- Pot (nang full) = chi so hien tai + 10 * so lan nang con lai (khop so game: 7,288,851)\n        if stats.maxUpgrades and stats.maxUpgrades > 0 then\n            local left = math.max(0, stats.maxUpgrades - (stats.upgrades or 0))\n            for _, f in ipairs({ \"physicalDamage\", \"physicalPower\", \"spellPower\" }) do\n                local v = stats[f]\n                if v ~= nil then pot[f] = v + UPGRADE_STEP * left end\n            end\n        end\n        if next(pot) then stats.pot = pot end\n        return next(stats) and stats or nil\n    end\n\n    local function addItem(name, kind, rarity, enchant, stats, equipped, imgId)\n        local statKey = stats and table.concat({\n            tostring(stats.physicalDamage or \"\"), tostring(stats.physicalPower or \"\"), tostring(stats.spellPower or \"\"),\n            tostring(stats.health or \"\"), tostring(stats.upgrades or \"\")\n        }, \"|\") or \"\"\n        local key = table.concat({ kind, name, rarity or \"\", enchant or \"\", statKey, equipped and \"1\" or \"0\" }, string.char(31))\n        if not counts[key] then\n            counts[key] = 0\n            meta[key] = { n = name, t = kind, r = rarity, e = enchant, s = stats, eq = equipped or nil, im = imgId }\n            table.insert(order, key)\n        end\n        counts[key] += 1\n    end\n\n    for _, category in ipairs(ITEM_CATEGORIES) do\n        local items = ownedItems[category.key]\n        if items ~= nil then\n            if type(items) ~= \"table\" then\n                error(\"Invalid \" .. category.key .. \" inventory data\")\n            end\n            foundCategory = true\n            for itemKey, item in pairs(items) do\n                if type(item) ~= \"table\" then\n                    error(\"Invalid item record in \" .. category.key)\n                end\n\n                local dbgKey = category.key .. \"|\" .. tostring(item.rarity)\n                if C.Debug and not dbgSamples[dbgKey] then\n                    local parts = {}\n                    for fk, fv in pairs(item) do\n                        local tf = type(fv)\n                        if tf == \"number\" or tf == \"string\" or tf == \"boolean\" then\n                            parts[#parts + 1] = tostring(fk) .. \"=\" .. tostring(fv)\n                        elseif tf == \"table\" then\n                            local sub = {}\n                            for sk, sv in pairs(fv) do\n                                if #sub >= 12 then break end\n                                local ts = type(sv)\n                                if ts == \"number\" or ts == \"string\" or ts == \"boolean\" then sub[#sub + 1] = tostring(sk) .. \"=\" .. tostring(sv) else sub[#sub + 1] = tostring(sk) .. \":\" .. ts end\n                            end\n                            table.sort(sub)\n                            parts[#parts + 1] = tostring(fk) .. \"={\" .. table.concat(sub, \", \") .. \"}\"\n                        end\n                    end\n                    table.sort(parts)\n                    dbgSamples[dbgKey] = string.sub(table.concat(parts, \", \"), 1, 700)\n                    log(\"item fields [\" .. dbgKey .. \"]:\", dbgSamples[dbgKey])\n                end\n\n                local name = directItemName(item)\n                local imageId = item.imageId or item.ImageId\n                if not name and imageId ~= nil and category.catalog then\n                    refreshItemCatalogs()\n                    local id = imageKey(imageId)\n                    local mapped = id and itemNamesByImage[category.key .. \"|\" .. id]\n                    if mapped then name = mapped end\n                end\n                if not name then\n                    local itemId = tostring(item.uniqueItemNum or itemKey)\n                    itemId = itemId:match(\"^[^_]+_(.+)$\") or itemId\n                    name = category.kind .. \" #\" .. itemId\n                end\n\n                local rarity = item.rarity\n                if rarity ~= nil then rarity = tostring(rarity) end\n                local enchant = item.enchant or item.Enchant\n                if enchant ~= nil then enchant = tostring(enchant)\n                elseif category.kind == \"ability\" then enchant = enchantCodeFromName(name) end\n                addItem(name, category.kind, rarity, enchant, itemStats(item), isEquipped(itemKey, item), imageId ~= nil and tostring(imageId):match(\"%d+\") or nil)\n            end\n        end\n    end\n\n    if C.Debug then\n        local keys, eqCount = {}, 0\n        for k in pairs(ownedItems) do keys[#keys + 1] = tostring(k) end\n        for _, k in ipairs(order) do if meta[k].eq then eqCount += 1 end end\n        log(\"reloadInvy keys:\", table.concat(keys, \", \"))\n        log(\"equip tables:\", table.concat(equippedTop, \", \"), \"| equipped items found:\", eqCount)\n    end\n\n    if not foundCategory then\n        if invCache then return invCache end\n        error(\"reloadInvy returned no weapons/chests/helmets/abilities; refusing to send guessed items\")\n    end\n\n    local inventory = {}\n    for _, key in ipairs(order) do\n        local item = meta[key]\n        item.q = counts[key]\n        table.insert(inventory, item)\n    end\n    log(\"Items read directly from reloadInvy:\", #inventory)\n    invCache = inventory\n    invCacheAt = os.clock()\n    return inventory\nend\n\nlocal function getGems()\n    local ls = player:FindFirstChild(\"leaderstats\")\n    local g = ls and ls:FindFirstChild(\"Gems\")\n    return math.max(0, tonumber(g and g.Value) or 0)\nend\nlocal XPBOOST_SHOP_IDS = { 1, 2, 3 }\nlocal XPBOOST_LABEL = { [1] = \"1 Hour\", [2] = \"2 Hours\", [3] = \"4 Hours\" }\nlocal xpBoostGoldCache = {}\nlocal xpBoostPriceAt = 0\nlocal function refreshXpBoostGoldPricesNow()\n    if os.clock() - xpBoostPriceAt < 15 then return end\n    xpBoostPriceAt = os.clock()\n    local remotes = ReplicatedStorage:FindFirstChild(\"remotes\")\n    local rf = remotes and remotes:FindFirstChild(\"getGoldXPBoostPrice\")\n    if not rf or not rf:IsA(\"RemoteFunction\") then return end\n    for _, boostId in ipairs(XPBOOST_SHOP_IDS) do\n        local ok, res = pcall(function() return rf:InvokeServer(boostId) end)\n        if ok and type(res) == \"table\" and res.ready ~= false then\n            xpBoostGoldCache[boostId] = { gold = tonumber(res.cost), gems = tonumber(res.gemCost), currency = res.currency, nextAvailableIn = tonumber(res.nextAvailableIn) }\n        end\n    end\nend\nlocal xpPriceBusy = false\nlocal function refreshXpBoostGoldPrices()   -- chay nen: khong chan buildPayload\n    if xpPriceBusy or os.clock() - xpBoostPriceAt < 15 then return end\n    xpPriceBusy = true\n    task.spawn(function() pcall(refreshXpBoostGoldPricesNow); xpPriceBusy = false end)\nend\nlocal MarketplaceService = game:GetService(\"MarketplaceService\")\nlocal GAMEPASS_SHOP_KEYS = { vip = \"vip\", gold2 = \"goldGamepass\", drop = \"extraItemGamepass\", freeResets = \"freeStatResets\" }\nlocal GAMEPASS_IDS = {\n    vip = 6028667,\n    gold2 = 6092989,\n    drop = 6092999,\n    freeResets = 6093005,\n}\nlocal gamepassCache = {}\nlocal gamepassPriceCache = {}\nlocal gamepassCheckAt = 0\nlocal gamepassCheckRunning = false\n\nlocal function localGamepassFlag(names)\n    for _, name in ipairs(names) do\n        local flag = player:FindFirstChild(name)\n        if flag and flag:IsA(\"BoolValue\") and flag.Value == true then return true end\n        if player:GetAttribute(name) == true then return true end\n    end\n    return false\nend\n\nlocal GAMEPASS_GOLD_FALLBACK = { vip = 480, gold2 = 540, drop = 1200 }   -- gia Gold mac dinh neu shop khong doc duoc\nlocal goldShopCache = {}   -- gia shop trong game tinh bang GOLD (khong phai Robux)\nlocal shopItemsCache = nil\nlocal shopFetching = false\nlocal shopItemsCacheAt = 0\nlocal function getShopItemsCached()\n    if shopItemsCache and (os.clock() - shopItemsCacheAt) < 60 then return shopItemsCache end\n    if shopItemsCache and shopFetching then return shopItemsCache end\n    if shopItemsCache then   -- cu > 60s: tra ban cu ngay, lam moi o luong nen\n        shopFetching = true\n        task.spawn(function()\n            local rm = ReplicatedStorage:FindFirstChild(\"remotes\")\n            local gd = rm and rm:FindFirstChild(\"getData\")\n            if gd and gd:IsA(\"RemoteFunction\") then\n                local ok, items = pcall(function() return gd:InvokeServer(\"GetShopItems\") end)\n                if ok and type(items) == \"table\" then shopItemsCache = items; shopItemsCacheAt = os.clock() end\n            end\n            shopFetching = false\n        end)\n        return shopItemsCache\n    end\n    local remotes = ReplicatedStorage:FindFirstChild(\"remotes\")\n    local getData = remotes and remotes:FindFirstChild(\"getData\")\n    if not getData or not getData:IsA(\"RemoteFunction\") then return nil end\n    local ok, items = pcall(function() return getData:InvokeServer(\"GetShopItems\") end)\n    if ok and type(items) == \"table\" then\n        shopItemsCache = items\n        shopItemsCacheAt = os.clock()\n        return items\n    end\n    return nil\nend\nlocal function getShopPriceForGold(itemName)\n    if not itemName or itemName == \"\" then return nil end\n    local items = getShopItemsCached()\n    if not items then return nil end\n    local want = itemName:lower()\n    for _, item in pairs(items) do\n        if type(item) == \"table\" then\n            local nm = tostring(item.name or item.displayName or \"\"):lower()\n            if nm == want then\n                local gp = tonumber(item.goldPrice) or tonumber(item.price) or tonumber(item.cost)\n                if gp and gp >= 0 then return gp end\n            end\n        end\n    end\n    return nil\nend\n\nlocal function refreshGamepassOwnership()\n    if gamepassCheckRunning or (gamepassCheckAt > 0 and os.clock() - gamepassCheckAt < 30) then return end\n    gamepassCheckRunning = true\n    gamepassCheckAt = os.clock()\n    task.spawn(function()\n        for key, passId in pairs(GAMEPASS_IDS) do\n            local ok, owns = pcall(function() return MarketplaceService:UserOwnsGamePassAsync(player.UserId, passId) end)\n            if ok and type(owns) == \"boolean\" then gamepassCache[key] = owns end\n        end\n        gamepassCheckRunning = false\n    end)\nend\n\nlocal function refreshOnePass(key)\n    local passId = GAMEPASS_IDS[key]\n    if not passId then return end\n    local ok, owns = pcall(function() return MarketplaceService:UserOwnsGamePassAsync(player.UserId, passId) end)\n    if ok and type(owns) == \"boolean\" then gamepassCache[key] = owns end\nend\n\n-- MUA GAMEPASS: mo cua so mua cua Roblox; gui duoc toi web (khong biet client nao dang mo)\nlocal buySeen = {}\nlocal function buyReport(id, status, msg)\n    if type(id) ~= \"string\" or id == \"\" then return end\n    pcall(api, \"POST\", \"/api/trade/buy/status\", { name = player.Name, id = id, status = status, msg = msg })\nend\nlocal function runBuy(job)\n    if type(job) ~= \"table\" or type(job.id) ~= \"string\" or buySeen[job.id] then return end\n    buySeen[job.id] = true\n    task.spawn(function()\n        if type(job.key) == \"table\" then\n            buyReport(job.id, \"running\", \"Dang mua XP Boost...\")\n            local bid = tonumber(job.key.id) or 1\n            local cur = job.key.currency or \"gold\"\n            local remotes = ReplicatedStorage:FindFirstChild(\"remotes\")\n            local rf = remotes and remotes:FindFirstChild(\"requestGoldXPBoostPurchase\")\n            if not rf then buyReport(job.id, \"failed\", \"Thieu remote requestGoldXPBoostPurchase\") return end\n            local ok, e = pcall(function() rf:FireServer(bid, cur) end)\n            if not ok then buyReport(job.id, \"failed\", \"FireServer loi: \" .. tostring(e)) return end\n            print(\"[DQR TRADE] Da gui mua XP Boost: \" .. tostring(XPBOOST_LABEL[bid] or bid) .. \" bang \" .. tostring(cur))\n            buyReport(job.id, \"done\", \"Da gui lenh mua XP Boost \" .. tostring(XPBOOST_LABEL[bid] or bid) .. \" bang \" .. tostring(cur))\n            task.delay(1.5, function() xpBoostPriceAt = 0; refreshXpBoostGoldPrices(); task.spawn(sendWithRetry) end)\n            return\n        end\n        local key = tostring(job.key or \"\")\n        buyReport(job.id, \"running\", \"Dang mua...\")\n        local shopKey = GAMEPASS_SHOP_KEYS[key] or key\n        local remotes = ReplicatedStorage:FindFirstChild(\"remotes\")\n        local rf = remotes and remotes:FindFirstChild(\"requestGoldGamepassPurchase\")\n        if not rf then buyReport(job.id, \"failed\", \"Thieu remote requestGoldGamepassPurchase\") return end\n        local ok, e = pcall(function() rf:FireServer(shopKey) end)\n        if not ok then buyReport(job.id, \"failed\", \"FireServer loi: \" .. tostring(e)) return end\n        print(\"[DQR TRADE] Da gui mua GP: \" .. tostring(key) .. \" (shop=\" .. tostring(shopKey) .. \")\")\n        buyReport(job.id, \"done\", \"Da gui lenh mua \" .. tostring(key))\n        task.delay(1.5, function() gamepassCheckAt = 0; refreshGamepassOwnership(); task.spawn(sendWithRetry) end)\n    end)\nend\n\nlocal function getXpBoostSeconds()\n    local boost = player:FindFirstChild(\"xpBoost\")\n    if boost and (boost:IsA(\"IntValue\") or boost:IsA(\"NumberValue\")) then\n        return math.max(0, tonumber(boost.Value) or 0)\n    end\n    return 0\nend\n\nlocal function getGamepasses()\n    refreshGamepassOwnership()\n    return {\n        vip = localGamepassFlag({ \"vip\" }) or gamepassCache.vip == true,\n        xp2 = localGamepassFlag({ \"xpGamepass\", \"doubleXPGamepass\", \"x2XPGamepass\", \"doubleExpGamepass\", \"experienceGamepass\", \"xp2Gamepass\" }),\n        gold2 = localGamepassFlag({ \"goldGamepass\" }) or gamepassCache.gold2 == true,\n        drop = localGamepassFlag({ \"extraItemGamepass\" }) or gamepassCache.drop == true,\n        freeResets = localGamepassFlag({ \"freeStatResets\" }) or gamepassCache.freeResets == true,\n    }\nend\n\nlocal gpPriceCache, gpPriceAt, gpPriceBusy = {}, 0, false\nlocal function getGoldGamepassPriceByKey(k)   -- gia Gold that tu getGoldGamepassPrice (khong phu thuoc ten hien thi theo ngon ngu)\n    if not gpPriceBusy and os.clock() - gpPriceAt >= 20 then\n        gpPriceBusy = true; gpPriceAt = os.clock()\n        task.spawn(function()\n            pcall(function()\n                local rm = ReplicatedStorage:FindFirstChild('remotes')\n                local rf = rm and rm:FindFirstChild('getGoldGamepassPrice')\n                if rf and rf:IsA('RemoteFunction') then\n                    for key, shopKey in pairs(GAMEPASS_SHOP_KEYS) do\n                        local ok, res = pcall(function() return rf:InvokeServer(shopKey) end)\n                        if ok and type(res) == 'table' and res.ready ~= false and tonumber(res.cost) then gpPriceCache[key] = tonumber(res.cost) end\n                    end\n                end\n            end)\n            gpPriceBusy = false\n        end)\n    end\n    return gpPriceCache[k]\nend\nlocal function getGamepassPrices()\n    local gpass = getGamepasses()\n    refreshXpBoostGoldPrices()\n    local price = {}\n    local shopMap = { vip = \"VIP\", gold2 = \"x2 Gold\", drop = \"Extra Item\", freeResets = \"Free Stat Resets\" }\n    for k, shopName in pairs(shopMap) do\n        if gpass[k] == true then\n            price[k] = nil\n        else\n            local gp = getGoldGamepassPriceByKey(k) or getShopPriceForGold(shopName)\n            if gp and gp >= 0 then price[k] = gp else price[k] = (GAMEPASS_GOLD_FALLBACK or {})[k] end\n        end\n    end\n    for _, bid in ipairs(XPBOOST_SHOP_IDS) do\n        local c = xpBoostGoldCache[bid]\n        if c then\n            local k = \"xp\" .. tostring(bid)\n            if c.gold and c.gold >= 0 then price[k] = c.gold end\n            if c.gems and c.gems >= 0 then price[k .. \"Gems\"] = c.gems end\n        end\n    end\n    return price\nend\nlocal lastWin, lastWinAt = nil, 0   -- phan thuong dungeon vua nhan (remote cloneRewardGui)\nlocal lastDailyStatus = nil   -- \"READY\" / chu cooldown / \"DA NHAN\" - khai bao som de buildPayload() doc duoc (gan gia tri o phan DAILY REWARD, phia sau)\n\n-- ===== TEN DUNGEON THAT (doc truc tiep tu game, khong doan qua config farm) =====\n-- Game tu dung ham nay o khap noi (camera zoom, loading screen, lighting...):\n-- require(ReplicatedStorage.Utility.PlaceManager).GetDungeonName() -> tra ve ten dungeon THAT khi dang\n-- o trong 1 dungeon (GetPlaceName() == \"Level\"), hoac nil khi o Lobby/noi khac. Day la nguon CHINH XAC\n-- tuyet doi (chinh server game dung de quyet dinh), khac voi altfarm.json chi la cau hinh \"dinh farm\" nguoi dung tu dat.\nlocal _placeManager, _placeManagerTried = nil, false\nlocal function getPlaceManager()\n    if _placeManagerTried then return _placeManager end\n    _placeManagerTried = true\n    local ok, pm = pcall(function()\n        local utility = ReplicatedStorage:WaitForChild(\"Utility\", 5)\n        return utility and require(utility:WaitForChild(\"PlaceManager\", 5))\n    end)\n    if ok and pm then _placeManager = pm end\n    return _placeManager\nend\nlocal function getLiveDungeonName()\n    local ok, name = pcall(function()\n        local pm = getPlaceManager()\n        return pm and pm.GetDungeonName and pm.GetDungeonName() or nil\n    end)\n    if ok and type(name) == \"string\" and name ~= \"\" then return name end\n    return nil\nend\n\n-- ===== TIN HIEU THANG/THUA + RAID RODIN: doc TRUC TIEP cac gia tri game dung de tu ket luan (nhai dung co che\n-- that cua script Harvard - xem ghi chu o server, ham runsTrack) thay vi doan qua rot do/placeId nhu truoc =====\nlocal function getDungeonProgress()   -- workspace.dungeonProgress.Value: \"bossKilled\" / \"noLivesLeft\" / \"outOfTime\" / ...\n    local ok, v = pcall(function() return workspace:FindFirstChild(\"dungeonProgress\") end)\n    if not (ok and v) then return nil end\n    local ok2, s = pcall(function() return tostring(v.Value) end)\n    return ok2 and s or nil\nend\nlocal function getDungeonFinished()   -- workspace.dungeon.bossRoom.dungeonFinished.Value == true\n    local ok, v = pcall(function()\n        local d = workspace:FindFirstChild(\"dungeon\")\n        local br = d and d:FindFirstChild(\"bossRoom\")\n        local df = br and br:FindFirstChild(\"dungeonFinished\")\n        return df ~= nil and df.Value == true\n    end)\n    return ok and v or nil\nend\nlocal function getDungeonStarted()   -- workspace.dungeonStarted.Value == true\n    local ok, v = pcall(function()\n        local ds = workspace:FindFirstChild(\"dungeonStarted\")\n        return ds ~= nil and ds.Value == true\n    end)\n    return ok and v or nil\nend\nlocal function getBonusBossVotingActive()   -- workspace.bonusBossVotingActive.Value == true\n    local ok, v = pcall(function()\n        local b = workspace:FindFirstChild(\"bonusBossVotingActive\")\n        return b ~= nil and b.Value == true\n    end)\n    return ok and v or nil\nend\nlocal function getTimeUp()   -- dong ho tren GUI (timeLeftGui) hien \"00:00\" - het gio\n    local ok, v = pcall(function()\n        local gui = player:FindFirstChild(\"PlayerGui\")\n        local tl = gui and gui:FindFirstChild(\"timeLeftGui\")\n        local fr = tl and tl:FindFirstChild(\"Frame\")\n        local tx = fr and fr:FindFirstChild(\"time\")\n        return tx ~= nil and tx:IsA(\"TextLabel\") and (tx.Text == \"00:00\" or tx.Text == \"0:00\")\n    end)\n    return ok and v or nil\nend\nlocal function getBonusBossStatus()   -- workspace.bonusBossStatus.Value: \"active\" (dang danh Rodin) / \"success\" (thang) / co chu \"fail\" (thua) - tach rieng hoan toan khoi thang/thua dungeon chinh\n    local ok, v = pcall(function() return workspace:FindFirstChild(\"bonusBossStatus\") end)\n    if not (ok and v) then return nil end\n    local ok2, s = pcall(function() return tostring(v.Value) end)\n    return ok2 and s or nil\nend\n\n-- ===== PHAT HIEN SCRIPT HARVARD (DungeonAutofarm) CON CHAY HAY DA DUNG/CRASH =====\n-- Harvard la 1 script rieng (khong phai script nay), tu luu so lieu hieu nang vao bien toan cuc\n-- _G.DungeonAutofarmPerf.cur.frames.last (thoi diem frame gan nhat no xu ly, kieu os.clock()). GUI cua Harvard\n-- khong nam trong PlayerGui/CoreGui (no tu ve rieng) nen khong the kiem tra kieu do - doc lai gia tri nay moi lan\n-- bao cao: server se tu so sanh, NEU gia tri con thay doi qua thoi gian tuc la vong lap chinh cua Harvard van con\n-- chay that (khong bi dung/crash du script nay va Roblox van binh thuong). Bien noi bo cua 1 script khac (khong\n-- phai API chinh thuc) nen bao ve bang pcall het muc - Harvard cap nhat/doi ten bien se tu dong tro ve nil, khong\n-- loi gi ca, chi la tam thoi mat tin hieu nay (khong anh huong cac tin hieu khac cua script nay).\nlocal function getHarvardFrameSig()\n    local ok, v = pcall(function() return _G.DungeonAutofarmPerf.cur.frames.last end)\n    if ok and type(v) == \"number\" then return v end\n    return nil\nend\n\nlocal function buildPayload()\n    local function str(v) return v ~= nil and tostring(v) or \"-\" end\n    return {\n        name    = player.Name,\n        userId  = player.UserId,\n        level   = tonumber(findValue(\"level\")) or 0,\n        gold    = tonumber(findValue(\"gold\")) or 0,\n        class   = str(findValue(\"class\")),\n        gpass   = getGamepasses(),\n        dbg     = C.Debug and dbgSamples or nil,\n        gpassPrices = getGamepassPrices(), -- gia GOLD doc tu shop game\n        xpBoostSeconds = getXpBoostSeconds(),\n        gems = getGems(),\n        inv     = getInventory(),\n        online  = true,\n        winCap  = true,   -- client nay biet gui tin hieu thang dungeon\n        win     = (lastWin and os.clock() - lastWinAt < 600) and lastWin or nil,\n        note    = C.Note,\n        dailyReward = lastDailyStatus,\n        dungeonName = getLiveDungeonName(),   -- ten dungeon THAT dang o trong (nil neu dang o Lobby) - de server tinh win/loss dung dungeon, khong con doan qua altfarm.json\n        dungeonProgress = getDungeonProgress(),   -- tin hieu goc server dung de tu ket luan thang/thua (xem runsTrack)\n        dungeonFinished = getDungeonFinished(),\n        dungeonStarted = getDungeonStarted(),\n        bonusBossVotingActive = getBonusBossVotingActive(),\n        timeUp = getTimeUp(),\n        bonusBossStatus = getBonusBossStatus(),\n        harvardAt = getHarvardFrameSig(),   -- nhip tim cua script Harvard (xem ghi chu o getHarvardFrameSig) - de server biet Harvard co dang chay that khong\n        placeId = game.PlaceId,\n        jobId   = game.JobId,\n        updated = os.time() * 1000,\n    }\nend\n\n-- ===== GỬI LÊN SERVER =====\nlocal HWID\ndo\n    local function try(f)\n        local ok, val = pcall(f)\n        if ok and type(val) == \"string\" and val ~= \"\" then return val end\n    end\n    HWID = try(function() return gethwid() end)\n        or try(function() return get_hwid() end)\n        or try(function() return game:GetService(\"RbxAnalyticsService\"):GetClientId() end)\nend\n\nlocal function post(payload)\n    local body = HttpService:JSONEncode(payload)\n    if not httpRequest then\n        error(\"Executor không hỗ trợ request/http_request\")\n    end\n    return httpRequest({\n        Url = C.Endpoint,\n        Method = \"POST\",\n        Headers = {\n            [\"Content-Type\"] = \"application/json\",\n            [\"x-api-key\"] = C.ApiKey,\n            [\"x-hwid\"] = HWID,\n        },\n        Body = body,\n    })\nend\n\n-- ===== LENH TU SERVER: \"Force lobby\" (nut trong tab Config > Parties & Runs cua dashboard) =====\n-- Moi lan bao cao (day du HOAC nhip tim) deu doc response cua server: neu co {lobby:true} (server chi tra\n-- DUNG 1 LAN ngay sau khi admin bam nut, roi tu xoa lenh) thi teleport VE LOBBY NGAY LAP TUC, khong can\n-- cho vong lap dinh ky tiep theo - giup thoat dungeon bang tay -> bam 1 nut tren web la xong.\nlocal LOBBY_PLACE_ID = 77649408247578   -- place Lobby chinh (giong DAILY_LOBBY_PLACE o duoi, dung chung 1 id)\nlocal lastForceLobbyAt = -1000\nlocal function handleServerCmd(res)\n    if not (res and res.Body) then return end\n    local ok, data = pcall(function() return HttpService:JSONDecode(res.Body) end)\n    if not ok or type(data) ~= \"table\" or data.lobby ~= true then return end\n    if os.clock() - lastForceLobbyAt < 5 then return end   -- chong teleport lap lai neu nhan lenh nhieu lan lien tiep\n    lastForceLobbyAt = os.clock()\n    log(\"[Force lobby] Nhan lenh tu server -> ve Lobby ngay\")\n    task.spawn(function()\n        pcall(function()\n            if game.PlaceId ~= LOBBY_PLACE_ID then\n                game:GetService(\"TeleportService\"):Teleport(LOBBY_PLACE_ID, player)\n            end\n        end)\n    end)\nend\n\nlocal sending = false\nlocal pendingSend = false\nlocal sendStartedAt, sendGen = 0, 0\nlocal lastReportOkAt = -1000   -- lan gan nhat gui bao cao day du thanh cong (os.clock()) - dung de bo bot nhip tim du thua\n-- chay fn trong luong rieng, toi da sec giay: reloadInvy:InvokeServer() / http co the TREO VO HAN -> khong de ket co \"sending\"\nlocal function withTimeout(sec, fn)\n    local done, ok, res = false, false, nil\n    task.spawn(function() ok, res = pcall(fn); done = true end)\n    local t0 = os.clock()\n    while not done and os.clock() - t0 < sec do task.wait(0.1) end\n    if not done then return false, \"timeout \" .. sec .. \"s\" end\n    return ok, res\nend\n-- sendWithRetry() duoc goi TRUC TIEP (khong pcall) tu vong lap bao cao chinh (task.spawn ... while true do ...) -\n-- neu than ham nay gap 1 loi khong luong truoc (vd Roblox doi cau truc du lieu, bug la...) va KHONG duoc bat,\n-- toan bo coroutine vong lap se chet VINH VIEN (im lang mai mai, trong khi cac script khac - vd script auto farm\n-- rieng - van chay binh thuong, nen nhin ngoai game van thay dang \"danh\"). Vi vay phan than that su nam o\n-- sendWithRetryUnsafe(), va sendWithRetry() ben duoi LUON pcall no de vong lap chinh khong bao gio bi chet hang.\n-- (sendWithRetry da duoc khai bao \"local sendWithRetry\" o dau file, de cac noi dung phia tren cung tham chieu dung)\nlocal function sendWithRetryUnsafe()\n    if sending then\n        if os.clock() - sendStartedAt > 15 then\n            -- lan gui truoc bi ket > 40s (treo doc kho / treo http) -> bo no, gui lan moi (khong de web dung cap nhat)\n            log(\"Lan gui truoc bi ket > 40s -> bo, gui lai\")\n            sending = false\n        else\n            pendingSend = true\n            return false\n        end\n    end\n    sending = true\n    sendStartedAt = os.clock()\n    sendGen = sendGen + 1\n    local myGen = sendGen\n    local sent = false\n    for attempt = 1, 3 do\n        if sendGen ~= myGen then return false end   -- da co lan gui moi thay the\n        -- KHONG ep invCacheAt=-10 o day nua: truoc day dong nay buoc doc lai kho moi lan gui (~1 lan/giay/acc),\n        -- vo hieu hoa hoan toan cache ben tren va la nguyen nhan chinh gay giat/lag game. Cac diem that su can\n        -- kho moi (nhan do, thang dungeon, GPO_SEND_NOW) da tu dat invCacheAt=-10 truoc khi goi sendWithRetry.\n        local ok, res = withTimeout(6, function() return post(buildPayload()) end)\n        if ok then handleServerCmd(res) end\n        if ok and res and res.StatusCode == 403 then\n            warn(\"[GPO-Tracker] Key đã gắn với thiết bị khác (HWID) hoặc bị từ chối. Liên hệ admin để reset HWID.\")\n            break\n        end\n        if ok and res and (res.StatusCode == nil or (res.StatusCode >= 200 and res.StatusCode < 300)) then\n            log(\"?? g?i l?n server (l?n th? \" .. attempt .. \")\")\n            sent = true\n            lastReportOkAt = os.clock()   -- bao cao day du da cap nhat \"updated\" ben server roi -> nhip tim ke tiep co the bo qua\n            break\n        end\n        log(\"G?i l?i:\", ok and (res and res.StatusCode) or res)\n        if attempt < 3 then task.wait(2) end\n    end\n    if sendGen ~= myGen then return sent end\n    sending = false\n    if pendingSend then\n        pendingSend = false\n        task.spawn(sendWithRetry)\n    end\n    return sent\nend\nsendWithRetry = function()\n    local ok, sentOrErr = pcall(sendWithRetryUnsafe)\n    if not ok then\n        sending = false   -- dam bao co \"sending\" khong bi ket mai mai neu loi giua chung\n        warn(\"[GPO-Tracker] sendWithRetry loi khong mong muon (da bo qua, se thu lai lan sau): \" .. tostring(sentOrErr))\n        return false\n    end\n    return sentOrErr\nend\n\nlocal remotes = ReplicatedStorage:FindFirstChild(\"remotes\")\nlocal reloadInventory = remotes and remotes:FindFirstChild(\"reloadInventory\")\nif reloadInventory and reloadInventory:IsA(\"RemoteEvent\") then\n    local updateQueued = false\n    reloadInventory.OnClientEvent:Connect(function()\n        if updateQueued then return end\n        updateQueued = true\n        invCacheAt = -10\n        task.delay(0.25, function()\n            updateQueued = false\n            task.spawn(sendWithRetry)\n        end)\n        task.delay(1.5, function()   -- kho tren server game co the cap nhat tre -> gui them 1 lan\n            invCacheAt = -10\n            task.spawn(sendWithRetry)\n        end)\n    end)\nend\n\n-- ===== TIN HIEU THANG DUNGEON =====\n-- Remote cloneRewardGui chi ban khi nhan phan thuong; data.items la danh sach do nhan duoc.\nlocal cloneRewardGui = remotes and remotes:FindFirstChild(\"cloneRewardGui\")\nif cloneRewardGui and cloneRewardGui:IsA(\"RemoteEvent\") then\n    cloneRewardGui.OnClientEvent:Connect(function(data)\n        local items = {}\n        if type(data) == \"table\" and type(data.items) == \"table\" then\n            for _, it in pairs(data.items) do\n                if type(it) == \"table\" and type(it.itemName) == \"string\" and it.itemName ~= \"gold\" then\n                    items[#items + 1] = { n = it.itemName, r = tostring(it.rarity or \"\"), t = tostring(it.itemType or \"\") }\n                end\n            end\n        end\n        lastWin = { id = os.time(), items = items }   -- bao thang du khi thuong chi co Gold (items co the rong)\n        lastWinAt = os.clock()\n        log(\"Thang dungeon, phan thuong:\", #items, \"item\")\n        task.delay(2, function() task.spawn(sendWithRetry) end)   -- gui lai sau khi kho do da cap nhat\n        task.delay(8, function() task.spawn(sendWithRetry) end)\n    end)\nend\n\n-- ===== DEBUG: in các giá trị tìm thấy để biết game lưu ở đâu =====\nlocal function dumpTree()\n    local n = 0\n    for _, d in ipairs(player:GetDescendants()) do\n        if d:IsA(\"ValueBase\") then\n            n += 1\n            if n > 200 then break end\n            print((\"  %s = %s\"):format(d:GetFullName(), tostring(d.Value)))\n        end\n    end\nend\n\nif C.Debug then\n    print(\"[GPO-Tracker] Giá trị tìm thấy dưới Player:\")\n    dumpTree()\n    print(\"[GPO-Tracker] Payload mẫu:\", HttpService:JSONEncode(buildPayload()))\nend\n\n-- ===== PHÁT HIỆN CAPTCHA / BỊ KICK =====\n-- Đọc thông báo lỗi của Roblox (GuiService / CoreGui ErrorPrompt) và gửi ngay lên server.\nlocal GuiService = game:GetService(\"GuiService\")\nlocal function coreErrorText()\n    local ok, txt = pcall(function()\n        local gui = game:GetService(\"CoreGui\"):FindFirstChild(\"RobloxPromptGui\")\n        local ov = gui and gui:FindFirstChild(\"promptOverlay\")\n        local ep = ov and ov:FindFirstChild(\"ErrorPrompt\")\n        if not ep then return nil end\n        for _, d in ipairs(ep:GetDescendants()) do\n            if d:IsA(\"TextLabel\") and d.Name == \"ErrorMessage\" and d.Text ~= \"\" then return d.Text end\n        end\n        return nil\n    end)\n    if ok and type(txt) == \"string\" and txt ~= \"\" then return txt end\n    return nil\nend\n\nlocal lastErrSent\nlocal function reportError(msg)\n    if type(msg) ~= \"string\" or msg == \"\" or msg == lastErrSent then return end\n    lastErrSent = msg\n    log(\"Error/kick message:\", msg)\n    task.spawn(function()\n        pcall(post, { statusOnly = true, name = player.Name, userId = player.UserId, err = string.sub(msg, 1, 200) })\n    end)\nend\n\npcall(function()\n    GuiService.ErrorMessageChanged:Connect(function()\n        task.wait(0.2)\n        local m = GuiService:GetErrorMessage()\n        reportError((m ~= nil and m ~= \"\") and m or coreErrorText())\n    end)\nend)\ntask.spawn(function()\n    pcall(function()\n        local gui = game:GetService(\"CoreGui\"):WaitForChild(\"RobloxPromptGui\", 10)\n        local ov = gui and gui:WaitForChild(\"promptOverlay\", 10)\n        if ov then\n            ov.ChildAdded:Connect(function(ch)\n                if ch.Name == \"ErrorPrompt\" then\n                    task.wait(0.3)\n                    reportError(coreErrorText() or GuiService:GetErrorMessage())\n                end\n            end)\n        end\n    end)\nend)\n\n-- ===== NHIP TIM: bao \"con song\" lien tuc (nhe, khong doc kho do) =====\n-- Web coi acc la OFF neu qua 4s khong nhan duoc gi.\ntask.spawn(function()\n    local hb = tonumber(C.Heartbeat) or 1\n    -- Gui NGAY 1 lan dau tien, khong doi: day la tin hieu nhanh nhat de web bao acc \"Online\" ngay luc script vua chay /\n    -- acc vua vao lai game (dang doi bao cao day du xong hinh anh/kho do moi gui thi cham hon han).\n    do\n        local pok, ok2, res2 = pcall(withTimeout, 3, function()\n            return post({ statusOnly = true, heartbeat = true, name = player.Name, userId = player.UserId })\n        end)\n        if pok and ok2 then handleServerCmd(res2) end\n    end\n    while true do\n        task.wait(hb)\n        -- Bao cao day du (sendWithRetry, chay song song moi C.Interval giay) cung tu cap nhat \"con song\" ben server\n        -- y het nhip tim. Neu vua gui thanh cong gan day roi thi bo qua lan nay, khoi gui 2 request/giay cho moi acc.\n        if os.clock() - lastReportOkAt < hb * 0.9 then\n            -- bo qua, bao cao day du vua lam viec nay roi\n        else\n            local pok, ok2, res2 = pcall(withTimeout, 3, function()\n                return post({ statusOnly = true, heartbeat = true, name = player.Name, userId = player.UserId })\n            end)\n            if pok and ok2 then handleServerCmd(res2) end\n        end\n    end\nend)\n\n-- ===== GỬI NGAY: gụi kho ngay sau trade / buy / sell (khong doi Interval) =====\n_G.GPO_SEND_NOW = function()\n    task.spawn(sendWithRetry)\nend\n\n-- ===== CHẠY: gụi định kỳ mỗi C.Interval giây (mac dinh 1s) =====\ntask.spawn(function()\n    while true do\n        local tickAt = os.clock()\n        sendWithRetry()\n        task.wait(math.max(0.1, (tonumber(C.Interval) or 1) - (os.clock() - tickAt)))   -- giu nhip deu 1 lan / Interval giay\n    end\nend)\n\n-- ===== DAILY REWARD: tu dong \"cham\" vao dailyRewardTouchPart khi dang o Lobby va con READY =====\n-- Co che xac nhan qua do tim truoc do: KHONG co nut bam / ClickDetector / remote claim rieng,\n-- chi co 1 Part ten \"dailyRewardTouchPart\" (CanTouch=true) trong Workspace.<Lobby|>.Map.Interactables.\n-- Trang thai hien tai doc tu PlayerGui.DailyRewardCountdown (TextLabel con) - khi \"READY\" la con nhan duoc,\n-- khi khac \"READY\" la dang cooldown (chu hien chinh xac se thay sau khi nhan lan dau).\n-- Cach lam: doi CFrame cua HumanoidRootPart toi vi tri Part do trong chop nhoang (Touched se tu kich hoat\n-- do Part khong Anchor can bi cham), roi tra lai vi tri cu. KHONG dung khi dang trong dungeon (chi lam o Lobby).\nlocal DAILY_LOBBY_PLACE = 77649408247578\nlocal lastDailyAttemptAt = -1e9\n-- (lastDailyStatus da khai bao o dau file, gan truoc buildPayload() - khong khai bao lai o day)\nlocal function findDailyTouchPart()\n    local ws = game:FindFirstChild(\"Workspace\") or workspace\n    local candidates = {}\n    local lobby0 = ws:FindFirstChild(\"Lobby\")\n    if lobby0 then table.insert(candidates, lobby0:FindFirstChild(\"Map\")) end\n    table.insert(candidates, ws:FindFirstChild(\"Map\"))\n    for _, map0 in ipairs(candidates) do\n        local inter = map0 and map0:FindFirstChild(\"Interactables\")\n        local part = inter and inter:FindFirstChild(\"dailyRewardTouchPart\")\n        if part and part:IsA(\"BasePart\") then return part end\n    end\n    return nil\nend\nlocal function getDailyRewardText()\n    local okp, res = pcall(function()\n        local pgx = player:FindFirstChild(\"PlayerGui\")\n        local cd = pgx and pgx:FindFirstChild(\"DailyRewardCountdown\")\n        if not cd then return nil end\n        for _, d in ipairs(cd:GetDescendants()) do\n            if (d:IsA(\"TextLabel\") or d:IsA(\"TextButton\")) and tostring(d.Text) ~= \"\" then return tostring(d.Text) end\n        end\n        return nil\n    end)\n    return okp and res or nil\nend\nlocal function tryAutoClaimDaily()\n    local ok, err = pcall(function()\n        if game.PlaceId ~= DAILY_LOBBY_PLACE then return end   -- chi lam khi dang o Lobby, khong dong vao luc farm dungeon\n        local txt = getDailyRewardText()\n        if txt ~= nil then lastDailyStatus = txt end\n        if txt ~= \"READY\" then return end\n        if os.clock() - lastDailyAttemptAt < 30 then return end   -- khong thu lai lien tuc\n        local char = player.Character\n        local hrp = char and char:FindFirstChild(\"HumanoidRootPart\")\n        local touchPart = findDailyTouchPart()\n        if not hrp or not touchPart then return end\n        lastDailyAttemptAt = os.clock()\n        local originalCF = hrp.CFrame\n        hrp.CFrame = CFrame.new(touchPart.Position)\n        task.wait(0.6)\n        pcall(function() hrp.CFrame = originalCF end)   -- tra ve vi tri cu, khong de lech cho dang farm\n        task.wait(0.3)\n        local after = getDailyRewardText()\n        if after ~= nil then lastDailyStatus = after end\n        if after and after ~= \"READY\" then\n            log(\"[Daily] Da nhan thuong hang ngay, trang thai moi: \" .. tostring(after))\n        else\n            log(\"[Daily] Da thu cham vao Part nhung van thay READY - co the chua thanh cong, se thu lai sau\")\n        end\n    end)\n    if not ok then log(\"[Daily] Loi tu nhan daily: \" .. tostring(err)) end\nend\ntask.spawn(function()\n    task.wait(10)   -- doi game + nhan vat load xong\n    while true do\n        tryAutoClaimDaily()\n        task.wait(90)   -- kiem tra lai moi 90s, du nhanh de khong bo lo qua lau, du cham de khong lam gi thua\n    end\nend)\n\n";
const DUMP = "-- ===== DQ DUMP =====\n-- Quét cấu trúc dữ liệu game để tìm chỗ lưu Class / Gold / Inventory.\n-- Kết quả gửi về server (file dump_<tên>.txt cạnh server.js), đồng thời writefile + clipboard nếu executor hỗ trợ.\n-- Không đọc cookie/token, chỉ đọc dữ liệu trong game.\n\nif not game:IsLoaded() then game.Loaded:Wait() end\nlocal Players = game:GetService(\"Players\")\nlocal HttpService = game:GetService(\"HttpService\")\nlocal RS = game:GetService(\"ReplicatedStorage\")\nlocal player = Players.LocalPlayer\nlocal C = _G.Config or {}\nlocal httpRequest = (syn and syn.request) or (http and http.request) or http_request or request\n\nlocal out = {}\nlocal function w(s) out[#out + 1] = s end\nlocal function short(v, n)\n    local s = tostring(v)\n    n = n or 60\n    if #s > n then s = s:sub(1, n) .. \"..\" end\n    return s\nend\n\nlocal function valueOf(i)\n    if i:IsA(\"ValueBase\") then\n        local ok, v = pcall(function() return i.Value end)\n        if ok then return \" = \" .. short(v, 80) end\n    end\n    return \"\"\nend\n\nlocal function attrsOf(i)\n    local t = {}\n    for k, v in pairs(i:GetAttributes()) do t[#t + 1] = k .. \"=\" .. short(v, 40) end\n    if #t == 0 then return \"\" end\n    return \" {\" .. table.concat(t, \", \") .. \"}\"\nend\n\nlocal budget = 0\nlocal function tree(inst, maxDepth, d)\n    if budget <= 0 then return end\n    budget -= 1\n    w(string.rep(\"  \", d) .. inst.Name .. \" [\" .. inst.ClassName .. \"]\" .. valueOf(inst) .. attrsOf(inst))\n    if d >= maxDepth then return end\n    local kids = inst:GetChildren()\n    for idx, k in ipairs(kids) do\n        if idx > 80 then\n            w(string.rep(\"  \", d + 1) .. \"... +\" .. (#kids - 80) .. \" nua\")\n            break\n        end\n        tree(k, maxDepth, d + 1)\n    end\nend\n\n-- 1. PLAYER (bỏ PlayerGui)\nw(\"##### PLAYER: \" .. player.Name .. \" (\" .. player.UserId .. \") #####\")\nw(attrsOf(player))\nbudget = 1500\nfor _, c in ipairs(player:GetChildren()) do\n    if c.Name ~= \"PlayerGui\" then pcall(tree, c, 6, 0) end\nend\n\n-- 2. CHARACTER\nw(\"\\n##### CHARACTER #####\")\nif player.Character then\n    budget = 200\n    pcall(tree, player.Character, 2, 0)\nend\n\n-- 3. REPLICATEDSTORAGE + instance mang tên người chơi\nw(\"\\n##### REPLICATEDSTORAGE #####\")\nbudget = 400\npcall(tree, RS, 2, 0)\n\nw(\"\\n##### INSTANCE MANG TEN NGUOI CHOI #####\")\nlocal function findNamed(root)\n    for _, d in ipairs(root:GetDescendants()) do\n        if d.Name == player.Name or d.Name == tostring(player.UserId) then\n            local mine = d:IsDescendantOf(player) or (player.Character and (d == player.Character or d:IsDescendantOf(player.Character)))\n            if not mine then\n                w(\"@ \" .. d:GetFullName())\n                budget = 300\n                pcall(tree, d, 5, 1)\n            end\n        end\n    end\nend\npcall(findNamed, RS)\npcall(findNamed, game:GetService(\"ReplicatedFirst\"))\npcall(findNamed, workspace)\n\n-- 4. PLAYERGUI (giao diện inventory / gold / level / class)\nw(\"\\n##### PLAYERGUI #####\")\nlocal kw = { \"inventory\", \"item\", \"weapon\", \"armor\", \"armour\", \"equip\", \"bag\", \"gold\", \"coin\", \"level\", \"class\", \"stat\", \"backpack\", \"slot\" }\nlocal function anyKw(s)\n    for _, k in ipairs(kw) do\n        if s:find(k, 1, true) then return true end\n    end\n    return false\nend\nlocal pg = player:FindFirstChildOfClass(\"PlayerGui\")\nif pg then\n    for _, g in ipairs(pg:GetChildren()) do w(g.Name .. \" [\" .. g.ClassName .. \"]\") end\n    w(\"--\")\n    local seen = 0\n    for _, d in ipairs(pg:GetDescendants()) do\n        if seen >= 500 then break end\n        local isText = d:IsA(\"TextLabel\") or d:IsA(\"TextButton\")\n        if isText then\n            if d.Text ~= \"\" and anyKw(d:GetFullName():lower()) then\n                seen += 1\n                w(d:GetFullName() .. ' [' .. d.ClassName .. '] text=\"' .. short(d.Text, 60) .. '\"')\n            end\n        elseif (d:IsA(\"Frame\") or d:IsA(\"ScrollingFrame\") or d:IsA(\"ImageLabel\")) and anyKw(d.Name:lower()) then\n            seen += 1\n            w(d:GetFullName() .. \" [\" .. d.ClassName .. \"]\")\n        end\n    end\nend\n\n-- 5. GC: tìm bảng dữ liệu của game (có >= 2 key: Inventory/Gold/Class/Level...)\nif getgc and C.GC ~= false then\n    w(\"\\n##### GC TABLES #####\")\n    local ok, gc = pcall(getgc, true)\n    if ok and type(gc) == \"table\" then\n        local keys = { \"Inventory\", \"Gold\", \"Class\", \"Level\", \"Items\", \"Weapons\", \"Armor\", \"Armors\", \"Equipped\", \"Coins\" }\n        local found = 0\n        local function describe(t, d, ind)\n            local n = 0\n            for k, v in pairs(t) do\n                n += 1\n                if n > 40 then\n                    w(ind .. \"...\")\n                    break\n                end\n                local tv = typeof(v)\n                if tv == \"table\" then\n                    w(ind .. short(k, 40) .. \" = {table}\")\n                    if d < 3 then describe(v, d + 1, ind .. \"  \") end\n                elseif tv == \"Instance\" then\n                    w(ind .. short(k, 40) .. \" = Instance \" .. v:GetFullName())\n                else\n                    w(ind .. short(k, 40) .. \" = \" .. short(v, 60))\n                end\n            end\n        end\n        for i, t in ipairs(gc) do\n            if type(t) == \"table\" then\n                local hit = 0\n                for _, k in ipairs(keys) do\n                    if rawget(t, k) ~= nil then hit += 1 end\n                end\n                if hit >= 2 then\n                    found += 1\n                    w(\"-- table #\" .. found)\n                    pcall(describe, t, 1, \"  \")\n                    if found >= 10 then break end\n                end\n            end\n            if i % 4000 == 0 then task.wait() end\n        end\n    end\nend\n\n-- ===== LƯU / GỬI =====\nlocal text = table.concat(out, \"\\n\")\nif #text > 900000 then text = text:sub(1, 900000) end\nif writefile then pcall(writefile, \"dq_dump_\" .. player.Name .. \".txt\", text) end\nif setclipboard then pcall(setclipboard, text) end\nlocal url = \"__DUMP__\"\nif httpRequest and C.ApiKey then\n    local ok, res = pcall(httpRequest, {\n        Url = url .. \"?name=\" .. HttpService:UrlEncode(player.Name),\n        Method = \"POST\",\n        Headers = { [\"Content-Type\"] = \"text/plain\", [\"x-api-key\"] = C.ApiKey },\n        Body = text,\n    })\n    print(\"[DQ-Dump] gui server:\", ok and res and res.StatusCode or \"loi\")\nend\nprint(\"[DQ-Dump] xong: \" .. #text .. \" ky tu. File: dump_\" .. player.Name .. \".txt canh server.js\")\n";
const s = (v, n) => String(v ?? '').slice(0, n);
// ---- Anh item tu wiki (dungeonquestroblox.fandom.com), cache vao imgcache.json ----
const WIKI = 'https://dungeonquestroblox.fandom.com/api.php';
async function wikiPages(extra) {
  const r = await fetch(WIKI + '?action=query&format=json&prop=pageimages&piprop=thumbnail&pithumbsize=96&redirects=1&' + extra, { headers: { 'user-agent': 'dq-tracker/1.0' } });
  const j = await r.json();
  return Object.values((j.query && j.query.pages) || {});
}
async function wikiFiles(extra) {   // tim trong muc File: (anh upload) khi trang item khong co anh dai dien
  const r = await fetch(WIKI + '?action=query&format=json&prop=imageinfo&iiprop=url&iiurlwidth=96&redirects=1&' + extra, { headers: { 'user-agent': 'dq-tracker/1.0' } });
  const j = await r.json();
  return Object.values((j.query && j.query.pages) || {});
}
const fileThumb = p => { const ii = p && p.imageinfo && p.imageinfo[0]; return (ii && (ii.thumburl || ii.url)) || ''; };
async function wikiImg(name) {
  const key = name.toLowerCase();
  try {   // ket qua rong cu ('' hoac '!<time>') chi tin trong 1 ngay, sau do hoi lai wiki
    const [hit] = await redis([['HGET', 'dq:img', key]]);
    if (hit) { if (hit[0] !== '!') return hit; if (Date.now() - (+hit.slice(1) || 0) < 864e5) return ''; }
  } catch {}
  if (typeof fetch !== 'function') return null;
  let url = '';
  try {
    const k = key.replace(/[^a-z0-9]/g, '');
    const near = p => { const t = p.title.replace(/^[^:]+:/, '').replace(/\.[a-z0-9]+$/i, '').toLowerCase().replace(/[^a-z0-9]/g, ''); return t.includes(k) || k.includes(t); };
    let pg = (await wikiPages('titles=' + encodeURIComponent(name))).find(p => p.thumbnail);
    if (!pg) {
      pg = (await wikiPages('generator=search&gsrnamespace=0&gsrlimit=5&gsrsearch=' + encodeURIComponent(name)))
        .sort((a, b) => (a.index || 0) - (b.index || 0))
        .find(p => p.thumbnail && near(p));
    }
    url = (pg && pg.thumbnail && pg.thumbnail.source) || '';
    if (!url) {   // thu truc tiep File:<ten>.png roi tim trong muc File
      for (const ext of ['png', 'webp', 'jpg']) {
        const f = (await wikiFiles('titles=' + encodeURIComponent('File:' + name + '.' + ext))).find(p => fileThumb(p));
        if (f) { url = fileThumb(f); break; }
      }
    }
    if (!url) {
      const f = (await wikiFiles('generator=search&gsrnamespace=6&gsrlimit=8&gsrsearch=' + encodeURIComponent(name)))
        .sort((a, b) => (a.index || 0) - (b.index || 0))
        .find(p => fileThumb(p) && near(p));
      if (f) url = fileThumb(f);
    }
  } catch { return null; }
  try { await redis([['HSET', 'dq:img', key, url || '!' + Date.now()]]); } catch {}
  return url;
}

// ---- Anh item lay truc tiep tu Roblox theo imageId (chinh xac hon wiki) ----
async function rbxImg(id) {
  const key = 'rbx:' + id;
  try { const [hit] = await redis([['HGET', 'dq:img', key]]); if (hit) return hit; } catch {}
  try {
    const r = await fetch('https://thumbnails.roblox.com/v1/assets?assetIds=' + id + '&returnPolicy=PlaceHolder&size=150x150&format=Png&isCircular=false', { headers: { 'user-agent': 'dq-tracker/1.0' } });
    if (!r.ok) return null;
    const d = (((await r.json()).data) || [])[0];
    if (!d || d.state === 'Pending') return null;
    const url = d.state === 'Completed' && /^https:\/\//.test(d.imageUrl || '') ? d.imageUrl : '';
    if (url) { try { await redis([['HSET', 'dq:img', key, url]]); } catch {} }
    return url;
  } catch { return null; }
}

// ---- Phan loai thong bao loi/kick: captcha lock hay captcha rejoin ----
const mkRe = v => { try { return v ? new RegExp(v, 'i') : null; } catch { console.error('[!] Regex khong hop le:', v); return null; } };
const LOCK_RE = mkRe(process.env.LOCK_ERR_REGEX), FACE_RE = mkRe(process.env.FACE_ERR_REGEX);
const FLAG_GRACE_MS = 90e3;   // acc bao cao binh thuong sau khoang nay ke tu luc bi gan co tu dong -> tu go co
const classifyErr = t => {
  const x = String(t || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/đ/g, 'd');
  if (!x) return null;
  const hasCode = n => new RegExp('(error|code|ma loi|loi)\\D{0,15}\\b' + n + '\\b|\\(' + n + '\\)').test(x);
  if (hasCode(773)) return 'rejoin';                    // Error Code 773 = captcha rejoin
  if (hasCode(267)) return 'kicked';                    // Error Code 267 = Kick
  if (hasCode(529)) return 'rejoin';                    // Error Code 529 = Captcha Rejoin
  if (hasCode(277)) return 'disconnect';                // Error Code 277 = Disconnected
  if (LOCK_RE && LOCK_RE.test(x)) return 'lock';        // Captcha lock (mau thong bao tu env LOCK_ERR_REGEX)
  if (FACE_RE && FACE_RE.test(x)) return 'face';        // Face lock (mau thong bao tu env FACE_ERR_REGEX)
  return null;
};

const send = (res, code, body, type = 'application/json') => {
  const h = { 'content-type': type + '; charset=utf-8', 'cache-control': 'no-store' };
  if (LOCAL_AGENT) { h['access-control-allow-origin'] = LOCAL_AGENT_ORIGIN; h['vary'] = 'Origin'; }
  res.writeHead(code, h);
  res.end(typeof body === 'string' ? body : JSON.stringify(body));
};
// =====================================================================
//  TRADE ALT -> MAIN DIEU KHIEN TU WEB
//  - Web tao "lenh trade" (theo item da chon hoac theo do hiem) cho 1 hoac nhieu acc alt.
//  - Lenh luu o Redis: dq:trade:<owner> = hash { <ten alt>: JSON } (moi alt toi da 1 lenh).
//  - Script Lua nhan lenh qua phan hoi cua /api/report (moi lan bao cao) va /api/trade/poll (hoi nhanh khi dang co lenh).
//  - Acc co lenh gui di = ALT; acc la dich cua lenh = MAIN (chi chap nhan trade tu alt co lenh).
//  - TU DONG TRA: main tu THEM N mon Common (truong back cua lenh, mac dinh 2, tuy chinh/tat tren web) vao CHINH phien trade dang mo
//    ngay khi alt dang dua do (khong doi trade rieng sau) -> ca hai xac nhan 1 lan duy nhat. Server chi gui kem so luong "back" qua
//    /api/trade/poll (truong back trong incoming); Lua ben MAIN tu doc kho va them item (xem pickBackItems trong TRADE_LUA).
//  - TRA DO: main cung co the la acc gui (lenh luu theo ten main, truong main = ten alt nhan, ret = true). Giao thuc Lua doi xung nen
//    alt nhan se tu accept + xac nhan giong main. Moi acc chi co 1 lenh gui di dang hoat dong -> main tra lan luot tung alt.
//  Bien moi truong (tuy chon): TRADE_TYPES=weapon  (them chest,helmet,ability CHI KHI da xac nhan chuoi loai cua game)
// =====================================================================
const TRADE_TYPES = new Set(String(process.env.TRADE_TYPES || 'weapon,chest,helmet,ability').split(',').map(x => x.trim().toLowerCase()).filter(x => /^[a-z]{1,10}$/.test(x)));
if (!TRADE_TYPES.size) TRADE_TYPES.add('weapon');
const TRADE_CAT_KEY = { weapon: 'weapons', chest: 'chests', helmet: 'helmets', ability: 'abilities' };
const TRADE_RARITIES = new Set(['ult', 'legend', 'epic']);
const TRADE_QUEUE_TTL = Math.max(3e3, +process.env.TRADE_QUEUE_TTL_MS || 10e3);   // lenh cho (queued) ma acc khong nhan/thuc hien sau ngan nay -> xoa (het han)
const TRADE_TTL = 10 * 60e3;          // lenh khong duoc cap nhat trong ngan nay -> het han
const TRADE_KEEP = 24 * 36e5;         // giu lich su lenh 24h
const TRADE_MOVE_MS = 90e3;     // alt dang teleport sang server cua main: lenh duoc giu o hang doi them ngan nay
const TRADE_GOLD_MAX = 1e15;   // tran gold cho 1 lenh (chong nhap nham)
const TRADE_MAX_ITEMS = 999;   // gioi han ky thuat (khong con gioi han thuc te) cho 1 lenh trade / 1 lan tra do
const tradeCatsLua = '{ ' + [...TRADE_TYPES].filter(t => TRADE_CAT_KEY[t]).map(t => TRADE_CAT_KEY[t] + ' = "' + t + '"').join(', ') + ' }';

const TRADE_LUA = String.raw`-- ===== TRADE ALT -> MAIN (dieu khien tu web) =====
-- Cung 1 script tracker chay tren moi acc. Web tao "lenh trade"; server tra lenh cho acc qua phan hoi cua /api/report
-- (va qua /api/trade/poll khi dang co lenh). Acc nao co lenh gui di thi la ALT, acc nao la dich cua lenh thi la MAIN.
-- Da doi chieu voi code game (Ui/trading, remoteGuiHandler, leaderboard):
--   sendTradeRequest:InvokeServer(player) -> "sent" | tradingDisabled | DND | busyTrading | playerGone
--   acceptTradeRequest:InvokeServer(player) -> "beginTrade" | tradingDisabled | DND | busyTrading | playerGone
--   addItemToTradeFolder:FireServer(loai, so)  loai = weapon|ability|chest|helmet ; so = phan so cuoi cua key (weapon_123 -> 123)
--   playerAcceptTrade:FireServer() la NUT BAT/TAT (bam lan 2 = huy xac nhan). Trang thai o workspace.trading/<phien>/<Ten>Accepted
-- Van nen thu DRY truoc.
do
    local TRADE_CATS = C.TradeCats or __TRADE_CATS__
    -- ===== CHE DO NHANH (chinh bang _G.Config neu game bi loi/reject thi tang cac so nay len) =====
    local TRADE_DELAY = tonumber(C.TradeDelay) or 0.15     -- giay giua cac lan them item (cu: 0.8)
    local TRADE_QUIET = tonumber(C.TradeQuiet) or 3        -- (main) chi dung cho DRY
    local IDLE_POLL = tonumber(C.TradeIdlePoll) or 5       -- hoi lenh moi N giay ke ca luc ranh (0 = tat, chi nhan lenh theo bao cao 60s). Moi lan hoi ton ~3 lenh Redis
    local FAST_POLL = tonumber(C.TradeFastPoll) or 1       -- dang co lenh -> hoi moi N giay (cu: 3)
    local BACK_WAIT = tonumber(C.TradeBackWait) or 8       -- (alt) toi da cho bao lau de main them xong Common truoc khi alt xac nhan
    local ACCEPT_DELAY = tonumber(C.TradeAcceptDelay) or 0.1   -- main cho bao lau truoc khi chap nhan loi moi trade (cu: 0.5)
    local SETTLE = tonumber(C.TradeSettle) or 0.2          -- cho phien trade on dinh truoc khi them item (cu: 1)
    local STEP = 0.05                                      -- chu ky kiem tra dieu kien (cu: 0.2)
    local MAIN_TICK = 0.1                                  -- (main) chu ky kiem tra de bam xac nhan (cu: 0.5)

    local function tlog(...)
        local parts = {}
        for i = 1, select("#", ...) do parts[i] = tostring((select(i, ...))) end
        print("[DQR TRADE]", table.concat(parts, " "))
    end
    tlog("module trade da nap (ban sua back v3 + sell v1 + buy-verify v4) - acc:", player.Name)   -- thay dong nay trong F9 (tim: DQR) = acc dang chay ban moi

    local base = (tostring(C.Endpoint):gsub("/api/report$", ""))
    local function api(method, path, body)
        if not httpRequest then return nil, 0 end
        local req = {
            Url = base .. path,
            Method = method,
            Headers = { ["Content-Type"] = "application/json", ["x-api-key"] = C.ApiKey, ["x-hwid"] = HWID },
        }
        if body ~= nil then req.Body = HttpService:JSONEncode(body) end
        local ok, res = pcall(httpRequest, req)
        if not ok or type(res) ~= "table" then return nil, 0 end
        local code = tonumber(res.StatusCode) or 200
        local okJ, data = pcall(function() return HttpService:JSONDecode(res.Body or "") end)
        if okJ and type(data) == "table" then return data, code end
        return nil, code
    end

    local function R(name)
        local rr = ReplicatedStorage:FindFirstChild("remotes")
        return rr and rr:FindFirstChild(name)
    end

    local function nrm(s) return (tostring(s or ""):lower():gsub("[^%a]", "")) end

    -- ===== SPY CHAN DOAN: in ra console tham so game gui khi BAN TU TRADE BANG TAY =====
    -- Cach dung: tren 1 acc, tu mo trade voi acc khac, tu them 1 item bang tay -> xem dong "[DQR TRADE] SPY ..." (F9 > Client).
    -- Mac dinh TAT. Bat bang cach them TradeSpy = true vao bang cau hinh C (hook namecall).
    if C.TradeSpy == true and hookmetamethod and getnamecallmethod then
        local watch = { addItemToTradeFolder = true, sendTradeRequest = true, acceptTradeRequest = true, playerAcceptTrade = true }
        local function fmt(v)
            local t = typeof(v)
            if t == "Instance" then return "Instance(" .. v.ClassName .. ":" .. v.Name .. ")" end
            if t == "table" then
                local parts = {}
                for k, x in pairs(v) do
                    parts[#parts + 1] = tostring(k) .. "=" .. tostring(x)
                    if #parts >= 8 then break end
                end
                return "table{" .. table.concat(parts, ", ") .. "}"
            end
            return t .. "(" .. tostring(v) .. ")"
        end
        local okHook, errHook = pcall(function()
            local old
            old = hookmetamethod(game, "__namecall", function(self, ...)
                local m = getnamecallmethod()
                if (m == "FireServer" or m == "InvokeServer") and typeof(self) == "Instance" and (watch[self.Name] or string.find(string.lower(self.Name), "gold", 1, true))
                    and not (checkcaller and checkcaller()) then
                    local args = table.pack(...)
                    local parts = {}
                    for i = 1, args.n do parts[i] = fmt(args[i]) end
                    task.spawn(tlog, "SPY", self.Name, m, table.concat(parts, " | "))
                end
                return old(self, ...)
            end)
        end)
        if okHook then
            tlog("SPY bat: tu thao tac trade bang tay de xem game gui tham so gi")
        else
            tlog("SPY loi:", errHook)
        end
    end

    -- ===== TRANG THAI TRADE TRONG WORKSPACE (theo code game: workspace.trading/<phien>/{ObjectValue(Value=Player) chua item, <Ten>Accepted}) =====
    local function tradeOf(plr)          -- tra ve (thu muc phien trade, ObjectValue cua plr) hoac nil
        local tr = workspace:FindFirstChild("trading")
        if not tr or not plr then return nil end
        for _, f in ipairs(tr:GetChildren()) do
            for _, c in ipairs(f:GetChildren()) do
                if c:IsA("ObjectValue") and c.Value == plr then return f, c end
            end
        end
        return nil
    end
    local function offerCount(ov)        -- so item (khong tinh vang) trong phan de nghi cua 1 nguoi
        local n = 0
        if ov then
            for _, c in ipairs(ov:GetChildren()) do
                if c.Name ~= "goldOffer" and c:FindFirstChild("type") then n = n + 1 end
            end
        end
        return n
    end
    local function acceptedBy(folder, plr)
        local v = folder and plr and folder:FindFirstChild(plr.Name .. "Accepted")
        return v ~= nil and v.Value == true
    end

    -- ===== GOLD TRONG TRADE (workspace.trading/<phien>/<ObjectValue cua nguoi choi>/goldOffer) =====
    local function goldOf(ov)            -- so gold dang de nghi trong phan cua 1 nguoi
        local g = ov and ov:FindFirstChild("goldOffer")
        if not g then return 0 end
        local ok, v = pcall(function() return g.Value end)
        v = ok and tonumber(v) or 0
        if v ~= v or v == math.huge or v == -math.huge then return 0 end
        return v
    end
    local function goldRemotes()         -- cac remote co chu "gold" (dung de chan doan)
        local out, rr = {}, ReplicatedStorage:FindFirstChild("remotes")
        if rr then
            for _, r in ipairs(rr:GetChildren()) do
                if (r:IsA("RemoteEvent") or r:IsA("RemoteFunction")) and string.find(string.lower(r.Name), "gold", 1, true) then
                    out[#out + 1] = r
                end
            end
        end
        return out
    end
    local function findGoldRemote()      -- chi nhan remote co ca "gold" va "trade/offer" trong ten (khong doan bua remote khac nhu mua/ban gold)
        if type(C.TradeGoldRemote) == "string" and C.TradeGoldRemote ~= "" then return R(C.TradeGoldRemote) end
        for _, r in ipairs(goldRemotes()) do
            local n = string.lower(r.Name)
            if string.find(n, "trade", 1, true) or string.find(n, "offer", 1, true) then return r end
        end
        return nil
    end

    -- ===== TRANG THAI TRADE (event server -> client) =====
    local st = { open = false, closed = false, lastChange = 0, changes = 0, partner = nil }
    local incoming = {}      -- [ten alt] = { id=, dry= }  (MAIN chi chap nhan trade tu cac alt nay)
    local activeUntil = 0    -- con thoi gian nay thi hoi server moi 3s
    local busy, lastJobId = false, nil
    -- ===== TRADE TRUC TIEP (khong qua server): MAIN chi can biet ten cac alt, ALT co nut "Xac nhan trade" ngay trong game =====
    -- Cau hinh: MAIN  -> _G.Config = { TradeAlts = { "alt1", "alt2" } }
    --           ALT   -> _G.Config = { TradeMain = "TenAccMain", TradeRarity = { "ult", "legend" }, TradeMax = 5 }
    local LOCAL_ALTS = {}
    if type(C.TradeAlts) == "table" then
        for _, n in ipairs(C.TradeAlts) do LOCAL_ALTS[tostring(n)] = { id = "local", dry = false } end
    end
    -- TradeAuto = true (tren MAIN): nhan trade tu bat ky ai va tu bam Accept ngay khi ben kia da them item + da Accept
    -- (chi khi phan de nghi cua main dang TRONG, nen main khong the bi mat do). Khong can danh sach alt, khong can cho lenh.
    local AUTO = C.TradeAuto == true
    local AUTO_ENTRY = { id = "auto", dry = false, auto = true }
    local function incOf(name)
        return incoming[name] or LOCAL_ALTS[name] or ((AUTO and name ~= player.Name) and AUTO_ENTRY or nil)
    end

    local function listen(name, fn)
        local r = R(name)
        if r and r:IsA("RemoteEvent") then
            r.OnClientEvent:Connect(function(...) fn(...) end)
        else
            tlog("thieu remote", name)
        end
    end

    -- MAIN: hoi server ngay luc co yeu cau trade den (khong cho bao cao dinh ky 60s) de biet alt nao dang co lenh gui toi minh
    local function refreshIncoming()
        local data = api("GET", "/api/trade/poll?name=" .. HttpService:UrlEncode(player.Name))
        if data and type(data.trade) == "table" and type(data.trade.incoming) == "table" then
            local inc = {}
            for _, x in ipairs(data.trade.incoming) do
                if type(x) == "table" and type(x.alt) == "string" then inc[x.alt] = { id = x.id, dry = x.dry == true, back = tonumber(x.back) or 0, backItems = (type(x.backItems) == "table" and #x.backItems > 0) and x.backItems or nil } end
            end
            incoming = inc
            activeUntil = os.clock() + 60
            return true
        end
        return false
    end

    local function findSender(args)
        for i = 1, args.n do
            local a = args[i]
            if typeof(a) == "Instance" and a:IsA("Player") and incOf(a.Name) then return a end
            if type(a) == "string" and incOf(a) and Players:FindFirstChild(a) then return Players[a] end
        end
        return nil
    end

    local function onTradeRequest(...)
        local args = table.pack(...)
        local found = findSender(args)
        if not found and next(LOCAL_ALTS) == nil and not AUTO then
            refreshIncoming()
            found = findSender(args)
        end
        if not found and next(incoming) == nil and next(LOCAL_ALTS) == nil and not AUTO then
            tlog("bo qua yeu cau trade: khong co lenh nao gui toi acc nay")
            return
        end
        task.wait(ACCEPT_DELAY)
        local accept = R("acceptTradeRequest")
        if not accept then return end
        local function try(plr)
            local ok, res = pcall(function() return accept:InvokeServer(plr) end)
            tlog("acceptTradeRequest", plr.Name, ok, res)
            if ok and res == "beginTrade" then st.partner = plr.Name; return true end
            return false
        end
        if found then
            try(found)
        else
            -- khong doc duoc nguoi gui tu event -> thu lan luot cac alt dang co lenh gui toi minh
            local tried = {}
            for name in pairs(incoming) do tried[name] = true end
            for name in pairs(LOCAL_ALTS) do tried[name] = true end
            if AUTO then
                for _, plr in ipairs(Players:GetPlayers()) do if plr ~= player then tried[plr.Name] = true end end
            end
            for name in pairs(tried) do
                local p = Players:FindFirstChild(name)
                if p then
                    if try(p) then break end
                    task.wait(ACCEPT_DELAY)
                end
            end
        end
    end

    task.spawn(function()
        if not ReplicatedStorage:WaitForChild("remotes", 30) then return end
        listen("openTradeGuis", function() st.open = true; st.closed = false; st.lastChange = os.clock(); st.changes = 0 end)
        listen("deleteTradeGuis", function() st.open = false; st.closed = true; st.partner = nil end)
        listen("updateTradeLocally", function() st.lastChange = os.clock(); st.changes = st.changes + 1 end)
        listen("addItemToTradeFolder", function() st.lastChange = os.clock(); st.changes = st.changes + 1 end)
        listen("cloneTradeRequestGui", onTradeRequest)
    end)

    -- MAIN: vong lap bam xac nhan trade (gom ca logic tu tra Common) duoc dat ben duoi, ngay sau readInv,
    -- vi can dung chung ham doc kho voi ALT.

    -- ===== ALT: DOC KHO, CHON ITEM =====
    local function idOf(key, item)       -- game gui so cuoi cua key (weapon_123 -> 123), khong dung uniqueItemNum
        local n = tonumber(tostring(key):match("(%d+)$") or "")
        if n then return n end
        local m = tonumber(tostring(item.uniqueItemNum or ""):match("(%d+)$") or "")
        return m or tonumber(key) or key
    end

    local function numStr(v)
        local n = tonumber(v)
        if n and n == n and n ~= math.huge and n ~= -math.huge then
            if n == math.floor(n) then return string.format("%.0f", n) end
            return tostring(n)
        end
        return ""
    end
    -- chu ky item: phai khop voi item hien tren web (loai, ten, do hiem, 4 chi so goc)
    local function sigOf(kind, name, rarity, stats)
        return table.concat({
            kind, string.lower(tostring(name)), nrm(rarity),
            numStr(stats.physicalDamage), numStr(stats.physicalPower), numStr(stats.spellPower), numStr(stats.health),
        }, "|")
    end

    local function equippedIds(inv)      -- giong cach tracker nhan biet "dang mang"
        local ids = {}
        local function collect(v, depth)
            if depth > 3 then return end
            if type(v) == "table" then
                for k, x in pairs(v) do
                    if type(x) == "table" then
                        collect(x, depth + 1)
                        if x.uniqueItemNum ~= nil then ids[tostring(x.uniqueItemNum)] = true end
                        if type(k) == "string" then ids[k] = true end
                    elseif type(x) == "string" or type(x) == "number" then
                        ids[tostring(x)] = true
                    elseif x == true and type(k) == "string" then
                        ids[k] = true
                    end
                end
            elseif type(v) == "string" or type(v) == "number" then
                ids[tostring(v)] = true
            end
        end
        for topKey, topVal in pairs(inv) do
            if type(topKey) == "string" and string.find(string.lower(topKey), "equip", 1, true) then collect(topVal, 0) end
        end
        return ids
    end
    local function isEq(key, item, ids)
        -- game: weapon/chest/helmet chi cho trade khi equipped == false; ability co equipped = { q, e, q2, e2 }
        local e = item.equipped
        if type(e) == "boolean" then return e end
        if type(e) == "table" then
            for _, v in pairs(e) do
                if v == true or v == 1 or v == "true" or (type(v) == "string" and v ~= "" and v ~= "false") then return true end
            end
            return false
        end
        for _, f in ipairs({ "Equipped", "isEquipped", "equip", "equiped", "worn", "inUse" }) do
            local v = item[f]
            if v == true or v == 1 or v == "true" then return true end
        end
        if ids[tostring(key)] then return true end
        if item.uniqueItemNum ~= nil and ids[tostring(item.uniqueItemNum)] then return true end
        return false
    end

    local function nameOf(cat, key, item)   -- cung cach dat ten voi tracker de khop ten tren web
        local name = directItemName(item)
        local imageId = item.imageId or item.ImageId
        if not name and imageId ~= nil and cat.catalog then
            refreshItemCatalogs()
            local id = imageKey(imageId)
            local mapped = id and itemNamesByImage[cat.key .. "|" .. id]
            if mapped then name = mapped end
        end
        if not name then
            local itemId = tostring(item.uniqueItemNum or key)
            itemId = itemId:match("^[^_]+_(.+)$") or itemId
            name = cat.kind .. " #" .. itemId
        end
        return name
    end

    -- Game (UI ban do) gui MOI item la CHUOI "<so>:<UniqueItemID hoac none>", vd "3:9d606111-d747-404b-babf-c5eeff5c5d2b"
    --   so = phan sau dau "_" dau tien cua key (helmet_3 -> "3"); UniqueItemID = item.UniqueItemID (rong / khong co -> "none")
    -- (ban cu gui SO tran -> server moi bo qua het -> "Game khong ban")
    -- DOC GOLD THAT ( dung de bao "nhan duoc bao nhieu gold" sau khi ban )
    local function goldNow()
        local remotes = ReplicatedStorage:FindFirstChild("remotes")
        for _, n in ipairs({ "getGold", "getData", "reloadGold", "getCoins" }) do
            local r = remotes and remotes:FindFirstChild(n)
            if r and r:IsA("RemoteFunction") then
                local ok, v = pcall(function() return r:InvokeServer("gold") end)
                v = ok and tonumber(v) or nil
                if v then return v end
                ok, v = pcall(function() return r:InvokeServer() end)
                v = ok and tonumber(v) or nil
                if v then return v end
            end
        end
        for _, d in ipairs(player:GetDescendants()) do
            if d:IsA("ValueBase") then
                local n = string.lower(tostring(d.Name)):gsub("[%s_]", "")
                if n == "gold" or n == "coins" or n == "money" or n == "cash" then
                    return tonumber(d.Value) or 0
                end
            end
        end
        return nil
    end

    local function sellNumOf(key, item)
        local n = tostring(key):match("^[^_]*_(.+)$")
        if n and n ~= "" then return n end
        return tostring(idOf(key, item))
    end
    local function sellUidOf(item)
        local u = item.UniqueItemID
        if type(u) ~= "string" or u == "" then return "none" end
        return u
    end

    local function readInv()
        local reload = R("reloadInvy")
        if not reload then return nil, "Thiếu remote reloadInvy" end
        local ok, inv = pcall(function() return reload:InvokeServer() end)
        if not ok or type(inv) ~= "table" then return nil, "Không đọc được kho: " .. tostring(inv) end
        local eq = equippedIds(inv)
        local out = {}
        for _, cat in ipairs(ITEM_CATEGORIES) do
            local tradeStr = TRADE_CATS[cat.key]
            local items = tradeStr and inv[cat.key]
            if type(items) == "table" then
                for key, item in pairs(items) do
                    if type(item) == "table" then
                        local name = nameOf(cat, key, item)
                        out[#out + 1] = {
                            cat = tradeStr, id = idOf(key, item), num = sellNumOf(key, item), uid = sellUidOf(item), key = key, name = name, rarity = item.rarity,
                            eq = isEq(key, item, eq), sig = sigOf(cat.kind, name, item.rarity, item),
                        }
                    end
                end
            end
        end
        return out
    end

    -- MAIN: khi ALT da them item va da bam xac nhan (doc tu workspace.trading) -> main bam xac nhan DUNG 1 LAN
    -- (playerAcceptTrade la nut bat/tat nen khong bam lap). Neu sau 5s main van chua o trang thai "da xac nhan" thi thu lai 1 lan.
    -- TU DONG TRA: neu lenh gui toi minh co "back" > 0 (so Common main tu tra lai), main tu THEM cac item do vao CHINH
    -- phien trade dang mo ngay luc nay (khong doi alt xac nhan truoc, khong tao phien trade rieng sau) -> ca hai cung xac nhan 1 lan.
    -- Main chon do y HET CACH alt chon do: dung chung ham pickItems(job, list) voi 1 "job" gia lap mode=rarity/common.
    local pickItems, pickBackItems, pickBackList   -- khai bao TRUOC vong lap main (2 ham nay duoc gan o phia duoi; neu khong se la nil -> vong lap main bi crash)
    task.spawn(function()
        local confirmedSession, retries, dryLogged, confirmAt, warned, backAdded = false, 0, false, 0, false, false
        local backTries, backNextAt = 0, 0
        while true do
            task.wait(MAIN_TICK)
            if not st.open then
                confirmedSession, retries, dryLogged, warned, backAdded = false, 0, false, false, false
                backTries, backNextAt = 0, 0
            else
                -- main chua biet doi tac (vd: tu bam chap nhan bang tay / event khong bao) -> tu nhan ra tu phien trade trong workspace.trading
                if not st.partner then
                    local f0 = tradeOf(player)
                    if f0 then
                        for _, plr in ipairs(Players:GetPlayers()) do
                            if plr ~= player and incOf(plr.Name) and tradeOf(plr) == f0 then
                                st.partner = plr.Name
                                tlog("main nhận ra đối tác trade:", plr.Name)
                                break
                            end
                        end
                        if not st.partner and not warned and not busy then
                            warned = true
                            tlog("main đang trade nhưng đối tác không nằm trong TradeAlts / lệnh web -> KHÔNG tự xác nhận")
                        end
                    end
                end
                local inc = st.partner and incOf(st.partner)
                local partnerPlr = st.partner and Players:FindFirstChild(st.partner)
                local folder, myOv = tradeOf(player)
                local backList = inc and inc.backItems or nil   -- do cu the main phai tra cho alt nay (web da chia san)
                -- MAIN tu them do tra lai vao CHINH phien nay, lam y het ALT: doi phan de nghi cua minh (myOv) san sang -> SETTLE -> them tung mon, doi game nhan.
                -- Neu chua them duoc mon nao (UI chua on dinh) thi thu lai toi da 3 lan; da them 1 phan thi KHONG thu lai (tranh them trung).
                local wantBack = (tonumber(inc and inc.back) or 0) > 0 or backList ~= nil
                if inc and folder and myOv and not inc.dry and not backAdded and wantBack and backTries < 3 and os.clock() >= backNextAt then
                    backTries = backTries + 1
                    local want = math.max(0, math.floor(tonumber(inc.back) or 0))
                    if backList then
                        want = 0
                        for _, d in ipairs(backList) do want = want + math.max(1, math.floor(tonumber(d.q) or 1)) end
                    end
                    local lbl = backList and "item chia" or "Common"
                    local function mainSay(msg)   -- bao log cua MAIN len web (khoi can mo F9): hien trong khung log trade cung dong voi lenh cua alt
                        tlog(msg)
                        if inc.id and st.partner then
                            task.spawn(api, "POST", "/api/trade/status", { name = st.partner, id = inc.id, status = "running", msg = "[MAIN] " .. msg })
                        end
                    end
                    local back = backList and pickBackList(backList, want) or pickBackItems(math.min(want * 3 + 4, 60))
                    local addR = R("addItemToTradeFolder")
                    local added = 0
                    if #back == 0 then
                        backAdded = true
                        mainSay("muốn thêm " .. want .. " " .. lbl .. " nhưng KHÔNG TÌM THẤY trong kho (đã bán / đang mang / loại chưa bật trong TRADE_TYPES)")
                    elseif not addR then
                        backAdded = true
                        mainSay("THIẾU remote addItemToTradeFolder, không thêm được " .. lbl)
                    else
                        task.wait(SETTLE)
                        for _, it in ipairs(back) do
                            if added >= want or not st.open then break end
                            local _, ov2 = tradeOf(player)
                            if ov2 then myOv = ov2 end
                            local before = offerCount(myOv)
                            local desc = ("%s#%s (%s, %s)"):format(tostring(it.cat), tostring(it.id), tostring(it.name), tostring(it.rarity))
                            tlog("main thêm " .. lbl .. ": " .. desc .. " key=" .. tostring(it.key))
                            addR:FireServer(it.cat, it.id)
                            local t0 = os.clock()
                            while os.clock() - t0 < 4 and st.open and offerCount(myOv) <= before do task.wait(0.05) end
                            if offerCount(myOv) > before then
                                added = added + 1
                            else
                                mainSay("game KHÔNG nhận " .. desc .. " sau 4s (kẹt itemType)" .. (backList and "" or " -> thử Common khác"))
                            end
                            task.wait(TRADE_DELAY)
                        end
                        mainSay("đã thêm " .. added .. "/" .. want .. " " .. lbl .. " vào trade")
                        if offerCount(myOv) > 0 or not st.open then backAdded = true else backNextAt = os.clock() + 1.5 end
                    end
                end
                if inc and partnerPlr and folder then
                    local _, theirOv = tradeOf(partnerPlr)
                    local theirs = offerCount(theirOv)
                    local theirGold = goldOf(theirOv)
                    local confirm = R("playerAcceptTrade")
                    if inc.dry then
                        if (theirs > 0 or theirGold > 0) and not dryLogged and os.clock() - st.lastChange >= TRADE_QUIET then
                            dryLogged = true
                            tlog("DRY: alt da them", theirs, "item. Main se bam xac nhan o day (dang tat).")
                        end
                    elseif not confirmedSession then
                        if (theirs > 0 or theirGold > 0) and acceptedBy(folder, partnerPlr) and not acceptedBy(folder, player) and confirm
                            and (not inc.auto or (offerCount(myOv) == 0 and goldOf(myOv) == 0)) then
                            confirmedSession = true; confirmAt = os.clock()
                            confirm:FireServer()
                            tlog("main da bam xac nhan (alt da xac nhan,", theirs, "item", theirGold > 0 and ("+ " .. theirGold .. " gold") or "", ")")
                        end
                    elseif retries < 4 and confirm and os.clock() - confirmAt > 1.5
                        and acceptedBy(folder, partnerPlr) and not acceptedBy(folder, player) then
                        retries = retries + 1; confirmAt = os.clock()
                        confirm:FireServer()
                        tlog("main bam xac nhan lai (lan " .. retries .. ": xac nhan truoc bi reset / chua ghi nhan)")
                    end
                end
            end
        end
    end)

    function pickItems(job, list)
        local picked, missing = {}, 0
        local cap = tonumber(job.max) or 5
        if job.mode == "items" then
            local used = {}
            for _, d in ipairs(job.items or {}) do
                local want = tonumber(d.q) or 1
                local dsig = sigOf(tostring(d.t or ""):lower(), d.n, d.r, type(d.s) == "table" and d.s or {})
                for _, it in ipairs(list) do
                    if want <= 0 or #picked >= cap then break end
                    if not it.eq and not used[it] and it.sig == dsig then
                        used[it] = true; picked[#picked + 1] = it; want = want - 1
                    end
                end
                if want > 0 then missing = missing + want end
            end
            return picked, missing
        end
        -- theo do hiem (+ loc ten tuy chon)
        local allow = {}
        for _, r in ipairs(job.rarity or {}) do local k = nrm(r); if k ~= "" then allow[k] = true end end
        local names
        if type(job.names) == "table" and #job.names > 0 then
            names = {}
            for _, n in ipairs(job.names) do names[nrm(n)] = true end
        end
        for _, it in ipairs(list) do
            if #picked >= cap then break end
            if not it.eq then
                local rr, okR = nrm(it.rarity), false
                for w in pairs(allow) do
                    if rr:sub(1, #w) == w then okR = true break end   -- "ult" khop ca "ultimate"
                end
                if okR and (not names or names[nrm(it.name)]) then picked[#picked + 1] = it end
            end
        end
        return picked, 0
    end

    -- MAIN tu tra Common: lam dung y het alt - doc kho (readInv) roi dua qua pickItems voi 1 "job" gia lap
    -- (mode=rarity, rarity={"common"}), hoan toan dung chung logic chon do (bo trang bi, gioi han so luong) voi ALT.
    function pickBackItems(n)
        local list, err = readInv()
        if not list then
            tlog("khong doc duoc kho de tra Common:", tostring(err))
            return {}
        end
        local picked = pickItems({ mode = "rarity", rarity = { "common" }, names = {}, max = n }, list)
        return picked
    end

    -- MAIN tra DUNG cac mon web da chia cho alt nay (mode=items, khop theo chu ky item) - cung phien trade, khong tra nguoc sau
    function pickBackList(descs, want)
        local list, err = readInv()
        if not list then
            tlog("khong doc duoc kho de tra do:", tostring(err))
            return {}
        end
        local picked, missing = pickItems({ mode = "items", items = descs, max = want }, list)
        if missing and missing > 0 then tlog("thieu", missing, "item chia cho alt nay khong con trong kho main") end
        return picked
    end

    local function waitFor(cond, timeout)
        local t0 = os.clock()
        while os.clock() - t0 < timeout do
            if cond() then return true end
            task.wait(STEP)
        end
        return false
    end

    -- ===== NUT "XAC NHAN TRADE" TRONG GAME (ALT) =====
    local tradeBtn
    local BTN_TEXT = "Xác nhận trade"
    local COL_IDLE, COL_RUN, COL_OK, COL_ERR = Color3.fromRGB(88, 101, 242), Color3.fromRGB(217, 119, 6), Color3.fromRGB(22, 163, 74), Color3.fromRGB(220, 38, 38)
    local function setBtn(text, color)
        if not tradeBtn then return end
        tradeBtn.Text = text
        tradeBtn.BackgroundColor3 = color
    end
    local function resetBtnLater()
        task.delay(3, function() if not busy then setBtn(BTN_TEXT, COL_IDLE) end end)
    end

    local function report(job, status, msg, sent, goldSent)
        tlog(status, msg or "")
        if job.isLocal then
            local short = string.sub(tostring(msg or ""), 1, 36)
            if status == "running" then setBtn("Đang trade...", COL_RUN)
            elseif status == "done" then setBtn("Xong: " .. short, COL_OK); resetBtnLater()
            elseif status == "failed" then setBtn("Lỗi: " .. short, COL_ERR); resetBtnLater() end
            return nil, 200
        end
        return api("POST", "/api/trade/status", { name = player.Name, id = job.id, status = status, msg = msg, sent = sent, goldSent = goldSent })
    end

    -- dat gold de nghi trong phien trade; xac minh bang goldOffer. Tra ve (gold game ghi nhan, loi)
    local function setGold(ov, amount)
        local r = findGoldRemote()
        if not r then
            local names = {}
            for _, x in ipairs(goldRemotes()) do names[#names + 1] = x.Name end
            return 0, "không tìm thấy remote gold của trade (remote có chữ gold: " .. (#names > 0 and table.concat(names, ", ") or "không có")
                .. "). Đặt _G.Config.TradeGoldRemote = \"tên remote\", hoặc bật TradeSpy và tự thêm gold bằng tay 1 lần để xem game gửi gì"
        end
        for _, arg in ipairs({ amount, tostring(amount) }) do   -- thu dang so, neu game khong nhan thu dang chuoi
            tlog("thêm gold:", r.Name, typeof(arg), tostring(arg))
            local okF, errF = pcall(function()
                if r:IsA("RemoteFunction") then return r:InvokeServer(arg) end
                r:FireServer(arg)
            end)
            if not okF then tlog("gửi gold lỗi:", tostring(errF)) end
            if waitFor(function() return st.closed or goldOf(ov) > 0 end, 3) and goldOf(ov) > 0 then return goldOf(ov), nil end
            if st.closed then break end
        end
        return 0, "game không nhận gold qua remote '" .. r.Name .. "' (sai tham số, hoặc vượt giới hạn gold của trade)"
    end

    -- ===== TU CHUYEN ALT SANG SERVER CUA MAIN =====
    local TeleportService = game:GetService("TeleportService")
    local function queueRejoin()         -- sau teleport script bi mat -> xep hang chay lai (cung _G.Config + cung link script)
        local qot = queue_on_teleport or (syn and syn.queue_on_teleport) or (fluxus and fluxus.queue_on_teleport)
        if not qot then return false end
        local okJ, js = pcall(function() return HttpService:JSONEncode(C) end)
        if not okJ then return false end
        local url = base .. "/script?t=" .. HttpService:UrlEncode(tostring(C.ApiKey))
        return pcall(qot, string.format('_G.Config = game:GetService("HttpService"):JSONDecode(%q) loadstring(game:HttpGet(%q))()', js, url))
    end
    local function moveToMain(job)       -- luon tra ve (false, ly do) neu khong chuyen duoc; neu teleport thanh cong thi client dong truoc khi return
        if job.isLocal or C.TradeAutoJoin == false then return false, nil end
        local w = api("GET", "/api/trade/where?name=" .. HttpService:UrlEncode(tostring(job.main)))
        if type(w) ~= "table" or w.ok ~= true then return false, "tracker chưa biết main đang ở server nào (main đã chạy script tracker chưa?)" end
        if not w.jobId or not w.placeId then return false, "chưa có thông tin server của main (main cần chạy bản script mới)" end
        if w.fresh ~= true then return false, "thông tin server của main đã cũ (main offline?)" end
        if tostring(w.jobId) == tostring(game.JobId) then return false, "tracker báo main cùng server nhưng chưa thấy trong game (đợi load rồi thử lại)" end
        local _, code = report(job, "requeue", "Alt khác server main -> đang chuyển sang server của main...")
        if code ~= 200 then return false, "không chuyển server được (code " .. tostring(code) .. ", đã thử quá 3 lần?)" end
        if not queueRejoin() then tlog("executor không có queue_on_teleport -> cần để script trong autoexec thì alt mới tự chạy lại ở server main") end
        local failMsg
        local conn = TeleportService.TeleportInitFailed:Connect(function(_, result, msg) failMsg = tostring(result) .. " " .. tostring(msg) end)
        tlog("teleport sang server của main:", tostring(w.placeId), tostring(w.jobId))
        local okT, errT = pcall(function() TeleportService:TeleportToPlaceInstance(tonumber(w.placeId), tostring(w.jobId), player) end)
        if not okT then failMsg = tostring(errT) end
        local t0 = os.clock()
        while not failMsg and os.clock() - t0 < 40 do task.wait(0.5) end
        conn:Disconnect()
        return false, "teleport sang server main thất bại: " .. tostring(failMsg or "quá 40s (main ở server riêng / khác place?)")
    end

    local SEND_ERR = {
        tradingDisabled = "Game đang tắt trade (tradingDisabled)",
        DND = "Acc chính đang bật Do Not Disturb",
        busyTrading = "Acc chính đang bận trade với người khác",
        playerGone = "Acc chính đã rời game",
    }

    local function runJob(job)
        if busy or job.id == lastJobId then return end
        busy = true; lastJobId = job.id
        local ok, err = pcall(function()
            local mainPlr = Players:FindFirstChild(job.main)
            if not mainPlr then
                local _, why = moveToMain(job)   -- khac server: tu teleport sang server cua main (neu thanh cong thi client nay dong lai, khong chay tiep)
                report(job, "failed", "Acc chính '" .. tostring(job.main) .. "' không ở cùng server" .. (why and (" · " .. why) or ""))
                return
            end
            -- nhan lenh (neu main dang trade voi alt khac -> server tra 409, doi vong sau); doc kho CHAY SONG SONG de do mat 1 luot cho
            local code, list, e, d1, d2
            task.spawn(function() local _, c = report(job, "running", "Đang gửi yêu cầu trade..."); code = c; d1 = true end)
            task.spawn(function() local okR, a1, a2 = pcall(readInv); if okR then list, e = a1, a2 else e = tostring(a1) end; d2 = true end)
            while not (d1 and d2) do task.wait(0.03) end
            if code == 409 then lastJobId = nil; tlog("main đang bận / lệnh đã bị huỷ, chờ vòng sau"); return end

            if ReplicatedStorage:GetAttribute("TradingLocked") and not player:GetAttribute("TradeAllowlisted") then
                report(job, "failed", "Game đang khoá trade (TradingLocked) và acc này không nằm trong allowlist")
                return
            end

            local goldOnly = job.mode == "gold"
            local goldWant = math.max(0, math.floor(tonumber(job.gold) or 0))
            if goldWant > 0 then
                local haveG = math.floor(tonumber(findValue("gold")) or 0)
                if haveG <= 0 then
                    if goldOnly then report(job, "failed", "Acc gửi không có gold để trade") return end
                    tlog("acc không có gold -> chỉ gửi item")
                    goldWant = 0
                end
                if goldWant > haveG then tlog("gold muốn gửi", goldWant, "> gold đang có", haveG, "-> gửi", haveG); goldWant = haveG end
            end
            if not list and not goldOnly then report(job, "failed", e) return end
            local items, missing = {}, 0
            if not goldOnly and list then items, missing = pickItems(job, list) end
            if #items == 0 and goldWant == 0 then report(job, "failed", "Không có item hợp lệ để trade (đã bán/đang mang/sai loại?)") return end
            local n = job.dry and math.min(1, #items) or #items

            local send = R("sendTradeRequest")
            local add = R("addItemToTradeFolder")
            if not send or not add then report(job, "failed", "Thiếu remote trade của game") return end

            st.open, st.closed, st.changes = false, false, 0
            local okS, res = pcall(function() return send:InvokeServer(mainPlr) end)
            tlog("sendTradeRequest ->", okS, res)
            if not okS then report(job, "failed", "Gửi yêu cầu trade lỗi: " .. tostring(res)) return end
            if SEND_ERR[res] then report(job, "failed", SEND_ERR[res]) return end
            if not waitFor(function() return st.open end, 40) then
                report(job, "failed", "Acc nhận '" .. tostring(job.main) .. "' không chấp nhận trong 40s (acc đó đã chạy script tracker chưa?)")
                return
            end

            local folder, ov = tradeOf(player)
            if not ov then
                waitFor(function() folder, ov = tradeOf(player); return ov ~= nil end, 5)
            end
            if not ov then report(job, "failed", "Không thấy phiên trade trong workspace.trading") return end
            task.wait(SETTLE)

            local fastAdd = job.isLocal and C.TradeFast ~= false
            local addDelay = fastAdd and (tonumber(C.TradeFastDelay) or 0.05) or TRADE_DELAY
            for i = 1, n do
                if st.closed then break end
                local it = items[i]
                tlog(("thêm item %d/%d: %s#%s key=%s tên=%s hiếm=%s"):format(i, n, it.cat, tostring(it.id), tostring(it.key), tostring(it.name), tostring(it.rarity)))
                local before = offerCount(ov)
                add:FireServer(it.cat, it.id)
                if not fastAdd then waitFor(function() return st.closed or offerCount(ov) > before end, 4) end
                task.wait(addDelay)
            end
            if fastAdd then waitFor(function() return st.closed or offerCount(ov) >= n end, 4) end

            local goldSet = 0
            if goldWant > 0 and not st.closed then
                local got, gerr = setGold(ov, goldWant)
                goldSet = got
                if got <= 0 then
                    if goldOnly or offerCount(ov) == 0 then report(job, "failed", "Không thêm được gold: " .. tostring(gerr)) return end
                    tlog("không thêm được gold:", gerr)
                else
                    tlog("đã đặt", got, "gold vào trade")
                end
            end
            local have = offerCount(ov)
            if st.closed and have == 0 and goldSet == 0 then report(job, "failed", "Trade bị đóng trước khi thêm được item") return end
            if not goldOnly and have == 0 then
                report(job, "failed", "Game không nhận item nào vào trade (kiểm tra loại/ID item trong log)")
                return
            end
            tlog("đã có", have, "item trong trade", goldSet > 0 and ("+ " .. goldSet .. " gold") or "")

            if job.dry then
                report(job, "done", "DRY: đã thêm " .. have .. " item (" .. (items[1] and tostring(items[1].name) or "gold") .. "...)" .. (goldSet > 0 and (" + " .. goldSet .. " gold") or "") .. ", KHÔNG xác nhận. Kiểm tra cửa sổ trade rồi huỷ tay.", 0)
                return
            end
            n = have

            -- main tu them Common (back) vao CHINH phien nay -> doi main them xong roi moi xac nhan
            -- (game reset xac nhan moi khi 1 ben doi offer; xac nhan truoc roi main them do se bi mat)
            local backGot = 0
            local wantBack = job.isLocal and 0 or math.max(0, math.floor(tonumber(job.back) or 0), math.floor(tonumber(job.backN) or 0))
            if wantBack > 0 and not st.closed then
                local t0, last, lastAt = os.clock(), -1, os.clock()
                local c = 0
                while os.clock() - t0 < BACK_WAIT + wantBack * 0.6 and not st.closed do
                    local _, mov = tradeOf(mainPlr)
                    c = offerCount(mov)
                    if c >= wantBack then break end
                    if c ~= last then last, lastAt = c, os.clock() end
                    if c > 0 and os.clock() - lastAt >= ((tonumber(job.backN) or 0) > 0 and 5 or 2) then break end   -- main het Common de them -> khong doi them
                    task.wait(STEP)
                end
                task.wait(0.3)
                do local _, mov2 = tradeOf(mainPlr); c = offerCount(mov2) end
                backGot = c
                tlog("main đã thêm", c, "/", wantBack, "item trả vào trade")
            end

            -- playerAcceptTrade la nut bat/tat -> chi bam khi chua xac nhan
            task.wait(ACCEPT_DELAY)
            local acc = R("playerAcceptTrade")
            local function iAccepted() return acceptedBy(folder, player) end
            if acc and not iAccepted() then acc:FireServer() end
            waitFor(function() return iAccepted() or st.closed end, 5)
            if not iAccepted() and not st.closed and acc then
                tlog("chưa ghi nhận xác nhận, bấm lại 1 lần")
                acc:FireServer()
                waitFor(function() return iAccepted() or st.closed end, 5)
            end
            tlog("đã xác nhận =", tostring(iAccepted()), "- chờ main xác nhận...")
            local reacc, tWait = 0, os.clock()
            local bothSeen = false   -- ca alt lan main deu da xac nhan = trade da thuc hien (khong phu thuoc so mon trong kho: alt con NHAN do tra tu main)
            while not st.closed and os.clock() - tWait < 60 do
                task.wait(0.1)
                if not bothSeen and acceptedBy(folder, player) and acceptedBy(folder, mainPlr) then bothSeen = true end
                if not st.closed and acc and reacc < 4 and not iAccepted() and os.clock() - st.lastChange > 1.5 then
                    reacc = reacc + 1
                    tlog("xác nhận của alt bị reset (offer đổi) -> bấm lại lần", reacc)
                    acc:FireServer()
                end
            end
            if not st.closed then
                report(job, "failed", "Hết 60s mà trade chưa đóng (acc nhận chưa xác nhận?)")
                return
            end
            -- trade da dong -> bao hoan tat nhu ban truoc (khong so lai kho nua)
            local note = missing > 0 and (" · thiếu " .. missing .. " item không còn trong kho") or ""
            if wantBack > 0 then note = note .. " · main trả kèm " .. backGot .. "/" .. wantBack .. " item" end
            local gnote = goldSet > 0 and (" · " .. goldSet .. " gold") or ""
            report(job, "done", goldOnly and ("Đã gửi " .. goldSet .. " gold" .. note) or ("Đã gửi " .. n .. " item" .. gnote .. note), n, goldSet)
        end)
        if not ok then report(job, "failed", "Lỗi script: " .. tostring(err)) end
        busy = false
        task.delay(1, function() task.spawn(sendWithRetry) end)   -- cap nhat kho len dashboard
    end

    local function startLocal()
        if busy then tlog("đang trade, bỏ qua lần bấm này"); return end
        local main = tostring(C.TradeMain or "")
        if main == "" then tlog("Chưa cấu hình TradeMain trong _G.Config"); return end
        runJob({
            id = "L" .. tostring(os.clock()), isLocal = true, mode = "rarity", main = main,
            rarity = type(C.TradeRarity) == "table" and C.TradeRarity or { "ult", "legend" },
            names = type(C.TradeNames) == "table" and C.TradeNames or {},
            max = math.clamp(math.floor(tonumber(C.TradeMax) or 5), 1, 999),
            dry = C.TradeDry == true,
        })
    end

    if tostring(C.TradeMain or "") ~= "" and C.TradeButton ~= false then
        pcall(function()
            local parent = (gethui and gethui()) or game:GetService("CoreGui")
            local gui = Instance.new("ScreenGui")
            gui.Name = "DQRTradeGui"; gui.ResetOnSpawn = false; gui.DisplayOrder = 999
            local b = Instance.new("TextButton")
            b.Size = UDim2.fromOffset(200, 38); b.Position = UDim2.new(0, 12, 0.5, 0)
            b.BackgroundColor3 = COL_IDLE; b.TextColor3 = Color3.new(1, 1, 1)
            b.Font = Enum.Font.GothamBold; b.TextSize = 14; b.Text = BTN_TEXT
            local corner = Instance.new("UICorner"); corner.CornerRadius = UDim.new(0, 8); corner.Parent = b
            b.Parent = gui
            gui.Parent = parent
            tradeBtn = b
            b.MouseButton1Click:Connect(function() task.spawn(startLocal) end)
        end)
        -- phim tat tuy chon: _G.Config = { TradeHotkey = "T" }
        if type(C.TradeHotkey) == "string" and Enum.KeyCode[C.TradeHotkey] then
            pcall(function()
                game:GetService("UserInputService").InputBegan:Connect(function(input, gp)
                    if not gp and input.KeyCode == Enum.KeyCode[C.TradeHotkey] then task.spawn(startLocal) end
                end)
            end)
        end
    end

    -- ===== SPY CHAN DOAN BAN DO: in ra tham so THAT ma game gui khi tu bam nut Ban bang tay =====
    -- Dung khi "Ban ngay" tren web bi loi "Game khong ban" o mot vi tri nao do (vd: Lobby) nhung ban tay van ban binh thuong:
    -- bat SellSpy = true trong _G.Config, dung dung vi tri do, tu bam nut Ban trong UI game 1 lan (1 item la du),
    -- roi xem dong "[DQR TRADE] SELLSPY ..." trong console (F9 > Client) de doi chieu voi payload script dang gui qua sellItemEvent.
    if C.SellSpy == true and hookmetamethod and getnamecallmethod then
        local function fmtSS(v, depth)
            depth = depth or 0
            local t = typeof(v)
            if t == "Instance" then return "Instance(" .. v.ClassName .. ":" .. v.Name .. ")" end
            if t == "table" then
                if depth >= 4 then return "table{...}" end
                local parts = {}
                for k, x in pairs(v) do
                    parts[#parts + 1] = tostring(k) .. "=" .. fmtSS(x, depth + 1)
                    if #parts >= 20 then
                        parts[#parts + 1] = "..."
                        break
                    end
                end
                return "table{" .. table.concat(parts, ", ") .. "}"
            end
            return t .. "(" .. tostring(v) .. ")"
        end
        local okHookSS, errHookSS = pcall(function()
            local oldSS
            oldSS = hookmetamethod(game, "__namecall", function(self, ...)
                local m = getnamecallmethod()
                if (m == "FireServer" or m == "InvokeServer") and typeof(self) == "Instance"
                    and (string.find(string.lower(self.Name), "sell", 1, true) or string.find(string.lower(self.Name), "shop", 1, true))
                    and not (checkcaller and checkcaller()) then
                    local args = table.pack(...)
                    local parts = {}
                    for i = 1, args.n do parts[i] = fmtSS(args[i]) end
                    task.spawn(tlog, "SELLSPY", self.Name, m, table.concat(parts, " | "))
                end
                return oldSS(self, ...)
            end)
        end)
        if okHookSS then
            tlog("SELLSPY bat: tu bam nut Ban trong game de xem tham so that")
        else
            tlog("SELLSPY loi:", errHookSS)
        end
    end

    -- ===== BAN DO NGAY (tab Buyer/Sell) =====
    -- Web gui danh sach item da chon -> doc kho (reloadInvy) -> chon dung item theo chu ky -> remotes.sellItemEvent:FireServer(payload)
    -- payload giong UI ban tay: { helmet = {"so:UniqueItemID"}, ability = {...}, chest = {...}, weapon = {...} } (vd "3:9d606111-d747-...")
    -- KHONG phu thuoc dang o dau (dungeon / lobby) va KHONG bao gio ban do dang mang. Chi chay khi web bam "Ban ngay".
    local sellSeen = {}
    local function sellReport(job, status, msg, sold, fresh)
        api("POST", "/api/trade/sell/status", { name = player.Name, id = job.id, status = status, msg = msg, sold = sold or 0, fresh = fresh == true })
    end
    local function runSell(job)
        if type(job) ~= "table" or type(job.id) ~= "string" or sellSeen[job.id] then return end
        sellSeen[job.id] = true
        task.spawn(function()
            local ev = R("sellItemEvent")
            if not ev then sellReport(job, "failed", "Thiếu remote sellItemEvent", 0) return end
            sellReport(job, "running", "Đang bán...", 0)
            local list, err = readInv()
            if not list then sellReport(job, "failed", tostring(err), 0) return end
            local payload = { helmet = {}, ability = {}, chest = {}, weapon = {} }   -- bien: so thuan (Dungeon)
            local payloadStr = { helmet = {}, ability = {}, chest = {}, weapon = {} }   -- bien: chuoi "so:uid" (Lobby)
            local used, ids, picked, want = {}, {}, 0, 0
            for _, d in ipairs(job.items or {}) do
                local need = math.max(1, math.floor(tonumber(d.q) or 1))
                want = want + need
                local dsig = sigOf(tostring(d.t or ""):lower(), d.n, d.r, type(d.s) == "table" and d.s or {})
                for _, it in ipairs(list) do
                    if need <= 0 then break end
                    if not it.eq and not used[it] and it.sig == dsig and it.num and payload[it.cat] ~= nil then
                        used[it] = true
                        -- DUNG 2 DANG: so thuan VA chuoi "so:uid" - noi nao cung ban duoc, ben con lai thu lai
                        local numId = tonumber(it.num)
                        table.insert(payload[it.cat], numId or it.num)
                        table.insert(payloadStr[it.cat], tostring(it.num) .. ":" .. tostring(it.uid or "none"))
                        ids[it.cat .. "|" .. it.num] = true
                        picked = picked + 1
                        need = need - 1
                    end
                end
            end
            if picked == 0 then sellReport(job, "failed", "Không tìm thấy item (đã bán / đang mang / chỉ số đã đổi)", 0) return end
            local function countLeft(catIds)
                for _ = 1, 4 do
                    task.wait(0.5)
                    local after = readInv()
                    if after then
                        local left = 0
                        for _, it in ipairs(after) do
                            if catIds[it.cat .. "|" .. tostring(it.num)] then left = left + 1 end
                        end
                        return left
                    end
                end
                return nil
            end

            local goldBefore = goldNow()

            -- Thu dang so thuan truoc (Dungeon ban duoc); neu game khong ban thi thu lai dang chuoi "so:uid" (Lobby ban duoc)
            local usedPayload = "so-thuan"
            local ok, e = pcall(function() ev:FireServer(payload) end)
            if not ok then sellReport(job, "failed", "FireServer loi: " .. tostring(e), 0) return end

            local leftNow = countLeft(ids)
            local sold = (leftNow ~= nil) and (picked - leftNow) or picked
            local verified = leftNow ~= nil

            if leftNow ~= nil and leftNow > 0 then
                -- khong ban duoc dang so thuan -> thu dang chuoi "so:uid"
                print("[DQR TRADE] Khong ban duoc dang SO THUAN (con " .. leftNow .. "/" .. picked .. "), thu lai dang CHUOI so:uid")
                pcall(function() ev:FireServer(payloadStr) end)
                usedPayload = "chuoi-so-uid"
                local left2 = countLeft(ids)
                if left2 ~= nil then sold = picked - left2; verified = true end
                print("[DQR TRADE] Sau khi thu dang chuoi: con " .. tostring(left2 or "?") .. "/" .. picked)
            else
                print("[DQR TRADE] Ban duoc bang dang SO THUAN (" .. sold .. "/" .. picked .. ")")
            end

            print(("[DQR TRADE] Dang payload su dung: " .. usedPayload))


            -- gui NGAY kho moi len web (khong doi chu ky Interval 60s) TRUOC khi bao "Hoan tat" -> danh sach tren web cap nhat ngay,
            -- tranh bam ban lan 2 vao item cu con hien tren web -> loi "Khong tim thay item"
            local freshSent = false   -- true = kho moi da len server TRUOC khi bao "Hoan tat" (server khong can tu xoa item da ban)
            do   -- LUON gui kho that (ke ca ban that bai / ban 1 phan) -> web khoi phuc dung phan chua ban
                local okS, sentS = pcall(sendWithRetry)
                freshSent = okS and sentS == true
            end
            tlog("ban do:", sold, "/", want, verified and "(da xac minh)" or "(chua xac minh)")
            -- bao SO GOLD NHAN DUOC (doc lai sau khi ban; chi co gia tri neu game doc duoc)
            do
                local goldAfter = nil
                for _ = 1, 4 do
                    task.wait(0.4)
                    goldAfter = goldNow()
                    if goldAfter then break end
                end
                if goldBefore and goldAfter then
                    local gained = goldAfter - goldBefore
                    print(("[DQR TRADE] Ban xong: nhan " .. tostring(gained) .. " gold (Gold " .. tostring(goldBefore) .. " -> " .. tostring(goldAfter) .. ")"))
                else
                    print(("[DQR TRADE] Ban xong: khong doc duoc Gold de tinh so nhan (ban " .. tostring(sold) .. "/" .. tostring(want) .. " item)"))
                end
            end
            if sold <= 0 then
                sellReport(job, "failed", "Game không bán (0/" .. picked .. ") - kiểm tra lại remote/item", 0)
            else
                sellReport(job, "done", "Đã bán " .. sold .. "/" .. want .. " item" .. (verified and "" or " (chưa xác minh)"), sold, freshSent)
            end
        end)
    end

    local buySeen = {}
    local XPBOOST_LABEL = { [1] = "1 Hour", [2] = "2 Hours", [3] = "4 Hours" }
    -- ID that game that su gui len server (lay bang spy khi bam mua tay). Dien them ID cua 2 Hours / 4 Hours vao day,
    -- hoac dat _G.Config.XPBoostIds = { [2] = ..., [3] = ... } truoc khi chay loadstring.
    local XPBOOST_IDS = { [1] = 1013086750, [2] = 1013086878, [3] = 1013087251 }   -- 1h Gems, 2h Gold, 4h Gold (spy bang tay)
    local function xpBoostArg(bid)
        local custom = type(C.XPBoostIds) == "table" and C.XPBoostIds[bid]
        return custom or XPBOOST_IDS[bid]
    end
    local GAMEPASS_SHOP_KEYS = { vip = "vip", gold2 = "goldGamepass", drop = "extraItemGamepass", freeResets = "freeStatResets" }
    local function findXpRemote(cur)
        local custom = cur == "gems" and C.XPGemsRemote or C.XPGoldRemote
        if type(custom) == "string" and custom ~= "" then return R(custom) end
        -- Game dung CHUNG 1 remote requestGoldXPBoostPurchase cho ca Gold va Gems, phan biet bang tham so thu 2 (currency).
        -- (client Lua cu cua tracker: rf:FireServer(bid, cur) voi cur = "gold" | "gems"; getGoldXPBoostPrice tra ve gold/gemCost/currency)
        local names = cur == "gems"
            and { "requestGoldXPBoostPurchase", "requestGemsXPBoostPurchase", "requestGemXPBoostPurchase", "requestGemsXpBoostPurchase", "requestGemXpBoostPurchase" }
            or { "requestGoldXPBoostPurchase" }
        for _, n in ipairs(names) do local r = R(n); if r then return r end end
        if cur == "gems" then   -- quet thu muc remotes: ten co "xp" + "gem" + "purchase"
            local fo = ReplicatedStorage:FindFirstChild("remotes")
            if fo then
                for _, r in ipairs(fo:GetChildren()) do
                    local ln = string.lower(r.Name)
                    if string.find(ln, "xp", 1, true) and string.find(ln, "gem", 1, true) and string.find(ln, "purchase", 1, true) then return r end
                end
            end
        end
        return nil
    end
    local function buyReport(id, status, msg)
        if type(id) ~= "string" or id == "" then return end
        api("POST", "/api/trade/buy/status", { name = player.Name, id = id, status = status, msg = msg })
    end
    -- Gui lenh mua roi CHO ket qua that tu server (goldGamepassPurchaseResult / goldXPBoostPurchaseResult).
    -- Payload cua game: { success = bool, message = string, gamepassKey = string }. Khong co phan hoi trong 5s -> coi la that bai.
    local function fireAndWait(rf, eventName, ...)
        local ev = R(eventName)
        local result, conn = nil, nil
        if ev and ev:IsA("RemoteEvent") then
            conn = ev.OnClientEvent:Connect(function(p) if result == nil then result = (type(p) == "table") and p or { success = (p == true), message = tostring(p) } end end)
        end
        local args = { ... }
        local ok, e = pcall(function() rf:FireServer(table.unpack(args)) end)
        if not ok then if conn then conn:Disconnect() end return nil, "FireServer loi: " .. tostring(e) end
        if not conn then return { success = true, message = "(khong doc duoc ket qua: thieu event " .. eventName .. ")", unknown = true } end
        local t0 = os.clock()
        while result == nil and os.clock() - t0 < 5 do task.wait(0.1) end
        conn:Disconnect()
        return result
    end
    -- Chup so du / boost truoc-sau de XAC MINH lenh mua thuc su co hieu luc (khong chi tin vao event cua game)
    local function snapState()
        local ls = player:FindFirstChild("leaderstats")
        local function v(o) return o and tonumber(o.Value) or 0 end
        local gold = ls and (ls:FindFirstChild("Gold") or ls:FindFirstChild("Coins") or ls:FindFirstChild("Money"))
        return { gems = v(ls and ls:FindFirstChild("Gems")), gold = v(gold), xp = v(player:FindFirstChild("xpBoost")) }
    end
    local function ownsPass(shopKey)
        local f = player:FindFirstChild(shopKey)
        return f ~= nil and f:IsA("ValueBase") and f.Value == true
    end
    local function runBuy(job, delaySec)
        if type(job) ~= "table" or type(job.id) ~= "string" or buySeen[job.id] then return end
        buySeen[job.id] = true
        task.spawn(function()
            if delaySec and delaySec > 0 then task.wait(delaySec) end
            if type(job.key) == "table" then
                local bid = tonumber(job.key.id) or 1
                local cur = job.key.currency == "gems" and "gems" or "gold"
                local label = tostring(XPBOOST_LABEL[bid] or bid) .. (cur == "gems" and " (Gems)" or " (Gold)")
                buyReport(job.id, "running", "Dang mua x2 XP " .. label .. "...")
                local rf = findXpRemote(cur)
                if not rf then
                    local found = {}
                    local fo = ReplicatedStorage:FindFirstChild("remotes")
                    if fo then
                        for _, r in ipairs(fo:GetChildren()) do
                            local ln = string.lower(r.Name)
                            if string.find(ln, "xp", 1, true) or string.find(ln, "boost", 1, true) or string.find(ln, "gem", 1, true) or string.find(ln, "purchase", 1, true) then
                                found[#found + 1] = r.Name .. "(" .. r.ClassName .. ")"
                            end
                        end
                    end
                    buyReport(job.id, "failed", (cur == "gems"
                        and "Khong tim thay remote mua XP bang Gems (dat _G.Config.XPGemsRemote = \"ten remote\")"
                        or "Thieu remote requestGoldXPBoostPurchase")
                        .. " | remote lien quan: " .. (#found > 0 and table.concat(found, ", ") or "khong co"))
                    return
                end
                local arg = xpBoostArg(bid)
                if not arg then
                    buyReport(job.id, "failed", "Chua co ID game cua boost " .. tostring(XPBOOST_LABEL[bid] or bid) .. " (spy khi bam mua tay roi dien vao XPBOOST_IDS / _G.Config.XPBoostIds)")
                    return
                end
                local pr = R("getGoldXPBoostPrice")
                if pr and pr:IsA("RemoteFunction") then
                    local okp, info = pcall(function() return pr:InvokeServer(bid) end)
                    if okp and type(info) == "table" and info.ready ~= false and tonumber(info.nextAvailableIn) and tonumber(info.nextAvailableIn) > 0 then
                        local sec = math.floor(tonumber(info.nextAvailableIn))
                        buyReport(job.id, "failed", "Dang cooldown, con " .. string.format("%dh%02dm", math.floor(sec / 3600), math.floor(sec % 3600 / 60)) .. " moi mua lai duoc")
                        return
                    end
                end
                local before = snapState()
                local res, ferr = fireAndWait(rf, "goldXPBoostPurchaseResult", arg, cur)
                print("[DQR TRADE] Mua x2 XP " .. label .. " qua " .. rf.Name .. " (" .. tostring(arg) .. ", " .. cur .. ") -> " .. tostring(res and res.success) .. " " .. tostring(res and res.message or ferr))
                if not res then
                    buyReport(job.id, "failed", ferr or "Game khong tra ket qua trong 5s (co the dang cooldown / thieu tien)")
                elseif res.success == false then
                    buyReport(job.id, "failed", "Game tu choi: " .. tostring(res.message or "khong ro ly do"))
                else
                    task.wait(1.5)
                    local after = snapState()
                    local changed = after.xp > before.xp or (cur == "gems" and after.gems < before.gems) or (cur == "gold" and after.gold < before.gold)
                    if changed then
                        buyReport(job.id, "done", "Da mua x2 XP " .. label)
                    else
                        buyReport(job.id, "failed", "Game bao OK nhung khong thay boost tang / tien khong giam (cooldown, thieu tien hoac da het luot)")
                    end
                end
                task.delay(1.5, function() task.spawn(sendWithRetry) end)
                return
            end
            local key = tostring(job.key or "")
            local shopKey = GAMEPASS_SHOP_KEYS[key] or key
            buyReport(job.id, "running", "Dang mua " .. key .. "...")
            local rf = R("requestGoldGamepassPurchase")
            if not rf then buyReport(job.id, "failed", "Thieu remote requestGoldGamepassPurchase") return end
            if ownsPass(shopKey) then buyReport(job.id, "failed", "Da so huu " .. key .. " roi") return end
            local before = snapState()
            local res, ferr = fireAndWait(rf, "goldGamepassPurchaseResult", shopKey)
            print("[DQR TRADE] Mua GP: " .. key .. " (shop=" .. shopKey .. ") -> " .. tostring(res and res.success) .. " " .. tostring(res and res.message or ferr))
            if not res then
                buyReport(job.id, "failed", ferr or "Game khong tra ket qua trong 5s (co the thieu Gold)")
            elseif res.success == false then
                buyReport(job.id, "failed", "Game tu choi: " .. tostring(res.message or "khong ro ly do"))
            else
                task.wait(1.5)
                local after = snapState()
                if ownsPass(shopKey) or after.gold < before.gold then
                    buyReport(job.id, "done", "Da mua " .. key)
                else
                    buyReport(job.id, "failed", "Game bao OK nhung chua thay so huu " .. key .. " / Gold khong giam (thieu Gold?)")
                end
            end
            task.delay(1.5, function() task.spawn(sendWithRetry) end)
        end)
    end

    -- ===== NHAN THONG TIN TU SERVER =====
    local function onInfo(info)
        if type(info) ~= "table" then return end
        local inc = {}
        if type(info.incoming) == "table" then
            for _, x in ipairs(info.incoming) do
                if type(x) == "table" and type(x.alt) == "string" then inc[x.alt] = { id = x.id, dry = x.dry == true, back = tonumber(x.back) or 0, backItems = (type(x.backItems) == "table" and #x.backItems > 0) and x.backItems or nil } end
            end
        end
        incoming = inc
        if info.active == true then activeUntil = os.clock() + 60 end
        if type(info.job) == "table" and not busy then task.spawn(runJob, info.job) end
        if type(info.sell) == "table" then
            for _, sj in ipairs(info.sell) do pcall(runSell, sj) end
        end
        if type(info.buy) == "table" then
            local bi = 0
            for _, bj in ipairs(info.buy) do
                if type(bj) == "table" then pcall(runBuy, bj, bi * 0.4); bi = bi + 1 end
            end
        end
    end

    -- moi lan bao cao dinh ky, server gui kem lenh trade trong phan hoi
    local rawPost = post
    post = function(payload)
        local res = rawPost(payload)
        if type(res) == "table" and type(res.Body) == "string" and res.Body ~= "" and not (payload and payload.statusOnly) then
            local okJ, data = pcall(function() return HttpService:JSONDecode(res.Body) end)
            if okJ and type(data) == "table" and type(data.trade) == "table" then pcall(onInfo, data.trade) end
        end
        return res
    end

    -- ===== NHAN LENH TUC THI (long-poll): server giu ket noi va tra loi NGAY khi web bam "xac nhan trade" =====
    local lpAt = -1e9
    if C.TradeLongPoll ~= false then
        task.spawn(function()
            local ver = ""
            while true do
                local data, code = api("GET", "/api/trade/wait?name=" .. HttpService:UrlEncode(player.Name) .. "&since=" .. HttpService:UrlEncode(ver))
                if data then
                    lpAt = os.clock()
                    if data.ver ~= nil then ver = tostring(data.ver) end
                    if type(data.trade) == "table" then pcall(onInfo, data.trade) end
                else
                    ver = ""
                    task.wait(code == 403 and 30 or 2)
                end
            end
        end)
    end

    -- dang co lenh -> hoi nhanh FAST_POLL giay/lan; ranh -> hoi moi IDLE_POLL giay (0 = chi dua vao bao cao dinh ky 60s)
    -- (khi long-poll dang chay tot thi khong can hoi luc ranh)
    task.spawn(function()
        local lastPoll = 0
        while true do
            task.wait(0.25)
            local now = os.clock()
            local gap = now < activeUntil and FAST_POLL or ((IDLE_POLL > 0 and now - lpAt > 25) and IDLE_POLL or nil)
            if gap and now - lastPoll >= gap then
                lastPoll = now
                local data = api("GET", "/api/trade/poll?name=" .. HttpService:UrlEncode(player.Name))
                if data and type(data.trade) == "table" then pcall(onInfo, data.trade) end
            end
        end
    end)
end

`.replace('__TRADE_CATS__', () => tradeCatsLua);
const TRADE_UI_JS = String.raw`(function(){
var LOBBY_PLACE='77649408247578';
function isOn(a){return !!a._t&&Date.now()-a._t<12000}
function where(a){if(!isOn(a))return '';var q=String(a.placeId||'');return !q||q===LOBBY_PLACE?'lobby':'dungeon'}
function placeLabel(a){return !isOn(a)?'Offline':where(a)==='lobby'?'Lobby':'Dungeon'}
function placeColor(a){return !isOn(a)?'#8a919c':where(a)==='lobby'?'#60a5fa':'#a78bfa'}
var TYPES=__TRADE_TYPES__,TJ={},tsel={},bulk=[];
var ST={queued:['Chờ acc nhận lệnh','#fbbf24'],running:['Đang trade','#60a5fa'],done:['Hoàn tất','#4ade80'],failed:['Lỗi','#f87171'],cancelled:['Đã huỷ','#8a919c'],expired:['Hết hạn','#8a919c']};
var SF=['physicalDamage','physicalPower','spellPower','health'];
var tmain=ld('dq_trade_main','')||'',tdry=ld('dq_trade_dry',true)!==false,TLOG=[],seenJ={},seenSt={},badgeUntil={},BADGE_MS=10000,tfr={},tft={},THIST=[],hsSt='',hsBusy=false,hsTimer=null;

var css=document.createElement('style');
css.textContent='.tsel{margin-top:6px;font-size:12px}.tsel label{cursor:pointer;display:inline-flex;gap:4px;align-items:center}'
+'.tbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:12px 0 4px;padding:10px 12px;border:1px solid var(--bd);border-radius:12px;background:var(--cd)}'
+'.tbar input[type=text],.tf input[type=text],.tf input[type=number]{background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:8px 12px;color:var(--tx)}'
+'.tbar label,.tf label{display:inline-flex;gap:6px;align-items:center;cursor:pointer}'
+'.tf{display:flex;flex-direction:column;gap:6px;margin-top:12px;font-size:12px;color:var(--mu)}.tf .rw{margin-top:0}'
+'.tj{display:flex;gap:10px;align-items:center;padding:7px 2px;border-bottom:1px solid var(--bd);flex-wrap:wrap}.tj:last-child{border-bottom:0}'
+'.tst{display:inline-block;padding:1px 9px;border-radius:999px;border:1px solid;font-size:12px;font-weight:600;white-space:nowrap}'
+'.tpbox{border:1px solid var(--bd);border-radius:12px;padding:12px;background:var(--cd)}.tpchips{display:flex;flex-wrap:wrap;gap:6px;max-height:180px;overflow-y:auto;margin-top:10px}'
+'.tchip{padding:3px 11px;border-radius:999px;border:1px solid var(--bd);background:var(--pn);cursor:pointer;font-size:12px;color:var(--tx)}.tchip:hover{border-color:var(--mu)}.tchip.on{border-color:#60a5fa;background:#60a5fa22;color:#60a5fa;font-weight:600}.tchip.mn{border-color:#fbbf24;background:#fbbf2422;color:#fbbf24;font-weight:700;cursor:default}'
+'.tpacc{margin-top:10px;padding:10px 12px;border:1px solid var(--bd);border-radius:12px}.tpacc .ig{margin-top:8px}.tpit{cursor:pointer;position:relative}.tpit.sel{outline:2px solid #4ade80;outline-offset:1px}.tpit .ck2{position:absolute;top:4px;right:6px;color:#4ade80;font-weight:800;display:none}.tpit.sel .ck2{display:block}.tpx{display:inline-flex;align-items:center;gap:6px;padding:2px 9px 2px 3px;border:1px solid;border-radius:8px;font-size:12px;line-height:1.2;white-space:nowrap;filter:saturate(.65) brightness(.9)}.tpx .tim{width:22px;height:22px;flex:none;border-radius:6px;background:var(--bg);display:flex;align-items:center;justify-content:center;overflow:hidden;font-weight:700;font-size:11px;color:var(--mu)}.tpx .tim img{width:100%;height:100%;object-fit:contain}.tpx b{font-weight:600}.tpx.pick{cursor:pointer}.tpx.pick:hover{filter:saturate(.8) brightness(1.12)}.tpx.sel{outline:1px solid #5fae82;outline-offset:1px}.tpx.dis{opacity:.5;cursor:not-allowed}.tpx .ck3{color:#7cc29b;font-weight:800}.tpitems{display:flex;flex-wrap:wrap;gap:6px;padding:2px 0}td.tpcell{white-space:normal}.tpi{display:block;width:100%;text-align:left;background:transparent;border:0;border-radius:8px;padding:6px 10px;cursor:pointer}.tpi:hover{background:var(--cd)}.tpi.mn{color:#fbbf24;font-weight:700}'
+'.tp-row{cursor:pointer;user-select:none;-webkit-user-select:none}.tp-row td{transition:background .18s,box-shadow .18s}.tp-row:hover td{background:var(--cd)}'
+'.tp-row.on td{background:rgba(95,174,130,.06);box-shadow:inset 0 1px 0 rgba(95,174,130,.28),inset 0 -1px 0 rgba(95,174,130,.28)}'
+'.tp-row.on td:first-child{box-shadow:inset 3px 0 0 #5fae82,inset 0 1px 0 rgba(95,174,130,.28),inset 0 -1px 0 rgba(95,174,130,.28)}'
+'.tp-row.on .tpname{color:#7cc29b}.tpsum{margin-top:4px;font-size:12px;color:#7cc29b;font-weight:600}'
+'.tpopen{padding:0 7px;margin-left:6px;font-size:12px;line-height:22px}#tp_all.on{border-color:#4ade80;color:#4ade80;background:rgba(74,222,128,.12);box-shadow:0 0 14px rgba(74,222,128,.35)}'
+'.tcon{margin-top:12px;border:1px solid var(--bd);border-radius:12px;background:var(--cd);overflow:hidden}.tcon-h{display:flex;justify-content:space-between;align-items:center;padding:6px 12px;border-bottom:1px solid var(--bd);font-size:12px;font-weight:600}.tlog{max-height:170px;overflow-y:auto;padding:8px 12px;font:12px/1.55 ui-monospace,Menlo,Consolas,monospace;background:var(--bg)}.tlog div{white-space:pre-wrap;word-break:break-word}'
+'.tlog .tlr{display:flex;flex-wrap:wrap;align-items:center;gap:4px 8px;padding:3px 0;border-bottom:1px dashed var(--bd)}.tlog .tlr:last-child{border-bottom:0}.tlog .tlr .tli{width:20px;text-align:center;flex:none}.tlog .tlr .tmsg{flex:1 1 100%;padding-left:28px;font-size:11px}'
+'.tpmb{display:inline-flex;flex-wrap:wrap;gap:6px;align-items:center}.tpf{padding:4px 12px;border-radius:999px;border-color:var(--c,var(--bd));font-size:12px}.tpf.on{background:var(--c,var(--tx));color:var(--onc,var(--bg));border-color:var(--c,var(--tx));font-weight:700}.tpfil{display:grid;grid-template-columns:minmax(0,3fr) minmax(0,2fr) auto;gap:10px;margin-top:10px}@media(max-width:900px){.tpfil{grid-template-columns:1fr}}.tpfg{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:8px 12px;border:1px solid var(--bd);border-radius:12px;background:var(--cd)}.tpfl{font-size:12px;font-weight:600;color:var(--mu);white-space:nowrap;min-width:58px}'
+'.tcon2{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px;margin-top:12px}.tcon2 .tcon{margin-top:0;display:flex;flex-direction:column}.tcon2 .tlog{flex:1 1 0;min-height:170px;max-height:none}.tcon2 .tpf{padding:2px 9px}@media(max-width:900px){.tcon2{grid-template-columns:1fr}}'
+'td.hswhat{white-space:normal;min-width:220px;max-width:420px}.hsrow td{vertical-align:top}'
+'.statbox{display:inline-flex;align-items:baseline;gap:6px;padding:5px 12px;border-radius:10px;border:1px solid;font-size:12px;font-weight:700;letter-spacing:.4px}.statbox b{font-size:20px;font-weight:800;letter-spacing:0}'
+'.statbox.gold{color:#facc15;border-color:#facc15;background:#facc151f}.statbox.gem{color:#c084fc;border-color:#c084fc;background:#c084fc1f}'
+'#xp_mod{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:1000;align-items:center;justify-content:center;padding:12px}#xp_mod.s{display:flex}'
+'.xpbox{background:var(--cd);border:1px solid var(--bd);border-radius:14px;padding:14px;width:min(470px,96vw);max-height:88vh;overflow:auto;box-shadow:0 10px 40px rgba(0,0,0,.5)}'
+'.xpcur{display:flex;gap:8px;margin:10px 0 6px}.xpcur button{flex:1;padding:7px;border-radius:10px;border:1px solid var(--bd);background:var(--pn);color:var(--tx);cursor:pointer;font-weight:700}'
+'.xpcur button.on.g{border-color:#facc15;color:#facc15;background:#facc151f}.xpcur button.on.m{border-color:#c084fc;color:#c084fc;background:#c084fc1f}'
+'.xpdur{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:8px}.xpdur button{padding:10px 4px;border-radius:10px;font-weight:700;cursor:pointer;line-height:1.3}.xpdur button:disabled{opacity:.45;cursor:not-allowed}'
+'.tbar button:disabled,#tm_go:disabled{opacity:.45;cursor:not-allowed}';
document.head.appendChild(css);

function th(){return {'x-api-key':KEYV,'x-requested-with':'dq','content-type':'application/json'}}
function tapi(path,body){var o={headers:th()};if(body!==undefined){o.method='POST';o.body=JSON.stringify(body)}
  return fetch(path,o).then(function(r){return r.json().then(function(j){j.__s=r.status;return j},function(){return {__s:r.status}})})}
function isAct(j){return !!j&&(j.status==='queued'||j.status==='running')&&Date.now()-(j.updatedAt||0)<6e5}
function sigOf(i){var s=i.s||{};return [i.t,i.n,i.r||'',i.e||''].concat(SF.map(function(k){return Number.isFinite(s[k])?s[k]:''})).join('|')}
function inTrade(){var b=document.querySelector('.tab.on');return !!b&&b.dataset.tab==='trade'}   // chi hien o kho do mo tu tab Trade
function canTrade(i){return !i.eq&&TYPES.indexOf(String(i.t||'').toLowerCase())>=0}
function shown(n){return hideAccounts?'HIDE':n}
function badge(j){var s=ST[j.status]||[j.status,'#8a919c'];return '<span class="tst" style="color:'+s[1]+';border-color:'+s[1]+'">'+esc(s[0])+'</span>'}
function persistMain(v){tmain=v;sv('dq_trade_main',v)}
function persistDry(v){tdry=v;sv('dq_trade_dry',v)}

var LC={failed:'#f87171',err:'#f87171',done:'#4ade80',ok:'#4ade80',running:'#60a5fa',queued:'#fbbf24',cancelled:'#8a919c',expired:'#8a919c',info:'var(--mu)'};
var TIC={ok:'✅',done:'✅',failed:'❌',err:'❌',running:'🔄',queued:'⏳',cancelled:'🚫',expired:'⌛',info:'ℹ️'};
var TPILL={ok:['Thành công','#4ade80'],done:['Hoàn tất','#4ade80'],failed:['Lỗi','#f87171'],err:['Lỗi','#f87171'],running:['Đang trade','#60a5fa'],queued:['Chờ acc nhận lệnh','#fbbf24'],cancelled:['Đã huỷ','#8a919c'],expired:['Hết hạn','#8a919c']};
function tlog(m,kind,ext){
  if(ext&&ext.jid){for(var q=TLOG.length-1;q>=0;q--){var o=TLOG[q][3];if(o&&o.jid===ext.jid){TLOG[q]=[TLOG[q][0],String(m),kind||'info',ext];renderLog();return}}}   // cung 1 lenh -> cap nhat trang thai tai cho, khong them dong moi
  TLOG.push([Date.now(),String(m),kind||'info',ext||null]);if(TLOG.length>200)TLOG.shift();renderLog()}
function tjCount(j){var m=String(j.msg||'').match(/(\d+) item/);if(j.status==='done'&&m)return m[1]+' item';
  if(Array.isArray(j.items)&&j.items.length){var t=j.items.reduce(function(x,i){return x+(+i.q||1)},0);return t+' item'}
  if(+j.sent>0)return (+j.sent)+' item';if(j.mode==='gold')return '';if(+j.max>0)return 'tối đa '+(+j.max)+' item';return ''}
function tlPill(k){var p=TPILL[k];return p?'<span class="tst" style="color:'+p[1]+';border-color:'+p[1]+'">'+esc(p[0])+'</span>':''}
function tlIcon(m,k){if(k==='info'){if(/^Gửi ngay|^Main .* trade cho/.test(m))return '📤';if(/^Bỏ chọn/.test(m))return '⬜';if(/^Chọn/.test(m))return '☑️';if(/^Bỏ/.test(m))return '⏭️'}return TIC[k]||'•'}
function tlRow(x){var d=new Date(x[0]),k=x[2],e=x[3],t='<span class="mu">['+d.toLocaleTimeString('en-GB')+']</span> ';
  if(e&&e.a){return '<div class="tlr">'+t+'<b>'+esc(e.a)+'</b> <span class="mu">→</span> <b>'+esc(e.b)+'</b>'+(e.n?' <span class="mu">('+esc(e.n)+')</span>':'')+' <span class="tli">'+(TIC[e.st]||'•')+'</span>'+tlPill(e.st)+(e.msg&&!(e.st==='done'&&/^Đã gửi \d+ item$/.test(e.msg))?'<span class="mu tmsg">'+esc(e.msg)+'</span>':'')+'</div>'}
  return '<div class="tlr">'+t+'<span class="tli">'+tlIcon(x[1],k)+'</span><span style="color:'+(k==='info'?'var(--tx)':(LC[k]||'var(--tx)'))+'">'+esc(x[1])+'</span> '+(k==='info'?'':tlPill(k))+'</div>'}
function renderLog(){
  var el=$('tp_log');if(!el)return;
  var near=el.scrollHeight-el.scrollTop-el.clientHeight<40;
  var L=TLOG.filter(function(x){return x[3]||x[2]==='failed'||x[2]==='err'});
  el.innerHTML=L.length?L.map(tlRow).join(''):'<div class="mu">Chưa có lệnh trade. Chọn acc rồi bấm Xác nhận trade để xem tại đây.</div>';
  if(near)el.scrollTop=el.scrollHeight}
function trackJobs(){
  Object.keys(TJ).forEach(function(k){var j=TJ[k];if(!j)return;var sg=(j.id||'')+'|'+j.status+'|'+(j.msg||'');
    if(seenJ[k]===sg)return;
    var fresh=seenJ[k]!==undefined||Date.now()-(j.updatedAt||0)<6e4;seenJ[k]=sg;
    var ss=(j.id||'')+'|'+j.status;
    if(seenSt[k]!==ss){seenSt[k]=ss;if(['done','failed','cancelled','expired'].indexOf(j.status)>=0)histSoon();if(fresh){badgeUntil[k]=Date.now()+BADGE_MS;setTimeout(function(){try{syncTp()}catch(x){}},BADGE_MS+100)}}
    if(fresh){var st=ST[j.status]||[j.status];tlog(shown(k)+' → '+shown(j.main)+' · '+st[0]+(j.msg?' · '+j.msg:''),j.status,{jid:String(j.id||k)+'|'+k,a:shown(k),b:shown(j.main),n:tjCount(j),st:j.status,msg:j.msg||''})}})}

// ---- gui lenh len server ----
function sendJob(body,done){
  return tapi('/api/trade/create',body).then(function(j){
    if(j.__s===401){toast('Chưa đăng nhập hoặc sai key');return}
    if(!j.ok){var m=j.message||j.error||'Lỗi';toast(m);tlog('Lỗi gửi lệnh: '+m,'failed');var e=$('tm_msg');if(e)e.textContent=m;return}
    var MSG={busy:'đang có lệnh khác',not_found:'không thấy acc',same_as_main:'trùng acc chính',sender_receiving:'acc gửi đang nhận đồ từ acc khác',receiver_sending:'acc nhận đang gửi đồ cho acc khác'};
    var okN=j.results.filter(function(r){return r.ok}).length,bad=j.results.filter(function(r){return !r.ok});
    toast('Đã gửi lệnh cho '+okN+' acc'+(bad.length?' · lỗi: '+bad.map(function(b){return shown(b.alt)+' ('+(MSG[b.error]||b.error)+')'}).join(', '):''));
    tlog('Server nhận lệnh cho '+okN+' acc'+(bad.length?' · lỗi: '+bad.map(function(b){return shown(b.alt)+' ('+(MSG[b.error]||b.error)+')'}).join(', '):''),bad.length?'failed':'ok');
    if(okN&&done)done();
    refreshJobs();
  }).catch(function(){toast('Lỗi kết nối');tlog('Lỗi kết nối tới server','failed')});
}
var GTOT=0,GDAY=864e5,GEV=(function(){var a=ld('dq_gold_ev',[]);return Array.isArray(a)?a.filter(function(e){return Array.isArray(e)&&e.length===3&&e[1]>Date.now()-GDAY}):[]})(),GSEEN=(function(){var o={},a=ld('dq_gold_seen2',[]);if(Array.isArray(a))a.forEach(function(k){o[k]=1});return o})();
function goldLocal(){var c=Date.now()-GDAY,t=0;GEV.forEach(function(e){if(e[1]>c)t+=e[2]});return t}
function goldShown(){return Math.max(goldLocal(),GTOT)}
function goldBtn(){var b=$('tp_goldtot');if(!b)return;var g=goldShown();b.textContent='💰 Tổng gold đã trade: '+sh(g);b.title='Tổng gold đã trade vào main: '+fmt(g)+' (bấm để xem số đầy đủ)'}
// cong gold tu dong "Da gui ... N gold" cua cac lenh trade hoan tat (moi lenh chi cong 1 lan, nho theo id)
function goldCount(x){if(!x||!x.id||x.status!=='done'||x.dry||x.ret||GSEEN[x.id])return false;
  var g=+x.goldSent>0?+x.goldSent:0;if(!g){var m=String(x.msg||'').match(/(\d{1,16}) gold/);if(m)g=+m[1]}
  var ts=+x.finishedAt||+x.updatedAt||Date.now();GSEEN[x.id]=1;
  if(g>0&&ts>Date.now()-GDAY)GEV.push([x.id,ts,g]);return true}
function goldScan(list){var ch=false;(list||[]).forEach(function(x){if(goldCount(x))ch=true});
  if(ch){var c=Date.now()-GDAY;GEV=GEV.filter(function(e){return e[1]>c});sv('dq_gold_ev',GEV);sv('dq_gold_seen2',Object.keys(GSEEN).slice(-1000));goldBtn()}}
setInterval(function(){try{goldBtn()}catch(x){}},60000);   // het 24h thi tu giam khoi tong
function refreshJobs(){return tapi('/api/trade/list').then(function(j){if(j&&j.goldTotal!==undefined){GTOT=+j.goldTotal||0;goldBtn()}if(j&&j.jobs){TJ=j.jobs;try{goldScan(Object.keys(TJ).map(function(k){return TJ[k]}))}catch(x){}try{trackJobs()}catch(x){}panel();bar();try{syncTp()}catch(x){}}}).catch(function(){})}

// ---- bang lenh trade ----
var pnl=document.createElement('div');pnl.id='tjobs';pnl.className='pn';pnl.style.cssText='margin-top:12px;display:none';
pnl.className='';pnl.style.cssText='display:block';
$('tabs').style.display='';$('tf').classList.add('joined');
$('tabs').insertAdjacentHTML('beforeend','<button class="tab" data-tab="trade" type="button">Trade</button>');
var tpane=document.createElement('div');tpane.className='tabpane';tpane.dataset.pane='trade';tpane.id='pane-trade';tpane.style.display='none';
tpane.innerHTML='<div>'
 +'<div class="rw" style="margin:0 0 12px;gap:10px;display:none"><label for="tp_maininp" style="font-weight:700;white-space:nowrap">👑 Main account</label><input id="tp_maininp" type="text" list="tp_dl" placeholder="Nhập tên acc Main nhận item" autocomplete="off" spellcheck="false" style="flex:1;min-width:220px;max-width:360px;background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:8px 12px;color:var(--tx)"><datalist id="tp_dl"></datalist><span id="tp_mainhint" class="mu" style="font-size:12px"></span></div>'
 +'<div class="rw" style="margin:0;justify-content:space-between;align-items:center"><div class="rw" style="margin:0"><button class="bt on" id="tp_igo" type="button" disabled>✅ Xác nhận trade (0)</button><div class="wr"><button class="bt" id="tp_goldbtn" type="button" data-p="tp_goldpop" title="Chọn acc alt sẽ trade HẾT gold về main">💰 All gold (0) ▾</button><div class="pop" id="tp_goldpop" style="max-height:300px;overflow:auto;min-width:320px"></div></div><button class="bt" id="tp_goldtot" type="button" title="Tổng gold đã trade vào main (bấm để xem số đầy đủ)">💰 Tổng gold đã trade: 0</button><button class="bt" id="tp_heq" type="button" title="Ẩn các item đang mang (⚔) khỏi danh sách trade">Ẩn đồ đang mang</button><span class="mu" id="tp_cnt" style="font-size:12px"></span></div><div class="wr"><button class="bt" id="tp_mainbtn" type="button" data-p="tp_pop">👑 Main Account ▾</button><div class="pop" id="tp_pop" style="left:auto;right:0;max-height:260px;overflow:auto;min-width:220px"></div></div></div>'
 +'<div class="tpfil"><div class="tpfg"><span class="tpfl">Độ hiếm</span><span class="tpmb" id="tp_r"><button type="button" class="bt tpf on" data-v="">Tất cả</button><button type="button" class="bt tpf" data-v="6" style="--c:#f43f5e;--onc:#fff">Ult</button><button type="button" class="bt tpf" data-v="5" style="--c:#f59e0b;--onc:#fff">Legend</button><button type="button" class="bt tpf" data-v="4" style="--c:#a78bfa;--onc:#fff">Epic</button><button type="button" class="bt tpf" data-v="3" style="--c:#60a5fa;--onc:#fff">Rare</button><button type="button" class="bt tpf" data-v="2" style="--c:#4ade80;--onc:#fff">Uncommon</button><button type="button" class="bt tpf" data-v="1" style="--c:#9ca3af;--onc:#fff">Common</button><button type="button" class="bt tpf" data-v="0" style="--c:#8a919c;--onc:#fff">Chưa rõ</button></span></div><div class="tpfg"><span class="tpfl">Loại item</span><span class="tpmb" id="tp_t"><button type="button" class="bt tpf on" data-v="">Tất cả</button><button type="button" class="bt tpf" data-v="weapon">WP</button><button type="button" class="bt tpf" data-v="chest">Áo</button><button type="button" class="bt tpf" data-v="helmet">Mũ</button><button type="button" class="bt tpf" data-v="skill">Skill</button></span></div><div class="tpfg"><span class="tpfl">REQ LV</span><div class="wr"><button class="bt" id="tp_lvbtn" type="button" data-p="tp_lvpop">⏷ REQ LV</button><div class="pop" id="tp_lvpop" style="left:auto;right:0;min-width:240px"><label style="display:flex;align-items:center;justify-content:space-between;gap:10px">REQ LV từ <input id="tp_la" type="number" min="0"></label><label style="display:flex;align-items:center;justify-content:space-between;gap:10px">REQ LV đến <input id="tp_lb" type="number" min="0"></label></div></div></div></div>'
 +'<div class="tcon2"><div class="tcon"><div class="tcon-h"><span>🖥 Console trade</span><button class="bt" id="tp_clr" type="button" style="padding:2px 10px;font-size:12px">Xoá log</button></div><div class="tlog" id="tp_log"></div></div><div class="tcon" id="tp_mainbox"><div class="tcon-h"><span id="tp_mainttl">👑 Main inventory</span><span style="display:inline-flex;align-items:center;gap:8px"><button class="bt" id="tp_ret" type="button" disabled style="padding:2px 10px;font-size:12px">📤 Trade cho alt (0)</button><span class="mu" id="tp_maincnt" style="font-weight:400;font-size:12px"></span></span></div><div style="padding:8px 12px;border-bottom:1px solid var(--bd);display:flex;flex-direction:column;gap:6px"><div id="mi_r" class="tpmb" style="display:flex"><span style="font-size:12px;font-weight:600;color:var(--mu);min-width:52px">Độ hiếm</span><button type="button" class="bt tpf on" data-v="">Tất cả</button><button type="button" class="bt tpf" data-v="6" style="--c:#f43f5e;--onc:#fff">Ult</button><button type="button" class="bt tpf" data-v="5" style="--c:#f59e0b;--onc:#fff">Legend</button><button type="button" class="bt tpf" data-v="4" style="--c:#a78bfa;--onc:#fff">Epic</button><button type="button" class="bt tpf" data-v="3" style="--c:#60a5fa;--onc:#fff">Rare</button><button type="button" class="bt tpf" data-v="2" style="--c:#4ade80;--onc:#fff">Uncommon</button><button type="button" class="bt tpf" data-v="1" style="--c:#9ca3af;--onc:#fff">Common</button><button type="button" class="bt tpf" data-v="0" style="--c:#8a919c;--onc:#fff">Chưa rõ</button></div><div id="mi_t" class="tpmb" style="display:flex"><span style="font-size:12px;font-weight:600;color:var(--mu);min-width:52px">Loại</span><button type="button" class="bt tpf on" data-v="">Tất cả</button><button type="button" class="bt tpf" data-v="weapon">WP</button><button type="button" class="bt tpf" data-v="chest">Áo</button><button type="button" class="bt tpf" data-v="helmet">Mũ</button><button type="button" class="bt tpf" data-v="skill">Skill</button></div></div><div class="tlog" id="tp_maininv"></div></div></div>'
 +'<div class="tw" style="margin-top:12px"><table style="min-width:0"><thead><tr><th>Account (clone/alt)<input id="tp_q" type="text" placeholder="🔍 Find account..." autocomplete="off" spellcheck="false" style="display:block;width:100%;min-width:170px;box-sizing:border-box;margin-top:6px;background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:7px 12px;color:var(--tx);font-weight:400"></th><th>Status</th><th>Item (tất cả item trong account)</th></tr></thead><tbody id="tp_tb"></tbody></table><div class="empty" id="tp_em" style="display:none">Không có account nào</div></div>'
 +'<div class="tcon" id="hs_box" style="margin-top:12px"><div class="tcon-h"><span>📜 Lịch sử trade <span class="mu" id="hs_cnt" style="font-weight:400"></span></span><span class="rw" style="margin:0;gap:6px"><select id="hs_f" style="padding:2px 8px;font-size:12px"><option value="">Tất cả trạng thái</option><option value="done">Hoàn tất</option><option value="failed">Lỗi</option><option value="cancelled">Đã huỷ</option><option value="expired">Hết hạn</option></select><button class="bt" id="hs_rf" type="button" style="padding:2px 10px;font-size:12px">⟳</button><button class="bt dg" id="hs_clr" type="button" style="padding:2px 10px;font-size:12px">Xoá lịch sử</button></span></div><div class="tw" style="margin:0;border:0;border-radius:0"><table style="min-width:640px"><thead><tr><th>Thời gian</th><th>Gửi → Nhận</th><th>Nội dung</th><th>Trạng thái</th><th>Ghi chú</th></tr></thead><tbody id="hs_tb"></tbody></table><div class="empty" id="hs_em" style="display:none">Chưa có lịch sử trade</div></div></div>'
 +'</div>';
$('tf').appendChild(tpane);

// ---- TAB BUYER/SELL: de trong, dung #bs_root de tu build giao dien ----
$('tabs').insertAdjacentHTML('beforeend','<button class="tab" data-tab="buysell" type="button">Sell</button>');
var bspane=document.createElement('div');bspane.className='tabpane';bspane.dataset.pane='buysell';bspane.id='pane-buysell';bspane.style.display='none';
bspane.innerHTML='<div id="bs_root" style="min-height:200px"></div>';
$('tf').appendChild(bspane);

// ---- TAB BUY: de trong, dung #buy_root de tu build giao dien ----
$('tabs').insertAdjacentHTML('beforeend','<button class="tab" data-tab="buy" type="button">Buy</button>');
var buypane=document.createElement('div');buypane.className='tabpane';buypane.dataset.pane='buy';buypane.id='pane-buy';buypane.style.display='none';
buypane.innerHTML='<div id="buy_root" style="min-height:200px"></div>';
$('tf').appendChild(buypane);

pnl.onclick=function(e){var b=e.target.closest('[data-cancel]');if(!b)return;
  tapi('/api/trade/cancel',{alts:[b.dataset.cancel]}).then(function(){toast('Đã huỷ lệnh');refreshJobs()})};
function panel(){
  var arr=Object.keys(TJ).map(function(k){return TJ[k]}).filter(Boolean).sort(function(a,b){return (b.updatedAt||0)-(a.updatedAt||0)}).slice(0,20);
  pnl.style.display='block';
  if(!arr.length){pnl.innerHTML='<div class="empty" style="padding:24px">Chưa có lệnh trade nào</div>';return}
  pnl.innerHTML=''
  +arr.map(function(j){
    var what=j.mode==='gold'?('chỉ gửi gold'):j.mode==='items'?((j.items||[]).reduce(function(s,x){return s+(+x.q||1)},0)+' item đã chọn'):('độ hiếm '+(j.rarity||[]).join('/')+(j.names&&j.names.length?' · '+esc(j.names.join(', ')):'')+' · tối đa '+j.max);
    if(j.goldAll)what+=(j.mode==='gold'?' ':' · +')+'toàn bộ gold';else if(+j.gold>0)what+=(j.mode==='gold'?' ':' · +')+(+j.gold).toLocaleString('en-US')+' gold';
    return '<div class="tj"><b>'+esc(shown(j.alt))+'</b><span class="mu">→ '+esc(shown(j.main))+'</span><span class="mu">'+what+(j.ret?' · ↩ Trả đồ':'')+(j.dry?' · DRY':'')+'</span>'+badge(j)
      +'<span class="mu" style="flex:1;min-width:140px">'+esc(j.msg||'')+'</span>'
      +(isAct(j)?'<button class="bt" data-cancel="'+esc(j.alt)+'">Huỷ</button>':'')+'</div>'}).join('');
}

// ---- thanh trade trong cua so account (chon item bang checkbox) ----
function findItem(acc,key){var a=data.find(function(x){return x.name===acc});if(!a)return null;
  var l=inv(a);for(var n=0;n<l.length;n++)if(acc+'|'+sigOf(l[n])===key)return l[n];return null}
function clampQ(v,max){v=Math.floor(+v);return Number.isFinite(v)?Math.min(max,Math.max(1,v)):1}
var barEl=null;
function ensureBar(){
  if(barEl&&barEl.isConnected)return;
  barEl=document.createElement('div');barEl.id='tbar';barEl.className='tbar';
  barEl.innerHTML='<b id="tcnt"></b><input id="tmain" type="text" list="tdl" placeholder="Tên acc chính (main)" autocomplete="off" spellcheck="false" style="min-width:150px">'
   +'<label title="Chỉ thêm 1 item vào cửa sổ trade, không bấm xác nhận"><input type="checkbox" id="tdry"> Chạy thử (DRY)</label>'
   +'<label title="Tick: tự động trade HẾT gold trong tài khoản cùng với item" style="display:inline-flex;align-items:center;gap:6px;cursor:pointer"><input type="checkbox" id="tgoldall"> All gold</label>'
   +'<button class="bt" id="tgo" type="button">🔁 Trade item đã chọn</button><button class="bt" id="trar" type="button">Trade theo độ hiếm…</button>'
   +'<span id="tst" class="mu" style="flex-basis:100%"></span>';
  $('ab').parentNode.insertBefore(barEl,$('ab'));
  $('tmain').oninput=function(e){persistMain(e.target.value.trim())};
  $('tdry').onchange=function(e){persistDry(e.target.checked)};
  $('trar').onclick=function(){openBulk([mAcc])};
  $('tgo').onclick=function(){
    var main=$('tmain').value.trim(),picks=Object.keys(tsel).filter(function(k){return tsel[k].acc===mAcc}).map(function(k){return tsel[k]});
    if(!main){toast('Nhập tên acc chính');return}
    if(!picks.length){toast('Chưa chọn item nào');return}
    var gAll=$('tgoldall').checked;
    persistMain(main);
    sendJob({alts:[mAcc],main:main,mode:'items',dry:$('tdry').checked,goldAll:gAll,items:picks.map(function(p){var it=p.item;return {n:it.n,t:it.t,r:it.r||'',e:it.e||'',s:it.s||{},q:p.q}})},
      function(){Object.keys(tsel).forEach(function(k){if(tsel[k].acc===mAcc)delete tsel[k]});renderModal()});
  };
}
function bar(){
  if(!barEl||!barEl.isConnected)return;
  var picks=Object.keys(tsel).filter(function(k){return tsel[k].acc===mAcc}),n=picks.reduce(function(s,k){return s+tsel[k].q},0),j=TJ[mAcc];
  $('tcnt').textContent='Đã chọn '+n+' item';
  if(document.activeElement!==$('tmain'))$('tmain').value=tmain;
  $('tdry').checked=tdry;
  $('tgo').disabled=!n||isAct(j);
  $('tst').innerHTML=j?(badge(j)+' <span class="mu">→ '+esc(shown(j.main))+' · '+esc(j.msg||'')+'</span>'):'<span class="mu">Tick “Trade” ở từng item (loại được phép: '+esc(TYPES.join(', '))+'; item đang mang không trade được).</span>';
}
$('ab').addEventListener('change',function(e){
  var t=e.target,k=t.dataset&&t.dataset.k;if(!k)return;
  if(t.classList.contains('tcb')){
    if(t.checked){var it=findItem(mAcc,k);if(!it){t.checked=false;return}
      var qn=t.closest('.tsel').querySelector('.tqn');tsel[k]={item:it,acc:mAcc,q:qn?clampQ(qn.value,+it.q||1):1}}
    else delete tsel[k];
  }else if(t.classList.contains('tqn')){var s=tsel[k];if(s){s.q=clampQ(t.value,+t.max||1);t.value=s.q}}
  bar();
});

// ---- cua so trade theo do hiem (nhieu acc cung luc) ----
var dl=document.createElement('datalist');dl.id='tdl';document.body.appendChild(dl);
var tm=document.createElement('div');tm.className='ov';tm.id='tm';
tm.innerHTML='<div class="am" style="width:min(560px,100%)"><div class="rw" style="margin-top:0;justify-content:space-between"><b style="font-size:17px">🔁 Trade về acc chính</b><button class="bt" id="tmx" type="button">✕</button></div>'
 +'<div class="mu" id="tm_alts" style="margin-top:8px"></div>'
 +'<label class="tf">Acc chính (main) — phải cùng server và đang chạy script tracker<input id="tm_main" type="text" list="tdl" autocomplete="off" spellcheck="false"></label>'
 +'<div class="tf">Độ hiếm cần gửi<div class="rw"><label><input type="checkbox" class="tm_r" value="ult" checked> Ult</label><label><input type="checkbox" class="tm_r" value="legend" checked> Legend</label><label><input type="checkbox" class="tm_r" value="epic"> Epic</label></div></div>'
 +'<label class="tf">Chỉ gửi các tên này (tuỳ chọn, cách nhau bằng dấu phẩy; để trống = mọi item đúng độ hiếm)<input id="tm_names" type="text" placeholder="Mjolnir, Gungnir"></label>'
 +'<label class="tf">Tối đa item mỗi acc<input id="tm_max" type="number" min="1" max="999" value="5"></label>'
 +'<label class="tf">Gold gửi mỗi acc (tuỳ chọn, ví dụ 500m, 1b, 1.5t)<input id="tm_gold" type="text" inputmode="decimal" autocomplete="off" spellcheck="false" placeholder="Để trống = không gửi gold"></label>'
 +'<div class="tf"><label><input type="checkbox" id="tm_goldonly"> Chỉ gửi gold (bỏ qua item)</label></div>'
 +'<div class="tf"><label><input type="checkbox" id="tm_dry"> Chạy thử (DRY): chỉ thêm 1 item, KHÔNG xác nhận</label></div>'
 +'<div class="mu" id="tm_msg" style="margin-top:10px;min-height:16px"></div>'
 +'<div class="rw" style="justify-content:flex-end"><button class="bt on" id="tm_go" type="button">Gửi lệnh</button></div></div>';
document.body.appendChild(tm);
function warnMain(){
  var m=$('tm_main').value.trim().toLowerCase(),known=data.some(function(a){return a.name.toLowerCase()===m});
  $('tm_msg').textContent=(m&&!known)?'Chưa thấy acc này trong tracker. Main cần chạy script tracker thì mới tự nhận trade.':'';
}
function openBulk(names){
  bulk=names.slice();
  $('tm_alts').textContent=bulk.length+' acc gửi: '+(hideAccounts?'HIDE':bulk.slice(0,8).join(', ')+(bulk.length>8?'…':''));
  $('tm_main').value=tmain;$('tm_dry').checked=tdry;warnMain();tm.classList.add('s');
}
$('tm_main').oninput=warnMain;
$('tmx').onclick=function(){tm.classList.remove('s')};
tm.onclick=function(e){if(e.target===tm)tm.classList.remove('s')};
document.addEventListener('keydown',function(e){if(e.key==='Escape')tm.classList.remove('s')});
function tmGold(v){v=String(v||'').trim().toLowerCase().replace(/,/g,'');if(!v)return 0;var m=v.match(/^(\d+(?:\.\d+)?)\s*([kmbt]?)$/);if(!m)return null;var f={k:1e3,m:1e6,b:1e9,t:1e12}[m[2]]||1,n=Math.floor(Number(m[1])*f);return isFinite(n)&&n>=0&&n<=1e15?n:null}
$('tm_go').onclick=function(){
  var main=$('tm_main').value.trim(),r=[].slice.call(document.querySelectorAll('.tm_r')).filter(function(x){return x.checked}).map(function(x){return x.value});
  if(!main){$('tm_msg').textContent='Nhập tên acc chính.';return}
  var gold=tmGold($('tm_gold').value),gOnly=$('tm_goldonly').checked;
  if(gold===null){$('tm_msg').textContent='Gold không hợp lệ. Ví dụ: 500m, 1b, 1.5t.';return}
  if(gOnly&&!gold){$('tm_msg').textContent='Nhập số gold cần gửi.';return}
  if(!gOnly&&!r.length){$('tm_msg').textContent='Chọn ít nhất một độ hiếm.';return}
  persistMain(main);persistDry($('tm_dry').checked);
  var names=$('tm_names').value.split(',').map(function(x){return x.trim()}).filter(Boolean);
  $('tm_go').disabled=true;
  sendJob({alts:bulk,main:main,mode:gOnly?'gold':'rarity',rarity:r,names:names,max:+$('tm_max').value||5,gold:gold,dry:tdry},function(){tm.classList.remove('s')}).then(function(){$('tm_go').disabled=false});
};

// nut gui hang loat o thanh cong cu (dung o checkbox chon acc co san)
var tpSig='';
function tpAcc(){return data.map(function(a){return a.name}).sort(function(x,y){return x.localeCompare(y)})}
function tpName(n){var a=data.find(function(x){return x.name===n});return a?displayName(a):n}
function tpOk(i){return true}   // hien TAT CA item trong account (item khong trade duoc se bi lam mo)
function tpTop(i){var m=i.m||{},st=i.s||{},best=null;[['Phys','physicalDamage'],['Phys','physicalPower'],['Spell','spellPower']].forEach(function(c){var v=Number.isFinite(m[c[1]])?m[c[1]]:Number.isFinite(st[c[1]])?st[c[1]]:null;if(v!==null&&(!best||v>best.v))best={l:c[0],v:v}});return best}
function tpChip(i,acc){
  var t=tier(i),c=cc(i)||(TIER[t]||{}).c||'#8a919c',u=IMG[ik(i)],L=esc(String(i.n||'?').slice(0,1).toUpperCase()),e=enchantCode(i),top=tpTop(i),q=+i.q||1,
      ok=canTrade(i),key=acc+'|'+sigOf(i),pk=ok?tsel[key]:null,
      img=/^https:\/\/[^\s"'<>]+$/.test(u||'')?'<img src="'+esc(u)+'" alt="'+L+'" loading="lazy" referrerpolicy="no-referrer">':L;
  return '<span class="tpx '+(ok?'pick':'dis')+(pk?' sel':'')+'"'+(ok?' data-k="'+esc(key)+'" data-a="'+esc(acc)+'"':'')+' style="border-color:'+c+';background:'+c+'22" title="'+esc(i.n)+' · '+esc((TIER[t]||{}).n||'')+(top?' · '+top.l+' '+fmt(top.v):'')+(ok?(q>1?' · bấm nhiều lần để tăng số lượng':' · bấm để chọn trade'):' · không trade được (đang mang / loại không hỗ trợ)')+'"><span class="tim">'+img+'</span><b>'+esc(i.n)+'</b>'
    +(top?'<span class="mu">'+top.l+' <b style="color:var(--tx)">'+sh(top.v)+'</b></span>':'')
    +(e?'<span class="enchant-badge '+(e==='EIR'?'eir':'eif')+'">'+e+'</span>':'')
    +(i.eq?'<span class="mu" title="Đang mang" style="white-space:nowrap">⚔ Đang mang</span>':'')
    +(pk?'<span class="ck3">✔'+(q>1?' '+pk.q+'/'+q:'')+'</span>':(q>1?'<span class="mu">×'+fmt(q)+'</span>':''))+'</span>'}
var mfr={},mft={},tpHideEq=ld('dq_trade_hide_eq',false)===true,tpLv={a:'',b:''};   // an item dang mang (tab Trade)
function mPass(i){return (!tpHideEq||!i.eq)&&(!tpHas(mfr)||mfr[tier(i)])&&(!tpHas(mft)||mft[tpGroup(i)])}
function mBtns(){[['mi_r',mfr],['mi_t',mft]].forEach(function(x){[].slice.call($(x[0]).querySelectorAll('[data-v]')).forEach(function(b){b.classList.toggle('on',b.dataset.v===''?!tpHas(x[1]):!!x[1][b.dataset.v])})})}
function mToggle(set,e){var b=e.target.closest('[data-v]');if(!b)return;var v=b.dataset.v;
  if(v==='')Object.keys(set).forEach(function(k){delete set[k]});else if(set[v])delete set[v];else set[v]=1;
  mBtns();syncTp();bar()}
function mainChip(i,acc){
  var ok=canTrade(i)&&!!acc,key=acc+'|'+sigOf(i),pk=ok?tsel[key]:null,t=tier(i),c=cc(i)||(TIER[t]||{}).c||'#8a919c',u=IMG[ik(i)],L=esc(String(i.n||'?').slice(0,1).toUpperCase()),e=enchantCode(i),top=tpTop(i),q=+i.q||1,
      img=/^https:\/\/[^\s"'<>]+$/.test(u||'')?'<img src="'+esc(u)+'" alt="'+L+'" loading="lazy" referrerpolicy="no-referrer">':L;
  return '<span class="tpx '+(ok?'pick':'dis')+(pk?' sel':'')+'"'+(ok?' data-k="'+esc(key)+'" data-a="'+esc(acc)+'"':'')+' style="border-color:'+c+';background:'+c+'22" title="'+esc(i.n)+' · '+esc((TIER[t]||{}).n||'')+(top?' · '+top.l+' '+fmt(top.v):'')+'"><span class="tim">'+img+'</span><b>'+esc(i.n)+'</b>'
    +(top?'<span class="mu">'+top.l+' <b style="color:var(--tx)">'+sh(top.v)+'</b></span>':'')
    +(e?'<span class="enchant-badge '+(e==='EIR'?'eir':'eif')+'">'+e+'</span>':'')
    +(i.eq?'<span class="mu" title="Đang mang" style="white-space:nowrap">⚔ Đang mang</span>':'')
    +(pk?'<span class="ck3">✔'+(q>1?' '+pk.q+'/'+q:'')+'</span>':(q>1?'<span class="mu">×'+fmt(q)+'</span>':''))+'</span>'}
function mainSelCount(){var m=(tmain||'').toLowerCase();return m?Object.keys(tsel).reduce(function(t,k){return t+(tsel[k].acc.toLowerCase()===m?tsel[k].q:0)},0):0}
function dropMainSel(){var m=(tmain||'').toLowerCase();Object.keys(tsel).forEach(function(k){if(tsel[k].acc.toLowerCase()===m)delete tsel[k]})}
function mainInv(){
  var box=$('tp_maininv');if(!box)return;
  var ml=(tmain||'').toLowerCase(),a=ml?data.find(function(x){return x.name.toLowerCase()===ml}):null,html,ttl,cnt='',its=[];
  if(!tmain){ttl='👑 Main inventory';html='<div class="mu">Chọn Main Account (nút bên phải) để xem inventory của main.</div>'}
  else if(!a){ttl='👑 Main inventory';html='<div class="mu">Main chưa có trong tracker nên chưa có inventory.</div>'}
  else{
    ttl='👑 '+tpName(a.name);
    its=inv(a).filter(mPass).sort(function(x,y){return tier(y)-tier(x)||dstat(y)-dstat(x)||String(x.n).localeCompare(String(y.n))});
    cnt=fmt(its.reduce(function(s,i){return s+(+i.q||1)},0))+' item'+(mainSelCount()?' · ✔ chọn '+mainSelCount()+' để trả':'');
    html=its.length?'<div class="tpitems" style="white-space:normal">'+its.map(function(i){return mainChip(i,a.name)}).join('')+'</div>':'<div class="mu">Main không có item nào (hoặc bị bộ lọc ẩn).</div>';
  }
  $('tp_mainttl').textContent=ttl;$('tp_maincnt').textContent=cnt;
  if(box.dataset.h!==html){box.dataset.h=html;box.innerHTML=html}
  if(its.length){var n1=its.filter(function(i){return IMG[ik(i)]}).length;
    loadImgs(its).then(function(){var n2=its.filter(function(i){return IMG[ik(i)]}).length;if(n2!==n1)mainInv()})}
}
function syncTp(force){
  var mi=$('tp_maininp');
  if(mi){if(document.activeElement!==mi)mi.value=tmain;mi.type=hideAccounts?'password':'text';
    $('tp_dl').innerHTML=hideAccounts?'':tpAcc().map(function(n){return '<option value="'+esc(n)+'"></option>'}).join('');
    try{mainHint()}catch(x){}}
  var q='',names=tpAcc(),sig=JSON.stringify([names,Array.from(sel),tmain,q,hideAccounts]);
  if(force||sig!==tpSig){
    tpSig=sig;var ml=tmain.toLowerCase();
    var picked=Array.from(sel).filter(function(n){return n.toLowerCase()!==ml});
    $('tp_pop').innerHTML=names.map(function(n){var isM=n.toLowerCase()===ml;return '<button type="button" class="tpi'+(isM?' mn':'')+'" data-m="'+esc(n)+'">'+(isM?'👑 ':'')+esc(tpName(n))+'</button>'}).join('')||'<div class="mu" style="padding:6px 10px">Chưa có account</div>';
    $('tp_mainbtn').textContent='👑 '+(tmain?'Main: '+shown(tmain):'Main Account')+' ▾';
  }
  var ml2=tmain.toLowerCase(),rowsA=tpRows(),nM=mainSelCount();
  var allShown=[];
  $('tp_tb').innerHTML=rowsA.map(function(a){
    var its=tpItems(a),
        shownI=its,nA=tpAccTotal(a.name);allShown=allShown.concat(shownI);
    return '<tr class="tp-row'+(nA?' on':'')+'" data-n="'+esc(a.name)+'"><td><b class="tpname">'+esc(displayName(a))+'</b>'+(nA?'<div class="tpsum">✔ Sẽ trade '+nA+' item</div>':'')+(TJ[a.name]&&(isAct(TJ[a.name])||(badgeUntil[a.name]||0)>Date.now())?'<div style="margin-top:4px" title="'+esc(TJ[a.name].msg||'')+'">'+badge(TJ[a.name])+'</div>':'')+'</td><td><span class="bg on" style="color:'+placeColor(a)+';border-color:'+placeColor(a)+'">'+placeLabel(a)+'</span></td><td class="tpcell"><div class="tpitems">'
      +(shownI.map(function(i){return tpChip(i,a.name)}).join('')||'<span class="mu">—</span>')+'</div></td></tr>'}).join('');
  var nImg=allShown.filter(function(i){return IMG[ik(i)]}).length;
  loadImgs(allShown).then(function(){var n2=allShown.filter(function(i){return IMG[ik(i)]}).length;if(n2!==nImg)syncTp()});
  var lvOn=tpLv.a!==''||tpLv.b!=='',noReq=lvOn&&!data.some(function(a){return inv(a).some(function(i){return tpReq(i)!==null})});
  $('tp_em').textContent=!rowsA.length&&noReq?'Chưa có dữ liệu REQ LV của item. Hãy chạy lại script Lua mới trên các acc rồi chờ tracker cập nhật.':!rowsA.length&&lvOn?'Không có item nào trong khoảng REQ LV này':'Không có account nào';
  $('tp_em').style.display=rowsA.length?'none':'block';
  var shownN={},hidN={};rowsA.forEach(function(a){shownN[a.name]=1});
  var nPick=Object.keys(tsel).reduce(function(t,k){var o=tsel[k].acc;if(o.toLowerCase()===ml2)return t;if(!shownN[o])hidN[o]=1;return t+tsel[k].q},0),nHid=Object.keys(hidN).length;
  $('tp_cnt').textContent=(nPick?'Đã chọn '+nPick+' item'+(nHid?' ('+nHid+' acc đang bị ẩn bởi bộ lọc/tìm kiếm)':''):'Bấm vào item để chọn trade');
  $('tp_igo').disabled=!nPick&&!gPicked().length;$('tp_igo').textContent='✅ Xác nhận trade ('+nPick+')';
  try{retBtn()}catch(x){}
  try{gUpd()}catch(x){}
  try{mainInv()}catch(x){}
  
}
function tpRows(){
  var ml=tmain.toLowerCase(),tq=($('tp_q')?$('tp_q').value:'').trim().toLowerCase();
  return data.filter(function(a){
    if(a.name.toLowerCase()===ml)return false;
    if(!isOn(a))return false;
    if(tq&&a.name.toLowerCase().indexOf(tq)<0&&String(displayName(a)).toLowerCase().indexOf(tq)<0)return false;
    return !tpFiltering()||tpItems(a).length
  }).sort(function(x,y){return (where(x)==='lobby'?0:1)-(where(y)==='lobby'?0:1)||tcn(y,6)-tcn(x,6)||tcn(y,5)-tcn(x,5)||tcn(y,4)-tcn(x,4)||x.name.localeCompare(y.name)})}
function tpAccTotal(acc){return Object.keys(tsel).reduce(function(t,k){return t+(tsel[k].acc===acc?tsel[k].q:0)},0)}
function tpGroup(i){var t=String(i.t||'').toLowerCase();return t==='ability'?'skill':t}
function tpHas(o){return Object.keys(o).length>0}
function tpFiltering(){return tpHas(tfr)||tpHas(tft)||tpLv.a!==''||tpLv.b!==''}
function tpReq(i){var r=i.s&&i.s.req;return Number.isFinite(r)?r:null}
function tpReqOk(i){if(tpLv.a===''&&tpLv.b==='')return true;var r=tpReq(i);if(r===null)return false;return !(tpLv.a!==''&&r<+tpLv.a)&&!(tpLv.b!==''&&r>+tpLv.b)}
function tpPass(i){return (!tpHideEq||!i.eq)&&tpReqOk(i)&&(!tpHas(tfr)||tfr[tier(i)])&&(!tpHas(tft)||tft[tpGroup(i)])}
function tpBtns(){
  [['tp_r',tfr],['tp_t',tft]].forEach(function(x){[].slice.call($(x[0]).querySelectorAll('[data-v]')).forEach(function(b){b.classList.toggle('on',b.dataset.v===''?!tpHas(x[1]):!!x[1][b.dataset.v])})})}
function tpToggle(boxId,set,e,label){
  var b=e.target.closest('[data-v]');if(!b)return false;var v=b.dataset.v;
  if(v==='')Object.keys(set).forEach(function(k){delete set[k]});else if(set[v])delete set[v];else set[v]=1;
  tpBtns();tpDrop();
  var on=[].slice.call($(boxId).querySelectorAll('[data-v].on')).map(function(x){return x.textContent});
  tlog(label+on.join(' + '),'info');syncTp();bar();return true}
function tpDrop(){}   // doi bo loc KHONG bo chon item da pick: item van giu trong tsel va van duoc trade du dang bi an boi bo loc
function tpItems(a){return inv(a).filter(tpOk).filter(tpPass).sort(function(x,y){return tier(y)-tier(x)||dstat(y)-dstat(x)||String(x.n).localeCompare(String(y.n))})}
function tpPicks(a){   // item dang hien + trade duoc, uu tien tu tren xuong, khong con gioi han so luong
  var left=999,out=[];
  tpItems(a).forEach(function(i){if(left<=0||!canTrade(i))return;var q=Math.min(+i.q||1,left);out.push({item:i,q:q});left-=q});
  return out}
function tpSet(a,on){
  Object.keys(tsel).forEach(function(k){if(tsel[k].acc===a.name)delete tsel[k]});
  if(!on)return 0;
  var p=tpPicks(a);
  p.forEach(function(x){var k=a.name+'|'+sigOf(x.item);if(!tsel[k])tsel[k]={item:x.item,acc:a.name,q:x.q}});
  return p.length}
function tpJobs(only){
  var ml=tmain.toLowerCase();
  return data.filter(function(a){return a.name.toLowerCase()!==ml&&(!only||only.indexOf(a.name)>=0)}).map(function(a){return {n:a.name,ps:Object.keys(tsel).filter(function(k){return tsel[k].acc===a.name}).map(function(k){return tsel[k]})}}).filter(function(x){return x.ps.length})}
function mainUnits(){   // do main da chon de tra: tach tung don vi, do tot nhat truoc
  var ml=(tmain||'').toLowerCase(),u=[];
  Object.keys(tsel).forEach(function(k){var e=tsel[k];if(e.acc.toLowerCase()!==ml)return;for(var n=0;n<e.q;n++)u.push({item:e.item,acc:e.acc})});
  u.sort(function(x,y){return tier(y.item)-tier(x.item)||dstat(y.item)-dstat(x.item)});
  return u}
function splitBack(jobs,units){   // chia DEU theo so luong: N mon / k alt -> moi alt floor(N/k), N%k alt dau them 1 (9/2=5-4, 9/3=3-3-3, 9/5=2-2-2-2-1)
  var give=jobs.map(function(){return []}),k=jobs.length,N=units.length;
  if(!N||!k)return give;
  var quota=jobs.map(function(j,i){return Math.floor(N/k)+(i<N%k?1:0)});
  units.forEach(function(u){var best=-1;   // mon tot nhat truoc, vao alt con thieu nhieu nhat -> gia tri cung chia deu
    for(var i=0;i<k;i++){if(give[i].length>=quota[i])continue;if(best<0||quota[i]-give[i].length>quota[best]-give[best].length)best=i}
    if(best>=0)give[best].push(u)});
  return give}
function packUnits(us){var m={},o=[];us.forEach(function(u){var it=u.item,k=sigOf(it);if(m[k])m[k].q++;else{m[k]={n:it.n,t:it.t,r:it.r||'',e:it.e||'',s:it.s||{},q:1};o.push(m[k])}});return o}
function tpRun(jobs){
  if(!tmain){toast('Nhập hoặc chọn Main Account trước');return}
  if(!/^[A-Za-z0-9_]{3,20}$/.test(tmain)){toast('Tên Main không hợp lệ (3–20 ký tự: chữ, số, _)');return}
  var gAlts=gPicked();
  if(!jobs.length&&!gAlts.length)return;
  var gSet={};gAlts.forEach(function(n){gSet[n]=1});
  var gOnly=gAlts.filter(function(n){return !jobs.some(function(j){return j.n===n})});   // alt chi gui gold (khong chon item)
  var dry=false,total=jobs.reduce(function(t,j){return t+j.ps.reduce(function(u,p0){return u+p0.q},0)},0);
  var units=mainUnits(),give=splitBack(jobs,units);
  tlog('Gửi ngay '+total+' item'+(gAlts.length?' + all gold của '+gAlts.length+' acc':'')+' từ '+jobs.length+' acc về main '+shown(tmain)+(units.length?' · main chia '+units.length+' item trả lại trong cùng phiên ('+give.map(function(g){return g.length}).join('/')+')':''),'info');
  $('tp_igo').disabled=true;
  Promise.all(jobs.map(function(j,ix){   // gui TAT CA lenh cung luc; server cho main xu ly lan luot tung alt (1 phien / alt)
    var body={alts:[j.n],main:tmain,mode:'items',dry:dry,items:j.ps.map(function(p0){var it=p0.item;return {n:it.n,t:it.t,r:it.r||'',e:it.e||'',s:it.s||{},q:p0.q}})};
    if(gSet[j.n])body.goldAll=true;   // alt duoc chon trong All gold: gui HET gold cung phien trade voi item
    var bk=packUnits(give[ix]);if(bk.length)body.backItems=bk;
    return sendJob(body,function(){
      Object.keys(tsel).forEach(function(k){if(tsel[k].acc===j.n)delete tsel[k]});delete gSel[j.n];
      give[ix].forEach(function(u){var k=u.acc+'|'+sigOf(u.item),e=tsel[k];if(e){e.q--;if(e.q<=0)delete tsel[k]}})})}).concat(gOnly.map(function(n){
    return sendJob({alts:[n],main:tmain,mode:'gold',goldAll:true,dry:false},function(){delete gSel[n]})})))
  .then(function(){syncTp();bar();gUpd()})}
$('tp_maininv').onclick=function(e){
  var c=e.target.closest('.tpx.pick');if(!c)return;
  var acc=c.dataset.a,k=c.dataset.k,a0=data.find(function(x){return x.name===acc});if(!a0)return;
  var it=inv(a0).filter(function(i){return canTrade(i)&&acc+'|'+sigOf(i)===k})[0];if(!it)return;
  var q=+it.q||1,cur=tsel[k];
  if(!cur){if(tpAccTotal(acc)+1>999){toast('Tối đa 999 item mỗi lệnh');return}tsel[k]={item:it,acc:acc,q:1}}
  else if(cur.q<q&&tpAccTotal(acc)+1<=999)cur.q++;
  else delete tsel[k];
  syncTp();bar()};
$('tp_tb').onclick=function(e){
  var c=e.target.closest('.tpx.pick');
  if(c){   // bam tung item de tinh chinh
    var acc=c.dataset.a,k=c.dataset.k,a0=data.find(function(x){return x.name===acc});if(!a0)return;
    var it=inv(a0).filter(function(i){return canTrade(i)&&acc+'|'+sigOf(i)===k})[0];if(!it)return;
    var q=+it.q||1,cur=tsel[k];
    if(!cur){if(tpAccTotal(acc)+1>999){toast('Tối đa 999 item mỗi acc trong 1 lệnh');return}tsel[k]={item:it,acc:acc,q:1}}
    else if(cur.q<q&&tpAccTotal(acc)+1<=999)cur.q++;
    else delete tsel[k];
    syncTp();bar();return;
  }
  var r=e.target.closest('tr.tp-row');if(!r)return;
  var a=data.find(function(x){return x.name===r.dataset.n});if(!a)return;
  if(tpAccTotal(a.name)>0){tpSet(a,false);tlog('Bỏ chọn '+shown(a.name),'info');syncTp();bar();return}
  var nn=tpSet(a,true);
  if(!nn){toast('Acc này không có item nào để trade');return}
  tlog('Chọn '+shown(a.name)+': '+nn+' loại item','info');
  syncTp();bar();
};
function mainHint(){var h=$('tp_mainhint'),n=tmain;if(!h)return;
  if(!n){h.textContent='Nhập tên acc nhận item (hoặc chọn bằng nút Main Account)';h.style.color='';return}
  if(!/^[A-Za-z0-9_]{3,20}$/.test(n)){h.textContent='Tên không hợp lệ (3–20 ký tự: chữ, số, _)';h.style.color='#f87171';return}
  var known=data.some(function(a){return a.name.toLowerCase()===n.toLowerCase()});
  h.textContent=known?'✔ Item của các acc đã chọn sẽ gửi về main này':'Main chưa có trong tracker — vẫn trade được nếu main cùng server';h.style.color=known?'var(--ok)':'#fbbf24'}
$('tp_maininp').oninput=function(e){persistMain(e.target.value.trim());mainHint()};
$('tp_maininp').onchange=function(e){var n=e.target.value.trim(),ml=n.toLowerCase();dropMainSel();persistMain(n);sel.delete(n);
  Object.keys(tsel).forEach(function(k){if(tsel[k].acc.toLowerCase()===ml)delete tsel[k]});render();syncTp(true);bar()};
$('tp_maininp').onkeydown=function(e){if(e.key==='Enter')e.target.blur()};
$('tp_clr').onclick=function(){TLOG=[];renderLog()};
$('tp_q').oninput=function(){syncTp()};
$('tp_r').onclick=function(e){tpToggle('tp_r',tfr,e,'Lọc độ hiếm: ')};
$('tp_t').onclick=function(e){tpToggle('tp_t',tft,e,'Lọc loại item: ')};
$('mi_r').onclick=function(e){mToggle(mfr,e)};$('mi_t').onclick=function(e){mToggle(mft,e)};
$('tp_igo').onclick=function(){tpRun(tpJobs(null))};
var gSel={};   // alt duoc chon de trade HET gold ve main
function gOnline(){var ml=(tmain||'').toLowerCase();return data.filter(function(a){return isOn(a)&&a.name.toLowerCase()!==ml}).sort(function(x,y){return (where(x)==='lobby'?0:1)-(where(y)==='lobby'?0:1)||(+y.gold||0)-(+x.gold||0)})}
function gPicked(){var on={};gOnline().forEach(function(a){on[a.name]=1});return Object.keys(gSel).filter(function(n){return on[n]})}
function gUpd(){var b=$('tp_goldbtn');if(!b)return;var n=gPicked().length;b.textContent='💰 All gold ('+n+') ▾';b.classList.toggle('on',n>0);
  var ig=$('tp_igo');if(ig)ig.disabled=ig.disabled&&!n?true:(n?false:ig.disabled)}
function gRender(){var on=gOnline(),pop=$('tp_goldpop');
  pop.innerHTML='<div class="rw" style="margin:0 0 8px;justify-content:space-between"><button class="bt" id="tp_gall" type="button" style="padding:3px 10px;font-size:12px">Chọn / bỏ tất cả</button><span class="mu" style="font-size:12px">'+on.length+' acc online</span></div>'
    +(on.length?on.map(function(a){return '<label style="display:flex;align-items:center;gap:8px;padding:4px 2px;cursor:pointer"><input type="checkbox" data-n="'+esc(a.name)+'"'+(gSel[a.name]?' checked':'')+'> <b style="flex:1">'+esc(shown(a.name))+'</b><span style="color:var(--cy)" title="'+fmt(a.gold)+'">'+sh(a.gold)+'</span></label>'}).join(''):'<div class="mu">Chưa có acc online (ngoài main)</div>')}
$('tp_goldbtn').addEventListener('click',gRender);
$('tp_goldpop').onchange=function(e){var n=e.target.dataset&&e.target.dataset.n;if(!n)return;if(e.target.checked)gSel[n]=1;else delete gSel[n];gUpd();syncTp()};
$('tp_goldpop').onclick=function(e){if(e.target.id!=='tp_gall')return;var cbs=[].slice.call($('tp_goldpop').querySelectorAll('input[data-n]')),allOn=cbs.length&&cbs.every(function(c){return c.checked});
  cbs.forEach(function(c){c.checked=!allOn;if(c.checked)gSel[c.dataset.n]=1;else delete gSel[c.dataset.n]});gUpd();syncTp()};
gUpd();
$('tp_goldtot').onclick=function(){refreshJobs().then(function(){toast('Tổng gold đã trade vào main: '+fmt(goldShown()))})};
goldBtn();
function tpEqBtn(){var b=$('tp_heq');if(!b)return;b.textContent=tpHideEq?'Hiện đồ đang mang':'Ẩn đồ đang mang';b.classList.toggle('on',tpHideEq);b.setAttribute('aria-pressed',String(tpHideEq))}
$('tp_heq').onclick=function(){tpHideEq=!tpHideEq;sv('dq_trade_hide_eq',tpHideEq);tpEqBtn();syncTp();bar()};
tpEqBtn();
function tpLvBtn(){var b=$('tp_lvbtn');if(b)b.classList.toggle('on',tpLv.a!==''||tpLv.b!=='')}
['tp_la','tp_lb'].forEach(function(id,k){$(id).oninput=function(e){tpLv[k?'b':'a']=e.target.value;tpLvBtn();syncTp();bar()}});

// ---- TRADE CHO ALT: main gui cac item da pick (o khung Main inventory) cho nhieu alt ----
// Bam "Trade cho alt" -> hien bang chon alt -> item chia DEU cho so alt da chon (3 item / 3 alt = 1-1-1, 3 item / 2 alt = 2-1)
// -> gui lan luot tung alt (moi acc chi co 1 lenh gui di cung luc): lenh co ret=true, acc gui = main, acc nhan = alt.
var rtSel={},retQ=[],retBusy=false,retSender='',retRun=0;
var rt=document.createElement('div');rt.className='ov';rt.id='rt';
rt.innerHTML='<div class="am" style="width:min(720px,100%)"><div class="rw" style="margin-top:0;justify-content:space-between"><b style="font-size:17px">📤 Trade cho alt</b><button class="bt" id="rt_x" type="button">✕</button></div>'
 +'<div class="mu" id="rt_sum" style="margin-top:8px"></div>'
 +'<div class="rw" style="margin-top:10px"><input id="rt_q" type="text" placeholder="🔍 Tìm alt..." autocomplete="off" spellcheck="false" style="flex:1;min-width:160px;background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:8px 12px;color:var(--tx)"><button class="bt" id="rt_all" type="button">Chọn tất cả</button><button class="bt" id="rt_none" type="button">Bỏ chọn</button></div>'
 +'<div class="tw" style="margin-top:10px;max-height:46vh;overflow:auto"><table style="min-width:0"><thead><tr><th class="ck"></th><th>Alt</th><th>Trạng thái</th><th>Sẽ nhận</th></tr></thead><tbody id="rt_tb"></tbody></table><div class="empty" id="rt_em" style="display:none">Không có alt nào</div></div>'
 +'<div class="mu" id="rt_msg" style="margin-top:10px;min-height:16px"></div>'
 +'<div class="rw" style="justify-content:flex-end"><button class="bt" id="rt_cancel" type="button">Đóng</button><button class="bt on" id="rt_go" type="button" disabled>Gửi trade</button></div></div>';
document.body.appendChild(rt);
function rtList(){   // tat ca alt (khong tinh main), online truoc; thu tu nay quyet dinh alt nao duoc them 1 item khi chia le
  var ml=(tmain||'').toLowerCase();
  return data.filter(function(a){return a.name.toLowerCase()!==ml}).sort(function(x,y){return (where(x)==='lobby'?0:1)-(where(y)==='lobby'?0:1)||(+isOn(y))-(+isOn(x))||x.name.localeCompare(y.name)})}
function rtPlan(){
  var picked=rtList().filter(function(a){return rtSel[a.name]}).map(function(a){return a.name}),units=mainUnits(),give=splitBack(picked,units),plan={};
  picked.forEach(function(n,i){plan[n]=give[i]});
  return {picked:picked,units:units,plan:plan}}
function rtGive(us){
  if(!us||!us.length)return '<span class="mu">—</span>';
  return packUnits(us).map(function(x){return '<span class="bg" style="margin:1px 3px 1px 0">'+esc(x.n)+(x.q>1?' ×'+x.q:'')+'</span>'}).join('')}
function rtRender(){
  var P=rtPlan(),q=($('rt_q').value||'').trim().toLowerCase(),all=rtList(),
      rows=all.filter(function(a){return !q||a.name.toLowerCase().indexOf(q)>=0||String(displayName(a)).toLowerCase().indexOf(q)>=0}),
      N=P.units.length,k=P.picked.length;
  $('rt_sum').textContent='Main '+shown(tmain)+' sẽ gửi '+N+' item · đã chọn '+k+' alt'+(k?' · chia đều: '+P.picked.map(function(n){return P.plan[n].length}).join(' / ')+' item':'');
  $('rt_tb').innerHTML=rows.map(function(a){
    var on=!!rtSel[a.name],g=P.plan[a.name];
    return '<tr class="r" data-n="'+esc(a.name)+'"><td class="ck"><input type="checkbox" class="cb" '+(on?'checked':'')+'></td><td><b>'+esc(displayName(a))+'</b></td><td><span class="bg on" style="color:'+placeColor(a)+';border-color:'+placeColor(a)+'">'+placeLabel(a)+'</span></td><td style="white-space:normal">'+(on?rtGive(g):'<span class="mu">—</span>')+'</td></tr>'}).join('');
  $('rt_em').style.display=rows.length?'none':'block';
  var off=P.picked.filter(function(n){var a=data.find(function(x){return x.name===n});return a&&!isOn(a)}).length,msg='';
  if(k&&N<k)msg='Chỉ có '+N+' item nên chỉ '+N+' alt đầu tiên trong danh sách được nhận.';
  else if(off)msg=off+' alt đang offline sẽ không nhận được trade.';
  $('rt_msg').textContent=msg;$('rt_msg').style.color=msg?'#fbbf24':'';
  $('rt_go').disabled=!k||!N||retBusy;$('rt_go').textContent=k?'Gửi trade ('+Math.min(k,N)+' alt)':'Gửi trade';
  var sa=rows.length&&rows.every(function(a){return rtSel[a.name]});$('rt_all').textContent=sa?'Bỏ chọn hiển thị':'Chọn tất cả'}
function retBtn(){
  var b=$('tp_ret');if(!b)return;var n=mainSelCount();
  if(retBusy){b.disabled=false;b.classList.add('dg');b.textContent='⏹ Dừng ('+retQ.length+' alt còn lại)'}
  else{b.disabled=!n;b.classList.remove('dg');b.textContent='📤 Trade cho alt ('+n+')'}}
function retWait(sender){   // doi lenh vua tao ket thuc (done/failed/cancelled/expired) roi moi gui cho alt ke tiep
  var t0=Date.now();
  return new Promise(function(resolve){(function tick(){
    refreshJobs().then(function(){
      var j=TJ[sender];
      if(!isAct(j))return resolve(j);
      if(Date.now()-t0>150000){tapi('/api/trade/cancel',{alts:[sender]});tlog('Lệnh trade quá 150s chưa xong -> huỷ để chuyển alt kế tiếp','failed');return resolve(j)}
      setTimeout(tick,1000)})})()})}
function retStart(sender,queue){
  var my=++retRun;retQ=queue.slice();retSender=sender;retBusy=true;
  tlog('Main '+shown(sender)+' trade cho '+queue.length+' alt (chia đều '+queue.map(function(x){return x.units.length}).join('/')+' item) · lần lượt từng alt','info');
  retBtn();
  (function next(){
    if(my!==retRun||!retBusy)return;
    var q=retQ.shift();
    if(!q){retBusy=false;retBtn();tlog('Đã xong hàng đợi trade cho alt','ok');syncTp();return}
    retBtn();
    var okSent=false,body={alts:[sender],main:q.alt,mode:'items',dry:false,ret:true,items:packUnits(q.units)};
    sendJob(body,function(){
      okSent=true;
      q.units.forEach(function(u){var k=u.acc+'|'+sigOf(u.item),e=tsel[k];if(e){e.q--;if(e.q<=0)delete tsel[k]}});
    }).then(function(){
      if(my!==retRun)return null;
      if(!okSent){tlog('Bỏ qua '+shown(q.alt)+': server không nhận lệnh','failed');return null}
      return retWait(sender)
    }).then(function(){next()})
  })()}
function retStop(){
  retRun++;retQ=[];retBusy=false;
  tapi('/api/trade/cancel',{alts:[retSender]}).then(function(){refreshJobs()}).catch(function(){});
  tlog('Đã dừng hàng đợi trade cho alt','cancelled');syncTp()}
function openRt(){
  if(retBusy){retStop();return}
  if(!tmain){toast('Chọn Main Account trước');return}
  if(!mainSelCount()){toast('Chưa pick item nào trong inventory của Main');return}
  rtSel={};$('rt_q').value='';rt.classList.add('s');rtRender()}
$('tp_ret').onclick=openRt;
function closeRt(){rt.classList.remove('s')}
$('rt_x').onclick=closeRt;$('rt_cancel').onclick=closeRt;
rt.onclick=function(e){if(e.target===rt)closeRt()};
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeRt()});
$('rt_q').oninput=rtRender;
$('rt_tb').onclick=function(e){
  var tr=e.target.closest('tr[data-n]');if(!tr)return;var n=tr.dataset.n;
  if(e.target.type==='checkbox'){if(e.target.checked)rtSel[n]=1;else delete rtSel[n]}
  else if(rtSel[n])delete rtSel[n];else rtSel[n]=1;
  rtRender()};
$('rt_all').onclick=function(){
  var q=($('rt_q').value||'').trim().toLowerCase(),rows=rtList().filter(function(a){return !q||a.name.toLowerCase().indexOf(q)>=0||String(displayName(a)).toLowerCase().indexOf(q)>=0}),
      allOn=rows.length&&rows.every(function(a){return rtSel[a.name]});
  rows.forEach(function(a){if(allOn)delete rtSel[a.name];else rtSel[a.name]=1});rtRender()};
$('rt_none').onclick=function(){rtSel={};rtRender()};
$('rt_go').onclick=function(){
  var P=rtPlan(),queue=[];
  P.picked.forEach(function(n){var g=P.plan[n];if(g&&g.length)queue.push({alt:n,units:g})});
  if(!queue.length){toast('Không có item để chia');return}
  closeRt();retStart(tmain,queue)};
$('tp_pop').onclick=function(e){var b=e.target.closest('[data-m]');if(!b)return;var n=b.dataset.m;dropMainSel();persistMain(n);sel.delete(n);Object.keys(tsel).forEach(function(k){if(tsel[k].acc===n)delete tsel[k]});$('tp_pop').classList.remove('s');render();syncTp(true)};



// ---- lich su trade (luu tren server, giu 30 ngay) ----
var hsSig='';
function hsTime(ts){var d=new Date(ts||0),p=function(n){return n<10?'0'+n:''+n};return p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds())+' '+p(d.getDate())+'/'+p(d.getMonth()+1)}
function hsDur(h){var s=Math.round(((h.finishedAt||0)-(h.createdAt||0))/1000);if(!(s>=0)||!h.createdAt)return '';return s<60?s+'s':Math.floor(s/60)+'p'+(s%60?' '+s%60+'s':'')}
function hsWhat(h){
  var gv=+h.goldSent>0?(+h.goldSent).toLocaleString('en-US')+' gold':h.goldAll?'toàn bộ gold':+h.gold>0?(+h.gold).toLocaleString('en-US')+' gold':'';
  var g=gv?'<b style="color:var(--cy)">'+gv+'</b>':'';
  if(h.mode==='gold')return g||'<span class="mu">—</span>';
  if(g)return hsWhat2(h)+' · '+g;
  return hsWhat2(h)}
function hsWhat2(h){
  if(h.mode==='items'){var l=(h.items||[]).map(function(x){return esc(x.n)+(x.q>1?' ×'+x.q:'')});return l.length?l.join(', '):'<span class="mu">—</span>'}
  return 'Theo độ hiếm <b>'+esc((h.rarity||[]).join('/'))+'</b>'+(h.names&&h.names.length?' · '+esc(h.names.join(', ')):'')+' · tối đa '+(+h.max||0)}
function histRender(force){
  var tb=$('hs_tb');if(!tb)return;
  var sig=JSON.stringify([THIST.length,THIST[0]&&THIST[0].id,THIST[0]&&THIST[0].finishedAt,hsSt,hideAccounts]);
  if(!force&&sig===hsSig)return;hsSig=sig;
  var l=THIST.filter(function(h){return !hsSt||h.status===hsSt});
  $('hs_cnt').textContent='('+l.length+(hsSt?'/'+THIST.length:'')+')';
  $('hs_em').style.display=l.length?'none':'block';
  $('hs_clr').disabled=!THIST.length;
  tb.innerHTML=l.map(function(h){
    var s=ST[h.status]||[h.status,'#8a919c'],dur=hsDur(h);
    return '<tr class="hsrow"><td title="'+esc(h.createdAt?'Tạo lúc '+hsTime(h.createdAt):'')+'" style="white-space:nowrap">'+esc(hsTime(h.finishedAt))+(dur?'<div class="mu" style="font-size:11px">'+esc(dur)+'</div>':'')+'</td>'
     +'<td style="white-space:nowrap"><b>'+esc(shown(h.alt))+'</b> <span class="mu">→ '+esc(shown(h.main))+'</span></td>'
     +'<td class="hswhat">'+(h.ret?'<span class="bg">↩ Trả đồ'+(h.auto?' (tự động)':'')+'</span> ':'')+hsWhat(h)+(h.dry?' <span class="bg">DRY</span>':'')+'</td>'
     +'<td style="white-space:nowrap"><span class="tst" style="color:'+s[1]+';border-color:'+s[1]+'">'+esc(s[0])+'</span>'+(h.status==='done'&&!h.dry&&h.sent>0?' <span class="mu">'+h.sent+' item</span>':'')+'</td>'
     +'<td class="hswhat mu">'+esc(h.msg||'')+'</td></tr>'}).join('')}
function histLoad(){
  if(hsBusy)return Promise.resolve();hsBusy=true;
  return tapi('/api/trade/history').then(function(j){hsBusy=false;if(j&&Array.isArray(j.history)){THIST=j.history;try{goldScan(THIST)}catch(x){}histRender()}}).catch(function(){hsBusy=false})}
function histSoon(){clearTimeout(hsTimer);hsTimer=setTimeout(histLoad,700)}   // cho server ghi xong lich su roi moi tai lai
$('hs_f').onchange=function(e){hsSt=e.target.value;histRender(true)};
$('hs_rf').onclick=function(){histLoad().then(function(){toast('Đã tải lại lịch sử')})};
$('hs_clr').onclick=function(){
  if(!THIST.length)return;
  tapi('/api/trade/history/clear',{}).then(function(j){if(j&&j.ok){THIST=[];histRender(true);toast('Đã xoá lịch sử trade')}else toast('Lỗi xoá lịch sử')}).catch(function(){toast('Lỗi kết nối')})};
// LUU Y: danh sach item trong tab Trade (syncTp) truoc day CHI ve lai khi nguoi dung tu thao tac (loc, chon item...),
// khong tu dong ve lai khi bien "data" (kho cua acc) duoc poll() cap nhat ngam o duoi (moi 0.5s) - nen neu ban/nhan do
// o tab khac roi quay lai tab Trade MA KHONG bam gi, man hinh van hien kho CU (da bi tru/them do nhung chua ve lai),
// nhin giong nhu "da ban roi ma Trade van con hien". Sua: ve lai ngay khi bam vao tab Trade, + tu dong ve lai dinh ky
// trong luc dang mo tab Trade de luon khop voi kho moi nhat.
$('tabs').addEventListener('click',function(e){var b=e.target.closest('.tab');if(b&&b.dataset.tab==='trade'){histLoad();try{syncTp(true)}catch(x){}}});
setInterval(function(){if(!document.hidden&&tpane.style.display!=='none')histLoad()},15000);
setInterval(function(){if(!document.hidden&&tpane.style.display!=='none'){try{syncTp()}catch(x){}}},2000);

// ---- TAB BUYER/SELL: chon item tren cac acc dang ONLINE -> bam "Ban ngay" (KHONG tu dong) ----
var bsS={r:{},t:{},lock:ld('dq_bs_lock',true)!==false},ssel={},sidx={},SJ=[],bsLast='',bsBusy=false,bsLogLast='',bsTick=0;
var SST={queued:['Chờ acc nhận lệnh','#fbbf24'],running:['Đang bán','#60a5fa'],done:['Hoàn tất','#4ade80'],failed:['Lỗi','#f87171'],expired:['Hết hạn','#8a919c']};
var BS_RB='<button type="button" class="bt tpf on" data-v="">Tất cả</button>'+[6,5,4,3,2,1,0].map(function(t){var T=TIER[t]||{n:'Chưa rõ',c:'#8a919c'};return '<button type="button" class="bt tpf" data-v="'+t+'" style="--c:'+T.c+';--onc:#fff">'+T.n+'</button>'}).join('');
var BS_TB='<button type="button" class="bt tpf on" data-v="">Tất cả</button><button type="button" class="bt tpf" data-v="weapon">WP</button><button type="button" class="bt tpf" data-v="chest">Áo</button><button type="button" class="bt tpf" data-v="helmet">Mũ</button><button type="button" class="bt tpf" data-v="skill">Skill</button>';
$('bs_root').innerHTML='<div class="rw" style="margin:0 0 10px;gap:10px;justify-content:space-between;align-items:center">'
 +'<div class="rw" style="margin:0;gap:10px"><button class="bt on" id="bs_go" type="button" disabled>💸 Bán ngay (0)</button><button class="bt" id="bs_clr" type="button" disabled>Bỏ chọn tất cả</button><button class="bt" id="bs_lock" type="button" title="Khoá không cho chọn item Legend/Ult để tránh bán nhầm"></button><button class=\"bt\" id=\"bs_heq\" type=\"button\" title=\"Ẩn các item đang mang (⚔) khỏi danh sách bán\">Ẩn đồ đang mang</button><span class="mu" id="bs_cnt" style="font-size:12px"></span></div>'
 +'<span class="rw" style="margin:0;gap:8px"><input id="bs_q" type="text" placeholder="🔍 Tìm account..." autocomplete="off" spellcheck="false" style="min-width:200px;background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:8px 12px;color:var(--tx)"><input id="bs_qi" type="text" placeholder="🔍 Tìm item..." autocomplete="off" spellcheck="false" style="min-width:200px;background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:8px 12px;color:var(--tx)"></span></div>'
 +'<div class="tpfil" style="grid-template-columns:minmax(0,3fr) minmax(0,2fr);margin-top:0"><div class="tpfg"><span class="tpfl">Độ hiếm</span><span class="tpmb" id="bs_r">'+BS_RB+'</span></div><div class="tpfg"><span class="tpfl">Loại item</span><span class="tpmb" id="bs_t">'+BS_TB+'</span></div></div>'
 +'<div class="tcon" style="margin-top:12px"><div class="tcon-h"><span>🖥 Console bán đồ</span><button class="bt" id="bs_lclr" type="button" style="padding:2px 10px;font-size:12px">Xoá log</button></div><div class="tlog" id="bs_log"></div></div>'
 +'<div class="tw" style="margin-top:12px"><table style="min-width:0"><thead><tr><th>Account (online)</th><th>Item (chưa mang)</th></tr></thead><tbody id="bs_tb"></tbody></table><div class="empty" id="bs_em" style="display:none">Không có account online nào</div></div>';
function bsVisible(){return bspane.style.display!=='none'}
function bsOk(i){return TYPES.indexOf(String(i.t||'').toLowerCase())>=0} function bsSellable(i){return bsOk(i)&&!i.eq}
function bsLocked(i){return bsS.lock&&tier(i)>=5}
function bsKey(acc,i){return acc+'|'+sigOf(i)+'|'+((i.s&&i.s.upgrades)||0)}
function bsPass(i){return bsOk(i)&&(!bsHideEq||!i.eq)&&(!tpHas(bsS.r)||bsS.r[tier(i)])&&(!tpHas(bsS.t)||bsS.t[tpGroup(i)])}
function bsQ(){var e=$('bs_q');return e?e.value.trim().toLowerCase():''}
function bsQi(){var e=$('bs_qi');return e?e.value.trim().toLowerCase():''}
function bsItems(a){var qi=bsQi();
  return inv(a).filter(bsPass).filter(function(i){return !qi||String(i.n).toLowerCase().indexOf(qi)>=0}).sort(function(x,y){return tier(y)-tier(x)||dstat(y)-dstat(x)||String(x.n).localeCompare(String(y.n))})}
function bsAccs(){var qa=bsQ(),filt=tpHas(bsS.r)||tpHas(bsS.t)||!!bsQi();
  return data.filter(function(a){return isOn(a)&&(!qa||a.name.toLowerCase().indexOf(qa)>=0||String(displayName(a)).toLowerCase().indexOf(qa)>=0)&&(!filt||bsItems(a).length)}).sort(function(x,y){return (where(x)==='lobby'?0:1)-(where(y)==='lobby'?0:1)||tcn(y,6)-tcn(x,6)||tcn(y,5)-tcn(x,5)||tcn(y,4)-tcn(x,4)||x.name.localeCompare(y.name)})}
function bsTotals(){var n=0,accs={};Object.keys(ssel).forEach(function(k){n+=ssel[k].q;accs[ssel[k].acc]=1});return {n:n,a:Object.keys(accs).length}}
function bsAccTotal(acc){return Object.keys(ssel).reduce(function(t,k){return t+(ssel[k].acc===acc?ssel[k].q:0)},0)}
function bsPrune(){var live={};data.forEach(function(a){if(!isOn(a))return;inv(a).forEach(function(i){if(bsSellable(i))live[bsKey(a.name,i)]=i})});
  Object.keys(ssel).forEach(function(k){var i=live[k];if(!i||bsLocked(i)){delete ssel[k];return}var max=+i.q||1;if(ssel[k].q>max)ssel[k].q=max;ssel[k].item=i})}
function bsChip(i,acc){
  var t=tier(i),c=cc(i)||(TIER[t]||{}).c||'#8a919c',u=IMG[ik(i)],L=esc(String(i.n||'?').slice(0,1).toUpperCase()),e=enchantCode(i),top=tpTop(i),q=+i.q||1,
      lk=bsLocked(i),eq=i.eq===true,dis=lk||eq,key=bsKey(acc,i),pk=dis?null:ssel[key],
      img=/^https:\/\/[^\s"'<>]+$/.test(u||'')?'<img src="'+esc(u)+'" alt="'+L+'" loading="lazy" referrerpolicy="no-referrer">':L;
  sidx[key]={acc:acc,item:i};
  return '<span class="tpx '+(dis?'dis':'pick')+(pk?' sel':'')+'"'+(dis?'':' data-sk="'+esc(key)+'"')+' style="border-color:'+c+';background:'+c+'22'+(eq?';opacity:.42;filter:grayscale(.7)':'')+'" title="'+esc(i.n)+' · '+esc((TIER[t]||{}).n||'')+(top?' · '+top.l+' '+fmt(top.v):'')+(eq?' · đang mang (không bán được — bật nút Hiện đồ đang mang chỉ để xem)':(lk?' · đang khoá (tắt nút Khoá Legend/Ult để bán)':(q>1?' · bấm nhiều lần để tăng số lượng':' · bấm để chọn bán')))+'"><span class="tim">'+img+'</span><b>'+esc(i.n)+'</b>'
    +(top?'<span class="mu">'+top.l+' <b style="color:var(--tx)">'+sh(top.v)+'</b></span>':'')
    +(e?'<span class="enchant-badge '+(e==='EIR'?'eir':'eif')+'">'+e+'</span>':'')
    +(i.eq?'<span class="mu" title="Đang mang" style="white-space:nowrap">⚔ Đang mang</span>':'')
    +(pk?'<span class="ck3">✔'+(q>1?' '+pk.q+'/'+q:'')+'</span>':(q>1?'<span class="mu">×'+fmt(q)+'</span>':''))+'</span>'}
function bsRow(a){
  var its=bsItems(a),n=bsAccTotal(a.name);
  return '<tr class="tp-row'+(n?' on':'')+'" style="cursor:default"><td style="vertical-align:top"><b class="tpname">'+esc(displayName(a))+'</b> <span class="bg on" style="color:'+placeColor(a)+';border-color:'+placeColor(a)+'">'+placeLabel(a)+'</span>'
    +'<div class="mu" style="font-size:12px;margin-top:2px">Lv '+fmt(a.level)+' · '+its.length+' loại item</div>'
    +(n?'<div class="tpsum">✔ Sẽ bán '+n+' item</div>':'')
    +'<div class="rw" style="margin:6px 0 0;gap:6px"><button type="button" class="bt" data-sall="'+esc(a.name)+'" style="padding:2px 10px;font-size:12px"'+(its.length?'':' disabled')+'>Chọn hết</button><button type="button" class="bt" data-sclr="'+esc(a.name)+'" style="padding:2px 10px;font-size:12px"'+(n?'':' disabled')+'>Bỏ chọn</button></div></td>'
    +'<td><div style="display:flex;flex-wrap:wrap;gap:6px;white-space:normal">'+(its.map(function(i){return bsChip(i,a.name)}).join('')||'<span class="mu">—</span>')+'</div></td></tr>'}
function bsBtns(){[['bs_r',bsS.r],['bs_t',bsS.t]].forEach(function(x){[].slice.call($(x[0]).querySelectorAll('[data-v]')).forEach(function(b){var v=b.dataset.v;b.classList.toggle('on',v===''?!tpHas(x[1]):!!x[1][v])})})}
var bsHideEq=ld("dq_sell_hide_eq",false)===true;
function bsHeqBtn(){var b=$("bs_heq");if(!b)return;b.textContent=bsHideEq?"Hi\u1ec7n \u0111\u1ed3 \u0111ang mang":"\u1ea8n \u0111\u1ed3 \u0111ang mang";b.classList.toggle("on",bsHideEq);b.setAttribute("aria-pressed",String(bsHideEq))}
function bsRender(force){
  if(!$('bs_tb'))return;
  sidx={};bsPrune();
  var accs=bsAccs(),tot=bsTotals(),html=accs.map(bsRow).join(''),key=html+'|'+tot.n+'|'+bsBusy+'|'+bsS.lock;
  if(force||key!==bsLast){bsLast=key;$('bs_tb').innerHTML=html}
  bsHeqBtn();
  $('bs_em').style.display=accs.length?'none':'block';
  $('bs_go').disabled=!tot.n||bsBusy;$('bs_go').textContent='💸 Bán ngay ('+tot.n+')';
  $('bs_clr').disabled=!tot.n;
  $('bs_lock').textContent=bsS.lock?'🔒 Khoá Legend/Ult: BẬT':'🔓 Khoá Legend/Ult: TẮT';$('bs_lock').classList.toggle('on',bsS.lock);
  $('bs_cnt').textContent=data.filter(isOn).length+' acc online'+(tot.n?' · đã chọn '+tot.n+' item / '+tot.a+' acc':'');
  var all=[];accs.forEach(function(a){all=all.concat(bsItems(a))});
  var nImg=all.filter(function(i){return IMG[ik(i)]}).length;
  loadImgs(all).then(function(){var n2=all.filter(function(i){return IMG[ik(i)]}).length;if(n2!==nImg&&bsVisible())bsRender()})}
function bsSync(){if(bsVisible())bsRender()}
function bsTog(set,e){var b=e.target.closest('[data-v]');if(!b)return;var v=b.dataset.v;
  if(v==='')Object.keys(set).forEach(function(k){delete set[k]});else if(set[v])delete set[v];else set[v]=1;bsBtns();bsRender(true)}
$('bs_r').onclick=function(e){bsTog(bsS.r,e)};$('bs_t').onclick=function(e){bsTog(bsS.t,e)};
$('bs_q').oninput=function(){bsRender(true)};$('bs_qi').oninput=function(){bsRender(true)};
$('bs_lock').onclick=function(){bsS.lock=!bsS.lock;sv('dq_bs_lock',bsS.lock);bsRender(true)};
$('bs_clr').onclick=function(){ssel={};bsRender(true)};
$('bs_heq').onclick=function(){bsHideEq=!bsHideEq;sv('dq_sell_hide_eq',bsHideEq);bsHeqBtn();bsRender(true)};
$('bs_tb').onclick=function(e){
  var sa=e.target.closest('[data-sall]'),sc=e.target.closest('[data-sclr]'),c=e.target.closest('[data-sk]');
  if(sa){var a=data.find(function(x){return x.name===sa.dataset.sall});if(!a)return;
    bsItems(a).forEach(function(i){if(!bsLocked(i))ssel[bsKey(a.name,i)]={acc:a.name,item:i,q:+i.q||1}});bsRender(true);return}
  if(sc){Object.keys(ssel).forEach(function(k){if(ssel[k].acc===sc.dataset.sclr)delete ssel[k]});bsRender(true);return}
  if(c){var k=c.dataset.sk,o=sidx[k];if(!o||bsLocked(o.item))return;
    var max=+o.item.q||1,cur=ssel[k]?ssel[k].q:0;cur++;
    if(cur>max)delete ssel[k];else ssel[k]={acc:o.acc,item:o.item,q:cur};
    bsRender(true)}};
function bsSell(){
  if(bsBusy)return;
  var by={},hi=0,n=0;
  Object.keys(ssel).forEach(function(k){var s=ssel[k];if(bsLocked(s.item))return;(by[s.acc]=by[s.acc]||[]).push({k:k,item:s.item,q:s.q});n+=s.q;if(tier(s.item)>=5)hi+=s.q});
  var accs=Object.keys(by);if(!accs.length)return;
  bsBusy=true;bsRender(true);
  Promise.all(accs.map(function(acc){
    return tapi('/api/trade/sell/create',{acc:acc,items:by[acc].map(function(s){var i=s.item;return {n:i.n,t:i.t,r:i.r||'',e:i.e||'',s:i.s||{},q:s.q}})}).then(function(j){
      if(j&&j.ok){by[acc].forEach(function(s){delete ssel[s.k]});return 1}
      toast((j&&j.message)||'Lỗi tạo lệnh bán');return 0},function(){toast('Lỗi kết nối');return 0})
  })).then(function(r){bsBusy=false;var ok=r.reduce(function(a,b){return a+b},0);if(ok)toast('Đã gửi lệnh bán cho '+ok+' account');bsPoll();bsRender(true);try{poll(true)}catch(x){}})}
$('bs_go').onclick=bsSell;
function bsTime(ts){var d=new Date(ts||0),p=function(n){return n<10?'0'+n:''+n};return p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds())}
function bsLogRender(){var el=$('bs_log');if(!el)return;
  // luoi nhieu cot: gio | acc · item | trang thai | ghi chu -> cac nhan "Hoan tat" thang hang du ten item dai/ngan
  var h=SJ.length?SJ.map(function(j){var s=SST[j.status]||[j.status,'#8a919c'];
    return '<div style=\"display:contents\"><span class=\"mu\">['+bsTime(j.createdAt)+']</span>'
      +'<span><b>'+esc(tpName(j.acc))+'</b> · '+esc(j.what)+' ('+j.total+' item)</span>'
      +'<span><span class=\"tst\" style=\"color:'+s[1]+';border-color:'+s[1]+'\">'+esc(s[0])+'</span></span>'
      +'<span class=\"mu\">'+(j.msg?esc(j.msg):'')+'</span></div>'}).join(''):'<div class=\"mu\" style=\"grid-column:1/-1\">Chưa có lệnh bán. Chọn item rồi bấm Bán ngay.</div>';
  if(h!==bsLogLast){bsLogLast=h;el.style.display='grid';el.style.gridTemplateColumns='max-content max-content max-content minmax(0,1fr)';el.style.columnGap='12px';el.style.alignItems='center';el.style.alignContent='start';el.innerHTML=h}}
var bsSeenJob={};
function bsAutoRefresh(){
  // Khong reload trang: khi co lenh dang ban / vua hoan tat thi tai ngay du lieu moi tu server de do da ban bien mat
  var need=false;
  SJ.forEach(function(j){
    var k=String(j.id),st=j.status,prev=bsSeenJob[k];
    if(st==='queued'||st==='running')need=true;               // lenh dang ban: keo du lieu lien tuc
    else if(st==='done'&&prev!=='done'&&prev!==undefined)need=true; // vua chuyen sang Hoan tat
    bsSeenJob[k]=st});
  if(!need)return;
  try{poll(true)}catch(x){}
  setTimeout(function(){try{poll(true)}catch(x){}},1500)}   // them 1 lan sau 1.5s vi acc gui kho moi tre
function bsPoll(){return tapi('/api/trade/sell/list').then(function(j){if(j&&j.jobs){SJ=j.jobs;bsLogRender();bsAutoRefresh();try{bsSync()}catch(x){}}}).catch(function(){})}
function bsActive(){return SJ.some(function(j){return j.status==='queued'||j.status==='running'})}
$('bs_lclr').onclick=function(){tapi('/api/trade/sell/clear',{}).then(function(){bsPoll()})};
setInterval(function(){if(document.hidden||!bsVisible())return;bsTick++;bsPoll()},1000);
$('tabs').addEventListener('click',function(e){var b=e.target.closest('.tab');if(b&&b.dataset.tab==='buysell')setTimeout(function(){bsBtns();bsPoll();bsRender(true)},0)});
bsBtns();bsLogRender();

function buyRootHTML(){
  $("buy_root").innerHTML =
    "<div class=\"tcon\" style=\"margin:0 0 12px\"><div class=\"tcon-h\"><span>📦 Console mua</span><button class=\"bt\" id=\"buy_lclr\" type=\"button\" style=\"padding:2px 10px;font-size:12px\">Xoá log</button></div><div class=\"tlog\" id=\"buy_log\"></div></div>"
    + "<div class=\"rw\" style=\"margin:0 0 10px;gap:10px;justify-content:space-between;align-items:center;flex-wrap:wrap\">"
    + "<div class=\"rw\" style=\"margin:0;gap:10px\">"
    + "<button class=\"bt on\" id=\"buy_go\" type=\"button\" disabled>🛒 Mua het gamepass con thieu (0)</button>"
    + "<button class=\"bt\" id=\"buy_xp_go\" type=\"button\" style=\"border-color:#22d3ee;color:#22d3ee;background:#22d3ee1f\">⚡ Mua x2 XP</button>"
    + "<span class=\"mu\" id=\"buy_cnt\" style=\"font-size:12px\"></span></div>"
    + "<input id=\"buy_q\" type=\"text\" placeholder=\"🔍 Tim account...\" autocomplete=\"off\" spellcheck=\"false\" style=\"min-width:220px;background:var(--pn);border:1px solid var(--bd);border-radius:10px;padding:8px 12px;color:var(--tx)\">"
    + "</div>"
    + "<div class=\"mu\" style=\"font-size:12px;margin:0 0 10px;line-height:1.5\">"
    + "Gia lay truc tiep tu shop trong game. Bam <b>Mua</b> de acc do nhan lenh; acc bao ket qua o <b>Console mua</b>. "
    + "Lenh nao acc khong nhan trong 5s se bi huy.</div>"
    + "<div class=\"tw\" style=\"overflow:auto\"><table style=\"min-width:640px;border-collapse:separate;border-spacing:2px\"><thead><tr>"
    + "<th style=\"text-align:left;padding:8px 10px;border:1px solid var(--bd);background:var(--pn)\">Account</th>"
    + GP.map(function(g){return "<th style=\"text-align:center;padding:8px 6px;border:1px solid var(--bd);background:var(--pn);min-width:96px\">"+g.i+" "+esc(g.n)+"</th>"}).join("")
    + "</tr></thead><tbody id=\"buy_tb\"></tbody></table>"
    + "<div class=\"empty\" id=\"buy_em\" style=\"display:none\">Khong co account online nao</div></div>";
}
// ---- TAB BUY: mua GAMEPASS / BOOST cho acc dang ONLINE ----
// ---- TAB BUY: mua GAMEPASS / BOOST cho acc dang ONLINE ----
var GP=[{k:"vip",n:"VIP",d:"Cosmetics + 20% exp",i:"👑",c:"#f59e0b"},
{k:"gold2",n:"x2 Gold",d:"Nhan doi gold khi ban",i:"💰",c:"#facc15"},
{k:"drop",n:"Extra Item",d:"+1 item khi hoan thanh dungeon",i:"🎁",c:"#a78bfa"},
{k:"freeResets",n:"Free Resets",d:"Reset stat khong tuyen gold",i:"🔄",c:"#4ade80"}];
var XP=[{k:"xp1",n:"1 Hour",i:"⚡",c:"#22d3ee"},{k:"xp2",n:"2 Hours",i:"⚡",c:"#22d3ee"},{k:"xp3",n:"4 Hours",i:"⚡",c:"#2dd4bf"}];
var GM={};GP.forEach(function(g){GM[g.k]=g});XP.forEach(function(g){GM[g.k]=g});
var BBST={queued:["Chờ acc nhận lệnh","#fbbf24"],running:["Đang mua","#60a5fa"],done:["Hoàn tất","#4ade80"],failed:["Lỗi","#f87171"],expired:["Chưa hoàn thành (quá 5s)","#8a919c"]};
var BBUY=ld("dq_buy_log",[])||[],BBYLast="",BBYBusy=false,BBTick=0,XPSEL=ld("dq_xp_sel",{})||{};
function buyMerge(srv){var m={},now=Date.now(),out=[],has={};BBUY.forEach(function(j){m[j.id]=j});srv.forEach(function(j){m[j.id]=j;has[j.id]=1});
  Object.keys(m).forEach(function(k){var j=m[k];if(now-(j.createdAt||0)>6e5)return;
    if((j.status==="queued"||j.status==="running")&&!has[k]&&now-(j.createdAt||0)>5500)j=Object.assign({},j,{status:"expired",msg:"Chua hoan thanh: qua 5s"});
    out.push(j)});
  out.sort(function(a,b){return (a.createdAt||0)-(b.createdAt||0)});return out.slice(-200)}
function buyVisible(){return buypane.style.display!=="none"}
function gpassOf(a){return a.gpass||{}}
function buyOwned(a,k){return gpassOf(a)[k]===true}
function gpassPriceOf(a,key){var p=a.gpassPrices||{};if(typeof p[key]==="number")return p[key];return null}
function buyPrice(a,g){var p=gpassPriceOf(a,g.k);return (typeof p==="number"&&isFinite(p)&&p>=0)?p:null}
function gemsPrice(a,k){var p=(a.gpassPrices||{})[k+"Gems"];return (typeof p==="number"&&isFinite(p)&&p>=0)?p:null}
function xpLeft(a){var b=Math.floor(+a.xpBoostSeconds||0);if(b<=0)return 0;var el=a.updated?Math.floor((Date.now()-a.updated)/1000):0;return Math.max(0,b-Math.max(0,el))}
function xpOwned(a){return xpLeft(a)>0}
function buyAccs(){var q=($("buy_q")?$("buy_q").value:"").trim().toLowerCase();
  return data.filter(function(a){if(!isOn(a))return false;if(q&&a.name.toLowerCase().indexOf(q)<0&&String(displayName(a)).toLowerCase().indexOf(q)<0)return false;return true})
  .sort(function(x,y){return (where(x)==="lobby"?0:1)-(where(y)==="lobby"?0:1)||x.name.localeCompare(y.name)})}
function buyRow(a){
  var cells=GP.map(function(g){
    var own=buyOwned(a,g.k),p=buyPrice(a,g);
    var priceHtml = own
      ? "<div style=\"margin-top:4px;font-size:10.5px;font-weight:700;color:#4ade80;letter-spacing:.3px\">✓ Sở hữu</div>"
      : "<div style=\"margin-top:4px;font-size:10.5px;color:var(--cy);\">"+(p!==null?sh(p)+" Gold":"Chua ro")+"</div>";
    var buyBtn = own
      ? ""
      : "<button type=\"button\" class=\"bt\" data-gk=\""+esc(a.name)+"|"+g.k+"\" style=\"margin-top:5px;padding:3px 11px;font-size:11px;border-color:"+g.c+";color:"+g.c+";background:"+g.c+"1f\" title=\"Mua "+esc(g.n)+" cho "+esc(displayName(a))+"\">🛒 Mua</button>";
    return "<td style=\"text-align:center;padding:8px 6px;border:1px solid var(--bd);background:"+(own?"#4ade8014":"#0b0f14")+";min-width:96px\">"
      + "<div style=\"font-size:19px;line-height:1\">"+g.i+"</div>"
      + "<div style=\"font-size:11.5px;font-weight:600;margin-top:3px;color:var(--tx)\">"+esc(g.n)+"</div>"
      + "<div style=\"font-size:10px;color:var(--mu);margin-top:2px;line-height:1.25\">"+esc(g.d)+"</div>"
      + priceHtml
      + buyBtn + "</td>";
  }).join("");
  var xl=xpLeft(a);
  return "<tr class=\"tp-row\">"
    + "<td style=\"vertical-align:top;padding:9px 10px;min-width:190px;border:1px solid var(--bd);background:var(--pn)\">"
    + "<b class=\"tpname\">"+esc(displayName(a))+"</b> "
    + "<span class=\"bg on\" style=\"color:"+placeColor(a)+";border-color:"+placeColor(a)+"\">"+placeLabel(a)+"</span>"
    + "<div class=\"mu\" style=\"font-size:12px;margin-top:3px\">Lv "+fmt(a.level)+"</div>"
    + "<div style=\"margin-top:6px;display:flex;gap:8px;flex-wrap:wrap\">"
    + "<span class=\"statbox gold\" title=\""+fmt(a.gold)+" Gold\">💰 <b>"+sh(a.gold)+"</b> <small>GOLD</small></span>"
    + "<span class=\"statbox gem\" title=\""+fmt(a.gems)+" Gems\">💎 <b>"+sh(a.gems)+"</b> <small>GEMS</small></span>"
    + "</div>"
    + "<div class=\"mu\" style=\"font-size:11px;margin-top:5px\">Co "+GP.filter(function(g){return buyOwned(a,g.k)}).length+"/"+GP.length+" gamepass"
    + (xl>0?" · ⚡ x2 XP con "+xpTime(xl):"")+"</div>"
    + "</td>" + cells + "</tr>";
}
function xpTime(s){var h=Math.floor(s/3600),m=Math.floor(s%3600/60);return h?h+"h "+m+"m":(m?m+"m "+(s%60)+"s":s+"s")}
function buyRender(force){
  var root=$("buy_root");if(!root)return;
  var accs=buyAccs(),html=accs.map(buyRow).join(""),key=html+"|"+BBYBusy+"|"+BBYLast;
  if(force||key!==buyRender._last){buyRender._last=key;$("buy_tb").innerHTML=html}
  var tot=0;accs.forEach(function(a){tot+=GP.filter(function(g){return !buyOwned(a,g.k)}).length});
  $("buy_em").style.display=accs.length?"none":"block";
  var nOn=data.filter(isOn).length;
  $("buy_cnt").textContent=nOn+" acc online | "+tot+" gamepass con thieu";
  $("buy_go").disabled=BBYBusy||!tot;
  $("buy_go").textContent=BBYBusy?"⏳ Dang gui...":"🛒 Mua het gamepass con thieu ("+tot+")";
  if(xpOpen())xpList();
}
var XPCUR=ld("dq_xp_cur","gold")==="gems"?"gems":"gold";
function xpOpen(){var m=$("xp_mod");return !!(m&&m.classList.contains("s"))}
function xpBuild(){
  if($("xp_mod"))return;
  var m=document.createElement("div");m.id="xp_mod";
  m.innerHTML="<div class=\"xpbox\">"
    +"<div style=\"display:flex;justify-content:space-between;align-items:center\"><b style=\"font-size:15px\">⚡ Mua x2 XP</b><button type=\"button\" class=\"bt\" id=\"xp_x\" style=\"padding:2px 10px\">✕</button></div>"
    +"<div class=\"mu\" style=\"font-size:12px;margin:4px 0 8px\">Tick acc muon mua. Tren moi acc co hien con bao nhieu gio x2 XP.</div>"
    +"<div style=\"display:flex;gap:8px;align-items:center;margin-bottom:6px\"><button type=\"button\" class=\"bt\" id=\"xp_all\" style=\"padding:3px 10px;font-size:12px\">Chon tat ca</button><button type=\"button\" class=\"bt\" id=\"xp_none\" style=\"padding:3px 10px;font-size:12px\">Bo chon</button><span class=\"mu\" id=\"xp_sel\" style=\"font-size:12px;margin-left:auto\"></span></div>"
    +"<div id=\"xp_ls\" style=\"max-height:260px;overflow:auto;border:1px solid var(--bd);border-radius:10px;padding:2px 8px;background:var(--pn)\"></div>"
    +"<div class=\"xpcur\"><button type=\"button\" id=\"xp_cg\" class=\"g\">💰 Mua bang Gold</button><button type=\"button\" id=\"xp_cm\" class=\"m\">💎 Mua bang Gems</button></div>"
    +"<div class=\"xpdur\" id=\"xp_dur\"></div>"
    +"</div>";
  document.body.appendChild(m);
  m.addEventListener("click",function(e){if(e.target===m)xpModClose()});
  $("xp_x").onclick=xpModClose;
  $("xp_all").onclick=function(){buyAccs().forEach(function(a){XPSEL[a.name]=1});sv("dq_xp_sel",XPSEL);xpList()};
  $("xp_none").onclick=function(){XPSEL={};sv("dq_xp_sel",XPSEL);xpList()};
  $("xp_cg").onclick=function(){XPCUR="gold";sv("dq_xp_cur",XPCUR);xpDur()};
  $("xp_cm").onclick=function(){XPCUR="gems";sv("dq_xp_cur",XPCUR);xpDur()};
  $("xp_ls").onchange=function(e){var c=e.target.closest("[data-xa]");if(!c)return;
    if(c.checked)XPSEL[c.dataset.xa]=1;else delete XPSEL[c.dataset.xa];
    sv("dq_xp_sel",XPSEL);xpSelCount()};
  $("xp_dur").onclick=function(e){var b=e.target.closest("[data-xk]");if(!b||b.disabled)return;
    var list=xpSelected().map(function(a){return {acc:a.name,key:b.dataset.xk,xp:1,cur:XPCUR}});
    if(!list.length){toast("Chua tick acc nao");return}
    xpModClose();buySend(list)};
}
function xpSelected(){return buyAccs().filter(function(a){return XPSEL[a.name]})}
function xpSelCount(){var el=$("xp_sel");if(el)el.textContent=xpSelected().length+" acc duoc chon";xpDur()}
function xpList(){
  var box=$("xp_ls");if(!box)return;
  var accs=buyAccs();
  box.innerHTML=accs.length?accs.map(function(a){
    var l=xpLeft(a),on=!!XPSEL[a.name];
    return "<label style=\"display:flex;align-items:center;gap:8px;padding:6px 2px;cursor:pointer;border-bottom:1px solid var(--bd)\">"
      +"<input type=\"checkbox\" data-xa=\""+esc(a.name)+"\""+(on?" checked":"")+">"
      +"<b style=\"flex:1;overflow:hidden;text-overflow:ellipsis\">"+esc(displayName(a))+"</b>"
      +(l>0?"<span class=\"bg on\" style=\"color:#22d3ee;border-color:#22d3ee;white-space:nowrap\">⚡ con "+xpTime(l)+"</span>":"<span class=\"mu\" style=\"font-size:12px;white-space:nowrap\">khong co x2 XP</span>")
      +"</label>";
  }).join(""):"<div class=\"mu\" style=\"padding:10px\">Chua co acc online</div>";
  xpSelCount();
}
function xpDur(){
  var g=$("xp_cg"),m=$("xp_cm"),box=$("xp_dur");if(!box)return;
  g.className="g"+(XPCUR==="gold"?" on":"");m.className="m"+(XPCUR==="gems"?" on":"");
  var n=xpSelected().length,col=XPCUR==="gems"?"#c084fc":"#facc15";
  box.innerHTML=XP.map(function(x){
    var a0=xpSelected()[0],pr=a0?(XPCUR==="gems"?gemsPrice(a0,x.k):buyPrice(a0,x)):null;
    return "<button type=\"button\" class=\"bt\" data-xk=\""+x.k+"\""+(n?"":" disabled")+" style=\"border-color:"+col+";color:"+col+";background:"+col+"1f\">"+x.i+" "+x.n+"<br><small>"+(pr!==null?(XPCUR==="gems"?"💎 ":"💰 ")+sh(pr):(XPCUR==="gems"?"💎 Gems":"💰 Gold"))+(n?" · "+n+" acc":"")+"</small></button>";
  }).join("");
}
function xpModOpen(){xpBuild();$("xp_mod").classList.add("s");xpList()}
function xpModClose(){$("xp_mod").classList.remove("s")}
function timeOf(ts){var d=new Date(ts||0),p=function(n){return n<10?"0"+n:""+n};return p(d.getHours())+":"+p(d.getMinutes())+":"+p(d.getSeconds())}
function buyKeyName(j){var k=GM[j.key]?j.key:(j.keyObj?("xp"+(j.keyObj.id||1)):j.key);var g=GM[k],isx=!!j.keyObj;return {i:g?g.i:"🛒",n:g?(isx?"x2 XP "+g.n:g.n):k,c:g?g.c:"#8a919c"}}
function buyLog(){
  var el=$("buy_log");if(!el)return;
  var h=BBUY.length?BBUY.slice().reverse().map(function(j){
    var g=buyKeyName(j),st=BBST[j.status]||[j.status,"#8a919c"];
    var extra=j.keyObj?" ("+(j.keyObj.currency==="gems"?"💎 Gems":"💰 Gold")+")":"";
    return "<div><span class=\"mu\">["+timeOf(j.createdAt)+"]</span> <b>"+esc(tpName(j.acc))+"</b> · "+g.i+" "+esc(g.n)+extra
      +" <span class=\"tst\" style=\"color:"+st[1]+";border-color:"+st[1]+"\">"+esc(st[0])+"</span>"
      +(j.msg?" <span class=\"mu\">"+esc(j.msg)+"</span>":"")+"</div>";
  }).join(""):"<div class=\"mu\">Chua co lenh mua. Bam 🛒 Mua o cot gamepass hoac ⚡ Mua x2 XP.</div>";
  if(h!==BBYLast){BBYLast=h;el.innerHTML=h}
}
function buyPoll(){return tapi("/api/trade/buy/list").then(function(j){if(j&&j.jobs){BBUY=buyMerge(j.jobs);sv("dq_buy_log",BBUY);buyLog()}}).catch(function(){})}
function buySend(list){
  if(!list.length){toast("Khong co gi de mua");return Promise.resolve()}
  if(BBYBusy)return Promise.resolve();
  BBYBusy=true;buyRender(true);
  return Promise.all(list.map(function(x){
    return tapi("/api/trade/buy/create",x.xp?{acc:x.acc,key:x.key,currency:x.cur}:{acc:x.acc,key:x.key}).then(function(j){return j&&j.ok?1:0},function(){return 0})
  })).then(function(r){
    BBYBusy=false;
    var ok=r.reduce(function(a,b){return a+b},0);
    if(ok)toast("Da gui lenh mua cho "+ok+" acc");else toast("Khong gui duoc lenh mua");
    buyPoll();buyRender(true);try{poll(true)}catch(x){}
  });
}
buyRootHTML();
$("buy_go").onclick=function(){
  var list=[];
  buyAccs().forEach(function(a){GP.forEach(function(g){if(!buyOwned(a,g.k))list.push({acc:a.name,key:g.k})})});
  buySend(list)};
$("buy_xp_go").onclick=function(){xpModOpen()};
$("buy_lclr").onclick=function(){BBUY=BBUY.filter(function(j){return j.status==="queued"||j.status==="running"});sv("dq_buy_log",BBUY);BBYLast="";buyLog();tapi("/api/trade/buy/clear",{}).then(function(){buyPoll()})};
$("buy_tb").onclick=function(e){
  var b=e.target.closest("[data-gk]");if(!b)return;
  var parts=b.dataset.gk.split("|");
  buySend([{acc:parts[0],key:parts[1]}])};
$("buy_q").oninput=function(){buyRender(true)};
setInterval(function(){if(document.hidden||!buyVisible())return;BBTick++;buyPoll();buyRender()},1000);
$("tabs").addEventListener("click",function(e){var b=e.target.closest(".tab");if(b&&b.dataset.tab==="buy")setTimeout(function(){buyPoll();buyRender(true)},0)});
buyLog();buyPoll();buyRender(true);

// ---- gan vao vong ve cua dashboard ----
function syncDl(){
  var h=data.map(function(a){return '<option value="'+esc(a.name)+'">'}).join('');
  if(dl.dataset.h!==h){dl.innerHTML=h;dl.dataset.h=h}
}
var _r=render;
render=function(){
  _r();
  try{
    var names={};data.forEach(function(a){names[a.name]=1;
      if(a.job&&(!TJ[a.name]||(a.job.updatedAt||0)>=(TJ[a.name].updatedAt||0)))TJ[a.name]=a.job});
    Object.keys(TJ).forEach(function(k){if(!names[k])delete TJ[k]});
    // moi buoc chay rieng trong try: 1 ham loi (vd panel/syncTp) KHONG duoc chan bsSync -> tab Sell luon ve lai ngay khi co du lieu moi
    try{bsSync()}catch(x){console.error('bsSync',x)}
    try{syncDl()}catch(x){}
    try{trackJobs()}catch(x){}
    try{panel()}catch(x){}
    try{syncTp()}catch(x){}
    try{histRender()}catch(x){}
    try{buyRender()}catch(x){}
    try{if(mAcc)bar()}catch(x){}
  }catch(e){}
};
setTab(ld('dq_tab','tracker'));if(tpane.style.display!=='none')histLoad();if(bsVisible())bsPoll();
// Hien ngay du lieu cu da luu (khong doi poll dau tien / script gui len); poll that se ghi de ngay sau do
(function(){try{if(data&&data.length)return;var c=localStorage.getItem('dq_acc_cache2');if(!c)return;var accs=(JSON.parse(c).accounts)||[];if(!accs.length)return;var t=Date.now();accs.forEach(function(a){a._t=a.online?t:(a.updated||0)});data=accs;render()}catch(e){}})();
setInterval(function(){if(document.hidden)return;if(Object.keys(TJ).some(function(k){return isAct(TJ[k])}))refreshJobs()},2000);
render();
})();
`.replace('__TRADE_TYPES__', () => JSON.stringify([...TRADE_TYPES]));
const LUA_FULL = (() => {
  const anchor = 'task.spawn(function()\n    while true do\n        local tickAt = os.clock()\n        sendWithRetry()';
  const i = LUA.indexOf(anchor);
  if (i < 0) { console.error('[!] Khong tim thay diem chen module trade trong script Lua -> trade TAT.'); return LUA; }
  return LUA.slice(0, i) + TRADE_LUA + LUA.slice(i);
})();

const GOLD_WINDOW = 864e5;   // tong gold chi tinh trong 24h gan nhat, qua 1 ngay tu dong bi xoa
function goldEvParse(raw) { try { const a = JSON.parse(raw); if (Array.isArray(a)) { const cut = Date.now() - GOLD_WINDOW; return a.filter(e => Array.isArray(e) && e[0] > cut && e[1] > 0); } } catch (e) {} return []; }
const goldTotal24 = () => (trade.goldEv || []).reduce((t, e) => t + (e[0] > Date.now() - GOLD_WINDOW ? e[1] : 0), 0);
async function loadTrade(o, withHist) {
  if (!o || trade.loaded) return;
  const cmds = [['HGETALL', 'dq:trade:' + o]];
  if (withHist) cmds.push(['HGETALL', 'dq:tradehist:' + o]);
  cmds.push(['GET', 'dq:goldev:' + o]);   // cac lan trade gold ve main: [[thoi diem, gold]], chi giu 24h   // chi nap lich su khi web hoi (khong nap o poll/report cua script Lua -> do ton Redis)
  const out = await redis(cmds), h = out[0];
  const raw = pairs(h); snap.trade = raw;
  trade = { owner: o, jobs: parseAll(raw), loaded: true, hist: withHist ? parseAll(pairs(out[1])) : {}, histNew: {}, histDel: [], goldEv: goldEvParse(out[out.length - 1]), goldDirty: false };
  const cut = Date.now() - TRADE_KEEP;
  for (const [k, j] of Object.entries(trade.jobs)) if (!j || (j.updatedAt || 0) < cut) delete trade.jobs[k];
}
// ===== LICH SU TRADE: moi lenh ket thuc (done/failed/cancelled/expired) duoc luu vao dq:tradehist:<owner> = hash { <id lenh>: JSON } =====
const TRADE_HIST_MAX = Math.max(50, +process.env.TRADE_HIST_MAX || 300);   // so dong lich su toi da
const TRADE_HIST_KEEP = 30 * 864e5;                                         // giu lich su 30 ngay
function histPush(j) {
  if (!trade.loaded || !j || !j.id) return;
  if (!trade.histNew) trade.histNew = {};
  trade.histNew[j.id] = {
    id: j.id, alt: j.alt, main: j.main, mode: j.mode, gold: Math.max(0, Math.floor(+j.gold || 0)), goldAll: !!j.goldAll, goldSent: Math.max(0, Math.floor(+j.goldSent || 0)), rarity: j.rarity || [], names: j.names || [], max: j.max, dry: !!j.dry, ret: !!j.ret, auto: !!j.auto,
    items: (j.items || []).map(i => ({ n: i.n, t: i.t, r: i.r || '', e: i.e || '', q: i.q })),
    status: j.status, msg: s(j.msg, 300), sent: j.sent || 0, backN: j.backN | 0, createdAt: j.createdAt || 0, finishedAt: j.updatedAt || Date.now()
  };
}
const tradeActive = j => !!j && (j.status === 'queued' || j.status === 'running') && Date.now() - (j.updatedAt || 0) < TRADE_TTL;
const sameMain = (a, b) => String(a.main).toLowerCase() === String(b.main).toLowerCase();
const mainBusy = j => Object.values(trade.jobs).some(o => o && o !== j && o.status === 'running' && tradeActive(o) && sameMain(o, j));
const hasJob = nm => Object.prototype.hasOwnProperty.call(trade.jobs, nm);
// ===== TU DONG TRA LAI: main tu them N mon Common vao CHINH phien trade dang mo voi alt (xem incoming.back) =====
// Khong con ban ghi "no tra" / lenh trade rieng nua: server chi tinh san so luong (back) va gui kem trong incoming
// cua /api/trade/poll; viec doc kho + them item do script Lua ben MAIN tu lam ngay trong luc alt dang dua do.
const BACK_DEFAULT = 0, BACK_MAX = TRADE_MAX_ITEMS;
function expireJobs() {
  if (!trade.loaded) return;
  const now = Date.now();
  for (const j of Object.values(trade.jobs)) {
    if (j && j.status === 'queued') {   // lenh chua duoc thuc hien: dem 10s tu luc tao; khi main dang ban trade voi alt khac thi tam dung dem (cho toi luot)
      if (!j.since) j.since = j.createdAt || now;
      if (mainBusy(j)) j.since = now;
      else if (now - j.since >= TRADE_QUEUE_TTL && !(j.moveUntil && now < j.moveUntil)) {
        j.msg = 'Đã xoá: acc không thực hiện lệnh sau ' + Math.round(TRADE_QUEUE_TTL / 1000) + 's (script có đang chạy không?)';
        j.status = 'expired'; j.updatedAt = now; histPush(j); bumpTradeVer();
        continue;
      }
    }
    if (j && (j.status === 'queued' || j.status === 'running') && now - (j.updatedAt || 0) >= TRADE_TTL) {
      j.msg = j.status === 'queued' ? 'Hết hạn: acc không nhận lệnh (script có đang chạy không?)' : 'Hết hạn: acc không báo kết quả';
      j.status = 'expired'; j.updatedAt = now; histPush(j);
    }
  }
}
const publicJob = j => ({ id: j.id, alt: j.alt, main: j.main, mode: j.mode, gold: Math.max(0, Math.floor(+j.gold || 0)), rarity: j.rarity || [], names: j.names || [], max: j.max, dry: !!j.dry, items: j.items || [], back: j.ret ? 0 : Math.min(BACK_MAX, j.back | 0), backN: j.ret ? 0 : (j.backN | 0) });
// ===== BAN DO TUC THI (tab Buyer/Sell) =====
// Web tao lenh ban cho 1 acc dang online (danh sach item da chon). Lenh giu trong BO NHO (khong ghi Redis/data.json: chi song vai chuc giay)
// va duoc day xuong acc qua tradeInfoFor().sell (long-poll /api/trade/wait duoc danh thuc NGAY bang bumpTradeVer).
// Lua ban qua remotes.sellItemEvent roi bao ket qua ve /api/trade/sell/status. KHONG tu dong: chi ban khi bam nut tren web.
const GAMEPASS_KEYS = new Set(["vip", "gold2", "drop", "freeResets", "xp1", "xp2", "xp3"]);   // 4 GP chinh + 3 moc XP Boost
const XP_KEY_ID = { xp1: 1, xp2: 2, xp3: 3 };
const SELL_MAX_ITEMS = 500;          // toi da item / 1 lenh
const SELL_TTL = 45e3;               // acc khong nhan / khong bao ket qua sau ngan nay -> het han
const SELL_LOG_MAX = 1000;           // log ban do KHONG tu xoa theo thoi gian nua; chi xoa khi bam "Xoa log" (toi da ngan nay lenh gan nhat de tranh day bo nho)
// sellStore/buyStore: TRUOC DAY chi giu trong RAM (bo nho tien trinh) - tren Vercel moi request co the roi vao
// MAY CHU (serverless instance) KHAC NHAU, nen 1 lenh tao o may A co the "bien mat" khi web poll trung may B, roi
// "tu nhien hien thanh cong" khi roi lai may A, hoac mat han khi may do bi Vercel thu hoi. Day la nguyen nhan that
// cua loi "bam Ban ngay -> hien dang ban -> mat -> hien thanh cong -> tu dong xoa". Fix: luu qua Redis (dq:sell:<owner>
// / dq:buy:<owner>, 1 key JSON cho ca box) - nap dau request lien quan, ghi lai cuoi request neu co thay doi (dirty).
const sellStore = new Map();
const buyStore = new Map();          // owner -> { jobs: [], loaded, dirty } don gian: moi acc 1 lenh mua
const buyBox = o => { let b = buyStore.get(o); if (!b) buyStore.set(o, b = { jobs: [], loaded: false, dirty: false }); return b; };
const sellBox = o => { let b = sellStore.get(o); if (!b) sellStore.set(o, b = { jobs: [], loaded: false, dirty: false }); return b; };
async function loadBuyBox(o) {
  if (buyBox(o).loaded) return;
  let jobs = []; try { const [raw] = await redis([['GET', 'dq:buy:' + o]]); jobs = raw ? JSON.parse(raw) : []; } catch (e) { jobs = []; }
  buyStore.set(o, { jobs, loaded: true, dirty: false });
}
async function loadSellBox(o) {
  if (sellBox(o).loaded) return;
  let jobs = []; try { const [raw] = await redis([['GET', 'dq:sell:' + o]]); jobs = raw ? JSON.parse(raw) : []; } catch (e) { jobs = []; }
  sellStore.set(o, { jobs, loaded: true, dirty: false });
}
const BUY_TTL = 5e3;
function buyExpire(b) {
  const now = Date.now();
  for (const j of b.jobs) if ((j.status === 'queued' || j.status === 'running') && now - (j.sentAt || j.createdAt) > BUY_TTL) {
    j.status = 'expired'; j.msg = 'Chưa hoàn thành: acc không nhận lệnh trong 5s'; j.updatedAt = now; b.dirty = true;
  }
  const n0 = b.jobs.length; b.jobs = b.jobs.filter(j => now - j.createdAt < 10 * 6e4).slice(-200); if (b.jobs.length !== n0) b.dirty = true;
}
function sellExpire(b) {
  const now = Date.now();
  for (const j of b.jobs) if ((j.status === 'queued' || j.status === 'running') && now - j.createdAt > SELL_TTL) {
    j.status = 'expired'; j.msg = 'Hết hạn: acc không nhận/không báo kết quả (script Lua có đang chạy không?)'; j.updatedAt = now; b.dirty = true;
  }
  const n0 = b.jobs.length; b.jobs = b.jobs.slice(-SELL_LOG_MAX); if (b.jobs.length !== n0) b.dirty = true;
}
// Lua bao da ban nhung kho moi chua len (fresh=false) -> tu go item da ban khoi kho luu tren server, web khong con hien item cu.
// Bao cao kho that cua acc (/api/report) se ghi de len ngay sau do.
// Tombstone: nho nhung item vua go de loai khoi bao cao kho CU con sot (Lua gui chen kho cu). Het han sau SOLD_TOMB_UPDATES lan bao cao kho -> xoa.
// Du lieu truyen lien tuc (lost, tombstone do vua ban...) het han sau STALE_UPDATES lan cap nhat. Drop (co dinh) KHONG bi xoa.
const STALE_UPDATES = Math.max(1, +process.env.STALE_UPDATES || +process.env.SOLD_TOMB_UPDATES || 2);
const SOLD_TOMB_UPDATES = STALE_UPDATES;
const ONLINE_MS = Math.max(4000, +process.env.ONLINE_MS || 12000);   // acc coi la ONLINE neu co tin trong ngan nay ms (tang tu 4s -> 12s de khong chap chon khi server/mang tre)
const SOLD_TOMB_MS = Math.max(0, +process.env.SOLD_TOMB_MS || 30000);   // game (reloadInvy) cap nhat kho TRE vai giay sau khi ban -> giu tombstone theo THOI GIAN, khong chi theo so lan bao cao
// So sanh stats "du linh hoat": truoc day doi JSON.stringify khop TUYET DOI, nen 2 ban ghi cua CUNG 1 item (1 ben la
// item client da echo luc bam ban, 1 ben la bao cao that moi nhat tu Lua) chi can thieu/du 1 truong (vd req/upgrades
// chua co o ban cu) hoac lech so le nho do lam tron la coi nhu "khac item" -> sellPrune()/tombstone khong nhan dien
// duoc, item da ban van hien lai trong kho (bug: Sell tuong ban roi nhung Trade van thay). Gio: truong nao chi co o
// 1 ben thi bo qua (khong tinh la khac), va cho sai so nho do lam tron o truong so.
function statsClose(a, b) {
  a = a || {}; b = b || {};
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  for (const k of keys) {
    const av = a[k], bv = b[k];
    if (av == null || bv == null) continue;   // 1 ben thieu truong nay -> bo qua, khong tinh la khac item
    if (typeof av !== 'number' || typeof bv !== 'number' || Math.abs(av - bv) > 1e-6) return false;
  }
  return true;
}
const sameRow = (row, it) => !(row.eq || row.n !== it.n || (row.t || '').toLowerCase() !== (it.t || '').toLowerCase() || (row.r || '') !== (it.r || '') || (row.e || '') !== (it.e || '') || !statsClose(row.s, it.s));
function applySoldTomb(prev, next) {   // prev/next: ban ghi acc truoc va moi (next.rec.inv la kho vua nhan)
  const tomb = prev && prev.soldTomb;
  if (!tomb || !Array.isArray(tomb.items) || !(tomb.left > 0 || tomb.until > Date.now()) || !next || !next.rec || !Array.isArray(next.rec.inv)) return;
  const prevInv = (prev.rec && Array.isArray(prev.rec.inv)) ? prev.rec.inv : [];
  const count = (inv, t) => inv.reduce((n, r) => n + (sameRow(r, t) ? (r.q | 0) : 0), 0);
  for (const t of tomb.items) {
    let extra = Math.min(count(next.rec.inv, t) - count(prevInv, t), t.q | 0);   // so item DA BAN ma bao cao nay van con mang ve
    for (const row of next.rec.inv) {
      if (extra <= 0) break;
      if (!sameRow(row, t)) continue;
      const take = Math.min(row.q | 0, extra);
      row.q -= take; extra -= take;
    }
  }
  next.rec.inv = next.rec.inv.filter(r => (r.q | 0) > 0);
  if (--tomb.left > 0 || tomb.until > Date.now()) next.soldTomb = tomb;   // het luot -> khong chep sang ban ghi moi = xoa
}
function sellPrune(mineObj, acc, items, sold) {
  const cur = Object.prototype.hasOwnProperty.call(mineObj, acc) ? mineObj[acc] : null;
  if (!cur || !cur.rec || !Array.isArray(cur.rec.inv)) return;
  const removed = [];
  let left = sold | 0;
  for (const it of (Array.isArray(items) ? items : [])) {
    let need = Math.min(it.q | 0, left);
    for (const row of cur.rec.inv) {
      if (need <= 0) break;
      if (row.eq || row.n !== it.n || (row.t || '').toLowerCase() !== it.t || (row.r || '') !== (it.r || '') || (row.e || '') !== (it.e || '')) continue;
      if (!statsClose(row.s, it.s)) continue;
      const take = Math.min(row.q | 0, need);
      row.q -= take; need -= take; left -= take;
      if (take > 0) removed.push({ n: it.n, t: it.t, r: it.r || '', e: it.e || '', s: it.s || {}, q: take });
    }
  }
  cur.rec.inv = cur.rec.inv.filter(r => (r.q | 0) > 0);
  { const got = removed.reduce((n, r) => n + r.q, 0); if (got < (sold | 0)) console.log('[sell] ' + acc + ': chi go duoc ' + got + '/' + (sold | 0) + ' item khoi kho server (khong khop ten/do hiem/chi so) -> item co the van hien'); }
  if (removed.length) cur.soldTomb = { left: SOLD_TOMB_UPDATES, until: Date.now() + SOLD_TOMB_MS, items: ((cur.soldTomb && cur.soldTomb.items) || []).concat(removed).slice(-60) };
  dirty = true;
}
function sellFor(name) {   // lenh ban dang cho acc nay (gui kem trong tradeInfoFor)
  const b = trade.owner && sellStore.get(trade.owner); if (!b || !b.jobs.length) return [];
  sellExpire(b);
  const now = Date.now(), out = [];
  for (const j of b.jobs) {
    if (j.acc !== name) continue;
    if (j.status === 'queued' || (j.status === 'running' && !j.ack && now - (j.sentAt || 0) > 4000)) {   // chua xac nhan -> gui lai (Lua tu chong ban 2 lan theo id)
      j.status = 'running'; j.sentAt = now; b.dirty = true; out.push({ id: j.id, items: j.items });
    }
  }
  return out;
}
const sellTopStat = i => {   // chi so cao nhat cua item (Phys/Spell) de log ghi ro da ban ban nao, vd "Jotunn Warrior Armor (5.26M)"
  const st = i.s || {}, v = Math.max(0, ...['physicalDamage', 'physicalPower', 'spellPower'].map(k => +st[k] || 0));
  if (!v) return '';
  const t = v >= 1e9 ? (v / 1e9).toFixed(2) + 'B' : v >= 1e6 ? (v / 1e6).toFixed(2) + 'M' : v >= 1e3 ? (v / 1e3).toFixed(1) + 'K' : String(v);
  return ' (' + t.replace(/\.?0+([BMK]?)$/, '$1') + ')';
};
const publicSell = j => ({
  id: j.id, acc: j.acc, total: j.total, status: j.status, msg: j.msg, sold: j.sold, createdAt: j.createdAt, updatedAt: j.updatedAt,
  what: j.items.slice(0, 3).map(i => i.n + sellTopStat(i) + (i.q > 1 ? ' ×' + i.q : '')).join(', ') + (j.items.length > 3 ? ' +' + (j.items.length - 3) + ' loại' : '')
});

// Thong tin gui kem cho 1 acc: lenh gui di (neu co) + danh sach alt dang gui toi acc nay (neu acc nay la main)
function tradeInfoFor(name) {
  if (!trade.loaded) return { job: null, incoming: [], active: false };
  expireJobs();
  const ln = String(name).toLowerCase(), incoming = [];
  let job = null, active = false;
  for (const [alt, j] of Object.entries(trade.jobs)) {
    if (!tradeActive(j)) continue;
    if (alt === name) { active = true; if (j.status === 'queued' && !mainBusy(j)) job = publicJob(j); }
    if (String(j.main).toLowerCase() === ln) { incoming.push({ alt, id: j.id, dry: !!j.dry, back: j.ret ? 0 : Math.min(BACK_MAX, j.back | 0), backItems: j.ret ? [] : (j.backItems || []), backN: j.ret ? 0 : (j.backN | 0) }); active = true; }
  }
function buyFor(name) {
  const box = buyStore.get(trade.owner);
  if (!box || !box.jobs.length) return [];
  buyExpire(box);
  const now = Date.now(), out = [];
  for (const j of box.jobs) {
    if (j.acc !== name) continue;
    if (j.status === 'queued' || (j.status === 'running' && !j.ack && now - (j.sentAt || 0) > 4000)) {
      j.status = 'running'; j.sentAt = now; box.dirty = true;
      out.push({ id: j.id, key: j.key });
    }
  }
  return out;
}
  return { job, incoming, active, sell: sellFor(name), buy: buyFor(name) };
}

// ===== DAY LENH TUC THI (long-poll) =====
// Acc (alt/main) goi GET /api/trade/wait?name=..&since=<ver>. Server GIU ket noi toi TRADE_WAIT_MS va tra loi NGAY khi web bam
// "xac nhan trade" (tao/huy lenh -> doi ver). Cung instance: bao tuc thi qua bo nho; khac instance: toi da TRADE_WAIT_REDIS_MS (1 lenh GET).
// Phan cho KHONG nam trong hang doi runExclusive nen khong chan cac request khac.
const TRADE_WAIT_MS = Math.min(Math.max(+process.env.TRADE_WAIT_MS || 7000, 1000), 55000);
const TRADE_WAIT_REDIS_MS = Math.max(+process.env.TRADE_WAIT_REDIS_MS || 1000, 250);
const tradeVerMem = new Map();
const bumpTradeVer = () => { trade.ver = String(Date.now()) + '-' + rnd(3); tradeVerMem.set(trade.owner, trade.ver); };
async function tradeWait(req, res) {
  let u; try { u = new URL(req.url, 'http://x'); } catch { u = new URL('http://x/'); }
  const nm = s(u.searchParams.get('name'), 50), since = s(u.searchParams.get('since'), 40);
  let owner = '';
  try { await runExclusive(async () => { await loadState(req); const a = authenticate(req); owner = (a && !a.denied && a.admin && a.owner) || ''; }); } catch (e) {}
  if (!owner || !nm) { req.url = '/api/trade/poll?name=' + encodeURIComponent(nm); return runExclusive(() => handleRequest(req, res)); }   // de luong thuong tra loi loi dung ma
  const curVer = async () => { const m = tradeVerMem.get(owner); if (m) return m; try { const [v] = await redis([['GET', 'dq:tradever:' + owner]]); return v == null ? '0' : String(v); } catch (e) { return '0'; } };
  let ver = await curVer(), changed = !since || ver !== since;
  if (!changed) {
    const end = Date.now() + TRADE_WAIT_MS; let lastR = Date.now();
    while (Date.now() < end) {
      const m = tradeVerMem.get(owner);
      if (m && m !== since) { ver = m; changed = true; break; }
      if (Date.now() - lastR >= TRADE_WAIT_REDIS_MS) {
        lastR = Date.now();
        try { const [v] = await redis([['GET', 'dq:tradever:' + owner]]); if (v != null && String(v) !== since) { ver = String(v); changed = true; break; } } catch (e) {}
      }
      await new Promise(r => setTimeout(r, 100));
      if (res.destroyed || res.writableEnded) return;
    }
  }
  if (!changed) return send(res, 200, { ok: true, ver, unchanged: true });
  req.url = '/api/trade/poll?name=' + encodeURIComponent(nm); req._tradeVer = ver;
  return runExclusive(() => handleRequest(req, res));
}

// ===== SSE (/api/stream): bao dashboard dang mo biet NGAY khi co account bao cao xong (xem notifyOwner) =====
// Giu ket noi MO VINH VIEN nen PHAI nam NGOAI hang doi runExclusive (giong tradeWait o tren) - neu khong se
// chan dung toan bo server (moi request khac phai doi ket noi SSE nay dong lai, ma no khong bao gio tu dong).
async function sseStream(req, res) {
  let owner = '', ok = false;
  try {
    await runExclusive(async () => {
      await loadState(req);
      const a = authenticate(req);
      if (a && !a.denied) { owner = a.owner; ok = true; }
    });
  } catch (e) {}
  if (!ok) { res.statusCode = 401; return res.end(); }
  res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.write(': ok\n\n');
  let set = sseClients.get(owner); if (!set) { set = new Set(); sseClients.set(owner, set); }
  set.add(res);
  const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 25000);   // giu ket noi song qua proxy/trinh duyet
  req.on('close', () => { clearInterval(ping); set.delete(res); if (!set.size) sseClients.delete(owner); });
}

// ===== TU DONG MUA x2 GOLD (Config > Alt gamepasses > Buy x2 Gold (auto)) =====
// Dung lai y het lenh mua cua tab Buy (/api/trade/buy/create, key 'gold2'): acc nao dang bao cao, chua co x2 Gold va du gold (+ goldReserve) thi tao 1 lenh.
// Bat/tat bang data\tracker_autobuy.json {gold2:true|false} (chinh trong tab Config). Chi chay khi tracker doc duoc workspace.
const AUTOBUY_COOLDOWN = 90e3, AUTOBUY_FAIL_COOLDOWN = 5 * 6e4;
let autoBuyCfg = { at: 0, on: false, reserve: 0 };
function autoBuyCfgGet() {
  const now = Date.now();
  if (now - autoBuyCfg.at < 4000) return autoBuyCfg;
  autoBuyCfg = { at: now, on: false, reserve: 0 };
  try {
    if (!CFG_DIR) return autoBuyCfg;
    const ab = pdRead('autobuy'), gp = pdRead('altpass');
    autoBuyCfg.on = !!(ab && ab.value && ab.value.gold2 === true);
    const r = gp && gp.value && +gp.value.goldReserve;
    autoBuyCfg.reserve = Number.isFinite(r) && r > 0 ? r : 0;
  } catch (e) {}
  return autoBuyCfg;
}
const autoBuyLast = new Map();   // owner|acc -> thoi diem tao lenh gan nhat
function autoBuyGold2(owner, name, rec) {
  try {
    if (!owner || !rec) return;
    const cfg = autoBuyCfgGet(); if (!cfg.on) return;
    if (rec.gpass && rec.gpass.gold2 === true) return;
    const price = rec.gpassPrices && rec.gpassPrices.gold2;
    if (typeof price !== 'number' || !Number.isFinite(price) || price < 0) return;
    if (!(rec.gold >= price + cfg.reserve)) return;
    const now = Date.now(), k = owner + '|' + name;
    const box = buyBox(owner); buyExpire(box);
    const last = box.jobs.filter(j => j.acc === name && j.keyName === 'gold2').sort((x, y) => y.createdAt - x.createdAt)[0];
    if (last && (last.status === 'queued' || last.status === 'running')) return;
    if (now - (autoBuyLast.get(k) || 0) < (last && last.status === 'failed' ? AUTOBUY_FAIL_COOLDOWN : AUTOBUY_COOLDOWN)) return;
    autoBuyLast.set(k, now);
    box.jobs.push({ id: now.toString(36) + rnd(3).replace(/[^A-Za-z0-9]/g, ''), acc: name, key: 'gold2', keyName: 'gold2', status: 'queued', msg: 'Tự động (Config)', createdAt: now, updatedAt: now, sentAt: 0, ack: false, seen: false, auto: true });
    bumpTradeVer();
  } catch (e) { console.error('[autobuy loi]', e && e.message); }
}

function tradeApi(req, res, p, u, a, mine) {
  if (p === '/api/trade/list' && req.method === 'GET') {
    expireJobs();
    return send(res, 200, { ok: true, jobs: trade.jobs, types: [...TRADE_TYPES], goldTotal: goldTotal24() });
  }
  if (p === '/api/trade/history' && req.method === 'GET') {
    expireJobs();
    const now = Date.now(), all = Object.assign({}, trade.hist || {}, trade.histNew || {});
    let list = Object.values(all).filter(e => e && e.id).sort((x, y) => (y.finishedAt || 0) - (x.finishedAt || 0));
    const drop = list.filter((e, i) => i >= TRADE_HIST_MAX || now - (e.finishedAt || 0) > TRADE_HIST_KEEP);   // don dep dong qua cu / qua nhieu
    if (drop.length) { const ds = new Set(drop.map(e => e.id)); trade.histDel = (trade.histDel || []).concat([...ds]); list = list.filter(e => !ds.has(e.id)); }
    return send(res, 200, { ok: true, history: list });
  }
  if (p === '/api/trade/buy/list' && req.method === 'GET') {
    const box = buyStore.get(a.owner); if (!box) return send(res, 200, { ok: true, jobs: [] });
    buyExpire(box);
    return send(res, 200, { ok: true, jobs: box.jobs.slice(-60).map(j => ({ id: j.id, acc: j.acc, key: j.keyName || (typeof j.key === 'string' ? j.key : 'xp'), keyObj: typeof j.key === 'object' ? j.key : null, status: j.status, msg: j.msg || '', createdAt: j.createdAt, updatedAt: j.updatedAt })) });
  }
  if (p === '/api/trade/sell/list' && req.method === 'GET') {
    const box = sellBox(a.owner); sellExpire(box);
    return send(res, 200, { ok: true, jobs: box.jobs.slice(-SELL_LOG_MAX).reverse().map(publicSell) });
  }
  if (p === '/api/trade/where' && req.method === 'GET') {   // alt hoi: acc chinh dang o server (jobId) nao
    const nm = s(u.searchParams.get('name'), 50).toLowerCase();
    const key = Object.keys(mine).find(k => k.toLowerCase() === nm), ent = key && mine[key];
    if (!ent || !ent.rec) return send(res, 404, { error: 'not_found' });
    return send(res, 200, { ok: true, placeId: ent.rec.placeId || null, jobId: ent.rec.jobId || null, fresh: Date.now() - (ent.updated || 0) < 3e5 });
  }
  if (p === '/api/trade/poll' && req.method === 'GET') {
    const nm = s(u.searchParams.get('name'), 50);
    if (!nm) return send(res, 400, { error: 'no name' });
    return send(res, 200, { ok: true, trade: tradeInfoFor(nm), ver: req._tradeVer });
  }
  if (req.method !== 'POST') return send(res, 404, { error: 'not found' });
  return readJson(req, res, b => {
    const now = Date.now();
    // --- BAN DO TUC THI (tab Buyer/Sell) ---
    // --- MUA GAMEPASS / BOOST (tab Buy) ---
    if (p === '/api/trade/buy/create') {
      const acc = s(b.acc, 50);
      if (!acc || !Object.prototype.hasOwnProperty.call(mine, acc)) return send(res, 404, { error: 'no_acc', message: 'Không tìm thấy account này.' });
      const _kraw = b.key && typeof b.key === 'object' ? b.key : null;
      const key = s(_kraw ? _kraw.key : b.key, 30);
      if (!GAMEPASS_KEYS.has(key)) return send(res, 400, { error: 'bad_key', message: 'Loại mua không hợp lệ.' });
      const isXp = Object.prototype.hasOwnProperty.call(XP_KEY_ID, key);
      const currency = isXp ? ((b.currency === 'gems' || b.currency === 'gold') ? b.currency : 'gold') : null;
      const box = buyBox(a.owner); buyExpire(box);
      const now = Date.now();
      const jobVal = isXp ? { id: XP_KEY_ID[key], currency: currency } : key;
      const job = { id: now.toString(36) + rnd(3).replace(/[^A-Za-z0-9]/g, ''), acc, key: jobVal, keyName: key, status: 'queued', msg: '', createdAt: now, updatedAt: now, sentAt: 0, ack: false, seen: false };
      box.jobs = box.jobs.filter(j => !(j.acc === acc && j.status === 'queued' && j.keyName === key && (!isXp || (j.key && j.key.currency === currency))));
      box.jobs.push(job); box.dirty = true; bumpTradeVer();
      return send(res, 200, { ok: true, id: job.id });
    }
    if (p === '/api/trade/buy/list') {
      const box = buyStore.get(a.owner); if (!box) return send(res, 200, { ok: true, jobs: [] });
      buyExpire(box);
      return send(res, 200, { ok: true, jobs: box.jobs.map(j => ({ id: j.id, acc: j.acc, key: j.keyName || (typeof j.key === 'string' ? j.key : 'xp'), keyObj: typeof j.key === 'object' ? j.key : null, status: j.status, msg: j.msg || '', createdAt: j.createdAt, updatedAt: j.updatedAt })) });
    }
    if (p === '/api/trade/sell/create') {
      const acc = s(b.acc, 50);
      if (!acc || !Object.prototype.hasOwnProperty.call(mine, acc)) return send(res, 404, { error: 'no_acc', message: 'Không tìm thấy account này.' });
      const items = [];
      for (const it of (Array.isArray(b.items) ? b.items : []).slice(0, SELL_MAX_ITEMS)) {
        if (!it || typeof it !== 'object') continue;
        const t = s(it.t, 10).toLowerCase();
        if (!TRADE_TYPES.has(t)) return send(res, 400, { error: 'type_not_allowed', message: 'Loại "' + t + '" chưa được bật (đang bật: ' + [...TRADE_TYPES].join(', ') + ').' });
        const n = s(it.n, 60); if (!n) continue;
        items.push({ n, t, r: s(it.r, 20), e: s(it.e, 20), s: cleanItemStats(it.s) || {}, q: clampInt(it.q, 1, SELL_MAX_ITEMS, 1) });
      }
      const total = items.reduce((x, i) => x + i.q, 0);
      if (!total) return send(res, 400, { error: 'no_items', message: 'Chưa chọn item nào.' });
      const box = sellBox(a.owner); sellExpire(box);
      const job = { id: now.toString(36) + rnd(3).replace(/[^A-Za-z0-9]/g, ''), acc, items, total, status: 'queued', msg: '', sold: 0, createdAt: now, updatedAt: now };
      box.jobs.push(job); box.dirty = true; bumpTradeVer();   // danh thuc long-poll cua cac acc -> acc nay nhan lenh ngay
      // go item khoi kho luu tren server NGAY (web hien tuc thi, khong doi game bao cao). Game se gui kho THAT len ngay sau khi ban -> ghi de/khoi phuc neu ban that bai.
      sellPrune(mine, acc, items, total); job.pruned = true;
      return send(res, 200, { ok: true, id: job.id, total });
    }
    if (p === '/api/trade/buy/status') {
      const box = buyStore.get(a.owner), nm = s(b.name, 50), id = s(b.id, 40);
      const j = box && box.jobs.find(x => x.id === id && x.acc === nm);
      if (!j) return send(res, 404, { error: 'no_job' });
      const st = s(b.status, 12);
      if (st !== 'running' && st !== 'done' && st !== 'failed') return send(res, 400, { error: 'bad_status' });
      if (j.status === 'done' || j.status === 'failed' || j.status === 'expired') return send(res, 200, { ok: true });
      j.ack = true; j.status = st; j.msg = s(b.msg, 200); j.updatedAt = Date.now(); box.dirty = true;
      bumpTradeVer();
      return send(res, 200, { ok: true, status: j.status });
    }
    if (p === '/api/trade/sell/status') {   // script Lua bao ket qua
      const box = sellStore.get(a.owner), nm = s(b.name, 50), id = s(b.id, 40);
      const j = box && box.jobs.find(x => x.id === id && x.acc === nm);
      if (!j) return send(res, 404, { error: 'no_job' });
      const st = s(b.status, 12);
      if (st !== 'running' && st !== 'done' && st !== 'failed') return send(res, 400, { error: 'bad_status' });
      if (j.status === 'done' || j.status === 'failed') return send(res, 200, { ok: true });   // da xong -> bo qua bao cao tre
      j.ack = true; j.status = st; j.msg = s(b.msg, 200); j.sold = clampInt(b.sold, 0, SELL_MAX_ITEMS, j.sold | 0); j.updatedAt = now; box.dirty = true;
      if (st !== 'failed') { const ct = Object.prototype.hasOwnProperty.call(mine, j.acc) ? mine[j.acc] : null; if (ct && ct.soldTomb) { ct.soldTomb.until = Date.now() + SOLD_TOMB_MS; dirty = true; } }   // tinh 30s tu luc ban that, khong tu luc tao lenh
      if (st === 'done' && j.sold > 0 && !j.pruned && b.fresh !== true) sellPrune(mine, j.acc, j.items, j.sold);
      // ban that bai / chi ban 1 phan -> bo tombstone de kho that (Lua vua gui) khong bi an nham
      if (st === 'failed' || (st === 'done' && j.sold < j.total)) { const ca = Object.prototype.hasOwnProperty.call(mine, j.acc) ? mine[j.acc] : null; if (ca) { delete ca.soldTomb; dirty = true; } }
      return send(res, 200, { ok: true });
    }
    if (p === '/api/trade/buy/clear') {   // xoa log cac lenh mua da ket thuc
      const box = buyStore.get(a.owner); if (box) { box.jobs = box.jobs.filter(j => j.status === 'queued' || j.status === 'running'); box.dirty = true; }
      return send(res, 200, { ok: true });
    }
    if (p === '/api/trade/sell/clear') {   // xoa log cac lenh da ket thuc
      const box = sellBox(a.owner); box.jobs = box.jobs.filter(j => j.status === 'queued' || j.status === 'running'); box.dirty = true;
      return send(res, 200, { ok: true });
    }
    // --- script Lua bao trang thai ---
    if (p === '/api/trade/status') {
      const nm = s(b.name, 50), j = hasJob(nm) ? trade.jobs[nm] : null;
      if (!j || j.id !== s(b.id, 20)) return send(res, 404, { error: 'no_job' });
      if (b.status === 'requeue') {   // alt khac server main -> dua lenh ve hang doi trong luc teleport (toi da 3 lan)
        if (j.status !== 'queued' && j.status !== 'running') return send(res, 409, { error: 'not_active' });
        if ((j.moves | 0) >= 3) return send(res, 409, { error: 'too_many_moves' });
        Object.assign(j, { moves: (j.moves | 0) + 1, status: 'queued', msg: s(b.msg, 300) || 'Đang chuyển alt sang server của main...', since: now, moveUntil: now + TRADE_MOVE_MS, updatedAt: now });
        bumpTradeVer();
        return send(res, 200, { ok: true });
      }
      let stt = ['running', 'done', 'failed'].includes(b.status) ? b.status : null;
      if (!stt) return send(res, 400, { error: 'bad status' });
      // Don gian hoa ket qua: trade da dong thi coi la thanh cong, KHONG dua vao so sanh kho cua script (alt nhan do tu main nen kho khong giam tuong ung).
      // Chuan hoa o server de dung ca khi acc con chay ban script cu (van gui "kho khong doi" / "Chi gui duoc x/y").
      let bMsg = s(b.msg, 300), bSent = b.sent;
      const jobTotal = (j.items || []).reduce((x, i) => x + (i.q | 0), 0) || (j.max | 0);
      if (!j.dry && stt === 'failed' && /kho không đổi/.test(bMsg)) { stt = 'done'; bSent = jobTotal; bMsg = 'Đã gửi ' + jobTotal + ' item'; }
      else if (!j.dry && stt === 'done' && /^Chỉ gửi được/.test(bMsg)) { const k = bMsg.indexOf(' · main trả'); bSent = jobTotal; bMsg = 'Đã gửi ' + jobTotal + ' item' + (k >= 0 ? bMsg.slice(k) : ''); }
      if (stt === 'running') {
        if (j.status !== 'queued' && j.status !== 'running') return send(res, 409, { error: 'not_active' });   // da huy / het han
        if (mainBusy(j)) return send(res, 409, { error: 'main_busy' });
      }
      const wasDone = j.status === 'done';
      j.status = stt; j.msg = bMsg; j.updatedAt = now;
      if (stt === 'done' && !j.dry && !j.ret && !wasDone) {   // cong don gold THUC SU da gui vao main
        let gRaw = b.goldSent;
        if (gRaw === undefined || gRaw === null) { const gm = String(b.msg || '').match(/(\d{1,16}) gold/); if (gm) gRaw = gm[1]; }   // script Lua cu khong gui goldSent -> lay tu dong thong bao "Da gui ... N gold"
        const gs = clampInt(gRaw, 0, TRADE_GOLD_MAX, 0);
        if (gs > 0) { j.goldSent = gs; const cutG = now - GOLD_WINDOW; trade.goldEv = (trade.goldEv || []).filter(e => e[0] > cutG).concat([[now, gs]]).slice(-500); trade.goldDirty = true; }
      }
      if (bSent !== undefined) j.sent = clampInt(bSent, 0, 999, 0);
      if (stt === 'done' && !j.dry && !j.ret && (j.back | 0) > 0 && (j.sent | 0) > 0) {   // main da tu them Common ngay trong phien trade nay (xem incoming.back)
        j.msg = s(String(j.msg || '') + ' · kèm main trả lại tối đa ' + Math.min(BACK_MAX, j.back | 0) + ' Common', 300);
      }
      if (stt !== 'running') { histPush(j); bumpTradeVer(); }
      return send(res, 200, { ok: true });
    }
    // --- web huy lenh ---
    if (p === '/api/trade/cancel') {
      let n = 0;
      for (const raw of (Array.isArray(b.alts) ? b.alts : []).slice(0, 200)) {
        const nm = s(raw, 50);
        if (hasJob(nm) && tradeActive(trade.jobs[nm])) { Object.assign(trade.jobs[nm], { status: 'cancelled', msg: 'Đã huỷ từ web', updatedAt: now }); histPush(trade.jobs[nm]); n++; }
      }
      if (n) bumpTradeVer();
      return send(res, 200, { ok: true, n });
    }
    // --- web xoa lich su ---
    if (p === '/api/trade/history/clear') {
      const ids = Object.keys(trade.hist || {});
      trade.histDel = ids; trade.histNew = {};
      return send(res, 200, { ok: true, n: ids.length });
    }
    // --- web tao lenh ---
    if (p === '/api/trade/create') {
      const main = s(b.main, 20).trim();
      if (!/^[A-Za-z0-9_]{3,20}$/.test(main)) return send(res, 400, { error: 'bad_main', message: 'Tên acc chính không hợp lệ (3–20 ký tự chữ, số, _).' });
      const alts = [...new Set((Array.isArray(b.alts) ? b.alts : []).map(x => s(x, 50)).filter(Boolean))].slice(0, 100);
      if (!alts.length) return send(res, 400, { error: 'no_alts', message: 'Chưa chọn acc nào.' });
      const job = { mode: b.mode === 'items' ? 'items' : b.mode === 'gold' ? 'gold' : 'rarity', gold: b.goldAll === true ? TRADE_GOLD_MAX : clampInt(b.gold, 0, TRADE_GOLD_MAX, 0), goldAll: b.goldAll === true, main, dry: b.dry !== false, max: clampInt(b.max, 1, TRADE_MAX_ITEMS, 5), ret: b.ret === true, back: b.ret === true ? 0 : clampInt(b.back, 0, BACK_MAX, BACK_DEFAULT) };   // back = so Common main tu tra lai alt sau khi trade xong (0 = tat)   // ret = main TRA do ve alt (acc gui = main, acc nhan = alt)
      if (job.mode === 'rarity') {
        job.rarity = [...new Set((Array.isArray(b.rarity) ? b.rarity : []).map(x => String(x).toLowerCase().replace(/[^a-z]/g, '')).filter(x => TRADE_RARITIES.has(x)))];
        if (!job.rarity.length) return send(res, 400, { error: 'no_rarity', message: 'Chọn ít nhất một độ hiếm (Ult / Legend / Epic).' });
        job.names = (Array.isArray(b.names) ? b.names : []).map(x => s(x, 60).trim()).filter(Boolean).slice(0, 30);
      } else if (job.mode === 'items') {
        if (alts.length !== 1) return send(res, 400, { error: 'one_alt', message: 'Trade item đã chọn chỉ áp dụng cho 1 acc.' });
        job.items = [];
        for (const it of (Array.isArray(b.items) ? b.items : []).slice(0, TRADE_MAX_ITEMS)) {
          if (!it || typeof it !== 'object') continue;
          const t = s(it.t, 10).toLowerCase();
          if (!TRADE_TYPES.has(t)) return send(res, 400, { error: 'type_not_allowed', message: 'Loại "' + t + '" chưa được bật để trade (đang bật: ' + [...TRADE_TYPES].join(', ') + ').' });
          const n = s(it.n, 60); if (!n) continue;
          job.items.push({ n, t, r: s(it.r, 20), e: s(it.e, 20), s: cleanItemStats(it.s) || {}, q: clampInt(it.q, 1, TRADE_MAX_ITEMS, 1) });
        }
        const total = job.items.reduce((x, i) => x + i.q, 0);
        if (!total) return send(res, 400, { error: 'no_items', message: 'Chưa chọn item nào.' });
        job.max = Math.min(total, TRADE_MAX_ITEMS);
        // main TRA lai cac mon da chia cho alt nay NGAY TRONG CUNG PHIEN trade (khong co phien tra nguoc sau do)
        job.backItems = [];
        if (!job.ret) for (const it of (Array.isArray(b.backItems) ? b.backItems : []).slice(0, TRADE_MAX_ITEMS)) {
          if (!it || typeof it !== 'object') continue;
          const t = s(it.t, 10).toLowerCase();
          if (!TRADE_TYPES.has(t)) continue;
          const n = s(it.n, 60); if (!n) continue;
          job.backItems.push({ n, t, r: s(it.r, 20), e: s(it.e, 20), s: cleanItemStats(it.s) || {}, q: clampInt(it.q, 1, TRADE_MAX_ITEMS, 1) });
        }
        job.backN = Math.min(job.backItems.reduce((x, i) => x + i.q, 0), TRADE_MAX_ITEMS);
        if (job.backN) job.back = 0;   // da co do chia cu the thi khong tra them Common
      } else {   // mode gold: chi gui gold, khong gui item
        if (!job.gold) return send(res, 400, { error: 'no_gold', message: 'Nhập số gold cần gửi.' });
        job.back = 0; job.max = 1; job.items = [];
      }
      const results = [];
      for (const alt of alts) {
        if (!Object.prototype.hasOwnProperty.call(mine, alt)) { results.push({ alt, ok: false, error: 'not_found' }); continue; }
        if (alt.toLowerCase() === main.toLowerCase()) { results.push({ alt, ok: false, error: 'same_as_main' }); continue; }
        // 1 acc khong the vua gui vua nhan cung luc (vd: main dang nhan do tu alt thi khong tra do duoc, va nguoc lai)
        const actJobs = Object.values(trade.jobs).filter(tradeActive);
        if (actJobs.some(o => String(o.main).toLowerCase() === alt.toLowerCase())) { results.push({ alt, ok: false, error: 'sender_receiving' }); continue; }
        if (actJobs.some(o => String(o.alt).toLowerCase() === main.toLowerCase())) { results.push({ alt, ok: false, error: 'receiver_sending' }); continue; }
        if (hasJob(alt) && tradeActive(trade.jobs[alt])) { results.push({ alt, ok: false, error: 'busy' }); continue; }
        const old = hasJob(alt) ? trade.jobs[alt] : null;   // lenh cu bi treo (chua kip danh dau het han) -> luu vao lich su truoc khi bi ghi de
        if (old && (old.status === 'queued' || old.status === 'running')) { old.status = 'expired'; old.msg = 'Hết hạn: bị thay bằng lệnh mới'; old.updatedAt = now; histPush(old); }
        trade.jobs[alt] = { id: rnd(6), alt, ...job, status: 'queued', msg: '', sent: 0, createdAt: now, updatedAt: now };
        results.push({ alt, ok: true });
      }
      if (results.some(r => r.ok)) bumpTradeVer();
      console.log('[trade] ' + (a.user ? a.user.name : 'admin') + ' tao lenh ' + job.mode + ' -> ' + main + ' cho ' + results.filter(r => r.ok).length + '/' + alts.length + ' acc' + (job.dry ? ' (DRY)' : ''));
      return send(res, 200, { ok: true, results });
    }
    send(res, 404, { error: 'not found' });
  }, 4e6);
}

// ===== INVENTORY PAGE + ACCOUNT MODAL (chay trong trinh duyet; duoc chen vao dashboard bang INV_FN.toString()) =====
const INV_FN = function () {
  // Tieu de header: tach thanh icon + nhan + ten de lam khung noi bat
  var hb = document.querySelector('.w>.rw:first-child>b');
  if (hb && /Tracker By/.test(hb.textContent)) {
    hb.removeAttribute('style');
    hb.className = 'hbrand';
    hb.innerHTML = '<span class="hb-i">⚔</span><span class="hb-t"><span class="hb-l">Tracker By</span><span class="hb-n">Nguyễn Phan Trí Bảo</span></span>';
  }
  // Nut View/Hide Name len goc tren phai, thay nut sang/toi (xoa nut sang/toi)
  var ha = $('ha'), th = $('th');
  if (ha && th && th.parentNode) {
    th.parentNode.insertBefore(ha, th);
    th.remove();
  }
  // Nut Get Script canh View Name: bam 1 cai la tu copy script tracker (khong can mo popup)
  if (ha && ha.parentNode) {
    var gs = document.createElement('button');
    gs.type = 'button'; gs.id = 'gsbtn'; gs.className = 'bt'; gs.textContent = 'Get Script';
    gs.onclick = function () {
      var k = (typeof KEYV !== 'undefined' && KEYV) || '';
      var txt = '_G.Config = { Note = "PC1" }\nloadstring(game:HttpGet("' + location.origin + '/script' + (k ? '?t=' + encodeURIComponent(k) : '') + '"))()';
      var done = function (ok) { var old = gs.textContent; gs.textContent = ok ? 'Đã copy!' : 'Lỗi copy'; setTimeout(function () { gs.textContent = old; }, 1500); };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(txt).then(function () { done(true); }, function () { done(false); });
      } else {
        var ta = document.createElement('textarea'); ta.value = txt; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.select();
        var ok = false; try { ok = document.execCommand('copy'); } catch (e) {}
        document.body.removeChild(ta); done(ok);
      }
    };
    ha.insertAdjacentElement('afterend', gs);
  }
  // Logo = nut thu gon / mo rong sidebar (thay cho nut tron ‹)
  var logo = document.querySelector('.sb-logo');
  if (logo) {
    logo.title = 'Thu gọn / mở rộng menu';
    logo.onclick = function (e) {
      e.stopPropagation();
      sbCollapsed = !sbCollapsed;
      sv('dq_sb_collapsed', sbCollapsed);
      applySb();
    };
  }
  var TC = { skill: '#8a919c', mage: '#a78bfa', dmg: '#f59e0b' };
  var GL = { skill: 'Skills', mage: 'Mage', dmg: 'War' };
  var GO = { skill: 0, mage: 1, dmg: 2 };
  var tinfo = function (t) { return TIER[t] || { n: 'Chưa rõ độ hiếm', c: '#8a919c' }; };
  var qsum = function (l) { return l.reduce(function (s, i) { return s + (+i.q || 1); }, 0); };
  var gk = function (i) { return String(i.t || '').toLowerCase() === 'ability' ? 'skill' : itemStatGroup(i); };

  function statChips(i) {
    var F = [['physicalDamage', 'Phys', 'p'], ['physicalPower', 'Phys', 'p'], ['spellPower', 'Spell', 's'], ['health', 'HP', 'h']];
    var s = i.s || {}, m = i.m || {}, out = '';
    F.forEach(function (f) {
      var k = f[0];
      if (Number.isFinite(m[k]) || Number.isFinite(s[k])) {
        var v = Number.isFinite(m[k]) ? m[k] : s[k];
        out += '<span class="st2 st2-' + f[2] + '" title="' + f[1] + ': ' + fmt(v) + '"><i>' + f[1] + '</i><b>' + sh(v) + '</b></span>';
      }
    });
    return out ? '<div class="ist">' + out + '</div>' : '';
  }

  // The item card (dung chung cho popup account + trang Inventory)
  window.card = function (i) {
    var c = cc(i) || (TIER[tier(i)] || {}).c || '', u = IMG[ik(i)], t = tier(i), e = enchantCode(i);
    var L = esc(String(i.n || '?').slice(0, 1).toUpperCase());
    var img = /^https:\/\/[^\s"'<>]+$/.test(u || '')
      ? '<img src="' + esc(u) + '" alt="' + L + '" loading="lazy" referrerpolicy="no-referrer" onerror="this.replaceWith(this.alt)">'
      : L;
    var tags = '';
    if (i.eq) tags += '<span class="bg eq">⚔ Đang mang</span>';
    if (e) tags += '<span class="bg on" title="' + (e === 'EIR' ? 'Enhanced Inner Rage' : 'Enhanced Inner Focus') + '">' + e + '</span>';
    return '<div class="ic ic2"' + (c ? ' style="--rc:' + c + '"' : '') + '>'
      + '<div class="im">' + img + '</div>'
      + '<div class="icb"><div class="icn">' + esc(i.n) + '</div>'
      + '<div class="icm">×' + fmt(i.q) + (t ? ' · <span style="color:' + TIER[t].c + '">' + TIER[t].n + '</span>' : '') + '</div>'
      + statChips(i)
      + (tags ? '<div class="ict">' + tags + '</div>' : '')
      + (i.own ? '<div class="own ico" title="Account đang sở hữu">👤 ' + esc(i.own) + '</div>' : '')
      + '</div></div>';
  };

  // ---- Popup account: rong hon, header co o thong ke, body cuon rieng ----
  function sections(list) {
    return [6, 5, 4, 3, 2, 1, 0].map(function (t) {
      var ri = list.filter(function (i) { return tier(i) === t; });
      if (!ri.length) return '';
      var T = tinfo(t);
      var groups = ['skill', 'mage', 'dmg'].map(function (g) {
        var gi = ri.filter(function (i) { return gk(i) === g; });
        return gi.length
          ? '<div class="gh" style="--gc:' + TC[g] + '"><span>' + GL[g] + '</span><em>' + fmt(qsum(gi)) + ' item</em></div><div class="ig2">' + gi.map(window.card).join('') + '</div>'
          : '';
      }).join('');
      return '<section class="rs" style="--rc:' + T.c + '"><div class="rh"><b>' + T.n.toUpperCase() + '</b><span>' + fmt(qsum(ri)) + ' item</span></div>' + groups + '</section>';
    }).join('');
  }

  window.renderModal = function () {
    var a = data.find(function (x) { return x.name === mAcc; });
    if (!a) { $('am').classList.remove('s'); return; }
    var its = inv(a).slice().sort(function (x, y) { return tier(y) - tier(x) || dstat(y) - dstat(x) || x.n.localeCompare(y.n); });
    var pc = placeColor(a), total = cnt(a, function () { return true; });
    var tile = function (l, v, c) { return '<div class="mt" style="--c:' + c + '"><small>' + l + '</small><b>' + v + '</b></div>'; };
    $('ah').innerHTML = '<div class="mh"><div class="mav">' + esc(String(displayName(a)).slice(0, 1).toUpperCase()) + '</div>'
      + '<div class="mhi"><div class="mnm"><b>' + esc(displayName(a)) + '</b><span class="bg on" style="color:' + pc + ';border-color:' + pc + '">' + placeLabel(a) + '</span></div>'
      + '<div class="mu">Level ' + fmt(a.level) + ' · Gold ' + sh(a.gold) + ' · ' + fmt(total) + ' item</div></div></div>'
      + '<div class="mts">' + tile('Level', fmt(a.level), 'var(--pk)') + tile('Gold', sh(a.gold), 'var(--cy)') + tile('Item', fmt(total), 'var(--tx)')
      + [6, 5, 4].map(function (t) { return tile(TIER[t].n, fmt(tcn(a, t)), TIER[t].c); }).join('') + '</div>';
    var eqs = its.filter(function (i) { return i.eq; });
    var ab = $('ab'), st = ab.scrollTop;
    ab.innerHTML = (eqs.length ? '<section class="rs" style="--rc:#60a5fa"><div class="rh"><b>⚔ ĐANG MANG</b><span>' + eqs.length + ' item</span></div><div class="ig2">' + eqs.map(window.card).join('') + '</div></section>' : '')
      + (sections(its) || '<div class="empty">Chưa ghi nhận item nào</div>');
    ab.scrollTop = st;
    loadImgs(its);
  };

  // ---- Trang Inventory: tat ca item cua moi account ----
  var lastSig = '';
  window.renderAll = function () {
    var k = ($('iq').value || '').toLowerCase().trim(), rf = $('ir').value, tf = $('it').value, sy = window.scrollY;
    var norm = function (t) { t = String(t || '').toLowerCase(); return t === 'ability' ? 'skill' : t === 'weapon' ? 'weapon' : (t === 'chest' || t === 'helmet') ? 'armor' : t; };
    var base = data.flatMap(function (a) { return inv(a).map(function (i) { return Object.assign({}, i, { own: displayName(a) }); }); })
      .filter(function (i) { return (!hideEq || !i.eq) && (!k || (i.n + ' ' + i.own).toLowerCase().includes(k)) && (tf === '' || norm(i.t) === tf); });
    var its = base.filter(function (i) { return rf === '' || tier(i) === +rf; });
    its.sort(function (x, y) { return tier(y) - tier(x) || GO[gk(x)] - GO[gk(y)] || dstat(y) - dstat(x) || x.n.localeCompare(y.n) || x.own.localeCompare(y.own); });
    var total = its.length, shownL = its.slice(0, iShow), ct = {}, cg = {}, cb = {};
    base.forEach(function (i) { var t = tier(i); cb[t] = (cb[t] || 0) + (+i.q || 1); });
    its.forEach(function (i) { var t = tier(i), n = +i.q || 1; ct[t] = (ct[t] || 0) + n; cg[t + '|' + gk(i)] = (cg[t + '|' + gk(i)] || 0) + n; });
    var chips = '<button type="button" class="isc' + (rf === '' ? ' on' : '') + '" data-v="" style="--c:var(--tx)">Tất cả <b>' + fmt(qsum(base)) + '</b></button>'
      + [6, 5, 4, 3, 2, 1, 0].filter(function (t) { return cb[t]; }).map(function (t) {
        return '<button type="button" class="isc' + (rf === String(t) ? ' on' : '') + '" data-v="' + t + '" style="--c:' + tinfo(t).c + '">' + tinfo(t).n + ' <b>' + fmt(cb[t]) + '</b></button>';
      }).join('');
    var html = '', curT = -1, curG = '', open = false;
    shownL.forEach(function (i) {
      var t = tier(i), g = gk(i), T = tinfo(t);
      if (t !== curT) {
        if (open) html += '</div>';
        open = false; curT = t; curG = '';
        html += '<div class="rh" style="--rc:' + T.c + '"><b>' + T.n.toUpperCase() + '</b><span>' + fmt(ct[t]) + ' item</span></div>';
      }
      if (g !== curG) {
        if (open) html += '</div>';
        curG = g;
        html += '<div class="gh" style="--gc:' + TC[g] + '"><span>' + GL[g] + '</span><em>' + fmt(cg[t + '|' + g]) + ' item</em></div><div class="ig2" style="--rc:' + T.c + '">';
        open = true;
      }
      html += window.card(i);
    });
    if (open) html += '</div>';
    html = (html || '<div class="empty">Không có item</div>')
      + (total > shownL.length ? '<div class="imorew"><button class="bt" id="imore" type="button">Hiện thêm (' + fmt(total - shownL.length) + ' còn lại)</button></div>' : '');
    $('icnt').textContent = '(' + fmt(total) + ' dòng)';
    var sig = chips + '||' + html;
    if (sig === lastSig) return;
    lastSig = sig;
    $('isum').innerHTML = chips;
    $('ib').innerHTML = html;
    if (Math.abs(window.scrollY - sy) > 1) window.scrollTo(0, sy);
    loadImgs(shownL);
  };

  // ---- Bien #im (popup) thanh 1 trang (tab) rieng trong khung #tf ----
  var im = $('im'), tfr = $('tf');
  if (!im || !tfr) return;
  var box = im.firstElementChild;
  im.className = 'tabpane invpane';
  im.setAttribute('data-pane', 'inventory');
  im.style.display = 'none';
  im.onclick = null;
  box.className = 'invbox';
  box.removeAttribute('style');
  box.children[0].innerHTML = '<div class="ihd"><b>📦 Inventory</b><span class="mu" id="icnt"></span></div>';
  var tb = box.children[1];
  var stick = document.createElement('div');
  stick.className = 'istick';
  box.insertBefore(stick, tb);
  stick.appendChild(tb);
  stick.insertAdjacentHTML('beforeend', '<div class="isum" id="isum"></div>');
  tfr.appendChild(im);
  $('isum').onclick = function (e) {
    var b = e.target.closest('.isc');
    if (!b) return;
    var v = b.dataset.v;
    $('ir').value = (v !== '' && $('ir').value === v) ? '' : v;
    iShow = 300;
    renderAll();
  };
  var invOn = false;
  window.invTab = function (t) {
    invOn = t === 'inventory';
    document.body.classList.toggle('inv-on', invOn);
    im.classList.toggle('s', invOn);
    if (invOn) { iShow = 300; lastSig = ''; renderAll(); }
  };
  // Esc cu (dong popup) lam mat co "s" -> bat lai neu dang o trang Inventory
  document.addEventListener('keydown', function () { if (invOn) im.classList.add('s'); });
};
const INV_BOOT = '(' + INV_FN.toString() + ')();\n';

const DASHBOARD_HTML = (() => {
  let page = HTML;
  const replace = (before, after) => {
    if (!page.includes(before)) throw new Error('Dashboard template did not contain the expected section.');
    page = page.replace(before, after);
  };
  replace('<title>NGUYEN PHAN TRI BAO</title>', '<title>DQR TRACKER</title><script>window.__BUILD__=' + JSON.stringify(BUILD_ID) + ';</script>');
  replace('⚔️ NGUYEN PHAN TRI BAO', 'Tracker By: Nguyễn Phan Trí Bảo');
  replace("'gold2','x2 Gold'", "'gold2','Gold'");
  replace(
    '.tw{overflow-x:auto;',
    '.rarity-legend{display:flex;flex-wrap:wrap;gap:8px 12px;margin:0 0 10px}.rarity-key{display:inline-flex;align-items:center;gap:6px;color:var(--tx);font-size:12px}.rarity-dot{width:10px;height:10px;border:1px solid #ffffff55;border-radius:50%;flex:none}.online-label{font-weight:800}.online-count{font-size:clamp(38px,7vw,52px);font-weight:800;line-height:1.1;letter-spacing:-1px}.online-total{font-size:clamp(20px,3.5vw,26px);font-weight:700}.enchant-badge{display:inline-flex;align-items:center;padding:2px 7px;border:1px solid;border-radius:999px;font-size:11px;font-weight:800;line-height:1.4;white-space:nowrap}.enchant-badge.eir{color:#f87171;background:#ef444433;border-color:#ef4444}.enchant-badge.eif{color:#60a5fa;background:#3b82f633;border-color:#3b82f6}.tw{overflow-x:auto;'
  );
  replace(
    '<small class="mu">▣ Online / Total</small><div style="margin-top:14px;font-size:34px;font-weight:700"><span id="on" style="color:var(--ok)"></span> <span id="tt" class="mu" style="font-size:18px"></span></div>',
    '<small class="mu online-label">▣ Online / Total</small><div class="online-count" style="margin-top:14px"><span id="on" style="color:var(--ok)"></span> <span id="tt" class="mu online-total"></span></div>'
  );
  replace('type="number"></label></div>', 'type="text" inputmode="decimal" placeholder="Ví dụ: 1b, 100b, 1t"></label></div>');
  replace(
    "function rows(){",
    "const parseGold=v=>{const m=String(v).trim().toLowerCase().replace(/,/g,'').match(/^(\\d+(?:\\.\\d+)?)\\s*([kmbt]?)$/);if(!m)return null;const n=Number(m[1]),factor={k:1e3,m:1e6,b:1e9,t:1e12}[m[2]]||1;return Number.isFinite(n*factor)?n*factor:null};\nfunction rows(){"
  );
  replace('G<+RG.g', 'G<RG.g');
  replace(
    "['fa','fb','fg'].forEach((id,i)=>$(id).oninput=e=>{RG['abg'[i]]=e.target.value;page=1;render()});",
    "['fa','fb'].forEach((id,i)=>$(id).oninput=e=>{RG['ab'[i]]=e.target.value;page=1;render()});$('fg').oninput=e=>{const value=e.target.value.trim(),parsed=parseGold(value);RG.g=parsed===null?'':parsed;e.target.setCustomValidity(value&&parsed===null?'Nhập số Gold như 1b, 100b hoặc 1t.':'');e.target.setAttribute('aria-invalid',String(!!value&&parsed===null));page=1;render()};$('fg').onblur=e=>{if(!e.target.checkValidity())e.target.reportValidity()};"
  );
  replace(
    'const COLS=[',
    "const ecn=(a,k)=>cnt(a,i=>enchantCode(i)===k),enchantBadges=a=>{const eir=ecn(a,'EIR'),eif=ecn(a,'EIF');return [eir?'<span class=\"enchant-badge eir\" title=\"Enhanced Inner Rage\">EIR '+eir+'</span>':'',eif?'<span class=\"enchant-badge eif\" title=\"Enhanced Inner Focus\">EIF '+eif+'</span>':''].filter(Boolean).join(' ')||'<span class=\"mu\">—</span>'};const COLS=["
  );
  replace(
    "['l','Legend',a=>tcn(a,5),a=>tbd(tcn(a,5),5)],",
    "['l','Legend',a=>tcn(a,5),a=>tbd(tcn(a,5),5)],\n['enchant','Enchant',a=>ecn(a,'EIR')+ecn(a,'EIF'),a=>enchantBadges(a)],"
  );
  replace('.tw{overflow-x:auto;', ".eqw{display:flex;flex-wrap:wrap;gap:4px;min-width:230px;max-width:420px;white-space:normal}.eq-chip{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border:1px solid;border-radius:999px;font-size:11px;font-weight:700;line-height:1.5;white-space:nowrap}.eq-chip small{font-weight:500;opacity:.8;font-size:10px}.eq-chip.sk{color:var(--mu);border-color:var(--bd);background:var(--cd)}" + '.tw{overflow-x:auto;');
  replace('const COLS=[', "const eqKind=i=>{const t=String(i.t||'').toLowerCase();return t==='weapon'||t==='chest'||t==='helmet'||t==='ability'?t:'other'};\nconst EQ_ICON={weapon:'\u2694',helmet:'\u26d1',chest:'\ud83d\udee1',other:'\u2022'},EQ_ORD={weapon:0,helmet:1,chest:2,other:3};\nconst eqList=a=>inv(a).filter(i=>i.eq);\nconst eqGear=a=>eqList(a).filter(i=>eqKind(i)!=='ability').sort((x,y)=>EQ_ORD[eqKind(x)]-EQ_ORD[eqKind(y)]);\nconst eqSkill=a=>eqList(a).filter(i=>eqKind(i)==='ability');\nfunction eqCell(a){const g=eqGear(a),s=eqSkill(a);if(!g.length&&!s.length)return'<span class=\"mu\">\u2014</span>';\n const chips=g.map(i=>{const t=tier(i),c=cc(i)||(TIER[t]||{}).c||'#8a919c',d=dstat(i),e=enchantCode(i);\n  return `<span class=\"eq-chip\" style=\"color:${c};border-color:${c};background:${c}22\" title=\"${esc(i.n)}${t?' \u00b7 '+TIER[t].n:''}${d?' \u00b7 '+fmt(d):''}\"><span>${EQ_ICON[eqKind(i)]}</span>${esc(i.n)}${d?` <small>${sh(d)}</small>`:''}${e?` <small>${e}</small>`:''}</span>`}).join('');\n const sk=s.length?`<span class=\"eq-chip sk\" title=\"${esc(s.map(i=>i.n+(enchantCode(i)?' ('+enchantCode(i)+')':'')).join(', '))}\">\u2726 ${s.length} skill</span>`:'';\n return `<div class=\"eqw\">${chips}${sk}</div>`}\n" + 'const COLS=[');
  replace("['enchant','Enchant',a=>ecn(a,'EIR')+ecn(a,'EIF'),a=>enchantBadges(a)],", "['enchant','Enchant',a=>ecn(a,'EIR')+ecn(a,'EIF'),a=>enchantBadges(a)],\n['equip','Trang bị',a=>eqGear(a).length,a=>eqCell(a)],");
  replace(
    "sort={k:'name',d:1}",
    "sort={k:'level',d:-1}"
  );
  replace(
    "$('pm').innerHTML='<div class=\"mu\" style=\"max-width:300px\">",
    "$('pm').innerHTML='<div class=\"rarity-legend\" aria-label=\"Chú thích màu độ hiếm\"><span class=\"rarity-key\"><i class=\"rarity-dot\" style=\"background:#f43f5e\"></i>Ult</span><span class=\"rarity-key\"><i class=\"rarity-dot\" style=\"background:#f59e0b\"></i>Legend</span><span class=\"rarity-key\"><i class=\"rarity-dot\" style=\"background:#a78bfa\"></i>Epic</span><span class=\"rarity-key\"><i class=\"rarity-dot\" style=\"background:#60a5fa\"></i>Rare</span><span class=\"rarity-key\"><i class=\"rarity-dot\" style=\"background:#4ade80\"></i>Uncommon</span><span class=\"rarity-key\"><i class=\"rarity-dot\" style=\"background:#9ca3af\"></i>Common</span></div><div class=\"mu\" style=\"max-width:300px\">"
  );
  const explanation = page.match(/<div class="mu" style="max-width:300px">[\s\S]*?<\/div>/);
  if (!explanation) throw new Error('Rarity explanation was not found in the dashboard template.');
  page = page.replace(explanation[0], '');
  replace(
    ":'<div class=\"mu\">Chưa có màu nào được ghi nhận</div>')}",
    ":'')}"
  );
  replace(
    '<span title="Enhanced Inner Rage" style="color:var(--ok)">EIR ${en(\'EIR\')}</span>',
    '<span class="enchant-badge eir" title="Enhanced Inner Rage">EIR ${en(\'EIR\')}</span>'
  );
  replace(
    '<span title="Enhanced Inner Focus" style="color:var(--pk)">EIF ${en(\'EIF\')}</span>',
    '<span class="enchant-badge eif" title="Enhanced Inner Focus">EIF ${en(\'EIF\')}</span>'
  );
  // ---- TABS: Tracker / Trade (nut tab dinh lien voi khung, giong khung Drops) ----
  const rep2 = (before, after) => {
    if (!page.includes(before)) throw new Error('Tab layout: khong tim thay doan can sua: ' + before.slice(0, 40));
    page = page.replace(before, () => after);
  };
  rep2('</style></head>',
    '.tabs{display:flex;gap:6px;margin-top:12px;position:relative;z-index:2}' +
    '.tab{background:var(--cd);border:1px solid var(--bd);border-bottom:0;border-radius:12px 12px 0 0;padding:9px 22px;cursor:pointer;color:var(--mu);font-weight:600;margin-bottom:0}' +
    '.tab{opacity:.6;transition:opacity .15s,color .15s}.tab:hover{color:var(--tx);opacity:1}.tab:focus{outline:none}.tab:focus-visible{outline:2px solid var(--cy);outline-offset:-2px}' +
    '.tab.on{opacity:1;background:var(--pn);color:var(--cy);font-weight:800;margin-bottom:-1px;padding-bottom:10px;position:relative;border-color:var(--cy);box-shadow:inset 0 3px 0 var(--cy),0 -6px 16px -8px var(--cy)}' +
    '.tabframe{margin-top:12px;position:relative;z-index:1}.tabframe.joined{margin-top:0;border-top-left-radius:0}' +
    '.tabpane>.rw:first-child{margin-top:0}' +
    'select{-webkit-appearance:none;-moz-appearance:none;appearance:none;color-scheme:dark;background:var(--pn) url("data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 width=%2712%27 height=%277%27 viewBox=%270 0 12 7%27%3E%3Cpath d=%27M1 1l5 5 5-5%27 fill=%27none%27 stroke=%27%238a919c%27 stroke-width=%271.7%27 stroke-linecap=%27round%27 stroke-linejoin=%27round%27/%3E%3C/svg%3E") no-repeat right 13px center;background-size:12px 7px;border:1px solid var(--bd);border-radius:12px;padding:8px 36px 8px 14px;color:var(--tx);cursor:pointer;line-height:1.25;transition:border-color .15s}' +
    'select:hover,select:focus{border-color:var(--mu);outline:none}select option{background:var(--pn);color:var(--tx)}.pop select{padding:6px 32px 6px 12px}' +
    ':root[data-theme="light"] select{color-scheme:light}@media (prefers-color-scheme:light){:root:not([data-theme="dark"]) select{color-scheme:light}}' +
    '.rd{gap:8px;flex-wrap:wrap}.rd label{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;border:1px solid var(--bd);border-radius:12px;background:var(--pn);cursor:pointer;transition:border-color .15s,background .15s,color .15s}.rd label:hover{border-color:var(--mu)}.rd input[type=radio]{position:absolute;opacity:0;width:0;height:0;margin:0;pointer-events:none}.rd label:has(input:checked){background:var(--tx);color:var(--bg);border-color:var(--tx);font-weight:600}' +
    'input[type=checkbox]{-webkit-appearance:none;-moz-appearance:none;appearance:none;position:relative;display:inline-block;flex:none;width:18px;height:18px;margin:0;vertical-align:middle;cursor:pointer;background:var(--bg);border:1.5px solid #4b5260;border-color:color-mix(in srgb,var(--mu) 55%,transparent);border-radius:6px;transition:border-color .15s,box-shadow .15s,background .15s}' +
    'input[type=checkbox]:hover{border-color:var(--mu)}input[type=checkbox]:focus-visible{outline:2px solid #60a5fa;outline-offset:2px}' +
    'input[type=checkbox]:checked,input[type=checkbox]:indeterminate{border-color:var(--tx);box-shadow:0 0 0 2px color-mix(in srgb,var(--tx) 14%,transparent)}' +
    'input[type=checkbox]::after{content:"";position:absolute;left:50%;top:50%;width:5px;height:10px;border:solid var(--tx);border-width:0 2px 2px 0;border-radius:1px;transform:translate(-50%,-58%) rotate(45deg) scale(0);transition:transform .12s ease}' +
    'input[type=checkbox]:checked::after{transform:translate(-50%,-58%) rotate(45deg) scale(1)}' +
    'input[type=checkbox]:indeterminate::after{width:8px;height:0;border-width:0 0 2px 0;transform:translate(-50%,-50%) scale(1)}' +
    'input[type=checkbox]:disabled{opacity:.45;cursor:not-allowed}' +
    '.pop.pf{padding:14px;gap:12px;min-width:300px;border-radius:16px;box-shadow:0 14px 34px rgba(0,0,0,.45)}.pf-t{font-weight:700;font-size:13px}.pf-g{display:grid;grid-template-columns:1fr 1fr;gap:10px}.pf-f{display:flex;flex-direction:column;gap:6px;font-size:12px;color:var(--mu)}.pf-i{position:relative}' +
    '.pop.pf .pf-i input{width:100%;box-sizing:border-box;padding:9px 38px 9px 14px;border-radius:12px;background:var(--bg);border:1px solid var(--bd);color:var(--tx);-moz-appearance:textfield;appearance:textfield}.pop.pf .pf-i input:hover,.pop.pf .pf-i input:focus{border-color:var(--mu);outline:none}' +
    '.pf-i input::-webkit-outer-spin-button,.pf-i input::-webkit-inner-spin-button{-webkit-appearance:none;margin:0}' +
    '.pf-x{position:absolute;right:7px;top:50%;transform:translateY(-50%);display:flex;align-items:center;justify-content:center;width:22px;height:22px;padding:0;border:1px solid var(--bd);border-radius:8px;background:var(--cd);color:var(--mu);font-size:10px;cursor:pointer;line-height:1}.pf-x:hover{color:var(--tx);border-color:var(--mu)}.pf-i input:placeholder-shown+.pf-x{display:none}.pf-reset{width:100%;border-radius:12px;text-align:center}' +
    '</style></head>');
  rep2('</style></head>',
    ':root{--hl:rgba(255,255,255,.055);--sh-s:0 1px 2px rgba(0,0,0,.45),0 3px 8px rgba(0,0,0,.28);--sh-m:0 2px 4px rgba(0,0,0,.35),0 10px 26px rgba(0,0,0,.42);--sh-l:0 4px 8px rgba(0,0,0,.35),0 18px 44px rgba(0,0,0,.55);--g1:rgba(255,255,255,.035);--glow:rgba(245,176,66,.07);--pnA:rgba(14,16,19,.3);--cdA:rgba(18,21,26,.28);--pblur:16px}' +
    '@media (prefers-color-scheme:light){:root:not([data-theme="dark"]){--hl:rgba(255,255,255,.9);--sh-s:0 1px 2px rgba(20,30,50,.10),0 2px 6px rgba(20,30,50,.06);--sh-m:0 2px 4px rgba(20,30,50,.08),0 10px 24px rgba(20,30,50,.10);--sh-l:0 4px 10px rgba(20,30,50,.10),0 18px 40px rgba(20,30,50,.16);--g1:rgba(255,255,255,.7);--glow:rgba(13,148,136,.07);--pnA:rgba(255,255,255,.6);--cdA:rgba(246,247,249,.55)}}' +
    ':root[data-theme="light"]{--hl:rgba(255,255,255,.9);--sh-s:0 1px 2px rgba(20,30,50,.10),0 2px 6px rgba(20,30,50,.06);--sh-m:0 2px 4px rgba(20,30,50,.08),0 10px 24px rgba(20,30,50,.10);--sh-l:0 4px 10px rgba(20,30,50,.10),0 18px 40px rgba(20,30,50,.16);--g1:rgba(255,255,255,.7);--glow:rgba(13,148,136,.07);--pnA:rgba(255,255,255,.6);--cdA:rgba(246,247,249,.55)}' +
    'body{background:var(--bg);background-attachment:fixed;overflow-x:hidden}' +
    '.gxy{position:fixed;inset:-5% -5%;pointer-events:none}' +
    '.gxy-video{position:fixed;inset:0;overflow:hidden;z-index:-9;pointer-events:none;background:#000}' +
    '.gxy-video iframe{position:absolute;top:50%;left:50%;width:177.77vh;min-width:100vw;height:56.25vw;min-height:100vh;transform:translate(-50%,-50%);border:0;pointer-events:none}' +
    '.gxy-video::after{content:"";position:absolute;inset:0;background:linear-gradient(180deg,rgba(2,3,6,.45),rgba(2,3,6,.62));}' +
    ':root[data-theme="light"] .gxy-video{opacity:.25}@media (prefers-color-scheme:light){:root:not([data-theme="dark"]) .gxy-video{opacity:.25}}' +
    '.gxy-neb{z-index:-7;background:radial-gradient(1200px 800px at 15% 20%,color-mix(in srgb,var(--pu) 30%,transparent),transparent 62%),radial-gradient(1000px 750px at 85% 12%,color-mix(in srgb,#60a5fa 26%,transparent),transparent 60%),radial-gradient(900px 900px at 50% 95%,color-mix(in srgb,#f472b6 20%,transparent),transparent 64%),radial-gradient(1500px 1000px at 50% 50%,color-mix(in srgb,#5eead4 11%,transparent),transparent 72%);filter:blur(14px) saturate(1.5);opacity:.3;animation:gxySwirl 90s ease-in-out infinite alternate}' +
    '.gxy-core{z-index:-6;background:radial-gradient(420px 420px at 82% 10%,rgba(255,255,255,.95),rgba(196,181,253,.5) 32%,rgba(96,165,250,.18) 55%,transparent 72%);mix-blend-mode:screen;filter:blur(1px);opacity:.3;animation:gxyPulse 7s ease-in-out infinite}' +
    '.gxy-band{z-index:-5;background:linear-gradient(115deg,transparent 6%,color-mix(in srgb,var(--pu) 20%,transparent) 34%,color-mix(in srgb,#60a5fa 24%,transparent) 48%,color-mix(in srgb,#f472b6 17%,transparent) 60%,transparent 88%);filter:blur(30px);mix-blend-mode:screen;opacity:.3;animation:gxyDrift 150s linear infinite}' +
    '.gxy-far{z-index:-4;background-image:radial-gradient(1px 1px at 10% 20%,#fff,transparent),radial-gradient(1px 1px at 40% 70%,#fff,transparent),radial-gradient(1px 1px at 65% 15%,#fff,transparent),radial-gradient(1px 1px at 85% 55%,#fff,transparent),radial-gradient(1px 1px at 25% 85%,#fff,transparent),radial-gradient(1px 1px at 55% 45%,#fff,transparent),radial-gradient(1px 1px at 92% 80%,#fff,transparent),radial-gradient(1px 1px at 5% 55%,#fff,transparent);background-size:220px 220px;background-repeat:repeat;opacity:.65;animation:gxyTwinkle 6s ease-in-out infinite alternate,gxyDriftFar 260s linear infinite}' +
    '.gxy-mid{z-index:-3;background-image:radial-gradient(1.6px 1.6px at 15% 35%,#fff,transparent),radial-gradient(1.6px 1.6px at 50% 10%,#fff,transparent),radial-gradient(1.6px 1.6px at 75% 60%,#fff,transparent),radial-gradient(1.6px 1.6px at 30% 80%,#fff,transparent),radial-gradient(1.6px 1.6px at 90% 25%,#fff,transparent),radial-gradient(1.6px 1.6px at 60% 90%,#fff,transparent);background-size:340px 340px;background-repeat:repeat;opacity:.85;animation:gxyTwinkle 4.5s ease-in-out infinite alternate-reverse,gxyDriftMid 170s linear infinite}' +
    '.gxy-near{z-index:-2;background-image:radial-gradient(2.2px 2.2px at 20% 30%,#fff,transparent),radial-gradient(2.2px 2.2px at 70% 20%,#fff,transparent),radial-gradient(2.2px 2.2px at 40% 65%,#fff,transparent),radial-gradient(2.2px 2.2px at 85% 75%,#fff,transparent),radial-gradient(2.2px 2.2px at 10% 85%,#fff,transparent),radial-gradient(3px 3px at 55% 48%,#fff,transparent);background-size:480px 480px;background-repeat:repeat;opacity:1;animation:gxyTwinkle 3s ease-in-out infinite alternate,gxyDriftNear 95s linear infinite}' +
    '@keyframes gxyTwinkle{0%{opacity:.4}50%{opacity:1}100%{opacity:.6}}' +
    '@keyframes gxyPulse{0%{opacity:.5;transform:scale(1)}50%{opacity:.85;transform:scale(1.06)}100%{opacity:.5;transform:scale(1)}}' +
    '@keyframes gxyDrift{0%{transform:translate3d(0,0,0) rotate(0deg) scale(1)}100%{transform:translate3d(-4%,2%,0) rotate(2deg) scale(1.03)}}' +
    '@keyframes gxySwirl{0%{transform:scale(1) rotate(0deg)}100%{transform:scale(1.09) rotate(6deg)}}' +
    '@keyframes gxyDriftFar{0%{background-position:0 0}100%{background-position:220px 220px}}' +
    '@keyframes gxyDriftMid{0%{background-position:0 0}100%{background-position:-340px 170px}}' +
    '@keyframes gxyDriftNear{0%{background-position:0 0}100%{background-position:240px -240px}}' +
    ':root[data-theme="light"] .gxy{opacity:.18}@media (prefers-color-scheme:light){:root:not([data-theme="dark"]) .gxy{opacity:.18}}' +
    '.pn,.tw,.tcon,.am{background:linear-gradient(180deg,var(--g1),transparent 42%),var(--pnA);box-shadow:inset 0 1px 0 var(--hl),var(--sh-m);backdrop-filter:blur(var(--pblur)) saturate(1.15);-webkit-backdrop-filter:blur(var(--pblur)) saturate(1.15)}' +
    '.tabframe{box-shadow:inset 0 1px 0 var(--hl),var(--sh-l)}' +
    '.top>.pn,.pn.tabframe{border-color:color-mix(in srgb,var(--bd) 80%,var(--tx) 8%)}' +
    '.st{background:linear-gradient(160deg,var(--g1),transparent 55%),var(--cdA);box-shadow:inset 0 1px 0 var(--hl),0 1px 0 rgba(0,0,0,.25),var(--sh-s);backdrop-filter:blur(12px) saturate(1.15);-webkit-backdrop-filter:blur(12px) saturate(1.15);transition:transform .15s,box-shadow .15s,border-color .15s}' +
    '.st:hover{transform:translateY(-2px);box-shadow:inset 0 1px 0 var(--hl),var(--sh-m);border-color:color-mix(in srgb,var(--bd) 60%,var(--mu))}' +
    '.st b{text-shadow:0 2px 14px currentColor}' +
    '.bt:not(.on):not(.dg){background:linear-gradient(180deg,color-mix(in srgb,var(--pn) 86%,var(--tx) 14%),var(--pn));box-shadow:inset 0 1px 0 var(--hl),var(--sh-s);transition:transform .12s,box-shadow .12s,border-color .12s}' +
    '.bt:not(:disabled):hover{transform:translateY(-1px);box-shadow:inset 0 1px 0 var(--hl),var(--sh-m)}' +
    '.bt:not(:disabled):active{transform:translateY(1px);box-shadow:inset 0 2px 5px rgba(0,0,0,.4)}' +
    '.bt.on{box-shadow:inset 0 1px 0 rgba(255,255,255,.35),0 4px 14px rgba(0,0,0,.4)}' +
    '.bt.dg:not(:disabled){box-shadow:var(--sh-s)}' +
    '#q,#iq,.pop input,select{box-shadow:inset 0 2px 5px rgba(0,0,0,.28),0 1px 0 var(--hl)}' +
    '#q:focus,#iq:focus,.pop input:focus{outline:none;border-color:var(--cy);box-shadow:inset 0 2px 5px rgba(0,0,0,.28),0 0 0 3px color-mix(in srgb,var(--cy) 22%,transparent)}' +
    '.pop{box-shadow:var(--sh-l)}' +
    '.tab{box-shadow:inset 0 1px 0 var(--hl),0 -2px 6px rgba(0,0,0,.25)}' +
    'thead th{background:linear-gradient(180deg,var(--g1),transparent),var(--cdA)}' +
    'tr.r td{background:transparent}tr.r{transition:background .12s}tr.r td{transition:background .12s,box-shadow .12s}' +
    'tr.r:hover td{background:color-mix(in srgb,var(--cy) 6%,var(--cd));box-shadow:inset 0 1px 0 color-mix(in srgb,var(--cy) 22%,transparent),inset 0 -1px 0 color-mix(in srgb,var(--cy) 22%,transparent)}' +
    'tr.r:hover td:first-child{box-shadow:inset 3px 0 0 var(--cy),inset 0 1px 0 color-mix(in srgb,var(--cy) 22%,transparent),inset 0 -1px 0 color-mix(in srgb,var(--cy) 22%,transparent)}' +
    '.ic{box-shadow:inset 0 1px 0 var(--hl),var(--sh-s);transition:transform .15s,box-shadow .15s}' +
    '.ic:hover{transform:translateY(-1px);box-shadow:inset 0 1px 0 var(--hl),var(--sh-m)}' +
    '.bg,.tb,.tst{box-shadow:inset 0 1px 0 var(--hl),0 1px 3px rgba(0,0,0,.3)}' +
    '.tlog{box-shadow:inset 0 3px 10px rgba(0,0,0,.35)}' +
    '.ov{backdrop-filter:blur(5px) saturate(1.1)}.am{box-shadow:inset 0 1px 0 var(--hl),var(--sh-l),0 0 0 1px rgba(255,255,255,.03)}' +
    '.statbox,.xpbox{box-shadow:inset 0 1px 0 var(--hl),var(--sh-s)}.xpbox{box-shadow:var(--sh-l)}' +
    '#t{box-shadow:var(--sh-l)}' +
    '.discord-button{box-shadow:0 8px 24px rgba(88,101,242,.35),inset 0 1px 0 rgba(255,255,255,.25)}' +
    '</style></head>');
  rep2('</style></head>',
    'body{font-family:"Be Vietnam Pro",system-ui,-apple-system,"Segoe UI",sans-serif}' +
    '.sb-brand,.sb-lbl{display:none}' +
    '.pn{border-radius:20px}' +
    '.st small{letter-spacing:.08em;text-transform:uppercase;font-size:11px}' +
    '@media (min-width:900px){' +
    '.w:has(#tabs:not([style*="none"])){max-width:none;padding-left:290px;padding-right:22px}' +
    '.w:has(#tabs:not([style*="none"])) .top,.w:has(#tabs:not([style*="none"])) .rw{max-width:1280px}' +
    '.tabs{position:fixed;left:0;top:0;bottom:0;width:250px;margin:0;padding:20px 14px 84px;flex-direction:column;gap:6px;background:linear-gradient(180deg,color-mix(in srgb,var(--pnA) 92%,var(--tx) 8%),var(--pnA));backdrop-filter:blur(var(--pblur)) saturate(1.15);-webkit-backdrop-filter:blur(var(--pblur)) saturate(1.15);border-right:1px solid var(--bd);box-shadow:10px 0 40px rgba(0,0,0,.35);z-index:20;overflow-y:auto}' +
    '.sb-brand{display:flex;align-items:center;gap:12px;padding:4px 6px 20px}' +
    '.sb-logo{width:52px;height:52px;flex:none;border-radius:14px;display:grid;place-items:center;overflow:hidden;background:color-mix(in srgb,#60a5fa 10%,transparent);border:1px solid color-mix(in srgb,#60a5fa 30%,transparent);box-shadow:inset 0 1px 0 var(--hl),0 4px 14px color-mix(in srgb,#60a5fa 18%,transparent)}' +
    '.sb-logo img{width:90%;height:90%;object-fit:contain;filter:drop-shadow(0 2px 6px rgba(96,165,250,.4))}' +
    '.sb-t{font:700 19px/1.1 Cinzel,"Cormorant SC",Georgia,serif;letter-spacing:.05em;text-transform:uppercase}' +
    '.sb-s{color:var(--mu);font-size:12px;margin-top:3px}' +
    '.sb-lbl{display:block;font-size:11px;letter-spacing:.18em;font-weight:700;color:var(--mu);text-transform:uppercase;padding:6px 10px 8px}' +
    '.tabs .tab{display:flex;align-items:center;gap:13px;margin:0;padding:12px 14px;border:1px solid transparent;border-radius:14px;background:transparent;box-shadow:none;opacity:1;color:var(--mu);font-weight:500;font-size:15px;text-align:left;transition:background .15s,color .15s,border-color .15s}' +
    '.tabs .tab::before{width:22px;text-align:center;font-size:17px;filter:grayscale(.55) opacity(.8);transition:filter .15s}' +
    '.tabs .tab[data-tab="tracker"]::before{content:"📊"}.tabs .tab[data-tab="trade"]::before{content:"🔁"}.tabs .tab[data-tab="buysell"]::before{content:"💸"}.tabs .tab[data-tab="buy"]::before{content:"🛒"}' +
    '.tabs .tab:hover{background:color-mix(in srgb,var(--tx) 5%,transparent);color:var(--tx)}' +
    '.tabs .tab.on{margin:0;padding:12px 14px;background:linear-gradient(90deg,color-mix(in srgb,var(--am) 22%,transparent),color-mix(in srgb,var(--am) 7%,transparent));border-color:color-mix(in srgb,var(--am) 24%,transparent);color:var(--am);font-weight:700;box-shadow:inset 0 1px 0 var(--hl),0 4px 16px color-mix(in srgb,var(--am) 8%,transparent)}' +
    '.tabs .tab.on::before{filter:none}' +
    '.tabframe.joined{margin-top:12px;border-top-left-radius:20px}' +
    '.tabs{transition:width .22s ease}' +
    '.sb-toggle{position:absolute;top:16px;right:-20px;width:40px;height:40px;border-radius:50%;border:1px solid var(--bd);background:var(--pn);color:var(--mu);display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:19px;line-height:1;box-shadow:0 2px 12px rgba(0,0,0,.45);transition:transform .25s ease,color .15s,border-color .15s;z-index:21}' +
    '.sb-toggle:hover{color:var(--tx);border-color:var(--mu)}' +
    '.tabs.collapsed{width:76px;padding-left:12px;padding-right:12px}' +
    '.tabs.collapsed .sb-toggle{transform:rotate(180deg)}' +
    '.tabs.collapsed .sb-brand{justify-content:center;padding:4px 0 20px;gap:0}' +
    '.tabs.collapsed .sb-brand>div:last-child,.tabs.collapsed .sb-lbl{display:none}' +
    '.tabs.collapsed .tab{justify-content:center;padding:12px 0;gap:0;font-size:0}' +
    '.w:has(#tabs.collapsed){padding-left:104px}' +
    '}' +
    '</style></head>');
  rep2('<div class="rw"><input id="q"',
    '<div class="tabs" id="tabs" style="display:none"><div class="sb-brand"><div class="sb-logo"><img src="data:image/png;base64,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" alt="DQR"></div><div><div class="sb-t">DQR Tracker</div><div class="sb-s">Dungeon Quest Reborn</div></div></div><div class="sb-lbl">Tracker</div><button class="tab on" data-tab="tracker" type="button">Tracker</button><div class="sb-lbl sb-lbl2">Script</div><div class="sb-lbl sb-lbl2 sb-lbl3">Roblox</div></div>' +
    '<div class="pn tabframe" id="tf"><div class="tabpane" data-pane="tracker" id="pane-tracker">' +
    '<div class="rw"><input id="q"');
  rep2('Không có tài khoản nào</div></div>\n</div>',
    'Không có tài khoản nào</div></div></div></div>\n</div>');
  rep2("render();poll();document.addEventListener('visibilitychange'",
    "function setTab(t){if(!document.querySelector('.tab[data-tab=\"'+t+'\"]'))t='tracker';document.querySelectorAll('.tab').forEach(function(b){b.classList.toggle('on',b.dataset.tab===t)});document.querySelectorAll('.tabpane').forEach(function(x){x.style.display=x.dataset.pane===t?'':'none'});sv('dq_tab',t);if(window.invTab)window.invTab(t)}\n" +
    "$('tabs').addEventListener('click',function(e){var b=e.target.closest('.tab');if(b)setTab(b.dataset.tab)});\n" +
    "var sbCollapsed=ld('dq_sb_collapsed',false)===true;function applySb(){var tb=$('tabs');if(!tb)return;tb.classList.toggle('collapsed',sbCollapsed);var sb=$('sbToggle');if(sb)sb.title=sbCollapsed?'Mở rộng menu':'Thu gọn menu'}\n" +
    "document.querySelectorAll('.tabs .tab').forEach(function(b){if(!b.title)b.title=b.textContent.trim()});\n" +
    "if($('sbToggle'))$('sbToggle').onclick=function(e){e.stopPropagation();sbCollapsed=!sbCollapsed;sv('dq_sb_collapsed',sbCollapsed);applySb()};\n" +
    "applySb();\n" +
    "render();poll();document.addEventListener('visibilitychange'");
  rep2('<div class="pop" id="pf"><label>Level từ <input id="fa" type="number"></label><label>Level đến <input id="fb" type="number"></label><label>Gold tối thiểu <input id="fg" type="text" inputmode="decimal" placeholder="Ví dụ: 1b, 100b, 1t"></label></div>',
    '<div class="pop pf" id="pf"><div class="pf-t">Lọc chỉ số</div><div class="pf-g"><div class="pf-f"><span>Level từ</span><div class="pf-i"><input id="fa" type="number" placeholder="Từ"><button type="button" class="pf-x" data-clr="fa" aria-label="Xoá">✕</button></div></div><div class="pf-f"><span>Level đến</span><div class="pf-i"><input id="fb" type="number" placeholder="Đến"><button type="button" class="pf-x" data-clr="fb" aria-label="Xoá">✕</button></div></div></div><div class="pf-f"><span>Gold tối thiểu</span><div class="pf-i"><input id="fg" type="text" inputmode="decimal" placeholder="Ví dụ: 1b, 100b, 1t"><button type="button" class="pf-x" data-clr="fg" aria-label="Xoá">✕</button></div></div><button type="button" class="bt pf-reset" id="pf_reset">↺ Xoá bộ lọc</button></div>');
  rep2("$('tabs').addEventListener('click',function(e){",
    "$('pf').addEventListener('click',function(e){var x=e.target.closest('.pf-x');if(x){var i=$(x.dataset.clr);i.value='';i.dispatchEvent(new Event('input',{bubbles:true}));i.focus();return}if(e.target.closest('#pf_reset'))['fa','fb','fg'].forEach(function(id){var i=$(id);i.value='';i.dispatchEvent(new Event('input',{bubbles:true}))})});\n" +
    "$('tabs').addEventListener('click',function(e){");
  rep2('<body><div class="w">',
    '<body><div class="gxy gxy-neb"></div><div class="gxy gxy-core"></div><div class="gxy gxy-band"></div><div class="gxy gxy-far"></div><div class="gxy gxy-mid"></div><div class="gxy gxy-near"></div><div class="w">');
  // ===== PATCH UI: tach Ultimate/Legend + EIR/EIF (stat cards + khung drop), dan deu cac nut =====
  const rep3 = (before, after) => {
    if (!page.includes(before)) throw new Error('Patch UI: khong tim thay doan can sua: ' + before.slice(0, 50));
    page = page.replace(before, () => after);
  };
  // 1) Stat cards: ALL GOLD | BEST LV | ULTIMATE | LEGEND | EIR | EIF | CAPTCHA/BAN
  {
    const NEW_STATS = String.raw`$('sg').innerHTML=(function(){
 var hold=function(f){return data.filter(function(a){return cnt(a,f)>0}).length},
  isU=function(i){return tier(i)===6},
  isL=function(i){return tier(i)===5&&!enchantCode(i)},
  isR=function(i){return enchantCode(i)==='EIR'},
  isF=function(i){return enchantCode(i)==='EIF'},
  ckn=(function(){var o={t:0,v:0,d:0,w:0,rj:0,dc:0,l:0,k:0};data.forEach(function(a){var s=a.ck&&a.ck.s;if(!s)return;o.t++;if(s==='valid')o.v++;else if(s==='invalid')o.d++;else if(s==='rejoin'){o.w++;o.rj++}else if(s==='disconnect'){o.w++;o.dc++}else if(s==='lock')o.l++;else if(s==='kicked')o.k++});return o})(),
  held=function(f){return '<span class="mu">'+hold(f)+' acc đang giữ</span>'},
  sc=function(lb,v,c,sub,cls){return '<div class="st st-x'+(cls?' '+cls:'')+'" style="--c:'+c+'"><small>'+lb+'</small>'+(v===null?'':'<b>'+v+'</b>')+'<div class="sub">'+sub+'</div></div>'};
 return [
  sc('ALL GOLD',sh(tg),'var(--cy)','<span class="mu">TB '+sh(n?tg/n:0)+'/acc</span>'),
  sc('BEST LV',fmt(Math.max(0,...data.map(function(a){return +a.level||0}))),'var(--pk)','<span class="mu">Level cao nhất</span>'),
  sc('ULTIMATE',all(isU),TIER[6].c,held(isU)),
  sc('LEGEND',all(isL),TIER[5].c,held(isL)),
  sc('EIR',all(isR),'#f87171','<span class="mu" title="Enhanced Inner Rage">'+hold(isR)+' acc · Inner Rage</span>'),
  sc('EIF',all(isF),'#60a5fa','<span class="mu" title="Enhanced Inner Focus">'+hold(isF)+' acc · Inner Focus</span>'),
  sc('COOKIE',null,'#4ade80','<div class="ck-main"><span style="color:#4ade80" title="Cookie còn sống">🍪✓ '+ckn.v+'</span><span style="color:#f43f5e" title="Dead '+ckn.d+' · Lock '+ckn.l+' · Kick '+ckn.k+'">🍪! '+(ckn.d+ckn.l+ckn.k)+'</span><span style="color:#fbbf24" title="Rejoin '+ckn.rj+' · Disconnect '+ckn.dc+'">🍪⚠ '+ckn.w+'</span></div>','st-ck')
 ].join('')})()}`;
    const a = page.indexOf("const S=[['ALL GOLD'");
    const b = page.indexOf('\nfunction chart(){', a);
    if (a < 0 || b < 0) throw new Error('Patch UI: khong tim thay stats()');
    page = page.slice(0, a) + NEW_STATS + page.slice(b);
  }
  // 2) Khung drop: 4 cot rieng Ultimate | Legend | EIR | EIF
  {
    const NEW_FEED = String.raw`
const DCATS=[['ult','Ultimate','#f43f5e',function(d){return d.r==='Ult'}],['leg','Legend','#f59e0b',function(d){return d.r==='Legend'&&!enchantCode(d)}],['eir','EIR','#f87171',function(d){return enchantCode(d)==='EIR'}],['eif','EIF','#60a5fa',function(d){return enchantCode(d)==='EIF'}]];
function feed(){
 var keep={};document.querySelectorAll('#df .dcol-l').forEach(function(n){keep[n.dataset.k]=n.scrollTop});
 var all=[];data.forEach(function(a){(a.drops||[]).forEach(function(d){if(!isFinal(d))return;all.push({d:d,a:a})})});
 all.sort(function(x,y){return y.d.at-x.d.at});
 var shown=[],sum=[],cols=DCATS.map(function(c){
  var list=all.filter(function(x){return c[3](x.d)}).slice(0,10),n=list.reduce(function(s,x){return s+(+x.d.q||1)},0);
  shown=shown.concat(list.map(function(x){return x.d}));
  sum.push('<span class="dchip" style="--c:'+c[2]+'">'+c[1]+' <b>'+n+'</b></span>');
  return '<div class="dcol" style="--c:'+c[2]+'"><div class="dcol-h"><span class="dcol-t"><i></i>'+c[1]+'</span><span class="dcol-n">'+n+'</span></div><div class="dcol-l" data-k="'+c[0]+'">'+(list.length?list.map(function(x){return dropCard(x.d,x.a)}).join(''):'<div class="dcol-e">Chưa có '+c[1]+' nào rớt</div>')+'</div></div>'});
 $('dn').innerHTML=sum.join('');
 $('df').innerHTML='<div class="dgrid">'+cols.join('')+'</div>';
 document.querySelectorAll('#df .dcol-l').forEach(function(n){if(keep[n.dataset.k])n.scrollTop=keep[n.dataset.k]});
 loadImgs(shown)}`;
    const endMark = 'loadImgs(top.map(x=>x.d))}';
    const a = page.indexOf('function feed(){');
    const b = page.indexOf(endMark, a);
    if (a < 0 || b < 0) throw new Error('Patch UI: khong tim thay feed()');
    page = page.slice(0, a) + NEW_FEED + page.slice(b + endMark.length);
  }
  // the item trong cot EIR/EIF mang mau enchant
  rep3("{Ult:TIER[6].c,Legend:TIER[5].c}[d.r]||'#8a919c'", "({EIR:'#f87171',EIF:'#60a5fa'})[enchantCode(d)]||{Ult:TIER[6].c,Legend:TIER[5].c}[d.r]||'#8a919c'");
  // 3) CSS: stat cards, khung drop, thanh nut
  rep3('</style></head>', String.raw`
.top{grid-template-columns:minmax(210px,260px) 1fr;max-width:none!important}
.sg{grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.sg .st.st-x{--c:var(--tx);position:relative;overflow:hidden;border-color:color-mix(in srgb,var(--c) 38%,var(--bd));background:linear-gradient(160deg,color-mix(in srgb,var(--c) 14%,transparent),transparent 65%),var(--cdA)}
.sg .st.st-x::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--c)}
.sg .st.st-x b{color:var(--c);text-shadow:0 2px 14px color-mix(in srgb,var(--c) 55%,transparent)}
.sg .st.st-x .sub{font-size:12px;font-weight:500}
.dgrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
@media(max-width:2300px){.dgrid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:1300px){.dgrid{grid-template-columns:1fr}}.dcol-l .ic{flex:none;min-height:64px;box-sizing:border-box}
.dcol{display:flex;flex-direction:column;min-width:0;overflow:hidden;border:1px solid color-mix(in srgb,var(--c) 40%,var(--bd));border-radius:16px;background:linear-gradient(180deg,color-mix(in srgb,var(--c) 10%,transparent),transparent 120px),var(--cdA)}
.dcol-h{display:flex;align-items:center;justify-content:space-between;padding:10px 14px;border-bottom:1px solid color-mix(in srgb,var(--c) 28%,var(--bd))}
.dcol-t{display:inline-flex;align-items:center;gap:8px;font-weight:800;font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--c)}
.dcol-t i{width:9px;height:9px;border-radius:50%;background:var(--c);box-shadow:0 0 10px var(--c)}
.dcol-n{min-width:28px;text-align:center;padding:2px 10px;border-radius:999px;border:1px solid var(--c);color:var(--c);background:color-mix(in srgb,var(--c) 14%,transparent);font-size:12px;font-weight:800}
.dcol-l{display:flex;flex-direction:column;gap:8px;padding:10px;height:228px;box-sizing:border-box;overflow-y:auto;scrollbar-width:thin;scrollbar-color:var(--mu) transparent}
.dcol-e{padding:22px 8px;text-align:center;color:var(--mu);font-size:12px}
.rw:has(#q),.rw:has([data-p="pc"]),.rw:has(#ha){gap:10px;align-items:stretch}
.rw:has(#q) #q{flex:1 1 320px;min-height:44px;border-radius:12px}
.rw:has(#q) .rd{flex:0 1 auto;display:flex;flex-wrap:nowrap;gap:2px;padding:4px;border:1px solid var(--bd);border-radius:14px;background:var(--pn)}
.rw:has(#q) .rd label{border:0;border-radius:10px;background:transparent;padding:0 18px;min-height:34px;justify-content:center;font-weight:500}
.rw:has(#q) .rd label:hover{background:color-mix(in srgb,var(--tx) 8%,transparent)}
.rw:has(#q) .rd label:has(input:checked){background:var(--tx);color:var(--bg);font-weight:700;box-shadow:0 2px 8px rgba(0,0,0,.35)}
.rw:has([data-p="pc"]) .wr{flex:1 1 150px;display:flex}
.rw:has([data-p="pc"]) .wr>.bt{flex:1;display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:44px;border-radius:12px;font-weight:600;white-space:nowrap}
.rw:has([data-p="pc"]) .wr>.bt:hover{border-color:var(--cy)}
.rw:has([data-p="pc"]) > span[style*="margin-left:auto"]{display:none}
.rw:has([data-p="pc"]) #lv{align-self:center;margin-left:auto;font-size:12px}
.rw:has([data-p="pc"]) #rf{min-height:44px;padding:0 22px;border-radius:12px;font-weight:700;color:var(--cy);border-color:color-mix(in srgb,var(--cy) 55%,var(--bd))}
.rw:has(#ha) .bt,.rw:has(#ha) select{display:inline-flex;align-items:center;justify-content:center;min-height:40px;padding:0 16px;border-radius:12px;font-weight:600}
.rw:has(#ha) #ic,.rw:has(#ha) #pgl{display:inline-flex;align-items:center;min-height:40px;padding:0 14px;border-radius:12px;font-weight:600}
.rw:has(#ha) > span.mu{display:inline-flex;align-items:center;margin-left:8px!important;padding-left:14px;border-left:1px solid var(--bd);font-weight:600}
.rw:has(#ha) [data-fl]{min-width:132px}
.rw:has(#ha) [data-fl="rejoin"]:not(:disabled){color:#fbbf24;border-color:#fbbf2466;background:#fbbf2418}
.rw:has(#ha) [data-fl="ban"]:not(:disabled){color:#fb7185;border-color:#fb718566;background:#fb718518}
.rw:has(#ha) [data-fl=""]:not(:disabled){color:var(--ok);border-color:#4ade8066;background:#4ade8018}
.rw:has(#ha) [data-fl]:disabled,.rw:has(#ha) #del:disabled{opacity:.45}
.rw:has(#ha) #pv,.rw:has(#ha) #nx{width:40px;padding:0;font-size:18px}
</style></head>`);
  // ===== HET PATCH UI =====
  // ===== PATCH UI 2: bo gioi han 1280px, status sat ten acc, chia nut 2 ben, Trade/Sell/Buy =====
  rep3("$('pc').innerHTML=COLS.slice(1)", "{var _si=COLS.findIndex(function(c){return c[0]==='st'});if(_si>1)COLS.splice(1,0,COLS.splice(_si,1)[0])}\n$('pc').innerHTML=COLS.slice(1)");
  rep3('</style></head>', String.raw`
.w .rw,.w .top{max-width:none!important}
.pn>.rw:has(#dn){margin:0 0 12px!important;align-items:center}
.pn>.rw:has(#dn)>b{font-size:17px;letter-spacing:.02em}
#dn{display:inline-flex;flex-wrap:wrap;justify-content:flex-end;gap:10px;font-size:14px!important}
.dchip{display:inline-flex;align-items:center;gap:8px;padding:6px 15px;border-radius:999px;font-size:14px;font-weight:700;letter-spacing:.02em;color:var(--c);border:1px solid color-mix(in srgb,var(--c) 60%,transparent);background:color-mix(in srgb,var(--c) 17%,transparent);box-shadow:0 0 16px color-mix(in srgb,var(--c) 24%,transparent),inset 0 1px 0 var(--hl);text-shadow:0 0 12px color-mix(in srgb,var(--c) 65%,transparent)}
.dchip b{font-size:17px;font-weight:800;color:#fff;text-shadow:0 0 10px var(--c)}
.sg .st.st-x small{display:block;font-size:15px;font-weight:800;letter-spacing:.12em;color:var(--c);text-shadow:0 0 8px var(--c),0 0 18px color-mix(in srgb,var(--c) 70%,transparent),0 0 34px color-mix(in srgb,var(--c) 45%,transparent)}
.sg .st.st-x b{font-size:30px}
.sg .st.st-ck .sub{margin-top:10px}.sg .st.st-ck .ck-main{display:flex;flex-wrap:wrap;gap:6px 18px;font-size:24px;font-weight:800;line-height:1.25}.sg .st.st-ck .ck-main span{white-space:nowrap}.sg .st.st-ck .ck-sub{margin-top:8px;font-size:15px;font-weight:700}
.top>.pn:first-child small.online-label{display:block;font-size:15px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:var(--ok)!important;text-shadow:0 0 8px var(--ok),0 0 18px color-mix(in srgb,var(--ok) 70%,transparent),0 0 34px color-mix(in srgb,var(--ok) 45%,transparent)}
.online-count #on{text-shadow:0 0 16px color-mix(in srgb,var(--ok) 70%,transparent),0 0 36px color-mix(in srgb,var(--ok) 40%,transparent)}
.online-count #tt{color:var(--tx);opacity:.75;text-shadow:0 0 12px color-mix(in srgb,var(--tx) 35%,transparent)}
.rw:has([data-p="pc"]) .wr{flex:0 0 auto;min-width:170px}
.rw:has(#ha) > span.mu{margin-left:auto!important;padding-left:0;border-left:0}
.tw th,.tw td{padding:13px 18px}
th[data-k="name"]{min-width:190px}
th[data-k="st"]{min-width:150px}
th[data-k="gpass"]{min-width:230px}
th[data-k="equip"]{width:100%}
.eqw{max-width:none!important;min-width:380px}
.eq-chip{font-size:12px;padding:3px 10px}
.rw:has(#tp_igo),.rw:has(#bs_go),.rw:has(#buy_go){align-items:center;gap:10px}
.rw:has(#tp_igo) .bt:not(.pop .bt),.rw:has(#bs_go) .bt,.rw:has(#buy_go) .bt{display:inline-flex;align-items:center;justify-content:center;min-height:42px;padding:0 18px;border-radius:12px;font-weight:600}
#bs_q,#bs_qi,#buy_q{min-height:42px;border-radius:12px;box-sizing:border-box}
.tw:has(#tp_tb) table,.tw:has(#bs_tb) table{table-layout:fixed;width:100%}
.tw:has(#tp_tb) thead th:nth-child(1),.tw:has(#bs_tb) thead th:nth-child(1){width:300px}
.tw:has(#tp_tb) thead th:nth-child(2){width:170px}
.tw:has(#buy_tb) thead th:nth-child(1){width:260px}
@media(max-width:900px){.tw:has(#tp_tb) thead th,.tw:has(#bs_tb) thead th,.tw:has(#buy_tb) thead th{width:auto!important}.tw:has(#tp_tb) table,.tw:has(#bs_tb) table{table-layout:auto}}
</style></head>`);
  // ===== PATCH UI 3: Inventory -> menu, "Xoa account" len hang tren, an Xuat / Mau do hiem / nut Tat ca item =====
  rep3('<button class="bt dg" id="del" type="button" disabled>Xoá account (0)</button>', '');
  rep3('<div class="wr"><button class="bt" id="iall" type="button">📦 Tất cả item</button></div>',
    '<div class="wr"><button class="bt dg" id="del" type="button" disabled>Xoá account (0)</button></div><div class="wr" style="display:none"><button class="bt" id="iall" type="button">📦 Tất cả item</button></div>');
  rep3('<button class="tab on" data-tab="tracker" type="button">Tracker</button>',
    '<button class="tab on" data-tab="tracker" type="button">Overview</button><button class="tab" data-tab="inventory" type="button">Inventory All</button>');
  rep3("applySb();\nrender();poll();",
    "applySb();\n" + INV_BOOT + "setTimeout(function(){var t=$('tabs');if(t&&t.style.display==='none'){t.style.display='';$('tf').classList.add('joined')}},0);\nrender();poll();");
  rep3('</style></head>', String.raw`
.wr:has([data-p="px"]),.wr:has([data-p="pm"]),.wr:has(#iall){display:none!important}
.rw:has([data-p="pc"]) .wr>.bt.dg:not(:disabled){color:#f43f5e;border-color:#f43f5e88;background:#f43f5e1a}
.rw:has([data-p="pc"]) .wr>.bt.dg:disabled{opacity:.5}
.rw:has([data-p="pc"]) .wr>.bt.dg:not(:disabled):hover{border-color:#f43f5e}
@media (min-width:900px){.tabs .tab[data-tab="inventory"]::before{content:"📦"}}
/* ---- Nhom sidebar: TRACKER (Overview, Inventory) / SCRIPT (Trade, Sell, Buy, Config) / ROBLOX (Cookie) ---- */
.tabs>.sb-lbl:not(.sb-lbl2){order:1}
.tabs .tab[data-tab="tracker"]{order:2}
.tabs .tab[data-tab="inventory"]{order:3}
.tabs>.sb-lbl2{order:5;margin-top:14px}
.tabs>.sb-lbl.sb-lbl3{order:10}
.tabs .tab[data-tab="cookie"]{order:11}
.tabs .tab[data-tab="trade"]{order:6}
.tabs .tab[data-tab="buysell"]{order:7}
.tabs .tab[data-tab="buy"]{order:8}
.rw:has(#ha) [data-fl],.rw:has(#ha) > span.mu{display:none!important}
</style></head>`);

  // ===== PATCH UI 4: trang Inventory rieng, popup account rong + dep, an toan bo thanh cuon =====
  rep3('</style></head>', String.raw`
.sb-toggle{display:none!important}
.hbrand{position:relative;display:inline-flex;align-items:center;gap:14px;padding:10px 26px 10px 12px;border-radius:18px;border:1.5px solid transparent;background:linear-gradient(135deg,color-mix(in srgb,var(--pn) 88%,#000),color-mix(in srgb,var(--pn) 70%,transparent)) padding-box,linear-gradient(120deg,var(--cy),#8b5cf6 50%,var(--pk)) border-box;box-shadow:0 0 24px color-mix(in srgb,var(--cy) 28%,transparent),0 0 48px color-mix(in srgb,var(--pk) 14%,transparent),inset 0 1px 0 var(--hl);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);animation:hbglow 3.4s ease-in-out infinite}
.hbrand::before,.hbrand::after{content:"";position:absolute;top:50%;width:7px;height:7px;border-radius:50%;background:var(--cy);box-shadow:0 0 10px var(--cy);transform:translateY(-50%)}
.hbrand::before{left:-4px}.hbrand::after{right:-4px;background:var(--pk);box-shadow:0 0 10px var(--pk)}
.hb-i{display:grid;place-items:center;width:42px;height:42px;border-radius:12px;font-size:21px;background:linear-gradient(135deg,color-mix(in srgb,var(--cy) 30%,transparent),color-mix(in srgb,var(--pk) 22%,transparent));border:1px solid color-mix(in srgb,var(--cy) 45%,transparent);box-shadow:0 0 14px color-mix(in srgb,var(--cy) 35%,transparent);filter:drop-shadow(0 0 4px var(--cy))}
.hb-t{display:flex;flex-direction:column;line-height:1.15}
.hb-l{font-size:11px;font-weight:800;letter-spacing:.28em;text-transform:uppercase;color:var(--cy);text-shadow:0 0 10px color-mix(in srgb,var(--cy) 70%,transparent)}
.hb-n{font:800 22px/1.2 Cinzel,"Be Vietnam Pro",Georgia,serif;letter-spacing:.04em;background:linear-gradient(90deg,#fff 0%,#000 25%,#fff 50%,#000 75%,#fff 100%);background-size:200% 100%;-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;color:transparent;filter:drop-shadow(0 0 1px rgba(255,255,255,.6)) drop-shadow(0 0 8px rgba(255,255,255,.18));animation:hbname 6s linear infinite}
@keyframes hbname{from{background-position:100% 0}to{background-position:0% 0}}
@keyframes hbglow{0%,100%{box-shadow:0 0 22px color-mix(in srgb,var(--cy) 24%,transparent),0 0 44px color-mix(in srgb,var(--pk) 10%,transparent),inset 0 1px 0 var(--hl)}50%{box-shadow:0 0 32px color-mix(in srgb,var(--cy) 42%,transparent),0 0 64px color-mix(in srgb,var(--pk) 22%,transparent),inset 0 1px 0 var(--hl)}}
@media(max-width:560px){.hb-n{font-size:17px}.hb-i{width:34px;height:34px}}
.rw:has(#ic) [data-fl],.rw:has(#ic)>span.mu{display:none!important}
#ha{min-height:42px;padding:0 22px;border-radius:12px;font-weight:800;letter-spacing:.03em;color:#04151a;cursor:pointer;background:linear-gradient(135deg,var(--cy),color-mix(in srgb,var(--cy) 50%,#60a5fa));border:1px solid color-mix(in srgb,var(--cy) 70%,#fff 12%);box-shadow:0 0 18px color-mix(in srgb,var(--cy) 40%,transparent),inset 0 1px 0 rgba(255,255,255,.35);transition:filter .15s,transform .15s}
#ha:hover{filter:brightness(1.1);transform:translateY(-1px);border-color:color-mix(in srgb,var(--cy) 70%,#fff 12%)}
#ha:active{transform:scale(.97)}
#ha[aria-pressed="true"]{color:#1a0510;background:linear-gradient(135deg,var(--pk),#fb923c);border-color:color-mix(in srgb,var(--pk) 70%,#fff 12%);box-shadow:0 0 18px color-mix(in srgb,var(--pk) 40%,transparent),inset 0 1px 0 rgba(255,255,255,.35)}
.tabs .tab.on{background:linear-gradient(90deg,color-mix(in srgb,var(--cy) 26%,transparent),color-mix(in srgb,var(--cy) 8%,transparent));border-color:color-mix(in srgb,var(--cy) 65%,transparent);color:var(--cy);font-weight:700;text-shadow:0 0 10px color-mix(in srgb,var(--cy) 80%,transparent),0 0 22px color-mix(in srgb,var(--cy) 50%,transparent);box-shadow:inset 0 1px 0 var(--hl),inset 3px 0 0 var(--cy),0 0 18px color-mix(in srgb,var(--cy) 35%,transparent),0 0 40px color-mix(in srgb,var(--cy) 18%,transparent)}
.tabs .tab.on::before{filter:drop-shadow(0 0 6px var(--cy))}
.tabs .tab:focus-visible{outline-color:var(--cy)}
.sb-logo{cursor:pointer;transition:transform .15s,box-shadow .15s,border-color .15s}
.sb-logo:hover{transform:scale(1.07);border-color:color-mix(in srgb,#60a5fa 60%,transparent)}
.sb-logo:active{transform:scale(.96)}
.tabs,.tabs *{overflow-x:hidden!important;scrollbar-width:none!important}
.tabs::-webkit-scrollbar,.tabs *::-webkit-scrollbar{display:none!important;width:0!important;height:0!important}
html{overflow-x:hidden}
/* --- an TOAN BO thanh cuon (van cuon duoc bang chuot/touch) --- */
html,body,*{scrollbar-width:none!important;-ms-overflow-style:none!important}
*::-webkit-scrollbar,*::-webkit-scrollbar-thumb,*::-webkit-scrollbar-track,*::-webkit-scrollbar-corner{display:none!important;width:0!important;height:0!important;background:transparent!important}
/* --- the item moi (dung cho popup account + trang Inventory) --- */
.ic.ic2{position:relative;display:flex;align-items:flex-start;gap:14px;padding:14px;min-width:0;border-radius:16px;border:1px solid color-mix(in srgb,var(--rc,var(--bd)) 70%,var(--bd));background:linear-gradient(160deg,color-mix(in srgb,var(--rc,var(--bd)) 17%,transparent),color-mix(in srgb,var(--rc,var(--bd)) 5%,transparent)),var(--cd);box-shadow:inset 0 1px 0 var(--hl,rgba(255,255,255,.05)),0 4px 14px rgba(0,0,0,.25);transition:transform .15s,box-shadow .15s,border-color .15s}
.ic.ic2:hover{transform:translateY(-2px);border-color:var(--rc,var(--mu));box-shadow:inset 0 1px 0 var(--hl,rgba(255,255,255,.05)),0 10px 26px rgba(0,0,0,.4),0 0 22px color-mix(in srgb,var(--rc,transparent) 24%,transparent)}
.ic.ic2 .im{width:60px;height:60px;border-radius:14px;font-size:22px;background:color-mix(in srgb,var(--bg) 70%,transparent);border:1px solid color-mix(in srgb,var(--rc,var(--bd)) 38%,transparent)}
.icb{min-width:0;flex:1;display:flex;flex-direction:column;align-items:flex-start;gap:6px}
.icn{font-weight:700;font-size:14px;line-height:1.25;overflow-wrap:anywhere}
.icm{font-size:12px;color:var(--mu)}
.ist{display:flex;flex-wrap:wrap;gap:6px}
.st2{display:inline-flex;align-items:baseline;gap:5px;padding:2px 9px;border-radius:8px;font-size:12px;background:color-mix(in srgb,var(--sc) 12%,transparent);border:1px solid color-mix(in srgb,var(--sc) 30%,transparent)}
.st2 i{font-style:normal;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--sc)}
.st2 b{font-weight:700;color:var(--tx)}
.st2-p{--sc:#f59e0b}.st2-s{--sc:#a78bfa}.st2-h{--sc:#4ade80}
.ict{display:flex;flex-wrap:wrap;gap:6px}
.ic2 .own.ico{margin:2px 0 0;padding:2px 10px;font-size:12px;border:1px solid color-mix(in srgb,var(--tx) 14%,transparent)}
/* --- tieu de nhom do hiem / loai --- */
.rs{margin-top:24px}.rs:first-child{margin-top:8px}
.rh{display:flex;align-items:center;gap:12px;margin:28px 0 12px;color:var(--rc,var(--tx))}
.rs>.rh{margin-top:0}
.rh b{font-size:14px;font-weight:800;letter-spacing:.14em}
.rh span{font-size:12px;font-weight:600;color:var(--mu)}
.rh::after{content:"";flex:1;height:1px;background:linear-gradient(90deg,color-mix(in srgb,var(--rc,var(--bd)) 60%,transparent),transparent)}
.gh{display:flex;align-items:center;gap:8px;margin:14px 0 10px;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:var(--gc,var(--mu))}
.gh em{font-style:normal;font-weight:600;letter-spacing:0;text-transform:none;color:var(--mu)}
.ig2{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:12px}
/* --- popup account: rong, dep, header co dinh + body cuon rieng --- */
#am{z-index:60;padding:24px;background:rgba(3,4,8,.72);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px)}
#am .am{width:min(1280px,100%)!important;height:min(92vh,940px);max-height:none;display:flex;flex-direction:column;overflow:hidden;padding:0;border-radius:22px;border:1px solid color-mix(in srgb,var(--tx) 12%,var(--bd));background:color-mix(in srgb,var(--pn) 97%,transparent);box-shadow:0 30px 90px rgba(0,0,0,.65)}
#am .am>.rw:first-child{flex:none;margin:0;flex-wrap:nowrap;align-items:flex-start;gap:16px;padding:22px 28px 18px;border-bottom:1px solid var(--bd);background:linear-gradient(180deg,color-mix(in srgb,var(--tx) 5%,transparent),transparent)}
#am #ah{flex:1;min-width:0;display:flex;flex-direction:column;gap:16px}
#am #ax{flex:none;width:40px;height:40px;padding:0;display:flex;align-items:center;justify-content:center;border-radius:12px}
#am #ab{flex:1;min-height:0;overflow-y:auto;overscroll-behavior:contain;padding:6px 28px 30px}
#am .am>.tbar{flex:none;margin:12px 28px 0}
#t{z-index:70}
.mh{display:flex;align-items:center;gap:16px}
.mav{flex:none;width:56px;height:56px;border-radius:16px;display:grid;place-items:center;font:800 24px/1 Cinzel,Georgia,serif;color:var(--am);background:linear-gradient(135deg,color-mix(in srgb,var(--am) 26%,transparent),color-mix(in srgb,var(--am) 6%,transparent));border:1px solid color-mix(in srgb,var(--am) 40%,transparent)}
.mhi{min-width:0}.mnm{display:flex;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:4px}.mnm b{font-size:22px;letter-spacing:.01em;overflow-wrap:anywhere}
.mts{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px}
.mt{padding:10px 14px;border-radius:12px;border:1px solid color-mix(in srgb,var(--c) 32%,var(--bd));background:color-mix(in srgb,var(--c) 7%,transparent)}
.mt small{display:block;font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--mu)}
.mt b{display:block;margin-top:2px;font-size:20px;color:var(--c)}
/* --- trang Inventory --- */
body.inv-on .top,body.inv-on .w>.pn:has(#df){display:none}
.invbox>.rw:first-child{margin:0 0 6px}
.ihd{display:flex;align-items:baseline;gap:12px}.ihd b{font-size:24px;letter-spacing:.01em}.ihd .mu{font-size:14px}
.istick{position:sticky;top:0;z-index:4;margin:0 -6px;padding:14px 6px 12px;background:color-mix(in srgb,var(--pn) 88%,transparent);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border-bottom:1px solid var(--bd)}
.istick .rw{margin:0;gap:10px}
.istick #iq{flex:1;min-width:240px;min-height:44px;box-sizing:border-box;padding:0 16px;border-radius:12px}
.istick select,.istick .bt{min-height:44px;border-radius:12px}
.isum{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}
.isc{cursor:pointer;display:inline-flex;align-items:center;gap:8px;padding:6px 14px;border-radius:999px;border:1px solid color-mix(in srgb,var(--c) 45%,var(--bd));background:color-mix(in srgb,var(--c) 9%,transparent);color:var(--c);font-weight:700;font-size:13px;transition:background .15s,box-shadow .15s}
.isc b{color:var(--tx);font-weight:700}
.isc:hover{background:color-mix(in srgb,var(--c) 18%,transparent)}
.isc.on{background:color-mix(in srgb,var(--c) 28%,transparent);box-shadow:0 0 16px color-mix(in srgb,var(--c) 30%,transparent)}
.invpane .ig2{grid-template-columns:repeat(auto-fill,minmax(270px,1fr))}
.invpane .rh:first-of-type{margin-top:22px}
.imorew{display:flex;justify-content:center;margin:26px 0 6px}
@media(max-width:720px){#am{padding:0}#am .am{height:100%;border-radius:0}#am .am>.rw:first-child{padding:16px}#am #ab{padding:4px 16px 24px}.ig2,.invpane .ig2{grid-template-columns:1fr}}
/* --- Menu bên trái (sidebar) nhiều mục có thể dài hơn chiều cao màn hình (vd mở rộng thêm tab):
   hiện lại con lăn RIÊNG cho menu này (toàn bộ nơi khác trên web vẫn ẩn con lăn như cũ) để biết còn
   mục ở dưới chưa thấy hết, tránh tưởng nhầm là mục bị mất/kẹt (lỗi UI) khi menu dài hơn màn hình --- */
.tabs{scrollbar-width:thin!important;scrollbar-color:color-mix(in srgb,var(--mu) 55%,transparent) transparent!important}
.tabs::-webkit-scrollbar{display:block!important;width:7px!important;height:7px!important}
.tabs::-webkit-scrollbar-track{background:transparent!important}
.tabs::-webkit-scrollbar-thumb{display:block!important;width:7px!important;height:7px!important;background:color-mix(in srgb,var(--mu) 55%,transparent)!important;border-radius:999px!important}
.tabs::-webkit-scrollbar-thumb:hover{background:var(--mu)!important}
</style></head>`);

  page = page.replace('</head>', () => '<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Be+Vietnam+Pro:wght@400;500;600;700&family=Cinzel:wght@700&display=swap"></head>');
  return page;
})();

const CK_UI_JS = String.raw`
(function(){
try{
var ST={valid:['OK','#4ade80'],invalid:['Dead','#f43f5e'],kicked:['Kick','#f43f5e'],mismatch:['Sai acc','#fbbf24'],lock:['Lock','#f43f5e'],rejoin:['Rejoin','#fbbf24'],disconnect:['Mất kết nối','#fbbf24']},FLB={rejoin:['Captcha rejoin','#fbbf24'],ban:['Ban','#fb7185'],lock:['Captcha lock','#f43f5e'],face:['Face lock','#a78bfa'],kicked:['Kick','#f43f5e'],disconnect:['Mất kết nối','#fbbf24']};
if(!COLS.some(function(c){return c[0]==='ck'})){
  var CK_ORD={valid:0,rejoin:1,disconnect:1,lock:2,kicked:2,invalid:2,mismatch:3};
  var ckCol=['ck','Cookie',
    function(a){var s=a.ck&&a.ck.s;return s?(CK_ORD[s]!=null?CK_ORD[s]:4):5},
    function(a){var ck=a.ck,ckSt=ck&&ST[ck.s];if(!ckSt)return '<span class="mu">—</span>';var bd=ckSt[1],ic=ck.s==='valid'?'✓':(bd==='#fbbf24'?'⚠':'!');return '<span class="bg" style="color:'+bd+';border-color:'+bd+';font-weight:700;white-space:nowrap;display:inline-block;min-width:54px;text-align:center" title="Cookie: '+esc(ckSt[0])+'">🍪'+ic+'</span>'}
  ];
  var stIdx=COLS.findIndex(function(c){return c[0]==='st'});
  COLS.splice(stIdx+1,0,ckCol);
  var pcEl=$('pc');if(pcEl)pcEl.innerHTML=COLS.slice(1).map(function(c){return '<label><input type="checkbox" data-c="'+c[0]+'" checked> '+c[1]+'</label>'}).join('');
  if(typeof render==='function')render();
}

var LB={valid:['Live','#4ade80'],invalid:['Dead (logout / hết hạn)','#f43f5e'],kicked:['Kick','#f43f5e'],mismatch:['Cookie sai account','#fbbf24'],lock:['Lock','#f43f5e'],rejoin:['Captcha rejoin','#fbbf24'],disconnect:['Mất kết nối','#fbbf24'],untracked:['Không có trong tracker','#8a919c'],badformat:['Cookie sai định dạng','#8a919c'],rate:['Roblox giới hạn tốc độ','#fbbf24'],error:['Lỗi kết nối Roblox','#8a919c'],wait:['Chờ kiểm tra','#8a919c'],nocookie:['Chưa có cookie','#8a919c']};
var savedTab=ld('dq_tab','tracker'),CK={},order=[],busy=false,every=+ld('dq_ck_every',60)||60,nextAt=0,lastSig='',MAXN=50;
var sleep=function(ms){return new Promise(function(r){setTimeout(r,ms)})};
var ago=function(t){if(!t)return'—';var s=Math.max(0,Math.floor((Date.now()-t)/1000));return s<60?s+'s trước':Math.floor(s/60)+'p trước'};
var lc=function(x){return String(x).toLowerCase()};
var shown=function(n,i){return hideAccounts?'HIDE':n};
var tr=function(n){return data.find(function(a){return lc(a.name)===lc(n)})};

function init(){
  var tabs=$('tabs'),tf=$('tf');
  if(!tabs||!tf||document.querySelector('.tab[data-tab="cookie"]'))return;
  var css=document.createElement('style');
  css.textContent='.tabs .tab[data-tab="cookie"]::before{content:"🍪"}'
   +'.ckpane{padding:4px}.ckhd{display:flex;flex-wrap:wrap;gap:12px;justify-content:space-between;align-items:flex-start}.ckti{font-size:22px}'
   +'#ck_in{width:100%;min-height:140px;margin-top:12px;box-sizing:border-box;font:12px/1.5 ui-monospace,Menlo,monospace;white-space:pre;overflow:auto}'
   +'.ckz{margin-top:18px}.ckzh{display:flex;align-items:center;gap:10px;font-size:15px;color:var(--c);text-shadow:0 0 8px var(--c);padding:0 2px 6px;border-bottom:1px solid color-mix(in srgb,var(--c) 40%,transparent)}.ckzh b{letter-spacing:.06em;text-transform:uppercase}.ckzh span{border:1px solid var(--c);border-radius:999px;padding:1px 10px;font-size:13px}'
   +'.ckgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:12px;margin-top:14px}'
   +'.ckc{position:relative;border:1px solid var(--bd);border-left:4px solid var(--c);border-radius:14px;padding:14px;background:color-mix(in srgb,var(--c) 7%,var(--cd))}'
   +'.ckc b{font-size:15px;word-break:break-all}.ckc .ckx{position:absolute;top:8px;right:8px;padding:0 8px}'
   +'.cks{display:inline-flex;align-items:center;gap:7px;margin-top:10px;padding:3px 12px;border-radius:999px;border:1px solid var(--c);color:var(--c);font-weight:700;font-size:13px}'
   +'.ckd{width:9px;height:9px;border-radius:50%;background:var(--c);box-shadow:0 0 8px var(--c)}.ckc.ing .ckd{animation:ckp 1s ease-in-out infinite}'
   +'@keyframes ckp{50%{opacity:.25;transform:scale(.7)}}.ckm{margin-top:8px;font-size:12px;color:var(--mu);line-height:1.6}'
   +'#ck_warn .ckw{margin-top:12px;padding:10px 14px;border-radius:12px;border:1px solid #f43f5e;background:#f43f5e1f;color:#fda4af;font-weight:600}';
   document.head.appendChild(css);
  tabs.insertAdjacentHTML('beforeend','<button class="tab" data-tab="cookie" type="button" title="Check cookie">Cookie</button>');
  var pane=document.createElement('div');pane.className='tabpane ckpane';pane.dataset.pane='cookie';pane.id='pane-cookie';pane.style.display='none';
  pane.innerHTML='<div class="ckhd"><div><b class="ckti">🍪 Check cookie Roblox</b></div>'
   +'<div class="rw" style="margin:0"><span id="ck_sum" class="rw" style="margin:0;gap:6px"></span><select id="ck_ev" title="Chu kỳ check"><option value="60">1 phút (mặc định)</option><option value="120">2 phút</option><option value="300">5 phút</option><option value="30">30 giây (nhanh)</option></select><button class="bt" id="ck_now" type="button">⟳ Kiểm tra ngay</button><button class="bt dg" id="ck_clr" type="button">Xoá tất cả</button></div></div>'
   +'<textarea id="ck_in" spellcheck="false" autocomplete="off" placeholder="_|WARNING:-DO-NOT-SHARE-THIS...&#10;_|WARNING:-DO-NOT-SHARE-THIS..."></textarea>'
   +'<div class="rw" style="justify-content:space-between"><span class="mu"><span id="ck_msg"></span> <span id="ck_nxw" style="display:none">Check lại sau: <b id="ck_nx"></b></span></span><span class="mu" style="margin-right:10px" title="Cookie tự lưu, chỉ mất khi bấm Xoá tất cả hoặc ✕">💾 Tự động lưu cookie (file .txt thô, không mã hoá)</span><button class="bt on" id="ck_add" type="button">Thêm & theo dõi liên tục</button></div>'
    +'<div id="ck_warn"></div><div class="ckgrid" id="ck_grid"></div>';
  tf.appendChild(pane);
  $('ck_ev').value=String(every);
  $('ck_ev').onchange=function(){every=+this.value||60;sv('dq_ck_every',every);nextAt=Date.now()+every*1000;render(true)};
  var canDeep=false;
  $('ck_add').onclick=add;$('ck_now').onclick=function(){nextAt=0;tick()};
  var haBtn=$('ha');if(haBtn)haBtn.addEventListener('click',function(){setTimeout(function(){render(true)},0)});
  async function deepScan(){
    var list=order.filter(function(n){return CK[n]});
    if(!list.length)return;
    if(busy)return;busy=true;
    list.forEach(function(n){if(CK[n])CK[n].ing=true});render(true);
    try{
      var j=await postDeep(list.map(function(n){return{name:n,cookie:CK[n].cookie}}));
      ((j&&j.results)||[]).forEach(apply);
    }catch(e){toast('Lỗi quét tool: '+(e&&e.message||e))}
    list.forEach(function(n){if(CK[n])CK[n].ing=false});
    busy=false;nextAt=Date.now()+every*1000;render(true);
  }
  $('ck_clr').onclick=function(){fetch('/api/cookie/stored?all=1',{method:'DELETE',headers:{'x-api-key':KEYV,'x-requested-with':'dq'}}).catch(function(){});CK={};order=[];render(true)};
  $('ck_grid').onclick=function(e){var b=e.target.closest('[data-rm]');if(!b)return;var n=b.dataset.rm;fetch('/api/cookie/stored?name='+encodeURIComponent(n),{method:'DELETE',headers:{'x-api-key':KEYV,'x-requested-with':'dq'}}).catch(function(){});delete CK[n];order=order.filter(function(x){return x!==n});render(true)};
  addFlagBtns();
  if(savedTab==='cookie')setTab('cookie');
  document.addEventListener&&document.addEventListener("visibilitychange",function(){if(!document.hidden){document.wasHidden=true}});
setInterval(tick,1000);render(true);
}

async function resolveBare(list){
  var ok=0,unk=0,oth=0;busy=true;
  try{
    for(var i=0;i<list.length;i+=10){
      var chunk=list.slice(i,i+10),j=await postItems(chunk.map(function(c){return{cookie:c}}));
      ((j&&j.results)||[]).forEach(function(x,k){
        if(['valid','kicked','lock','rejoin','disconnect'].includes(x.s)){
          var n=order.find(function(q){return lc(q)===lc(x.name)});
          if(!n){if(order.length>=MAXN){oth++;return}order.push(x.name);n=x.name;CK[n]={s:x.s,at:0}}
          var c=CK[n];c.s=x.s;c.at=Date.now();c.stored=!!x.stored;if(x.stored)delete c.cookie;else c.cookie=chunk[k];ok++;
        }else if(x.s==='untracked')unk++;else oth++;
      });
      if(i+10<list.length)await sleep(4500);
    }
  }catch(e){oth+=list.length-ok-unk}
  busy=false;nextAt=Date.now()+every*1000;
  $('ck_msg').textContent='Thêm '+ok+' acc.'+(unk?' '+unk+' cookie không nhận diện được hoặc acc chưa có trong tracker (cookie chết/sai cũng rơi vào đây).':'')+(oth?' '+oth+' cookie lỗi/bị Roblox giới hạn, thử lại sau.':'');
  render(true);
}
function add(){
  var added=0,bad=0,bare=[];
  $('ck_in').value.split(/\r?\n/).forEach(function(l){
    l=l.trim();if(!l)return;
    var w0=l.indexOf('_|WARNING'),pre=w0>=0?l.slice(0,w0).replace(/[:\s]+$/,''):null;
    if(w0===0||(w0>0&&!pre)||(w0<0&&l.indexOf(':')<0&&l.length>=80&&/^[\w\-.|:%+\/=]+$/.test(l))){bare.push(w0>=0?l.slice(w0):l);return}
    var i=l.indexOf(':'),n=i>0?l.slice(0,i).trim():'',c=i>0?l.slice(i+1).trim():'',w=c.indexOf('_|WARNING');
    if(w>0)c=c.slice(w);
    if(!/^[A-Za-z0-9_]{3,20}$/.test(n)||c.length<80){bad++;return}
    var k=order.find(function(x){return lc(x)===lc(n)});
    if(!k){if(order.length>=MAXN){bad++;return}order.push(n);CK[n]={s:'wait',at:0};k=n}
    CK[k].cookie=c;CK[k].s='wait';added++;
  });
  $('ck_in').value='';
  $('ck_msg').textContent=(added?'Đã thêm '+added+' account. ':'')+(bad?bad+' dòng bị bỏ qua (cần cookie hoặc tenaccount:cookie, tối đa '+MAXN+' account).':'');
  if(added){persistNow();nextAt=0;tick()}
  render(true);
  if(bare.length){$('ck_msg').textContent='Đang nhận diện '+bare.length+' cookie…';resolveBare(bare)}
}

function persistNow(){var items=order.filter(function(n){return CK[n]&&CK[n].cookie}).map(function(n){return{name:n,cookie:CK[n].cookie}});
  if(!items.length)return;
  fetch('/api/cookie/save',{method:'POST',headers:{'x-api-key':KEYV,'x-requested-with':'dq','content-type':'application/json'},body:JSON.stringify({items:items})}).catch(function(){});
}
async function post(chunk){return postItems(chunk.map(function(n){return{name:n,cookie:CK[n].cookie}}))}
async function postItems(items){
  for(var t=0;t<2;t++){
    var r=await fetch('/api/cookie/check',{method:'POST',headers:{'x-api-key':KEYV,'x-requested-with':'dq','content-type':'application/json'},
      body:JSON.stringify({save:true,items:items})});
    if(r.status===429&&!t){await sleep(5000);continue}
    if(!r.ok)throw new Error('http '+r.status);
    return r.json();
  }
}
async function postDeep(items){
  var r=await fetch('/api/cookie/deepcheck',{method:'POST',headers:{'x-api-key':KEYV,'x-requested-with':'dq','content-type':'application/json'},
    body:JSON.stringify({save:true,items:items})});
  if(r.status===403)throw new Error('Chỉ localhost hoặc admin mới dùng được quét tool');
  if(!r.ok)throw new Error('http '+r.status);
  return r.json();
}
function apply(x){
  var n=order.find(function(k){return lc(k)===lc(x.name)});if(!n||!CK[n])return;
  var c=CK[n],prev=c.s;c.s=x.s;c.stored=!!x.stored;if(x.stored)delete c.cookie;if(x.deep)c.deep=x.deep;
  if(x.s!=='rate'&&x.s!=='error')c.at=Date.now();
  if(['invalid','kicked','lock','rejoin','disconnect'].includes(x.s)&&prev!==x.s&&prev!=='wait'){toast('⚠ '+shown(n,order.indexOf(n))+': '+LB[x.s][0]);document.title='⚠ '+document.title.replace(/^⚠ /,'')}
}
async function cycle(){
  if(busy)return;busy=true;
  var list=order.slice();list.forEach(function(n){if(CK[n])CK[n].ing=true});render(true);
  try{
    for(var i=0;i<list.length;i+=10){
      var chunk=list.slice(i,i+10).filter(function(n){return CK[n]});
      if(chunk.length){var j=await post(chunk);((j&&j.results)||[]).forEach(apply);render(true)}
      if(i+10<list.length)await sleep(4500);
    }
  }catch(e){list.forEach(function(n){if(CK[n]&&CK[n].ing)CK[n].s='error'})}
  list.forEach(function(n){if(CK[n])CK[n].ing=false});
  busy=false;nextAt=Date.now()+every*1000;render(true);
}
var loadedStored=false,storedTry=0;
function loadStored(){
  if(!KEYV||loadedStored)return;
  fetch('/api/cookie/stored',{headers:{'x-api-key':KEYV}}).then(function(r){if(!r.ok)throw 0;return r.json()}).then(function(j){

    loadedStored=true;var cb=$('ck_save');canDeep=!!(j&&j.canDeep);

    if(!j||!j.enabled){if(cb){cb.checked=false;cb.disabled=true;if(cb.parentNode)cb.parentNode.title='Server chưa đặt COOKIE_SECRET nên chưa lưu cookie được'}return}
    (j.names||[]).forEach(function(n){if(order.find(function(x){return lc(x)===lc(n)}))return;order.push(n);CK[n]={s:(j.st&&j.st[n]&&LB[j.st[n]])?j.st[n]:'wait',at:(j.at&&j.at[n])||0,stored:true}});
    if(order.length)nextAt=0;render(true)
  }).catch(function(){});
}
function addFlagBtns(){
  var ban=document.querySelector('[data-fl="ban"]');if(!ban||document.querySelector('[data-fl="lock"]'))return;
  [['face','Face lock'],['lock','Captcha lock'],['kicked','Kick'],['disconnect','Mất kết nối']].forEach(function(p){
    var b=document.createElement('button');b.className='bt';b.type='button';b.dataset.fl=p[0];b.textContent=p[1];b.disabled=!sel.size;ban.after(b);
    b.onclick=async function(){var ns=Array.from(sel);if(!ns.length)return;
      try{var r=await fetch('/api/flag',{method:'POST',headers:{'x-api-key':KEYV,'x-requested-with':'dq','content-type':'application/json'},body:JSON.stringify({names:ns,flag:p[0]})});
        if(r.ok){data.forEach(function(a){if(sel.has(a.name)){a.flag=p[0];a.manual=true}});sel.clear();if(window.render)window.render();toast('Đã gắn cờ '+ns.length+' account')}else toast('Lỗi gắn cờ')}catch(e){toast('Lỗi kết nối')}}
  });
}
function tick(){
  if(!loadedStored&&KEYV&&Date.now()-storedTry>15000){storedTry=Date.now();loadStored()}
  var vis=typeof document!=="undefined"&&!document.hidden&&(!document.querySelector||!!document.querySelector("#pane-cookie"));
  if(vis&&order.length&&!busy&&Date.now()>=nextAt)cycle();
  if(vis&&document.wasHidden){nextAt=0;document.wasHidden=false;}
  var pane=$('pane-cookie');if(!pane||pane.style.display==='none')return;
  document.querySelectorAll('[data-ago]').forEach(function(e){e.textContent=ago(+e.dataset.ago)});
  var nx=$('ck_nx');if(nx)nx.textContent=busy?'đang kiểm tra…':Math.max(0,Math.ceil((nextAt-Date.now())/1000))+'s';
  var sig=order.map(function(n){var a=tr(n);return n+(a?isOn(a)+'|'+a.level:'')}).join()+'|'+hideAccounts;if(sig!==lastSig)render(true);
}
function render(force){
  var grid=$('ck_grid');if(!grid)return;
  order.forEach(function(n){if(CK[n].s==='banned'||CK[n].s==='banned600')CK[n].s='invalid'});   // du lieu cu da luu trang thai Banned -> coi la Dead
  var cnt={valid:0,lock:0,rejoin:0};
  order.forEach(function(n){var s=CK[n].s;if(s==='valid')cnt.valid++;else if(s==='lock')cnt.lock++;else if(s==='rejoin')cnt.rejoin++});
  var bgx=function(t,c){return '<span class="bg" style="color:'+c+';border-color:'+c+'">'+t+'</span>'};
  $('ck_sum').innerHTML=order.length?(bgx('Live '+cnt.valid,'#4ade80')+bgx('Lock '+cnt.lock,'#f43f5e')+bgx('Rejoin '+cnt.rejoin,'#fbbf24')):'';
  var badn=order.filter(function(n){return CK[n].s==='invalid'});
  $('ck_warn').innerHTML=badn.length?'<div class="ckw">⚠ '+badn.length+' account cần chú ý: '+badn.map(function(n){return esc(shown(n,order.indexOf(n)))}).join(', ')+'</div>':'';
  lastSig=order.map(function(n){var a=tr(n);return n+(a?isOn(a)+'|'+a.level:'')}).join()+'|'+hideAccounts;
  var CKG=[['valid','Live','#4ade80',{valid:1}],['rejoin','Captcha rejoin','#fbbf24',{rejoin:1,disconnect:1}],['lock','Lock','#f43f5e',{lock:1}],['other','Khác','#8a919c',null]];
  var cardOf=function(n,i){
    var c=CK[n],l=LB[c.s]||['?','#8a919c'],a=tr(n),info=(a?placeLabel(a)+' · Lv '+fmt(a.level):'Chưa thấy trong tracker')+(a&&FLB[a.flag]?' · Cờ: '+FLB[a.flag][0]:'')+(c.stored?' · 🔒 đã lưu cookie':'');
    return '<div class="ckc'+(c.ing?' ing':'')+'" style="--c:'+l[1]+'"><button class="bt ckx" data-rm="'+esc(n)+'" type="button" title="Bỏ theo dõi">✕</button><b>'+esc(shown(n,i))+'</b>'
     +'<div><span class="cks"><span class="ckd"></span>'+(c.ing?'Check…':l[0])+'</span></div>'
     +'<div class="ckm">'+esc(info)+'<br>Check lần cuối: <span data-ago="'+(c.at||0)+'">'+ago(c.at)+'</span></div></div>';
  };
  if(!order.length){grid.style.display='';grid.innerHTML='<div class="empty" style="grid-column:1/-1">Chưa có account nào. Dán cookie vào ô phía trên rồi bấm "Thêm & theo dõi liên tục".</div>'}
  else{
    // chia theo dang: moi dang 1 vung rieng; doi trang thai -> the tu nhay sang vung moi ngay lan render sau
    var buckets={};CKG.forEach(function(g){buckets[g[0]]=[]});
    order.forEach(function(n,i){
      var s=CK[n].s,g=CKG.filter(function(x){return x[3]&&x[3][s]})[0]||CKG[CKG.length-1];buckets[g[0]].push(cardOf(n,i));
    });
    grid.style.display='block';
    grid.innerHTML=CKG.filter(function(g){return buckets[g[0]].length}).map(function(g){
      return '<section class="ckz" style="--c:'+g[2]+'"><div class="ckzh"><b>'+g[1]+'</b><span>'+buckets[g[0]].length+'</span></div><div class="ckgrid" style="margin-top:8px">'+buckets[g[0]].join('')+'</div></section>';
    }).join('');
  }
  var nw=$('ck_nxw');if(nw)nw.style.display=order.length?'':'none';
}
setTimeout(init,0);
}catch(e){console.error('cookie ui',e)}
})();
`;
// ===== TAB CONFIG: link workspace + chon account -> sua config trong 6 o (Farm / Parties / Mod / Auto Sell / Webhook / Visuals) =====
const CFG_UI_JS = String.raw`
(function(){
try{
var tabs=$('tabs'),tf=$('tf');
if(!tabs||!tf||document.querySelector('.tab[data-tab="config"]'))return;
var css=document.createElement('style');
css.textContent='.tabs .tab[data-tab="config"]::before{content:"⚙️"}.tabs .tab[data-tab="config"]{order:9}'
+'.cfg-bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-top:8px}'
+'.cfg-bar select,.cfg-bar input{background:var(--cd);color:var(--tx);border:1px solid var(--bd);border-radius:8px;padding:7px 9px;font:inherit;min-width:0}'
+'.cfg-bar .ws{flex:1 1 360px}'
+'.cfg-lb{font-size:14px;color:var(--mu);font-weight:700}'
+'.cfg-b{background:var(--cd);color:var(--tx);border:1px solid var(--bd);border-radius:8px;padding:7px 12px;cursor:pointer;font:600 13px system-ui,sans-serif}'
+'.cfg-b.p{background:var(--cy);color:#042;border-color:var(--cy)}.cfg-b:disabled{opacity:.4;cursor:default}'
+'.cfg-b.p,#ck_add,#ha{background:linear-gradient(135deg,#3b3e46,#17181c)!important;color:#fff!important;border:1px solid #4a4d56!important;border-radius:12px;padding:9px 18px;font-weight:600;box-shadow:0 2px 10px rgba(0,0,0,.45),inset 0 1px 0 rgba(255,255,255,.1)!important;text-shadow:none;transition:filter .15s,transform .15s}.cfg-b.p:hover:not(:disabled),#ck_add:hover:not(:disabled),#ha:hover{filter:brightness(1.25)}.cfg-b.p:active:not(:disabled),#ck_add:active:not(:disabled),#ha:active{transform:translateY(1px)}'
+'.cfg-msg{margin-top:8px;font-size:13px;color:var(--mu)}.cfg-msg.err{color:var(--pk)}.cfg-msg.ok{color:var(--ok)}'
+'.cfg-g6{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:12px}'
+'@media(max-width:1100px){.cfg-g6{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:720px){.cfg-g6{grid-template-columns:1fr}}'
+'.cfg-box{border:1px solid var(--c);border-radius:12px;background:var(--cd);display:flex;flex-direction:column;max-height:760px;min-width:0;box-shadow:inset 0 0 40px -28px var(--c)}'
+'.cfg-box>h3{display:flex;justify-content:space-between;align-items:center;margin:0;padding:10px 14px;font-size:15px;letter-spacing:.08em;text-transform:uppercase;color:var(--c);border-bottom:1px solid var(--c)}'
+'.cfg-badge{font-size:12px;font-weight:700;letter-spacing:.1em;padding:2px 10px;border:1px solid var(--c);border-radius:999px;box-shadow:0 0 8px var(--c)}'
+'.cfg-bb{overflow:auto;padding:4px 14px 14px}'
+'.cfg-st{font-size:14px;font-weight:700;color:var(--c);margin:14px 0 3px;text-transform:uppercase;letter-spacing:.05em}'
+'.cfg-f{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:12px 14px;margin:7px 0;background:var(--bg);border:1px solid var(--bd);border-radius:10px}'
+'.cfg-f label{color:var(--tx);font-size:15px;word-break:break-word}.cfg-f.chg label{color:var(--am)}'
+'.cfg-f input[type=text],.cfg-f input[type=number],.cfg-f textarea{width:160px;background:var(--bg);color:var(--tx);border:1px solid var(--bd);border-radius:6px;padding:6px 8px;font:15px ui-monospace,monospace}.cfg-f input[type=checkbox]{width:20px;height:20px}'
+'.cfg-sub{padding:6px 14px 0;font-size:12px;color:var(--mu)}'
+'.cfg-sc>summary{cursor:pointer;list-style:none;display:flex;justify-content:space-between;align-items:center;padding:10px 12px;margin-top:10px;background:var(--bg);border:1px solid var(--bd);border-radius:10px;font-size:15px;font-weight:700;color:var(--c)}'
+'.cfg-sc>summary::-webkit-details-marker{display:none}.cfg-sc>summary::after{content:"⌃";font-size:16px}.cfg-sc:not([open])>summary::after{content:"⌄"}'
+'.cfg-sd{font-size:12px;color:var(--mu);padding:6px 4px 2px;line-height:1.4}'
+'.cfg-f select{background:var(--bg);color:var(--tx);border:1px solid var(--bd);border-radius:6px;padding:6px 8px;font:15px system-ui,sans-serif;min-width:140px}'
+'.cfg-f .sw{position:relative;width:68px;height:38px;flex:none}.cfg-f .sw input{position:absolute;inset:0;width:100%;height:100%;margin:0;opacity:0;cursor:pointer;z-index:1}'
+'.cfg-f .sw i{position:absolute;inset:0;border-radius:999px;background:#2b2e36;transition:.15s}.cfg-f .sw i::after{content:"";position:absolute;top:4px;left:4px;width:30px;height:30px;border-radius:50%;background:#e8eaed;transition:transform .15s}'
+'.cfg-box,.cfg-box *{color:#fff!important}.cfg-box .cfg-f input[type=text],.cfg-box .cfg-f input[type=number],.cfg-box .cfg-f textarea,.cfg-box .cfg-f select{color:#fff!important}.cfg-box .cfg-chip button{color:#ff6b8a!important}'
+'.pd-dgrid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:6px;margin:6px 0 8px}.pd-dg{width:100%;text-align:center;padding:8px 4px!important;font-size:14px!important}'
+'.pd-bt{background:var(--bg);border:1px solid var(--c);border-radius:8px;padding:6px 12px;cursor:pointer;font:600 14px system-ui,sans-serif}.pd-bt.on{background:color-mix(in srgb,var(--c) 38%,#0b0c0f);box-shadow:0 0 10px var(--c)}.pd-bt:hover{box-shadow:0 0 8px var(--c)}'
+'.pd-pop{position:absolute;left:0;right:0;top:calc(100% + 6px);z-index:20;max-height:300px;overflow:auto;background:#0b0c0f;border:1px solid var(--c);border-radius:10px;padding:6px;box-shadow:0 8px 24px #000c}'   // left:0;right:0 (khong min-width co dinh nua) de khung luon khop dung be rong hang chua no, tranh tran ra ngoai/de len cac o khac khi hang hep (vd: tren dien thoai)
+'.pd-opt{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:8px;cursor:pointer;font-size:15px}.pd-opt:hover{background:#ffffff14}.pd-opt input{width:18px;height:18px}'
+'.cfg-box .pd-on,.cfg-box .pd-on *{color:#4ade80!important}.cfg-box .pd-off,.cfg-box .pd-off *{color:#8a919c!important}'
+'.cfg-f .sw input:disabled{cursor:default}.cfg-f select:disabled{opacity:.7;cursor:default}'
+'.cfg-box .sw input:checked+i{background:var(--c);box-shadow:0 0 6px var(--c),0 0 18px var(--c)}.cfg-box .sw input:checked+i::after{transform:translateX(30px);background:#fff;box-shadow:0 0 6px #fff,0 0 14px #fff,0 0 22px var(--c)}'
+'.cfg-rt{display:flex;justify-content:space-between;align-items:baseline;gap:10px}.cfg-rv{font-size:15px;opacity:.85;white-space:nowrap}.cfg-rs{font-size:12px;opacity:.55;margin-top:2px}'
+'.cfg-trk{position:relative;height:30px;margin:12px 8px 4px;background:#0b0c0f;border:1px solid var(--bd);border-radius:8px;touch-action:none;cursor:pointer}'
+'.cfg-fill{position:absolute;top:5px;bottom:5px;border-radius:4px;background:linear-gradient(90deg,#f7ff9d,#4ade80);pointer-events:none}'
+'.cfg-h{position:absolute;top:-3px;bottom:-3px;width:12px;margin-left:-6px;border-radius:6px;background:#f2f3f5;border:1px solid #8a919c;cursor:ew-resize;touch-action:none;box-shadow:0 0 8px rgba(0,0,0,.6)}'
+'.cfg-one{height:26px;margin:8px 0 0;overflow:hidden}.cfg-one .cfg-fill{left:0;top:0;bottom:0;border-radius:0;background:linear-gradient(90deg,#7dd3fc,#4ade80)}.cfg-ov{position:absolute;right:10px;top:0;line-height:26px;font:600 14px ui-monospace,monospace;pointer-events:none}'
+'.cfg-box>h3{color:var(--c)!important;text-shadow:0 0 6px var(--c),0 0 16px var(--c),0 0 28px var(--c)}'
+'.cfg-box .cfg-sc>summary{color:var(--c)!important;text-shadow:0 0 6px var(--c),0 0 16px var(--c)}'
+'.cfg-title{font:800 30px/1.2 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;letter-spacing:.04em;margin:6px 2px 10px;color:#5eead4;text-shadow:0 0 8px #5eead4,0 0 22px #5eead4}.cfg-caret{display:inline-block;width:.55em;height:1em;margin-left:4px;vertical-align:-.12em;background:#5eead4;box-shadow:0 0 8px #5eead4;animation:cfgblink 1s steps(1) infinite}@keyframes cfgblink{50%{opacity:0}}'
+'.cfg-b.cfg-del{background:#2a0f14;border-color:#f43f5e;color:#ff6b8a!important;text-shadow:0 0 8px #f43f5e}.cfg-b.cfg-del:hover{background:#3a1219}'
+'.cfg-th{display:grid;grid-template-columns:repeat(7,1fr);gap:4px;margin-top:10px;padding:5px;background:#0b0c0f;border:1px solid var(--bd);border-radius:10px}.cfg-th{position:relative;touch-action:none;cursor:pointer}.cfg-ts{height:24px;border-radius:5px;opacity:.9}.cfg-th.auto{cursor:default}.cfg-th.auto .cfg-tk{transition:left 1s linear}.cfg-tk{position:absolute;top:0;bottom:0;width:12px;margin-left:-6px;border-radius:6px;background:#f2f3f5;border:1px solid #8a919c;box-shadow:0 0 8px rgba(0,0,0,.6);transition:left .28s cubic-bezier(.3,.9,.3,1);pointer-events:none}.cfg-tv{color:#5eead4!important}'
+'.cfg-xl{display:grid;grid-template-columns:1fr;gap:6px;margin:8px 0 2px}.cfg-ids{display:flex;flex-direction:column;gap:6px;margin-top:8px}.cfg-ids input{width:100%!important;box-sizing:border-box}'
+'.cfg-f.g-sell .cfg-fill{background:linear-gradient(90deg,#a5b4fc,#f9a8d4)}.cfg-f.g-fps .cfg-fill{background:linear-gradient(90deg,#7f1010,#ff1e1e)}.cfg-f.g-wait .cfg-fill{background:linear-gradient(90deg,#a5f3fc,#a78bfa 50%,#f9a8d4)}.cfg-nt{font-size:13px;opacity:.7;padding:6px 4px 0}'
+'.cfg-f.off .cfg-trk{opacity:.35}.cfg-f.off .cfg-fill{background:#555}.cfg-f.off .cfg-rt{opacity:.5}'
+'.cfg-box input[type=number]{-moz-appearance:textfield;appearance:textfield}.cfg-box input[type=number]::-webkit-inner-spin-button,.cfg-box input[type=number]::-webkit-outer-spin-button{-webkit-appearance:none;appearance:none;margin:0;display:none}'
+'.cfg-f input.n2{width:70px}.cfg-f textarea{height:64px;resize:vertical}'
+'.cfg-f input.bad{border-color:var(--pk)}.cfg-f input[type=color]{width:30px;height:24px;padding:0;border:0;background:none}'
+'.cfg-chips{display:flex;flex-wrap:wrap;gap:6px;padding:6px 0}.cfg-chip{background:var(--bg);border:1px solid var(--bd);border-radius:999px;padding:3px 6px 3px 12px;font-size:14px}'
+'.cfg-chip button{background:none;border:0;color:var(--pk);cursor:pointer;font-size:14px}'
+'.cfg-adv{margin-top:12px;border:1px solid var(--bd);border-radius:12px;background:var(--cd)}.cfg-adv>summary{cursor:pointer;padding:10px 14px;font-weight:700}'
+'.cfg-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:2px 16px;padding:2px 14px 12px}'
+'.pty-wrap{margin-top:8px}.pty-head{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:12px 16px;border:1px solid var(--bd);border-radius:14px;background:var(--cd)}'
+'.pty-head .sw{flex:none}.pty-htxt{flex:1;min-width:160px}.pty-htxt b{font-size:15px}.pty-htxt div{font-size:12px;color:var(--mu);margin-top:2px}'
+'.pty-badges{display:flex;gap:8px;flex-wrap:wrap}.pty-badge{background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:6px 10px;font-size:12px;color:var(--mu);white-space:nowrap}.pty-badge b{color:var(--tx)}'
+'.pty-act{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-top:10px}'
+'.pty-list{margin-top:10px;display:flex;flex-direction:column;gap:8px}'
+'.pty{border:1px solid var(--bd);border-radius:12px;background:var(--bg)}.pty[open]{border-color:var(--c)}.pty>summary{border-radius:12px}.pty[open]>summary{border-radius:12px 12px 0 0}'
+'.pty>summary{list-style:none;cursor:pointer;display:flex;align-items:center;gap:10px;padding:10px 14px}.pty>summary::-webkit-details-marker{display:none}'
+'.pty-chev{flex:none;font-size:12px;color:var(--mu);transition:transform .15s;display:inline-block}.pty[open] .pty-chev{transform:rotate(90deg)}'
+'.pty-main{flex:1;min-width:0;display:flex;align-items:center;gap:14px;flex-wrap:wrap}'
+'.pty-nm{min-width:96px}.pty-nm b{font-size:14px}'
+'.pty-dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px}.pty-dot.on{background:#4ade80;box-shadow:0 0 6px #4ade80}.pty-dot.off{background:#8a919c}'
+'.pty-stat{font-size:12px;color:var(--mu);margin-top:3px}'
+'.pty-av{display:flex;align-items:center;gap:3px;flex-wrap:wrap;max-width:200px}'
+'.pty-avi{width:20px;height:20px;border-radius:50%;background:var(--cd);border:1px solid var(--bd);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:var(--mu);flex:none}.pty-avi.on{border-color:#4ade80;color:#4ade80}'
+'.pty-dun{flex:1;min-width:170px}.pty-dun b{font-size:13px}'
+'.pty-sub{font-size:12px;color:var(--mu);margin-top:3px}'
+'.pty-tgl{flex:none}'
+'.pty-body{padding:2px 14px 14px;border-top:1px solid var(--bd)}'
+'.pty-subhead{margin:10px 0 2px}.pty-subhead b{font-size:14px;color:var(--c)}.pty-subhead div{font-size:12px;color:var(--mu);margin-top:3px;line-height:1.4}'
+'.pty-sw{position:relative;width:38px;height:22px;flex:none}.pty-sw input{position:absolute;inset:0;width:100%;height:100%;margin:0;opacity:0;cursor:pointer;z-index:1}'
+'.pty-sw i{position:absolute;inset:0;border-radius:999px;background:#2b2e36;transition:.15s}.pty-sw i::after{content:"";position:absolute;top:3px;left:3px;width:16px;height:16px;border-radius:50%;background:#e8eaed;transition:transform .15s}'
+'.pty-sw input:checked+i{background:var(--c);box-shadow:0 0 6px var(--c),0 0 18px var(--c)}.pty-sw input:checked+i::after{transform:translateX(16px);background:#fff;box-shadow:0 0 6px #fff,0 0 14px #fff,0 0 22px var(--c)}'; // ten class rieng (khong phai .sw) de khong bi cac rule .cfg-box .sw / .cfg-f .sw co san de ra (dinh cho nut gat 68x38 trong .cfg-box) danh de len, gay lech nut gat 38x22 cua Party
document.head.appendChild(css);
tabs.insertAdjacentHTML('beforeend','<button class="tab" data-tab="config" type="button" title="Config">Config</button>');
var pane=document.createElement('div');pane.className='tabpane';pane.dataset.pane='config';pane.id='pane-config';pane.style.display='none';
pane.innerHTML='<div id="cfg_root" style="min-height:200px"></div>';
tf.appendChild(pane);
var root=pane.querySelector('#cfg_root');

// ---------- 6 O: moi muc = dung key that trong file release_<userId>.json (key khong co trong file thi tu an) ----------
// t: bool | num | text | color | list | secret | pair (k + k2 = min/max) | protect (object ten-item -> true)
var PANELS=[
{t:'Farm Settings',c:'#f43f5e',sub:'The loop, the class, and the master switch.',s:[
 {t:'Class',r:[{k:'classProfile',l:'Class (auto)',t:'sel',lock:true,acc:true,o:[['mage','Mage'],['warrior','Warrior']]},{virtual:true,l:'Class from the stats',t:'lockon'}]},
 {t:'Autofarm',d:'The master switch and the bonus boss, for every account.',r:[{k:'autoQueue',l:'Enable Autofarm',t:'bool'},{k:'bonusBoss',l:'Stay for the bonus boss',t:'bool'}]},
 {t:'Gear, Spells & Stats',d:'Wears the strongest weapon, helmet and armor for the Class picked above, puts its best spells on and spends its skill points on its stat. Auto sell never sells what it picks. Same on every account on this PC.',r:[{k:'bestGearAuto',l:'Auto mode',t:'bool'},{k:'bestGearPriority',l:'Priority',t:'xsel',o:[['Damage','Damage'],['Balanced','Balanced']]},{k:'bestGearSpells',l:'Equip best spells',t:'bool'},{k:'bestGearSkills',l:'Spend skill points',t:'bool'},{k:'bestGearOutOfRun',l:'Only swap outside a run',t:'bool'}]},
 {t:'Connection',d:'How far ahead of itself the bot has to dodge to allow for the round trip.',r:[{k:'pingManual',l:'Ping',t:'slide',min:0,max:500,step:1,unit:'ms',sub:'ms'}]}
]},
{t:'Parties & Runs',c:'#fbbf24',sub:'Every run starts here, from a party - even a party of one.',custom:'parties',s:[]},
{t:'Mod Detection',c:'#4ade80',sub:'Strangers in your runs and big drops: what every account does about them.',s:[
 {t:'Detection',d:'A stranger in the run: every account stops, then rests before queueing again. A big drop: leave and wait before queueing again.',r:[{k:'modDetect',l:'Enabled',t:'bool'},{k:'modKick',l:'Kick instead of the lobby',t:'bool'},{k:'whModDetect',l:'Webhook on detection',t:'bool'}]},
 {t:'Whitelist',d:'Friends who may be in your runs. Usernames, not display names.',r:[{k:'modWhitelist',l:'Friends',t:'friends'}]},
 {t:'After a big drop',d:'A drop ticked here: the account finishes the run, goes to the lobby and waits before queueing again.',r:[{l:'Leave the run after a drop of:',t:'note',virtual:true},{k:'modLeaveLegendary',l:'Legendary',t:'bool'},{k:'modLeaveUltimate',l:'Ultimate',t:'bool'},{k:'modLeaveValT3',l:'Valhalla T3 Mage / Warrior',t:'bool'},{k:'modLeaveEpic',l:'Any epic',t:'bool'},{k:'modLeaveValAny',l:'Any Valhalla piece',t:'bool'},{k:'modWaitMin',k2:'modWaitMax',l:'Wait before requeueing',t:'range',min:1,max:60,step:1,unit:'min',sub:'A random time between the two handles',grad:'wait'}]},
 {t:'Breaks',d:'Farm for a while, then rest in the lobby before queueing again - the whole party together, like the wait after a big drop.',r:[{k:'breakFarmMin',k2:'breakFarmMax',l:'Take breaks',t:'brkon'},{k:'breakFarmMin',k2:'breakFarmMax',l:'Farm for',t:'range',min:1,max:24,step:0.5,unit:'h',sub:'A random time between the two handles',brk:1},{k:'breakRestMin',k2:'breakRestMax',l:'Then rest for',t:'range',min:5,max:360,step:5,unit:'min',def:[20,40],virtual:true,sub:'A random time between the two handles',brk:1}]}
]},
{t:'Auto Sell',c:'#60a5fa',sub:'Sell what drops, keep what matters.',s:[
 {t:'Auto sell',d:'Sells unequipped items out of your bag every few games right in the dungeon, and whenever it arrives in the lobby.',r:[{k:'autoSell',l:'Enabled',t:'bool'},{k:'sellAfterGames',l:'Sell after',t:'slide',min:1,max:50,step:1,unit:'games',sub:'games',grad:'sell'}]},
 {t:'Rarities to sell',d:'Rarity belongs to the drop, so this is read off each item as it is found.',r:[{k:'sellCommon',l:'Common',t:'bool'},{k:'sellUncommon',l:'Uncommon',t:'bool'},{k:'sellRare',l:'Rare',t:'bool'},{k:'sellEpic',l:'Epic',t:'bool'},{k:'sellLegendary',l:'Legendary',t:'bool'},{k:'sellUltimate',l:'Ultimate',t:'bool'}]},
 {t:'Limits',d:'The guards that apply on top of the rarities.',r:[{k:'sellKeepLevel',l:'Keep level and above',t:'num'}]},
 {t:'Protected items',d:'Items you keep in every rarity - they are never sold.',r:[{k:'sellKeep',l:'Protected',t:'protect'}]}
]},
{t:'Webhook',c:'#a78bfa',sub:'Post a summary of every run to a Discord channel.',s:[
 {t:'',r:[{k:'webhook',l:'Send to Discord',t:'bool'},{k:'webhookUrl',l:'Webhook URL',t:'secret'},{k:'webhookUser',l:'Bot name',t:'text'}]},
 {t:'What to send',d:'Everything is on to begin with; switch off what you do not want posted.',r:[{k:'whHideName',l:'Hide your name',t:'bool'},{k:'whStarted',l:'Dungeon started',t:'bool'},{k:'whWins',l:'Wins / losses',t:'bool'},{k:'whRodin',l:'Rodin clears',t:'bool'},{k:'whRatio',l:'Win rate',t:'bool'},{k:'whDeaths',l:'Deaths that round',t:'bool'},{k:'whItems',l:'Items',t:'bool'},{k:'whTime',l:'Run time',t:'bool'},{k:'whDungeon',l:'Dungeon and difficulty',t:'bool'},{k:'whGold',l:'Gold and gems',t:'bool'},{k:'whWorth',l:'Haul value',t:'bool'},{k:'whInv',l:'Bag space',t:'bool'},{k:'whLevel',l:'Character level',t:'bool'},{k:'whLate',l:'Late drops',t:'bool'},{k:'whBest',l:'Best drop',t:'bool'},{k:'whGodpot',l:'Godpot %',t:'bool'},{k:'whImages',l:'Drop artwork',t:'bool'},{k:'whAccent',l:'Rarity color',t:'bool'}]},
 {t:'Stats',d:'Send what each run did to the key bot, so it can be totalled up and shown with /stats.',r:[{k:'statsEnabled',l:'Send run stats',t:'bool'},{k:'statsKey',l:'License key',t:'secret'}]},
 {t:'Ping on a good drop',d:'Mention yourself - or everyone you list - in the post when something worth stopping for lands.',r:[{k:'whPingMin',l:'Ping on',t:'xsel',o:[['off','Off'],['epic','Epic and up'],['legendary','Legendary and up'],['ultimate','Ultimate only']]},{k:'whPingValT3',l:'Valhalla T3 Mage / Warrior',t:'bool'},{k:'whPingId',l:'Discord user IDs',t:'ids'}]}
]},
{t:'Visuals',c:'#f472b6',sub:'Theme, notifications, screen, minimap, item cards, streamer mode.',s:[
 {t:'Color theme',d:'Seven colors, every one animated. Chroma runs a rainbow through all of it instead.',r:[{k:'colorThemeBase',k2:'colorTheme',l:'Color',t:'theme'},{k:'colorTheme',l:'Chroma',t:'chroma'}]},
 {t:'Streamer mode',d:'Hide anything on screen that says who is playing.',r:[{k:'streamer',l:'Streamer mode',t:'bool'},{k:'streamerNameText',l:'Name',t:'text'},{k:'streamerHealthText',l:'Health',t:'text'},{k:'streamerLevelText',l:'Level',t:'text'},{k:'streamerGoldText',l:'Gold',t:'text'},{k:'streamerGemsText',l:'Gems',t:'text'},{k:'streamerXpText',l:'XP',t:'text'},{k:'streamerBody',l:'Hitbox as body',t:'bool'},{k:'streamerBodyColor',l:'Hitbox color',t:'color'},{k:'streamerOthers',l:"Hide other players' names",t:'bool'},{k:'streamerLeaderboard',l:'Hide the player lists',t:'bool'},{k:'streamerChat',l:'Hide the chat',t:'bool'}]},
 {t:'Notifications',d:'A card in the bottom right whenever something happens.',r:[{k:'notifications',l:'Notifications',t:'bool'}]},
 {t:'Screen',d:'What is on screen and how much the game draws: the HUD, the FPS cap, and the world not drawn at all.',r:[{k:'showHud',l:'Show the HUD',t:'bool',virtual:true,def:false},{k:'fpsCap',l:'Cap FPS',t:'bool',virtual:true,def:true},{k:'fpsCarry',l:'Carry FPS',t:'slide',min:5,max:120,step:1,unit:'fps',sub:'fps',grad:'fps',virtual:true,def:45},{k:'fpsLeech',l:'Leech FPS',t:'slide',min:5,max:120,step:1,unit:'fps',sub:'fps',grad:'fps',virtual:true,def:15},{k:'fpsUncap',l:'Uncap FPS',t:'bool'},{k:'renderOff',l:'Ultimate low quality',t:'bool'}]},
 {t:'Minimap',d:'The minimap in the top right, the MAP VIEW it opens and the key for it.',r:[{k:'minimap',l:'Show the minimap',t:'bool'},{k:'mapButton',l:'Map button',t:'bool'},{k:'mapFollow',l:'Follow player',t:'bool'},{k:'minimapSmooth',l:'Smooth 2D map',t:'bool'},{k:'mapAttackOneColor',l:'Custom attack color',t:'bool'},{k:'mapAttackRainbow',l:'Rainbow attacks',t:'bool'},{k:'mapAttackColor',l:'Attack color',t:'color'},{k:'mapKey',l:'Map key',t:'text'}]},
 {t:'Item cards',d:'How the game\'s item cards show their numbers.',r:[{k:'itemStatsAbbrev',l:'Abbreviate item stats',t:'bool'},{k:'upgradeCountAbbrev',l:'Abbreviate the upgrade count',t:'bool'},{k:'itemGodpot',l:'Godpot %',t:'bool'}]}
]}
];
var SECRET={webhookUrl:1,statsKey:1};
var st={vals:{},orig:{}};   // chi de cac o du lieu chung (Parties) khong loi
var ALT={vals:{},orig:{},mixed:[],files:0,secrets:[],err:'',msg:'',bad:false},STORE=ALT;
var SELUID='';   // '' = Alt (TAT CA account). Khac '' = uid cua 1 account rieng -> Save/doc chi ap dung cho account nay
function dqLocal(path){try{var L=window.__DQ_LOCAL__;if(L&&L.url&&/^\/api\/(config|rejoin|perf)\//.test(path))return L}catch(e){}return null}
function hdr(loc){return {'x-api-key':(loc?loc.key:(typeof KEYV!=='undefined'?KEYV:'')),'x-requested-with':'dq','content-type':'application/json'}}
function el(t,c,x){var e=document.createElement(t);if(c)e.className=c;if(x!=null)e.textContent=x;return e}
function same(a,b){return JSON.stringify(a)===JSON.stringify(b)}
function lsGet(k,d){try{var v=localStorage.getItem(k);return v==null?d:JSON.parse(v)}catch(e){return d}}
function lsSet(k,v){try{localStorage.setItem(k,JSON.stringify(v))}catch(e){}}
async function api(path,opt){
  var L=dqLocal(path);
  var r=await fetch(L?(L.url+path):path,Object.assign({headers:hdr(L)},opt||{}));
  var j=null;try{j=await r.json()}catch(e){}
  if(!r.ok)throw new Error(((j&&(j.message||j.error))||('HTTP '+r.status))+((j&&j.key)?' ['+j.key+']':''));
  return j;
}

// ---------- khung: workspace + chon account (mac dinh = Alt = TAT CA account; chon 1 account roi Save = chi rieng account do) + nut ----------
var wsBar=el('div','cfg-bar'),wsIn=el('input','ws'),wsBtn=el('button','cfg-b','Lưu link');
wsIn.type='text';wsIn.placeholder='Link workspace, vd: C:\\Users\\Ten\\AppData\\Local\\Potassium\\workspace\\HarvardLabs';
wsBar.appendChild(el('span','cfg-lb','Workspace'));wsBar.appendChild(wsIn);wsBar.appendChild(wsBtn);
var bar=el('div','cfg-bar'),acSel=document.createElement('select'),bSave=el('button','cfg-b p','Save'),bDel=el('button','cfg-b cfg-del','Delete config (TẤT CẢ)');
bar.appendChild(el('span','cfg-lb','Áp dụng cho'));bar.appendChild(acSel);bar.appendChild(bSave);bar.appendChild(bDel);
var speedHost=el('span','cfg-speed');speedHost.style.cssText='margin-left:auto;min-width:220px';bar.appendChild(speedHost);
var SPEED_FIELD={k:'altFollow',l:'Chạy nhanh',t:'slide',max:100,step:1,unit:'%',sub:'Tốc độ chạy (key altFollow trong file config)'};
function speedSync(){STORE=ALT;speedHost.textContent='';if(has(SPEED_FIELD.k))speedHost.appendChild(row(SPEED_FIELD))}
function acOptions(){
  var opts=[['','Alt (tất cả account)']],had=false;
  accounts().forEach(function(a){if(a.uid)opts.push([String(a.uid),nm(a.name)])});
  acSel.textContent='';
  opts.forEach(function(o){var op=document.createElement('option');op.value=o[0];op.textContent=o[1];if(o[0]===SELUID)had=true;acSel.appendChild(op)});
  if(!had)SELUID='';
  acSel.value=SELUID;
}
var msg=el('div','cfg-msg'),g6=el('div','cfg-g6');
var ttl=el('div','cfg-title'),ttx=el('span',0,''),tcr=el('span','cfg-caret','');ttl.appendChild(ttx);ttl.appendChild(tcr);root.appendChild(ttl);
(function(){var full='Config ALT',n=0;function tick(){if(!ttl.isConnected&&n>0)return;ttx.textContent=full.slice(0,++n);if(n<full.length)setTimeout(tick,110+Math.random()*90)}setTimeout(tick,250)})();
root.appendChild(wsBar);root.appendChild(bar);root.appendChild(msg);root.appendChild(g6);
function say(t,c){msg.className='cfg-msg'+(c?' '+c:'');msg.textContent=t||''}
function altChanged(){var o=[];for(var k in ALT.vals)if(!same(ALT.vals[k],ALT.orig[k]))o.push(k);return o}
function refresh(){
  var n=altChanged().length;
  bSave.disabled=!(ALT.files>0||PD.alt||PD.pass||PD.auto);
  bSave.textContent=SELUID?'Save (account này)':'Save (tất cả Alt)';
  root.querySelectorAll('.cfg-f[data-k]').forEach(function(r){if(!r.dataset.alt)return;var k2=r.dataset.k2;r.classList.toggle('chg',!same(ALT.vals[r.dataset.k],ALT.orig[r.dataset.k])||(k2&&!same(ALT.vals[k2],ALT.orig[k2])))});
}
function setS(S,k,v){S.vals[k]=v;refresh()}
function has(k){return Object.prototype.hasOwnProperty.call(STORE.vals,k)}

// ---------- tung kieu o nhap ----------
function numInput(k,cls){
  var S=STORE,i=document.createElement('input');i.type='number';i.step='any';i.value=S.vals[k];if(cls)i.className=cls;
  i.oninput=function(){var n=parseFloat(i.value);i.classList.toggle('bad',!isFinite(n));if(isFinite(n))setS(S,k,n)};return i;
}
function ctl(c,row){
  var S=STORE,k=c.k,v=S.vals[k],i;
  if(c.t==='pair'){
    var w=el('div');w.style.cssText='display:flex;gap:4px;align-items:center';
    w.appendChild(numInput(k,'n2'));w.appendChild(el('span','cfg-lb','–'));w.appendChild(numInput(c.k2,'n2'));return w;
  }
  if(c.t==='sel'){
    i=document.createElement('select');var seen=false;
    c.o.forEach(function(o){var op=el('option',0,o[1]);op.value=o[0];if(o[0]===v)seen=true;i.appendChild(op)});
    if(!seen){var o2=el('option',0,String(v));o2.value=v;i.appendChild(o2)}
    i.value=v;i.onchange=function(){setS(S,k,i.value)};if(c.lock)i.disabled=true;return i;
  }
  if(c.t==='brkon'){
    var swb=el('div','sw');i=document.createElement('input');i.type='checkbox';
    function isOn(){return !(S.vals[c.k]>=24&&S.vals[c.k2]>=24)}
    i.checked=isOn();
    i.onchange=function(){
      if(i.checked){var p=BRK.prev||[3,5];setS(S,c.k,p[0]);setS(S,c.k2,p[1])}
      else{BRK.prev=[S.vals[c.k],S.vals[c.k2]];setS(S,c.k,24);setS(S,c.k2,24)}
      brkSync();
    };
    row._sync=function(){i.checked=isOn()};row.dataset.brkon='1';
    swb.appendChild(i);swb.appendChild(document.createElement('i'));return swb;
  }
  if(c.t==='chroma'){
    var swc=el('div','sw');i=document.createElement('input');i.type='checkbox';i.checked=S.vals.colorTheme==='Chroma';
    i.onchange=function(){S.vals.colorTheme=i.checked?'Chroma':(S.vals.colorThemeBase||'Crimson');refresh();syncChroma()};
    row.dataset.chroma='1';row._sync=function(){i.checked=S.vals.colorTheme==='Chroma'};
    swc.appendChild(i);swc.appendChild(document.createElement('i'));return swc;
  }
  if(c.t==='lockon'){var sw0=el('div','sw');var i0=document.createElement('input');i0.type='checkbox';i0.checked=true;i0.disabled=true;sw0.appendChild(i0);sw0.appendChild(document.createElement('i'));return sw0}
  if(c.t==='bool'){
    var sw=el('div','sw');i=document.createElement('input');i.type='checkbox';i.checked=(typeof v==='boolean')?v:!!c.def;i.onchange=function(){setS(S,k,i.checked)};
    sw.appendChild(i);sw.appendChild(document.createElement('i'));return sw;
  }
  if(c.t==='num')return numInput(k);
  if(c.t==='color'){
    var w2=el('div');w2.style.cssText='display:flex;gap:6px;align-items:center';
    var cc=document.createElement('input');cc.type='color';cc.value=v;
    var ct=document.createElement('input');ct.type='text';ct.value=v;
    cc.oninput=function(){ct.value=cc.value;setS(S,k,cc.value)};
    ct.oninput=function(){setS(S,k,ct.value);if(/^#[0-9a-fA-F]{6}$/.test(ct.value))cc.value=ct.value};
    w2.appendChild(cc);w2.appendChild(ct);return w2;
  }
  if(c.t==='list'){
    i=document.createElement('textarea');i.value=(v||[]).join('\n');
    i.oninput=function(){setS(S,k,i.value.split(/\r?\n/).map(function(x){return x.trim()}).filter(Boolean))};return i;
  }
  if(c.t==='secret'){
    i=document.createElement('input');i.type='text';i.value=(typeof v==='string')?v:'';
    i.placeholder=(ALT.secrets.indexOf(k)>=0)?'(đã ẩn - nhập mới để đổi)':'(trống)';
    i.oninput=function(){setS(S,k,i.value)};return i;
  }
  i=document.createElement('input');i.type='text';i.value=v;
  if(c.dl){var id='cfgdl_'+k,dl=document.createElement('datalist');dl.id=id;c.dl.forEach(function(o){var op=document.createElement('option');op.value=o;dl.appendChild(op)});row.appendChild(dl);i.setAttribute('list',id)}
  i.oninput=function(){setS(S,k,i.value)};return i;
}
var DG={"Desert Temple":{"a":["Whirlwind","Fireball","Slam","Blue Fireball","Arcane Spray","Poison Cloud","Battle Shout","Innervate","Gale Slice","Lightning Strikes","Holy Barrier","Redemption","Holy Circle"],"w":["Bronze Gladius","Bronze Daggers","Thorned Club","Double Edged Battle Axe","Steel Serrated Sword","Steel Scimitars","Steel Lance","Steel Warhammer","Steel Executioner","Golden Hammer","Golden Palm Axe","Golden Kopesh","Desert Wrath","Elemental Wand","Elemental Staff","Silver Gladius","Infernal Staff","Amber Wand","Silver Koftgari","Crystal Scepter","Silver Scythe","Golden Cutlasses","Golden Kopesh Glaive","Holy Sun Staff","Desert Mirage"],"s":["Apprentice Warrior","Adept Warrior","Sandstone Warrior","Apprentice Mage","Adept Mage","Sandstone Mage","Apprentice Guardian","Adept Guardian","Sandstone Guardian"]},"Winter Outpost":{"a":["Battle Shout","Innervate","Gale Slice","Arcane Barrage","Ice Needles","Rejuvenating Spray","Fire Bomb","Lightning Beam","Taunting Aura","Aura of Life","Ground Slam","Searing Beam","Piercing Rain"],"w":["Iceblade","Frost Maul","Ice Sickles","Coldstone Cleaver","Coldstone Glaive","Coldstone Axes","Frostshard Battle Axe","Frostshard Hammer","Frostshard Twinblades","Winterborne","Ice Wand","Frozen Spelldaggers","Frosty Staff","Coldstone Blades","Coldstone Greatstaff","Coldstone Spellscythe","Frostshard Trident","Frostshard Lance","Frostshard Greatstaff","Frostwarden"],"s":["Tundra Warrior","Frostbound Warrior","Winter Lord's Warrior","Tundra Mage","Frostbound Mage","Winter Lord's Mage","Tundra Guardian","Frostbound Guardian","Winter Lord's Guardian"]},"Pirate Island":{"a":["Demonic Strike","Ghostly Cannon Barrage","Guardian Call","Universal Heal","Pulsefire","Enchanted Spinning Blades","Phantom Flames","Skull Flames"],"w":["Stone Strongsword","Mighty Flatblade","Molten Blade","Cultist Blade","Empowered Crystal Blade","Templar Blade","Ocean Splitter","Onyx Empowered Greatsword","Devilish Greataxe","Titan's Greataxe","Molten Greataxe","Soulstealer Greatsword","Ruby Staff","Holy Wand","Arcane Orb Staff","Greater Dark Scythe","Earth Wand","Solar Staff","Angelic Staff","Dark Priest Staff","Greater Prophet Staff","Staff of the Gods"],"s":["Pirate King's Warrior","Inferno Plate","Godly Warrior","Pirate King's Mage","Angelic Plate","Godly Mage","Pirate King's Guardian","Void Plate","Godly Guardian"]},"King's Castle":{"a":["Chain Heal","Glacial Blows","Thunderous Blast","Guardian's Blessing","Blade Throw","Void Beam","Electric Boom"],"w":["Goblin Cleaver","Dual Cutlasses","Beast Cleaver","Dual Royal Axes","Heavenly Greatsword","High Priest Battleaxe","Ethereal Greataxe","Dual Frozen Blades","Dual Inferno Slicers","Dual Jade Daggers","Beastmaster War Scythe","Tribal Staff","Eagle Staff","Infernal Sun Staff","Elven Staff","Apothecary Staff","Moonlight Staff","Sunlight Staff","Celestial Fire Staff","Arclight Battlestaff","Fire and Ice Staff","Beastmaster Spell Scythe"],"s":["Barbaric Warrior","Beastly Warrior","Titan-forged Warrior","Barbaric Mage","Arch-Mage","Titan-forged Mage","Barbaric Guardian","Beastly Guardian","Titan-forged Guardian"]},"The Underworld":{"a":["Rending Slice","Infernal Orbs","Demonic Spikes","Infernal Strike","Ice Nova","Ice Totem"],"w":["Dual Sapphire Slicers","Blacksteel Greatsword","Dual Flatblade Slicers","Royal Crimson Greatsword","Dual Ancient Daggers","Timelost Greatsword","Timelost Warhammer","Nature Scythe","Enchanted Demonic Greataxe","Dual Phoenix Daggers","Sapphire Greatstaff","Luminous Greatstaff","Hope's Triumph","Dragon's Claw Scythe","Timelost Spellbook","Dual Arcane Spelldaggers","Vampiric Greatstaff","Ancient Elder Staff","Demon Lord's Greatstaff","Phoenix Greatstaff"],"s":["Tribal Warrior","Timelost Warrior","Glorious Warrior","Tribal Mage","Timelost Mage","Glorious Mage","Reinforced Guardian","Timelost Guardian","Glorious Guardian"]},"Samurai Palace":{"a":["Berserk","Lava Lash","Hand Cannon","Earth Clap","Enchanted Shuriken","Guardian Roar","Flame Cyclone","Ghostly Rampage","Illusion Blast"],"w":["Bone Scythe","Royal Katana","Honorable Greatsword","Dual Infernal Daggers","Dual Samurai Stars","Dual Samurai Sickles","Crimson Kris","Toxic Warspear","Ruby Greatsword","Sakura Katana","Staff of Heat","Industrial Spellscythe","Sapphire Spellspear","Amethyst Spelldagger","Violet Spellblade","Amethyst Crescent Spellblade","Jade Spelldagger","Pink Pearl Spellscythe","Dual Elegant Mage Fans","Sakura Greatstaff"],"s":["Samurai Warrior","Infernal Warrior","Ancestral Warrior","Samurai Mage","Infernal Mage","Ancestral Mage","Samurai Guardian","Infernal Guardian","Ancestral Guardian"]},"The Canals":{"a":["Runic Strike","Electric Slash","Earth Kick","Tsunami","Icicle Barrage","Life Pulse","Blade Revolver","Blade Storm","Forgotten Army","Vortex"],"w":["Stone Scythe","Dual Crimson Daggers","Dual Razor Slicers","Giant Royal Axe","Dual Magma Fangs","Dark Ice Blades","Dual Infernal Scimitars","Bloodthirster","Overlord's Rageblade","Sapphire Stone Wand","Runic Pike","Elementalist Staff","Toxic Spellscythe","Amethyst Spellscythe","Dual Arcane Slicers","Runic Spellscythe","Mana Infused Spellblade","Overlord's Manablade"],"s":["Salvaged Warrior","Rogue Warrior","Overlord's Warrior","Salvaged Mage","Rogue Mage","Overlord's Mage","Salvaged Guardian","Rogue Guardian","Overlord's Guardian"]},"Steampunk Sewers":{"a":["Arrow Rain","Star Barrage","Pulse Waves","Starfall","Life Dash","Explosive Punch","Orb Of Destruction","Chained Energy Blasts","Triple Blade Throw","Overcharge","Chromatic Rain","Arrow Barrage","Chain Lightning"],"w":["Hextech Katana","Dual Fusion Blades","Fusion Greatsword","Bioforged Greatsword","Hextech Greataxe","Bioforged Reinforced Slicer","Bioforged Greataxe","Mighty Fusion Maul","Inventor's Greatsword","Plasma Spellblade","Dual Hextech Spelldaggers","Dual Bioforged Spelldaggers","Hextech Polearm","Plasma Twin Spellblade","Hextech Wired Spellblade","Plasma Infused Cutlass","Mighty Fusion Spellblade","Inventor's Spellblade"],"s":["Titanium Warrior","Industrial Warrior","War-forged Warrior","Titanium Mage","Industrial Mage","War-forged Mage","Titanium Guardian","Industrial Guardian","War-forged Guardian"]},"Ghastly Harbor":{"a":["Earth Spikes","Mighty Leap","Spirit Bomb","Smite","Pulse Beam","Phantom Blades","Void Spheres","Phantom Striker"],"w":["Cracked Tooth Cleaver","Dual Serrated Knives","Dual Pirate Cutlasses","Captain's Anchor","Sharktooth Blade","Seafarer's Lost Blade","Dual Phantom Axes","Dual Phantom Daggers","Kraken Slayer","Poison Spear","Dual Toxic Daggers","Dual Bladed Mage Daggers","Dual Ocean Fists","Magic Trident","Dual Phantom Defenders","Ocean Dweller's Mageblade","Seafarer's Magic Device","Sea Serpent's Wings","Cursed Shipwright"],"s":["Raider's Warrior","Hunter's Warrior","Mythical Warrior","Raider's Mage","Hunter's Mage","Mythical Mage","Raider's Guardian","Hunter's Guardian","Mythical Guardian"]},"Orbital Outpost":{"a":["Focus Beam","Mystery Matter","Explosive Mine","Vortex Grenade","Electric Field","Energy Orb"],"w":["Blazing Star","Dual Solar Blades","Solar Katana","Dual Crystalized Knives","Void Greatsword","Solar Greatsword","Galactic Dual Blades","Electron Spell Dagger","Void Spell Dagger","Void Spell Blade","Moon Crystal Spell Blade","Energized Spell Dagger","Solar Essence Staff","Galactic Pike","Space Mercenary"],"s":["Space Warrior","Commander's Warrior","Alien Warrior","Space Mage","Commander's Mage","Alien Mage","Space Guardian","Commander's Guardian","Alien Guardian"]},"Volcanic Chambers":{"a":["Inner Rage","Molten Shards","Inner Focus","Lava Beam Orb","Enhanced Inner Rage","Enhanced Inner Focus","Blade Fall","Lava Barrage","Amethyst Blast","Lava Cage"],"w":["Dual Fist Blades","Darkstone Greatsword","Hellstone Greataxe","Magma Infused Waraxe","Shattered Magma Blade","Volcanic Greatsword","Lava King's Warscythe","Lava Shard Staff","Lava Tendril Pike","Lava Spellblade","Dual Magma Shields","Ancient Lava Axe","Dual Hellfire Blades","Lava King's Spell Daggers","Lava Demigod"],"s":["Burned Warrior","Molten Forged Warrior","Lava King's Warrior","Burned Mage","Molten Forged Mage","Lava King's Mage","Burned Guardian","Molten Forged Guardian","Lava King's Guardian"]},"Enchanted Forest":{"a":["Piercing Roots","Fungal Poison","Revitalize","Enhanced Inner Rage","Enhanced Inner Focus","Crystalline Cannon","Wind Blast","Agony Orbs","Lightning Burst"],"w":["Fungal Warhammer","Fungal Lance","Enchanted Shard Dual Blade","Enchanted Shard War Axe","Forest Vine Hatchet","Overgrown War Scythe","Eldenbark Greatsword","Fungal Pike","Fungal Staff","Enchanted Shard Spelldagger","Enchanted Shard Staff","Forest Vine Spellblade","Crystalline Shard Staff","Eldenbark Greatstaff","Mushroom Assassin"],"s":["Fungal Warrior","Crystalline Warrior","Eldenbark Warrior","Fungal Mage","Crystalline Mage","Eldenbark Mage","Fungal Guardian","Crystalline Guardian","Eldenbark Guardian"]},"Aquatic Temple":{"a":["Spear Strike","Water Orb","Enhanced Inner Rage","Enhanced Inner Focus","Ice Barrage","Ice Crash","Aquatic Smite","Ice Spikes"],"w":["Serpent Scimitars","Aquatic Executioner","Aquatic Scimitars","Serpent Pike","Aquatic Spear","Oceanic Greatsword","Sea King's Trident","Sea Staff","Enchanted Sea Axes","Aquatic Defender","Enchanted Sea Daggers","Ocean Pearl Staff","Enchanted Serpent Daggers","Sea King's Greatstaff"],"s":["Aquatic Warrior","Riptide Warrior","Triton Warrior","Aquatic Mage","Riptide Mage","Triton Mage","Aquatic Guardian","Riptide Guardian","Triton Guardian"]},"Northern Lands":{"a":["Frost Cone","Flame Strike","Enhanced Inner Rage","Enhanced Inner Focus","Flame Shuriken","Gale Barrage","Geyser","Soul Drain"],"w":["Norse Blade","Northern Runic Blade","Viking Hatchets","Ice Greatspear","Norse Greataxe","Frost Giant Greataxe","Mjolnir","Hofund","Northern Spellstaff","Viking Shield","Ice Spellblade","Viking Sceptre","Runic Mace","Staff of the North","Gungnir","Laevateinn"],"s":["Midgardian Warrior","Jotunn Warrior","Valhalla Warrior","Midgardian Mage","Jotunn Mage","Valhalla Mage","Midgardian Guardian","Jotunn Guardian","Valhalla Guardian"]}};
var DG_NAMES=['Desert Temple','Winter Outpost','Pirate Island',"King's Castle",'The Underworld','Samurai Palace','The Canals','Steampunk Sewers','Ghastly Harbor','Orbital Outpost','Volcanic Chambers','Enchanted Forest','Aquatic Temple','Northern Lands'];
var DG_IDX=null;
function dgIndex(){
  if(DG_IDX)return DG_IDX;
  var norm=function(s){return String(s).toLowerCase().replace(/[^a-z0-9]/g,'')},ex={},pre=[];
  DG_NAMES.forEach(function(d){var e=DG[d];if(!e)return;
    e.a.forEach(function(n){var id=norm(n),o=ex[id]||(ex[id]={n:n,t:'ability',d:{}});o.d[d]=1});
    e.w.forEach(function(n){var id=norm(n),o=ex[id]||(ex[id]={n:n,t:'weapon',d:{}});o.d[d]=1});
    e.s.forEach(function(n){pre.push([norm(n),d])});
  });
  DG_IDX={ex:ex,pre:pre};return DG_IDX;
}
function dgOf(id){
  var ix=dgIndex(),s={};
  if(ix.ex[id])Object.keys(ix.ex[id].d).forEach(function(d){s[d]=1});
  ix.pre.forEach(function(p){if(id.indexOf(p[0])===0)s[p[1]]=1});
  return s;
}
function protect(c,host){
  var S=STORE,k=c.k,box=el('div'),F={q:'',type:'any',rar:'any',dun:'any'},OPEN={};
  function norm(s){return String(s).toLowerCase().replace(/[^a-z0-9]/g,'')}
  function cur(){return S.vals[k]||{}}
  // danh sach item: du lieu wiki (14 dungeon) + kho cac account trong tracker + item dang duoc bao ve
  function catalog(){
    var m={},ix=dgIndex();
    Object.keys(ix.ex).forEach(function(id){var e=ix.ex[id];m[id]={id:id,n:e.n,t:e.t,r:{},d:dgOf(id)}});
    accounts().forEach(function(a){(a.inv||[]).forEach(function(it){
      if(!it||!it.n)return;var id=norm(it.n);if(!id)return;
      var e=m[id]||(m[id]={id:id,n:it.n,t:'',r:{},d:dgOf(id)});
      if(it.t&&!m[id].t)m[id].t=String(it.t).toLowerCase();
      if(it.t&&e.t==='weapon'&&/^(chest|helmet)$/i.test(it.t))e.t=String(it.t).toLowerCase();
      if(it.r)e.r[String(it.r).toLowerCase()]=1;
    })});
    Object.keys(cur()).forEach(function(id){if(!m[id])m[id]={id:id,n:id,t:'',r:{},d:{}}});
    return Object.keys(m).map(function(x){return m[x]}).sort(function(a,b){return a.n.localeCompare(b.n)});
  }
  var all=catalog();
  function uniq(list,fn,base){var s={};(base||[]).forEach(function(x){s[x]=1});list.forEach(function(e){fn(e).forEach(function(x){if(x)s[x]=1})});return Object.keys(s)}
  var TYPES=uniq(all,function(e){return [e.t]},['weapon','chest','helmet','ability']).sort();
  var RARS=uniq(all,function(e){return Object.keys(e.r)},['common','uncommon','rare','epic','legendary','ultimate']);
  var ORD=['common','uncommon','rare','epic','legendary','ultimate'];RARS.sort(function(a,b){var x=ORD.indexOf(a),y=ORD.indexOf(b);return (x<0?99:x)-(y<0?99:y)||a.localeCompare(b)});
  var head=el('div'),sr=el('div','cfg-f');sr.appendChild(el('label',0,'Search'));
  var si=document.createElement('input');si.type='text';si.placeholder='mjolnir';si.oninput=function(){F.q=si.value;drawList()};sr.appendChild(si);
  var ddBox=el('div');head.appendChild(sr);head.appendChild(ddBox);
  function dd(label,key,values,anyLabel){
    var wrap=el('div'),r=el('div','cfg-f');r.style.cursor='pointer';r.appendChild(el('label',0,label));r.firstChild.style.cursor='pointer';
    r.appendChild(el('div',0,(F[key]==='any'?'any':F[key])+'   '+(OPEN[key]?'⌃':'⌄')));
    r.onclick=function(){OPEN[key]=!OPEN[key];drawDD()};wrap.appendChild(r);
    if(OPEN[key]){
      var g=el('div','pd-dgrid');
      [['any',anyLabel||'any']].concat(values.map(function(v){return [v,v]})).forEach(function(o){
        var b=pdBt(o[1],F[key]===o[0],function(){F[key]=o[0];OPEN[key]=false;drawDD();drawList()});b.classList.add('pd-dg');g.appendChild(b)});
      wrap.appendChild(g);
    }
    return wrap;
  }
  function drawDD(){ddBox.textContent='';ddBox.appendChild(dd('Type','type',TYPES));ddBox.appendChild(dd('Dungeon','dun',DG_NAMES,'Every dungeon'));ddBox.appendChild(dd('Rarity','rar',RARS))}
  var cnt=el('div','cfg-sd'),list=el('div');
  function drawList(){
    list.textContent='';var q=norm(F.q),p=cur(),shown=0,tot=0;
    all.forEach(function(e){
      if(F.type!=='any'&&e.t!==F.type)return;
      if(F.rar!=='any'&&!e.r[F.rar])return;
      if(F.dun!=='any'&&!e.d[F.dun])return;
      if(q&&e.id.indexOf(q)<0)return;
      tot++;if(shown>=80)return;shown++;
      var r=el('div','cfg-f'),lb=el('label',0,e.n);r.appendChild(lb);
      var sw=el('div','sw'),i=document.createElement('input');i.type='checkbox';i.checked=!!p[e.id];
      i.onchange=function(){var o=Object.assign({},cur());if(i.checked)o[e.id]=true;else delete o[e.id];setS(S,k,o);cntDraw()};
      sw.appendChild(i);sw.appendChild(document.createElement('i'));r.appendChild(sw);list.appendChild(r);
    });
    if(q&&!tot){
      var ar=el('div','cfg-f');ar.appendChild(el('label',0,'Thêm "'+F.q.trim()+'" (không có trong danh sách)'));
      var ab=el('button','cfg-b','Thêm');ab.type='button';ab.onclick=function(){var o=Object.assign({},cur());o[q]=true;setS(S,k,o);all=catalog();si.value='';F.q='';drawList()};
      ar.appendChild(ab);list.appendChild(ar);
    }
    cntDraw(tot,shown);
  }
  function cntDraw(tot,shown){
    if(tot!==undefined){cnt._t=tot;cnt._s=shown}
    cnt.textContent=Object.keys(cur()).length+' item đang được bảo vệ - '+all.length+' item trong danh sách (dữ liệu wiki + kho các account)'+(cnt._t>cnt._s?' - đang hiện '+cnt._s+'/'+cnt._t+', dùng Search/bộ lọc để thu hẹp':'')+'.';
  }
  box.appendChild(head);box.appendChild(cnt);box.appendChild(list);drawDD();drawList();return box;
}
function friends(c,r){
  var S=STORE,k=c.k;r.style.display='block';
  var top=el('div');top.style.cssText='display:flex;gap:8px;align-items:center;justify-content:space-between';
  top.appendChild(el('label',0,c.l));
  var w=el('div');w.style.cssText='display:flex;gap:6px;align-items:center;position:relative';
  var un=document.createElement('input');un.type=hid()?'password':'text';un.placeholder='roblox username';
  var chosen={},pop=el('div','pd-pop');pop.style.display='none';PD.pops.push(pop);
  function cur(){return Array.isArray(S.vals[k])?S.vals[k]:[]}
  function has2(n){return cur().some(function(x){return String(x).toLowerCase()===n.toLowerCase()})}
  function freeList(){
    var q=un.value.trim().toLowerCase();
    return accounts().filter(function(x){return /^\w{3,20}$/.test(x.name)&&!has2(x.name)&&(!q||String(x.name).toLowerCase().indexOf(q)>=0)})
      .sort(function(x,y){return (y.online?1:0)-(x.online?1:0)||String(x.name).localeCompare(String(y.name))});
  }
  function drawPop(){
    pop.textContent='';var L=freeList();
    if(!L.length)pop.appendChild(el('div','cfg-sd','Không có account nào trong tracker (hoặc đã nằm trong whitelist).'));
    L.forEach(function(x){
      var o=el('div','pd-opt '+(x.online?'pd-on':'pd-off')),cb=document.createElement('input');cb.type='checkbox';cb.checked=!!chosen[x.name];
      cb.onchange=function(){if(cb.checked)chosen[x.name]=true;else delete chosen[x.name]};
      o.onclick=function(e){if(e.target!==cb){cb.checked=!cb.checked;cb.onchange()}};
      o.appendChild(cb);o.appendChild(el('span',0,(x.online?'● ':'○ ')+nm(x.name)+(x.online?'':'  (offline)')));pop.appendChild(o);
    });
  }
  function showPop(){drawPop();pop.style.display='block'}
  un.onfocus=showPop;un.onclick=showPop;un.oninput=showPop;
  var chips=el('div','cfg-chips');chips.style.marginTop='8px';
  function draw(){
    chips.textContent='';var L=cur();
    if(!L.length)chips.appendChild(el('span','cfg-lb','Chưa có friend nào'));
    L.forEach(function(n,ix){var ch=el('span','cfg-chip',hid()?'HIDE':n),x=el('button',0,'×');x.type='button';
      x.onclick=function(){var a=cur().slice();a.splice(ix,1);setS(S,k,a);draw();drawPop()};ch.appendChild(x);chips.appendChild(ch)});
  }
  var ok=el('button','pd-bt','✓');ok.type='button';ok.title='Thêm các account đã tích vào whitelist';
  ok.onclick=function(){
    var names=Object.keys(chosen),t=un.value.trim();
    if(t&&/^\w{3,20}$/.test(t)&&!names.some(function(n){return n.toLowerCase()===t.toLowerCase()}))names.push(t);
    names=names.filter(function(n){return !has2(n)});
    if(!names.length){say('Bấm vào ô username, tích chọn tên account rồi bấm ✓.','err');return}
    setS(S,k,cur().concat(names));chosen={};un.value='';pop.style.display='none';draw();
  };
  un.onkeydown=function(e){if(e.key==='Enter')ok.onclick()};
  w.appendChild(un);w.appendChild(ok);w.appendChild(pop);top.appendChild(w);r.appendChild(top);r.appendChild(chips);draw();
}
var THEME_COLORS=[['Crimson','#dc2626','#ef4444'],['Orange','#ea580c','#fb923c'],['Gold','#ca8a04','#facc15'],['Emerald','#16a34a','#4ade80'],['Azure','#2563eb','#60a5fa'],['Violet','#7c3aed','#a78bfa'],['Rose','#db2777','#f472b6']];
function themeRow(c,r){
  var S=STORE;r.style.display='block';
  var top=el('div','cfg-rt');top.appendChild(el('label',0,c.l));var val=el('span','cfg-rv cfg-tv');top.appendChild(val);r.appendChild(top);
  var bar=el('div','cfg-th'),knob=el('div','cfg-tk');r.appendChild(bar);
  THEME_COLORS.forEach(function(x){var s=el('div','cfg-ts');s.style.background='linear-gradient(180deg,'+x[2]+','+x[1]+')';s.title=x[0];bar.appendChild(s)});
  bar.appendChild(knob);
  function idxOf(){var b=S.vals.colorThemeBase;for(var i=0;i<THEME_COLORS.length;i++)if(THEME_COLORS[i][0]===b)return i;return -1}
  var timer=0,ai=0,dir=1;
  function place(ix){knob.style.left='calc(5px + (100% - 34px) / 7 * '+(ix+0.5)+' + '+(4*ix)+'px)'}
  function stop(){if(timer){clearInterval(timer);timer=0}}
  function draw(){
    var base=S.vals.colorThemeBase,chroma=S.vals.colorTheme==='Chroma',ix=idxOf();
    bar.classList.toggle('auto',chroma);
    if(chroma){
      knob.style.display='block';
      if(!timer){
        ai=ix<0?0:ix;dir=ai>=6?-1:1;place(ai);val.textContent='Chroma - '+THEME_COLORS[ai][0];
        timer=setInterval(function(){
          if(!r.isConnected){stop();return}
          if(ai+dir>6||ai+dir<0)dir=-dir;ai+=dir;place(ai);val.textContent='Chroma - '+THEME_COLORS[ai][0];
        },1100);
      }
      return;
    }
    stop();
    val.textContent=String(base);
    knob.style.display=ix<0?'none':'block';
    if(ix>=0)place(ix);
  }
  function pick(ix){
    if(S.vals.colorTheme==='Chroma')return;   // Chroma bat: nup tu chay, khong keo duoc
    if(ix<0||ix>6)return;var n=THEME_COLORS[ix][0];if(S.vals.colorThemeBase===n)return;
    S.vals.colorThemeBase=n;if(S.vals.colorTheme!=='Chroma')S.vals.colorTheme=n;refresh();syncChroma();
  }
  function at(x){var b=bar.getBoundingClientRect(),f=(x-b.left-5)/Math.max(1,b.width-10);pick(Math.min(6,Math.max(0,Math.floor(f*7))))}
  var down=false;
  bar.onpointerdown=function(e){e.preventDefault();down=true;bar.setPointerCapture(e.pointerId);at(e.clientX)};
  bar.onpointermove=function(e){if(down)at(e.clientX)};
  bar.onpointerup=bar.onpointercancel=function(){down=false};
  r._sync=draw;r.dataset.theme='1';draw();
}
function syncChroma(){root.querySelectorAll('.cfg-f[data-theme],.cfg-f[data-chroma]').forEach(function(x){if(x._sync)x._sync()})}
function xsel(c,r){
  var S=STORE,k=c.k;r.style.display='block';var open=false,wrap=el('div');r.appendChild(wrap);
  function lab(v){for(var i=0;i<c.o.length;i++)if(c.o[i][0]===v)return c.o[i][1];return String(v)}
  function draw(){
    wrap.textContent='';var h=el('div','cfg-rt');h.style.cursor='pointer';h.appendChild(el('label',0,c.l));h.firstChild.style.cursor='pointer';
    var v=el('span','cfg-rv',lab(S.vals[k])+'   '+(open?'⌃':'⌄'));h.appendChild(v);h.onclick=function(){open=!open;draw()};wrap.appendChild(h);
    if(open){var g=el('div','cfg-xl');c.o.forEach(function(o){var b=pdBt(o[1],S.vals[k]===o[0],function(){S.vals[k]=o[0];refresh();open=false;draw()});b.classList.add('pd-dg');g.appendChild(b)});wrap.appendChild(g)}
  }
  draw();
}
function idsBox(c,r){
  var S=STORE,k=c.k;r.style.display='block';
  r.appendChild(el('label',0,c.l));r.appendChild(el('div','cfg-rs','One ID per box. A new box opens as you fill the last.'));
  var host=el('div','cfg-ids');r.appendChild(host);
  function vals(){return [].map.call(host.children,function(x){return x.value}).filter(Boolean)}
  function commit(){S.vals[k]=vals().join(',');refresh()}
  function add(v){
    var i=document.createElement('input');i.type='text';i.placeholder='Discord user ID';i.value=v||'';i.inputMode='numeric';
    i.oninput=function(){var d=i.value.replace(/\D/g,'');if(d!==i.value)i.value=d;commit();if(d&&host.lastChild===i)add('')};
    i.onblur=function(){if(!i.value&&host.lastChild!==i&&host.children.length>1)host.removeChild(i)};
    host.appendChild(i);
  }
  String(S.vals[k]==null?'':S.vals[k]).split(/[\s,;]+/).filter(Boolean).forEach(add);add('');
}
function sld(c,r){
  var S=STORE,k=c.k,mx=c.max;
  var v0=S.vals[k];if(typeof v0==='number'&&v0>mx)mx=Math.ceil(v0/100)*100;
  r.style.display='block';if(c.grad)r.classList.add('g-'+c.grad);
  r.appendChild(el('label',0,c.l));if(c.sub)r.appendChild(el('div','cfg-rs',c.sub));
  var tr=el('div','cfg-trk cfg-one'),fill=el('div','cfg-fill'),val=el('span','cfg-ov');tr.appendChild(fill);tr.appendChild(val);r.appendChild(tr);
  function draw(){var v=typeof S.vals[k]==='number'?S.vals[k]:(typeof c.def==='number'?c.def:0);fill.style.width=(Math.min(mx,Math.max(0,v))/mx*100)+'%';val.textContent=String(v)}
  function at(x){var b=tr.getBoundingClientRect(),f=Math.min(1,Math.max(0,(x-b.left)/Math.max(1,b.width)));var n=Math.round(f*mx/c.step)*c.step;S.vals[k]=n;refresh();draw()}
  var d=false;tr.onpointerdown=function(e){e.preventDefault();d=true;tr.setPointerCapture(e.pointerId);at(e.clientX)};
  tr.onpointermove=function(e){if(d)at(e.clientX)};tr.onpointerup=tr.onpointercancel=function(){d=false};
  draw();
}
var BRK={prev:null};
function brkSync(){
  var on=!(ALT.vals.breakFarmMin>=24&&ALT.vals.breakFarmMax>=24);
  root.querySelectorAll('.cfg-f[data-brk],.cfg-f[data-brkon]').forEach(function(r){if(r._sync)r._sync();if(r.dataset.brk)r.classList.toggle('off',!on)});
}
function rng(c,r){
  var S=STORE,k=c.k,k2=c.k2;
  function cur(){var a=S.vals[k],b=S.vals[k2];if(typeof a!=='number')a=c.def?c.def[0]:c.min;if(typeof b!=='number')b=c.def?c.def[1]:c.max;return [a,b]}
  function pct(x){return (Math.min(c.max,Math.max(c.min,x))-c.min)/(c.max-c.min)*100}
  function fmt(x){return String(Math.round(x*10)/10)}
  r.style.display='block';if(c.brk)r.dataset.brk='1';if(c.grad)r.classList.add('g-'+c.grad);
  var top=el('div','cfg-rt');top.appendChild(el('label',0,c.l));var val=el('span','cfg-rv');top.appendChild(val);r.appendChild(top);
  if(c.sub)r.appendChild(el('div','cfg-rs',c.sub));
  var tr=el('div','cfg-trk'),fill=el('div','cfg-fill'),h1=el('div','cfg-h'),h2=el('div','cfg-h');
  tr.appendChild(fill);tr.appendChild(h1);tr.appendChild(h2);r.appendChild(tr);
  function draw(){var v=cur();fill.style.left=pct(v[0])+'%';fill.style.width=(pct(v[1])-pct(v[0]))+'%';h1.style.left=pct(v[0])+'%';h2.style.left=pct(v[1])+'%';val.textContent=fmt(v[0])+' - '+fmt(v[1])+' '+c.unit}
  function put(a,b){S.vals[k]=a;S.vals[k2]=b;refresh();draw();if(c.brk&&k==='breakFarmMin')brkSync()}
  function fromX(x){var b=tr.getBoundingClientRect(),f=(x-b.left)/Math.max(1,b.width);f=Math.min(1,Math.max(0,f));var v=c.min+f*(c.max-c.min);return Math.round(v/c.step)*c.step}
  function move(which,x){var v=cur(),n=fromX(x);if(which===1)put(Math.min(n,v[1]),v[1]);else put(v[0],Math.max(n,v[0]))}
  [h1,h2].forEach(function(h,ix){
    h.onpointerdown=function(e){e.preventDefault();e.stopPropagation();h.setPointerCapture(e.pointerId);h._d=1};
    h.onpointermove=function(e){if(h._d)move(ix+1,e.clientX)};
    h.onpointerup=h.onpointercancel=function(){h._d=0};
  });
  tr.onpointerdown=function(e){if(e.target!==tr&&e.target!==fill)return;var v=cur(),n=fromX(e.clientX);if(Math.abs(n-v[0])<=Math.abs(n-v[1]))put(Math.min(n,v[1]),v[1]);else put(v[0],Math.max(n,v[0]))};
  draw();r._sync=function(){draw()};
}
function row(c){
  var r=el('div','cfg-f');r.dataset.alt='1';if(c.k)r.dataset.k=c.k;if(c.k2)r.dataset.k2=c.k2;r.dataset.s=(c.l+' '+(c.k||'')+' '+(c.k2||'')).toLowerCase();
  if(c.t==='protect'){r.className='cfg-pr';r.appendChild(protect(c,r));return r}
  if(c.t==='note'){r.className='cfg-nt';r.textContent=c.l;return r}
  if(c.t==='range'){rng(c,r);return r}
  if(c.t==='slide'){sld(c,r);return r}
  if(c.t==='xsel'){xsel(c,r);return r}
  if(c.t==='theme'){themeRow(c,r);return r}
  if(c.t==='ids'){idsBox(c,r);return r}
  if(c.t==='friends'){friends(c,r);return r}
  var lb=el('label',0,c.l+((ALT.mixed.indexOf(c.k)>=0||(c.k2&&ALT.mixed.indexOf(c.k2)>=0))?'  ≠':''));lb.title=(c.k||'')+(c.k2?' / '+c.k2:'')+((ALT.mixed.indexOf(c.k)>=0)?' - các account đang khác nhau, đây là giá trị phổ biến nhất':'');r.appendChild(lb);r.appendChild(ctl(c,r));return r;
}
function renderPanels(){
  acOptions();
  var sc=[].map.call(g6.querySelectorAll('.cfg-bb'),function(x){return x.scrollTop});
  g6.textContent='';
  var miss=[];
  PANELS.forEach(function(p){
    var box=el('div','cfg-box');box.style.setProperty('--c',p.c);var h3=el('h3',0,p.t);if(p.badge){h3.appendChild(el('span','cfg-badge',p.badge))}box.appendChild(h3);if(p.sub)box.appendChild(el('div','cfg-sub',p.sub));
    var bb=el('div','cfg-bb'),shown=0;
    STORE=ALT;
    if(p.custom){partiesBody(bb);box.appendChild(bb);g6.appendChild(box);return}
    p.s.forEach(function(s){
      var rows=[];
      s.r.forEach(function(c){
        if(c.acc)return;
        if(!c.virtual&&(!has(c.k)||(c.k2&&!has(c.k2)))){miss.push(c.k);return}
        rows.push(row(c));
      });
      if(!rows.length)return;
      var host=bb;
      if(s.t){host=document.createElement('details');host.className='cfg-sc';host.open=false;host.appendChild(el('summary',0,s.t));if(s.d)host.appendChild(el('div','cfg-sd',s.d));bb.appendChild(host)}
      rows.forEach(function(r){host.appendChild(r)});shown+=rows.length;
    });
    if(!shown)bb.appendChild(el('div','cfg-msg','Không có key nào của ô này trong file.'));
    box.appendChild(bb);g6.appendChild(box);
  });
  STORE=ALT;brkSync();
  speedSync();
  [].forEach.call(g6.querySelectorAll('.cfg-bb'),function(x,i){if(sc[i])x.scrollTop=sc[i]});
  refresh();
}

function renderAll(){renderPanels()}


// ---------- Config chung: moi thay doi duoc ghi vao TAT CA file release_<userId>.json trong workspace ----------
function hid(){return typeof hideAccounts!=='undefined'&&hideAccounts===true}
function nm(n){if(!hid())return n;var L=accounts(),ix=-1;for(var i=0;i<L.length;i++)if(String(L[i].name).toLowerCase()===String(n).toLowerCase()){ix=i;break}return 'HIDE'}
function accounts(){var d=(typeof data!=='undefined'&&Array.isArray(data))?data:[];return d.filter(function(a){return a&&a.name}).sort(function(x,y){return String(x.name).localeCompare(String(y.name))})}
async function altLoad(){
  try{
    var j=await api('/api/config/alt'+(SELUID?('?only='+encodeURIComponent(SELUID)):''));
    ALT.vals=JSON.parse(JSON.stringify(j.values));ALT.orig=JSON.parse(JSON.stringify(j.values));ALT.mixed=j.mixed||[];ALT.secrets=j.secrets||[];ALT.files=j.files.length;ALT.err='';
  }catch(e){ALT.err=e.message;ALT.vals={};ALT.orig={};ALT.files=0;say('Không đọc được config: '+e.message,'err')}
  renderPanels();
}
async function altSave(){
  var vals={};Object.keys(ALT.vals).forEach(function(k){vals[k]=ALT.vals[k]});
  ['webhookUrl','statsKey'].forEach(function(k){if(vals[k]===ALT.orig[k])delete vals[k]});   // khong doi thi khong gui lai (tranh bi chan boi kiem tra dinh dang)
  PANELS.forEach(function(p){(p.s||[]).forEach(function(s){s.r.forEach(function(c){   // dong dang hien san (mac dinh, key chua co trong file) cung duoc ghi: Save = dung nhung gi dang hien tren man hinh
    if(!c.virtual||c.def===undefined||!c.k||c.acc)return;
    if(c.t==='range'&&c.k2&&Array.isArray(c.def)){if(!(c.k in vals))vals[c.k]=c.def[0];if(!(c.k2 in vals))vals[c.k2]=c.def[1];return}
    if(!(c.k in vals))vals[c.k]=c.def;
  })})});
  var ks=Object.keys(vals);if(!ks.length)return null;   // Save = ghi TOAN BO gia tri dang hien trong 6 o, khong chi nhung muc vua doi
  var classes={};accounts().forEach(function(a){if(a.uid&&a.class){var c=/warrior/i.test(a.class)?'warrior':(/mage/i.test(a.class)?'mage':'');if(c)classes[a.uid]=c}});   // Class van tu dong theo tracker cho tung account
  ALT.msg='';
  try{
    var j=await api('/api/config/alt',{method:'POST',body:JSON.stringify({values:vals,exclude:[],only:SELUID?[SELUID]:[],classes:classes})});
    ALT.msg='ok';await altLoad();ALT.msg='';
    return SELUID?{ok:true,msg:'Đã lưu config ('+ks.length+' mục) riêng cho account này.'}:{ok:true,msg:'Đã lưu toàn bộ config ('+ks.length+' mục) cho '+j.files+' account ('+j.changed+' file thay đổi).'};
  }catch(e){refresh();return {ok:false,msg:'Lưu lỗi: '+e.message}}
}

// ---------- Parties & Runs (du lieu chung: data\altfarm.json, bossraid_extension.json, alt_gamepasses.json) ----------
var PDN={alt:'altfarm.json',boss:'bossraid_extension.json',pass:'alt_gamepasses.json',auto:'tracker_autobuy.json'},PDK={alt:'altfarm',boss:'bossraid',pass:'altpass',auto:'autobuy'};
var PD={alt:null,boss:null,pass:null,auto:null,mt:{},orig:{},cur:0,err:'',open:{},msg:'',bad:false},pdBtn=null;
PD.pops=[];
function closePops(e){PD.pops=PD.pops.filter(function(p){return p.isConnected});PD.pops.forEach(function(p){if(p.style.display!=='none'&&!(e&&e.target&&p.parentNode&&p.parentNode.contains(e.target))){p.style.display='none';if(typeof p._onHide==='function')p._onHide()}})}
document.addEventListener('mousedown',closePops,true);document.addEventListener('touchstart',closePops,true);
document.addEventListener('focusin',closePops,true);
document.addEventListener('keydown',function(e){if(e.key==='Escape')closePops(null)});
function pdDirty(n){return !!PD[n]&&JSON.stringify(PD[n])!==PD.orig[n]}
function pdAny(){return pdDirty('alt')||pdDirty('boss')||pdDirty('pass')||pdDirty('auto')}
function pdTouch(){refresh()}
async function pdLoad(){
  try{
    var j=await api('/api/config/pdata');
    ['alt','boss','pass','auto'].forEach(function(n){var f=j.files[PDN[n]];if(f){PD[n]=f.value;PD.mt[n]=f.mtime;PD.orig[n]=JSON.stringify(f.value)}else{PD[n]=null}});
    PD.err='';
  }catch(e){PD.err=e.message}
  renderPanels();
}
async function pdSave(){
  var ns=['alt','boss','pass','auto'].filter(pdDirty);if(!ns.length)return null;   // party / gamepass: chi ghi file nao that su doi (script game cung ghi cac file nay)
  var res;
  try{
    for(var i=0;i<ns.length;i++){var n=ns[i];await api('/api/config/pdata',{method:'POST',body:JSON.stringify({name:PDK[n],mtime:PD.mt[n],value:PD[n]})})}
    res={ok:true,msg:'Đã lưu '+ns.map(function(n){return PDN[n]}).join(', ')+'.'};
  }catch(e){res={ok:false,msg:'Lưu lỗi: '+e.message}}
  await pdLoad();return res;
}
async function saveAll(){
  bSave.disabled=true;var out=[];
  var a=await altSave();if(a)out.push(a);
  var p=await pdSave();if(p)out.push(p);
  var bad=out.some(function(x){return !x.ok});
  say(bad?out.filter(function(x){return !x.ok}).map(function(x){return x.msg}).join(' '):'',bad?'err':'');   // luu xong thi khong hien thong bao, chi hien khi loi
  refresh();
}
function pdSw(obj,key,after,def){
  var sw=el('div','sw'),i=document.createElement('input');i.type='checkbox';i.checked=(key in obj)?!!obj[key]:!!def;
  i.onchange=function(){obj[key]=i.checked;pdTouch();if(after)after()};sw.appendChild(i);sw.appendChild(document.createElement('i'));return sw;
}
function pdNum(obj,key,cls){
  var i=document.createElement('input');i.type='number';i.step='any';i.value=obj[key];if(cls)i.className=cls;
  i.oninput=function(){var n=parseFloat(i.value);i.classList.toggle('bad',!isFinite(n));if(isFinite(n)){obj[key]=n;pdTouch()}};return i;
}
function pdRow(label,obj,key,type,o){
  o=o||{};if(!obj||(!(key in obj)&&o.def===undefined)||(o.k2&&!(o.k2 in obj)))return null;   // o.def: dong van hien khi file chua co key (hien gia tri mac dinh, chi ghi khi bam)
  var r=el('div','cfg-f');r.appendChild(el('label',0,label));
  if(type==='bool')r.appendChild(pdSw(obj,key,o.after,o.def));
  else if(type==='num')r.appendChild(pdNum(obj,key));
  else if(type==='pair'){var w=el('div');w.style.cssText='display:flex;gap:4px;align-items:center';w.appendChild(pdNum(obj,key,'n2'));w.appendChild(el('span','cfg-lb','–'));w.appendChild(pdNum(obj,o.k2,'n2'));r.appendChild(w)}
  else{
    var i=document.createElement('input');i.type='text';i.value=obj[key];
    if(o.dl){var id='pddl_'+key,dl=document.createElement('datalist');dl.id=id;o.dl.forEach(function(x){var op=document.createElement('option');op.value=x;dl.appendChild(op)});r.appendChild(dl);i.setAttribute('list',id)}
    i.oninput=function(){obj[key]=i.value;pdTouch()};r.appendChild(i);
  }
  return r;
}
function pdSld(label,obj,key,c){   // thanh keo kieu game cho file party (giong sld cua 6 o)
  if(!obj||!(key in obj)&&c.def===undefined)return null;
  var r=el('div','cfg-f g-'+c.grad);r.style.display='block';r.appendChild(el('label',0,label));if(c.sub)r.appendChild(el('div','cfg-rs',c.sub));
  var tr=el('div','cfg-trk cfg-one'),fill=el('div','cfg-fill'),val=el('span','cfg-ov');tr.appendChild(fill);tr.appendChild(val);r.appendChild(tr);
  function cur(){return typeof obj[key]==='number'?obj[key]:(typeof c.def==='number'?c.def:0)}
  var mx=Math.max(c.max,Math.ceil(cur()/60)*60);
  function draw(){var v=cur();fill.style.width=(Math.min(mx,Math.max(0,v))/mx*100)+'%';val.textContent=String(v)}
  function at(x){var b=tr.getBoundingClientRect(),f=Math.min(1,Math.max(0,(x-b.left)/Math.max(1,b.width)));obj[key]=Math.max(c.min,Math.round(f*mx/c.step)*c.step);pdTouch();draw()}
  var d=false;tr.onpointerdown=function(e){e.preventDefault();d=true;tr.setPointerCapture(e.pointerId);at(e.clientX)};
  tr.onpointermove=function(e){if(d)at(e.clientX)};tr.onpointerup=tr.onpointercancel=function(){d=false};
  draw();return r;
}
function pdAutoRow(label,obj,key){   // cong tac bi khoa, chay theo 'Auto buy with gold' (khong co key rieng trong file)
  if(!obj||!(key in obj))return null;
  var r=el('div','cfg-f');r.appendChild(el('label',0,label));
  var sw=el('div','sw'),i=document.createElement('input');i.type='checkbox';i.checked=!!obj[key];i.disabled=true;sw.appendChild(i);sw.appendChild(document.createElement('i'));r.appendChild(sw);return r;
}
// ---------- doc/ghi 1 field CHUNG cho TAT CA party cung luc (doc hien thi tu party dau tien, ghi thi dong bo xuong het) ----------
// 'key in obj' van phan anh dung party dau tien THAT SU co key do hay chua (dung Proxy 'has' trap), de pdRow/pdSw
// van hien dung gia tri mac dinh (o.def) khi party dau tien chua tung luu field nay, khong bi hien sai thanh tat.
function pdAllProxy(P,keys){
  var keySet={};keys.forEach(function(k){keySet[k]=true});
  return new Proxy({},{
    has:function(t,k){return !!keySet[k]&&P.length>0&&(k in P[0])},
    get:function(t,k){return P.length?P[0][k]:undefined},
    set:function(t,k,v){P.forEach(function(p){p[k]=v});return true}
  });
}
function partiesGlobalDungeon(bb,P){
  if(!P.length)return;
  var dun=pdAllProxy(P,['auto','hardest','start','hardcore']);
  pdSec(bb,'dungeon_all','Dungeon','Which dungeon every party queues - applies to all parties at once.',[
    pdRow('Pick the dungeon for me',dun,'auto','bool'),
    pdDun('Hardest dungeon',dun,'hardest'),pdDun('Starting dungeon',dun,'start'),pdHard(dun)]);
}
function partiesGlobalOptions(bb,P){
  if(!P.length)return;
  var opt=pdAllProxy(P,['replay','waitMax','newSave100','leechRender','swapCarries']);
  pdSec(bb,'options_all','Options','Extras - the defaults are fine. Applies to all parties at once.',[
    pdRow('Replay with the party',opt,'replay','bool'),
    pdSld('Wait for accounts',opt,'waitMax',{min:0,max:360,step:5,sub:'s',grad:'wait'}),
    pdRow('New save at level 100',opt,'newSave100','bool',{def:false}),
    pdRow('Leechers draw the world',opt,'leechRender','bool',{def:false}),
    pdRow('Swap carries',opt,'swapCarries','bool',{def:true})]);
}
function pdSec(bb,id,title,desc,rows){
  rows=rows.filter(Boolean);if(!rows.length)return;
  var d=document.createElement('details');d.className='cfg-sc';d.open=!!PD.open[id];
  d.addEventListener('toggle',function(){PD.open[id]=d.open});
  d.appendChild(el('summary',0,title));if(desc)d.appendChild(el('div','cfg-sd',desc));
  rows.forEach(function(r){d.appendChild(r)});bb.appendChild(d);
}
function pdOnline(name){
  var a=accounts().filter(function(x){return String(x.name).toLowerCase()===name.toLowerCase()})[0];
  return a?(a.online?'online':'offline'):'chưa thấy trong tracker';
}
function pdBt(txt,on,fn,title){var b=el('button','pd-bt'+(on?' on':''),txt);b.type='button';if(title)b.title=title;b.onclick=fn;return b}
var DUNGEONS=['Desert Temple','Winter Outpost','Pirate Island',"King's Castle",'The Underworld','Samurai Palace','The Canals','Ghastly Harbor','Steampunk Sewers','Boss Raid','Orbital Outpost','Volcanic Chambers','Aquatic Temple','Enchanted Forest','Northern Lands','Gilded Skies'];
PD.dd={};
function pdDun(label,p,key){
  if(!(key in p))return null;
  var wrap=el('div'),r=el('div','cfg-f');r.style.cursor='pointer';r.appendChild(el('label',0,label));r.firstChild.style.cursor='pointer';
  r.appendChild(el('div',0,p[key]+'   '+(PD.dd[key]?'⌃':'⌄')));
  r.onclick=function(){PD.dd[key]=!PD.dd[key];renderPanels()};wrap.appendChild(r);
  if(PD.dd[key]){
    var g=el('div','pd-dgrid');
    DUNGEONS.forEach(function(d){var b=pdBt(d,d===p[key],function(){p[key]=d;PD.dd[key]=false;pdTouch();renderPanels()});b.classList.add('pd-dg');g.appendChild(b)});
    wrap.appendChild(g);
  }
  return wrap;
}
function pdHard(p){var t={hardcore:!!p.hardcore};return pdRow('Hardcore',t,'hardcore','bool',{after:function(){p.hardcore=t.hardcore;pdTouch()}})}
// ---------- tinh trang THAT cua 1 party, doc tu du lieu tracker (accounts()): online/offline, dang o dungeon nao ----------
function ptyAcc(name){return accounts().filter(function(x){return x.name.toLowerCase()===String(name).toLowerCase()})[0]||null}
function ptyStatus(p){
  var L=p.accounts||[],accs=L.map(function(pa){return ptyAcc(pa.name)}),online=accs.filter(function(a){return a&&a.online}).length;
  var leader=L.filter(function(a){return a.role==='leader'})[0]||null,leaderAcc=leader?ptyAcc(leader.name):null,liveDun=null;
  if(leaderAcc&&leaderAcc.online&&leaderAcc.dungeonName)liveDun=leaderAcc.dungeonName;
  else{var any=accs.filter(function(a){return a&&a.online&&a.dungeonName})[0];if(any)liveDun=any.dungeonName}
  return {total:L.length,online:online,leader:leader,leaderAcc:leaderAcc,liveDun:liveDun};
}
function ptyDunText(p,st){return st.liveDun?(st.liveDun+(p.difficulty?(' - '+p.difficulty):'')):('Up to '+(p.hardest||'?'))}
function ptySubText(st){
  if(!st.total)return 'No accounts yet';
  if(!st.leader)return st.online+' of '+st.total+' online';
  var phase=!st.leaderAcc?'not running the script':!st.leaderAcc.online?'offline - not running the script':(st.liveDun?'in a run':'in the lobby');
  return nm(st.leader.name)+' leads - '+phase+' | '+st.online+' of '+st.total+' online';
}
function ptyAvatars(p){
  var wrap=el('div','pty-av'),L=p.accounts||[];
  L.slice(0,6).forEach(function(pa){
    var a=ptyAcc(pa.name),av=el('div','pty-avi'+(a&&a.online?' on':''),String(nm(pa.name)).slice(0,1).toUpperCase());
    av.title=nm(pa.name)+(a?(a.online?' - online':' - offline'):' - chưa thấy trong tracker');wrap.appendChild(av);
  });
  if(L.length>6)wrap.appendChild(el('div','pty-avi','+'+(L.length-6)));
  return wrap;
}
// ---------- sua 1 party: them/xoa account, Carry/Leech, Find accounts, Delete party ----------
function partyAccountsRows(P,p){
  var rows=[],inAny=function(n){return P.some(function(q){return q.accounts.some(function(a){return a.name.toLowerCase()===n.toLowerCase()})})};
  var idxLbl='Party '+(P.indexOf(p)+1);
  var ar=el('div','cfg-f');ar.style.flexWrap='wrap';
  var w=el('div');w.style.cssText='display:flex;gap:6px;align-items:center;flex:1;min-width:220px;position:relative';   // position:relative lai o DAY (chi o nhap) de popup chi rong bang thanh add, khong lan sang 2 nut ben canh
  var un=document.createElement('input');un.type=hid()?'password':'text';un.placeholder='+ Add an account...';un.style.flex='1';
  var pop=el('div','pd-pop');pop.style.display='none';PD.pops.push(pop);
  var chosen={};   // ten da tich nhung CHUA add vao party - cho den khi dong popup (bam ra ngoai) moi add 1 the, thay vi add ngay tung cai khi tich
  function freeList(){
    var q=un.value.trim().toLowerCase();
    return accounts().filter(function(x){return /^\w{3,20}$/.test(x.name)&&!inAny(x.name)&&(!q||String(x.name).toLowerCase().indexOf(q)>=0)})
      .sort(function(x,y){return (y.online?1:0)-(x.online?1:0)||String(x.name).localeCompare(String(y.name))});
  }
  function addOne(name){   // dung cho go ten moi + Enter (them ngay, khong qua chosen)
    if(!name||inAny(name))return false;
    p.accounts.push({enabled:true,role:p.accounts.some(function(a){return a.role==='leader'})?'leech':'leader',name:name});
    pdTouch();renderPanels();say('Đã thêm '+nm(name)+' vào '+idxLbl+'.','ok');return true;
  }
  function commitChosen(){   // goi khi popup dong (bam ra ngoai/Esc): add 1 the tat ca account da tich
    var names=Object.keys(chosen);chosen={};if(!names.length)return;
    var added=0;names.forEach(function(n){if(!inAny(n)){p.accounts.push({enabled:true,role:p.accounts.some(function(a){return a.role==='leader'})?'leech':'leader',name:n});added++}});
    if(added){pdTouch();renderPanels();say('Đã thêm '+added+' account vào '+idxLbl+'.','ok')}
  }
  pop._onHide=commitChosen;
  function drawPop(){
    pop.textContent='';var L=freeList();
    if(!L.length)pop.appendChild(el('div','cfg-sd','Không có account nào trong tracker (hoặc đã nằm trong party).'));
    L.forEach(function(x){
      var r=el('div','pd-opt '+(x.online?'pd-on':'pd-off')),cb=document.createElement('input');cb.type='checkbox';cb.checked=!!chosen[x.name];
      cb.onchange=function(){if(cb.checked)chosen[x.name]=true;else delete chosen[x.name]};
      r.onclick=function(e){if(e.target!==cb){cb.checked=!cb.checked;cb.onchange()}};
      r.appendChild(cb);r.appendChild(el('span',0,(x.online?'● ':'○ ')+nm(x.name)+(x.online?'':'  (offline)')));pop.appendChild(r);
    });
  }
  function showPop(){drawPop();pop.style.display='block'}
  un.onfocus=showPop;un.onclick=showPop;un.oninput=showPop;
  un.onkeydown=function(e){   // go ten account chua co trong tracker roi Enter -> them thang, khong can nut rieng
    if(e.key!=='Enter')return;
    var t=un.value.trim();
    if(!t||!/^\w{3,20}$/.test(t)){say('Nhập tên account (3-20 ký tự chữ/số).','err');return}
    if(inAny(t)){say('Account này đã có trong 1 party rồi.','err');return}
    if(addOne(t))un.value='';
  };
  w.appendChild(un);w.appendChild(pop);ar.appendChild(w);
  var ac=el('div');ac.style.cssText='display:flex;gap:6px;flex-wrap:wrap;flex:none';
  ac.appendChild(pdBt('Find accounts',false,function(){
    var add=0;accounts().forEach(function(a){
      if(!a.online||!/^\w{3,20}$/.test(a.name)||inAny(a.name))return;
      p.accounts.push({enabled:true,role:p.accounts.some(function(x){return x.role==='leader'})?'leech':'leader',name:a.name});add++;
    });
    if(add){pdTouch();renderPanels();say('Đã thêm '+add+' account đang online vào '+idxLbl+'.','ok')}else say('Không có account online nào chưa nằm trong party.','err');
  },'Thêm các account đang online (tracker thấy) chưa có party'));
  ac.appendChild(pdBt('Delete party',false,function(){
    var ix=P.indexOf(p);if(ix<0)return;P.splice(ix,1);delete PD.open['pty_'+p.id];delete PD.open['dungeon_'+p.id];delete PD.open['options_'+p.id];pdTouch();renderPanels();
  }));
  ar.appendChild(ac);rows.push(ar);
  p.accounts.forEach(function(a,idx){
    var r=el('div','cfg-f'),L=el('div');L.appendChild(el('div',0,nm(a.name)));
    var sub=el('div','cfg-sd',pdOnline(a.name)+(a.role==='leader'?' - leader':''));sub.style.padding='0';L.appendChild(sub);r.appendChild(L);
    var bt=el('div');bt.style.cssText='display:flex;gap:6px;align-items:center';
    bt.appendChild(pdBt('♛',a.role==='leader',function(){
      if(a.role==='leader')return;
      p.accounts.forEach(function(x){if(x.role==='leader')x.role='fight'});a.role='leader';pdTouch();renderPanels();
    },'Leader (vào dungeon trước, bấm queue)'));
    bt.appendChild(pdBt('Carry',a.role==='leader'||a.role==='fight',function(){if(a.role==='leech'){a.role='fight';pdTouch();renderPanels()}},'Carry: tự chơi dungeon bằng skill của nó'));
    bt.appendChild(pdBt('Leech',a.role==='leech',function(){if(a.role==='leader'){say('Leader không thể là Leech - chọn leader khác trước.','err');return}a.role='leech';pdTouch();renderPanels()},'Leech: alt đứng chờ ăn XP'));
    bt.appendChild(pdBt('×',false,function(){p.accounts.splice(idx,1);pdTouch();renderPanels()},'Bỏ khỏi party'));
    r.appendChild(bt);rows.push(r);
  });
  return rows;
}
// ---------- 1 dong party trong list (anh thu gon) + bam mui ten de xo xuong phan sua ----------
function ptyCard(P,p){
  var id='pty_'+p.id,st=ptyStatus(p);
  var card=document.createElement('details');card.className='pty';card.open=!!PD.open[id];card.dataset.ptyId=p.id;
  card.addEventListener('toggle',function(){PD.open[id]=card.open});
  var sum=document.createElement('summary');
  sum.appendChild(el('span','pty-chev','▸'));
  var main=el('div','pty-main');
  var nmBox=el('div','pty-nm');
  nmBox.appendChild(el('b',0,'Party '+(P.indexOf(p)+1)));
  var dotLine=el('div','pty-stat');
  dotLine.appendChild(el('span','pty-dot '+(p.on?'on':'off')));
  dotLine.appendChild(document.createTextNode(p.on?'On':'Off'));
  nmBox.appendChild(dotLine);
  main.appendChild(nmBox);
  if(p.accounts.length)main.appendChild(ptyAvatars(p));
  var dunBox=el('div','pty-dun');
  dunBox.appendChild(el('b',0,ptyDunText(p,st)));
  dunBox.appendChild(el('div','pty-sub',ptySubText(st)));
  main.appendChild(dunBox);
  sum.appendChild(main);
  var tglWrap=el('div','pty-tgl pty-sw'),tgi=document.createElement('input');tgi.type='checkbox';tgi.checked=!!p.on;
  tgi.onchange=function(){p.on=tgi.checked;pdTouch();renderPanels()};
  tglWrap.appendChild(tgi);tglWrap.appendChild(document.createElement('i'));
  tglWrap.onclick=function(e){e.stopPropagation()};tglWrap.onpointerdown=function(e){e.stopPropagation()};
  sum.appendChild(tglWrap);
  card.appendChild(sum);
  var body=el('div','pty-body');
  var accHead=el('div','pty-subhead');
  accHead.appendChild(el('b',0,'Accounts'));
  accHead.appendChild(el('div',0,'The crown is the party leader and every account on the list joins it. Carries fight, leeches wait at spawn for XP.'));
  body.appendChild(accHead);
  partyAccountsRows(P,p).forEach(function(r){body.appendChild(r)});
  // Dungeon/Options: chuyen ra ngoai, dung CHUNG cho TAT CA party (xem partiesGlobalDungeon/partiesGlobalOptions
  // trong partiesBody, nam rieng 1 khu vuc ngay tren Alt gamepasses) - khong con rieng tung party nua.
  card.appendChild(body);
  return card;
}
// ---------- toan bo khung Parties: thanh "Run every party" + +New party/Force lobby + list cac party ----------
function ptyHeader(P){
  var wrap=el('div','pty-wrap'),tot=0,online=0,running=0;
  P.forEach(function(p){tot+=p.accounts.length;if(p.on)running++;p.accounts.forEach(function(pa){var a=ptyAcc(pa.name);if(a&&a.online)online++})});
  var head=el('div','pty-head');
  var allOn=P.length>0&&P.every(function(p){return p.on});
  var sw=el('div','pty-sw'),i=document.createElement('input');i.type='checkbox';i.checked=allOn;
  i.onchange=function(){P.forEach(function(p){p.on=i.checked});pdTouch();renderPanels()};
  sw.appendChild(i);sw.appendChild(document.createElement('i'));head.appendChild(sw);
  var txt=el('div','pty-htxt');
  txt.appendChild(el('b',0,'Run every party'));
  txt.appendChild(el('div',0,running?(running+' of '+P.length+' parties are running.'):'No party is running, nothing will queue.'));
  head.appendChild(txt);
  var badges=el('div','pty-badges');
  badges.appendChild(el('div','pty-badge',running+'/'+P.length+' parties running'));
  badges.appendChild(el('div','pty-badge pty-online-badge',online+'/'+tot+' online'));
  head.appendChild(badges);
  wrap.appendChild(head);
  var act=el('div','pty-act');
  var left=el('div');
  left.appendChild(pdBt('+ New party',false,function(){
    var base=P[0]||{},np={map:base.map||'Northern Lands',auto:true,replay:true,on:false,v:2,difficulty:base.difficulty||'Nightmare',hardest:base.hardest||'Northern Lands',waitMax:120,start:base.start||'Desert Temple',accounts:[],id:(self.crypto&&crypto.randomUUID?crypto.randomUUID():String(Date.now())).toUpperCase()};
    if(base.cap)np.cap=base.cap;P.push(np);PD.open['pty_'+np.id]=true;pdTouch();renderPanels();
  }));
  act.appendChild(left);
  var right=el('div');
  right.appendChild(pdBt('⎋ Force lobby',false,function(btn){
    var names=[];P.forEach(function(p){p.accounts.forEach(function(a){if(names.indexOf(a.name)<0)names.push(a.name)})});
    if(!names.length){say('Chưa có account nào trong các party.','err');return}
    api('/api/parties/lobby',{method:'POST',body:JSON.stringify({names:names})}).then(function(r){
      say('Đã gửi lệnh về Lobby cho '+((r&&r.n)||names.length)+' account - script sẽ về Lobby trong vài giây.','ok');
    }).catch(function(e){say('Lỗi gửi lệnh Force lobby: '+e.message,'err')});
  },'Bắt TẤT CẢ account trong các party thoát dungeon, về Lobby ngay lập tức (cần script tracker mới nhất)'));
  act.appendChild(right);
  wrap.appendChild(act);
  var list=el('div','pty-list');
  P.forEach(function(p){list.appendChild(ptyCard(P,p))});
  wrap.appendChild(list);
  return wrap;
}
// ---------- lam moi trang thai live (online/dungeon) MA KHONG dong lai toan bo DOM - tranh xoa mat o dang go/popup dang mo khi tu lam moi dinh ky ----------
function ptyPatchLive(P){
  var list=root.querySelector('.pty-list');if(!list)return;
  var tot=0,online=0;
  P.forEach(function(p){
    tot+=p.accounts.length;
    p.accounts.forEach(function(pa){var a=ptyAcc(pa.name);if(a&&a.online)online++});
    var card=list.querySelector('[data-pty-id="'+p.id+'"]');if(!card)return;
    var st=ptyStatus(p);
    var avKids=card.querySelectorAll('.pty-av .pty-avi');
    p.accounts.slice(0,6).forEach(function(pa,ix){var a=ptyAcc(pa.name),node=avKids[ix];if(node)node.classList.toggle('on',!!(a&&a.online))});
    var b=card.querySelector('.pty-dun b');if(b)b.textContent=ptyDunText(p,st);
    var s=card.querySelector('.pty-sub');if(s)s.textContent=ptySubText(st);
  });
  var ob=root.querySelector('.pty-online-badge');if(ob)ob.textContent=online+'/'+tot+' online';
}
function partiesBody(bb){
  if(PD.err){var bar=el('div','cfg-f');bar.style.marginTop='8px';var lb=el('label',0,'Không đọc được: '+PD.err);lb.style.setProperty('color','#ff6b8a','important');bar.appendChild(lb);bb.appendChild(bar)}
  refresh();
  if(PD.err||!PD.alt&&!PD.boss&&!PD.pass)return;
  var P=PD.alt&&PD.alt.parties||[],G=PD.pass;
  if(PD.alt)bb.appendChild(ptyHeader(P));
  if(PD.alt){partiesGlobalDungeon(bb,P);partiesGlobalOptions(bb,P)}
  if(G)pdSec(bb,'gp','Alt gamepasses',"Buy selected passes with each alt's gold as it levels up. Uses the game's own gold shop. Buy x2 Gold (auto) buys the x2 Gold pass the same way as the Buy tab once the account has enough gold (after Keep this much gold).",[
    pdRow('Auto buy with gold',G,'enabled','bool',{after:renderPanels}),pdRow('Buy VIP',G,'buyVIP','bool'),pdRow('Buy Extra Item',G,'buyExtra','bool'),pdRow('Buy x2 Gold (auto)',PD.auto,'gold2','bool'),pdRow('VIP first',G,'vipFirst','bool'),
    pdRow('VIP minimum level',G,'vipLevel','num'),pdRow('Extra Item minimum level',G,'extraLevel','num'),pdRow('Keep this much gold',G,'goldReserve','num'),
    pdRow('Buy x2 EXP with candy',G,'buyXpCandy','bool'),pdRow('Keep this much candy',G,'candyReserve','num')]);
}

// ---------- nut ----------
function guard(){return true}
acSel.onchange=function(){
  if(altChanged().length||pdAny()){say('Bạn đang có thay đổi chưa lưu - Save hoặc tải lại trước khi đổi account.','err');acSel.value=SELUID;return}
  SELUID=acSel.value;say('');altLoad();
};
bSave.onclick=saveAll;
bDel.onclick=async function(){
  bDel.disabled=true;
  try{
    var j=await api('/api/config/delete',{method:'POST',body:JSON.stringify({confirm:'DELETE_ALL_ALT_CONFIG'})});
    var txt=j.cleared?('Đã xoá dữ liệu config của '+j.cleared+' account (file vẫn còn; bản gốc lưu tại '+j.dir+').'+(j.failed&&j.failed.length?' Lỗi ở: '+j.failed.join(', '):'')):'Không có file config alt nào để xoá.';
    await altLoad();say(txt,j.ok?'ok':'err');
  }catch(e){say('Xoá lỗi: '+e.message,'err')}
  bDel.disabled=false;
};

// ---------- link workspace ----------
async function wsLoad(){
  try{var j=await api('/api/config/workspace');wsIn.value=j.input||j.dir||'';return j}catch(e){say(e.message,'err');return null}
}
wsBtn.onclick=async function(){
  var p=wsIn.value.trim();if(!p){say('Dán link workspace trước.','err');return}
  if(!guard())return;
  wsBtn.disabled=true;
  try{var j=await api('/api/config/workspace',{method:'POST',body:JSON.stringify({path:p})});wsIn.value=j.input||p;lsSet('dq_cfg_ws',wsIn.value);if(j.savedTo)say('');else say('KHÔNG ghi được file nhớ link ('+j.saveError+') - link chỉ được nhớ trong trình duyệt này.','err');ALT.msg='';await altLoad();pdLoad()}
  catch(e){say('Link workspace lỗi: '+e.message,'err')}
  wsBtn.disabled=false;
};
wsIn.onkeydown=function(e){if(e.key==='Enter')wsBtn.onclick()};
var started=false,tries=0;
async function start(){
  if(started)return;started=true;
  var w=await wsLoad();
  if(w===null){started=false;if(++tries>=25)started=true;return}   // loi mang / chua co key dang nhap -> tu thu lai o lan quet sau
  if(!(w&&w.ok)){   // server chua nho link (file bi xoa / doi may) -> nop lai link da luu trong trinh duyet
    var saved=lsGet('dq_cfg_ws','');
    if(saved){try{w=await api('/api/config/workspace',{method:'POST',body:JSON.stringify({path:saved})});wsIn.value=saved}catch(e){w=null}}
  }
  if(w&&w.ok)lsSet('dq_cfg_ws',wsIn.value);
  if(w&&w.ok){altLoad();pdLoad()}else say('Chưa nối workspace. Dán link thư mục workspace (hoặc thư mục configs) rồi bấm "Lưu link".','err');
}
document.querySelector('.tab[data-tab="config"]').addEventListener('click',start);
document.addEventListener('click',function(e){if(e.target.closest&&e.target.closest('#ha'))setTimeout(function(){if(started)renderPanels()},0)});
// Tai lai trang khi dang o tab Config: tab duoc mo lai ma KHONG co su kien click -> tu nap khi pane hien ra (thu lai neu key chua san sang)
setInterval(function(){if(!started&&pane.style.display!=='none'&&pane.offsetParent!==null)start()},600);
// Parties: tinh trang (online/offline, dang o dungeon nao) doc truc tiep tu bien data chung cua ca trang (tu cap nhat ~500ms/lan hoac qua SSE).
// Chi va cham vao vai chu/class dong (khong goi renderPanels/xay lai DOM) de khong lam mat chu dang go hay dong popup dang mo trong luc nguoi dung sua party.
setInterval(function(){if(started&&PD.alt&&pane.style.display!=='none'&&pane.offsetParent!==null)ptyPatchLive(PD.alt.parties||[])},3000);
}catch(e){console.error('config tab',e)}
})();
`;

// ===== TU TAI LAI SAU MOI LENH: sau khi web gui lenh (trade / ban / mua / xoa / gan co...) hoac trang thai lenh doi -> tai lai du lieu ngay + vai lan lien tiep =====
const RJ_UI_JS = String.raw`
(function(){
try{
var tabs=$('tabs'),tf=$('tf');
if(!tabs||!tf||document.querySelector('.tab[data-tab="rejoin"]'))return;
var css=document.createElement('style');
css.textContent='.tabs .tab[data-tab="rejoin"]::before{content:"\\21BB\\FE0E";font-family:"Segoe UI Symbol","Arial Unicode MS",sans-serif;font-size:19px;color:var(--mu)}.tabs .tab[data-tab="rejoin"].on::before,.tabs .tab[data-tab="rejoin"].act::before{font-weight:700;color:#5eead4;filter:none!important;text-shadow:0 0 6px #5eead4,0 0 14px #4ade8099}.tabs .tab[data-tab="rejoin"]{order:12}.rj-ic{font-family:"Segoe UI Symbol","Arial Unicode MS",sans-serif;font-weight:700;color:#5eead4;text-shadow:0 0 6px #5eead4,0 0 14px #4ade8099;margin-right:4px}'
 +'.rj-title{background:linear-gradient(90deg,#4ade80,#5eead4,#c4b5fd,#4ade80);background-size:200% auto;-webkit-background-clip:text;background-clip:text;color:transparent!important;-webkit-text-fill-color:transparent;filter:drop-shadow(0 0 6px #5eead4cc) drop-shadow(0 0 14px #4ade8099);animation:rjShine 4s linear infinite}'
 +'@keyframes rjShine{to{background-position:200% center}}'
 +'.tabs .tab[data-tab="rejoin"].on,.tabs .tab[data-tab="rejoin"].act{color:#5eead4;text-shadow:0 0 6px #5eead4,0 0 14px #4ade8099;font-weight:700}'
 +'.rj-top{display:flex;align-items:center;gap:14px;margin:12px 0 4px;padding:12px 16px;border:1px solid var(--bd);border-radius:14px;background:var(--cd);transition:.3s}'
 +'.rj-top.on{border-color:#4ade8080;background:linear-gradient(135deg,#4ade8018,#5eead412);box-shadow:0 0 22px #4ade8040,inset 0 0 18px #4ade8014}'
 +'#rj_sw{position:relative;width:68px;height:38px;flex:none;display:inline-block;border-radius:999px}'
 +'#rj_sw input{position:absolute;inset:0;width:100%;height:100%;margin:0;opacity:0;cursor:pointer;z-index:1}'
 +'#rj_sw i{position:absolute;inset:0;border-radius:999px;background:#2b2e36;transition:.25s}'
 +'#rj_sw i::after{content:"";position:absolute;top:4px;left:4px;width:30px;height:30px;border-radius:50%;background:#e8eaed;transition:transform .25s,box-shadow .25s}'
 +'#rj_sw input:checked+i{background:linear-gradient(90deg,#22c55e,#5eead4);animation:rjGlow 2s ease-in-out infinite}'
 +'#rj_sw input:checked+i::after{transform:translateX(30px);background:#fff;box-shadow:0 0 8px #fff,0 0 18px #5eead4}'
 +'@keyframes rjGlow{0%,100%{box-shadow:0 0 8px #4ade80,0 0 18px #4ade8099}50%{box-shadow:0 0 14px #5eead4,0 0 32px #5eead4aa}}'
 +'#rj_lb{font-size:17px;letter-spacing:.3px;color:var(--mu);transition:.3s}.rj-top.on #rj_lb{color:#4ade80;text-shadow:0 0 8px #4ade80,0 0 18px #4ade8099}'
 +'.rj-g{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:12px}@media(max-width:900px){.rj-g{grid-template-columns:1fr}}'
 +'.rj-it{display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--bd)}.rj-it:last-child{border-bottom:0}'
 +'.rj-it b{word-break:break-all}.rj-m{font-size:12px;color:var(--mu);line-height:1.6}.rj-dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:8px;background:var(--c);box-shadow:0 0 8px var(--c)}'
 +'.rj-log{font:12px/1.6 ui-monospace,Menlo,monospace;height:150px;overflow-y:auto;overflow-x:hidden;white-space:pre-wrap;word-break:break-word;color:var(--mu);scrollbar-width:none;-ms-overflow-style:none}.rj-log::-webkit-scrollbar{display:none}'
 +'.vr-g{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:8px;margin:6px 0 4px}.vr-c{background:var(--bg);border:1px solid var(--bd);border-radius:12px;padding:10px 12px;min-width:0}.vr-c small{color:var(--mu);font-size:11.5px}.vr-id{display:block;font:600 13px/1.5 ui-monospace,Menlo,Consolas,monospace;margin-top:4px;word-break:break-all}span.vr-id{display:inline;margin:0}.vr-m{font-size:12px;color:var(--mu);line-height:1.6;margin-top:2px}.vr-w{font-size:12px;color:#fbbf24;line-height:1.6;margin-top:4px}.vr-ok{font-size:12px;color:#4ade80;margin-top:4px}'
 +'.vr-sec{font-size:12px;color:var(--mu);margin:12px 0 6px}.vr-l{border:1px solid var(--bd);border-radius:12px;overflow:hidden}.vr-o{display:flex;align-items:center;gap:12px;padding:10px 12px;cursor:pointer;border-bottom:1px solid var(--bd);background:var(--bg)}.vr-o:last-child{border-bottom:0}.vr-o.on{background:linear-gradient(135deg,#60a5fa14,#5eead40d)}.vr-o input{accent-color:#60a5fa;margin:0;flex:none}.vr-t{flex:1;min-width:0}.vr-t b{font-size:13px}.vr-rec{font-size:11px;color:#60a5fa;border:1px solid #60a5fa55;border-radius:999px;padding:1px 8px}.vr-sel{background:var(--cd);border:1px solid var(--bd);border-radius:8px;padding:5px 8px;font-size:12px;max-width:260px;color:var(--tx)}'
 +'.rj-tip{font-size:12px;color:var(--mu);line-height:1.8;padding:8px 2px}.rj-tip b{color:var(--tx)}'
 +'.rj-ta{width:100%;min-height:110px;box-sizing:border-box;margin-top:8px;font:12px/1.5 ui-monospace,Menlo,monospace;white-space:pre;overflow:auto;background:var(--cd);color:var(--tx);border:1px solid var(--bd);border-radius:8px;padding:8px}';
document.head.appendChild(css);
tabs.insertAdjacentHTML('beforeend','<button class="tab" data-tab="rejoin" type="button" title="Auto Rejoin">Auto Rejoin</button>');
var pane=document.createElement('div');pane.className='tabpane';pane.dataset.pane='rejoin';pane.id='pane-rejoin';pane.style.display='none';
pane.innerHTML='<div class="ckhd"><div><b class="ckti"><span class="rj-ic">\u21BB\uFE0E</span> <span class="rj-title">Auto Rejoin</span></b></div></div><div class="rj-top" id="rj_top"><div id="rj_sw"><input type="checkbox" id="rj_on"><i></i></div><b id="rj_lb">Tắt</b></div><div class="cfg-msg" id="rj_msg"></div><div id="rj_root"></div>';
tf.appendChild(pane);
var root=pane.querySelector('#rj_root'),msg=pane.querySelector('#rj_msg'),onBox=pane.querySelector('#rj_on'),onLb=pane.querySelector('#rj_lb');
function dqLocal(path){try{var L=window.__DQ_LOCAL__;if(L&&L.url&&/^\/api\/(config|rejoin|perf)\//.test(path))return L}catch(e){}return null}
function hdr(loc){return {'x-api-key':(loc?loc.key:(typeof KEYV!=='undefined'?KEYV:'')),'x-requested-with':'dq','content-type':'application/json'}}
async function api(path,opt){var L=dqLocal(path),url=L?(L.url+path):path,ac=new AbortController(),tm=setTimeout(function(){ac.abort()},45000),r;try{r=await fetch(url,Object.assign({headers:hdr(L),signal:ac.signal},opt||{}))}catch(e){throw new Error(e&&e.name==='AbortError'?'Hết thời gian chờ (Roblox không trả lời)':'Không gọi được tracker')}finally{clearTimeout(tm)}var j=null;try{j=await r.json()}catch(e){}if(!r.ok)throw new Error((j&&(j.message||j.error))||('HTTP '+r.status));return j}
function el(t,c,x){var e=document.createElement(t);if(c)e.className=c;if(x!=null)e.textContent=x;return e}
function say(t,c){msg.className='cfg-msg'+(c?' '+c:'');msg.textContent=t||''}
function ago(t){if(!t)return '-';var s=Math.max(0,Math.round((Date.now()-t)/1000));return s<60?s+' giây trước':s<3600?Math.round(s/60)+' phút trước':Math.round(s/3600)+' giờ trước'}
function nm(n,i){try{return hideAccounts===true?'HIDE':n}catch(e){return n}}
function maskLog(t){try{if(hideAccounts!==true)return t;ST.items.forEach(function(it,i){if(it.name)t=t.split(it.name).join('HIDE')})}catch(e){}return t}
var C=null,ST={items:[],log:[],counts:{rejoin:0,kicked:0,disconnect:0},mine:true},dirty=false,listBox=null,logBox=null,started=false;
var WHY={rejoin:['Captcha rejoin','#fbbf24'],disconnect:['Mất kết nối','#fbbf24'],kicked:['Bị kick','#f43f5e'],'harvard-stuck':['Harvard bị đứng - đang mở lại','#fbbf24'],run:['Đang chạy','#4ade80'],ingame:['Trong game, script chưa báo','#fbbf24'],out:['Không trong game','#f43f5e'],check:['Đang kiểm tra…','#8a919c'],idle:['Auto Rejoin đang tắt','#8a919c'],off:['Đã tạm tắt acc này','#8a919c']};
var saveT=null;
function syncOn(){onBox.checked=!!(C&&C.enabled);onLb.textContent=onBox.checked?'Đang bật':'Tắt';var tp=pane.querySelector('#rj_top');if(tp)tp.classList.toggle('on',onBox.checked)}
var MY_KEY_CACHE=null;
function myKey(){return MY_KEY_CACHE||''}   // key ca nhan dang dang nhap - tu gan vao cfg de Auto Rejoin (chay qua App Windows) tu goi ve server that lay dung trang thai bao cao, khong can nguoi dung tu dan
function primeMyKey(){   // KEYV chi la bien JS duoc server "nuong" san vao trang luc render (khong nam trong localStorage) nen khong doc duoc tu day - phai tu goi /api/me (xac thuc bang cookie dang dang nhap, KHONG lam xoay token nhu /api/me/token) de lay dung token that
  if(MY_KEY_CACHE!=null)return;
  fetch('/api/me',{headers:{'x-requested-with':'dq'}}).then(function(r){return r.json()}).then(function(j){MY_KEY_CACHE=(j&&j.token)||''}).catch(function(){MY_KEY_CACHE=''})
}
primeMyKey();
async function saveNow(){   // tự lưu mọi thay đổi (không có nút Save)
  if(!C)return;
  try{await api('/api/rejoin/config',{method:'POST',body:JSON.stringify(Object.assign({},C,{apiKey:myKey()}))});dirty=false;ST.mine=true;say('');return true}
  catch(e){say('Lưu lỗi: '+e.message,'err');return false}
}
function touch(){dirty=true;clearTimeout(saveT);saveT=setTimeout(saveNow,600)}
function flushNow(){if(!dirty||!C)return;try{var L=dqLocal('/api/rejoin/config');fetch(L?(L.url+'/api/rejoin/config'):'/api/rejoin/config',{method:'POST',headers:hdr(L),body:JSON.stringify(Object.assign({},C,{apiKey:myKey()})),keepalive:true});dirty=false}catch(e){}}   // đóng / ẩn trang khi chưa kịp lưu
window.addEventListener('pagehide',flushNow);document.addEventListener('visibilitychange',function(){if(document.visibilityState==='hidden')flushNow()});
onBox.onchange=async function(){
  if(!C){onBox.checked=false;return}
  C.enabled=onBox.checked;syncOn();clearTimeout(saveT);
  if(!(await saveNow()))return;
  if(C.enabled){try{var j=await api('/api/rejoin/test',{method:'POST',body:'{}'});if(!j.ok)say(j.message,'err')}catch(e){}}   // bật mà chưa tìm thấy Roblox thì báo
};
function box(title,color){var b=el('div','cfg-box');b.style.setProperty('--c',color);b.appendChild(el('h3',0,title));var bb=el('div','cfg-bb');b.appendChild(bb);return [b,bb]}
function rowBool(bb,label,k){var r=el('div','cfg-f');r.appendChild(el('label',0,label));var sw=el('div','sw'),i=document.createElement('input');i.type='checkbox';i.checked=!!C[k];i.onchange=function(){C[k]=i.checked;touch()};sw.appendChild(i);sw.appendChild(document.createElement('i'));r.appendChild(sw);bb.appendChild(r)}
function rowNum(bb,label,k,unit){var r=el('div','cfg-f');r.appendChild(el('label',0,label+(unit?' ('+unit+')':'')));var i=document.createElement('input');i.type='text';i.inputMode='numeric';i.value=(typeof C[k]==='number'&&(C[k]>0||k==='waitSec'||k==='openDelay'))?String(C[k]):'';var rg=k==='batch'?[1,20]:k==='openDelay'?[0,600]:null;
  if(rg){i.oninput=function(){var d=i.value.replace(/\D/g,'');if(d!==i.value)i.value=d;if(d==='')return;var n=+d;if(n>=rg[0]&&n<=rg[1]){C[k]=n;touch()}};i.onblur=function(){var n=i.value===''?C[k]:+i.value;n=Math.min(rg[1],Math.max(rg[0],n));C[k]=n;i.value=String(n);touch()}}
  else i.oninput=function(){var d=i.value.replace(/\D/g,'');if(d!==i.value)i.value=d;C[k]=d?+d:0;touch()};
  r.appendChild(i);bb.appendChild(r)}
function rowText(bb,label,k,ph,filt){var r=el('div','cfg-f');r.appendChild(el('label',0,label));var i=document.createElement('input');i.type='text';i.value=C[k]||'';i.placeholder=ph||'';i.oninput=function(){if(filt){var d=i.value.replace(filt,'');if(d!==i.value)i.value=d}C[k]=i.value;touch()};r.appendChild(i);bb.appendChild(r);return r}
// ===== VERSIONS UI =====
var VI=null,vbox=null,vbody=null,vbusy=false;
function vmt(t){try{return new Date(t).toLocaleDateString()}catch(e){return ''}}
function vcell(ti){var c=el('div','vr-c');c.appendChild(el('small',0,ti));return c}
async function vload(chk){
  if(!vbody||vbusy)return;vbusy=true;
  try{VI=await api('/api/rejoin/versions'+(chk?'?check=1':''))}catch(e){VI=null;vbody.textContent='';vbody.appendChild(el('div','rj-tip','Không đọc được: '+e.message));vbusy=false;return}
  vbusy=false;vdraw()}
function vdraw(){
  if(!vbody||!VI||!C)return;vbody.textContent='';
  if(!VI.win)vbody.appendChild(el('div','rj-tip','Chỉ dùng được khi tracker chạy trên Windows.'));
  if(VI.custom)vbody.appendChild(el('div','rj-tip','Đang dùng đường dẫn Roblox tự điền: '+VI.custom+' — mục chọn bản bên dưới bị bỏ qua.'));
  var cells=el('div','vr-g');
  var a=vcell('Account mở bằng bản');var u=VI.use;
  a.appendChild(el('b','vr-id',u?u.id:(VI.custom?'Tự điền':'Chưa cài Roblox')));
  var WH={newest:'Bản mới nhất đã cài',latest:'Bản mới nhất của Roblox',pick:'Bản bạn chọn'};
  if(u)a.appendChild(el('div','vr-m',WH[u.why]||''));
  if(u&&u.warn)a.appendChild(el('div','vr-w',u.warn));
  cells.appendChild(a);
  var b=vcell('Bản mới nhất của Roblox');var L=VI.latest||{};
  b.appendChild(el('b','vr-id',L.id||'Chưa biết'));
  if(L.num)b.appendChild(el('div','vr-m',L.num));
  if(L.id){var ins=VI.installed.some(function(x){return x.id.toLowerCase()===L.id.toLowerCase()});b.appendChild(el('div',ins?'vr-ok':'vr-w',ins?'Đã cài trên máy này':'Chưa cài trên máy này (Roblox sẽ tự cài khi bạn mở bằng app Roblox thường)'))}
  else if(L.err)b.appendChild(el('div','vr-w',L.err));
  cells.appendChild(b);
  var c=vcell('Client đang chạy');var rk=Object.keys(VI.running||{});
  if(!rk.length)c.appendChild(el('div','vr-m','Chưa có'));
  rk.forEach(function(k){var d=el('div','vr-m');d.appendChild(el('span','vr-id',k));d.appendChild(document.createTextNode('  ×'+VI.running[k]));c.appendChild(d)});
  cells.appendChild(c);vbody.appendChild(cells);
  vbody.appendChild(el('div','vr-sec','Chọn bản để mở'));
  var lst=el('div','vr-l');
  function opt(mode,ti,sub,rec,extra){var r=el('label','vr-o'+(C.verMode===mode?' on':''));var i=document.createElement('input');i.type='radio';i.name='vr_mode';i.checked=C.verMode===mode;
    i.onchange=async function(){C.verMode=mode;touch();clearTimeout(saveT);await saveNow();vload()};
    r.appendChild(i);var t=el('div','vr-t');t.appendChild(el('b',0,ti));t.appendChild(el('div','vr-m',sub));r.appendChild(t);if(rec)r.appendChild(el('span','vr-rec','Mặc định'));if(extra)r.appendChild(extra);lst.appendChild(r)}
  opt('newest','Bản mới nhất đã cài','Bản Roblox có file mới nhất trên máy (cách tool vẫn dùng từ trước)',true);
  opt('latest','Bản mới nhất của Roblox',L.id?('Dùng '+L.id+' nếu đã cài, chưa cài thì dùng bản mới nhất đã cài'):'Bấm Kiểm tra để biết bản mới nhất');
  var sel=document.createElement('select');sel.className='vr-sel';
  var o0=document.createElement('option');o0.value='';o0.textContent='— chọn bản đã cài —';sel.appendChild(o0);
  (VI.installed||[]).forEach(function(x){var o=document.createElement('option');o.value=x.id;o.textContent=x.id+' · '+vmt(x.mtime);sel.appendChild(o)});
  if(C.verPick&&!(VI.installed||[]).some(function(x){return x.id===C.verPick})){var o=document.createElement('option');o.value=C.verPick;o.textContent=C.verPick+' · (không còn trên máy)';sel.appendChild(o)}
  sel.value=C.verPick||'';
  sel.onclick=function(e){e.stopPropagation()};
  sel.onchange=async function(){C.verPick=sel.value;if(sel.value)C.verMode='pick';touch();clearTimeout(saveT);await saveNow();vload()};
  opt('pick','Bản khác','Chọn một bản Roblox còn trên máy (thư mục Versions)',false,sel);
  vbody.appendChild(lst);
  vbody.appendChild(el('div','rj-tip','Roblox tự xoá bản cũ khi cập nhật, nên chỉ chọn được bản còn trong thư mục Versions. Đổi bản chỉ áp dụng cho lần mở / rejoin kế tiếp, client đang chạy giữ nguyên.'))}
function vBuild(){
  var bx=box('Versions','#60a5fa');bx[0].style.marginTop='12px';vbox=bx[0];vbody=bx[1];
  var bar=el('div','cfg-bar');var chk=el('button','cfg-b','Kiểm tra bản mới');chk.onclick=async function(){chk.disabled=true;await vload(true);chk.disabled=false};bar.appendChild(chk);vbody.appendChild(bar);
  var hold=el('div');vbody.appendChild(hold);vbody=hold;vbody.appendChild(el('div','rj-tip','Đang đọc…'));
  setTimeout(vload,50);return bx[0]}
function build(){
  root.textContent='';
  var ck=box('Thêm cookie','#a78bfa'),cb=ck[1];
  var ta=document.createElement('textarea');ta.className='rj-ta';ta.spellcheck=false;ta.autocomplete='off';ta.placeholder='_|WARNING:-DO-NOT-SHARE-THIS...';cb.appendChild(ta);
  var br=el('div','cfg-bar');var add=el('button','cfg-b p','Thêm & theo dõi');br.appendChild(add);cb.appendChild(br);
  var out=el('div','rj-m');cb.appendChild(out);
  add.onclick=async function(){
    var list=ta.value.split(/\r?\n/).map(function(x){return x.trim()}).filter(Boolean);if(!list.length)return;
    add.disabled=true;var ok=0,bad=[],keep=[];
    try{
      for(var i=0;i<list.length;i++){   // từng cookie một để thấy tiến độ, không bị đơ
        out.textContent='Đang kiểm tra '+(i+1)+'/'+list.length+'…'+(ok?' (đã thêm '+ok+')':'');
        try{
          var j=await api('/api/rejoin/cookies',{method:'POST',body:JSON.stringify({cookies:[list[i]]})});
          var x=(j.results||[])[0];
          if(x&&x.ok)ok++;else{bad.push('#'+(i+1)+': '+(x?x.msg:'lỗi'));keep.push(list[i])}
        }catch(e){bad.push('#'+(i+1)+': '+e.message);keep.push(list[i])}
      }
      out.textContent='Đã thêm '+ok+'/'+list.length+' account.'+(bad.length?' Lỗi: '+bad.join(' · '):'');
      ta.value=keep.join('\n');   // cookie thêm được thì xoá sạch; cookie lỗi giữ lại để thử lại
      try{await poll()}catch(e){}
    }finally{add.disabled=false}
  };
  var g=el('div','rj-g');g.style.marginBottom='12px';g.appendChild(ck[0]);
  var a=box('Mở game','#5eead4'),ab=a[1];
  rowNum(ab,'PlaceId','placeId');
  var rm=el('div','cfg-f');rm.appendChild(el('label',0,'Vào lại bằng'));var w=el('div');w.style.cssText='display:flex;gap:6px';
  function modeBtns(){w.textContent='';[['place','Place'],['party','Follow leader']].forEach(function(m){var b=el('button','pd-bt'+(C.mode===m[0]?' on':''),m[1]);b.type='button';b.onclick=function(){C.mode=m[0];touch();modeBtns();fu.style.display=C.mode==='party'?'':'none'};w.appendChild(b)})}
  rm.appendChild(w);ab.appendChild(rm);
  var fu=rowText(ab,'Leader để follow','followUser','tên Roblox của leader',/[^\w]/g);fu.style.display=C.mode==='party'?'':'none';modeBtns();
  rowBool(ab,'Mở nhiều client cùng lúc','multi');
  rowBool(ab,'Phát hiện Harvard bị đứng (tự đóng+mở lại)','hvCheck');
  rowNum(ab,'Mở cùng lúc','batch','client');
  rowNum(ab,'Delay mở Roblox','openDelay','giây');
  g.insertBefore(a[0],g.firstChild);   // Mở game bên trái, Thêm cookie bên phải
  var lg=box('Log','#8a919c');logBox=lg[1];lg[0].style.marginTop='12px';root.appendChild(lg[0]);   // Log nằm trên cùng, cao cố định, cuộn bằng chuột (ẩn thanh cuộn)
  root.appendChild(g);
  var l=box('Account đang theo dõi','#fb7185');listBox=l[1];root.appendChild(l[0]);
  root.appendChild(vBuild());
  live();
}
function live(){
  if(!listBox)return;
  listBox.textContent='';
  if(!ST.mine)listBox.appendChild(el('div','rj-tip','Cấu hình đang thuộc tài khoản khác. Bật/tắt hoặc đổi một ô ở đây để chạy cho account của bạn.'));
  if(!ST.items.length)listBox.appendChild(el('div','rj-tip','Chưa có account nào. Dán cookie vào ô Thêm cookie.'));
  ST.items.forEach(function(it,i){
    var w=WHY[it.view||it.why]||null,col=w?w[1]:'#8a919c',lb=w?w[0]:'Đang kiểm tra…';
    var r=el('div','rj-it'),l=el('div');r.style.setProperty('--c',col);var b=el('b');var d=el('span','rj-dot');b.appendChild(d);b.appendChild(document.createTextNode(nm(it.name,i)));l.appendChild(b);
    l.appendChild(el('div','rj-m',lb+(it.last?' · mở lại '+ago(it.last):'')+(it.result?' · '+it.result:'')));
    if(it.off)r.style.opacity='.55';
    r.appendChild(l);var bt=el('div');bt.style.cssText='display:flex;gap:6px';
    var tg=el('button','cfg-b'+(it.off?'':' on'),it.off?'Bật':'Tắt');tg.title=it.off?'Bật lại auto rejoin cho account này':'Tạm tắt auto rejoin cho account này (vẫn còn trong danh sách)';
    tg.onclick=async function(){tg.disabled=true;try{await api('/api/rejoin/toggle',{method:'POST',body:JSON.stringify({name:it.name,on:!!it.off})});await poll()}catch(e){say(e.message,'err')}tg.disabled=false};bt.appendChild(tg);
    var go=el('button','cfg-b','Rejoin ngay');go.onclick=async function(){go.disabled=true;try{var j=await api('/api/rejoin/now',{method:'POST',body:JSON.stringify({names:[it.name]})});var x=j.results&&j.results[0];say(x&&!x.ok?'Lỗi: '+x.msg:'',x&&!x.ok?'err':'');await poll()}catch(e){say(e.message,'err')}go.disabled=false};bt.appendChild(go);
    var rmv=el('button','cfg-b','✕');rmv.title='Bỏ khỏi tool Rejoin (cookie vẫn còn trong tab Cookie)';rmv.onclick=async function(){try{await api('/api/rejoin/remove',{method:'POST',body:JSON.stringify({names:[it.name]})});await poll()}catch(e){say(e.message,'err')}};bt.appendChild(rmv);
    r.appendChild(bt);listBox.appendChild(r);
  });
  var po=logBox.querySelector('.rj-log'),atEnd=!po||po.scrollHeight-po.scrollTop-po.clientHeight<24,pt=po?po.scrollTop:0;logBox.textContent='';
  var cnt=ST.counts||{rejoin:0,kicked:0,disconnect:0},cg=el('div','vr-g');cg.style.marginBottom='10px';
  [['rejoin','Số lần Rejoin','#fbbf24'],['kicked','Bị kick','#f43f5e'],['disconnect','Mất kết nối','#fbbf24']].forEach(function(d){
    var c=el('div','vr-c');c.style.cssText='display:flex;align-items:center;justify-content:space-between;gap:10px';
    c.appendChild(el('small',0,d[1]));
    var b=el('b',0,String(cnt[d[0]]||0));b.style.cssText='font-size:20px;color:'+d[2];
    c.appendChild(b);cg.appendChild(c);
  });
  logBox.appendChild(cg);
  var rb=el('button','cfg-b','Reset số lần');rb.style.marginBottom='10px';
  rb.onclick=async function(){rb.disabled=true;try{var j=await api('/api/rejoin/statsreset',{method:'POST'});ST.counts=j.counts;live()}catch(e){say('Lỗi: '+e.message,'err')}finally{rb.disabled=false}};
  logBox.appendChild(rb);
  var pre=el('div','rj-log');pre.textContent=ST.log.length?maskLog(ST.log.map(function(x){return new Date(x.t).toLocaleTimeString()+'  '+x.m}).join('\n')):'(chưa có)';logBox.appendChild(pre);pre.scrollTop=atEnd?pre.scrollHeight:pt;
}
async function poll(){
  try{var j=await api('/api/rejoin/state');ST.items=j.items||[];ST.log=j.log||[];ST.counts=j.counts||ST.counts;ST.mine=j.mine!==false;if(!C){C=j.cfg;syncOn();build()}else live();
    var mk=myKey();if(mk&&C&&C.apiKey!==mk){C.apiKey=mk;saveNow()}}   // tu sua neu apiKey dang luu sai/thieu (kiem tra moi vong poll, khong chi lan dau - vi /api/me co the chua tra ve kip luc trang vua mo)
  catch(e){if(!C){root.textContent='';say('Không đọc được: '+e.message,'err')}}
}
document.addEventListener('click',function(e){if(e.target&&e.target.id==='ha')setTimeout(function(){if(C)live()},30)});
setInterval(function(){if(document.hidden||pane.style.display==='none')return;poll()},5000);
setInterval(function(){if(!started&&pane.style.display!=='none'&&pane.offsetParent!==null){started=true;poll()}},600);
}catch(e){console.error('rejoin ui',e)}
})();
`;
const PF_UI_JS = String.raw`
(function(){
try{
var tabs=$('tabs'),tf=$('tf');
if(!tabs||!tf||document.querySelector('.tab[data-tab="perf"]'))return;
var css=document.createElement('style');
css.textContent='.tabs .tab[data-tab="perf"]::before{content:"⚡"}.tabs .tab[data-tab="perf"]{order:13}'
 +'.tabs .tab[data-tab="monitor"]::before{content:"\\25A6\\FE0E";font-family:"Segoe UI Symbol","Arial Unicode MS",sans-serif;font-size:19px}.tabs .tab[data-tab="monitor"]{order:14}'
 +'.pf-g{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:12px}@media(max-width:900px){.pf-g{grid-template-columns:1fr}}'
 +'.pf-g{align-items:stretch}.pf-g .cfg-box{max-height:none;height:auto;overflow:visible}.pf-g .cfg-bb{overflow:visible;max-height:none}'
 +'.pf-g .pf-wide{grid-column:1/-1}.pf-g .pf-wide .cfg-bb{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:8px 12px;align-items:stretch}.pf-g .pf-wide .cfg-bb>.pf-hd,.pf-g .pf-wide .cfg-bb>.pf-st,.pf-g .pf-wide .cfg-bb>.cfg-bar{grid-column:1/-1}.pf-g .pf-wide .cfg-f{margin:0}'
 +'.pf-hd{display:flex;align-items:center;gap:14px;padding:10px 12px;margin-bottom:8px;border:1px solid var(--bd);border-radius:12px;background:var(--bg);transition:.3s}'
 +'.pf-hd.on{border-color:#4ade8080;background:linear-gradient(135deg,#4ade8018,#5eead412);box-shadow:0 0 18px #4ade8038}'
 +'.pf-sw{position:relative;width:68px;height:38px;flex:none;border-radius:999px}.pf-sw input{position:absolute;inset:0;width:100%;height:100%;margin:0;opacity:0;cursor:pointer;z-index:1}'
 +'.pf-sw i{position:absolute;inset:0;border-radius:999px;background:#2b2e36;transition:.25s}.pf-sw i::after{content:"";position:absolute;top:4px;left:4px;width:30px;height:30px;border-radius:50%;background:#e8eaed;transition:transform .25s}'
 +'.pf-sw input:checked+i{background:linear-gradient(90deg,#22c55e,#5eead4);animation:rjGlow 2s ease-in-out infinite}.pf-sw input:checked+i::after{transform:translateX(30px);background:#fff;box-shadow:0 0 8px #fff,0 0 18px #5eead4}'
 +'.pf-hd b{font-size:16px;color:var(--mu);transition:.3s}.pf-hd.on b{color:#4ade80;text-shadow:0 0 8px #4ade80,0 0 18px #4ade8099}'
 +'#pane-perf .cfg-f,#pane-rejoin .cfg-f,#pane-config .cfg-f{padding:7px 10px}#pane-perf .cfg-f label,#pane-rejoin .cfg-f label,#pane-config .cfg-f label{font-size:12.5px}'
 +'#pane-perf .cfg-f input[type=text],#pane-perf .cfg-f select,#pane-rejoin .cfg-f input[type=text],#pane-rejoin .cfg-f select{font-size:12.5px;padding:5px 7px;width:110px;min-width:0}'
 +'#pane-perf .cfg-f select{width:auto;min-width:130px}'
 +'#pane-perf .cfg-f .sw,#pane-rejoin .cfg-f .sw,#pane-config .cfg-f .sw{width:38px;height:22px}'
 +'#pane-perf .cfg-f .sw i::after,#pane-rejoin .cfg-f .sw i::after,#pane-config .cfg-f .sw i::after{top:3px;left:3px;width:16px;height:16px}'
 +'#pane-perf .cfg-f .sw input:checked+i::after,#pane-rejoin .cfg-f .sw input:checked+i::after,#pane-config .cfg-f .sw input:checked+i::after{transform:translateX(16px)}'
 +'.pf-sw,#rj_sw{width:46px!important;height:26px!important}.pf-sw i::after,#rj_sw i::after{top:3px!important;left:3px!important;width:20px!important;height:20px!important}'
 +'.pf-sw input:checked+i::after,#rj_sw input:checked+i::after{transform:translateX(20px)!important}'
 +'.pf-hd,.rj-top{padding:8px 12px!important;gap:12px!important;cursor:pointer}.pf-hd b,#rj_lb{font-size:14px!important}'
 +'#pane-perf .cfg-f:has(.sw),#pane-rejoin .cfg-f:has(.sw),#pane-config .cfg-f:has(.sw){cursor:pointer}#pane-perf .cfg-f:has(.sw):hover,#pane-rejoin .cfg-f:has(.sw):hover,#pane-config .cfg-f:has(.sw):hover{border-color:var(--mu)}'
 +'.mon{margin-top:12px;max-height:none!important;height:auto!important;overflow:visible!important}.mon-hd{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;flex-wrap:wrap;margin-bottom:10px}.mon-sub{font-size:12px;color:var(--mu);margin-top:2px}'
 +'.mon-rg{display:flex;gap:4px;background:var(--bg);border:1px solid var(--bd);border-radius:10px;padding:3px}.mon-rb{border:0;background:transparent;color:var(--mu);padding:4px 11px;border-radius:7px;cursor:pointer;font-size:12px}.mon-rb.on{background:linear-gradient(180deg,#3a3d44,#1c1e22);color:#fff}'
 +'.mon-tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin-bottom:10px}.mon-t{background:var(--bg);border:1px solid var(--bd);border-top:2px solid var(--c);border-radius:12px;padding:9px 11px;min-width:0}.mon-t small{color:var(--mu);font-size:11.5px}.mon-t b{display:block;font-size:20px;margin-top:2px;white-space:nowrap}.mon-ts{display:block;font-size:11px;color:var(--mu);margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
 +'.mon-cg{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px}.mon-cg>.mon-c{grid-column:span 2}.mon-cg>.mon-c:nth-child(n+4){grid-column:span 3}@media(max-width:900px){.mon-cg>.mon-c,.mon-cg>.mon-c:nth-child(n+4){grid-column:1/-1}}.mon-c{background:var(--bg);border:1px solid var(--bd);border-radius:12px;padding:9px 11px;min-width:0}.mon-ch{display:flex;justify-content:space-between;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:4px}.mon-ch b{font-size:12.5px}.mon-lg{display:flex;gap:10px;font-size:11px;color:var(--mu)}.mon-lg span{display:inline-flex;align-items:center;gap:4px}.mon-lg i{width:8px;height:8px;border-radius:50%;display:inline-block}.mon-lg em{font-style:normal}'
 +'.mon-an{animation:monSl 2s linear both;will-change:transform}@keyframes monSl{from{transform:translateX(var(--dx))}to{transform:translateX(0)}}.mon-sv{position:relative;height:84px;overflow:hidden}.mon-sv svg{width:100%;height:84px;display:block}.mon-sv .gl{stroke:var(--bd);stroke-width:1;vector-effect:non-scaling-stroke}.mon-mx{position:absolute;top:0;left:2px;font-size:10px;color:var(--mu);opacity:.8}'
 +'.mon-tb{margin-top:10px;background:var(--bg);border:1px solid var(--bd);border-radius:12px;padding:9px 11px}.mon-tb h4{margin:0 0 6px;font-size:12.5px}.mon-tw{overflow-x:auto}.mon-tw table{width:100%;border-collapse:collapse;font-size:12px}.mon-tw th{text-align:left;color:var(--mu);font-weight:500;padding:4px 8px;border-bottom:1px solid var(--bd)}.mon-tw td{padding:5px 8px;border-bottom:1px solid var(--bd)}.mon-tw tr:last-child td{border-bottom:0}.mon-tw td.ok{color:#4ade80!important}.mon-tw td.bad{color:#f43f5e!important}'
 +'.pf-st{font-size:12px;color:var(--mu);line-height:1.7;padding:6px 2px;min-height:1.7em}.pf-st.err{color:#f43f5e}.pf-st.ok{color:#4ade80}'
 +'.pf-title{background:linear-gradient(90deg,#fbbf24,#f472b6,#c4b5fd,#fbbf24);background-size:200% auto;-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;filter:drop-shadow(0 0 6px #f472b6aa);animation:rjShine 4s linear infinite}';
document.head.appendChild(css);
tabs.insertAdjacentHTML('beforeend','<button class="tab" data-tab="perf" type="button" title="Performance">Performance</button>');
var pane=document.createElement('div');pane.className='tabpane';pane.dataset.pane='perf';pane.id='pane-perf';pane.style.display='none';
pane.innerHTML='<div class="ckhd"><div><b class="ckti">⚡ <span class="pf-title">Performance</span></b></div></div><div class="cfg-msg" id="pf_msg"></div><div id="pf_root"></div>';
tf.appendChild(pane);
tabs.insertAdjacentHTML('beforeend','<button class="tab" data-tab="monitor" type="button" title="Monitor">Monitor</button>');
var monPane=document.createElement('div');monPane.className='tabpane';monPane.dataset.pane='monitor';monPane.id='pane-monitor';monPane.style.display='none';
monPane.innerHTML='<div class="ckhd"><div><b class="ckti">\u25A6\uFE0E <span class="pf-title">Monitor</span></b></div></div><div id="mon_root"></div>';
tf.appendChild(monPane);
var root=pane.querySelector('#pf_root'),msg=pane.querySelector('#pf_msg');
function dqLocal(path){try{var L=window.__DQ_LOCAL__;if(L&&L.url&&/^\/api\/(config|rejoin|perf)\//.test(path))return L}catch(e){}return null}
function hdr(loc){return {'x-api-key':(loc?loc.key:(typeof KEYV!=='undefined'?KEYV:'')),'x-requested-with':'dq','content-type':'application/json'}}
async function api(path,opt){var L=dqLocal(path),url=L?(L.url+path):path,ac=new AbortController(),tm=setTimeout(function(){ac.abort()},70000),r;try{r=await fetch(url,Object.assign({headers:hdr(L),signal:ac.signal},opt||{}))}catch(e){throw new Error(e&&e.name==='AbortError'?'Hết thời gian chờ':'Không gọi được tracker')}finally{clearTimeout(tm)}var j=null;try{j=await r.json()}catch(e){}if(!r.ok)throw new Error((j&&(j.message||j.error))||('HTTP '+r.status));return j}
function el(t,c,x){var e=document.createElement(t);if(c)e.className=c;if(x!=null)e.textContent=x;return e}
function say(t,c){msg.className='cfg-msg'+(c?' '+c:'');msg.textContent=t||''}
var C=null,ST={},SUP=true,dirty=false,saveT=null,started=false,els={};
async function saveNow(){if(!C)return;try{await api('/api/perf/config',{method:'POST',body:JSON.stringify(C)});dirty=false;say('');return true}catch(e){say('Lưu lỗi: '+e.message,'err');return false}}
function touch(){dirty=true;clearTimeout(saveT);saveT=setTimeout(saveNow,600)}
function flushNow(){if(!dirty||!C)return;try{var L=dqLocal('/api/perf/config');fetch(L?(L.url+'/api/perf/config'):'/api/perf/config',{method:'POST',headers:hdr(L),body:JSON.stringify(C),keepalive:true});dirty=false}catch(e){}}
window.addEventListener('pagehide',flushNow);document.addEventListener('visibilitychange',function(){if(document.visibilityState==='hidden')flushNow()});
function box(title,color){var b=el('div','cfg-box');b.style.setProperty('--c',color);b.appendChild(el('h3',0,title));var bb=el('div','cfg-bb');b.appendChild(bb);return [b,bb]}
function head(bb,g,label){var h=el('div','pf-hd'),sw=el('div','pf-sw'),i=document.createElement('input');i.type='checkbox';i.checked=!!C[g].enabled;var b=el('b',0,i.checked?label+' · đang bật':label+' · tắt');
  h.classList.toggle('on',i.checked);
  i.onchange=async function(){C[g].enabled=i.checked;h.classList.toggle('on',i.checked);b.textContent=i.checked?label+' · đang bật':label+' · tắt';clearTimeout(saveT);await saveNow();poll()};
  sw.appendChild(i);sw.appendChild(document.createElement('i'));h.appendChild(sw);h.appendChild(b);bb.appendChild(h)}
var RNG={cpu:{total:[1,100],max:[0.5,100],interval:[2,600]},ram:{interval:[5,3600],threshold:[0,32000],batch:[1,50],delay:[0,10000]},arr:{w:[0,8000],h:[0,8000],gx:[0,500],gy:[0,500],perRow:[1,50],step:[0,500],interval:[2,600]}};
function rowNum(bb,label,g,k,unit,dec){var r=el('div','cfg-f');r.appendChild(el('label',0,label+(unit?' ('+unit+')':'')));var i=document.createElement('input');i.type='text';i.inputMode=dec?'decimal':'numeric';i.value=String(C[g][k]);var rg=(RNG[g]||{})[k]||[0,1e9];
  i.oninput=function(){var d=i.value.replace(dec?/[^0-9.]/g:/\D/g,'');if(d!==i.value)i.value=d;if(d===''||d==='.')return;var n=+d;if(n>=rg[0]&&n<=rg[1]){C[g][k]=n;touch()}};   // đang gõ dở (ngoài khoảng) thì chưa lưu, không báo lỗi
  i.onblur=function(){var n=i.value===''||i.value==='.'?C[g][k]:+i.value;n=Math.min(rg[1],Math.max(rg[0],n));if(!dec)n=Math.round(n);C[g][k]=n;i.value=String(n);touch()};   // rời ô: ép về khoảng hợp lệ
  r.appendChild(i);bb.appendChild(r)}
function rowBool(bb,label,g,k){var r=el('div','cfg-f');r.appendChild(el('label',0,label));var sw=el('div','sw'),i=document.createElement('input');i.type='checkbox';i.checked=!!C[g][k];i.onchange=function(){C[g][k]=i.checked;touch()};sw.appendChild(i);sw.appendChild(document.createElement('i'));r.appendChild(sw);bb.appendChild(r)}
var DIRS=[['grid_lr','Lưới: Trái → Phải'],['grid_rl','Lưới: Phải → Trái'],['grid_bl_tr','Lưới: Dưới-Trái → Trên-Phải'],['col_left','Cột bên trái'],['col_right','Cột bên phải'],['zigzag','ZigZag ngang'],['casc_tl','Xếp chồng: Trên-Trái → Dưới-Phải'],['casc_tr','Xếp chồng: Trên-Phải → Dưới-Trái']];
function rowDir(bb){var r=el('div','cfg-f');r.appendChild(el('label',0,'Kiểu sắp xếp'));var s=document.createElement('select');DIRS.forEach(function(d){var o=document.createElement('option');o.value=d[0];o.textContent=d[1];s.appendChild(o)});s.value=C.arr.dir;s.onchange=function(){C.arr.dir=s.value;touch()};r.appendChild(s);bb.appendChild(r)}
function stLine(){var d=el('div','pf-st');return d}
// ===== MONITOR UI =====
var MR=60,MS=[],MT=0,MM=null,MCL=[],MNOW=0,MERR='',MSUP=true,mroot=null,mel={},mBusy=false;
var MCOL={a:'#5eead4',b:'#f59e0b',c:'#a78bfa',d:'#60a5fa',e:'#f472b6'};
function fB(n){if(n==null)return '-';var u=['B','KB','MB','GB'],i=0;n=+n;while(n>=1024&&i<3){n/=1024;i++}return (n>=100||i===0?Math.round(n):n.toFixed(1))+' '+u[i]}
function fR(n){return n==null?'-':fB(n)+'/s'}
function fP(n){return n==null?'-':(+n).toFixed(n>=10?0:1)+'%'}
function fMB(mb){return mb==null?'-':mb>=1024?(mb/1024).toFixed(1)+' GB':Math.round(mb)+' MB'}
function fUp(ms){if(!(ms>0))return '-';var s=Math.floor(ms/1000),d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);return d?d+'d '+h+'h':h?h+'h '+m+'m':m?m+'m '+(s%60)+'s':s+'s'}
function hid(){try{return typeof hideAccounts!=='undefined'&&hideAccounts}catch(e){return false}}
function mtile(k,ti,col){var d=el('div','mon-t');d.style.setProperty('--c',col);d.appendChild(el('small',0,ti));var v=el('b',0,'-');d.appendChild(v);var s=el('span','mon-ts','');d.appendChild(s);mel['t_'+k]=[v,s];return d}
function mchart(k,ti,leg){var d=el('div','mon-c');var h=el('div','mon-ch');h.appendChild(el('b',0,ti));var lg=el('div','mon-lg');leg.forEach(function(l,i){var s=el('span');var dt=el('i');dt.style.background=l[1];s.appendChild(dt);var tx=el('em',0,l[0]);s.appendChild(tx);lg.appendChild(s);l.push(tx)});h.appendChild(lg);d.appendChild(h);var sv=el('div','mon-sv');d.appendChild(sv);mel['c_'+k]={sv:sv,leg:leg};return d}
function monBuild(){
  var w=el('div','cfg-box mon');w.style.setProperty('--c','#60a5fa');
  w.appendChild(el('h3',0,'Monitor'));var bb=el('div','cfg-bb');w.appendChild(bb);var hd=el('div','mon-hd');var l=el('div');mel.sub=el('div','mon-sub','');l.appendChild(mel.sub);hd.appendChild(l);
  var rg=el('div','mon-rg');[['1m',60],['5m',300],['15m',900],['1h',3600]].forEach(function(r){var b=el('button','mon-rb'+(r[1]===MR?' on':''),r[0]);b.type='button';b.onclick=function(){MR=r[1];[].forEach.call(rg.children,function(x){x.classList.toggle('on',x===b)});monDraw('range')};rg.appendChild(b)});hd.appendChild(rg);bb.appendChild(hd);
  var tl=el('div','mon-tiles');tl.appendChild(mtile('cpu','CPU',MCOL.a));tl.appendChild(mtile('mem','Memory',MCOL.c));tl.appendChild(mtile('gpu','GPU',MCOL.d));tl.appendChild(mtile('dk','Disk',MCOL.e));tl.appendChild(mtile('net','Network',MCOL.b));tl.appendChild(mtile('rbx','Roblox','#4ade80'));bb.appendChild(tl);
  var cg=el('div','mon-cg');
  cg.appendChild(mchart('cpu','CPU',[['Total',MCOL.a],['Roblox','#4ade80']]));
  cg.appendChild(mchart('mem','Memory',[['Used',MCOL.c],['Roblox','#4ade80']]));
  cg.appendChild(mchart('gpu','GPU',[['3D',MCOL.d]]));
  cg.appendChild(mchart('net','Network',[['Down',MCOL.b],['Up',MCOL.a]]));
  cg.appendChild(mchart('dsk','Disk',[['Read',MCOL.e],['Write',MCOL.d]]));
  bb.appendChild(cg);
  var tb=el('div','mon-tb');tb.appendChild(el('h4',0,'Roblox clients'));mel.tw=el('div','mon-tw');tb.appendChild(mel.tw);bb.appendChild(tb);
  mel.err=el('div','pf-st err');bb.appendChild(mel.err);
  mroot=w;monDraw();return w}
function mpick(){var tl=MS.length?MS[MS.length-1].t:Date.now(),cut=tl-MR*1000-40000,o=[];for(var i=0;i<MS.length;i++)if(MS[i].t>=cut)o.push(MS[i]);
  var bm=Math.max(2000,Math.ceil(MR*1000/160/2000)*2000);if(bm<=2000)return o;   // gộp theo mốc thời gian cố định để đường không "nhảy" khi có mẫu mới
  var r=[],cur=null,ck=-1,F=['cpu','mem','gpu','dk','dr','dw','rx','tx','rc','rm'];
  o.forEach(function(x){var k=Math.floor(x.t/bm);if(k!==ck){cur={t:x.t,n:{},s:{}};F.forEach(function(p){cur.s[p]=0;cur.n[p]=0});r.push(cur);ck=k}cur.t=x.t;F.forEach(function(p){if(x[p]!=null){cur.s[p]+=x[p];cur.n[p]++}})});
  return r.map(function(c){var m={t:c.t};F.forEach(function(p){m[p]=c.n[p]?c.s[p]/c.n[p]:null});return m})}
function msmooth(a){if(a.length<3){var d=a.length?'M'+a[0][0].toFixed(1)+' '+a[0][1].toFixed(1):'';for(var i=1;i<a.length;i++)d+='L'+a[i][0].toFixed(1)+' '+a[i][1].toFixed(1);return d}
  var d='M'+a[0][0].toFixed(1)+' '+a[0][1].toFixed(1);
  for(var i=1;i<a.length-1;i++){var mx=(a[i][0]+a[i+1][0])/2,my=(a[i][1]+a[i+1][1])/2;d+='Q'+a[i][0].toFixed(1)+' '+a[i][1].toFixed(1)+' '+mx.toFixed(1)+' '+my.toFixed(1)}
  var l=a[a.length-1];return d+'L'+l[0].toFixed(1)+' '+l[1].toFixed(1)}
function msvg(sv,pts,ser,max,fmt,leg,dx,dur){
  var W=300,H=84,tl=MS.length?MS[MS.length-1].t:Date.now(),m=max;
  function X(t){return W-(tl-t)/(MR*1000)*W}
  if(!m){var mm=0;ser.forEach(function(s){pts.forEach(function(p){if(p[s.k]!=null&&p[s.k]>mm&&X(p.t)>=0)mm=p[s.k]})});
    var q=[1024,10240,102400,1048576,5242880,10485760,52428800,104857600,524288000,1073741824],z=1024;for(var i=0;i<q.length;i++){z=q[i];if(mm<=z)break}if(mm>z)z=Math.ceil(mm/1073741824)*1073741824;
    m=Math.max(z,(sv._m||0)*0.94);sv._m=m}   // trục Y nhích lên ngay, hạ xuống từ từ cho đỡ giật
  var h='<svg viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none">';
  for(var g=1;g<4;g++)h+='<line x1="0" x2="'+W+'" y1="'+(H*g/4)+'" y2="'+(H*g/4)+'" class="gl"/>';
  h+='<g class="'+(dx>0?'mon-an':'')+'" style="--dx:'+(dx||0).toFixed(2)+'px;animation-duration:'+(dur||2000)+'ms">';
  ser.forEach(function(s,si){var a=[];pts.forEach(function(p){var v=p[s.k];if(v==null)return;var x=X(p.t);if(x<-24)return;a.push([x,H-Math.min(1,Math.max(0,v/m))*(H-2)-1])});
    if(!a.length)return;var d=msmooth(a);
    if(si===0)h+='<path d="'+d+'L'+a[a.length-1][0].toFixed(1)+' '+H+'L'+a[0][0].toFixed(1)+' '+H+'Z" fill="'+s.c+'" opacity=".14"/>';
    h+='<path d="'+d+'" fill="none" stroke="'+s.c+'" stroke-width="1.6" vector-effect="non-scaling-stroke" stroke-linejoin="round" stroke-linecap="round"/>'});
  h+='</g></svg><span class="mon-mx">'+fmt(m)+'</span>';sv.innerHTML=h;
  if(leg){var last=pts.length?pts[pts.length-1]:null;ser.forEach(function(s,i){if(leg[i])leg[i][2].textContent=leg[i][0]+(last&&last[s.k]!=null?' '+s.f(last[s.k]):'')})}}
var MPT=0;
function monDraw(mode){
  if(!mroot)return;var meta=MM||{},tot=meta.memTotalMB||0,last=MS.length?MS[MS.length-1]:null;
  var up=meta.bootMs?fUp((MNOW||Date.now())-meta.bootMs):'-';
  mel.sub.textContent='Updated every 2 s · Windows up '+up;
  if(last){
    mel.t_cpu[0].textContent=fP(last.cpu);mel.t_cpu[1].textContent=(meta.threads?meta.threads+' threads':'')+(meta.cpuName?' · '+String(meta.cpuName).replace(/\(R\)|\(TM\)|CPU|Processor/g,'').replace(/\s+/g,' ').trim():'');
    mel.t_mem[0].textContent=last.mem==null?'-':fMB(last.mem);mel.t_mem[1].textContent=tot?(last.mem==null?'':Math.round(last.mem/tot*100)+'% of '+fMB(tot)):'';
    mel.t_gpu[0].textContent=fP(last.gpu);mel.t_gpu[1].textContent=last.gpu==null?'no GPU counter':'3D engine';
    mel.t_dk[0].textContent=fP(last.dk);mel.t_dk[1].textContent='R '+fR(last.dr)+' · W '+fR(last.dw)+(last.df!=null?' · free '+Math.round(last.df)+' GB':'');
    mel.t_net[0].textContent='↓ '+fR(last.rx);mel.t_net[1].textContent='↑ '+fR(last.tx);
    mel.t_rbx[0].textContent=last.n+' client'+(last.n===1?'':'s');mel.t_rbx[1].textContent='CPU '+fP(last.rc)+' · RAM '+fMB(last.rm)}
  var pc=function(v){return fP(v)},B=function(v){return fR(v)};
  if(mode==='tick'&&MPT)mode='skip';
  if(mode!=='skip'){var pts=mpick(),tl=last?last.t:0,dx=0,dur=2000;if(mode==='new'&&MPT>0&&tl>MPT){dx=(tl-MPT)/(MR*1000)*300;dur=Math.min(5000,Math.max(800,tl-MPT));if(dx>45)dx=0}MPT=tl;
  msvg(mel.c_cpu.sv,pts,[{k:'cpu',c:MCOL.a,f:pc},{k:'rc',c:'#4ade80',f:pc}],100,function(){return '100%'},mel.c_cpu.leg,dx,dur);
  msvg(mel.c_mem.sv,pts,[{k:'mem',c:MCOL.c,f:fMB},{k:'rm',c:'#4ade80',f:fMB}],tot||0,function(m){return fMB(m)},mel.c_mem.leg,dx,dur);
  msvg(mel.c_gpu.sv,pts,[{k:'gpu',c:MCOL.d,f:pc}],100,function(){return '100%'},mel.c_gpu.leg,dx,dur);
  msvg(mel.c_net.sv,pts,[{k:'rx',c:MCOL.b,f:B},{k:'tx',c:MCOL.a,f:B}],0,B,mel.c_net.leg,dx,dur);
  msvg(mel.c_dsk.sv,pts,[{k:'dr',c:MCOL.e,f:B},{k:'dw',c:MCOL.d,f:B}],0,B,mel.c_dsk.leg,dx,dur);}
  var t='<table><thead><tr><th>Account</th><th>PID</th><th>CPU</th><th>Memory</th><th>Uptime</th><th>Status</th></tr></thead><tbody>';
  if(!MCL.length)t+='<tr><td colspan="6" class="mu">No Roblox client running.</td></tr>';
  MCL.forEach(function(c){var nm=hid()?'HIDE':(c.name||'-');t+='<tr><td>'+esc2(nm)+'</td><td>'+c.pid+'</td><td>'+fP(c.cpu)+'</td><td>'+fMB(c.mb)+'</td><td>'+(c.start?fUp((MNOW||Date.now())-c.start):'-')+'</td><td class="'+(c.resp?'ok':'bad')+'">'+(c.resp?'Running':'Not responding')+'</td></tr>'});
  mel.tw.innerHTML=t+'</tbody></table>';
  mel.err.textContent=MERR?('Monitor: '+MERR):'';
  if(!MSUP)mel.sub.textContent='Monitor only runs on Windows.'}
function esc2(s){return String(s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
async function monPoll(){
  if(mBusy||!mroot||document.hidden||monPane.style.display==='none')return;mBusy=true;
  try{var j=await api('/api/perf/monitor?since='+MT);MSUP=j.supported!==false;if(MSUP){MM=j.meta||MM;MNOW=j.now||Date.now();MERR=j.err||'';MCL=j.clients||[];var add=0;(j.samples||[]).forEach(function(s){if(s.t>MT){MS.push(s);MT=s.t;add++}});if(MS.length>1800)MS.splice(0,MS.length-1800);monDraw(add?'new':'tick')}else monDraw('tick')}
  catch(e){}
  mBusy=false}
setInterval(monPoll,1000);

function build(){
  root.textContent='';
  if(!SUP)root.appendChild(el('div','rj-tip','Performance chỉ chạy trên Windows (tracker đang chạy trên hệ điều hành khác).'));
  var g=el('div','pf-g');
  var a=box('Giới hạn CPU','#f87171');head(a[1],'cpu','Giới hạn CPU');
  rowNum(a[1],'Tổng CPU cho tất cả Roblox','cpu','total','%',true);rowNum(a[1],'Tối đa mỗi client','cpu','max','%',true);rowNum(a[1],'Kiểm tra lại mỗi','cpu','interval','giây');
  els.cpu=stLine();a[1].appendChild(els.cpu);g.appendChild(a[0]);
  var b=box('Dọn RAM','#5eead4');head(b[1],'ram','Dọn RAM');
  rowNum(b[1],'Chu kỳ dọn','ram','interval','giây');rowNum(b[1],'Chỉ dọn client dùng từ','ram','threshold','MB');rowNum(b[1],'Dọn mỗi đợt','ram','batch','client');rowNum(b[1],'Nghỉ giữa các đợt','ram','delay','ms');
  var bt=el('div','cfg-bar');var rb=el('button','cfg-b p','Dọn RAM ngay');rb.onclick=async function(){rb.disabled=true;try{var j=await api('/api/perf/ram',{method:'POST',body:'{}'});say(j.ok?'Đã giải phóng '+(j.freed||0)+' MB ('+(j.done||0)+'/'+(j.n||0)+' client).':'Lỗi: '+j.msg,j.ok?'':'err');poll()}catch(e){say(e.message,'err')}rb.disabled=false};bt.appendChild(rb);b[1].appendChild(bt);
  els.ram=stLine();b[1].appendChild(els.ram);g.appendChild(b[0]);
  var c=box('Sắp xếp cửa sổ','#a78bfa');head(c[1],'arr','Tự sắp xếp (lặp)');
  rowDir(c[1]);rowNum(c[1],'Tối đa mỗi hàng','arr','perRow','client');rowNum(c[1],'Rộng','arr','w','px');rowNum(c[1],'Cao','arr','h','px');rowNum(c[1],'Khoảng cách ngang','arr','gx','px');rowNum(c[1],'Khoảng cách dọc','arr','gy','px');rowNum(c[1],'Bước xếp chồng','arr','step','px');rowBool(c[1],'Bỏ viền cửa sổ','arr','border');rowBool(c[1],'Thu nhỏ cửa sổ sau khi mở (khi game đã tải xong)','arr','minimize');rowBool(c[1],'Hiện tên account trên mỗi cửa sổ','arr','showName');
  var bt2=el('div','cfg-bar');var ab=el('button','cfg-b p','Kill RBL');ab.onclick=async function(){ab.disabled=true;try{var j=await api('/api/perf/killall',{method:'POST',body:'{}'});say(j.ok?j.msg:'Lỗi: '+j.msg,j.ok?'':'err');poll()}catch(e){say(e.message,'err')}ab.disabled=false};bt2.appendChild(ab);c[1].appendChild(bt2);
  els.arr=stLine();c[1].appendChild(els.arr);c[0].classList.add('pf-wide');g.insertBefore(c[0],g.firstChild);   // Sắp xếp cửa sổ nằm trên, dài hết chiều ngang; CPU + RAM bên dưới 2 ô bằng nhau
  root.appendChild(g);live();
}
function ago(t){if(!t)return '-';var s=Math.max(0,Math.round((Date.now()-t)/1000));return s<60?s+' giây trước':s<3600?Math.round(s/60)+' phút trước':Math.round(s/3600)+' giờ trước'}
function setSt(d,t,c){if(!d)return;d.className='pf-st'+(c?' '+c:'');d.textContent=t}
function live(){
  if(!C||!els.cpu)return;
  var s=ST.cpu;
  if(!C.cpu.enabled)setSt(els.cpu,'Đang tắt.');
  else if(!s)setSt(els.cpu,'Đang chờ lượt kiểm tra đầu…');
  else if(s.error)setSt(els.cpu,'Lỗi: '+s.error,'err');
  else if(!s.n)setSt(els.cpu,'Chưa thấy Roblox nào đang chạy.');
  else setSt(els.cpu,'Đang giới hạn '+s.ok+'/'+s.n+' client · mỗi client '+(+s.cap).toFixed(2)+'% CPU'+(s.fail?' · lỗi '+s.fail+(s.err?' ('+s.err+')':'')+' (thử chạy tracker bằng Administrator)':''),s.fail?'err':'ok');
  var r=ST.ram||{};
  if(r.err)setSt(els.ram,'Lỗi: '+r.err,'err');
  else if(r.last)setSt(els.ram,'Lần cuối '+ago(r.last.at)+': giải phóng '+r.last.freed+' MB ('+r.last.done+'/'+r.last.n+' client) · tổng '+r.total+' MB',r.last.freed>0?'ok':'');
  else setSt(els.ram,C.ram.enabled?'Đang chờ lượt dọn đầu…':'Đang tắt.');
  var a=ST.arr;
  if(!a)setSt(els.arr,C.arr.enabled?'Đang chờ lượt xếp đầu…':'Chưa chạy.');
  else setSt(els.arr,(a.ok?'':'Lỗi: ')+(a.msg||'')+' ('+ago(a.at)+')',a.ok?'ok':'err');
}
async function poll(){
  try{var j=await api('/api/perf/state');ST=j.st||{};SUP=j.supported!==false;if(!C){C=j.cfg;build()}else live();if(j.diag&&j.diag.e&&C)say('Lỗi biên dịch worker: '+j.diag.e,'err')}
  catch(e){if(!C){root.textContent='';say('Không đọc được: '+e.message,'err')}}
}
mel={};monPane.querySelector('#mon_root').appendChild(monBuild());monPoll();
setInterval(function(){if(document.hidden||pane.style.display==='none')return;poll()},4000);
setInterval(function(){if(!started&&pane.style.display!=='none'&&pane.offsetParent!==null){started=true;poll()}},600);
}catch(e){console.error('perf ui',e)}
})();
`;
const RUNS_UI_JS = String.raw`
(function(){
try{
var tabs=$('tabs'),tf=$('tf');
if(!tabs||!tf||document.querySelector('.tab[data-tab="runs"]'))return;
var css=document.createElement('style');
css.textContent='.tabs .tab[data-tab="runs"]::before{content:"\\1F3C6"}.tabs .tab[data-tab="runs"]{order:3}'
 +'.rn-top{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin:12px 0}'
 +'.rn-s{border:1px solid var(--bd);border-radius:14px;padding:12px 16px;background:var(--cd);min-width:0}.rn-s small{color:var(--mu);font-size:12px;letter-spacing:.06em;text-transform:uppercase}.rn-s b{display:block;font-size:30px;line-height:1.15;margin-top:4px}'
 +'.rn-s.sM b{color:#60a5fa;text-shadow:0 0 14px #60a5fa66}.rn-sub{font-size:11.5px;color:var(--mu);margin-top:2px}.rn-s.sW b{color:#4ade80;text-shadow:0 0 14px #4ade8066}.rn-s.sL b{color:#f87171;text-shadow:0 0 14px #f8717166}.rn-s.sR b{color:#fbbf24;text-shadow:0 0 14px #fbbf2466}.rn-s.sT b{color:#e8eaed}'
 +'.rn-g{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:12px}'
 +'.rn-c{position:relative;overflow:hidden;border:1px solid var(--bd);border-radius:16px;min-height:200px;display:flex;flex-direction:column;justify-content:space-between;padding:12px 14px;background-size:cover;background-position:center;background-color:#0b0d12;isolation:isolate}'
 +'.rn-c::before{content:"";position:absolute;inset:0;z-index:-1;background:linear-gradient(180deg,rgba(2,3,8,.15) 0%,rgba(2,3,8,.55) 45%,rgba(2,3,8,.92) 100%)}'
 +'.rn-h{display:flex;justify-content:space-between;align-items:flex-start;gap:8px}.rn-n{font-weight:800;font-size:15px;letter-spacing:.02em;text-shadow:0 1px 6px #000}'
 +'.rn-wl{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}.rn-p{display:inline-flex;align-items:baseline;gap:5px;border:1px solid #ffffff26;border-radius:999px;padding:3px 10px;background:rgba(0,0,0,.45);font-weight:800;font-size:13px}'
 +'.rn-p.pW{color:#4ade80;border-color:#4ade8066}.rn-p.pL{color:#f87171;border-color:#f8717166}.rn-p.pR{color:#fbbf24;border-color:#fbbf2466}.rn-p i{font-style:normal;font-weight:600;font-size:10.5px;opacity:.85;letter-spacing:.05em}'
 +'.rn-m{display:flex;align-items:flex-end;justify-content:space-between;gap:8px;margin-top:10px}.rn-tot b{display:block;font-size:34px;line-height:1;text-shadow:0 2px 10px #000}.rn-tot small{color:#c8ccd4;font-size:11.5px;letter-spacing:.05em;text-transform:uppercase}'
 +'.rn-bar{height:6px;border-radius:999px;background:rgba(255,255,255,.14);overflow:hidden;margin-top:8px}.rn-bar i{display:block;height:100%;background:linear-gradient(90deg,#22c55e,#5eead4)}'
 +'.rn-rod{margin-top:8px;border:1px solid #fbbf2455;border-radius:10px;padding:6px 10px;background:rgba(251,191,36,.08);display:flex;justify-content:space-between;align-items:center;gap:8px;font-size:12px}.rn-rod b{color:#fbbf24;letter-spacing:.06em}'
 +'.rn-ed{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}.rn-ed button{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.18);border-radius:8px;color:#fff;padding:3px 9px;cursor:pointer;font-size:12px;font-weight:700}.rn-ed button:hover{background:rgba(255,255,255,.16)}'
 +'.rn-bar2{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:6px 0 2px}';
document.head.appendChild(css);
var inv=tabs.querySelector('.tab[data-tab="inventory"]');
var btn=document.createElement('button');btn.className='tab';btn.dataset.tab='runs';btn.type='button';btn.title='Total Run';btn.textContent='Total Run';
if(inv)tabs.insertBefore(btn,inv);else tabs.appendChild(btn);
var pane=document.createElement('div');pane.className='tabpane';pane.dataset.pane='runs';pane.id='pane-runs';pane.style.display='none';
pane.innerHTML='<div class="ckhd" style="justify-content:space-between"><div><b class="ckti">🏆 <span class="pf-title">Total Run</span></b></div></div><div class="cfg-msg" id="rn_msg"></div><div id="rn_root"></div>';
tf.appendChild(pane);
var root=pane.querySelector('#rn_root'),msg=pane.querySelector('#rn_msg');
function hdr(){return {'x-api-key':(typeof KEYV!=='undefined'?KEYV:''),'x-requested-with':'dq','content-type':'application/json'}}
async function api(path,opt){var r=await fetch(path,Object.assign({headers:hdr()},opt||{})),j={};try{j=await r.json()}catch(e){}if(!r.ok)throw new Error(j.message||j.error||('HTTP '+r.status));return j}
function el(t,c,x){var e=document.createElement(t);if(c)e.className=c;if(x!=null)e.textContent=x;return e}
var D=[],IE={},TM={ms:0,acc:0},rev=0,edit=false,started=false,busy=false;
var IMGC={},CARDS={},IMGT={};
function trimImg(name,done){   // cắt viền đen/trong suốt và dải chữ tên ở ảnh wiki để ảnh phủ kín ô
  var im=new Image();im.onload=function(){try{
    var sc=Math.min(1,900/im.naturalWidth),w=Math.max(1,Math.round(im.naturalWidth*sc)),h=Math.max(1,Math.round(im.naturalHeight*sc)),cv=document.createElement('canvas');cv.width=w;cv.height=h;var cx=cv.getContext('2d');cx.drawImage(im,0,0,w,h);
    var d=cx.getImageData(0,0,w,h).data,rm=new Float32Array(h),cm=new Float32Array(w);
    for(var y=0;y<h;y++)for(var x=0;x<w;x++){var i=(y*w+x)*4,a=d[i+3]/255,l=(0.299*d[i]+0.587*d[i+1]+0.114*d[i+2])*a;rm[y]+=l/w;cm[x]+=l/h}
    var T=26,y0=0,y1=h-1,x0=0,x1=w-1;while(y0<h&&rm[y0]<T)y0++;while(y1>y0&&rm[y1]<T)y1--;while(x0<w&&cm[x0]<T)x0++;while(x1>x0&&cm[x1]<T)x1--;
    function eS(a,lim){var best=0,bi=0;for(var i=0;i<lim-3;i++){var st=a[i+3]-a[i];if(st>best){best=st;bi=i+2}}return best>5?bi:0}   // mép sắc nét giữa viền tối và ảnh (đầu)
    function eE(a,lim){var n=a.length,best=0,bi=n-1;for(var i=n-1;i>n-lim+3;i--){var st=a[i-3]-a[i];if(st>best){best=st;bi=i-2}}return best>5?bi:n-1}   // (cuối)
    y0=Math.max(y0,eS(rm,Math.round(h*0.3)));y1=Math.min(y1,eE(rm,Math.round(h*0.3)));x0=Math.max(x0,eS(cm,Math.round(w*0.25)));x1=Math.min(x1,eE(cm,Math.round(w*0.25)));
    var mx=Math.round((x1-x0)*0.02),my=Math.round((y1-y0)*0.04);x0+=mx;x1-=mx;y0+=my;
    var cw=x1-x0+1,ch=y1-y0+1;if(cw<w*0.5||ch<h*0.5||cw<20||ch<20){x0=0;y0=0;cw=w;ch=h}
    else{var cut=Math.round(ch*0.09);if(ch-cut>=h*0.45)ch-=cut}   // cắt thêm sát đáy cho chắc để không lộ chữ tên dungeon trong ảnh (tên đã có trên thẻ)
    var o=document.createElement('canvas');o.width=cw;o.height=ch;o.getContext('2d').drawImage(cv,x0,y0,cw,ch,0,0,cw,ch);
    o.toBlob(function(b){done(b?URL.createObjectURL(b):null)},'image/jpeg',0.9)}catch(e){done(null)}};
  im.onerror=function(){done(null)};im.src='/api/runs/img?d='+encodeURIComponent(name)+'&r='+rev}
function paintCard(name){var c=CARDS[name],u=IMGC[name],hu=HUE[name]||[220,60];if(!c)return;
  c.style.backgroundImage=(typeof u==='string'&&u?'url("'+u+'"),':'')+'radial-gradient(120% 90% at 20% 0%,hsla('+hu[0]+','+hu[1]+'%,38%,.55),transparent 70%),linear-gradient(135deg,hsl('+hu[0]+','+hu[1]+'%,14%),hsl('+hu[0]+','+(hu[1]-15)+'%,6%))'}
function paintRodinCard(){   // the Raid Rodin: dung lai anh Northern Lands nhung nhuom do hon de phan biet
  var c=CARDS['Raid Rodin'];if(!c)return;var u=IMGC['Northern Lands'];
  if(typeof u==='string'&&u){
    c.style.backgroundImage='linear-gradient(rgba(190,20,20,.72),rgba(100,8,8,.72)),url("'+u+'"),radial-gradient(120% 90% at 20% 0%,hsla(0,70%,38%,.55),transparent 70%),linear-gradient(135deg,hsl(0,70%,14%),hsl(0,55%,6%))';
    c.style.backgroundBlendMode='color,normal,normal,normal';
  }else{
    c.style.backgroundImage='radial-gradient(120% 90% at 20% 0%,hsla(0,70%,38%,.55),transparent 70%),linear-gradient(135deg,hsl(0,70%,14%),hsl(0,55%,6%))';
    c.style.backgroundBlendMode='';
  }
}
function fT(ms){var m=Math.floor((ms||0)/60000),d=Math.floor(m/1440),h=Math.floor(m%1440/60);m=m%60;return d?d+'d '+h+'h '+m+'m':h?h+'h '+m+'m':m+'m'}
var HUE={'Desert Temple':[38,75],'Winter Outpost':[195,65],'Pirate Island':[185,60],"King's Castle":[8,70],'The Underworld':[285,55],'Samurai Palace':[340,60],'The Canals':[215,45],'Ghastly Harbor':[150,45],'Steampunk Sewers':[28,55],'Boss Raid':[0,70],'Orbital Outpost':[245,65],'Volcanic Chambers':[16,80],'Aquatic Temple':[205,70],'Enchanted Forest':[130,55],'Northern Lands':[200,55],'Gilded Skies':[48,85]};   // nền dự phòng khi chưa tải được ảnh wiki
function rate(w,l){var t=w+l;return t?Math.round(w/t*1000)/10:0}
function pill(c,k,v){var s=el('span','rn-p '+c,String(v));s.appendChild(el('i',0,k));return s}
async function bump(name,f,dv){var d=D.find(function(x){return x.name===name});if(!d)return;var b={dungeon:name};b[f]=Math.max(0,(d[f]||0)+dv);d[f]=b[f];draw();try{await api('/api/runs/set',{method:'POST',body:JSON.stringify(b)})}catch(e){msg.className='cfg-msg err';msg.textContent=e.message}}
function draw(){
  var W=0,L=0;D.forEach(function(d){W+=d.w;L+=d.l});   // rw/rl (Raid Rodin) la tap con cua w/l (moi luot gap Rodin da duoc cong vao w/l roi) -> khong cong them, tranh dem 2 lan
  root.textContent='';
  var top=el('div','rn-top');
  [['sT','Total Run',W+L],['sW','Win',W],['sL','Loss',L],['sR','Win Rate',rate(W,L)+'%'],['sM','Total Run Time',fT(TM.ms),TM.acc>TM.ms?'Cộng dồn các client: '+fT(TM.acc):'']].forEach(function(x){var s=el('div','rn-s '+x[0]);s.appendChild(el('small',0,x[1]));s.appendChild(el('b',0,x[2]));if(x[3])s.appendChild(el('div','rn-sub',x[3]));top.appendChild(s)});
  root.appendChild(top);
  var rs=el('button','cfg-b','Reset');rs.style.margin='0 0 10px';rs.onclick=async function(){rs.disabled=true;try{await api('/api/runs/reset',{method:'POST',body:'{}'});TM={ms:0,acc:0};await load()}catch(e){msg.className='cfg-msg err';msg.textContent=e.message}rs.disabled=false};root.appendChild(rs);
  var ik=Object.keys(IE||{});if(ik.length){var nt=el('div','pf-st err','Chưa tải được ảnh wiki của '+ik.length+' dungeon. Lỗi: '+ik[0]+' → '+IE[ik[0]]);root.appendChild(nt)}
  var g=el('div','rn-g');
  D.forEach(function(d){
    if(d.name==='Unknown'&&!(d.w+d.l+d.rw+d.rl))return;   // chi hien the Unknown khi THAT SU co luot roi vao day (khong doan duoc dungeon); binh thuong an di cho do roi dashboard
    var c=el('div','rn-c');CARDS[d.name]=c;paintCard(d.name);if(IMGC[d.name]===undefined){IMGC[d.name]=null;(function(nm){trimImg(nm,function(u){IMGC[nm]=u||false;if(!u)IMGT[nm]=Date.now();paintCard(nm);if(nm==='Northern Lands')paintRodinCard()})})(d.name)}else if(IMGC[d.name]===false&&Date.now()-(IMGT[d.name]||0)>600000){IMGC[d.name]=undefined}
    var h=el('div','rn-h');h.appendChild(el('div','rn-n',d.name));var wl=el('div','rn-wl');wl.appendChild(pill('pW','WIN',d.w));wl.appendChild(pill('pL','LOSS',d.l));wl.appendChild(pill('pR','RATE',rate(d.w,d.l)+'%'));h.appendChild(wl);c.appendChild(h);
    var bt=el('div');var m=el('div','rn-m');var tt=el('div','rn-tot');tt.appendChild(el('b',0,d.w+d.l));tt.appendChild(el('small',0,'runs'));m.appendChild(tt);bt.appendChild(m);
    var br=el('div','rn-bar');var bi=el('i');bi.style.width=rate(d.w,d.l)+'%';br.appendChild(bi);bt.appendChild(br);
    if(edit){var ed=el('div','rn-ed');[['w','Win'],['l','Loss']].forEach(function(f){var mi=el('button',0,'− '+f[1]);mi.onclick=function(){bump(d.name,f[0],-1)};var pl=el('button',0,'+ '+f[1]);pl.onclick=function(){bump(d.name,f[0],1)};ed.appendChild(mi);ed.appendChild(pl)});bt.appendChild(ed)}
    c.appendChild(bt);g.appendChild(c);
    if(d.name==='Northern Lands'){   // o rieng "Raid Rodin" chen giua Northern Lands va Gilded Skies - dung chung du lieu rw/rl da luu tren entry Northern Lands
      var rc=el('div','rn-c');CARDS['Raid Rodin']=rc;paintRodinCard();
      var rh=el('div','rn-h');rh.appendChild(el('div','rn-n','Raid Rodin'));var rwl=el('div','rn-wl');rwl.appendChild(pill('pW','WIN',d.rw));rwl.appendChild(pill('pL','LOSS',d.rl));rwl.appendChild(pill('pR','RATE',rate(d.rw,d.rl)+'%'));rh.appendChild(rwl);rc.appendChild(rh);
      var rbt=el('div');var rm=el('div','rn-m');var rtt=el('div','rn-tot');rtt.appendChild(el('b',0,d.rw+d.rl));rtt.appendChild(el('small',0,'runs'));rm.appendChild(rtt);rbt.appendChild(rm);
      var rbr=el('div','rn-bar');var rbi=el('i');rbi.style.width=rate(d.rw,d.rl)+'%';rbr.appendChild(rbi);rbt.appendChild(rbr);
      if(edit){var red2=el('div','rn-ed');[['rw','Win'],['rl','Loss']].forEach(function(f){var mi=el('button',0,'− '+f[1]);mi.onclick=function(){bump('Northern Lands',f[0],-1)};var pl=el('button',0,'+ '+f[1]);pl.onclick=function(){bump('Northern Lands',f[0],1)};red2.appendChild(mi);red2.appendChild(pl)});rbt.appendChild(red2)}
      rc.appendChild(rbt);g.appendChild(rc);
    }
  });
  root.appendChild(g);
}
async function load(){if(busy)return;busy=true;try{var j=await api('/api/runs');D=j.dungeons||[];IE=j.imgErr||{};TM=j.time||TM;msg.textContent='';draw()}catch(e){if(!D.length){msg.className='cfg-msg err';msg.textContent='Không đọc được: '+e.message}}busy=false}
setInterval(function(){if(!document.hidden&&pane.style.display!=='none')load()},5000);
setInterval(function(){if(!started&&pane.style.display!=='none'&&pane.offsetParent!==null){started=true;load()}},600);
// Cap nhat NGAY khi co du lieu moi tu server (bao gom vua thang 1 luot), khong doi toi vong 5s dinh ky: trang chinh
// phat su kien 'dq-update' moi lan poll() thanh cong (ca khi SSE bao 'vua co acc bao cao xong'). Don nhieu lan ban
// don lien tiep (nhieu acc cung bao gan nhau) lai thanh 1 lan goi /api/runs, tranh goi API lien tuc qua nhiu.
var runsFastTimer=null;
window.addEventListener('dq-update',function(){
  if(pane.style.display==='none')return;
  if(runsFastTimer)return;
  runsFastTimer=setTimeout(function(){runsFastTimer=null;load()},700);
});
}catch(e){console.error('runs ui',e)}
})();
`;
const I18N_JS = String.raw`
(function(){
try{
var D=[["Tất cả Item","All Items"],["Tất cả độ hiếm","All rarities"],["Tất cả","All"],["Chọn / bỏ tất cả","Select / clear all"],["Chọn tất cả","Select all"],["Bỏ chọn tất cả","Clear all"],["Bỏ chọn hiển thị","Clear visible"],["Bỏ chọn","Deselect"],["Chọn hết","Select all"],["Account nhận được item (rớt từ dungeon, không tính trade)","Accounts that received the item (dungeon drops, trades excluded)"],["Account đang sở hữu","Account that owns it"],["Bị ban (Error Code 600)","Banned (Error Code 600)"],["Bị captcha rejoin (Error Code 773)","Captcha rejoin (Error Code 773)"],["Bỏ cờ","Remove flag"],["Chưa có lịch sử dữ liệu","No history data yet"],["Chưa có màu nào được ghi nhận","No colors recorded yet"],["Chưa có đồ Ult/Legend nào rớt từ map cuối (cần ít nhất 2 lần cập nhật để so sánh)","No Ult/Legend drops from the last map yet (needs at least 2 updates to compare)"],["Chưa ghi nhận item nào","No items recorded yet"],["Chưa nhập key","No key entered"],["Chưa rõ độ hiếm","Unknown rarity"],["Chưa rõ","Unknown"],["Cập nhật","Updated"],["Dữ liệu xuất","Export data"],["Gold tối thiểu","Min gold"],["Gắn cờ:","Flag:"],["Hiện thêm","Show more"],["còn lại","left"],["Hiện đồ đang mang","Show equipped"],["Ẩn đồ đang mang","Hide equipped"],["Không copy tự động được — hãy chọn và copy thủ công.","Auto-copy failed — select and copy manually."],["Không có tài khoản nào","No accounts"],["Không có item","No items"],["Level từ","Level from"],["Level đến","Level to"],["Lỗi gắn cờ","Flag error"],["Lỗi kết nối server","Server connection error"],["Lỗi kết nối tới server","Cannot reach server"],["Lỗi kết nối Roblox","Roblox connection error"],["Lỗi kết nối","Connection error"],["Mất kết nối server","Lost connection to server"],["Mất kết nối","Disconnected"],["Nhập API key của tracker:","Enter the tracker API key:"],["Sai key - bấm Làm mới để nhập lại","Wrong key - press Refresh to re-enter"],["Tìm tên item hoặc tên account...","Search item or account name..."],["Tìm username, ghi chú, item...","Search username, note, item..."],["Tìm trong 6 ô...","Search in the 6 boxes..."],["Tìm account...","Search account..."],["Tìm alt...","Search alt..."],["Xoá account","Delete account"],["Xoá được","Deleted"],["Xoá toàn bộ cookie đang theo dõi (kể cả cookie đã lưu trên server)?","Delete all tracked cookies (including cookies saved on the server)?"],["Xoá toàn bộ lịch sử trade","Delete all trade history"],["Xoá log","Clear log"],["Xoá lỗi:","Delete error:"],["Xoá Party","Delete Party"],["Xoá dữ liệu config của","Delete config data of"],["Đã xoá dữ liệu config của","Deleted config data of"],["Đã xoá lịch sử trade","Trade history deleted"],["Đã xoá:","Deleted:"],["Đã xoá","Deleted"],["Đã bỏ cờ","Flag removed"],["Đã copy","Copied"],["Đã gắn cờ","Flagged"],["Đóng","Close"],["Đang mang","Equipped"],["ĐANG MANG","EQUIPPED"],["Xuất","Export"],["Lọc chỉ số","Filter stats"],["Cột","Columns"],["Làm mới","Refresh"],["Màu độ hiếm","Rarity colors"],["Chú thích màu độ hiếm","Rarity color legend"],["Tất cả item","All items"],["Chọn tất cả","Select all"],["Thu gọn / mở rộng menu","Collapse / expand menu"],["Mở rộng menu","Expand menu"],["Thu gọn menu","Collapse menu"],["Cookie còn sống","Cookie alive"],["Từ","From"],["Đến","To"],[" ngày trước"," d ago"],[" tiếng trước"," h ago"],[" giờ trước"," h ago"],[" phút trước"," min ago"],[" giây trước"," s ago"],["d trước","d ago"],["h trước","h ago"],["p trước","m ago"],["s trước","s ago"],[" điểm dữ liệu"," data points"],[" dòng)"," rows)"],["Level cao nhất","Highest level"],["Sở hữu","Owned"],["Trả đồ","Return items"],["Chưa có lệnh trade nào","No trade orders yet"],["Chưa có lệnh trade. Chọn acc rồi bấm Xác nhận trade để xem tại đây.","No trade orders yet. Pick accounts and press Confirm trade to see them here."],["Chưa có lệnh bán. Chọn item rồi bấm Bán ngay.","No sell orders yet. Pick items and press Sell now."],["Chưa có acc online (ngoài main)","No online accounts (besides main)"],["Chưa có account","No accounts yet"],["Chọn Main Account (nút bên phải) để xem inventory của main.","Choose a Main Account (button on the right) to view its inventory."],["Main chưa có trong tracker nên chưa có inventory.","Main is not in the tracker, so no inventory yet."],["Main không có item nào (hoặc bị bộ lọc ẩn).","Main has no items (or they are hidden by the filter)."],["Account đang sở hữu","Owning account"],["Lọc chỉ số","Filter stats"],["Level từ","Level from"],["Level đến","Level to"],["Gold tối thiểu","Min gold"],["Gửi lệnh","Send order"],["Gửi trade","Send trade"],["Gửi ngay","Send now"],["Chạy thử (DRY): chỉ thêm 1 item, KHÔNG xác nhận","Dry run: add only 1 item, do NOT confirm"],["Chạy thử (DRY)","Dry run"],["Chỉ gửi gold (bỏ qua item)","Gold only (skip items)"],["Độ hiếm cần gửi","Rarities to send"],["Độ hiếm","Rarity"],["Loại item","Item type"],["Áo","Chest"],["Trade về acc chính","Trade to main account"],["Trade item đã chọn chỉ áp dụng cho 1 acc.","Trading selected items works for 1 account only."],["Trade item đã chọn","Trade selected items"],["Trade theo độ hiếm…","Trade by rarity…"],["Trade theo độ hiếm","Trade by rarity"],["Theo độ hiếm","By rarity"],["Cookie sai định dạng","Invalid cookie format"],["Acc chính (main) — phải cùng server và đang chạy script tracker","Main account — must be on the same server and running the tracker script"],["Chỉ gửi các tên này (tuỳ chọn, cách nhau bằng dấu phẩy; để trống = mọi item đúng độ hiếm)","Only send these names (optional, comma separated; empty = every item of that rarity)"],["Gold gửi mỗi acc (tuỳ chọn, ví dụ 500m, 1b, 1.5t)","Gold to send per account (optional, e.g. 500m, 1b, 1.5t)"],["Để trống = không gửi gold","Empty = do not send gold"],["Tối đa item mỗi acc","Max items per account"],["Tick: tự động trade HẾT gold trong tài khoản cùng với item","Tick: also trade ALL gold in the account together with items"],["Chỉ thêm 1 item vào cửa sổ trade, không bấm xác nhận","Adds only 1 item to the trade window, no confirm"],["Tên acc chính","Main account name"],["Account (clone/alt)","Account (clone/alt)"],["Account (online)","Account (online)"],["Item (chưa mang)","Item (not equipped)"],["Trạng thái","Status"],["Sẽ nhận","Will receive"],["Ví dụ: 1b, 100b, 1t","e.g. 1b, 100b, 1t"],["Main account","Main account"],["Lịch sử trade","Trade history"],["Console bán đồ","Sell console"],["Console trade","Trade console"],["Console mua","Buy console"],["Bán ngay","Sell now"],["Xác nhận trade","Confirm trade"],["Khoá Legend/Ult: BẬT","Lock Legend/Ult: ON"],["Khoá Legend/Ult: TẮT","Lock Legend/Ult: OFF"],["Tổng gold đã trade vào main:","Total gold traded to main:"],["Tổng gold đã trade:","Total gold traded:"],["Dừng","Stop"],["✔ Sẽ bán","✔ Will sell"],["✔ Sẽ trade","✔ Will trade"],[" sẽ gửi "," will send "],[" từ "," from "],[" loại item"," item types"],[" loại"," types"],[" acc đang giữ"," accounts holding"],[" acc gửi:"," sending accounts:"],[" acc về main"," accounts to main"],[" alt còn lại"," alts left"],[" alt đang offline sẽ không nhận được trade."," offline alts will not receive trades."],[" alt đầu tiên trong danh sách được nhận."," first alt in the list receives."],[" item trả lại trong cùng phiên"," items returned in the same session"],[" item đã chọn"," items selected"],[" item · đã chọn"," items · selected"],[" item nên chỉ"," items so only"],[" nào rớt"," dropped"],[" · bấm nhiều lần để tăng số lượng"," · click repeatedly to raise the amount"],[" · bấm để chọn bán"," · click to pick for selling"],[" · bấm để chọn trade"," · click to pick for trading"],[" (bấm để xem số đầy đủ)"," (click to see full number)"],[" (tự động)"," (automatic)"],[" · chia đều:"," · split evenly:"],[" · không trade được (đang mang / loại không hỗ trợ)"," · cannot trade (equipped / unsupported type)"],[" · kèm main trả lại tối đa"," · main returns at most"],[" · lỗi:"," · error:"],[" · main trả"," · main returns"],[" · tối đa"," · max"],[" · đang khoá (tắt nút Khoá Legend/Ult để bán)"," · locked (turn off the Lock Legend/Ult button to sell)"],[" · đang mang (không bán được — bật nút Hiện đồ đang mang chỉ để xem)"," · equipped (cannot be sold — the Show equipped button is view-only)"],[" · đã chọn"," · selected"],[" để trả"," to return"],[" + all gold của"," + all gold of"],["Chưa có ","No "],["Acc này không có item nào để trade","This account has no items to trade"],["Bấm vào item để chọn trade","Click an item to pick it for trading"],["Bỏ qua","Skipped"],["Chưa chọn acc nào.","No account selected."],["Chưa chọn item nào.","No item selected."],["Chưa chọn item nào","No item selected"],["Chưa có dữ liệu REQ LV của item. Hãy chạy lại script Lua mới trên các acc rồi chờ tracker cập nhật.","No REQ LV data for items yet. Re-run the new Lua script on the accounts and wait for the tracker to update."],["Chưa hoàn thành (quá 5s)","Not finished (over 5s)"],["Chưa hoàn thành: acc không nhận lệnh trong 5s","Not finished: account did not receive the order within 5s"],["Chưa pick item nào trong inventory của Main","No item picked in Main's inventory"],["Chưa thấy acc này trong tracker. Main cần chạy script tracker thì mới tự nhận trade.","This account is not in the tracker yet. Main must run the tracker script to accept trades."],["Chưa đăng nhập hoặc sai key","Not signed in or wrong key"],["Chỉ có","Only"],["Chọn Main Account trước","Choose a Main Account first"],["Chọn ít nhất một độ hiếm (Ult / Legend / Epic).","Pick at least one rarity (Ult / Legend / Epic)."],["Chọn ít nhất một độ hiếm.","Pick at least one rarity."],["Chờ acc nhận lệnh","Waiting for the account to take the order"],["Gold không hợp lệ. Ví dụ: 500m, 1b, 1.5t.","Invalid gold. Example: 500m, 1b, 1.5t."],["Hoàn tất","Done"],["Hết hạn: acc không báo kết quả","Expired: account did not report a result"],["Hết hạn: acc không nhận lệnh (script có đang chạy không?)","Expired: account did not take the order (is the script running?)"],["Hết hạn: acc không nhận/không báo kết quả (script Lua có đang chạy không?)","Expired: account did not take/report the order (is the Lua script running?)"],["Hết hạn: bị thay bằng lệnh mới","Expired: replaced by a new order"],["Hết hạn","Expired"],["Không có account nào","No accounts"],["Không có item nào trong khoảng REQ LV này","No items in this REQ LV range"],["Không có item để chia","No items to split"],["Không tìm thấy account này.","Account not found."],["Loại mua không hợp lệ.","Invalid buy type."],["Lệnh trade quá 150s chưa xong -> huỷ để chuyển alt kế tiếp","Trade order over 150s unfinished -> cancelled to move to the next alt"],["Lọc loại item:","Item type filter:"],["Lọc độ hiếm:","Rarity filter:"],["Lỗi gửi lệnh:","Order error:"],["Lỗi tạo lệnh bán","Failed to create sell order"],["Lỗi xoá lịch sử","Failed to delete history"],["Lỗi quét tool:","Tool scan error:"],["Main chưa có trong tracker — vẫn trade được nếu main cùng server","Main is not in the tracker — trade still works if main is on the same server"],["Nhập hoặc chọn Main Account trước","Enter or choose a Main Account first"],["Nhập số gold cần gửi.","Enter the amount of gold to send."],["Nhập tên acc chính.","Enter the main account name."],["Nhập tên acc chính","Enter the main account name"],["Nhập tên acc nhận item (hoặc chọn bằng nút Main Account)","Enter the receiving account name (or use the Main Account button)"],["Server nhận lệnh cho","Server accepted the order for"],["Tạo lúc","Created at"],["Tắt khoá? Bạn sẽ có thể chọn bán cả item Legend/Ult.","Turn off the lock? You will be able to sell Legend/Ult items."],["Tên Main không hợp lệ (3–20 ký tự: chữ, số, _)","Invalid Main name (3–20 chars: letters, digits, _)"],["Tên acc chính không hợp lệ (3–20 ký tự chữ, số, _).","Invalid main account name (3–20 chars: letters, digits, _)."],["Tên không hợp lệ (3–20 ký tự: chữ, số, _)","Invalid name (3–20 chars: letters, digits, _)"],["Tính năng trade chỉ dành cho admin.","Trading is for admins only."],["Tối đa 999 item mỗi acc trong 1 lệnh","Max 999 items per account per order"],["Tối đa 999 item mỗi lệnh","Max 999 items per order"],["Tự động (Config)","Automatic (Config)"],["Tick “Trade” ở từng item (loại được phép:","Tick “Trade” on each item (allowed types:"],["acc gửi đang nhận đồ từ acc khác","sending account is receiving items from another account"],["acc nhận đang gửi đồ cho acc khác","receiving account is sending items to another account"],["chỉ gửi gold","gold only"],["không thấy acc","account not found"],["toàn bộ gold","all gold"],["trùng acc chính","same as main account"],["tối đa","max"],["Đang bán","Selling"],["Đang chuyển alt sang server của main...","Moving alt to main's server..."],["Đang mua","Buying"],["Đang trade","Trading"],["Đã chọn","Selected"],["Đã dừng hàng đợi trade cho alt","Stopped the trade queue for the alt"],["Đã xong hàng đợi trade cho alt","Finished the trade queue for the alt"],["Đã gửi lệnh bán cho","Sent sell order to"],["Đã gửi lệnh cho","Sent order to"],["Đã gửi","Sent"],["Đã huỷ từ web","Cancelled from the web"],["Đã huỷ lệnh","Order cancelled"],["Đã huỷ","Cancelled"],["Đã tải lại lịch sử","History reloaded"],["Đã xoá: acc không thực hiện lệnh sau","Removed: account did not carry out the order after"],["đang có lệnh khác","another order in progress"],["độ hiếm","rarity"],["Thành công","Success"],["Lỗi","Error"],["Huỷ","Cancel"],["Chờ kiểm tra","Pending check"],["Chưa có cookie","No cookie"],["Chưa thấy trong tracker","Not seen in tracker"],["Chỉ localhost hoặc admin mới dùng được quét tool","Only localhost or admin can use the tool scan"],["Dead (logout / hết hạn)","Dead (logged out / expired)"],["Khác","Other"],["Không có trong tracker","Not in tracker"],["Roblox giới hạn tốc độ","Roblox rate limited"],["Server chưa đặt COOKIE_SECRET nên chưa lưu cookie được","Server has no COOKIE_SECRET, so cookies cannot be saved"],["Thêm & theo dõi liên tục","Add & track continuously"],["Thêm & theo dõi","Add & track"],["Thêm cookie","Add cookie"],["Thêm các account đang online (tracker thấy) chưa có party","Add online accounts (seen by tracker) that have no party"],["Thêm các account đã tích vào party","Add the ticked accounts to the party"],["Thêm các account đã tích vào whitelist","Add the ticked accounts to the whitelist"],["Thêm ","Add "],["Đang nhận diện","Identifying"],["Đã thêm","Added"],["đang kiểm tra…","checking…"],["Đang kiểm tra…","Checking…"],["Đang kiểm tra","Checking"],["account cần chú ý:","accounts need attention:"],["cookie không nhận diện được hoặc acc chưa có trong tracker (cookie chết/sai cũng rơi vào đây).","cookies not recognised or accounts not in the tracker (dead/wrong cookies also land here)."],["cookie lỗi/bị Roblox giới hạn, thử lại sau.","cookies failed / rate limited by Roblox, try again later."],["dòng bị bỏ qua (cần cookie hoặc tenaccount:cookie, tối đa","lines skipped (need a cookie or accountname:cookie, max"],["· Cờ:","· Flag:"],["· 🔒 đã lưu cookie","· 🔒 cookie saved"],["Bỏ theo dõi","Stop tracking"],["Check lần cuối:","Last check:"],["Check lại sau:","Next check in:"],["Chu kỳ check","Check interval"],["1 phút (mặc định)","1 minute (default)"],["2 phút","2 minutes"],["phút","min"],["Chưa có account nào. Dán cookie vào ô phía trên rồi bấm \"Thêm & theo dõi liên tục\".","No accounts yet. Paste cookies above and press \"Add & track continuously\"."],["Chưa có account nào. Dán cookie vào ô Thêm cookie.","No accounts yet. Paste cookies into the Add cookie box."],[" - các account đang khác nhau, đây là giá trị phổ biến nhất"," - accounts differ, this is the most common value"],[" - đang hiện "," - showing "],[" Lỗi ở:"," Error in:"],[" account vào Party"," accounts into Party"],[" account đang online vào Party"," online accounts into Party"],[" file thay đổi)."," files changed)."],[" item trong danh sách (dữ liệu wiki + kho các account)"," items in the list (wiki data + account inventories)"],[" item đang được bảo vệ -"," items protected -"],[" mục) cho"," entries) for"],["(trống)","(empty)"],["(đã ẩn - nhập mới để đổi)","(hidden - type a new one to change)"],["Bấm vào ô username, tích chọn tên account rồi bấm ✓.","Click the username box, tick account names, then press ✓."],["Bỏ các thay đổi chưa lưu?","Discard unsaved changes?"],["Bỏ khỏi party","Remove from party"],["Carry: tự chơi dungeon bằng skill của nó","Carry: plays the dungeon by itself with its own skills"],["Chưa có friend nào","No friends yet"],["Chưa nối workspace. Dán link thư mục workspace (hoặc thư mục configs) rồi bấm \"Lưu link\".","Workspace not linked. Paste the workspace (or configs) folder link, then press \"Save link\"."],["Chỉ giữ leader (party một mình)","Keep leader only (solo party)"],["Dán link workspace trước.","Paste the workspace link first."],["Không có account nào trong tracker (hoặc đã nằm trong party).","No accounts in the tracker (or already in a party)."],["Không có account nào trong tracker (hoặc đã nằm trong whitelist).","No accounts in the tracker (or already in the whitelist)."],["Không có account online nào chưa nằm trong party.","No online accounts outside a party."],["Không có file config alt nào để xoá.","No alt config files to delete."],["Không có key nào của ô này trong file.","No keys for this box in the file."],["Không có mục khớp.","No matching entries."],["Không đọc được config:","Cannot read config:"],["Không đọc được:","Cannot read:"],["Leader (vào dungeon trước, bấm queue)","Leader (enters the dungeon first, presses queue)"],["Leader không thể là Leech - chọn leader khác trước.","A Leader cannot be a Leech - pick another leader first."],["Leech: alt đứng chờ ăn XP","Leech: alt waits to gain XP"],["Link workspace lỗi:","Workspace link error:"],["Lưu link","Save link"],["Lưu lỗi:","Save error:"],["Thêm","Add"],["chưa thấy trong tracker","not seen in tracker"],["tất cả","all"],["Đã lưu toàn bộ config (","Saved all config ("],["Đã lưu","Saved"],["Auto Rejoin đang tắt","Auto Rejoin is off"],["Account đang theo dõi","Tracked accounts"],["Bị kick","Kicked"],["Bỏ khỏi tool Rejoin (cookie vẫn còn trong tab Cookie)","Remove from Rejoin tool (cookie stays in the Cookie tab)"],["Cấu hình đang thuộc tài khoản khác. Bật/tắt hoặc đổi một ô ở đây để chạy cho account của bạn.","This config belongs to another account. Toggle or change a field here to run it for your account."],["Delay mở Roblox","Roblox open delay"],["Dán cookie .ROBLOSECURITY, mỗi dòng một cookie. Tracker lấy tên account, lưu cookie (dùng chung với tab Cookie) rồi theo dõi: account bị kick, mất kết nối, văng game hoặc không lên script thì tự mở lại game.","Paste .ROBLOSECURITY cookies, one per line. The tracker reads the account name, saves the cookie (shared with the Cookie tab), then watches: if an account is kicked, disconnects, leaves the game or the script does not come up, it reopens the game."],["Hết thời gian chờ (Roblox không trả lời)","Timed out (Roblox is not responding)"],["Hết thời gian chờ","Timed out"],["Không gọi được tracker","Cannot reach the tracker"],["Không trong game","Not in game"],["Leader để follow","Leader to follow"],["Mở cùng lúc","Open at once"],["Mở game","Open game"],["Mở nhiều client cùng lúc","Open many clients at once"],["Trong game, script chưa báo","In game, script has not reported"],["Vào lại bằng","Rejoin by"],["Tắt","Off"],["Đang bật","On"],["Đang chạy","Running"],["Captcha rejoin","Captcha rejoin"],["Mất kết nối","Disconnected"],["tên Roblox của leader","leader's Roblox name"],["giây","sec"],["lỗi","error"],[" · mở lại "," · reopened "],["(chưa có)","(none)"],[" (đã thêm "," (added "],["Rejoin ngay","Rejoin now"],["Bước xếp chồng","Cascade step"],["Bỏ viền cửa sổ","Borderless windows"],["Chu kỳ dọn","Clean interval"],["Chưa chạy.","Not run yet."],["Chưa thấy Roblox nào đang chạy.","No Roblox running yet."],["Chỉ dọn client dùng từ","Only clean clients using more than"],["Cột bên phải","Right column"],["Cột bên trái","Left column"],["Dọn RAM ngay","Clean RAM now"],["Dọn RAM","Clean RAM"],["Dọn mỗi đợt","Per batch"],["Giới hạn CPU","CPU limit"],["Hiện tên account trên mỗi cửa sổ","Show account name on each window"],["Khoảng cách dọc","Vertical gap"],["Khoảng cách ngang","Horizontal gap"],["Kiểm tra lại mỗi","Re-check every"],["Kiểu sắp xếp","Layout style"],["Lưới: Dưới-Trái → Trên-Phải","Grid: Bottom-Left → Top-Right"],["Lưới: Phải → Trái","Grid: Right → Left"],["Lưới: Trái → Phải","Grid: Left → Right"],["Xếp chồng: Trên-Phải → Dưới-Trái","Cascade: Top-Right → Bottom-Left"],["Xếp chồng: Trên-Trái → Dưới-Phải","Cascade: Top-Left → Bottom-Right"],["Lần cuối","Last"],["Mỗi client Roblox bị cap CPU tối đa (chia đều từ tổng). Giúp mở được nhiều client mà máy không đơ.","Each Roblox client is capped at a maximum CPU share (split evenly from the total). Lets you run many clients without the PC freezing."],["Nghỉ giữa các đợt","Pause between batches"],["Performance chỉ chạy trên Windows (tracker đang chạy trên hệ điều hành khác).","Performance only runs on Windows (the tracker is running on another OS)."],["Rộng","Width"],["Cao","Height"],["Sắp xếp cửa sổ","Arrange windows"],["Sắp xếp ngay","Arrange now"],["Thu gọn bộ nhớ làm việc (working set) của từng client Roblox định kỳ, không đóng client.","Periodically trims the working set of each Roblox client without closing it."],["Thu nhỏ cửa sổ sau khi mở (khi game đã tải xong)","Minimize window after opening (once the game has loaded)"],["Tối đa mỗi client","Max per client"],["Tối đa mỗi hàng","Max per row"],["Tổng CPU cho tất cả Roblox","Total CPU for all Roblox"],["Tự sắp xếp (lặp)","Auto arrange (loop)"],["Xếp các cửa sổ Roblox gọn trên màn hình. Rộng/Cao = 0 là tự vừa màn hình. Thu nhỏ và hiện tên chỉ áp dụng cho client do Auto Rejoin mở; nếu executor không gắn (attach) được thì tắt \"Hiện tên account\".","Tidily arranges the Roblox windows on screen. Width/Height = 0 fits the screen automatically. Minimize and show-name only apply to clients opened by Auto Rejoin; if your executor cannot attach, turn off \"Show account name\"."],["Xếp lại mỗi","Re-arrange every"],["Đang chờ lượt dọn đầu…","Waiting for the first clean…"],["Đang chờ lượt kiểm tra đầu…","Waiting for the first check…"],["Đang chờ lượt xếp đầu…","Waiting for the first arrange…"],["Đang giới hạn","Limiting"],["Đang tắt.","Off."],["Đã giải phóng","Freed"],[": giải phóng",": freed"],[" (thử chạy tracker bằng Administrator)"," (try running the tracker as Administrator)"],[" client · mỗi client"," clients · each client"],[" client) · tổng"," clients) · total"],[" · tắt"," · off"],[" · đang bật"," · on"],[" · lỗi "," · error "],["client","client"],["Dang trong game (script chua bao) - khong dong / khong mo lai","In game (script has not reported) - not closing / not reopening"],["Chua hoi duoc Roblox - tam thoi khong mo lai","Could not ask Roblox - not reopening for now"],["Dang trong game (script da tung chay, tam im) - khong dong / khong mo lai","In game (the script has run before and is just quiet for now) - not closing / not reopening"],["Dang trong game (script chua kip bao lan dau) - khong dong / khong mo lai","In game (the script has not had time to report yet) - not closing / not reopening"],["Script khong len sau khi mo (chua bao cao lan nao) - dong va mo lai.","The script never came up after opening (no report at all) - closing and reopening."],["Trong game nhung script khong bao qua ","In game but the script has not reported for "],[" phut (co the executor loi auto-exec) - dong va mo lai."," minutes (the executor's auto-exec may have failed) - closing and reopening."],["Khong goi duoc Roblox (timeout / mang)","Cannot reach Roblox (timeout / network)"],["Khong goi duoc Roblox","Cannot reach Roblox"],["Cookie het han / sai","Cookie expired / wrong"],["Roblox khoa tai khoan (lock)","Roblox locked the account (lock)"],["Roblox ban / gioi han (","Roblox ban / limit ("],["Roblox tra ","Roblox returned "],["Khong lay duoc CSRF (cookie het han?)","Could not get CSRF (cookie expired?)"],["Khong co cookie cua ","No cookie for "],["Khong tim thay RobloxPlayerBeta.exe - dien duong dan trong cau hinh.","RobloxPlayerBeta.exe not found - set the path in the settings."],["Khong tim thay RobloxPlayerBeta.exe. Cai Roblox ban thuong (khong phai Microsoft Store) hoac dien duong dan .exe.","RobloxPlayerBeta.exe not found. Install the normal Roblox (not the Microsoft Store one) or set the .exe path."],["Khong mo duoc Roblox: ","Could not open Roblox: "],["Da mo Roblox (","Opened Roblox ("],["da kill client cu (script khong chay)","killed the old client (script not running)"],["da kill client cu","killed the old client"],["da them.","added."],["Da them.","Added."],["Da luu cau hinh (BAT).","Config saved (ON)."],["Da luu cau hinh (TAT).","Config saved (OFF)."],["bam tay","manual"],["LOI - ","ERROR - "],["Loi: ","Error: "],["Performance chi chay tren Windows.","Performance only runs on Windows."],["Khong mo duoc tien trinh nen","Could not start the background process"],["Khong gui duoc lenh","Could not send the command"],["Het thoi gian cho / tien trinh nen dung","Timed out / background process stopped"],["Khong thay cua so Roblox nao.","No Roblox window found."],["Khong doc duoc vung man hinh.","Could not read the screen area."],["Da xep ","Arranged "],[" cua so Roblox khong chiu doi kich thuoc (chi di chuyen duoc)."," Roblox windows refuse to resize (can only be moved)."],[" cua so."," windows."],["Performance chi dung duoc khi mo tracker tren chinh may choi (localhost) bang key admin.","Performance only works when the tracker is opened on the gaming PC itself (localhost) with the admin key."],["Khong thay file release_*.json / *_shared.json trong link nay. Dan link thu muc workspace\\HarvardLabs (hoac thu muc configs).","No release_*.json / *_shared.json files found in this link. Paste the workspace\\HarvardLabs folder link (or the configs folder)."],["Thieu xac nhan.","Missing confirmation."],["Du lieu khong hop le.","Invalid data."],["Huong khong hop le.","Invalid direction."],["Chưa đăng nhập","Not signed in"],["Cookie tự lưu, chỉ mất khi bấm Xoá all hoặc ✕","Cookies are saved automatically; they are only lost when you press Delete all or ✕"],["Xoá all","Delete all"],["Kiểm tra ngay","Check now"],["Tự động lưu cookie (file .txt thô, không mã hoá)","Auto-save cookies (plain .txt file, not encrypted)"],["Da kill client cu (script khong chay)","Killed the old client (script not running)"],["Da kill client cu","Killed the old client"],
["Tất cả trạng thái","All statuses"],["Chưa có lịch sử trade","No trade history yet"],["Nhập tên acc Main nhận item","Enter the Main account name to receive items"],["Chọn acc alt sẽ trade HẾT gold về main","Select an alt account to trade ALL gold to main"],["Mũ","Helmet"],["REQ LV từ","REQ LV from"],["REQ LV đến","REQ LV to"],["Thời gian","Time"],["Gửi → Nhận","Sent → Received"],["Nội dung","Content"],["Ghi chú","Note"],["Xoá lịch sử","Clear history"],["Xoá bộ lọc","Clear filters"],["Xoá","Clear"],["Account mở bằng bản","Account opened with version"],["Chưa cài Roblox","Roblox not installed"],["Chưa biết","Unknown"],["Client đang chạy","Running clients"],["Chưa có","None"],["Chọn bản để mở","Choose a version to open"],["Mặc định","Default"],["Bản mới nhất đã cài","Latest installed version"],["Bản Roblox có file mới nhất trên máy (cách tool vẫn dùng từ trước)","The Roblox version with the newest files on this machine (as the tool has always used)"],["Bản mới nhất của Roblox","Latest Roblox version"],["Bấm Kiểm tra để biết bản mới nhất","Click Check to see the latest version"],["Bản khác","Another version"],["Chọn một bản Roblox còn trên máy (thư mục Versions)","Choose a Roblox version still on this machine (Versions folder)"],["— chọn bản đã cài —","— choose an installed version —"],["Kiểm tra bản mới","Check for new version"],["Roblox tự xoá bản cũ khi cập nhật, nên chỉ chọn được bản còn trong thư mục Versions. Đổi bản chỉ áp dụng cho lần mở / rejoin kế tiếp, client đang chạy giữ nguyên.","Roblox automatically deletes old versions when it updates, so only versions still in the Versions folder can be selected. Changing the version only applies to the next open/rejoin — a client that's already running stays as is."],["Reset số lần","Reset count"],["Không có account online nào chưa nằm trong party.","No online account is outside a party."],["Tìm item","Search item"],["Chỉ dùng được khi tracker chạy trên Windows.","Only works when the tracker is running on Windows."],["Tự điền","Custom path"],["Bản bạn chọn","The version you picked"],["Đã cài trên máy này","Installed on this machine"],["Chưa cài trên máy này (Roblox sẽ tự cài khi bạn mở bằng app Roblox thường)","Not installed on this machine (Roblox will install it automatically when you open with the regular Roblox app)"],["Đang đọc…","Loading…"],
["Trang bị","Equipment"],["Cookie tự lưu, chỉ mất khi bấm Xoá tất cả hoặc ✕","Cookies are saved automatically; they are only lost when you press Clear all or ✕"],["Khoá không cho chọn item Legend/Ult để tránh bán nhầm","Lock Legend/Ult items to prevent selling them by mistake"],["Ẩn các item đang mang (⚔) khỏi danh sách trade","Hide equipped items (⚔) from the trade list"],["Ẩn các item đang mang (⚔) khỏi danh sách bán","Hide equipped items (⚔) from the sell list"],["Không có account online nào","No account is online"],["Loại","Type"],
["; item đang mang không trade được).","; equipped items cannot be traded)."],["(không có trong danh sách)","(not in the list)"],[", dùng Search/bộ lọc để thu hẹp",", use Search/filter to narrow it down"],["Đã lưu config (","Saved config ("],[" mục) riêng cho account này."," entries) just for this account."],["Nhập tên account (3-20 ký tự chữ/số).","Enter the account name (3-20 letters/digits)."],["Account này đã có trong 1 party rồi.","This account is already in a party."],["Bắt TẤT CẢ account trong các party thoát dungeon, về Lobby ngay lập tức (cần script tracker mới nhất)","Force ALL accounts in the parties to leave the dungeon, back to Lobby immediately (requires the latest tracker script)"],["Bạn đang có thay đổi chưa lưu - Save hoặc tải lại trước khi đổi account.","You have unsaved changes - Save or reload before switching accounts."],[" (file vẫn còn; bản gốc lưu tại "," (the file is still there; the original is kept at "],["KHÔNG ghi được file nhớ link (","Could NOT write the saved-link file ("],[") - link chỉ được nhớ trong trình duyệt này.",") - the link will only be remembered in this browser."],["Delete config (TẤT CẢ)","Delete config (ALL)"],["Áp dụng cho","Apply to"],["Save (account này)","Save (this account)"],["Harvard bị đứng - đang mở lại","Harvard is stuck - reopening"],["Đang dùng đường dẫn Roblox tự điền: ","Using the custom Roblox path: "],[" — mục chọn bản bên dưới bị bỏ qua."," — the version choice below is ignored."],["Dùng ","Use "],[" nếu đã cài, chưa cài thì dùng bản mới nhất đã cài"," if installed, otherwise falls back to the latest installed version"],[" · (không còn trên máy)"," · (no longer on this machine)"],["Bật lại auto rejoin cho account này","Turn Auto Rejoin back on for this account"],["Tạm tắt auto rejoin cho account này (vẫn còn trong danh sách)","Temporarily turn off Auto Rejoin for this account (it stays in the list)"],["Bật","On"],["Số lần Rejoin","Rejoin count"],["Chưa có account nào trong các party.","No accounts in any party yet."],["Đã gửi lệnh về Lobby cho ","Sent the Lobby command to "],[" account - script sẽ về Lobby trong vài giây."," account(s) - the script will return to Lobby within a few seconds."],["Cộng dồn các client: ","Combined across clients: "],["Chưa tải được ảnh wiki của ","Could not load the wiki image for "],["Tốc độ chạy (key altFollow trong file config)","Run speed (altFollow key in the config file)"],["Chạy nhanh","Speed run"],["Lỗi gửi lệnh Force lobby: ","Error sending the Force lobby command: "],["Nhập số Gold như 1b, 100b hoặc 1t.","Enter a Gold amount like 1b, 100b or 1t."],["Phát hiện Harvard bị đứng (tự đóng+mở lại)","Detect Harvard being stuck (auto close+reopen)"],["Đã tạm tắt acc này","Temporarily turned off for this account"],["Chuyển sang giao diện nhẹ (nền đen, tắt hiệu ứng mờ/bóng) để đỡ tốn tài nguyên máy","Switch to a lightweight UI (black background, no blur/shadow effects) to save system resources"]];
var map={},keys=[];
D.forEach(function(p){if(!Object.prototype.hasOwnProperty.call(map,p[0])){map[p[0]]=p[1];keys.push(p[0])}});
keys.sort(function(a,b){return b.length-a.length});
var re=new RegExp(keys.map(function(k){return k.replace(/[.*+?^\x24{}()|[\]\\]/g,'\\$&')}).join('|'),'g');
var cache=new Map();
function tr(t){
  if(!t)return t;var c=cache.get(t);if(c!==undefined)return c;
  var r=t.replace(re,function(m){return map[m]});
  if(cache.size>6000)cache.clear();cache.set(t,r);return r;
}
var lang='vi';try{lang=localStorage.getItem('dq_lang')==='en'?'en':'vi'}catch(e){}
var origT=new Map(),ATT=['title','placeholder','aria-label'],obs=null;
function skip(n){var p=n.parentNode;if(!p)return true;var g=p.nodeName;return g==='SCRIPT'||g==='STYLE'||g==='TEXTAREA'}
function doText(n){
  if(skip(n))return;var t=n.data,r=tr(t);
  if(r!==t){origT.set(n,t);n.data=r}
}
function doAttr(e){
  if(!e||e.nodeType!==1)return;
  for(var i=0;i<ATT.length;i++){
    var a=ATT[i],v=e.getAttribute(a);if(!v)continue;
    var r=tr(v);
    if(r!==v){if(!e.__vi)e.__vi={};e.__vi[a]=v;e.setAttribute(a,r)}
  }
}
function walk(root){
  if(!root)return;
  if(root.nodeType===3){doText(root);return}
  if(root.nodeType!==1&&root.nodeType!==11)return;
  if(root.nodeType===1){if(root.nodeName==='SCRIPT'||root.nodeName==='STYLE')return;doAttr(root)}
  var w=document.createTreeWalker(root,NodeFilter.SHOW_ELEMENT|NodeFilter.SHOW_TEXT,null),n;
  while((n=w.nextNode())){if(n.nodeType===3)doText(n);else if(n.nodeName!=='SCRIPT'&&n.nodeName!=='STYLE')doAttr(n)}
}
function onMut(list){
  if(lang!=='en')return;
  for(var i=0;i<list.length;i++){
    var m=list[i];
    if(m.type==='childList'){for(var j=0;j<m.addedNodes.length;j++)walk(m.addedNodes[j])}
    else if(m.type==='characterData')doText(m.target);
    else if(m.type==='attributes')doAttr(m.target);
  }
  if(origT.size>20000){origT.forEach(function(v,k){if(!k.isConnected)origT.delete(k)})}
}
function start(){
  if(obs)return;
  obs=new MutationObserver(onMut);
  obs.observe(document.body,{childList:true,subtree:true,characterData:true,attributes:true,attributeFilter:ATT});
}
function revert(){
  origT.forEach(function(v,k){if(k.isConnected)k.data=v});origT.clear();
  var all=document.querySelectorAll('*');
  for(var i=0;i<all.length;i++){var e=all[i];if(e.__vi){for(var a in e.__vi)e.setAttribute(a,e.__vi[a]);e.__vi=null}}
}
function paintBtns(){
  var b=document.querySelectorAll('#langsw button');
  for(var i=0;i<b.length;i++){var on=b[i].dataset.l===lang;b[i].classList.toggle('on',on);b[i].setAttribute('aria-pressed',String(on))}
}
function setLang(l){
  if(l===lang)return;
  lang=l;try{localStorage.setItem('dq_lang',l)}catch(e){}
  document.documentElement.lang=l;
  if(l==='en'){walk(document.body)}else{revert()}
  paintBtns();
}
['confirm','alert','prompt'].forEach(function(k){
  var o=window[k];if(typeof o!=='function')return;
  window[k]=function(m,d){return o.call(window,lang==='en'?tr(String(m)):m,d)};
});
var css=document.createElement('style');
css.textContent='html{overflow-x:hidden}body{overflow:visible!important}.hdrbar{position:sticky;top:0;z-index:60;margin-top:0!important;padding:8px 0;background:transparent!important;-webkit-backdrop-filter:none;backdrop-filter:none;pointer-events:none}.hdrbar>*{pointer-events:auto}'
 +'.istick{top:var(--hbh,0px)!important}'
 +'.hnw{display:inline-flex;align-items:center;gap:10px}'
 +'#langsw{display:inline-flex;border:1px solid #2a2d33;border-radius:10px;overflow:hidden;vertical-align:middle}'
 +'#langsw button{background:#000;border:0;padding:9px 12px;cursor:pointer;font-weight:700;color:#fff;letter-spacing:.4px;transition:background .15s}'
 +'#langsw button+button{border-left:1px solid #2a2d33}'
 +'#langsw button:hover{background:#1c1d20}'
 +'#langsw button.on{background:#6b7280;color:#fff}'
 +'.gxy,.gxy-video{display:none!important}body{background:#020306!important}'
 +'.bt:not(.on):not(.dg):not(:disabled){background:rgba(255,255,255,.045)!important;border-color:rgba(255,255,255,.12)!important;box-shadow:none!important}.bt:not(.on):not(.dg):not(:disabled):hover{background:rgba(255,255,255,.1)!important}'
 +'.cfg-b{background:rgba(255,255,255,.04)!important;border-color:rgba(255,255,255,.12)!important}.cfg-b:hover{background:rgba(255,255,255,.09)!important}'
 +'.cfg-b.p,#ck_add,#ha{background:rgba(255,255,255,.07)!important;border:1px solid rgba(255,255,255,.16)!important;box-shadow:none!important}.cfg-b.p:hover,#ck_add:hover,#ha:hover{background:rgba(255,255,255,.14)!important}'
 +'#stbar button{background:rgba(0,0,0,.22)!important;border-color:rgba(255,255,255,.1)!important}#stbar button:hover{background:rgba(255,255,255,.1)!important}'
 +'#langsw{border-color:rgba(255,255,255,.14)!important}#langsw button{background:rgba(0,0,0,.25)!important}#langsw button.on{background:rgba(107,114,128,.5)!important}#langsw button+button{border-left-color:rgba(255,255,255,.14)!important}'
 +'.mon-rb.on{background:rgba(255,255,255,.12)!important}'
 +'#pane-perf,#pane-monitor,#pane-rejoin{--pn:rgba(8,10,16,.26);--cd:rgba(14,17,25,.24);--bg:rgba(3,4,9,.3)}#pane-perf option,#pane-monitor option,#pane-rejoin option{background:#0b0d12!important}'
 +'#pane-perf .cfg-box,#pane-monitor .cfg-box,#pane-rejoin .cfg-box,#pane-monitor .mon-t,#pane-monitor .mon-c,#pane-monitor .mon-tb,#pane-rejoin .vr-c,#pane-rejoin .vr-l{-webkit-backdrop-filter:none!important;backdrop-filter:none!important}'
 +'.istick{background:rgba(8,10,16,.3)!important;-webkit-backdrop-filter:none!important;backdrop-filter:none!important}';
document.head.appendChild(css);
var sw=document.createElement('span');sw.id='langsw';sw.title='Language / Ngôn ngữ';
sw.innerHTML='<button type="button" data-l="vi">VI</button><button type="button" data-l="en">EN</button>';
sw.addEventListener('click',function(e){var b=e.target.closest&&e.target.closest('button[data-l]');if(b)setLang(b.dataset.l)});
var ha=document.getElementById('ha');
if(ha&&ha.parentNode){var row=ha.parentNode,wr=document.createElement('span');row.classList.add('hdrbar');wr.className='hnw';row.insertBefore(wr,ha);var hb=row.querySelector('.hbrand');if(hb)hb.remove();wr.appendChild(ha);var gs=document.getElementById('gsbtn');if(gs)wr.appendChild(gs);
  var mid=document.createElement('span');mid.id='hdrmid';mid.className='hnw';row.insertBefore(mid,wr);
  var badge=document.createElement('span');badge.id='hdrbadge';badge.className='hnw';row.insertBefore(badge,mid)}
var dock=document.createElement('div');dock.id='langdock';dock.style.cssText='position:fixed;left:12px;bottom:calc(12px + env(safe-area-inset-bottom,0px));z-index:60';
dock.appendChild(sw);document.body.appendChild(dock);
function hbh(){var h=document.querySelector('.hdrbar');if(h)document.documentElement.style.setProperty('--hbh',Math.max(0,h.offsetHeight-6)+'px')}
hbh();window.addEventListener('resize',hbh);setTimeout(hbh,500);setTimeout(hbh,2000);
function galaxy(){
  var cv=document.getElementById('galaxy');if(!cv){cv=document.createElement('canvas');cv.id='galaxy';cv.style.cssText='position:fixed;left:0;top:0;width:100vw;height:100vh;z-index:-8;pointer-events:none;display:block';document.body.insertBefore(cv,document.body.firstChild)}
  var w=Math.max(320,window.innerWidth),h=Math.max(320,window.innerHeight);if(cv._w===w&&cv._h===h)return;cv._w=w;cv._h=h;cv.width=w;cv.height=h;
  var c=cv.getContext('2d');if(!c)return;var sd=20261014;function R(){sd=(sd*1664525+1013904223)>>>0;return sd/4294967296}function G(){return (R()+R()+R()+R()-2)/2}
  var mn=Math.min(w,h),g0=c.createLinearGradient(0,0,0,h);g0.addColorStop(0,'#03040b');g0.addColorStop(1,'#010207');c.fillStyle=g0;c.fillRect(0,0,w,h);
  var ax=-0.05*w,ay=1.02*h,bx=1.08*w,by=-0.02*h,dxl=bx-ax,dyl=by-ay,ln=Math.sqrt(dxl*dxl+dyl*dyl),nx=-dyl/ln,ny=dxl/ln;
  c.globalCompositeOperation='lighter';
  var cols=[[120,92,210],[70,118,225],[190,100,175],[95,80,190]];
  for(var i=0;i<110;i++){var t=R(),off=G()*0.15*mn,cx=ax+dxl*t+nx*off,cy=ay+dyl*t+ny*off,r=(0.1+R()*0.22)*mn,k=cols[(R()*cols.length)|0],a=0.04+R()*0.06;
    var gr=c.createRadialGradient(cx,cy,0,cx,cy,r);gr.addColorStop(0,'rgba('+k[0]+','+k[1]+','+k[2]+','+a+')');gr.addColorStop(1,'rgba('+k[0]+','+k[1]+','+k[2]+',0)');c.fillStyle=gr;c.fillRect(cx-r,cy-r,r*2,r*2)}
  var hx=w*0.84,hy=h*0.08,hr=0.42*mn,hg=c.createRadialGradient(hx,hy,0,hx,hy,hr);hg.addColorStop(0,'rgba(235,225,255,.42)');hg.addColorStop(.22,'rgba(160,140,245,.22)');hg.addColorStop(.55,'rgba(95,105,215,.08)');hg.addColorStop(1,'rgba(60,70,190,0)');c.fillStyle=hg;c.fillRect(hx-hr,hy-hr,hr*2,hr*2);
  c.globalCompositeOperation='source-over';
  for(var j=0;j<26;j++){var t2=R(),o2=G()*0.1*mn,dx2=ax+dxl*t2+nx*o2,dy2=ay+dyl*t2+ny*o2,r2=(0.04+R()*0.1)*mn,dg=c.createRadialGradient(dx2,dy2,0,dx2,dy2,r2);dg.addColorStop(0,'rgba(1,2,6,.3)');dg.addColorStop(1,'rgba(1,2,6,0)');c.fillStyle=dg;c.fillRect(dx2-r2,dy2-r2,r2*2,r2*2)}
  function near(x,y){var d=Math.abs((x-ax)*nx+(y-ay)*ny);return Math.exp(-d*d/(2*Math.pow(0.22*mn,2)))}
  var N=Math.round(w*h/700),sc=[[255,255,255],[205,220,255],[255,238,210],[225,205,255]];
  for(var s=0;s<N;s++){var x=R()*w,y=R()*h;if(R()>0.45+0.55*near(x,y))continue;var rr=0.45+Math.pow(R(),3)*1.0,al=0.35+R()*0.65,k2=sc[(R()*sc.length)|0];
    c.fillStyle='rgba('+k2[0]+','+k2[1]+','+k2[2]+','+al.toFixed(2)+')';c.beginPath();c.arc(x,y,rr,0,6.2832);c.fill()}
  for(var b=0;b<18;b++){var bx2=R()*w,by2=R()*h,br=1.1+R()*0.7,k3=sc[(R()*sc.length)|0],gg=c.createRadialGradient(bx2,by2,0,bx2,by2,br*5);
    gg.addColorStop(0,'rgba('+k3[0]+','+k3[1]+','+k3[2]+',.55)');gg.addColorStop(1,'rgba('+k3[0]+','+k3[1]+','+k3[2]+',0)');c.fillStyle=gg;c.fillRect(bx2-br*5,by2-br*5,br*10,br*10);
    c.fillStyle='rgba(255,255,255,.95)';c.beginPath();c.arc(bx2,by2,br*0.8,0,6.2832);c.fill()}
  c.fillStyle='rgba(0,0,0,.1)';c.fillRect(0,0,w,h)}   // nền tĩnh: chỉ vẽ 1 lần (và khi đổi kích thước), không tốn GPU liên tục
galaxy();(function(){var tm=null;window.addEventListener('resize',function(){clearTimeout(tm);tm=setTimeout(galaxy,250)})})();
paintBtns();
if(lang==='en'){document.documentElement.lang='en';walk(document.body)}
start();
}catch(e){console.error('i18n',e)}
})();
`;
// ===== LOW UI: nut tat hieu ung mo (backdrop-filter)/bong do/vien gradient, chuyen nen den phang + nut xam chu trang
// de do ton tai nguyen may (GPU) khi chay web tracker song song voi nhieu client Roblox =====
const LOWUI_JS = String.raw`
(function(){
try{
var on=false;try{on=localStorage.getItem('dq_lowui')==='1'}catch(e){}
var css=document.createElement('style');
css.textContent='html.lowui *{backdrop-filter:none!important;-webkit-backdrop-filter:none!important;box-shadow:none!important;text-shadow:none!important;animation:none!important}'
 +'html.lowui body{background:#000!important}'
 +'html.lowui #galaxy,html.lowui .gxy,html.lowui .gxy-video{display:none!important}'
 +'html.lowui .pn,html.lowui .tw,html.lowui .tcon,html.lowui .am,html.lowui .st,html.lowui .tabs,html.lowui .istick{background:#0a0a0a!important;background-image:none!important}'
 +'html.lowui .bt:not(.on):not(.dg):not(:disabled),html.lowui .cfg-b,html.lowui .cfg-b.p,html.lowui #ck_add,html.lowui #ha,html.lowui #stbar button,html.lowui #langsw button{background:#2b2b2e!important;color:#fff!important;border:1px solid #777!important}'
 +'html.lowui .bt:not(.on):not(.dg):not(:disabled):hover,html.lowui .cfg-b:hover,html.lowui .cfg-b.p:hover,html.lowui #ck_add:hover,html.lowui #ha:hover,html.lowui #stbar button:hover,html.lowui #langsw button:hover{background:#3a3a3e!important}'
 +'html.lowui .bt.on,html.lowui #langsw button.on{background:#5b5f66!important;border-color:#aeb4bd!important;color:#fff!important}';
document.head.appendChild(css);
function apply(){
  document.documentElement.classList.toggle('lowui',on);
  var b=document.getElementById('lowuibtn');
  if(b){b.classList.toggle('on',on);b.setAttribute('aria-pressed',String(on))}
}
function mk(){
  if(document.getElementById('lowuibtn'))return;
  var host=(document.getElementById('ha')&&document.getElementById('ha').parentNode)||document.getElementById('hdrmid')||document.querySelector('.hnw');
  if(!host)return;
  var b=document.createElement('button');
  b.type='button';b.id='lowuibtn';b.className='bt';b.textContent='Low UI';
  b.title='Chuyển sang giao diện nhẹ (nền đen, tắt hiệu ứng mờ/bóng) để đỡ tốn tài nguyên máy';
  b.onclick=function(){on=!on;try{localStorage.setItem('dq_lowui',on?'1':'0')}catch(e){}apply()};
  host.appendChild(b);
}
apply();
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mk);else mk();
}catch(e){console.error('lowui',e)}
})();
`;
const DOTS_JS = String.raw`
(function(){
try{
var tabs=document.getElementById('tabs');if(!tabs)return;
var css=document.createElement('style');
css.textContent='#stbar{display:inline-flex;flex-wrap:wrap;gap:8px;align-items:center}'
 +'#stbar button{display:inline-flex;align-items:center;gap:8px;background:rgba(0,0,0,.45);border:1px solid var(--bd);border-radius:10px;padding:9px 13px;cursor:pointer;font-weight:600;font-size:13px;color:#fff;white-space:nowrap}'
 +'#stbar button:hover{background:#1c1d20}'
 +'#stbar .sdot{display:inline-block;flex:none;margin:0;width:9px;height:9px;border-radius:50%;background:#f43f5e;box-shadow:0 0 7px #f43f5e;transition:background .25s,box-shadow .25s}'
 +'#stbar .sdot.on{background:#4ade80;box-shadow:0 0 9px #4ade80}'
 +'.hnw{flex-wrap:wrap;justify-content:flex-end;max-width:100%}';
document.head.appendChild(css);
var NAMES=['cookie','rejoin','perf'],dots={};
function dqLocal(path){try{var L=window.__DQ_LOCAL__;if(L&&L.url&&/^\/api\/(config|rejoin|perf)\//.test(path))return L}catch(e){}return null}
function hdr(loc){return {'x-api-key':(loc?loc.key:(typeof KEYV!=='undefined'?KEYV:'')),'x-requested-with':'dq'}}
function mk(){}
var LBL={cookie:'Cookie',rejoin:'Auto Rejoin',perf:'Performance'},bar=null,bdots={};
function mkBar(){
  if(bar&&bar.isConnected)return;
  var host=document.getElementById('hdrmid')||document.querySelector('.hnw')||(document.getElementById('ha')&&document.getElementById('ha').parentNode);
  if(!host)return;
  bar=document.createElement('span');bar.id='stbar';bdots={};
  NAMES.forEach(function(n){
    var b=document.createElement('button');b.type='button';b.dataset.go=n;
    var d=document.createElement('i');d.className='sdot na';b.appendChild(d);b.appendChild(document.createTextNode(LBL[n]));
    b.onclick=function(){var t=tabs.querySelector('.tab[data-tab="'+n+'"]');if(t)t.click()};
    bar.appendChild(b);bdots[n]=d;
  });
  host.insertBefore(bar,host.firstChild);
}
function set(n,v){
  var cls='sdot'+(v?' on':''),tt=v?'ON':'OFF';
  var d=dots[n];if(d){d.className=cls;d.title=tt}
  var e=bdots[n];if(e){e.className=cls;e.title=tt}
}
async function get(u){var L=dqLocal(u),url=L?(L.url+u):u;var r=await fetch(url,{headers:hdr(L)});if(!r.ok)throw 0;return r.json()}
async function poll(){
  mk();mkBar();
  var ckN=document.querySelectorAll('#ck_grid [data-rm]').length,ckS=0;try{var j=await get('/api/cookie/stored');ckS=(j&&j.enabled&&(j.names||[]).length)||0}catch(e){}set('cookie',ckN>0||ckS>0)
  try{var j2=await get('/api/rejoin/state');set('rejoin',!!(j2&&j2.cfg&&j2.cfg.enabled))}catch(e){set('rejoin',false)}
  try{var j3=await get('/api/perf/state'),c=j3&&j3.cfg;set('perf',!!(c&&((c.cpu&&c.cpu.enabled)||(c.ram&&c.ram.enabled)||(c.arr&&c.arr.enabled))))}catch(e){set('perf',false)}
}
document.addEventListener('click',function(e){
  var t=e.target;if(!t||!t.closest)return;
  var host=t.closest('#pane-perf .cfg-f,#pane-perf .pf-hd,#pane-rejoin .cfg-f,#pane-rejoin .rj-top,#pane-config .cfg-f');if(!host)return;
  if(t.closest('input,select,textarea,button,a'))return;
  if(host.querySelector('input[type=text],input[type=number],select,textarea'))return;
  var cb=host.querySelector('input[type=checkbox]');if(!cb||cb.disabled)return;
  cb.click();
});
var busy=false;function run(){if(busy)return;busy=true;poll().then(function(){busy=false},function(){busy=false})}
document.addEventListener('change',function(){setTimeout(run,1500)});
document.addEventListener('click',function(e){if(e.target&&e.target.closest&&e.target.closest('.sw,#rj_sw,.pf-sw,.tab,#ck_clr,#ck_add,[data-rm]'))setTimeout(run,1500)});
setInterval(function(){if(!document.hidden)run()},5000);
setTimeout(run,600);
}catch(e){console.error('status dots',e)}
})();
`;
const AUTO_REFRESH_JS = String.raw`
(function(){
try{
var of=window.fetch;if(!of||window.__arf)return;window.__arf=1;
var STEPS=[0,300,800,1600,3000,5000],timers=[],lastAt=0,last={};
function burst(force){
  var now=Date.now();if(!force&&now-lastAt<1500)return;lastAt=now;
  timers.forEach(clearTimeout);
  timers=STEPS.map(function(ms){return setTimeout(function(){try{if(!document.hidden&&typeof poll==='function')poll(true)}catch(e){}},ms)});
}
window.refreshNow=function(){burst(true)};
window.fetch=function(input,init){
  var p=of.apply(this,arguments);
  try{
    var url=typeof input==='string'?input:((input&&input.url)||''),path=String(url).split('?')[0];
    var m=String((init&&init.method)||(input&&input.method)||'GET').toUpperCase();
    if(/\/api\//.test(path)&&!/\/api\/(report|img|dump|cookie|accounts$)/.test(path)){
      if(m!=='GET'){
        p.then(function(r){if(r&&r.ok)burst(true)},function(){});   // lenh vua gui xong -> tai lai ngay
      }else if(/\/api\/trade\//.test(path)&&!/\/wait$/.test(path)){
        p.then(function(r){return r.clone().text().then(function(t){if(last[path]!==undefined&&last[path]!==t)burst(false);last[path]=t})}).catch(function(){});   // trang thai lenh doi (dang chay -> xong) -> tai lai
      }
    }
  }catch(e){}
  return p;
};
}catch(e){console.error('auto refresh',e)}
})();
`;

// BUILD_WATCH_JS: theo doi window.__BUILD__ (ban dau ghi luc tai trang) voi field "build" ma /api/accounts tra ve
// moi lan poll. Khac nhau = server vua restart voi file index.js moi (vua sua xong, vua gui lai) -> tu bao + tu
// tai lai trang sau it giay, khong can nguoi dung nho bam Ctrl+F5 nua.
const BUILD_WATCH_JS = String.raw`
(function(){
try{
  var warned=false;
  function show(){
    if(warned)return;warned=true;
    var el=document.createElement('div');
    el.style.cssText='position:fixed;left:50%;bottom:18px;transform:translateX(-50%);z-index:99999;background:#16a34a;color:#fff;padding:10px 16px;border-radius:10px;font:13px system-ui,-apple-system,sans-serif;box-shadow:0 4px 18px #0006;display:flex;gap:10px;align-items:center';
    el.innerHTML='Đã có bản cập nhật mới — tự tải lại sau <b id="bwc">10</b>s <button id="bwb" type="button" style="background:#fff;color:#16a34a;border:0;border-radius:6px;padding:4px 10px;cursor:pointer;font-weight:700">Tải lại ngay</button> <button id="bwx" type="button" style="background:transparent;color:#fff;border:1px solid #ffffff88;border-radius:6px;padding:4px 10px;cursor:pointer">Để sau</button>';
    document.body.appendChild(el);
    var n=10,t=setInterval(function(){n--;var c=document.getElementById('bwc');if(c)c.textContent=n;if(n<=0){clearInterval(t);location.reload()}},1000);
    document.getElementById('bwb').onclick=function(){location.reload()};
    document.getElementById('bwx').onclick=function(){clearInterval(t);el.remove();warned=false};
  }
  window.__checkBuild=function(b){ if(b&&window.__BUILD__&&b!==window.__BUILD__) show(); };
}catch(e){}
})();
`;

const DASHBOARD_HTML_CK = DASHBOARD_HTML.replace('</body>', () => '<script>' + CK_UI_JS + '</script><script>' + CFG_UI_JS + '</script><script>' + RJ_UI_JS + '</script><script>' + PF_UI_JS + '</script><script>' + RUNS_UI_JS + '</script><script>' + I18N_JS + '</script><script>' + LOWUI_JS + '</script><script>' + DOTS_JS + '</script><script>' + AUTO_REFRESH_JS + '</script><script>' + BUILD_WATCH_JS + '</script><script>' + TRADE_UI_JS + '</script></body>');   // tat ca tinh nang (gom ca Trade) deu Public cho moi user co key, khong rieng admin
const DASHBOARD_ADMIN_HTML = DASHBOARD_HTML_CK;

const isLocalHost = host => {
  const value = String(host || '').toLowerCase().replace(/\[|\]/g, '');
  return ['localhost', '127.0.0.1', '0.0.0.0', '::1'].includes(value)
    || /^localhost:\d+$/.test(host)
    || /^127\.0\.0\.1:\d+$/.test(host)
    || /^0\.0\.0\.0:\d+$/.test(host)
    || /^::1:\d+$/.test(host)
    || /^\[::1\]:\d+$/.test(host);
};

// =====================================================================
//  MULTI-USER: dang nhap Discord + key kich hoat
// =====================================================================
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const sha = v => crypto.createHash('sha256').update(String(v)).digest('hex');
const rnd = n => crypto.randomBytes(n).toString('base64url');
const newToken = () => 'dqu_' + rnd(24);
const KEY_ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 32 ky tu, bo O/I/0/1 cho de doc
const newKey = () => { const b = crypto.randomBytes(16); let k = ''; for (let i = 0; i < 16; i++) k += KEY_ALPHA[b[i] % 32]; return 'DQ-' + k.match(/.{4}/g).join('-'); };
const normKey = v => { const c = String(v || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); return /^DQ[A-Z0-9]{16}$/.test(c) ? 'DQ-' + c.slice(2).match(/.{4}/g).join('-') : ''; };
const safeEq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const clampInt = (v, lo, hi, d) => { v = Math.floor(Number(v)); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };
const fmtDate = ms => new Date(ms).toLocaleDateString('vi-VN');

const isAdminUser = u => !!u && ADMIN_IDS.has(u.id);
const ownerOf = u => isAdminUser(u) ? 'admin' : u.id;            // admin dung chung kho 'admin' (du lieu cu)
const licActive = u => {
  if (!u) return false;
  if (isAdminUser(u)) return true;
  if (u.banned) return false;
  const l = u.license; return !!l && (l.expiresAt === null || l.expiresAt > Date.now());
};
const maxAcc = u => isAdminUser(u) ? Infinity : (u.license && u.license.maxAccounts > 0 ? u.license.maxAccounts : Infinity);
const avatarUrl = u => u.avatar
  ? 'https://cdn.discordapp.com/avatars/' + u.id + '/' + u.avatar + '.png?size=64'
  : 'https://cdn.discordapp.com/embed/avatars/' + Number((BigInt(u.id) >> 22n) % 6n) + '.png';

// ---- HTTP helpers ----
const sendX = (res, code, body, type = 'application/json', extra = {}) => {
  res.writeHead(code, { 'content-type': type + '; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff', 'referrer-policy': 'same-origin', ...extra });
  res.end(typeof body === 'string' ? body : JSON.stringify(body));
};
const sendPage = (res, html, code = 200) => sendX(res, code, html, 'text/html', { 'x-frame-options': 'DENY' });
const redirect = (res, loc, extra = {}) => { res.writeHead(302, { location: loc, 'cache-control': 'no-store', ...extra }); res.end(); };
const cookieStr = (name, val, maxAge) => name + '=' + encodeURIComponent(val) + '; Path=/; HttpOnly; SameSite=Lax; Max-Age=' + maxAge + (PUBLIC_URL.startsWith('https') ? '; Secure' : '');
const parseCookies = req => {
  const o = Object.create(null);
  for (const part of String(req.headers.cookie || '').split(';')) {
    const i = part.indexOf('='); if (i < 1) continue;
    const k = part.slice(0, i).trim(); let v = part.slice(i + 1).trim();
    try { v = decodeURIComponent(v); } catch {}
    if (!(k in o)) o[k] = v;
  }
  return o;
};
// ---- KIEM TRA COOKIE ROBLOX (.ROBLOSECURITY) cho CHINH account dang co trong tracker ----
// - Cookie chi nam trong body request (HTTPS), dung 1 lan de goi Roblox, KHONG luu, KHONG log.
// - Chi kiem tra account da ton tai trong tracker cua nguoi goi; cookie phai thuoc dung username do (tranh bi dung lam tool check cookie bua bai).
// - Chi luu KET QUA (valid/invalid/banned/mismatch + thoi gian) vao record account.
const ckLast = new Map();
const ckDeepLast = new Map();   // rieng cho Quet tool, khong dung chung voi /api/cookie/check
const CK_RE = /^[\w\-.|:%+/=]{80,4000}$/;
// Tham gia game de lay ma loi 267 / 529 / 277 (chi khi acc da xac nhan dung + cookie con song)
const CK_PLACE = process.env.CK_PLACE || '85776757589518';   // placeId Dungeon Quest: co captcha rejoin / lock
const CK_JOIN_ON = process.env.CK_JOIN_ON !== '0';            // CK_JOIN_ON=0 de tat
const ckClean = raw => String(raw || '').trim().replace(/^\.?ROBLOSECURITY\s*=\s*/i, '').replace(/;.*$/, '').replace(/^["']|["']$/g, '');
// ---- KHO COOKIE MA HOA (tuy chon) ----
// Luu .ROBLOSECURITY da ma hoa AES-256-GCM (rang buoc owner+ten acc) o Redis hash dq:ck:<owner> de check lai nhung lan sau.
// Khoa: env COOKIE_SECRET (bat buoc tren Vercel/Upstash). Chay local ma khong dat -> tu tao file <data.json>.key (giu bi mat!).
// Cookie KHONG BAO GIO tra ve trinh duyet, KHONG log. Cookie chet (401) bi xoa khoi kho.
const CK_VAULT_KEY = (() => {
  let sec = process.env.COOKIE_SECRET || '';
  if (!sec && !USE_REMOTE) {
    const kf = DATA_FILE + '.key';
    try { sec = fs.readFileSync(kf, 'utf8').trim(); } catch {}
    if (!sec) { try { sec = crypto.randomBytes(32).toString('hex'); fs.writeFileSync(kf, sec, { mode: 0o600 }); console.log('[cookie] Da tao khoa ma hoa cookie: ' + kf + ' (GIU BI MAT, dung de chung voi data.json)'); } catch { sec = ''; } }
  }
  return sec ? crypto.createHash('sha256').update('dq-ck:' + sec).digest() : null;
})();
const CK_MAX_SAVED = 50;
// ---- Luu cookie THO vao file .txt (khong ma hoa): data/cookies/<sanitized-owner>.txt ----
// Moi dong: <name>TAB<cookie-tho>. Xoá tất ca -> gỡ file + dung check.
const ckOwnerFile = o => path.join(path.dirname(DATA_FILE), 'data', 'cookies', (String(o||'').replace(/[^A-Za-z0-9_-]/g,'_').slice(0,80) || 'owner') + '.txt');
const ckParseFile = txt => { const m=Object.create(null); String(txt||'').split(/\r?\n/).forEach(function(l){const i=l.indexOf('\t'); if(i<1) return; const n=l.slice(0,i).trim(); const c=l.slice(i+1).trim(); if(/^[A-Za-z0-9_]{3,20}$/.test(n)&&c.length>=80) m[n]=c; }); return m; };
const ckFileRead = o => { try{ return ckParseFile(fs.readFileSync(ckOwnerFile(o),'utf8')); }catch{ return Object.create(null); } };
const ckFileWrite = (o, map) => { const dir=path.dirname(ckOwnerFile(o)); try{fs.mkdirSync(dir,{recursive:true})}catch{}; const lines=Object.keys(map).map(function(k){return k+'\t'+map[k]}).join('\n'); if(lines) fs.writeFileSync(ckOwnerFile(o), lines, 'utf8'); else try{fs.unlinkSync(ckOwnerFile(o))}catch{} };
const ckVK = o => 'dq:ck:' + o;
// ckFileRead/ckFileWrite (phia tren) ghi ra DIA LOCAL - CHAY DUOC khi tu host (may/VPS rieng) hoac
// trong "local agent" cua app Windows, nhung KHONG CHAY DUOC tren Vercel (serverless, dia chi doc
// duy nhat /var/task, khong ghi duoc -> loi ENOENT khi luu cookie). ckLoad/ckSave la ban THAY THE
// dung chung ham redis() co san (ham nay TU DONG chon Upstash that neu co cau hinh KV_REST_API_*,
// hoac gia lap bang file data.json neu chay local/self-host) - nen luon ghi duoc du server dang
// chay o dau. Dung CHO MOI noi trong code thay vi goi thang ckFileRead/ckFileWrite.
async function ckLoad(o) {
  try { const out = await redis([['GET', ckVK(o)]]); return ckParseFile(out[0] || ''); }
  catch (e) { console.error('[ck-load loi]', e && e.message); return ckFileRead(o); }   // Redis loi -> thu doc file local (phong khi dang chay self-host co du lieu cu)
}
async function ckSave(o, map) {
  const lines = Object.keys(map).map(k => k + '\t' + map[k]).join('\n');
  try { if (lines) await redis([['SET', ckVK(o), lines]]); else await redis([['SET', ckVK(o), '']]); }
  catch (e) { console.error('[ck-save loi]', e && e.message); throw e; }
}
// ================== TOOL CHECK COOKIE SAU (checkcookie.exe) ==================
// Chi chay tren MAY CHU Windows va CHI khi: (client goi truc tiep tu localhost, khong qua proxy/tunnel) HOAC (nguoi goi la ADMIN).
// Khong tim thay file exe -> tinh nang OFF (khong anh huong gi khac).
// Moi lan chay: tao 1 thu muc tam trong %TEMP% (cookie.txt + config.json + data/), chay xong XOA SACH.
// Cookie KHONG BAO GIO duoc log, KHONG BAO GIO tra ve trinh duyet, KHONG luu vao dia lau dai.
const CHK_ENGINE = (() => {
  const IS_WIN = process.platform === 'win32', ON_VERCEL = !!process.env.VERCEL;
  let dir = '', exe = '';
  if (process.env.CHECKCOOKIE_PATH) {
    try { if (fs.statSync(process.env.CHECKCOOKIE_PATH).isFile()) { exe = process.env.CHECKCOOKIE_PATH; dir = path.dirname(exe); } } catch {}
  }
  if (!exe && IS_WIN && !ON_VERCEL) {
    const roots = [path.join(__dirname, 'tools'), path.join(__dirname, 'check cookie'), path.join(__dirname, 'check cookie', 'check cookie')];
    const seen = new Set();
    const consider = (d, depth) => {
      if (!d || exe || seen.has(d) || depth > 3) return;
      seen.add(d);
      try { if (!fs.statSync(d).isDirectory()) return; } catch { return; }
      let names = [];
      try { names = fs.readdirSync(d).filter(n => /\.(exe|cmd|bat)$/i.test(n) && /cookie|check/i.test(n)); } catch {}
      names.sort((x, y) => x.length - y.length);
      for (const n of names.concat(['checkcookie.exe', 'check_cookie.exe', 'checkcookie.cmd', 'checkcookie.bat'])) {
        const f = path.join(d, n);
        try { if (fs.statSync(f).isFile()) { exe = f; dir = d; return; } } catch {}
      }
      try { for (const n of fs.readdirSync(d)) { try { if (fs.statSync(path.join(d, n)).isDirectory()) consider(path.join(d, n), depth + 1); } catch {} } } catch {}
    };
    for (const r of roots) { consider(r, 0); if (exe) break; }
  }
  return { on: !!exe, exe, dir };
})();
if (CHK_ENGINE.on) console.log('[cookie-deep] Tool check sau: ' + CHK_ENGINE.exe + '  (chi localhost hoac admin)');
else console.log('[cookie-deep] Khong thay checkcookie.exe -> tinh nang check sau DANG TAT.');
const CK_DEEP_PLATFORMS = ['valid', 'banned', 'faceid', 'dead'];   // thu tu uu tien khi cookie trung o nhieu file
const CK_DEEP_FILES = { valid: 'live.txt', banned: 'banned.txt', faceid: 'faceid.txt', dead: 'dead.txt' };
const CK_DEEP_MAP = { valid: 'valid', banned: 'invalid', faceid: 'invalid', dead: 'invalid' };   // valid = con song; banned/faceid/dead = khong dung duoc (Dead)
const CK_DEEP_MAX = 100;
// Duoc dung tinh nang sau khong?
function ckDeepAllowed(req, a) {
  if (!CHK_ENGINE.on) return false;
  if (isLoopbackReq(req)) return true;   // goi truc tiep tu localhost (khong co x-forwarded-*)
  return !!(a && a.admin);               // hoac admin (Discord ID admin / key admin)
}
// Chay tool 1 lan voi danh sach cookie; tra ve ket qua theo dung thu tu dau vao.
function ckRunExe(cookies, opts) {
  return new Promise(resolve => {
    const o = opts || {}, need = Array.isArray(cookies) ? cookies.slice() : [];
    const work = path.join(os.tmpdir(), 'dq-ck-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8));
    let done = false, killer = null, proc = null;
    const cleanup = () => { try { fs.rmSync(work, { recursive: true, force: true }); } catch {} };
    const finish = out => { if (done) return; done = true; try { clearTimeout(killer); } catch {} cleanup(); resolve(out); };
    try {
      fs.mkdirSync(path.join(work, 'data'), { recursive: true });
      fs.writeFileSync(path.join(work, 'cookie.txt'), need.join('\n'), 'utf8');
      if (!need.length) return finish({ ok: true, results: [], stats: { total: 0 } });
      let cfg = null;
      try { cfg = JSON.parse(fs.readFileSync(path.join(dirOfConfig(), 'config.json'), 'utf8')); } catch {}
      if (!cfg || typeof cfg !== 'object' || Array.isArray(cfg)) cfg = {};
      cfg.language = 'vi';
      cfg.concurrency = clampInt(o.concurrency, 1, 300, 100);
      cfg.timeout_sec = clampInt(o.timeoutSec, 1, 60, 5);
      fs.writeFileSync(path.join(work, 'config.json'), JSON.stringify(cfg, null, 2));
      const child = spawn(CHK_ENGINE.exe, [], {
        cwd: work,
        windowsHide: true,
        stdio: ['pipe', 'pipe', 'pipe'],
        env: Object.assign({}, process.env, { PYTHONIOENCODING: 'utf-8', PYTHONUTF8: '1', PYTHONLEGACYWINDOWSSTDIO: '0' })
      });
      proc = child;
      let log = '';
      const tap = c => { log += c.toString('utf8'); if (log.length > 1e5) log = log.slice(-5e4); };
      child.stdout.on('data', tap); child.stderr.on('data', tap);
      child.on('error', e => finish({ ok: false, error: String((e && e.message) || e), results: [] }));
      const readSets = () => {
        const sets = {};
        for (const st of CK_DEEP_PLATFORMS) {
          sets[st] = new Set();
          try { for (const l of fs.readFileSync(path.join(work, 'data', CK_DEEP_FILES[st]), 'utf8').split(/\r?\n/)) { const x = l.trim(); if (x) sets[st].add(x); } } catch {}
        }
        return sets;
      };
      const build = () => {
        const sets = readSets(), res = new Array(need.length);
        for (let i = 0; i < need.length; i++) {
          let hit = null;
          for (const st of CK_DEEP_PLATFORMS) if (sets[st].has(need[i])) { hit = st; break; }
          res[i] = { s: hit || 'dead' };
        }
        return res;
      };
      const poll = tries => {
        const sets = readSets();
        let n = 0; for (const st of CK_DEEP_PLATFORMS) n += sets[st].size;
        if (n >= need.length || tries <= 0) {
          const results = build();
          const stats = { total: need.length, live: 0, faceid: 0, dead: 0 };
          for (const r of results) stats[r.s] = (stats[r.s] || 0) + 1;
          return finish({ ok: true, results, stats, log });
        }
        setTimeout(() => poll(tries - 1), 150);
      };
      child.on('close', () => setTimeout(() => poll(10), 150));   // doi tool ghi xong file ket qua
      child.stdin.on('error', () => {});
      try { child.stdin.end(); } catch {}
      killer = setTimeout(() => { try { if (proc) proc.kill(); } catch {} finish({ ok: false, error: 'timeout', results: [] }); }, clampInt(o.timeoutMs, 5000, 3e5, Math.max(3e4, need.length * 400)));
    } catch (e) { finish({ ok: false, error: String((e && e.message) || e), results: [] }); }
  });
}
function dirOfConfig() { return CHK_ENGINE.dir || __dirname; }
// ================== HET TOOL CHECK COOKIE SAU ==================
const ckEnc = (c, aad) => { const iv = crypto.randomBytes(12), ci = crypto.createCipheriv('aes-256-gcm', CK_VAULT_KEY, iv); ci.setAAD(Buffer.from(aad)); const ct = Buffer.concat([ci.update(c, 'utf8'), ci.final()]); return JSON.stringify({ iv: iv.toString('base64'), tag: ci.getAuthTag().toString('base64'), ct: ct.toString('base64'), at: Date.now() }); };
const ckDec = (raw, aad) => { try { const j = JSON.parse(raw), d = crypto.createDecipheriv('aes-256-gcm', CK_VAULT_KEY, Buffer.from(j.iv, 'base64')); d.setAAD(Buffer.from(aad)); d.setAuthTag(Buffer.from(j.tag, 'base64')); return Buffer.concat([d.update(Buffer.from(j.ct, 'base64')), d.final()]).toString('utf8'); } catch { return null; } };
async function rbxFetchJson(url, headers) {
  const ac = new AbortController(), t = setTimeout(() => ac.abort(), 6000);
  try {
    const r = await fetch(url, { headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', accept: 'application/json', ...headers }, signal: ac.signal, redirect: 'manual' });
    let j = null; try { j = await r.json(); } catch {}
    return { status: r.status, json: j };
  } finally { clearTimeout(t); }
}
// Thu join game de doc ma loi 267 (kick) / 529 (captcha rejoin) / 277 (disconnected).
// Tra ve: { s:'valid' } | { s:'lock' } | { s:'rejoin' } | { s:'ban' } | { s:'disconnect' } | { s:'error' }
async function rbxJoinCheck(rawCookie) {
  const c = ckClean(rawCookie);
  try {
    const post = tok => fetch('https://gamejoin.roblox.com/v1/join-game', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'user-agent': 'Roblox/WinInet', cookie: '.ROBLOSECURITY=' + c, ...(tok ? { 'x-csrf-token': tok } : {}) },
      body: JSON.stringify({ placeId: +CK_PLACE })
    });
    let r = await post('');
    const isCh = x => !!(x.headers.get('rblx-challenge-type') || x.headers.get('rblx-challenge-id'));
    if (r.status === 403 && !isCh(r)) { const tok = r.headers.get('x-csrf-token'); if (tok) r = await post(tok); }   // lan dau thieu CSRF -> Roblox tra 403 + token, gui lai kem token
    if (r.status === 403) {
      let eb = null; try { eb = await r.clone().json(); } catch {}
      const em = String((eb && eb.errors && eb.errors[0] && eb.errors[0].message) || (eb && eb.message) || '');
      if (isCh(r) || /challenge is required/i.test(em)) return { s: 'rejoin' };   // 403 kem rblx-challenge-* = bat captcha (client hien Error Code 529) -> Captcha rejoin, KHONG phai khoa acc
      if (/token validation|xsrf/i.test(em)) return { s: 'error' };               // van loi CSRF: khong ket luan
      return { s: 'lock' };                                                       // 403 khac: bi khoa (not-approved / moderated)
    }
    if (r.status === 401) return { s: 'invalid' };              // cookie chet
    if (r.status === 429) return { s: 'rate' };
    if (r.status !== 200) return { s: 'error' };
    let j = null; try { j = await r.json(); } catch {}
    if (!j) return { s: 'error' };
    const st = +j.status, msg = String(j.message || '') + ' ' + String((j.statusData && j.statusData.message) || '');
    const code = /(\d{2,3})/.exec(msg);
    const n = code ? +code[1] : 0;
    if (n === 267) return { s: 'kicked' };                      // Error Code 267 = Kick
    if (n === 529) return { s: 'rejoin' };                       // Error Code 529 = Captcha Rejoin
    if (n === 277) return { s: 'disconnect' };                  // Error Code 277 = disconnected
    if (st === 2 && j.jobId) return { s: 'valid' };             // join duoc -> acc khoe
    return { s: 'error' };
  } catch { return { s: 'error' }; }
}
async function rbxCheckCookie(expectName, rawCookie) {
  const c = ckClean(rawCookie);
  if (!CK_RE.test(c)) return { s: 'badformat' };
  let me;
  try { me = await rbxFetchJson('https://users.roblox.com/v1/users/authenticated', { cookie: '.ROBLOSECURITY=' + c }); }
  catch { return { s: 'error' }; }
  if (me.status === 403) return { s: 'lock', code: 403 };   // Roblox khoa tai khoan (not-approved / moderated) - cookie VAN SONG
  if (me.status === 401) return { s: 'invalid' };           // cookie het han / bi logout / bi vo hieu
  if (me.status === 429) return { s: 'rate' };
  if (me.status !== 200 || !me.json || !me.json.id) return { s: 'error', http: me.status };
  if (String(me.json.name || '').toLowerCase() !== String(expectName).toLowerCase()) return { s: 'mismatch' };
  if (CK_JOIN_ON) { const j = await rbxJoinCheck(c); if (j && j.s && j.s !== 'valid' && j.s !== 'error') return j; }   // ma loi 267 / 529 / 277
  return { s: 'valid' };
}
// Hoi Roblox: cookie nay cua acc nao? (dung cho che do chi dan cookie, khong nhap ten)
async function rbxCookieOwner(rawCookie) {
  const c = ckClean(rawCookie);
  if (!CK_RE.test(c)) return { s: 'badformat' };
  let me;
  try { me = await rbxFetchJson('https://users.roblox.com/v1/users/authenticated', { cookie: '.ROBLOSECURITY=' + c }); }
  catch { return { s: 'error' }; }
  if (me.status === 401) return { s: 'invalid' };
  if (me.status === 429) return { s: 'rate' };
  if (me.status !== 200 || !me.json || !me.json.name) return { s: 'error', http: me.status };
  return { s: 'ok', name: String(me.json.name) };
}
function readJson(req, res, cb, limit = 2e4) {
  let body = '', dead = false;
  req.on('data', c => { body += c; if (body.length > limit) { dead = true; req.destroy(); } });
  req.on('end', () => {
    if (dead) return;
    let b; try { b = JSON.parse(body || '{}'); } catch { return send(res, 400, { error: 'bad json' }); }
    cb(b && typeof b === 'object' ? b : {});
  });
}
// ===== CONFIG: doc/ghi config trong Potassium\workspace\HarvardLabs\configs (CHI admin + localhost) =====
// Doi thu muc bang bien moi truong CONFIG_DIR. Mac dinh: %LOCALAPPDATA%\Potassium\workspace\HarvardLabs\configs
// Link workspace nhap tu web (tab Config) duoc ghi vao file workspace.txt canh index.js (1 dong = duong dan). Khoi dong lai tu doc lai, khong can nhap nua.
// Neu khong ghi duoc canh index.js (thu muc chi doc) thi ghi them vao thu muc nguoi dung. CONFIG_DIR (neu dat) luon thang.
// Neu chua co link nao: tu tim %LOCALAPPDATA%\Potassium\workspace\HarvardLabs\configs.
const CFG_WS_FILE = process.env.CFG_WS_FILE || path.join(__dirname, 'workspace.txt');
const CFG_WS_ALT = path.join(os.homedir(), '.dq-tracker-workspace.txt');
const cfgNormPath = p => String(p || '').trim().replace(/^["']+|["']+$/g, '').replace(/(.)\\{2,}/g, '$1\\');   // "C:\\Users\\x" (hai dau \) -> "C:\Users\x"
let CFG_DIR = process.env.CONFIG_DIR || '', CFG_INPUT = '', CFG_SAVED_AT = '';
if (!CFG_DIR) {
  for (const f of [CFG_WS_FILE, CFG_WS_ALT]) {
    let t = ''; try { t = fs.readFileSync(f, 'utf8').replace(/^\uFEFF/, '').split(/\r?\n/).map(x => x.trim()).find(Boolean) || ''; } catch {}
    t = cfgNormPath(t); if (!t) continue;
    const d = cfgResolveDir(t); if (d) { CFG_INPUT = t; CFG_DIR = d; CFG_SAVED_AT = f; break; }
  }
  if (!CFG_DIR) { try { const j = JSON.parse(fs.readFileSync(path.join(__dirname, 'tracker_workspace.json'), 'utf8')); const t = cfgNormPath(j.input || j.dir), d = t && cfgResolveDir(t); if (d) { CFG_INPUT = t; CFG_DIR = d; } } catch {} }
}
if (!CFG_DIR && process.env.LOCALAPPDATA) { const d = cfgResolveDir(path.join(process.env.LOCALAPPDATA, 'Potassium', 'workspace', 'HarvardLabs')); if (d) { CFG_DIR = d; CFG_INPUT = path.dirname(d); } }
const CFG_FILE_RE = /^[A-Za-z0-9_.-]{1,80}\.json$/, CFG_SECRET = new Set(['webhookUrl', 'statsKey']);
const CFG_STATSKEY_RE = /^[\w-]{8,120}$/;
const CFG_UID_CACHE = new Map();
// Kieu gia tri co dung voi gia tri cu khong. Bang rong cua Lua duoc ghi thanh [] hoac {} tuy lan -> [] / {} rong coi nhu nhau (game doc ca hai la bang rong)
const cfgEmptyC = x => x !== null && typeof x === 'object' && Object.keys(x).length === 0;
const cfgTypeOk = (v, old) => typeof v === typeof old && (v === null) === (old === null) && !(typeof v === 'number' && !Number.isFinite(v)) && (Array.isArray(v) === Array.isArray(old) || cfgEmptyC(old) || cfgEmptyC(v));
// Key thuoc 3 o ALT (Farm Settings / Mod Detection) - ap dung cho moi account ALT. Class KHONG nam day (moi account mot class, tu dat theo tracker).
const CFG_ALT_NEW = new Set(['breakRestMin', 'breakRestMax', 'showHud', 'fpsCap', 'fpsCarry', 'fpsLeech']);   // thoi gian nghi cua Breaks: file cu chua co thi tao moi
const CFG_ALT_KEYS = new Set([
  // Farm Settings
  'autoQueue', 'bonusBoss', 'bestGearAuto', 'bestGearPriority', 'bestGearSpells', 'bestGearSkills', 'bestGearOutOfRun', 'pingManual', 'altFollow',
  // Mod Detection
  'modDetect', 'modKick', 'whModDetect', 'modWhitelist', 'modLeaveLegendary', 'modLeaveUltimate', 'modLeaveValT3', 'modLeaveEpic', 'modLeaveValAny', 'modWaitMin', 'modWaitMax', 'breakFarmMin', 'breakFarmMax', 'breakRestMin', 'breakRestMax',
  // Auto Sell
  'autoSell', 'sellAfterGames', 'sellCommon', 'sellUncommon', 'sellRare', 'sellEpic', 'sellLegendary', 'sellUltimate', 'sellKeepLevel', 'sellKeep',
  // Webhook
  'webhook', 'webhookUrl', 'webhookUser', 'whHideName', 'whStarted', 'whWins', 'whRodin', 'whRatio', 'whDeaths', 'whItems', 'whTime', 'whDungeon', 'whGold', 'whWorth', 'whInv', 'whLevel', 'whLate', 'whBest', 'whGodpot', 'whImages', 'whAccent',
  'statsEnabled', 'statsKey', 'whPingMin', 'whPingValT3', 'whPingId',
  // Visuals
  'colorTheme', 'colorThemeBase', 'streamer', 'streamerNameText', 'streamerHealthText', 'streamerLevelText', 'streamerGoldText', 'streamerGemsText', 'streamerXpText', 'streamerBody', 'streamerBodyColor', 'streamerOthers', 'streamerLeaderboard', 'streamerChat',
  'notifications', 'showHud', 'fpsCap', 'fpsCarry', 'fpsLeech', 'fpsUncap', 'renderOff', 'minimap', 'mapButton', 'mapFollow', 'minimapSmooth', 'mapAttackOneColor', 'mapAttackRainbow', 'mapAttackColor', 'mapKey', 'itemStatsAbbrev', 'upgradeCountAbbrev', 'itemGodpot']);
const cfgExSet = arr => new Set((arr || []).map(x => String(x).trim()).filter(x => /^\d{1,20}$/.test(x)));
const cfgAltFiles = ex => fs.readdirSync(CFG_DIR).map(f => /^release_(\d+)\.json$/.exec(f)).filter(m => m && !ex.has(m[1])).map(m => ({ file: m[0], uid: m[1] }));
// File dung chung (khong theo account) nam trong ...\HarvardLabs\data: party / boss raid / mua gamepass cho alt
const PD_FILES = { altfarm: 'altfarm.json', bossraid: 'bossraid_extension.json', altpass: 'alt_gamepasses.json', autobuy: 'tracker_autobuy.json' };
const PD_DEFAULT = { autobuy: { gold2: false } };   // file rieng cua tracker (script Lua khong dung) -> chua co thi coi nhu tat
const pdDir = () => path.join(CFG_DIR, '..', 'data');
function pdRead(n) {
  try { const f = path.join(pdDir(), PD_FILES[n]); return { value: JSON.parse(fs.readFileSync(f, 'utf8').replace(/^﻿/, '')), mtime: Math.floor(fs.statSync(f).mtimeMs) }; } catch { return PD_DEFAULT[n] && CFG_DIR ? { value: { ...PD_DEFAULT[n] }, mtime: 0 } : null; }
}
function pdValidate(n, v, old) {   // tra ve object hop le de ghi, hoac chuoi loi
  if (!v || typeof v !== 'object' || Array.isArray(v)) return 'Du lieu khong hop le.';
  if (n !== 'altfarm') {   // bossraid / altpass: chi nhan key da co, dung kieu
    const o = { ...old };
    for (const [k, x] of Object.entries(v)) {
      if (!(k in old)) continue;
      if (typeof x !== typeof old[k] || (typeof x === 'number' && !Number.isFinite(x))) return 'Sai kieu o key ' + k;
      o[k] = x;
    }
    return o;
  }
  if (!Array.isArray(v.parties) || v.parties.length > 30) return 'Danh sach party khong hop le.';
  const seen = new Set(), S = x => typeof x === 'string' && x.length <= 80;
  for (const p of v.parties) {
    if (!p || typeof p !== 'object' || Array.isArray(p)) return 'Party khong hop le.';
    for (const k of ['map', 'difficulty', 'hardest', 'start', 'cap', 'id']) if (k in p && !S(p[k])) return 'Sai kieu o ' + k;
    for (const k of ['auto', 'replay', 'on', 'hardcore', 'newSave100', 'leechRender', 'swapCarries']) if (k in p && typeof p[k] !== 'boolean') return 'Sai kieu o ' + k;
    for (const k of ['waitMax', 'v']) if (k in p && (typeof p[k] !== 'number' || !Number.isFinite(p[k]) || p[k] < 0 || p[k] > 86400)) return 'Sai kieu o ' + k;
    if (!Array.isArray(p.accounts) || p.accounts.length > 60) return 'Danh sach account khong hop le.';
    let leaders = 0;
    for (const a of p.accounts) {
      if (!a || typeof a !== 'object' || !/^\w{3,20}$/.test(String(a.name || ''))) return 'Ten account khong hop le.';
      if (!['leader', 'fight', 'leech'].includes(a.role)) return 'Role khong hop le: ' + a.role;
      if (typeof a.enabled !== 'boolean') return 'enabled khong hop le.';
      if (a.role === 'leader') leaders++;
      const key = a.name.toLowerCase(); if (seen.has(key)) return 'Account ' + a.name + ' nam trong 2 party.'; seen.add(key);
    }
    if (leaders > 1) return 'Moi party chi co 1 leader.';
  }
  return v;
}
// Nhan link workspace (thu muc workspace\HarvardLabs, workspace, hoac chinh thu muc configs) -> tra ve thu muc configs that
function cfgResolveDir(input) {
  const p0 = cfgNormPath(input);
  if (!p0 || p0.length > 500 || p0.indexOf('\0') >= 0) return '';
  const base = path.resolve(p0);
  for (const c of [base, path.join(base, 'configs'), path.join(base, 'HarvardLabs', 'configs'), path.join(base, 'workspace', 'HarvardLabs', 'configs')]) {
    try { if (fs.statSync(c).isDirectory() && fs.readdirSync(c).some(f => /^release_\d+\.json$/.test(f) || /_shared\.json$/.test(f))) return c; } catch {}
  }
  return '';
}
const cfgCountReleases = () => { try { return fs.readdirSync(CFG_DIR).filter(f => /^release_\d+\.json$/.test(f)).length; } catch { return 0; } };
const CFG_WEBHOOK_RE = /^https:\/\/(?:discord|discordapp)\.com\/api\/webhooks\/[\w\/-]+$/;
function cfgRead(f) {
  const raw = JSON.parse(fs.readFileSync(path.join(CFG_DIR, f), 'utf8').replace(/^\uFEFF/, ''));
  if (Array.isArray(raw)) {   // file *_shared.json: [[key,value],...]
    const pairs = raw.filter(x => Array.isArray(x) && typeof x[0] === 'string');
    return { kind: 'shared', obj: Object.fromEntries(pairs), order: pairs.map(x => x[0]) };
  }
  return { kind: 'release', obj: raw && typeof raw === 'object' ? raw : {}, order: null };
}
function cfgWrite(f, c) {
  const file = path.join(CFG_DIR, f);
  const txt = c.kind === 'shared' ? JSON.stringify(c.order.map(k => [k, c.obj[k]])) : JSON.stringify(c.obj, null, 2);
  try {   // backup ban cu (ngoai thu muc configs de script khong nhat nham)
    const bd = path.join(CFG_DIR, '..', 'tracker_cfg_backup'); fs.mkdirSync(bd, { recursive: true });
    fs.copyFileSync(file, path.join(bd, f + '.bak'));
  } catch {}
  const tmp = file + '.tmp' + process.pid; fs.writeFileSync(tmp, txt); fs.renameSync(tmp, file);
  return Math.floor(fs.statSync(file).mtimeMs);
}
// ================== REJOIN: dan cookie -> tu mo lai game khi bi kick / tat script / end task (khong can tool ngoai) ==================
// Cookie luu chung voi tab Cookie (data\cookies\<owner>.txt). Mo game truc tiep: cookie -> auth ticket (chi gui toi roblox.com qua https) -> RobloxPlayerBeta.exe (Windows).
// KHONG giai captcha: chi mo lai game. Khong bao gio ghi cookie vao log.
const RJ_DEFAULT = { enabled: false, owner: '', placeId: 0, mode: 'place', followUser: '', multi: true, openDelay: 10, batch: 1, exePath: '', accounts: [], disabled: [], verMode: 'newest', verPick: '', hvCheck: true, apiKey: '' };   // apiKey: token ca nhan (trang "Lay Script") - CHI dung khi chay qua App Windows (LOCAL_AGENT) de App goi ve server thuc lay dung trang thai bao cao moi nhat (xem rjRemoteRefresh) - khong co thi App tu doc nham kho du lieu rieng/rong cua App, tuong acc "chua tung bao cao" du script van chay binh thuong tren server, dan den dong+mo lai Roblox lap vo le moi dung RJ_GAP_MS du khong co loi gi   // disabled: ten acc tam tat khoi vong tu dong rejoin (van con trong accounts, bam Rejoin ngay/X van duoc); hvCheck: co bat phat hien Harvard bi dung/crash khong (tat neu dung script khac, khong phai Harvard - tranh dong nham tab)
const RJ_FRESH_MS = 20e3;   // script bao cao trong 20s gan day = dang chay that (hien tren UI)
const RJ_OFFLINE_MS = 20e3, RJ_GAP_MS = 180e3;   // khong bao cao 20s = rot game / tat script (script bao cao moi 1s nen 20s la rat an toan, phat hien nhanh hon ban cu 60s); sau khi mo thi 3 phut moi mo lai (client can thoi gian khoi dong) - co dinh, khong hien tren UI
const HV_STUCK_MS = 20e3;   // "nhip tim" cua Harvard (hvAt) dung yen qua 20s (du script tracker van bao cao binh thuong) -> coi nhu Harvard bi dung/crash rieng, can dong+mo lai client - xem rjState
const RJ_SILENT_HARD_MS = 60e3;   // tran tren CUNG cho truong hop "Roblox bao van dang trong game, script tracker da tung chay" (binh thuong KHONG bao gio tu dong dong vi ly do nay - xem rjTick): neu tracker im lang LIEN TUC qua 1 phut, du da tung chay duoc, van coi la treo that va dong+mo lai
// Kiem tra "script khong len" (tuong tu Yummy): CHI danh gia dung 1 lan duy nhat tinh tu lan mo gan nhat (hoac luc
// tracker khoi dong, neu acc khong phai do tool mo) - neu Roblox van bao dang trong game nhung tu luc do toi gio
// CHUA TUNG co bao cao nao thanh cong (rat co the auto-exec loi, khong inject duoc script) thi moi dong+mo lai.
// Mot khi da tung bao cao thanh cong SAU moc do it nhat 1 lan, coi nhu script chac chan da chay duoc -> tu do VE SAU
// KHONG BAO GIO tu dong dong acc nay nua vi ly do im lang, du im lang bao lau (dang danh boss, lag, mat mang tam
// thoi...). Tranh dung kieu "im lang X phut la kill" ap dung cho ca acc dang chay on dinh lau dai (gay kill oan).
const RJ_NOTLOADED_GRACE_MS = 30e3;   // cho toi thieu 30s sau lan mo / sau khi tracker khoi dong truoc khi ket luan "script khong len" - co dinh, khong hien tren UI
const RJ = { st: new Map(), log: [], lastLaunch: 0, busy: false, boot: Date.now(), pids: new Map(), mutex: null, remote: null, remoteAt: 0, remoteErr: '' };
const RJ_REMOTE_TTL = 5e3;   // App Windows (LOCAL_AGENT) tu lay du lieu bao cao THAT tu server chinh (khong dung kho du lieu rieng/rong cua App) - xem ghi chu o apiKey
async function rjRemoteRefresh(cfg) {
  if (!LOCAL_AGENT || !cfg.apiKey) { RJ.remote = null; return; }
  const now = Date.now();
  if (RJ.remote && now - RJ.remoteAt < RJ_REMOTE_TTL) return;
  RJ.remoteAt = now;
  try {
    const r = await fetch(LOCAL_AGENT_ORIGIN + '/api/accounts', { headers: { 'x-api-key': cfg.apiKey } });
    if (!r.ok) { const msg = 'Server tu choi (HTTP ' + r.status + ') - kiem tra lai API Key.'; if (RJ.remoteErr !== msg) rjLog(msg); RJ.remoteErr = msg; return; }
    const j = await r.json(), prev = RJ.remote || {}, m = {};
    for (const x of (Array.isArray(j.accounts) ? j.accounts : [])) {
      if (!x || !x.name) continue;
      const key = String(x.name).toLowerCase(), p = prev[key], hv = x.hvAt;
      const hvSeen = hv != null || !!(p && p.hvSeen);
      const hvChangedAt = (p && hv === p._hvAt) ? (p.hvChangedAt || now) : now;
      m[key] = { updated: x.updated || 0, rec: x, hvSeen, hvChangedAt, _hvAt: hv };
    }
    RJ.remote = m; if (RJ.remoteErr) rjLog('Da ket noi lai duoc voi server (lay du lieu that).'); RJ.remoteErr = '';
  } catch (e) {
    const msg = 'Khong goi duoc server de lay du lieu that: ' + String((e && e.message) || e).slice(0, 100);
    if (RJ.remoteErr !== msg) rjLog(msg); RJ.remoteErr = msg;
  }
}
// Cau hinh Rejoin luu o VI TRI CO DINH (canh cookie: <thu muc tracker>\data\tracker_rejoin.json), khong phu thuoc duong dan config / workspace -> dong tracker mo lai van con
const rjFile = () => path.join(path.dirname(DATA_FILE), 'data', 'tracker_rejoin.json');
const rjLegacyFile = () => { try { return CFG_DIR ? path.join(pdDir(), 'tracker_rejoin.json') : ''; } catch { return ''; } };   // ban cu (luu canh thu muc config)
function rjReadFile(f) { const raw = JSON.parse(fs.readFileSync(f, 'utf8').replace(/^\uFEFF/, '')), c = { ...RJ_DEFAULT }; for (const k of Object.keys(RJ_DEFAULT)) if (k in raw && typeof raw[k] === typeof RJ_DEFAULT[k]) c[k] = raw[k]; return c; }
function rjCfg() {
  const f = rjFile(), cand = [f, f + '.bak', rjLegacyFile()].filter(Boolean);
  for (const x of cand) { try { if (fs.existsSync(x)) return rjReadFile(x); } catch {} }
  return { ...RJ_DEFAULT };
}
function rjWrite(c) {   // ghi tam roi doi ten (khong bao gio de file hong giua chung) + giu 1 ban .bak
  const f = rjFile(); fs.mkdirSync(path.dirname(f), { recursive: true });
  const tmp = f + '.tmp', txt = JSON.stringify(c, null, 2);
  fs.writeFileSync(tmp, txt); try { if (fs.existsSync(f)) fs.copyFileSync(f, f + '.bak'); } catch {}
  fs.renameSync(tmp, f);
}
function rjLog(m) { RJ.log.push({ t: Date.now(), m: String(m).slice(0, 300) }); if (RJ.log.length > 80) RJ.log.shift(); console.log('[rejoin] ' + m); }
// ===== BANG SO LAN: dem so lan Rejoin (captcha) / Kick / Mat ket noi da kich hoat mo lai Roblox, luu rieng de qua restart khong mat =====
const rjStatsFile = () => path.join(path.dirname(DATA_FILE), 'data', 'tracker_rejoin_stats.json');
let RJ_STATS_DB = null, rjStatsTimer = null;
function rjStatsDb() { if (RJ_STATS_DB) return RJ_STATS_DB; try { RJ_STATS_DB = JSON.parse(fs.readFileSync(rjStatsFile(), 'utf8').replace(/^﻿/, '')); } catch { RJ_STATS_DB = {}; } if (!RJ_STATS_DB || typeof RJ_STATS_DB !== 'object') RJ_STATS_DB = {}; return RJ_STATS_DB; }
function rjStatsSave() {
  if (rjStatsTimer) return;
  rjStatsTimer = setTimeout(() => { rjStatsTimer = null; try { const f = rjStatsFile(); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f + '.tmp', JSON.stringify(RJ_STATS_DB)); fs.renameSync(f + '.tmp', f); } catch (e) { console.error('[rejoin-stats] luu loi: ' + e.message); } }, 400);
}
process.on('exit', () => { try { if (rjStatsTimer && RJ_STATS_DB) { const f = rjStatsFile(); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f, JSON.stringify(RJ_STATS_DB)); } } catch {} });
function rjStatsEntry(owner) { const d = rjStatsDb(); return d[owner] || (d[owner] = { rejoin: 0, kicked: 0, disconnect: 0 }); }
function rjStatsBump(owner, why) {   // 'rejoin' = TONG so lan tu dong mo lai (moi ly do, ke ca tab bi kill / mat tab im lang); kicked/disconnect dem rieng khi biet ro ly do
  if (!owner) return;
  const e = rjStatsEntry(owner);
  e.rejoin = (e.rejoin || 0) + 1;
  if (why === 'kicked' || why === 'disconnect') e[why] = (e[why] || 0) + 1;
  rjStatsSave();
}
function rjClean(v, old) {   // tra ve config hop le hoac chuoi loi (danh sach account di qua endpoint rieng, khong sua o day)
  if (!v || typeof v !== 'object' || Array.isArray(v)) return 'Du lieu khong hop le.';
  const o = { ...old }, int = (x, lo, hi) => Number.isInteger(x) && x >= lo && x <= hi;
  for (const k of ['enabled', 'multi', 'hvCheck']) if (k in v) { if (typeof v[k] !== 'boolean') return 'Sai kieu o ' + k; o[k] = v[k]; }
  if ('batch' in v) { if (typeof v.batch !== 'number' || !Number.isFinite(v.batch)) return 'Sai kieu o batch'; o.batch = Math.min(20, Math.max(1, Math.round(v.batch))); }
  if ('openDelay' in v) { if (typeof v.openDelay !== 'number' || !Number.isFinite(v.openDelay)) return 'Sai kieu o openDelay'; o.openDelay = Math.min(600, Math.max(0, Math.round(v.openDelay))); }
  if ('placeId' in v) { if (!int(v.placeId, 0, 9e15)) return 'PlaceId khong hop le.'; o.placeId = v.placeId; }
  if ('mode' in v) { if (v.mode !== 'place' && v.mode !== 'party') return 'mode phai la place hoac party.'; o.mode = v.mode; }
  if ('followUser' in v) { if (typeof v.followUser !== 'string' || !/^\w{0,20}$/.test(v.followUser)) return 'Ten leader khong hop le.'; o.followUser = v.followUser; }
  if ('verMode' in v) { if (!['newest', 'latest', 'pick'].includes(v.verMode)) return 'verMode khong hop le.'; o.verMode = v.verMode; }
  if ('verPick' in v) { if (typeof v.verPick !== 'string' || (v.verPick && !RJ_VER_RE.test(v.verPick))) return 'Ten ban phai dang version-xxxxxxxx.'; o.verPick = v.verPick; }
  if ('exePath' in v) { if (typeof v.exePath !== 'string' || v.exePath.length > 260 || /["\r\n<>|]/.test(v.exePath) || (v.exePath.trim() && !/\.exe$/i.test(v.exePath.trim()))) return 'Duong dan Roblox phai la file .exe (hoac de trong de tu tim).'; o.exePath = v.exePath.trim(); }
  if ('apiKey' in v) { if (typeof v.apiKey !== 'string' || v.apiKey.length > 120 || /["\r\n\s]/.test(v.apiKey)) return 'API Key khong hop le.'; o.apiKey = v.apiKey.trim(); }
  return o;
}
// ===== VERSIONS: chon ban Roblox (thu muc Versions\version-xxxx) de mo client =====
const RJ_VER_RE = /^version-[0-9a-f]{8,32}$/i;
const RJV = { latest: { id: '', num: '', at: 0, err: '' }, busy: false, ver: new Map() };   // ver: ten account -> ban da mo
function rjVerRoots() { return [process.env.LOCALAPPDATA && path.join(process.env.LOCALAPPDATA, 'Roblox', 'Versions'), process.env.PROGRAMFILES && path.join(process.env.PROGRAMFILES, 'Roblox', 'Versions'), process.env['PROGRAMFILES(X86)'] && path.join(process.env['PROGRAMFILES(X86)'], 'Roblox', 'Versions')].filter(Boolean); }
function rjInstalled() {   // cac ban da cai, moi nhat truoc
  const out = [];
  for (const r of rjVerRoots()) { try { for (const d of fs.readdirSync(r)) { if (!RJ_VER_RE.test(d)) continue; const f = path.join(r, d, 'RobloxPlayerBeta.exe'); if (fs.existsSync(f)) out.push({ id: d, exe: f, mtime: fs.statSync(f).mtimeMs }); } } catch {} }
  return out.sort((a, b) => b.mtime - a.mtime);
}
function rjFetchLatest() {
  return new Promise(resolve => {
    if (RJV.busy) return resolve(); RJV.busy = true;
    const done = () => { RJV.busy = false; resolve(); };
    try {
      const rq = require('https').get('https://clientsettings.roblox.com/v2/client-version/WindowsPlayer', { timeout: 8000, headers: { 'user-agent': 'DQR-Tracker' } }, rs => {
        let b = ''; rs.setEncoding('utf8'); rs.on('data', d => { if (b.length < 20000) b += d; });
        rs.on('end', () => { try { const j = JSON.parse(b); if (RJ_VER_RE.test(String(j.clientVersionUpload || ''))) RJV.latest = { id: j.clientVersionUpload, num: String(j.version || '').slice(0, 40), at: Date.now(), err: '' }; else RJV.latest.err = 'Roblox trả về dữ liệu lạ'; } catch { RJV.latest.err = 'Không đọc được phản hồi của Roblox'; } done(); });
      });
      rq.on('timeout', () => { RJV.latest.err = 'Hết thời gian chờ Roblox'; rq.destroy(); });
      rq.on('error', e => { RJV.latest.err = String((e && e.message) || e).slice(0, 80); done(); });
    } catch (e) { RJV.latest.err = String((e && e.message) || e).slice(0, 80); done(); }
  });
}
function rjPickBuild(cfg) {   // ban se dung cho lan mo toi: { id, exe, why, warn }
  const inst = rjInstalled(), by = id => inst.find(x => x.id.toLowerCase() === String(id).toLowerCase());
  if (cfg.verMode === 'latest') { const x = RJV.latest.id && by(RJV.latest.id); if (x) return { ...x, why: 'latest' }; if (inst[0]) return { ...inst[0], why: 'newest', warn: RJV.latest.id ? 'Bản mới nhất của Roblox (' + RJV.latest.id + ') chưa được cài - đang dùng bản mới nhất đã cài.' : 'Chưa biết bản mới nhất của Roblox - đang dùng bản mới nhất đã cài.' }; }
  else if (cfg.verMode === 'pick' && cfg.verPick) { const x = by(cfg.verPick); if (x) return { ...x, why: 'pick' }; if (inst[0]) return { ...inst[0], why: 'newest', warn: 'Bản ' + cfg.verPick + ' không còn trên máy - đang dùng bản mới nhất đã cài.' }; }
  return inst[0] ? { ...inst[0], why: 'newest' } : null;
}
function rjFindExe(cfg) {
  if (cfg.exePath) return fs.existsSync(cfg.exePath) ? cfg.exePath : '';
  const b = rjPickBuild(cfg); return b ? b.exe : '';
}
function rjHoldMutex() {   // giu ROBLOX_singletonMutex de mo duoc nhieu client cung luc (chi Windows)
  if (process.platform !== 'win32' || (RJ.mutex && RJ.mutex.exitCode === null)) return;
  try {
    RJ.mutex = require('child_process').spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-WindowStyle', 'Hidden', '-Command', "$m=New-Object System.Threading.Mutex($true,'ROBLOX_singletonMutex');Start-Sleep -Seconds 2147483"], { stdio: 'ignore', windowsHide: true });
    RJ.mutex.on('error', () => { RJ.mutex = null; }); process.on('exit', () => { try { RJ.mutex && RJ.mutex.kill(); } catch {} });
  } catch { RJ.mutex = null; }
}
async function rjPost(url, headers, body) {
  const ac = new AbortController(), t = setTimeout(() => ac.abort(), 8000);
  try { return await fetch(url, { method: 'POST', headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', ...headers }, body: body || '{}', signal: ac.signal, redirect: 'manual' }); } finally { clearTimeout(t); }
}
async function rjTicket(cookie, placeId) {   // -> { ticket } | { err }
  try {
    const ck = '.ROBLOSECURITY=' + cookie;
    const a = await rjPost('https://auth.roblox.com/v2/logout', { cookie: ck, 'content-type': 'application/json' });
    const csrf = a.headers.get('x-csrf-token'); if (!csrf) return { err: 'Khong lay duoc CSRF (cookie het han?)' };
    const b = await rjPost('https://auth.roblox.com/v1/authentication-ticket', { cookie: ck, 'x-csrf-token': csrf, 'content-type': 'application/json', referer: 'https://www.roblox.com/games/' + placeId + '/', origin: 'https://www.roblox.com' });
    const t = b.headers.get('rbx-authentication-ticket');
    return t ? { ticket: t } : { err: 'Roblox khong cap ticket (HTTP ' + b.status + ') - cookie het han hoac bi chan.' };
  } catch { return { err: 'Khong goi duoc Roblox.' }; }
}
async function rjUserId(name) {
  const hit = CFG_UID_CACHE.get(String(name).toLowerCase()); if (hit) return hit;
  try {
    const r = await rjPost('https://users.roblox.com/v1/usernames/users', { 'content-type': 'application/json' }, JSON.stringify({ usernames: [name], excludeBannedUsers: false }));
    const j = await r.json(), id = j && j.data && j.data[0] && j.data[0].id; if (id) { CFG_UID_CACHE.set(String(name).toLowerCase(), String(id)); return String(id); }
  } catch {}
  return '';
}
// Roblox co dang chay client cua account nay khong? (presence API: 2 = dang trong game) -> 'in' | 'on' | 'off' | '' (khong ro)
// Dung de KHONG mo them client thu 2 cho account dang chay (mo lan 2 se bi Roblox kick client cu: Error 273)
async function rjPresence(cookie, name) {
  try {
    const uid = await rjUserId(name); if (!uid) return '';
    const ck = '.ROBLOSECURITY=' + cookie, body = JSON.stringify({ userIds: [+uid] }), url = 'https://presence.roblox.com/v1/presence/users';
    let r = await rjPost(url, { cookie: ck, 'content-type': 'application/json' }, body);
    if (r.status === 403) { const tok = r.headers.get('x-csrf-token'); if (tok) r = await rjPost(url, { cookie: ck, 'x-csrf-token': tok, 'content-type': 'application/json' }, body); }
    if (r.status !== 200) return '';
    const j = await r.json(), t = j && j.userPresences && j.userPresences[0] && j.userPresences[0].userPresenceType;
    return t === 2 ? 'in' : t === 1 ? 'on' : t === 0 ? 'off' : '';
  } catch { return ''; }
}
// Mac dinh moi client Roblox do tool mo: tat tieng + do hoa thap nhat (ghi vao GlobalBasicSettings_13.xml cua Roblox truoc khi mo; chi sua dung 3 gia tri nay)
function rjLowSettings() {
  try {
    const dir = process.env.LOCALAPPDATA && path.join(process.env.LOCALAPPDATA, 'Roblox'); if (!dir || !fs.existsSync(dir)) return;
    const f = path.join(dir, 'GlobalBasicSettings_13.xml'); let x = fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : '';
    const want = [['float', 'MasterVolume', '0'], ['int', 'GraphicsQualityLevel', '1'], ['token', 'SavedQualityLevel', '1'], ['bool', 'Fullscreen', 'false']];
    if (!x) x = '<roblox xmlns:xmime="http://www.w3.org/2005/05/xmlmime" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.roblox.com/roblox.xsd" version="4">\r\n\t<Item class="UserGameSettings" referent="RBX0">\r\n\t\t<Properties>\r\n\t\t</Properties>\r\n\t</Item>\r\n</roblox>';
    let y = x;
    for (const [ty, k, v] of want) {
      const re = new RegExp('(<(?:float|int|token|double|bool) name="' + k + '">)[^<]*(</(?:float|int|token|double|bool)>)');
      if (re.test(y)) y = y.replace(re, '$1' + v + '$2');
      else if (/<\/Properties>/.test(y)) y = y.replace(/<\/Properties>/, '\t<' + ty + ' name="' + k + '">' + v + '</' + ty + '>\r\n\t\t</Properties>');
    }
    if (y !== x) fs.writeFileSync(f, y);
  } catch {}
}
async function rjLaunch(cfg, name, rec) {   // mo lai game cho 1 account
  const cookie = (await ckLoad(cfg.owner))[name];
  if (!cookie) return { ok: false, msg: 'Khong co cookie cua ' + name + ' (dan lai o phan Them cookie).' };
  const place = cfg.placeId || (rec && /^\d{1,16}$/.test(String(rec.placeId || '')) ? +rec.placeId : 0);
  if (!place) return { ok: false, msg: 'Chua co PlaceId (dien o cau hinh).' };
  if (process.platform !== 'win32') return { ok: false, msg: 'Mo game chi chay tren Windows.' };
  const exe = rjFindExe(cfg); if (!exe) return { ok: false, msg: 'Khong tim thay RobloxPlayerBeta.exe - dien duong dan trong cau hinh.' };
  const tk = await rjTicket(cookie, place); if (tk.err) return { ok: false, msg: tk.err };
  const bt = String(Math.floor(Math.random() * 1e11) + 1e11), jid = crypto.randomUUID();
  let req = 'request=RequestGame&browserTrackerId=' + bt + '&placeId=' + place + '&isPlayTogetherGame=false', how = 'Place ' + place;
  if (cfg.mode === 'party' && cfg.followUser && cfg.followUser.toLowerCase() !== name.toLowerCase()) {
    const uid = await rjUserId(cfg.followUser);
    if (uid) { req = 'request=RequestFollowUser&browserTrackerId=' + bt + '&userId=' + uid + '&isPlayTogetherGame=false'; how = 'Follow ' + cfg.followUser; }
  }
  const url = 'https://assetgame.roblox.com/game/PlaceLauncher.ashx?' + req + '&joinAttemptId=' + jid + '&joinAttemptOrigin=PlayButton';
  const old = RJ.pids.get(name); if (old) { try { if (process.platform === 'win32') require('child_process').spawnSync('taskkill', ['/PID', String(old), '/T', '/F'], { stdio: 'ignore', timeout: 5000 }); else process.kill(old); rjLog(name + ': da kill client cu (script khong chay).'); } catch {} RJ.pids.delete(name); }   // dong client cu do tool mo (script da tat / treo)
  if (cfg.multi) rjHoldMutex();
  rjLowSettings();
  try {
    const child = require('child_process').spawn(exe, ['--app', '-t', tk.ticket, '-j', url, '-b', bt, '--launchtime=' + Date.now(), '--rloc', 'en_us', '--gloc', 'en_us'], { detached: true, stdio: 'ignore' });
    child.on('error', () => {}); child.on('exit', () => { if (RJ.pids.get(name) === child.pid) RJ.pids.delete(name); }); child.unref();
    if (child.pid) { RJ.pids.set(name, child.pid); try { RJV.ver.set(name, cfg.exePath ? 'custom' : path.basename(path.dirname(exe))); } catch {} }
  } catch (e) { return { ok: false, msg: 'Khong mo duoc Roblox: ' + String((e && e.message) || e).slice(0, 100) }; }
  return { ok: true, msg: 'Da mo Roblox (' + how + ')' };
}
// Account can mo lai game khi: bi kick / mat ket noi / captcha rejoin (co do script bao) hoac khong con bao cao ve tracker (rot game, tat script, end task)
// Tim ban ghi cua account (uu tien kho cua owner; khong co thi tim khong phan biet hoa/thuong trong moi kho - phong khi script bao bang key khac)
function rjEntry(owner, name) {
  if (LOCAL_AGENT && RJ.remote) { const r = RJ.remote[String(name).toLowerCase()]; if (r) return r; }   // App Windows: uu tien du lieu THAT lay tu server (xem rjRemoteRefresh) thay vi kho rieng/rong cua App
  const own = owners[owner]; if (own && Object.prototype.hasOwnProperty.call(own, name)) return own[name];
  const ln = String(name).toLowerCase(); let best = null;
  for (const o of Object.keys(owners)) { const b = owners[o]; for (const k of Object.keys(b)) if (k.toLowerCase() === ln && (!best || (b[k].updated || 0) > (best.updated || 0))) best = b[k]; }
  return best;
}
function rjState(e, now, cfg) {
  const rec = e && e.rec;
  if (rec && ['rejoin', 'disconnect', 'kicked'].includes(rec.flag)) return { why: rec.flag };
  const last = Math.max((e && e.updated) || 0, RJ.boot);   // moi khoi dong lai: cho cac account kip bao cao
  if (now - last > RJ_OFFLINE_MS) return { why: 'offline' };
  // Script tracker VAN bao cao binh thuong (khong offline) nhung "nhip tim" cua Harvard (hvAt) dung yen qua lau ->
  // Harvard (farm script rieng) bi dung/crash rieng, acc khong con farm gi ca du Roblox/tracker van song - can
  // dong+mo lai client de Harvard duoc inject lai. Chi bao khi DA TUNG thay Harvard chay (e.hvSeen), tranh bao nham
  // cho acc khong dung Harvard hoac chua kip load xong. cfg.hvCheck===false: nguoi dung dung script khac (khong
  // phai Harvard) co the tat han o UI de khong bi dong nham tab du lo co tin hieu gi do trung ten bien.
  if ((!cfg || cfg.hvCheck !== false) && e && e.hvSeen && now - (e.hvChangedAt || now) > HV_STUCK_MS) return { why: 'harvard-stuck' };
  return null;
}
async function rjTick() {
  if (RJ.busy) return; RJ.busy = true;
  try {
    const cfg = rjCfg();
    const active = cfg.accounts.filter(n => !(cfg.disabled || []).includes(n));   // bo qua acc dang tat (van nam trong accounts, chi khong tu dong rejoin)
    if (!cfg.enabled || !cfg.owner || !active.length) return;
    await rjRemoteRefresh(cfg);
    if (cfg.multi) rjHoldMutex();   // bat Auto rejoin la tracker giu san khoa multi-client (khong can app multi rieng)
    const now = Date.now(), mine = owners[cfg.owner] || {};
    {   // cap nhat trang thai that (presence) cho account khong thay script bao cao - de hien dung tren UI va khong mo trung
      const cks0 = await ckLoad(cfg.owner); let n = 0;
      for (const name of active) {
        const e0 = rjEntry(cfg.owner, name), st0 = RJ.st.get(name) || { last: 0, result: '' };
        if (e0 && e0.updated && now - e0.updated <= RJ_FRESH_MS) continue;
        if (st0.presAt && now - st0.presAt < 5e3) continue;
        if (n++ >= 3) break;
        st0.pres = cks0[name] ? await rjPresence(cks0[name], name) : ''; st0.presAt = Date.now(); RJ.st.set(name, st0);
      }
    }
    const cands = [];
    for (const name of active) {
      const e = rjEntry(cfg.owner, name), s = rjState(e, now, cfg), st = RJ.st.get(name) || { last: 0, result: '' };
      if (!s || now - st.last < RJ_GAP_MS) continue;
      cands.push({ name, rec: e && e.rec, st, why: s.why, upd: (e && e.updated) || 0 });
    }
    cands.sort((x, y) => x.st.last - y.st.last);
    const cks = cands.length ? await ckLoad(cfg.owner) : {};
    if (now - RJ.lastLaunch < cfg.openDelay * 1000) return;   // delay giua 2 dot mo Roblox (cau hinh, mac dinh 10 giay)
    const picks = [];
    for (const c of cands) {
      if (picks.length >= cfg.batch) break;   // mo toi da N client cung luc (cau hinh)
      if (c.why === 'offline') {   // khong thay script bao cao: co the client van dang chay (script dang tai / da tat) -> hoi Roblox truoc khi mo them
        if (!c.st.presAt || now - c.st.presAt > 5e3) { c.st.pres = cks[c.name] ? await rjPresence(cks[c.name], c.name) : ''; c.st.presAt = Date.now(); RJ.st.set(c.name, c.st); }
        if (c.st.pres !== 'on' && c.st.pres !== 'off') {
          if (c.st.pres === 'in') {
            const baseline = Math.max(c.st.last, RJ.boot);   // lan mo gan nhat cua tool (hoac luc tracker khoi dong, neu acc khong phai do tool mo)
            if (c.upd > baseline) {
              // Da co bao cao thanh cong SAU moc nay -> script chac chan da tung chay duoc, hien chi dang tam im ->
              // KHONG tu dong dong vi ly do nay, du im lang mot luc (danh boss, lag, mat mang tam thoi...) - TRU KHI
              // im lang LIEN TUC qua RJ_SILENT_HARD_MS (1 phut), luc do coi nhu treo that, van dong+mo lai binh thuong.
              if (now - c.upd < RJ_SILENT_HARD_MS) {
                c.st.result = 'Dang trong game (script da tung chay, tam im) - khong dong / khong mo lai';
                continue;
              }
              c.why = 'notloaded';
              c.st.result = 'Tracker im lang qua 1 phut (du da tung chay duoc) - dong va mo lai.';
            } else if (now - baseline < RJ_NOTLOADED_GRACE_MS) {
              c.st.result = 'Dang trong game (script chua kip bao lan dau) - khong dong / khong mo lai';
              continue;
            } else {
              // Qua thoi gian cho ma VAN CHUA TUNG co bao cao nao ke tu lan mo/khoi dong nay, du Roblox van bao dang
              // trong game -> rat co the auto-exec loi, script khong duoc inject -> dong client cu, mo lai de thu
              // inject lan nua (tuong tu Yummy: chi kill dung 1 lan khi script that su "khong len" sau khi mo).
              c.why = 'notloaded';
              c.st.result = 'Script khong len sau khi mo (chua bao cao lan nao) - dong va mo lai.';
            }
          } else { c.st.result = 'Chua hoi duoc Roblox - tam thoi khong mo lai'; continue; }
        }
      }
      picks.push(c);
    }
    if (!picks.length) return;
    RJ.lastLaunch = Date.now();
    for (let i = 0; i < picks.length; i++) {
      const pick = picks[i]; pick.st.last = Date.now(); RJ.st.set(pick.name, pick.st);
      const r = await rjLaunch(cfg, pick.name, pick.rec); pick.st.result = r.msg;
      rjLog(pick.name + ' [' + pick.why + ']: ' + (r.ok ? 'OK - ' : 'LOI - ') + r.msg);
      rjStatsBump(cfg.owner, pick.why);   // dem vao Bang so lan (Rejoin / Kick / Mat ket noi)
      if (i < picks.length - 1) await new Promise(rs => setTimeout(rs, 1500));   // cach nhau ~1.5s trong cung 1 dot
    }
    RJ.lastLaunch = Date.now();
  } catch (e) { rjLog('Loi: ' + String((e && e.message) || e)); } finally { RJ.busy = false; }
}
async function rjWho(cookie) {   // cookie -> ten Roblox (thu lai khi Roblox tra 429 / 5xx / timeout)
  let last = { err: 'Khong goi duoc Roblox' };
  for (let i = 0; i < 3; i++) {
    if (i) await new Promise(r => setTimeout(r, 1500 * i));
    try {
      const me = await rbxFetchJson('https://users.roblox.com/v1/users/authenticated', { cookie: '.ROBLOSECURITY=' + cookie });
      if (me.status === 401) return { err: 'Cookie het han / sai' };
      if (me.status === 403) return { err: 'Roblox khoa tai khoan (lock)' };
      if (me.status === 429 || me.status >= 500) { last = { err: 'Roblox ban / gioi han (' + me.status + '), thu lai sau' }; continue; }
      if (me.status !== 200 || !me.json || !me.json.name) return { err: 'Roblox tra ' + me.status };
      return { name: String(me.json.name) };
    } catch { last = { err: 'Khong goi duoc Roblox (timeout / mang)' }; }
  }
  return last;
}
function rejoinApi(req, res, p, u, a) {   // chi admin + localhost
  if (!isLoopbackReq(req)) return send(res, 403, { error: 'local_only', message: 'Rejoin chi dung duoc tu App Windows (Tai App) vi can dieu khien Roblox tren chinh may ban - trinh duyet thuong khong lam duoc.' });
  const mine = bucket(a.owner);
  if (p === '/api/rejoin/state' && req.method === 'GET') return (async () => {
    const cfg = rjCfg(); await rjRemoteRefresh(cfg);
    const now = Date.now(), items = [], pub = { ...cfg }; delete pub.owner; delete pub.accounts; delete pub.disabled; pub.remoteErr = RJ.remote ? '' : (cfg.apiKey ? RJ.remoteErr : '');
    const dis = new Set(cfg.disabled || []);
    for (const name of cfg.accounts) {
      const e = rjEntry(cfg.owner || a.owner, name), s = rjState(e, now, cfg), st = RJ.st.get(name);
      const fresh = !!(e && e.updated && now - e.updated <= RJ_FRESH_MS), flagged = !!(s && s.why !== 'offline'), pr = st && st.pres, off = dis.has(name);
      const view = off ? 'off' : flagged ? s.why : fresh ? 'run' : !cfg.enabled ? 'idle' : pr === 'in' ? 'ingame' : (pr === 'on' || pr === 'off') ? 'out' : 'check';
      items.push({ name, why: flagged ? s.why : null, view, online: fresh, last: st ? st.last : 0, result: st ? st.result : '', off });
    }
    return send(res, 200, { cfg: pub, mine: !cfg.owner || cfg.owner === a.owner, items, log: RJ.log.slice(-50), counts: rjStatsEntry(cfg.owner || a.owner), now });
  })();
  if (p === '/api/rejoin/toggle' && req.method === 'POST') return readJson(req, res, b => {   // bat/tat 1 acc khoi vong tu dong rejoin (van con trong danh sach, khong xoa)
    const cfg = rjCfg(), name = s(b.name, 20);
    if (!name || !cfg.accounts.includes(name)) return send(res, 400, { error: 'bad_name', message: 'Khong tim thay account.' });
    const on = b.on !== false, dis = new Set(cfg.disabled || []);
    if (on) dis.delete(name); else dis.add(name);
    cfg.disabled = [...dis];
    try { rjWrite(cfg); } catch (e) { return send(res, 500, { error: 'write_failed', message: String((e && e.message) || e) }); }
    rjLog(name + ': ' + (on ? 'bật lại' : 'tạm tắt') + ' auto rejoin.'); send(res, 200, { ok: true, off: !on });
  });
  if (p === '/api/rejoin/config' && req.method === 'POST') return readJson(req, res, b => {
    const out = rjClean(b, rjCfg()); if (typeof out === 'string') return send(res, 400, { error: 'bad_value', message: out });
    out.owner = a.owner;   // loop chi chay cho account cua admin dang luu cau hinh
    try { rjWrite(out); } catch (e) { return send(res, 500, { error: 'write_failed', message: String((e && e.message) || e) }); }
    rjLog('Da luu cau hinh (' + (out.enabled ? 'BAT' : 'TAT') + ').'); send(res, 200, { ok: true });
  }, 6e4);
  if (p === '/api/rejoin/statsreset' && req.method === 'POST') {
    const e = rjStatsEntry(a.owner); e.rejoin = 0; e.kicked = 0; e.disconnect = 0; rjStatsSave();
    rjLog('Da reset Bang so lan (Rejoin/Kick/Mat ket noi).'); return send(res, 200, { ok: true, counts: e });
  }
  if (p === '/api/rejoin/versions' && req.method === 'GET') return (async () => {
    if (u.searchParams.get('check') === '1' || !RJV.latest.at || Date.now() - RJV.latest.at > 6e5) await rjFetchLatest();
    const cfg = rjCfg(), inst = rjInstalled(), use = cfg.exePath ? null : rjPickBuild(cfg), run = {};
    monEnsure(); for (const x of ((MON.last && MON.last.rbx) || [])) { const v = x.ver || 'Không rõ'; run[v] = (run[v] || 0) + 1; }
    send(res, 200, { win: process.platform === 'win32', custom: cfg.exePath || '', installed: inst.map(x => ({ id: x.id, mtime: x.mtime })), use: use ? { id: use.id, why: use.why, warn: use.warn || '' } : null, latest: RJV.latest, running: run });
  })();
  if (p === '/api/rejoin/test' && req.method === 'POST') {   // kiem tra: Windows + tim thay Roblox
    const cfg = rjCfg();
    if (process.platform !== 'win32') return send(res, 200, { ok: false, message: 'Mo game chi chay tren Windows (tracker dang chay tren ' + process.platform + ').' });
    const exe = rjFindExe(cfg); if (!exe) return send(res, 200, { ok: false, message: 'Khong tim thay RobloxPlayerBeta.exe. Cai Roblox ban thuong (khong phai Microsoft Store) hoac dien duong dan .exe.' });
    return send(res, 200, { ok: true, exe });
  }
  if (p === '/api/rejoin/cookies' && req.method === 'POST') return readJson(req, res, async b => {   // dan cookie -> luu (chung voi tab Cookie) + theo doi
    const cfg = rjCfg(), list = (Array.isArray(b.cookies) ? b.cookies : []).map(ckClean).filter(Boolean).slice(0, 60), out = [];
    const store = await ckLoad(a.owner); let storeDirty = false, cfgDirty = false;
    for (const c of list) {
      if (!CK_RE.test(c)) { out.push({ ok: false, msg: 'Cookie sai dinh dang.' }); continue; }
      const w = await rjWho(c); if (w.err) { out.push({ ok: false, msg: w.err }); continue; }
      if (!/^[A-Za-z0-9_]{3,20}$/.test(w.name)) { out.push({ ok: false, msg: 'Ten account la: ' + w.name.slice(0, 30) }); continue; }
      if (store[w.name] !== c) { store[w.name] = c; storeDirty = true; }
      if (!cfg.accounts.includes(w.name)) { if (cfg.accounts.length >= 60) { out.push({ ok: false, name: w.name, msg: 'Toi da 60 account.' }); continue; } cfg.accounts.push(w.name); cfgDirty = true; }
      out.push({ ok: true, name: w.name, msg: 'Da them.' }); rjLog(w.name + ': da them.');
    }
    try { if (storeDirty) await ckSave(a.owner, store); if (cfgDirty) { if (!cfg.owner) cfg.owner = a.owner; rjWrite(cfg); } } catch (e) { return send(res, 500, { error: 'write_failed', message: String((e && e.message) || e) }); }
    send(res, 200, { results: out });
  }, 3e5);
  if (p === '/api/rejoin/now' && req.method === 'POST') return readJson(req, res, async b => {
    const cfg = rjCfg(), names = (Array.isArray(b.names) ? b.names : []).map(String).filter(n => cfg.accounts.includes(n)).slice(0, 20), out = [];
    for (let i = 0; i < names.length; i++) {
      const n = names[i];
      const e = rjEntry(cfg.owner || a.owner, n), r = await rjLaunch(cfg, n, e && e.rec), st = RJ.st.get(n) || { last: 0, result: '' };
      st.last = Date.now(); RJ.lastLaunch = st.last; st.result = r.msg; RJ.st.set(n, st);
      rjLog(n + ' (bam tay): ' + (r.ok ? 'OK - ' : 'LOI - ') + r.msg); out.push({ name: n, ok: r.ok, msg: r.msg });
      if (i < names.length - 1) await new Promise(rs => setTimeout(rs, (i + 1) % cfg.batch === 0 ? Math.max(cfg.openDelay, 1) * 1000 : 1500));   // du N client thi nghi "Delay mo Roblox" roi mo dot tiep
    }
    send(res, 200, { results: out });
  });
  if (p === '/api/rejoin/remove' && req.method === 'POST') return readJson(req, res, b => {   // bo khoi tool Rejoin (cookie van con trong tab Cookie)
    const cfg = rjCfg(), rm = new Set((Array.isArray(b.names) ? b.names : []).map(String));
    cfg.accounts = cfg.accounts.filter(n => !rm.has(n)); rm.forEach(n => RJ.st.delete(n));
    try { rjWrite(cfg); } catch (e) { return send(res, 500, { error: 'write_failed', message: String((e && e.message) || e) }); }
    send(res, 200, { ok: true });
  });
  return send(res, 404, { error: 'not_found' });
}
// ================== PERFORMANCE: gioi han CPU / don RAM / sap xep cua so Roblox (Windows) ==================
// Gioi han CPU (Job Object hard cap), don RAM (EmptyWorkingSet), sap xep / thu nho / dat ten cua so Roblox. Chay qua 1 tien trinh PowerShell nen, khong can file .exe ngoai.
const PF_DEFAULT = {
  cpu: { enabled: false, total: 85, max: 5, interval: 5 },
  ram: { enabled: false, interval: 60, threshold: 300, batch: 4, delay: 500 },
  arr: { enabled: false, w: 300, h: 300, gx: 100, gy: 100, dir: 'grid_lr', perRow: 5, step: 30, border: false, interval: 10, minimize: false, showName: false }
};
const PF_DIRS = ['grid_lr', 'grid_rl', 'grid_bl_tr', 'col_left', 'col_right', 'zigzag', 'casc_tl', 'casc_tr'];
const PF = { w: null, buf: '', lines: [], res: null, chain: Promise.resolve(), tmp: '', due: { cpu: Date.now(), ram: Date.now(), arr: Date.now() }, cpuOn: false, ready: false, busy: false, named: new Map(), mined: new Set(), sized: new Map(), sizedN: 0, st: { cpu: null, ram: { total: 0, last: null }, arr: null }, err: '' };
const pfFile = () => path.join(path.dirname(DATA_FILE), 'data', 'tracker_perf.json');
const PF_PS = String.raw`
$ErrorActionPreference = 'Continue'
$legacyErr = ''
try {
Add-Type -ErrorAction Stop -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
public static class RbxPerf {
  [StructLayout(LayoutKind.Sequential)] public struct RECT { public int Left; public int Top; public int Right; public int Bottom; }
  public delegate bool EnumProc(IntPtr h, IntPtr l);
  [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] static extern IntPtr CreateJobObjectW(IntPtr a, string n);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j, int c, IntPtr i, int l);
  [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr OpenProcess(uint a, bool b, uint pid);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr j, IntPtr p);
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr h);
  [DllImport("psapi.dll", SetLastError=true)] static extern bool EmptyWorkingSet(IntPtr h);
  [DllImport("user32.dll")] static extern bool EnumWindows(EnumProc cb, IntPtr l);
  [DllImport("user32.dll")] static extern bool IsWindowVisible(IntPtr h);
  [DllImport("user32.dll")] static extern uint GetWindowThreadProcessId(IntPtr h, out uint pid);
  [DllImport("user32.dll")] static extern int GetWindowTextLengthW(IntPtr h);
  [DllImport("user32.dll")] static extern bool SetWindowPos(IntPtr h, IntPtr after, int x, int y, int cx, int cy, uint f);
  [DllImport("user32.dll", EntryPoint="GetWindowLongW")] static extern int GetWindowLong(IntPtr h, int i);
  [DllImport("user32.dll", EntryPoint="SetWindowLongW")] static extern int SetWindowLong(IntPtr h, int i, int v);
  [DllImport("user32.dll", EntryPoint="SystemParametersInfoW")] static extern bool SystemParametersInfo(uint a, uint b, ref RECT r, uint c);
  [DllImport("user32.dll", CharSet=CharSet.Unicode, EntryPoint="SetWindowTextW")] static extern bool SetWindowText(IntPtr h, string t);
  [DllImport("user32.dll")] static extern bool ShowWindow(IntPtr h, int c);
  static IntPtr FindWin(uint pid) {
    IntPtr found = IntPtr.Zero;
    EnumWindows(delegate(IntPtr h, IntPtr l) {
      uint p; GetWindowThreadProcessId(h, out p);
      if (p == pid && IsWindowVisible(h) && GetWindowTextLengthW(h) > 0) { found = h; return false; }
      return true;
    }, IntPtr.Zero);
    return found;
  }
  public static bool SetTitle(uint pid, string t) { IntPtr h = FindWin(pid); return h != IntPtr.Zero && SetWindowText(h, t); }
  public static bool Minimize(uint pid) { IntPtr h = FindWin(pid); return h != IntPtr.Zero && ShowWindow(h, 7); }
  static Dictionary<uint, IntPtr> jobs = new Dictionary<uint, IntPtr>();
  static Dictionary<uint, double> caps = new Dictionary<uint, double>();
  static bool SetRate(IntPtr job, double pct) {
    int rate = (int)Math.Round(pct * 100.0); if (rate < 1) rate = 1; if (rate > 10000) rate = 10000;
    IntPtr p = Marshal.AllocHGlobal(8);
    try { Marshal.WriteInt32(p, 0, 5); Marshal.WriteInt32(p, 4, rate); return SetInformationJobObject(job, 15, p, 8); }
    finally { Marshal.FreeHGlobal(p); }
  }
  public static string Cap(uint[] pids, double cap) {
    HashSet<uint> alive = new HashSet<uint>(pids == null ? new uint[0] : pids);
    List<uint> dead = new List<uint>();
    foreach (uint k in jobs.Keys) if (!alive.Contains(k)) dead.Add(k);
    foreach (uint k in dead) { CloseHandle(jobs[k]); jobs.Remove(k); caps.Remove(k); }
    int ok = 0, fail = 0; string err = "";
    foreach (uint pid in pids) {
      IntPtr job;
      if (jobs.TryGetValue(pid, out job)) {
        if (Math.Abs(caps[pid] - cap) > 0.001) { SetRate(job, cap); caps[pid] = cap; }
        ok++; continue;
      }
      job = CreateJobObjectW(IntPtr.Zero, null);
      if (job == IntPtr.Zero) { fail++; err = "CreateJob " + Marshal.GetLastWin32Error(); continue; }
      if (!SetRate(job, cap)) { err = "SetRate " + Marshal.GetLastWin32Error(); CloseHandle(job); fail++; continue; }
      IntPtr hp = OpenProcess(0x0101, false, pid);
      if (hp == IntPtr.Zero) { err = "OpenProcess " + Marshal.GetLastWin32Error(); CloseHandle(job); fail++; continue; }
      bool a = AssignProcessToJobObject(job, hp); int e = Marshal.GetLastWin32Error(); CloseHandle(hp);
      if (!a) { err = "Assign " + e; CloseHandle(job); fail++; continue; }
      jobs[pid] = job; caps[pid] = cap; ok++;
    }
    return ok + ";" + fail + ";" + err;
  }
  public static void Release() {
    foreach (KeyValuePair<uint, IntPtr> kv in jobs) { SetRate(kv.Value, 100.0); CloseHandle(kv.Value); }
    jobs.Clear(); caps.Clear();
  }
  public static int Trim(uint[] pids) {
    int n = 0;
    foreach (uint pid in pids) {
      IntPtr h = OpenProcess(0x0500, false, pid);
      if (h == IntPtr.Zero) continue;
      if (EmptyWorkingSet(h)) n++;
      CloseHandle(h);
    }
    return n;
  }
  public static string Wins(uint[] pids) {
    HashSet<uint> want = new HashSet<uint>(pids == null ? new uint[0] : pids);
    HashSet<uint> seen = new HashSet<uint>();
    List<string> res = new List<string>();
    EnumWindows(delegate(IntPtr h, IntPtr l) {
      uint pid; GetWindowThreadProcessId(h, out pid);
      if (want.Contains(pid) && !seen.Contains(pid) && IsWindowVisible(h) && GetWindowTextLengthW(h) > 0) { seen.Add(pid); res.Add(h.ToInt64() + ":" + pid); }
      return true;
    }, IntPtr.Zero);
    return string.Join(",", res.ToArray());
  }
  public static string Area() { RECT r = new RECT(); SystemParametersInfo(0x30, 0, ref r, 0); return r.Left + "," + r.Top + "," + r.Right + "," + r.Bottom; }
  [DllImport("user32.dll")] static extern bool GetWindowRect(IntPtr h, out RECT r);
  [DllImport("user32.dll")] static extern bool MoveWindow(IntPtr h, int x, int y, int w, int hh, bool repaint);
  [DllImport("user32.dll")] static extern bool IsZoomed(IntPtr h);
  // 2 = xep dung kich thuoc, 1 = da di chuyen nhung Roblox khong cho doi kich thuoc, 0 = loi
  public static int Move(long hwnd, int x, int y, int w, int h, bool borderless) {
    IntPtr hw = new IntPtr(hwnd);
    if (IsZoomed(hw)) ShowWindow(hw, 9);
    int st = GetWindowLong(hw, -16);
    int popup = unchecked((int)0x80000000), frame = 0x00CF0000, cap = 0x00CB0000;
    // Co vien: giu thanh tieu de nhung BO khung keo gian (THICKFRAME) - Roblox chan thu nho cua so duoi ~800x600 khi con khung keo gian
    int want = borderless ? ((st & ~frame) | popup) : (((st & ~popup) & ~frame) | cap);
    if (want != st) SetWindowLong(hw, -16, want);
    if (!SetWindowPos(hw, IntPtr.Zero, x, y, w, h, 0x0034)) return 0;
    RECT r; GetWindowRect(hw, out r);
    if (Math.Abs((r.Right - r.Left) - w) > 6 || Math.Abs((r.Bottom - r.Top) - h) > 6) { MoveWindow(hw, x, y, w, h, true); GetWindowRect(hw, out r); }
    return (Math.Abs((r.Right - r.Left) - w) <= 6 && Math.Abs((r.Bottom - r.Top) - h) <= 6) ? 2 : 1;
  }
}
'@
} catch { $legacyErr = ($_.Exception.Message -replace '[\r\n|]+', ' '); if ($legacyErr.Length -gt 220) { $legacyErr = $legacyErr.Substring(0, 220) } }

$v2 = $false; $v2err = ''
try {
Add-Type -ErrorAction Stop -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
public static class RbxPerf2 {
  [StructLayout(LayoutKind.Sequential)] public struct RECT { public int Left; public int Top; public int Right; public int Bottom; }
  public delegate bool EnumProc(IntPtr h, IntPtr l);
  [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] static extern IntPtr CreateJobObjectW(IntPtr a, string n);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j, int c, IntPtr i, int l);
  [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] static extern IntPtr OpenJobObjectW(uint access, bool inherit, string name);
  [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr OpenProcess(uint a, bool b, uint pid);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr j, IntPtr p);
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr h);
  [DllImport("user32.dll")] static extern bool EnumWindows(EnumProc cb, IntPtr l);
  [DllImport("user32.dll")] static extern bool IsWindowVisible(IntPtr h);
  [DllImport("user32.dll")] static extern uint GetWindowThreadProcessId(IntPtr h, out uint pid);
  [DllImport("user32.dll")] static extern int GetWindowTextLengthW(IntPtr h);
  [DllImport("user32.dll")] static extern bool SetWindowPos(IntPtr h, IntPtr after, int x, int y, int cx, int cy, uint f);
  [DllImport("user32.dll", EntryPoint="GetWindowLongW")] static extern int GetWindowLong(IntPtr h, int i);
  [DllImport("user32.dll", EntryPoint="SetWindowLongW")] static extern int SetWindowLong(IntPtr h, int i, int v);
  [DllImport("user32.dll")] static extern bool ShowWindow(IntPtr h, int c);
  [DllImport("user32.dll")] static extern bool GetWindowRect(IntPtr h, out RECT r);
  [DllImport("user32.dll")] static extern bool MoveWindow(IntPtr h, int x, int y, int w, int hh, bool repaint);
  [DllImport("user32.dll")] static extern bool IsZoomed(IntPtr h);
  [DllImport("user32.dll")] static extern bool SetProcessDPIAware();
  public static void Dpi() { try { SetProcessDPIAware(); } catch (Exception) { } }
  static long WinArea(IntPtr h) { RECT r; if (!GetWindowRect(h, out r)) return 0; return (long)(r.Right - r.Left) * (long)(r.Bottom - r.Top); }
  static Dictionary<uint, IntPtr> jobs = new Dictionary<uint, IntPtr>();
  static Dictionary<uint, double> caps = new Dictionary<uint, double>();
  static bool SetRate(IntPtr job, double pct) {
    int rate = (int)Math.Round(pct * 100.0); if (rate < 1) rate = 1; if (rate > 10000) rate = 10000;
    IntPtr p = Marshal.AllocHGlobal(8);
    try { Marshal.WriteInt32(p, 0, 5); Marshal.WriteInt32(p, 4, rate); return SetInformationJobObject(job, 15, p, 8); }
    finally { Marshal.FreeHGlobal(p); }
  }
  // Job dat TEN theo pid: tracker tat/bat lai van mo lai dung job cu, gioi han cu khong bi ket lai
  public static string Cap(uint[] pids, double cap) {
    HashSet<uint> alive = new HashSet<uint>(pids == null ? new uint[0] : pids);
    List<uint> dead = new List<uint>();
    foreach (uint k in jobs.Keys) { if (!alive.Contains(k)) dead.Add(k); }
    foreach (uint k in dead) { CloseHandle(jobs[k]); jobs.Remove(k); caps.Remove(k); }
    int ok = 0, fail = 0; string err = "";
    foreach (uint pid in pids) {
      IntPtr job;
      if (jobs.TryGetValue(pid, out job)) {
        if (Math.Abs(caps[pid] - cap) > 0.001) { SetRate(job, cap); caps[pid] = cap; }
        ok++; continue;
      }
      job = CreateJobObjectW(IntPtr.Zero, "DQRCap_" + pid.ToString());
      int lastErr = Marshal.GetLastWin32Error();
      if (job == IntPtr.Zero) { fail++; err = "CreateJob " + lastErr; continue; }
      if (!SetRate(job, cap)) { err = "SetRate " + Marshal.GetLastWin32Error(); CloseHandle(job); fail++; continue; }
      if (lastErr == 183) { jobs[pid] = job; caps[pid] = cap; ok++; continue; }
      IntPtr hp = OpenProcess(0x0101, false, pid);
      if (hp == IntPtr.Zero) { err = "OpenProcess " + Marshal.GetLastWin32Error(); CloseHandle(job); fail++; continue; }
      bool okAssign = AssignProcessToJobObject(job, hp); int asErr = Marshal.GetLastWin32Error(); CloseHandle(hp);
      if (!okAssign) { err = "Assign " + asErr; CloseHandle(job); fail++; continue; }
      jobs[pid] = job; caps[pid] = cap; ok++;
    }
    return ok.ToString() + ";" + fail.ToString() + ";" + err;
  }
  public static void Release() {
    foreach (KeyValuePair<uint, IntPtr> kv in jobs) { SetRate(kv.Value, 100.0); CloseHandle(kv.Value); }
    jobs.Clear(); caps.Clear();
  }
  public static int Uncap(uint[] pids) {
    int n = 0;
    foreach (uint pid in pids) {
      IntPtr jh = OpenJobObjectW(2, false, "DQRCap_" + pid.ToString());
      if (jh == IntPtr.Zero) continue;
      if (SetRate(jh, 100.0)) n++;
      CloseHandle(jh);
    }
    return n;
  }
  // Cua so chinh cua moi process = cua so hien thi, co tieu de, dien tich lon nhat
  public static string Wins(uint[] pids) {
    HashSet<uint> want = new HashSet<uint>(pids == null ? new uint[0] : pids);
    Dictionary<uint, IntPtr> bestH = new Dictionary<uint, IntPtr>();
    Dictionary<uint, long> bestA = new Dictionary<uint, long>();
    EnumWindows(delegate(IntPtr h, IntPtr l) {
      uint wp; GetWindowThreadProcessId(h, out wp);
      if (want.Contains(wp) && IsWindowVisible(h) && GetWindowTextLengthW(h) > 0) {
        long ar = WinArea(h);
        if (!bestA.ContainsKey(wp) || ar > bestA[wp]) { bestA[wp] = ar; bestH[wp] = h; }
      }
      return true;
    }, IntPtr.Zero);
    List<string> res = new List<string>();
    foreach (KeyValuePair<uint, IntPtr> kv in bestH) { res.Add(kv.Value.ToInt64().ToString() + ":" + kv.Key.ToString()); }
    return string.Join(",", res.ToArray());
  }
  // 2 = dung kich thuoc, 1 = chi di chuyen duoc, 0 = loi
  public static int Move(long hwnd, int x, int y, int w, int h, bool borderless) {
    IntPtr hw = new IntPtr(hwnd);
    if (IsZoomed(hw)) ShowWindow(hw, 9);
    int st = GetWindowLong(hw, -16);
    int popup = unchecked((int)0x80000000), frame = 0x00CF0000, capBits = 0x00CB0000;
    int want = borderless ? ((st & ~frame) | popup) : (((st & ~popup) & ~frame) | capBits);
    RECT r; GetWindowRect(hw, out r);
    if (want == st && Math.Abs(r.Left - x) <= 2 && Math.Abs(r.Top - y) <= 2 && Math.Abs((r.Right - r.Left) - w) <= 2 && Math.Abs((r.Bottom - r.Top) - h) <= 2) return 2;
    if (want != st) SetWindowLong(hw, -16, want);
    uint flags = (want != st) ? 0x0034u : 0x0014u;
    if (!SetWindowPos(hw, IntPtr.Zero, x, y, w, h, flags)) return 0;
    GetWindowRect(hw, out r);
    if (Math.Abs((r.Right - r.Left) - w) > 6 || Math.Abs((r.Bottom - r.Top) - h) > 6) { MoveWindow(hw, x, y, w, h, true); GetWindowRect(hw, out r); }
    return (Math.Abs((r.Right - r.Left) - w) <= 6 && Math.Abs((r.Bottom - r.Top) - h) <= 6) ? 2 : 1;
  }
}
'@
$v2 = $true
} catch { $v2err = ($_.Exception.Message -replace '[\r\n|]+', ' '); if ($v2err.Length -gt 220) { $v2err = $v2err.Substring(0, 220) } }
if ($v2) { try { [RbxPerf2]::Dpi() } catch { } }

function Get-RbxPids { @(Get-Process -Name RobloxPlayerBeta -ErrorAction SilentlyContinue | Sort-Object Id | ForEach-Object { [uint32]$_.Id }) }
try { if ($v2) { [RbxPerf2]::Uncap([uint32[]]@(Get-RbxPids)) | Out-Null } } catch { }   # don sach gioi han CPU con sot lai tu lan chay truoc
[Console]::Out.WriteLine('READY')
while ($true) {
  $line = [Console]::In.ReadLine()
  if ($null -eq $line) { break }
  try {
    $p = $line.Split('|')
    switch ($p[0]) {
      'ping' { [Console]::Out.WriteLine('OK|pong|v2=' + [int]$v2 + '|e=' + $v2err + $legacyErr) }
      'info' {
        $ps = @(Get-Process -Name RobloxPlayerBeta -ErrorAction SilentlyContinue)
        $mb = 0; foreach ($x in $ps) { $mb += $x.WorkingSet64 / 1MB }
        [Console]::Out.WriteLine('OK|n=' + $ps.Count + '|mb=' + [int]$mb)
      }
      'cap' {
        $ids = Get-RbxPids; $n = $ids.Count
        if ($n -eq 0) { if ($v2) { [RbxPerf2]::Cap([uint32[]]@(), 1.0) | Out-Null } else { [RbxPerf]::Cap([uint32[]]@(), 1.0) | Out-Null }; [Console]::Out.WriteLine('OK|n=0|cap=0|ok=0|fail=0|err='); break }
        $total = [double]::Parse($p[1], [Globalization.CultureInfo]::InvariantCulture); $mx = [double]::Parse($p[2], [Globalization.CultureInfo]::InvariantCulture); $mn = [double]::Parse($p[3], [Globalization.CultureInfo]::InvariantCulture)
        $cap = [Math]::Max($mn, [Math]::Min($mx, $total / $n))
        $cr = if ($v2) { [RbxPerf2]::Cap([uint32[]]$ids, $cap) } else { [RbxPerf]::Cap([uint32[]]$ids, $cap) }; $r = ([string]$cr).Split(';')
        [Console]::Out.WriteLine('OK|n=' + $n + '|cap=' + $cap.ToString([Globalization.CultureInfo]::InvariantCulture) + '|ok=' + $r[0] + '|fail=' + $r[1] + '|err=' + $r[2])
      }
      'uncap' { if ($v2) { [RbxPerf2]::Release(); [RbxPerf2]::Uncap([uint32[]]@(Get-RbxPids)) | Out-Null } else { [RbxPerf]::Release() }; [Console]::Out.WriteLine('OK') }
      'trim' {
        $thr = [double]$p[1]; $batch = [Math]::Max(1, [int]$p[2]); $delay = [Math]::Max(0, [int]$p[3])
        $ps = @(Get-Process -Name RobloxPlayerBeta -ErrorAction SilentlyContinue | Where-Object { ($_.WorkingSet64 / 1MB) -ge $thr })
        $before = 0; foreach ($x in $ps) { $before += $x.WorkingSet64 / 1MB }
        $done = 0; $ids = @($ps | ForEach-Object { [uint32]$_.Id })
        for ($i = 0; $i -lt $ids.Count; $i += $batch) {
          $chunk = [uint32[]]@($ids[$i..([Math]::Min($i + $batch, $ids.Count) - 1)])
          $done += [RbxPerf]::Trim($chunk)
          if ($delay -gt 0 -and ($i + $batch) -lt $ids.Count) { Start-Sleep -Milliseconds $delay }
        }
        Start-Sleep -Milliseconds 300
        $after = 0; foreach ($id in $ids) { $q = Get-Process -Id $id -ErrorAction SilentlyContinue; if ($q) { $after += $q.WorkingSet64 / 1MB } }
        $freed = [Math]::Max(0, [int]($before - $after))
        [Console]::Out.WriteLine('OK|n=' + $ids.Count + '|done=' + $done + '|freed=' + $freed)
      }
      'wins' {
        $ids = Get-RbxPids
        $order = @(Get-Process -Name RobloxPlayerBeta -ErrorAction SilentlyContinue | Sort-Object { try { $_.StartTime } catch { [DateTime]::MaxValue } } | ForEach-Object { [uint32]$_.Id })
        $w = if ($v2) { [RbxPerf2]::Wins([uint32[]]@($ids)) } else { [RbxPerf]::Wins([uint32[]]@($ids)) }; $map = @{}
        foreach ($t in $w.Split(',')) { if ($t) { $a = $t.Split(':'); $map[$a[1]] = $a[0] } }
        $out = @(); foreach ($id in $order) { if ($map.ContainsKey([string]$id)) { $out += ($map[[string]$id] + ':' + $id) } }
        [Console]::Out.WriteLine('OK|' + ($out -join ','))
      }
      'area' { [Console]::Out.WriteLine('OK|' + [RbxPerf]::Area()) }
      'title' { $ok = [RbxPerf]::SetTitle([uint32]$p[1], $p[2]); [Console]::Out.WriteLine('OK|ok=' + $ok) }
      'min' { $ok = [RbxPerf]::Minimize([uint32]$p[1]); [Console]::Out.WriteLine('OK|ok=' + $ok) }
      'move' {
        $b = ($p[1] -eq '1'); $ok = 0; $bad = 0
        for ($i = 2; $i -lt $p.Count; $i++) {
          if (-not $p[$i]) { continue }
          $a = $p[$i].Split(':')
          $r = if ($v2) { [RbxPerf2]::Move([long]$a[0], [int]$a[1], [int]$a[2], [int]$a[3], [int]$a[4], $b) } else { [RbxPerf]::Move([long]$a[0], [int]$a[1], [int]$a[2], [int]$a[3], [int]$a[4], $b) }
          if ($r -ge 1) { $ok++ }
          if ($r -eq 1) { $bad++ }
        }
        [Console]::Out.WriteLine('OK|moved=' + $ok + '|bad=' + $bad)
      }
      default { [Console]::Out.WriteLine('ERR|lenh la: ' + $p[0]) }
    }
  } catch { [Console]::Out.WriteLine('ERR|' + ($_.Exception.Message -replace '[\r\n|]+', ' ')) }
}
try { if ($v2) { [RbxPerf2]::Release(); [RbxPerf2]::Uncap([uint32[]]@(Get-RbxPids)) | Out-Null } else { [RbxPerf]::Release() } } catch { }   # tracker tat -> go gioi han CPU, khong de Roblox bi ket o muc cap
`;
function pfKill() { try { PF.w && PF.w.kill(); } catch {} PF.w = null; PF.ready = false; PF.buf = ''; PF.lines = []; if (PF.res) { const r = PF.res; PF.res = null; r(null); } }
function pfSpawn() {
  if (PF.w && PF.w.exitCode === null && !PF.w.killed) return true;
  PF.buf = ''; PF.lines = []; PF.ready = false;
  try {
    const cp = require('child_process');
    if (process.env.PF_WORKER) PF.w = cp.spawn(process.execPath, [process.env.PF_WORKER], { stdio: ['pipe', 'pipe', 'ignore'] });   // chi de thu nghiem
    else if (process.platform === 'win32') {
      const td = require('os').tmpdir(); try { for (const f of fs.readdirSync(td)) if (/^dqr_perf_\d+\.ps1$/.test(f)) { try { fs.unlinkSync(path.join(td, f)); } catch {} } } catch {}   // don file tam cua cac lan chay truoc
      PF.tmp = path.join(td, 'dqr_perf_' + process.pid + '.ps1'); fs.writeFileSync(PF.tmp, PF_PS, 'utf8');
      PF.w = cp.spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-WindowStyle', 'Hidden', '-File', PF.tmp], { stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true });
    } else { PF.err = 'Performance chi chay tren Windows.'; return false; }
    PF.errBuf = ''; if (PF.w.stderr) { PF.w.stderr.setEncoding('utf8'); PF.w.stderr.on('data', d => { PF.errBuf = (PF.errBuf + d).slice(-300); }); }
    PF.w.stdout.setEncoding('utf8');
    PF.w.stdout.on('data', d => {
      PF.buf += d; let i;
      while ((i = PF.buf.indexOf('\n')) >= 0) {
        const l = PF.buf.slice(0, i).replace(/\r$/, ''); PF.buf = PF.buf.slice(i + 1);
        if (PF.res) { const r = PF.res; PF.res = null; r(l); } else PF.lines.push(l);
      }
    });
    const me = PF.w;
    PF.w.on('error', e => { PF.err = String((e && e.message) || e); if (PF.w === me) pfKill(); });
    PF.w.on('exit', () => { if (PF.w === me) { PF.w = null; if (PF.res) { const r = PF.res; PF.res = null; r(null); } } });
    PF.w.stdin.on('error', () => {});
    process.on('exit', () => { try { me.stdin.write('uncap\n'); me.stdin.end(); } catch { try { me.kill(); } catch {} } });   // worker tu go gioi han CPU roi tu thoat (khong kill dot ngot)
    return true;
  } catch (e) { PF.err = String((e && e.message) || e); PF.w = null; return false; }
}
function pfRead(ms) {
  if (PF.lines.length) return Promise.resolve(PF.lines.shift());
  return new Promise(resolve => {
    const t = setTimeout(() => { if (PF.res === done) PF.res = null; resolve(null); }, ms);
    const done = l => { clearTimeout(t); resolve(l); };
    PF.res = done;
  });
}
function pfCall(line, ms = 20000) {   // gui 1 lenh cho tien trinh nen, nhan 1 dong tra loi: 'OK|...' / 'ERR|...' (xep hang, moi lan 1 lenh)
  const run = async () => {
    if (!pfSpawn()) return { ok: false, msg: PF.err || 'Khong mo duoc tien trinh nen' };
    if (!PF.ready) { const r = await pfRead(ms + 15000); if (r !== 'READY') { const em = String(PF.errBuf || '').replace(/\s+/g, ' ').trim().slice(0, 200); pfKill(); return { ok: false, msg: 'Tien trinh nen khong khoi dong duoc (PowerShell / Add-Type loi)' + (em ? ': ' + em : '') }; } PF.ready = true; }
    try { PF.w.stdin.write(line + '\n'); } catch { pfKill(); PF.ready = false; return { ok: false, msg: 'Khong gui duoc lenh' }; }
    const l = await pfRead(ms);
    if (l === null) { pfKill(); PF.ready = false; return { ok: false, msg: 'Het thoi gian cho / tien trinh nen dung' }; }
    if (l.startsWith('OK')) { const o = { ok: true, f: {} }; for (const part of l.split('|').slice(1)) { const k = part.indexOf('='); if (k > 0) o.f[part.slice(0, k)] = part.slice(k + 1); else o.raw = part; } return o; }
    return { ok: false, msg: l.replace(/^ERR\|/, '') };
  };
  const p = PF.chain.then(run, run); PF.chain = p.catch(() => {}); return p;
}
function perfClean(v, old) {
  if (!v || typeof v !== 'object' || Array.isArray(v)) return 'Du lieu khong hop le.';
  const o = JSON.parse(JSON.stringify(old)), num = (x, lo, hi) => typeof x === 'number' && Number.isFinite(x) && x >= lo && x <= hi, int = (x, lo, hi) => Number.isInteger(x) && x >= lo && x <= hi;
  const spec = {
    cpu: { enabled: 'b', total: [1, 100, 0], max: [0.5, 100, 0], interval: [2, 600, 1] },
    ram: { enabled: 'b', interval: [5, 3600, 1], threshold: [0, 32000, 1], batch: [1, 50, 1], delay: [0, 10000, 1] },
    arr: { enabled: 'b', w: [0, 8000, 1], h: [0, 8000, 1], gx: [0, 500, 1], gy: [0, 500, 1], dir: 'd', perRow: [1, 50, 1], step: [0, 500, 1], border: 'b', interval: [2, 600, 1], minimize: 'b', showName: 'b' }
  };
  for (const g of Object.keys(spec)) {
    if (!(g in v)) continue; if (!v[g] || typeof v[g] !== 'object' || Array.isArray(v[g])) return 'Sai kieu o ' + g;
    for (const k of Object.keys(spec[g])) {
      if (!(k in v[g])) continue; const sp = spec[g][k], x = v[g][k];
      if (sp === 'b') { if (typeof x !== 'boolean') return 'Sai kieu o ' + g + '.' + k; }
      else if (sp === 'd') { if (!PF_DIRS.includes(x)) return 'Huong khong hop le.'; }
      else { if (typeof x !== 'number' || !Number.isFinite(x)) continue; o[g][k] = Math.min(sp[1], Math.max(sp[0], sp[2] ? Math.round(x) : x)); continue; }   // so ngoai khoang -> ep ve gioi han (khong bao loi)
      o[g][k] = x;
    }
  }
  return o;
}
function pfRead_() {
  const c = JSON.parse(JSON.stringify(PF_DEFAULT));
  try { const raw = JSON.parse(fs.readFileSync(pfFile(), 'utf8').replace(/^﻿/, '')); const m = perfClean(raw, c); if (typeof m !== 'string') return m; } catch {}
  return c;
}
function pfWrite(c) { const f = pfFile(); fs.mkdirSync(path.dirname(f), { recursive: true }); const tmp = f + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(c, null, 2)); try { if (fs.existsSync(f)) fs.copyFileSync(f, f + '.bak'); } catch {} fs.renameSync(tmp, f); }
// vi tri cac cua so theo huong sap xep
function pfLayout(n, area, a) {
  const [L, T, R, B] = area, W = R - L, H = B - T, per = Math.max(1, a.perRow), rows = Math.max(1, Math.ceil(n / per));
  const casc = a.dir === 'casc_tl' || a.dir === 'casc_tr';
  let w = a.w, h = a.h;
  const colMode = a.dir === 'col_left' || a.dir === 'col_right';   // cot: "Toi da moi hang" = so cua so xep doc trong 1 cot
  if (!w) w = casc ? Math.round(W * 0.5) : colMode ? Math.max(160, Math.floor((W - a.gx * (rows - 1)) / rows)) : Math.max(160, Math.floor((W - a.gx * (per - 1)) / Math.min(per, n)));
  if (!h) h = casc ? Math.round(H * 0.6) : colMode ? Math.max(120, Math.floor((H - a.gy * (Math.min(per, n) - 1)) / Math.min(per, n))) : Math.max(120, Math.floor((H - a.gy * (rows - 1)) / rows));
  const out = [];
  for (let i = 0; i < n; i++) {
    const col = i % per, row = Math.floor(i / per); let x = L, y = T;
    switch (a.dir) {
      case 'grid_rl': x = R - w - col * (w + a.gx); y = T + row * (h + a.gy); break;
      case 'grid_bl_tr': x = L + col * (w + a.gx); y = B - h - row * (h + a.gy); break;
      case 'col_left': x = L + row * (w + a.gx); y = T + col * (h + a.gy); break;
      case 'col_right': x = R - w - row * (w + a.gx); y = T + col * (h + a.gy); break;
      case 'zigzag': { const c2 = row % 2 ? per - 1 - col : col; x = L + c2 * (w + a.gx); y = T + row * (h + a.gy); break; }
      case 'casc_tl': x = L + i * a.step; y = T + i * a.step; break;
      case 'casc_tr': x = R - w - i * a.step; y = T + i * a.step; break;
      default: x = L + col * (w + a.gx); y = T + row * (h + a.gy);
    }
    out.push([Math.round(x), Math.round(y), Math.round(w), Math.round(h)]);
  }
  return out;
}
async function pfArrange(cfg) {
  const a = cfg.arr, ws = await pfCall('wins'); if (!ws.ok) return { ok: false, msg: ws.msg };
  const list = (ws.raw || '').split(',').filter(Boolean).map(s => s.split(':')[0]);
  if (!list.length) return { ok: true, n: 0, msg: 'Khong thay cua so Roblox nao.' };
  const ar = await pfCall('area'); if (!ar.ok) return { ok: false, msg: ar.msg };
  const area = (ar.raw || '').split(',').map(Number); if (area.length !== 4 || area.some(x => !Number.isFinite(x))) return { ok: false, msg: 'Khong doc duoc vung man hinh.' };
  const pos = pfLayout(list.length, area, a);
  const mv = await pfCall('move|' + (a.border ? '1' : '0') + '|' + list.map((h, i) => h + ':' + pos[i].join(':')).join('|')); if (!mv.ok) return { ok: false, msg: mv.msg };
  const bad = +(mv.f.bad || 0);
  return { ok: true, n: list.length, moved: +(mv.f.moved || 0), bad, msg: 'Da xep ' + (mv.f.moved || 0) + '/' + list.length + ' cua so.' + (bad ? ' ' + bad + ' cua so Roblox khong chiu doi kich thuoc (chi di chuyen duoc).' : '') + ((mv.f.moved || 0) < list.length ? ' ' + (list.length - (mv.f.moved || 0)) + ' cua so khong di chuyen duoc (neu Roblox chay quyen Admin thi mo tracker bang Administrator).' : '') };
}
async function pfRam(cfg) {
  const r = await pfCall('trim|' + cfg.ram.threshold + '|' + cfg.ram.batch + '|' + cfg.ram.delay, 60000);
  if (!r.ok) return { ok: false, msg: r.msg };
  const freed = +r.f.freed || 0; PF.st.ram.total += freed; PF.st.ram.last = { at: Date.now(), n: +r.f.n || 0, done: +r.f.done || 0, freed };
  return { ok: true, ...PF.st.ram.last };
}
// Kill RBL: dong toan bo client Roblox dang chay tren may (ke ca client khong phai do tool nay mo), dung taskkill theo ten tien trinh (khong qua worker PS)
function pfKillAll() {
  if (process.platform !== 'win32') return { ok: false, msg: 'Kill RBL chi chay tren Windows (tracker dang chay tren ' + process.platform + ').' };
  try {
    const r = require('child_process').spawnSync('taskkill', ['/IM', 'RobloxPlayerBeta.exe', '/T', '/F'], { encoding: 'utf8', timeout: 8000 });
    const out = String((r.stdout || '') + (r.stderr || ''));
    if (/not found/i.test(out)) return { ok: true, n: 0, msg: 'Khong co client Roblox nao dang chay.' };
    const n = (out.match(/SUCCESS/gi) || []).length;
    RJ.pids.clear();   // cac PID tool dang theo doi gio da chet het, de Auto Rejoin tu mo lai tu dau
    return { ok: true, n, msg: 'Da kill ' + n + ' client Roblox.' };
  } catch (e) { return { ok: false, msg: 'Loi: ' + String((e && e.message) || e) }; }
}
async function pfTick() {
  if (process.platform !== 'win32' && !process.env.PF_WORKER) return;
  if (PF.busy) return; PF.busy = true;
  try {
    if (!PF.diagDone) { const pr = await pfCall('ping'); if (pr.ok) { PF.diagDone = true; PF.diag = { v2: String(pr.f.v2 || ''), e: String(pr.f.e || '') }; } }
    const cfg = pfRead_(), now = Date.now();
    if (cfg.cpu.enabled && now - PF.due.cpu >= cfg.cpu.interval * 1000) {
      PF.due.cpu = now; const r = await pfCall('cap|' + cfg.cpu.total + '|' + cfg.cpu.max + '|0.5'); PF.cpuOn = true;
      PF.st.cpu = r.ok ? { at: Date.now(), n: +r.f.n, cap: +r.f.cap, ok: +r.f.ok, fail: +r.f.fail, err: r.f.err || '' } : { at: Date.now(), error: r.msg };
    } else if (!cfg.cpu.enabled && PF.cpuOn) { PF.cpuOn = false; PF.st.cpu = null; await pfCall('uncap'); }   // tat -> go gioi han (cap 100%)
    if (cfg.ram.enabled && now - PF.due.ram >= cfg.ram.interval * 1000) { PF.due.ram = now; const r = await pfRam(cfg); if (!r.ok) PF.st.ram.err = r.msg; else delete PF.st.ram.err; }
    if (cfg.arr.minimize || cfg.arr.showName) {   // chi ap dung cho client do Auto Rejoin mo (tool biet PID -> ten account)
      const rc = rjCfg(), tnow = Date.now();
      for (const [name, pid] of RJ.pids) {
        const st = RJ.st.get(name); if (!st || !st.last) continue;
        const key = pid + ':' + st.last;
        if (cfg.arr.showName && tnow - (PF.named.get(key) || 0) > 30e3) { PF.named.set(key, tnow); await pfCall('title|' + pid + '|' + name.replace(/[^\w]/g, '')); }
        if (cfg.arr.minimize && !PF.mined.has(key)) {
          const e = rjEntry(rc.owner, name), loaded = !!(e && e.updated > st.last) || tnow - st.last > 90e3;   // da tai xong: script bat dau bao cao (hoac qua 90s)
          if (loaded && tnow - st.last > 15e3) { PF.mined.add(key); await pfCall('min|' + pid); }
        }
      }
      if (PF.named.size > 200) PF.named.clear(); if (PF.mined.size > 200) PF.mined.clear();
    }
    if (RJ.pids.size) {   // client moi mo: dua ve cua so nho (khong de Roblox mo full man hinh)
      const tnow = Date.now(), todo = [];
      for (const [name, pid] of RJ.pids) {
        const st = RJ.st.get(name); if (!st || !st.last) continue;
        const key = pid + ':' + st.last, age = tnow - st.last, k = PF.sized.get(key) || 0;
        if ((k === 0 && age > 2e3 && age < 300e3) || (k === 1 && !cfg.arr.enabled && age > 25e3 && age < 300e3)) todo.push([key, pid, k]);
      }
      if (todo.length) {
        const ws = await pfCall('wins'), ar = ws.ok ? await pfCall('area') : null;
        const area = ar && ar.ok ? (ar.raw || '').split(',').map(Number) : [];
        if (ws.ok && area.length === 4 && area.every(Number.isFinite)) {
          const m = new Map((ws.raw || '').split(',').filter(Boolean).map(x => { const q = x.split(':'); return [q[1], q[0]]; }));
          const w = Math.max(160, cfg.arr.w || 300), hh = Math.max(120, cfg.arr.h || 300), parts = [];
          for (const [key, pid, k] of todo) {
            const h = m.get(String(pid)); if (!h) continue;
            PF.sized.set(key, k + 1); const off = (PF.sizedN++ % 10) * 30;
            parts.push(h + ':' + (area[0] + off) + ':' + (area[1] + off) + ':' + w + ':' + hh);
          }
          if (parts.length) await pfCall('move|' + (cfg.arr.border ? '1' : '0') + '|' + parts.join('|'));
        }
      }
      if (PF.sized.size > 200) PF.sized.clear();
    }
    if (cfg.arr.enabled && now - PF.due.arr >= cfg.arr.interval * 1000) { PF.due.arr = now; const r = await pfArrange(cfg); PF.st.arr = { at: Date.now(), ...r }; }
  } catch (e) { PF.err = String((e && e.message) || e); } finally { PF.busy = false; }
}
// ===== MONITOR (CPU / RAM / GPU / o dia / mang / Roblox): 1 tien trinh PowerShell rieng, lay mau moi 2s, giu 1 gio lich su =====
const MON = { w: null, buf: '', meta: null, hist: [], last: null, err: '', tmp: '', nextTry: 0 };
const MON_PS = String.raw`$ErrorActionPreference = 'SilentlyContinue'
$parent = __PARENT__
$cores = [Environment]::ProcessorCount
$meta = @{ threads = $cores; cpuName = ''; memTotalMB = 0; bootMs = 0; diskTotalGB = 0 }
try { $meta.cpuName = [string]((Get-CimInstance Win32_Processor | Select-Object -First 1).Name) } catch { }
try { $meta.memTotalMB = [int]((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1MB) } catch { }
$epoch = New-Object DateTime 1970, 1, 1, 0, 0, 0, ([DateTimeKind]::Utc)
try {
  $bt = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime
  $meta.bootMs = [int64](($bt.ToUniversalTime() - $epoch).TotalMilliseconds)
} catch { }
$drv = $null
try { $drv = New-Object System.IO.DriveInfo -ArgumentList ([string]$env:SystemDrive); $meta.diskTotalGB = [math]::Round($drv.TotalSize / 1GB, 1) } catch { }
[Console]::Out.WriteLine('META' + (ConvertTo-Json $meta -Compress))
$cats = @{}
foreach ($cn in @('Processor', 'Memory', 'PhysicalDisk', 'Network Interface', 'GPU Engine')) {
  try { $cats[$cn] = New-Object System.Diagnostics.PerformanceCounterCategory -ArgumentList $cn } catch { }
}
$prevS = @{}
$lastErr = ''
function Rc([string]$cn) { if ($script:cats.ContainsKey($cn)) { try { return $script:cats[$cn].ReadCategory() } catch { $script:lastErr = $cn + ': ' + $_.Exception.Message } } else { $script:lastErr = $cn + ': khong mo duoc category' } return $null }
# gia tri/giay cua 1 counter (mau truoc -> mau nay); inst = $null => cong tat ca instance bo qua loopback
function Cv($data, [string]$cn, [string]$ctr, $inst, [bool]$raw) {
  if (-not $data) { return $null }
  $col = $data[$ctr]; if (-not $col) { return $null }
  $sum = 0.0; $seen = $false
  foreach ($k in @($col.Keys)) {
    if ($inst) { if ($k -ne $inst) { continue } }
    elseif ($k -match 'Loopback|isatap|Teredo|Pseudo|_Total') { continue }
    $cs = $col[$k].Sample
    if ($raw) { $sum += [double]$cs.RawValue; $seen = $true; continue }
    $key = $cn + '|' + $ctr + '|' + $k
    if ($script:prevS.ContainsKey($key)) { try { $sum += [System.Diagnostics.CounterSample]::Calculate($script:prevS[$key], $cs); $seen = $true } catch { } }
    $script:prevS[$key] = $cs
  }
  if ($seen) { return $sum } return $null
}
$gpuPrev = @{}
function GpuNow {
  $d = $null; $c = Rc 'GPU Engine'; if ($c) { $d = $c['Utilization Percentage'] }
  if (-not $d) { return $null }
  try {
    $grp = @{}; $seen = $false; $cur = @{}
    foreach ($k in @($d.Keys)) {
      if ($k -notlike '*engtype_3D*') { continue }
      $cs = $d[$k].Sample
      $cur[$k] = $cs
      if ($script:gpuPrev.ContainsKey($k)) {
        $v = [System.Diagnostics.CounterSample]::Calculate($script:gpuPrev[$k], $cs); $seen = $true
        $g = 'x'; if ($k -match 'luid_(0x[0-9a-fA-F]+_0x[0-9a-fA-F]+).*_eng_(\d+)_engtype') { $g = $matches[1] + '_' + $matches[2] }
        if ($grp.ContainsKey($g)) { $grp[$g] += $v } else { $grp[$g] = $v }
      }
    }
    $script:gpuPrev = $cur
    if (-not $seen) { return $null }
    $m = 0.0; foreach ($g in @($grp.Keys)) { if ($grp[$g] -gt $m) { $m = $grp[$g] } }
    return [math]::Round([math]::Min(100, $m), 1)
  } catch { return $null }
}
function Cl($v) { if ($null -eq $v) { return $null } return [math]::Round([math]::Max(0, [math]::Min(100, $v)), 1) }
$prevP = @{}
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while ($true) {
  $t0 = $sw.ElapsedMilliseconds
  if (-not (Get-Process -Id $parent -ErrorAction SilentlyContinue)) { break }
  $dP = Rc 'Processor'; $dM = Rc 'Memory'; $dD = Rc 'PhysicalDisk'; $dN = Rc 'Network Interface'
  $cpu = Cl (Cv $dP 'Processor' '% Processor Time' '_Total' $false)
  $memFree = Cv $dM 'Memory' 'Available MBytes' $null $true
  if ($null -eq $memFree) { try { $memFree = [math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory / 1024) } catch { } }
  $rx = Cv $dN 'Network Interface' 'Bytes Received/sec' $null $false
  $tx = Cv $dN 'Network Interface' 'Bytes Sent/sec' $null $false
  $dk = Cl (Cv $dD 'PhysicalDisk' '% Disk Time' '_Total' $false)
  $dr = Cv $dD 'PhysicalDisk' 'Disk Read Bytes/sec' '_Total' $false
  $dw = Cv $dD 'PhysicalDisk' 'Disk Write Bytes/sec' '_Total' $false
  $free = $null; try { if ($drv) { $free = [math]::Round($drv.AvailableFreeSpace / 1GB, 1) } } catch { }
  $list = @(); $newPrev = @{}
  foreach ($p in @(Get-Process -Name RobloxPlayerBeta -ErrorAction SilentlyContinue)) {
    $ms = 0.0; try { $ms = $p.TotalProcessorTime.TotalMilliseconds } catch { }
    $pc = 0.0
    if ($prevP.ContainsKey($p.Id)) { $pr = $prevP[$p.Id]; $dt = $t0 - $pr[1]; if ($dt -gt 0) { $pc = ($ms - $pr[0]) / ($dt * $cores) * 100 } }
    $newPrev[$p.Id] = @($ms, $t0)
    $st = 0; try { $st = [int64](($p.StartTime.ToUniversalTime() - $epoch).TotalMilliseconds) } catch { }
    $rs = $true; try { $rs = [bool]$p.Responding } catch { }
    $pth = ''; try { $pth = [string]$p.Path } catch { }
    $list += @{ path = $pth; pid = $p.Id; cpu = [math]::Round([math]::Max(0, $pc), 1); mb = [int]($p.WorkingSet64 / 1MB); start = $st; resp = $rs }
  }
  $prevP = $newPrev
  $o = @{ cpu = $cpu; memFree = $memFree; gpu = (GpuNow); dk = $dk; dr = $dr; dw = $dw; rx = $rx; tx = $tx; df = $free; e = $(if ($null -eq $cpu -or $null -eq $rx) { $lastErr } else { '' }); rbx = @($list) }
  [Console]::Out.WriteLine('S' + (ConvertTo-Json $o -Compress -Depth 4))
  $sl = 2000 - ($sw.ElapsedMilliseconds - $t0)
  if ($sl -gt 50) { Start-Sleep -Milliseconds ([int]$sl) }
}
`;
function monSupported() { return process.platform === 'win32' || !!process.env.MON_WORKER; }
function monLine(l) {
  try {
    if (l.startsWith('META')) { MON.meta = JSON.parse(l.slice(4)); return; }
    if (!l.startsWith('S')) return;
    const j = JSON.parse(l.slice(1)); if (!j || typeof j !== 'object') return;
    const rbx = (Array.isArray(j.rbx) ? j.rbx : (j.rbx ? [j.rbx] : [])).map(x => ({ pid: +x.pid, cpu: +x.cpu || 0, mb: +x.mb || 0, start: +x.start || 0, resp: x.resp !== false, ver: (/version-[0-9a-f]{8,32}/i.exec(String(x.path || '')) || [''])[0] }));
    const tot = MON.meta && MON.meta.memTotalMB || 0, num = v => (typeof v === 'number' && Number.isFinite(v)) ? v : null;
    const s = { t: Date.now(), cpu: num(j.cpu), mem: num(j.memFree) == null || !tot ? null : Math.max(0, tot - j.memFree), gpu: num(j.gpu), dk: num(j.dk), dr: num(j.dr), dw: num(j.dw), rx: num(j.rx), tx: num(j.tx), df: num(j.df), rc: Math.round(rbx.reduce((a, x) => a + x.cpu, 0) * 10) / 10, rm: rbx.reduce((a, x) => a + x.mb, 0), n: rbx.length };
    // Day nhip thoi gian nay len server chinh de cong vao "Total Run Time" cua DUNG owner (MON chi chay duoc tren
    // may user qua Local Agent - Vercel khong co quyen doc CPU/Roblox cua may nguoi dung nen khong tu dem duoc;
    // truoc day ghi vao file rieng cua Local Agent nhung KHONG AI DOC lai - tab Total Run luon lay tu server chinh,
    // nen thoi gian cu dung yen 0 dung nhu bao loi). Fire-and-forget, khong chan vong lap MON.
    { const prev = MON.lastT || s.t, dt = Math.min(5000, Math.max(0, s.t - prev)); MON.lastT = s.t;
      if (LOCAL_AGENT && s.n > 0 && dt > 0) {
        try {
          const cfg = rjCfg();
          if (cfg.apiKey) fetch(LOCAL_AGENT_ORIGIN + '/api/runs/timeadd', { method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': cfg.apiKey }, body: JSON.stringify({ dt, n: s.n }) }).catch(() => {});
        } catch {}
      }
    }
    MON.hist.push(s); if (MON.hist.length > 1800) MON.hist.splice(0, MON.hist.length - 1800);
    MON.last = { t: s.t, rbx }; MON.err = String(j.e || '').slice(0, 200);
  } catch {}
}
function monEnsure() {
  if (MON.w && MON.w.exitCode === null && !MON.w.killed) return true;
  if (Date.now() < MON.nextTry) return false;
  MON.nextTry = Date.now() + 15000; MON.buf = '';
  try {
    const cp = require('child_process');
    if (process.env.MON_WORKER) MON.w = cp.spawn(process.execPath, [process.env.MON_WORKER], { stdio: ['ignore', 'pipe', 'ignore'] });   // chi de thu nghiem
    else if (process.platform === 'win32') {
      const td = require('os').tmpdir(); try { for (const f of fs.readdirSync(td)) if (/^dqr_mon_\d+\.ps1$/.test(f)) { try { fs.unlinkSync(path.join(td, f)); } catch {} } } catch {}
      MON.tmp = path.join(td, 'dqr_mon_' + process.pid + '.ps1'); fs.writeFileSync(MON.tmp, MON_PS.replace('__PARENT__', String(process.pid)), 'utf8');
      MON.w = cp.spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-WindowStyle', 'Hidden', '-File', MON.tmp], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
      let eb = ''; if (MON.w.stderr) { MON.w.stderr.setEncoding('utf8'); MON.w.stderr.on('data', d => { eb = (eb + d).slice(-300); MON.err = eb.replace(/\s+/g, ' ').trim(); }); }
    } else return false;
    const me = MON.w; me.stdout.setEncoding('utf8');
    me.stdout.on('data', d => { MON.buf += d; let i; while ((i = MON.buf.indexOf('\n')) >= 0) { const l = MON.buf.slice(0, i).replace(/\r$/, ''); MON.buf = MON.buf.slice(i + 1); monLine(l); } });
    me.on('error', e => { MON.err = String((e && e.message) || e); });
    me.on('exit', () => { if (MON.w === me) MON.w = null; });
    process.on('exit', () => { try { me.kill(); } catch {} });
    return true;
  } catch (e) { MON.err = String((e && e.message) || e); MON.w = null; return false; }
}
// ===== TOTAL RUN: so lan di tung dungeon, thang / thua (Northern Lands co them Rodin); anh nen lay tu wiki =====
const RUNS_NAMES = ['Desert Temple', 'Winter Outpost', 'Pirate Island', "King's Castle", 'The Underworld', 'Samurai Palace', 'The Canals', 'Ghastly Harbor', 'Steampunk Sewers', 'Orbital Outpost', 'Volcanic Chambers', 'Aquatic Temple', 'Enchanted Forest', 'Northern Lands', 'Gilded Skies', 'Unknown'];   // doi chieu dung tu file game (u2 list trong DungeonReleases). Bo 'Boss Raid' theo yeu cau (khong hien the nay nua); luot nao bao dungeonName='Boss Raid' se roi vao 'Unknown' nhu cac dungeon la khong nhan dien duoc khac. 'Unknown': khong doan duoc dungeon (chua gan Party / map khong khop / script cu chua bao dungeonName) - van dem win/loss vao day thay vi bo qua
const RUNS = { db: null, timer: null, img: new Map(), imgBusy: new Map(), live: {}, dbg: [], boxes: new Map() };
function runsDbgPush(o) { RUNS.dbg.push(o); if (RUNS.dbg.length > 300) RUNS.dbg.splice(0, RUNS.dbg.length - 300); }
// Thang/thua tung dungeon (runsEntry/runsTrack/runsConclude) TRUOC DAY nam chung trong 1 file tracker_runs.json
// ghi bang fs.writeFileSync - tren Vercel dia chi doc duoc nhung KHONG ghi lau dai (serverless, moi lan goi co
// the roi vao may chu khac nhau, file ghi bi mat khi instance bi thu hoi) nen Win/Loss luon ve lai 0. Fix giong
// het sellStore/buyStore: chuyen qua Redis (dq:runs:<owner>, 1 key JSON/owner), nap dau request can, ghi lai cuoi
// request neu co thay doi (dirty). "Total Run Time" (runsTime/MON, chi chay tren may user qua Local Agent, van
// con duong ghi file cu) KHONG dung toi, giu nguyen.
const runsBox = o => { let b = RUNS.boxes.get(o); if (!b) RUNS.boxes.set(o, b = { data: {}, loaded: false, dirty: false }); return b; };
async function loadRunsBox(o) {
  if (runsBox(o).loaded) return;
  let data = {}; try { const [raw] = await redis([['GET', 'dq:runs:' + o]]); data = raw ? JSON.parse(raw) : {}; } catch (e) { data = {}; }
  if (!data || typeof data !== 'object') data = {};
  RUNS.boxes.set(o, { data, loaded: true, dirty: false });
}
const runsFile = () => path.join(path.dirname(DATA_FILE), 'data', 'tracker_runs.json');
const runsImgDir = () => path.join(path.dirname(DATA_FILE), 'data', 'dungeon_img');
const runsSlug = n => n.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
function runsDb() {
  if (RUNS.db) return RUNS.db;
  try { RUNS.db = JSON.parse(fs.readFileSync(runsFile(), 'utf8').replace(/^﻿/, '')); } catch { RUNS.db = {}; }
  if (!RUNS.db || typeof RUNS.db !== 'object') RUNS.db = {};
  // Di chuyen du lieu 1 lan: truoc day luot gap Rodin CHI cong vao rw/rl (khong cong vao w/l cua Northern Lands).
  // Gio Raid Rodin la bo dem CONG THEM (khong thay the), nen can gop so cu vao w/l 1 lan duy nhat de khong mat du lieu da ghi.
  if (!RUNS.db._nlMerged) {
    for (const k of Object.keys(RUNS.db)) {
      const o = RUNS.db[k]; const e = o && o['Northern Lands'];
      if (e && typeof e === 'object') { e.w = (e.w || 0) + (e.rw || 0); e.l = (e.l || 0) + (e.rl || 0); }
    }
    RUNS.db._nlMerged = true;
    try { fs.mkdirSync(path.dirname(runsFile()), { recursive: true }); fs.writeFileSync(runsFile(), JSON.stringify(RUNS.db)); } catch {}
  }
  return RUNS.db;
}
function runsSave() {
  if (RUNS.timer) return;
  RUNS.timer = setTimeout(() => { RUNS.timer = null; try { const f = runsFile(); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f + '.tmp', JSON.stringify(RUNS.db)); fs.renameSync(f + '.tmp', f); } catch (e) { console.error('[runs] luu loi: ' + e.message); } }, 400);
}
process.on('exit', () => { try { if (RUNS.timer && RUNS.db) { const f = runsFile(); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f, JSON.stringify(RUNS.db)); } } catch {} });
const runsName = v => { const x = String(v || '').trim().toLowerCase(); return RUNS_NAMES.find(n => n.toLowerCase() === x) || ''; };
const runsInt = v => { const n = Math.floor(+v); return Number.isFinite(n) && n >= 0 && n <= 1e9 ? n : 0; };
const runsTime = owner => { const d = runsBox(owner).data; return d._time || (d._time = { ms: 0, acc: 0 }); };   // tong thoi gian co Roblox chay (ms) va tong gio cua tung client (ms) - rieng tung owner (xem /api/runs/timeadd)
function runsEntry(owner, name, mk) { const b = runsBox(owner), d = b.data; let e = d[name]; if (!e && mk) { e = d[name] = { w: 0, l: 0, rw: 0, rl: 0 }; b.dirty = true; } return e || null; }

// ===== CHONG DEM TRUNG KHI NHIEU ACCOUNT CUNG 1 PARTY (CUNG jobId) CUNG BAO CAO 1 LUOT CHUNG (dung trong runsConclude) =====
// Nhieu account trong CUNG 1 party/CUNG 1 server Roblox (CUNG jobId) deu nhan tin hieu ket qua cho CUNG 1 lan
// thang/thua chung (game thuong gui rieng cho tung thanh vien party), neu cong het thi se bi CONG TRUNG nhieu lan
// cho dung 1 lan thang/thua that. Co che nay dam bao: voi CUNG 1 (owner, jobId, loai ket qua), chi acc nao "chot"
// TRUOC moi duoc cong diem - acc khac trong CUNG party bao cao SAU cho CUNG jobId + loai ket qua do se bi bo qua.
const RUNS_JOB_MARK_TTL_MS = 60 * 60 * 1000;   // don rac mark qua cu (jobId Roblox la GUID, thuc te khong bao gio lap lai - chi de phong phinh neu server chay rat lau)
// TRUOC DAY: Map trong RAM (trong 1 tien trinh). Tren Vercel moi request co the roi vao 1 may chu (serverless
// instance) KHAC NHAU - moi may co RAM rieng, khong thay duoc mark cua may khac - nen 1 luot thang/thua THAT co
// the bi NHIEU may chu khac nhau DOC LAP chot cung 1 luot do (vi co che grace 20s o rjTrack cung chi "nho" trong
// RAM cua tung may) -> 1 luot thuc te bi dem thanh 2-3-4 luot. Day la nguyen nhan "dang ngoi canh, moi di 3 luot
// ma no nhay len 13". Fix: chuyen qua Redis SET...NX (chi 1 may duoc "chot" dau tien cho 1 jobId+loai ket qua,
// moi noi khac deu thay la da co -> tu dong bo qua), dam bao dung 1 lan du request roi vao may chu nao.
async function runsJobMarkClaim(owner, jobId, kind) {   // tra ve true neu DAY LA LAN DAU claim duoc (duoc cong diem); false neu da co noi khac claim roi (bo qua, tranh dem trung)
  if (!jobId) return true;   // khong co jobId (hiem, du lieu cu/thieu) thi khong du de dedup - cho qua nhu truoc gio, khong chan
  const key = 'dq:runsmark:' + owner + '\u0001' + jobId + '\u0001' + kind;
  try { const [r] = await redis([['SET', key, '1', 'NX', 'PX', String(RUNS_JOB_MARK_TTL_MS)]]); return r === 'OK'; }
  catch (e) { return true; }   // loi mang luc check: tha qua (thieu 1 vai luot con hon la bi ket treo khong dem duoc gi)
}

// ===== TU DONG DEM WIN/LOSS: suy ra tu placeId (vao/ra dungeon) + tin hieu thang (cloneRewardGui, da co san o applyWin) =====
// Khong sua duoc Lua o may nguoi dung tu xa nen chi dung du lieu da co san: placeId gui dinh ky + tin hieu thang + file altfarm.json (ten dungeon dang farm).
// Do la suy luan (khong phai API chinh thuc cua game) nen co the thieu/sai vai luot; dung nut +/- trong Total Run de sua tay neu lech.
const RUNS_LOBBY_PLACE = '77649408247578';
const RUNS_CONCLUDE_MS = 20000;   // khong thay acc nao cua owner con trong dungeon sau tung nay ms -> coi nhu luot da xong (du rong de khong bi ngat som neu Interval gui cao hon mac dinh)
// Nhai dung co che dem thang/thua goc cua Harvard (doc truc tiep tu source Lua cua ho - statisticsWebhooks):
// thay vi suy doan qua tin hieu "nhan thuong" (cloneRewardGui) / "ve lobby = mac dinh thua" / co mat boss Odin
// Reincarnation nhu truoc day, gio dung dung 6 tin hieu game ma Harvard dung: dungeonProgress, dungeonFinished,
// dungeonStarted, timeUp (tu dong ho timeLeftGui), va bonusBossStatus (Rodin, HOAN TOAN TACH RIENG - xem o duoi).
const VERDICT_GRACE_MS = 20000;   // = whVerdictAfter mac dinh cua Harvard: tin hieu thang/thua "mem" (dungeonFinished/bossKilled, timeUp, roi dungeonStarted) phai giu nguyen it nhat tung nay ms moi duoc chot, tranh chot nham do tin hieu thoang qua 1-2 lan bao cao roi tu doi lai binh thuong. Rieng tin hieu THUA CHAC CHAN (het mang/het gio - noLivesLeft/outOfTime, giong E() ben Harvard) duoc chot NGAY, khong can doi.
function runsDungeonFor(owner, name) {   // doan dungeon dang chay tu altfarm.json (party co acc nay -> p.map)
  try {
    const r = pdRead('altfarm'); const parties = (r && r.value && Array.isArray(r.value.parties)) ? r.value.parties : [];
    const nm = String(name || '').toLowerCase();
    for (const p of parties) {
      if (p && Array.isArray(p.accounts) && p.accounts.some(x => x && String(x.name || '').toLowerCase() === nm)) { const dn = runsName(p.map); if (dn) return dn; }
    }
  } catch {}
  return '';
}
async function runsTrack(owner, name, rec, now0) {
  if (!owner || !rec) return;
  // QUAN TRONG: trang thai luot dang chay phai rieng theo TUNG ACC (owner+name), khong duoc dung chung 1 object cho
  // ca owner - neu dung chung, acc nay vua thoat/thang se lam "dinh" sang acc khac dang o dungeon KHAC (hoac vao
  // sau). Moi acc theo doi rieng, cuoi cung van cong don ve chung 1 owner khi ket luan (runsConclude).
  const liveKey = owner + '\u0001' + name;
  const live = RUNS.live || (RUNS.live = {}), st = live[liveKey] || (live[liveKey] = {});
  const inDungeon = !!rec.placeId && rec.placeId !== RUNS_LOBBY_PLACE;
  // Ten dungeon THAT, Lua doc truc tiep tu game (PlaceManager.GetDungeonName()) - CHINH XAC TUYET DOI, uu tien hon han
  // doan qua altfarm.json (chi la cau hinh "dinh farm" nguoi dung tu dat, co the sai neu ho di dungeon khac voi cau hinh).
  const liveDungeon = runsName(rec.dungeonName);
  const jobId = rec.jobId || null;
  if (process.env.RUNS_DEBUG) console.log('[runs-debug] track', { owner, name, placeId: rec.placeId, inDungeon, liveDungeon, jobId, dungeonProgress: rec.dungeonProgress, dungeonFinished: rec.dungeonFinished, dungeonStarted: rec.dungeonStarted, timeUp: rec.timeUp, bonusBossStatus: rec.bonusBossStatus });

  // ===== RAID RODIN (bonusBossStatus): HOAN TOAN TACH RIENG khoi thang/thua dungeon chinh, dung dung cach Harvard
  // lam (xem statisticsWebhooks trong source goc: rodinClear/rodinFail KHONG BAO GIO goi sessMark - ham dung de
  // cong thang/thua chinh - ma chi cong rieng vao webhooks.rodinClears/rodinFails). O day cung vay: Raid Rodin w/l
  // duoc cong NGAY khi phat hien bonusBossStatus chuyen tu "active" sang "success"/chua "fail", BAT KE luot dungeon
  // chinh (Northern Lands) cuoi cung thang hay thua - day la diem khac biet chinh so voi co che cu (truoc day Rodin
  // bi dinh theo ket qua cua dungeon chinh, dan den 1 luot thang dungeon nhung thua Rodin bi tinh nham thanh Rodin win).
  if (jobId) {
    if (st.bbJobId !== jobId) { st.bbJobId = jobId; st.bbStatus = rec.bonusBossStatus || null; }
    else {
      const prevBb = st.bbStatus || null, curBb = rec.bonusBossStatus || null;
      if (prevBb === 'active' && curBb && curBb !== prevBb) {
        const rWin = curBb === 'success', rKind = 'rodin\u0001' + (rWin ? 'win' : 'loss');
        if (rWin || /fail/i.test(curBb)) {
          if (await runsJobMarkClaim(owner, jobId, rKind)) {   // chong dem trung Rodin qua nhieu may chu Vercel, giong het dungeon chinh (xem runsJobMarkClaim)
            const e = runsEntry(owner, 'Northern Lands', true);
            if (rWin) { e.rw++; runsBox(owner).dirty = true; if (process.env.RUNS_DEBUG) console.log('[runs-debug] Rodin win (doc lap hoan toan voi dungeon chinh)', owner, name); }
            else { e.rl++; runsBox(owner).dirty = true; if (process.env.RUNS_DEBUG) console.log('[runs-debug] Rodin loss (doc lap hoan toan voi dungeon chinh)', owner, name); }
          }
        }
      }
      st.bbStatus = curBb;
    }
  }

  if (!inDungeon) {
    // Ve lobby (hoac chua vao dungeon nao): neu dang theo doi 1 luot ma chua kip chot, chot THUA (mac dinh - day la
    // cach chinh de biet thua: thua that trong game se tu dong bi day ve lobby, khong co tin hieu "thua" rieng).
    // Tru khi do bam nut "Force lobby" tren web - CHAC CHAN khong phai thua trong game (chinh web ra lenh keo ve).
    if (st.active) {
      if (takeForceLobbyPending(owner, name)) {
        if (process.env.RUNS_DEBUG) console.log('[runs-debug] bo qua (ve lobby do bam Force lobby, khong phai thua)', owner, name);
        runsDbgPush({ t: now0, k: 'discard', owner, run: { dungeon: st.dungeon || null, win: false, forced: true } });
      } else {
        // Da tung thay tin hieu thang (vd thang trum chinh roi qua danh Raid Rodin) ma ve lobby truoc khi kip
        // chot (grace 20s) thi van tinh THANG, khong tinh thua - xem st.sawWin o tren.
        await runsConclude(owner, { dungeon: st.dungeon, jobId: st.jobId, win: !!st.sawWin }, now0);
      }
      st.active = false; st.verdictSince = null; st.pendingWin = false; st.sawWin = false;
    }
    return;
  }

  // Dang trong dungeon: bat dau theo doi 1 luot moi neu chua co, hoac cap nhat luot dang theo doi.
  if (!st.active) { st.active = true; st.dungeon = liveDungeon || runsDungeonFor(owner, name); st.jobId = jobId; st.verdictSince = null; st.pendingWin = false; st.sawWin = false; if (process.env.RUNS_DEBUG) console.log('[runs-debug] start run', { owner, name, dungeon: st.dungeon, jobId: st.jobId }); }
  st.lastAt = now0;
  if (liveDungeon) st.dungeon = liveDungeon;
  if (jobId) st.jobId = jobId;

  // THANG (giong V() ben Harvard): dungeonFinished=true hoac dungeonProgress=="bossKilled". Mot khi DA THAY tin
  // hieu nay 1 lan cho luot dang theo doi, GHI NHO lai (st.sawWin) va KHONG CHO cac tin hieu sau do (kieu nhu
  // dungeonStarted=false/timeUp khi buoc sang man Raid Rodin phu, hoac het mang/het gio bao tre) lat lai thanh
  // thua nua - trong game da thang tr bum chinh thi khong the "thua lai" duoc. Thieu dong nay la nguyen nhan
  // loi "thang trum chinh, qua danh Rodin thua, Northern Lands lai bi tinh thua theo" nguoi dung bao.
  const winSignal = rec.dungeonFinished === true || rec.dungeonProgress === 'bossKilled';
  if (winSignal) st.sawWin = true;

  // THUA CHAC CHAN (giong E() ben Harvard): het mang (noLivesLeft) hoac het gio (outOfTime) - chot NGAY, khong doi
  // grace - TRU KHI da thay tin hieu thang cho luot nay roi (xem st.sawWin o tren).
  if (!st.sawWin && (rec.dungeonProgress === 'noLivesLeft' || rec.dungeonProgress === 'outOfTime')) {
    if (process.env.RUNS_DEBUG) console.log('[runs-debug] thua chac chan (dungeonProgress=' + rec.dungeonProgress + ')', { owner, name });
    await runsConclude(owner, { dungeon: st.dungeon, jobId: st.jobId, win: false }, now0);
    st.active = false; st.verdictSince = null; st.pendingWin = false; st.sawWin = false;
    return;
  }

  // THUA "mem" (giong p()/not-inDungeon/not-dungeonStarted ben Harvard): dong ho timeLeftGui het gio (00:00), hoac
  // game bao dungeonStarted=false (luot coi nhu da ket thuc/chua thuc su bat dau) - cung bo qua neu da sawWin.
  const loseSignal = !st.sawWin && !winSignal && (rec.timeUp === true || rec.dungeonStarted === false);
  const effWin = winSignal || st.sawWin;

  if (effWin || loseSignal) {
    if (st.verdictSince == null || st.pendingWin !== effWin) { st.verdictSince = now0; st.pendingWin = effWin; }
    if (now0 - st.verdictSince >= VERDICT_GRACE_MS) {
      if (process.env.RUNS_DEBUG) console.log('[runs-debug] chot sau grace', { owner, name, win: effWin });
      await runsConclude(owner, { dungeon: st.dungeon, jobId: st.jobId, win: effWin }, now0);
      st.active = false; st.verdictSince = null; st.pendingWin = false; st.sawWin = false;
    }
  } else {
    st.verdictSince = null; st.pendingWin = false;
  }
}
async function runsConclude(owner, run, now) {   // ghi 1 luot da ket thuc vao bang thang/thua (dungeon chinh - Rodin da duoc cong rieng tu runsTrack, xem o tren)
  const dn = run.dungeon || 'Unknown';   // khong doan duoc dungeon (chua gan Party/map khong khop) -> van dem vao "Unknown" thay vi bo qua luot
  const jid = run.jobId || null;
  const mainKind = dn + '\u0001' + (run.win ? 'win' : 'loss');
  if (!(await runsJobMarkClaim(owner, jid, mainKind))) {
    // Acc KHAC trong CUNG party (cung jobId) da bao cao ket qua nay cho CUNG luot roi (xem RUNS_JOB_MARK_TTL_MS o
    // tren) - bo qua, tranh dem trung theo so acc trong party. (Day cung la gioi han da biet so voi ban goc cua
    // Harvard: ho dung file .w/.l rieng theo jobId va cho THANG luon de ghi de len 1 THUA cu cung jobId; o day don
    // gian hon - ai claim (thang hoac thua) TRUOC cho 1 jobId thi tinh theo ket qua do, khong ghi de nguoc lai.)
    if (process.env.RUNS_DEBUG) console.log('[runs-debug] bo qua (acc khac trong party da bao cao luot nay roi - cung jobId)', owner, jid, dn);
    runsDbgPush({ t: now, k: 'discard', owner, run: { dungeon: dn, win: !!run.win, partyDedup: true } });
    return;
  }
  const e = runsEntry(owner, dn, true);
  runsDbgPush({ t: now, k: 'conclude', owner, dn, win: !!run.win });
  if (run.win) e.w++; else e.l++;
  runsBox(owner).dirty = true; if (process.env.RUNS_DEBUG) console.log('[runs-debug] concluded', owner, dn, e);
}
function runsTick() {   // goi dinh ky: don cac luot bi "dinh" do im lang qua lau (crash/mat ket noi - khong co tin hieu thang/thua/ve lobby ro rang) - BO QUA, khong cong vao dau (giong nguyen tac cu: khong chac la thua that nen khong tinh)
  const live = RUNS.live; if (!live) return; const now = Date.now();
  for (const liveKey of Object.keys(live)) {
    const st = live[liveKey]; if (!st || !st.active) continue;
    const owner = liveKey.slice(0, liveKey.indexOf('\u0001'));
    if (process.env.RUNS_DEBUG) console.log('[runs-debug] tick check', owner, st, now - st.lastAt);
    if (now - st.lastAt < RUNS_CONCLUDE_MS) continue;
    if (process.env.RUNS_DEBUG) console.log('[runs-debug] discarded (im lang qua lau, co the crash/mat ket noi)', owner, st);
    runsDbgPush({ t: now, k: 'discard', owner, run: { dungeon: st.dungeon || null, win: false, silent: true } });
    st.active = false; st.verdictSince = null; st.pendingWin = false;
  }
}
function runsGetHttps(host, pth, max, cb, depth) {   // chi toi host wiki (khong cho nguoi dung chon URL)
  if (!/(^|\.)(fandom\.com|wikia\.nocookie\.net|wikia\.com)$/.test(host)) return cb(new Error('host'));
  let done = false; const fin = (e, b, t) => { if (!done) { done = true; cb(e, b, t); } };
  try {
    const rq = require('https').get({ host, path: pth, timeout: 10000, headers: { 'user-agent': 'Mozilla/5.0 DQR-Tracker', accept: '*/*' } }, rs => {
      if ([301, 302, 303, 307, 308].includes(rs.statusCode) && rs.headers.location && (depth || 0) < 3) { rs.resume(); try { const u = new URL(rs.headers.location, 'https://' + host + pth); return runsGetHttps(u.hostname, u.pathname + u.search, max, fin, (depth || 0) + 1); } catch { return fin(new Error('redirect')); } }
      if (rs.statusCode !== 200) { rs.resume(); return fin(new Error('http ' + rs.statusCode)); }
      const ch = []; let n = 0; rs.on('data', d => { n += d.length; if (n > max) { rs.destroy(); fin(new Error('too big')); } else ch.push(d); });
      rs.on('end', () => fin(null, Buffer.concat(ch), String(rs.headers['content-type'] || '')));
      rs.on('error', e => fin(e));
    });
    rq.on('timeout', () => { rq.destroy(); fin(new Error('timeout')); }); rq.on('error', e => fin(e));
  } catch (e) { fin(e); }
}
const runsGet = (host, pth, max, depth) => new Promise((ok, no) => runsGetHttps(host, pth, max, (e, b, t) => e ? no(e) : ok({ b, t }), depth));
const RUNS_WIKIS = ['dungeonquestroblox.fandom.com', 'dqrroblox.fandom.com'];
async function runsFindImg(name, errs) {   // tra ve danh sach ung vien { host, path } (anh trang dungeon tren wiki)
  const out = [], seen = new Set(), add = src => { try { const u = new URL(src); if (!seen.has(u.href)) { seen.add(u.href); out.push({ host: u.hostname, path: u.pathname + u.search }); } } catch {} };
  const slug = name.replace(/ /g, '_'), word = name.split(' ').filter(w => w.length > 3)[0] || name;
  for (const host of RUNS_WIKIS) {
    for (const t of [name, name + ' (Dungeon)']) {   // 1) anh dai dien trang qua API
      try {
        const r = await runsGet(host, '/api.php?action=query&redirects=1&prop=pageimages&piprop=thumbnail%7Coriginal&pithumbsize=900&format=json&titles=' + encodeURIComponent(t), 3e5);
        const pg = (JSON.parse(r.b.toString('utf8')).query || {}).pages || {};
        for (const k of Object.keys(pg)) { const x = pg[k]; if (x && x.thumbnail && x.thumbnail.source) add(x.thumbnail.source); if (x && x.original && x.original.source) add(x.original.source); }
      } catch (e) { errs.push(host + ' api: ' + e.message); }
    }
    try {   // 2) og:image trong HTML trang
      const r = await runsGet(host, '/wiki/' + encodeURIComponent(slug), 2e6), h = r.b.toString('utf8'), m = /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i.exec(h) || /<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i.exec(h);
      if (m) add(m[1].replace(/&amp;/g, '&'));
    } catch (e) { errs.push(host + ' html: ' + e.message); }
    try {   // 3) file anh co ten giong ten dungeon trong trang
      const r = await runsGet(host, '/api.php?action=query&redirects=1&prop=images&imlimit=40&format=json&titles=' + encodeURIComponent(name), 3e5);
      const pg = (JSON.parse(r.b.toString('utf8')).query || {}).pages || {}; let files = [];
      for (const k of Object.keys(pg)) for (const im of (pg[k].images || [])) if (/\.(png|jpe?g|webp)$/i.test(im.title)) files.push(im.title);
      files.sort((a, b) => (new RegExp(word, 'i').test(b) ? 1 : 0) - (new RegExp(word, 'i').test(a) ? 1 : 0));
      if (files[0]) { const r2 = await runsGet(host, '/api.php?action=query&prop=imageinfo&iiprop=url&iiurlwidth=900&format=json&titles=' + encodeURIComponent(files[0]), 3e5); const p2 = (JSON.parse(r2.b.toString('utf8')).query || {}).pages || {}; for (const k of Object.keys(p2)) { const ii = (p2[k].imageinfo || [])[0]; if (ii) add(ii.thumburl || ii.url); } }
    } catch (e) { errs.push(host + ' files: ' + e.message); }
    if (out.length) break;
  }
  return out;
}
RUNS.err = {};
async function runsImg(name, res) {
  const dir = runsImgDir(), slug = runsSlug(name), fp = path.join(dir, slug + '.bin'), tp = path.join(dir, slug + '.type');
  const out = (b, t) => { res.writeHead(200, { 'content-type': t, 'cache-control': 'public, max-age=3600' }); res.end(b); };
  try { if (fs.existsSync(fp)) return out(fs.readFileSync(fp), fs.existsSync(tp) ? fs.readFileSync(tp, 'utf8') : 'image/jpeg'); } catch {}
  const neg = RUNS.img.get(slug); if (neg && Date.now() - neg < 6e5) return send(res, 404, { error: 'no_image' });
  let p = RUNS.imgBusy.get(slug);
  if (!p) {
    p = (async () => {
      const errs = [], c = await runsFindImg(name, errs);
      for (const f of c) {
        try { const r = await runsGet(f.host, f.path, 8e6), t = /^image\/(png|jpe?g|webp|gif)/i.exec(r.t) ? r.t.split(';')[0] : 'image/jpeg'; try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(fp, r.b); fs.writeFileSync(tp, t); } catch {} delete RUNS.err[name]; return { b: r.b, t }; }
        catch (e) { errs.push('tai anh: ' + e.message); }
      }
      RUNS.err[name] = (errs.length ? errs.slice(-2).join(' | ') : 'wiki khong co anh cho trang nay').slice(0, 200); return null;
    })().catch(e => { RUNS.err[name] = String((e && e.message) || e).slice(0, 200); return null; }).finally(() => RUNS.imgBusy.delete(slug));
    RUNS.imgBusy.set(slug, p);
  }
  const r = await p; if (!r) { RUNS.img.set(slug, Date.now()); return send(res, 404, { error: 'no_image' }); }
  out(r.b, r.t);
}
function runsApi(req, res, p, u, a) {
  const owner = a.owner;
  if (p === '/api/runs' && req.method === 'GET') {
    const o = runsBox(owner).data;
    return send(res, 200, { dungeons: RUNS_NAMES.map(n => { const e = o[n] || {}; return { name: n, w: e.w || 0, l: e.l || 0, rw: e.rw || 0, rl: e.rl || 0 }; }), imgErr: RUNS.err, time: runsTime(owner) });
  }
  if (p === '/api/runs/debug' && req.method === 'GET') {   // xem nhanh log Total Run (vao/ra dungeon, ten dungeon doc tu game, Rodin, tin hieu thang) - nut "Xem Debug Northern/Rodin" trong tab Total Run
    return send(res, 200, { events: RUNS.dbg.filter(x => x.owner === owner).slice(-150) });
  }
  if (req.method !== 'POST') return send(res, 404, { error: 'not_found' });
  if (p === '/api/runs/imgreset') { RUNS.img.clear(); RUNS.err = {}; return send(res, 200, { ok: true }); }
  if (p === '/api/runs/add') return readJson(req, res, b => {   // script game goi: { dungeon, win: true|false, rodin: true|false, n: 1 }
    const name = runsName(b.dungeon); if (!name) return send(res, 400, { error: 'bad_dungeon', message: 'Ten dungeon khong co trong danh sach.' });
    const n = Math.max(1, Math.min(1000, runsInt(b.n) || 1)), e = runsEntry(owner, name, true), win = b.win === true || String(b.result || '').toLowerCase() === 'win';
    if (b.rodin === true && name === 'Northern Lands') { if (win) e.rw += n; else e.rl += n; } else { if (win) e.w += n; else e.l += n; }
    runsBox(owner).dirty = true; send(res, 200, { ok: true });
  });
  if (p === '/api/runs/set') return readJson(req, res, b => {   // sua tay
    const name = runsName(b.dungeon); if (!name) return send(res, 400, { error: 'bad_dungeon' });
    const e = runsEntry(owner, name, true);
    for (const k of ['w', 'l', 'rw', 'rl']) if (k in b) e[k] = runsInt(b[k]);
    runsBox(owner).dirty = true; send(res, 200, { ok: true });
  });
  if (p === '/api/runs/reset') return readJson(req, res, b => {
    const box = runsBox(owner), d = box.data;
    if (b.dungeon) { const name = runsName(b.dungeon); if (!name) return send(res, 400, { error: 'bad_dungeon' }); delete d[name]; }
    else { for (const k of Object.keys(d)) delete d[k]; }
    box.dirty = true; send(res, 200, { ok: true });
  });
  // App Windows (Local Agent) day len tung nhip thoi gian Roblox dang chay thuc te (MON chi chay duoc tren may
  // user, khong chay tren Vercel) de cong vao "Total Run Time" cua DUNG owner - xem monLine()/rjCfg().apiKey.
  if (p === '/api/runs/timeadd') return readJson(req, res, b => {
    const dt = Math.max(0, Math.min(5000, runsInt(b.dt))), n = Math.max(0, Math.min(200, runsInt(b.n)));
    if (dt > 0 && n > 0) { const T = runsTime(owner); T.ms += dt; T.acc += dt * n; runsBox(owner).dirty = true; }
    send(res, 200, { ok: true });
  });
  return send(res, 404, { error: 'not_found' });
}
function perfApi(req, res, p, u, a) {   // chi admin + localhost
  if (!isLoopbackReq(req)) return send(res, 403, { error: 'local_only', message: 'Performance chi dung duoc tu App Windows (Tai App) vi can dieu khien Roblox tren chinh may ban - trinh duyet thuong khong lam duoc.' });
  const supported = process.platform === 'win32' || !!process.env.PF_WORKER;
  if (p === '/api/perf/state' && req.method === 'GET') return send(res, 200, { supported, cfg: pfRead_(), st: PF.st, err: PF.err, diag: PF.diag || null, now: Date.now() });
  if (p === '/api/perf/config' && req.method === 'POST') return readJson(req, res, b => {
    const out = perfClean(b, pfRead_()); if (typeof out === 'string') return send(res, 400, { error: 'bad_value', message: out });
    try { pfWrite(out); } catch (e) { return send(res, 500, { error: 'write_failed', message: String((e && e.message) || e) }); }
    PF.due = { cpu: 0, ram: 0, arr: 0 }; send(res, 200, { ok: true });   // doi cau hinh -> ap dung ngay o nhip ke tiep
  }, 6e4);
  if (p === '/api/perf/monitor' && req.method === 'GET') {
    if (!monSupported()) return send(res, 200, { supported: false });
    monEnsure();
    const since = +u.searchParams.get('since') || 0, names = new Map(); for (const [n, pid] of RJ.pids) names.set(pid, n);
    const cl = ((MON.last && MON.last.rbx) || []).map(x => ({ ver: x.ver, pid: x.pid, name: names.get(x.pid) || '', cpu: x.cpu, mb: x.mb, start: x.start, resp: x.resp }));
    return send(res, 200, { supported: true, meta: MON.meta, now: Date.now(), err: MON.err, samples: MON.hist.filter(x => x.t > since), clients: cl });
  }
  if (!supported) return send(res, 400, { error: 'not_supported', message: 'Performance chi chay tren Windows.' });
  if (p === '/api/perf/arrange' && req.method === 'POST') return readJson(req, res, async () => { const r = await pfArrange(pfRead_()); PF.st.arr = { at: Date.now(), ...r }; send(res, 200, r); });
  if (p === '/api/perf/ram' && req.method === 'POST') return readJson(req, res, async () => { send(res, 200, await pfRam(pfRead_())); });
  if (p === '/api/perf/killall' && req.method === 'POST') return readJson(req, res, () => { send(res, 200, pfKillAll()); });
  return send(res, 404, { error: 'not_found' });
}
function cfgApi(req, res, p, u, a) {
  if (!isLoopbackReq(req)) return send(res, 403, { error: 'local_only', message: 'Config chi dung duoc tu App Windows (Tai App) tren chinh may co Potassium - trinh duyet thuong khong lam duoc.' });
  if (p === '/api/config/resolve' && req.method === 'GET') {   // ten account -> userId Roblox (de biet file release_<userId>.json) - cache
    const nm = u.searchParams.get('name') || '';
    if (!/^[\w]{3,20}$/.test(nm)) return send(res, 400, { error: 'bad_name' });
    const hit = CFG_UID_CACHE.get(nm.toLowerCase()); if (hit) return send(res, 200, { uid: hit });
    fetch('https://users.roblox.com/v1/usernames/users', { method: 'POST', headers: { 'content-type': 'application/json', 'user-agent': 'dq-tracker/1.0' }, body: JSON.stringify({ usernames: [nm], excludeBannedUsers: false }) })
      .then(r => r.json()).then(j => {
        const id = j && j.data && j.data[0] && j.data[0].id;
        if (!id) return send(res, 404, { error: 'not_found', message: 'Roblox khong tra userId cho ' + nm });
        CFG_UID_CACHE.set(nm.toLowerCase(), String(id)); send(res, 200, { uid: String(id) });
      }).catch(e => send(res, 502, { error: 'roblox_failed', message: 'Khong goi duoc Roblox: ' + String(e && e.message || e) }));
    return;
  }
  if (p === '/api/config/workspace' && req.method === 'GET') return send(res, 200, { dir: CFG_DIR, input: CFG_INPUT, ok: !!CFG_DIR && fs.existsSync(CFG_DIR), releases: cfgCountReleases(), savedTo: CFG_SAVED_AT });
  if (p === '/api/config/workspace' && req.method === 'POST') {
    return readJson(req, res, b => {
      const dir = cfgResolveDir(b.path);
      if (!dir) return send(res, 400, { error: 'bad_dir', message: 'Khong thay file release_*.json / *_shared.json trong link nay. Dan link thu muc workspace\\HarvardLabs (hoac thu muc configs).' });
      CFG_DIR = dir; CFG_INPUT = cfgNormPath(b.path).slice(0, 500);
      let savedTo = '', err = '';   // ghi canh index.js; khong duoc thi ghi vao thu muc nguoi dung
      for (const f of [CFG_WS_FILE, CFG_WS_ALT]) { try { fs.writeFileSync(f, CFG_INPUT + '\n'); savedTo = f; break; } catch (e) { err = String(e.message || e); } }
      CFG_SAVED_AT = savedTo;
      return send(res, 200, { ok: true, dir, input: CFG_INPUT, releases: cfgCountReleases(), savedTo, saveError: savedTo ? '' : err });
    }, 2e4);
  }
  if (!CFG_DIR || !fs.existsSync(CFG_DIR)) return send(res, 404, { error: 'no_dir', dir: CFG_DIR, message: 'Chua noi workspace. Dan link workspace o tab Config roi bam Luu link.' });
  if (p === '/api/config/delete' && req.method === 'POST') {   // xoa du lieu config cua TAT CA alt: GIU file, chi dua cac key cua web ve gia tri trong (bool=false, so=0, chuoi='', list=[]); ban goc luu o tracker_cfg_deleted
    return readJson(req, res, b => {
      if (!CFG_DIR || !fs.existsSync(CFG_DIR)) return send(res, 400, { error: 'no_workspace', message: 'Chua noi workspace.' });
      if (b.confirm !== 'DELETE_ALL_ALT_CONFIG') return send(res, 400, { error: 'not_confirmed', message: 'Thieu xac nhan.' });
      let files, shared; try { files = cfgAltFiles(new Set()).map(f => f.file); shared = fs.readdirSync(CFG_DIR).filter(x => CFG_FILE_RE.test(x) && /_shared\.json$/.test(x)); } catch (e) { return send(res, 500, { error: 'read_failed', message: String(e.message || e) }); }
      if (!files.length && !shared.length) return send(res, 200, { ok: true, cleared: 0, dir: '' });
      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      const dest = path.join(CFG_DIR, '..', 'tracker_cfg_deleted', stamp);
      try { fs.mkdirSync(dest, { recursive: true }); } catch (e) { return send(res, 500, { error: 'mkdir_failed', message: String(e.message || e) }); }
      const blank = v => typeof v === 'boolean' ? false : typeof v === 'number' ? 0 : typeof v === 'string' ? '' : Array.isArray(v) ? [] : (v && typeof v === 'object') ? {} : v;
      let cleared = 0; const failed = [];
      for (const f of files.concat(shared)) {
        try {
          fs.copyFileSync(path.join(CFG_DIR, f), path.join(dest, f));   // ban goc de khoi phuc
          const c = cfgRead(f); let ch = false;
          for (const k of Object.keys(c.obj)) if (CFG_ALT_KEYS.has(k)) { const nv = blank(c.obj[k]); if (JSON.stringify(nv) !== JSON.stringify(c.obj[k])) { c.obj[k] = nv; ch = true; } }
          if (ch) cfgWrite(f, c);
          if (/^release_/.test(f)) cleared++;
        } catch (e) { failed.push(f); }
      }
      return send(res, 200, { ok: !failed.length, cleared, failed, dir: dest });
    }, 2e3);
  }
  if (p === '/api/config/files' && req.method === 'GET') {
    const files = fs.readdirSync(CFG_DIR).filter(f => CFG_FILE_RE.test(f)).map(f => {
      const st = fs.statSync(path.join(CFG_DIR, f));
      const um = /^release_(\d+)\.json$/.exec(f);
      return { file: f, size: st.size, mtime: Math.floor(st.mtimeMs), kind: /_shared\.json$/.test(f) ? 'shared' : 'release', uid: um ? um[1] : null };
    }).sort((x, y) => y.mtime - x.mtime);
    return send(res, 200, { dir: CFG_DIR, files });
  }
  if (p === '/api/config/create' && req.method === 'POST') {   // tao release_<uid>.json cho account chua co config (sao tu mot file mau, KHONG ghi de file co san)
    return readJson(req, res, b => {
      const uid = String(b.uid || ''), from = String(b.from || '');
      if (!/^\d{1,20}$/.test(uid)) return send(res, 400, { error: 'bad_uid' });
      if (!/^release_\d+\.json$/.test(from) || !fs.existsSync(path.join(CFG_DIR, from))) return send(res, 400, { error: 'bad_template', message: 'File mau khong hop le.' });
      const f = 'release_' + uid + '.json';
      try { fs.copyFileSync(path.join(CFG_DIR, from), path.join(CFG_DIR, f), fs.constants.COPYFILE_EXCL); }
      catch (e) { return send(res, e && e.code === 'EEXIST' ? 409 : 500, { error: e && e.code === 'EEXIST' ? 'exists' : 'create_failed', message: e && e.code === 'EEXIST' ? 'Config cua account nay da ton tai.' : String(e.message || e) }); }
      return send(res, 200, { ok: true, file: f, mtime: Math.floor(fs.statSync(path.join(CFG_DIR, f)).mtimeMs) });
    }, 2e4);
  }
  if (p === '/api/config/alt' && req.method === 'GET') {   // gia tri ALT = gia tri pho bien nhat giua cac file release_*.json (tru account bo qua); ?only=uid1,uid2 -> CHI doc (cac) file nay, bo qua exclude (dung khi chon rieng 1 account)
    const only = cfgExSet((u.searchParams.get('only') || '').split(','));
    const ex = cfgExSet((u.searchParams.get('exclude') || '').split(','));
    const files = only.size ? cfgAltFiles(new Set()).filter(f => only.has(f.uid)) : cfgAltFiles(ex), counts = {};
    for (const f of files) {
      let c; try { c = cfgRead(f.file); } catch { continue; }
      for (const k of CFG_ALT_KEYS) if (k in c.obj) { const j = JSON.stringify(c.obj[k]); const m = counts[k] || (counts[k] = new Map()); m.set(j, (m.get(j) || 0) + 1); }
    }
    const values = {}, mixed = [], secrets = [];
    for (const [k, m] of Object.entries(counts)) { let best = null; for (const [j, n] of m) if (!best || n > best[1]) best = [j, n]; values[k] = JSON.parse(best[0]); if (m.size > 1) mixed.push(k); }
    // webhook url / license key tra lai cho admin tren localhost (endpoint nay da chan nguoi khac) de web hien lai gia tri da luu
    return send(res, 200, { files: files.map(f => f.file), values, mixed, secrets });
  }
  if (p === '/api/config/alt' && req.method === 'POST') {   // ghi cac key ALT vao TAT CA file release_*.json (tru account bo qua) + file *_shared.json
    return readJson(req, res, b => {
      const vals = b.values && typeof b.values === 'object' && !Array.isArray(b.values) ? b.values : {};
      for (const k of Object.keys(vals)) if (!CFG_ALT_KEYS.has(k)) return send(res, 400, { error: 'bad_key', key: k });
      for (const k of CFG_SECRET) if (k in vals) { if (vals[k] === '') delete vals[k]; else if (typeof vals[k] !== 'string' || !(k === 'webhookUrl' ? CFG_WEBHOOK_RE : CFG_STATSKEY_RE).test(vals[k])) return send(res, 400, { error: 'bad_value', key: k, message: (k === 'webhookUrl' ? 'Webhook URL' : 'License key') + ' khong hop le.' }); }
      const classes = b.classes && typeof b.classes === 'object' ? b.classes : {};
      const only = cfgExSet(Array.isArray(b.only) ? b.only : []);   // chon rieng (cac) uid nay -> CHI ghi file cua chung, bo qua exclude va KHONG dong bo file _shared.json (file do la cau hinh dung chung cho moi account)
      const ex = cfgExSet(Array.isArray(b.exclude) ? b.exclude : []);
      let changedFiles = 0; const files = only.size ? cfgAltFiles(new Set()).filter(f => only.has(f.uid)) : cfgAltFiles(ex);
      try {
        for (const f of files) {
          const c = cfgRead(f.file); let ch = false;
          for (const [k, v] of Object.entries(vals)) {
            if (!(k in c.obj)) { if (CFG_ALT_NEW.has(k) && ((typeof v === 'number' && Number.isFinite(v) && v >= 0 && v <= 100000) || (typeof v === 'boolean' && (k === 'showHud' || k === 'fpsCap')))) { c.obj[k] = v; ch = true; } continue; }   // key chua co: chi tao cho cac key trong CFG_ALT_NEW
            const old = c.obj[k];
            if (!cfgTypeOk(v, old)) return send(res, 400, { error: 'bad_type', key: k });
            if (JSON.stringify(v) !== JSON.stringify(old)) { c.obj[k] = v; ch = true; }
          }
          const cl = classes[f.uid];
          if ((cl === 'mage' || cl === 'warrior') && typeof c.obj.classProfile === 'string' && c.obj.classProfile !== cl) { c.obj.classProfile = cl; ch = true; }
          if (ch) { cfgWrite(f.file, c); changedFiles++; }
        }
        if (!only.size) for (const sf of fs.readdirSync(CFG_DIR).filter(x => CFG_FILE_RE.test(x) && /_shared\.json$/.test(x))) {
          const sc = cfgRead(sf); let ch = false;
          for (const [k, v] of Object.entries(vals)) if (k in sc.obj && typeof sc.obj[k] === typeof v && Array.isArray(sc.obj[k]) === Array.isArray(v) && JSON.stringify(sc.obj[k]) !== JSON.stringify(v)) { sc.obj[k] = v; ch = true; }
          if (ch) cfgWrite(sf, sc);
        }
      } catch (e) { return send(res, 500, { error: 'write_failed', message: String(e.message || e) }); }
      return send(res, 200, { ok: true, files: files.length, changed: changedFiles });
    }, 2e5);
  }
  if (p === '/api/config/pdata' && req.method === 'GET') {
    const files = {};
    for (const n of Object.keys(PD_FILES)) files[PD_FILES[n]] = pdRead(n);
    if (!Object.keys(PD_FILES).some(n => !PD_DEFAULT[n] && files[PD_FILES[n]])) return send(res, 404, { error: 'no_data', message: 'Khong thay file party trong thu muc data canh configs (' + pdDir() + ').' });
    return send(res, 200, { dir: pdDir(), files });
  }
  if (p === '/api/config/pdata' && req.method === 'POST') {
    return readJson(req, res, b => {
      const n = String(b.name || ''); if (!PD_FILES[n]) return send(res, 400, { error: 'bad_name' });
      const cur = pdRead(n); if (!cur) return send(res, 404, { error: 'no_file', message: 'Khong thay file ' + PD_FILES[n] });
      if (b.mtime && cur.mtime && Math.abs(cur.mtime - +b.mtime) > 1) return send(res, 409, { error: 'conflict', message: 'File ' + PD_FILES[n] + ' vua bi thay doi (script dang ghi?). Tai lai roi sua lai.' });
      const out = pdValidate(n, b.value, cur.value);
      if (typeof out === 'string') return send(res, 400, { error: 'bad_value', message: out });
      try {
        const f = path.join(pdDir(), PD_FILES[n]); fs.mkdirSync(pdDir(), { recursive: true });
        try { const bd = path.join(CFG_DIR, '..', 'tracker_cfg_backup'); fs.mkdirSync(bd, { recursive: true }); fs.copyFileSync(f, path.join(bd, PD_FILES[n] + '.bak')); } catch {}
        const tmp = f + '.tmp' + process.pid; fs.writeFileSync(tmp, JSON.stringify(out)); fs.renameSync(tmp, f);
        return send(res, 200, { ok: true, mtime: Math.floor(fs.statSync(f).mtimeMs) });
      } catch (e) { return send(res, 500, { error: 'write_failed', message: String(e.message || e) }); }
    }, 2e5);
  }
  if (p === '/api/config/get' && req.method === 'GET') {
    const f = u.searchParams.get('file') || '';
    if (!CFG_FILE_RE.test(f)) return send(res, 400, { error: 'bad_file' });
    try {
      const c = cfgRead(f), values = { ...c.obj }, secrets = [];
      for (const k of CFG_SECRET) if (k in values) { values[k] = ''; secrets.push(k); }   // khong bao gio tra webhook url ra web
      return send(res, 200, { file: f, kind: c.kind, mtime: Math.floor(fs.statSync(path.join(CFG_DIR, f)).mtimeMs), values, secrets });
    } catch (e) { return send(res, 500, { error: 'read_failed', message: String(e.message || e) }); }
  }
  if (p === '/api/config/save' && req.method === 'POST') {
    return readJson(req, res, b => {
      const f = String(b.file || '');
      if (!CFG_FILE_RE.test(f) || !fs.existsSync(path.join(CFG_DIR, f))) return send(res, 400, { error: 'bad_file' });
      if (!b.values || typeof b.values !== 'object' || Array.isArray(b.values)) return send(res, 400, { error: 'bad_values' });
      try {
        const st = fs.statSync(path.join(CFG_DIR, f));
        if (b.mtime && Math.abs(st.mtimeMs - +b.mtime) > 1) return send(res, 409, { error: 'conflict', message: 'File vua bi thay doi (script dang chay ghi de?). Tai lai roi sua lai.' });
        const c = cfgRead(f); let n = 0;
        for (const [k, v] of Object.entries(b.values)) {
          if (!(k in c.obj)) return send(res, 400, { error: 'unknown_key', key: k });
          if (k.startsWith('__')) continue;
          const old = c.obj[k];
          if (CFG_SECRET.has(k)) { if (v === '' || v === old) continue; if (typeof v !== 'string' || !(k === 'webhookUrl' ? CFG_WEBHOOK_RE : CFG_STATSKEY_RE).test(v)) return send(res, 400, { error: 'bad_value', key: k, message: (k === 'webhookUrl' ? 'Webhook URL' : 'License key') + ' khong hop le.' }); }
          if (!cfgTypeOk(v, old)) return send(res, 400, { error: 'bad_type', key: k });
          if (JSON.stringify(v) === JSON.stringify(old)) continue;
          c.obj[k] = v; n++;
        }
        if (!n) return send(res, 200, { ok: true, saved: 0, mtime: Math.floor(st.mtimeMs) });
        const mtime = cfgWrite(f, c);
        // Cac key nam trong *_shared.json (bestgear / streamer / webhook, dung chung moi account): ghi them vao file shared, neu khong script se nap lai gia tri cu
        let shared = 0;
        if (c.kind !== 'shared') for (const sf of fs.readdirSync(CFG_DIR).filter(x => CFG_FILE_RE.test(x) && /_shared\.json$/.test(x))) {
          try {
            const sc = cfgRead(sf); let ch = false;
            for (const k of Object.keys(b.values)) if (k in sc.obj && k in c.obj && typeof sc.obj[k] === typeof c.obj[k] && Array.isArray(sc.obj[k]) === Array.isArray(c.obj[k]) && JSON.stringify(sc.obj[k]) !== JSON.stringify(c.obj[k])) { sc.obj[k] = c.obj[k]; ch = true; }
            if (ch) { cfgWrite(sf, sc); shared++; }
          } catch {}
        }
        return send(res, 200, { ok: true, saved: n, mtime, shared });
      } catch (e) { return send(res, 500, { error: 'write_failed', message: String(e.message || e) }); }
    }, 2e5);
  }
  return send(res, 404, { error: 'not_found' });
}

// Chi coi la "may chu" khi: Host la localhost + ket noi tu loopback + khong qua proxy/tunnel
const isLoopbackReq = req => /^(::1|127\.|::ffff:127\.)/.test(req.socket.remoteAddress || '')
  && !req.headers['x-forwarded-for'] && !req.headers['cf-connecting-ip'] && !req.headers['x-forwarded-host'];
const baseUrl = (req, host) => (MULTI && !isLocalHost(host)) ? PUBLIC_URL : (req.headers['x-forwarded-proto'] || 'http') + '://' + host;
const jsonForScript = v => JSON.stringify(v).replace(/</g, '\\u003c').replace(/>/g, '\\u003e').replace(/&/g, '\\u0026');

// ---- Phien dang nhap ----
const sessionUser = req => {
  const sid = parseCookies(req).dq_sid; if (!sid) return null;
  const h = sha(sid), ss = auth.sessions[h]; if (!ss) return null;
  if (ss.exp < Date.now()) { delete auth.sessions[h]; saveAuth(); return null; }
  return auth.users[ss.uid] || null;
};
const newSession = uid => {
  const now = Date.now();
  for (const [h, ss] of Object.entries(auth.sessions)) if (!ss || ss.exp < now) delete auth.sessions[h];
  const sid = rnd(32); auth.sessions[sha(sid)] = { uid, exp: now + SESSION_MS }; saveAuth(); return sid;
};

// ---- Xac thuc API: header x-api-key (admin key HOAC token cua user) hoac cookie phien ----
// -> null (chua dang nhap) | { denied } | { user, owner, admin, via }
function authenticate(req) {
  let k = req.headers['x-api-key'];
  if (!k) { try { k = new URL(req.url, 'http://x').searchParams.get('key') || undefined; } catch {} }   // EventSource (SSE) khong gui dc header rieng -> cho phep ?key= tren URL cho /api/stream
  if (k) {
    if (safeEq(k, KEY)) return { user: null, owner: 'admin', admin: true, via: 'key' };
    const uid = tokenIdx.get(String(k)), u = uid && auth.users[uid];
    if (u) return licActive(u) ? { user: u, owner: ownerOf(u), admin: isAdminUser(u), via: 'token' } : { denied: u.banned ? 'banned' : 'license_required' };
  }
  const su = MULTI ? sessionUser(req) : null;
  if (su) return licActive(su) ? { user: su, owner: ownerOf(su), admin: isAdminUser(su), via: 'cookie' } : { denied: su.banned ? 'banned' : 'license_required' };
  return null;
}

// ---- Key / license ----
const tries = new Map(); // chong do key: toi da 10 lan sai / 15 phut / user
const tooMany = uid => { const t = tries.get(uid); return !!t && t.reset > Date.now() && t.n >= 10; };
const failTry = uid => { const now = Date.now(); let t = tries.get(uid); if (!t || t.reset < now) t = { n: 0, reset: now + 15 * 60e3 }; t.n++; tries.set(uid, t); };
function redeemKey(u, input) {
  const k = normKey(input), lic = k && auth.licenses[k], now = Date.now();
  if (!lic || lic.revoked) return { error: 'invalid', message: 'Key không tồn tại hoặc đã bị thu hồi.' };
  if (lic.usedBy) return { error: 'used', message: 'Key này đã được sử dụng.' };
  const cur = u.license && (u.license.expiresAt === null || u.license.expiresAt > now) ? u.license : null; // license cu con han moi cong don
  const exp = (lic.days === 0 || (cur && cur.expiresAt === null)) ? null : Math.max(now, cur ? cur.expiresAt : 0) + lic.days * 864e5;
  const mx = !cur ? lic.maxAccounts : (cur.maxAccounts === 0 || lic.maxAccounts === 0 ? 0 : Math.max(cur.maxAccounts, lic.maxAccounts));
  u.license = { expiresAt: exp, maxAccounts: mx };
  u.redeemed = (u.redeemed || []).concat(k).slice(-50);
  lic.usedBy = u.id; lic.usedAt = now; saveAuth();
  console.log('[key] ' + u.name + ' (' + u.id + ') kich hoat ' + k + ' (' + (lic.days || 'vinh vien') + ' ngay)');
  return { ok: true };
}
const meInfo = (u, req, host) => {
  const admin = isAdminUser(u), o = ownerOf(u), l = u.license;
  return {
    id: u.id, name: u.name, username: u.username, avatar: avatarUrl(u), admin, active: licActive(u), banned: !!u.banned,
    lifetime: admin || !!(l && l.expiresAt === null), expiresAt: admin ? null : (l ? l.expiresAt : 0),
    maxAccounts: admin ? 0 : (l ? l.maxAccounts : 0), accounts: accCount(o),
    token: u.token, base: baseUrl(req, host)
  };
};

// ---- Giao dien: trang dang nhap / nhap key (cung phong cach trang /discord) ----
const SHELL_CSS = '*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;padding:20px;background:#08090b;color:#e8eaed;font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}main{width:min(440px,100%);padding:28px;border:1px solid #292d36;border-radius:20px;background:#12151a;text-align:center}h1{margin:0 0 4px;font-size:22px}p{margin:8px 0 18px;color:#a1a7b0}.btn{display:block;width:100%;padding:14px;border:0;border-radius:12px;background:#5865f2;color:#fff;text-decoration:none;font:700 15px system-ui,sans-serif;cursor:pointer;margin-top:10px}.btn:hover{background:#4752c4}.btn:disabled{opacity:.6;cursor:wait}.btn.alt{background:#1d2128;color:#e8eaed}.btn.alt:hover{background:#292d36}input{width:100%;padding:13px;border-radius:12px;border:1px solid #292d36;background:#0e1013;color:#e8eaed;font:600 15px ui-monospace,Consolas,monospace;text-align:center;letter-spacing:1px}.msg{min-height:20px;margin-top:10px;font-size:13px}.err{color:#f87171}.ok{color:#4ade80}.av{width:64px;height:64px;border-radius:50%;margin:0 auto 10px;display:block}';
const shell = (title, inner, script = '') => '<!doctype html><html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(title) + '</title><style>' + SHELL_CSS + '</style></head><body><main>' + inner + '</main>' + (script ? '<script>' + script + '</script>' : '') + '</body></html>';
const BUY_BTN = '<a class="btn alt" href="' + DISCORD_INVITE + '" target="_blank" rel="noopener noreferrer">Vào server Discord để mua key</a>';
const loginPage = err => shell('DQR Tracker',
  '<h1>DQR Tracker</h1><p>Đăng nhập bằng Discord để sử dụng tracker. Cần có key do admin cấp.</p>'
  + (err ? '<div class="msg err">' + esc(err) + '</div>' : '') + '<a class="btn" href="/auth/login">Đăng nhập với Discord</a>' + BUY_BTN);
const keyPage = u => {
  let note = 'Tài khoản chưa có key. Nhập key bạn đã mua để kích hoạt.';
  if (u.banned) note = 'Tài khoản đã bị khoá. Hãy liên hệ admin trên Discord.';
  else if (u.license) note = 'Key của bạn đã hết hạn ngày ' + fmtDate(u.license.expiresAt) + '. Nhập key mới để gia hạn (dữ liệu cũ vẫn được giữ).';
  const form = u.banned ? '' : '<input id="k" placeholder="DQ-XXXX-XXXX-XXXX-XXXX" autocomplete="off" spellcheck="false"><button class="btn" id="go" type="button">Kích hoạt key</button><div class="msg" id="m"></div>';
  return shell('Nhập key', '<img class="av" src="' + esc(avatarUrl(u)) + '" alt=""><h1>' + esc(u.name) + '</h1><p>' + esc(note) + '</p>' + form + BUY_BTN + '<button class="btn alt" id="out" type="button">Đăng xuất</button>', KEYPAGE_JS);
};

// ---- JS phia trinh duyet (khong dung backtick / dau $ { } trong cac khoi String.raw nay) ----
const KEYPAGE_JS = String.raw`
var H={'content-type':'application/json','x-requested-with':'dq'};
var m=document.getElementById('m'),k=document.getElementById('k'),go=document.getElementById('go');
function say(t,c){if(m){m.textContent=t;m.className='msg '+c}}
if(go){go.onclick=function(){var v=k.value.trim();if(!v)return;go.disabled=true;say('Đang kiểm tra...','');
  fetch('/api/redeem',{method:'POST',headers:H,credentials:'same-origin',body:JSON.stringify({key:v})})
   .then(function(r){return r.json()}).then(function(j){
    if(j.ok){say('Kích hoạt thành công!','ok');setTimeout(function(){location.href='/'},600)}
    else{go.disabled=false;say(j.message||'Key không hợp lệ','err')}})
   .catch(function(){go.disabled=false;say('Lỗi mạng, thử lại','err')})};
  k.addEventListener('keydown',function(e){if(e.key==='Enter')go.click()})}
document.getElementById('out').onclick=function(){fetch('/auth/logout',{method:'POST',headers:H,credentials:'same-origin'}).then(function(){location.href='/'})};
`;

// Nut tai khoan nho gan vao dashboard (script lua, token, nhap them key, dang xuat)
const WIDGET = String.raw`
(function(){
var H={'x-requested-with':'dq','content-type':'application/json'};
function api(p,body){var o={credentials:'same-origin',headers:H};if(body!==undefined){o.method='POST';o.body=JSON.stringify(body)}
  return fetch(p,o).then(function(r){return r.json().then(function(j){j.__s=r.status;return j})})}
function el(t,a,k){var e=document.createElement(t);a=a||{};for(var x in a){if(x==='text')e.textContent=a[x];else if(x==='on')e.onclick=a[x];else e.setAttribute(x,a[x])}(k||[]).forEach(function(c){e.appendChild(c)});return e}
var css=['#dqw{position:fixed;left:12px;bottom:calc(64px + env(safe-area-inset-bottom,0px));z-index:60;font:13px system-ui,sans-serif;color:var(--tx,#e8eaed)}',
'#dqb{display:flex;align-items:center;gap:10px;padding:6px 16px 6px 6px;border:1px solid var(--bd,#292d36);border-radius:999px;background:var(--cd,#12151a);color:var(--tx,#e8eaed);cursor:pointer;font:inherit;text-align:left}',
'#dqb img{width:36px;height:36px;border-radius:50%;flex:none;object-fit:cover;background:#5865f2}',
'#dqb .dqi{display:flex;flex-direction:column;line-height:1.25;min-width:0}',
'#dqb .dqn{font-weight:600;font-size:13px;color:var(--tx,#e8eaed);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
'#dqb .dqd{font:11px ui-monospace,Consolas,monospace;color:var(--mu,#8a9099)}',
'#dqb .dqr{display:flex;align-items:center;gap:6px;min-width:0}',
'#dqb .dqt{flex:none;padding:1px 8px;border-radius:999px;border:1px solid #7983f5;background:#5865f233;color:#c7ccff;font-size:10px;font-weight:700;letter-spacing:.03em;line-height:1.4}',
'#dqb .dqt.host{border-color:#f43f5e;background:#f43f5e33;color:#fda4af}',
'#dqp{position:absolute;left:0;bottom:46px;width:min(340px,calc(100vw - 24px));padding:14px;border:1px solid var(--bd,#292d36);border-radius:14px;background:var(--cd,#12151a);box-shadow:0 10px 30px #0008}',
'#dqp[hidden]{display:none}#dqp h4{margin:0 0 6px;font-size:14px}#dqp .mu{color:var(--mu,#8a9099);font-size:12px;margin:2px 0 8px}',
'#dqp textarea{width:100%;height:92px;resize:none;padding:8px;border-radius:8px;border:1px solid var(--bd,#292d36);background:var(--bg,#08090b);color:inherit;font:11px/1.4 ui-monospace,Consolas,monospace}',
'#dqp input{width:100%;padding:8px;border-radius:8px;border:1px solid var(--bd,#292d36);background:var(--bg,#08090b);color:inherit;font:12px ui-monospace,Consolas,monospace}',
'#dqp .row{display:flex;gap:6px;margin-top:6px;flex-wrap:wrap}',
'#dqp button,#dqp a.b{padding:7px 10px;border:1px solid var(--bd,#292d36);border-radius:8px;background:transparent;color:inherit;cursor:pointer;font:inherit;text-decoration:none}',
'#dqp button:hover,#dqp a.b:hover{background:#ffffff14}#dqp .ok{color:#4ade80}#dqp .er{color:#f87171}',
'#dqw.hdr{position:relative!important;left:auto!important;bottom:auto!important}',
'#dqw.hdr #dqp{left:0;bottom:auto;top:calc(100% + 8px)}',
'#dqp.v2{width:min(360px,calc(100vw - 24px));text-align:center;padding:22px 18px 16px}',
'#dqp.v2 .x{position:absolute;top:10px;right:10px;width:26px;height:26px;border:none;border-radius:8px;background:transparent;color:var(--mu,#8a9099);font-size:16px;line-height:1;cursor:pointer;display:flex;align-items:center;justify-content:center}',
'#dqp.v2 .x:hover{background:#ffffff14;color:var(--tx,#e8eaed)}',
'#dqp.v2 .bigav{width:72px;height:72px;border-radius:50%;object-fit:cover;background:#5865f2;margin:0 auto 10px;display:block;border:3px solid var(--bd,#292d36)}',
'#dqp.v2 .signed{display:flex;align-items:center;justify-content:center;gap:6px;font-size:13px;color:var(--mu,#8a9099);margin-bottom:8px}',
'#dqp.v2 .keyb{display:inline-block;padding:4px 12px;border-radius:999px;border:1px solid #f43f5e55;background:#f43f5e1a;color:#fda4af;font-size:11px;font-weight:600;margin-bottom:16px}',
'#dqp.v2 .sect{text-align:left}',
'#dqp.v2 .dlcard{display:flex;align-items:center;gap:10px;padding:10px;border:1px solid var(--bd,#292d36);border-radius:12px;background:var(--bg,#08090b);margin:14px 0 4px}',
'#dqp.v2 .dlic{width:34px;height:34px;border-radius:9px;background:#ffffff10;display:flex;align-items:center;justify-content:center;flex:none;font-size:16px}',
'#dqp.v2 .dltxt{flex:1;min-width:0;text-align:left}',
'#dqp.v2 .dltitle{font-size:13px;font-weight:600;color:var(--tx,#e8eaed)}',
'#dqp.v2 .dlsub{font-size:11px;color:var(--mu,#8a9099)}',
'#dqp.v2 .dlbtn{flex:none;padding:6px 12px;border-radius:8px;border:1px solid var(--bd,#292d36);background:transparent;color:inherit;text-decoration:none;font-size:12px}',
'#dqp.v2 .dlbtn:hover{background:#ffffff14}',
'#dqp.v2 .bottom{display:flex;gap:8px;margin-top:14px}',
'#dqp.v2 .bottom button,#dqp.v2 .bottom a{flex:1;padding:9px;border-radius:9px;border:1px solid #f43f5e55;background:#f43f5e14;color:#fda4af;font:inherit;cursor:pointer;text-align:center;text-decoration:none}',
'#dqp.v2 .bottom .done{border-color:#f43f5e;background:#f43f5e;color:#fff}',
'#dqp.v2 .bottom button:hover,#dqp.v2 .bottom a:hover{filter:brightness(1.15)}'].join('');
var st=el('style',{text:css});document.head.appendChild(st);
var root=el('div',{id:'dqw'}),panel=el('div',{id:'dqp',hidden:''}),btn=el('button',{id:'dqb',type:'button'});
btn.onclick=function(){panel.hidden=!panel.hidden};
root.appendChild(panel);root.appendChild(btn);
var hdrSlot=document.getElementById('hdrbadge');
if(hdrSlot){root.classList.add('hdr');hdrSlot.appendChild(root)}else{document.body.appendChild(root)}
function snippet(me){return '_G.Config = { Note = "PC1" }\nloadstring(game:HttpGet("'+me.base+'/script?t='+me.token+'"))()'}
function licText(me){
  if(me.admin)return 'Admin - không giới hạn';
  if(me.lifetime)return 'Key vĩnh viễn';
  var d=Math.ceil((me.expiresAt-Date.now())/864e5);
  return 'Còn '+d+' ngày (hết hạn '+new Date(me.expiresAt).toLocaleDateString('vi-VN')+')'}
function ensureInfoTab(me){
  var tabs2=document.getElementById('tabs'),tf2=document.getElementById('tf');
  if(!tabs2||!tf2||tabs2.querySelector('.tab[data-tab="info"]'))return;
  var DISCORD_JOIN_URL='https://discord.gg/5VAGQ6Bvfr';
  var css2=document.createElement('style');
  css2.textContent='.tabs>.sb-lbl.sb-lbl0{order:0!important}.tabs .tab[data-tab="info"]{order:0!important}'
    +'#pane-info .info-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}'
    +'@media (max-width:560px){#pane-info .info-grid{grid-template-columns:1fr}}'
    +'#pane-info .info-card{display:flex;align-items:center;gap:14px;padding:16px;border:1px solid var(--bd,#292d36);border-radius:14px;background:var(--cd,#12151a)}'
    +'#pane-info .info-card img{width:56px;height:56px;border-radius:50%;background:#5865f2;flex:none;object-fit:cover}'
    +'#pane-info .info-n{font-weight:700;font-size:16px;color:var(--tx,#e8eaed)}'
    +'#pane-info .info-d{font:12px ui-monospace,Consolas,monospace;color:var(--mu,#8a9099);margin-top:4px;cursor:pointer;display:inline-block}'
    +'#pane-info .info-d:hover{color:var(--tx,#e8eaed)}'
    +'#pane-info .info-by{margin-left:auto;padding-left:14px;text-align:right;font-size:12px;font-weight:600;color:var(--mu,#8a9099);line-height:1.4}'
    +'#pane-info .info-dc{display:flex;align-items:center;gap:14px;padding:16px;border-radius:14px;background:#5865f2;color:#fff;text-decoration:none;cursor:pointer;transition:filter .15s}'
    +'#pane-info .info-dc:hover{filter:brightness(1.08)}'
    +'#pane-info .info-dc:active{filter:brightness(.9)}'
    +'#pane-info .info-dc svg{width:32px;height:32px;flex:none;fill:#fff}'
    +'#pane-info .info-dt{font-weight:700;font-size:15px}'
    +'#pane-info .info-ds{font-size:12px;opacity:.85;margin-top:3px}'
    +'#pane-info .info-note{display:flex;align-items:center;gap:10px;margin-top:14px;padding:12px 14px;border:1px solid #7a5c12;border-radius:12px;background:rgba(251,191,36,.08);font-size:12px;color:var(--mu,#8a9099)}'
    +'#pane-info .info-note img{width:22px;height:22px;flex:none;border-radius:5px}'
    +'#pane-info .info-note b{color:var(--tx,#e8eaed)}';
  document.head.appendChild(css2);
  var lbl2=document.createElement('div');lbl2.className='sb-lbl sb-lbl0';lbl2.textContent='Information';
  var firstLbl=tabs2.querySelector('.sb-lbl');
  if(firstLbl)tabs2.insertBefore(lbl2,firstLbl);else tabs2.insertBefore(lbl2,tabs2.firstChild);
  var btn2=document.createElement('button');btn2.className='tab';btn2.dataset.tab='info';btn2.type='button';btn2.textContent='Information';
  tabs2.insertBefore(btn2,lbl2.nextSibling);
  var pane2=document.createElement('div');pane2.className='tabpane';pane2.dataset.pane='info';pane2.id='pane-info';pane2.style.display='none';
  pane2.innerHTML='<div class="ckhd"><div><b class="ckti">ℹ️ <span class="pf-title">Information</span></b></div></div>'
    +'<div class="info-grid">'
    +'<div class="info-card"><img src="'+me.avatar+'" alt="" referrerpolicy="no-referrer"><div><div class="info-n"></div><div class="info-d" title="Bấm để copy ID">ID Discord: '+me.id+'</div></div><div class="info-by">Tracker By<br>Nguyễn Phan Trí Bảo</div></div>'
    +'<a class="info-dc" href="'+DISCORD_JOIN_URL+'" target="_blank" rel="noopener noreferrer"><img src="data:image/png;base64,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" alt="" style="width:32px;height:32px;flex:none;border-radius:8px;object-fit:cover">'
    +'<div><div class="info-dt">Tham gia Discord</div><div class="info-ds">Bấm để tham gia sever Rô Bờ Lốc👿</div></div></a>'
    +'</div>'
    +'<div class="info-note"><span>⚠️ Lưu ý: tab <b>Config</b> hiện chỉ tích hợp cho <b>Harvard Labs</b>.</span></div>';
  pane2.querySelector('.info-n').textContent=me.name;
  tf2.appendChild(pane2);
  var didEl=pane2.querySelector('.info-d');
  didEl.onclick=function(){
    var orig='ID Discord: '+me.id;
    if(navigator.clipboard)navigator.clipboard.writeText(me.id).then(function(){didEl.textContent='Đã copy ID!';setTimeout(function(){didEl.textContent=orig},1200)});
  };
}
function render(me){
  ensureInfoTab(me);
  btn.textContent='';
  var av=el('img',{src:me.avatar,alt:'',referrerpolicy:'no-referrer'});
  av.onerror=function(){av.onerror=null;av.src='https://cdn.discordapp.com/embed/avatars/0.png'};
  btn.appendChild(av);
  btn.appendChild(el('span',{class:'dqi'},[el('span',{class:'dqr'},[el('span',{class:'dqn',text:me.username||me.name}),el('span',{class:me.admin?'dqt host':'dqt',text:me.admin?'HOST':'Members'})]),el('span',{class:'dqd',text:'id:'+me.id})]));
  panel.textContent='';
  panel.className='v2';

  panel.appendChild(el('button',{type:'button',class:'x','aria-label':'Đóng',text:'×',on:function(){panel.hidden=true}}));
  var av2=el('img',{class:'bigav',src:me.avatar,alt:'',referrerpolicy:'no-referrer'});
  av2.onerror=function(){av2.onerror=null;av2.src='https://cdn.discordapp.com/embed/avatars/0.png'};
  panel.appendChild(av2);
  panel.appendChild(el('div',{class:'signed',text:'Đã đăng nhập bằng Discord'}));
  panel.appendChild(el('div',{class:'keyb',text:licText(me)}));

  panel.appendChild(el('div',{class:'dlcard'},[
    el('span',{class:'dlic',text:'⬇'}),
    el('span',{class:'dltxt'},[el('div',{class:'dltitle',text:'DQR Tracker for Windows'}),el('div',{class:'dlsub',text:'Cho mọi máy tính và VPS bạn chạy'})]),
    el('a',{class:'dlbtn',href:'https://github.com/HuntMinex5/dqr-tracker-app/releases/latest/download/DQR-Tracker-Setup.exe',text:'Tải xuống'})]));

  var bottom=[];
  if(me.admin)bottom.push(el('a',{href:'/admin',text:'DEV Key'}));
  bottom.push(el('button',{type:'button',text:'Đăng xuất',on:function(){api('/auth/logout',{}).then(function(){location.href='/'})}}));
  bottom.push(el('button',{type:'button',class:'done',text:'Xong',on:function(){panel.hidden=true}}));
  panel.appendChild(el('div',{class:'bottom'},bottom));
}
function load(){return api('/api/me').then(function(me){if(me.__s!==200||!me.active){location.reload();return}render(me)}).catch(function(){})}
load();setInterval(function(){api('/api/me').then(function(me){if(me.__s!==200||!me.active)location.reload()}).catch(function(){})},60000);
})();
`;

// ---- O "lay script" co dinh o goc duoi trai (che do 1 nguoi dung / may ca nhan) ----
const SCRIPT_BOX = String.raw`
(function(){
var NAME='nguyenphantribao',DID='1226009317053042719';
var css='#dqs{position:fixed;left:12px;bottom:calc(12px + env(safe-area-inset-bottom,0px));z-index:60;font:13px system-ui,sans-serif;color:var(--tx,#e8eaed)}'
+'#dqsb{display:flex;align-items:center;gap:0;padding:4px;border:1px solid var(--bd,#292d36);border-radius:999px;background:var(--cd,#12151a);color:var(--tx,#e8eaed);cursor:pointer;font:inherit;text-align:left}'
+'#dqsb .dqsa{width:36px;height:36px;border-radius:50%;flex:none;background:#5865f2;display:flex;align-items:center;justify-content:center;color:#fff}#dqsb .dqsa svg{width:20px;height:20px}'
+'#dqsb .dqsi{display:flex;flex-direction:column;line-height:1.25;min-width:0;max-width:0;opacity:0;overflow:hidden;white-space:nowrap;margin:0;transition:max-width .25s ease,opacity .2s ease,margin .25s ease}#dqsb:hover .dqsi,#dqsb:focus-visible .dqsi{max-width:260px;opacity:1;margin:0 12px 0 10px}#dqsb .dqsn{font-weight:600;font-size:13px}#dqsb .dqsd{font:11px ui-monospace,Consolas,monospace;color:var(--mu,#8a9099)}'
+'#dqsp{position:absolute;left:0;bottom:54px;width:min(360px,calc(100vw - 24px));padding:14px;border:1px solid var(--bd,#292d36);border-radius:14px;background:var(--cd,#12151a);box-shadow:0 10px 30px #0008}#dqsp[hidden]{display:none}'
+'#dqsp h4{margin:0 0 4px;font-size:14px}#dqsp .mu{color:var(--mu,#8a9099);font-size:12px;margin:2px 0 8px}'
+'#dqsp textarea{width:100%;height:92px;resize:vertical;padding:8px;border-radius:8px;border:1px solid var(--bd,#292d36);background:var(--bg,#08090b);color:inherit;font:11px/1.4 ui-monospace,Consolas,monospace;box-sizing:border-box}'
+'#dqsp button{margin-top:8px;padding:7px 12px;border:1px solid var(--bd,#292d36);border-radius:8px;background:transparent;color:inherit;cursor:pointer;font:inherit}#dqsp button:hover{background:#ffffff14}#dqsp .ok{color:#4ade80}#dqsp .er{color:#f87171}';
var st=document.createElement('style');st.textContent=css;document.head.appendChild(st);
var root=document.createElement('div');root.id='dqs';
root.innerHTML='<div id="dqsp" hidden><h4>Script của bạn</h4><div class="mu">Dán vào executor (sửa Note = tên máy/acc):</div><textarea id="dqst" spellcheck="false"></textarea><div><button type="button" id="dqsc">Copy script</button></div><div class="mu" id="dqsm" style="margin:6px 0 0"></div></div>'
+'<button id="dqsb" type="button"><span class="dqsa"><svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M19.7 5.4A18.8 18.8 0 0 0 15 4l-.6 1.2a17.3 17.3 0 0 0-4.8 0L9 4a18.8 18.8 0 0 0-4.7 1.4C1.3 9.8.5 14.1.9 18.4a19 19 0 0 0 5.8 2.9l1.2-2a12 12 0 0 1-1.9-.9l.5-.4a13.6 13.6 0 0 0 15 0l.5.4a12 12 0 0 1-1.9.9l1.2 2a19 19 0 0 0 5.8-2.9c.5-5-.8-9.2-3.4-13zM8.8 15.7c-1 0-1.8-.9-1.8-2s.8-2 1.8-2 1.8.9 1.8 2-.8 2-1.8 2zm6.4 0c-1 0-1.8-.9-1.8-2s.8-2 1.8-2 1.8.9 1.8 2-.8 2-1.8 2z"/></svg></span><span class="dqsi"><span class="dqsn"></span><span class="dqsd"></span></span></button>';
document.body.appendChild(root);
root.querySelector('.dqsn').textContent=NAME;root.querySelector('.dqsd').textContent='ID Dis:'+DID;
var panel=root.querySelector('#dqsp'),ta=root.querySelector('#dqst'),msg=root.querySelector('#dqsm');
function say(t,c){msg.textContent=t;msg.className='mu '+(c||'')}
function snippet(){var k=(typeof KEYV!=='undefined'&&KEYV)||'';return '_G.Config = { Note = "PC1" }\nloadstring(game:HttpGet("'+location.origin+'/script'+(k?'?t='+encodeURIComponent(k):'')+'"))()'}
root.querySelector('#dqsb').onclick=function(){panel.hidden=!panel.hidden;if(!panel.hidden){ta.value=snippet();say('')}};
root.querySelector('#dqsc').onclick=function(){ta.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}
  if(navigator.clipboard)navigator.clipboard.writeText(ta.value).then(function(){say('Đã copy','ok')},function(){say(ok?'Đã copy':'Hãy copy thủ công','er')});else say(ok?'Đã copy':'Hãy copy thủ công',ok?'ok':'er')};
document.addEventListener('click',function(e){if(!panel.hidden&&!root.contains(e.target))panel.hidden=true});
})();
`;
// Badge co dinh (anh + ten + ID Discord chu tool) cho che do DON (khong dang nhap Discord) - dat o goc cu cua SCRIPT_BOX, xep tren nut VI/EN de khong de nhau
const OWNER_BADGE_JS = String.raw`
(function(){
var NAME='nguyenphantribao',DID='1226009317053042719';
var css='#dqo{position:fixed;left:12px;bottom:calc(64px + env(safe-area-inset-bottom,0px));z-index:60;font:13px system-ui,sans-serif;color:var(--tx,#e8eaed)}'
+'#dqo .dqob{display:flex;align-items:center;gap:10px;padding:6px 16px 6px 6px;border:1px solid var(--bd,#292d36);border-radius:999px;background:var(--cd,#12151a)}'
+'#dqo img{width:36px;height:36px;border-radius:50%;flex:none;object-fit:cover;background:#5865f2}'
+'#dqo .dqoi{display:flex;flex-direction:column;line-height:1.25;min-width:0}'
+'#dqo .dqon{font-weight:600;font-size:13px;color:var(--tx,#e8eaed);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
+'#dqo .dqod{font:11px ui-monospace,Consolas,monospace;color:var(--mu,#8a9099)}'
+'#dqo.hdr{position:relative!important;left:auto!important;bottom:auto!important}';
var st=document.createElement('style');st.textContent=css;document.head.appendChild(st);
var root=document.createElement('div');root.id='dqo';
root.innerHTML='<div class="dqob"><img src="https://cdn.discordapp.com/embed/avatars/0.png" alt=""><span class="dqoi"><span class="dqon"></span><span class="dqod"></span></span></div>';
root.querySelector('.dqon').textContent=NAME;
root.querySelector('.dqod').textContent='id:'+DID;
var hdrSlot=document.getElementById('hdrbadge');
if(hdrSlot){root.classList.add('hdr');hdrSlot.appendChild(root)}else{document.body.appendChild(root)}
// ---- Tab "Information" rieng, dat tren cung sidebar (tren ca nhom TRACKER) - hien ten + ID Discord chu tool ----
var tabs2=document.getElementById('tabs'),tf2=document.getElementById('tf');
if(tabs2&&tf2&&!tabs2.querySelector('.tab[data-tab="info"]')){
  var DISCORD_JOIN_URL='https://discord.gg/5VAGQ6Bvfr';
  var css2=document.createElement('style');
  css2.textContent='.tabs>.sb-lbl.sb-lbl0{order:0!important}.tabs .tab[data-tab="info"]{order:0!important}'
    +'#pane-info .info-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}'
    +'@media (max-width:560px){#pane-info .info-grid{grid-template-columns:1fr}}'
    +'#pane-info .info-card{display:flex;align-items:center;gap:14px;padding:16px;border:1px solid var(--bd,#292d36);border-radius:14px;background:var(--cd,#12151a)}'
    +'#pane-info .info-card img{width:56px;height:56px;border-radius:50%;background:#5865f2;flex:none;object-fit:cover}'
    +'#pane-info .info-n{font-weight:700;font-size:16px;color:var(--tx,#e8eaed)}'
    +'#pane-info .info-d{font:12px ui-monospace,Consolas,monospace;color:var(--mu,#8a9099);margin-top:4px;cursor:pointer;display:inline-block}'
    +'#pane-info .info-d:hover{color:var(--tx,#e8eaed)}'
    +'#pane-info .info-by{margin-left:auto;padding-left:14px;text-align:right;font-size:12px;font-weight:600;color:var(--mu,#8a9099);line-height:1.4}'
    +'#pane-info .info-dc{display:flex;align-items:center;gap:14px;padding:16px;border-radius:14px;background:#5865f2;color:#fff;text-decoration:none;cursor:pointer;transition:filter .15s}'
    +'#pane-info .info-dc:hover{filter:brightness(1.08)}'
    +'#pane-info .info-dc:active{filter:brightness(.9)}'
    +'#pane-info .info-dc svg{width:32px;height:32px;flex:none;fill:#fff}'
    +'#pane-info .info-dt{font-weight:700;font-size:15px}'
    +'#pane-info .info-ds{font-size:12px;opacity:.85;margin-top:3px}'
    +'#pane-info .info-note{display:flex;align-items:center;gap:10px;margin-top:14px;padding:12px 14px;border:1px solid #7a5c12;border-radius:12px;background:rgba(251,191,36,.08);font-size:12px;color:var(--mu,#8a9099)}'
    +'#pane-info .info-note img{width:22px;height:22px;flex:none;border-radius:5px}'
    +'#pane-info .info-note b{color:var(--tx,#e8eaed)}';
  document.head.appendChild(css2);
  var lbl2=document.createElement('div');lbl2.className='sb-lbl sb-lbl0';lbl2.textContent='Information';
  var firstLbl=tabs2.querySelector('.sb-lbl');
  if(firstLbl)tabs2.insertBefore(lbl2,firstLbl);else tabs2.insertBefore(lbl2,tabs2.firstChild);
  var btn2=document.createElement('button');btn2.className='tab';btn2.dataset.tab='info';btn2.type='button';btn2.textContent='Information';
  tabs2.insertBefore(btn2,lbl2.nextSibling);
  var pane2=document.createElement('div');pane2.className='tabpane';pane2.dataset.pane='info';pane2.id='pane-info';pane2.style.display='none';
  pane2.innerHTML='<div class="ckhd"><div><b class="ckti">ℹ️ <span class="pf-title">Information</span></b></div></div>'
    +'<div class="info-grid">'
    +'<div class="info-card"><img src="https://cdn.discordapp.com/embed/avatars/0.png" alt=""><div><div class="info-n"></div><div class="info-d" title="Bấm để copy ID">ID Discord: '+DID+'</div></div><div class="info-by">Tracker By<br>Nguyễn Phan Trí Bảo</div></div>'
    +'<a class="info-dc" href="'+DISCORD_JOIN_URL+'" target="_blank" rel="noopener noreferrer"><img src="data:image/png;base64,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" alt="" style="width:32px;height:32px;flex:none;border-radius:8px;object-fit:cover">'
    +'<div><div class="info-dt">Tham gia Discord</div><div class="info-ds">Bấm để tham gia sever Rô Bờ Lốc👿</div></div></a>'
    +'</div>'
    +'<div class="info-note"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACoAAAAmCAYAAACyAQkgAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAASxSURBVFhHlZi/i51FFIafmbsbYyMhKhFD1jWNRRKwEFMoWERELMRCISBoIdhaW1paCFYiign6D4iWRlEEI0gKS4PI7mJSBExAG0129xuLe9677z073927LxxmvplzzzxzZr4fd8qZyaQxogKcatPuJ1rjZLSvRVsxv2zeLzVgqxRaa1AKN4BbpbAB3CkFor1XljOTScsdrTUeA94YhtlgNcoMIqudPpXKxGD1lmwT+LZWNqM/awpqqsCFYeB8a/uy5EC57JnkQILN1yq3SuFSKQz7MlrrDLQC70YWexA1bGJ12RgkKZNDgtW12wZwuVYGi1EpZUpdChc6WRTICrAKHAm7zyxf576jYdlvNdkk7DTwYtwHlOm0p1uvNdZamy13zpYgHdQtg+U+lQLOkzsSgCu2Ws+0xuPBtgdayuzG8Uxqhj3I3rVspTM5r+esyleggn1hGOYzetyWXFnUDB0gQwnIg/f2r8dTmc1/J4514KhndK0Dmc1n7APk4Fg97/c8hmJ4bMVQnLNRrwAnWhuFVKDc7nCC0J18kDKwxlFcwbZgQ20nOz/MATKgTGrALvB7lMsAY+NmU3y9DStAiYz6jLJ5nwNKDfgHeBW4cwhQOoCqE2xN1+sJprc0DujZVJCfgYvANeBhG2hMeSIOq7EKzN/13unOOcN5uQmfz4EPgW3g/mj3iS2rPH4B1n2PCiCbw3p7Dv4HcN2yNAE+A75PvovURsbWeDNQV57VWDaxNr2XG/AD8CXwNfCT+Up52V0OqhIr5ySHPKuDNMSN9DFwE7gCfAT8vQAux85ZncuoSx05kzlgVgNuA88CnwDnov1X4MKCR1azZZcyAxlUgTLkMirAg8CPwDFrexL4bomVyZPICdqXUczhsLAVeChlZDUmoLYMJPkY8nHfOVB3XhbQP4oZ+V03G0ljE1B7N0ZvsJ4K8FvszWU0BiNpv/ZUfZncpEXAK8AH9ghqwL8LBjuM/G8IwN4fplAGX6Rt4FPg5bj+C3gqyhx3WY1ldZbRXicL2qUBeAd4L26kK8DxJSYp+Y3j5n1ojyrNnsWxmfV0DHgOeB84Mbbxl9Ai2FnMnkN2HlONR9DTMVH5nwOej4d9T83887jeRilTUDXqUeP/uw+ClIrN+s/41HsFeBPYSb4uj5/HFAvxvcxGKXOQQ2RhN0EfpBZQbwFvA69FDM9yT73kiGPTv0dbfEULrmcepDdoi4+P8/GuPz3i58oxBee269+jNyOjQ2RkN0pZL7s94AeAr4BHFtxQ+fe+ehpD5QBcq3Vvj95KzjvxjNwG7kUpaJ+tZ4FY4kfjA8R1ENyYDUxXe7ZHr8eBlEPuGOS9MPXJrwfuYN7Xg/NVyysn26zTtakA91qbAxVktrtWzxmX5cHVtsjH4XL9tg6Nz8b56FprvB7nT9X+feo0xE8z5KMXhBudt5LvZ687lE9E7ZdrZbMU2mzPt8aNUvglbirPrH5818y3gzJ9F/gv+bmPZ9Pj57ogr8aRufboLKME9cVh4FQ6fvS/zfm8CXtOLsqufFQf4lpwyuQ2sAV8MZkw2FH9FFRH0AFwZhh4qXPC17MsB8vWA92JujJ6tRS+CRZNvgH/A8aSLCXO4D20AAAAAElFTkSuQmCC" alt=""><span>Lưu ý: tab <b>Config</b> hiện chỉ tích hợp cho <b>Harvard Labs</b>.</span></div>';
  pane2.querySelector('.info-n').textContent=NAME;
  tf2.appendChild(pane2);
  var didEl=pane2.querySelector('.info-d');
  didEl.onclick=function(){
    var orig='ID Discord: '+DID;
    if(navigator.clipboard)navigator.clipboard.writeText(DID).then(function(){didEl.textContent='Đã copy ID!';setTimeout(function(){didEl.textContent=orig},1200)});
  };
}
})();
`;
const injectBox = page => page.replace('</body>', () => '<script>' + OWNER_BADGE_JS + '</script></body>');   // che do don: hien badge anh/ten/ID co dinh thay cho nut Discord/Get Script cu

// ---- Trang quan tri /admin ----
const ADMIN_CSS = ':root{--bg:#08090b;--cd:#12151a;--bd:#292d36;--tx:#e8eaed;--mu:#8a9099;--ac:#5865f2}*{box-sizing:border-box}body{margin:0;padding:16px;background:var(--bg);color:var(--tx);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}header{display:flex;justify-content:space-between;align-items:center;max-width:1100px;margin:0 auto 14px}h1{margin:0;font-size:20px}h2{margin:0 0 10px;font-size:16px}a{color:#8ea1ff}.card{max-width:1100px;margin:0 auto 14px;padding:16px;border:1px solid var(--bd);border-radius:14px;background:var(--cd)}.row{display:flex;gap:8px;flex-wrap:wrap;align-items:end}label{display:flex;flex-direction:column;gap:3px;color:var(--mu);font-size:12px}input,select,textarea{padding:8px 10px;border-radius:8px;border:1px solid var(--bd);background:var(--bg);color:var(--tx);font:inherit}input[type=number]{width:110px}button{padding:8px 12px;border:1px solid var(--bd);border-radius:8px;background:#1d2128;color:var(--tx);cursor:pointer;font:inherit}button:hover{background:#292d36}button.pri{background:var(--ac);border-color:var(--ac);font-weight:700}button.pri:hover{background:#4752c4}button.dg:hover{background:#7f1d1d}.mu{color:var(--mu)}.tw{overflow-x:auto;margin-top:10px}table{width:100%;border-collapse:collapse;min-width:640px}th,td{padding:7px 8px;border-bottom:1px solid var(--bd);text-align:left;white-space:nowrap}th{color:var(--mu);font-weight:600;font-size:12px}td.k{font-family:ui-monospace,Consolas,monospace}.tag{display:inline-block;padding:1px 8px;border-radius:999px;font-size:11px;font-weight:700}.t-free{background:#4ade8026;color:#4ade80}.t-used{background:#60a5fa26;color:#60a5fa}.t-rev{background:#f8717126;color:#f87171}.av{width:22px;height:22px;border-radius:50%;vertical-align:middle;margin-right:6px}textarea{width:100%;height:120px;font:12px ui-monospace,Consolas,monospace}.acts button{padding:3px 8px;font-size:12px;margin-right:4px}';
const ADMIN_BODY = '<header><h1>Quản lý key &amp; người dùng</h1><a href="/">← Dashboard</a></header>'
  + '<section class="card"><h2>Tạo key mới</h2><div class="row">'
  + '<label>Số lượng<input id="n" type="number" value="1" min="1" max="200"></label>'
  + '<label>Số ngày (0 = vĩnh viễn)<input id="d" type="number" value="30" min="0" max="3650"></label>'
  + '<label>Số acc tối đa (0 = không giới hạn)<input id="m" type="number" value="5" min="0" max="1000"></label>'
  + '<label>Ghi chú (tên người mua)<input id="nt" maxlength="60" placeholder="vd: Minh - 30 ngày"></label>'
  + '<button class="pri" id="gen" type="button">Tạo key</button></div>'
  + '<div class="row" style="margin-top:8px"><span class="mu">Chọn nhanh:</span><button data-d="7" type="button">7 ngày</button><button data-d="30" type="button">30 ngày</button><button data-d="90" type="button">90 ngày</button><button data-d="365" type="button">1 năm</button><button data-d="0" type="button">Vĩnh viễn</button></div>'
  + '<div id="outw" hidden style="margin-top:10px"><div class="mu">Key vừa tạo (mỗi dòng một key):</div><textarea id="out" readonly></textarea><div class="row" style="margin-top:6px"><button id="cp" type="button">Copy tất cả</button></div></div></section>'
  + '<section class="card"><h2>Danh sách key <span class="mu" id="kc"></span></h2><div class="row"><select id="kf"><option value="all">Tất cả</option><option value="free">Chưa dùng</option><option value="used">Đã dùng</option><option value="rev">Đã thu hồi</option></select><input id="ks" placeholder="Tìm key / ghi chú / người dùng"></div><div class="tw"><table><thead><tr><th>Key</th><th>Thời hạn</th><th>Max acc</th><th>Ghi chú</th><th>Trạng thái</th><th>HWID</th><th>Tạo lúc</th><th></th></tr></thead><tbody id="kb"></tbody></table></div></section>'
  + '<section class="card"><h2>Người dùng <span class="mu" id="uc"></span></h2><div class="row" style="margin-bottom:10px"><button type="button" class="dg" id="pg"></button></div><div class="tw"><table><thead><tr><th>Người dùng</th><th>Discord ID</th><th>Key</th><th>Acc</th><th>Đăng nhập gần nhất</th><th></th></tr></thead><tbody id="ub"></tbody></table></div></section>';
const ADMIN_JS = String.raw`
var AK='';
var H={'x-requested-with':'dq','content-type':'application/json'};if(AK)H['x-api-key']=AK;
function api(path,body){var o={credentials:'same-origin',headers:H};if(body!==undefined){o.method='POST';o.body=JSON.stringify(body)}
  return fetch(path,o).then(function(r){return r.json().then(function(j){if(!r.ok)throw new Error(j.error||('HTTP '+r.status));return j})})}
var $=function(id){return document.getElementById(id)};
function el(t,a,k){var e=document.createElement(t);a=a||{};for(var x in a){if(x==='text')e.textContent=a[x];else if(x==='on')e.onclick=a[x];else e.setAttribute(x,a[x])}(k||[]).forEach(function(c){if(c)e.appendChild(c)});return e}
function fmt(ms){return ms?new Date(ms).toLocaleString('vi-VN'):'-'}
function lic(u){if(u.admin)return 'Admin';if(!u.license)return 'Chưa có';if(u.banned)return 'Bị khoá';if(u.license.expiresAt===null)return 'Vĩnh viễn'+(u.license.maxAccounts?' (max '+u.license.maxAccounts+')':'');
  var d=Math.ceil((u.license.expiresAt-Date.now())/864e5);return (d>0?'Còn '+d+' ngày':'Hết hạn')+' (max '+(u.license.maxAccounts||'∞')+')'}
var S={users:[],licenses:[]},OPEN={};
function load(){return api('/admin/api/state').then(function(j){S=j;render()}).catch(function(e){alert('Lỗi: '+e.message)})}
function act(path,body,confirmMsg){api(path,body).then(load).catch(function(e){alert('Lỗi: '+e.message)})}
function keyTerm(l){if(l.eff){if(l.eff.expiresAt===null)return 'Vĩnh viễn';var d=Math.ceil((l.eff.expiresAt-Date.now())/864e5);return d>0?'Còn '+d+' ngày':'Hết hạn'}return l.days?l.days+' ngày':'Vĩnh viễn'}
function keyMax(l){return l.eff?(l.eff.maxAccounts||'∞'):(l.maxAccounts||'∞')}
function stTag(l){return l.revoked?el('span',{class:'tag t-rev',text:'Thu hồi'}):l.usedBy?el('span',{class:'tag t-used',text:'Đã dùng: '+(l.usedByName||l.usedBy)}):el('span',{class:'tag t-free',text:'Chưa dùng'})}
function render(){
  var f=$('kf').value,q=$('ks').value.trim().toLowerCase(),kb=$('kb');kb.textContent='';var shown=0;
  S.licenses.forEach(function(l){
    var s=l.revoked?'rev':l.usedBy?'used':'free';if(f!=='all'&&f!==s)return;
    if(q&&(l.key+' '+(l.note||'')+' '+(l.usedByName||'')).toLowerCase().indexOf(q)<0)return;shown++;
    var a=el('td',{class:'acts'});
    a.appendChild(el('button',{type:'button',text:'Copy',on:function(){navigator.clipboard&&navigator.clipboard.writeText(l.key)}}));
    if(l.usedBy&&l.hwid)a.appendChild(el('button',{type:'button',text:'Reset HWID',on:function(){act('/admin/api/keys/resethwid',{key:l.key},'Reset HWID của key này? Người dùng sẽ gắn lại được với thiết bị mới.')}}));
    if(!l.revoked)a.appendChild(el('button',{type:'button',class:'dg',text:'Thu hồi',on:function(){act('/admin/api/keys/revoke',{key:l.key},'Thu hồi key này'+(l.usedBy?' (người dùng sẽ mất quyền ngay)':'')+'?')}}));
    if(!l.usedBy||l.revoked)a.appendChild(el('button',{type:'button',class:'dg',text:'Xoá',on:function(){act('/admin/api/keys/delete',{key:l.key},'Xoá hẳn key này?')}}));
    kb.appendChild(el('tr',{},[el('td',{class:'k',text:l.key}),el('td',{text:keyTerm(l)}),el('td',{text:keyMax(l)}),el('td',{text:l.note||''}),el('td',{},[stTag(l)]),el('td',{class:'k',title:l.hwidAt?fmt(l.hwidAt):'',text:l.usedBy?(l.hwid||'Chưa gắn'):'-'}),el('td',{text:fmt(l.createdAt)}),a]));
  });
  $('kc').textContent='('+shown+'/'+S.licenses.length+')';
  var ub=$('ub');ub.textContent='';
  S.users.forEach(function(u){
    var a=el('td',{class:'acts'});
    if(!u.admin){
      a.appendChild(el('button',{type:'button',text:'+/- ngày',on:function(){var d=prompt('Cộng thêm bao nhiêu ngày? (số âm để trừ)','30');if(d===null)return;act('/admin/api/users',{id:u.id,action:'addDays',days:Number(d)})}}));
      a.appendChild(el('button',{type:'button',text:'Max acc',on:function(){var d=prompt('Số acc tối đa (0 = không giới hạn):',(u.license&&u.license.maxAccounts)||'5');if(d===null)return;act('/admin/api/users',{id:u.id,action:'setMax',maxAccounts:Number(d)})}}));
      a.appendChild(el('button',{type:'button',text:'Vĩnh viễn',on:function(){act('/admin/api/users',{id:u.id,action:'setLifetime'},'Cấp quyền vĩnh viễn cho '+u.name+'?')}}));
      if(u.license)a.appendChild(el('button',{type:'button',class:'dg',text:'Xoá quyền',on:function(){act('/admin/api/users',{id:u.id,action:'clearLicense'},'Xoá quyền của '+u.name+'?')}}));
      a.appendChild(el('button',{type:'button',class:'dg',text:u.banned?'Mở khoá':'Khoá',on:function(){act('/admin/api/users',{id:u.id,action:u.banned?'unban':'ban'},(u.banned?'Mở khoá ':'Khoá ')+u.name+'?')}}));
    }
    var name=el('td',{},[el('img',{class:'av',src:u.avatar,alt:''}),document.createTextNode(u.name+(u.username&&u.username!==u.name?' (@'+u.username+')':''))]);
    var cnt=el('td',{},[el('button',{type:'button',text:u.accounts+(OPEN[u.id]?' ▴':' ▾'),on:function(){
      if(OPEN[u.id]){delete OPEN[u.id];render();return}
      api('/admin/api/accounts?id='+encodeURIComponent(u.id)).then(function(j){OPEN[u.id]=j.accounts||[];render()}).catch(function(e){alert('Lỗi: '+e.message)})}})]);
    ub.appendChild(el('tr',{},[name,el('td',{class:'k',text:u.id}),el('td',{text:lic(u)}),cnt,el('td',{text:fmt(u.lastLogin)}),a]));
    if(OPEN[u.id]){
      var box=el('div',{style:'display:flex;flex-wrap:wrap;gap:6px'});
      if(!OPEN[u.id].length)box.appendChild(el('span',{style:'color:#8a9099',text:'Chưa có account nào'}));
      OPEN[u.id].forEach(function(x){
        var on=x.online;
        box.appendChild(el('span',{style:'display:inline-flex;align-items:center;gap:6px;padding:3px 10px;border:1px solid #292d36;border-radius:999px;font-size:12px',title:(x.note?x.note+' · ':'')+'Lv '+x.level+' · '+(on?'Online':'Offline')+' · '+fmt(x.updated)},[
          el('span',{style:'width:8px;height:8px;border-radius:50%;background:'+(on?'#4ade80':'#6b7280')}),
          document.createTextNode(x.name+' (Lv '+x.level+')')]));
      });
      var td=el('td',{colspan:'6',style:'background:#0e1013'},[box]);
      ub.appendChild(el('tr',{},[td]));
    }
  });
  $('uc').textContent='('+S.users.length+')';
  var nk=S.users.filter(function(u){return !u.admin&&!u.license&&!u.banned}).length;
  $('pg').textContent='Xoá user không có key ('+nk+')';$('pg').disabled=!nk;
}
$('pg').onclick=function(){var nk=S.users.filter(function(u){return !u.admin&&!u.license&&!u.banned}).length;
  act('/admin/api/users/purge',{},'Xoá ngay '+nk+' người dùng không có key? (Admin và user bị khoá không bị xoá)')};
document.querySelectorAll('button[data-d]').forEach(function(b){b.onclick=function(){$('d').value=b.getAttribute('data-d')}});
$('gen').onclick=function(){var b=$('gen');b.disabled=true;
  api('/admin/api/keys',{count:Number($('n').value),days:Number($('d').value),maxAccounts:Number($('m').value),note:$('nt').value})
   .then(function(j){$('out').value=j.keys.join('\n');$('outw').hidden=false;return load()}).catch(function(e){alert('Lỗi: '+e.message)}).then(function(){b.disabled=false})};
$('cp').onclick=function(){var t=$('out');t.select();if(navigator.clipboard)navigator.clipboard.writeText(t.value);else document.execCommand('copy')};
$('kf').onchange=render;$('ks').oninput=render;
load();
setInterval(function(){if(document.hidden||String(window.getSelection()))return;api('/admin/api/state').then(function(j){S=j;render()}).catch(function(){})},15000);
`;
const ADMIN_HTML = '<!doctype html><html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Admin - DQR Tracker</title><style>' + ADMIN_CSS + '</style></head><body>' + ADMIN_BODY + '<script>' + ADMIN_JS + '</script></body></html>';

// ---- Ghi nhan do Ult/Legend moi rot: so sanh kho do cu va moi cua tung account ----
const dropTier = r => { const x = String(r || '').toLowerCase().replace(/[^a-z]/g, ''); return x.startsWith('ult') ? 'Ult' : x.startsWith('legend') ? 'Legend' : ''; };
const dropKey = i => [i.t, i.n, i.r || '', i.e || ''].join('|');
const dropStat = i => { const st = i.s || {}; return Math.max(0, st.physicalDamage || 0, st.physicalPower || 0, st.spellPower || 0); };
const STAT_FIELDS = ['physicalDamage', 'physicalPower', 'spellPower', 'health'];
const hasStats = i => !!i.s && STAT_FIELDS.some(f => Number.isFinite(i.s[f]));
const dropSig = i => dropKey(i) + '|' + STAT_FIELDS.map(f => (i.s && Number.isFinite(i.s[f])) ? i.s[f] : '').join(',');
// ===== GIOI HAN LICH SU - cật dọc đững khi nhận dữ liệu mới để không tích lũy dữ liệu cũ, tranh lag web =====
const DROP_HIST_MAX = Math.max(5,  +process.env.DROP_HIST_MAX  || 10);   // nại dung kho cu (drops) giịi toi da này so moc
const LOST_HIST_MAX = Math.max(5,  +process.env.LOST_HIST_MAX  || 15);   // item bi mất cu (lost) giịi toi da này so moc
const CAND_HIST_MAX = Math.max(5,  +process.env.CAND_HIST_MAX  || 8);    // phạn thưởng dườ tra (cand) giịi toi da này so moc
const TRADE_WINDOW = 15 * 60e3;   // trong khoang nay, do bien mat o acc nay + xuat hien o acc khac = trade, khong tinh la drop
// Cac dong trong 'target' ma 'base' khong co (so sanh theo SO LUONG tung loai item -> nang cap khong bi tinh la do moi)
function rowsAdded(base, target) {
  const cnt = {}, sigCnt = {}, tcnt = {}, byKey = {};
  for (const i of base) { const k = dropKey(i), sg = dropSig(i), q = +i.q || 1; cnt[k] = (cnt[k] || 0) + q; sigCnt[sg] = (sigCnt[sg] || 0) + q; }
  for (const i of target) { const k = dropKey(i); tcnt[k] = (tcnt[k] || 0) + (+i.q || 1); (byKey[k] = byKey[k] || []).push(i); }
  const out = [];
  for (const k of Object.keys(tcnt)) {
    let add = tcnt[k] - (cnt[k] || 0);
    if (add <= 0) continue;
    const rows = byKey[k].slice().sort((x, y) => ((sigCnt[dropSig(x)] || 0) > 0) - ((sigCnt[dropSig(y)] || 0) > 0));   // dong co chi so chua tung co -> truoc
    for (const i of rows) { if (add <= 0) break; const take = Math.min(+i.q || 1, add); out.push({ i, q: take }); add -= take; }
  }
  return out;
}
function detectDrops(oldInv, newInv, now) {
  if (!Array.isArray(oldInv) || !oldInv.length || !Array.isArray(newInv)) return [];
  const out = [];
  for (const { i, q } of rowsAdded(oldInv, newInv)) {
    const tier = dropTier(i.r);
    if (!tier || /^(weapon|chest|helmet|ability) #/i.test(i.n)) continue;
    out.push({ n: i.n, t: i.t, r: tier, e: i.e || null, s: i.s || null, m: i.m || null, im: i.im || null, q, at: now, sig: dropSig(i), hs: hasStats(i) });
  }
  return out.length > 8 ? [] : out;   // qua nhieu mon cung luc = nhieu kha nang la nap lai kho, bo qua
}
// Ult/Legend vua MAT khoi kho (ban/trade di) -> dung de nhan ra trade giua cac acc
function detectLost(oldInv, newInv, now) {
  if (!Array.isArray(oldInv) || !oldInv.length || !Array.isArray(newInv)) return [];
  const out = [];
  for (const { i } of rowsAdded(newInv, oldInv)) if (dropTier(i.r)) out.push({ sig: dropSig(i), at: now });
  return out.length > 8 ? [] : out;
}
// Do vua nhan duoc co phai tu trade acc khac (acc khac cung dang theo doi) khong?
function isTradedIn(accs, g, name, now) {
  for (const [on, o] of Object.entries(accs)) {
    if (on === name || !o || !o.rec) continue;
    if ((o.lost || []).some(l => l.sig === g.sig && now - l.at < TRADE_WINDOW)) return true;          // acc kia vua mat dung mon nay
    if (g.hs && (o.rec.inv || []).some(i => dropSig(i) === g.sig)) return true;                       // acc kia van dang giu mon y het (cung chi so)
  }
  return false;
}

// ---- Tin hieu thang dungeon (client Lua bat remote cloneRewardGui) ----
// Chi do nam trong PHAN THUONG cua mot lan thang moi tinh la drop. Do tu trade/mua khong bao gio nam trong phan thuong.
// Do phat hien qua kho do (cand) duoc giu toi da WIN_WINDOW cho tin hieu thang den (kho do va phan thuong co the den theo thu tu nao cung duoc).
const WIN_WINDOW = 5 * 60e3;
function applyWin(prev, bw, capable, detected, now0) {
  let win = (prev && prev.win) || null, winSeen = !!(prev && prev.winSeen) || capable === true;
  if (bw && typeof bw === 'object' && Array.isArray(bw.items) && Number.isFinite(+bw.id)) {
    winSeen = true;
    const id = String(Math.floor(+bw.id));
    if (!win || win.id !== id) win = { id, at: now0, names: bw.items.slice(0, 30).map(x => ({ n: s(x && x.n, 60).toLowerCase(), r: dropTier(x && x.r), low: /^(common|uncommon|rare|epic)/.test(String((x && x.r) || '').toLowerCase().replace(/[^a-z]/g, '')) })).filter(x => x.n) };
  }
  if (!winSeen) return { real: detected, win: null, cand: [], winSeen: false };   // client cu chua gui tin hieu thang -> giu cach cu
  let cand = ((prev && prev.cand) || []).filter(c => now0 - c.at < WIN_WINDOW).concat(detected);
  const real = [];
  if (win && now0 - win.at < WIN_WINDOW) {
    const rest = [];
    for (const c of cand) {
      const k = win.names.findIndex(w => w.n === String(c.n).toLowerCase() && !w.low && (!w.r || w.r === c.r));   // cung ten nhung do hiem thap (vd Ice Greatspear common/epic) khong duoc khop voi do Ult/Legend
      if (k >= 0) { win.names.splice(k, 1); real.push(c); } else rest.push(c);
    }
    cand = rest;
  }
  return { real, win, cand: cand.slice(-CAND_HIST_MAX), winSeen };
}

// ---- Xoa user khong co key (khong dong toi admin, user bi khoa, user da co key; du lieu acc tracker giu nguyen) ----
function purgeNoKeyUsers(now, graceMs) {
  const g = graceMs === undefined ? NOKEY_GRACE_MS : graceMs;
  if (graceMs === undefined && !NOKEY_GRACE_MS) return 0;
  let n = 0;
  for (const [id, u] of Object.entries(auth.users)) {
    if (!u || isAdminUser(u) || u.banned || u.license) continue;
    if (now - Math.max(u.lastLogin || 0, u.createdAt || 0) < g) continue;
    delete auth.users[id]; n++;
    for (const [h, ss] of Object.entries(auth.sessions)) if (ss && ss.uid === id) delete auth.sessions[h];
    console.log('[purge] xoa user khong co key: ' + (u.name || id) + ' (' + id + ')');
  }
  if (n) saveAuth();
  return n;
}
function adminState() {
  const users = Object.values(auth.users).map(x => ({
    id: x.id, name: x.name, username: x.username, avatar: avatarUrl(x), admin: isAdminUser(x), banned: !!x.banned,
    license: x.license || null, accounts: accCount(ownerOf(x)), lastLogin: x.lastLogin || 0, createdAt: x.createdAt || 0
  })).sort((a, b) => b.lastLogin - a.lastLogin);
  const licenses = Object.values(auth.licenses).map(l => ({ ...l, usedByName: l.usedBy && auth.users[l.usedBy] ? auth.users[l.usedBy].name : null, hwid: ((l.usedBy && auth.users[l.usedBy] && auth.users[l.usedBy].hwid) || '').slice(0, 8) || null, hwidAt: (l.usedBy && auth.users[l.usedBy] && auth.users[l.usedBy].hwidAt) || 0, eff: (!l.revoked && l.usedBy && auth.users[l.usedBy] && auth.users[l.usedBy].license) ? { expiresAt: auth.users[l.usedBy].license.expiresAt, maxAccounts: auth.users[l.usedBy].license.maxAccounts } : null })).sort((a, b) => b.createdAt - a.createdAt);
  return { users, licenses, now: Date.now() };
}
function adminApi(req, res, p) {
  if (req.method === 'GET' && p === '/admin/api/state') return send(res, 200, adminState());
  if (req.method === 'GET' && p === '/admin/api/accounts') {
    const tu = auth.users[new URL(req.url, 'http://x').searchParams.get('id') || ''];
    if (!tu) return send(res, 404, { error: 'not found' });
    const o = ownerOf(tu);
    return loadOwner(o).then(() => {
      const now = Date.now();
      const list = Object.values(owners[o] || {}).filter(x => x && x.rec).map(x => ({
        name: x.rec.name, level: x.rec.level, gold: x.rec.gold, note: x.rec.note || '',
        updated: x.updated || 0, online: now - (x.updated || 0) < 150000
      })).sort((a, b) => (b.online - a.online) || String(a.name).localeCompare(String(b.name)));
      send(res, 200, { accounts: list });
    }).catch(e => { console.error('[admin acc]', e && e.message); send(res, 500, { error: 'server error' }); });
  }
  if (req.method !== 'POST') return send(res, 404, { error: 'not found' });
  return readJson(req, res, b => {
    if (p === '/admin/api/keys') {
      const n = clampInt(b.count, 1, 200, 1), days = clampInt(b.days, 0, 3650, 30), max = clampInt(b.maxAccounts, 0, 1000, 5), note = s(b.note, 60);
      const keys = [];
      for (let i = 0; i < n; i++) {
        let k; do { k = newKey(); } while (auth.licenses[k]);
        auth.licenses[k] = { key: k, days, maxAccounts: max, note, createdAt: Date.now(), usedBy: null, usedAt: null, revoked: false };
        keys.push(k);
      }
      saveAuth(); console.log('[admin] tao ' + n + ' key (' + (days || 'vinh vien') + ' ngay, max ' + (max || 'inf') + ')');
      return send(res, 200, { ok: true, keys });
    }
    if (p === '/admin/api/keys/revoke' || p === '/admin/api/keys/delete') {
      const k = normKey(b.key), l = k && auth.licenses[k];
      if (!l) return send(res, 404, { error: 'key not found' });
      if (p.endsWith('/delete')) {
        if (l.usedBy && !l.revoked) return send(res, 400, { error: 'key da duoc dung, hay dung Thu hoi truoc' });
        delete auth.licenses[k];
      } else {
        l.revoked = true;
        const u = l.usedBy && auth.users[l.usedBy];
        if (u && u.license) u.license.expiresAt = Date.now(); // thu hoi key da dung = cat quyen ngay
      }
      saveAuth(); return send(res, 200, { ok: true });
    }
    if (p === '/admin/api/users/purge') {
      const n = purgeNoKeyUsers(Date.now(), 0);
      return send(res, 200, { ok: true, deleted: n });
    }
    if (p === '/admin/api/keys/resethwid') {
      const k = normKey(b.key), l = k && auth.licenses[k];
      if (!l) return send(res, 404, { error: 'key not found' });
      const u = l.usedBy && auth.users[l.usedBy];
      if (!u) return send(res, 400, { error: 'key chua duoc dung hoac nguoi dung da bi xoa' });
      delete u.hwid; delete u.hwidAt; saveAuth();
      console.log('[admin] reset HWID key ' + k + ' (' + (u.name || u.id) + ')');
      return send(res, 200, { ok: true });
    }
    if (p === '/admin/api/users') {
      const id = String(b.id || ''), u = /^\d{5,25}$/.test(id) && auth.users[id];
      if (!u) return send(res, 404, { error: 'user not found' });
      if (isAdminUser(u)) return send(res, 400, { error: 'khong sua duoc admin' });
      const now = Date.now(), cur = u.license;
      switch (b.action) {
        case 'ban': u.banned = true; break;
        case 'unban': u.banned = false; break;
        case 'addDays': {
          const d = clampInt(b.days, -3650, 3650, 0);
          if (!cur) u.license = { expiresAt: now + Math.max(0, d) * 864e5, maxAccounts: 5 };
          else if (cur.expiresAt !== null) cur.expiresAt = Math.max(now, cur.expiresAt) + d * 864e5;
          break;
        }
        case 'setMax': if (!cur) return send(res, 400, { error: 'user chua co key' }); cur.maxAccounts = clampInt(b.maxAccounts, 0, 1000, 5); break;
        case 'setLifetime': u.license = { expiresAt: null, maxAccounts: cur ? cur.maxAccounts : 5 }; break;
        case 'clearLicense': u.license = null; break;
        default: return send(res, 400, { error: 'bad action' });
      }
      saveAuth(); return send(res, 200, { ok: true });
    }
    send(res, 404, { error: 'not found' });
  });
}

// ---- Dashboard cho user da dang nhap: tu dien token cua ho + gan widget tai khoan ----
const dashboardFor = u => (isAdminUser(u) ? DASHBOARD_ADMIN_HTML : DASHBOARD_HTML_CK)
  .replace("let KEYV=ld('dq_key','')", () => 'let KEYV=' + JSON.stringify(u.token))
  .replace('</body>', () => '<script>' + WIDGET + '</script></body>');

const handle = async (req, res) => {
  const host = req.headers.host || 'localhost:' + PORT;
  let u; try { u = new URL(req.url, 'http://' + host); } catch { return send(res, 400, 'Bad request', 'text/plain'); }
  const p = u.pathname;
  const isLocal = isLocalHost(host) && isLoopbackReq(req);
  if (LOCAL_AGENT && req.method === 'OPTIONS') {   // preflight CORS: trang web (domain that) goi sang local agent nay
    res.writeHead(204, { 'access-control-allow-origin': LOCAL_AGENT_ORIGIN, 'access-control-allow-methods': 'GET,POST,OPTIONS', 'access-control-allow-headers': 'content-type,x-requested-with,x-api-key', 'access-control-max-age': '86400', 'vary': 'Origin' });
    return res.end();
  }
  try {
  if (p === '/discord' && req.method === 'GET') {
    return send(res, 200, `<!doctype html>
<html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Server Discord</title>
<style>
*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;padding:20px;background:#08090b;color:#e8eaed;font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
main{width:min(440px,100%);padding:28px;border:1px solid #292d36;border-radius:20px;background:#12151a;text-align:center}
.icon{width:52px;height:52px;margin:0 auto 12px;color:#5865f2}.icon svg{width:100%;height:100%}h1{margin:0;font-size:24px}p{margin:8px 0 22px;color:#a1a7b0}
a{display:block;padding:15px;border-radius:12px;background:#5865f2;color:#fff;text-decoration:none;font-weight:700}a:hover{background:#4752c4}.back{display:inline-block;margin-top:16px;background:none;color:#a1a7b0;font-weight:500}.back:hover{background:none;color:#fff}
</style></head><body><main><div class="icon"><svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M19.7 5.4A18.8 18.8 0 0 0 15 4l-.6 1.2a17.3 17.3 0 0 0-4.8 0L9 4a18.8 18.8 0 0 0-4.7 1.4C1.3 9.8.5 14.1.9 18.4a19 19 0 0 0 5.8 2.9l1.2-2a12 12 0 0 1-1.9-.9l.5-.4a13.6 13.6 0 0 0 15 0l.5.4a12 12 0 0 1-1.9.9l1.2 2a19 19 0 0 0 5.8-2.9c.5-5-.8-9.2-3.4-13zM8.8 15.7c-1 0-1.8-.9-1.8-2s.8-2 1.8-2 1.8.9 1.8 2-.8 2-1.8 2zm6.4 0c-1 0-1.8-.9-1.8-2s.8-2 1.8-2 1.8.9 1.8 2-.8 2-1.8 2z"/></svg></div><h1>Server Discord</h1><p>Nhấn nút bên dưới để tham gia máy chủ Discord.</p><a href="https://discord.gg/5VAGQ6Bvfr" target="_blank" rel="noopener noreferrer">Tham gia Discord</a><a class="back" href="/">Quay lại dashboard</a></main></body></html>`, 'text/html');
  }
  if (p === '/') {
    // Mo tu chinh may chay server -> tu dien key admin, khong can nhap / dang nhap
    if (isLocal) {
      let page = DASHBOARD_ADMIN_HTML.replace("let KEYV=ld('dq_key','')", () => 'let KEYV=' + JSON.stringify(KEY));
      if (!MULTI) page = injectBox(page);
      if (MULTI) page = page.replace('</body>', () => '<a href="/admin" style="position:fixed;left:12px;bottom:12px;z-index:60;padding:7px 12px;border:1px solid #292d36;border-radius:999px;background:#12151a;color:#e8eaed;font:13px system-ui;text-decoration:none">⚙ Quản lý key</a></body>');
      return send(res, 200, page, 'text/html');
    }
    if (!MULTI) return send(res, 200, injectBox(DASHBOARD_ADMIN_HTML), 'text/html');
    const su = sessionUser(req);
    if (!su) return sendPage(res, loginPage(u.searchParams.get('err') ? 'Đăng nhập thất bại, hãy thử lại.' : ''));
    if (!licActive(su)) return sendPage(res, keyPage(su));
    return send(res, 200, dashboardFor(su), 'text/html');
  }

  // ---- Dang nhap Discord (OAuth2, chi xin quyen "identify") ----
  if (MULTI && p === '/auth/login' && req.method === 'GET') {
    const state = rnd(18);
    return redirect(res, DISCORD_AUTHORIZE + '?' + new URLSearchParams({ client_id: DISCORD_ID, response_type: 'code', redirect_uri: PUBLIC_URL + '/auth/callback', scope: 'identify', state, prompt: 'none' }),
      { 'set-cookie': cookieStr('dq_state', state, 600) });
  }
  if (MULTI && p === '/auth/callback' && req.method === 'GET') {
    const code = u.searchParams.get('code'), state = u.searchParams.get('state'), st = parseCookies(req).dq_state;
    const clearState = cookieStr('dq_state', '', 0);
    if (!code || !state || !st || !safeEq(st, state)) return redirect(res, '/?err=1', { 'set-cookie': clearState });
    (async () => {
      const tr = await fetch(DISCORD_API + '/oauth2/token', {
        method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded', 'user-agent': 'DQTracker (1.0)' },
        body: new URLSearchParams({ client_id: DISCORD_ID, client_secret: DISCORD_SECRET, grant_type: 'authorization_code', code, redirect_uri: PUBLIC_URL + '/auth/callback' })
      });
      if (!tr.ok) throw new Error('token ' + tr.status);
      const tk = await tr.json();
      const ur = await fetch(DISCORD_API + '/users/@me', { headers: { authorization: 'Bearer ' + tk.access_token, 'user-agent': 'DQTracker (1.0)' } });
      if (!ur.ok) throw new Error('users/@me ' + ur.status);
      const d = await ur.json();
      if (!d || !/^\d{5,25}$/.test(String(d.id))) throw new Error('bad discord user');
      let usr = auth.users[d.id];
      if (!usr) {
        usr = auth.users[d.id] = { id: String(d.id), token: newToken(), createdAt: Date.now(), license: null, banned: false };
        tokenIdx.set(usr.token, usr.id);
      }
      usr.username = s(d.username, 40); usr.name = s(d.global_name || d.username, 60) || usr.username;
      usr.avatar = d.avatar && /^\w{1,64}$/.test(d.avatar) ? d.avatar : null; usr.lastLogin = Date.now();
      const sid = newSession(usr.id);
      console.log('[login] ' + usr.name + ' (' + usr.id + ')' + (licActive(usr) ? '' : ' - chua co key'));
      redirect(res, '/', { 'set-cookie': [cookieStr('dq_sid', sid, SESSION_MS / 1000), clearState] });
    })().catch(e => { console.error('[login loi]', e.message); redirect(res, '/?err=1', { 'set-cookie': clearState }); });
    return;
  }
  if (MULTI && p === '/auth/logout' && req.method === 'POST') {
    if (req.headers['x-requested-with'] !== 'dq') return send(res, 403, { error: 'csrf' });
    const sid = parseCookies(req).dq_sid; if (sid) { delete auth.sessions[sha(sid)]; saveAuth(); }
    return sendX(res, 200, { ok: true }, 'application/json', { 'set-cookie': cookieStr('dq_sid', '', 0) });
  }

  // ---- Trang quan tri ----
  if (p === '/admin' || p.startsWith('/admin/')) {
    if (!MULTI) return send(res, 404, 'Not found', 'text/plain');
    const su = sessionUser(req);
    const viaKey = req.headers['x-api-key'] && safeEq(req.headers['x-api-key'], KEY);
    const okAdmin = viaKey || (su && isAdminUser(su));
    if (p === '/admin' && req.method === 'GET') {
      if (!isLocal && !okAdmin) return su ? send(res, 403, 'Chi admin moi vao duoc trang nay.', 'text/plain') : sendPage(res, loginPage('Đăng nhập bằng tài khoản Discord admin để vào trang này.'));
      return sendPage(res, isLocal ? ADMIN_HTML.replace("var AK='';", () => 'var AK=' + jsonForScript(KEY) + ';') : ADMIN_HTML);
    }
    if (p.startsWith('/admin/api/')) {
      if (!okAdmin) return send(res, 401, { error: 'unauthorized' });
      if (!viaKey && req.method !== 'GET' && req.headers['x-requested-with'] !== 'dq') return send(res, 403, { error: 'csrf' });
      return adminApi(req, res, p);
    }
    return send(res, 404, 'Not found', 'text/plain');
  }

  if (p === '/script') {
    const base = baseUrl(req, host);
    const tkn = u.searchParams.get('t'), tuid = tkn && tokenIdx.get(tkn), tuser = tuid && auth.users[tuid];
    let lua = ((tuser && !isAdminUser(tuser)) ? LUA : LUA_FULL).replace(/\[GPO-Tracker\]/g, '[DQR TRACKER]').replace('__ENDPOINT__', () => base + '/api/report');
    const t = u.searchParams.get('t');
    if (t && /^[\w-]{8,80}$/.test(t)) lua = lua.replace('doi-thanh-key-bi-mat-cua-ban', () => t); // dien san token cua user
    return send(res, 200, lua, 'text/plain');
  }
  if (p === '/dump') {
    return send(res, 200, DUMP.replace('__DUMP__', () => baseUrl(req, host) + '/api/dump'), 'text/plain');
  }
  if (!p.startsWith('/api/')) return send(res, 404, 'Not found', 'text/plain');

  // ---- API theo phien dang nhap (user chua co key van goi duoc: /api/me, /api/redeem) ----
  if (MULTI && (p === '/api/me' || p === '/api/me/token' || p === '/api/redeem')) {
    const su = sessionUser(req);
    if (!su) return send(res, 401, { error: 'unauthorized' });
    if (req.method !== 'GET' && req.headers['x-requested-with'] !== 'dq') return send(res, 403, { error: 'csrf' });
    if (p === '/api/me' && req.method === 'GET') return send(res, 200, meInfo(su, req, host));
    if (p === '/api/me/token' && req.method === 'POST') {
      if (!licActive(su)) return send(res, 403, { error: 'license_required' });
      tokenIdx.delete(su.token); su.token = newToken(); tokenIdx.set(su.token, su.id); saveAuth();
      return send(res, 200, { ok: true });
    }
    if (p === '/api/redeem' && req.method === 'POST') {
      if (su.banned && !isAdminUser(su)) return send(res, 403, { error: 'banned', message: 'Tài khoản đã bị khoá.' });
      if (tooMany(su.id)) return send(res, 429, { error: 'rate', message: 'Thử sai quá nhiều lần, hãy đợi 15 phút.' });
      return readJson(req, res, b => {
        const r = redeemKey(su, b.key);
        if (r.error) { failTry(su.id); return send(res, 400, r); }
        send(res, 200, { ok: true });
      });
    }
    return send(res, 404, { error: 'not found' });
  }

  if (p === '/api/runs/img' && req.method === 'GET') {   // anh nen dungeon (chi ten dungeon co trong danh sach, khong can dang nhap vi <img> khong gui duoc header)
    const rn = runsName(u.searchParams.get('d')); if (!rn) return send(res, 404, { error: 'bad_dungeon' });
    return void runsImg(rn, res);
  }
  // ---- API du lieu: moi nguoi chi thay / sua du lieu cua minh ----
  const a = authenticate(req);
  if (!a) return send(res, 401, { error: 'unauthorized' });
  if (a.denied) return send(res, 403, { error: a.denied });
  if (a.via === 'cookie' && req.method !== 'GET' && req.headers['x-requested-with'] !== 'dq') return send(res, 403, { error: 'csrf' });
  const mine = bucket(a.owner);
  if (p.startsWith('/api/config/')) return cfgApi(req, res, p, u, a);
  if (p.startsWith('/api/rejoin/')) return rejoinApi(req, res, p, u, a);
  if (p.startsWith('/api/perf/')) return perfApi(req, res, p, u, a);
  if (p.startsWith('/api/runs')) return runsApi(req, res, p, u, a);
  if (p.startsWith('/api/trade/')) return tradeApi(req, res, p, u, a, mine);

  // ---- HWID: user (theo key da dung) bi gan voi 1 may o lan bao cao dau tien; admin reset o /admin ----
  if (p === '/api/report' && req.method === 'POST' && a.via === 'token' && !a.admin && a.user) {
    const hw = String(req.headers['x-hwid'] || '').trim().slice(0, 200), hh = hw ? sha('hwid:' + hw) : '';
    if (a.user.hwid) {
      if (!hh || !safeEq(hh, a.user.hwid)) return send(res, 403, { error: 'hwid_mismatch', message: 'Key đã gắn với thiết bị khác. Liên hệ admin để reset HWID.' });
    } else if (hh) { a.user.hwid = hh; a.user.hwidAt = Date.now(); saveAuth(); }
  }
  if (p === '/api/parties/lobby' && req.method === 'POST') {   // nut "Force lobby" (Config > Parties & Runs): bao script Lua cua tung account ve Lobby ngay
    return readJson(req, res, b => {
      const names = (Array.isArray(b.names) ? b.names : []).slice(0, 200);
      let n = 0;
      for (const raw of names) {
        const nm = String(raw || '').trim();
        if (!/^\w{3,20}$/.test(nm)) continue;
        forceLobbyCmd.set(a.owner + '\u0001' + nm.toLowerCase(), Date.now());
        n++;
      }
      return send(res, 200, { ok: true, n });
    }, 2e4);
  }
  if (p === '/api/cookie/stored' && req.method === 'GET') {   // chi tra TEN acc da luu, khong bao gio tra cookie
    const canDeep = ckDeepAllowed(req, a);   // tool check sau: chi localhost hoac admin, va phai co file exe
    try {
      const saved = await ckLoad(a.owner);                       // doc du lieu cookie da luu (Redis neu co, khong thi file local)
      const names = Object.keys(saved);   // giu TAT CA cookie da luu (ke ca acc chua thay trong tracker), chi mat khi bam Xoa
      const at = {}, st = {}; for (const n of names) { const r = mine[n] && mine[n].rec; at[n] = (r && r.ck && r.ck.at) || 0; st[n] = (r && r.ck && r.ck.s) || ''; }
      return send(res, 200, { enabled: true, names, at, st, canDeep });
    } catch { return send(res, 500, { error: 'server error' }); }
  }
  if (p === '/api/cookie/stored' && req.method === 'DELETE') {   // quen cookie cua 1 acc (?name=) hoac tat ca (?all=1)
    const nm = String(u.searchParams.get('name') || '');
    const all = u.searchParams.get('all') === '1';
    try {
      const saved = await ckLoad(a.owner);
      let n = 0;
      if (all) { n = Object.keys(saved).length; await ckSave(a.owner, Object.create(null)); }             // xoá sạch
      else if (Object.prototype.hasOwnProperty.call(saved, nm)) { delete saved[nm]; await ckSave(a.owner, saved); n = 1; }
      for (const k of Object.keys(mine)) {
        if (!(all || k === nm)) continue;
        if (mine[k] && mine[k].rec && mine[k].rec.ck) delete mine[k].rec.ck;
        dirty = true;
      }
      return send(res, 200, { ok: true, n });
    } catch { return send(res, 500, { error: 'server error' }); }
  }
  if (p === '/api/cookie/save' && req.method === 'POST') {   // luu NGAY cookie vua nhap (chua check) de reload giua chung khong bi mat
    return readJson(req, res, async b => {
      try {
        const items = (Array.isArray(b.items) ? b.items : []).slice(0, 80);
        const stored = await ckLoad(a.owner); let n = 0, changed = false;
        for (const it of items) {
          const nm = String((it && it.name) || '').slice(0, 50);
          const key = nm && Object.keys(mine).find(k => k.toLowerCase() === nm.toLowerCase());
          if (!key) continue;
          const c = ckClean(it.cookie);
          if (!CK_RE.test(c)) continue;
          if (!stored[key] && Object.keys(stored).length >= CK_MAX_SAVED) continue;
          if (stored[key] !== c) { stored[key] = c; changed = true; }
          n++;
        }
        if (changed) await ckSave(a.owner, stored);
        send(res, 200, { ok: true, n });
      } catch (e) { send(res, 500, { error: 'server error' }); }
    }, 4e5);
  }
  if (p === '/api/cookie/check' && req.method === 'POST') {
    const last = ckLast.get(a.owner) || 0;
    if (Date.now() - last < 4000) return send(res, 429, { error: 'too_fast' });
    ckLast.set(a.owner, Date.now());
    return readJson(req, res, async b => {
      try {
        const items = (Array.isArray(b.items) ? b.items : []).slice(0, 10);
        const wantSave = b.save !== false;   // mac dinh LUON luu
        let stored = await ckLoad(a.owner);   // 1 lan doc (Redis/file) cho cả đợt
        let savedCount = Object.keys(stored).length;
        const out = new Array(items.length);
        let idx = 0;
        const worker = async () => {
          while (idx < items.length) {
            const i = idx++, it = items[i] || {};
            let nm = String(it.name || '').slice(0, 50), cookie = it.cookie, fromVault = false;
            if (!nm) {   // chi co cookie, khong co ten: hoi Roblox cookie la cua acc nao, roi doi chieu voi tracker
              if (!cookie) { out[i] = { name: '', s: 'untracked' }; continue; }
              const o = await rbxCookieOwner(cookie);
              if (o.s !== 'ok') { out[i] = { name: '', s: ['rate', 'error', 'badformat'].includes(o.s) ? o.s : 'untracked' }; continue; }   // cookie chet/sai: khong ro acc nao -> coi nhu khong nhan dien duoc
              nm = o.name;
            }
            let key = Object.keys(mine).find(k => k.toLowerCase() === nm.toLowerCase());
            if (!key) {   // acc chua co trong tracker (chua tung chay script): tu tao 1 ban ghi rong de co the check cookie ngay
              const limit = a.admin ? Infinity : maxAcc(a.user);
              if (Object.keys(mine).length >= limit) { out[i] = { name: it.name ? nm : '', s: 'untracked' }; continue; }
              mine[nm] = {
                rec: {
                  name: nm, level: 0, class: '-', gold: 0, note: '',
                  placeId: null, jobId: null,
                  gpass: { vip: false, xp2: false, gold2: false, drop: false, freeResets: false },
                  gpassPrices: {}, gems: 0, xpBoostSeconds: 0, inv: []
                },
                updated: 0
              };
              key = nm; dirty = true;
            }
            if (!cookie && stored[key]) { cookie = stored[key]; fromVault = true; }   // lay cookie THO tu file
            if (!cookie) { out[i] = { name: key, s: 'nocookie' }; continue; }   // chua dan cookie va kho cung khong co
            const r = await rbxCheckCookie(key, cookie);
            out[i] = { name: key, s: r.s };
            let isSaved = !!stored[key];
            if (r.s === 'mismatch' && isSaved && !fromVault) {   // cookie cua acc KHAC -> khong giu duoi ten nay
              delete stored[key]; await ckSave(a.owner, stored); isSaved = false; savedCount--;
            } else if (wantSave && !fromVault && !['mismatch', 'badformat'].includes(r.s) && (isSaved || savedCount < CK_MAX_SAVED)) {   // luu MOI trang thai (Live/Dead/Rejoin/Lock/...), chi mat khi bam Xoa
              const cc = ckClean(cookie);
              if (stored[key] !== cc) { stored[key] = cc; await ckSave(a.owner, stored); }   // luu qua Redis/file
              if (!isSaved) savedCount++; isSaved = true;
            }
            out[i].stored = isSaved;
            if (['valid', 'invalid', 'kicked', 'mismatch', 'lock', 'rejoin', 'disconnect'].includes(r.s) && mine[key] && mine[key].rec) {
              mine[key].rec.ck = { s: r.s, at: Date.now(), saved: isSaved };
              dirty = true;
            }
          }
        };
        await Promise.all([worker(), worker(), worker()]);
        send(res, 200, { results: out });
      } catch (e) { console.error('[cookie check loi]', e && e.message); send(res, 500, { error: 'server error' }); }
    }, 6e4);
  }
  // ---- CHECK SAU (tool checkcookie.exe): quet hang loat LIVE / BANNED / FaceID / DEAD ----
  // CHI chay khi client goi tu localhost HOAC nguoi goi la ADMIN (xem ckDeepAllowed). Khong co exe -> 404.
  // Cookie chi ghi tam vao %TEMP% roi xoa; ket qua tra ve giong /api/cookie/check de UI dung chung.
  if (p === '/api/cookie/deepcheck' && req.method === 'POST') {
    if (!CHK_ENGINE.on) return send(res, 404, { error: 'not found' });
    if (!ckDeepAllowed(req, a)) return send(res, 403, { error: 'deep_forbidden', message: 'Check sâu chỉ dùng được trên localhost hoặc bởi admin.' });
    const lastD = ckDeepLast.get(a.owner) || 0;
    if (Date.now() - lastD < 6000) return send(res, 429, { error: 'too_fast' });
    ckDeepLast.set(a.owner, Date.now());
    return readJson(req, res, async b => {
      try {
        const items = (Array.isArray(b.items) ? b.items : []).slice(0, CK_DEEP_MAX);
        const wantSave = b.save !== false;
        let stored = await ckLoad(a.owner);
        let savedCount = Object.keys(stored).length;
        const use = [], jobs = [], out = new Array(items.length);
        for (let i = 0; i < items.length; i++) {
          const it = items[i] || {};
          const nm = String(it.name || '').slice(0, 50);
          const key = nm ? Object.keys(mine).find(k => k.toLowerCase() === nm.toLowerCase()) : null;
          if (!key) { out[i] = { name: '', s: 'untracked' }; continue; }
          let cookie = it.cookie, fromVault = false;
          if (!cookie && stored[key]) { cookie = stored[key]; fromVault = true; }
          if (!cookie) { out[i] = { name: key, s: 'nocookie' }; continue; }
          const c = ckClean(cookie);
          if (!CK_RE.test(c)) { out[i] = { name: key, s: 'badformat' }; continue; }
          out[i] = { name: key, s: 'wait' };
          use.push(c); jobs.push({ i, key, c, fromVault });
        }
        const run = await ckRunExe(use, { concurrency: clampInt(b.concurrency, 1, 300, 100), timeoutSec: clampInt(b.timeoutSec, 1, 60, 5) });
        if (!run.ok) {
          for (const j of jobs) out[j.i] = { name: j.key, s: 'error' };
          console.error('[cookie-deep] tool loi:', String(run.error || '').slice(0, 200));
          return send(res, 200, { results: out, toolError: true });
        }
        for (let k = 0; k < jobs.length; k++) {
          const j = jobs[k], deep = (run.results[k] || {}).s || 'dead';
          const st = CK_DEEP_MAP[deep] || 'invalid';
          let isSaved = !!stored[j.key];
          if (wantSave && !j.fromVault && (isSaved || savedCount < CK_MAX_SAVED)) {   // luu moi trang thai, chi mat khi bam Xoa
            if (stored[j.key] !== ckClean(j.c)) { stored[j.key] = ckClean(j.c); await ckSave(a.owner, stored); }
            if (!isSaved) savedCount++; isSaved = true;
          }
          out[j.i] = { name: j.key, s: st, deep, stored: isSaved };
          if (mine[j.key] && mine[j.key].rec) { mine[j.key].rec.ck = { s: st, at: Date.now(), saved: isSaved, deep }; dirty = true; }
        }
        send(res, 200, { results: out, stats: run.stats || null, deep: true });
      } catch (e) { console.error('[cookie-deep loi]', e && e.message); send(res, 500, { error: 'server error' }); }
    }, 3e5);
  }
  if (p === '/api/flag' && req.method === 'POST') {   // gan / bo co lock, rejoin, ban bang tay cho nhieu account
    let body = '';
    req.on('data', c => { body += c; if (body.length > 1e5) req.destroy(); });
    req.on('end', () => {
      let b; try { b = JSON.parse(body); } catch { return send(res, 400, { error: 'bad json' }); }
      const flag = ['rejoin', 'ban', 'lock', 'face', 'kicked', 'disconnect'].includes(b.flag) ? b.flag : null;
      let n = 0;
      for (const raw of (Array.isArray(b.names) ? b.names : []).slice(0, 500)) {
        const nm = String(raw), cur = Object.prototype.hasOwnProperty.call(mine, nm) ? mine[nm] : null;
        if (!cur) continue;
        if (flag) { cur.rec.flag = flag; cur.rec.manual = true; cur.rec.flagAt = Date.now(); }
        else { delete cur.rec.flag; delete cur.rec.manual; delete cur.rec.err; delete cur.rec.flagAt; }
        n++;
      }
      dirty = true;
      send(res, 200, { ok: true, n });
    });
    return;
  }
  if (p === '/api/report' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 3e5) req.destroy(); });
    req.on('end', async () => {   // async: runsTrack/runsConclude gio can await (claim chong-dem-trung qua Redis)
      let b; try { b = JSON.parse(body); } catch { return send(res, 400, { error: 'bad json' }); }
      const name = s(b.name, 50); if (!name) return send(res, 400, { error: 'no name' });
      if (b.statusOnly === true) {   // chi cap nhat trang thai loi/captcha, khong dung toi kho do
        const cur = Object.prototype.hasOwnProperty.call(mine, name) ? mine[name] : null;
        if (!cur) return send(res, 200, { ok: true, ignored: true });
        if (/^\d{1,20}$/.test(String(b.userId || ''))) cur.rec.uid = String(b.userId);   // userId Roblox = so trong release_<userId>.json (tab Config)
        if (b.heartbeat === true) { cur.updated = Date.now(); notifyOwner(a.owner); return send(res, 200, { ok: true, ...(takeLobbyCmd(a.owner, name) ? { lobby: true } : {}) }); }   // nhip tim: bao acc con song, khong ghi dia
        const err = s(b.err, 200), f = classifyErr(err);
        if (f) { cur.rec.flag = f; cur.rec.err = err; cur.rec.flagAt = Date.now(); delete cur.rec.manual; }
        else if (cur.rec.flag !== 'ban' && !cur.rec.manual) { cur.rec.flag = null; cur.rec.err = err || null; cur.rec.flagAt = Date.now(); }
        dirty = true;
        return send(res, 200, { ok: true, flag: cur.rec.flag || null, ...(takeLobbyCmd(a.owner, name) ? { lobby: true } : {}) });
      }
      const limit = a.admin ? Infinity : maxAcc(a.user);
      if (!Object.prototype.hasOwnProperty.call(mine, name) && Object.keys(mine).length >= limit) return send(res, 403, { error: 'account_limit', max: limit });
      const gpass = b.gpass && typeof b.gpass === 'object' ? b.gpass : {};
      const gpPrices = b.gpassPrices && typeof b.gpassPrices === 'object' ? b.gpassPrices : {};
      const price = key => typeof gpPrices[key] === 'number' && Number.isFinite(gpPrices[key]) && gpPrices[key] >= 0
        ? Math.floor(gpPrices[key]) : null;
      const prev = Object.prototype.hasOwnProperty.call(mine, name) ? mine[name] : null;
      mine[name] = {
        rec: {
          name, level: +b.level || 0, class: s(b.class, 30) || '-', gold: +b.gold || 0, note: s(b.note, 40),
          dailyReward: b.dailyReward ? s(b.dailyReward, 24) : null,   // "READY" / chu dem gio cooldown / "DA NHAN" - tu Lua doc PlayerGui.DailyRewardCountdown
          dungeonName: b.dungeonName ? s(b.dungeonName, 30) : null,   // ten dungeon THAT dang o trong, tu Lua doc PlaceManager.GetDungeonName() (nil neu dang o Lobby) - nguon chinh xac cho Total Run
          dungeonProgress: b.dungeonProgress ? s(b.dungeonProgress, 30) : null,   // "bossKilled" / "noLivesLeft" / "outOfTime" / ... - tin hieu goc dung de tu ket luan thang/thua (xem runsTrack, nhai dung co che cua Harvard)
          dungeonFinished: b.dungeonFinished === true,
          dungeonStarted: b.dungeonStarted === true,
          bonusBossVotingActive: b.bonusBossVotingActive === true,
          timeUp: b.timeUp === true,
          bonusBossStatus: b.bonusBossStatus ? s(b.bonusBossStatus, 30) : null,   // "active"/"success"/"...fail..." - Raid Rodin, tach rieng hoan toan khoi thang/thua dungeon chinh
          hvAt: typeof b.harvardAt === 'number' && Number.isFinite(b.harvardAt) ? b.harvardAt : null,   // "nhip tim" cua script Harvard (_G.DungeonAutofarmPerf.cur.frames.last) - xem rjState: neu gia tri nay dung yen qua lau (du script tracker van bao cao binh thuong) tuc la Harvard bi dung/crash, can mo lai
          uid: /^\d{1,20}$/.test(String(b.userId || '')) ? String(b.userId) : ((prev && prev.rec && prev.rec.uid) || null),
          placeId: /^\d{1,20}$/.test(String(b.placeId || '')) ? String(b.placeId) : null, jobId: /^[\w-]{8,64}$/.test(String(b.jobId || '')) ? String(b.jobId) : null,
          gpass: {
            vip: gpass.vip === true, xp2: gpass.xp2 === true, gold2: gpass.gold2 === true,
            drop: gpass.drop === true, freeResets: gpass.freeResets === true
          },
          gpassPrices: {
            vip: price('vip'), gold2: price('gold2'), drop: price('drop'), freeResets: price('freeResets'),
            xp1: price('xp1'), xp2: price('xp2'), xp3: price('xp3'),
            xp1Gems: price('xp1Gems'), xp2Gems: price('xp2Gems'), xp3Gems: price('xp3Gems')
          },
          gems: typeof b.gems === 'number' && Number.isFinite(b.gems) ? Math.max(0, Math.floor(b.gems)) : 0,
          xpBoostSeconds: typeof b.xpBoostSeconds === 'number' && Number.isFinite(b.xpBoostSeconds)
            ? Math.max(0, Math.floor(b.xpBoostSeconds)) : 0,
          dbg: b.dbg && typeof b.dbg === 'object' ? Object.fromEntries(Object.entries(b.dbg).slice(0, 40).map(([k, x]) => [s(k, 60), s(x, 700)])) : undefined,
          inv: (Array.isArray(b.inv) ? b.inv : []).slice(0, INV_MAX).map(i => ({
            n: s(i.n, 60), q: +i.q || 1, t: s(i.t, 10) || 'item', r: i.r ? s(i.r, 20) : null, e: i.e ? s(i.e, 20) : null, c: /^#[0-9a-f]{6}$/i.test(i.c) ? String(i.c).toLowerCase() : null,
            s: cleanItemStats(i.s), m: cleanItemStats(i.s && i.s.pot), ...(i.eq === true ? { eq: true } : {}), ...(/^\d{1,20}$/.test(String(i.im || '')) ? { im: String(i.im) } : {})
          }))
        },
        updated: Date.now()
      };
      { const ce = s(b.err, 200), cf = ce ? classifyErr(ce) : null; if (cf) Object.assign(mine[name].rec, { flag: cf, err: ce, flagAt: Date.now() }); }   // dang hien captcha -> giu co sau moi lan bao dinh ky
      if (prev && prev.rec && prev.rec.manual && prev.rec.flag) Object.assign(mine[name].rec, { flag: prev.rec.flag, err: prev.rec.err || null, flagAt: prev.rec.flagAt, manual: true });   // co gan tay: giu den khi bo co
      if (prev && prev.rec && prev.rec.flag && !prev.rec.manual && !mine[name].rec.flag && Date.now() - (prev.rec.flagAt || 0) < FLAG_GRACE_MS)   // co tu dong: chi giu ~90s (tranh bao cao sot sau khi bi kick); sau do acc bao cao binh thuong -> tu go co
        Object.assign(mine[name].rec, { flag: prev.rec.flag, err: prev.rec.err || null, flagAt: prev.rec.flagAt });
      if (prev && prev.rec && prev.rec.ck) mine[name].rec.ck = prev.rec.ck;   // giu ket qua check cookie
      // Daily Reward: uu tien doc DUNG chu dem gio that tu game (PlayerGui.DailyRewardCountdown hien dang "H:MM:SS",
      // vi du "11:59:40") -> tinh ra moc het gio CHINH XAC, tu lam tuoi moi lan bao cao (tu sua neu game lech vai giay).
      // Luon giu moc cu tu lan truoc TRUOC, roi moi ghi de neu lan nay co du lieu moi -> tranh mat so/nhay ve "—"
      // khi 1 lan bao cao bi thieu truong dailyReward (mat ket noi, GUI chua kip load...).
      if (prev && prev.rec && prev.rec.dailyResetAt) mine[name].rec.dailyResetAt = prev.rec.dailyResetAt;
      if (prev && prev.rec && prev.rec.dailyClaimedAt) mine[name].rec.dailyClaimedAt = prev.rec.dailyClaimedAt;
      if (mine[name].rec.dailyReward === 'READY') {
        delete mine[name].rec.dailyResetAt;   // da ve READY (vua het han hoac chua tung nhan) -> bo dem gio cu di, hien READY sach
      } else if (mine[name].rec.dailyReward) {
        const dm = /^(\d{1,3}):(\d{2}):(\d{2})$/.exec(mine[name].rec.dailyReward);
        if (dm) mine[name].rec.dailyResetAt = Date.now() + ((+dm[1]) * 3600 + (+dm[2]) * 60 + (+dm[3])) * 1000;
        if (prev && prev.rec && prev.rec.dailyReward === 'READY') mine[name].rec.dailyClaimedAt = Date.now();   // vua chuyen READY -> cooldown = vua nhan xong (fallback neu khong parse duoc gio)
      }
      mine[name].n = (Number(prev && prev.n) || 0) + 1;   // dem so lan cap nhat kho cua acc
      applySoldTomb(prev, mine[name]);   // loai item vua ban con sot trong bao cao cu; het han sau 2 lan cap nhat
      const now0 = mine[name].updated, prevInv = prev && prev.rec && prev.rec.inv, newInv = mine[name].rec.inv;
      {   // Theo doi "nhip tim" cua Harvard (hvAt): con thay doi -> con song, dung yen qua lau -> bi dung/crash (xem rjState)
        const hv = mine[name].rec.hvAt, prevHv = prev && prev.rec && prev.rec.hvAt, prevChangedAt = (prev && prev.hvChangedAt) || 0;
        if (hv != null) { mine[name].hvSeen = true; mine[name].hvChangedAt = (hv !== prevHv) ? now0 : (prevChangedAt || now0); }
        else { mine[name].hvSeen = !!(prev && prev.hvSeen); mine[name].hvChangedAt = prevChangedAt; }   // chua/khong con thay tin hieu Harvard - giu moc cu (chi bao "dung" sau khi DA TUNG thay Harvard chay, tranh bao nham cho acc khong dung Harvard)
      }
      const detected = detectDrops(prevInv, newInv, now0).filter(g => !isTradedIn(mine, g, name, now0)).map(({ hs, ...g }) => g);   // do moi xuat hien trong kho, bo do nhan tu trade
      const wr = applyWin(prev, b.win, b.winCap === true, detected, now0), real = wr.real;   // chi giu do nam trong phan thuong thang dungeon
      const lostNow = detectLost(prevInv, newInv, now0);
      for (const l of lostNow) for (const [on, o] of Object.entries(mine)) {   // acc khac da ghi nhan 'rot' mon nay truoc khi thay acc nay mat -> do la trade, go ra
        if (on === name || !o || !Array.isArray(o.drops)) continue;
        o.drops = o.drops.filter(d => !(d.sig === l.sig && Math.abs(now0 - d.at) < TRADE_WINDOW));
      }
      const drops = ((prev && prev.drops) || []).concat(real).slice(-DROP_HIST_MAX);
      if (drops.length) mine[name].drops = drops;
      const lost = ((prev && prev.lost) || []).filter(l => now0 - l.at < TRADE_WINDOW && mine[name].n - (l.n || 0) < STALE_UPDATES).concat(lostNow.map(l => ({ ...l, n: mine[name].n }))).slice(-LOST_HIST_MAX);   // 'lost' het han sau 2 lan cap nhat
      if (lost.length) mine[name].lost = lost;
      if (wr.winSeen) { mine[name].winSeen = true; if (wr.win) mine[name].win = wr.win; if (wr.cand.length) mine[name].cand = wr.cand; else delete mine[name].cand; }
      try { await runsTrack(a.owner, name, mine[name].rec, now0); } catch (e) { console.error('[runs-track] loi: ' + e.message); }   // tu dong dem Total Run - nhai dung co che cua Harvard (dungeonProgress/dungeonFinished/timeLeftGui..., xem runsTrack)
      // ===== XOA DU LIEU CU: khi co du lieu moi, bo cac ban ghi cu khong con lien quan (tranh tich luy -> lag web) =====
      if (mine[name].drops && mine[name].drops.length > DROP_HIST_MAX) mine[name].drops = mine[name].drops.slice(-DROP_HIST_MAX);
      if (mine[name].lost && mine[name].lost.length > LOST_HIST_MAX) mine[name].lost = mine[name].lost.slice(-LOST_HIST_MAX);
      if (mine[name].cand && mine[name].cand.length > CAND_HIST_MAX) mine[name].cand = mine[name].cand.slice(-CAND_HIST_MAX);
      if (mine[name].win && Date.now() - (mine[name].win.at || 0) > WIN_WINDOW) delete mine[name].win;   // tin hieu thang qua cu -> bo
      dirty = true;
      if (trade.loaded && a.owner) autoBuyGold2(a.owner, name, mine[name].rec);   // tu dong mua x2 Gold neu bat trong Config
      notifyOwner(a.owner);   // bao ngay cho dashboard dang mo (SSE) - khong doi toi vong poll tiep theo
      send(res, 200, { ok: true, trade: tradeInfoFor(name), ...(takeLobbyCmd(a.owner, name) ? { lobby: true } : {}) });   // kem lenh trade + lenh Force lobby (neu co) cho acc nay
    });
    return;
  }
  if (p === '/api/dump' && req.method === 'POST') {
    if (!a.admin) return send(res, 403, { error: 'admin only' }); // dump ghi file len may chu -> chi admin
    let body = '';
    req.on('data', c => { body += c; if (body.length > 4e6) req.destroy(); });
    req.on('end', () => {
      const nm = 'dump_' + s(u.searchParams.get('name') || 'acc', 40).replace(/[^\w.-]/g, '_');
      redis([['SET', 'dq:' + nm, body]]).then(() => send(res, 200, { ok: true, file: nm })).catch(() => send(res, 500, { error: 'save failed' }));
    });
    return;
  }
  if (p === '/api/img' && req.method === 'GET') {
    const n = s(u.searchParams.get('name'), 80).trim();
    if (!n) return send(res, 400, { error: 'no name' });
    const rid = /^\d{1,20}$/.test(u.searchParams.get('id') || '') ? u.searchParams.get('id') : '';
    if (rid && u.searchParams.get('debug') === '1') {   // chan doan: xem Roblox tra gi cho id nay
      fetch('https://thumbnails.roblox.com/v1/assets?assetIds=' + rid + '&returnPolicy=PlaceHolder&size=150x150&format=Png&isCircular=false', { headers: { 'user-agent': 'dq-tracker/1.0' } })
        .then(async r => send(res, 200, { id: rid, http: r.status, roblox: await r.json().catch(() => null) }))
        .catch(e => send(res, 200, { id: rid, error: String(e && e.message) }));
      return;
    }
    (rid ? rbxImg(rid).then(url => url || wikiImg(n)) : wikiImg(n)).then(url => send(res, 200, url === null ? { url: '', retry: true } : { url })).catch(() => send(res, 200, { url: '', retry: true }));
    return;
  }
  if (p === '/api/accounts' && req.method === 'GET') {
    const now = Date.now(); expireJobs();
    res.setHeader('x-now', String(now));   // gio server: dashboard dung de bu lech dong ho
    return send(res, 200, { build: BUILD_ID, accounts: Object.values(mine).map(x => { const { dbg, ...r } = x.rec; return { ...r, updated: x.updated, online: now - x.updated < ONLINE_MS, drops: x.drops || [], job: (trade.loaded && hasJob(r.name)) ? trade.jobs[r.name] : null }; }) });
  }
  if (p === '/api/debug' && req.method === 'GET') {   // mau truong item theo loai|do hiem (de tim ten truong pot max)
    return send(res, 200, { debug: Object.values(mine).filter(x => x.rec.dbg).map(x => ({ name: x.rec.name, dbg: x.rec.dbg })) });
  }
  if (p === '/api/accounts' && req.method === 'DELETE') {
    try { const nm = u.searchParams.get('name') || ''; const sv = await ckLoad(a.owner); if (Object.prototype.hasOwnProperty.call(sv, nm)) { delete sv[nm]; await ckSave(a.owner, sv); } } catch {}
    delete mine[u.searchParams.get('name') || '']; if (trade.loaded) delete trade.jobs[u.searchParams.get('name') || '']; dirty = true;
    return send(res, 200, { ok: true });
  }
  send(res, 404, { error: 'not found' });
  } catch (e) {
    console.error('[loi]', req.method, req.url, e && e.message);
    if (!res.headersSent) send(res, 500, { error: 'server error' }); else res.end();
  }
};

// ---- Diem vao cua Vercel: nap du lieu -> chay handler -> luu du lieu truoc khi tra loi ----
// FIX: auth/owners/snap/has la bien toan cuc dung chung. Vercel chay nhieu request cung luc tren 1 instance,
// request nay reset bien cua request kia -> sessionUser() thay rong -> bi day ra trang dang nhap.
// Giai phap: xep hang, moi instance chi xu ly 1 request tai 1 thoi diem.
let queueTail = Promise.resolve(), qDepth = 0;
const runExclusive = fn => {
  const run = queueTail.then(fn, fn);
  queueTail = run.catch(() => {});
  return run;
};
module.exports = (req, res) => {
  if (req.method === 'GET' && String(req.url || '').split('?')[0] === '/api/trade/wait') return tradeWait(req, res).catch(e => { console.error('[wait loi]', e && e.message); try { res.statusCode = 500; res.end(); } catch (x) {} });
  if (req.method === 'GET' && String(req.url || '').split('?')[0] === '/api/stream') return sseStream(req, res).catch(e => { console.error('[stream loi]', e && e.message); try { res.statusCode = 500; res.end(); } catch (x) {} });
  const t0 = Date.now(); qDepth++;
  return runExclusive(() => handleRequest(req, res)).finally(() => {
    qDepth--; const d = Date.now() - t0;
    if (d > 1500) console.log('[cham] ' + req.method + ' ' + String(req.url || '').split('?')[0] + ' mat ' + d + 'ms (dang xep hang: ' + qDepth + ' request) -> server xu ly cham, acc co the bi hien offline');
  });
};
const handleRequest = async (req, res) => {
  try { await loadState(req); }
  catch (e) { console.error('[nap loi]', e && e.message); res.statusCode = 503; res.setHeader('content-type', 'text/plain'); return res.end('Storage unavailable'); }
  const origEnd = res.end.bind(res); let ended = false;
  res.end = (...args) => {
    if (ended) return res; ended = true;
    saveState().catch(e => console.error('[luu loi]', e && e.message)).then(() => origEnd(...args));
    return res;
  };
  const closed = new Promise(r => { res.on('close', r); res.on('finish', r); setTimeout(r, 20000).unref?.(); });  // timeout: tranh 1 request treo chan ca hang doi
  try { await handle(req, res); } catch (e) { console.error('[loi]', e && e.message); if (!res.headersSent) res.statusCode = 500; res.end(); }
  await closed;
};

// ---- CHAY DOC LAP TREN MAY: node index.js ----
if (require.main === module || process.env.DQR_APP === '1') {   // DQR_APP=1: chay trong ban app dong goi (loader ma hoa)
  setInterval(() => { rjTick().catch(() => {}); }, 5000).unref();
  setInterval(() => { try { monEnsure(); } catch {} }, 15000).unref(); try { monEnsure(); } catch {}   // Monitor chay san de dem tong thoi gian chay
  setInterval(() => { pfTick().catch(() => {}); }, 2000).unref();   // Performance: gioi han CPU / don RAM / sap xep (chi chay khi bat)   // Rejoin (chi chay khi bat trong tab Rejoin)
  setInterval(() => { try { runsTick(); } catch (e) { console.error('[runs-tick] loi: ' + (e && e.stack || e)); } }, 5000).unref();   // Total Run: ket luan cac luot da ket thuc (xem ham runsTick); log loi ra console thay vi nuot im lang, de con biet khi bi gay
  http.createServer(module.exports).listen(PORT, HOST, () => {
    console.log('==============================================');
    console.log(' DQR Tracker dang chay: http://localhost:' + PORT);
    console.log(' Du lieu luu tai: ' + (USE_REMOTE ? 'Upstash Redis' : DATA_FILE));
    console.log(' Lua script:  http://localhost:' + PORT + '/script   (ApiKey = ' + KEY + ')');
    console.log('==============================================');
  }).on('error', e => { console.error('Khong mo duoc cong ' + PORT + ': ' + e.message); process.exit(1); });
}
