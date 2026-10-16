// ===== DQR Tracker - app Windows rieng =====
// App nay mo 1 cua so that su, nap giao dien tu domain that cua ban (Vercel) - du lieu/tai khoan
// Discord/dungeon dang theo deu la MOT, dung chung voi web.
//
// THEM (so voi ban cu "chi la 1 vo"): app nay cong chay NGAM 1 "local agent" - mot ban rieng,
// KHONG obfuscate, cua chinh index.js server cua ban (copy trong thu muc app\), nhung chi de
// phuc vu 3 chuc nang CAN DIEU KHIEN TRUC TIEP MAY TINH ma mot server tren mang (Vercel) khong
// bao gio lam duoc: Auto Rejoin (mo/dong Roblox), Performance (gioi han CPU/sap xep cua so),
// Config (doc/ghi file config Harvard Labs). Local agent nay KHONG dung Redis/Discord, chi nghe
// o 127.0.0.1 (may nay moi goi duoc) va chi tra loi cho dung domain web cua ban (CORS).
// Trang web (du mo trong app nay) tu nhan ra co local agent va goi thang vao day cho 3 chuc
// nang tren; moi thu khac (Trade, Cookie, Inventory, dang nhap...) van di qua domain that nhu cu.

const { app, BrowserWindow, Menu, ipcMain, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { spawn } = require('child_process');

const DEFAULT_URL = 'https://trackerdqrtbao.online';
const LOCAL_AGENT_PORT = 38245;
const CONFIG_PATH = path.join(app.getPath('userData'), 'config.json');
const AGENT_INDEX = path.join(__dirname, 'app', 'index.js');

function loadConfig() {
  try { return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch { return {}; }
}
function saveConfig(cfg) {
  try {
    fs.mkdirSync(path.dirname(CONFIG_PATH), { recursive: true });
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2));
  } catch (e) { console.error('[config]', e); }
}

// ---- Khoa rieng cho local agent: moi may tu sinh 1 lan, luu lai, KHONG lien quan gi den
// key/license tren web - chi de ngan trang khac (khong phai web tracker cua ban) goi vao agent.
function getLocalKey() {
  const cfg = loadConfig();
  if (cfg.localKey) return cfg.localKey;
  const k = crypto.randomBytes(24).toString('hex');
  saveConfig(Object.assign({}, cfg, { localKey: k }));
  return k;
}

let win = null;
let agentProc = null;

function startLocalAgent() {
  if (!fs.existsSync(AGENT_INDEX)) {
    console.error('[local-agent] Khong thay app\\index.js - 3 chuc nang Rejoin/Performance/Config se khong dung duoc. Xem HUONG_DAN.txt.');
    return;
  }
  const key = getLocalKey();
  const env = Object.assign({}, process.env, {
    ELECTRON_RUN_AS_NODE: '1',   // chay file .js bang Node co san trong Electron, khong can cai Node rieng tren may user
    PORT: String(LOCAL_AGENT_PORT),
    HOST: '127.0.0.1',
    API_KEY: key,
    LOCAL_AGENT: '1',
    LOCAL_AGENT_ORIGIN: DEFAULT_URL,
    // KHONG truyen DISCORD_CLIENT_ID/SECRET/PUBLIC_URL/KV_REST_API_* -> agent tu chay che do
    // 1 may/1 nguoi, khong dung Redis, khong dung Discord (dung du lieu JSON rieng trong app).
    DISCORD_CLIENT_ID: '', DISCORD_CLIENT_SECRET: '', PUBLIC_URL: '',
    KV_REST_API_URL: '', KV_REST_API_TOKEN: '', UPSTASH_REDIS_REST_URL: '', UPSTASH_REDIS_REST_TOKEN: '',
    DATA_FILE: path.join(app.getPath('userData'), 'local-agent-data.json'),
    // Ban portable se tu giai nen vao thu muc tam moi lan mo app (__dirname doi moi lan) nen
    // workspace.txt (link toi Potassium\HarvardLabs) phai luu o noi co dinh de khong bi mat lien link.
    CFG_WS_FILE: path.join(app.getPath('userData'), 'workspace.txt'),
  });
  try {
    agentProc = spawn(process.execPath, [AGENT_INDEX], { env, stdio: 'ignore', windowsHide: true });
    agentProc.on('exit', (code) => { console.log('[local-agent] da dung, code=' + code); agentProc = null; });
    agentProc.on('error', (e) => console.error('[local-agent] loi khoi dong:', e && e.message));
  } catch (e) {
    console.error('[local-agent] khong spawn duoc:', e && e.message);
  }
}

function stopLocalAgent() {
  if (agentProc) { try { agentProc.kill(); } catch {} agentProc = null; }
}

// Bao cho trang web (du dang mo o domain that) biet co local agent o day, va key de goi vao.
// Chay lai sau MOI lan trang nap xong (kem ca khi dang nhap Discord xong -> redirect ve lai).
function injectLocalInfo() {
  if (!win) return;
  const key = getLocalKey();
  const js = 'window.__DQ_LOCAL__=' + JSON.stringify({ url: 'http://127.0.0.1:' + LOCAL_AGENT_PORT, key: key }) + ';';
  win.webContents.executeJavaScript(js).catch(() => {});
}

function showSetup() {
  if (win) win.loadFile(path.join(__dirname, 'setup.html'));
}

function buildMenu() {
  // Khong hien thanh menu "Server" tren cung nua (giao dien gon hon, giong app that).
  // Reload/Doi server URL van dung duoc qua phim tat, khong can menu:
  //   Ctrl+R: tai lai trang.  Ctrl+Shift+S: doi server URL.
  Menu.setApplicationMenu(null);
}

function registerShortcuts() {
  if (!win) return;
  win.webContents.on('before-input-event', (_evt, input) => {
    if (!input || input.type !== 'keyDown') return;
    const k = (input.key || '').toLowerCase();
    if ((input.control || input.meta) && !input.shift && k === 'r') { win.reload(); }
    if ((input.control || input.meta) && input.shift && k === 's') { showSetup(); }
  });
}

function createWindow() {
  win = new BrowserWindow({
    width: 1320,
    height: 860,
    minWidth: 900,
    minHeight: 600,
    title: 'DQR Tracker',
    icon: path.join(__dirname, 'icon.ico'),
    backgroundColor: '#0a0a0a',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  const cfg = loadConfig();
  // Mac dinh mo thang domain that cua ban - khong can nguoi dung tu nhap URL nua.
  // Neu truoc do da tung doi server URL (qua menu "Server > Doi server URL...") thi dung URL da luu.
  win.loadURL(cfg.url || DEFAULT_URL);

  // Sau MOI lan trang nap xong (mo lan dau, F5, hoac redirect sau khi dang nhap Discord) ->
  // bao lai cho trang biet local agent van o day (bien window bi mat moi lan nap trang moi).
  win.webContents.on('did-finish-load', injectLocalInfo);

  // link mo ra ngoai (vd nut Discord trong trang) -> mo bang trinh duyet that, khong mo
  // tab moi trong chinh app (app nay chi la 1 cua so duy nhat).
  win.webContents.setWindowOpenHandler(({ url }) => { shell.openExternal(url); return { action: 'deny' }; });

  win.on('closed', () => { win = null; });
}

ipcMain.handle('save-url', (_evt, url) => {
  let u = String(url || '').trim();
  if (!u) return { ok: false, error: 'Chưa nhập địa chỉ.' };
  if (!/^https?:\/\//i.test(u)) u = 'https://' + u;
  saveConfig(Object.assign({}, loadConfig(), { url: u }));
  if (win) win.loadURL(u);
  return { ok: true };
});

app.whenReady().then(() => { buildMenu(); startLocalAgent(); createWindow(); registerShortcuts(); });

app.on('window-all-closed', () => { stopLocalAgent(); app.quit(); });
app.on('before-quit', stopLocalAgent);

app.on('activate', () => { if (BrowserWindow.getAllWindows().length === 0) { createWindow(); registerShortcuts(); } });
