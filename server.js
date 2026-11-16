// Kulübe — arkadaşlarla yazılı + sesli sohbet + canlı yayın arka ucu
// Tek bir arka uç, birden çok "sunucu" (Discord'daki gibi: isimli, kendi kanalları ve üyeleri olan topluluk) barındırır.
// Çalıştır: node server.js   (varsayılan port 3000; PORT ve DATA_DIR ortam değişkenleri desteklenir)
const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const dns = require('dns');
const net = require('net');
const httpMod = require('http');
const httpsMod = require('https');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.KULUBE_DATA || path.join(process.env.DATA_DIR || __dirname, 'data.json');
const MAX_HISTORY = 300;   // kanal başına saklanan mesaj sayısı
const MAX_SERVERS = 300;   // toplam sunucu sınırı
const MAX_FILE = 8 * 1024 * 1024;                                  // tek dosya sınırı (8 MB)
const FILE_QUOTA = (process.env.DATABASE_URL ? 150 : 1000) * 1024 * 1024; // sunucu başına toplam dosya alanı; dolunca en eski dosyalar silinir
const FILE_DIR = path.join(path.dirname(DATA_FILE), 'dosyalar');
let publicUrl = null;      // masaüstü uygulaması tünel adresini buraya yazar (davet linki için)

// ---- Kalıcı veri (data.json) ----
// data.servers[kod] = { name, adminKey, textChannels, voiceChannels, messages: { kanal: [mesaj] }, created }
// "kod" hem sunucunun kimliği hem davet kodudur. adminKey sunucuyu kuran kişide durur (yönetici).
//
// İki depo var:
//  - Dosya (varsayılan): her şey data.json içinde. Kendi bilgisayarında barındırırken kullanılır.
//  - Postgres (DATABASE_URL tanımlıysa): diski kalıcı olmayan barındırma servisleri için.
//    Tablo: kulube_servers(code, doc jsonb) — her sunucu tek satır; sadece değişen sunucular yazılır.
let data = { servers: {} };
const usePg = !!process.env.DATABASE_URL;
let pool = null;
const dirty = new Set(); // kaydedilmeyi bekleyen sunucu kodları
let saveTimer = null, saving = false;

// Sunuculardan bağımsız veriler (aynı arka uçtaki herkes için ortak): data.global
//   users[uid] = { name, seen }            bu arka uca bir kez girmiş herkes (arkadaş kodu = uid)
//   friends['uidA:uidB'] = { from, ts, ok } arkadaşlık (ok yoksa: from'un gönderdiği, bekleyen istek)
//   dms['uidA:uidB'] = [mesaj]             özel mesajlar (1.6.0'dan önce sunucu kaydındaydı; açılışta buraya taşınır)
// Postgres'te kulube_servers tablosunda "~global" kodlu satırda durur.
const GLOBAL = '~global';
function normGlobal() {
  const g = (data.global ||= {});
  g.users ||= {}; g.friends ||= {}; g.dms ||= {};
  return g;
}
async function loadData() {
  if (usePg) {
    const { Pool } = require('pg');
    pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 3, idleTimeoutMillis: 30000 });
    pool.on('error', (e) => console.error('veritabani baglanti hatasi:', e.message));
    await pool.query('CREATE TABLE IF NOT EXISTS kulube_servers (code text PRIMARY KEY, doc jsonb NOT NULL)');
    const r = await pool.query('SELECT code, doc FROM kulube_servers');
    for (const row of r.rows) {
      const doc = typeof row.doc === 'string' ? JSON.parse(row.doc) : row.doc;
      if (row.code === GLOBAL) data.global = doc; else data.servers[row.code] = doc; // "~global" satırı: arkadaşlar ve özel mesajlar
    }
    await pool.query('CREATE TABLE IF NOT EXISTS kulube_files (id text PRIMARY KEY, data bytea NOT NULL)');
    indexAll();
    return;
  }
  // Ana dosya bozuksa (ör. eski sürümün yazdığı yarım dosya) bir önceki sağlam kayıt olan yedekten açılır
  let raw = null;
  for (const f of [DATA_FILE, BAK_FILE]) {
    let text;
    try { text = fs.readFileSync(f, 'utf8'); } catch { continue; } // dosya yok
    try { raw = JSON.parse(text); } catch {
      const keep = f + '.bozuk-' + Date.now();
      try { fs.copyFileSync(f, keep); } catch {}
      console.error('Kayit dosyasi bozuk: ' + f + ' (kopyasi: ' + path.basename(keep) + '). Yedege bakiliyor.');
      continue;
    }
    if (f === BAK_FILE) console.error('Kayit yedekten acildi: ' + f);
    break;
  }
  if (raw) {
    if (raw.servers) data = raw;
    else if (raw.code) { // eski tek odalı biçimden geçiş
      data.servers[raw.code] = {
        name: 'Kulübe', adminKey: raw.adminKey || crypto.randomBytes(12).toString('hex'),
        textChannels: raw.textChannels || ['genel'], voiceChannels: raw.voiceChannels || ['Sohbet Odası'],
        messages: raw.messages || {}, created: Date.now(),
      };
      save(raw.code);
    }
  }
  indexAll();
}

// ---- Güvenli kayıt (dosya modu) ----
// Önce geçici dosyaya yazılır ve diske işlenmesi beklenir (fsync), sonra tek hamlede ana dosyanın yerine konur.
// Böylece yazma sırasında elektrik kesilse bile ana dosya ya eski ya yeni hâliyle sağlam kalır; yarım dosya oluşmaz.
// Her kayıttan önce bir önceki sağlam hâl ".yedek" olarak saklanır.
const BAK_FILE = DATA_FILE + '.yedek';
const isBusy = (e) => ['EPERM', 'EBUSY', 'EACCES'].includes(e.code); // Windows: virüs tarayıcısı dosyayı o an tutuyor olabilir
async function saveFileAsync() {
  const text = JSON.stringify(data), tmp = DATA_FILE + '.tmp';
  const fh = await fs.promises.open(tmp, 'w');
  try { await fh.writeFile(text); await fh.sync(); } finally { await fh.close(); }
  await fs.promises.copyFile(DATA_FILE, BAK_FILE).catch(() => {});
  for (let i = 0; ; i++) {
    try { await fs.promises.rename(tmp, DATA_FILE); return; }
    catch (e) { if (i >= 6 || !isBusy(e)) throw e; await new Promise((r) => setTimeout(r, 60 * (i + 1))); }
  }
}
function saveFileSync() {
  const text = JSON.stringify(data), tmp = DATA_FILE + '.tmp2';
  const fd = fs.openSync(tmp, 'w');
  try { fs.writeSync(fd, text); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  try { fs.copyFileSync(DATA_FILE, BAK_FILE); } catch {}
  for (let i = 0; ; i++) {
    try { fs.renameSync(tmp, DATA_FILE); return; }
    catch (e) { if (i >= 6 || !isBusy(e)) throw e; const until = Date.now() + 60 * (i + 1); while (Date.now() < until); }
  }
}
// Kapanırken bekleyen değişiklikleri hemen (beklemeden) yazar
function flushSync() {
  if (usePg || (!dirty.size && !saveTimer)) return;
  clearTimeout(saveTimer); saveTimer = null; dirty.clear();
  try { saveFileSync(); } catch (e) { console.error('kapanista kayit hatasi:', e.message); }
}

// Eski kayıtlarda olmayan alanları tamamlar.
//  members[uid] = { name, role?, avatar?, av?, seen }   uid: cihazdaki gizli anahtarın özeti (kalıcı kimlik)
//  bans[uid] = { name, ts }      perms['t:kanal' | 'v:kanal'] = 'staff' (sadece yetkililer yazar/konuşur)
//  dms['uidA:uidB'] = [mesaj]    files = [{ id, name, type, size, ts, uid }]
//  sounds = [{ id (dosya id), name, uid, ts }]   ses efekt panosuna eklenen özel sesler
function norm(s) {
  s.members ||= {}; s.bans ||= {}; s.perms ||= {}; s.dms ||= {}; s.files ||= []; s.messages ||= {}; s.sounds ||= [];
  // roles = [{ id, name, color, perms: [yetki] }]  kurucunun tanımladığı özel roller; yetkiler için aşağıdaki PERMS'e bak
  // roleNames = { owner, mod, member }     hazır rollerin görünen adları;  members[uid].roles = [rol id]
  // audit = [{ ts, by, act, target, detail }]  denetim kaydı (son 300 işlem)
  s.roles ||= []; s.roleNames ||= {}; s.audit ||= [];
  for (const r of s.roles) { if (!Array.isArray(r.perms)) r.perms = r.staff ? [...PERMS] : []; delete r.staff; } // 1.4.x: "yetkili" kutusu = tüm yetkiler
  return s;
}
const fileIndex = new Map(); // dosya id -> { code, meta }
const imgAllow = new Set();  // link önizlemelerindeki resim adresleri (sadece bunlar /p üzerinden getirilir)
function indexAll() {
  const g = normGlobal();
  const note = (list) => list.forEach((m) => m.preview && m.preview.image && imgAllow.add(m.preview.image));
  for (const [code, s] of Object.entries(data.servers)) {
    for (const f of norm(s).files) fileIndex.set(f.id, { code, meta: f });
    Object.values(s.messages).forEach(note);
    // Eski sunucuya bağlı özel mesajları ortak alana taşı (aynı iki kişi birden çok sunucuda yazıştıysa birleştir)
    let moved = false;
    for (const [key, list] of Object.entries(s.dms)) {
      if (list.length) { const have = new Set((g.dms[key] || []).map((m) => m.id)); g.dms[key] = (g.dms[key] || []).concat(list.filter((m) => !have.has(m.id))).sort((a, b) => a.ts - b.ts).slice(-MAX_HISTORY); }
      delete s.dms[key]; moved = true;
    }
    for (const [u, m] of Object.entries(s.members)) if (!g.users[u] || (m.seen || 0) > (g.users[u].seen || 0)) g.users[u] = { name: m.name, seen: m.seen || 0 };
    if (moved) { dirty.add(code); dirty.add(GLOBAL); }
  }
  Object.values(g.dms).forEach(note);
  if (dirty.size) save();
}

// ---- Dosya deposu: diskte "dosyalar" klasörü ya da Postgres'te kulube_files tablosu ----
async function putFile(id, buf) {
  if (usePg) return pool.query('INSERT INTO kulube_files (id, data) VALUES ($1, $2)', [id, buf]);
  await fs.promises.mkdir(FILE_DIR, { recursive: true });
  await fs.promises.writeFile(path.join(FILE_DIR, id), buf);
}
async function getFile(id) {
  if (usePg) { const r = await pool.query('SELECT data FROM kulube_files WHERE id = $1', [id]); return r.rows[0] ? r.rows[0].data : null; }
  return fs.promises.readFile(path.join(FILE_DIR, id)).catch(() => null);
}
function delFile(id) {
  const f = fileIndex.get(id); if (!f) return;
  fileIndex.delete(id);
  const s = data.servers[f.code];
  if (s) { s.files = s.files.filter((x) => x.id !== id); s.sounds = (s.sounds || []).filter((x) => x.id !== id); save(f.code); }
  if (usePg) pool.query('DELETE FROM kulube_files WHERE id = $1', [id]).catch(() => {});
  else fs.unlink(path.join(FILE_DIR, id), () => {});
}

// Değişikliği kısa bir gecikmeyle diske/veritabanına yazar. code: değişen sunucu.
function save(code) {
  if (code) dirty.add(code);
  clearTimeout(saveTimer);
  saveTimer = setTimeout(flush, usePg ? 1500 : 500);
}

async function flush() {
  clearTimeout(saveTimer); saveTimer = null;
  if (saving) return save(); // önceki kayıt sürüyor: bitince yeniden denenir
  if (!usePg) {
    saving = true; dirty.clear();
    try { await saveFileAsync(); }
    catch (e) { console.error('kayit hatasi:', e.message); saveTimer = setTimeout(flush, 2000); }
    finally { saving = false; }
    return;
  }
  saving = true;
  const codes = [...dirty]; dirty.clear();
  try {
    for (const c of codes) {
      if (c === GLOBAL) { await pool.query('INSERT INTO kulube_servers (code, doc) VALUES ($1, $2) ON CONFLICT (code) DO UPDATE SET doc = EXCLUDED.doc', [c, JSON.stringify(data.global)]); continue; }
      if (!data.servers[c]) continue;
      await pool.query('INSERT INTO kulube_servers (code, doc) VALUES ($1, $2) ON CONFLICT (code) DO UPDATE SET doc = EXCLUDED.doc', [c, JSON.stringify(data.servers[c])]);
    }
  } catch (e) { // veritabanı uyuyor/ulaşılamıyor olabilir: kaybetme, biraz sonra yeniden dene
    console.error('kayit hatasi:', e.message);
    codes.forEach((c) => dirty.add(c));
    saveTimer = setTimeout(flush, 5000);
  } finally { saving = false; }
}

function cleanName(s, max) {
  return String(s || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function createServer(name) {
  let code;
  do { code = crypto.randomBytes(4).toString('hex'); } while (data.servers[code]);
  data.servers[code] = {
    name, adminKey: crypto.randomBytes(12).toString('hex'),
    textChannels: ['genel', 'oyun', 'müzik'], voiceChannels: ['Sohbet Odası', 'Oyun Odası'],
    messages: {}, created: Date.now(),
  };
  norm(data.servers[code]);
  save(code);
  return code;
}

// ---- Web sunucusu ----
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e5 });
app.set('trust proxy', true);
app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (_req, res) => res.send('ok'));

// Yeni sunucu kurma. Kötüye kullanıma karşı: IP başına saatte 5, toplamda MAX_SERVERS.
const createLog = new Map(); // ip -> [zaman]
app.post('/api/servers', express.json({ limit: '2kb' }), (req, res) => {
  const name = cleanName(req.body && req.body.name, 30);
  if (!name) return res.status(400).json({ error: 'Sunucuya bir isim vermelisin.' });
  if (Object.keys(data.servers).length >= MAX_SERVERS) return res.status(429).json({ error: 'Sunucu sınırına ulaşıldı.' });
  const now = Date.now(), ip = req.ip || '?';
  const recent = (createLog.get(ip) || []).filter((t) => now - t < 3600000);
  if (recent.length >= 5) return res.status(429).json({ error: 'Çok fazla sunucu kurdun, biraz sonra tekrar dene.' });
  recent.push(now); createLog.set(ip, recent);
  const code = createServer(name);
  res.json({ code, name, adminKey: data.servers[code].adminKey });
});

// ---- Dosya yükleme / indirme ----
// Yükleme: giriş yapmış kişinin gizli yükleme anahtarıyla (init ile verilir). Dosya, bir mesaja eklenince görünür olur.
const tokens = new Map();    // yükleme anahtarı -> socket.id
const uploadLog = new Map(); // uid -> [zaman]
const INLINE = /^(image\/(png|jpeg|gif|webp)|video\/(mp4|webm)|audio\/(mpeg|ogg|wav|webm|mp4))$/;
app.post('/api/upload', express.raw({ type: () => true, limit: MAX_FILE }), async (req, res) => {
  const user = users.get(tokens.get(String(req.get('x-kulube-token') || '')));
  if (!user) return res.status(403).json({ error: 'Önce sunucuya bağlanmalısın.' });
  const buf = req.body;
  if (!Buffer.isBuffer(buf) || !buf.length) return res.status(400).json({ error: 'Dosya boş.' });
  const now = Date.now(), recent = (uploadLog.get(user.uid) || []).filter((t) => now - t < 600000);
  if (recent.length >= 30) return res.status(429).json({ error: 'Çok fazla dosya gönderdin, biraz bekle.' });
  recent.push(now); uploadLog.set(user.uid, recent);
  const srv = data.servers[user.server];
  let name = cleanName(req.query.name, 100).replace(/[\\/:*?"<>|\x00-\x1f]/g, '_') || 'dosya';
  const type = String(req.query.type || '').toLowerCase().slice(0, 80);
  const meta = { id: crypto.randomBytes(16).toString('hex'), name, type: /^[a-z0-9.+-]+\/[a-z0-9.+-]+$/.test(type) ? type : 'application/octet-stream', size: buf.length, ts: now, uid: user.uid };
  try { await putFile(meta.id, buf); } catch (e) { console.error('dosya kayit hatasi:', e.message); return res.status(500).json({ error: 'Dosya kaydedilemedi.' }); }
  srv.files.push(meta); fileIndex.set(meta.id, { code: user.server, meta });
  let total = srv.files.reduce((n, f) => n + f.size, 0);
  while (total > FILE_QUOTA && srv.files.length > 1) { total -= srv.files[0].size; delFile(srv.files[0].id); } // alan dolduysa en eskiler gider
  save(user.server);
  res.json({ id: meta.id, name: meta.name, type: meta.type, size: meta.size });
});
app.use('/api/upload', (err, _req, res, _next) => {
  res.status(err.type === 'entity.too.large' ? 413 : 400).json({ error: err.type === 'entity.too.large' ? 'Dosya çok büyük (en fazla 8 MB).' : 'Dosya yüklenemedi.' });
});

// İndirme: kimlik tahmin edilemez (32 haneli). Resim/video/ses sayfada gösterilir; diğer her şey "indir" olarak gelir.
app.get('/f/:id/:name?', async (req, res) => {
  const f = fileIndex.get(req.params.id);
  const buf = f && await getFile(f.meta.id).catch(() => null);
  if (!buf) return res.status(404).send('Dosya bulunamadı ya da silinmiş.');
  const inline = INLINE.test(f.meta.type);
  res.set({
    'Content-Type': inline ? f.meta.type : 'application/octet-stream',
    'Content-Disposition': (inline ? 'inline' : 'attachment') + "; filename*=UTF-8''" + encodeURIComponent(f.meta.name),
    'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': "default-src 'none'; sandbox",
    'Cache-Control': 'private, max-age=31536000, immutable', 'Accept-Ranges': 'bytes',
  });
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || ''); // video ileri sarma için
  if (m && (m[1] || m[2])) {
    let a = m[1] ? Number(m[1]) : Math.max(0, buf.length - Number(m[2])), b = m[1] && m[2] ? Math.min(Number(m[2]), buf.length - 1) : buf.length - 1;
    if (a > b || a >= buf.length) return res.status(416).set('Content-Range', 'bytes */' + buf.length).end();
    return res.status(206).set('Content-Range', 'bytes ' + a + '-' + b + '/' + buf.length).end(buf.subarray(a, b + 1));
  }
  res.end(buf);
});

// Profil resmi (üye kaydında küçük bir resim olarak durur)
app.get('/a/:code/:uid', (req, res) => {
  const s = data.servers[req.params.code], m = s && s.members[req.params.uid];
  const d = m && m.avatar && /^data:(image\/(?:jpeg|png|webp));base64,(.+)$/.exec(m.avatar);
  if (!d) return res.status(404).end();
  res.set({ 'Content-Type': d[1], 'X-Content-Type-Options': 'nosniff', 'Cache-Control': 'public, max-age=31536000, immutable' }).end(Buffer.from(d[2], 'base64'));
});

// ---- Link önizlemesi ----
// Mesajdaki ilk link arka uçta açılıp başlığı, açıklaması ve resmi alınır (YouTube için oEmbed).
// İç ağ adreslerine (localhost, 192.168.x.x, bulut meta veri adresi vb.) asla gidilmez: adres çözüldükten sonra,
// bağlantı kurulmadan önce denetlenir (DNS ile kandırmaya karşı) ve her yönlendirmede yeniden denetlenir.
const BLOCK = new net.BlockList();
for (const [a, p] of [['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8], ['169.254.0.0', 16], ['172.16.0.0', 12], ['192.0.0.0', 24], ['192.0.2.0', 24],
  ['192.88.99.0', 24], ['192.168.0.0', 16], ['198.18.0.0', 15], ['198.51.100.0', 24], ['203.0.113.0', 24], ['224.0.0.0', 3]]) BLOCK.addSubnet(a, p, 'ipv4');
for (const [a, p] of [['::', 128], ['::1', 128], ['fc00::', 7], ['fe80::', 10], ['ff00::', 8], ['64:ff9b::', 96], ['2001:db8::', 32]]) BLOCK.addSubnet(a, p, 'ipv6');
const localOk = () => process.env.KULUBE_PREVIEW_LOCAL === '1'; // sadece testler için
function badIp(addr) {
  if (localOk()) return false;
  if (net.isIPv6(addr)) { const m = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(addr); return m ? BLOCK.check(m[1], 'ipv4') : BLOCK.check(addr, 'ipv6'); }
  return BLOCK.check(addr, 'ipv4');
}
function safeLookup(host, opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {}; }
  dns.lookup(host, { all: true }, (err, list) => {
    if (err) return cb(err);
    if (!list.length || list.some((a) => badIp(a.address))) return cb(Object.assign(new Error('iç ağ adresine izin yok'), { code: 'EBLOCKED' }));
    if (opts && opts.all) cb(null, list); else cb(null, list[0].address, list[0].family);
  });
}
const UA = 'Mozilla/5.0 (compatible; KulubeOnizleme/1.0)';
// Güvenli indirme: en fazla maxBytes; whole=false ise sınırı aşan gövde kesilir (HTML için), true ise hata verir (resim için)
function fetchSafe(url, maxBytes, whole, hops = 0) {
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(url); } catch { return reject(new Error('geçersiz adres')); }
    if (!/^https?:$/.test(u.protocol) || u.username || u.password) return reject(new Error('geçersiz adres'));
    if (u.port && !['80', '443', '8080', '8443'].includes(u.port) && !localOk()) return reject(new Error('izin verilmeyen kapı'));
    // Adres doğrudan IP ise Node DNS'e hiç sormaz (safeLookup çalışmaz); bu yüzden burada ayrıca denetlenir
    const ip = u.hostname.replace(/^\[|\]$/g, '');
    if (net.isIP(ip) && badIp(ip)) return reject(Object.assign(new Error('iç ağ adresine izin yok'), { code: 'EBLOCKED' }));
    const mod = u.protocol === 'https:' ? httpsMod : httpMod;
    let done = false;
    const fin = (fn, v) => { if (!done) { done = true; fn(v); } };
    const req = mod.get(u, { lookup: safeLookup, timeout: 6000, headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml,image/*;q=0.9,*/*;q=0.5', 'Accept-Language': 'tr,en;q=0.8' } }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        if (hops >= 4) return fin(reject, new Error('çok fazla yönlendirme'));
        return fetchSafe(new URL(res.headers.location, u).href, maxBytes, whole, hops + 1).then((v) => fin(resolve, v), (e) => fin(reject, e));
      }
      if (res.statusCode >= 400) { res.resume(); return fin(reject, new Error('durum ' + res.statusCode)); }
      const chunks = []; let n = 0;
      const out = () => ({ url: u.href, type: String(res.headers['content-type'] || '').toLowerCase(), buf: Buffer.concat(chunks) });
      res.on('data', (c) => {
        n += c.length;
        if (n > maxBytes) {
          res.destroy();
          if (whole) return fin(reject, new Error('çok büyük'));
          chunks.push(c.subarray(0, c.length - (n - maxBytes))); return fin(resolve, out());
        }
        chunks.push(c);
      });
      res.on('end', () => fin(resolve, out()));
      res.on('error', (e) => fin(reject, e));
    });
    req.on('timeout', () => req.destroy(new Error('zaman aşımı')));
    req.on('error', (e) => fin(reject, e));
  });
}
const ENT = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', '#39': "'" };
const decodeEnt = (s) => String(s || '').replace(/&(#x[0-9a-f]+|#\d+|[a-z]+\d*);/gi, (m, e) => {
  if (e[0] === '#') { const n = e[1] === 'x' || e[1] === 'X' ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10); return n > 0 && n < 0x110000 ? String.fromCodePoint(n) : m; }
  return ENT[e.toLowerCase()] ?? m;
}).replace(/\s+/g, ' ').trim();
const firstUrl = (text) => { const m = /https?:\/\/[^\s<>"']+/i.exec(text || ''); return m ? m[0].replace(/[),.!?;:'"]+$/, '') : null; };
const absImg = (src, base) => { if (!String(src || '').trim()) return null; try { const u = new URL(decodeEnt(src), base); return /^https?:$/.test(u.protocol) && u.href.length <= 1000 ? u.href : null; } catch { return null; } };

async function unfurl(url) {
  const host = new URL(url).hostname.replace(/^www\.|^m\./, '');
  if (/^(youtube\.com|youtu\.be|music\.youtube\.com)$/.test(host)) {
    try { // bazı videolar oEmbed vermez (gömme kapalı vb.); o zaman sayfanın kendisine bakılır
      const r = await fetchSafe('https://www.youtube.com/oembed?format=json&url=' + encodeURIComponent(url), 200000, true);
      const d = JSON.parse(r.buf.toString('utf8'));
      if (d.title) return { url, kind: 'video', site: 'YouTube', title: String(d.title).slice(0, 200), desc: String(d.author_name || '').slice(0, 300), image: absImg(d.thumbnail_url || '', url) };
    } catch {}
  }
  const r = await fetchSafe(url, 800000, false);
  if (/^image\/(png|jpeg|gif|webp|avif)/.test(r.type)) return { url: r.url, kind: 'image', site: new URL(r.url).hostname, title: '', desc: '', image: r.url.length <= 1000 ? r.url : null };
  const looksHtml = !r.type && /^\s*(<!doctype html|<html|<head)/i.test(r.buf.toString('latin1', 0, 200)); // türünü bildirmeyen sayfalar
  if (!/text\/html|application\/xhtml/.test(r.type) && !looksHtml) return null;
  let cs = (/charset=([\w-]+)/i.exec(r.type) || [])[1];
  const head = r.buf.toString('latin1', 0, Math.min(r.buf.length, 300000));
  cs ||= (/<meta[^>]+charset=["']?([\w-]+)/i.exec(head) || [])[1] || 'utf-8';
  let html;
  try { html = new TextDecoder(cs.toLowerCase()).decode(r.buf); } catch { html = r.buf.toString('utf8'); }
  html = html.slice(0, (html.search(/<\/head>/i) + 1 || 300000) + 7);
  const meta = {};
  for (const t of html.match(/<meta\b[^>]*>/gi) || []) {
    const key = (/\b(?:property|name|itemprop)\s*=\s*["']([^"']+)["']/i.exec(t) || [])[1];
    const val = (/\bcontent\s*=\s*"([^"]*)"/i.exec(t) || /\bcontent\s*=\s*'([^']*)'/i.exec(t) || [])[1];
    if (key && val != null && !(key.toLowerCase() in meta)) meta[key.toLowerCase()] = val;
  }
  const title = decodeEnt(meta['og:title'] || meta['twitter:title'] || (/<title[^>]*>([^<]*)<\/title>/i.exec(html) || [])[1] || '').slice(0, 200);
  const desc = decodeEnt(meta['og:description'] || meta['twitter:description'] || meta.description || '').slice(0, 300);
  const image = absImg(meta['og:image'] || meta['og:image:url'] || meta['twitter:image'] || meta['twitter:image:src'] || '', r.url);
  if ((!title || /^[-–\s]*YouTube$/.test(title)) && !image) return null; // boş sayfa (ör. YouTube onay sayfası)
  return { url: r.url, kind: meta['og:type'] && /video/.test(meta['og:type']) ? 'video' : 'link', site: decodeEnt(meta['og:site_name'] || new URL(r.url).hostname).slice(0, 60), title, desc, image };
}
const unfurlCache = new Map(); // adres -> { p, t }
let unfurling = 0;
async function preview(url) {
  const c = unfurlCache.get(url);
  if (c && Date.now() - c.t < 6 * 3600000) return c.p;
  if (unfurling >= 6) return null;
  unfurling++;
  let p = null;
  try { p = await unfurl(url); } catch {} finally { unfurling--; }
  unfurlCache.set(url, { p, t: Date.now() });
  if (unfurlCache.size > 400) unfurlCache.delete(unfurlCache.keys().next().value);
  return p;
}

// Önizleme resimleri arka uç üzerinden gelir: izleyenlerin adresi bağlantı verilen siteye gitmez, sadece önizlemedeki resimler getirilir
const imgCache = new Map();
app.get('/p', async (req, res) => {
  const u = String(req.query.u || '');
  if (!imgAllow.has(u)) return res.status(404).end();
  let hit = imgCache.get(u);
  if (!hit) {
    try {
      const r = await fetchSafe(u, 5 * 1024 * 1024, true);
      const type = (/^image\/(png|jpeg|gif|webp|avif)/.exec(r.type) || [])[0];
      if (!type) return res.status(415).end();
      hit = { type, buf: r.buf };
      if (r.buf.length < 1500000) { imgCache.set(u, hit); if (imgCache.size > 80) imgCache.delete(imgCache.keys().next().value); }
    } catch { return res.status(502).end(); }
  }
  res.set({ 'Content-Type': hit.type, 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': "default-src 'none'; sandbox", 'Cache-Control': 'public, max-age=86400' }).end(hit.buf);
});

// Yüklenip hiçbir mesaja eklenmemiş dosyalar (gönderimden vazgeçildi, bağlantı koptu) 1 saat sonra silinir
setInterval(() => {
  const cutoff = Date.now() - 3600000;
  for (const [id, f] of [...fileIndex]) if (!f.meta.used && f.meta.ts < cutoff) delFile(id);
}, 600000).unref();

// ---- Ağ engeli çözümü (TURN aktarma sunucusu) ----
// Ya sabit bilgiler:  TURN_URLS (virgülle), TURN_USER, TURN_PASS
// ya da Cloudflare:   CF_TURN_KEY_ID, CF_TURN_API_TOKEN (kısa ömürlü bilgiler arka uçta üretilir)
const STUN = { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] };
let iceCache = null, iceUntil = 0;
async function getIce() {
  if (process.env.TURN_URLS) {
    return [STUN, { urls: process.env.TURN_URLS.split(',').map((x) => x.trim()).filter(Boolean), username: process.env.TURN_USER || '', credential: process.env.TURN_PASS || '' }];
  }
  if (process.env.CF_TURN_KEY_ID && process.env.CF_TURN_API_TOKEN) {
    if (iceCache && Date.now() < iceUntil) return iceCache;
    try {
      const r = await fetch('https://rtc.live.cloudflare.com/v1/turn/keys/' + process.env.CF_TURN_KEY_ID + '/credentials/generate-ice-servers', {
        method: 'POST', headers: { Authorization: 'Bearer ' + process.env.CF_TURN_API_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl: 86400 }), signal: AbortSignal.timeout(5000),
      });
      const d = await r.json();
      const list = (Array.isArray(d.iceServers) ? d.iceServers : [d.iceServers]).filter(Boolean)
        .map((x) => ({ ...x, urls: [].concat(x.urls).filter((u) => !/:53(\?|$)/.test(u)) })); // 53. kapıyı tarayıcılar engeller
      if (r.ok && list.length) { iceCache = list; iceUntil = Date.now() + 12 * 3600000; return list; }
      console.error('TURN bilgisi alinamadi:', r.status);
    } catch (e) { console.error('TURN bilgisi alinamadi:', e.message); }
  }
  return [STUN];
}

// ---- Çevrimiçi kullanıcılar ----
const users = new Map(); // socket.id -> kullanıcı (server alanı hangi sunucuda olduğunu söyler)
const COLORS = ['#f2a65a', '#6fcfb0', '#e88fb4', '#8fb8f0', '#d7c35a', '#b79cf0', '#f08a7a', '#7fd0e0'];
function colorFor(name) {
  let h = 0;
  for (const c of name) h = (h * 31 + c.codePointAt(0)) >>> 0;
  return COLORS[h % COLORS.length];
}
const room = (code) => 's:' + code;
// ---- Yetkiler ----
// Özel rollere tek tek verilebilen yetkiler. Kurucu ve (elle atanmış) yardımcı yöneticide hepsi vardır.
//  rename: üyelerin ismini değiştirir      mute: mikrofon/kulaklık kapatır      kick: sunucudan atar      ban: yasaklar, yasağı kaldırır
//  msgs: başkasının mesajını ve efekt seslerini siler      channels: kanalları yeniden adlandırır, siler, kilitler
//  locked: kilitli kanallarda yazar/konuşur      audit: denetim kaydını görür
const PERMS = ['rename', 'mute', 'move', 'kick', 'ban', 'msgs', 'channels', 'locked', 'audit']; // move: seste başka kanala taşır
const PERM_TR = { move: 'kanala taşıma', rename: 'isim değiştirme', mute: 'susturma', kick: 'atma', ban: 'yasaklama', msgs: 'mesaj silme', channels: 'kanal yönetimi', locked: 'kilitli kanalda yazma', audit: 'denetim kaydı' };
const can = (u, p) => u.role === 'owner' || u.role === 'mod' || (u.perms || []).includes(p);
// Sıra: kurucu 3 > yardımcı yönetici 2 > herhangi bir yetkisi olan 1 > üye 0. Kişi sadece kendinden alt sıradakine işlem yapabilir.
const rankOf = (u) => (u.role === 'owner' ? 3 : u.role === 'mod' ? 2 : (u.perms || []).length ? 1 : 0);
function permsOf(s, m, role) {
  if (role !== 'member') return [...PERMS];
  const set = new Set();
  for (const id of (m && m.roles) || []) { const r = s.roles.find((x) => x.id === id); if (r) r.perms.forEach((p) => set.add(p)); }
  return PERMS.filter((p) => set.has(p));
}

// ---- Özel roller ----
// Kişinin yetkisi: kurucu (yönetici anahtarı) > yardımcı (elle atanmış ya da "yetkili" işaretli bir özel rolü var) > üye.
function roleOf(s, m, admin) {
  if (admin) return 'owner';
  if (!m) return 'member';
  return m.role === 'mod' ? 'mod' : 'member';
}
// İsim rengi: kişinin (listede en üstteki) renkli özel rolü; yoksa isme göre sabit renk.
function colorOf(s, m, name) {
  const r = m && m.roles && s.roles.find((x) => x.color && m.roles.includes(x.id));
  return r ? r.color : colorFor(name);
}
// Roller değişince çevrimiçi herkesin yetkisini ve rengini yeniler.
function refreshRoles(code) {
  const s = data.servers[code];
  for (const u of users.values()) {
    if (u.server !== code) continue;
    const m = s.members[u.uid];
    u.role = roleOf(s, m, u.admin); u.color = colorOf(s, m, u.name); u.roles = (m && m.roles) || []; u.perms = permsOf(s, m, u.role);
  }
}
function audit(code, by, act, target, detail) {
  const s = data.servers[code];
  s.audit.push({ ts: Date.now(), by: by.name, act, target: target || '', detail: detail || '' });
  if (s.audit.length > 300) s.audit.splice(0, s.audit.length - 300);
  save(code);
}

// ---- Birlikte izleme / müzik sırası ----
// Her ses kanalının ortak bir YouTube oynatıcısı vardır (bellekte durur, kanal boşalınca silinir).
// Durum: { cur: { qid, vid, title, by } | null, queue: [aynı], playing, pos (saniye), at (pos'un geçerli olduğu an, ms) }
// Görüntü/ses sunucudan geçmez: herkes videoyu kendi bilgisayarında YouTube'dan oynatır, sunucu sadece "ne, nerede" bilgisini eşitler.
const medias = new Map(); // 'kod\nkanal' -> durum
const mkey = (code, ch) => code + '\n' + ch;
function ytId(url) {
  const m = /(?:youtube\.com\/(?:watch\?(?:.*&)?v=|shorts\/|embed\/|live\/)|youtu\.be\/)([A-Za-z0-9_-]{11})/.exec(String(url || ''));
  return m ? m[1] : /^[A-Za-z0-9_-]{11}$/.test(String(url || '').trim()) ? String(url).trim() : null;
}
// --- İsimle arama ---
// İki yol var:
//  1) YT_API_KEY tanımlıysa YouTube'un resmi arama servisi (ücretsiz anahtar; günde yaklaşık 100 arama hakkı verir).
//  2) Anahtar yoksa YouTube'un arama sayfası okunur. Anahtar gerektirmez ama YouTube sayfayı değiştirirse ya da
//     barındırma servisinin adresini engellerse çalışmayabilir; o zaman link yapıştırmak her zaman çalışır.
const searchCache = new Map(); // sorgu -> { ts, list }
const titleCache = new Map();  // video kimliği -> başlık (aramadan eklenenlerde başlık hemen görünsün)
const clockText = (sec) => { sec = Math.max(0, Math.floor(sec)); const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = sec % 60; return (h ? h + ':' + String(m).padStart(2, '0') : m) + ':' + String(s).padStart(2, '0'); };
// Arama sayfasındaki gömülü veriden (ytInitialData) videoları çıkarır
function parseYtSearch(html) {
  const m = /(?:var ytInitialData|window\["ytInitialData"\])\s*=\s*(\{.+?\})\s*;\s*<\/script>/s.exec(String(html));
  if (!m) return null;
  let root; try { root = JSON.parse(m[1]); } catch { return null; }
  const out = [], seen = new Set(), text = (x) => (x && (x.simpleText || (x.runs || []).map((r) => r.text).join(''))) || '';
  (function walk(o, depth) {
    if (!o || typeof o !== 'object' || depth > 40 || out.length >= 10) return;
    if (o.videoRenderer && /^[A-Za-z0-9_-]{11}$/.test(o.videoRenderer.videoId || '') && !seen.has(o.videoRenderer.videoId)) {
      const v = o.videoRenderer, title = cleanName(text(v.title), 120);
      if (title) { seen.add(v.videoId); out.push({ vid: v.videoId, title, channel: cleanName(text(v.ownerText || v.longBylineText), 60), dur: cleanName(text(v.lengthText), 12) }); }
      return;
    }
    for (const k of Array.isArray(o) ? o.keys() : Object.keys(o)) walk(o[k], depth + 1);
  })(root, 0);
  return out;
}
function isoDur(s) { const m = /^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/.exec(s || ''); return m ? clockText((+m[1] || 0) * 3600 + (+m[2] || 0) * 60 + (+m[3] || 0)) : ''; }
async function ytSearch(q) {
  const key = lower(q), hit = searchCache.get(key);
  if (hit && Date.now() - hit.ts < 600000) return hit.list;
  const base = process.env.KULUBE_YT_BASE || ''; // sadece testler: sahte YouTube adresi
  let list = null;
  if (process.env.YT_API_KEY) {
    const api = (base || 'https://www.googleapis.com') + '/youtube/v3/';
    const r = await fetch(api + 'search?part=snippet&type=video&maxResults=8&videoEmbeddable=true&q=' + encodeURIComponent(q) + '&key=' + encodeURIComponent(process.env.YT_API_KEY), { signal: AbortSignal.timeout(6000) });
    if (!r.ok) throw new Error('YouTube araması yanıt vermedi (' + r.status + '). Anahtarın günlük hakkı dolmuş olabilir; link yapıştırmayı dene.');
    list = ((await r.json()).items || []).filter((x) => x.id && /^[A-Za-z0-9_-]{11}$/.test(x.id.videoId || ''))
      .map((x) => ({ vid: x.id.videoId, title: cleanName(decodeEnt(x.snippet.title), 120), channel: cleanName(decodeEnt(x.snippet.channelTitle), 60), dur: '' }));
    try { // süreler ayrı bir istekle gelir; gelmezse süresiz gösterilir
      const d = await fetch(api + 'videos?part=contentDetails&id=' + list.map((x) => x.vid).join(',') + '&key=' + encodeURIComponent(process.env.YT_API_KEY), { signal: AbortSignal.timeout(4000) });
      if (d.ok) for (const it of (await d.json()).items || []) { const x = list.find((y) => y.vid === it.id); if (x) x.dur = isoDur(it.contentDetails && it.contentDetails.duration); }
    } catch {}
  } else {
    const r = await fetch((base || 'https://www.youtube.com') + '/results?sp=EgIQAQ%253D%253D&hl=tr&search_query=' + encodeURIComponent(q), { signal: AbortSignal.timeout(8000),
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36', 'Accept-Language': 'tr-TR,tr;q=0.9,en;q=0.5', Cookie: 'SOCS=CAI; CONSENT=YES+1' } });
    if (!r.ok) throw new Error('YouTube araması yanıt vermedi (' + r.status + '). Link yapıştırmayı dene.');
    list = parseYtSearch(await r.text());
    if (!list) throw new Error('YouTube arama sonucu okunamadı. Link yapıştırmayı dene.');
    list = list.slice(0, 8);
  }
  for (const x of list) titleCache.set(x.vid, x.title);
  if (titleCache.size > 2000) for (const k of [...titleCache.keys()].slice(0, 500)) titleCache.delete(k);
  searchCache.set(key, { ts: Date.now(), list });
  if (searchCache.size > 300) searchCache.delete(searchCache.keys().next().value);
  return list;
}
const lower = (t) => String(t).toLocaleLowerCase('tr');

async function ytTitle(vid) {
  if (titleCache.has(vid)) return titleCache.get(vid);
  if (process.env.KULUBE_NO_YT === '1') return null; // testler internete çıkmasın
  try {
    const r = await fetch('https://www.youtube.com/oembed?format=json&url=' + encodeURIComponent('https://www.youtube.com/watch?v=' + vid), { signal: AbortSignal.timeout(4000) });
    if (!r.ok) return null;
    return cleanName((await r.json()).title, 120) || null;
  } catch { return null; }
}
function mediaPos(st) { return st.playing ? st.pos + (Date.now() - st.at) / 1000 : st.pos; }
function mediaSend(code, ch) {
  const st = medias.get(mkey(code, ch)) || null;
  for (const u of users.values()) if (u.server === code && u.voice === ch) io.to(u.id).emit('media', { channel: ch, state: st, now: Date.now() });
}
function mediaNext(code, ch) {
  const st = medias.get(mkey(code, ch)); if (!st) return;
  st.cur = st.queue.shift() || null; st.pos = 0; st.at = Date.now(); st.playing = !!st.cur;
}
// Kanal boşaldıysa (ya da silindiyse) oynatıcıyı kaldır
function mediaGc(code, ch) {
  if (!ch || !medias.has(mkey(code, ch))) return;
  for (const u of users.values()) if (u.server === code && u.voice === ch) return;
  medias.delete(mkey(code, ch));
}

// ---- Çok izleyicili yayın: zincirleme aktarım planı ----
// Yayıncı görüntüyü en fazla ROOT izleyiciye kendisi gönderir; diğer izleyicilere, görüntüyü zaten alan izleyiciler
// (her biri en fazla FAN kişiye) aktarır. Böylece yayıncının yükleme hızı izleyici sayısıyla artmaz.
// Sıra ses kanalına giriş zamanına göredir (önce gelen, aktarıcı olmaya daha yakın; plan girip çıkmalarda az değişir).
// Sonuç: relay[yayıncıId] = { izleyiciId: görüntüyü kimden alacağı }
const ROOT = Math.max(1, Number(process.env.KULUBE_FANOUT_ROOT) || 3);
const FAN = Math.max(1, Number(process.env.KULUBE_FANOUT) || 2);
const voiceTs = new Map(); // socket.id -> ses kanalına giriş zamanı
function relayPlan(online) {
  const plan = {};
  for (const s of online) {
    if (!s.screen || !s.voice) continue;
    const viewers = online.filter((u) => u.voice === s.voice && u.id !== s.id)
      .sort((a, b) => (voiceTs.get(a.id) || 0) - (voiceTs.get(b.id) || 0) || (a.id < b.id ? -1 : 1));
    const parents = {}, queue = [{ id: s.id, cap: ROOT }];
    let qi = 0;
    for (const v of viewers) {
      while (queue[qi].cap === 0) qi++;
      parents[v.id] = queue[qi].id; queue[qi].cap--;
      queue.push({ id: v.id, cap: FAN });
    }
    plan[s.id] = parents;
  }
  return plan;
}

function publicState(code) {
  const s = data.servers[code];
  const online = [...users.values()].filter((u) => u.server === code);
  const here = new Set(online.map((u) => u.uid));
  return {
    name: s.name, textChannels: s.textChannels, voiceChannels: s.voiceChannels, perms: s.perms,
    roles: s.roles, roleNames: s.roleNames,
    sounds: s.sounds.map((x) => ({ id: x.id, name: x.name, uid: x.uid })),
    relay: relayPlan(online),
    // chMuted: bulunduğu ses kanalında sadece yetkililer konuşabiliyor
    users: online.map((u) => ({ ...u, chMuted: !!u.voice && s.perms['v:' + u.voice] === 'staff' && !can(u, 'locked') })),
    // çevrimdışı üyeler (en son görülen 100 kişi)
    members: Object.entries(s.members).filter(([uid]) => !here.has(uid) && !s.bans[uid])
      .sort((a, b) => (b[1].seen || 0) - (a[1].seen || 0)).slice(0, 100)
      .map(([uid, m]) => ({ uid, name: m.name, color: colorOf(s, m, m.name), role: uid === s.owner ? 'owner' : roleOf(s, m, false), roles: m.roles || [], perms: permsOf(s, m, uid === s.owner ? 'owner' : roleOf(s, m, false)), av: m.av || null })),
  };
}
function broadcastState(code) {
  io.to(room(code)).emit('state', publicState(code));
}

// Girişte isim + davet kodu (hangi sunucu) kontrolü
io.use((socket, next) => {
  const auth = socket.handshake.auth || {};
  const n = cleanName(auth.name, 20);
  const code = String(auth.code || '').trim().toLowerCase();
  if (!n) return next(new Error('Bir isim yazmalısın.'));
  const s = data.servers[code];
  if (!s) return next(new Error('Sunucu bulunamadı. Davet kodu yanlış ya da sunucu artık yok.'));
  // Kalıcı kimlik: cihazda duran gizli anahtarın özeti. Anahtar yoksa (eski istemci) geçici misafir kimliği.
  const key = typeof auth.key === 'string' && auth.key.length >= 16 && auth.key.length <= 100 ? auth.key : null;
  socket.data.uid = key ? crypto.createHash('sha256').update(key).digest('hex').slice(0, 16) : 'g' + crypto.randomBytes(6).toString('hex');
  socket.data.guest = !key;
  norm(s);
  socket.data.admin = typeof auth.adminKey === 'string' && auth.adminKey === s.adminKey;
  if (socket.data.admin) {
    // Kurucu kendi sunucusundan yasaklanamaz: çevrimdışıyken konmuş bir yasak varsa kalkar, kimliği kaydedilir
    if (key) { if (s.bans[socket.data.uid]) { delete s.bans[socket.data.uid]; save(code); } s.owner = socket.data.uid; }
  } else if (s.bans[socket.data.uid]) return next(new Error('Bu sunucudan yasaklandın.'));
  socket.data.name = n;
  socket.data.code = code;
  next();
});

io.on('connection', (socket) => {
  const code = socket.data.code;
  const srv = data.servers[code];
  // deaf: kulaklığını kendi kapattı. forceMuted / forceDeaf: yönetici kapattı (kişi kendisi açamaz).
  // screen / cam: yayın ya da kamera açıksa o akışın kimliği (izleyiciler hangi akışın ne olduğunu buradan anlar)
  // role: owner (sunucuyu kuran, yönetici anahtarı olan) | mod (yardımcı yönetici) | member
  const uid = socket.data.uid, guest = socket.data.guest;
  let member = null;
  if (!guest) {
    member = (srv.members[uid] ||= { name: socket.data.name });
    member.name = socket.data.name; member.seen = Date.now();
    save(code);
  }
  // activity: "şu an oynuyor" yazısı.  rec: ses kaydı yapıyor (kanaldakiler görsün diye).  roles: özel rol kimlikleri
  const user = { id: socket.id, uid, server: code, name: socket.data.name, color: colorOf(srv, member, socket.data.name), admin: socket.data.admin,
    role: roleOf(srv, member, socket.data.admin), roles: (member && member.roles) || [], perms: permsOf(srv, member, roleOf(srv, member, socket.data.admin)), av: (member && member.av) || null, status: 'online', activity: '',
    voice: null, muted: false, deaf: false, forceMuted: false, forceDeaf: false, screen: null, screenQ: null, cam: null, rec: false };
  users.set(socket.id, user);
  const token = crypto.randomBytes(16).toString('hex');
  tokens.set(token, socket.id);
  socket.join(room(code));
  socket.emit('init', { me: user, code, upToken: token, maxFile: MAX_FILE, ...publicState(code) });
  socket.to(room(code)).emit('state', publicState(code));
  const update = () => broadcastState(code);
  const notice = (text) => socket.emit('notice', text);
  const socketsOf = (u) => [...users.values()].filter((x) => x.server === code && x.uid === u);
  const saveCh = (c) => { save(code); if (c.dm) save(GLOBAL); }; // özel mesajlar ortak belgede durur
  const everywhere = (u) => [...users.values()].filter((x) => x.uid === u); // hangi sunucuda olursa olsun o kişinin bağlantıları

  // --- Arkadaşlar (sunuculardan bağımsız) ---
  const G = data.global;
  const fkey = (a, b) => [a, b].sort().join(':');
  const friendOk = (other) => { const r = G.friends[fkey(uid, other)]; return !!(r && r.ok); };
  const myFriendKeys = () => Object.keys(G.friends).filter((k) => k.split(':').includes(uid));
  // Arkadaş listesi değişti (ya da biri çevrimiçi/çevrimdışı oldu): ilgili kişilerin açık pencerelerine haber ver
  const pushFriends = (who) => { for (const u of everywhere(who)) io.to(u.id).emit('friends'); };
  const pushPresence = () => { for (const k of myFriendKeys()) if (G.friends[k].ok) pushFriends(k.split(':').find((x) => x !== uid)); };
  if (!guest) {
    const first = everywhere(uid).length === 1; // bu kişinin ilk bağlantısı: arkadaşlarına "çevrimiçi" görünsün
    G.users[uid] = { name: socket.data.name, seen: Date.now() }; save(GLOBAL);
    if (first) pushPresence();
  }
  socket.on('friends', (cb) => {
    if (typeof cb !== 'function') return;
    cb(guest ? [] : myFriendKeys().map((k) => {
      const other = k.split(':').find((x) => x !== uid), r = G.friends[k], on = everywhere(other);
      return { uid: other, name: on.length ? on[0].name : (G.users[other] || {}).name || 'Bilinmeyen', state: r.ok ? 'ok' : r.from === uid ? 'out' : 'in', online: !!r.ok && on.length > 0 };
    }));
  });
  // Arkadaş ekle: kişinin koduyla (kod = kimliği; profilde görünür) ya da aynı sunucudaysa isminin üstünden. Karşı taraf zaten istek yolladıysa kabul sayılır.
  socket.on('friendAdd', ({ uid: who } = {}, cb) => {
    const done = (err) => { if (typeof cb === 'function') cb(err || null); };
    who = String(who || '').toLowerCase().replace(/[^0-9a-f]/g, '');
    if (guest) return done('Bu cihazda kimliğin yok; sayfayı yenileyip tekrar dene.');
    if (who === uid) return done('Kendini ekleyemezsin.');
    if (who.length !== 16 || !G.users[who]) return done('Bu kodla kimse bulunamadı. Kodu arkadaşından yeniden iste.');
    const k = fkey(uid, who), r = G.friends[k];
    if (r && r.ok) return done('Zaten arkadaşsınız.');
    if (r && r.from === uid) return done('İstek zaten gönderildi, kabul etmesini bekle.');
    if (r) r.ok = true; // o bana istek yollamıştı: kabul
    else {
      if (myFriendKeys().length >= 200) return done('Arkadaş listen dolu (en fazla 200).');
      G.friends[k] = { from: uid, ts: Date.now() };
    }
    save(GLOBAL); pushFriends(uid); pushFriends(who); done();
  });
  socket.on('friendAccept', ({ uid: who } = {}) => {
    const r = G.friends[fkey(uid, String(who))];
    if (guest || !r || r.ok || r.from === uid) return;
    r.ok = true; save(GLOBAL); pushFriends(uid); pushFriends(who);
  });
  // Arkadaşlıktan çıkar / isteği reddet / gönderdiğim isteği geri al
  socket.on('friendRemove', ({ uid: who } = {}) => {
    const k = fkey(uid, String(who));
    if (guest || !G.friends[k]) return;
    delete G.friends[k]; save(GLOBAL); pushFriends(uid); pushFriends(who);
  });

  socket.on('inviteBase', (cb) => {
    if (typeof cb === 'function') cb(publicUrl);
  });
  socket.on('ice', async (cb) => { if (typeof cb === 'function') cb(await getIce()); });
  socket.on('pingme', (cb) => { if (typeof cb === 'function') cb(Date.now()); }); // bağlantı testi: gidiş-dönüş süresi
  const log = (act, target, detail) => audit(code, user, act, target, detail);

  // --- Yazılı sohbet ---
  // Kanal ya bir yazı kanalıdır ya da "@uid" biçiminde bir özel mesajdır (iki kişi arasında).
  // Döner: { list, send(olay, veri), dm }  — send, veriye alıcının göreceği kanal adını ekler.
  function chanOf(channel) {
    channel = String(channel || '');
    if (channel[0] === '@') {
      const other = channel.slice(1);
      const key = [uid, other].sort().join(':');
      // Özel mesaj sunucudan bağımsızdır. Yazabilmek için: arkadaş olmak, aynı sunucuda olmak ya da daha önce yazışmış olmak.
      if (guest || other === uid || !(friendOk(other) || srv.members[other] || (G.dms[key] || []).length)) return null;
      return { dm: true, key, list: G.dms[key] || [], send: (ev, payload) => {
        for (const u of everywhere(uid)) io.to(u.id).emit(ev, { ...payload, channel: '@' + other });
        for (const u of everywhere(other)) io.to(u.id).emit(ev, { ...payload, channel: '@' + uid });
      } };
    }
    if (!srv.textChannels.includes(channel)) return null;
    return { dm: false, list: (srv.messages[channel] ||= []), send: (ev, payload) => io.to(room(code)).emit(ev, { ...payload, channel }) };
  }
  const canWrite = (channel) => srv.perms['t:' + channel] !== 'staff' || can(user, 'locked');

  socket.on('history', (channel, cb) => {
    if (typeof cb !== 'function') return;
    const c = chanOf(channel);
    cb(c ? c.list : []);
  });

  // Özel mesajlaştığım kişiler
  socket.on('dmList', (cb) => {
    if (typeof cb !== 'function') return;
    const out = [];
    for (const [key, list] of Object.entries(G.dms)) {
      const [a, b] = key.split(':');
      if ((a !== uid && b !== uid) || !list.length) continue;
      const other = a === uid ? b : a, m = srv.members[other] || G.users[other];
      out.push({ uid: other, name: m ? m.name : 'Bilinmeyen', ts: list[list.length - 1].ts });
    }
    cb(out.sort((x, y) => y.ts - x.ts));
  });

  let burst = [];
  socket.on('message', ({ channel, text, replyTo, file } = {}) => {
    text = String(text || '').trim().slice(0, 2000);
    const c = chanOf(channel);
    if (!c || (!text && !file)) return;
    if (!c.dm && !canWrite(channel)) return notice('Bu kanala sadece yetkililer yazabilir.');
    const now = Date.now(); burst = burst.filter((t) => now - t < 5000);
    if (burst.length >= 10) return notice('Çok hızlı yazıyorsun, biraz yavaşla.');
    burst.push(now);
    const msg = { id: crypto.randomUUID(), channel: c.dm ? undefined : channel, uid, name: user.name, color: user.color, text, ts: now };
    if (file) { // önceden yüklenmiş, bu kişiye ait ve henüz kullanılmamış dosya
      const f = fileIndex.get(String(file.id));
      if (!f || f.code !== code || f.meta.uid !== uid || f.meta.used) return;
      f.meta.used = true;
      const dim = (n) => (Number.isFinite(n) && n > 0 && n < 20000 ? Math.round(n) : undefined);
      msg.file = { id: f.meta.id, name: f.meta.name, type: f.meta.type, size: f.meta.size, w: dim(file.w), h: dim(file.h) };
    }
    const r = replyTo && c.list.find((m) => m.id === replyTo);
    if (r) msg.reply = { id: r.id, name: r.name, text: r.text ? r.text.slice(0, 120) : r.file ? '📎 ' + r.file.name : '' };
    if (c.dm) G.dms[c.key] = c.list;
    c.list.push(msg);
    while (c.list.length > MAX_HISTORY) { const old = c.list.shift(); if (old.file) delFile(old.file.id); }
    saveCh(c);
    c.send('message', msg);
    addPreview(c, msg);
  });

  // Mesajdaki ilk linkin önizlemesi hazır olunca mesaja eklenir ve herkese güncelleme gider
  function addPreview(c, msg) {
    const url = firstUrl(msg.text);
    if (!url) return;
    preview(url).then((p) => {
      const m = c.list.find((x) => x.id === msg.id);
      if (!p || !m || firstUrl(m.text) !== url) return;
      m.preview = p;
      if (p.image) imgAllow.add(p.image);
      saveCh(c); c.send('msgUpdate', { msg: m });
    });
  }

  socket.on('msgEdit', ({ channel, id, text } = {}) => {
    const c = chanOf(channel), m = c && c.list.find((x) => x.id === id);
    text = String(text || '').trim().slice(0, 2000);
    if (!m || guest || m.uid !== uid || !text) return;
    const before = firstUrl(m.text);
    m.text = text; m.edited = Date.now();
    if (firstUrl(text) !== before) { delete m.preview; addPreview(c, m); }
    saveCh(c); c.send('msgUpdate', { msg: m });
  });

  // --- Mesaj arama: tüm yazı kanalları + kendi özel mesajlarım (en yeni önce, en fazla 50) ---
  socket.on('search', ({ q } = {}, cb) => {
    if (typeof cb !== 'function') return;
    q = String(q || '').trim().toLocaleLowerCase('tr').slice(0, 100);
    if (q.length < 2) return cb([]);
    const out = [];
    const scan = (list, chan) => {
      for (const m of list) {
        const hay = ((m.text || '') + ' ' + (m.file ? m.file.name : '')).toLocaleLowerCase('tr');
        if (hay.includes(q)) out.push({ channel: chan, id: m.id, uid: m.uid, name: m.name, color: m.color, ts: m.ts, text: (m.text || (m.file ? '📎 ' + m.file.name : '')).slice(0, 300) });
      }
    };
    for (const ch of srv.textChannels) scan(srv.messages[ch] || [], ch);
    if (!guest) for (const [key, list] of Object.entries(G.dms)) { const [a, b] = key.split(':'); if (a === uid || b === uid) scan(list, '@' + (a === uid ? b : a)); }
    cb(out.sort((x, y) => y.ts - x.ts).slice(0, 50));
  });

  // --- Ses efekt panosu ---
  // Hazır sesler istemcide üretilir ("b:ad"); özel sesler sunucuya yüklenmiş kısa ses dosyalarıdır (en fazla 1 MB, 30 ses).
  // Efekt ses akışına karıştırılmaz: aynı ses kanalındaki herkese "çal" mesajı gider, herkes kendi bilgisayarında çalar.
  socket.on('soundAdd', ({ id, name } = {}, cb) => {
    const done = (e) => { if (typeof cb === 'function') cb(e || null); };
    if (guest) return done('Misafirler ses ekleyemez.');
    const f = fileIndex.get(String(id || ''));
    if (!f || f.code !== code || f.meta.uid !== uid || f.meta.used) return done('Ses dosyası bulunamadı.');
    if (!/^audio\/(mpeg|ogg|wav|webm|mp4)$/.test(f.meta.type)) return done('Sadece MP3, OGG, WAV, WEBM ya da M4A ses dosyası eklenebilir.');
    if (f.meta.size > 1024 * 1024) return done('Ses en fazla 1 MB olabilir.');
    if (srv.sounds.length >= 30) return done('Panoda en fazla 30 ses olabilir; önce birini sil.');
    f.meta.used = true;
    srv.sounds.push({ id: f.meta.id, name: cleanName(name, 24) || f.meta.name.replace(/\.[^.]+$/, '').slice(0, 24), uid, ts: Date.now() });
    save(code); update(); done();
  });

  socket.on('soundDel', ({ id } = {}) => {
    const s = srv.sounds.find((x) => x.id === id);
    if (!s || !((!guest && s.uid === uid) || can(user, 'msgs'))) return;
    delFile(s.id); update();
  });

  let sfxLog = [];
  socket.on('sfx', (sound) => {
    sound = String(sound || '');
    if (!user.voice || user.forceMuted || (srv.perms['v:' + user.voice] === 'staff' && !can(user, 'locked'))) return;
    if (!/^b:[a-z]{2,12}$/.test(sound) && !srv.sounds.some((x) => x.id === sound)) return;
    const now = Date.now(); sfxLog = sfxLog.filter((t) => now - t < 20000);
    if (sfxLog.length && now - sfxLog[sfxLog.length - 1] < 1200) return;
    if (sfxLog.length >= 8) return notice('Çok sık efekt çalıyorsun, biraz bekle.');
    sfxLog.push(now);
    for (const u of users.values()) if (u.server === code && u.voice === user.voice) io.to(u.id).emit('sfx', { sound, from: user.id, name: user.name });
  });

  socket.on('msgDelete', ({ channel, id } = {}) => {
    const c = chanOf(channel), i = c ? c.list.findIndex((x) => x.id === id) : -1;
    if (i < 0) return;
    const m = c.list[i];
    if (!((!guest && m.uid === uid) || (!c.dm && can(user, 'msgs')))) return; // kendi mesajı ya da "mesaj siler" yetkisi
    if (m.uid !== uid) log('mesaj-sil', m.name, '#' + channel + ': ' + (m.text || (m.file ? '📎 ' + m.file.name : '')).slice(0, 80));
    c.list.splice(i, 1);
    if (m.file) delFile(m.file.id);
    saveCh(c); c.send('msgDelete', { id });
  });

  socket.on('msgReact', ({ channel, id, emoji } = {}) => {
    const c = chanOf(channel), m = c && c.list.find((x) => x.id === id);
    emoji = String(emoji || '');
    if (!m || emoji.length > 16 || !/\p{Extended_Pictographic}/u.test(emoji)) return;
    const r = (m.reactions ||= {}), who = (r[emoji] ||= []), at = who.indexOf(uid);
    if (at >= 0) who.splice(at, 1);
    else if (Object.keys(r).length <= 12 && who.length < 100) who.push(uid);
    if (!who.length) delete r[emoji];
    saveCh(c); c.send('msgUpdate', { msg: m });
  });

  socket.on('typing', (channel) => {
    const c = chanOf(channel);
    if (!c) return;
    if (!c.dm) socket.to(room(code)).emit('typing', { channel, name: user.name });
    else for (const u of everywhere(String(channel).slice(1))) io.to(u.id).emit('typing', { channel: '@' + uid, name: user.name });
  });

  socket.on('createChannel', ({ name, type } = {}) => {
    const list = type === 'voice' ? srv.voiceChannels : srv.textChannels;
    let n = cleanName(name, 24);
    if (type !== 'voice') n = n.toLowerCase().replace(/ /g, '-').replace(/^@+/, '');
    if (!n || list.includes(n) || list.length >= 30) return;
    list.push(n);
    log('kanal-ekle', (type === 'voice' ? '🔊 ' : '# ') + n);
    update();
  });

  // --- Kanal yönetimi (yetkililer): yeniden adlandırma, silme, yazma/konuşma izni ---
  const chanList = (type) => (type === 'voice' ? srv.voiceChannels : srv.textChannels);
  const pk = (type, name) => (type === 'voice' ? 'v:' : 't:') + name;

  socket.on('channelRename', ({ type, name, to } = {}) => {
    const list = chanList(type), i = list.indexOf(name);
    let n = cleanName(to, 24);
    if (type !== 'voice') n = n.toLowerCase().replace(/ /g, '-').replace(/^@+/, '');
    if (!can(user, 'channels') || i < 0 || !n || list.includes(n)) return;
    list[i] = n;
    if (srv.perms[pk(type, name)]) { srv.perms[pk(type, n)] = srv.perms[pk(type, name)]; delete srv.perms[pk(type, name)]; }
    if (type === 'voice') { for (const u of users.values()) if (u.server === code && u.voice === name) u.voice = n; }
    else if (srv.messages[name]) { srv.messages[n] = srv.messages[name]; delete srv.messages[name]; srv.messages[n].forEach((m) => (m.channel = n)); }
    if (type === 'voice' && medias.has(mkey(code, name))) { medias.set(mkey(code, n), medias.get(mkey(code, name))); medias.delete(mkey(code, name)); }
    log('kanal-ad', (type === 'voice' ? '🔊 ' : '# ') + name, n);
    io.to(room(code)).emit('channelRenamed', { type: type === 'voice' ? 'voice' : 'text', from: name, to: n });
    update();
  });

  socket.on('channelDelete', ({ type, name } = {}) => {
    const list = chanList(type), i = list.indexOf(name);
    if (!can(user, 'channels') || i < 0) return;
    if (type !== 'voice' && list.length <= 1) return notice('En az bir yazı kanalı kalmalı.');
    list.splice(i, 1);
    delete srv.perms[pk(type, name)];
    if (type === 'voice') { for (const u of users.values()) if (u.server === code && u.voice === name) { u.voice = null; u.screen = null; u.screenQ = null; u.cam = null; voiceTs.delete(u.id); } }
    else { (srv.messages[name] || []).forEach((m) => m.file && delFile(m.file.id)); delete srv.messages[name]; }
    if (type === 'voice') medias.delete(mkey(code, name));
    log('kanal-sil', (type === 'voice' ? '🔊 ' : '# ') + name); update();
  });

  socket.on('channelPerm', ({ type, name, mode } = {}) => {
    if (!can(user, 'channels') || !chanList(type).includes(name)) return;
    if (mode === 'staff') srv.perms[pk(type, name)] = 'staff'; else delete srv.perms[pk(type, name)];
    log('kanal-izin', (type === 'voice' ? '🔊 ' : '# ') + name, mode === 'staff' ? 'sadece yetkililer' : 'herkes'); update();
  });

  // --- Profil: resim ve durum ---
  socket.on('profile', ({ avatar, status, activity } = {}) => {
    if (['online', 'away', 'dnd'].includes(status)) user.status = status;
    if (typeof activity === 'string') user.activity = cleanName(activity, 40);
    if (avatar !== undefined && member) {
      if (avatar === null) { delete member.avatar; delete member.av; }
      else if (typeof avatar === 'string' && avatar.length <= 40000 && /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(avatar)) {
        member.avatar = avatar; member.av = crypto.createHash('sha1').update(avatar).digest('hex').slice(0, 10);
      } else return;
      for (const u of socketsOf(uid)) u.av = member.av || null;
      save(code);
    }
    update();
  });

  // --- Sesli sohbet (WebRTC sinyalleşmesi; ses ve yayın tarayıcılar arasında doğrudan akar) ---
  socket.on('voiceJoin', (channel, cb) => {
    if (!srv.voiceChannels.includes(channel)) return;
    const others = [...users.values()].filter((u) => u.server === code && u.voice === channel && u.id !== user.id).map((u) => u.id);
    const before = user.voice;
    user.voice = channel;
    user.muted = false;
    user.screen = null; user.screenQ = null; user.cam = null; user.rec = false;
    voiceTs.set(socket.id, Date.now());
    if (before && before !== channel) mediaGc(code, before);
    update();
    if (typeof cb === 'function') cb(others); // yeni gelen, odadakilerle bağlantıyı kendisi başlatır
    socket.emit('media', { channel, state: medias.get(mkey(code, channel)) || null, now: Date.now() });
  });

  socket.on('voiceLeave', () => {
    const before = user.voice;
    user.voice = null;
    user.muted = false;
    user.screen = null; user.screenQ = null; user.cam = null; user.rec = false;
    voiceTs.delete(socket.id);
    mediaGc(code, before);
    update();
  });

  // Ses kaydı başladı/bitti: kanaldaki herkes görsün (gizli kayıt olmasın)
  socket.on('voiceRec', (on) => {
    if (!user.voice || user.rec === !!on) return;
    user.rec = !!on;
    if (user.rec) for (const u of users.values()) if (u.server === code && u.voice === user.voice && u.id !== user.id) io.to(u.id).emit('notice', user.name + ' ses kaydı başlattı.');
    update();
  });

  // --- Birlikte izleme / müzik sırası (aynı ses kanalındakiler) ---
  let mediaBurst = [];
  const mediaOk = () => { const now = Date.now(); mediaBurst = mediaBurst.filter((t) => now - t < 10000); if (mediaBurst.length >= 20) return false; mediaBurst.push(now); return true; };
  socket.on('mediaAdd', async (url, cb) => {
    const done = (err) => { if (typeof cb === 'function') cb(err || null); };
    const ch = user.voice, vid = ytId(url);
    if (!ch) return done('Önce bir ses kanalına gir.');
    if (!vid) return done('Bu bir YouTube linki değil.');
    if (!mediaOk()) return done('Çok hızlı, biraz bekle.');
    let st = medias.get(mkey(code, ch));
    if (st && st.queue.length >= 50) return done('Sıra dolu (en fazla 50).');
    const item = { qid: crypto.randomBytes(6).toString('hex'), vid, title: titleCache.get(vid) || 'YouTube videosu', by: user.name };
    if (!st) medias.set(mkey(code, ch), (st = { cur: null, queue: [], playing: false, pos: 0, at: Date.now() }));
    st.queue.push(item);
    if (!st.cur) mediaNext(code, ch);
    mediaSend(code, ch); done();
    const title = await ytTitle(vid); // başlık sonradan gelir
    if (title && medias.get(mkey(code, ch)) === st && (st.cur === item || st.queue.includes(item))) { item.title = title; mediaSend(code, ch); }
  });
  // Şarkı/video adıyla arama: sonuçlardan biri seçilince mediaAdd ile sıraya girer
  let lastSearch = 0;
  socket.on('mediaSearch', async (q, cb) => {
    if (typeof cb !== 'function') return;
    q = cleanName(q, 100);
    if (!user.voice) return cb({ error: 'Önce bir ses kanalına gir.' });
    if (q.length < 2) return cb({ error: 'Aramak için en az 2 harf yaz.' });
    if (Date.now() - lastSearch < 1500) return cb({ error: 'Çok hızlı, bir saniye bekle.' });
    lastSearch = Date.now();
    try { cb({ list: await ytSearch(q) }); }
    catch (e) { cb({ error: /YouTube/.test(e.message) ? e.message : 'YouTube aramasına ulaşılamadı. Link yapıştırmayı dene.' }); }
  });
  socket.on('mediaCtl', ({ a, pos, qid } = {}) => {
    const ch = user.voice, st = ch && medias.get(mkey(code, ch));
    if (!st || !mediaOk()) return;
    if (a === 'play' && st.cur && !st.playing) { st.playing = true; st.at = Date.now(); }
    else if (a === 'pause' && st.playing) { st.pos = mediaPos(st); st.playing = false; st.at = Date.now(); }
    else if (a === 'seek' && st.cur && Number.isFinite(pos) && pos >= 0 && pos < 86400) { st.pos = pos; st.at = Date.now(); }
    else if (a === 'next') mediaNext(code, ch);
    else if (a === 'remove') st.queue = st.queue.filter((x) => x.qid !== qid);
    else if (a === 'stop') { st.cur = null; st.queue = []; st.playing = false; st.pos = 0; }
    else return;
    mediaSend(code, ch);
  });
  // Video bitti (ya da oynatılamıyor): ilk bildiren sıradakine geçirir
  socket.on('mediaEnded', (qid) => {
    const ch = user.voice, st = ch && medias.get(mkey(code, ch));
    if (!st || !st.cur || st.cur.qid !== qid) return;
    mediaNext(code, ch); mediaSend(code, ch);
  });

  socket.on('voiceMute', (muted) => { user.muted = !!muted; update(); });
  socket.on('voiceDeaf', (deaf) => { user.deaf = !!deaf; update(); });

  socket.on('voiceScreen', (streamId, q) => {
    if (!user.voice) return;
    user.screen = streamId ? String(streamId).slice(0, 100) : null;
    user.screenQ = user.screen ? (/^(720p30|720p60|1080p30|1080p60)$/.test(q) ? q : '1080p30') : null; // aktaranlar aynı kaliteyle gönderir
    update();
  });

  socket.on('voiceCam', (streamId) => {
    if (!user.voice) return;
    user.cam = streamId ? String(streamId).slice(0, 100) : null;
    update();
  });

  socket.on('signal', ({ to, data: payload } = {}) => {
    const target = users.get(to);
    if (!target || target.server !== code || !user.voice || target.voice !== user.voice) return;
    io.to(to).emit('signal', { from: user.id, data: payload });
  });

  // --- Yetkili işlemleri ---
  // Kurucu (owner) her şeyi yapar. Yardımcı yönetici (mod): isim değiştirme, susturma, atma, yasaklama, mesaj silme, kanal yönetimi.
  // Yardımcı yönetici kurucuya ve diğer yardımcılara dokunamaz; rol vermek ve sunucu adını değiştirmek sadece kurucuya aittir.
  const outranks = (t) => t.id === user.id || rankOf(user) > rankOf(t);
  // Hedef: aynı sunucuda, benden alt sırada (ya da kendim) ve bu işlem için yetkim var
  const targetOf = (id, perm) => { const t = users.get(id); return t && t.server === code && can(user, perm) && outranks(t) ? t : null; };

  socket.on('adminRename', ({ id, name } = {}) => {
    const target = targetOf(id, 'rename'), n = cleanName(name, 20);
    if (!target || !n) return;
    const old = target.name;
    target.name = n; target.color = colorOf(srv, srv.members[target.uid], n);
    if (srv.members[target.uid]) srv.members[target.uid].name = n;
    if (old !== n) log('isim', old, n);
    update();
  });

  socket.on('adminSet', ({ id, forceMuted, forceDeaf } = {}) => {
    const target = targetOf(id, 'mute');
    if (!target) return;
    if (typeof forceMuted === 'boolean' && target.forceMuted !== forceMuted) { target.forceMuted = forceMuted; log(forceMuted ? 'sustur' : 'sustur-kaldir', target.name); }
    if (typeof forceDeaf === 'boolean' && target.forceDeaf !== forceDeaf) { target.forceDeaf = forceDeaf; log(forceDeaf ? 'sagir' : 'sagir-kaldir', target.name); }
    update();
  });

  function throwOut(t, why) {
    io.to(t.id).emit('kicked', why);
    const s = io.sockets.sockets.get(t.id);
    if (s) s.disconnect(true);
  }
  // Sesteki birini başka ses kanalına taşı (ses bağlantısını kişinin kendi uygulaması kurar; ona "şuraya geç" denir)
  socket.on('voiceMove', ({ id, channel } = {}) => {
    const target = targetOf(id, 'move');
    if (!target || !target.voice || target.voice === channel || !srv.voiceChannels.includes(channel)) return;
    log('tasi', target.name, '🔊 ' + channel);
    io.to(target.id).emit('moved', { channel, by: user.name });
  });

  socket.on('kick', ({ id } = {}) => {
    const target = targetOf(id, 'kick');
    if (!target || target.id === user.id) return;
    log('at', target.name);
    throwOut(target, 'kick');
  });

  socket.on('ban', ({ uid: who } = {}) => {
    who = String(who || '');
    if (!can(user, 'ban') || who === uid || who[0] === 'g' || who === srv.owner) return; // kurucu çevrimdışıyken de yasaklanamaz
    const online = socketsOf(who), m = srv.members[who];
    if (!m && !online.length) return;
    if (online.some((t) => !outranks(t))) return;
    if (m) { const mr = roleOf(srv, m, false); if (rankOf(user) <= rankOf({ role: mr, perms: permsOf(srv, m, mr) })) return; } // çevrimdışı üyede de sıra geçerli
    srv.bans[who] = { name: (m && m.name) || online[0].name, ts: Date.now() };
    if (m) { delete m.role; delete m.roles; }
    log('yasakla', srv.bans[who].name);
    online.forEach((t) => throwOut(t, 'ban'));
    update();
  });

  socket.on('unban', ({ uid: who } = {}) => {
    if (!can(user, 'ban') || !srv.bans[who]) return;
    log('yasak-kaldir', srv.bans[who].name);
    delete srv.bans[who];
    update();
  });

  socket.on('bans', (cb) => {
    if (typeof cb === 'function') cb(can(user, 'ban') ? Object.entries(srv.bans).map(([u, b]) => ({ uid: u, name: b.name, ts: b.ts })) : []);
  });

  socket.on('setRole', ({ uid: who, role } = {}) => {
    const m = srv.members[who];
    if (user.role !== 'owner' || !m || who === uid) return;
    if (role === 'mod') m.role = 'mod'; else delete m.role;
    refreshRoles(code);
    log(role === 'mod' ? 'yardimci-yap' : 'yardimci-al', m.name); update();
  });

  // --- Özel roller (sadece kurucu): ekle/düzenle, sil, hazır rollerin adını değiştir, kişiye rol ver ---
  socket.on('roleSave', ({ id, name, color, staff, perms } = {}) => {
    perms = Array.isArray(perms) ? PERMS.filter((p) => perms.includes(p)) : staff ? [...PERMS] : []; // staff: eski istemci ("yetkili" kutusu = hepsi)
    const n = cleanName(name, 20);
    if (user.role !== 'owner' || !n) return;
    const c = /^#[0-9a-f]{6}$/i.test(color) ? color.toLowerCase() : '';
    let r = srv.roles.find((x) => x.id === id);
    if (!r) {
      if (srv.roles.length >= 20) return notice('En fazla 20 özel rol olabilir.');
      srv.roles.push((r = { id: crypto.randomBytes(4).toString('hex'), perms: [] }));
      log('rol-ekle', n);
    } else if (r.name !== n || r.color !== c || r.perms.join() !== perms.join()) log('rol-duzenle', r.name, n + (perms.length ? ' (yetkiler: ' + perms.map((x) => PERM_TR[x]).join(', ') + ')' : ' (yetkisiz)'));
    r.name = n; r.color = c; r.perms = perms;
    refreshRoles(code); save(code); update();
  });
  socket.on('roleDelete', ({ id } = {}) => {
    const r = srv.roles.find((x) => x.id === id);
    if (user.role !== 'owner' || !r) return;
    srv.roles = srv.roles.filter((x) => x !== r);
    for (const m of Object.values(srv.members)) if (m.roles) { m.roles = m.roles.filter((x) => x !== id); if (!m.roles.length) delete m.roles; }
    refreshRoles(code); log('rol-sil', r.name); update();
  });
  socket.on('roleNames', (names = {}) => {
    if (user.role !== 'owner') return;
    for (const k of ['owner', 'mod', 'member']) { const n = cleanName(names && names[k], 20); if (n) srv.roleNames[k] = n; else delete srv.roleNames[k]; }
    log('rol-adlari', '', ['owner', 'mod', 'member'].map((k) => srv.roleNames[k]).filter(Boolean).join(', ')); update();
  });
  socket.on('memberRoles', ({ uid: who, roles } = {}) => {
    const m = srv.members[who];
    if (user.role !== 'owner' || !m || !Array.isArray(roles)) return;
    const ids = srv.roles.map((r) => r.id).filter((id) => roles.includes(id)); // tanımlı sırayla, sadece var olanlar
    const names = (list) => (list || []).map((id) => (srv.roles.find((r) => r.id === id) || {}).name).filter(Boolean).join(', ') || 'yok';
    const was = names(m.roles);
    if (ids.length) m.roles = ids; else delete m.roles;
    refreshRoles(code);
    if (was !== names(ids)) log('rol-ver', m.name, names(ids));
    save(code); update();
  });

  socket.on('audit', (cb) => { if (typeof cb === 'function') cb(can(user, 'audit') ? srv.audit.slice(-300).reverse() : []); });

  socket.on('serverRename', (name) => {
    const n = cleanName(name, 30);
    if (!user.admin || !n) return;
    if (srv.name !== n) log('sunucu-ad', srv.name, n);
    srv.name = n;
    save(code);
    update();
  });

  socket.on('disconnect', () => {
    users.delete(socket.id); tokens.delete(token); voiceTs.delete(socket.id);
    mediaGc(code, user.voice);
    if (!guest) { if (G.users[uid]) G.users[uid].seen = Date.now(); if (!everywhere(uid).length) pushPresence(); }
    if (member) { member.seen = Date.now(); save(code); }
    update();
  });
});

// Port doluysa boş bir porta geçer. Masaüstü uygulaması `ready` ile portu ve (varsa) ilk sunucuyu öğrenir.
const ready = new Promise((resolve, reject) => {
  server.on('error', (e) => {
    if (e.code === 'EADDRINUSE') server.listen(0);
    else reject(e);
  });
  server.on('listening', () => {
    const port = server.address().port;
    const first = Object.keys(data.servers)[0];
    console.log('');
    console.log('  Kulube calisiyor:  http://localhost:' + port);
    if (process.env.NODE_ENV !== 'production') {
      for (const [code, s] of Object.entries(data.servers).slice(0, 10)) {
        console.log('  Sunucu "' + s.name + '"  davet kodu: ' + code);
        console.log('    yonetici girisi (kimseyle paylasma): http://localhost:' + port + '/#' + code + '~' + s.adminKey);
      }
    }
    console.log('');
    resolve({ port, first: first ? { code: first, adminKey: data.servers[first].adminKey } : null });
  });
  loadData().then(() => server.listen(PORT), reject); // veri yüklenmeden bağlantı kabul etme
});

// ---- Uyanık tutma ----
// Ücretsiz barındırma (Render) 15 dakika istek gelmezse servisi uyutur; uyanması ~1 dakika sürer.
// Kendi genel adresimize 10 dakikada bir küçük bir istek atarak uyumasını önleriz. Render bu adresi RENDER_EXTERNAL_URL ile verir;
// başka bir yerde KEEPALIVE_URL ile verilebilir. Kapatmak için KEEPALIVE=0.
const SELF_URL = String(process.env.KEEPALIVE_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/+$/, '');
if (/^https?:\/\//.test(SELF_URL) && process.env.KEEPALIVE !== '0') {
  const every = Math.max(1000, Number(process.env.KEEPALIVE_MS) || 10 * 60 * 1000);
  ready.then(() => {
    setInterval(() => { fetch(SELF_URL + '/healthz', { signal: AbortSignal.timeout(20000) }).catch(() => {}); }, every).unref();
    console.log('  Uyanik tutma acik: ' + SELF_URL + '/healthz (' + Math.round(every / 60000) + ' dk arayla)');
  }, () => {});
}

if (require.main === module) {
  ready.catch((e) => { console.error('Baslatilamadi:', e.message); process.exit(1); });
  // Barındırma servisi kapatırken bekleyen kayıtları yaz
  // Kapatılırken bekleyen kayıtları yaz: barındırma servisi (SIGTERM), Ctrl+C (SIGINT), Windows'ta konsol penceresinin kapatılması (SIGHUP)
  const bye = async () => { if (usePg) await flush(); else flushSync(); process.exit(0); };
  for (const sig of ['SIGTERM', 'SIGINT', 'SIGHUP']) process.on(sig, bye);
}

module.exports = { ready, flush, flushSync, server, unfurl, ytId, parseYtSearch, setPublicUrl: (u) => { publicUrl = u; } };
