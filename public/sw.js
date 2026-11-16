// Kulübe servis çalışanı. Telefonda "ana ekrana ekle" için gerekir.
// Kulübe sunucuya bağlı çalıştığı için çevrimdışı çalışmaz; burada sadece:
//  - açılış sayfası saklanır (internet yokken boş ekran yerine sayfa açılır ve "ulaşılamadı" der),
//  - bildirime dokununca uygulama öne gelir ve o kanal açılır.
// Mesajlara, dosyalara, sese ve sunucuyla bağlantıya dokunulmaz.
const CACHE = 'kulube-v1';
const SHELL = ['/', '/icon.png', '/manifest.webmanifest', '/simge/icon-192.png'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim()));
});

self.addEventListener('fetch', (e) => {
  const r = e.request;
  if (r.method !== 'GET' || r.mode !== 'navigate') return; // sadece sayfa açılışları; geri kalan her şey olduğu gibi sunucuya gider
  e.respondWith(
    fetch(r).then((res) => {
      if (res.ok) { const copy = res.clone(); caches.open(CACHE).then((c) => c.put('/', copy)); }
      return res;
    }).catch(() => caches.match('/'))
  );
});

self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const channel = e.notification.data && e.notification.data.channel;
  e.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
    const w = list[0];
    if (w) { w.postMessage({ type: 'kanal', channel }); return w.focus(); }
    return self.clients.openWindow('/');
  }));
});
