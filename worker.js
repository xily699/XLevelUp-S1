/**
 * ════════════════════════════════════════════════════════════════════════════
 *  XLevelUp Intelligence Edition X5.3 — Unified Edge Engine  (Cloudflare Worker)
 *  Creator: Ily
 * ════════════════════════════════════════════════════════════════════════════
 *  نسخه‌ی X5.3 روی معماری X5G.1 (edge + gateway + fail-over + circuit-breaker)
 *  یک لایه‌ی هوش/اعتبارسنجی/رصد اضافه می‌کنه، بدون حذف هیچ رفتار قبلی:
 *
 *   • Vlees Validation Engine (AI) — POST /api/validate یک کانفیگ (لینک
 *     vless:// یا فیلدهای جدا) رو مرحله‌به‌مرحله واقعاً تست می‌کنه: Syntax →
 *     UUID → Domain → SNI (auto sni=host) → TCP Handshake (واقعی، با
 *     cloudflare:sockets) → TLS Handshake (واقعی، با startTls) → توصیه‌ی
 *     تعمیر خودکار → Health Score وزن‌دار → Publish/Reject.
 *   • IP Intelligence Scanner (چندمرحله‌ای) — POST /api/scan حالا TCP +
 *     TLS واقعی رو با هم می‌سنجه و طبق فرمول وزن‌دار
 *     (Latency 35% / TLS 20% / Stability 25% / Jitter 10% / Error 10%)
 *     رتبه‌بندی می‌کنه و بهترین نود رو معرفی می‌کنه.
 *   • Protocol Lab Registry — یک رجیستری واحد از تمام پروتکل‌های پشتیبانی‌شده
 *     (VLESS-WS, VLESS-XHTTP, gRPC, TCP, SOCKS5, HTTP(S), HTTP/2, HTTP/3,
 *     TUIC, Hybrid) به همراه اینکه واقعاً از edge قابل تست زنده هستن یا نه —
 *     صادقانه، بدون ادعای دروغ. HTTP/3 و TUIC روی QUIC/UDP هستن و
 *     cloudflare:sockets فقط TCP پشتیبانی می‌کنه؛ اینها فقط Config-Builder‌ می‌شن.
 *   • /health، /api/sync (امن با EDGE_SECRET) برای هماهنگی با Railway Core.
 *   • Circuit Breaker per-مقصد، Adaptive Usage Reporting (EWMA)، Generic
 *     WebSocket Gateway، Latency Telemetry — همه دقیقاً مثل قبل، دست‌نخورده.
 *
 *  متغیرهای محیطی: همون قبلی (BACKEND_ORIGIN, EDGE_SECRET, ALLOWED_UUIDS,
 *  EDGE_FAILOPEN, CONFIG_TTL_MS) — چیز جدیدی برای تنظیم دستی لازم نیست.
 *
 *  دیپلوی: Copy/Paste در Cloudflare Dashboard → Quick Edit (ES Module).
 * ════════════════════════════════════════════════════════════════════════════
 */
import { connect } from 'cloudflare:sockets';

const ENGINE_VERSION = '5.3';
const ENGINE_NAME = 'XLevelUp Intelligence Edition';
const ENGINE_CREATOR = 'Ily';
const STATIC_UUID_FALLBACK = '41dca55b-7cbe-43a3-9915-0470cb7aca0a';
const WS_PATH_LEGACY = '/vlees';
const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const WS_ROUTE_RE = /^\/ws\/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/;

// ── وضعیت درون-ایزوله (per-isolate؛ با کولد استارت ری‌ست می‌شه، بی‌ضرره) ──
let isolateConfigCache = { ts: 0, list: [], byUuid: new Map() };
const speedBuckets = new Map();       // uuid -> token bucket
const circuitBreakers = new Map();    // hostname -> { failCount, openUntil }
const validationLog = [];             // آخرین نتایج Validation Engine (حداکثر 40 مورد، حافظه‌ی ایزوله)
const VALIDATION_LOG_MAX = 40;

// ══════════════════════════════════════════════════════════════════════════════
// Protocol Lab — رجیستری واحد پروتکل‌ها (صادقانه: فقط چیزی که واقعاً edge
// می‌تونه تست زنده کنه edgeTestable:true می‌گیره)
// ══════════════════════════════════════════════════════════════════════════════
const PROTOCOLS = {
  'vless-ws': {
    id: 'vless-ws', name: 'VLESS / WebSocket', family: 'vless', transport: 'ws',
    edgeTestable: true, edgeTunnel: true,
    desc: 'رله‌ی بومی edge با cloudflare:sockets + circuit-breaker + fail-over به Railway.',
  },
  'vless-xhttp': {
    id: 'vless-xhttp', name: 'VLESS / XHTTP', family: 'vless', transport: 'xhttp',
    edgeTestable: 'partial', edgeTunnel: false,
    desc: 'سشن XHTTP چندریکوئستی روی Railway (xhttp_siz10.py) مدیریت می‌شه؛ Worker فقط gateway/pass-through است.',
  },
  grpc: {
    id: 'grpc', name: 'VLESS / gRPC', family: 'vless', transport: 'grpc',
    edgeTestable: false, edgeTunnel: false,
    desc: 'fetch() در Workers به فریم‌های HTTP/2 trailer دسترسی نداره — فقط Config Builder، بدون تست زنده‌ی edge.',
  },
  'tcp-ping': {
    id: 'tcp-ping', name: 'TCP Ping', family: 'probe', transport: 'tcp',
    edgeTestable: true, edgeTunnel: false,
    desc: 'اتصال TCP خام با cloudflare:sockets — Real Ping واقعی (نه ICMP، نه fetch).',
  },
  socks5: {
    id: 'socks5', name: 'SOCKS5', family: 'proxy', transport: 'tcp',
    edgeTestable: 'partial', edgeTunnel: false,
    desc: 'هندشیک لایه‌ی TCP قابل تست است؛ negotiation کامل SOCKS5 باید سمت کلاینت/بک‌اند انجام شود.',
  },
  http: {
    id: 'http', name: 'HTTP', family: 'web', transport: 'tcp',
    edgeTestable: true, edgeTunnel: true, desc: 'fetch() مستقیم + gateway به بک‌اند.',
  },
  https: {
    id: 'https', name: 'HTTPS', family: 'web', transport: 'tls',
    edgeTestable: true, edgeTunnel: true, desc: 'TLS handshake واقعی با startTls روی cloudflare:sockets.',
  },
  http2: {
    id: 'http2', name: 'HTTP/2', family: 'web', transport: 'tls',
    edgeTestable: 'partial', edgeTunnel: true,
    desc: 'fetch() Workers به‌صورت خودکار ALPN=h2 مذاکره می‌کنه؛ تشخیص نسخه از پاسخ ممکنه، کنترل فریم دستی ممکن نیست.',
  },
  http3: {
    id: 'http3', name: 'HTTP/3 (Ready)', family: 'web', transport: 'quic',
    edgeTestable: false, edgeTunnel: false,
    desc: 'HTTP/3 روی QUIC/UDP است. cloudflare:sockets فقط TCP پشتیبانی می‌کنه → فقط Config Builder، بدون تست زنده‌ی edge.',
  },
  tuic: {
    id: 'tuic', name: 'TUIC (Ready)', family: 'proxy', transport: 'quic',
    edgeTestable: false, edgeTunnel: false,
    desc: 'TUIC مبتنی بر QUIC/UDP است — همون محدودیت HTTP/3. فقط Config Builder.',
  },
  hybrid: {
    id: 'hybrid', name: 'Hybrid Profile', family: 'profile', transport: 'mixed',
    edgeTestable: 'partial', edgeTunnel: true,
    desc: 'ترکیب Primary/Backup/Fallback از پروتکل‌های بالا — منطق انتخاب در Smart Transport Engine.',
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// Smart Transport Engine — پیشنهاد پروتکل بر اساس شرایط شبکه
// ══════════════════════════════════════════════════════════════════════════════
function recommendProfile({ latencyMs, jitterMs, errorRate, mode }) {
  const lat = Number(latencyMs) || 0;
  const jit = Number(jitterMs) || 0;
  const err = Number(errorRate) || 0;
  if (mode === 'gaming') {
    return { primary: 'vless-xhttp', backup: 'vless-ws', fallback: 'grpc', reason: 'Gaming: تأخیر کم مهم‌تر از پایداری طولانی‌مدته → XHTTP اول.' };
  }
  if (mode === 'long-session') {
    return { primary: 'grpc', backup: 'vless-ws', fallback: 'vless-xhttp', reason: 'Long Session: gRPC روی HTTP/2 multiplexing پایدارتره.' };
  }
  if (err > 0.15 || jit > 120) {
    return { primary: 'vless-ws', backup: 'vless-xhttp', fallback: 'https', reason: 'نرخ خطا/جیتر بالا → WebSocket پایدار اولویت داره.' };
  }
  if (lat > 0 && lat < 90 && jit < 40) {
    return { primary: 'vless-xhttp', backup: 'vless-ws', fallback: 'grpc', reason: 'شبکه سریع و باثبات → XHTTP برای کمترین تأخیر.' };
  }
  return { primary: 'vless-ws', backup: 'vless-xhttp', fallback: 'grpc', reason: 'حالت پیش‌فرض/Stable: WebSocket → XHTTP → gRPC.' };
}

const HYBRID_PROFILES = {
  gaming: { label: 'Gaming Mode', primary: 'vless-xhttp', backup: 'vless-ws', fallback: 'grpc' },
  stable: { label: 'Stable Mode', primary: 'vless-ws', backup: 'vless-xhttp', fallback: null },
  mobile: { label: 'Mobile Mode', primary: 'adaptive', backup: null, fallback: null, adaptive: true },
};

// ── تنظیمات Circuit Breaker ────────────────────────────────────────────────
const CB_FAIL_THRESHOLD = 2;      // بعد این‌قدر شکست پشت‌سرهم، مدار باز می‌شه
const CB_OPEN_MS = 30000;         // مدت باز موندن مدار قبل از تلاش مجدد
const CB_PROBE_TIMEOUT_MS = 2500; // وقتی مقصد سابقه‌ی شکست داره، فقط این‌قدر صبر کن
const CB_FULL_TIMEOUT_MS = 8000;  // تلاش اول/سالم، تایم‌اوت کامل

export default {
  async fetch(request, env, ctx) {
    const cfg = buildRuntimeConfig(env);
    const url = new URL(request.url);

    try {
      if (url.pathname === '/api/ping') {
        return jsonResp({ ok: true, ts: Date.now(), node: 'x5g-edge', version: ENGINE_VERSION });
      }
      // ── /health: لایت‌ترین endpoint ممکن، فقط برای liveness/uptime-monitor ──
      if (url.pathname === '/health') {
        return jsonResp({ ok: true, engine: ENGINE_NAME, version: ENGINE_VERSION, ts: Date.now() });
      }
      if (url.pathname === '/api/status' || url.pathname === '/api/edge-info') {
        return jsonResp(await buildStatus(cfg, request, ctx));
      }
      // ── داشبورد مستقل Worker (XLevelUp X5.3) — با زدن /dashboard روی خودِ ساب‌دامین ورکر باز می‌شه ──
      if (url.pathname === '/dashboard' || url.pathname === '/vless-dashboard') {
        return new Response(renderDashboardHTML(), { headers: { 'content-type': 'text/html; charset=utf-8' } });
      }
      if (url.pathname === '/api/tcp-ping') {
        return await handleTcpPing(url);
      }
      if (url.pathname === '/api/scan' && request.method === 'POST') {
        return await handleScan(request);
      }
      // ── Vlees Validation Engine (AI) ────────────────────────────────────────
      if (url.pathname === '/api/validate' && request.method === 'POST') {
        return await handleValidate(request, url);
      }
      if (url.pathname === '/api/validate' && request.method === 'GET') {
        return jsonResp({ ok: true, recent: validationLog.slice(-20).reverse() });
      }
      // ── Config Center: Build + Validate در یک درخواست ──────────────────────
      if (url.pathname === '/api/config/build' && request.method === 'POST') {
        return await handleConfigBuild(request, url);
      }
      // ── Protocol Lab: رجیستری کامل پروتکل‌ها + پیشنهاد پروفایل هوشمند ──────
      if (url.pathname === '/api/protocols') {
        const mode = url.searchParams.get('mode') || 'stable';
        return jsonResp({
          ok: true,
          protocols: PROTOCOLS,
          hybrid_profiles: HYBRID_PROFILES,
          recommend: recommendProfile({
            latencyMs: Number(url.searchParams.get('latency') || 0),
            jitterMs: Number(url.searchParams.get('jitter') || 0),
            errorRate: Number(url.searchParams.get('error') || 0),
            mode,
          }),
        });
      }
      // ── Secure Sync API با Railway Core (فقط با EDGE_SECRET درست) ──────────
      if (url.pathname === '/api/sync') {
        return await handleSync(request, cfg, ctx);
      }

      const upgrade = (request.headers.get('Upgrade') || '').toLowerCase();
      const isWsUpgrade = upgrade === 'websocket';
      const wsMatch = url.pathname.match(WS_ROUTE_RE);

      // ── مسیر اصلی چندکاربره: /ws/{uuid} ────────────────────────────────────
      if (isWsUpgrade && wsMatch) {
        return await handleVlessWS(request, wsMatch[1], cfg, ctx);
      }

      // ── مسیر قدیمی تک‌کاربره برای سازگاری با نسخه‌ی قبلی: /vlees ──────────
      if (isWsUpgrade && url.pathname === WS_PATH_LEGACY) {
        const uuid = cfg.allowedUuids.values().next().value || STATIC_UUID_FALLBACK;
        return await handleVlessWS(request, uuid, cfg, ctx);
      }

      // ── X5G.1: هر مسیر WS دیگه (غیر VLESS) → پراکسی عمومی به بک‌اند ────────
      // (برای هر سوکت زنده‌ی آینده مثل داشبورد realtime، بدون نیاز به تغییر
      //  این فایل — فقط بک‌اند باید همون مسیر رو implement کنه)
      if (isWsUpgrade && cfg.backendOrigin) {
        return await genericWsProxy(request, cfg, url.pathname + url.search);
      }

      // ── هر چیز دیگه: gateway یکپارچه به سمت بک‌اند Railway ─────────────────
      if (cfg.backendOrigin) {
        return await proxyToBackend(request, cfg);
      }

      return new Response(
        `${ENGINE_NAME} X${ENGINE_VERSION} — Edge Engine (standalone mode, no BACKEND_ORIGIN configured)`,
        { status: 200 }
      );
    } catch (err) {
      return new Response('XLevelUp edge error: ' + (err && err.stack ? err.stack : String(err)), { status: 500 });
    }
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// Config / وضعیت
// ══════════════════════════════════════════════════════════════════════════════
function buildRuntimeConfig(env) {
  const backendOrigin =
    (env.BACKEND_ORIGIN || '').trim().replace(/^https?:\/\//i, '').replace(/\/+$/, '') || null;
  const edgeSecret = (env.EDGE_SECRET || '').trim() || null;
  const rawUuids = (env.ALLOWED_UUIDS || STATIC_UUID_FALLBACK)
    .split(',')
    .map((s) => s.trim())
    .filter((s) => UUID_RE.test(s));
  return {
    backendOrigin,
    edgeSecret,
    allowedUuids: new Set(rawUuids.length ? rawUuids : [STATIC_UUID_FALLBACK]),
    failOpen: (env.EDGE_FAILOPEN ?? 'true').toString().toLowerCase() !== 'false',
    configTtlMs: parseInt(env.CONFIG_TTL_MS || '20000', 10) || 20000,
  };
}

async function buildStatus(cfg, request, ctx) {
  let backendReachable = null;
  if (cfg.backendOrigin && cfg.edgeSecret) {
    try {
      const r = await fetchWithTimeout(
        `https://${cfg.backendOrigin}/api/edge/ping`,
        { headers: { 'X-Edge-Secret': cfg.edgeSecret } },
        2500
      );
      backendReachable = r.ok;
    } catch (_) {
      backendReachable = false;
    }
  }
  const mode = cfg.backendOrigin
    ? backendReachable
      ? 'hybrid-connected'
      : 'hybrid-degraded-failopen'
    : 'standalone';

  const openCircuits = [];
  for (const [host, c] of circuitBreakers) {
    if (Date.now() < c.openUntil) openCircuits.push({ host, fail_count: c.failCount, reopen_in_ms: c.openUntil - Date.now() });
  }

  // ── System Status: هر جزء با وضعیت (green/yellow/red) + دلیل دقیق ──────────
  const components = [];
  components.push({
    id: 'worker', label: 'Cloudflare Worker (Edge)', status: 'green',
    reason: `Isolate فعال — ${ENGINE_NAME} v${ENGINE_VERSION}`,
  });
  if (cfg.backendOrigin) {
    if (backendReachable) {
      components.push({ id: 'railway', label: 'Railway Core', status: 'green', reason: `پاسخ 2xx از ${cfg.backendOrigin}/api/edge/ping` });
    } else if (backendReachable === false) {
      components.push({
        id: 'railway', label: 'Railway Core', status: cfg.failOpen ? 'yellow' : 'red',
        reason: cfg.failOpen
          ? `عدم دسترسی به ${cfg.backendOrigin} — fail-open فعال است، از کش آخرین کانفیگ استفاده می‌شود`
          : `عدم دسترسی به ${cfg.backendOrigin} و fail-open خاموش است — اتصال‌های جدید رد می‌شوند`,
      });
    } else {
      components.push({ id: 'railway', label: 'Railway Core', status: 'yellow', reason: 'EDGE_SECRET تنظیم نشده — نمی‌توان سلامت بک‌اند را واقعاً بررسی کرد' });
    }
  } else {
    components.push({ id: 'railway', label: 'Railway Core', status: 'yellow', reason: 'BACKEND_ORIGIN تنظیم نشده — حالت Standalone' });
  }
  components.push({
    id: 'sync', label: 'Secure Sync API', status: cfg.edgeSecret ? 'green' : 'yellow',
    reason: cfg.edgeSecret ? 'EDGE_SECRET تنظیم شده — /api/sync قابل احراز هویت است' : 'EDGE_SECRET خالی است — /api/sync درخواست‌ها را رد می‌کند',
  });
  components.push({
    id: 'validation-engine', label: 'Vlees Validation Engine (AI)', status: 'green',
    reason: `آماده — cloudflare:sockets در دسترس است (TCP + TLS واقعی). ${validationLog.length} تست اخیر در حافظه‌ی این ایزوله.`,
  });
  components.push({
    id: 'circuit-breaker', label: 'Circuit Breaker', status: openCircuits.length ? 'yellow' : 'green',
    reason: openCircuits.length
      ? `${openCircuits.length} مقصد فعلاً مدارشان باز است (fail-over خودکار فعال): ${openCircuits.map((c) => c.host).join(', ')}`
      : 'هیچ مداری باز نیست — همه‌ی مقصدهای اخیر سالم بوده‌اند',
  });

  return {
    engine: `${ENGINE_NAME} — Unified Edge Engine`,
    engine_version: ENGINE_VERSION,
    creator: ENGINE_CREATOR,
    components,
    mode,
    colo: (request.cf && request.cf.colo) || null,
    country: (request.cf && request.cf.country) || null,
    backend_origin: cfg.backendOrigin,
    backend_reachable: backendReachable,
    fail_open: cfg.failOpen,
    transports: {
      'vless-ws': 'edge-native (cloudflare:sockets) + circuit-breaker + auto-failover to Railway',
      xhttp: 'proxied to Railway backend (stateful multi-request session)',
      'http-proxy': 'proxied to Railway backend',
      'other-ws': 'generic websocket gateway to Railway backend',
    },
    intelligence: {
      circuit_breaker_open_now: openCircuits,
      tracked_destinations: circuitBreakers.size,
    },
    allowed_static_uuids: cfg.backendOrigin ? undefined : Array.from(cfg.allowedUuids),
    timestamp: new Date().toISOString(),
  };
}

function jsonResp(obj) {
  return new Response(JSON.stringify(obj, null, 2), { headers: { 'content-type': 'application/json' } });
}

async function fetchWithTimeout(url, init, ms) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    return await fetch(url, { ...(init || {}), signal: ac.signal });
  } finally {
    clearTimeout(t);
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// Circuit Breaker per-مقصد
// ══════════════════════════════════════════════════════════════════════════════
function cbGet(host) {
  let c = circuitBreakers.get(host);
  if (!c) {
    c = { failCount: 0, openUntil: 0 };
    circuitBreakers.set(host, c);
  }
  return c;
}
function cbIsOpen(host) {
  return Date.now() < cbGet(host).openUntil;
}
function cbRecordSuccess(host) {
  const c = cbGet(host);
  c.failCount = 0;
  c.openUntil = 0;
}
function cbRecordFailure(host) {
  const c = cbGet(host);
  c.failCount += 1;
  if (c.failCount >= CB_FAIL_THRESHOLD) {
    c.openUntil = Date.now() + CB_OPEN_MS;
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gateway یکپارچه — پراکسی هر چیزی به‌جز رله VLESS به سمت بک‌اند
// ══════════════════════════════════════════════════════════════════════════════
async function proxyToBackend(request, cfg) {
  const url = new URL(request.url);
  const target = new URL(url.pathname + url.search, `https://${cfg.backendOrigin}`);
  const headers = new Headers(request.headers);
  headers.set('x-forwarded-host', url.hostname);
  headers.set('x-forwarded-proto', 'https');
  headers.set('x-real-ip', request.headers.get('cf-connecting-ip') || '');
  headers.delete('host');

  const init = {
    method: request.method,
    headers,
    body: ['GET', 'HEAD'].includes(request.method) ? undefined : request.body,
    redirect: 'manual',
  };
  const resp = await fetch(target.toString(), init);
  const outHeaders = new Headers(resp.headers);
  return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers: outHeaders });
}

/** X5G.1: پراکسی عمومی WebSocket برای هر مسیری غیر از VLESS (مثلاً یک
 *  سوکت زنده‌ی مدیریتی که بعداً روی بک‌اند اضافه کنی — این فایل بدون
 *  تغییر خودش پشتیبانی می‌کنه). کوکی هم منتقل می‌شه تا سشن ادمین حفظ بمونه. */
async function genericWsProxy(request, cfg, path) {
  const { 0: client, 1: server } = new WebSocketPair();
  server.accept();

  let backendWs;
  try {
    backendWs = await connectBackendWSPath(path, cfg, request);
  } catch (e) {
    safeCloseWebSocket(server);
    return new Response('backend ws proxy failed: ' + e.message, { status: 502 });
  }

  server.addEventListener('message', (evt) => {
    if (backendWs.readyState === 1) backendWs.send(evt.data);
  });
  server.addEventListener('close', () => safeCloseWebSocket(backendWs));
  server.addEventListener('error', () => safeCloseWebSocket(backendWs));

  backendWs.addEventListener('message', (evt) => {
    if (server.readyState === 1) server.send(evt.data);
  });
  backendWs.addEventListener('close', () => safeCloseWebSocket(server));
  backendWs.addEventListener('error', () => safeCloseWebSocket(server));

  return new Response(null, { status: 101, webSocket: client });
}

async function connectBackendWSPath(path, cfg, request) {
  const backendUrl = `https://${cfg.backendOrigin}${path}`;
  const headers = new Headers();
  headers.set('Upgrade', 'websocket');
  headers.set('Connection', 'Upgrade');
  const cookie = request && request.headers.get('cookie');
  if (cookie) headers.set('cookie', cookie);
  const resp = await fetch(backendUrl, { headers });
  if (!resp.webSocket) {
    throw new Error('backend did not upgrade to websocket (status ' + resp.status + ')');
  }
  resp.webSocket.accept();
  return resp.webSocket;
}

// ══════════════════════════════════════════════════════════════════════════════
// Edge Sync — هماهنگی احراز هویت/کوتا/IP-limit با بک‌اند Railway
// ══════════════════════════════════════════════════════════════════════════════
async function getConfigsSnapshot(cfg) {
  const now = Date.now();
  if (now - isolateConfigCache.ts < cfg.configTtlMs && isolateConfigCache.list.length) {
    return isolateConfigCache;
  }
  if (!cfg.backendOrigin || !cfg.edgeSecret) return isolateConfigCache;
  try {
    const resp = await fetchWithTimeout(
      `https://${cfg.backendOrigin}/api/edge/configs`,
      { headers: { 'X-Edge-Secret': cfg.edgeSecret } },
      3000
    );
    if (!resp.ok) return isolateConfigCache;
    const data = await resp.json();
    const byUuid = new Map();
    for (const c of data.configs || []) byUuid.set(c.uuid, c);
    isolateConfigCache = { ts: now, list: data.configs || [], byUuid };
  } catch (_) {
    // بک‌اند در دسترس نیست؛ آخرین کش معتبر رو نگه می‌داریم (fail-open)
  }
  return isolateConfigCache;
}

async function authorizeConnect(uuid, ip, cfg) {
  if (cfg.backendOrigin && cfg.edgeSecret) {
    try {
      const resp = await fetchWithTimeout(
        `https://${cfg.backendOrigin}/api/edge/connect`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'X-Edge-Secret': cfg.edgeSecret },
          body: JSON.stringify({ uuid, ip, transport: 'edge-ws' }),
        },
        3000
      );
      if (resp.ok) {
        const data = await resp.json();
        return {
          allow: !!data.allow,
          connId: data.conn_id || null,
          speedLimit: data.speed_limit_bytes || 0,
          source: 'backend',
        };
      }
    } catch (_) {
      // بک‌اند در دسترس نیست، می‌ریم سراغ fail-open
    }
    if (cfg.failOpen) {
      const snap = await getConfigsSnapshot(cfg);
      const c = snap.byUuid.get(uuid);
      return {
        allow: !!(c && c.active),
        connId: null,
        speedLimit: c ? c.speed_limit_bytes || 0 : 0,
        source: 'cache-failopen',
      };
    }
    return { allow: false, connId: null, speedLimit: 0, source: 'backend-unreachable' };
  }
  return { allow: cfg.allowedUuids.has(uuid), connId: null, speedLimit: 0, source: 'standalone' };
}

function reportUsage(ctx, cfg, connId, uuid, nbytes) {
  if (!connId || !cfg.backendOrigin || !cfg.edgeSecret || nbytes <= 0) return;
  ctx.waitUntil(
    fetchWithTimeout(
      `https://${cfg.backendOrigin}/api/edge/usage`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'X-Edge-Secret': cfg.edgeSecret },
        body: JSON.stringify({ conn_id: connId, uuid, bytes: nbytes }),
      },
      4000
    ).catch(() => {})
  );
}

function reportDisconnect(ctx, cfg, connId) {
  if (!connId || !cfg.backendOrigin || !cfg.edgeSecret) return;
  ctx.waitUntil(
    fetchWithTimeout(
      `https://${cfg.backendOrigin}/api/edge/disconnect`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'X-Edge-Secret': cfg.edgeSecret },
        body: JSON.stringify({ conn_id: connId }),
      },
      4000
    ).catch(() => {})
  );
}

/** X5G.1: نمونه‌ی latency/circuit-breaker رو به بک‌اند گزارش می‌ده تا در
 *  داشبورد دیده بشه. کاملاً fire-and-forget و سبک — فقط یک بار در پایان
 *  هر اتصال، نه در حین رله. */
function reportLatency(ctx, cfg, request, sample) {
  if (!cfg.backendOrigin || !cfg.edgeSecret) return;
  const openHosts = [];
  for (const [host, c] of circuitBreakers) {
    if (Date.now() < c.openUntil) openHosts.push(host);
  }
  ctx.waitUntil(
    fetchWithTimeout(
      `https://${cfg.backendOrigin}/api/v1/edge-latency-report`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'X-Edge-Secret': cfg.edgeSecret },
        body: JSON.stringify({
          colo: (request.cf && request.cf.colo) || null,
          edge_connect_ms: sample.connectMs ?? null,
          circuit_open_hosts: openHosts,
          failover_count: sample.failover ? 1 : 0,
        }),
      },
      3000
    ).catch(() => {})
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// محدودیت سرعت (Token Bucket سبک، best-effort روی هر ایزوله)
// ══════════════════════════════════════════════════════════════════════════════
function getBucket(uuid, rateBytesPerSec) {
  let b = speedBuckets.get(uuid);
  if (!b || b.rate !== rateBytesPerSec) {
    const rate = Math.max(rateBytesPerSec, 1024);
    b = { rate, capacity: Math.max(rate, 16 * 1024), tokens: 0, last: Date.now() };
    b.tokens = b.capacity;
    speedBuckets.set(uuid, b);
  }
  return b;
}

async function throttle(uuid, n, rateBytesPerSec) {
  if (!rateBytesPerSec || rateBytesPerSec <= 0) return;
  const b = getBucket(uuid, rateBytesPerSec);
  for (;;) {
    const now = Date.now();
    const elapsed = (now - b.last) / 1000;
    if (elapsed > 0) {
      b.last = now;
      b.tokens = Math.min(b.capacity, b.tokens + elapsed * b.rate);
    }
    if (b.tokens >= n) {
      b.tokens -= n;
      return;
    }
    const deficit = n - b.tokens;
    const wait = Math.min(Math.max((deficit / b.rate) * 1000, 4), 500);
    await new Promise((r) => setTimeout(r, wait));
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// X5G.1: Adaptive Usage Tracker — همون فلسفه‌ی AdaptiveQuotaGate بک‌اند،
// اینجا برای batch گزارش مصرف (نه چک کوتا، چون چک کوتا خودِ بک‌انده)
// ══════════════════════════════════════════════════════════════════════════════
function makeUsageTracker() {
  return { pending: 0, lastFlush: Date.now(), batchBytes: 65536, rateEwma: 0 };
}
function usageTrackerAdd(tracker, n) {
  tracker.pending += n;
}
function usageTrackerShouldFlush(tracker) {
  return tracker.pending >= tracker.batchBytes || Date.now() - tracker.lastFlush >= 200;
}
function usageTrackerFlushed(tracker, flushedBytes) {
  const now = Date.now();
  const elapsedMs = now - tracker.lastFlush;
  if (elapsedMs > 0 && flushedBytes > 0) {
    const instRate = flushedBytes / (elapsedMs / 1000);
    tracker.rateEwma = tracker.rateEwma === 0 ? instRate : 0.7 * tracker.rateEwma + 0.3 * instRate;
    const target = Math.floor(tracker.rateEwma * 0.2);
    tracker.batchBytes = Math.max(32 * 1024, Math.min(1024 * 1024, target || 32 * 1024));
  }
  tracker.pending = 0;
  tracker.lastFlush = now;
}

// ══════════════════════════════════════════════════════════════════════════════
// رله VLESS روی WebSocket — با Circuit Breaker + Fail-over خودکار
// ══════════════════════════════════════════════════════════════════════════════
async function handleVlessWS(request, pathUuid, cfg, ctx) {
  const { 0: client, 1: server } = new WebSocketPair();
  server.accept();

  const ip = request.headers.get('cf-connecting-ip') || request.headers.get('x-forwarded-for') || 'edge-unknown';
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readable = makeWebSocketReadable(server, earlyDataHeader);

  let headerParsed = false;
  let mode = null; // 'edge' | 'failover'
  let remoteSocket = null;
  let backendWs = null;
  let connId = null;
  let speedLimitBytes = 0;
  const usage = makeUsageTracker();
  const telemetrySample = { connectMs: null, failover: false };

  const flushUsage = (force) => {
    if (mode !== 'edge') return;
    if (usage.pending <= 0) return;
    if (!force && !usageTrackerShouldFlush(usage)) return;
    const flushed = usage.pending;
    reportUsage(ctx, cfg, connId, pathUuid, flushed);
    usageTrackerFlushed(usage, flushed);
  };

  const cleanup = () => {
    flushUsage(true);
    if (connId) {
      reportDisconnect(ctx, cfg, connId);
      connId = null;
    }
    reportLatency(ctx, cfg, request, telemetrySample);
    safeCloseWebSocket(server);
    if (backendWs) safeCloseWebSocket(backendWs);
  };

  readable
    .pipeTo(
      new WritableStream({
        async write(chunkIn) {
          const chunk = new Uint8Array(chunkIn);

          if (headerParsed) {
            if (mode === 'edge' && remoteSocket) {
              await throttle(pathUuid, chunk.byteLength, speedLimitBytes);
              const writer = remoteSocket.writable.getWriter();
              await writer.write(chunk);
              writer.releaseLock();
              usageTrackerAdd(usage, chunk.byteLength);
              flushUsage(false);
            } else if (mode === 'failover' && backendWs) {
              backendWs.send(chunk);
            }
            return;
          }

          const parsed = parseVlessHeader(
            chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength),
            pathUuid
          );
          if (parsed.hasError) {
            safeCloseWebSocket(server);
            throw new Error('VLESS header error: ' + parsed.message);
          }
          if (parsed.isUDP) {
            safeCloseWebSocket(server);
            throw new Error('UDP در این نسخه پشتیبانی نمی‌شه (فقط TCP)');
          }
          headerParsed = true;

          const vlessResponseHeader = new Uint8Array([parsed.vlessVersion[0], 0]);
          const rawClientData = chunk.slice(parsed.rawDataIndex);

          const [authRes, connRes] = await Promise.allSettled([
            authorizeConnect(pathUuid, ip, cfg),
            tryConnectDest(parsed.addressRemote, parsed.portRemote, telemetrySample),
          ]);

          const auth = authRes.status === 'fulfilled' ? authRes.value : { allow: false, connId: null, speedLimit: 0 };
          if (!auth.allow) {
            if (connRes.status === 'fulfilled' && connRes.value) {
              try { connRes.value.close(); } catch (_) {}
            }
            safeCloseWebSocket(server);
            throw new Error('not authorized (quota/expired/ip-limit/unknown-uuid)');
          }
          connId = auth.connId;
          speedLimitBytes = auth.speedLimit;

          const destSocket = connRes.status === 'fulfilled' ? connRes.value : null;

          if (destSocket) {
            mode = 'edge';
            remoteSocket = destSocket;
            const writer = remoteSocket.writable.getWriter();
            await writer.write(rawClientData);
            writer.releaseLock();
            usageTrackerAdd(usage, chunk.byteLength);
            flushUsage(false);
            pipeRemoteToWebSocket(remoteSocket, server, vlessResponseHeader, (n) => {
              usageTrackerAdd(usage, n);
              flushUsage(false);
            });
          } else {
            // ── Fail-over: مقصد از edge قابل اتصال نبود (یا مدار باز بود) ──
            telemetrySample.failover = true;
            if (connId) {
              reportDisconnect(ctx, cfg, connId);
              connId = null;
            }
            if (!cfg.backendOrigin) {
              safeCloseWebSocket(server);
              throw new Error('مقصد از edge قابل اتصال نیست و بک‌اندی برای fail-over تنظیم نشده');
            }
            backendWs = await connectBackendWS(pathUuid, cfg);
            mode = 'failover';
            backendWs.addEventListener('message', (evt) => {
              if (server.readyState === 1) server.send(evt.data);
            });
            backendWs.addEventListener('close', () => safeCloseWebSocket(server));
            backendWs.addEventListener('error', () => safeCloseWebSocket(server));
            backendWs.send(chunk);
          }
        },
        close() {
          cleanup();
        },
        abort() {
          cleanup();
        },
      })
    )
    .catch(() => {
      cleanup();
    });

  return new Response(null, { status: 101, webSocket: client });
}

/** X5G.1: اتصال به مقصد با آگاهی از Circuit Breaker — اگه مدار باز باشه
 *  اصلاً امتحان نمی‌کنه (میان‌بر فوری به fail-over)، وگرنه بسته به
 *  سابقه‌ی شکست، تایم‌اوت کوتاه‌تر یا کامل استفاده می‌کنه. */
async function tryConnectDest(hostname, port, telemetrySample) {
  if (cbIsOpen(hostname)) {
    return null;
  }
  const hasRecentFailure = cbGet(hostname).failCount > 0;
  const timeoutMs = hasRecentFailure ? CB_PROBE_TIMEOUT_MS : CB_FULL_TIMEOUT_MS;
  const t0 = Date.now();
  try {
    const socket = connect({ hostname, port });
    await Promise.race([
      socket.opened,
      new Promise((_, rej) => setTimeout(() => rej(new Error('edge connect timeout')), timeoutMs)),
    ]);
    cbRecordSuccess(hostname);
    if (telemetrySample) telemetrySample.connectMs = Date.now() - t0;
    return socket;
  } catch (_) {
    cbRecordFailure(hostname);
    if (telemetrySample) telemetrySample.connectMs = Date.now() - t0;
    return null;
  }
}

async function connectBackendWS(uuid, cfg) {
  return connectBackendWSPath(`/ws/${uuid}`, cfg, null);
}

function pipeRemoteToWebSocket(remoteSocket, webSocket, vlessResponseHeader, onBytes) {
  let headerSent = false;
  remoteSocket.readable
    .pipeTo(
      new WritableStream({
        write(chunk) {
          if (webSocket.readyState !== 1) return;
          const len = chunk.byteLength;
          if (!headerSent) {
            const combined = new Uint8Array(vlessResponseHeader.length + len);
            combined.set(vlessResponseHeader, 0);
            combined.set(new Uint8Array(chunk), vlessResponseHeader.length);
            webSocket.send(combined);
            headerSent = true;
          } else {
            webSocket.send(chunk);
          }
          if (onBytes) onBytes(len);
        },
        close() {
          safeCloseWebSocket(webSocket);
        },
        abort() {
          safeCloseWebSocket(webSocket);
        },
      })
    )
    .catch(() => {
      safeCloseWebSocket(webSocket);
    });
}

// --- تبدیل رویدادهای WebSocket به ReadableStream (پشتیبانی از 0-RTT early data) ---
function makeWebSocketReadable(ws, earlyDataHeader) {
  let cancelled = false;
  return new ReadableStream({
    start(controller) {
      ws.addEventListener('message', (event) => {
        if (cancelled) return;
        controller.enqueue(event.data);
      });
      ws.addEventListener('close', () => {
        if (cancelled) return;
        controller.close();
      });
      ws.addEventListener('error', (err) => {
        controller.error(err);
      });

      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) controller.error(error);
      else if (earlyData) controller.enqueue(earlyData);
    },
    cancel() {
      cancelled = true;
      safeCloseWebSocket(ws);
    },
  });
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { earlyData: null, error: null };
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decoded = atob(base64Str);
    const bytes = Uint8Array.from(decoded, (c) => c.charCodeAt(0));
    return { earlyData: bytes.buffer, error: null };
  } catch (error) {
    return { earlyData: null, error };
  }
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === 1 || socket.readyState === 2) socket.close();
  } catch (_) {}
}

// --- پارس هدر واقعی پروتکل VLESS (پارامتری‌شده برای چند-کاربره بودن) ---
function parseVlessHeader(buffer, expectedUuid) {
  if (buffer.byteLength < 24) {
    return { hasError: true, message: 'داده خیلی کوتاهه' };
  }
  const view = new DataView(buffer);
  const version = new Uint8Array(buffer.slice(0, 1));

  const uuidBytes = new Uint8Array(buffer.slice(1, 17));
  const uuidStr = bytesToUUID(uuidBytes);
  if (uuidStr !== expectedUuid) {
    return { hasError: true, message: 'UUID نامعتبر است' };
  }

  const optLength = new Uint8Array(buffer.slice(17, 18))[0];
  const command = new Uint8Array(buffer.slice(18 + optLength, 19 + optLength))[0];

  let isUDP = false;
  if (command === 2) isUDP = true;
  else if (command !== 1) {
    return { hasError: true, message: `دستور پشتیبانی‌نشده: ${command}` };
  }

  const portIndex = 19 + optLength;
  const portRemote = view.getUint16(portIndex);

  const addressIndex = portIndex + 2;
  const addressType = new Uint8Array(buffer.slice(addressIndex, addressIndex + 1))[0];

  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';

  switch (addressType) {
    case 1: // IPv4
      addressLength = 4;
      addressValue = Array.from(
        new Uint8Array(buffer.slice(addressValueIndex, addressValueIndex + addressLength))
      ).join('.');
      break;
    case 2: // Domain
      addressLength = new Uint8Array(buffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(
        buffer.slice(addressValueIndex, addressValueIndex + addressLength)
      );
      break;
    case 3: { // IPv6
      addressLength = 16;
      const dv = new DataView(buffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const parts = [];
      for (let i = 0; i < 8; i++) parts.push(dv.getUint16(i * 2).toString(16));
      addressValue = parts.join(':');
      break;
    }
    default:
      return { hasError: true, message: `نوع آدرس پشتیبانی‌نشده: ${addressType}` };
  }

  if (!addressValue) {
    return { hasError: true, message: 'آدرس مقصد خالیه' };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

function bytesToUUID(bytes) {
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, '0'));
  return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex.slice(6, 8).join('')}-${hex
    .slice(8, 10)
    .join('')}-${hex.slice(10, 16).join('')}`;
}

// ══════════════════════════════════════════════════════════════════════════════
// X5G.1 — Standalone Dashboard: TCP Probe + IP Scanner + Vlees Status
// ══════════════════════════════════════════════════════════════════════════════

/** یک اتصال TCP خام (نه HTTP) با cloudflare:sockets باز می‌کنه و زمان واقعی
 *  handshake رو اندازه می‌گیره — این همون «Real Ping» است، نه ICMP (که روی
 *  Workers اصلاً در دسترس نیست) و نه یک fetch ساده‌ی HTTP. */
async function tcpProbe(ip, port, timeoutMs) {
  const p = port || 443;
  const t0 = Date.now();
  let socket = null;
  try {
    socket = connect({ hostname: ip, port: p });
    await Promise.race([
      socket.opened,
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), timeoutMs || 3000)),
    ]);
    const ms = Date.now() - t0;
    socket.close().catch(() => {});
    return { ip, port: p, ok: true, ms };
  } catch (e) {
    if (socket) socket.close().catch(() => {});
    return { ip, port: p, ok: false, ms: Date.now() - t0, error: String((e && e.message) || e) };
  }
}

async function handleTcpPing(url) {
  const ip = url.searchParams.get('ip');
  const port = parseInt(url.searchParams.get('port') || '443', 10);
  if (!ip) return jsonResp({ ok: false, error: 'پارامتر ip لازم است' });
  const r = await tcpProbe(ip, port, 3000);
  return jsonResp(r);
}

/** X5.3: هندشیک TLS واقعی روی cloudflare:sockets (secureTransport: 'starttls'
 *  + socket.startTls()). این API در Workers runtime گاهی روی بعضی مقصدها
 *  ناپایدار است (باگ شناخته‌شده‌ی workerd) — به همین دلیل کاملاً best-effort
 *  و timeout-دار پیاده‌سازی شده و هیچ‌وقت باعث کرش کل اسکن نمی‌شه.
 *  نکته‌ی صادقانه: SNI ارسالی همون مقداریه که به connect() داده شده (اینجا
 *  خودِ IP) — Workers راهی رسمی برای «SNI جعلی روی IP دلخواه» (تونل با
 *  SNI یک دامنه‌ی دیگر) در اختیار نمی‌ذاره، پس این قابلیت ادعا نمی‌شه. */
async function tlsProbe(ip, port, timeoutMs) {
  const p = port || 443;
  const t0 = Date.now();
  let raw = null;
  try {
    raw = connect({ hostname: ip, port: p }, { secureTransport: 'starttls' });
    await Promise.race([
      raw.opened,
      new Promise((_, rej) => setTimeout(() => rej(new Error('tcp timeout')), timeoutMs || 3000)),
    ]);
    const tcpMs = Date.now() - t0;
    const tls = raw.startTls();
    await Promise.race([
      tls.opened,
      new Promise((_, rej) => setTimeout(() => rej(new Error('tls timeout')), timeoutMs || 3000)),
    ]);
    const tlsMs = Date.now() - t0;
    tls.close().catch(() => {});
    return { ip, port: p, ok: true, tcp_ms: tcpMs, tls_ms: tlsMs };
  } catch (e) {
    if (raw) raw.close().catch(() => {});
    return { ip, port: p, ok: false, stage: 'tls', ms: Date.now() - t0, error: String((e && e.message) || e) };
  }
}

function stdev(nums) {
  if (!nums.length) return 0;
  const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
  const variance = nums.reduce((a, b) => a + (b - mean) ** 2, 0) / nums.length;
  return Math.sqrt(variance);
}

/** X5.3: اسکنر چندمرحله‌ای واقعی برای یک IP — IP Discovery (ورودی) → TCP →
 *  TLS → Latency (چند نمونه) → Jitter → Stability Score، دقیقاً طبق وزن‌های
 *  خواسته‌شده: Latency 35% / TLS 20% / Stability 25% / Jitter 10% / Error 10% */
async function deepScanIP(ip, port, samples) {
  const n = Math.max(1, Math.min(samples || 3, 5));
  const tcpSamples = [];
  let tcpOk = 0;
  for (let i = 0; i < n; i++) {
    const r = await tcpProbe(ip, port, 2200);
    if (r.ok) {
      tcpOk++;
      tcpSamples.push(r.ms);
    }
  }
  const errorRate = 1 - tcpOk / n;
  const avgLatency = tcpSamples.length ? tcpSamples.reduce((a, b) => a + b, 0) / tcpSamples.length : null;
  const jitter = tcpSamples.length > 1 ? stdev(tcpSamples) : 0;

  let tls = { ok: false };
  if (tcpOk > 0) {
    tls = await tlsProbe(ip, port, 2500);
  }

  // ── نرمال‌سازی هر معیار به بازه‌ی 0..100 ──
  const latencyScore = avgLatency == null ? 0 : Math.max(0, 100 - Math.min(avgLatency, 600) / 6); // 0ms=100 .. 600ms+=0
  const tlsScore = tls.ok ? 100 : tcpOk > 0 ? 30 : 0; // TCP سالم ولی TLS رد شد => نمره‌ی جزئی
  const stabilityScore = (tcpOk / n) * 100;
  const jitterScore = Math.max(0, 100 - Math.min(jitter, 200) / 2); // 0ms=100 .. 200ms+=0
  const errorScore = (1 - errorRate) * 100;

  const finalScore = Math.round(
    latencyScore * 0.35 + tlsScore * 0.2 + stabilityScore * 0.25 + jitterScore * 0.1 + errorScore * 0.1
  );

  return {
    ip,
    port,
    ok: tcpOk > 0,
    samples: n,
    tcp_ok_count: tcpOk,
    avg_latency_ms: avgLatency == null ? null : Math.round(avgLatency),
    jitter_ms: Math.round(jitter),
    error_rate: Math.round(errorRate * 100) / 100,
    tls: tls.ok ? { ok: true, tls_ms: tls.tls_ms } : { ok: false, reason: tls.error || 'TLS handshake انجام نشد (یا TCP قبلش رد شد)' },
    score: Math.max(0, Math.min(100, finalScore)),
    breakdown: {
      latency_pct: Math.round(latencyScore), tls_pct: Math.round(tlsScore),
      stability_pct: Math.round(stabilityScore), jitter_pct: Math.round(jitterScore), error_pct: Math.round(errorScore),
    },
    recommended_protocol: !tcpOk ? null : tls.ok ? 'vless-ws (TLS)' : 'vless-ws (plain — بررسی گواهی مقصد لازم است)',
  };
}

/** اسکن دسته‌ای چندمرحله‌ای — با concurrency محدود تا از timeout کلی Worker رد نشیم. */
async function handleScan(request) {
  let body;
  try {
    body = await request.json();
  } catch (_) {
    return jsonResp({ ok: false, error: 'بدنه‌ی JSON نامعتبر' });
  }
  const ips = Array.isArray(body.ips) ? body.ips.filter(Boolean).slice(0, 60) : [];
  const port = parseInt(body.port || 443, 10);
  const deep = !!body.deep; // true => TCP+TLS+jitter+score, false => فقط TCP سریع (سازگار با نسخه‌ی قبلی)
  if (!ips.length) return jsonResp({ ok: false, error: 'لیست ips خالی است' });

  const CONCURRENCY = deep ? 4 : 8;
  const results = new Array(ips.length);
  let idx = 0;
  async function worker() {
    while (idx < ips.length) {
      const my = idx++;
      const ip = ips[my].trim();
      results[my] = deep ? await deepScanIP(ip, port, body.samples) : await tcpProbe(ip, port, 2500);
    }
  }
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, ips.length) }, worker));

  if (deep) {
    results.sort((a, b) => b.score - a.score);
    const best = results.find((r) => r.ok) || null;
    return jsonResp({ ok: true, mode: 'deep', port, count: results.length, best_node: best, results });
  }
  results.sort((a, b) => (a.ok === b.ok ? a.ms - b.ms : a.ok ? -1 : 1));
  return jsonResp({ ok: true, mode: 'fast', port, count: results.length, results });
}

// ══════════════════════════════════════════════════════════════════════════════
// Vlees Validation Engine (AI) — X5.3
// Workflow: Syntax → UUID → Domain → SNI(auto) → TCP → TLS → WS-Upgrade →
//           Transport → Latency → Stability Score → Publish/Reject
// هر مرحله واقعاً روی شبکه تست می‌شه (به‌جز مواردی که صراحتاً N/A علامت می‌خورن).
// ══════════════════════════════════════════════════════════════════════════════

/** یک لینک vless://uuid@host:port?params#remark رو به فیلدهای جدا تبدیل می‌کنه. */
function parseVlessLink(link) {
  try {
    const u = new URL(link.trim());
    if (u.protocol !== 'vless:') return null;
    const uuid = decodeURIComponent(u.username || '');
    const host = u.hostname;
    const port = parseInt(u.port || '443', 10);
    const p = u.searchParams;
    return {
      uuid, host, port,
      type: p.get('type') || 'ws',
      security: p.get('security') || 'none',
      path: decodeURIComponent(p.get('path') || '/'),
      sni: p.get('sni') || p.get('host') || '',
      hostHeader: p.get('host') || '',
      fp: p.get('fp') || '',
      remark: decodeURIComponent((u.hash || '').replace(/^#/, '')) || 'XLevelUp',
    };
  } catch (_) {
    return null;
  }
}

function wsUpgradeProbe(host, path, timeoutMs) {
  const t0 = Date.now();
  const target = `https://${host}${path && path.startsWith('/') ? path : '/' + (path || '')}`;
  const key = btoa(String(Math.random())).slice(0, 22);
  return fetchWithTimeout(
    target,
    { headers: { Upgrade: 'websocket', Connection: 'Upgrade', 'Sec-WebSocket-Version': '13', 'Sec-WebSocket-Key': key } },
    timeoutMs || 4000
  )
    .then((resp) => {
      const ms = Date.now() - t0;
      if (resp.webSocket) {
        try {
          resp.webSocket.accept();
          resp.webSocket.close();
        } catch (_) {}
        return { ok: true, ms, status: resp.status };
      }
      return { ok: false, ms, status: resp.status, error: `سرور Upgrade را قبول نکرد (HTTP ${resp.status})` };
    })
    .catch((e) => ({ ok: false, ms: Date.now() - t0, error: String((e && e.message) || e) }));
}

/** پیشنهاد تعمیر خودکار بر اساس دلیل شکست هر مرحله */
function suggestFix(stepId, detail) {
  const map = {
    uuid: 'Fix UUID — UUID باید دقیقاً فرمت 8-4-4-4-12 هگزادسیمال باشد و با UUID مجاز روی edge/بک‌اند یکسان باشد.',
    domain: 'Fix Address — هاست/آدرس مقصد نامعتبر یا خالی است؛ یک دامنه یا IP معتبر وارد کنید.',
    sni: 'Set SNI = host — وقتی SNI خالی یا متفاوت از Host است، اکثر سرورها هندشیک TLS را رد می‌کنند.',
    tcp: 'Backend Offline / Port Closed — پورت مقصد باز نیست؛ فایروال، پورت اشتباه یا سرویس خاموش را بررسی کنید.',
    tls: 'TLS Failed — گواهی دامنه/SNI را بررسی کنید یا از security=none روی مسیرهای غیر-TLS استفاده کنید.',
    ws: 'Invalid Path — مسیر WebSocket را با مسیر تنظیم‌شده در سرور (مثلاً /ws/<uuid>) مطابقت دهید.',
    syntax: 'Fix Config Syntax — لینک vless:// یا فیلدهای کانفیگ ناقص/نامعتبر است.',
  };
  return map[stepId] || `بررسی دستی لازم است: ${detail || stepId}`;
}

async function runValidationEngine(fields, request) {
  const steps = [];
  const push = (id, name, ok, detail, weight, ms) => steps.push({ id, name, ok, detail, weight, ms: ms ?? null });

  // 1) Syntax
  const hasCore = !!(fields && fields.uuid && fields.host && fields.port);
  push('syntax', 'Syntax Validation', hasCore, hasCore ? 'فیلدهای اصلی (uuid/host/port) کامل هستند' : 'فیلد الزامی خالی است', 10);

  // 2) UUID
  const uuidOk = !!(fields.uuid && UUID_RE.test(fields.uuid));
  push('uuid', 'UUID Validation', uuidOk, uuidOk ? 'UUID فرمت معتبر دارد' : `UUID نامعتبر: "${fields.uuid || ''}"`, 15);

  // 3) Domain
  const domainOk = !!(fields.host && /^[a-zA-Z0-9.\-:]+$/.test(fields.host) && fields.host.length <= 253);
  push('domain', 'Domain Validation', domainOk, domainOk ? `آدرس مقصد: ${fields.host}` : 'آدرس مقصد نامعتبر یا خالی', 10);

  // 4) SNI — auto sni=host اگر خالی بود
  let sni = fields.sni;
  let sniAuto = false;
  if (!sni) {
    sni = fields.host;
    sniAuto = true;
  }
  const sniOk = sni === fields.host || fields.security !== 'tls' || sniAuto;
  push(
    'sni', 'SNI Validation', sniOk,
    sniAuto ? `SNI خالی بود → به‌صورت خودکار روی host تنظیم شد (${sni})` : sni === fields.host ? `SNI با host یکسان است (${sni})` : `SNI (${sni}) با host (${fields.host}) مغایرت دارد`,
    10
  );

  // 5) TCP Handshake — واقعی
  let tcpRes = { ok: false };
  if (domainOk) tcpRes = await tcpProbe(fields.host, fields.port, 3000);
  push('tcp', 'TCP Handshake Test', tcpRes.ok, tcpRes.ok ? `اتصال TCP موفق در ${tcpRes.ms}ms` : `اتصال TCP ناموفق: ${tcpRes.error || 'timeout'}`, 20, tcpRes.ms);

  // 6) TLS Handshake — فقط اگر security=tls
  let tlsStep = { ok: true, detail: 'N/A — پروتکل بدون TLS (security=none)' };
  if (fields.security === 'tls' && tcpRes.ok) {
    const t = await tlsProbe(fields.host, fields.port, 3000);
    tlsStep = { ok: t.ok, detail: t.ok ? `TLS handshake موفق در ${t.tls_ms}ms` : `TLS handshake ناموفق: ${t.error}` };
  } else if (fields.security === 'tls' && !tcpRes.ok) {
    tlsStep = { ok: false, detail: 'TCP رد شد؛ تست TLS انجام نشد' };
  }
  push('tls', 'TLS Handshake Test', tlsStep.ok, tlsStep.detail, 15);

  // 7) WebSocket Upgrade Test — واقعی، از طریق fetch() با هدر Upgrade
  let wsStep = { ok: true, detail: 'N/A — نوع transport این کانفیگ ws نیست' };
  if ((fields.type || 'ws') === 'ws' && domainOk) {
    const w = await wsUpgradeProbe(fields.host, fields.path || '/', 4000);
    wsStep = { ok: w.ok, detail: w.ok ? `WebSocket Upgrade موفق (${w.status}) در ${w.ms}ms` : `WebSocket Upgrade ناموفق: ${w.error}` };
  }
  push('ws', 'WebSocket Upgrade Test', wsStep.ok, wsStep.detail, 15);

  // 8) Transport Test (بر اساس Protocol Lab registry)
  const protoKey = fields.type === 'xhttp' ? 'vless-xhttp' : fields.type === 'grpc' ? 'grpc' : 'vless-ws';
  const proto = PROTOCOLS[protoKey];
  push('transport', 'Transport Test', proto.edgeTestable === true, `${proto.name}: ${proto.desc}`, 5);

  // 9) Latency + Stability (از روی نمونه‌ی TCP بالا)
  const latencyMs = tcpRes.ms || null;
  push('latency', 'Latency Test', tcpRes.ok, latencyMs != null ? `${latencyMs}ms` : 'در دسترس نیست', 0, latencyMs);

  // ── Health Score وزن‌دار ──
  const totalWeight = steps.reduce((a, s) => a + s.weight, 0) || 1;
  const earned = steps.reduce((a, s) => a + (s.ok ? s.weight : 0), 0);
  let score = Math.round((earned / totalWeight) * 100);
  if (tcpRes.ok && latencyMs != null) {
    // بونوس/جریمه‌ی کوچک بر اساس تأخیر واقعی، سقف ±5 امتیاز
    score += latencyMs < 150 ? 5 : latencyMs > 500 ? -5 : 0;
  }
  score = Math.max(0, Math.min(100, score));

  const failedSteps = steps.filter((s) => !s.ok);
  const status = score >= 80 ? 'active' : score >= 50 ? 'warning' : 'failed';
  const suggestions = failedSteps.map((s) => ({ step: s.id, problem: s.detail, suggestion: suggestFix(s.id, s.detail) }));

  const result = {
    ok: true,
    engine: 'Vlees Validation Engine (AI)',
    config: { uuid: fields.uuid, host: fields.host, port: fields.port, type: fields.type, security: fields.security, path: fields.path, sni },
    steps,
    score,
    status, // active | warning | failed
    status_label: status === 'active' ? '🟢 Active' : status === 'warning' ? '🟡 Warning' : '🔴 Failed',
    suggestions,
    tested_at: new Date().toISOString(),
  };

  validationLog.push({ host: fields.host, uuid: fields.uuid, score, status, ts: Date.now() });
  if (validationLog.length > VALIDATION_LOG_MAX) validationLog.shift();

  return result;
}

async function handleValidate(request, url) {
  let body = {};
  try {
    body = await request.json();
  } catch (_) {
    return jsonResp({ ok: false, error: 'بدنه‌ی JSON نامعتبر است' });
  }
  let fields = body.link ? parseVlessLink(body.link) : body;
  if (!fields) return jsonResp({ ok: false, error: 'لینک vless:// نامعتبر است' });
  fields = {
    uuid: fields.uuid || '',
    host: fields.host || url.hostname,
    port: parseInt(fields.port || 443, 10),
    type: fields.type || 'ws',
    security: fields.security || 'tls',
    path: fields.path || '/',
    sni: fields.sni || '',
  };
  const result = await runValidationEngine(fields, request);
  return jsonResp(result);
}

/** Config Center: یک کانفیگ VLESS جدید می‌سازه (با auto-SNI و مسیر درست)
 *  و بلافاصله همون کانفیگ رو از Validation Engine رد می‌کنه — طبق قانون
 *  «هیچ کانفیگی بدون اعتبارسنجی نمایش داده نشه». */
async function handleConfigBuild(request, url) {
  let body = {};
  try {
    body = await request.json();
  } catch (_) {
    return jsonResp({ ok: false, error: 'بدنه‌ی JSON نامعتبر است' });
  }
  const uuid = (body.uuid || '').trim();
  if (!UUID_RE.test(uuid)) return jsonResp({ ok: false, error: 'UUID نامعتبر است', suggestion: suggestFix('uuid') });
  const host = (body.host || url.hostname).trim();
  const port = parseInt(body.port || 443, 10);
  const type = body.type || 'ws';
  const security = body.security || 'tls';
  const path = body.path && body.path.startsWith('/') ? body.path : `/ws/${uuid}`;
  const sni = body.sni || host; // ── هوشمند: پیش‌فرض sni = host ──
  const remark = body.remark || 'XLevelUp';

  const link =
    `vless://${uuid}@${host}:${port}?encryption=none&security=${security}&type=${type}` +
    `&host=${encodeURIComponent(host)}&path=${encodeURIComponent(path)}&sni=${encodeURIComponent(sni)}&fp=chrome` +
    `#${encodeURIComponent(remark)}`;

  const validation = await runValidationEngine({ uuid, host, port, type, security, path, sni }, request);
  return jsonResp({ ok: true, link, validation });
}

/** X5.3: Secure Sync API با Railway Core — دو جهته:
 *  GET  → وضعیت/تله‌متری فعلی این ایزوله (برای اینکه Railway بکشه)
 *  POST → پیام sync از Railway (مثلاً invalidate کش کانفیگ) — با X-Edge-Secret */
async function handleSync(request, cfg, ctx) {
  const provided = request.headers.get('x-edge-secret') || '';
  if (!cfg.edgeSecret || provided !== cfg.edgeSecret) {
    return jsonResp({ ok: false, error: 'EDGE_SECRET نامعتبر یا تنظیم‌نشده — /api/sync قفل است' , status: 'unauthorized' });
  }
  if (request.method === 'GET') {
    return jsonResp({
      ok: true,
      isolate_cache_age_ms: Date.now() - isolateConfigCache.ts,
      cached_configs: isolateConfigCache.list.length,
      open_circuits: Array.from(circuitBreakers.entries()).filter(([, c]) => Date.now() < c.openUntil).length,
      recent_validations: validationLog.slice(-10),
      speed_buckets_active: speedBuckets.size,
    });
  }
  if (request.method === 'POST') {
    let body = {};
    try {
      body = await request.json();
    } catch (_) {}
    if (body.invalidate_config_cache) {
      isolateConfigCache = { ts: 0, list: [], byUuid: new Map() };
    }
    return jsonResp({ ok: true, applied: Object.keys(body || {}) });
  }
  return jsonResp({ ok: false, error: 'method not allowed' });
}

/** داشبورد مستقل تک‌فایلی Worker — بدون هیچ وابستگی به بک‌اند Railway.
 *  هاست/SNI خودش رو از location.hostname توی مرورگر تشخیص می‌ده (دقیقاً
 *  همون دامنه‌ای که کاربر داره باهاش صحبت می‌کنه) — نیازی به هاردکد نیست. */
/** داشبورد مستقل تک‌فایلی Worker — XLevelUp Intelligence Edition X5.3
 *  بدون هیچ وابستگی به بک‌اند Railway برای رندر اولیه. هاست/SNI خودش رو از
 *  location.hostname توی مرورگر تشخیص می‌ده — نیازی به هاردکد نیست.
 *  طراحی: Glassmorphism / Cyber-Premium، انیمیشن‌های زنده، موبایل‌فرندلی. */
function renderDashboardHTML() {
  return `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>XLevelUp X5.3 — Intelligence Dashboard</title>
<style>
  :root{
    --bg:#070a12; --bg2:#0c1120; --glass:rgba(20,26,45,.55); --glass-brd:rgba(255,255,255,.08);
    --txt:#e8ecf7; --mut:#8b96b8; --ok:#22e592; --bad:#ff4d6d; --warn:#ffb84d; --acc:#7c5cff; --acc2:#22d3ee;
  }
  *{box-sizing:border-box; -webkit-tap-highlight-color:transparent}
  html,body{margin:0;padding:0;background:var(--bg);color:var(--txt);font-family:-apple-system,Segoe UI,Roboto,sans-serif;overflow-x:hidden}
  body{
    background:
      radial-gradient(1200px 600px at 15% -10%, rgba(124,92,255,.25), transparent 60%),
      radial-gradient(900px 500px at 110% 10%, rgba(34,211,238,.18), transparent 55%),
      linear-gradient(180deg,var(--bg),var(--bg2));
    min-height:100vh;
  }
  @keyframes floatBg{0%{background-position:0% 0%}100%{background-position:200% 200%}}
  @keyframes pulseDot{0%{box-shadow:0 0 0 0 rgba(34,229,146,.55)}70%{box-shadow:0 0 0 9px rgba(34,229,146,0)}100%{box-shadow:0 0 0 0 rgba(34,229,146,0)}}
  @keyframes pulseDotBad{0%{box-shadow:0 0 0 0 rgba(255,77,109,.55)}70%{box-shadow:0 0 0 9px rgba(255,77,109,0)}100%{box-shadow:0 0 0 0 rgba(255,77,109,0)}}
  @keyframes shine{0%{background-position:-150% 0}100%{background-position:250% 0}}
  @keyframes fadeUp{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
  @keyframes spin{to{transform:rotate(360deg)}}

  header{padding:16px 16px 10px;position:sticky;top:0;z-index:20;background:rgba(7,10,18,.72);backdrop-filter:blur(14px);border-bottom:1px solid var(--glass-brd)}
  .topline{display:flex;align-items:center;justify-content:space-between;gap:10px}
  .brand{display:flex;align-items:center;gap:10px}
  .logo{
    width:36px;height:36px;border-radius:11px;flex:0 0 auto;
    background:linear-gradient(135deg,var(--acc),var(--acc2));
    display:flex;align-items:center;justify-content:center;font-weight:800;font-size:15px;color:#fff;
    position:relative;overflow:hidden;box-shadow:0 4px 18px rgba(124,92,255,.45);
  }
  .logo::after{
    content:'';position:absolute;inset:0;
    background:linear-gradient(100deg,transparent 35%,rgba(255,255,255,.55) 50%,transparent 65%);
    background-size:250% 100%; animation:shine 2.8s ease-in-out infinite;
  }
  h1{font-size:15px;margin:0;font-weight:700;letter-spacing:.2px}
  .hostTag{font-size:11px;color:var(--mut);direction:ltr;display:block}
  .badge{
    font-size:10px;padding:3px 9px;border-radius:20px;font-weight:700;letter-spacing:.4px;
    background:linear-gradient(90deg,rgba(124,92,255,.25),rgba(34,211,238,.25));
    border:1px solid rgba(124,92,255,.4); color:#cfd6ff;
  }
  .creator{font-size:10.5px;color:var(--mut);margin-top:2px}
  .creator b{color:#cfd6ff}

  nav{display:flex;overflow-x:auto;gap:7px;padding:10px 12px 12px;-webkit-overflow-scrolling:touch;scrollbar-width:none}
  nav::-webkit-scrollbar{display:none}
  nav button{
    flex:0 0 auto;background:var(--glass);border:1px solid var(--glass-brd);color:var(--txt);
    padding:9px 14px;border-radius:14px;font-size:12.5px;white-space:nowrap;backdrop-filter:blur(8px);
    transition:all .18s ease;
  }
  nav button.active{background:linear-gradient(90deg,var(--acc),#5a3fe0);border-color:transparent;color:#fff;box-shadow:0 4px 14px rgba(124,92,255,.4)}

  main{padding:14px 14px 40px;max-width:720px;margin:0 auto}
  .tab{display:none}
  .tab.active{display:block;animation:fadeUp .35s ease}

  .card{
    background:var(--glass);border:1px solid var(--glass-brd);border-radius:16px;padding:15px;margin-bottom:13px;
    backdrop-filter:blur(14px);box-shadow:0 8px 24px rgba(0,0,0,.25);
  }
  .card h3{margin:0 0 10px;font-size:13px;color:#cfd6ff;display:flex;align-items:center;gap:7px}
  .row{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px dashed var(--glass-brd);font-size:12.5px;gap:10px}
  .row:last-child{border-bottom:none}
  .row span{color:var(--mut)}
  .row b{text-align:left;direction:ltr;word-break:break-all}
  .mut{color:var(--mut)}
  .small{font-size:11px;color:var(--mut);line-height:1.7}

  .dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-inline-end:7px;flex:0 0 auto}
  .dot.ok{background:var(--ok);animation:pulseDot 1.8s infinite}
  .dot.bad{background:var(--bad);animation:pulseDotBad 1.8s infinite}
  .dot.warn{background:var(--warn)}

  .statline{display:flex;align-items:flex-start;gap:8px;padding:9px 0;border-bottom:1px dashed var(--glass-brd)}
  .statline:last-child{border-bottom:none}
  .statline .label{font-size:12.5px;font-weight:600}
  .statline .reason{font-size:11px;color:var(--mut);margin-top:2px;line-height:1.6}
  .statdotwrap{padding-top:3px}

  .scorewrap{display:flex;align-items:center;gap:12px;margin:6px 0 2px}
  .scorering{
    width:58px;height:58px;border-radius:50%;flex:0 0 auto;
    display:flex;align-items:center;justify-content:center;font-weight:800;font-size:15px;
    background:conic-gradient(var(--ok) calc(var(--pct,0) * 1%), rgba(255,255,255,.08) 0);
    position:relative;
  }
  .scorering::before{content:'';position:absolute;inset:5px;border-radius:50%;background:#0c1120}
  .scorering span{position:relative;z-index:1}
  .statusPill{font-size:11px;padding:4px 10px;border-radius:20px;font-weight:700;display:inline-block}
  .statusPill.active{background:rgba(34,229,146,.15);color:var(--ok);border:1px solid rgba(34,229,146,.4)}
  .statusPill.warning{background:rgba(255,184,77,.15);color:var(--warn);border:1px solid rgba(255,184,77,.4)}
  .statusPill.failed{background:rgba(255,77,109,.15);color:var(--bad);border:1px solid rgba(255,77,109,.4)}

  .steplist{margin-top:10px}
  .step{display:flex;gap:8px;align-items:flex-start;padding:6px 0;font-size:11.5px}
  .step .icon{flex:0 0 auto;width:16px;text-align:center}
  .step .txt b{display:block;font-size:12px}
  .step .txt span{color:var(--mut)}
  .suggestbox{margin-top:8px;padding:9px 10px;border-radius:10px;background:rgba(255,184,77,.08);border:1px solid rgba(255,184,77,.25);font-size:11.5px}

  input,textarea,select{
    width:100%;background:rgba(255,255,255,.04);border:1px solid var(--glass-brd);color:var(--txt);
    padding:10px 11px;border-radius:10px;font-size:13px;margin:6px 0;outline:none;
  }
  input:focus,textarea:focus,select:focus{border-color:var(--acc)}
  textarea{min-height:88px;font-family:monospace;direction:ltr;text-align:left}
  label.small{display:block;margin-top:6px}

  button.act{
    background:linear-gradient(90deg,var(--acc),#5a3fe0);color:#fff;border:none;padding:11px 14px;
    border-radius:11px;font-size:13px;width:100%;margin-top:8px;font-weight:700;letter-spacing:.2px;
    box-shadow:0 6px 18px rgba(124,92,255,.35); transition:transform .12s ease;
  }
  button.act:active{transform:scale(.98)}
  button.act:disabled{opacity:.5}
  button.ghost{background:transparent;border:1px solid var(--glass-brd);color:var(--txt);box-shadow:none}

  .spinner{width:13px;height:13px;border:2px solid rgba(255,255,255,.35);border-top-color:#fff;border-radius:50%;display:inline-block;animation:spin .7s linear infinite;vertical-align:-2px;margin-left:6px}

  table{width:100%;border-collapse:collapse;font-size:11.5px;margin-top:8px;direction:ltr}
  th,td{padding:7px 5px;border-bottom:1px solid var(--glass-brd);text-align:left}
  th{color:var(--mut);font-weight:600;font-size:10.5px;text-transform:uppercase}
  code{background:rgba(255,255,255,.06);padding:2px 6px;border-radius:6px;font-size:11.5px;word-break:break-all;direction:ltr;display:inline-block}
  .linkbox{word-break:break-all;background:rgba(255,255,255,.05);border:1px solid var(--glass-brd);padding:11px;border-radius:11px;font-family:monospace;font-size:11.5px;direction:ltr;text-align:left}

  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
  .protocard{background:rgba(255,255,255,.04);border:1px solid var(--glass-brd);border-radius:12px;padding:10px;margin-bottom:8px}
  .protocard .ph{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
  .ptag{font-size:9.5px;padding:2px 8px;border-radius:10px;font-weight:700}
  .ptag.live{background:rgba(34,229,146,.15);color:var(--ok)}
  .ptag.partial{background:rgba(255,184,77,.15);color:var(--warn)}
  .ptag.ready{background:rgba(139,150,184,.18);color:var(--mut)}

  .liveTicker{display:flex;gap:8px;overflow-x:auto;padding-bottom:2px;-webkit-overflow-scrolling:touch;scrollbar-width:none}
  .liveTicker::-webkit-scrollbar{display:none}
  .liveChip{flex:0 0 auto;background:rgba(255,255,255,.04);border:1px solid var(--glass-brd);border-radius:10px;padding:6px 10px;font-size:10.5px;color:var(--mut)}
  .liveChip b{color:var(--txt)}
</style>
</head>
<body>
<header>
  <div class="topline">
    <div class="brand">
      <div class="logo">X5</div>
      <div>
        <h1>XLevelUp <span class="badge">Intelligence Edition</span></h1>
        <span class="hostTag" id="hostTag">—</span>
      </div>
    </div>
  </div>
  <div class="creator">Creator: <b>Ily</b> · X5.3 · Vlees Validation Engine (AI)</div>
</header>

<nav id="tabs">
  <button data-t="engine" class="active">⚡ Engine</button>
  <button data-t="validate">🧪 Validation AI</button>
  <button data-t="scanner">📡 IP Scanner</button>
  <button data-t="protocols">🧬 Protocol Lab</button>
  <button data-t="config">🛠 Config Center</button>
  <button data-t="statusTab">🩺 System Status</button>
  <button data-t="backend">🔗 Backend/Sync</button>
</nav>

<main>

  <!-- ── Engine ─────────────────────────────────────────────────────────── -->
  <div class="tab active" id="tab-engine">
    <div class="card">
      <h3>⚡ Edge Engine</h3>
      <div class="row"><span>Engine</span><b id="e-engine">—</b></div>
      <div class="row"><span>Mode</span><b id="e-mode">—</b></div>
      <div class="row"><span>Colo</span><b id="e-colo">—</b></div>
      <div class="row"><span>Country</span><b id="e-country">—</b></div>
      <div class="row"><span>Backend reachable</span><b id="e-backend">—</b></div>
      <div class="row"><span>Fail-open</span><b id="e-failopen">—</b></div>
      <div class="row"><span>Circuit breakers باز</span><b id="e-cb">—</b></div>
      <button class="act" onclick="loadEngine()">Refresh <span id="e-spin"></span></button>
    </div>
    <div class="card">
      <h3>📈 Live Feed</h3>
      <div class="liveTicker" id="liveTicker"><span class="liveChip">در حال بارگذاری…</span></div>
    </div>
  </div>

  <!-- ── Validation Engine (AI) ─────────────────────────────────────────── -->
  <div class="tab" id="tab-validate">
    <div class="card">
      <h3>🧪 Vlees Validation Engine <span class="badge">AI</span></h3>
      <p class="small">یک لینک <code>vless://</code> بده یا خودت خالی بذار تا UUID فعلی همین ورکر رو تست کنه. هر مرحله (Syntax → UUID → Domain → SNI → TCP → TLS → WebSocket → Transport → Latency) واقعاً روی شبکه اجرا می‌شه.</p>
      <label class="small">لینک vless:// (اختیاری)</label>
      <textarea id="valLink" placeholder="vless://uuid@host:443?type=ws&security=tls&path=/ws/uuid&sni=host#Remark"></textarea>
      <button class="act" id="valBtn" onclick="runValidation()">اجرای Validation Engine <span id="val-spin"></span></button>
      <div id="valOut" style="margin-top:12px"></div>
    </div>
  </div>

  <!-- ── IP Scanner ─────────────────────────────────────────────────────── -->
  <div class="tab" id="tab-scanner">
    <div class="card">
      <h3>📡 IP Intelligence Scanner</h3>
      <p class="small">هاست فعلی به‌عنوان مقصد پیش‌فرض SNI/Host استفاده می‌شه: <code id="sniShown">—</code></p>
      <label class="small">لیست IP (هرکدام یک خط)</label>
      <textarea id="ipList">104.16.0.0
104.17.0.0
104.18.0.0
1.1.1.1
1.0.0.1</textarea>
      <label class="small">حالت اسکن</label>
      <select id="scanMode">
        <option value="deep">Deep (TCP + TLS + Jitter + Score)</option>
        <option value="fast">Fast (فقط TCP)</option>
      </select>
      <button class="act" onclick="scanIPs()" id="scanBtn">Scan <span id="scan-spin"></span></button>
      <div id="bestNode"></div>
      <div id="scanResults"></div>
    </div>
  </div>

  <!-- ── Protocol Lab ───────────────────────────────────────────────────── -->
  <div class="tab" id="tab-protocols">
    <div class="card">
      <h3>🧬 Protocol Lab</h3>
      <p class="small">وضعیت هر پروتکل صادقانه نشون داده می‌شه: چه چیزی از خودِ edge واقعاً قابل تست/تونل زدنه و چه چیزی فقط Config Builder داره.</p>
      <div id="protoList">در حال بارگذاری…</div>
    </div>
    <div class="card">
      <h3>🧠 Smart Transport Engine</h3>
      <label class="small">حالت مصرف</label>
      <select id="stMode">
        <option value="stable">Stable</option>
        <option value="gaming">Gaming</option>
        <option value="long-session">Long Session</option>
        <option value="mobile">Mobile / Adaptive</option>
      </select>
      <button class="act" onclick="loadRecommend()">پیشنهاد بگیر</button>
      <div id="recommendOut" style="margin-top:10px"></div>
    </div>
  </div>

  <!-- ── Config Center ──────────────────────────────────────────────────── -->
  <div class="tab" id="tab-config">
    <div class="card">
      <h3>🛠 Config Center — Build + Validate</h3>
      <label class="small">UUID</label>
      <input id="cfgUuid" placeholder="مثلاً 41dca55b-7cbe-43a3-9915-0470cb7aca0a">
      <label class="small">Type</label>
      <select id="cfgType"><option value="ws">WebSocket</option><option value="xhttp">XHTTP</option><option value="grpc">gRPC</option></select>
      <label class="small">Security</label>
      <select id="cfgSecurity"><option value="tls">TLS</option><option value="none">None</option></select>
      <label class="small">Remark</label>
      <input id="cfgRemark" value="XLevelUp">
      <button class="act" onclick="buildConfig()" id="cfgBtn">Build + Validate <span id="cfg-spin"></span></button>
      <div id="cfgOut" style="margin-top:12px"></div>
    </div>
  </div>

  <!-- ── System Status ──────────────────────────────────────────────────── -->
  <div class="tab" id="tab-statusTab">
    <div class="card">
      <h3>🩺 System Status — Live</h3>
      <div id="componentsOut">در حال بارگذاری…</div>
      <button class="act ghost" onclick="loadEngine()">Refresh</button>
    </div>
    <div class="card">
      <h3>🔌 WebSocket Handshake — تست دستی</h3>
      <label class="small">UUID برای تست Handshake</label>
      <input id="statusUuid" placeholder="UUID">
      <button class="act" onclick="testHandshake()" id="hsBtn">تست واقعی WebSocket Handshake</button>
      <div id="hsOut" style="margin-top:10px"></div>
      <p class="small">این تست یک WebSocket واقعی از همین مرورگر به <code>/ws/&lt;uuid&gt;</code> باز می‌کند — مستقل از اینکه کلاینت VLESS چه می‌گوید.</p>
    </div>
  </div>

  <!-- ── Backend / Sync ─────────────────────────────────────────────────── -->
  <div class="tab" id="tab-backend">
    <div class="card">
      <h3>🔗 Backend Mode</h3>
      <div class="row"><span>حالت فعلی</span><b id="b-mode">—</b></div>
      <div class="row"><span>Backend Origin</span><b id="b-origin">—</b></div>
      <div class="row"><span>در دسترس؟</span><b id="b-reach">—</b></div>
      <p class="small">Standalone = فقط خود Worker کانفیگ می‌سازد. Hybrid = Worker به Railway وصل است و Railway منطق پیشرفته را انجام می‌دهد.</p>
    </div>
    <div class="card">
      <h3>🔁 Link URL Builder (سریع)</h3>
      <label class="small">UUID</label>
      <input id="linkUuid" placeholder="مثلاً 41dca55b-7cbe-43a3-9915-0470cb7aca0a">
      <label class="small">Remark</label>
      <input id="linkRemark" value="XLevelUp">
      <button class="act" onclick="buildLink()">Build Link</button>
      <div id="linkOut" style="margin-top:10px"></div>
    </div>
  </div>

</main>

<script>
var HOST = location.hostname;
document.getElementById('hostTag').textContent = HOST;
document.getElementById('sniShown').textContent = HOST;

document.querySelectorAll('#tabs button').forEach(function(btn){
  btn.onclick = function(){
    document.querySelectorAll('#tabs button').forEach(function(b){ b.classList.remove('active'); });
    document.querySelectorAll('.tab').forEach(function(t){ t.classList.remove('active'); });
    btn.classList.add('active');
    document.getElementById('tab-'+btn.dataset.t).classList.add('active');
    if(btn.dataset.t === 'protocols') loadProtocols();
  };
});

function dotHtml(status){
  var cls = status === 'green' ? 'ok' : status === 'red' ? 'bad' : 'warn';
  return '<span class="dot '+cls+'"></span>';
}

function pushTicker(text){
  var el = document.getElementById('liveTicker');
  if(!el) return;
  var chip = document.createElement('span');
  chip.className = 'liveChip';
  chip.innerHTML = text;
  el.insertBefore(chip, el.firstChild);
  while(el.children.length > 8) el.removeChild(el.lastChild);
}

async function loadEngine(){
  var spin = document.getElementById('e-spin'); if(spin) spin.innerHTML = '<span class="spinner"></span>';
  try{
    var r = await fetch('/api/status'); var d = await r.json();
    document.getElementById('e-engine').textContent = d.engine || '—';
    document.getElementById('e-mode').textContent = d.mode || '—';
    document.getElementById('e-colo').textContent = d.colo || '—';
    document.getElementById('e-country').textContent = d.country || '—';
    document.getElementById('e-backend').textContent = d.backend_reachable===null? 'N/A' : (d.backend_reachable? 'بله':'خیر');
    document.getElementById('e-failopen').textContent = d.fail_open? 'فعال':'غیرفعال';
    document.getElementById('e-cb').textContent = (d.intelligence && d.intelligence.circuit_breaker_open_now.length) || 0;
    document.getElementById('b-mode').textContent = d.mode || '—';
    document.getElementById('b-origin').textContent = d.backend_origin || '(standalone — تنظیم نشده)';
    document.getElementById('b-reach').textContent = d.backend_reachable===null? 'N/A' : (d.backend_reachable? 'بله':'خیر');

    var compHtml = '';
    (d.components||[]).forEach(function(c){
      compHtml += '<div class="statline"><div class="statdotwrap">'+dotHtml(c.status)+'</div><div><div class="label">'+c.label+'</div><div class="reason">'+c.reason+'</div></div></div>';
    });
    document.getElementById('componentsOut').innerHTML = compHtml || '—';

    pushTicker('<b>'+ (d.mode||'—') +'</b> · colo '+(d.colo||'—')+' · CB باز: '+((d.intelligence&&d.intelligence.circuit_breaker_open_now.length)||0));
  }catch(e){ document.getElementById('e-engine').textContent = 'خطا: '+e.message; }
  if(spin) spin.innerHTML = '';
}
loadEngine();
setInterval(loadEngine, 15000);

function scoreRingHtml(score, statusLabel){
  return '<div class="scorewrap"><div class="scorering" style="--pct:'+score+'"><span>'+score+'</span></div><div><div class="statusPill '+
    (score>=80?'active':score>=50?'warning':'failed')+'">'+statusLabel+'</div></div></div>';
}

function stepIcon(ok){ return ok ? '🟢' : '🔴'; }

async function runValidation(){
  var btn = document.getElementById('valBtn'); var spin = document.getElementById('val-spin');
  btn.disabled = true; spin.innerHTML = '<span class="spinner"></span>';
  var out = document.getElementById('valOut');
  out.innerHTML = '<p class="small">در حال اجرای مراحل Validation Engine…</p>';
  var link = document.getElementById('valLink').value.trim();
  try{
    var body = link ? {link: link} : {uuid: document.getElementById('statusUuid').value.trim() || undefined, host: HOST};
    var r = await fetch('/api/validate', {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(body)});
    var d = await r.json();
    if(!d.ok){ out.innerHTML = '<p class="small" style="color:#ff4d6d">خطا: '+(d.error||'نامشخص')+'</p>'; btn.disabled=false; spin.innerHTML=''; return; }
    var html = scoreRingHtml(d.score, d.status_label);
    html += '<div class="steplist">';
    (d.steps||[]).forEach(function(s){
      html += '<div class="step"><div class="icon">'+stepIcon(s.ok)+'</div><div class="txt"><b>'+s.name+'</b><span>'+s.detail+(s.ms!=null?(' — '+s.ms+'ms'):'')+'</span></div></div>';
    });
    html += '</div>';
    if((d.suggestions||[]).length){
      html += '<div class="suggestbox"><b>توصیه‌ی تعمیر خودکار:</b><br>';
      d.suggestions.forEach(function(s){ html += '• '+s.suggestion+'<br>'; });
      html += '</div>';
    }
    out.innerHTML = html;
    pushTicker('Validation: <b>'+d.status_label+'</b> ('+d.score+'/100) — '+(d.config&&d.config.host||''));
  }catch(e){ out.innerHTML = '<p class="small" style="color:#ff4d6d">خطا: '+e.message+'</p>'; }
  btn.disabled = false; spin.innerHTML = '';
}

async function scanIPs(){
  var btn = document.getElementById('scanBtn'); var spin = document.getElementById('scan-spin');
  btn.disabled = true; spin.innerHTML = '<span class="spinner"></span>';
  var ips = document.getElementById('ipList').value.split('\\n').map(function(s){return s.trim();}).filter(Boolean);
  var deep = document.getElementById('scanMode').value === 'deep';
  try{
    var r = await fetch('/api/scan', {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ips:ips, port:443, deep:deep, samples:3})});
    var d = await r.json();
    if(d.best_node){
      document.getElementById('bestNode').innerHTML = '<div class="card" style="margin-top:10px;border-color:rgba(34,229,146,.4)"><h3>🏆 Best Node</h3><div class="row"><span>IP</span><b>'+d.best_node.ip+'</b></div><div class="row"><span>Latency</span><b>'+d.best_node.avg_latency_ms+'ms</b></div><div class="row"><span>Score</span><b>'+d.best_node.score+'/100</b></div><div class="row"><span>Protocol پیشنهادی</span><b>'+d.best_node.recommended_protocol+'</b></div></div>';
    } else { document.getElementById('bestNode').innerHTML = ''; }

    var html = '<table><tr><th>IP</th><th>وضعیت</th><th>ms</th>'+(deep?'<th>Jitter</th><th>TLS</th><th>Score</th>':'')+'</tr>';
    (d.results||[]).forEach(function(x){
      html += '<tr><td>'+x.ip+'</td><td>'+dotHtml(x.ok?'green':'red')+(x.ok?'سالم':'قطع')+'</td><td>'+(x.ok? (x.avg_latency_ms!=null?x.avg_latency_ms:x.ms) : '-')+'</td>';
      if(deep){
        html += '<td>'+(x.jitter_ms!=null?x.jitter_ms+'ms':'-')+'</td><td>'+(x.tls&&x.tls.ok?'✅':'❌')+'</td><td>'+(x.score!=null?x.score:'-')+'</td>';
      }
      html += '</tr>';
    });
    html += '</table>';
    document.getElementById('scanResults').innerHTML = html;
    pushTicker('Scan: '+(d.count||0)+' IP · بهترین: <b>'+((d.best_node&&d.best_node.ip)||'—')+'</b>');
  }catch(e){ document.getElementById('scanResults').textContent = 'خطا: '+e.message; }
  btn.disabled=false; spin.innerHTML='';
}

async function loadProtocols(){
  try{
    var r = await fetch('/api/protocols'); var d = await r.json();
    var html = '';
    Object.keys(d.protocols||{}).forEach(function(k){
      var p = d.protocols[k];
      var tagCls = p.edgeTestable===true?'live':p.edgeTestable==='partial'?'partial':'ready';
      var tagTxt = p.edgeTestable===true?'Live Test':p.edgeTestable==='partial'?'Partial':'Config Only';
      html += '<div class="protocard"><div class="ph"><b>'+p.name+'</b><span class="ptag '+tagCls+'">'+tagTxt+'</span></div><div class="small">'+p.desc+'</div></div>';
    });
    document.getElementById('protoList').innerHTML = html;
  }catch(e){ document.getElementById('protoList').textContent = 'خطا: '+e.message; }
}

async function loadRecommend(){
  var mode = document.getElementById('stMode').value;
  try{
    var r = await fetch('/api/protocols?mode='+encodeURIComponent(mode));
    var d = await r.json();
    var rec = d.recommend;
    document.getElementById('recommendOut').innerHTML =
      '<div class="row"><span>Primary</span><b>'+rec.primary+'</b></div>'+
      '<div class="row"><span>Backup</span><b>'+(rec.backup||'—')+'</b></div>'+
      '<div class="row"><span>Fallback</span><b>'+(rec.fallback||'—')+'</b></div>'+
      '<p class="small">'+rec.reason+'</p>';
  }catch(e){ document.getElementById('recommendOut').textContent = 'خطا: '+e.message; }
}

async function buildConfig(){
  var btn = document.getElementById('cfgBtn'); var spin = document.getElementById('cfg-spin');
  var uuid = document.getElementById('cfgUuid').value.trim();
  var out = document.getElementById('cfgOut');
  if(!uuid){ out.innerHTML = '<p class="small" style="color:#ff4d6d">UUID را وارد کنید</p>'; return; }
  btn.disabled = true; spin.innerHTML = '<span class="spinner"></span>';
  out.innerHTML = '<p class="small">در حال ساخت و اعتبارسنجی کانفیگ…</p>';
  try{
    var body = {
      uuid: uuid, host: HOST, port: 443,
      type: document.getElementById('cfgType').value,
      security: document.getElementById('cfgSecurity').value,
      remark: document.getElementById('cfgRemark').value.trim() || 'XLevelUp',
    };
    var r = await fetch('/api/config/build', {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(body)});
    var d = await r.json();
    if(!d.ok){ out.innerHTML = '<p class="small" style="color:#ff4d6d">خطا: '+(d.error||'نامشخص')+'</p>'; btn.disabled=false; spin.innerHTML=''; return; }
    var v = d.validation;
    var html = '<div class="linkbox">'+d.link+'</div>';
    html += scoreRingHtml(v.score, v.status_label);
    if((v.suggestions||[]).length){
      html += '<div class="suggestbox"><b>توصیه:</b><br>';
      v.suggestions.forEach(function(s){ html += '• '+s.suggestion+'<br>'; });
      html += '</div>';
    }
    out.innerHTML = html;
    pushTicker('Config Build: <b>'+v.status_label+'</b> ('+v.score+'/100)');
  }catch(e){ out.innerHTML = '<p class="small" style="color:#ff4d6d">خطا: '+e.message+'</p>'; }
  btn.disabled = false; spin.innerHTML = '';
}

function buildLink(){
  var uuid = document.getElementById('linkUuid').value.trim();
  var remark = document.getElementById('linkRemark').value.trim() || 'XLevelUp';
  if(!uuid){ document.getElementById('linkOut').innerHTML = '<p class="small" style="color:#ff4d6d">UUID را وارد کنید</p>'; return; }
  var path = encodeURIComponent('/ws/'+uuid);
  var link = 'vless://'+uuid+'@'+HOST+':443?encryption=none&security=tls&type=ws&host='+HOST+'&path='+path+'&sni='+HOST+'&fp=chrome#'+encodeURIComponent(remark);
  document.getElementById('linkOut').innerHTML = '<div class="linkbox">'+link+'</div><p class="small">هاست/SNI به‌صورت خودکار از همین ساب‌دامین (<code>'+HOST+'</code>) گرفته شد.</p>';
}

async function testHandshake(){
  var uuid = document.getElementById('statusUuid').value.trim();
  var out = document.getElementById('hsOut');
  var btn = document.getElementById('hsBtn');
  if(!uuid){ out.innerHTML='<p class="small" style="color:#ff4d6d">UUID را وارد کنید</p>'; return; }
  btn.disabled = true; btn.innerHTML = 'در حال تست... <span class="spinner"></span>';
  out.innerHTML = '<p class="small">در حال باز کردن WebSocket به wss://'+HOST+'/ws/'+uuid+' ...</p>';
  var t0 = Date.now();
  try{
    var ws = new WebSocket('wss://'+HOST+'/ws/'+uuid);
    var result = await new Promise(function(resolve){
      var timer = setTimeout(function(){ resolve({ok:false, reason:'timeout (بدون پاسخ در ۵ ثانیه)'}); }, 5000);
      ws.onopen = function(){ clearTimeout(timer); resolve({ok:true, ms: Date.now()-t0}); };
      ws.onerror = function(){ clearTimeout(timer); resolve({ok:false, reason:'اتصال رد شد یا خطای شبکه'}); };
      ws.onclose = function(ev){ clearTimeout(timer); resolve({ok:false, reason:'بسته شد — کد '+ev.code}); };
    });
    try{ ws.close(); }catch(_){}
    if(result.ok){
      out.innerHTML = '<div class="row">'+dotHtml('green')+' Handshake موفق<b>'+result.ms+' ms</b></div><p class="small">Worker مسیر /ws/'+uuid+' را قبول کرد (101 Switching Protocols).</p>';
      pushTicker('Handshake ✅ '+result.ms+'ms');
    } else {
      out.innerHTML = '<div class="row">'+dotHtml('red')+' Handshake ناموفق<b>—</b></div><p class="small">'+result.reason+' — یعنی یا UUID مجاز نیست، یا مسیر اشتباه است، یا Worker/Backend این کانفیگ را رد کرده.</p>';
      pushTicker('Handshake ❌ '+result.reason);
    }
  }catch(e){
    out.innerHTML = '<p class="small" style="color:#ff4d6d">خطا: '+e.message+'</p>';
  }
  btn.disabled = false; btn.textContent = 'تست واقعی WebSocket Handshake';
}
</script>
</body>
</html>`;
}
