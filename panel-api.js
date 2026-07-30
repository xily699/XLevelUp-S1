/**
 * XLevelUp Panel API — Railway
 * وظیفه: نگه‌داری یک استخر تازه از بهترین IPهای کلودفلر (رفرش دوره‌ای در پس‌زمینه)
 * و تحویل یک فایل JSON کامل و آماده که خود Xray-core روی گوشی کاربر
 * با Observatory+Balancer(leastPing) به‌صورت زنده بهترین IP رو انتخاب می‌کنه.
 *
 * چرا این‌طوری؟ چون latency واقعی به موقعیت کاربره، نه به Railway.
 * پس این API فقط پول رو "تمیز و زنده" نگه می‌داره؛ تصمیم لحظه‌ای روی گوشی خودشه.
 */

const express = require("express");
const net = require("net");
const https = require("https");
const { randomUUID } = require("crypto");

const app = express();
app.use(express.json());

// اکثر پلتفرم‌ها (Railway, Render, Fly.io, Heroku) پورت رو با env PORT تزریق می‌کنن
const PORT = process.env.PORT || process.env.PANEL_PORT || 8080;
const DOMAIN = process.env.XLEVELUP_DOMAIN || "YOUR-DOMAIN.app"; // دامنه Railway (فالبک)
const UUID_VLESS = process.env.UUID_VLESS || "REPLACE-WITH-UUID-1";
const REFRESH_INTERVAL_MS = 15 * 60 * 1000; // هر ۱۵ دقیقه رفرش استخر IP در پس‌زمینه
const SAMPLE_PER_RANGE = 6; // چند IP نمونه از هر رنج CIDR رسمی کلودفلر تست بشه

// --- دریافت رنج رسمی IPv4 کلودفلر ---
function fetchCFRanges() {
  return new Promise((resolve, reject) => {
    https
      .get("https://www.cloudflare.com/ips-v4", (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve(data.trim().split("\n").filter(Boolean)));
      })
      .on("error", reject);
  });
}

// --- تبدیل یک CIDR به چند IP نمونه (برای اسکن سریع‌تر به‌جای کل رنج) ---
function sampleIPsFromCIDR(cidr, count) {
  const [base, bits] = cidr.split("/");
  const parts = base.split(".").map(Number);
  const hostBits = 32 - parseInt(bits, 10);
  const hostCount = Math.min(Math.pow(2, hostBits) - 2, 65534); // سقف امنی برای رنج‌های بزرگ
  const ips = [];
  const baseInt = (parts[0] << 24) + (parts[1] << 16) + (parts[2] << 8) + parts[3];
  for (let i = 0; i < count; i++) {
    const offset = Math.floor(Math.random() * hostCount) + 1;
    const ipInt = (baseInt + offset) >>> 0;
    ips.push(
      [(ipInt >>> 24) & 255, (ipInt >>> 16) & 255, (ipInt >>> 8) & 255, ipInt & 255].join(".")
    );
  }
  return ips;
}

// --- تست تأخیر واقعی با TCP connect روی پورت 443 ---
function tcpLatency(ip, port = 443, timeoutMs = 1200) {
  return new Promise((resolve) => {
    const start = Date.now();
    const socket = new net.Socket();
    let done = false;
    const finish = (result) => {
      if (done) return;
      done = true;
      socket.destroy();
      resolve(result);
    };
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish({ ip, latency: Date.now() - start, ok: true }));
    socket.once("timeout", () => finish({ ip, latency: null, ok: false, error: "timeout" }));
    socket.once("error", (err) => finish({ ip, latency: null, ok: false, error: err.code }));
    socket.connect(port, ip);
  });
}

// --- کش زنده‌ی استخر بهترین IPها ---
let poolCache = { updatedAt: null, results: [], updating: false };

async function refreshPool() {
  if (poolCache.updating) return;
  poolCache.updating = true;
  try {
    const ranges = await fetchCFRanges();
    let candidates = [];
    for (const r of ranges) {
      candidates.push(...sampleIPsFromCIDR(r, SAMPLE_PER_RANGE));
    }
    const results = await Promise.all(candidates.map((ip) => tcpLatency(ip)));
    const alive = results.filter((r) => r.ok).sort((a, b) => a.latency - b.latency);
    poolCache = { updatedAt: new Date().toISOString(), results: alive, updating: false };
    console.log(`[pool] رفرش شد: ${alive.length}/${candidates.length} IP زنده`);
  } catch (e) {
    console.error("[pool] خطا در رفرش:", e.message);
    poolCache.updating = false;
  }
}

// اولین اجرا هنگام استارت + رفرش دوره‌ای در پس‌زمینه
refreshPool();
setInterval(refreshPool, REFRESH_INTERVAL_MS);

// --- سلامت سرویس ---
app.get("/health", (req, res) => {
  res.json({ status: "ok", engine: "xray", time: new Date().toISOString() });
});

// --- خواندن استخر کش‌شده (فوری، بدون اسکن لحظه‌ای) ---
app.get("/api/best-ips", (req, res) => {
  const limit = parseInt(req.query.limit || "15", 10);
  res.json({
    ok: true,
    updatedAt: poolCache.updatedAt,
    count: poolCache.results.length,
    results: poolCache.results.slice(0, limit),
  });
});

app.post("/api/force-refresh", async (req, res) => {
  refreshPool(); // بدون await؛ در پس‌زمینه اجرا می‌شه
  res.json({ ok: true, message: "رفرش استخر IP شروع شد، چند ثانیه بعد /api/best-ips رو چک کن" });
});

// --- ساخت فایل کامل و آماده برای Xray-core: ورکر (چند IP) + Railway (فالبک تکی) ---
app.get("/api/full-config", (req, res) => {
  const workerDomain = req.query.workerDomain;
  const workerUUID = req.query.workerUUID;
  const ipCount = parseInt(req.query.ipCount || "10", 10);

  if (!workerDomain || !workerUUID) {
    return res.status(400).json({ ok: false, error: "پارامتر workerDomain و workerUUID الزامیه" });
  }
  if (poolCache.results.length === 0) {
    return res.status(503).json({ ok: false, error: "استخر IP هنوز آماده نیست، چند لحظه بعد دوباره امتحان کن" });
  }

  const bestIps = poolCache.results.slice(0, ipCount);

  // هر IP کلودفلر یک outbound جدا به سمت *ورکر* (چون فقط ورکر پشت anycast کلودفلره)
  const workerOutbounds = bestIps.map(({ ip }, idx) => ({
    tag: `cf-worker-ip-${idx}`,
    protocol: "vless",
    settings: { vnext: [{ address: ip, port: 443, users: [{ id: workerUUID, encryption: "none" }] }] },
    streamSettings: {
      network: "ws",
      security: "tls",
      tlsSettings: { serverName: workerDomain },
      wsSettings: { path: "/vlees", headers: { Host: workerDomain } },
    },
  }));

  // Railway به‌عنوان فالبک مستقل و تکی (بدون چند IP)
  const railwayOutbound = {
    tag: "railway-fallback",
    protocol: "vless",
    settings: { vnext: [{ address: DOMAIN, port: 443, users: [{ id: UUID_VLESS, encryption: "none" }] }] },
    streamSettings: {
      network: "ws",
      security: "tls",
      tlsSettings: { serverName: DOMAIN },
      wsSettings: { path: "/xvless", headers: { Host: DOMAIN } },
    },
  };

  const allTags = [...workerOutbounds.map((o) => o.tag), railwayOutbound.tag];

  const fullConfig = {
    log: { loglevel: "warning" },
    outbounds: [...workerOutbounds, railwayOutbound, { tag: "direct", protocol: "freedom" }],
    observatory: {
      subjectSelector: allTags,
      probeUrl: "https://www.gstatic.com/generate_204",
      probeInterval: "10s",
      enableConcurrency: true,
    },
    routing: {
      balancers: [{ tag: "xlevelup-balancer", selector: allTags, strategy: { type: "leastPing" } }],
      rules: [{ type: "field", network: "tcp,udp", balancerTag: "xlevelup-balancer" }],
    },
  };

  res.setHeader("Content-Disposition", "attachment; filename=xlevelup-full-config.json");
  res.setHeader("Content-Type", "application/json");
  res.send(JSON.stringify(fullConfig, null, 2));
});

// --- ساخت یوزر جدید (UUID تصادفی) ---
app.post("/api/create-user", (req, res) => {
  const id = randomUUID();
  res.json({ ok: true, uuid: id, note: "این UUID رو دستی به xray-config.json (clients) اضافه کن و Xray رو ریستارت کن." });
});

app.listen(PORT, () => {
  console.log(`XLevelUp Panel API روی پورت ${PORT} اجرا شد`);
});
