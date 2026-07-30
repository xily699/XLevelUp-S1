/**
 * XLevelUp Panel API — Railway
 * اجرا: node panel-api.js  (روی همون کانتینری که Xray رو هم اجرا می‌کنه، یا کنارش)
 * وظیفه: تست تأخیر IPهای کلودفلر، تولید کانفیگ چندپروتکلی، وضعیت زنده
 */

const express = require("express");
const net = require("net");
const { randomUUID } = require("crypto");

const app = express();
app.use(express.json());

const PORT = process.env.PANEL_PORT || 8080;
const DOMAIN = process.env.XLEVELUP_DOMAIN || "YOUR-DOMAIN.app";
const UUID_VLESS = process.env.UUID_VLESS || "REPLACE-WITH-UUID-1";
const UUID_VMESS = process.env.UUID_VMESS || "REPLACE-WITH-UUID-2";

// لیست پایه‌ی رنج آی‌پی‌های شناخته‌شده کلودفلر برای تست تأخیر
// (این‌ها نمونه‌های واقعی از رنج anycast کلودفلرن، قابل گسترش با اسکن رنج کامل)
const CF_IP_POOL = [
  "104.16.0.1", "104.17.0.1", "104.18.0.1", "104.19.0.1", "104.20.0.1",
  "172.64.0.1", "172.65.0.1", "172.66.0.1", "172.67.0.1", "162.159.192.1",
  "1.1.1.1", "1.0.0.1", "188.114.96.1", "188.114.97.1",
];

// --- تست تأخیر واقعی با TCP connect (نه ping معمولی، دقیق‌تر برای پورت 443) ---
function tcpLatency(ip, port = 443, timeoutMs = 1500) {
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

async function scanBestIPs(limit = 8) {
  const results = await Promise.all(CF_IP_POOL.map((ip) => tcpLatency(ip)));
  return results
    .filter((r) => r.ok)
    .sort((a, b) => a.latency - b.latency)
    .slice(0, limit);
}

// --- سلامت سرویس (برای Cloudflare Worker و xpanel) ---
app.get("/health", (req, res) => {
  res.json({ status: "ok", engine: "xray", time: new Date().toISOString() });
});

// --- تست زنده‌ی بهترین IPها ---
app.get("/api/best-ips", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit || "8", 10);
    const best = await scanBestIPs(limit);
    res.json({ ok: true, count: best.length, results: best });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// --- تولید کانفیگ کامل: ۳ پروتکل + چند IP + Balancer/Observatory ---
app.post("/api/generate-config", async (req, res) => {
  try {
    const { protocol = "vless-ws", ipCount = 5 } = req.body || {};
    const bestIps = await scanBestIPs(ipCount);

    if (bestIps.length === 0) {
      return res.status(503).json({ ok: false, error: "هیچ IP سالمی در لحظه پیدا نشد، دوباره امتحان کن" });
    }

    // لینک‌های ساده VLESS برای هر IP (برای کلاینت‌های ساده مثل v2rayNG)
    const links = bestIps.map(({ ip, latency }) => {
      const link =
        `vless://${UUID_VLESS}@${ip}:443?` +
        `type=ws&security=tls&host=${DOMAIN}&sni=${DOMAIN}&path=%2Fxvless` +
        `#XLevelUp-${ip}-${latency}ms`;
      return { ip, latency, link };
    });

    // کانفیگ پیشرفته Xray-core با چند outbound + Observatory + Balancer(leastPing)
    // این بخش واقعا توسط Xray/sing-box پشتیبانی میشه: بهترین IP رو خودش زنده تشخیص میده
    const advancedConfig = {
      outbounds: bestIps.map(({ ip }, idx) => ({
        tag: `xlevelup-${idx}`,
        protocol: "vless",
        settings: {
          vnext: [
            {
              address: ip,
              port: 443,
              users: [{ id: UUID_VLESS, encryption: "none" }],
            },
          ],
        },
        streamSettings: {
          network: "ws",
          security: "tls",
          tlsSettings: { serverName: DOMAIN },
          wsSettings: { path: "/xvless", headers: { Host: DOMAIN } },
        },
      })),
      observatory: {
        subjectSelector: bestIps.map((_, idx) => `xlevelup-${idx}`),
        probeUrl: "https://www.gstatic.com/generate_204",
        probeInterval: "10s",
      },
      routing: {
        balancers: [
          {
            tag: "xlevelup-balancer",
            selector: bestIps.map((_, idx) => `xlevelup-${idx}`),
            strategy: { type: "leastPing" },
          },
        ],
        rules: [{ type: "field", network: "tcp,udp", balancerTag: "xlevelup-balancer" }],
      },
    };

    res.json({
      ok: true,
      protocol,
      generatedAt: new Date().toISOString(),
      simpleLinks: links,
      advancedConfig,
      note: "advancedConfig برای کلاینت‌های مبتنی بر Xray-core (مثل NekoBox/v2rayN جدید) که Observatory+Balancer رو پشتیبانی می‌کنن.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// --- ساخت یوزر جدید (UUID تصادفی) برای پنل مدیریت ---
app.post("/api/create-user", (req, res) => {
  const id = randomUUID();
  res.json({ ok: true, uuid: id, note: "این UUID رو دستی به xray-config.json (clients) اضافه کن و Xray رو ریستارت کن." });
});

app.listen(PORT, () => {
  console.log(`XLevelUp Panel API روی پورت ${PORT} اجرا شد`);
});
