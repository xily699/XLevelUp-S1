import asyncio
import json
import os
import hashlib
import secrets
import time
import aiofiles
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from collections import deque, defaultdict
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("X5G")

IRAN_TZ = ZoneInfo("Asia/Tehran")

app = FastAPI(title="X5G", docs_url=None, redoc_url=None)

# ── Persistence ───────────────────────────────────────────────────────────────
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DATA_FILE = DATA_DIR / "x4g_state.json"
SECRET_FILE = DATA_DIR / "x4g_secret.key"
SAVE_LOCK = asyncio.Lock()

def _load_or_create_secret() -> str:
    """SECRET_KEY را روی دیسک ذخیره و ثابت نگه می‌دارد.
    قبلاً وقتی متغیر محیطی SECRET_KEY تنظیم نشده بود، با هر ری‌استارت سرویس
    (که روی Railway هر چند ساعت یک‌بار اتفاق می‌افتد) یک مقدار تصادفی جدید
    ساخته می‌شد. چون هش پسورد بر پایه‌ی همین secret ساخته می‌شود، تغییر آن
    باعث می‌شد پسورد درست هم دیگر قبول نشود. حالا secret یک‌بار ساخته و در
    فایل ذخیره می‌شود و در ری‌استارت‌های بعدی همان مقدار خوانده می‌شود."""
    env_secret = os.environ.get("SECRET_KEY")
    if env_secret:
        return env_secret
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if SECRET_FILE.exists():
            existing = SECRET_FILE.read_text(encoding="utf-8").strip()
            if existing:
                return existing
        new_secret = secrets.token_urlsafe(32)
        SECRET_FILE.write_text(new_secret, encoding="utf-8")
        return new_secret
    except Exception as e:
        logger.warning(f"Could not persist SECRET_KEY, sessions/password may reset on restart: {e}")
        return secrets.token_urlsafe(32)

CONFIG = {
    "port": int(os.environ.get("PORT", 8000)),
    "secret": _load_or_create_secret(),
    "host": os.environ.get("RAILWAY_PUBLIC_DOMAIN", "localhost"),
    # دامنه‌ی عمومی Worker کلودفلر (worker.js) که جلوی این سرویس Railway به‌عنوان
    # لبه‌ی رایگان قرار می‌گیره. اگه ست بشه، لینک‌های VLESS/ساب که برای کلاینت
    # ساخته می‌شن به‌جای دامنه‌ی مستقیم Railway، از همین دامنه استفاده می‌کنن —
    # یعنی کلاینت واقعاً از مسیر ترکیبی Worker+Railway وصل می‌شه، نه مستقیم.
    "worker_domain": (os.environ.get("WORKER_PUBLIC_DOMAIN", "").strip().rstrip("/").replace("https://", "").replace("http://", "")),
    # رمز مشترک برای سینک Worker↔Railway. اگه از قبل به‌عنوان env ست شده باشه
    # (روش قدیمی) همون اولویت داره؛ وگرنه هر چیزی که تو ویزارد اول ورود ذخیره
    # بشه از دیسک خونده می‌شه — یعنی دیگه لازم نیست حتماً از قبل تو Railway
    # به‌صورت دستی ست بشه.
    "edge_secret": os.environ.get("EDGE_SHARED_SECRET", "").strip(),
    "setup_done": False,
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def load_state():
    global LINKS, AUTH, SUBS, USERS
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if DATA_FILE.exists():
            async with aiofiles.open(DATA_FILE, "r", encoding="utf-8") as f:
                raw = await f.read()
            data = json.loads(raw)
            LINKS.update(data.get("links", {}))
            SUBS.update(data.get("subs", {}))
            USERS.update(data.get("users", {}))
            if "password_hash" in data:
                AUTH["password_hash"] = data["password_hash"]
            saved_cfg = data.get("config") or {}
            if saved_cfg.get("worker_domain") and not os.environ.get("WORKER_PUBLIC_DOMAIN"):
                CONFIG["worker_domain"] = saved_cfg["worker_domain"]
            if saved_cfg.get("edge_secret") and not os.environ.get("EDGE_SHARED_SECRET"):
                CONFIG["edge_secret"] = saved_cfg["edge_secret"]
            CONFIG["setup_done"] = bool(saved_cfg.get("setup_done"))
            # لینک پیش‌فرضی که در نسخه‌های قبلی به‌صورت خودکار ساخته می‌شد دیگر
            # پشتیبانی نمی‌شود؛ اگر از قبل روی دیسک ذخیره شده باشد، حذفش می‌کنیم.
            legacy_default_uids = [uid for uid, l in LINKS.items() if l.get("is_default")]
            for uid in legacy_default_uids:
                LINKS.pop(uid, None)
            if legacy_default_uids:
                asyncio.create_task(save_state())
            logger.info(f"State loaded: {len(LINKS)} links, {len(SUBS)} subs, {len(USERS)} users")
    except Exception as e:
        logger.warning(f"Could not load state: {e}")

async def save_state():
    async with SAVE_LOCK:
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            data = {
                "links": dict(LINKS),
                "subs": dict(SUBS),
                "users": dict(USERS),
                "password_hash": AUTH["password_hash"],
                "config": {
                    "worker_domain": CONFIG.get("worker_domain") or "",
                    "edge_secret": CONFIG.get("edge_secret") or "",
                    "setup_done": bool(CONFIG.get("setup_done")),
                },
                "saved_at": datetime.now().isoformat(),
            }
            tmp = DATA_FILE.with_suffix(".tmp")
            async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            tmp.replace(DATA_FILE)
        except Exception as e:
            logger.warning(f"Could not save state: {e}")

# ── In-memory state ───────────────────────────────────────────────────────────
connections: dict = {}
stats = {
    "total_bytes": 0,
    "total_requests": 0,
    "total_errors": 0,
    "start_time": time.time(),
}
error_logs: deque = deque(maxlen=50)
activity_logs: deque = deque(maxlen=200)
hourly_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None
LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
SUBS: dict = {}
SUBS_LOCK = asyncio.Lock()
# X5.2: کاربران پنل — هر کانفیگ می‌تونه به یک کاربر «تخصیص» داده بشه تا مصرف/تعداد
# کانفیگ‌های هر کاربر رو جدا از هم ببینی. جدا از ADMIN_PASSWORD (که ورود به خودِ
# پنل مدیریتیه) — این‌ها فقط رکوردهای مشتری/کاربر نهایی برای دسته‌بندی هستن.
USERS: dict = {}
USERS_LOCK = asyncio.Lock()

# پروتکل‌های پشتیبانی‌شده برای هر کانفیگ
PROTOCOLS = ("vless-ws", "xhttp")
DEFAULT_PROTOCOL = "vless-ws"

# Fingerprint (uTLS) های قابل انتخاب برای هر کانفیگ
FINGERPRINTS = ("chrome", "firefox", "safari", "ios", "android", "edge", "360", "qq", "random", "randomized")
DEFAULT_FINGERPRINT = "chrome"

# پیش‌فرض ALPN بر اساس نوع ترابرد (اگر کاربر مقدار دستی نده)
DEFAULT_ALPN_BY_PROTOCOL = {
    "vless-ws": "http/1.1",
    "xhttp": "h2,http/1.1",
}
DEFAULT_PORT = 443
MIN_PORT, MAX_PORT = 1, 65535

# محدودیت سرعت (0 = نامحدود). واحد ذخیره‌سازی داخلی همیشه بایت‌بر‌ثانیه است.
DEFAULT_SPEED_LIMIT = 0


# X5G.1: هوک اختیاری برای webhook — توسط edge_intel.py در زمان import رجیستر
# می‌شه (نه برعکس)، که وابستگی چرخه‌ای بین main.py و edge_intel.py پیش نیاد.
_webhook_hook = None

def register_webhook_hook(fn):
    global _webhook_hook
    _webhook_hook = fn

def log_activity(kind: str, message: str, level: str = "info"):
    """ثبت یک رخداد در لاگ فعالیت‌ها (ساخت/حذف/ویرایش کانفیگ، ورود، و...).
    رخدادهای warn/err در صورت تنظیم WEBHOOK_URL به‌صورت fire-and-forget
    به بیرون هم گزارش می‌شن (کوتای تموم‌شده، IP بن‌شده، خطای پشت‌سرهم)."""
    activity_logs.append({
        "kind": kind,
        "level": level,
        "message": message,
        "time": datetime.now().isoformat(),
    })
    if level in ("warn", "err") and _webhook_hook is not None:
        try:
            asyncio.create_task(_webhook_hook(f"{kind}.{level}", {"message": message, "kind": kind, "level": level}))
        except RuntimeError:
            pass  # هیچ event loop فعالی نیست (مثلاً موقع startup خیلی زودهنگام)

# ── Auth ──────────────────────────────────────────────────────────────────────
SESSION_COOKIE = "x4g_session"
SESSION_TTL = 60 * 60 * 24 * 365

def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()

AUTH = {"password_hash": hash_password(os.environ.get("ADMIN_PASSWORD", "X4GKING"))}
SESSIONS: dict = {}
SESSIONS_LOCK = asyncio.Lock()

async def create_session() -> str:
    token = secrets.token_urlsafe(32)
    async with SESSIONS_LOCK:
        SESSIONS[token] = time.time() + SESSION_TTL
    return token

async def is_valid_session(token: str | None) -> bool:
    if not token:
        return False
    async with SESSIONS_LOCK:
        exp = SESSIONS.get(token)
        if exp is None:
            return False
        if exp < time.time():
            SESSIONS.pop(token, None)
            return False
        return True

async def destroy_session(token: str | None):
    if not token:
        return
    async with SESSIONS_LOCK:
        SESSIONS.pop(token, None)

async def require_auth(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if not await is_valid_session(token):
        raise HTTPException(status_code=401, detail="unauthorized")
    return token

# ── Startup / Shutdown ────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global http_client
    limits = httpx.Limits(max_connections=500, max_keepalive_connections=100)
    timeout = httpx.Timeout(30.0, connect=10.0)
    http_client = httpx.AsyncClient(
        limits=limits, timeout=timeout, follow_redirects=True,
    )
    await load_state()
    await _tg_start_bot()
    try:
        from xray_bridge import xray_sync_all_users
        asyncio.create_task(xray_sync_all_users(LINKS))
    except Exception as e:
        logger.warning(f"[xray_bridge] sync اولیه انجام نشد: {e}")
    log_activity("system", "سرور راه‌اندازی شد", "ok")
    logger.info(f"X5G 5.3-LevelUp started on port {CONFIG['port']}")

@app.on_event("shutdown")
async def shutdown():
    await save_state()
    await _tg_stop_bot()
    if http_client:
        await http_client.aclose()

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_host(request: Request | None = None) -> str:
    """آدرس دامنه رو ترجیحاً از خودِ درخواست HTTP می‌گیره (هدر Host/X-Forwarded-Host)
    چون این همیشه دقیقاً همون دامنه‌ایه که کاربر واقعاً بهش وصل شده. متغیر محیطی
    RAILWAY_PUBLIC_DOMAIN فقط به‌عنوان fallback استفاده می‌شه، چون گاهی موقع بالا اومدن
    کانتینر هنوز مقداردهی نشده و باعث می‌شد لینک‌ها گاهی با "localhost" ساخته بشن."""
    if request is not None:
        h = request.headers.get("x-forwarded-host") or request.headers.get("host")
        if h:
            h = h.split(":")[0]
            CONFIG["host"] = h  # کش آخرین دامنه‌ی واقعی دیده‌شده، برای جاهایی که request نداریم (مثل ربات تلگرام)
            return h
    return os.environ.get("RAILWAY_PUBLIC_DOMAIN", CONFIG["host"])

def edge_host(request: Request | None = None) -> str:
    """دامنه‌ای که باید به کلاینت‌ها داده بشه (لینک VLESS/ساب). اگه WORKER_PUBLIC_DOMAIN
    تنظیم شده باشه، همیشه همون رو برمی‌گردونه — چون Worker جلوی Railway قرار داره و
    مسیر اصلی اتصال کلاینت‌ها باید از همونجا رد بشه (ترکیب Worker رایگان + Railway).
    وگرنه، دقیقاً مثل قبل، به دامنه‌ی مستقیم Railway برمی‌گرده."""
    if CONFIG["worker_domain"]:
        return CONFIG["worker_domain"]
    return get_host(request)

def generate_uuid() -> str:
    h = secrets.token_hex(16)
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    
def now_ir() -> datetime:
    return datetime.now(IRAN_TZ)

def generate_vless_link(
    uuid: str,
    host: str,
    remark: str = "X4G",
    protocol: str = DEFAULT_PROTOCOL,
    fingerprint: str | None = None,
    alpn: str | None = None,
    port: int | None = None,
) -> str:
    """می‌سازد VLESS share-link متناسب با پروتکل انتخاب‌شده (WS کلاسیک یا یکی از مدهای XHTTP).
    fingerprint / alpn / port در صورت ندادن، از پیش‌فرض‌های خود پروتکل استفاده می‌شوند."""
    fp = (fingerprint or DEFAULT_FINGERPRINT).strip() or DEFAULT_FINGERPRINT
    if fp not in FINGERPRINTS:
        fp = DEFAULT_FINGERPRINT
    alpn_val = (alpn or "").strip() or DEFAULT_ALPN_BY_PROTOCOL.get(protocol, "http/1.1")
    port_val = port or DEFAULT_PORT
    if not (MIN_PORT <= port_val <= MAX_PORT):
        port_val = DEFAULT_PORT

    if protocol == "vless-ws":
        path = f"/ws/{uuid}"
        params = {
            "encryption": "none",
            "security": "tls",
            "type": "ws",
            "host": host,
            "path": path,
            "sni": host,
            "fp": fp,
            "alpn": alpn_val,
        }
    else:
        # xhttp — مود auto: خود کلاینت بر اساس نوع اتصال (H2/REALITY یا نه)
        # بین packet-up و stream-up انتخاب می‌کنه؛ مسیر سرور به مود بستگی نداره.
        path = f"/xhttp-siz10/{uuid}"
        params = {
            "encryption": "none",
            "security": "tls",
            "type": "xhttp",
            "mode": "auto",
            "host": host,
            "path": path,
            "sni": host,
            "fp": fp,
            "alpn": alpn_val,
        }
    query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    return f"vless://{uuid}@{host}:{port_val}?{query}#{quote(remark)}"

def vless_link_for_link(link: dict, uid: str, host: str) -> str:
    """generate_vless_link رو با تنظیمات دستی همون کانفیگ (fingerprint/alpn/port) صدا می‌زنه."""
    proto = link.get("protocol", DEFAULT_PROTOCOL)
    return generate_vless_link(
        uid, host,
        remark=f"X4G-{link.get('label','')}",
        protocol=proto,
        fingerprint=link.get("fingerprint"),
        alpn=link.get("alpn"),
        port=link.get("port"),
    )

def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_size_to_bytes(value: float, unit: str) -> int:
    unit = unit.upper()
    if unit == "GB": return int(value * 1024 ** 3)
    if unit == "MB": return int(value * 1024 ** 2)
    if unit == "KB": return int(value * 1024)
    return int(value)

def parse_speed_to_bytes(value: float, unit: str) -> int:
    """محدودیت سرعت رو به بایت‌بر‌ثانیه تبدیل می‌کنه.
    واحدهای پشتیبانی‌شده: MBIT (مگابیت‌بر‌ثانیه، رایج‌ترین)، KB (کیلوبایت‌بر‌ثانیه)، MB (مگابایت‌بر‌ثانیه)."""
    if value <= 0:
        return 0
    unit = (unit or "MBIT").upper()
    if unit == "MBIT":
        return int(value * 1024 * 1024 / 8)
    if unit == "KB":
        return int(value * 1024)
    if unit == "MB":
        return int(value * 1024 * 1024)
    return int(value)

def is_link_expired(link: dict) -> bool:
    exp = link.get("expires_at")
    if not exp:
        return False
    try:
        return datetime.now() > datetime.fromisoformat(exp)
    except Exception:
        return False

def is_link_allowed(link: dict | None) -> bool:
    if link is None:
        return False
    if not link.get("active", True):
        return False
    if is_link_expired(link):
        return False
    lb = link.get("limit_bytes", 0)
    if lb > 0 and link.get("used_bytes", 0) >= lb:
        return False
    return True

def fmt_bytes(b: int) -> str:
    if b < 1024: return f"{b} B"
    if b < 1024**2: return f"{b/1024:.1f} KB"
    if b < 1024**3: return f"{b/1024**2:.2f} MB"
    return f"{b/1024**3:.2f} GB"

def unique_ips_for_uuid(uuid: str) -> set:
    """آی‌پی‌های یکتای همین لحظه متصل به یک UUID خاص (بر اساس dict اتصالات زنده)."""
    return {c.get("ip") for c in connections.values() if c.get("uuid") == uuid and c.get("ip")}

def is_ip_allowed(link: dict | None, uuid: str, ip: str) -> bool:
    """محدودیت تعداد آی‌پی/کاربر هم‌زمان برای هر کانفیگ. ip_limit=0 یعنی نامحدود.
    اگر همین آی‌پی از قبل روی این کانفیگ سشن باز داشته باشه، همیشه مجازه (برای چند اتصال
    هم‌زمان از یک دستگاه/مرورگر مشکلی پیش نمیاد)."""
    if link is None:
        return False
    limit = int(link.get("ip_limit", 0) or 0)
    if limit <= 0:
        return True
    ips = unique_ips_for_uuid(uuid)
    if ip in ips:
        return True
    return len(ips) < limit

def client_ip(request: Request) -> str:
    """آی‌پی واقعی کلاینت رو با احتساب هدرهای پراکسی (Railway/Cloudflare) برمی‌گردونه."""
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "نامشخص"

# ── Default link ──────────────────────────────────────────────────────────────

# ── Basic endpoints ───────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "X5G Level Up",
        "version": "1.0",
        "status": "active",
        "engine": "worker+railway hybrid",
        "edge_sync": bool(os.environ.get("EDGE_SHARED_SECRET", "").strip()),
        "channel": "https://t.me/X4GHUB",
    }

@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections), "uptime": uptime()}

# ── Subscription (single link) ────────────────────────────────────────────────
@app.get("/sub/{uuid}")
async def subscription_single(uuid: str, request: Request):
    import base64
    async with LINKS_LOCK:
        link = LINKS.get(uuid)
    if not link or not is_link_allowed(link):
        raise HTTPException(status_code=404, detail="not found or inactive")
    host = edge_host(request)
    vless = vless_link_for_link(link, uuid, host)
    content = base64.b64encode(vless.encode()).decode()
    return Response(content=content, media_type="text/plain",
                    headers={"profile-title": quote(link["label"]), "support-url": "https://t.me/X4GHUB"})

@app.get("/sub-all")
async def subscription_all(request: Request, _=Depends(require_auth)):
    import base64
    host = edge_host(request)
    async with LINKS_LOCK:
        lines = [
            vless_link_for_link(d, uid, host)
            for uid, d in LINKS.items()
            if is_link_allowed(d)
        ]
    content = base64.b64encode("\n".join(lines).encode()).decode()
    return Response(content=content, media_type="text/plain")

# ── Auth endpoints ────────────────────────────────────────────────────────────
@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    ip = client_ip(request)
    if hash_password(str(body.get("password", ""))) != AUTH["password_hash"]:
        log_activity("auth", f"تلاش ورود ناموفق از {ip}", "err")
        raise HTTPException(status_code=401, detail="رمز عبور اشتباه است")
    token = await create_session()
    log_activity("auth", f"ورود موفق به پنل از {ip}", "ok")
    resp = JSONResponse({"ok": True})
    resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL, httponly=True, samesite="lax", path="/")
    return resp

@app.post("/api/logout")
async def api_logout(request: Request):
    await destroy_session(request.cookies.get(SESSION_COOKIE))
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp

@app.get("/api/me")
async def api_me(request: Request):
    return {
        "authenticated": await is_valid_session(request.cookies.get(SESSION_COOKIE)),
        "worker_domain": CONFIG["worker_domain"] or None,
    }

@app.post("/api/change-password")
async def api_change_password(request: Request, token=Depends(require_auth)):
    body = await request.json()
    if hash_password(str(body.get("current_password", ""))) != AUTH["password_hash"]:
        raise HTTPException(status_code=400, detail="رمز فعلی اشتباه است")
    new = str(body.get("new_password", ""))
    if len(new) < 4:
        raise HTTPException(status_code=400, detail="رمز جدید باید حداقل ۴ کاراکتر باشد")
    AUTH["password_hash"] = hash_password(new)
    async with SESSIONS_LOCK:
        SESSIONS.clear()
        SESSIONS[token] = time.time() + SESSION_TTL
    await save_state()
    log_activity("auth", "رمز عبور پنل تغییر کرد", "ok")
    return {"ok": True}

# ── Stats ─────────────────────────────────────────────────────────────────────
@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    async with LINKS_LOCK:
        snap = dict(LINKS)
    return {
        "active_connections": len(connections),
        "total_traffic_mb": round(stats["total_bytes"] / (1024 ** 2), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "timestamp": datetime.now().isoformat(),
        "hourly": dict(hourly_traffic),
        "recent_errors": list(error_logs)[-10:],
        "links_count": len(snap),
        "active_links": sum(1 for l in snap.values() if is_link_allowed(l)),
        "expired_links": sum(1 for l in snap.values() if is_link_expired(l)),
    }

# ── Activity Logs ─────────────────────────────────────────────────────────────
@app.get("/api/activity")
async def get_activity(_=Depends(require_auth)):
    return {"logs": list(activity_logs)[-150:]}

# ── Live connections (با دسته‌بندی بر اساس کانفیگ) ────────────────────────────
@app.get("/api/connections")
async def get_connections(_=Depends(require_auth)):
    """
    خروجی این endpoint حالا بر اساس کانفیگ (uuid) گروه‌بندی شده: هر کانفیگ
    یک آیتم با تعداد آی‌پی/سشن و مجموع ترافیکشه، و داخل هرکدوم لیست
    آی‌پی‌های متصل به همون کانفیگ (با جمع بایت و تعداد سشن هر آی‌پی) هست.
    raw_count همچنان تعداد واقعی اتصالات باز (سشن‌های خام) را برمی‌گرداند.
    """
    async with LINKS_LOCK:
        snap = dict(LINKS)

    by_uuid: dict[str, dict] = {}
    for conn_id, c in connections.items():
        uid = c.get("uuid", "نامشخص")
        ip = c.get("ip", "نامشخص")
        link = snap.get(uid)
        label = link.get("label") if link else "کانفیگ حذف‌شده"
        proto = link.get("protocol", DEFAULT_PROTOCOL) if link else "?"

        cfg = by_uuid.get(uid)
        if cfg is None:
            cfg = {
                "uuid": uid,
                "label": label,
                "protocol": proto,
                "sessions": 0,
                "bytes": 0,
                "ips": {},
                "first_connected_at": c.get("connected_at"),
                "last_connected_at": c.get("connected_at"),
            }
            by_uuid[uid] = cfg
        cfg["sessions"] += 1
        cfg["bytes"] += c.get("bytes", 0)

        ip_entry = cfg["ips"].get(ip)
        if ip_entry is None:
            ip_entry = {
                "ip": ip, "sessions": 0, "bytes": 0, "transports": set(),
                "first_connected_at": c.get("connected_at"),
                "last_connected_at": c.get("connected_at"),
            }
            cfg["ips"][ip] = ip_entry
        ip_entry["sessions"] += 1
        ip_entry["bytes"] += c.get("bytes", 0)
        ip_entry["transports"].add(c.get("transport", "vless-ws"))

        ca = c.get("connected_at")
        for entry in (cfg, ip_entry):
            if ca:
                if not entry["first_connected_at"] or ca < entry["first_connected_at"]:
                    entry["first_connected_at"] = ca
                if not entry["last_connected_at"] or ca > entry["last_connected_at"]:
                    entry["last_connected_at"] = ca

    configs = []
    for uid, cfg in by_uuid.items():
        ip_list = []
        for ip, e in cfg["ips"].items():
            ip_list.append({
                "ip": ip,
                "sessions": e["sessions"],
                "bytes": e["bytes"],
                "bytes_fmt": fmt_bytes(e["bytes"]),
                "transports": sorted(e["transports"]),
                "connected_at": e["first_connected_at"],
                "last_connected_at": e["last_connected_at"],
            })
        ip_list.sort(key=lambda x: x.get("last_connected_at") or "", reverse=True)
        configs.append({
            "uuid": uid,
            "label": cfg["label"],
            "protocol": cfg["protocol"],
            "ip_count": len(ip_list),
            "sessions": cfg["sessions"],
            "bytes": cfg["bytes"],
            "bytes_fmt": fmt_bytes(cfg["bytes"]),
            "connected_at": cfg["first_connected_at"],
            "last_connected_at": cfg["last_connected_at"],
            "connections": ip_list,
        })
    configs.sort(key=lambda x: x.get("last_connected_at") or "", reverse=True)

    return {
        "configs": configs,
        "count": len(configs),          # تعداد کانفیگ‌های دارای اتصال فعال
        "raw_count": len(connections),  # تعداد کل اتصالات باز (بدون گروه‌بندی)
    }

# ── Shared link create/delete helpers (استفاده مشترک API و ربات تلگرام) ───────
async def make_link(
    label: str = "لینک جدید",
    limit_bytes: int = 0,
    expires_at: str | None = None,
    note: str = "",
    protocol: str = DEFAULT_PROTOCOL,
    fingerprint: str = DEFAULT_FINGERPRINT,
    alpn: str = "",
    port: int = DEFAULT_PORT,
    ip_limit: int = 0,
    speed_limit_bytes: int = 0,
    upload_limit_bytes: int = 0,
    download_limit_bytes: int = 0,
    owner_user_id: str | None = None,
) -> tuple[str, dict]:
    if protocol not in PROTOCOLS:
        protocol = DEFAULT_PROTOCOL
    fingerprint = (fingerprint or DEFAULT_FINGERPRINT).strip().lower()
    if fingerprint not in FINGERPRINTS:
        fingerprint = DEFAULT_FINGERPRINT
    if not (MIN_PORT <= port <= MAX_PORT):
        port = DEFAULT_PORT
    if owner_user_id and owner_user_id not in USERS:
        owner_user_id = None
    uid = generate_uuid()
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": (label or "لینک جدید").strip()[:60] or "لینک جدید",
            "limit_bytes": max(0, limit_bytes),
            "used_bytes": 0,
            "created_at": datetime.now().isoformat(),
            "active": True,
            "expires_at": expires_at,
            "note": (note or "").strip()[:200],
            "is_default": False,
            "protocol": protocol,
            "fingerprint": fingerprint,
            "alpn": (alpn or "").strip()[:100],
            "port": port,
            "ip_limit": max(0, ip_limit),
            "speed_limit_bytes": max(0, speed_limit_bytes),
            "upload_limit_bytes": max(0, upload_limit_bytes),
            "download_limit_bytes": max(0, download_limit_bytes),
            "owner_user_id": owner_user_id,
        }
    if owner_user_id:
        USERS[owner_user_id]["link_uuids"].append(uid)
        asyncio.create_task(save_state())
    asyncio.create_task(save_state())
    log_activity("link", f"کانفیگ «{LINKS[uid]['label']}» ساخته شد", "ok")
    try:
        from xray_bridge import xray_add_user
        asyncio.create_task(xray_add_user(uid, LINKS[uid]["label"]))
    except Exception as e:
        logger.warning(f"[xray_bridge] add_user صدا زده نشد: {e}")
    return uid, LINKS[uid]

async def remove_link(uid: str) -> str | None:
    async with LINKS_LOCK:
        if uid not in LINKS:
            return None
        label = LINKS[uid].get("label", uid)
        owner_id = LINKS[uid].get("owner_user_id")
        del LINKS[uid]
        if owner_id and owner_id in USERS:
            USERS[owner_id]["link_uuids"] = [u for u in USERS[owner_id]["link_uuids"] if u != uid]
    from speed_limit import reset_bucket
    reset_bucket(uid)
    asyncio.create_task(save_state())
    log_activity("link", f"کانفیگ «{label}» حذف شد", "err")
    try:
        from xray_bridge import xray_remove_user
        asyncio.create_task(xray_remove_user(label))
    except Exception as e:
        logger.warning(f"[xray_bridge] remove_user صدا زده نشد: {e}")
    return label

async def set_link_active(uid: str, active: bool) -> dict | None:
    async with LINKS_LOCK:
        if uid not in LINKS:
            return None
        LINKS[uid]["active"] = bool(active)
        label = LINKS[uid]["label"]
    log_activity("link", f"کانفیگ «{label}» {'فعال' if active else 'غیرفعال'} شد", "ok" if active else "warn")
    asyncio.create_task(save_state())
    return LINKS[uid]

# ── Link Management ───────────────────────────────────────────────────────────
@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    lv = float(body.get("limit_value") or 0)
    lu = body.get("limit_unit") or "GB"
    limit_bytes = 0 if lv <= 0 else parse_size_to_bytes(lv, lu)
    exp_days = int(body.get("expires_days") or 0)
    expires_at = (datetime.now() + timedelta(days=exp_days)).isoformat() if exp_days > 0 else None
    try:
        port = int(body.get("port") or DEFAULT_PORT)
    except (TypeError, ValueError):
        port = DEFAULT_PORT
    try:
        ip_limit = int(body.get("ip_limit") or 0)
    except (TypeError, ValueError):
        ip_limit = 0

    sv = float(body.get("speed_limit_value") or 0)
    su = body.get("speed_limit_unit") or "MBIT"
    speed_limit_bytes = 0 if sv <= 0 else parse_speed_to_bytes(sv, su)

    uv = float(body.get("upload_limit_value") or 0)
    uu = body.get("upload_limit_unit") or "MBIT"
    upload_limit_bytes = 0 if uv <= 0 else parse_speed_to_bytes(uv, uu)

    dv = float(body.get("download_limit_value") or 0)
    du = body.get("download_limit_unit") or "MBIT"
    download_limit_bytes = 0 if dv <= 0 else parse_speed_to_bytes(dv, du)

    owner_user_id = (body.get("owner_user_id") or "").strip() or None

    uid, link = await make_link(
        label=body.get("label") or "لینک جدید",
        limit_bytes=limit_bytes,
        expires_at=expires_at,
        note=body.get("note") or "",
        protocol=body.get("protocol") or DEFAULT_PROTOCOL,
        fingerprint=body.get("fingerprint") or DEFAULT_FINGERPRINT,
        alpn=body.get("alpn") or "",
        port=port,
        ip_limit=ip_limit,
        speed_limit_bytes=speed_limit_bytes,
        upload_limit_bytes=upload_limit_bytes,
        download_limit_bytes=download_limit_bytes,
        owner_user_id=owner_user_id,
    )

    host = edge_host(request)
    return {
        "uuid": uid,
        **link,
        "expired": False,
        "vless_link": vless_link_for_link(link, uid, host),
        "sub_url": f"https://{host}/p/{uid}",
        "raw_sub_url": f"https://{host}/sub/{uid}",
    }

@app.get("/api/links")
async def list_links(request: Request, _=Depends(require_auth)):
    host = edge_host(request)
    async with LINKS_LOCK:
        snap = dict(LINKS)
    result = []
    for uid, d in snap.items():
        proto = d.get("protocol", DEFAULT_PROTOCOL)
        owner_id = d.get("owner_user_id")
        result.append({
            "uuid": uid,
            **d,
            "protocol": proto,
            "expired": is_link_expired(d),
            "vless_link": vless_link_for_link(d, uid, host),
            "sub_url": f"https://{host}/p/{uid}",
            "raw_sub_url": f"https://{host}/sub/{uid}",
            "connected_ips": len(unique_ips_for_uuid(uid)),
            "owner_name": (USERS.get(owner_id) or {}).get("name") if owner_id else None,
        })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": result}

@app.patch("/api/links/{uid}")
async def update_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="link not found")
        link = LINKS[uid]
        label = link.get("label")
        if "active" in body:
            link["active"] = bool(body["active"])
            log_activity("link", f"کانفیگ «{label}» {'فعال' if link['active'] else 'غیرفعال'} شد", "ok" if link["active"] else "warn")
        if "label" in body:
            link["label"] = str(body["label"])[:60]
        if "note" in body:
            link["note"] = str(body["note"])[:200]
        if "reset_usage" in body and body["reset_usage"]:
            link["used_bytes"] = 0
            log_activity("link", f"مصرف کانفیگ «{label}» ریست شد", "info")
        if "limit_value" in body:
            lv = float(body.get("limit_value") or 0)
            lu = body.get("limit_unit") or "GB"
            link["limit_bytes"] = 0 if lv <= 0 else parse_size_to_bytes(lv, lu)
        if "expires_days" in body:
            ed = int(body["expires_days"] or 0)
            link["expires_at"] = (datetime.now() + timedelta(days=ed)).isoformat() if ed > 0 else None
        if "fingerprint" in body:
            fp = str(body.get("fingerprint") or DEFAULT_FINGERPRINT).strip().lower()
            link["fingerprint"] = fp if fp in FINGERPRINTS else DEFAULT_FINGERPRINT
        if "alpn" in body:
            link["alpn"] = str(body.get("alpn") or "").strip()[:100]
        if "port" in body:
            try:
                p = int(body.get("port") or DEFAULT_PORT)
            except (TypeError, ValueError):
                p = DEFAULT_PORT
            link["port"] = p if (MIN_PORT <= p <= MAX_PORT) else DEFAULT_PORT
        if "ip_limit" in body:
            try:
                il = int(body.get("ip_limit") or 0)
            except (TypeError, ValueError):
                il = 0
            link["ip_limit"] = max(0, il)
        if "speed_limit_value" in body:
            sv = float(body.get("speed_limit_value") or 0)
            su = body.get("speed_limit_unit") or "MBIT"
            link["speed_limit_bytes"] = 0 if sv <= 0 else parse_speed_to_bytes(sv, su)
            from speed_limit import reset_bucket
            reset_bucket(uid)
        if "upload_limit_value" in body:
            uv = float(body.get("upload_limit_value") or 0)
            uu = body.get("upload_limit_unit") or "MBIT"
            link["upload_limit_bytes"] = 0 if uv <= 0 else parse_speed_to_bytes(uv, uu)
            from speed_limit import reset_bucket
            reset_bucket(uid)
        if "download_limit_value" in body:
            dv = float(body.get("download_limit_value") or 0)
            du = body.get("download_limit_unit") or "MBIT"
            link["download_limit_bytes"] = 0 if dv <= 0 else parse_speed_to_bytes(dv, du)
            from speed_limit import reset_bucket
            reset_bucket(uid)
        if "owner_user_id" in body:
            new_owner = (body.get("owner_user_id") or "").strip() or None
            old_owner = link.get("owner_user_id")
            if new_owner and new_owner not in USERS:
                raise HTTPException(status_code=404, detail="user not found")
            if old_owner and old_owner in USERS:
                USERS[old_owner]["link_uuids"] = [u for u in USERS[old_owner]["link_uuids"] if u != uid]
            if new_owner:
                USERS[new_owner]["link_uuids"].append(uid)
            link["owner_user_id"] = new_owner
        if any(k in body for k in ("label", "note", "limit_value", "expires_days", "fingerprint", "alpn", "port", "ip_limit", "speed_limit_value", "upload_limit_value", "download_limit_value", "owner_user_id")):
            log_activity("link", f"کانفیگ «{link['label']}» ویرایش شد", "info")

    asyncio.create_task(save_state())
    return {"ok": True}

@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    label = await remove_link(uid)
    if label is None:
        raise HTTPException(status_code=404, detail="link not found")
    return {"ok": True, "deleted": uid}

# ── Xray-core Engine Status (X5.3) ─────────────────────────────────────────────
@app.get("/api/xray/status")
async def api_xray_status(_=Depends(require_auth)):
    try:
        from xray_bridge import xray_status
        return await xray_status()
    except Exception as e:
        return {"enabled": False, "reachable": False, "state": "error", "detail": str(e)[:300]}

# ── Xray-core Native Protocol Links (X5.3) ──────────────────────────────────────
# این endpoint دقیقاً همون «فاز بعدی» است که در XRAY-SETUP.md یادداشت شده بود:
# لینک واقعی و قابل‌اتصال برای REALITY و VLESS Post-Quantum Encryption بساز —
# نه فقط اسمشون تو تنظیمات باشه. چون این دو پروتکل روی پورت‌های اختصاصی Xray
# (نه پشت Worker) هستن، معمولاً باید Railway Networking → TCP Proxy را برای
# آن‌ها فعال کنی؛ Railway یک هاست/پورت عمومی *رندوم* می‌دهد (نه لزوماً همون
# 12004/12005 داخلی) — برای همین host/port را می‌شود override کرد، وگرنه از
# XRAY_PUBLIC_HOST/XRAY_REALITY_PUBLIC_PORT/XRAY_PQ_PUBLIC_PORT (اگر ست شده)
# یا در نهایت از دامنه‌ی همین درخواست استفاده می‌کند (که در عمل احتمالاً درست
# نیست تا وقتی TCP Proxy را دستی تنظیم نکنی — به همین دلیل «warning» برمی‌گردد).
@app.get("/api/xray/links/{uid}")
async def api_xray_links(uid: str, request: Request, host: str | None = None,
                          reality_port: int | None = None, pq_port: int | None = None,
                          _=Depends(require_auth)):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
    if not link:
        raise HTTPException(status_code=404, detail="link not found")
    email = link.get("label", uid)
    pub_host = (host or os.environ.get("XRAY_PUBLIC_HOST", "").strip() or get_host(request))
    r_port = reality_port or int(os.environ.get("XRAY_REALITY_PUBLIC_PORT", "0") or 0) or 12004
    p_port = pq_port or int(os.environ.get("XRAY_PQ_PUBLIC_PORT", "0") or 0) or 12005
    warning = None
    if not (host or os.environ.get("XRAY_PUBLIC_HOST", "").strip()):
        warning = (
            "هاست به‌صورت خودکار از دامنه‌ی همین درخواست گرفته شد. برای اینکه این لینک‌ها واقعاً "
            "کار کنند، باید در Railway → Settings → Networking یک TCP Proxy برای پورت‌های "
            "12004 (REALITY) و 12005 (VLESS-PQ) بسازی و هاست/پورت *عمومی* که Railway می‌دهد را "
            "با پارامترهای ?host=&reality_port=&pq_port= اینجا بدهی (یا در env با "
            "XRAY_PUBLIC_HOST/XRAY_REALITY_PUBLIC_PORT/XRAY_PQ_PUBLIC_PORT ثابتش کنی)."
        )
    try:
        from xray_bridge import reality_client_link, pq_client_link, XRAY_ENABLED
    except Exception as e:
        return {"ok": False, "error": f"xray_bridge در دسترس نیست: {e}"}
    if not XRAY_ENABLED:
        return {"ok": False, "error": "XRAY_ENABLED=true نیست — این لینک‌ها موجود نیستند."}
    reality_link = reality_client_link(uid, pub_host, r_port, remark=link.get("label", "X5G-Reality"))
    pq_link = pq_client_link(uid, pub_host, p_port, remark=link.get("label", "X5G-PQ"))
    return {
        "ok": True,
        "warning": warning,
        "reality": {"available": bool(reality_link), "link": reality_link,
                     "note": None if reality_link else "REALITY هنوز کانفیگ نشده (کلید تولید نشده)."},
        "post_quantum": {"available": bool(pq_link), "link": pq_link,
                          "note": None if pq_link else "VLESS-PQ هنوز کانفیگ نشده — یا Xray-core نسخه‌ی قدیمی است (نیازمند v26+)."},
    }

# ── User Management (X5.2) ─────────────────────────────────────────────────────
# کاربر اینجا یعنی «مشتری/کاربر نهایی» که یک یا چند کانفیگ بهش تخصیص داده می‌شه —
# جدا از ADMIN_PASSWORD که فقط برای ورود خودِ ادمین به این پنله.
@app.get("/api/users")
async def list_users(request: Request, _=Depends(require_auth)):
    host = edge_host(request)
    async with USERS_LOCK, LINKS_LOCK:
        result = []
        for uid, u in USERS.items():
            owned = [LINKS[l] for l in u.get("link_uuids", []) if l in LINKS]
            result.append({
                "id": uid,
                "name": u["name"],
                "note": u.get("note", ""),
                "created_at": u["created_at"],
                "links_count": len(owned),
                "active_links_count": sum(1 for l in owned if is_link_allowed(l)),
                "total_used_bytes": sum(l.get("used_bytes", 0) for l in owned),
                "total_used_fmt": fmt_bytes(sum(l.get("used_bytes", 0) for l in owned)),
            })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"users": result}

@app.post("/api/users")
async def create_user(request: Request, _=Depends(require_auth)):
    body = await request.json()
    name = (body.get("name") or "").strip()[:60]
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    uid = "u_" + secrets.token_hex(6)
    async with USERS_LOCK:
        USERS[uid] = {
            "name": name,
            "note": (body.get("note") or "").strip()[:200],
            "created_at": datetime.now().isoformat(),
            "link_uuids": [],
        }
    asyncio.create_task(save_state())
    log_activity("user", f"کاربر «{name}» ساخته شد", "ok")
    return {"id": uid, **USERS[uid]}

@app.patch("/api/users/{uid}")
async def update_user(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with USERS_LOCK:
        if uid not in USERS:
            raise HTTPException(status_code=404, detail="user not found")
        if "name" in body:
            USERS[uid]["name"] = str(body["name"]).strip()[:60] or USERS[uid]["name"]
        if "note" in body:
            USERS[uid]["note"] = str(body.get("note") or "").strip()[:200]
    asyncio.create_task(save_state())
    return {"ok": True}

@app.delete("/api/users/{uid}")
async def delete_user(uid: str, unassign_only: bool = True, _=Depends(require_auth)):
    """پیش‌فرض: کانفیگ‌های این کاربر حذف نمی‌شن، فقط تخصیصشون برداشته می‌شه (unassign_only=true).
    برای حذف کامل کانفیگ‌های این کاربر هم، ?unassign_only=false رو ست کن."""
    async with USERS_LOCK, LINKS_LOCK:
        if uid not in USERS:
            raise HTTPException(status_code=404, detail="user not found")
        name = USERS[uid]["name"]
        owned_uuids = list(USERS[uid].get("link_uuids", []))
        if unassign_only:
            for l_uid in owned_uuids:
                if l_uid in LINKS:
                    LINKS[l_uid]["owner_user_id"] = None
        else:
            for l_uid in owned_uuids:
                LINKS.pop(l_uid, None)
        del USERS[uid]
    asyncio.create_task(save_state())
    log_activity("user", f"کاربر «{name}» حذف شد", "warn")
    return {"ok": True, "deleted": uid, "links_removed": (not unassign_only) and len(owned_uuids) or 0}

@app.get("/api/users/{uid}/links")
async def user_links(uid: str, request: Request, _=Depends(require_auth)):
    host = edge_host(request)
    async with USERS_LOCK, LINKS_LOCK:
        if uid not in USERS:
            raise HTTPException(status_code=404, detail="user not found")
        uuids = list(USERS[uid].get("link_uuids", []))
        links = []
        for l_uid in uuids:
            d = LINKS.get(l_uid)
            if not d:
                continue
            links.append({
                "uuid": l_uid, **d,
                "expired": is_link_expired(d),
                "vless_link": vless_link_for_link(d, l_uid, host),
            })
    return {"user": USERS[uid]["name"], "links": links}

# ── Per-config usage detail (X5.2) ─────────────────────────────────────────────
@app.get("/api/links/{uid}/usage")
async def link_usage(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
    if not link:
        raise HTTPException(status_code=404, detail="link not found")
    limit = link.get("limit_bytes", 0)
    used = link.get("used_bytes", 0)
    return {
        "uuid": uid,
        "label": link.get("label"),
        "owner_user_id": link.get("owner_user_id"),
        "owner_name": (USERS.get(link.get("owner_user_id")) or {}).get("name"),
        "used_bytes": used,
        "used_fmt": fmt_bytes(used),
        "limit_bytes": limit,
        "limit_fmt": fmt_bytes(limit) if limit else "نامحدود",
        "percent_used": round(used / limit * 100, 1) if limit else None,
        "speed_limit_mbps": round(link.get("speed_limit_bytes", 0) * 8 / 1024 / 1024, 2) if link.get("speed_limit_bytes") else None,
        "upload_limit_mbps": round(link.get("upload_limit_bytes", 0) * 8 / 1024 / 1024, 2) if link.get("upload_limit_bytes") else None,
        "download_limit_mbps": round(link.get("download_limit_bytes", 0) * 8 / 1024 / 1024, 2) if link.get("download_limit_bytes") else None,
        "connected_ips": len(unique_ips_for_uuid(uid)),
        "ip_limit": link.get("ip_limit", 0),
        "expires_at": link.get("expires_at"),
        "expired": is_link_expired(link),
        "active": link.get("active", True),
        "allowed_now": is_link_allowed(link),
    }

# ══════════════════════════════════════════════════════════════════════════════
# VLESS Relay — جدا شده به relay_vless.py (دست نخورده)
# ══════════════════════════════════════════════════════════════════════════════

from relay_vless import (
    RELAY_BUF,
    parse_vless_header,
    check_and_use,
    relay_ws_to_tcp,
    relay_tcp_to_ws,
    websocket_tunnel,
)

app.add_api_websocket_route("/ws/{uuid}", websocket_tunnel)

# ══════════════════════════════════════════════════════════════════════════════
# XHTTP — Siz10a XHTTP Ultra (ترابرد جدید، جدا از VLESS/WS، هر ۳ مد)
# ══════════════════════════════════════════════════════════════════════════════
from xhttp_siz10 import router as xhttp_router
app.include_router(xhttp_router)

# ══════════════════════════════════════════════════════════════════════════════
# X5G Level Up — Edge Sync (هماهنگی با ورکر Cloudflare؛ فقط با
# EDGE_SHARED_SECRET فعال می‌شه، در غیر این صورت 503 بی‌خطر برمی‌گردونه)
# ══════════════════════════════════════════════════════════════════════════════
from edge_sync import router as edge_sync_router
app.include_router(edge_sync_router)

# ══════════════════════════════════════════════════════════════════════════════
# X5G.1 — Edge Intelligence: /api/v1 (capabilities/API-key/webhook/telemetry)
# ══════════════════════════════════════════════════════════════════════════════
from edge_intel import router as edge_intel_router, dispatch_webhook
app.include_router(edge_intel_router)
register_webhook_hook(dispatch_webhook)

# ══════════════════════════════════════════════════════════════════════════════
# X5.3 — IP Intelligence: /api/v2 (اسکن چندنمونه‌ای + Pool پایدار + Neighbor + Subscription)
# ══════════════════════════════════════════════════════════════════════════════
from ip_intel import router as ip_intel_router
app.include_router(ip_intel_router)

# ══════════════════════════════════════════════════════════════════════════════
# ربات مدیریت تلگرام (اختیاری — فقط اگه TELEGRAM_BOT_TOKEN ست شده باشه فعال می‌شه)
# ══════════════════════════════════════════════════════════════════════════════
from telegram_bot import start_bot as _tg_start_bot, stop_bot as _tg_stop_bot

# ── HTTP Proxy ────────────────────────────────────────────────────────────────
_HOP = {"connection","keep-alive","proxy-authenticate","proxy-authorization",
        "te","trailers","transfer-encoding","upgrade","content-encoding","content-length"}

@app.api_route("/proxy/{target_url:path}", methods=["GET","POST","PUT","DELETE","PATCH","HEAD","OPTIONS"])
async def http_proxy(target_url: str, request: Request):
    if not target_url.startswith("http"):
        target_url = "https://" + target_url
    try:
        body = await request.body()
        headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP and k.lower() != "host"}
        resp = await http_client.request(method=request.method, url=target_url, headers=headers, content=body)
        stats["total_bytes"] += len(resp.content)
        stats["total_requests"] += 1
        hourly_traffic[now_ir().strftime("%H:00")] += len(resp.content)
        return Response(content=resp.content, status_code=resp.status_code,
                        headers={k: v for k, v in resp.headers.items() if k.lower() not in _HOP})
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "url": target_url, "time": datetime.now().isoformat()})
        raise HTTPException(status_code=502, detail=f"Proxy error: {exc}")

# ── Public sub page (یک صفحه‌ی زیبا و مستقل به‌ازای هر کانفیگ) ────────────────
@app.get("/p/{uuid_key}", response_class=HTMLResponse)
async def public_sub_page(uuid_key: str, request: Request):
    from pages import get_public_page_html
    async with LINKS_LOCK:
        exists = uuid_key in LINKS
    if not exists:
        return HTMLResponse("<h2 style='font-family:sans-serif;padding:40px'>کانفیگ پیدا نشد</h2>", status_code=404)
    return HTMLResponse(content=get_public_page_html(uuid_key))

@app.get("/api/public/sub/{uuid_key}")
async def public_sub_data(uuid_key: str, request: Request):
    async with LINKS_LOCK:
        link = LINKS.get(uuid_key)
    if not link:
        raise HTTPException(status_code=404, detail="not found")

    host = edge_host(request)
    allowed = is_link_allowed(link)
    conn_count = sum(1 for c in connections.values() if c.get("uuid") == uuid_key)
    proto = link.get("protocol", DEFAULT_PROTOCOL)
    link_out = {
        "uuid": uuid_key,
        "label": link["label"],
        "active": allowed,
        "protocol": proto,
        "used_bytes": link.get("used_bytes", 0),
        "used_fmt": fmt_bytes(link.get("used_bytes", 0)),
        "limit_bytes": link.get("limit_bytes", 0),
        "limit_fmt": "∞" if link.get("limit_bytes", 0) == 0 else fmt_bytes(link["limit_bytes"]),
        "expires_at": link.get("expires_at"),
        "vless_link": vless_link_for_link(link, uuid_key, host),
        "sub_url": f"https://{host}/sub/{uuid_key}",
        "connections": conn_count,
        "ip_limit": link.get("ip_limit", 0),
        "speed_limit_bytes": link.get("speed_limit_bytes", 0),
    }

    return {
        "locked": False,
        "name": link["label"],
        "desc": link.get("note", ""),
        "sub_url": f"https://{host}/p/{uuid_key}",
        "active_connections": conn_count,
        "total_used_fmt": fmt_bytes(link.get("used_bytes", 0)),
        "links": [link_out],
    }

# ── HTML Pages (login + dashboard) ───────────────────────────────────────────
from pages import LOGIN_HTML, DASHBOARD_HTML

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if await is_valid_session(request.cookies.get(SESSION_COOKIE)):
        return RedirectResponse(url="/dashboard")
    return HTMLResponse(content=LOGIN_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not await is_valid_session(request.cookies.get(SESSION_COOKIE)):
        return RedirectResponse(url="/login")
    return HTMLResponse(content=DASHBOARD_HTML)

@app.get("/test-ws", response_class=HTMLResponse)
async def test_ws_redirect():
    return HTMLResponse(content="<script>location.href='/dashboard'</script>")

if __name__ == "__main__":
    uvicorn.run(
        "main:app", host="0.0.0.0", port=CONFIG["port"], log_level="info", workers=1,
        # X5.2: تنظیم صریح ping/timeout برای اتصالات WebSocket طولانی‌مدت (VLESS-WS) —
        # روی شبکه‌های موبایل/NAT، این باعث می‌شه سرور زودتر (حدود ۲۵ ثانیه به‌جای
        # پیش‌فرض نامشخص کتابخانه) بفهمه یک اتصال واقعاً مرده و منابعش رو آزاد کنه؛
        # ping_interval هم به‌عنوان heartbeat به باز نگه‌داشتن NAT/CGNAT کمک می‌کنه.
        ws_ping_interval=20, ws_ping_timeout=25,
        timeout_keep_alive=75,  # برای اتصالات XHTTP طولانی (long-polling/stream) کافی‌تره
    )
