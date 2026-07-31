# edge_intel.py
# ══════════════════════════════════════════════════════════════════════════════
# X5G.1 — Edge Intelligence
#
# سه قابلیت جدید که این نسخه رو از یک بک‌اند ساده به یک «پلتفرم قابل‌اتصال»
# تبدیل می‌کنه:
#
#   1) /api/v1/capabilities  — یک اندپوینت عمومی (بدون نیاز به رمز) که
#      می‌گه این بک‌اند دقیقاً چه چیزهایی بلده. اگه بعداً یک پنل مدیریتی
#      جدا ساختی، فقط با گرفتن همین آدرس + یک API Key، خودش می‌فهمه با
#      چی طرفه — بدون هاردکد کردن هیچ جزئیاتی توی پنل جدید.
#
#   2) API Key لایه‌ی دوم احراز هویت (جدا از رمز داشبورد) — برای پنل‌های
#      بیرونی/اسکریپت‌ها/اتوماسیون، با دو سطح دسترسی: read-only و full.
#
#   3) Webhook dispatcher — روی رویدادهای مهم (کوتای یک کانفیگ تموم شد،
#      IP بن شد، خطای پشت‌سرهم) یک POST به آدرس دلخواه (مثلاً یک بات
#      تلگرام دیگه، n8n، Discord webhook) می‌فرسته. غیرفعاله مگر
#      WEBHOOK_URL ست بشه.
#
#   4) Latency telemetry — Worker می‌تونه گزارش latency/circuit-breaker
#      خودش رو اینجا push کنه تا در داشبورد دیده بشه (ring buffer ساده،
#      نیازی به دیتابیس نیست).
# ══════════════════════════════════════════════════════════════════════════════

import os
import time
from collections import deque

import httpx
from fastapi import APIRouter, Request, HTTPException, Header

from main import (
    LINKS,
    LINKS_LOCK,
    USERS,
    USERS_LOCK,
    connections,
    stats,
    logger,
    is_link_allowed,
    fmt_bytes,
    uptime,
    DEFAULT_PROTOCOL,
    PROTOCOLS,
    CONFIG,
)

router = APIRouter(prefix="/api/v1", tags=["v1-public-api"])

# ── API Keys: چند تا می‌تونی بدی، جدا با کاما. پیشوند "ro-" یعنی read-only ──
_raw_keys = [k.strip() for k in os.environ.get("API_KEYS", "").split(",") if k.strip()]
API_KEYS_FULL = {k for k in _raw_keys if not k.startswith("ro-")}
API_KEYS_RO = {k for k in _raw_keys if k.startswith("ro-")}

WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").strip()
WEBHOOK_EVENTS = deque(maxlen=100)  # آخرین رویدادهای ارسال‌شده، برای دیباگ سریع

_latency_reports = deque(maxlen=200)  # {colo, edge_ms, backend_ms, mode, ts}


def require_api_key(x_api_key: str | None = Header(default=None), allow_ro: bool = True):
    if not (API_KEYS_FULL or API_KEYS_RO):
        raise HTTPException(status_code=503, detail="API key access disabled: API_KEYS not set")
    if not x_api_key:
        raise HTTPException(status_code=401, detail="X-API-Key header required")
    if x_api_key in API_KEYS_FULL:
        return "full"
    if allow_ro and x_api_key in API_KEYS_RO:
        return "read-only"
    raise HTTPException(status_code=403, detail="invalid or insufficient API key")


async def dispatch_webhook(event: str, payload: dict):
    """Fire-and-forget؛ هیچ‌وقت نباید جریان اصلی رله رو کند کنه یا خطا پرت کنه."""
    entry = {"event": event, "payload": payload, "ts": datetime_now_iso()}
    WEBHOOK_EVENTS.append(entry)
    if not WEBHOOK_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            await client.post(WEBHOOK_URL, json=entry)
    except Exception as exc:
        logger.warning(f"webhook dispatch failed: {exc}")


def datetime_now_iso() -> str:
    from datetime import datetime
    return datetime.now().isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# Capabilities — برای کشف خودکار توسط پنل‌های آینده
# ══════════════════════════════════════════════════════════════════════════════
@router.get("/capabilities")
async def capabilities():
    return {
        "product": "X5.2-LevelUp",
        "version": "5.2",
        "engine": "worker+railway hybrid",
        "protocols": list(PROTOCOLS) if isinstance(PROTOCOLS, (list, set, tuple)) else [DEFAULT_PROTOCOL],
        "default_protocol": DEFAULT_PROTOCOL,
        "features": {
            "quota_per_config": True,
            "speed_limit_per_config": True,
            "directional_speed_limit": True,  # X5.2: آپلود/دانلود مستقل
            "ip_limit_per_config": True,
            "expiring_links": True,
            "adaptive_flow_control": True,
            "edge_sync": bool(os.environ.get("EDGE_SHARED_SECRET", "").strip()),
            "worker_edge_domain": bool(CONFIG.get("worker_domain")),  # X5.2
            "user_management": True,  # X5.2
            "webhooks": bool(WEBHOOK_URL),
            "api_key_auth": bool(API_KEYS_FULL or API_KEYS_RO),
            "telegram_bot": bool(os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()),
        },
        "endpoints": {
            "capabilities": "/api/v1/capabilities",
            "status_public": "/api/v1/status",
            "links_readonly": "/api/v1/links",
            "connections_readonly": "/api/v1/connections",
            "users_readonly": "/api/v1/users",
            "edge_sync": "/api/edge/* (requires X-Edge-Secret)",
            "admin_dashboard": "/dashboard",
            "worker_dashboard": "https://<WORKER_PUBLIC_DOMAIN>/ (standalone edge dashboard, served by worker.js itself)",
        },
        "auth": {
            "dashboard": "session cookie (ADMIN_PASSWORD)",
            "external_api": "X-API-Key header (see API_KEYS env var, prefix ro- for read-only)",
        },
    }


@router.get("/status")
async def public_status():
    """نسخه‌ی عمومی و بدون رمز از وضعیت کلی — برای مانیتورینگ بیرونی/uptime-checkerها."""
    async with LINKS_LOCK:
        active = sum(1 for l in LINKS.values() if is_link_allowed(l))
        total = len(LINKS)
    return {
        "status": "ok",
        "uptime": uptime(),
        "active_links": active,
        "total_links": total,
        "connections": len(connections),
    }


# ══════════════════════════════════════════════════════════════════════════════
# سطح دسترسی با API Key — سطح فقط-خواندنی برای پنل‌های بیرونی
# ══════════════════════════════════════════════════════════════════════════════
@router.get("/links")
async def v1_links(scope: str = None, x_api_key: str | None = Header(default=None)):
    require_api_key(x_api_key)
    async with LINKS_LOCK:
        return {
            "links": [
                {
                    "uuid": uid,
                    "label": d.get("label"),
                    "active": is_link_allowed(d),
                    "protocol": d.get("protocol", DEFAULT_PROTOCOL),
                    "used_bytes": d.get("used_bytes", 0),
                    "used_fmt": fmt_bytes(d.get("used_bytes", 0)),
                    "limit_bytes": d.get("limit_bytes", 0),
                    "expires_at": d.get("expires_at"),
                }
                for uid, d in LINKS.items()
            ]
        }


@router.get("/connections")
async def v1_connections(x_api_key: str | None = Header(default=None)):
    require_api_key(x_api_key)
    return {
        "count": len(connections),
        "connections": [
            {"uuid": c.get("uuid"), "ip": c.get("ip"), "transport": c.get("transport"),
             "source": c.get("source", "direct"), "bytes": c.get("bytes", 0)}
            for c in connections.values()
        ],
    }


@router.get("/users")
async def v1_users(x_api_key: str | None = Header(default=None)):
    require_api_key(x_api_key)
    async with USERS_LOCK, LINKS_LOCK:
        return {
            "users": [
                {
                    "id": uid,
                    "name": u["name"],
                    "links_count": len(u.get("link_uuids", [])),
                    "total_used_bytes": sum(LINKS[l]["used_bytes"] for l in u.get("link_uuids", []) if l in LINKS),
                }
                for uid, u in USERS.items()
            ]
        }


# ══════════════════════════════════════════════════════════════════════════════
# Latency telemetry از Worker (بخشی از Edge Sync، ولی جدا نگه داشته شده چون
# مربوط به تله‌متری/دیباگه نه احراز هویت رله)
# ══════════════════════════════════════════════════════════════════════════════
@router.post("/edge-latency-report", tags=["edge-sync"])
async def edge_latency_report(request: Request, x_edge_secret: str | None = Header(default=None)):
    edge_secret = os.environ.get("EDGE_SHARED_SECRET", "").strip()
    if not edge_secret:
        raise HTTPException(status_code=503, detail="edge sync disabled")
    if not x_edge_secret or x_edge_secret != edge_secret:
        raise HTTPException(status_code=401, detail="invalid edge secret")
    body = await request.json()
    _latency_reports.append({
        "colo": body.get("colo"),
        "edge_connect_ms": body.get("edge_connect_ms"),
        "circuit_open_hosts": body.get("circuit_open_hosts", []),
        "failover_count": body.get("failover_count", 0),
        "ts": time.time(),
    })
    return {"ok": True}


@router.get("/edge-latency-report", tags=["edge-sync"])
async def edge_latency_report_read(x_api_key: str | None = Header(default=None)):
    require_api_key(x_api_key)
    return {"reports": list(_latency_reports)[-50:]}
