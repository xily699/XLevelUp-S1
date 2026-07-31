# edge_sync.py
# ══════════════════════════════════════════════════════════════════════════════
# X5G Level Up — Edge Sync API
#
# این ماژول پل ارتباطی بین ورکر Cloudflare (edge) و بک‌اند Railway (این
# کانتینر) رو فراهم می‌کنه. بک‌اند همیشه «منبع حقیقت» برای احراز هویت،
# کوتای حجم، محدودیت IP و آمار می‌مونه؛ ورکر فقط داده رو رله می‌کنه و برای
# هر تصمیم مهم از همین اندپوینت‌ها سوال می‌پرسه یا گزارش می‌ده.
#
# امنیت: تمام اندپوینت‌ها با هدر X-Edge-Secret محافظت می‌شن که باید دقیقاً
# با متغیر محیطی EDGE_SHARED_SECRET همین سرویس یکی باشه (و با EDGE_SECRET
# روی ورکر هم‌خونی داشته باشه). بدون تنظیم EDGE_SHARED_SECRET، این API
# کاملاً غیرفعاله (503) — یعنی اگه سینک نمی‌خوای، هیچ سطح حمله‌ی جدیدی هم
# باز نمی‌شه.
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import os
import secrets
import time
from datetime import datetime

from fastapi import APIRouter, Request, HTTPException, Header

from main import (
    LINKS,
    LINKS_LOCK,
    connections,
    stats,
    hourly_traffic,
    logger,
    is_link_allowed,
    is_ip_allowed,
    save_state,
    log_activity,
    now_ir,
    DEFAULT_PROTOCOL,
)

router = APIRouter(prefix="/api/edge", tags=["edge-sync"])

EDGE_SECRET = os.environ.get("EDGE_SHARED_SECRET", "").strip()


def _check_secret(x_edge_secret: str | None):
    if not EDGE_SECRET:
        raise HTTPException(status_code=503, detail="edge sync disabled: EDGE_SHARED_SECRET not set")
    if not x_edge_secret or not secrets.compare_digest(x_edge_secret, EDGE_SECRET):
        raise HTTPException(status_code=401, detail="invalid edge secret")


@router.get("/ping")
async def edge_ping(x_edge_secret: str | None = Header(default=None)):
    """چک سریع در دسترس بودن بک‌اند — ورکر برای تشخیص حالت hybrid/degraded صداش می‌زنه."""
    _check_secret(x_edge_secret)
    return {"ok": True, "ts": time.time(), "connections": len(connections)}


@router.get("/configs")
async def edge_configs(x_edge_secret: str | None = Header(default=None)):
    """اسنپ‌شات همه‌ی کانفیگ‌ها — ورکر این رو کش می‌کنه و در حالت fail-open
    (وقتی خودِ بک‌اند موقتاً در دسترس نیست) روی همین کش تصمیم می‌گیره."""
    _check_secret(x_edge_secret)
    async with LINKS_LOCK:
        out = [
            {
                "uuid": uid,
                "active": is_link_allowed(d),
                "protocol": d.get("protocol", DEFAULT_PROTOCOL),
                "port": d.get("port"),
                "ip_limit": d.get("ip_limit", 0),
                "speed_limit_bytes": d.get("speed_limit_bytes", 0),
                "limit_bytes": d.get("limit_bytes", 0),
                "used_bytes": d.get("used_bytes", 0),
                "expires_at": d.get("expires_at"),
            }
            for uid, d in LINKS.items()
        ]
    return {"configs": out, "count": len(out), "ts": time.time()}


@router.post("/connect")
async def edge_connect(request: Request, x_edge_secret: str | None = Header(default=None)):
    """ورکر قبل از شروع رله برای هر اتصال جدید این رو صدا می‌زنه.
    همون منطق is_link_allowed / is_ip_allowed که برای WS مستقیم استفاده
    می‌شه اینجا هم اجرا می‌شه، پس محدودیت IP/کوتا/انقضا برای اتصالات edge
    هم دقیقاً به همون اندازه‌ی اتصالات مستقیم به کانتینر معتبره."""
    _check_secret(x_edge_secret)
    body = await request.json()
    uuid = (body.get("uuid") or "").strip()
    ip = (body.get("ip") or "edge-unknown").strip()
    transport = (body.get("transport") or "edge-ws").strip()

    async with LINKS_LOCK:
        link = LINKS.get(uuid)
        if not is_link_allowed(link):
            return {"allow": False, "reason": "not-authorized-or-expired-or-quota"}
        if not is_ip_allowed(link, uuid, ip):
            return {"allow": False, "reason": "ip-limit-reached"}

        conn_id = "edge-" + secrets.token_urlsafe(6)
        connections[conn_id] = {
            "uuid": uuid,
            "ip": ip,
            "transport": transport,
            "connected_at": datetime.now().isoformat(),
            "bytes": 0,
            "source": "edge",
        }
        label = link.get("label", "?")
        speed_limit_bytes = link.get("speed_limit_bytes", 0)

    log_activity("connection", f"اتصال Edge از {ip} (کانفیگ «{label}»)", "info")
    return {"allow": True, "conn_id": conn_id, "speed_limit_bytes": speed_limit_bytes}


@router.post("/usage")
async def edge_usage(request: Request, x_edge_secret: str | None = Header(default=None)):
    """ورکر به‌صورت batched (نه هر پکت) مصرف رو گزارش می‌ده تا هم آمار
    داشبورد درست بمونه، هم کوتا/انقضا لحظه‌ای اعمال بشه. اگه کوتا در
    همین لحظه تموم شده باشه allow=false برمی‌گرده تا ورکر سشن رو ببنده."""
    _check_secret(x_edge_secret)
    body = await request.json()
    conn_id = body.get("conn_id")
    uuid = (body.get("uuid") or "").strip()
    nbytes = int(body.get("bytes") or 0)
    if nbytes <= 0:
        return {"ok": True, "allow": True}

    async with LINKS_LOCK:
        link = LINKS.get(uuid)
        allow = is_link_allowed(link)
        if allow and link is not None:
            link["used_bytes"] = link.get("used_bytes", 0) + nbytes
            stats["total_bytes"] += nbytes
            hourly_traffic[now_ir().strftime("%H:00")] += nbytes
        if conn_id in connections:
            connections[conn_id]["bytes"] += nbytes
        stats["total_requests"] += 1

    return {"ok": True, "allow": allow}


@router.post("/disconnect")
async def edge_disconnect(request: Request, x_edge_secret: str | None = Header(default=None)):
    _check_secret(x_edge_secret)
    body = await request.json()
    conn_id = body.get("conn_id")
    async with LINKS_LOCK:
        connections.pop(conn_id, None)
    asyncio.create_task(save_state())
    return {"ok": True}


@router.get("/status")
async def edge_status(x_edge_secret: str | None = Header(default=None)):
    """آمار کلی برای داشبورد/مانیتورینگ — تفکیک اتصالات edge از اتصالات مستقیم."""
    _check_secret(x_edge_secret)
    edge_count = sum(1 for c in connections.values() if c.get("source") == "edge")
    direct_count = len(connections) - edge_count
    return {
        "ok": True,
        "connections_total": len(connections),
        "connections_edge": edge_count,
        "connections_direct": direct_count,
        "links_count": len(LINKS),
        "sync_enabled": bool(EDGE_SECRET),
    }
