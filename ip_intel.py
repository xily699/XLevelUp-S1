# ══════════════════════════════════════════════════════════════════════════════
# X5.3 — IP Intelligence: اسکن چندنمونه‌ای + امتیازدهی + Neighbor Discovery + Pool پایدار
#
# صادقانه: این ماژول هیچ‌جوره نمی‌تونه یه کلاینت VLESS معمولی رو مجبور کنه که
# وسط اتصال IP عوض کنه — اون قابلیت باید خودِ کلاینت (sing-box با گروه
# urltest/fallback) پشتیبانی کنه. کاری که اینجا واقعاً انجام می‌شه:
#   ۱) IPهای کلودفلر رو با چند نمونه (نه یه پینگ تنها) امتیاز می‌ده (تأخیر/جیتر/نرخ موفقیت)
#   ۲) نتیجه‌ی هر اسکن رو با EMA به یه Pool پایدار روی دیسک merge می‌کنه، نه overwrite
#   ۳) IPهای همسایه‌ی یک IP سالم (همون subnet /24) رو خودکار تست می‌کنه
#   ۴) یه subscription واقعی sing-box (urltest group با interval/tolerance کوتاه) می‌سازه
#      که در عمل نزدیک‌ترین چیز به "چنج خودکار و سریع IP" است که از سرور قابل ارائه‌ست.
# ══════════════════════════════════════════════════════════════════════════════
import asyncio
import ipaddress
import json
import statistics
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/v2", tags=["ip-intel"])

DATA_DIR = Path(__import__("os").environ.get("DATA_DIR", "/data"))
POOL_FILE = DATA_DIR / "x5g_ip_pool.json"
POOL_LOCK = asyncio.Lock()

# نگه‌داری در حافظه؛ روی دیسک هم persist می‌شه تا با ری‌استارت از دست نره
POOL: dict[str, dict] = {}

MAX_POOL_SIZE = 60          # بیشتر از این نگه نمی‌داریم (فقط بهترین‌ها)
DEAD_AFTER_FAILS = 3        # این‌قدر شکست پشت‌سرهم → از Pool حذف می‌شه
EMA_ALPHA = 0.35            # وزنِ نمونه‌ی تازه در میانگین متحرک (تعادل بین پایداری و واکنش سریع)


def _load_pool():
    global POOL
    try:
        if POOL_FILE.exists():
            POOL = json.loads(POOL_FILE.read_text(encoding="utf-8"))
    except Exception:
        POOL = {}


def _save_pool():
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = POOL_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(POOL, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(POOL_FILE)
    except Exception:
        pass


_load_pool()


async def _probe_once(ip: str, port: int, timeout: float) -> float | None:
    """یک اتصال TCP خام (نه HTTP) باز می‌کنه و RTT واقعی handshake رو برمی‌گردونه.
    None یعنی شکست (timeout/رد شدن/غیرقابل‌دسترس)."""
    t0 = time.perf_counter()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout=timeout)
        ms = (time.perf_counter() - t0) * 1000
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return ms
    except Exception:
        return None


async def score_ip(ip: str, port: int = 443, samples: int = 5, timeout: float = 2.0) -> dict:
    """چند بار پشت‌سرهم تست می‌کنه (نه یه‌بار) تا جیتر واقعی به دست بیاد، نه فقط یه پینگ تصادفی."""
    results = []
    for _ in range(samples):
        ms = await _probe_once(ip, port, timeout)
        results.append(ms)
        await asyncio.sleep(0.05)
    ok = [r for r in results if r is not None]
    success_rate = len(ok) / len(results)
    if not ok:
        return {"ip": ip, "ok": False, "avg_ms": None, "jitter_ms": None, "success_rate": 0.0, "score": 0.0}
    avg_ms = sum(ok) / len(ok)
    jitter_ms = statistics.pstdev(ok) if len(ok) > 1 else 0.0
    # امتیاز ترکیبی: تأخیر کم = خوب، جیتر کم = خوب، نرخ موفقیت بالا = خوب.
    # نرمال‌سازی تقریبی روی بازه‌های واقعی (avg تا ۴۰۰ms، جیتر تا ۱۵۰ms)
    latency_score = max(0.0, 1 - min(avg_ms, 400) / 400)
    jitter_score = max(0.0, 1 - min(jitter_ms, 150) / 150)
    composite = round((latency_score * 0.5 + jitter_score * 0.3 + success_rate * 0.2) * 100, 1)
    return {
        "ip": ip, "ok": True, "avg_ms": round(avg_ms, 1), "jitter_ms": round(jitter_ms, 1),
        "success_rate": round(success_rate, 2), "score": composite,
    }


def _merge_into_pool(result: dict, port: int):
    """نتیجه‌ی جدید رو با EMA به رکورد قبلی merge می‌کنه — یعنی یه IP که یه‌بار
    ضعیف جواب داده فوراً حذف نمی‌شه، ولی روند افت مداومش رو هم گم نمی‌کنیم."""
    ip = result["ip"]
    now = time.time()
    prev = POOL.get(ip)
    if not result["ok"]:
        fails = (prev.get("consecutive_fails", 0) + 1) if prev else 1
        if prev:
            prev["consecutive_fails"] = fails
            prev["last_seen"] = now
            prev["last_ok"] = False
            if fails >= DEAD_AFTER_FAILS:
                POOL.pop(ip, None)
        return
    if prev and prev.get("score") is not None:
        new_score = EMA_ALPHA * result["score"] + (1 - EMA_ALPHA) * prev["score"]
        new_avg = EMA_ALPHA * result["avg_ms"] + (1 - EMA_ALPHA) * prev.get("avg_ms", result["avg_ms"])
        new_jit = EMA_ALPHA * result["jitter_ms"] + (1 - EMA_ALPHA) * prev.get("jitter_ms", result["jitter_ms"])
    else:
        new_score, new_avg, new_jit = result["score"], result["avg_ms"], result["jitter_ms"]
    POOL[ip] = {
        "ip": ip, "port": port, "score": round(new_score, 1), "avg_ms": round(new_avg, 1),
        "jitter_ms": round(new_jit, 1), "success_rate": result["success_rate"],
        "consecutive_fails": 0, "last_seen": now, "last_ok": True,
        "first_seen": (prev or {}).get("first_seen", now),
    }


def _trim_pool():
    if len(POOL) <= MAX_POOL_SIZE:
        return
    ranked = sorted(POOL.values(), key=lambda r: r["score"], reverse=True)[:MAX_POOL_SIZE]
    POOL.clear()
    for r in ranked:
        POOL[r["ip"]] = r


def neighbor_subnet(seed_ip: str) -> list[str]:
    """آدرس‌های همسایه‌ی همون /24 رو برمی‌گردونه (بدون .0 و .255) —
    چون رنج‌های کلودفلر پیوسته‌ان، اگه یه IP سالم پیدا شد، همسایه‌هاش شانس خوبی دارن."""
    try:
        net = ipaddress.ip_network(f"{seed_ip}/24", strict=False)
    except ValueError:
        return []
    return [str(h) for h in net.hosts()]


@router.post("/scan")
async def api_scan(request: Request):
    """body: {ips:[...]} یا {seed:'x.x.x.x', neighbors:true} یا {seed, neighbors:false}
    محدود به ۲۵۶ آدرس در هر فراخوانی تا اجرای Railway طولانی/سنگین نشه."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    ips = list(body.get("ips") or [])
    seed = (body.get("seed") or "").strip()
    if seed and body.get("neighbors"):
        ips = list(dict.fromkeys(ips + neighbor_subnet(seed)))
    elif seed:
        ips = list(dict.fromkeys(ips + [seed]))
    ips = [ip.strip() for ip in ips if ip.strip()][:256]
    if not ips:
        raise HTTPException(400, "هیچ IPای برای اسکن داده نشده")
    port = int(body.get("port") or 443)
    samples = max(3, min(int(body.get("samples") or 5), 8))

    sem = asyncio.Semaphore(24)

    async def _run(ip):
        async with sem:
            return await score_ip(ip, port, samples)

    results = await asyncio.gather(*[_run(ip) for ip in ips])
    async with POOL_LOCK:
        for r in results:
            _merge_into_pool(r, port)
        _trim_pool()
        _save_pool()
    ranked = sorted(results, key=lambda r: r["score"], reverse=True)
    return JSONResponse({"ok": True, "scanned": len(ips), "results": ranked})


@router.get("/pool")
async def api_pool(limit: int = 20):
    ranked = sorted(POOL.values(), key=lambda r: r["score"], reverse=True)[:max(1, min(limit, MAX_POOL_SIZE))]
    return JSONResponse({"ok": True, "count": len(ranked), "pool": ranked})


@router.get("/subscription/{uuid}")
async def api_subscription(uuid: str, host: str = "", path: str = "", top: int = 6):
    """خروجی sing-box واقعی: یه outbound به‌ازای هر IP برتر (server=IP، ولی
    tls.server_name/transport.headers.Host = دامنه‌ی Worker)، همه زیر یک گروه
    urltest با interval/tolerance کوتاه — این دقیقاً همون چیزیه که باعث می‌شه
    sing-box خودش ظرف چند ثانیه، بدون صبر ۱۰-۱۵ ثانیه‌ای، بره سراغ IP بعدی."""
    if not host:
        raise HTTPException(400, "پارامتر host (دامنه‌ی Worker) لازم است")
    ranked = sorted(POOL.values(), key=lambda r: r["score"], reverse=True)[:max(1, min(top, 12))]
    if not ranked:
        raise HTTPException(404, "هنوز هیچ IP سالمی تو Pool نیست — اول یه اسکن بزن")
    ws_path = path or f"/ws/{uuid}"
    outbounds = []
    tags = []
    for i, r in enumerate(ranked):
        tag = f"x5g-edge-{i+1} ({r['ip']})"
        tags.append(tag)
        outbounds.append({
            "type": "vless", "tag": tag,
            "server": r["ip"], "server_port": 443, "uuid": uuid,
            "tls": {"enabled": True, "server_name": host, "utls": {"enabled": True, "fingerprint": "chrome"}},
            "transport": {"type": "ws", "path": ws_path, "headers": {"Host": host}},
        })
    outbounds.append({
        "type": "urltest", "tag": "x5g-auto", "outbounds": tags,
        "url": "https://cp.cloudflare.com/generate_204",
        "interval": "10s", "tolerance": 50, "idle_timeout": "5m",
    })
    return JSONResponse({
        "outbounds": outbounds,
        "route": {"final": "x5g-auto"},
        "_note": "این یه بلوک outbounds/route جزئیه — تو کانفیگ کامل sing-boxت زیر outbounds/route ادغامش کن.",
    })
