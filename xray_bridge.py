# xray_bridge.py — X5.3-LevelUp
# ══════════════════════════════════════════════════════════════════════════════
# پل ارتباطی بین پنل پایتون (کنترل‌پلین فعلی، دست‌نخورده) و Xray-core که به‌عنوان
# process مستقل کنار همین کانتینر اجرا می‌شه (نگاه کن به Dockerfile / start.sh).
#
# اصل طراحی: هیچ‌کدوم از توابع این فایل نباید بتونن جریان اصلی پنل (ساخت/حذف
# کانفیگ روی relay_vless.py/xhttp_siz10.py) رو بشکنن. هر تابع خودش try/except
# داره و در بدترین حالت فقط لاگ می‌کنه — Xray یک قابلیت اضافه‌ست، نه یک وابستگی.
#
# فعال‌سازی: با env var زیر کنترل می‌شه؛ پیش‌فرض خاموشه (رفتار فعلی دست‌نخورده):
#   XRAY_ENABLED=true
#   XRAY_API_PORT=10085   (پیش‌فرض)
#   XRAY_BRIDGE_TOKEN=<یک secret دلخواه>  — چون خودِ gRPC API این‌جا توکن ندیجه‌ای
#     نداره (فقط روی 127.0.0.1 گوش می‌ده)، این توکن در سطح پنل پایتون چک می‌شه:
#     فقط درخواست‌های داخلی/ادمین که این توکن رو بدونن می‌تونن وضعیت/عملیات Xray
#     رو صدا بزنن — یعنی لایه‌ی امنیتی واقعی همینه، نه یک "توکن" ساختگی سمت Xray.
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import json
import logging
import os
import shutil

logger = logging.getLogger("xray_bridge")

XRAY_ENABLED = os.environ.get("XRAY_ENABLED", "false").strip().lower() == "true"
XRAY_API_ADDR = f"127.0.0.1:{os.environ.get('XRAY_API_PORT', '10085')}"
XRAY_BRIDGE_TOKEN = os.environ.get("XRAY_BRIDGE_TOKEN", "").strip()
XRAY_BIN = shutil.which("xray") or "/opt/xray/xray"
GRPCURL_BIN = shutil.which("grpcurl") or "/usr/local/bin/grpcurl"

# سه inbound که با xray/config.json.template ساخته می‌شن — همون‌هایی که کاربر
# خواست (Reality عمداً اضافه نشده):
XRAY_INBOUND_TAGS = ("vless-ws-xray", "vless-grpc", "vless-splithttp")


def bridge_token_ok(token: str | None) -> bool:
    """چک می‌کنه که درخواست‌کننده توکن داخلی درست رو داره. اگه XRAY_BRIDGE_TOKEN
    اصلاً ست نشده باشه، این لایه غیرفعاله (یعنی فقط نیاز به لاگین ادمین پنل کافیه —
    که همه‌ی endpointهای این بریج پشت همون require_auth هم هستن)."""
    if not XRAY_BRIDGE_TOKEN:
        return True
    return token == XRAY_BRIDGE_TOKEN


async def _run(cmd: list[str], timeout: float = 6.0) -> tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, out.decode(errors="ignore"), err.decode(errors="ignore")
    except asyncio.TimeoutError:
        return -1, "", "timeout"
    except FileNotFoundError:
        return -1, "", "binary not found"
    except Exception as e:
        return -1, "", str(e)


async def xray_status() -> dict:
    """وضعیت کامل موتور Xray برای نمایش تو داشبورد (پنل «امنیت وضعیت Xray Engine»)."""
    if not XRAY_ENABLED:
        return {"enabled": False, "reachable": False, "state": "disabled",
                "detail": "XRAY_ENABLED=true نیست — پنل فقط با موتور پایتونی فعلی کار می‌کنه."}
    if not os.path.exists(XRAY_BIN):
        return {"enabled": True, "reachable": False, "state": "not_installed",
                "detail": "باینری Xray پیدا نشد — باید با Dockerfile نصب بشه (نه Nixpacks)."}
    rc, out, err = await _run([XRAY_BIN, "api", "statsquery", "-s", XRAY_API_ADDR, "-pattern", "inbound"])
    if rc != 0:
        return {"enabled": True, "reachable": False, "state": "unreachable",
                "detail": (err or out or "بدون پاسخ از API داخلی Xray").strip()[:300]}
    try:
        data = json.loads(out or "{}")
        stat_count = len(data.get("stat", []))
    except Exception:
        stat_count = 0
    return {
        "enabled": True, "reachable": True, "state": "connected",
        "api_addr": XRAY_API_ADDR,
        "inbounds": list(XRAY_INBOUND_TAGS),
        "tracked_stats": stat_count,
        "detail": "Xray-core متصل و پاسخگوست.",
    }


async def xray_add_user(uuid: str, email: str) -> dict:
    """کاربر رو به هر سه inbound (ws/grpc/splithttp) اضافه می‌کنه — بدون ری‌استارت.
    Best-effort: اگه Xray خاموش/غیرفعال باشه، فقط لاگ می‌کنه و هیچ استثنایی به
    بیرون (main.py) پرت نمی‌کنه، تا هیچ‌وقت ساخت کانفیگ اصلی رو نشکنه."""
    if not XRAY_ENABLED:
        return {"ok": False, "skipped": True}
    results = {}
    for tag in XRAY_INBOUND_TAGS:
        req = {
            "tag": tag,
            "operation": {
                "@type": "xray.app.proxyman.command.AddUserOperation",
                "user": {
                    "email": email,
                    "account": {"@type": "xray.proxy.vless.Account", "id": uuid, "flow": ""},
                },
            },
        }
        rc, out, err = await _run([
            "sh", "-c",
            f"echo {json.dumps(json.dumps(req))} | {GRPCURL_BIN} -plaintext -d @ {XRAY_API_ADDR} "
            f"xray.app.proxyman.command.HandlerService/AlterInbound"
        ])
        results[tag] = "ok" if rc == 0 else (err or out)[:200]
        if rc != 0:
            logger.warning(f"[xray_bridge] add_user failed on {tag}: {results[tag]}")
    return {"ok": all(v == "ok" for v in results.values()), "per_inbound": results}


async def xray_remove_user(email: str) -> dict:
    """کاربر رو از هر سه inbound حذف می‌کنه — بدون ری‌استارت. مشابه xray_add_user،
    کاملاً best-effort و هیچ‌وقت باعث fail‌شدن remove_link در main.py نمی‌شه."""
    if not XRAY_ENABLED:
        return {"ok": False, "skipped": True}
    results = {}
    for tag in XRAY_INBOUND_TAGS:
        req = {
            "tag": tag,
            "operation": {"@type": "xray.app.proxyman.command.RemoveUserOperation", "email": email},
        }
        rc, out, err = await _run([
            "sh", "-c",
            f"echo {json.dumps(json.dumps(req))} | {GRPCURL_BIN} -plaintext -d @ {XRAY_API_ADDR} "
            f"xray.app.proxyman.command.HandlerService/AlterInbound"
        ])
        results[tag] = "ok" if rc == 0 else (err or out)[:200]
    return {"ok": all(v == "ok" for v in results.values()), "per_inbound": results}


async def xray_sync_all_users(links: dict):
    """موقع بالا اومدن پنل صدا زده می‌شه (main.py startup) — همه‌ی UUIDهای فعلی
    LINKS رو best-effort به Xray اضافه می‌کنه (چون بعد از هر ری‌استارت Xray،
    inboundهاش با clients خالی شروع می‌شن — تمپلیت config.json.template همین‌طوره)."""
    if not XRAY_ENABLED:
        return
    ok, fail = 0, 0
    for uid, link in dict(links).items():
        r = await xray_add_user(uid, link.get("label", uid))
        ok += 1 if r.get("ok") else 0
        fail += 0 if r.get("ok") else 1
    logger.info(f"[xray_bridge] sync اولیه‌ی کاربران: {ok} موفق، {fail} ناموفق (از کل {ok+fail})")


async def xray_usage(email: str) -> dict:
    """مصرف uplink/downlink یک کاربر مشخص از Stats API (اگه Xray فعال باشه)."""
    if not XRAY_ENABLED:
        return {"ok": False, "skipped": True}
    rc, out, err = await _run([XRAY_BIN, "api", "statsquery", "-s", XRAY_API_ADDR,
                                "-pattern", f"user>>>{email}>>>traffic"])
    if rc != 0:
        return {"ok": False, "error": (err or out)[:200]}
    try:
        data = json.loads(out or "{}")
        up = down = 0
        for s in data.get("stat", []):
            if s["name"].endswith("uplink"):
                up = s.get("value", 0)
            elif s["name"].endswith("downlink"):
                down = s.get("value", 0)
        return {"ok": True, "uplink": up, "downlink": down, "total": up + down}
    except Exception as e:
        return {"ok": False, "error": str(e)}
