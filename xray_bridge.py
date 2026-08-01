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

# چهار inbound که با xray/config.json.template ساخته می‌شن — REALITY هم اضافه شد
# (کلید x25519ش تو start.sh یه‌بار ساخته و رو دیسک پایدار نگه داشته می‌شه):
XRAY_INBOUND_TAGS = ("vless-ws-xray", "vless-grpc", "vless-splithttp", "vless-reality-vision", "vless-pq-encryption")

# فقط این تگ به flow نیاز داره (بقیه اگه xtls-rprx-vision بگیرن اتصال می‌شکنه):
REALITY_TAG = "vless-reality-vision"

# X5.3: تگ inbound جدید «VLESS Post-Quantum Encryption» (ML-KEM-768 + X25519).
# برخلاف REALITY، این تگ نیاز به TLS بیرونی نداره — امنیتش داخل خودِ VLESS است.
PQ_TAG = "vless-pq-encryption"


def bridge_token_ok(token: str | None) -> bool:
    """چک می‌کنه که درخواست‌کننده توکن داخلی درست رو داره. اگه XRAY_BRIDGE_TOKEN
    اصلاً ست نشده باشه، این لایه غیرفعاله (یعنی فقط نیاز به لاگین ادمین پنل کافیه —
    که همه‌ی endpointهای این بریج پشت همون require_auth هم هستن)."""
    if not XRAY_BRIDGE_TOKEN:
        return True
    return token == XRAY_BRIDGE_TOKEN


def _reality_env_file() -> str:
    return os.path.join(os.environ.get("DATA_DIR", "/data"), "x5g_reality.env")


def reality_info() -> dict:
    """اطلاعات عمومی REALITY (کلید خصوصی هرگز از اینجا خونده/برگردونده نمی‌شه)."""
    path = _reality_env_file()
    info = {"configured": False}
    try:
        if os.path.exists(path):
            kv = {}
            with open(path) as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        kv[k] = v
            info = {
                "configured": bool(kv.get("REALITY_PUBLIC_KEY")),
                "public_key": kv.get("REALITY_PUBLIC_KEY", ""),
                "short_id": kv.get("REALITY_SHORT_ID", ""),
                "server_name": kv.get("REALITY_SERVER_NAME", ""),
                "dest": kv.get("REALITY_DEST", ""),
            }
    except Exception as e:
        logger.warning(f"[xray_bridge] reality_info خواندنی نبود: {e}")
    return info


def reality_client_link(uuid: str, server_ip_or_domain: str, port: int, remark: str = "X5G-Reality") -> str | None:
    """لینک vless:// آماده برای کلاینت. پورت رو صریح از ورودی می‌گیریم (هاردکد
    443 نمی‌کنیم) چون Railway TCP Proxy معمولاً یه پورت رندوم می‌ده، نه ۴۴۳."""
    info = reality_info()
    if not info.get("configured"):
        return None
    from urllib.parse import quote
    q = (
        f"security=reality&sni={info['server_name']}&fp=chrome&pbk={info['public_key']}"
        f"&sid={info['short_id']}&spx=%2F&type=tcp&flow=xtls-rprx-vision&encryption=none"
    )
    return f"vless://{uuid}@{server_ip_or_domain}:{port}?{q}#{quote(remark)}"


def _pq_env_file() -> str:
    return os.path.join(os.environ.get("DATA_DIR", "/data"), "x5g_vless_pq.env")


def pq_info() -> dict:
    """اطلاعات عمومی VLESS Post-Quantum Encryption. رشته‌ی «encryption» همون
    چیزیه که کلاینت لازم داره (شبیه public key) — رشته‌ی «decryption» هرگز از
    اینجا برنمی‌گرده چون فقط سمت سرور/Xray معتبره و نباید به بیرون درز کنه."""
    path = _pq_env_file()
    info = {"configured": False, "xray_version_note": "نیازمند Xray-core v26.x یا بالاتر (دستور «xray vlessenc»)"}
    try:
        if os.path.exists(path):
            kv = {}
            with open(path) as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        kv[k] = v
            enc = kv.get("VLESS_PQ_ENCRYPTION", "")
            dec_is_none = kv.get("VLESS_PQ_DECRYPTION", "none") == "none"
            info["configured"] = bool(enc) and not dec_is_none
            info["encryption_param"] = enc
    except Exception as e:
        logger.warning(f"[xray_bridge] pq_info خواندنی نبود: {e}")
    return info


def pq_client_link(uuid: str, server_ip_or_domain: str, port: int, remark: str = "X5G-PQ") -> str | None:
    """لینک vless:// برای inbound جدید VLESS Post-Quantum Encryption. برخلاف
    REALITY، اینجا security=none است چون رمزنگاری داخل خودِ VLESS انجام می‌شه —
    نه یک لایه‌ی TLS بیرونی. نیازمند کلاینتی که از encryption غیر از none
    پشتیبانی کنه (نسخه‌های خیلی جدید Xray-core/v2rayN/v2rayNG)."""
    info = pq_info()
    if not info.get("configured"):
        return None
    from urllib.parse import quote
    q = f"security=none&type=tcp&encryption={quote(info['encryption_param'], safe='')}"
    return f"vless://{uuid}@{server_ip_or_domain}:{port}?{q}#{quote(remark)}"


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
        "reality": reality_info(),
        "post_quantum": pq_info(),
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
        # نکته: «packetEncoding: xudp» (برای UDP به‌صورت full-cone — کیفیت
        # گیمینگ/صدا بهتر) یک تنظیم سمت outbound/کلاینت است، نه سمت Account
        # سرور؛ اضافه‌کردنش اینجا به AddUserOperation فقط باعث خطای grpcurl
        # می‌شه. کلاینت‌هایی مثل v2rayN/NekoBox خودشون این گزینه رو در تنظیمات
        # outbound دارن — نیازی به تغییر سمت سرور نیست.
        account = {"@type": "xray.proxy.vless.Account", "id": uuid, "flow": "xtls-rprx-vision" if tag == REALITY_TAG else ""}
        req = {
            "tag": tag,
            "operation": {
                "@type": "xray.app.proxyman.command.AddUserOperation",
                "user": {"email": email, "account": account},
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
