#!/bin/sh
# ══════════════════════════════════════════════════════════════════════════════
# X5.3-LevelUp — اسکریپت استارت دوگانه: Xray-core (پس‌زمینه) + پنل پایتون (اصلی)
#
# اصل طراحی: اگه Xray به هر دلیلی بالا نیاد یا کرش کنه، پنل پایتون (که همون
# اتصال‌های vless-ws / xhttp فعلی رو مستقل مدیریت می‌کنه) باید همچنان کار کنه —
# یعنی خرابی Xray هرگز نباید کل کانتینر رو بخوابونه. برای همین Xray در پس‌زمینه
# اجرا می‌شه و اگه crash کنه، فقط لاگ می‌کنیم و دوباره تلاش می‌کنیم؛ uvicorn
# (فرآیند اصلی/foreground) هیچ‌وقت به این وابسته نیست.
# ══════════════════════════════════════════════════════════════════════════════
set -eu

XRAY_BIN="/opt/xray/xray"
XRAY_CONFIG="/opt/xray/config.json"
XRAY_TEMPLATE="/opt/xray/config.json.template"

start_xray() {
  if [ "${XRAY_ENABLED:-false}" != "true" ]; then
    echo "[start.sh] XRAY_ENABLED != true — Xray-core اجرا نمی‌شه (فقط پنل پایتون بالا میاد)"
    return
  fi
  : "${XRAY_API_PORT:=10085}"
  export XRAY_API_PORT

  # ── REALITY: کلید x25519 باید بین ری‌استارت‌ها ثابت بمونه، وگرنه هر لینکی که
  # قبلاً به کاربرها دادی از کار می‌افته. برای همین یه‌بار می‌سازیم و رو ولیوم
  # پایدار (همون DATA_DIR که پنل پایتون هم استفاده می‌کنه) نگه می‌داریم.
  REALITY_ENV_FILE="${DATA_DIR:-/data}/x5g_reality.env"
  if [ ! -f "$REALITY_ENV_FILE" ]; then
    echo "[start.sh] کلید REALITY پیدا نشد — یک‌بار می‌سازیم و ذخیره می‌کنیم..."
    mkdir -p "$(dirname "$REALITY_ENV_FILE")"
    KEYPAIR_OUT="$("$XRAY_BIN" x25519 2>/dev/null || true)"
    R_PRIV=$(echo "$KEYPAIR_OUT" | grep -i "Private" | sed -E 's/.*: *//')
    R_PUB=$(echo "$KEYPAIR_OUT" | grep -iE "Public|Password" | sed -E 's/.*: *//')
    R_SID=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
    {
      echo "REALITY_PRIVATE_KEY=$R_PRIV"
      echo "REALITY_PUBLIC_KEY=$R_PUB"
      echo "REALITY_SHORT_ID=$R_SID"
      echo "REALITY_DEST=${REALITY_DEST:-www.cloudflare.com:443}"
      echo "REALITY_SERVER_NAME=${REALITY_SERVER_NAME:-www.cloudflare.com}"
    } > "$REALITY_ENV_FILE"
  fi
  # shellcheck disable=SC1090
  . "$REALITY_ENV_FILE"
  export REALITY_PRIVATE_KEY REALITY_PUBLIC_KEY REALITY_SHORT_ID REALITY_DEST REALITY_SERVER_NAME

  echo "[start.sh] در حال ساخت کانفیگ Xray از روی تمپلیت..."
  envsubst < "$XRAY_TEMPLATE" > "$XRAY_CONFIG" 2>/dev/null || cp "$XRAY_TEMPLATE" "$XRAY_CONFIG"

  ( while true; do
      echo "[start.sh] در حال اجرای Xray-core..."
      "$XRAY_BIN" run -c "$XRAY_CONFIG" || echo "[start.sh] Xray-core متوقف/کرش کرد — ۵ ثانیه دیگه دوباره تلاش می‌شه"
      sleep 5
    done ) &
}

start_xray
echo "[start.sh] در حال اجرای پنل پایتون (uvicorn)..."
# نکته‌ی حیاتی: عمداً «python3 main.py» اجرا نمی‌کنیم. وقتی main.py مستقیم اجرا
# بشه، پایتون اسمش رو __main__ می‌ذاره، نه main — و پایین همون فایل خط
# `uvicorn.run("main:app", ...)` هست که به یوویکورن می‌گه ماژول «main» رو (که
# هنوز تو sys.modules نیست، چون چیزی که الان داره اجرا می‌شه اسمش __main__ـه نه
# main) از اول ایمپورت کن. یعنی main.py یه‌بار دیگه، از صفر، این‌بار با اسم
# «main» اجرا می‌شه — و درست همون لحظه‌ای که به خط `from relay_vless import`
# می‌رسه (خط ۱۰۳۲)، relay_vless.py هم وسط اجرای اولش (که با پایتون اجرای اول
# شروع شده بود) گیر کرده رو `from main import (...)` — یعنی دو تا اجرای موازی و
# ناقص از main.py که منتظر همدیگه‌ان: دقیقاً همون ImportError با متن
# «cannot import name 'RELAY_BUF' from partially initialized module
# 'relay_vless'». راه‌حل: از همون اول با -m uvicorn اجرا کنیم تا ماژول با اسم
# درست («main») فقط یک‌بار لود بشه، نه دوبار زیر دو اسم متفاوت.
exec python3 -m uvicorn main:app \
  --host 0.0.0.0 --port "${PORT:-8000}" \
  --ws-ping-interval 20 --ws-ping-timeout 25 \
  --timeout-keep-alive 75
