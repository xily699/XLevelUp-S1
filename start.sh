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
  # UUID پیش‌فرض API/آزمایشی از env — اگه ست نشده بود، یک UUID موقت می‌سازیم
  # (کاربرهای واقعی بعداً توسط پنل پایتون از طریق xray_bridge.py اضافه می‌شن)
  : "${XRAY_API_PORT:=10085}"
  export XRAY_API_PORT
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
exec python3 main.py
