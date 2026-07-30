#!/bin/sh
###############################################################################
# XLevelUp Entrypoint — اجرای هم‌زمان Xray-core و Panel API با خاموشی تمیز
###############################################################################
set -e

echo "[entrypoint] شروع XLevelUp Engine Container..."

xray run -config /app/xray-config.json &
XRAY_PID=$!
echo "[entrypoint] Xray-core استارت شد (PID $XRAY_PID)"

node panel-api.js &
NODE_PID=$!
echo "[entrypoint] Panel API استارت شد (PID $NODE_PID)"

term_handler() {
    echo "[entrypoint] سیگنال خاموشی دریافت شد، در حال بستن تمیز پروسه‌ها..."
    kill -TERM "$XRAY_PID" 2>/dev/null || true
    kill -TERM "$NODE_PID" 2>/dev/null || true
    wait "$XRAY_PID" 2>/dev/null || true
    wait "$NODE_PID" 2>/dev/null || true
    exit 0
}
trap term_handler TERM INT

# مانیتور سبک: اگر یکی از دو پروسه از کار افتاد، کل کانتینر متوقف بشه
# (بهتره healthcheck/restart policy پلتفرم بلافاصله دوباره بالاش بیاره)
while true; do
    if ! kill -0 "$XRAY_PID" 2>/dev/null; then
        echo "[entrypoint] ⚠ Xray-core از کار افتاد. کانتینر متوقف می‌شه."
        kill -TERM "$NODE_PID" 2>/dev/null || true
        exit 1
    fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
        echo "[entrypoint] ⚠ Panel API از کار افتاد. کانتینر متوقف می‌شه."
        kill -TERM "$XRAY_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 3
done
