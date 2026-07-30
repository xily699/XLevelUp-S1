#!/bin/sh
###############################################################################
# XLevelUp Entrypoint — نسخه پیشرفته مخصوص Railway
###############################################################################
set -e

CERT_DIR="${CERT_DIR:-/etc/xray}"
MAX_XRAY_RETRIES=5

if [ -z "$XLEVELUP_DOMAIN" ] && [ -n "$RAILWAY_PUBLIC_DOMAIN" ]; then
    export XLEVELUP_DOMAIN="$RAILWAY_PUBLIC_DOMAIN"
    echo "[entrypoint] دامنه از RAILWAY_PUBLIC_DOMAIN تشخیص داده شد: $XLEVELUP_DOMAIN"
fi

echo "============================================================"
echo " XLevelUp Engine — Startup Banner"
echo "  Xray version : $(cat /xray-version.txt 2>/dev/null || echo 'نامشخص')"
echo "  Domain       : ${XLEVELUP_DOMAIN:-تنظیم نشده}"
echo "  Port (HTTP)  : ${PORT:-8080}"
echo "  Environment  : ${RAILWAY_ENVIRONMENT:-local}"
echo "  Git SHA      : ${RAILWAY_GIT_COMMIT_SHA:-نامشخص}"
echo "  Cert dir     : $CERT_DIR"
echo "============================================================"

mkdir -p "$CERT_DIR"
if [ ! -f "$CERT_DIR/cert.pem" ] || [ ! -f "$CERT_DIR/key.pem" ]; then
    echo "[entrypoint] گواهی TLS پیدا نشد، در حال ساخت self-signed..."
    openssl req -x509 -nodes -newkey rsa:2048 \
        -keyout "$CERT_DIR/key.pem" -out "$CERT_DIR/cert.pem" \
        -days 3650 -subj "/CN=${XLEVELUP_DOMAIN:-xlevelup.local}" 2>/dev/null
    echo "[entrypoint] گواهی ساخته شد در: $CERT_DIR"
else
    echo "[entrypoint] گواهی موجود از قبل استفاده می‌شه (پایدار از Volume)."
fi

if [ "$CERT_DIR" != "/etc/xray" ]; then
    mkdir -p /etc/xray
    ln -sf "$CERT_DIR/cert.pem" /etc/xray/cert.pem
    ln -sf "$CERT_DIR/key.pem" /etc/xray/key.pem
fi

NODE_PID=""
XRAY_PID=""

term_handler() {
    echo "[entrypoint] سیگنال خاموشی دریافت شد، در حال بستن تمیز پروسه‌ها..."
    [ -n "$XRAY_PID" ] && kill -TERM "$XRAY_PID" 2>/dev/null || true
    [ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    exit 0
}
trap term_handler TERM INT

run_xray_with_backoff() {
    attempt=0
    delay=2
    while [ "$attempt" -lt "$MAX_XRAY_RETRIES" ]; do
        xray run -config /app/xray-config.json &
        XRAY_PID=$!
        echo "[entrypoint] Xray-core استارت شد (PID $XRAY_PID، تلاش $((attempt+1)))"
        wait "$XRAY_PID"
        EXIT_CODE=$?
        if [ "$EXIT_CODE" -eq 0 ]; then
            echo "[entrypoint] Xray-core تمیز خارج شد."
            return 0
        fi
        attempt=$((attempt+1))
        echo "[entrypoint] ⚠ Xray-core با خطا خارج شد (کد $EXIT_CODE). تلاش دوباره تا ${delay}s دیگر... ($attempt/$MAX_XRAY_RETRIES)"
        sleep "$delay"
        delay=$((delay*2))
    done
    echo "[entrypoint] ✘ Xray-core بعد از $MAX_XRAY_RETRIES تلاش هم بالا نیومد. کانتینر متوقف می‌شه."
    return 1
}

run_xray_with_backoff &
XRAY_SUPERVISOR_PID=$!

node panel-api.js &
NODE_PID=$!
echo "[entrypoint] Panel API استارت شد (PID $NODE_PID)"

while true; do
    if ! kill -0 "$XRAY_SUPERVISOR_PID" 2>/dev/null; then
        echo "[entrypoint] ⚠ زیرسیستم Xray کاملاً از کار افتاد. کانتینر متوقف می‌شه."
        kill -TERM "$NODE_PID" 2>/dev/null || true
        exit 1
    fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
        echo "[entrypoint] ⚠ Panel API از کار افتاد. کانتینر متوقف می‌شه."
        kill -TERM "$XRAY_SUPERVISOR_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 3
done