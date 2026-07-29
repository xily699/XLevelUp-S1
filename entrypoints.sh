#!/bin/sh
# =====================================================================
#  entrypoint.sh — تنظیمات پویا و پایدار Xray
#  جایگزینی متغیرهای محیطی در config.json
# =====================================================================

set -e

# ===== مسیر فایل‌ها =====
TEMPLATE="/etc/xray/config.template.json"
CONFIG="/etc/xray/config.json"

# ===== متغیرهای پیش‌فرض =====
UUID="${UUID:-133ec15a-6e65-4564-8045-63213c3e7c70}"
DOMAIN="${DOMAIN:-your-domain.railway.app}"
REALITY_PUBLIC_KEY="${REALITY_PUBLIC_KEY:-2d11d8d0-6e65-4564-8045-63213c3e7c70}"
REALITY_PRIVATE_KEY="${REALITY_PRIVATE_KEY:-3e22e9e1-6e65-4564-8045-63213c3e7c70}"
REALITY_SHORT_ID="${REALITY_SHORT_ID:-6e657479}"

# ===== اعتبارسنجی UUID =====
if ! echo "$UUID" | grep -qE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
    echo "❌ ERROR: UUID is not valid. Generating a new one..."
    UUID=$(cat /proc/sys/kernel/random/uuid)
    echo "   New UUID: $UUID"
fi

# ===== جایگزینی متغیرها با استفاده از jq =====
echo "🔧 Updating config.json with environment variables..."

# استفاده از jq برای جایگزینی مقادیر در ساختار JSON
jq --arg uuid "$UUID" \
   --arg domain "$DOMAIN" \
   --arg pub "$REALITY_PUBLIC_KEY" \
   --arg priv "$REALITY_PRIVATE_KEY" \
   --arg sid "$REALITY_SHORT_ID" \
   '
   # جایگزینی UUID در همه inboundها
   .inbounds |= map(
     if .settings.clients then
       .settings.clients |= map(.id = $uuid)
     else . end
   ) |
   # جایگزینی Domain در TLS/Reality settings
   .inbounds |= map(
     if .streamSettings.tlsSettings then
       .streamSettings.tlsSettings.serverName = $domain
     else . end
   ) |
   .inbounds |= map(
     if .streamSettings.realitySettings then
       .streamSettings.realitySettings.serverNames = [$domain, "cloudflare.com", "www.cloudflare.com"] |
       .streamSettings.realitySettings.privateKey = $priv |
       .streamSettings.realitySettings.shortIds = [$sid, "6e657480", "6e657481"]
     else . end
   ) |
   .inbounds |= map(
     if .streamSettings.wsSettings then
       .streamSettings.wsSettings.headers.Host = $domain
     else . end
   ) |
   .inbounds |= map(
     if .streamSettings.xhttpSettings then
       .streamSettings.xhttpSettings.extra //= {} |
       .streamSettings.xhttpSettings.extra.download //= {"speed": 0, "conn": 0} |
       .streamSettings.xhttpSettings.extra.upload //= {"speed": 0, "conn": 0}
     else . end
   ) |
   .inbounds |= map(
     if .streamSettings.realitySettings then
       .streamSettings.realitySettings.publicKey = $pub
     else . end
   )
   ' "$TEMPLATE" > "$CONFIG"

# ===== تنظیمات نهایی امنیتی =====
chown xray:xray "$CONFIG"
chmod 600 "$CONFIG"

echo "✅ Configuration ready. Starting Xray..."

# ===== اجرای Xray =====
exec "$@"