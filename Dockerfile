# ══════════════════════════════════════════════════════════════════════════════
# X5.3-LevelUp — Dockerfile
# پایتون (پنل/کنترل‌پلین فعلی، دست‌نخورده) + باینری رسمی Xray-core به‌عنوان یک
# process مستقل کنار همون کانتینر. هیچ‌کدوم از فایل‌های relay_vless.py /
# xhttp_siz10.py جایگزین نمی‌شن — دقیقاً همون‌جوری که هستن اجرا می‌مونن.
# ══════════════════════════════════════════════════════════════════════════════
FROM python:3.11-slim

# ── نصب Xray-core (باینری رسمی از GitHub Releases) ──────────────────────────
# نسخه‌ی v26.7.28: شامل «VLESS Post-Quantum Encryption» (ML-KEM-768 + X25519،
# دستور CLI «xray vlessenc») است — نسخه‌ی قبلی (v25.3.6) این قابلیت را نداشت.
ARG XRAY_VERSION=v26.7.28
ARG GRPCURL_VERSION=1.9.1
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip gettext-base ca-certificates \
    && ARCH=$(dpkg --print-architecture) \
    && if [ "$ARCH" = "amd64" ]; then XARCH="64"; GARCH="x86_64"; elif [ "$ARCH" = "arm64" ]; then XARCH="arm64-v8a"; GARCH="arm64"; else XARCH="64"; GARCH="x86_64"; fi \
    && curl -L -o /tmp/xray.zip "https://github.com/XTLS/Xray-core/releases/download/${XRAY_VERSION}/Xray-linux-${XARCH}.zip" \
    && mkdir -p /opt/xray \
    && unzip -o /tmp/xray.zip -d /opt/xray \
    && chmod +x /opt/xray/xray \
    && rm /tmp/xray.zip \
    && curl -L -o /tmp/grpcurl.tar.gz "https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_linux_${GARCH}.tar.gz" \
    && tar -xzf /tmp/grpcurl.tar.gz -C /usr/local/bin grpcurl \
    && chmod +x /usr/local/bin/grpcurl \
    && rm /tmp/grpcurl.tar.gz \
    && apt-get purge -y unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY xray/config.json.template /opt/xray/config.json.template
RUN chmod +x /app/start.sh

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["/app/start.sh"]
