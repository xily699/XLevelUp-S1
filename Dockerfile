# =====================================================================
#  Xray Ultra — Dockerfile پیشرفته برای اجرای Xray-core
#  با پشتیبانی از متغیرهای محیطی، Health Check، و پایداری بالا
# =====================================================================

# ===== مرحله ۱: استفاده از تصویر رسمی Xray-core =====
FROM teddysun/xray:latest AS builder

# ===== مرحله ۲: نصب ابزارهای کمکی برای پردازش JSON و متغیرها =====
RUN apk add --no-cache jq curl bash gettext

# ===== مرحله ۳: کپی فایل‌های پروژه =====
COPY config.json /etc/xray/config.template.json
COPY entrypoint.sh /entrypoint.sh

# ===== مرحله ۴: ایجاد کاربر غیر‌root برای امنیت =====
RUN adduser -D -h /var/lib/xray xray && \
    chown -R xray:xray /etc/xray /var/log/xray /var/lib/xray && \
    chmod +x /entrypoint.sh

# ===== مرحله ۵: Health Check =====
# Xray API روی پورت ۱۰۰۸۵ در دسترس است
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:10085/stats || exit 1

# ===== مرحله ۶: تنظیمات نهایی =====
USER xray
EXPOSE 443 444 445 446 10085

# ===== مرحله ۷: اجرا با اسکریپت ورودی =====
ENTRYPOINT ["/entrypoint.sh"]
CMD ["xray", "-config", "/etc/xray/config.json"]