# speed_limit.py
# محدودیت سرعت (Bandwidth Throttling) به‌ازای هر کانفیگ — پیاده‌سازی با الگوی Token Bucket
# جدا شده از relay_vless.py و xhttp_siz10.py؛ هر دو این ماژول رو صدا می‌زنن (منطق اونا دست‌نخورده).
#
# X5.2-LevelUp: محدودیت سرعت حالا می‌تونه جهت‌دار باشه — آپلود (کلاینت → مقصد) و
# دانلود (مقصد → کلاینت) هر کدوم Bucket مستقل خودشون رو دارن. اگه upload_limit_bytes
# یا download_limit_bytes صفر باشه ولی speed_limit_bytes قدیمی مقدار داشته باشه،
# همون مقدار قدیمی به‌عنوان fallback متقارن هر دو جهت استفاده می‌شه (سازگاری کامل
# با کانفیگ‌های قدیمی‌تر، بدون نیاز به مهاجرت داده).

import asyncio
import time

from main import LINKS

# هر (uuid, direction) یک Bucket جدا داره؛ Bucket با نرخ صفر (بدون محدودیت) اصلاً ساخته نمی‌شه.
_buckets: dict = {}

MIN_RATE = 1024          # حداقل نرخ برای جلوگیری از تقسیم بر صفر یا سرعت‌های غیرمنطقی (1 KB/s)
MIN_BURST = 16 * 1024    # حداقل ظرفیت بافر burst (برای اینکه چانک‌های کوچیک بی‌دلیل صف نکشن)


class _Bucket:
    __slots__ = ("rate", "capacity", "tokens", "last")

    def __init__(self, rate_bytes_per_sec: float):
        self.rate = max(rate_bytes_per_sec, MIN_RATE)
        # ظرفیت burst: معادل ۱ ثانیه از نرخ مجاز (حداقل ۱۶ کیلوبایت) تا چانک‌های نرمال گیر نکنن
        self.capacity = max(self.rate, MIN_BURST)
        self.tokens = self.capacity
        self.last = time.monotonic()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last
        if elapsed > 0:
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

    async def consume(self, n: int):
        """تا وقتی n بایت توکن آماده نشه، به‌صورت غیرمسدودکننده (async sleep) صبر می‌کنه."""
        while True:
            self._refill()
            if self.tokens >= n:
                self.tokens -= n
                return
            deficit = n - self.tokens
            wait = deficit / self.rate
            # سقف sleep کوتاهه تا اگه نرخ کانفیگ از پنل تغییر کرد، زود متوجه بشیم
            await asyncio.sleep(min(max(wait, 0.004), 0.5))


def _bucket_key(uuid: str, direction: str) -> str:
    return f"{uuid}:{direction}"


def _get_bucket(uuid: str, direction: str, rate: int) -> _Bucket:
    key = _bucket_key(uuid, direction)
    b = _buckets.get(key)
    if b is None or b.rate != max(rate, MIN_RATE):
        b = _Bucket(rate)
        _buckets[key] = b
    return b


def _effective_rate(link: dict, field: str) -> int:
    """نرخ مؤثر یک جهت رو برمی‌گردونه: مقدار جهت‌دار جدید اگه ست شده باشه، وگرنه
    fallback به speed_limit_bytes قدیمی (برای سازگاری با کانفیگ‌های قبلی)."""
    v = int((link or {}).get(field, 0) or 0)
    if v > 0:
        return v
    return int((link or {}).get("speed_limit_bytes", 0) or 0)


async def _throttle_dir(uuid: str, n: int, direction: str, field: str):
    if n <= 0:
        return
    link = LINKS.get(uuid)
    rate = _effective_rate(link, field)
    if rate <= 0:
        return
    bucket = _get_bucket(uuid, direction, rate)
    await bucket.consume(n)


async def throttle_upload(uuid: str, nbytes: int):
    """محدودیت سرعت آپلود (کلاینت → مقصد/اینترنت). از upload_limit_bytes استفاده
    می‌کنه، وگرنه از speed_limit_bytes قدیمی به‌صورت متقارن."""
    await _throttle_dir(uuid, nbytes, "up", "upload_limit_bytes")


async def throttle_download(uuid: str, nbytes: int):
    """محدودیت سرعت دانلود (مقصد/اینترنت → کلاینت). از download_limit_bytes استفاده
    می‌کنه، وگرنه از speed_limit_bytes قدیمی به‌صورت متقارن."""
    await _throttle_dir(uuid, nbytes, "down", "download_limit_bytes")


async def throttle(uuid: str, nbytes: int):
    """[Deprecated] نگه‌داشته‌شده برای سازگاری با کد قدیمی/ماژول‌های شخص ثالث —
    معادل throttle_upload (رفتار نسخه‌های قبل از X5.2)."""
    await throttle_upload(uuid, nbytes)


def reset_bucket(uuid: str):
    """وقتی محدودیت سرعت یک کانفیگ از پنل تغییر کرد یا کانفیگ حذف شد صدا زده می‌شه،
    تا بافر توکن قدیمی (هر دو جهت) پاک بشه."""
    _buckets.pop(_bucket_key(uuid, "up"), None)
    _buckets.pop(_bucket_key(uuid, "down"), None)
