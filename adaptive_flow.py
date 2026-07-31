# adaptive_flow.py
# ══════════════════════════════════════════════════════════════════════════════
# X5G.1 — موتور تطبیقی مشترک (Adaptive Flow Engine)
#
# قبلاً این منطق (_QuotaGate و _AdaptiveFlow) فقط داخل xhttp_siz10.py برای
# stream-up وجود داشت. توی X5G.1 این‌ها رو به یک ماژول مستقل منتقل کردیم تا
# relay_vless.py (یعنی مسیر اصلی VLESS-WS، پرترافیک‌ترین بخش پروژه) هم از
# همون هوش adaptive بهره ببره — به‌جای چک‌کردن کوتا به‌ازای هر چانک (سربار
# lock/await زیاد) و یک high-water ثابت برای backpressure.
#
# دو کلاس:
#   • AdaptiveQuotaGate  — batch چک‌کردن کوتا رو بر اساس نرخ واقعی هر سشن
#     (EWMA) زنده تنظیم می‌کنه: سشن پرسرعت → batch بزرگ (await کمتر)،
#     سشن کم‌ترافیک → batch کوچیک (کوتا دقیق‌تر، قطع سریع‌تر).
#   • AdaptiveFlowControl — high-water تطبیقی برای backpressure، دقیقاً
#     مثل AIMD در TCP congestion control: drain سریع → سقف رو زیاد کن،
#     drain کند (backpressure واقعی) → سقف رو فوری نصف کن.
#
# منطق عیناً از نسخه‌ی اثبات‌شده‌ی xhttp_siz10 گرفته شده؛ چیزی در رفتار
# stream-up تغییر نکرده، فقط قابل استفاده‌ی مجدد شده.
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import time
from typing import Awaitable, Callable

# ── تنظیمات پیش‌فرض (برای هر دو مصرف‌کننده یکسان؛ در صورت نیاز per-call قابل override) ──
QUOTA_MIN_BATCH = 32 * 1024
QUOTA_MAX_BATCH = 1 * 1024 * 1024
QUOTA_START_BATCH = 64 * 1024
QUOTA_CHECK_INTERVAL = 0.2

FLOW_MIN_HW = 256 * 1024
FLOW_MAX_HW = 16 * 1024 * 1024
FLOW_START_HW = 2 * 1024 * 1024
FLOW_FAST_DRAIN_MS = 2.0
FLOW_SLOW_DRAIN_MS = 25.0


class AdaptiveQuotaGate:
    """چک کوتای adaptive و مشترک بین تمام ترابردها (WS، XHTTP، و هر چیز آینده).

    check_fn: تابعی که (uuid, nbytes) می‌گیره و await می‌شه، خروجی bool
    (همون check_and_use از relay_vless.py — قابل تزریق تا وابستگی چرخه‌ای
    بین ماژول‌ها پیش نیاد).
    """
    __slots__ = ("uuid", "check_fn", "pending", "last_check", "ok", "batch_bytes", "rate_ewma")

    def __init__(self, uuid: str, check_fn: Callable[[str, int], Awaitable[bool]]):
        self.uuid = uuid
        self.check_fn = check_fn
        self.pending = 0
        self.last_check = time.monotonic()
        self.ok = True
        self.batch_bytes = QUOTA_START_BATCH
        self.rate_ewma = 0.0

    async def add(self, nbytes: int) -> bool:
        if not self.ok:
            return False
        self.pending += nbytes
        now = time.monotonic()
        elapsed = now - self.last_check
        if self.pending >= self.batch_bytes or elapsed >= QUOTA_CHECK_INTERVAL:
            flush, self.pending = self.pending, 0
            if elapsed > 0:
                inst_rate = flush / elapsed
                self.rate_ewma = inst_rate if self.rate_ewma == 0 else (0.7 * self.rate_ewma + 0.3 * inst_rate)
                target = int(self.rate_ewma * QUOTA_CHECK_INTERVAL)
                self.batch_bytes = max(QUOTA_MIN_BATCH, min(QUOTA_MAX_BATCH, target or QUOTA_MIN_BATCH))
            self.last_check = now
            self.ok = await self.check_fn(self.uuid, flush)
            return self.ok
        return True

    async def flush(self) -> bool:
        if self.pending:
            flush, self.pending = self.pending, 0
            self.ok = self.ok and await self.check_fn(self.uuid, flush)
        return self.ok


class AdaptiveFlowControl:
    """high-water تطبیقی برای backpressure روی asyncio.StreamWriter (AIMD)."""
    __slots__ = ("high_water", "last_drain_ms")

    def __init__(self):
        self.high_water = FLOW_START_HW
        self.last_drain_ms = 0.0

    def should_drain(self, buf_size: int) -> bool:
        return buf_size > self.high_water

    async def drain(self, writer: asyncio.StreamWriter):
        t0 = time.monotonic()
        await writer.drain()
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.last_drain_ms = elapsed_ms
        if elapsed_ms < FLOW_FAST_DRAIN_MS:
            self.high_water = min(FLOW_MAX_HW, int(self.high_water * 1.5) + 65536)
        elif elapsed_ms > FLOW_SLOW_DRAIN_MS:
            self.high_water = max(FLOW_MIN_HW, self.high_water // 2)
