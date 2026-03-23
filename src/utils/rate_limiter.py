import asyncio
import time


class AsyncRateLimiter:
    def __init__(self, rate_per_sec: float):
        self._interval = 1.0 / max(rate_per_sec, 1e-9)
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait_time = max(0.0, self._next_allowed - now)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                now = time.monotonic()
            self._next_allowed = now + self._interval
