from __future__ import annotations

import time

from forward_monitor.utils import RateLimiter


def test_rate_limiter_spacing() -> None:
    limiter = RateLimiter(5.0)

    async def runner() -> None:
        start = time.perf_counter()
        for _ in range(3):
            await limiter.wait()
        assert time.perf_counter() - start >= 0

        limiter.update_rate(1.0)
        start = time.perf_counter()
        for _ in range(2):
            await limiter.wait()
        assert time.perf_counter() - start >= 1.0

    import asyncio

    asyncio.run(runner())
