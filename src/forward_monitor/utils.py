"""Miscellaneous helpers."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

try:  # pragma: no cover - zoneinfo availability depends on platform
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for environments without tzdata
    ZoneInfo = None  # type: ignore[misc,assignment]


class RateLimiter:
    """Simple rate limiter using sleep between events."""

    def __init__(self, rate_per_second: float):
        self.update_rate(rate_per_second)
        self._lock = asyncio.Lock()
        self._next_time = 0.0

    def update_rate(self, rate_per_second: float) -> None:
        self._interval = 0.0 if rate_per_second <= 0 else 1.0 / rate_per_second

    async def wait(self) -> None:
        async with self._lock:
            if self._interval <= 0:
                return
            now = time.perf_counter()
            if now < self._next_time:
                await asyncio.sleep(self._next_time - now)
            self._next_time = time.perf_counter() + self._interval


class ChannelProcessingGuard:
    """Coordinate access to channel-specific operations across coroutines."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._registry_lock = asyncio.Lock()

    @asynccontextmanager
    async def lock(self, channel_id: str) -> AsyncIterator[None]:
        async with self._registry_lock:
            lock = self._locks.get(channel_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[channel_id] = lock
        async with lock:
            yield


def parse_delay_setting(value: str | None, default: float = 0.0) -> float:
    if value is None:
        return default
    stripped = value.strip()
    if not stripped:
        return default
    try:
        if any(symbol in stripped for symbol in ".eE"):
            parsed = float(stripped)
        else:
            parsed = float(int(stripped) / 1000)
    except ValueError:
        return default
    return max(0.0, parsed)


def parse_bool(value: str | None, default: bool = False) -> bool:
    """Parse textual boolean configuration values.

    Supported truthy values: ``on``, ``true``, ``yes``, ``1`` (case-insensitive).
    Supported falsy values: ``off``, ``false``, ``no``, ``0``.
    Any other value returns ``default``.
    """

    if value is None:
        return default
    normalized = value.strip().lower()
    if not normalized:
        return default
    if normalized in {"on", "true", "yes", "1"}:
        return True
    if normalized in {"off", "false", "no", "0"}:
        return False
    return default


def normalize_username(username: str | None) -> str | None:
    """Return a lowercase username without @ prefix."""

    if username is None:
        return None
    normalized = username.strip()
    if normalized.startswith("@"):
        normalized = normalized[1:]
    normalized = normalized.strip().lower()
    return normalized or None


if ZoneInfo is not None:  # pragma: no cover - executed when tzdata available
    MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")
else:  # pragma: no cover - fallback branch for limited platforms
    MOSCOW_TIMEZONE = timezone(timedelta(hours=3))


def as_moscow_time(moment: datetime) -> datetime:
    """Return ``moment`` converted to the Moscow timezone."""

    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(MOSCOW_TIMEZONE)
