"""Application bootstrap for Forward Monitor."""

from __future__ import annotations

import asyncio
import html
import logging
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TypeVar

import aiohttp

from .config_store import ConfigStore
from .deduplication import MessageDeduplicator, build_message_signature
from .discord import DiscordClient
from .filters import FilterEngine
from .formatting import format_discord_message
from .models import ChannelConfig, DiscordMessage, NetworkOptions, RuntimeOptions
from .telegram import TelegramAPI, TelegramController, send_formatted
from .utils import ChannelProcessingGuard, RateLimiter, parse_bool, parse_delay_setting


def _parse_discord_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None


def _discord_snowflake_from_datetime(moment: datetime | None) -> int | None:
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    epoch = datetime(2015, 1, 1, tzinfo=timezone.utc)
    delta = moment - epoch
    milliseconds = int(delta.total_seconds() * 1000)
    if milliseconds < 0:
        return 0
    return milliseconds << 22

logger = logging.getLogger(__name__)

_FORWARDABLE_MESSAGE_TYPES: set[int] = {0, 19, 20, 21, 23}


_T = TypeVar("_T")
_HEALTHCHECK_RETRY_ATTEMPTS = 3
_HEALTHCHECK_RETRY_DELAY = 1.0


async def _retry_async(
    factory: Callable[[], Awaitable[_T]],
    *,
    attempts: int = _HEALTHCHECK_RETRY_ATTEMPTS,
    delay: float = _HEALTHCHECK_RETRY_DELAY,
    predicate: Callable[[_T], bool] | None = None,
) -> _T:
    if attempts <= 1:
        return await factory()

    result = await factory()
    check = predicate or (lambda value: bool(value))

    for _ in range(1, attempts):
        if check(result):
            return result
        await asyncio.sleep(delay)
        result = await factory()
    return result


@dataclass(slots=True)
class MonitorState:
    channels: list[ChannelConfig]
    runtime: RuntimeOptions
    network: NetworkOptions
    discord_token: str | None
    discord_token_ok: bool


@dataclass(slots=True)
class HealthUpdate:
    """Single subject health result produced by the checker."""

    key: str
    status: str
    message: str | None
    label: str


class ForwardMonitorApp:
    """High level coordinator tying together Discord, Telegram and configuration."""

    def __init__(self, *, db_path: Path, telegram_token: str):
        self._store = ConfigStore(db_path)
        self._telegram_token = telegram_token
        self._refresh_event = asyncio.Event()
        self._health_wakeup = asyncio.Event()
        self._health_status: dict[str, str] = self._load_initial_health_statuses()
        self._health_ready = asyncio.Event()
        self._config_version = 0
        self._health_version = -1
        self._startup_time = datetime.now(timezone.utc)
        self._channel_guard = ChannelProcessingGuard()
        self._deduplicator = MessageDeduplicator()
        self._mark_config_dirty()
        self._refresh_event.set()

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            discord_client = DiscordClient(session)
            telegram_api = TelegramAPI(self._telegram_token, session)
            controller = TelegramController(
                telegram_api,
                self._store,
                discord_client=discord_client,
                on_change=self._signal_refresh,
                channel_guard=self._channel_guard,
                deduplicator=self._deduplicator,
            )

            async def run_monitor() -> None:
                await self._monitor_loop(discord_client, telegram_api)

            async def run_controller() -> None:
                await controller.run()

            monitor_task = asyncio.create_task(
                self._supervise("forward-monitor", run_monitor),
                name="forward-monitor-supervisor",
            )
            bot_task = asyncio.create_task(
                self._supervise("telegram-controller", run_controller),
                name="telegram-controller-supervisor",
            )
            health_task = asyncio.create_task(
                self._supervise(
                    "health-monitor",
                    lambda: self._healthcheck_loop(discord_client, telegram_api),
                ),
                name="health-monitor-supervisor",
            )

            await asyncio.gather(monitor_task, bot_task, health_task)

    def _signal_refresh(self) -> None:
        self._mark_config_dirty()
        self._refresh_event.set()
        self._health_wakeup.set()

    def _mark_config_dirty(self) -> None:
        self._config_version += 1
        self._health_ready.clear()

    async def _monitor_loop(
        self,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
    ) -> None:
        runtime = self._load_runtime()
        discord_rate = RateLimiter(runtime.rate_per_second)
        telegram_rate = RateLimiter(runtime.rate_per_second)
        token_value = self._store.get_setting("discord.token")
        token_status, _ = self._store.get_health_status("discord_token")
        state = MonitorState(
            channels=[],
            runtime=runtime,
            network=self._store.load_network_options(),
            discord_token=token_value,
            discord_token_ok=token_status == "ok",
        )
        state_version = 0

        while True:
            if self._refresh_event.is_set():
                while True:
                    self._refresh_event.clear()
                    target_version = self._config_version
                    await self._wait_for_health(target_version)
                    if self._refresh_event.is_set():
                        continue
                    state = self._reload_state()
                    state_version = target_version
                    discord_client.set_token(state.discord_token)
                    discord_client.set_network_options(state.network)
                    discord_rate.update_rate(state.runtime.rate_per_second)
                    telegram_rate.update_rate(state.runtime.rate_per_second)
                    logger.info(
                        "Конфигурация обновлена: %d каналов", len(state.channels)
                    )
                    break

            if not state.discord_token or not state.discord_token_ok:
                await asyncio.sleep(3.0)
                continue

            for channel in list(state.channels):
                if state_version < self._config_version:
                    break
                if self._refresh_event.is_set():
                    break
                await discord_rate.wait()
                try:
                    await self._process_channel(
                        channel,
                        discord_client,
                        telegram_api,
                        telegram_rate,
                        state.runtime,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Ошибка при обработке канала Discord %s", channel.discord_id
                    )
                    await asyncio.sleep(1.0)

            try:
                await asyncio.wait_for(
                    self._refresh_event.wait(),
                    timeout=state.runtime.poll_interval,
                )
            except asyncio.TimeoutError:
                pass

    async def _healthcheck_loop(
        self,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
        *,
        interval_override: float | None = None,
    ) -> None:
        first_iteration = True
        interval = interval_override if interval_override is not None else 180.0
        while True:
            if first_iteration:
                first_iteration = False
                if self._health_wakeup.is_set():
                    self._health_wakeup.clear()
            else:
                try:
                    await asyncio.wait_for(self._health_wakeup.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
                else:
                    self._health_wakeup.clear()

            target_version = self._config_version
            state = self._reload_state()
            await self._run_health_checks(state, discord_client, telegram_api)
            self._health_version = target_version
            self._health_ready.set()

            if interval_override is None:
                interval = max(10.0, state.runtime.healthcheck_interval)

    async def _supervise(
        self,
        name: str,
        factory: Callable[[], Awaitable[None]],
        *,
        retry_delay: float = 5.0,
    ) -> None:
        while True:
            try:
                await factory()
            except asyncio.CancelledError:
                logger.info("Задача %s остановлена", name)
                raise
            except Exception:
                logger.exception("Задача %s завершилась с ошибкой", name)
            else:
                logger.warning("Задача %s завершилась неожиданно, будет перезапущена", name)
            await asyncio.sleep(retry_delay)

    async def _run_health_checks(
        self,
        state: MonitorState,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
    ) -> None:
        updates: list[HealthUpdate] = []

        discord_client.set_token(state.discord_token)
        discord_client.set_network_options(state.network)

        proxy_result = await _retry_async(
            lambda: discord_client.check_proxy(state.network),
            predicate=lambda result: result.ok,
        )
        proxy_configured = bool(state.network.discord_proxy_url)
        if proxy_configured:
            proxy_status = "ok" if proxy_result.ok else "error"
            proxy_message = proxy_result.error
        else:
            proxy_status = "disabled"
            proxy_message = None
        updates.append(
            HealthUpdate(
                key="proxy",
                status=proxy_status,
                message=proxy_message,
                label="Прокси Discord",
            )
        )

        proxy_blocked = proxy_configured and not proxy_result.ok
        token_value = (state.discord_token or "").strip()
        token_status = "unknown"
        token_message: str | None = None
        token_ok = False

        if proxy_blocked:
            token_status = "unknown"
            token_message = "Проверка недоступна: прокси не отвечает."
        elif not token_value:
            token_status = "error"
            token_message = "Токен Discord не задан."
        else:
            token_result = await _retry_async(
                lambda: discord_client.verify_token(
                    token_value, network=state.network
                ),
                predicate=lambda result: result.ok,
            )
            token_ok = token_result.ok
            token_status = "ok" if token_result.ok else "error"
            token_message = token_result.error
            if token_result.ok and token_result.normalized_token:
                normalized = token_result.normalized_token
                if normalized != token_value:
                    self._store.set_setting("discord.token", normalized)
                    token_value = normalized
                    state.discord_token = normalized
                    discord_client.set_token(normalized)
                    self._signal_refresh()
        updates.append(
            HealthUpdate(
                key="discord_token",
                status=token_status,
                message=token_message,
                label="Discord токен",
            )
        )

        channel_ids: set[str] = {channel.discord_id for channel in state.channels}
        check_rate = RateLimiter(max(1.0, state.runtime.rate_per_second))
        for channel in state.channels:
            key = f"channel.{channel.discord_id}"
            label = f"Канал {channel.label or channel.discord_id}"
            if not channel.active:
                updates.append(
                    HealthUpdate(key=key, status="disabled", message=None, label=label)
                )
                continue
            if proxy_blocked:
                updates.append(
                    HealthUpdate(
                        key=key,
                        status="unknown",
                        message="Проверка недоступна: прокси не отвечает.",
                        label=label,
                    )
                )
                continue
            if not token_ok:
                updates.append(
                    HealthUpdate(
                        key=key,
                        status="unknown",
                        message="Проверка недоступна: нет валидного токена Discord.",
                        label=label,
                    )
                )
                continue
            async def _check_channel() -> bool:
                await check_rate.wait()
                return await discord_client.check_channel_exists(channel.discord_id)

            exists = await _retry_async(_check_channel, predicate=lambda value: bool(value))
            if exists:
                updates.append(HealthUpdate(key=key, status="ok", message=None, label=label))
            else:
                updates.append(
                    HealthUpdate(
                        key=key,
                        status="error",
                        message="Discord канал недоступен или нет прав.",
                        label=label,
                    )
                )

        self._store.clean_channel_health_statuses(channel_ids)
        for update in updates:
            self._store.set_health_status(update.key, update.status, update.message)

        current_keys = {update.key for update in updates}
        for key in list(self._health_status.keys()):
            if key.startswith("channel.") and key not in current_keys:
                self._health_status.pop(key, None)

        await self._emit_health_notifications(updates, telegram_api)

    async def _emit_health_notifications(
        self, updates: list[HealthUpdate], telegram_api: TelegramAPI
    ) -> None:
        errors: list[HealthUpdate] = []
        recoveries: list[HealthUpdate] = []
        for update in updates:
            previous = self._health_status.get(update.key)
            if previous == update.status:
                continue
            self._health_status[update.key] = update.status
            if update.status == "error":
                logger.warning(
                    "Проблема со здоровьем компонента %s: %s",
                    update.key,
                    update.message,
                )
                errors.append(update)
            elif previous == "error" and update.status == "ok":
                recoveries.append(update)

        if errors:
            message = self._format_health_summary(errors, recovered=False)
            await self._notify_admins(telegram_api, message)
        if recoveries:
            message = self._format_health_summary(recoveries, recovered=True)
            await self._notify_admins(telegram_api, message)
        if errors or recoveries:
            self._refresh_event.set()

    async def _notify_admins(self, telegram_api: TelegramAPI, message: str) -> None:
        for admin in self._store.list_admins():
            if admin.user_id is None:
                continue
            try:
                await telegram_api.send_message(
                    admin.user_id,
                    message,
                    parse_mode="HTML",
                )
            except Exception:
                logger.exception(
                    "Не удалось отправить уведомление администратору %s", admin.user_id
                )

    def _format_health_summary(
        self, updates: Sequence[HealthUpdate], *, recovered: bool
    ) -> str:
        if not updates:
            return ""
        if recovered:
            header = "✅ <b>Компоненты восстановлены</b>"
            lines = [header, ""]
            for update in updates:
                lines.append(f"• {html.escape(update.label)}")
            return "\n".join(lines)

        header = "🔴 <b>Обнаружены проблемы</b>"
        lines = [header, ""]
        for update in updates:
            description = html.escape(update.message or "Причина не указана.")
            lines.append(f"• <b>{html.escape(update.label)}</b> — {description}")
        return "\n".join(lines)

    async def _process_channel(
        self,
        channel: ChannelConfig,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
        telegram_rate: RateLimiter,
        runtime: RuntimeOptions,
    ) -> None:
        guard = self._channel_guard
        if guard is None:
            await self._process_channel_inner(
                channel,
                discord_client,
                telegram_api,
                telegram_rate,
                runtime,
            )
            return
        async with guard.lock(channel.discord_id):
            await self._process_channel_inner(
                channel,
                discord_client,
                telegram_api,
                telegram_rate,
                runtime,
            )

    async def _process_channel_inner(
        self,
        channel: ChannelConfig,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
        telegram_rate: RateLimiter,
        runtime: RuntimeOptions,
    ) -> None:
        if not channel.active or channel.blocked_by_health:
            return

        if channel.pinned_only:
            await self._process_pinned_channel_inner(
                channel,
                discord_client,
                telegram_api,
                telegram_rate,
                runtime,
            )
            return

        if channel.is_forum:
            await self._process_forum_channel_inner(
                channel,
                discord_client,
                telegram_api,
                telegram_rate,
                runtime,
            )
            return

        baseline = channel.added_at
        bootstrap = channel.last_message_id is None
        if baseline is not None and baseline.tzinfo is None:
            baseline = baseline.replace(tzinfo=timezone.utc)
        startup = self._startup_time
        startup_marker = _discord_snowflake_from_datetime(startup)
        if bootstrap:
            if baseline is None or baseline < startup:
                baseline = startup
        baseline_marker = _discord_snowflake_from_datetime(baseline)

        try:
            messages = await discord_client.fetch_messages(
                channel.discord_id,
                after=channel.last_message_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Ошибка при запросе сообщений Discord для канала %s",
                channel.discord_id,
            )
            await asyncio.sleep(1.0)
            return
        if not messages:
            return

        def sort_key(message_id: str) -> tuple[int, str]:
            return (int(message_id), message_id) if message_id.isdigit() else (0, message_id)

        unique_messages: dict[str, DiscordMessage] = {}
        for msg in messages:
            unique_messages.setdefault(msg.id, msg)

        ordered = sorted(unique_messages.values(), key=lambda msg: sort_key(msg.id))
        engine = FilterEngine(channel.filters)
        deduplicate_messages = (
            runtime.deduplicate_messages
            if channel.deduplicate_inherited
            else channel.deduplicate_messages
        )
        previous_last_message = channel.last_message_id
        last_seen = previous_last_message
        startup_ts = startup
        for msg in ordered:
            if self._refresh_event.is_set():
                break
            candidate_id = msg.id
            if startup_marker is not None and candidate_id.isdigit():
                candidate_numeric = int(candidate_id)
                if candidate_numeric <= startup_marker:
                    last_seen = candidate_id
                    continue
            message_timestamp = _parse_discord_timestamp(msg.timestamp)
            if message_timestamp is not None and message_timestamp.tzinfo is None:
                message_timestamp = message_timestamp.replace(tzinfo=timezone.utc)
            if message_timestamp is not None and message_timestamp <= startup_ts:
                last_seen = candidate_id
                continue
            if msg.message_type not in _FORWARDABLE_MESSAGE_TYPES and not (
                msg.attachments or msg.embeds
            ):
                last_seen = candidate_id
                continue
            if bootstrap:
                if baseline_marker is not None and candidate_id.isdigit():
                    marker = baseline_marker
                    candidate_numeric = int(candidate_id)
                    if candidate_numeric <= marker:
                        last_seen = candidate_id
                        continue
                if baseline is not None:
                    baseline_ts = baseline
                    if message_timestamp is not None and message_timestamp <= baseline_ts:
                        last_seen = candidate_id
                        continue
                bootstrap = False
            decision = engine.evaluate(msg)
            if not decision.allowed:
                last_seen = candidate_id
                continue
            signature: str | None = None
            if deduplicate_messages:
                signature = build_message_signature(msg)
                if self._deduplicator.is_duplicate(signature):
                    last_seen = candidate_id
                    continue
            formatted = format_discord_message(msg, channel, message_kind="message")
            await telegram_rate.wait()
            if self._refresh_event.is_set():
                break
            try:
                await send_formatted(
                    telegram_api,
                    channel.telegram_chat_id,
                    formatted,
                    thread_id=channel.telegram_thread_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Не удалось отправить сообщение %s в Telegram чат %s",
                    msg.id,
                    channel.telegram_chat_id,
                )
                last_seen = candidate_id
                continue
            last_seen = candidate_id
            await self._sleep_within(runtime)

        if last_seen and last_seen != previous_last_message:
            channel.last_message_id = last_seen
            if channel.storage_id is not None:
                self._store.set_last_message(channel.storage_id, last_seen)

    async def _process_pinned_channel(
        self,
        channel: ChannelConfig,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
        telegram_rate: RateLimiter,
        runtime: RuntimeOptions,
    ) -> None:
        guard = self._channel_guard
        if guard is None:
            await self._process_pinned_channel_inner(
                channel,
                discord_client,
                telegram_api,
                telegram_rate,
                runtime,
            )
            return
        async with guard.lock(channel.discord_id):
            await self._process_pinned_channel_inner(
                channel,
                discord_client,
                telegram_api,
                telegram_rate,
                runtime,
            )

    async def _process_pinned_channel_inner(
        self,
        channel: ChannelConfig,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
        telegram_rate: RateLimiter,
        runtime: RuntimeOptions,
    ) -> None:
        if not channel.active or channel.blocked_by_health:
            return

        baseline = channel.added_at
        if baseline is not None and baseline.tzinfo is None:
            baseline = baseline.replace(tzinfo=timezone.utc)
        startup = self._startup_time
        cutoff_ts = startup
        if baseline is not None and baseline > cutoff_ts:
            cutoff_ts = baseline
        cutoff_marker = _discord_snowflake_from_datetime(cutoff_ts)
        try:
            messages = await discord_client.fetch_pinned_messages(channel.discord_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Ошибка при запросе закреплённых сообщений Discord для канала %s",
                channel.discord_id,
            )
            await asyncio.sleep(1.0)
            return

        current_ids = {msg.id for msg in messages}
        previous_known = set(channel.known_pinned_ids)

        if not channel.pinned_synced:
            if channel.storage_id is not None:
                self._store.set_known_pinned_messages(channel.storage_id, current_ids)
                self._store.set_pinned_synced(channel.storage_id, synced=True)
                channel.known_pinned_ids = set(current_ids)
                channel.pinned_synced = True
            else:
                channel.pinned_synced = True
            return

        new_ids = current_ids - previous_known

        if not messages:
            if channel.storage_id is not None and channel.known_pinned_ids:
                self._store.set_known_pinned_messages(channel.storage_id, [])
                channel.known_pinned_ids = set()
            return

        if not new_ids and previous_known == current_ids:
            return

        def sort_key(message_id: str) -> tuple[int, str]:
            return (int(message_id), message_id) if message_id.isdigit() else (0, message_id)

        engine = FilterEngine(channel.filters)
        ordered = sorted(
            (msg for msg in messages if msg.id in new_ids),
            key=lambda msg: sort_key(msg.id),
        )
        deduplicate_messages = (
            runtime.deduplicate_messages
            if channel.deduplicate_inherited
            else channel.deduplicate_messages
        )
        processed_ids: set[str] = set()
        interrupted = False

        for msg in ordered:
            if self._refresh_event.is_set():
                interrupted = True
                break
            candidate_id = msg.id
            if cutoff_marker is not None and candidate_id.isdigit():
                candidate_numeric = int(candidate_id)
                if candidate_numeric <= cutoff_marker:
                    processed_ids.add(candidate_id)
                    continue
            message_timestamp = _parse_discord_timestamp(msg.timestamp)
            if message_timestamp is not None and message_timestamp.tzinfo is None:
                message_timestamp = message_timestamp.replace(tzinfo=timezone.utc)
            if message_timestamp is not None and message_timestamp <= cutoff_ts:
                processed_ids.add(candidate_id)
                continue
            if (
                msg.message_type not in _FORWARDABLE_MESSAGE_TYPES
                and not (msg.attachments or msg.embeds)
            ):
                processed_ids.add(candidate_id)
                continue
            decision = engine.evaluate(msg)
            if not decision.allowed:
                processed_ids.add(msg.id)
                continue
            signature: str | None = None
            if deduplicate_messages:
                signature = build_message_signature(msg)
                if self._deduplicator.is_duplicate(signature):
                    processed_ids.add(candidate_id)
                    continue
            formatted = format_discord_message(msg, channel, message_kind="pinned")
            await telegram_rate.wait()
            if self._refresh_event.is_set():
                interrupted = True
                break
            try:
                await send_formatted(
                    telegram_api,
                    channel.telegram_chat_id,
                    formatted,
                    thread_id=channel.telegram_thread_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Не удалось отправить закреплённое сообщение %s в Telegram чат %s",
                    msg.id,
                    channel.telegram_chat_id,
                )
                continue
            processed_ids.add(msg.id)
            await self._sleep_within(runtime)

        if channel.storage_id is None:
            return

        base_known = previous_known & current_ids
        if interrupted:
            updated_known = base_known | processed_ids
        else:
            updated_known = current_ids

        if updated_known != channel.known_pinned_ids:
            self._store.set_known_pinned_messages(channel.storage_id, updated_known)
            self._store.set_pinned_synced(channel.storage_id, synced=True)
            channel.known_pinned_ids = updated_known
            channel.pinned_synced = True

    async def _process_forum_channel_inner(
        self,
        channel: ChannelConfig,
        discord_client: DiscordClient,
        telegram_api: TelegramAPI,
        telegram_rate: RateLimiter,
        runtime: RuntimeOptions,
    ) -> None:
        """Monitor forum channel for new threads and forward their first messages."""
        if not channel.active or channel.blocked_by_health:
            return

        if not channel.guild_id:
            logger.warning(
                "Пропуск форума %s: не указан guild_id", channel.discord_id
            )
            return

        try:
            threads = await discord_client.fetch_forum_threads(
                channel.discord_id, channel.guild_id
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Ошибка при получении тредов форума %s",
                channel.discord_id,
            )
            await asyncio.sleep(1.0)
            return

        current_ids = {thread.id for thread in threads}

        # First sync - just remember current threads, don't forward
        if not channel.forum_synced:
            if channel.storage_id is not None:
                self._store.set_known_thread_ids(channel.storage_id, current_ids)
                self._store.set_forum_synced(channel.storage_id, synced=True)
                channel.known_thread_ids = set(current_ids)
                channel.forum_synced = True
            else:
                channel.forum_synced = True
            return

        new_ids = current_ids - channel.known_thread_ids
        if not new_ids:
            return

        # Sort threads by ID (chronological order for snowflakes)
        def sort_key(thread_id: str) -> tuple[int, str]:
            return (int(thread_id), thread_id) if thread_id.isdigit() else (0, thread_id)

        threads_by_id = {thread.id: thread for thread in threads}
        ordered_new_ids = sorted(new_ids, key=sort_key)

        processed_ids: set[str] = set()
        interrupted = False

        for thread_id in ordered_new_ids:
            if self._refresh_event.is_set():
                interrupted = True
                break

            thread = threads_by_id.get(thread_id)
            if not thread:
                processed_ids.add(thread_id)
                continue

            # Fetch the first message of the thread
            try:
                messages = await discord_client.fetch_messages(thread_id, limit=1)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Ошибка при получении первого сообщения треда %s",
                    thread_id,
                )
                continue

            if not messages:
                processed_ids.add(thread_id)
                continue

            # Get the first (oldest) message
            first_msg = min(messages, key=lambda m: sort_key(m.id))

            # Format message with thread title
            formatted = format_discord_message(
                first_msg, channel, message_kind="forum_thread", thread_name=thread.name
            )

            await telegram_rate.wait()
            if self._refresh_event.is_set():
                interrupted = True
                break

            try:
                await send_formatted(
                    telegram_api,
                    channel.telegram_chat_id,
                    formatted,
                    thread_id=channel.telegram_thread_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Не удалось отправить сообщение из треда %s в Telegram чат %s",
                    thread_id,
                    channel.telegram_chat_id,
                )
                continue

            processed_ids.add(thread_id)
            await self._sleep_within(runtime)

        # Update known threads
        if channel.storage_id is not None:
            if interrupted:
                updated_known = channel.known_thread_ids | processed_ids
            else:
                updated_known = current_ids

            if updated_known != channel.known_thread_ids:
                self._store.set_known_thread_ids(channel.storage_id, updated_known)
                channel.known_thread_ids = updated_known

    async def _sleep_within(self, runtime: RuntimeOptions) -> None:
        delay_seconds = 0.0
        if runtime.max_delay_seconds > 0:
            delay_seconds = random.uniform(
                runtime.min_delay_seconds, runtime.max_delay_seconds
            )
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    def _reload_state(self) -> MonitorState:
        runtime = self._load_runtime()
        network = self._store.load_network_options()
        channels = self._store.load_channel_configurations()
        discord_token = self._store.get_setting("discord.token")
        token_status, _ = self._store.get_health_status("discord_token")
        return MonitorState(
            channels=channels,
            runtime=runtime,
            network=network,
            discord_token=discord_token,
            discord_token_ok=token_status == "ok",
        )

    def _load_runtime(self) -> RuntimeOptions:
        def _float(key: str, default: float) -> float:
            value = self._store.get_setting(key)
            if value is None:
                return default
            try:
                return float(value)
            except ValueError:
                return default

        rate = self._store.get_setting("runtime.rate")
        if rate is not None:
            try:
                rate_value = float(rate)
            except ValueError:
                rate_value = 8.0
        else:
            # Backwards compatibility with older split settings
            legacy_discord = _float("runtime.discord_rate", 8.0)
            legacy_telegram = _float("runtime.telegram_rate", legacy_discord)
            rate_value = max(legacy_discord, legacy_telegram)

        min_delay = parse_delay_setting(self._store.get_setting("runtime.delay_min"), 0.0)
        max_delay = parse_delay_setting(self._store.get_setting("runtime.delay_max"), 0.0)
        if max_delay < min_delay:
            max_delay = min_delay

        health_interval = _float("runtime.health_interval", 180.0)
        deduplicate = parse_bool(
            self._store.get_setting("runtime.deduplicate_messages"), False
        )

        return RuntimeOptions(
            poll_interval=_float("runtime.poll", 2.0),
            min_delay_seconds=min_delay,
            max_delay_seconds=max_delay,
            rate_per_second=rate_value,
            healthcheck_interval=health_interval,
            deduplicate_messages=deduplicate,
        )

    def _load_network_options(self) -> NetworkOptions:
        return self._store.load_network_options()

    def _load_initial_health_statuses(self) -> dict[str, str]:
        statuses: dict[str, str] = {}
        prefix = "health."
        suffix = ".status"
        for key, value in self._store.iter_settings(prefix):
            if not key.endswith(suffix):
                continue
            subject = key[len(prefix) : -len(suffix)]
            if not subject:
                continue
            statuses[subject] = value
        return statuses

    async def _wait_for_health(self, target_version: int) -> None:
        while self._health_version < target_version:
            await self._health_ready.wait()
            if self._health_version < target_version:
                self._health_ready.clear()
