from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast

from forward_monitor.app import (
    ForwardMonitorApp,
    HealthUpdate,
    _discord_snowflake_from_datetime,
)
from forward_monitor.config_store import ConfigStore
from forward_monitor.discord import DiscordClient, ProxyCheckResult, TokenCheckResult
from forward_monitor.models import DiscordMessage, NetworkOptions, RuntimeOptions
from forward_monitor.telegram import TelegramAPI
from forward_monitor.utils import RateLimiter


class DummyDiscordClient:
    def __init__(self) -> None:
        self.token: str | None = None
        self.network: NetworkOptions | None = None
        self.fetch_calls: list[str] = []
        self.verify_calls: list[str] = []
        self.channel_checks: list[str] = []

    def set_token(self, token: str | None) -> None:
        self.token = token

    def set_network_options(self, options: NetworkOptions) -> None:
        self.network = options

    async def fetch_messages(
        self,
        channel_id: str,
        *,
        limit: int = 50,
        after: str | None = None,
        before: str | None = None,
    ) -> list[dict[str, object]]:
        self.fetch_calls.append(channel_id)
        return []

    async def fetch_pinned_messages(self, channel_id: str) -> list[dict[str, object]]:
        return []

    async def check_channel_exists(self, channel_id: str) -> bool:
        self.channel_checks.append(channel_id)
        return False

    async def check_proxy(self, network: NetworkOptions) -> ProxyCheckResult:
        return ProxyCheckResult(ok=True)

    async def verify_token(
        self,
        token: str,
        *,
        network: NetworkOptions | None = None,
    ) -> TokenCheckResult:
        self.verify_calls.append(token)
        return TokenCheckResult(ok=False, error="bad token", status=401)


class DummyTelegramAPI:
    def __init__(self) -> None:
        self.messages: list[tuple[int | str, str]] = []

    async def send_message(
        self,
        chat_id: int | str,
        text: str,
        *,
        parse_mode: str | None = None,
        disable_preview: bool = True,
        message_thread_id: int | None = None,
    ) -> None:
        self.messages.append((chat_id, text))

    async def send_photo(
        self,
        chat_id: int | str,
        photo: str,
        *,
        caption: str | None = None,
        parse_mode: str | None = None,
        message_thread_id: int | None = None,
    ) -> None:
        return None


async def _run_monitor_with_health(
    app: ForwardMonitorApp,
    discord: DummyDiscordClient,
    telegram: DummyTelegramAPI,
) -> None:
    monitor_task = asyncio.create_task(
        app._monitor_loop(cast(DiscordClient, discord), cast(TelegramAPI, telegram))
    )
    health_task = asyncio.create_task(
        app._healthcheck_loop(
            cast(DiscordClient, discord),
            cast(TelegramAPI, telegram),
            interval_override=0.05,
        )
    )

    try:
        await asyncio.sleep(0.2)
    finally:
        monitor_task.cancel()
        health_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task
        with contextlib.suppress(asyncio.CancelledError):
            await health_task


def test_monitor_skips_messages_before_start(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        record = store.add_channel("discord", "telegram", label="Test")
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE channels SET added_at=? WHERE id=?",
            ("2020-01-01T00:00:00+00:00", record.id),
        )
        conn.commit()
        conn.close()

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        app._startup_time = datetime(2023, 1, 1, tzinfo=timezone.utc)

        state = app._reload_state()
        assert state.channels
        channel = state.channels[0]

        old_time = app._startup_time - timedelta(days=1)
        new_time = app._startup_time + timedelta(minutes=5)
        old_id = str(_discord_snowflake_from_datetime(old_time))
        new_id = str(_discord_snowflake_from_datetime(new_time))

        messages = [
            DiscordMessage(
                id=old_id,
                channel_id=channel.discord_id,
                guild_id="1",
                author_id="42",
                author_name="Old",
                content="Old message",
                attachments=(),
                embeds=(),
                stickers=(),
                role_ids=set(),
                timestamp=old_time.isoformat(),
                message_type=0,
            ),
            DiscordMessage(
                id=new_id,
                channel_id=channel.discord_id,
                guild_id="1",
                author_id="43",
                author_name="New",
                content="New message",
                attachments=(),
                embeds=(),
                stickers=(),
                role_ids=set(),
                timestamp=new_time.isoformat(),
                message_type=0,
            ),
        ]

        class MessageDiscord:
            async def fetch_messages(
                self,
                channel_id: str,
                *,
                limit: int = 50,
                after: str | None = None,
                before: str | None = None,
            ) -> list[DiscordMessage]:
                return messages

        class RecordingTelegram:
            def __init__(self) -> None:
                self.sent: list[str] = []

            async def send_message(
                self,
                chat_id: int | str,
                text: str,
                *,
                parse_mode: str | None = None,
                disable_preview: bool = True,
                message_thread_id: int | None = None,
            ) -> None:
                self.sent.append(text)

            async def send_photo(
                self,
                chat_id: int | str,
                photo: str,
                *,
                caption: str | None = None,
                parse_mode: str | None = None,
                message_thread_id: int | None = None,
            ) -> None:
                raise AssertionError("photos are not expected")

        telegram_api = RecordingTelegram()
        runtime = RuntimeOptions()
        telegram_rate = RateLimiter(1000)

        app._refresh_event.clear()

        await app._process_channel(
            channel,
            cast(DiscordClient, MessageDiscord()),
            cast(TelegramAPI, telegram_api),
            telegram_rate,
            runtime,
        )

        assert len(telegram_api.sent) == 1
        assert "New message" in telegram_api.sent[0]
        assert "Old message" not in telegram_api.sent[0]

        stored = app._store.get_channel(channel.discord_id)
        assert stored is not None
        assert stored.last_message_id == new_id

        app._store.close()

    asyncio.run(runner())


def test_monitor_skips_duplicate_content_across_channels(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        store.add_channel("111", "telegram-1", label="First")
        store.add_channel("222", "telegram-2", label="Second")

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        reference_time = datetime.now(timezone.utc)
        app._startup_time = reference_time - timedelta(minutes=5)
        app._refresh_event.clear()

        def _make_message(identifier: str, channel_id: str) -> DiscordMessage:
            timestamp = reference_time + timedelta(minutes=1)
            message_id = str(_discord_snowflake_from_datetime(timestamp))
            return DiscordMessage(
                id=message_id,
                channel_id=channel_id,
                guild_id=None,
                author_id="user",
                author_name="User",
                content="Repeated text",
                attachments=(),
                embeds=(),
                stickers=(),
                role_ids=set(),
                timestamp=timestamp.isoformat(),
            )

        class DedupDiscord(DummyDiscordClient):
            def __init__(self) -> None:
                super().__init__()
                self.payloads = {
                    "111": [_make_message("10", "111")],
                    "222": [_make_message("20", "222")],
                }

            async def fetch_messages(
                self,
                channel_id: str,
                *,
                limit: int = 50,
                after: str | None = None,
                before: str | None = None,
            ) -> list[dict[str, object]]:
                self.fetch_calls.append(channel_id)
                return cast(list[dict[str, object]], list(self.payloads.get(channel_id, [])))

        discord = DedupDiscord()
        telegram = DummyTelegramAPI()
        runtime = RuntimeOptions(deduplicate_messages=True)
        telegram_rate = RateLimiter(1000)

        channels = app._store.load_channel_configurations()
        for channel in channels:
            await app._process_channel(
                channel,
                cast(DiscordClient, discord),
                cast(TelegramAPI, telegram),
                telegram_rate,
                runtime,
            )

        assert len(telegram.messages) == 1
        app._store.close()

    asyncio.run(runner())


def test_monitor_skips_messages_before_restart(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        record = store.add_channel("discord", "telegram", label="Test")

        processed_time = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
        pending_time = datetime(2023, 1, 1, 11, 45, tzinfo=timezone.utc)
        startup_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
        fresh_time = datetime(2023, 1, 1, 12, 5, tzinfo=timezone.utc)

        processed_id = str(_discord_snowflake_from_datetime(processed_time))
        pending_id = str(_discord_snowflake_from_datetime(pending_time))
        fresh_id = str(_discord_snowflake_from_datetime(fresh_time))

        store.set_last_message(record.id, processed_id)
        store.close()

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        app._startup_time = startup_time

        state = app._reload_state()
        channel = state.channels[0]

        pending_message = DiscordMessage(
            id=pending_id,
            channel_id=channel.discord_id,
            guild_id="1",
            author_id="42",
            author_name="Pending",
            content="Pending message",
            attachments=(),
            embeds=(),
            stickers=(),
            role_ids=set(),
            timestamp=pending_time.isoformat(),
            message_type=0,
        )
        fresh_message = DiscordMessage(
            id=fresh_id,
            channel_id=channel.discord_id,
            guild_id="1",
            author_id="43",
            author_name="Fresh",
            content="Fresh message",
            attachments=(),
            embeds=(),
            stickers=(),
            role_ids=set(),
            timestamp=fresh_time.isoformat(),
            message_type=0,
        )

        class MessageDiscord:
            async def fetch_messages(
                self,
                channel_id: str,
                *,
                limit: int = 50,
                after: str | None = None,
                before: str | None = None,
            ) -> list[DiscordMessage]:
                return [pending_message, fresh_message, fresh_message]

        class RecordingTelegram:
            def __init__(self) -> None:
                self.sent: list[str] = []

            async def send_message(
                self,
                chat_id: int | str,
                text: str,
                *,
                parse_mode: str | None = None,
                disable_preview: bool = True,
                message_thread_id: int | None = None,
            ) -> None:
                self.sent.append(text)

            async def send_photo(
                self,
                chat_id: int | str,
                photo: str,
                *,
                caption: str | None = None,
                parse_mode: str | None = None,
                message_thread_id: int | None = None,
            ) -> None:
                raise AssertionError("photos are not expected")

        telegram_api = RecordingTelegram()
        runtime = RuntimeOptions()
        telegram_rate = RateLimiter(1000)

        app._refresh_event.clear()
        await app._process_channel(
            channel,
            cast(DiscordClient, MessageDiscord()),
            cast(TelegramAPI, telegram_api),
            telegram_rate,
            runtime,
        )

        assert len(telegram_api.sent) == 1
        assert "Fresh message" in telegram_api.sent[0]
        assert "Pending message" not in telegram_api.sent[0]

        stored = app._store.get_channel(channel.discord_id)
        assert stored is not None
        assert stored.last_message_id == fresh_id

        app._store.close()

    asyncio.run(runner())


def test_pinned_monitor_skips_messages_before_start(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        record = store.add_channel("discord", "telegram", label="Test")
        store.set_channel_option(record.id, "monitoring.mode", "pinned")
        store.set_known_pinned_messages(record.id, [])
        store.set_pinned_synced(record.id, synced=True)
        store.close()

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        startup_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
        app._startup_time = startup_time

        state = app._reload_state()
        channel = state.channels[0]
        assert channel.pinned_only is True

        old_time = startup_time - timedelta(days=1)
        old_id = str(_discord_snowflake_from_datetime(old_time))

        old_message = DiscordMessage(
            id=old_id,
            channel_id=channel.discord_id,
            guild_id="guild",
            author_id="42",
            author_name="Old",
            content="Pinned before start",
            attachments=(),
            embeds=(),
            stickers=(),
            role_ids=set(),
            timestamp=old_time.isoformat(),
            message_type=0,
        )

        class PinnedDiscord:
            async def fetch_pinned_messages(self, channel_id: str) -> list[DiscordMessage]:
                return [old_message]

        class RecordingTelegram:
            def __init__(self) -> None:
                self.sent: list[str] = []

            async def send_message(
                self,
                chat_id: int | str,
                text: str,
                *,
                parse_mode: str | None = None,
                disable_preview: bool = True,
                message_thread_id: int | None = None,
            ) -> None:
                self.sent.append(text)

            async def send_photo(
                self,
                chat_id: int | str,
                photo: str,
                *,
                caption: str | None = None,
                parse_mode: str | None = None,
                message_thread_id: int | None = None,
            ) -> None:
                raise AssertionError("photos are not expected")

        telegram_api = RecordingTelegram()
        runtime = RuntimeOptions()
        telegram_rate = RateLimiter(1000)

        app._refresh_event.clear()

        await app._process_channel(
            channel,
            cast(DiscordClient, PinnedDiscord()),
            cast(TelegramAPI, telegram_api),
            telegram_rate,
            runtime,
        )

        assert telegram_api.sent == []
        assert old_id in channel.known_pinned_ids

        refreshed = app._store.load_channel_configurations()
        assert refreshed and old_id in refreshed[0].known_pinned_ids

        app._store.close()

    asyncio.run(runner())


def test_pinned_monitor_records_unforwardable_messages(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        record = store.add_channel("discord", "telegram", label="Test")
        store.set_channel_option(record.id, "monitoring.mode", "pinned")
        store.set_known_pinned_messages(record.id, [])
        store.set_pinned_synced(record.id, synced=True)
        store.close()

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        startup_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
        app._startup_time = startup_time

        state = app._reload_state()
        channel = state.channels[0]

        recent_time = startup_time + timedelta(minutes=5)
        system_id = str(_discord_snowflake_from_datetime(recent_time))

        system_message = DiscordMessage(
            id=system_id,
            channel_id=channel.discord_id,
            guild_id="guild",
            author_id="99",
            author_name="System",
            content="System notice",
            attachments=(),
            embeds=(),
            stickers=(),
            role_ids=set(),
            timestamp=recent_time.isoformat(),
            message_type=6,
        )

        class PinnedDiscord:
            async def fetch_pinned_messages(self, channel_id: str) -> list[DiscordMessage]:
                return [system_message]

        class RecordingTelegram:
            def __init__(self) -> None:
                self.sent: list[str] = []

            async def send_message(
                self,
                chat_id: int | str,
                text: str,
                *,
                parse_mode: str | None = None,
                disable_preview: bool = True,
                message_thread_id: int | None = None,
            ) -> None:
                self.sent.append(text)

            async def send_photo(
                self,
                chat_id: int | str,
                photo: str,
                *,
                caption: str | None = None,
                parse_mode: str | None = None,
                message_thread_id: int | None = None,
            ) -> None:
                raise AssertionError("photos are not expected")

        telegram_api = RecordingTelegram()
        runtime = RuntimeOptions()
        telegram_rate = RateLimiter(1000)

        app._refresh_event.clear()

        await app._process_channel(
            channel,
            cast(DiscordClient, PinnedDiscord()),
            cast(TelegramAPI, telegram_api),
            telegram_rate,
            runtime,
        )

        assert telegram_api.sent == []
        assert system_id in channel.known_pinned_ids

        refreshed = app._store.load_channel_configurations()
        assert refreshed and system_id in refreshed[0].known_pinned_ids

        app._store.close()

    asyncio.run(runner())


def test_monitor_waits_for_health_before_processing(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        store.set_setting("discord.token", "bad-token")
        store.set_setting("runtime.poll", "0.1")
        store.add_channel("123", "456", label="Test")

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        discord = DummyDiscordClient()
        telegram = DummyTelegramAPI()

        await _run_monitor_with_health(app, discord, telegram)

        assert discord.fetch_calls == []
        assert discord.verify_calls != []
        app._store.close()

    asyncio.run(runner())


def test_health_check_skips_when_proxy_fails(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        store.set_setting("discord.token", "token")
        store.add_channel("123", "456", label="Test")
        store.set_setting("proxy.discord.url", "http://proxy.local")

        class ProxyFailDiscord(DummyDiscordClient):
            def __init__(self) -> None:
                super().__init__()
                self.verify_attempts = 0

            async def check_proxy(self, network: NetworkOptions) -> ProxyCheckResult:
                return ProxyCheckResult(ok=False, error="proxy down")

            async def verify_token(
                self, token: str, *, network: NetworkOptions | None = None
            ) -> TokenCheckResult:
                self.verify_attempts += 1
                return TokenCheckResult(ok=True)

            async def check_channel_exists(self, channel_id: str) -> bool:
                raise AssertionError("channel check should not be called when proxy fails")

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        discord = ProxyFailDiscord()
        telegram = DummyTelegramAPI()

        state = app._reload_state()
        await app._run_health_checks(
            state,
            cast(DiscordClient, discord),
            cast(TelegramAPI, telegram),
        )

        assert discord.verify_attempts == 0
        status, message = store.get_health_status("discord_token")
        assert status == "unknown"
        assert message == "Проверка недоступна: прокси не отвечает."
        channel_status, channel_message = store.get_health_status("channel.123")
        assert channel_status == "unknown"
        assert channel_message == "Проверка недоступна: прокси не отвечает."
        app._store.close()

    asyncio.run(runner())


def test_run_health_checks_configures_client(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        store.set_setting("discord.token", "token-123")
        store.add_channel("123", "456", label="Test")

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        discord = DummyDiscordClient()
        telegram = DummyTelegramAPI()

        state = app._reload_state()
        await app._run_health_checks(
            state,
            cast(DiscordClient, discord),
            cast(TelegramAPI, telegram),
        )

        assert discord.token == "token-123"
        assert discord.network is not None
        app._store.close()

    asyncio.run(runner())


def test_app_restores_existing_health_status(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        store.add_channel("123", "456", label="Test")
        store.set_health_status(
            "channel.123", "error", "Discord канал недоступен или нет прав."
        )
        store.close()

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        assert app._health_status.get("channel.123") == "error"

        telegram = DummyTelegramAPI()
        update = HealthUpdate(
            key="channel.123",
            status="error",
            message="Discord канал недоступен или нет прав.",
            label="Test",
        )
        await app._emit_health_notifications([update], cast(TelegramAPI, telegram))

        assert telegram.messages == []
        app._store.close()

    asyncio.run(runner())


def test_health_check_normalizes_stored_token(tmp_path: Path) -> None:
    async def runner() -> None:
        db_path = tmp_path / "db.sqlite"
        store = ConfigStore(db_path)
        store.set_setting("discord.token", "raw-token")
        store.add_channel("123", "456", label="Test")

        class NormalizingDiscord(DummyDiscordClient):
            async def verify_token(
                self,
                token: str,
                *,
                network: NetworkOptions | None = None,
            ) -> TokenCheckResult:
                self.verify_calls.append(token)
                return TokenCheckResult(
                    ok=True,
                    display_name="bot",
                    normalized_token="Bot raw-token",
                )

            async def check_channel_exists(self, channel_id: str) -> bool:
                self.channel_checks.append(channel_id)
                return True

        app = ForwardMonitorApp(db_path=db_path, telegram_token="token")
        discord = NormalizingDiscord()
        telegram = DummyTelegramAPI()

        state = app._reload_state()
        await app._run_health_checks(
            state,
            cast(DiscordClient, discord),
            cast(TelegramAPI, telegram),
        )

        assert store.get_setting("discord.token") == "Bot raw-token"
        assert discord.token == "Bot raw-token"

        store.close()
        app._store.close()

    asyncio.run(runner())
