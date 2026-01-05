from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Iterable, cast

from forward_monitor.config_store import ConfigStore
from forward_monitor.discord import DiscordClient, ProxyCheckResult, TokenCheckResult
from forward_monitor.models import DiscordMessage, NetworkOptions
from forward_monitor.telegram import BOT_COMMANDS, CommandContext, TelegramController
from forward_monitor.utils import ChannelProcessingGuard


class DummyAPI:
    def __init__(self) -> None:
        self.messages: list[tuple[int | str, str]] = []
        self.commands: list[tuple[str, str]] = []

    def set_proxy(self, proxy: str | None) -> None:
        return None

    async def get_updates(
        self,
        offset: int | None = None,
        timeout: int = 30,
    ) -> list[dict[str, object]]:
        await asyncio.sleep(0)
        return []

    async def set_my_commands(self, commands: Iterable[tuple[str, str]]) -> None:
        self.commands = list(commands)

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

    async def answer_callback_query(self, callback_id: str, text: str) -> None:
        return None

    async def send_photo(
        self,
        chat_id: int | str,
        photo: str,
        *,
        caption: str | None = None,
        parse_mode: str | None = None,
        message_thread_id: int | None = None,
    ) -> None:
        self.messages.append((chat_id, f"PHOTO:{photo}"))


class DummyDiscordClient:
    def __init__(self) -> None:
        self.tokens: list[str] = []
        self.proxies: list[str | None] = []

    async def verify_token(
        self, token: str, *, network: NetworkOptions | None = None
    ) -> TokenCheckResult:
        self.tokens.append(token)
        return TokenCheckResult(ok=True, display_name="tester")

    async def check_proxy(self, network: NetworkOptions) -> ProxyCheckResult:
        self.proxies.append(getattr(network, "discord_proxy_url", None))
        return ProxyCheckResult(ok=True)

    async def fetch_pinned_messages(self, channel_id: str) -> list[dict[str, object]]:
        return []


def test_controller_respects_admin_permissions(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        changed = False

        def on_change() -> None:
            nonlocal changed
            changed = True

        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=on_change,
        )
        ctx = CommandContext(
            chat_id=1,
            user_id=100,
            username="user",
            handle="user",
            args="token",
            message={"chat": {"id": 1, "type": "private"}},
        )

        await controller._dispatch("set_discord_token", ctx)
        assert store.get_setting("discord.token") is None
        assert api.messages == []

        await controller._dispatch("claim", ctx)
        admins = store.list_admins()
        assert len(admins) == 1
        assert admins[0].user_id == 100
        assert admins[0].username == "user"

        await controller._dispatch("set_discord_token", ctx)
        assert store.get_setting("discord.token") == "token"
        assert changed is True

    import asyncio

    asyncio.run(runner())


def test_set_discord_token_uses_normalized_value(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()

        class NormalizingDiscord(DummyDiscordClient):
            async def verify_token(
                self,
                token: str,
                *,
                network: NetworkOptions | None = None,
            ) -> TokenCheckResult:
                self.tokens.append(token)
                return TokenCheckResult(
                    ok=True,
                    display_name="bot",
                    normalized_token="Bot normalized-token",
                )

        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, NormalizingDiscord()),
            on_change=lambda: None,
        )

        ctx = CommandContext(
            chat_id=1,
            user_id=1,
            username="user",
            handle="user",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )

        await controller._dispatch("claim", ctx)
        ctx.args = "raw-token"
        await controller._dispatch("set_discord_token", ctx)

        assert store.get_setting("discord.token") == "Bot normalized-token"

    asyncio.run(runner())


def test_grant_admin_by_username(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        store.add_admin(1, "root")
        admin_ctx = CommandContext(
            chat_id=1,
            user_id=1,
            username="Root",
            handle="root",
            args="@newbie",
            message={"chat": {"id": 1, "type": "private"}},
        )
        await controller._dispatch("grant", admin_ctx)
        admins = store.list_admins()
        assert any(admin.username == "newbie" for admin in admins)

        newcomer_ctx = CommandContext(
            chat_id=1,
            user_id=222,
            username="Newbie",
            handle="newbie",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )
        store.remember_user(newcomer_ctx.user_id, newcomer_ctx.handle)
        await controller._dispatch("status", newcomer_ctx)
        assert any("статус" in text.lower() for _, text in api.messages)

    asyncio.run(runner())


def test_controller_persists_update_offset(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        store.set_telegram_offset(50)

        class UpdateAPI(DummyAPI):
            def __init__(self) -> None:
                super().__init__()
                self.offsets: list[int] = []
                self._served = False

            async def get_updates(
                self,
                offset: int | None = None,
                timeout: int = 30,
            ) -> list[dict[str, object]]:
                self.offsets.append(offset or 0)
                if not self._served:
                    self._served = True
                    return [
                        {
                            "update_id": 55,
                            "message": {
                                "message_id": 1,
                                "chat": {"id": 1, "type": "private"},
                                "from": {
                                    "id": 1,
                                    "first_name": "Tester",
                                    "username": "tester",
                                },
                                "text": "/start",
                            },
                        }
                    ]
                await asyncio.sleep(0)
                return []

        api = UpdateAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        task = asyncio.create_task(controller.run())
        await asyncio.sleep(0.1)
        controller.stop()
        await task

        assert api.offsets and api.offsets[0] == 50
        assert store.get_telegram_offset() == 56
        assert any("Forward Monitor" in text for _, text in api.messages)

    asyncio.run(runner())


def test_non_admin_cannot_invoke_commands_after_admin_exists(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        admin_ctx = CommandContext(
            chat_id=1,
            user_id=1,
            username="Admin",
            handle="admin",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )
        await controller._dispatch("claim", admin_ctx)

        outsider_ctx = CommandContext(
            chat_id=1,
            user_id=200,
            username="Visitor",
            handle="visitor",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )

        before = len(api.messages)
        await controller._dispatch("status", outsider_ctx)
        assert len(api.messages) == before + 1
        assert any(
            "Нет доступа" in text for _, text in api.messages[before:]
        )

        before = len(api.messages)
        await controller._dispatch("help", outsider_ctx)
        assert len(api.messages) > before
        assert any(
            "/help" in text or "🚀" in text for _, text in api.messages[before:]
        )

    asyncio.run(runner())


def test_claim_rejected_for_non_admin_when_admin_exists(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        store.add_admin(1, "root")

        outsider_ctx = CommandContext(
            chat_id=1,
            user_id=200,
            username="Visitor",
            handle="visitor",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )

        before = len(api.messages)
        await controller._dispatch("claim", outsider_ctx)
        assert len(api.messages) == before + 1
        assert any(
            "Нет доступа" in text for _, text in api.messages[before:]
        )
        admins = store.list_admins()
        assert len(admins) == 1
        assert admins[0].username == "root"

    asyncio.run(runner())


def test_controller_handles_command_errors(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        admin_ctx = CommandContext(
            chat_id=1,
            user_id=1,
            username="Admin",
            handle="admin",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )
        await controller._dispatch("claim", admin_ctx)

        async def failing(_: CommandContext) -> None:
            raise RuntimeError("boom")

        controller.cmd_status = failing  # type: ignore[assignment]

        before = len(api.messages)
        await controller._dispatch("status", admin_ctx)
        assert len(api.messages) == before + 1
        assert "ошиб" in api.messages[-1][1].lower()

        await controller._dispatch("help", admin_ctx)
        assert any("Основные команды" in text for _, text in api.messages)

    asyncio.run(runner())


def test_controller_registers_bot_commands(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )
        controller.stop()
        await controller.run()
        expected = [(info.name, info.summary) for info in BOT_COMMANDS]
        assert api.commands == expected

    asyncio.run(runner())


def test_help_lists_all_commands(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        ctx = CommandContext(
            chat_id=1,
            user_id=42,
            username="Explorer",
            handle="explorer",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )

        await controller._dispatch("help", ctx)
        combined = "\n".join(text for _, text in api.messages)
        for info in BOT_COMMANDS:
            assert f"/{info.name}" in combined

    asyncio.run(runner())


def test_list_channels_grouped_output(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        admin = CommandContext(
            chat_id=1,
            user_id=1,
            username="Admin",
            handle="admin",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )
        await controller._dispatch("claim", admin)

        store.add_channel("100", "-1001", "Alpha")
        store.add_channel("200", "-1001", "Beta", telegram_thread_id=2)
        store.add_channel("300", "-1002", "Gamma")
        store.set_health_status("channel.200", "error", "Нет доступа")

        api.messages.clear()
        await controller._dispatch("list_channels", admin)
        payload = "\n".join(text for _, text in api.messages)

        assert payload.count("<b>Telegram <code>") == 2
        assert "Alpha" in payload and "Beta" in payload and "Gamma" in payload
        assert payload.index("Alpha") < payload.index("Beta")
        normalized_payload = payload.replace("\u00A0", " ")
        assert "🧵 <b>Тема <code>2</code></b>" in normalized_payload
        assert "Нет доступа" in payload

    asyncio.run(runner())


def test_status_groups_channels_by_chat(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, DummyDiscordClient()),
            on_change=lambda: None,
        )

        admin = CommandContext(
            chat_id=1,
            user_id=1,
            username="Admin",
            handle="admin",
            args="",
            message={"chat": {"id": 1, "type": "private"}},
        )
        await controller._dispatch("claim", admin)

        store.add_channel("100", "-1001", "Alpha")
        store.add_channel("200", "-1001", "Beta", telegram_thread_id=2)
        store.add_channel("300", "-1002", "Gamma")
        store.set_health_status("channel.200", "error", "Нет доступа")

        api.messages.clear()
        await controller._dispatch("status", admin)
        combined = "\n".join(text for _, text in api.messages)

        assert combined.count("<b>Telegram <code>") == 2
        first_idx = combined.index("-1001")
        second_idx = combined.index("-1002")
        assert first_idx < second_idx
        first_group = combined[first_idx:second_idx]
        assert "Alpha" in first_group and "Beta" in first_group
        assert "Gamma" in combined[second_idx:]
        normalized_combined = combined.replace("\u00A0", " ")
        assert "🧵 <b>Тема <code>2</code></b>" in normalized_combined

    asyncio.run(runner())


def test_manual_forward_uses_channel_guard(tmp_path: Path) -> None:
    async def runner() -> None:
        store = ConfigStore(tmp_path / "db.sqlite")
        api = DummyAPI()

        class ManualDiscord(DummyDiscordClient):
            def __init__(self) -> None:
                super().__init__()
                self.token: str | None = None
                self.options: NetworkOptions | None = None

            def set_token(self, token: str | None) -> None:
                self.token = token

            def set_network_options(self, options: NetworkOptions) -> None:
                self.options = options

            async def fetch_messages(
                self,
                channel_id: str,
                *,
                limit: int = 50,
                after: str | None = None,
                before: str | None = None,
            ) -> list[DiscordMessage]:
                return [
                    DiscordMessage(
                        id="1001",
                        channel_id=channel_id,
                        guild_id=None,
                        author_id="42",
                        author_name="Tester",
                        content="hello",
                        attachments=(),
                        embeds=(),
                        stickers=(),
                        role_ids=set(),
                    )
                ]

        class RecordingGuard(ChannelProcessingGuard):
            def __init__(self) -> None:
                super().__init__()
                self.calls: list[str] = []

            @asynccontextmanager
            async def lock(self, key: str) -> AsyncIterator[None]:
                self.calls.append(f"enter:{key}")
                try:
                    async with super().lock(key):
                        yield
                finally:
                    self.calls.append(f"exit:{key}")

        guard = RecordingGuard()
        discord = ManualDiscord()
        controller = TelegramController(
            api,
            store,
            discord_client=cast(DiscordClient, discord),
            on_change=lambda: None,
            channel_guard=guard,
        )

        store.add_admin(1, "admin")
        store.set_setting("discord.token", "token")
        store.add_channel("123", "456", label="Demo")

        ctx = CommandContext(
            chat_id=1,
            user_id=1,
            username="Admin",
            handle="admin",
            args="1 123",
            message={"chat": {"id": 1, "type": "private"}},
        )

        await controller._dispatch("send_recent", ctx)

        assert guard.calls == ["enter:123", "exit:123"]
        assert any(chat_id == 1 for chat_id, _ in api.messages)

    asyncio.run(runner())
