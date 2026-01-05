"""Simple benchmark comparing legacy formatter and the new pipeline."""

from __future__ import annotations

import re
import statistics
import sys
import time
from html import escape
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterable

if TYPE_CHECKING:
    from forward_monitor.models import ChannelConfig as ChannelConfigType
    from forward_monitor.models import DiscordMessage as DiscordMessageType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _sample_channel() -> "ChannelConfigType":
    from forward_monitor.models import ChannelConfig, FilterConfig, FormattingOptions

    return ChannelConfig(
        discord_id="1",
        telegram_chat_id="2",
        telegram_thread_id=None,
        label="Bench",
        formatting=FormattingOptions(max_length=1000, attachments_style="summary"),
        filters=FilterConfig(),
        last_message_id=None,
        storage_id=1,
    )


def _sample_message() -> "DiscordMessageType":
    from forward_monitor.models import DiscordMessage

    attachments = [
        {"url": "https://cdn.example.com/file.png", "filename": "file.png", "size": 2048},
        {"url": "https://cdn.example.com/info.txt", "filename": "info.txt", "size": 8192},
    ]
    embeds = [
        {
            "title": "Release notes",
            "description": "All the details about the new version",
            "fields": [
                {"name": "Feature", "value": "Something big"},
            ],
        }
    ]
    return DiscordMessage(
        id="1",
        channel_id="1",
        guild_id="1",
        author_id="1",
        author_name="Bench",
        content="foo" * 20,
        attachments=tuple(attachments),
        embeds=tuple(embeds),
        stickers=(),
        role_ids=set(),
    )


class LegacyFormatter:
    """Minimal recreation of the previous heavier formatter."""

    def __init__(self) -> None:
        self._channel = _sample_channel()

    def run(self, message: "DiscordMessageType") -> None:
        text = message.content.replace("foo", "bar")
        # Simulate legacy regex cleanup
        text = re.sub(r"\s+", " ", text)
        for _ in range(5):
            text = re.sub(r"[A-Z]", lambda m: m.group(0).lower(), text)
            text = re.sub(r"[aeiou]", "*", text)
        attachments = [f"{item.get('filename')}: {item.get('url')}" for item in message.attachments]
        embed_parts: list[str] = []
        for embed in message.embeds:
            embed_parts.extend(
                part
                for part in (
                    embed.get("title", ""),
                    embed.get("description", ""),
                    "\n".join(
                        f"{field.get('name')}: {field.get('value')}"
                        for field in embed.get("fields", [])
                    ),
                )
                if part
            )
        lines = list(
            chain(
                [self._channel.label + " • " + message.author_name],
                [text],
                embed_parts,
                attachments,
            )
        )
        joined = "\n".join(filter(None, (escape(line) for line in lines)))
        if len(joined) > self._channel.formatting.max_length:
            joined[: self._channel.formatting.max_length]


def _time(callable_obj: Callable[[], None], iterations: int) -> float:
    start = time.perf_counter()
    for _ in range(iterations):
        callable_obj()
    return time.perf_counter() - start


def benchmark_formatter(iterations: int) -> None:
    from forward_monitor.formatting import format_discord_message

    channel = _sample_channel()
    message = _sample_message()

    for _ in range(iterations):
        format_discord_message(message, channel)


async def benchmark_forwarding(iterations: int) -> None:
    from forward_monitor.formatting import format_discord_message
    from forward_monitor.telegram import TelegramAPIProtocol, send_formatted

    channel = _sample_channel()
    message = _sample_message()

    class _NoopAPI:
        def set_proxy(self, proxy: str | None) -> None:
            return None

        async def get_updates(
            self,
            offset: int | None = None,
            timeout: int = 30,
        ) -> list[dict[str, object]]:
            return []

        async def set_my_commands(self, commands: Iterable[tuple[str, str]]) -> None:
            return None

        async def send_message(
            self,
            chat_id: int | str,
            text: str,
            *,
            parse_mode: str | None = None,
            disable_preview: bool = True,
            message_thread_id: int | None = None,
        ) -> None:
            return None

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
            return None

    api: TelegramAPIProtocol = _NoopAPI()

    for _ in range(iterations):
        formatted = format_discord_message(message, channel)
        await send_formatted(
            api,
            channel.telegram_chat_id,
            formatted,
            thread_id=channel.telegram_thread_id,
        )


def main() -> None:
    iterations = 5_000
    new_channel = _sample_channel()
    message = _sample_message()
    legacy = LegacyFormatter()

    def run_new() -> None:
        from forward_monitor.formatting import format_discord_message

        format_discord_message(message, new_channel)

    def run_old() -> None:
        legacy.run(message)

    new_times = [_time(run_new, iterations) for _ in range(5)]
    old_times = [_time(run_old, iterations) for _ in range(5)]

    print("Benchmark results (smaller is better)")
    print("Iterations per batch:", iterations)
    print()
    print(f"Legacy average: {statistics.mean(old_times):.4f}s")
    print(f"Legacy stdev:   {statistics.pstdev(old_times):.4f}s")
    print(f"New average:    {statistics.mean(new_times):.4f}s")
    print(f"New stdev:      {statistics.pstdev(new_times):.4f}s")
    improvement = statistics.mean(old_times) / statistics.mean(new_times)
    print(f"Speedup:        x{improvement:.2f}")


if __name__ == "__main__":
    main()
