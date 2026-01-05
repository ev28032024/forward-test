from __future__ import annotations

import asyncio
from collections.abc import Iterable

from forward_monitor.models import FormattedTelegramMessage
from forward_monitor.telegram import TelegramAPIProtocol, send_formatted


class RecordingTelegramAPI(TelegramAPIProtocol):
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.photos: list[dict[str, object]] = []

    async def get_updates(
        self, offset: int | None = None, timeout: int = 30
    ) -> list[dict[str, object]]:
        raise NotImplementedError

    async def set_my_commands(
        self, commands: Iterable[tuple[str, str]]
    ) -> None:
        raise NotImplementedError

    async def send_message(
        self,
        chat_id: int | str,
        text: str,
        *,
        parse_mode: str | None = None,
        disable_preview: bool = True,
        message_thread_id: int | None = None,
    ) -> None:
        self.messages.append(
            {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_preview": disable_preview,
                "thread_id": message_thread_id,
            }
        )

    async def send_photo(
        self,
        chat_id: int | str,
        photo: str,
        *,
        caption: str | None = None,
        parse_mode: str | None = None,
        message_thread_id: int | None = None,
    ) -> None:
        self.photos.append(
            {
                "chat_id": chat_id,
                "photo": photo,
                "caption": caption,
                "parse_mode": parse_mode,
                "thread_id": message_thread_id,
            }
        )

    async def answer_callback_query(self, callback_id: str, text: str) -> None:
        raise NotImplementedError


def test_send_formatted_forwards_images() -> None:
    api = RecordingTelegramAPI()
    message = FormattedTelegramMessage(
        text="Primary text",
        extra_messages=("Extra",),
        parse_mode="HTML",
        disable_preview=True,
        image_urls=(
            "https://cdn.example.com/image-a.png",
            "https://cdn.example.com/image-b.png",
        ),
    )

    asyncio.run(send_formatted(api, "chat", message, thread_id=77))

    assert len(api.messages) == 2
    assert api.messages[0]["text"] == "Primary text"
    assert api.messages[0]["parse_mode"] == "HTML"
    assert api.messages[0]["disable_preview"] is True
    assert api.messages[0]["thread_id"] == 77
    assert api.messages[1]["text"] == "Extra"
    assert api.messages[1]["thread_id"] == 77

    assert [photo["photo"] for photo in api.photos] == [
        "https://cdn.example.com/image-a.png",
        "https://cdn.example.com/image-b.png",
    ]
    assert all(photo["caption"] is None for photo in api.photos)
    assert all(photo["parse_mode"] is None for photo in api.photos)
    assert all(photo["thread_id"] == 77 for photo in api.photos)
