from datetime import datetime, timezone

from forward_monitor.deduplication import (
    MessageDeduplicator,
    build_message_signature,
)
from forward_monitor.models import DiscordMessage


def _make_message(
    content: str,
    *,
    message_id: str,
    attachments: tuple[dict[str, str], ...] = (),
    embeds: tuple[dict[str, str], ...] = (),
) -> DiscordMessage:
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).isoformat()
    return DiscordMessage(
        id=message_id,
        channel_id="chan",
        guild_id=None,
        author_id="user",
        author_name="User",
        content=content,
        attachments=attachments,
        embeds=embeds,
        stickers=(),
        role_ids=set(),
        timestamp=now,
    )


def test_message_signature_includes_attachments() -> None:
    message = _make_message("Hello", message_id="1")
    with_attachments = _make_message(
        "Hello",
        message_id="2",
        attachments=(
            {
                "filename": "file.txt",
                "url": "https://example.com/file.txt",
            },
        ),
    )

    base_signature = build_message_signature(message)
    attachment_signature = build_message_signature(with_attachments)

    assert base_signature and "Hello" in base_signature
    assert attachment_signature and "attachments:" in attachment_signature
    assert base_signature != attachment_signature


def test_deduplicator_recognizes_repeated_signatures() -> None:
    deduplicator = MessageDeduplicator(capacity=2)
    first = build_message_signature(_make_message("Same text", message_id="1"))
    second = build_message_signature(_make_message("Same text", message_id="2"))

    assert deduplicator.is_duplicate(first) is False
    assert deduplicator.is_duplicate(second) is True

    another = build_message_signature(_make_message("Other", message_id="3"))
    deduplicator.is_duplicate(another)
    deduplicator.is_duplicate("unique")

    # First signature should be evicted once capacity is exceeded
    assert deduplicator.is_duplicate(first) is False
