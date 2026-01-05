from __future__ import annotations

from typing import Any, Iterable, Mapping

from forward_monitor.filters import FilterEngine
from forward_monitor.models import DiscordMessage, FilterConfig


def make_message(**kwargs: Any) -> DiscordMessage:
    attachments: Iterable[Mapping[str, Any]] = kwargs.get("attachments", [])
    embeds: Iterable[Mapping[str, Any]] = kwargs.get("embeds", [])
    return DiscordMessage(
        id=str(kwargs.get("id", "1")),
        channel_id=str(kwargs.get("channel_id", "10")),
        guild_id=str(kwargs.get("guild_id", "20")),
        author_id=str(kwargs.get("author_id", "42")),
        author_name=str(kwargs.get("author_name", "User")),
        content=str(kwargs.get("content", "")),
        attachments=tuple(attachments),
        embeds=tuple(embeds),
        stickers=tuple(kwargs.get("stickers", ())),
        role_ids=set(kwargs.get("role_ids", set())),
    )


def test_filter_engine_whitelist_and_blacklist() -> None:
    config = FilterConfig(whitelist={"promo"}, blacklist={"spam"})
    engine = FilterEngine(config)

    allowed = engine.evaluate(make_message(content="Big promo today"))
    rejected = engine.evaluate(make_message(content="spam only"))
    missing = engine.evaluate(make_message(content="nothing interesting"))

    assert allowed.allowed is True
    assert rejected.allowed is False
    assert missing.allowed is False


def test_filter_engine_types() -> None:
    config = FilterConfig(allowed_types={"image"})
    engine = FilterEngine(config)
    image_message = make_message(attachments=[{"filename": "image.png"}])
    file_message = make_message(attachments=[{"filename": "report.pdf"}])

    assert engine.evaluate(image_message).allowed is True
    assert engine.evaluate(file_message).allowed is False


def test_filter_engine_blocks_stickers() -> None:
    engine = FilterEngine(FilterConfig())
    sticker_message = make_message(stickers=[{"id": "1", "name": "hi"}])

    decision = engine.evaluate(sticker_message)

    assert decision.allowed is False
    assert decision.reason == "sticker_blocked"


def test_filter_engine_allowed_and_blocked_senders_by_name() -> None:
    allowed_config = FilterConfig(allowed_senders={"coded"})
    allowed_engine = FilterEngine(allowed_config)

    permitted = allowed_engine.evaluate(make_message(author_name="Coded", author_id="99"))
    rejected = allowed_engine.evaluate(make_message(author_name="Other", author_id="99"))

    assert permitted.allowed is True
    assert rejected.allowed is False
    assert rejected.reason == "sender_not_allowed"

    blocked_config = FilterConfig(blocked_senders={"coded"})
    blocked_engine = FilterEngine(blocked_config)
    blocked = blocked_engine.evaluate(make_message(author_name="Coded", author_id="42"))

    assert blocked.allowed is False
    assert blocked.reason == "sender_blocked"


def test_filter_engine_roles() -> None:
    config = FilterConfig(allowed_roles={"123"})
    engine = FilterEngine(config)

    allowed = engine.evaluate(make_message(role_ids={"123"}))
    rejected = engine.evaluate(make_message(role_ids=set()))

    assert allowed.allowed is True
    assert rejected.allowed is False
    assert rejected.reason == "role_not_allowed"

    blocked_engine = FilterEngine(FilterConfig(blocked_roles={"555"}))
    blocked = blocked_engine.evaluate(make_message(role_ids={"555", "777"}))
    assert blocked.allowed is False
    assert blocked.reason == "role_blocked"
