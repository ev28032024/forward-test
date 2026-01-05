from __future__ import annotations

from forward_monitor.formatting import format_discord_message
from forward_monitor.models import ChannelConfig, DiscordMessage, FilterConfig, FormattingOptions


def test_links_style_shows_urls() -> None:
    channel = ChannelConfig(
        discord_id="1",
        telegram_chat_id="2",
        telegram_thread_id=None,
        label="Test",
        formatting=FormattingOptions(attachments_style="links"),
        filters=FilterConfig(),
        last_message_id=None,
        storage_id=1,
    )
    message = DiscordMessage(
        id="1",
        channel_id="1",
        guild_id="g",
        author_id="1",
        author_name="User",
        content="",
        attachments=({"url": "https://example.com/a.png", "filename": "a.png"},),
        embeds=(),
        stickers=(),
        role_ids=set(),
    )
    formatted = format_discord_message(message, channel)
    assert formatted.parse_mode == "HTML"
    assert formatted.image_urls == ("https://example.com/a.png",)
    assert "Ссылки на вложения" not in formatted.text
