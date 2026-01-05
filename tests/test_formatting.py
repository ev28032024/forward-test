from __future__ import annotations

from forward_monitor.formatting import format_discord_message
from forward_monitor.models import ChannelConfig, DiscordMessage, FilterConfig, FormattingOptions


def sample_channel(**overrides: object) -> ChannelConfig:
    formatting = FormattingOptions()
    channel = ChannelConfig(
        discord_id="123",
        telegram_chat_id="456",
        telegram_thread_id=None,
        label="Label",
        formatting=formatting,
        filters=FilterConfig(),
        last_message_id=None,
        storage_id=1,
    )
    for key, value in overrides.items():
        setattr(channel, key, value)
    return channel


def test_formatting_includes_label_and_author() -> None:
    message = DiscordMessage(
        id="1",
        channel_id="123",
        guild_id="456",
        author_id="99",
        author_name="Author",
        content="original content",
        attachments=(
            {
                "url": "https://example.com/file.txt",
                "filename": "file.txt",
                "size": 1024,
            },
        ),
        embeds=(),
        stickers=(),
        role_ids=set(),
        timestamp="2024-01-02T03:04:05+00:00",
    )
    formatted = format_discord_message(message, sample_channel())
    assert formatted.parse_mode == "HTML"
    assert formatted.text.startswith("<b>────── ✦ ──────</b>")
    assert "📣 <b>Label</b>" in formatted.text
    assert "💬 <b>Новое сообщение</b>" in formatted.text
    assert "👤 <b>Author</b>" in formatted.text
    assert "original content" in formatted.text
    assert "file.txt" in formatted.text
    assert "📅 <b>02.01.2024 06:04 MSK</b>" in formatted.text
    assert formatted.text.rstrip().endswith("</b>")


def test_formatting_chunks_long_text() -> None:
    channel = sample_channel()
    channel.formatting.max_length = 50
    message = DiscordMessage(
        id="1",
        channel_id="123",
        guild_id="456",
        author_id="99",
        author_name="Author",
        content="long text " * 20,
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
    )
    formatted = format_discord_message(message, channel)
    assert len(formatted.extra_messages) >= 1


def test_channel_mentions_converted_in_content() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="2",
        channel_id="123",
        guild_id="456",
        author_id="99",
        author_name="Author",
        content="See <#1234567890> for details",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
        timestamp="2024-01-02T03:04:05+00:00",
    )
    message.mention_channels = {"1234567890": "general"}

    formatted = format_discord_message(message, channel)

    assert formatted.parse_mode == "HTML"
    assert "#general" in formatted.text
    assert "See" in formatted.text


def test_discord_link_appended_when_enabled() -> None:
    channel = sample_channel()
    channel.formatting.show_discord_link = True
    message = DiscordMessage(
        id="2",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
        timestamp="2024-01-02T03:04:05+00:00",
    )

    formatted = format_discord_message(message, channel)

    assert "https://discord.com/channels/999/123/2" in formatted.text
    assert "Открыть в Discord" in formatted.text


def test_basic_markdown_translated_to_html() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="3",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="**Bold** __Underline__ ~~Strike~~ ||Spoiler||",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
        timestamp="2024-01-02T03:04:05+00:00",
    )

    formatted = format_discord_message(message, channel)

    assert "<b>Bold</b>" in formatted.text
    assert "<u>Underline</u>" in formatted.text
    assert "<s>Strike</s>" in formatted.text
    assert "<tg-spoiler>Spoiler</tg-spoiler>" in formatted.text


def test_pinned_header_icon() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="5",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="Pinned text",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
        timestamp="2024-01-02T03:04:05+00:00",
    )

    formatted = format_discord_message(message, channel, message_kind="pinned")

    assert "📌 <b>Закреплённое сообщение</b>" in formatted.text


def test_markdown_escapes_are_removed() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="6",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="\\#TOKEN2049Singapore takeaways\n1\\.\n2\\.",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
    )

    formatted = format_discord_message(message, channel)

    assert "\\#" not in formatted.text
    assert "#TOKEN2049Singapore takeaways" in formatted.text
    assert "1." in formatted.text
    assert "2." in formatted.text


def test_id_tags_and_timestamps_are_formatted() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="7",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="<id:guide> <id:customize> <t:1757318400:f>",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
    )

    formatted = format_discord_message(message, channel)

    assert "#guide" in formatted.text
    assert "#customize" in formatted.text
    assert "08.09.2025 11:00 MSK" in formatted.text


def test_mentions_display_names() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="4",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="Hi <@555> and role <@&777> in <#888>",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
        timestamp="2024-01-02T03:04:05+00:00",
    )
    message.mention_users = {"555": "UserName"}
    message.mention_roles = {"777": "Moderators"}
    message.mention_channels = {"888": "general"}

    formatted = format_discord_message(message, channel)

    assert "@UserName" in formatted.text
    assert "@Moderators" in formatted.text
    assert "#general" in formatted.text


def test_angle_bracket_links_are_unwrapped() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="5",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="Visit <https://example.com> for details",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
        timestamp="2024-01-02T03:04:05+00:00",
    )

    formatted = format_discord_message(message, channel)

    assert "<https://example.com>" not in formatted.text
    assert "https://example.com" in formatted.text


def test_numeric_hashtags_are_plain_text() -> None:
    channel = sample_channel()
    message = DiscordMessage(
        id="6",
        channel_id="123",
        guild_id="999",
        author_id="42",
        author_name="Author",
        content="Numbers #123456 and mix #token plus #987654.",
        attachments=(),
        embeds=(),
        stickers=(),
        role_ids=set(),
    )

    formatted = format_discord_message(message, channel)

    assert "#123456" in formatted.text
    assert "#987654" in formatted.text
    assert "https://t.me/s/hashtag?hashtag=123456" not in formatted.text
    assert "https://t.me/s/hashtag?hashtag=987654" not in formatted.text
    assert "#token" in formatted.text
