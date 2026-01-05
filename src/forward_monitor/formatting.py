"""Telegram formatting helpers."""

from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse

from .models import ChannelConfig, DiscordMessage, FormattedTelegramMessage
from .utils import as_moscow_time

EmbedPayload = Mapping[str, Any]
AttachmentPayload = Mapping[str, Any]


_MESSAGE_KIND_ICONS = {"message": "💬", "pinned": "📌"}
_MESSAGE_KIND_LABELS = {
    "message": "Новое сообщение",
    "pinned": "Закреплённое сообщение",
}
_CHANNEL_ICON = "📣"
_MESSAGE_SEPARATOR = "<b>────── ✦ ──────</b>"


def format_discord_message(
    message: DiscordMessage,
    channel: ChannelConfig,
    *,
    message_kind: str = "message",
) -> FormattedTelegramMessage:
    """Convert a Discord message into Telegram text respecting the channel profile."""

    formatting = channel.formatting
    content = _sanitize_content(message.content or "", message)
    embed_blocks = list(_clean_embed_text(message.embeds, message))
    image_urls, file_attachments = _split_attachments(message.attachments)
    attachments_block = _render_attachments_block(
        file_attachments, formatting.attachments_style
    )
    link_block = _build_link_block(message, formatting.show_discord_link)

    inner_blocks: list[str] = []
    header = _build_header(channel.label, message.author_name, message_kind)
    if header:
        inner_blocks.append(header)

    if content:
        inner_blocks.append(_format_text_block(content))
    for embed in embed_blocks:
        inner_blocks.append(_format_text_block(embed))
    if attachments_block:
        inner_blocks.append(attachments_block)
    if link_block:
        inner_blocks.append(link_block)

    date_line = _format_timestamp_line(message)
    if date_line:
        inner_blocks.append(date_line)

    decorated_blocks: list[str] = []
    separator = _MESSAGE_SEPARATOR
    decorated_blocks.append(separator)
    decorated_blocks.extend(block for block in inner_blocks if block)
    decorated_blocks.append(separator)

    combined = "\n\n".join(block for block in decorated_blocks if block)
    chunks = _chunk_html_text(combined, formatting.max_length, formatting.ellipsis)

    return FormattedTelegramMessage(
        text=chunks[0] if chunks else "",
        extra_messages=tuple(chunks[1:]),
        parse_mode="HTML",
        disable_preview=formatting.disable_preview,
        image_urls=image_urls,
    )


def _normalize_message_kind(kind: str) -> str:
    if not kind:
        return "message"
    lowered = kind.lower()
    return lowered if lowered in _MESSAGE_KIND_ICONS else "message"


def _build_header(label: str, author: str, kind: str) -> str:
    kind_key = _normalize_message_kind(kind)
    parts: list[str] = []
    if label:
        parts.append(f"{_CHANNEL_ICON} <b>{_escape(label)}</b>")
    icon = _MESSAGE_KIND_ICONS.get(kind_key, "💬")
    kind_label = _MESSAGE_KIND_LABELS.get(kind_key, "Новое сообщение")
    parts.append(f"{icon} <b>{_escape(kind_label)}</b>")
    if author:
        parts.append(f"👤 <b>{_escape(author)}</b>")
    return "\n".join(parts)


def _format_timestamp_line(message: DiscordMessage) -> str:
    moment = _parse_timestamp(message.edited_timestamp or message.timestamp)
    if moment is None:
        moment = datetime.now(timezone.utc)
    formatted = as_moscow_time(moment).strftime("%d.%m.%Y %H:%M %Z")
    return f"📅 <b>{_escape(formatted)}</b>"


def _clean_embed_text(
    embeds: Sequence[EmbedPayload], message: DiscordMessage | None = None
) -> Iterable[str]:
    for embed in embeds:
        title = str(embed.get("title") or "").strip()
        description = str(embed.get("description") or "").strip()
        url = str(embed.get("url") or "").strip()
        fields_text = []
        for field in embed.get("fields") or []:
            name = str(field.get("name") or "").strip()
            value = str(field.get("value") or "").strip()
            if name and value:
                fields_text.append(f"{name}: {value}")
            elif value:
                fields_text.append(value)
        segments = [
            segment for segment in (title, description, "\n".join(fields_text), url) if segment
        ]
        if segments:
            cleaned_segments = [
                part
                for part in (
                    _sanitize_content(segment, message) for segment in segments if segment
                )
                if part
            ]
            if cleaned_segments:
                yield "\n".join(cleaned_segments)


def _summarise_attachments(
    attachments: Sequence[AttachmentPayload],
    style: str,
) -> Iterable[str]:
    if not attachments:
        return []

    style = style.lower()
    if style not in {"summary", "links"}:
        style = "summary"

    lines: list[str] = []
    for index, attachment in enumerate(attachments, start=1):
        url = str(attachment.get("url") or attachment.get("proxy_url") or "").strip()
        if not url:
            continue
        filename = str(attachment.get("filename") or "").strip()
        content_type = str(attachment.get("content_type") or "").strip()
        size = attachment.get("size")
        size_text = _human_size(size) if isinstance(size, (int, float)) else ""
        domain = urlparse(url).netloc

        if style == "links":
            label = filename or domain or f"Attachment {index}"
            lines.append(f"{label}: {url}")
        else:
            parts = [part for part in (filename, content_type, size_text, url) if part]
            lines.append(" • ".join(parts))
    if lines:
        header = "Вложения" if style == "summary" else "Ссылки на вложения"
        return [header, *lines]
    return []


def _render_attachments_block(
    attachments: Sequence[AttachmentPayload], style: str
) -> str:
    summary = list(_summarise_attachments(attachments, style))
    if not summary:
        return ""
    header_text = summary[0].rstrip(":")
    icon = "🔗" if style.lower() == "links" else "📎"
    header = f"{icon} <b>{_escape(header_text)}</b>"
    lines = [f"• {_escape(line)}" for line in summary[1:]]
    if lines:
        return "\n".join([header, *lines])
    return header


def _split_attachments(
    attachments: Sequence[AttachmentPayload],
) -> tuple[tuple[str, ...], tuple[AttachmentPayload, ...]]:
    images: list[str] = []
    others: list[AttachmentPayload] = []
    for attachment in attachments:
        url = str(attachment.get("url") or attachment.get("proxy_url") or "").strip()
        if not url:
            continue
        if _is_image_attachment(attachment):
            images.append(url)
        else:
            others.append(attachment)
    return tuple(images), tuple(others)


def _is_image_attachment(attachment: AttachmentPayload) -> bool:
    filename = str(attachment.get("filename") or "").lower()
    content_type = str(attachment.get("content_type") or "").lower()
    if any(filename.endswith(ext) for ext in _IMAGE_EXTENSIONS):
        return True
    if content_type.startswith("image/"):
        subtype = content_type.split("/", 1)[1] if "/" in content_type else ""
        return not subtype.startswith("gifv")
    return False


def _human_size(value: float | int | None) -> str:
    if value is None:
        return ""
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    return f"{size:.1f}{units[index]}"


def _build_link_block(message: DiscordMessage, enabled: bool) -> str:
    if not enabled:
        return ""
    message_id = message.id.strip()
    channel_id = message.channel_id.strip()
    if not message_id or not channel_id:
        return ""
    guild_id = message.guild_id.strip() if message.guild_id else "@me"
    link = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    return f"🔗 <a href=\"{_escape(link)}\">Открыть в Discord</a>"


def _chunk_html_text(text: str, limit: int, ellipsis: str) -> list[str]:
    if not text:
        return []
    if limit <= 0 or len(text) <= limit:
        return [text]

    remaining = text
    chunks: list[str] = []
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        split = remaining.rfind("\n", 0, limit)
        if split == -1 or split < limit // 4:
            split = remaining.rfind(" ", 0, limit)
        if split == -1 or split < limit // 4:
            split = limit

        chunk = remaining[:split].rstrip()
        tail = remaining[split:].lstrip("\n")
        if chunk:
            if tail:
                chunk = f"{chunk}{ellipsis}"
            chunks.append(chunk)
        else:
            chunk = remaining[:limit]
            chunk = f"{chunk}{ellipsis}" if len(remaining) > limit else chunk
            chunks.append(chunk)
            tail = remaining[limit:]
        remaining = tail.lstrip()
    return chunks


def _escape(text: str) -> str:
    return html.escape(text, quote=False)


def _format_text_block(text: str) -> str:
    return _apply_basic_markdown(text)


_USER_MENTION_RE = re.compile(r"<@!?([0-9]+)>")
_ROLE_MENTION_RE = re.compile(r"<@&([0-9]+)>")
_CHANNEL_MENTION_RE = re.compile(r"<#([0-9]+)>")
_CUSTOM_EMOJI_RE = re.compile(r"<a?:[a-zA-Z0-9_~]+:[0-9]+>")
_EXTRA_SPACE_RE = re.compile(r"[ \t]{2,}")
_TRIPLE_NEWLINES_RE = re.compile(r"\n{3,}")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_ANGLE_LINK_RE = re.compile(r"<(https?://[^\s<>]+)>")
_GENERIC_ID_TAG_RE = re.compile(r"<id:([a-zA-Z0-9_\-]+)>")
_TIMESTAMP_TAG_RE = re.compile(r"<t:([-?0-9]+)(?::([a-zA-Z]))?>")
_ESCAPED_MARKDOWN_RE = re.compile(r"\\([\\*_`~|.!?#\-])")
_ALLOWED_LINK_SCHEMES = {"http", "https"}


def _sanitize_content(text: str, message: DiscordMessage | None = None) -> str:
    if not text:
        return ""
    cleaned = text
    if message is not None:
        cleaned = _USER_MENTION_RE.sub(
            lambda match: _format_user_mention(match.group(1), message), cleaned
        )
        cleaned = _ROLE_MENTION_RE.sub(
            lambda match: _format_role_mention(match.group(1), message), cleaned
        )
        cleaned = _CHANNEL_MENTION_RE.sub(
            lambda match: _format_channel_mention(match.group(1), message), cleaned
        )
    else:
        cleaned = _CHANNEL_MENTION_RE.sub(lambda match: f"#{match.group(1)}", cleaned)
    cleaned = _ANGLE_LINK_RE.sub(r"\1", cleaned)
    cleaned = _CUSTOM_EMOJI_RE.sub("", cleaned)
    cleaned = _GENERIC_ID_TAG_RE.sub(
        lambda match: _format_generic_id_tag(match.group(1)), cleaned
    )
    cleaned = _TIMESTAMP_TAG_RE.sub(_format_timestamp_tag, cleaned)
    cleaned = _ESCAPED_MARKDOWN_RE.sub(r"\1", cleaned)
    cleaned = _EXTRA_SPACE_RE.sub(" ", cleaned)
    cleaned = _TRIPLE_NEWLINES_RE.sub("\n\n", cleaned)
    return cleaned.strip()


def _format_user_mention(user_id: str, message: DiscordMessage) -> str:
    display = (message.mention_users.get(user_id) or "").strip()
    if display:
        return f"@{display}"
    return f"@{user_id}"


def _format_role_mention(role_id: str, message: DiscordMessage) -> str:
    display = (message.mention_roles.get(role_id) or "").strip()
    if display:
        return f"@{display}"
    return f"@{role_id}"


def _format_channel_mention(channel_id: str, message: DiscordMessage) -> str:
    display = (message.mention_channels.get(channel_id) or "").strip()
    if display:
        return f"#{display}"
    return f"#{channel_id}"


def _apply_basic_markdown(text: str) -> str:
    placeholders: dict[str, str] = {}

    def _store(value: str) -> str:
        token = f"§§FMPLACEHOLDER_{len(placeholders)}§§"
        placeholders[token] = value
        return token

    def _format_code_block(match: re.Match[str]) -> str:
        content = match.group(1).strip("\n")
        escaped = html.escape(content, quote=False)
        return _store(f"<pre><code>{escaped}</code></pre>")

    def _format_inline_code(match: re.Match[str]) -> str:
        content = match.group(1)
        escaped = html.escape(content, quote=False)
        return _store(f"<code>{escaped}</code>")

    text_without_code = _CODE_BLOCK_RE.sub(_format_code_block, text)
    text_without_code = _CODE_SPAN_RE.sub(_format_inline_code, text_without_code)

    def _format_link(match: re.Match[str]) -> str:
        label = match.group(1)
        url = match.group(2).strip()
        if not _is_allowed_link(url):
            return match.group(0)
        safe_url = html.escape(url, quote=True)
        safe_label = html.escape(label, quote=False)
        return _store(f"<a href=\"{safe_url}\">{safe_label}</a>")

    text_with_links = _LINK_RE.sub(_format_link, text_without_code)

    escaped = html.escape(text_with_links, quote=False)

    escaped = _BOLD_RE.sub(lambda m: f"<b>{m.group(1)}</b>", escaped)
    escaped = _UNDERLINE_RE.sub(lambda m: f"<u>{m.group(1)}</u>", escaped)
    escaped = _STRIKE_RE.sub(lambda m: f"<s>{m.group(1)}</s>", escaped)
    escaped = _SPOILER_RE.sub(lambda m: f"<tg-spoiler>{m.group(1)}</tg-spoiler>", escaped)
    escaped = _NUMERIC_HASHTAG_RE.sub(_format_numeric_hashtag, escaped)

    result = escaped
    for token, value in placeholders.items():
        result = result.replace(token, value)
    return result


def _format_generic_id_tag(raw: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_\-]+", "", raw)
    if not cleaned:
        return ""
    return f"#{cleaned}"


def _format_numeric_hashtag(match: re.Match[str]) -> str:
    value = match.group(1)
    if not value:
        return match.group(0)
    return f"#{value}"


def _format_timestamp_tag(match: re.Match[str]) -> str:
    raw_value = match.group(1)
    style = (match.group(2) or "f").lower()
    try:
        timestamp = int(raw_value)
    except ValueError:
        return match.group(0)
    try:
        moment = datetime.fromtimestamp(timestamp, timezone.utc)
    except (OverflowError, OSError):
        return match.group(0)

    local = as_moscow_time(moment)

    if style in _DISCORD_TIMESTAMP_FORMATS:
        return local.strftime(_DISCORD_TIMESTAMP_FORMATS[style])
    if style == "r":
        return _format_relative_time(moment)
    return local.strftime("%d.%m.%Y %H:%M %Z")


def _format_relative_time(target: datetime) -> str:
    now = datetime.now(timezone.utc)
    delta = target - now
    seconds = int(delta.total_seconds())
    remaining = abs(seconds)
    units: list[tuple[int, str]] = [
        (31536000, "г."),
        (2592000, "мес."),
        (604800, "нед."),
        (86400, "д."),
        (3600, "ч."),
        (60, "мин."),
        (1, "сек."),
    ]
    value = 0
    label = "сек."
    for unit_seconds, unit_label in units:
        if remaining >= unit_seconds:
            value = max(1, remaining // unit_seconds)
            label = unit_label
            break
    else:
        value = 0
        label = "сек."

    if value == 0:
        return "прямо сейчас"

    if seconds < 0:
        return f"{value} {label} назад"
    return f"через {value} {label}"


_DISCORD_TIMESTAMP_FORMATS = {
    "t": "%H:%M",
    "T": "%H:%M:%S",
    "d": "%d.%m.%Y",
    "D": "%d %B %Y",
    "f": "%d.%m.%Y %H:%M %Z",
    "F": "%d %B %Y %H:%M %Z",
}


_CODE_BLOCK_RE = re.compile(r"```(.*?)```", re.DOTALL)
_CODE_SPAN_RE = re.compile(r"`([^`]+?)`")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_UNDERLINE_RE = re.compile(r"__(.+?)__", re.DOTALL)
_STRIKE_RE = re.compile(r"~~(.+?)~~", re.DOTALL)
_SPOILER_RE = re.compile(r"\|\|(.+?)\|\|", re.DOTALL)
_NUMERIC_HASHTAG_RE = re.compile(
    r"(?:(?<=^)|(?<=[^0-9A-Za-z_/:]))#([0-9]{1,64})(?![0-9A-Za-z_])"
)

_IMAGE_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".bmp",
    ".tiff",
    ".svg",
)


def _is_allowed_link(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if not parsed.scheme or parsed.scheme.lower() not in _ALLOWED_LINK_SCHEMES:
        return False
    return bool(parsed.netloc)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
