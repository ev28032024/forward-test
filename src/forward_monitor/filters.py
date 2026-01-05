"""Filtering rules applied before forwarding messages."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from .models import DiscordMessage, FilterConfig
from .utils import normalize_username


@dataclass(slots=True)
class FilterDecision:
    """Result of evaluating filters."""

    allowed: bool
    reason: str | None = None


class FilterEngine:
    """Apply allow/deny filters to Discord messages."""

    def __init__(self, config: FilterConfig):
        self._config = config
        (
            self._allowed_sender_ids,
            self._allowed_sender_names,
        ) = _split_sender_values(config.allowed_senders)
        (
            self._blocked_sender_ids,
            self._blocked_sender_names,
        ) = _split_sender_values(config.blocked_senders)
        self._allowed_roles = {role.strip() for role in config.allowed_roles if role.strip()}
        self._blocked_roles = {role.strip() for role in config.blocked_roles if role.strip()}

    def evaluate(self, message: DiscordMessage) -> FilterDecision:
        content = message.content or ""
        lowered = content.lower()
        author_id = message.author_id.strip()
        author_name = normalize_username(message.author_name)

        if message.stickers:
            return FilterDecision(False, "sticker_blocked")

        if (self._allowed_sender_ids or self._allowed_sender_names) and not (
            author_id in self._allowed_sender_ids
            or (author_name is not None and author_name in self._allowed_sender_names)
        ):
            return FilterDecision(False, "sender_not_allowed")
        if author_id in self._blocked_sender_ids:
            return FilterDecision(False, "sender_blocked")
        if author_name and author_name in self._blocked_sender_names:
            return FilterDecision(False, "sender_blocked")

        if self._allowed_roles and not (message.role_ids & self._allowed_roles):
            return FilterDecision(False, "role_not_allowed")
        if message.role_ids & self._blocked_roles:
            return FilterDecision(False, "role_blocked")

        if self._config.whitelist:
            if not any(token in lowered for token in _normalise_tokens(self._config.whitelist)):
                return FilterDecision(False, "whitelist_miss")
        if any(token in lowered for token in _normalise_tokens(self._config.blacklist)):
            return FilterDecision(False, "blacklist_hit")

        message_types = set(_infer_types(message))
        if self._config.allowed_types and not (message_types & self._config.allowed_types):
            return FilterDecision(False, "type_not_allowed")
        if self._config.blocked_types and (message_types & self._config.blocked_types):
            return FilterDecision(False, "type_blocked")

        return FilterDecision(True)


def _infer_types(message: DiscordMessage) -> Iterable[str]:
    if message.content:
        yield "text"

    if message.stickers:
        yield "sticker"

    for attachment in message.attachments:
        content_type = str(attachment.get("content_type") or "").lower()
        filename = str(attachment.get("filename") or "").lower()
        if any(filename.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp")):
            yield "image"
        elif any(filename.endswith(ext) for ext in (".mp4", ".mov", ".mkv", ".webm")):
            yield "video"
        elif any(filename.endswith(ext) for ext in (".mp3", ".ogg", ".wav", ".flac")):
            yield "audio"
        elif content_type.startswith("image/"):
            yield "image"
        elif content_type.startswith("video/"):
            yield "video"
        elif content_type.startswith("audio/"):
            yield "audio"
        else:
            yield "attachment"

    if message.embeds:
        yield "embed"

    if not message.content and not message.attachments and not message.embeds:
        yield "empty"


def _normalise_tokens(tokens: Iterable[str]) -> set[str]:
    cleaned: set[str] = set()
    for token in tokens:
        text = token.strip().lower()
        if text:
            cleaned.add(text)
    return cleaned


_WORD_RE = re.compile(r"\w+")


def tokenise(text: str) -> set[str]:
    """Public helper used in tests to inspect tokenisation."""

    return {match.group(0).lower() for match in _WORD_RE.finditer(text)}


def _split_sender_values(values: Iterable[str]) -> tuple[set[str], set[str]]:
    ids: set[str] = set()
    names: set[str] = set()
    for entry in values:
        text = entry.strip()
        if not text:
            continue
        if text.lstrip("-").isdigit():
            ids.add(str(int(text)))
            continue
        normalized = normalize_username(text) or text.strip().lower()
        names.add(normalized)
    return ids, names
