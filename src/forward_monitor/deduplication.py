"""Helpers for detecting duplicate Discord messages."""
from __future__ import annotations

from collections import deque
from typing import Any, Mapping

from .models import DiscordMessage


class MessageDeduplicator:
    """Track recently forwarded messages to skip duplicates."""

    def __init__(self, capacity: int = 512) -> None:
        self._capacity = max(1, int(capacity))
        self._order: deque[str] = deque()
        self._known: set[str] = set()

    def is_duplicate(self, signature: str | None) -> bool:
        """Return True if the signature was seen recently, recording new ones."""

        if not signature:
            return False
        if signature in self._known:
            return True
        self._known.add(signature)
        self._order.append(signature)
        if len(self._order) > self._capacity:
            removed = self._order.popleft()
            self._known.discard(removed)
        return False


def build_message_signature(message: DiscordMessage) -> str | None:
    """Create a normalized signature for a Discord message content payload."""

    content = (message.content or "").strip()
    attachment_tokens = sorted(
        token
        for token in (
            _attachment_token(attachment) for attachment in message.attachments
        )
        if token
    )
    embed_tokens = sorted(
        token for token in (_embed_token(embed) for embed in message.embeds) if token
    )

    if not content and not attachment_tokens and not embed_tokens:
        return None

    parts: list[str] = []
    if content:
        parts.append(content)
    if attachment_tokens:
        parts.append("attachments:" + "|".join(attachment_tokens))
    if embed_tokens:
        parts.append("embeds:" + "|".join(embed_tokens))
    return "\n".join(parts)


def _attachment_token(attachment: Mapping[str, Any]) -> str | None:
    url = str(attachment.get("url") or attachment.get("proxy_url") or "").strip()
    filename = str(attachment.get("filename") or "").strip()
    if not url and not filename:
        return None
    return f"{filename}|{url}"


def _embed_token(embed: Mapping[str, Any]) -> str | None:
    title = str(embed.get("title") or "").strip()
    description = str(embed.get("description") or "").strip()
    combined = "\n".join(part for part in (title, description) if part).strip()
    return combined or None
