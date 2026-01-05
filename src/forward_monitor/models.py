"""Data models used across the forwarding service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence, Set


@dataclass(slots=True)
class FormattingOptions:
    """Configuration affecting Telegram output."""

    disable_preview: bool = True
    max_length: int = 3500
    ellipsis: str = "…"
    attachments_style: str = "summary"
    show_discord_link: bool = False


@dataclass(slots=True)
class FilterConfig:
    """Allow and deny lists for Discord content."""

    whitelist: set[str] = field(default_factory=set)
    blacklist: set[str] = field(default_factory=set)
    allowed_senders: set[str] = field(default_factory=set)
    blocked_senders: set[str] = field(default_factory=set)
    allowed_types: set[str] = field(default_factory=set)
    blocked_types: set[str] = field(default_factory=set)
    allowed_roles: set[str] = field(default_factory=set)
    blocked_roles: set[str] = field(default_factory=set)

    def merge(self, other: "FilterConfig") -> "FilterConfig":
        return FilterConfig(
            whitelist=self.whitelist | other.whitelist,
            blacklist=self.blacklist | other.blacklist,
            allowed_senders=self.allowed_senders | other.allowed_senders,
            blocked_senders=self.blocked_senders | other.blocked_senders,
            allowed_types=self.allowed_types | other.allowed_types,
            blocked_types=self.blocked_types | other.blocked_types,
            allowed_roles=self.allowed_roles | other.allowed_roles,
            blocked_roles=self.blocked_roles | other.blocked_roles,
        )


@dataclass(slots=True)
class ChannelConfig:
    """Runtime representation of a Discord → Telegram mapping."""

    discord_id: str
    telegram_chat_id: str
    telegram_thread_id: int | None
    label: str
    formatting: FormattingOptions
    filters: FilterConfig
    deduplicate_messages: bool = False
    deduplicate_inherited: bool = True
    last_message_id: str | None = None
    active: bool = True
    storage_id: int | None = None
    added_at: datetime | None = None
    pinned_only: bool = False
    known_pinned_ids: Set[str] = field(default_factory=set)
    pinned_synced: bool = False
    health_status: str = "unknown"
    health_message: str | None = None
    blocked_by_health: bool = False
    is_forum: bool = False

    def with_updates(
        self,
        *,
        formatting: FormattingOptions | None = None,
        filters: FilterConfig | None = None,
        last_message_id: str | None = None,
        added_at: datetime | None = None,
    ) -> "ChannelConfig":
        return ChannelConfig(
            discord_id=self.discord_id,
            telegram_chat_id=self.telegram_chat_id,
            telegram_thread_id=self.telegram_thread_id,
            label=self.label,
            formatting=formatting or self.formatting,
            filters=filters or self.filters,
            deduplicate_messages=self.deduplicate_messages,
            deduplicate_inherited=self.deduplicate_inherited,
            last_message_id=(
                last_message_id if last_message_id is not None else self.last_message_id
            ),
            active=self.active,
            storage_id=self.storage_id,
            added_at=added_at if added_at is not None else self.added_at,
            pinned_only=self.pinned_only,
            known_pinned_ids=set(self.known_pinned_ids),
            pinned_synced=self.pinned_synced,
            health_status=self.health_status,
            health_message=self.health_message,
            blocked_by_health=self.blocked_by_health,
            is_forum=self.is_forum,
        )


@dataclass(slots=True)
class RuntimeOptions:
    """Tunable behaviour of the monitor loop."""

    poll_interval: float = 2.0
    min_delay_seconds: float = 0.0
    max_delay_seconds: float = 0.0
    rate_per_second: float = 8.0
    healthcheck_interval: float = 180.0
    deduplicate_messages: bool = False


@dataclass(slots=True)
class NetworkOptions:
    """Proxy and client identity overrides."""

    discord_proxy_url: str | None = None
    discord_proxy_login: str | None = None
    discord_proxy_password: str | None = None
    discord_user_agent: str | None = None


@dataclass(slots=True)
class DiscordMessage:
    """Subset of the Discord payload used by the forwarder."""

    id: str
    channel_id: str
    guild_id: str | None
    author_id: str
    author_name: str
    content: str
    attachments: Sequence[Mapping[str, Any]]
    embeds: Sequence[Mapping[str, Any]]
    stickers: Sequence[Mapping[str, Any]]
    role_ids: Set[str]
    mention_users: Mapping[str, str] = field(default_factory=dict)
    mention_roles: Mapping[str, str] = field(default_factory=dict)
    mention_channels: Mapping[str, str] = field(default_factory=dict)
    timestamp: str | None = None
    edited_timestamp: str | None = None
    message_type: int = 0


@dataclass(slots=True)
class FormattedTelegramMessage:
    """Outgoing Telegram payload produced by the formatter."""

    text: str
    extra_messages: Sequence[str]
    parse_mode: str | None
    disable_preview: bool
    image_urls: Sequence[str] = ()
