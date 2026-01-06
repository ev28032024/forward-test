"""SQLite backed storage for Forward Monitor configuration."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence
from urllib.parse import urlsplit, urlunsplit

from .models import ChannelConfig, FilterConfig, FormattingOptions, NetworkOptions
from .utils import normalize_username, parse_bool

_DB_PRAGMA = "PRAGMA journal_mode=WAL;" "PRAGMA synchronous=NORMAL;" "PRAGMA foreign_keys=ON;"


@dataclass(slots=True)
class AdminRecord:
    """Admin identity stored in the configuration."""

    user_id: int | None
    username: str | None


@dataclass(slots=True)
class ChannelRecord:
    """Raw channel record loaded from SQLite."""

    id: int
    discord_id: str
    telegram_chat_id: str
    telegram_thread_id: int | None
    label: str
    active: bool
    last_message_id: str | None
    added_at: datetime | None


@dataclass(slots=True)
class ManualForwardEntry:
    """Single channel outcome recorded for manual forwarding."""

    discord_id: str
    label: str
    forwarded: int
    mode: str
    note: str


@dataclass(slots=True)
class ManualForwardActivity:
    """Stored metadata about the latest /send_recent invocation."""

    timestamp: datetime
    requested: int
    limit: int
    total_forwarded: int
    entries: list[ManualForwardEntry]


class ConfigStore:
    """Persisted settings and channel mappings."""

    def __init__(self, path: Path):
        self._path = path
        self._conn = sqlite3.connect(self._path)
        self._conn.row_factory = sqlite3.Row
        self._setup()

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------
    def _setup(self) -> None:
        with closing(self._conn.cursor()) as cur:
            for statement in _DB_PRAGMA.split(";"):
                if statement.strip():
                    cur.execute(statement)
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    username TEXT UNIQUE COLLATE NOCASE
                );

                CREATE TABLE IF NOT EXISTS user_profiles (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE COLLATE NOCASE
                );

                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_id TEXT NOT NULL UNIQUE,
                    telegram_chat_id TEXT NOT NULL,
                    telegram_thread_id TEXT,
                    label TEXT DEFAULT '',
                    active INTEGER DEFAULT 1,
                    last_message_id TEXT,
                    added_at TEXT
                );

                CREATE TABLE IF NOT EXISTS channel_options (
                    channel_id INTEGER NOT NULL,
                    option_key TEXT NOT NULL,
                    option_value TEXT NOT NULL,
                    PRIMARY KEY (channel_id, option_key)
                );

                CREATE TABLE IF NOT EXISTS filters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER NOT NULL,
                    filter_type TEXT NOT NULL,
                    value TEXT NOT NULL,
                    UNIQUE(channel_id, filter_type, value)
                );

                """
            )
            self._migrate_admins(cur)
            self._migrate_channels(cur)
            self._conn.commit()

    def _migrate_admins(self, cur: sqlite3.Cursor) -> None:
        cur.execute("PRAGMA table_info(admins)")
        columns = {str(row[1]) for row in cur.fetchall()}
        expected = {"id", "user_id", "username"}
        if columns == expected:
            return
        if columns == {"user_id"}:
            cur.executescript(
                """
                ALTER TABLE admins RENAME TO admins_legacy;
                CREATE TABLE admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    username TEXT UNIQUE COLLATE NOCASE
                );
                INSERT INTO admins(user_id)
                SELECT user_id FROM admins_legacy;
                DROP TABLE admins_legacy;
                """
            )
            return
        if not columns:
            cur.executescript(
                """
                DROP TABLE IF EXISTS admins;
                CREATE TABLE admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    username TEXT UNIQUE COLLATE NOCASE
                );
                """
            )
            return
        raise RuntimeError("Unsupported admins schema detected")

    def _migrate_channels(self, cur: sqlite3.Cursor) -> None:
        cur.execute("PRAGMA table_info(channels)")
        columns = {str(row[1]) for row in cur.fetchall()}
        if not columns:
            return
        if "telegram_thread_id" not in columns:
            cur.execute(
                "ALTER TABLE channels ADD COLUMN telegram_thread_id TEXT"
            )
            columns.add("telegram_thread_id")
        if "added_at" not in columns:
            cur.execute("ALTER TABLE channels ADD COLUMN added_at TEXT")
            columns.add("added_at")
        if "added_at" in columns:
            cur.execute(
                "SELECT COUNT(*) FROM channels WHERE added_at IS NULL OR added_at=''"
            )
            row = cur.fetchone()
            if row and int(row[0]) > 0:
                timestamp = datetime.now(timezone.utc).isoformat()
                cur.execute(
                    "UPDATE channels SET added_at=? WHERE added_at IS NULL OR added_at=''",
                    (timestamp,),
                )

    # ------------------------------------------------------------------
    # Basic settings
    # ------------------------------------------------------------------
    def set_setting(self, key: str, value: str) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT value FROM settings WHERE key=?", (key,))
            row = cur.fetchone()
        return row["value"] if row else default

    def delete_setting(self, key: str) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM settings WHERE key=?", (key,))
            self._conn.commit()

    def set_telegram_offset(self, offset: int) -> None:
        safe_value = max(0, int(offset))
        self.set_setting("state.telegram.offset", str(safe_value))

    def get_telegram_offset(self) -> int | None:
        value = self.get_setting("state.telegram.offset")
        if value is None:
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return max(0, parsed)

    def clear_telegram_offset(self) -> None:
        self.delete_setting("state.telegram.offset")

    def iter_settings(self, prefix: str | None = None) -> Iterator[tuple[str, str]]:
        query = "SELECT key, value FROM settings"
        params: tuple[str, ...] = ()
        if prefix:
            query += " WHERE key LIKE ?"
            params = (f"{prefix}%",)
        with closing(self._conn.cursor()) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        for row in rows:
            yield str(row["key"]), str(row["value"])

    # ------------------------------------------------------------------
    # Health status helpers
    # ------------------------------------------------------------------
    def set_health_status(self, subject: str, status: str, message: str | None) -> None:
        status_key = f"health.{subject}.status"
        message_key = f"health.{subject}.message"
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (status_key, status),
            )
            if message is None:
                cur.execute("DELETE FROM settings WHERE key=?", (message_key,))
            else:
                cur.execute(
                    "INSERT INTO settings(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (message_key, message),
                )
            self._conn.commit()

    def get_health_status(self, subject: str) -> tuple[str, str | None]:
        status = self.get_setting(f"health.{subject}.status") or "unknown"
        message = self.get_setting(f"health.{subject}.message")
        return status, message

    def clean_channel_health_statuses(self, channel_ids: Iterable[str]) -> None:
        base_keys = {f"health.channel.{channel_id}" for channel_id in channel_ids}
        to_remove: list[str] = []
        for key, _value in list(self.iter_settings("health.channel.")):
            prefix, sep, _ = key.partition(".status")
            if not sep:
                prefix, sep, _ = key.partition(".message")
            if not sep:
                prefix = key
            if prefix not in base_keys:
                to_remove.append(key)
        if not to_remove:
            return
        with closing(self._conn.cursor()) as cur:
            cur.executemany("DELETE FROM settings WHERE key=?", ((key,) for key in to_remove))
            self._conn.commit()

    # ------------------------------------------------------------------
    # Network options helpers
    # ------------------------------------------------------------------
    def load_network_options(self) -> NetworkOptions:
        proxy_url = self.get_setting("proxy.discord.url")
        proxy_login = self.get_setting("proxy.discord.login")
        proxy_password = self.get_setting("proxy.discord.password")

        legacy_proxy = None
        if not proxy_url:
            legacy_proxy = self.get_setting("proxy.discord")
            if legacy_proxy:
                parsed = urlsplit(legacy_proxy)
                if parsed.username or parsed.password:
                    proxy_login = proxy_login or parsed.username or None
                    proxy_password = proxy_password or parsed.password or None
                    host = parsed.hostname or ""
                    if parsed.port:
                        host = f"{host}:{parsed.port}"
                    components = (
                        parsed.scheme,
                        host,
                        parsed.path,
                        parsed.query,
                        parsed.fragment,
                    )
                    proxy_url = urlunsplit(components)
                else:
                    proxy_url = legacy_proxy

        if legacy_proxy and proxy_url and not self.get_setting("proxy.discord.url"):
            self.set_setting("proxy.discord.url", proxy_url)
            if proxy_login:
                self.set_setting("proxy.discord.login", proxy_login)
            if proxy_password:
                self.set_setting("proxy.discord.password", proxy_password)
            self.delete_setting("proxy.discord")

        return NetworkOptions(
            discord_proxy_url=proxy_url,
            discord_proxy_login=proxy_login,
            discord_proxy_password=proxy_password,
            discord_user_agent=self.get_setting("ua.discord"),
        )

    # ------------------------------------------------------------------
    # Admins
    # ------------------------------------------------------------------
    def list_admins(self) -> list[AdminRecord]:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "SELECT user_id, username FROM admins ORDER BY "
                "COALESCE(username, CAST(user_id AS TEXT)) COLLATE NOCASE"
            )
            rows = cur.fetchall()
        return [
            AdminRecord(
                user_id=int(row["user_id"]) if row["user_id"] is not None else None,
                username=str(row["username"]) if row["username"] else None,
            )
            for row in rows
        ]

    def add_admin(self, user_id: int | None = None, username: str | None = None) -> None:
        normalized = normalize_username(username)
        with closing(self._conn.cursor()) as cur:
            if user_id is not None:
                cur.execute(
                    "INSERT INTO admins(user_id, username) VALUES(?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username",
                    (int(user_id), normalized),
                )
            elif normalized is not None:
                cur.execute(
                    "INSERT INTO admins(username) VALUES(?) "
                    "ON CONFLICT(username) DO UPDATE SET username=excluded.username",
                    (normalized,),
                )
            else:
                raise ValueError("Either user_id or username must be provided")
            self._conn.commit()

    def remove_admin(self, identifier: int | str) -> bool:
        with closing(self._conn.cursor()) as cur:
            if isinstance(identifier, int):
                cur.execute("DELETE FROM admins WHERE user_id=?", (int(identifier),))
            else:
                normalized = normalize_username(identifier)
                cur.execute(
                    "DELETE FROM admins WHERE username=? COLLATE NOCASE",
                    (normalized,),
                )
            deleted = cur.rowcount > 0
            self._conn.commit()
        return deleted

    def has_admins(self) -> bool:
        with closing(self._conn.cursor()) as cur:
            cur.execute("SELECT 1 FROM admins LIMIT 1")
            row = cur.fetchone()
        return bool(row)

    def remember_user(self, user_id: int, username: str | None) -> None:
        normalized = normalize_username(username)
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO user_profiles(user_id, username) VALUES(?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username",
                (int(user_id), normalized),
            )
            if normalized is not None:
                cur.execute(
                    "INSERT INTO user_profiles(user_id, username) VALUES(?, ?) "
                    "ON CONFLICT(username) DO UPDATE SET user_id=excluded.user_id, "
                    "username=excluded.username",
                    (int(user_id), normalized),
                )
                cur.execute(
                    "UPDATE admins SET user_id=COALESCE(user_id, ?), username=? "
                    "WHERE username=? COLLATE NOCASE",
                    (int(user_id), normalized, normalized),
                )
                cur.execute(
                    "UPDATE admins SET username=? WHERE user_id=?",
                    (normalized, int(user_id)),
                )
            self._conn.commit()

    def resolve_user_id(self, username: str) -> int | None:
        normalized = normalize_username(username)
        if normalized is None:
            return None
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "SELECT user_id FROM user_profiles WHERE username=? COLLATE NOCASE",
                (normalized,),
            )
            row = cur.fetchone()
        return int(row["user_id"]) if row and row["user_id"] is not None else None

    # ------------------------------------------------------------------
    # Channels
    # ------------------------------------------------------------------
    def add_channel(
        self,
        discord_id: str,
        telegram_chat_id: str,
        label: str = "",
        *,
        telegram_thread_id: int | None = None,
        last_message_id: str | None = None,
        added_at: datetime | None = None,
    ) -> ChannelRecord:
        timestamp = added_at or datetime.now(timezone.utc)
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                (
                    "INSERT INTO channels("
                    "discord_id, telegram_chat_id, label, telegram_thread_id, "
                    "last_message_id, added_at"
                    ") VALUES(?, ?, ?, ?, ?, ?)"
                ),
                (
                    discord_id,
                    telegram_chat_id,
                    label,
                    str(int(telegram_thread_id)) if telegram_thread_id is not None else None,
                    last_message_id,
                    timestamp.isoformat(),
                ),
            )
            channel_id_raw = cur.lastrowid
            if channel_id_raw is None:
                raise RuntimeError("Failed to insert channel record")
            channel_id = int(channel_id_raw)
            self._conn.commit()
        # Ensure pinned state is initialised lazily to avoid replaying all pins.
        self.set_channel_option(channel_id, "state.pinned_synced", "false")
        return ChannelRecord(
            id=channel_id,
            discord_id=discord_id,
            telegram_chat_id=telegram_chat_id,
            telegram_thread_id=telegram_thread_id,
            label=label,
            active=True,
            last_message_id=last_message_id,
            added_at=timestamp,
        )

    def remove_channel(self, discord_id: str) -> bool:
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM channels WHERE discord_id=?", (discord_id,))
            deleted = cur.rowcount > 0
            self._conn.commit()
        return deleted

    def list_channels(self) -> list[ChannelRecord]:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                (
                    "SELECT id, discord_id, telegram_chat_id, telegram_thread_id, "
                    "label, active, last_message_id, added_at FROM channels ORDER BY discord_id"
                )
            )
            rows = cur.fetchall()
        return [
            ChannelRecord(
                id=int(row["id"]),
                discord_id=str(row["discord_id"]),
                telegram_chat_id=str(row["telegram_chat_id"]),
                telegram_thread_id=_parse_thread_id(row["telegram_thread_id"]),
                label=str(row["label"] or ""),
                active=bool(row["active"]),
                last_message_id=str(row["last_message_id"]) if row["last_message_id"] else None,
                added_at=_parse_timestamp(row["added_at"]),
            )
            for row in rows
        ]

    def get_channel(self, discord_id: str) -> ChannelRecord | None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                (
                    "SELECT id, discord_id, telegram_chat_id, telegram_thread_id, "
                    "label, active, last_message_id, added_at FROM channels WHERE discord_id=?"
                ),
                (discord_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        record_id = int(row["id"])
        ensured_added_at = self._ensure_channel_added_at(
            record_id, _parse_timestamp(row["added_at"])
        )
        return ChannelRecord(
            id=record_id,
            discord_id=str(row["discord_id"]),
            telegram_chat_id=str(row["telegram_chat_id"]),
            telegram_thread_id=_parse_thread_id(row["telegram_thread_id"]),
            label=str(row["label"] or ""),
            active=bool(row["active"]),
            last_message_id=str(row["last_message_id"]) if row["last_message_id"] else None,
            added_at=ensured_added_at,
        )

    def set_channel_option(self, channel_id: int, option_key: str, option_value: str) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                """
                INSERT INTO channel_options(channel_id, option_key, option_value)
                VALUES(?, ?, ?)
                ON CONFLICT(channel_id, option_key)
                    DO UPDATE SET option_value=excluded.option_value
                """,
                (channel_id, option_key, option_value),
            )
            self._conn.commit()

    def delete_channel_option(self, channel_id: int, option_key: str) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "DELETE FROM channel_options WHERE channel_id=? AND option_key=?",
                (channel_id, option_key),
            )
            self._conn.commit()

    def set_channel_thread(self, channel_id: int, thread_id: int | None) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "UPDATE channels SET telegram_thread_id=? WHERE id=?",
                (
                    str(int(thread_id)) if thread_id is not None else None,
                    channel_id,
                ),
            )
            self._conn.commit()

    def iter_channel_options(self, channel_id: int) -> Iterator[tuple[str, str]]:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "SELECT option_key, option_value FROM channel_options WHERE channel_id=?",
                (channel_id,),
            )
            rows = cur.fetchall()
        for row in rows:
            yield str(row["option_key"]), str(row["option_value"])

    def set_last_message(self, channel_id: int, message_id: str) -> None:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "UPDATE channels SET last_message_id=? WHERE id=?",
                (message_id, channel_id),
            )
            self._conn.commit()

    def set_known_pinned_messages(self, channel_id: int, message_ids: Iterable[str]) -> None:
        payload = json.dumps(sorted({str(mid) for mid in message_ids if str(mid)}))
        self.set_channel_option(channel_id, "state.pinned_ids", payload)

    def clear_known_pinned_messages(self, channel_id: int) -> None:
        self.delete_channel_option(channel_id, "state.pinned_ids")
        self.delete_channel_option(channel_id, "state.pinned_synced")

    def set_pinned_synced(self, channel_id: int, *, synced: bool) -> None:
        self.set_channel_option(
            channel_id,
            "state.pinned_synced",
            "true" if synced else "false",
        )

    def set_known_thread_ids(self, channel_id: int, thread_ids: Iterable[str]) -> None:
        """Store known forum thread IDs for tracking new threads."""
        payload = json.dumps(sorted({str(tid) for tid in thread_ids if str(tid)}))
        self.set_channel_option(channel_id, "state.thread_ids", payload)

    def clear_known_thread_ids(self, channel_id: int) -> None:
        """Clear forum thread tracking state."""
        self.delete_channel_option(channel_id, "state.thread_ids")
        self.delete_channel_option(channel_id, "state.forum_synced")

    def set_forum_synced(self, channel_id: int, *, synced: bool) -> None:
        """Mark forum as synced (initial threads recorded)."""
        self.set_channel_option(
            channel_id,
            "state.forum_synced",
            "true" if synced else "false",
        )

    def set_guild_id(self, channel_id: int, guild_id: str) -> None:
        """Store guild ID needed for forum thread API calls."""
        self.set_channel_option(channel_id, "state.guild_id", guild_id)

    # ------------------------------------------------------------------
    # Filters
    # ------------------------------------------------------------------
    def add_filter(self, channel_id: int, filter_type: str, value: str) -> bool:
        filter_type_key = filter_type.strip().lower()
        if filter_type_key not in _SUPPORTED_FILTER_TYPES:
            raise ValueError("unknown filter type")
        prepared = normalize_filter_value(filter_type_key, value)
        if prepared is None:
            raise ValueError("invalid filter value")
        stored_value, compare_key = prepared
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "SELECT id, value FROM filters WHERE channel_id=? AND filter_type=?",
                (channel_id, filter_type_key),
            )
            rows = cur.fetchall()
            for row in rows:
                existing = normalize_filter_value(filter_type_key, str(row["value"]))
                if existing and existing[1] == compare_key:
                    return False
            cur.execute(
                "INSERT INTO filters(channel_id, filter_type, value) VALUES(?, ?, ?)",
                (channel_id, filter_type_key, stored_value),
            )
            self._conn.commit()
        return True

    def remove_filter(self, channel_id: int, filter_type: str, value: str | None = None) -> int:
        filter_type_key = filter_type.strip().lower()
        if filter_type_key not in _SUPPORTED_FILTER_TYPES and filter_type_key not in {"all", "*"}:
            return 0
        with closing(self._conn.cursor()) as cur:
            if value is None:
                cur.execute(
                    "DELETE FROM filters WHERE channel_id=? AND filter_type=?",
                    (channel_id, filter_type_key),
                )
                removed = cur.rowcount
            else:
                prepared = normalize_filter_value(filter_type_key, value)
                if prepared is None:
                    return 0
                _, compare_key = prepared
                cur.execute(
                    "SELECT id, value FROM filters WHERE channel_id=? AND filter_type=?",
                    (channel_id, filter_type_key),
                )
                rows = cur.fetchall()
                matched_ids = [
                    int(row["id"])
                    for row in rows
                    if (
                        existing := normalize_filter_value(
                            filter_type_key, str(row["value"])
                        )
                    )
                    and existing[1] == compare_key
                ]
                for entry_id in matched_ids:
                    cur.execute("DELETE FROM filters WHERE id=?", (entry_id,))
                removed = len(matched_ids)
            self._conn.commit()
        return removed

    def clear_filters(self, channel_id: int) -> int:
        with closing(self._conn.cursor()) as cur:
            cur.execute("DELETE FROM filters WHERE channel_id=?", (channel_id,))
            removed = cur.rowcount
            self._conn.commit()
        return removed

    def iter_filters(self, channel_id: int) -> Iterator[tuple[str, str]]:
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "SELECT filter_type, value FROM filters WHERE channel_id=?",
                (channel_id,),
            )
            rows = cur.fetchall()
        for row in rows:
            yield str(row["filter_type"]), str(row["value"])

    def get_filter_config(self, channel_id: int) -> FilterConfig:
        return self._load_filter_config(channel_id)

    # ------------------------------------------------------------------
    # Composite loads
    # ------------------------------------------------------------------
    def _ensure_channel_added_at(
        self, channel_id: int, value: datetime | None
    ) -> datetime:
        if value is not None:
            return value
        timestamp = datetime.now(timezone.utc)
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                "UPDATE channels SET added_at=? WHERE id=?",
                (timestamp.isoformat(), channel_id),
            )
            self._conn.commit()
        return timestamp

    def load_channel_configurations(self) -> list[ChannelConfig]:
        defaults = self._load_default_options()
        default_filters = self._load_filter_config(0)
        default_deduplicate = parse_bool(
            self.get_setting("runtime.deduplicate_messages"), False
        )
        configs: list[ChannelConfig] = []
        for record in self.list_channels():
            ensured_added_at = self._ensure_channel_added_at(record.id, record.added_at)
            formatting = defaults["formatting"].copy()
            channel_options = dict(self.iter_channel_options(record.id))
            channel_formatting = _formatting_from_options(formatting, channel_options)
            filters = default_filters.merge(self._load_filter_config(record.id))
            (
                pinned_only,
                known_pinned_ids,
                pinned_synced,
                is_forum,
                guild_id,
                known_thread_ids,
                forum_synced,
            ) = _monitoring_from_options(defaults.get("monitoring", {}), channel_options)
            raw_deduplicate = channel_options.get("runtime.deduplicate_messages")
            deduplicate_inherited = raw_deduplicate is None
            deduplicate_messages = parse_bool(raw_deduplicate, default_deduplicate)
            health_status, health_message = self.get_health_status(
                f"channel.{record.discord_id}"
            )
            blocked_by_health = health_status == "error" and record.active
            configs.append(
                ChannelConfig(
                    discord_id=record.discord_id,
                    telegram_chat_id=record.telegram_chat_id,
                    telegram_thread_id=record.telegram_thread_id,
                    label=record.label or record.discord_id,
                    formatting=channel_formatting,
                    filters=filters,
                    deduplicate_messages=deduplicate_messages,
                    deduplicate_inherited=deduplicate_inherited,
                    last_message_id=record.last_message_id,
                    active=record.active,
                    storage_id=record.id,
                    added_at=ensured_added_at,
                    pinned_only=pinned_only,
                    known_pinned_ids=known_pinned_ids,
                    pinned_synced=pinned_synced,
                    is_forum=is_forum,
                    guild_id=guild_id,
                    known_thread_ids=known_thread_ids,
                    forum_synced=forum_synced,
                    health_status=health_status,
                    health_message=health_message,
                    blocked_by_health=blocked_by_health,
                )
            )
        return configs

    def record_manual_forward_activity(
        self,
        *,
        requested: int,
        limit: int,
        total_forwarded: int,
        entries: Sequence[ManualForwardEntry],
    ) -> None:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "requested": requested,
            "limit": limit,
            "total": total_forwarded,
            "channels": [
                {
                    "discord_id": entry.discord_id,
                    "label": entry.label,
                    "forwarded": entry.forwarded,
                    "mode": entry.mode,
                    "note": entry.note,
                }
                for entry in entries
            ],
        }
        self.set_setting("activity.send_recent", json.dumps(payload, ensure_ascii=False))

    def load_manual_forward_activity(self) -> ManualForwardActivity | None:
        raw = self.get_setting("activity.send_recent")
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        timestamp = _parse_timestamp(data.get("timestamp"))
        if timestamp is None:
            return None
        try:
            requested = int(data.get("requested") or 0)
        except (TypeError, ValueError):
            requested = 0
        try:
            limit = int(data.get("limit") or 0)
        except (TypeError, ValueError):
            limit = 0
        try:
            total_forwarded = int(data.get("total") or 0)
        except (TypeError, ValueError):
            total_forwarded = 0
        entries_data = data.get("channels")
        entries: list[ManualForwardEntry] = []
        if isinstance(entries_data, Sequence):
            for item in entries_data:
                if not isinstance(item, dict):
                    continue
                discord_id = str(item.get("discord_id") or "")
                label = str(item.get("label") or discord_id)
                try:
                    forwarded = int(item.get("forwarded") or 0)
                except (TypeError, ValueError):
                    forwarded = 0
                mode = str(item.get("mode") or "messages")
                note = str(item.get("note") or "")
                entries.append(
                    ManualForwardEntry(
                        discord_id=discord_id,
                        label=label,
                        forwarded=forwarded,
                        mode=mode,
                        note=note,
                    )
                )
        return ManualForwardActivity(
            timestamp=timestamp,
            requested=requested,
            limit=limit,
            total_forwarded=total_forwarded,
            entries=entries,
        )

    def _load_default_options(self) -> dict[str, dict[str, str]]:
        settings: dict[str, dict[str, str]] = {"formatting": {}, "monitoring": {}}
        for key, value in self.iter_settings("formatting."):
            settings.setdefault("formatting", {})[key.removeprefix("formatting.")] = value
        monitoring_mode = self.get_setting("monitoring.mode")
        if monitoring_mode:
            settings.setdefault("monitoring", {})["mode"] = monitoring_mode
        return settings

    def _load_filter_config(self, channel_id: int) -> FilterConfig:
        filters = FilterConfig()
        seen: dict[str, set[str]] = {}
        for filter_type_raw, value in self.iter_filters(channel_id):
            filter_type = filter_type_raw.strip().lower()
            normalized = normalize_filter_value(filter_type, value)
            if not normalized:
                continue
            stored_value, compare_key = normalized
            seen.setdefault(filter_type, set())
            if compare_key in seen[filter_type]:
                continue
            seen[filter_type].add(compare_key)
            target = _filter_target(filters, filter_type)
            if target is None:
                continue
            if filter_type in _TEXT_FILTER_TYPES:
                target.add(value.strip())
            else:
                target.add(stored_value)
        return filters

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def close(self) -> None:
        self._conn.close()


_TEXT_FILTER_TYPES = {"whitelist", "blacklist"}
_SENDER_FILTER_TYPES = {"allowed_senders", "blocked_senders"}
_TYPE_FILTER_TYPES = {"allowed_types", "blocked_types"}
_ROLE_FILTER_TYPES = {"allowed_roles", "blocked_roles"}
_SUPPORTED_FILTER_TYPES = (
    _TEXT_FILTER_TYPES
    | _SENDER_FILTER_TYPES
    | _TYPE_FILTER_TYPES
    | _ROLE_FILTER_TYPES
)


def _parse_thread_id(value: object) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed


def _parse_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        text = str(value)
    except (TypeError, ValueError):
        return None
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None


def normalize_filter_value(filter_type: str, value: str) -> tuple[str, str] | None:
    filter_key = filter_type.strip().lower()
    trimmed = value.strip()
    if not trimmed:
        return None
    if filter_key not in _SUPPORTED_FILTER_TYPES:
        return None
    if filter_key in _SENDER_FILTER_TYPES:
        if trimmed.lstrip("-").isdigit():
            numeric = str(int(trimmed))
            return numeric, f"id:{numeric}"
        normalized_name = normalize_username(trimmed)
        if normalized_name is None:
            return None
        return normalized_name, f"name:{normalized_name}"
    if filter_key in _TYPE_FILTER_TYPES:
        normalized_value = trimmed.lower()
        return normalized_value, normalized_value
    if filter_key in _ROLE_FILTER_TYPES:
        normalized_role = _normalize_role_value(trimmed)
        if normalized_role is None:
            return None
        return normalized_role, f"role:{normalized_role}"
    if filter_key in _TEXT_FILTER_TYPES:
        return trimmed, trimmed.casefold()
    return trimmed, trimmed


def format_filter_value(filter_type: str, value: str) -> str:
    filter_key = filter_type.strip().lower()
    cleaned = value.strip()
    if not cleaned:
        return cleaned
    if filter_key in _SENDER_FILTER_TYPES and cleaned.lstrip("-").isdigit():
        return str(int(cleaned))
    if filter_key in _ROLE_FILTER_TYPES and cleaned.lstrip("-").isdigit():
        return str(int(cleaned))
    return cleaned


def _filter_target(filters: FilterConfig, filter_type: str) -> set[str] | None:
    normalized_type = filter_type.strip().lower()
    mapping = {
        "whitelist": filters.whitelist,
        "blacklist": filters.blacklist,
        "allowed_senders": filters.allowed_senders,
        "blocked_senders": filters.blocked_senders,
        "allowed_types": filters.allowed_types,
        "blocked_types": filters.blocked_types,
        "allowed_roles": filters.allowed_roles,
        "blocked_roles": filters.blocked_roles,
    }
    return mapping.get(normalized_type)


def _monitoring_from_options(
    defaults: dict[str, str], options: dict[str, str]
) -> tuple[bool, set[str], bool, bool, str | None, set[str], bool]:
    """Parse monitoring options, returning pinned and forum state.
    
    Returns:
        (pinned_only, known_pinned_ids, pinned_synced,
         is_forum, guild_id, known_thread_ids, forum_synced)
    """
    default_mode = defaults.get("mode", "messages").strip().lower()
    mode_raw = options.get("monitoring.mode", default_mode)
    mode = str(mode_raw).strip().lower()
    pinned_only = mode == "pinned"
    is_forum = mode == "forum"
    known_pinned = _parse_known_pinned(options.get("state.pinned_ids"))
    pinned_synced = _parse_bool_option(options.get("state.pinned_synced"))
    guild_id = options.get("state.guild_id")
    known_threads = _parse_known_threads(options.get("state.thread_ids"))
    forum_synced = _parse_bool_option(options.get("state.forum_synced"))
    return pinned_only, known_pinned, pinned_synced, is_forum, guild_id, known_threads, forum_synced


def _parse_bool_option(value: str | None) -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return False


def _parse_known_pinned(payload: str | None) -> set[str]:
    if not payload:
        return set()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return set()
    result: set[str] = set()
    for entry in data:
        text = str(entry)
        if text:
            result.add(text)
    return result


def _parse_known_threads(payload: str | None) -> set[str]:
    """Parse stored thread IDs (same format as pinned)."""
    return _parse_known_pinned(payload)


def _normalize_role_value(value: str) -> str | None:
    cleaned = value.strip()
    if cleaned.startswith("<@&") and cleaned.endswith(">"):
        cleaned = cleaned[3:-1]
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    if cleaned.lstrip("-").isdigit():
        return str(int(cleaned))
    return None


def _formatting_from_options(
    base: dict[str, str],
    overrides: dict[str, str],
) -> FormattingOptions:
    formatting_overrides = {
        key.removeprefix("formatting."): value
        for key, value in overrides.items()
        if key.startswith("formatting.")
    }
    options = {**base, **formatting_overrides}
    return FormattingOptions(
        disable_preview=options.get("disable_preview", "true").lower() == "true",
        max_length=int(options.get("max_length", "3500")),
        ellipsis=options.get("ellipsis", "…"),
        attachments_style=options.get("attachments_style", "summary"),
        show_discord_link=options.get("show_discord_link", "false").lower() == "true",
    )
