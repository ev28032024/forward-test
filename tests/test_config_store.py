from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from forward_monitor.config_store import ConfigStore


def test_channel_lifecycle(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path / "db.sqlite")
    store.set_setting("formatting.disable_preview", "false")
    store.add_filter(0, "whitelist", "hello")

    record = store.add_channel("123", "456", "Label", telegram_thread_id=777)
    assert record.added_at is not None
    store.set_channel_option(record.id, "formatting.attachments_style", "links")
    store.set_last_message(record.id, "900")

    stored_record = store.get_channel("123")
    assert stored_record is not None
    assert stored_record.telegram_thread_id == 777
    assert stored_record.added_at is not None

    configs = store.load_channel_configurations()
    assert len(configs) == 1
    channel = configs[0]
    assert channel.label == "Label"
    assert channel.formatting.disable_preview is False
    assert channel.formatting.attachments_style == "links"
    assert channel.filters.whitelist == {"hello"}
    assert channel.last_message_id == "900"
    assert channel.telegram_thread_id == 777
    assert channel.added_at is not None
    assert channel.pinned_only is False
    assert channel.known_pinned_ids == set()
    assert channel.pinned_synced is False

    store.set_channel_option(record.id, "monitoring.mode", "pinned")
    store.set_known_pinned_messages(record.id, ["10", "20"])
    store.set_pinned_synced(record.id, synced=True)
    configs = store.load_channel_configurations()
    channel = configs[0]
    assert channel.pinned_only is True
    assert channel.known_pinned_ids == {"10", "20"}
    assert channel.pinned_synced is True


def test_telegram_offset_helpers(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path / "offsets.sqlite")

    assert store.get_telegram_offset() is None

    store.set_telegram_offset(120)
    assert store.get_telegram_offset() == 120

    store.set_telegram_offset(-5)
    assert store.get_telegram_offset() == 0

    store.clear_telegram_offset()
    assert store.get_telegram_offset() is None

    store.set_setting("state.telegram.offset", "not-a-number")
    assert store.get_telegram_offset() is None


def test_filter_management(tmp_path: Path) -> None:
    store = ConfigStore(tmp_path / "filters.sqlite")

    assert store.add_filter(0, "whitelist", "Hello") is True
    assert store.add_filter(0, "whitelist", "hello") is False
    assert store.get_filter_config(0).whitelist == {"Hello"}

    removed = store.remove_filter(0, "whitelist", "HELLO")
    assert removed == 1
    assert store.remove_filter(0, "whitelist", "HELLO") == 0

    assert store.add_filter(0, "allowed_senders", " 1090758325299314818 ") is True
    assert store.add_filter(0, "allowed_senders", "1090758325299314818") is False
    assert store.add_filter(0, "allowed_senders", "@CoDeD") is True
    allowed = store.get_filter_config(0).allowed_senders
    assert allowed == {"1090758325299314818", "coded"}

    assert store.remove_filter(0, "allowed_senders", "@coded") == 1
    assert store.remove_filter(0, "allowed_senders", "coded") == 0

    assert store.add_filter(0, "allowed_roles", "<@&123>") is True
    assert store.add_filter(0, "allowed_roles", "123") is False
    roles = store.get_filter_config(0).allowed_roles
    assert roles == {"123"}

    with pytest.raises(ValueError):
        store.add_filter(0, "unknown", "value")

    store.add_filter(0, "blacklist", "Spam")
    assert store.get_filter_config(0).blacklist == {"Spam"}
    cleared = store.clear_filters(0)
    assert cleared >= 1
    config = store.get_filter_config(0)
    assert not any(
        getattr(config, name)
        for name in (
            "whitelist",
            "blacklist",
            "allowed_senders",
            "blocked_senders",
            "allowed_types",
            "blocked_types",
            "allowed_roles",
            "blocked_roles",
        )
    )


def test_added_at_backfilled_for_existing_channels(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    store = ConfigStore(db_path)
    store.add_channel("123", "456", "Label")
    store.close()

    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE channels SET added_at=NULL")
    conn.commit()
    conn.close()

    store = ConfigStore(db_path)
    record = store.get_channel("123")
    assert record is not None
    assert record.added_at is not None
    configs = store.load_channel_configurations()
    assert configs and configs[0].added_at is not None

