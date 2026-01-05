from forward_monitor.utils import parse_bool, parse_delay_setting


def test_parse_delay_setting_ms_backwards_compatibility() -> None:
    assert parse_delay_setting("250", 0.0) == 0.25


def test_parse_delay_setting_seconds_float() -> None:
    assert parse_delay_setting("1.50", 0.0) == 1.5


def test_parse_delay_setting_invalid_returns_default() -> None:
    assert parse_delay_setting("not-a-number", 2.0) == 2.0


def test_parse_bool_supports_truthy_and_falsy() -> None:
    assert parse_bool("on") is True
    assert parse_bool("NO") is False
    assert parse_bool(None, default=True) is True
    assert parse_bool("unexpected", default=False) is False
