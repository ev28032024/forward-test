"""Command line entry point."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from .app import ForwardMonitorApp


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward Discord updates to Telegram")
    parser.add_argument("--db-path", default="forward.db", help="Путь к файлу хранилища")
    parser.add_argument(
        "--telegram-token",
        help=("Токен Telegram бота. Можно передать через " "FORWARD_TELEGRAM_TOKEN"),
    )
    parser.add_argument("--log-level", default="INFO", help="Уровень логирования")
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    token = args.telegram_token or os.getenv("FORWARD_TELEGRAM_TOKEN")
    if not token:
        parser.error(
            "Нужно передать --telegram-token или переменную окружения " "FORWARD_TELEGRAM_TOKEN"
        )

    app = ForwardMonitorApp(db_path=Path(args.db_path), telegram_token=token)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Остановка по запросу пользователя")


if __name__ == "__main__":
    main()
