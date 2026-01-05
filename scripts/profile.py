#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import pstats
import sys
import sysconfig
from pathlib import Path

STDLIB_PROFILE = Path(sysconfig.get_path("stdlib")) / "profile.py"
if STDLIB_PROFILE.exists():
    spec = importlib.util.spec_from_file_location("_stdlib_profile", STDLIB_PROFILE)
    if spec and spec.loader:
        std_profile = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(std_profile)
        sys.modules.setdefault("profile", std_profile)

import cProfile  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from scripts.bench import benchmark_formatter, benchmark_forwarding  # noqa: E402

PROFILES_DIR = Path("profiles")


def profile_formatter(iterations: int, output: Path) -> None:
    profiler = cProfile.Profile()
    profiler.enable()
    benchmark_formatter(iterations)
    profiler.disable()
    output.parent.mkdir(parents=True, exist_ok=True)
    profiler.dump_stats(str(output))
    stats = pstats.Stats(profiler)
    stats.sort_stats("cumulative").print_stats(20)


def profile_forwarding(iterations: int, output: Path) -> None:
    profiler = cProfile.Profile()

    async def runner() -> None:
        profiler.enable()
        await benchmark_forwarding(iterations)
        profiler.disable()

    asyncio.run(runner())
    output.parent.mkdir(parents=True, exist_ok=True)
    profiler.dump_stats(str(output))
    stats = pstats.Stats(profiler)
    stats.sort_stats("cumulative").print_stats(20)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CPU profiles for hot paths",
    )
    parser.add_argument(
        "--format-count",
        type=int,
        default=1000,
        help="Iterations for formatter benchmark",
    )
    parser.add_argument(
        "--forward-count",
        type=int,
        default=300,
        help="Iterations for forwarding benchmark",
    )
    parser.add_argument(
        "--formatter-output",
        type=Path,
        default=PROFILES_DIR / "formatter.prof",
        help="Profile output path for formatter",
    )
    parser.add_argument(
        "--forward-output",
        type=Path,
        default=PROFILES_DIR / "forwarding.prof",
        help="Profile output path for forwarding",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    profile_formatter(args.format_count, args.formatter_output)
    profile_forwarding(args.forward_count, args.forward_output)


if __name__ == "__main__":
    main()
