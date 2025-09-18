#!/usr/bin/env python3
"""Utility to wipe Watchacha database tables.

Reads the Postgres connection URL from config.yml and truncates all
application tables (snapshots, item_clusters, items, clusters, providers)
with CASCADE semantics. Requires explicit confirmation unless --yes is
passed.
"""

import argparse
import asyncio
import sys
from pathlib import Path

import yaml
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config.yml"


async def wipe_database(db_url: str) -> None:
    engine = create_async_engine(db_url, future=True)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "TRUNCATE TABLE snapshots, item_clusters, items, clusters, providers "
                    "RESTART IDENTITY CASCADE"
                )
            )
    finally:
        await engine.dispose()


def load_db_url() -> str:
    if not CONFIG_PATH.exists():
        raise RuntimeError("config.yml not found – create it from config.yml.sample")
    cfg = yaml.safe_load(CONFIG_PATH.read_text()) or {}
    db_cfg = cfg.get("database") or {}
    db_url = db_cfg.get("url")
    if not db_url:
        raise RuntimeError("database.url missing in config.yml")
    return db_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wipe Watchacha database contents.")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="skip confirmation prompt",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    if not args.yes:
        prompt = (
            "This will DELETE all scraped data (providers, items, clusters, snapshots).\n"
            "Type 'wipe it' to continue: "
        )
        try:
            confirmation = input(prompt)
        except KeyboardInterrupt:
            print("\nAborted.")
            return 1
        if confirmation.strip().lower() != "wipe it":
            print("Aborted – confirmation mismatch.")
            return 1

    db_url = load_db_url()
    await wipe_database(db_url)
    print("Database wiped successfully.")
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except Exception as exc:  # pragma: no cover - CLI convenience
        print(f"[error] {exc}", file=sys.stderr)
        exit_code = 1
    sys.exit(exit_code)
