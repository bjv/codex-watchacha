#!/usr/bin/env python3
"""Send a synthetic Ultra-Priority alert via the configured Telegram bot.

Usage:
    python scripts/send_ultra_alert.py [--config path/to/config.yml]

The script reuses the existing notification helpers so the message formatting
matches the live watcher. It injects a dummy set-match with high confidence to
trigger the ultra headline."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any, Dict

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db import SetMatchPayload
from watcha import build_notification_payload, notify


def _default_item() -> Dict[str, Any]:
    return {
        "id": "ULTRA-TEST-001",
        "title": "🔥 Beispiel Hype-Listing – Sofort zugreifen",
        "href": "https://example.com/ultra-test",
        "bid": "-",
        "buyout": "199.99 €",
        "listing_type": "fixed",
        "shipping_note": "Kostenloser Versand",
        "set_match_info": SetMatchPayload(
            set_nr="99999",
            set_name="Demo Ultra Set",
            score=0.95,
            matched_number=True,
            name_score=0.92,
            parts_difference=0,
            above_threshold=True,
        ),
    }


async def _run(config_path: Path) -> None:
    cfg = yaml.safe_load(config_path.read_text()) or {}

    item = _default_item()
    provider_key = "test"
    provider_label = "TEST"
    search_name = "Ultra Test Alert"

    title, msg = build_notification_payload(
        provider_key,
        provider_label,
        search_name,
        item,
        is_ultra=True,
        price_insights=None,
    )

    await notify(cfg, title, msg, item["href"])
    print("[send-ultra-alert] Testbenachrichtigung ausgelöst.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Send an ultra alert test message")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yml"),
        help="Pfad zur Konfigurationsdatei (Standard: ./config.yml)",
    )
    args = parser.parse_args()

    if not args.config.exists():
        raise FileNotFoundError(f"Konfigurationsdatei nicht gefunden: {args.config}")

    asyncio.run(_run(args.config))


if __name__ == "__main__":
    main()
