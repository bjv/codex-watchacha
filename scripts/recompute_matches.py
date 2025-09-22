#!/usr/bin/env python3
"""Re-run set matching for items that currently lack an assignment."""

import argparse
import asyncio
import sys
from pathlib import Path

import yaml
from sqlalchemy import select

from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database, Item, ItemSetMatch
from watcha import SetMatcher
CONFIG_PATH = ROOT / "config.yml"


def load_config_path() -> Path:
    if not CONFIG_PATH.exists():
        raise RuntimeError("config.yml not found – create it from config.yml.sample")
    return CONFIG_PATH


def load_db_url() -> str:
    cfg = yaml.safe_load(load_config_path().read_text()) or {}
    db_cfg = cfg.get("database") or {}
    db_url = db_cfg.get("url")
    if not db_url:
        raise RuntimeError("database.url missing in config.yml")
    return db_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recompute set matches for unmatched items.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N items (debugging)")
    parser.add_argument("--dry-run", action="store_true", help="Only report what would change")
    return parser.parse_args()


async def recompute(database: Database, limit: Optional[int], dry_run: bool) -> int:
    set_records = await database.fetch_set_records()
    matcher = SetMatcher(set_records)

    processed = 0
    matched = 0
    skipped_details = []

    async with database.session_factory() as session:
        async with session.begin():
            stmt = (
                select(Item)
                .outerjoin(ItemSetMatch, ItemSetMatch.item_id == Item.id)
                .where(ItemSetMatch.id.is_(None))
                .order_by(Item.last_seen_ts.desc())
            )
            if limit:
                stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            items = result.scalars().all()

            for item in items:
                processed += 1
                match_payload = matcher.match(item.title)
                if not match_payload:
                    skipped_details.append((item, "keine Kandidaten"))
                    continue

                if not match_payload.above_threshold:
                    skipped_details.append(
                        (
                            item,
                            f"score {match_payload.score:.2f} below threshold",
                        )
                    )
                    if dry_run:
                        print(
                            f"[dry-run] skip {item.provider_key}:{item.provider_item_id} -> "
                            f"{match_payload.set_nr} (score {match_payload.score:.2f})"
                        )
                    continue

                matched += 1
                if dry_run:
                    print(
                        f"[dry-run] would assign {item.provider_key}:{item.provider_item_id} -> "
                        f"{match_payload.set_nr} (score {match_payload.score:.2f})"
                    )
                    continue

                session.add(
                    ItemSetMatch(
                        item_id=item.id,
                        set_nr=match_payload.set_nr,
                        score=match_payload.score,
                        name_score=match_payload.name_score,
                        matched_number=match_payload.matched_number,
                        parts_difference=match_payload.parts_difference,
                    )
                )

    print(f"Processed {processed} unmatched items, added matches for {matched}.")
    if skipped_details:
        print("\nSkipped items (no match):")
        for item, reason in skipped_details[:20]:
            print(f"  - {item.provider_key}:{item.provider_item_id} :: {item.title[:80]} ({reason})")
        if len(skipped_details) > 20:
            print(f"  … {len(skipped_details) - 20} more")
    return matched


async def main() -> int:
    args = parse_args()
    db_url = load_db_url()

    database = Database(db_url)
    await database.init_models()
    try:
        await recompute(database, args.limit, args.dry_run)
    finally:
        await database.dispose()
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        exit_code = 1
    sys.exit(exit_code)
