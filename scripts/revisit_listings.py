#!/usr/bin/env python3
"""Revisit stale listings to detect availability changes."""

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import yaml
from playwright.async_api import Browser, Page, TimeoutError as PWTimeout, async_playwright, Error as PWError


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import (
    COMPLETED_AVAILABILITIES,
    DEFAULT_AVAILABILITY,
    Database,
    RevisitCandidate,
)


CONFIG_PATH = ROOT / "config.yml"


@dataclass(slots=True)
class RevisitOutcome:
    availability: str
    note: Optional[str]


def load_config() -> Dict[str, object]:
    if not CONFIG_PATH.exists():
        raise RuntimeError("config.yml not found – create it from config.yml.sample")
    return yaml.safe_load(CONFIG_PATH.read_text()) or {}


async def detect_kleinanzeigen(page: Page, url: str) -> RevisitOutcome:
    try:
        response = await page.goto(url, wait_until="domcontentloaded")
    except PWTimeout:
        return RevisitOutcome(None, "Kleinanzeigen: Timeout beim Laden")
    except Exception as exc:  # pragma: no cover - network variance
        return RevisitOutcome(None, f"Kleinanzeigen: Fehler {exc}")

    status = response.status if response else None
    if status and status >= 400:
        return RevisitOutcome("deleted", f"Kleinanzeigen: HTTP {status}")

    try:
        await page.wait_for_selector(".pvap-reserved-title", timeout=1_000)
    except PWTimeout:
        pass

    async def _capture_marker(substring: str) -> Optional[str]:
        return await page.evaluate(
            """
            (needle) => {
                const spans = Array.from(document.querySelectorAll('.pvap-reserved-title'));
                for (const span of spans) {
                    const text = (span.textContent || '').toLowerCase();
                    if (!text.includes(needle)) {
                        continue;
                    }
                    if (span.classList.contains('is-hidden')) {
                        continue;
                    }
                    return (span.outerHTML || text).trim();
                }
                return null;
            }
            """,
            substring.lower(),
        )

    reserved_marker = await _capture_marker("reserviert")
    if reserved_marker:
        return RevisitOutcome(
            "reserved",
            f"Kleinanzeigen: sichtbarer Reserviert-Marker :: {reserved_marker}",
        )

    deleted_marker = await _capture_marker("gelöscht")
    if deleted_marker:
        return RevisitOutcome(
            "deleted",
            f"Kleinanzeigen: sichtbarer Gelöscht-Marker :: {deleted_marker}",
        )

    sold_marker = await _capture_marker("verkauft")
    if sold_marker:
        return RevisitOutcome(
            "sold",
            f"Kleinanzeigen: sichtbarer Verkauft-Marker :: {sold_marker}",
        )

    return RevisitOutcome("active", None)


async def fetch_ebay_html_fallback(url: str) -> Optional[str]:
    def _fetch() -> Optional[str]:
        req = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        try:
            with urlopen(req, timeout=10) as resp:
                data = resp.read()
        except (HTTPError, URLError, TimeoutError):
            return None
        except Exception:
            return None
        try:
            return data.decode("utf-8", errors="ignore")
        except Exception:
            return data.decode("latin-1", errors="ignore")

    try:
        return await asyncio.to_thread(_fetch)
    except Exception:
        return None


async def detect_ebay(page: Page, url: str) -> RevisitOutcome:
    try:
        response = await page.goto(url, wait_until="domcontentloaded")
    except PWTimeout:
        return RevisitOutcome(None, "eBay: Timeout beim Laden")
    except Exception as exc:  # pragma: no cover - network variance
        return RevisitOutcome(None, f"eBay: Fehler {exc}")

    status = response.status if response else None
    if status and status >= 400:
        return RevisitOutcome("deleted", f"eBay: HTTP {status}")

    await page.wait_for_timeout(5_000)

    async def fetch_html() -> Optional[str]:
        try:
            return await page.content()
        except PWError:
            return None
        except Exception:
            return None

    html_source = "playwright"
    html = await fetch_html()
    if html is None:
        try:
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_timeout(3_000)
        except Exception:
            pass
        html = await fetch_html()

    if html is None:
        html = await fetch_ebay_html_fallback(url)
        html_source = "urllib"
        if html is None:
            return RevisitOutcome(None, "eBay: DOM Zugriff fehlgeschlagen (Fallback fehlgeschlagen)")

    def extract_snippet(tag: str, class_keyword: str) -> Dict[str, Optional[str]]:
        pattern = re.compile(
            rf"<" + tag + r"[^>]*class\s*=\s*(?:\"[^\"]*" + re.escape(class_keyword) + r"[^\"]*\"|'[^']*" + re.escape(class_keyword) + r"[^']*')[^>]*>.*?</" + tag + r">",
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.search(html)
        snippet = match.group(0) if match else None
        if not snippet:
            return {"snippet": None, "text": "", "text_lower": ""}
        text = re.sub(r"<[^>]+>", " ", snippet)
        text = re.sub(r"\s+", " ", text).strip()
        return {"snippet": snippet, "text": text, "text_lower": text.lower()}

    panel_info = extract_snippet("div", "d-top-panel-message")
    signal_info = extract_snippet("span", "signal")
    price_info = extract_snippet("div", "x-item-condensed-card__price")

    panel_html: Optional[str] = panel_info["snippet"]
    panel_text = panel_info["text_lower"]
    signal_html: Optional[str] = signal_info["snippet"]
    signal_text = signal_info["text_lower"]
    price_text = price_info["text"]

    note_parts = []
    if panel_html:
        note_parts.append(f"Panel={panel_html}")
    if signal_html:
        note_parts.append(f"Signal={signal_html}")
    if price_text:
        note_parts.append(f"Preis={price_text}")
    note_parts.append(f"Quelle={html_source}")
    note = " | ".join(note_parts) if note_parts else None

    if "verkauft" in panel_text and "verkauft" in signal_text:
        return RevisitOutcome("sold", note)

    if ("beendet" in panel_text or "endete" in panel_text) and "beendet" in signal_text:
        return RevisitOutcome("unsold", note)

    # fallback: no explicit status change detected
    return RevisitOutcome(None, note)


DETECTORS: Dict[str, Callable[[Page, str], Awaitable[RevisitOutcome]]] = {
    "kleinanzeigen": detect_kleinanzeigen,
    "ebay": detect_ebay,
}


async def process_candidate(
    browser: Browser,
    database: Database,
    candidate: RevisitCandidate,
    *,
    navigation_timeout: int,
    dry_run: bool,
) -> None:
    detector = DETECTORS.get(candidate.provider_key)
    if detector is None:
        print(f"[revisit] überspringe unbekannten Anbieter {candidate.provider_key}")
        return

    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_navigation_timeout(navigation_timeout)
    page.set_default_timeout(navigation_timeout)
    availability_before = candidate.availability or DEFAULT_AVAILABILITY
    note = None
    try:
        outcome = await detector(page, candidate.href)
        availability_after = outcome.availability or availability_before
        note = outcome.note
        snapshot_created = False
        if not dry_run:
            snapshot_created = await database.record_revisit_outcome(
                candidate.item_id,
                availability=availability_after,
                note=note,
                source="revisit",
            )
        marker = "✓" if availability_after in COMPLETED_AVAILABILITIES else "·"
        change_suffix = "*" if snapshot_created else ""
        note_part = f" – {note}" if note else ""
        prefix = "[revisit:dry]" if dry_run else f"[revisit]{change_suffix}"
        print(
            f"{prefix} {marker} {candidate.provider_key}:{candidate.provider_item_id} -> "
            f"{availability_after} (vorher {availability_before}){note_part}\n"
            f"        URL: {candidate.href}"
        )
        if note and "::" in note:
            snippet = note.split("::", 1)[-1].strip()
            if snippet:
                print(f"        DOM: {snippet}")
    finally:
        await page.close()
        await context.close()


async def check_single_url(provider_key: str, url: str) -> int:
    detector = DETECTORS.get(provider_key)
    if detector is None:
        print(f"[check:error] unbekannter Provider: {provider_key}", file=sys.stderr)
        return 1

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
            try:
                context = await browser.new_context()
                page = await context.new_page()
                outcome = await detector(page, url)
                availability = outcome.availability or DEFAULT_AVAILABILITY
                note = outcome.note
                print(f"[check] Provider : {provider_key}")
                print(f"[check] URL      : {url}")
                print(f"[check] Status   : {availability}")
                if note:
                    print(f"[check] Hinweis  : {note}")
                    if "::" in note:
                        snippet = note.split("::", 1)[-1].strip()
                        if snippet:
                            print(f"[check] DOM      : {snippet}")
            finally:
                await browser.close()
    except Exception as exc:  # pragma: no cover - convenience output
        print(f"[check:error] {exc}", file=sys.stderr)
        return 1

    return 0


async def run_once(
    browser: Browser,
    database: Database,
    cfg: Dict[str, object],
    *,
    stale_days_override: Optional[int],
    dry_run: bool,
) -> None:
    revisit_cfg = cfg.get("revisit") or {}
    if not revisit_cfg:
        print("[revisit] kein revisit-Abschnitt in config.yml gefunden")
        return

    stale_days = (
        int(stale_days_override) if stale_days_override is not None else int(revisit_cfg.get("stale_days", 7) or 7)
    )
    batch_size = int(revisit_cfg.get("batch_size", 5) or 5)
    providers = revisit_cfg.get("providers") or []
    navigation_timeout = int(revisit_cfg.get("navigation_timeout_ms", 20000) or 20000)

    cutoff = datetime.now(timezone.utc) - timedelta(days=stale_days)
    candidates = await database.fetch_revisit_candidates(
        providers=providers,
        stale_before=cutoff,
        limit=batch_size,
    )
    if not candidates:
        print("[revisit] keine veralteten Listings gefunden")
        return

    for candidate in candidates:
        await process_candidate(
            browser,
            database,
            candidate,
            navigation_timeout=navigation_timeout,
            dry_run=dry_run,
        )


async def main() -> int:
    parser = argparse.ArgumentParser(description="Revisit stale listings and record availability changes.")
    parser.add_argument("--once", action="store_true", help="Nur einen Durchlauf ausführen")
    parser.add_argument("--dry-run", action="store_true", help="Änderungen nicht in der Datenbank speichern")
    parser.add_argument(
        "--stale-days",
        type=int,
        default=None,
        help="Listings erst ab dieser Anzahl Tage ohne Sichtung revisiten (überschreibt config)",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Einzelnes Listing prüfen und Ergebnis ausgeben",
    )
    parser.add_argument(
        "--provider",
        type=str,
        default=None,
        choices=sorted(DETECTORS.keys()),
        help="Provider für --url (Standard: kleinanzeigen)",
    )
    args = parser.parse_args()

    if args.url:
        provider_key = args.provider or "kleinanzeigen"
        return await check_single_url(provider_key, args.url)

    if args.dry_run:
        print("[revisit] Dry-run aktiv – es werden keine Datenbankänderungen geschrieben")

    cfg = load_config()
    db_cfg = (cfg.get("database") or {})
    db_url = db_cfg.get("url")
    if not db_url:
        raise RuntimeError("database.url missing in config.yml")

    database = Database(db_url, echo=bool(db_cfg.get("echo", False)))
    await database.init_models()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
            try:
                while True:
                    cfg = load_config()
                    revisit_cfg = cfg.get("revisit") or {}
                    interval_seconds = int(revisit_cfg.get("interval_seconds", 900) or 900)
                    await run_once(
                        browser,
                        database,
                        cfg,
                        stale_days_override=args.stale_days,
                        dry_run=args.dry_run,
                    )
                    if args.once:
                        break
                    await asyncio.sleep(interval_seconds)
            finally:
                await browser.close()
    finally:
        await database.dispose()

    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        exit_code = 0
    except Exception as exc:  # pragma: no cover - CLI convenience
        print(f"[revisit:error] {exc}", file=sys.stderr)
        exit_code = 1
    sys.exit(exit_code)
