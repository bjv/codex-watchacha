import asyncio, html, re, sys, time, urllib.parse
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import List, Dict, Any, Tuple, Callable, Optional

import yaml, httpx

from rapidfuzz import fuzz, process

from db import Database, SetMatchPayload, SetRecord
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# =========================
# Setup & Utilities
# =========================
ITEM_ID_EBAY_RE = re.compile(r"/itm/(?:[^/]+/)?(?P<id>\d{9,15})")
ITEM_ID_KA_RE   = re.compile(r"/s-anzeige/.*?/(\d+)")
PRICE_RE        = re.compile(r"([\d\.\s,]+)\s*€|EUR\s*([\d\.\s,]+)")
SET_NR_RE       = re.compile(r"\b\d{5,6}\b")
PARTS_RE        = re.compile(r"(\d{2,4})\s*(teile|piece|pieces|pcs|stk|stück)", re.IGNORECASE)
REQUEST_FAIL_IGNORES = ("liberty-metrics", "frontend-metrics", "googlesyndication", "organic-ad-tracking","googletagmanager","9S5VSJJQL24zcSOuuJ")

ULTRA_PRIORITY_TAGS = {"ultra", "ultra-power-prio"}


def parse_eur(text: str) -> Tuple[Optional[int], str]:
    if not text:
        return None, ""
    t = text.replace("\xa0", " ")
    m = PRICE_RE.search(t)
    if not m:
        return None, ""
    raw = (m.group(1) or m.group(2) or "").strip()
    cleaned = raw.replace(".", "").replace(" ", "").replace(",", ".")
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None, ""
    quantized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    cents = int((quantized * 100).to_integral_value(rounding=ROUND_HALF_UP))
    return cents, f"{quantized:.2f} €"


def parse_shipping_info(text: Optional[str]) -> Tuple[Optional[int], str]:
    if not text:
        return None, ""
    cleaned = text.replace("\xa0", " ").strip()
    if not cleaned:
        return None, ""
    lower = cleaned.lower()
    if any(keyword in lower for keyword in ("kostenlos", "gratis", "frei")):
        return 0, cleaned
    cents, _display = parse_eur(cleaned)
    return cents, cleaned


def determine_listing_type(direct_buy: bool, vb_flag: bool, has_bid: bool) -> str:
    if has_bid and direct_buy:
        return "combo"
    if has_bid:
        return "auction"
    if direct_buy and vb_flag:
        return "fixed_vb"
    if direct_buy:
        return "fixed"
    if vb_flag:
        return "vb"
    return "unknown"


TRANSLATION_TABLE = str.maketrans({
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
})


def normalize_text(text: str) -> str:
    lowered = text.lower().translate(TRANSLATION_TABLE)
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def extract_set_numbers(text: str) -> List[str]:
    return list({match.group(0) for match in SET_NR_RE.finditer(text)})


def extract_parts_count(text: str) -> Optional[int]:
    match = PARTS_RE.search(text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


class SetMatcher:
    def __init__(self, records: List[SetRecord], *, threshold: float = 0.55) -> None:
        self.records = records
        self.records_by_nr = {record.set_nr: record for record in records}
        self.normalized_names = {record.set_nr: normalize_text(record.name) for record in records}
        self.threshold = threshold

    def match(self, title: str) -> Optional[SetMatchPayload]:
        if not title:
            return None

        normalized_title = normalize_text(title)
        numbers = extract_set_numbers(title)
        parts_in_title = extract_parts_count(title)

        candidate_nrs = set()
        for nr in numbers:
            if nr in self.records_by_nr:
                candidate_nrs.add(nr)

        if len(candidate_nrs) < 3:
            name_matches = process.extract(
                normalized_title,
                self.normalized_names,
                scorer=fuzz.token_set_ratio,
                limit=5,
            )
            for _matched_name, score, set_nr in name_matches:
                if score >= 50:
                    candidate_nrs.add(set_nr)

        best_score = 0.0
        best_info: Optional[Tuple[SetRecord, float, bool, float, Optional[int]]] = None

        for set_nr in candidate_nrs:
            record = self.records_by_nr.get(set_nr)
            if record is None:
                continue

            matched_number = set_nr in numbers
            name_score = fuzz.token_set_ratio(normalized_title, self.normalized_names[set_nr]) / 100.0

            parts_difference: Optional[int] = None
            if parts_in_title is not None and record.parts is not None:
                parts_difference = abs(record.parts - parts_in_title)

            score = 0.0
            if matched_number:
                score += 0.6
            score += 0.35 * name_score
            if parts_difference is not None:
                if parts_difference == 0:
                    score += 0.1
                elif parts_difference <= 5:
                    score += 0.05

            score = min(score, 1.0)

            if score > best_score:
                best_score = score
                best_info = (record, score, matched_number, name_score, parts_difference)

        if not best_info:
            return None

        record, score, matched_number, name_score, parts_difference = best_info

        return SetMatchPayload(
            set_nr=record.set_nr,
            set_name=record.name,
            score=score,
            matched_number=matched_number,
            name_score=name_score,
            parts_difference=parts_difference,
            above_threshold=score >= self.threshold,
        )


def format_set_line(match: Optional[SetMatchPayload]) -> str:
    if not match:
        return "-"
    status = "" if match.above_threshold else "?"
    probability = f"{match.score * 100:.1f}%"
    fragments = [f"{match.set_name} [{match.set_nr}{status}] {probability}"]
    details = []
    if match.matched_number:
        details.append("Nr")
    if match.parts_difference is not None:
        details.append(f"±{match.parts_difference}")
    if details:
        fragments.append("[" + ", ".join(details) + "]")
    return " ".join(fragments)


def handle_request_failed(request) -> None:
    url = request.url or ""
    if any(token in url for token in REQUEST_FAIL_IGNORES):
        return
    failure = request.failure if hasattr(request, "failure") else None
    failure_text = failure if failure else ""
    print(f"[reqfail] {request.method} {url} -> {failure_text}")

def query_key(terms: List[str]) -> str:
    return "+".join(terms)

# Round-trip Dedup Key (provider + id fallback href)
def make_round_key(provider_key: str, it: Dict[str, str]) -> str:
    return f"{provider_key}:{it.get('id') or it.get('href')}"


def search_identifier(search_cfg: Dict[str, Any]) -> str:
    return search_cfg.get("name") or query_key(search_cfg.get("terms", []))


async def notify_telegram(cfg: dict, title: str, msg: str, link: str):
    tg = cfg.get("telegram") or {}
    if not tg or not tg.get("enabled"):
        return
    token = tg.get("bot_token")
    chat_id = tg.get("chat_id")
    if not token or not chat_id:
        print("[tg] missing bot_token or chat_id")
        return

    text = f"<b>{title}</b>\n\n{msg}"

    api = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": bool(tg.get("disable_web_page_preview", True)),
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(api, json=payload)
            if r.status_code != 200:
                print(f"[telegram] failed {r.status_code}: {r.text}")
    except Exception as e:
        print(f"[tg] error: {e}")

async def notify_ntfy(cfg: dict, title: str, msg: str, link: str):
    ntfy_topic = cfg.get("ntfy_topic", "")
    if not ntfy_topic:
        print(f"[DRY-RUN] {title}\n{msg}\n{link}\n")
        return
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.post(ntfy_topic, data=msg, headers={
                "Title": title[:100],
                "Click": link,
                "Tags": "watch,new"
            })
            if r.status_code >= 300:
                print(f"[ntfy] {r.status_code} {r.text[:200]}")
        except Exception as e:
            print(f"[ntfy] error: {e}")

async def notify(cfg: dict, title: str, msg: str, link: str):
    # beides feuern; Fehler jeweils loggen
    #await notify_ntfy(cfg, title, msg, link)
    await notify_telegram(cfg, title, msg, link)


def build_notification_payload(
    provider_label: str,
    search_name: str,
    item: Dict[str, Any],
    *,
    is_ultra: bool = False,
) -> Tuple[str, str]:
    match: Optional[SetMatchPayload] = item.get("set_match_info")

    def esc(value: str) -> str:
        return html.escape(value, quote=False)

    listing_title = item.get("title") or ""
    bid_display = item.get("bid") or "-"
    buyout_display = item.get("buyout") or "-"
    shipping_note = item.get("shipping_note") or ""
    listing_type = item.get("listing_type") or "unknown"

    if match:
        set_headline = f"{match.set_name} ({match.set_nr} - {match.score * 100:.1f}%)"
        title_plain = f"[{provider_label}] {set_headline}"
        set_text = format_set_line(match)
        probability_value = f"{match.score * 100:.1f}%"
    else:
        title_plain = f"[{provider_label}] {search_name}"
        set_text = "-"
        probability_value = "–"

    if is_ultra:
        title_plain = "🚨⚠️‼️ULTRA‼️⚠️🚨 --- " + title_plain

    title = html.escape(title_plain, quote=False)

    lines = [
        f"Suche: {esc(search_name)}",
#        f"Set: {esc(set_text)}",
#        f"Wahrscheinlichkeit: {esc(probability_value)}",
        f"Inserat-Titel: {esc(listing_title)}",
        f"Typ: {esc(listing_type)}",
        f"Gebot: {esc(bid_display)}",
        f"Sofort: {esc(buyout_display)}",
    ]

    if shipping_note:
        lines.append(f"Versand: {esc(shipping_note)}")

    listing_id_raw = str(item.get("id") or "").strip()
    listing_href = item.get("href") or ""
    if listing_id_raw and listing_href:
        id_line = (
            f'ID: 👉 <a href="{html.escape(listing_href, quote=True)}">{html.escape(listing_id_raw)}</a>'
        )
    elif listing_id_raw:
        id_line = f"ID: {esc(listing_id_raw)}"
    else:
        id_line = "ID: –"
    lines.append(id_line)

    return title, "\n".join(lines)

async def navigate_with_fallback(page, url: str, *, log_prefix: str = ""):
    attempts = [
#        ("networkidle", 10_000),
#        ("domcontentloaded", 25_000),
        ("load", 10_000),
    ]
    last_err = None
    for wait_until, timeout in attempts:
        prefix = f"{log_prefix}" if log_prefix else ""
        print(f"{prefix}trying: {wait_until}: {url}")
        try:
            await page.goto(url, wait_until=wait_until, timeout=timeout)
            return
        except PWTimeout as exc:
            print(f"{prefix}[warn] goto timeout for {wait_until}: {url}")
            last_err = exc
    if last_err:
        raise last_err


async def create_browser_context(browser, cfg: Dict[str, Any]):
    record_video_dir = (
        "videos"
        if not bool(cfg.get("headless", True)) and bool(cfg.get("record_video", False))
        else None
    )
    return await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="de-DE",
        record_video_dir=record_video_dir,
    )

# =========================
# Provider: eBay
# =========================

def ebay_build_url(terms: List[str]) -> str:
    q = "+".join(urllib.parse.quote_plus(t) for t in terms)
    return f"https://www.ebay.de/sch/i.html?_nkw={q}&_sacat=0&_sop=10"

async def ebay_parse_results(page) -> List[Dict[str, Any]]:
    # Finde alle Karten/Links
    selectors = [
        "li.s-item a.s-item__link",
        "a.s-item__link",
        "ul.srp-results li a[href*='/itm/']",
        "a[href*='/itm/']",
    ]
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=5_000)
            links = await page.query_selector_all(sel)
            if links:
                break
        except PWTimeout:
            continue
    else:
        return []

    out: List[Dict[str, Any]] = []
    for a in links:
        href = (await a.get_attribute("href")) or ""
        m = ITEM_ID_EBAY_RE.search(href)
        if not m:
            continue
        item_id = m.group("id")
        title = (await a.text_content()) or ""
        li = await a.evaluate_handle("el => el.closest('li') || el.parentElement")

        bid_price = ""
        bid_cents: Optional[int] = None
        buy_price = ""
        buy_cents: Optional[int] = None
        item_vb_flag = False
        item_direct_buy = False
        shipping_text = ""
        shipping_cents: Optional[int] = None

        if li:
            rows = await li.query_selector_all(
                ".su-card-container__attributes__primary .s-card__attribute-row"
            )
            if rows:
                i = 0
                while i < len(rows):
                    row = rows[i]
                    txt = ((await row.text_content()) or "").strip()
                    low = txt.lower()

                    if low.startswith("+eur") or "lieferung" in low or "versand" in low:
                        if not shipping_text:
                            sc, sd = parse_shipping_info(txt)
                            shipping_text = sd
                            if sc is not None:
                                shipping_cents = sc
                        i += 1
                        continue

                    has_price_span = await row.query_selector(".s-card__price") is not None
                    is_price_like = ("eur" in low) or ("€" in txt)
                    if has_price_span or is_price_like:
                        price_txt = txt
                        vb_flag = False
                        mark_bid = False
                        mark_buy = False

                        j = i + 1
                        while j < len(rows):
                            nxt_txt = ((await rows[j].text_content()) or "").strip()
                            l2 = nxt_txt.lower()

                            if l2.startswith("+eur") or "lieferung" in l2 or "versand" in l2:
                                if not shipping_text:
                                    sc, sd = parse_shipping_info(nxt_txt)
                                    shipping_text = sd
                                    if sc is not None:
                                        shipping_cents = sc
                                j += 1
                                continue

                            has_next_price = await rows[j].query_selector(".s-card__price") is not None
                            is_next_price_like = ("eur" in l2) or ("€" in nxt_txt)
                            if has_next_price or is_next_price_like:
                                break

                            if "sofort-kaufen" in l2 or "buy it now" in l2:
                                mark_buy = True
                            elif "gebot" in l2 or "bids" in l2:
                                mark_bid = True
                            elif "preisvorschlag" in l2:
                                vb_flag = True

                            j += 1

                        cents, normalized = parse_eur(price_txt)
                        if normalized:
                            display_value = normalized
                            if vb_flag:
                                item_vb_flag = True
                                display_value = f"{display_value} (VB)"

                            if mark_bid:
                                if not bid_price:
                                    bid_price = display_value
                                    bid_cents = cents
                            elif mark_buy:
                                if not buy_price:
                                    buy_price = display_value
                                    buy_cents = cents
                                    item_direct_buy = True
                            else:
                                if not buy_price:
                                    buy_price = display_value
                                    buy_cents = cents
                                    item_direct_buy = True

                        i = j
                        continue

                    i += 1

            if not bid_price and not buy_price:
                for selector in [
                    ".s-item__price",
                    ".x-price-primary span",
                    ".s-item__detail--primary span.s-item__price",
                    "span[aria-label*='€']",
                ]:
                    try:
                        el = await li.query_selector(selector)
                    except Exception:
                        el = None
                    if el:
                        main_txt = (await el.text_content()) or ""
                        cents, normalized = parse_eur(main_txt)
                        if not normalized:
                            continue
                        li_text_full = ((await li.text_content()) or "").lower()
                        display_value = normalized
                        if any(token in li_text_full for token in ("preisvorschlag", "best offer", "vb")):
                            item_vb_flag = True
                            display_value = f"{display_value} (VB)"
                        if "gebot" in li_text_full or "bids" in li_text_full:
                            bid_price = display_value
                            bid_cents = cents
                        else:
                            buy_price = display_value
                            buy_cents = cents
                            item_direct_buy = True
                        break

            if not shipping_text and li:
                try:
                    ship_el = await li.query_selector(
                        ".s-item__shipping, .x-shipping-cost, .s-card__shipping, .s-item__logisticsCost"
                    )
                except Exception:
                    ship_el = None
                if ship_el:
                    shipping_raw = ((await ship_el.text_content()) or "").replace(" ", " ").strip()
                    sc, sd = parse_shipping_info(shipping_raw)
                    shipping_text = sd
                    if sc is not None:
                        shipping_cents = sc

        if buy_price:
            item_direct_buy = True

        listing_type = determine_listing_type(item_direct_buy, item_vb_flag, bid_cents is not None)

        if shipping_text:
            sc, sd = parse_shipping_info(shipping_text)
            if sc is not None:
                shipping_cents = sc
            shipping_text = sd

        out.append({
            "id": item_id,
            "title": title.strip(),
            "href": href.split("?")[0],
            "bid": bid_price,
            "bid_cents": bid_cents,
            "buyout": buy_price,
            "buyout_cents": buy_cents,
            "direct_buy": item_direct_buy,
            "vb_flag": item_vb_flag,
            "currency": "EUR",
            "shipping_note": shipping_text,
            "shipping_cents": shipping_cents,
            "listing_type": listing_type,
            "price_origin": "live",
            "settled_price_cents": None,
        })
    return out

# =========================
# Provider: Kleinanzeigen
# =========================

def ka_build_url(terms: List[str]) -> str:
    # Einfach & robust: Volltextsuche
    # https://www.kleinanzeigen.de/s-suchanfrage.html?keywords=term1+term2
    q = "-".join(urllib.parse.quote_plus(t) for t in terms)
    #return f"https://www.kleinanzeigen.de/s-suchanfrage.html?keywords={q}"
    
    #return f"https://www.kleinanzeigen.de/s-{q}/k0"
    return f"https://www.kleinanzeigen.de/anzeige:angebote/{q}/k0"

async def ka_parse_results(page) -> List[Dict[str, Any]]:
    try:
        await page.wait_for_selector("article.aditem", timeout=6000)
    except PWTimeout:
        return []

    cards = await page.query_selector_all("article.aditem")
    out: List[Dict[str, Any]] = []
    seen_ids = set()

    for card in cards:
        ad_id = await card.get_attribute("data-adid")
        href_rel = await card.get_attribute("data-href")
        if not ad_id or not href_rel:
            a = await card.query_selector("h2 a[href*='/s-anzeige/']")
            href_rel = await a.get_attribute("href") if a else None
            if not ad_id and href_rel:
                m = re.search(r"/s-anzeige/.*?/(\d+)-", href_rel)
                ad_id = m.group(1) if m else None
        if not ad_id or not href_rel or ad_id in seen_ids:
            continue
        seen_ids.add(ad_id)

        href = "https://www.kleinanzeigen.de" + href_rel if href_rel.startswith("/") else href_rel
        title_el = await card.query_selector("h2 a")
        title = ((await title_el.text_content()) or "").strip() if title_el else ""

        price_el = await card.query_selector(".aditem-main--middle--price-shipping--price")
        price_txt = ((await price_el.text_content()) or "").replace(" ", " ").strip() if price_el else ""
        vb_flag = ("vb" in price_txt.lower()) or ("preisvorschlag" in price_txt.lower())
        price_cents, price_display = parse_eur(price_txt)
        if price_display and vb_flag and "(VB)" not in price_display:
            price_display = f"{price_display} (VB)"

        shipping_el = await card.query_selector(".aditem-main--middle--price-shipping--shipping")
        shipping_raw = ((await shipping_el.text_content()) or "").replace(" ", " ").strip() if shipping_el else ""
        shipping_cents, shipping_text = parse_shipping_info(shipping_raw)
        if not shipping_text:
            body_text = ((await card.text_content()) or "")
            if "Nur Abholung" in body_text:
                shipping_text = "Nur Abholung"
            elif "Versand möglich" in body_text:
                shipping_text = "Versand möglich"

        direct_buy = False
        try:
            tag = await card.query_selector(".simpletag:has-text('Direkt kaufen'), span:has-text('Direkt kaufen')")
            if tag:
                direct_buy = True
            else:
                ctext = ((await card.text_content()) or "").lower()
                if "direkt kaufen" in ctext:
                    direct_buy = True
        except Exception:
            pass

        bid_price = ""
        bid_cents = None
        buy_price = ""
        buy_cents = None
        if price_display:
            if direct_buy or not vb_flag:
                buy_price = price_display
                buy_cents = price_cents
            else:
                bid_price = price_display
                bid_cents = price_cents

        listing_type = determine_listing_type(direct_buy, vb_flag, bid_cents is not None)

        out.append({
            "id": ad_id,
            "title": title,
            "href": href,
            "bid": bid_price,
            "bid_cents": bid_cents,
            "buyout": buy_price,
            "buyout_cents": buy_cents,
            "direct_buy": direct_buy,
            "vb_flag": vb_flag,
            "currency": "EUR",
            "shipping_note": shipping_text,
            "shipping_cents": shipping_cents,
            "listing_type": listing_type,
            "price_origin": "live",
            "settled_price_cents": None,
        })

    return out


# =========================
# Provider Registry
# =========================
Provider = Dict[str, Callable[..., Any]]

PROVIDERS: Dict[str, Dict[str, Callable[..., Any]]] = {
    "ebay": {
        "build_url": ebay_build_url,
        "parse": ebay_parse_results,
        "label": "🅔eBay",
    },
    "kleinanzeigen": {
        "build_url": ka_build_url,
        "parse": ka_parse_results,
        "label": "🟢KA",
    },
}

# =========================
# Scrape One (per provider)
# =========================
async def scrape_provider(
    page,
    provider_key: str,
    terms: List[str],
    exclude_terms: Optional[List[str]] = None,
    *,
    log_prefix: str = "",
) -> Tuple[str, List[Dict[str, Any]]]:
    p = PROVIDERS[provider_key]
    url = p["build_url"](terms)
    await navigate_with_fallback(page, url, log_prefix=log_prefix)
    items = await p["parse"](page)
    # Terms-Filter: jeder Begriff muss im Titel vorkommen
    filtered = []
    excludes = [t.lower() for t in (exclude_terms or [])]
    for it in items:
        tl = it["title"].lower()
        if all(t.lower() in tl for t in terms):
            if excludes and any(ex in tl for ex in excludes):
                continue
            filtered.append(it)
    return url, filtered

# =========================
# Run Loop
# =========================
async def execute_search(
    page,
    cfg: Dict[str, Any],
    database: Database,
    set_matcher: SetMatcher,
    search_cfg: Dict[str, Any],
    *,
    reported_keys: Optional[set] = None,
    log_prefix: str = "",
) -> None:
    name = search_identifier(search_cfg)
    prefix_base = f"{log_prefix}{name}".strip()
    prefix = f"{time.strftime('%H:%M:%S')} {prefix_base}" if prefix_base else time.strftime("%H:%M:%S")
    tags = {str(tag).lower() for tag in search_cfg.get("tags", [])}
    is_ultra = bool(tags & ULTRA_PRIORITY_TAGS)
    terms = search_cfg.get("terms") or []
    if not terms:
        print(f"{prefix}: [skip] ohne terms")
        return

    exclude_terms = search_cfg.get("exclude_terms", [])
    providers = search_cfg.get("providers", ["ebay"])

    for provider_key in providers:
        if provider_key not in PROVIDERS:
            print(f"{prefix}: [skip] unknown provider: {provider_key}")
            continue

        try:
            url, items = await scrape_provider(
                page,
                provider_key,
                terms,
                exclude_terms,
                log_prefix=f"{prefix}: " if prefix else "",
            )
        except PWTimeout:
            print(f"{prefix}: [warn] navigation failed, skipping {provider_key}:{terms}")
            continue

        new_items: List[Dict[str, Any]] = []
        for it in items:
            if not it.get("id"):
                continue
            match_payload = set_matcher.match(it.get("title", ""))
            if match_payload:
                it["set_match_info"] = match_payload
            payload = {
                "provider_item_id": str(it.get("id")),
                "title": it.get("title", ""),
                "href": it.get("href", ""),
                "direct_buy": bool(it.get("direct_buy")),
                "vb_flag": bool(it.get("vb_flag")),
                "bid_cents": it.get("bid_cents"),
                "buyout_cents": it.get("buyout_cents"),
                "currency": it.get("currency", "EUR"),
                "availability": "active",
                "listing_type": it.get("listing_type"),
                "shipping_cents": it.get("shipping_cents"),
                "shipping_note": it.get("shipping_note"),
                "price_origin": it.get("price_origin"),
                "settled_price_cents": it.get("settled_price_cents"),
                "set_match": match_payload if match_payload and match_payload.above_threshold else None,
            }
            try:
                db_result = await database.upsert_item_and_snapshot(
                    provider_key,
                    PROVIDERS[provider_key]["label"],
                    payload,
                )
                it["_db_result"] = db_result
                if db_result.is_new_item:
                    new_items.append(it)
            except Exception as exc:
                print(f"{prefix}: [db] upsert failed for {provider_key}:{it.get('id')}: {exc}")

        if new_items:
            for it in new_items:
                provider_label = PROVIDERS[provider_key]["label"]
                title, msg = build_notification_payload(
                    provider_label,
                    name,
                    it,
                    is_ultra=is_ultra,
                )
                gk = make_round_key(provider_key, it)
                if reported_keys is not None and gk in reported_keys:
                    print(f"{prefix}: [skip-round] already reported this round: {gk}")
                    continue
                await notify(cfg, title, msg, it["href"])
                if reported_keys is not None:
                    reported_keys.add(gk)

        print(f"{prefix} [{provider_key}]: {len(new_items)} neu, {len(items)} gesamt")


async def run_once(
    browser,
    cfg: Dict[str, Any],
    database: Database,
    set_matcher: SetMatcher,
    searches: List[Dict[str, Any]],
):
    if not searches:
        return

    context = await create_browser_context(browser, cfg)
    page = await context.new_page()
    page.on("pageerror", lambda e: print(f"[pageerror] {e}"))
    page.on("requestfailed", handle_request_failed)
    page.on("response", lambda r: r.status >= 400 and print(f"[resp {r.status}] {r.url}"))

    # In-Memory De-Dup for a single run_once roundtrip
    reported_this_round = set()

    try:
        for search_cfg in searches:
            await execute_search(
                page,
                cfg,
                database,
                set_matcher,
                search_cfg,
                reported_keys=reported_this_round,
                log_prefix="[main] ",
            )
    finally:
        await context.close()


class SharedState:
    def __init__(self) -> None:
        self.cfg: Dict[str, Any] = {}
        self.set_matcher: Optional[SetMatcher] = None
        self.priority_searches: Dict[str, Dict[str, Any]] = {}


async def priority_search_loop(
    search_key: str,
    shared_state: "SharedState",
    browser,
    database: Database,
):
    context = None
    page = None
    try:
        while True:
            search_cfg = shared_state.priority_searches.get(search_key)
            if search_cfg is None:
                await asyncio.sleep(1)
                if shared_state.priority_searches.get(search_key) is None:
                    break
                continue

            if context is None:
                context = await create_browser_context(browser, shared_state.cfg)
                page = await context.new_page()
                page.on("pageerror", lambda e: print(f"[pageerror] {e}"))
                page.on("requestfailed", handle_request_failed)
                page.on("response", lambda r: r.status >= 400 and print(f"[resp {r.status}] {r.url}"))

            matcher = shared_state.set_matcher
            if matcher is None:
                await asyncio.sleep(1)
                continue

            cfg_snapshot = shared_state.cfg or {}
            search_name = search_identifier(search_cfg)
            log_prefix = f"[prio:{search_name}] "
            try:
                await execute_search(
                    page,
                    cfg_snapshot,
                    database,
                    matcher,
                    search_cfg,
                    reported_keys=None,
                    log_prefix=log_prefix,
                )
            except Exception as exc:
                print(f"{log_prefix}error: {exc}")

            interval_value = search_cfg.get("interval_seconds") or cfg_snapshot.get("interval_seconds", 60)
            try:
                interval_seconds = int(interval_value)
            except (TypeError, ValueError):
                interval_seconds = 60
            await asyncio.sleep(max(1, interval_seconds))
    except asyncio.CancelledError:
        pass
    finally:
        if context is not None:
            await context.close()

async def main():
    CONFIG_PATH = Path("config.yml")

    cfg = yaml.safe_load(CONFIG_PATH.read_text()) or {}
    db_cfg = cfg.get("database") or {}
    db_url = db_cfg.get("url")
    if not db_url:
        raise RuntimeError("config.yml missing database.url for Postgres connection")

    database = Database(db_url, echo=bool(db_cfg.get("echo", False)))
    await database.init_models()

    shared_state = SharedState()
    shared_state.cfg = cfg

    try:
        async with async_playwright() as p:
            # initiale Config nur für Browser-Start (headless/devtools)
            headless = bool(cfg.get("headless", True))

            browser = await p.chromium.launch(
                headless=headless,
                slow_mo=0 if headless else 300,
                args=["--disable-blink-features=AutomationControlled"],
                devtools=not headless,
            )
            try:
                priority_tasks: Dict[str, asyncio.Task] = {}
                while True:
                    # Config NEU laden (für searches, ntfy_topic, interval_seconds, etc.)
                    try:
                        new_cfg = yaml.safe_load(CONFIG_PATH.read_text()) or {}
                        new_db_url = (new_cfg.get("database") or {}).get("url")
                        if new_db_url and new_db_url != db_url:
                            print("[warn] database.url change detected – restart watcher to apply")
                        cfg = new_cfg
                    except Exception as e:
                        print(f"[warn] config reload failed: {e} (verwende letzte gültige)")

                    shared_state.cfg = cfg

                    set_records = await database.fetch_set_records()
                    matcher = SetMatcher(set_records)
                    shared_state.set_matcher = matcher

                    searches = cfg.get("searches", []) or []
                    priority_map: Dict[str, Dict[str, Any]] = {}
                    normal_searches: List[Dict[str, Any]] = []

                    for search_cfg in searches:
                        tags = {str(tag).lower() for tag in search_cfg.get("tags", [])}
                        if tags & ULTRA_PRIORITY_TAGS:
                            key = search_identifier(search_cfg)
                            priority_map[key] = {**search_cfg}
                        else:
                            normal_searches.append(search_cfg)

                    shared_state.priority_searches = priority_map

                    # Cancel priority loops no longer configured
                    cancelled_tasks: List[asyncio.Task] = []
                    for key in list(priority_tasks.keys()):
                        if key not in priority_map:
                            task = priority_tasks.pop(key)
                            task.cancel()
                            cancelled_tasks.append(task)
                    if cancelled_tasks:
                        await asyncio.gather(*cancelled_tasks, return_exceptions=True)

                    # Clean up finished tasks and optionally restart
                    for key, task in list(priority_tasks.items()):
                        if task.done():
                            if not task.cancelled():
                                exc = task.exception()
                                if exc:
                                    print(f"[prio:{key}] task ended with error: {exc}")
                            priority_tasks.pop(key, None)

                    # Start new priority loops
                    for key in priority_map.keys():
                        if key not in priority_tasks:
                            priority_tasks[key] = asyncio.create_task(
                                priority_search_loop(key, shared_state, browser, database)
                            )

                    await run_once(browser, cfg, database, matcher, normal_searches)

                    interval = int(cfg.get("interval_seconds", 60))
                    await asyncio.sleep(max(1, interval))
            finally:
                for task in priority_tasks.values():
                    task.cancel()
                if priority_tasks:
                    await asyncio.gather(*priority_tasks.values(), return_exceptions=True)
                await browser.close()
    finally:
        await database.dispose()



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
