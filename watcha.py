import asyncio, json, re, sys, time, urllib.parse
from pathlib import Path
from typing import List, Dict, Any, Tuple, Callable
import yaml, httpx
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# =========================
# Setup & Utilities
# =========================
STATE_DIR = Path("state")
STATE_DIR.mkdir(exist_ok=True)

ITEM_ID_EBAY_RE = re.compile(r"/itm/(?:[^/]+/)?(?P<id>\d{9,15})")
ITEM_ID_KA_RE   = re.compile(r"/s-anzeige/.*?/(\d+)")
PRICE_RE        = re.compile(r"([\d\.\s,]+)\s*€|EUR\s*([\d\.\s,]+)")

def parse_eur(text: str) -> str:
    if not text:
        return ""
    t = text.replace("\xa0", " ")
    m = PRICE_RE.search(t)
    if not m:
        return ""
    raw = (m.group(1) or m.group(2) or "").strip()
    val = raw.replace(".", "").replace(" ", "").replace(",", ".")
    try:
        return f"{float(val):.2f} €"
    except:
        return ""

def query_key(terms: List[str]) -> str:
    return "+".join(terms)

def load_seen(key: str) -> set:
    p = STATE_DIR / f"{key}.json"
    if not p.exists():
        return set()
    try:
        return set(json.loads(p.read_text()))
    except Exception:
        return set()

def save_seen(key: str, seen: set):
    (STATE_DIR / f"{key}.json").write_text(json.dumps(sorted(seen)))

# Round-trip Dedup Key (provider + id fallback href)
def make_round_key(provider_key: str, it: Dict[str, str]) -> str:
    return f"{provider_key}:{it.get('id') or it.get('href')}"


async def notify_telegram(cfg: dict, title: str, msg: str, link: str):
    tg = cfg.get("telegram") or {}
    if not tg or not tg.get("enabled"):
        return
    token = tg.get("bot_token")
    chat_id = tg.get("chat_id")
    if not token or not chat_id:
        print("[tg] missing bot_token or chat_id")
        return

    text = (
        f"<b>{title}</b>\n"
        f"{msg}\n\n"
        f'<a href="{link}">👉 Zum Angebot</a>'
    )

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

async def navigate_with_fallback(page, url: str):
    attempts = [
#        ("networkidle", 10_000),
#        ("domcontentloaded", 25_000),
        ("load", 10_000),
    ]
    last_err = None
    for wait_until, timeout in attempts:
        print(f"trying: {wait_until}: {url}")
        try:
            await page.goto(url, wait_until=wait_until, timeout=timeout)
            return
        except PWTimeout as exc:
            print(f"[warn] goto timeout for {wait_until}: {url}")
            last_err = exc
    if last_err:
        raise last_err

# =========================
# Provider: eBay
# =========================

def ebay_build_url(terms: List[str]) -> str:
    q = "+".join(urllib.parse.quote_plus(t) for t in terms)
    return f"https://www.ebay.de/sch/i.html?_nkw={q}&_sacat=0&_sop=10"

async def ebay_parse_results(page) -> List[Dict[str, str]]:
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

    out = []
    for a in links:
        href = (await a.get_attribute("href")) or ""
        m = ITEM_ID_EBAY_RE.search(href)
        if not m:
            continue
        item_id = m.group("id")
        title = (await a.text_content()) or ""
        # Karte
        li = await a.evaluate_handle("el => el.closest('li') || el.parentElement")
        bid_price = ""
        buy_price = ""

        if li:
            # Primär: neues Kartenlayout (dein Beispiel mit "su-card...")
            rows = await li.query_selector_all(
                ".su-card-container__attributes__primary .s-card__attribute-row"
            )
            if rows:
                i = 0
                while i < len(rows):
                    row = rows[i]
                    txt = ((await row.text_content()) or "").strip()
                    low = txt.lower()

                    # Lieferkosten ignorieren
                    if low.startswith("+eur") or "lieferung" in low:
                        i += 1
                        continue

                    # Preiszeile?
                    has_price_span = await row.query_selector(".s-card__price") is not None
                    is_price_like  = ("eur" in low) or ("€" in txt)
                    if has_price_span or is_price_like:
                        price_txt = txt
                        vb_flag = False
                        mark_bid = False
                        mark_buy = False

                        j = i + 1
                        while j < len(rows):
                            nxt_txt = ((await rows[j].text_content()) or "").strip()
                            l2 = nxt_txt.lower()

                            if l2.startswith("+eur") or "lieferung" in l2:
                                j += 1
                                continue

                            # Nächster Preis? -> stop Labelsammeln
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

                        pnorm = parse_eur(price_txt)
                        if vb_flag and pnorm:
                            pnorm = pnorm + " (VB)"

                        if mark_bid and pnorm:
                            if not bid_price:
                                bid_price = pnorm
                        elif mark_buy and pnorm:
                            if not buy_price:
                                buy_price = pnorm
                        else:
                            # kein explizites Label -> Festpreis
                            if not buy_price and pnorm:
                                buy_price = pnorm

                        i = j
                        continue

                    i += 1

            # Fallback: ältere/andere Layouts
            if not bid_price and not buy_price:
                for psel in [
                    ".s-item__price",
                    ".x-price-primary span",
                    ".s-item__detail--primary span.s-item__price",
                    "span[aria-label*='€']",
                ]:
                    try:
                        el = await li.query_selector(psel)
                        if el:
                            main_txt = (await el.text_content()) or ""
                            main_norm = parse_eur(main_txt)
                            if main_norm:
                                li_text = ((await li.text_content()) or "").lower()
                                if "gebot" in li_text or "bids" in li_text:
                                    bid_price = main_norm
                                else:
                                    buy_price = main_norm
                                break
                    except Exception:
                        pass

        out.append({
            "id": item_id,
            "title": title.strip(),
            "href": href.split("?")[0],
            "bid": bid_price,
            "buyout": buy_price,
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

async def ka_parse_results(page) -> List[Dict[str, str]]:
    try:
        await page.wait_for_selector("article.aditem", timeout=6000)
    except PWTimeout:
        return []

    cards = await page.query_selector_all("article.aditem")
    out, seen_ids = [], set()

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

        # Preis + VB
        price_el = await card.query_selector(".aditem-main--middle--price-shipping--price")
        price_txt = ((await price_el.text_content()) or "").replace("\xa0", " ").strip() if price_el else ""
        vb_flag = ("vb" in price_txt.lower()) or ("preisvorschlag" in price_txt.lower())
        price_norm = parse_eur(price_txt)
        if price_norm and vb_flag and "(VB)" not in price_norm:
            price_norm = price_norm + " (VB)"

        # Direkt kaufen?
        direct_buy = False
        try:
            # robust: spezifischer Tag ODER Volltextsuche im Card-Text
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
        buy_price = ""
        if price_norm:
            if direct_buy:
                buy_price = price_norm
            else:
                bid_price = price_norm

        out.append({
            "id": ad_id,
            "title": title,
            "href": href,
            "bid": bid_price,
            "buyout": buy_price,
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
async def scrape_provider(page, provider_key: str, terms: List[str]) -> Tuple[str, List[Dict[str, str]]]:
    p = PROVIDERS[provider_key]
    url = p["build_url"](terms)
    await navigate_with_fallback(page, url)
    items = await p["parse"](page)
    # Terms-Filter: jeder Begriff muss im Titel vorkommen
    filtered = []
    for it in items:
        tl = it["title"].lower()
        if all(t.lower() in tl for t in terms):
            filtered.append(it)
    return url, filtered

# =========================
# Run Loop
# =========================
async def run_once(browser, cfg):
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="de-DE",
        record_video_dir="videos" if not bool(cfg.get("headless", True)) and bool(cfg.get("record_video", False)) else None,
    )
    page = await context.new_page()
    page.on("pageerror", lambda e: print(f"[pageerror] {e}"))
    page.on("requestfailed", lambda r: print(f"[reqfail] {r.method} {r.url} -> {r.failure if r.failure else ''}"))
    page.on("response", lambda r: r.status >= 400 and print(f"[resp {r.status}] {r.url}"))

    # In-Memory De-Dup for a single run_once roundtrip
    reported_this_round = set()

    try:
        for s in cfg["searches"]:
            name = s.get("name") or query_key(s["terms"])
            terms = s["terms"]
            providers = s.get("providers", ["ebay"])  # default eBay

            for provider_key in providers:
                if provider_key not in PROVIDERS:
                    print(f"[skip] unknown provider: {provider_key}")
                    continue

                # State pro provider+query
                state_key = f"{provider_key}-{query_key(terms)}"
                seen = load_seen(state_key)

                try:
                    url, items = await scrape_provider(page, provider_key, terms)
                except PWTimeout:
                    print(f"[warn] navigation failed, skipping {provider_key}:{terms}")
                    continue

                new_items = [it for it in items if it["id"] not in seen]
                if new_items:
                    for it in new_items:
                        prov_label = PROVIDERS[provider_key]["label"]
                        title = f"[{prov_label}] {name}"
                        msg = (
                            f"{it['title']}\n"
                            f"Gebot: {it.get('bid') or '-'}\n"
                            f"Sofort: {it.get('buyout') or '-'}\n"
                            f"ID: {it['id']}"
                        )
                        gk = make_round_key(provider_key, it)
                        seen.add(it["id"])
                        if gk in reported_this_round:
                            print(f"[skip-round] already reported this round: {gk}")
                            continue
                        await notify(cfg, title, msg, it["href"])
                        reported_this_round.add(gk)
                    save_seen(state_key, seen)

                print(f"{time.strftime('%H:%M:%S')} {name} [{provider_key}]: {len(new_items)} neu, {len(items)} gesamt")
    finally:
        await context.close()

async def main():
    CONFIG_PATH = Path("config.yml")

    async with async_playwright() as p:
        # initiale Config nur für Browser-Start (headless/devtools)
        cfg = yaml.safe_load(CONFIG_PATH.read_text()) or {}
        headless = bool(cfg.get("headless", True))

        browser = await p.chromium.launch(
            headless=headless,
            slow_mo=0 if headless else 300,
            args=["--disable-blink-features=AutomationControlled"],
            devtools=not headless,
        )
        try:
            while True:
                # Config NEU laden (für searches, ntfy_topic, interval_seconds, etc.)
                try:
                    new_cfg = yaml.safe_load(CONFIG_PATH.read_text()) or {}
                    cfg = new_cfg
                except Exception as e:
                    print(f"[warn] config reload failed: {e} (verwende letzte gültige)")

                await run_once(browser, cfg)

                interval = int(cfg.get("interval_seconds", 60))
                await asyncio.sleep(interval)
        finally:
            await browser.close()



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
