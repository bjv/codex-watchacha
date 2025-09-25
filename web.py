from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
import json
import re

import statistics

import yaml
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import Select, func, select, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from rapidfuzz import fuzz, process

from db import (
    COMPLETED_AVAILABILITIES,
    DEFAULT_AVAILABILITY,
    Database,
    Item,
    ItemSetMatch,
    Provider,
    Set,
    Snapshot,
)


CONFIG_PATH = Path("config.yml")
templates = Jinja2Templates(directory="templates")


app = FastAPI(title="Watchacha Dashboard")


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise RuntimeError("config.yml missing – create it from config.yml.sample")
    return yaml.safe_load(CONFIG_PATH.read_text()) or {}


database: Optional[Database] = None


SORT_CONFIGS = {
    "title": {
        "label": "Listing",
        "asc": [Item.title.asc(), Item.id.asc()],
        "desc": [Item.title.desc(), Item.id.desc()],
    },
    "price": {
        "label": "Preis",
        "asc": [func.coalesce(Item.current_buyout_cents, Item.current_bid_cents).asc(), Item.id.asc()],
        "desc": [func.coalesce(Item.current_buyout_cents, Item.current_bid_cents).desc(), Item.id.desc()],
    },
    "last_seen": {
        "label": "Gesehen",
        "asc": [Item.last_seen_ts.asc(), Item.id.asc()],
        "desc": [Item.last_seen_ts.desc(), Item.id.desc()],
    },
    "set": {
        "label": "Set",
        "asc": [ItemSetMatch.set_nr.asc(), Item.id.asc()],
        "desc": [ItemSetMatch.set_nr.desc(), Item.id.desc()],
    },
}

AVAILABILITY_LABELS = {
    "active": "Aktiv",
    "reserved": "Reserviert",
    "deleted": "Gelöscht",
    "sold": "Verkauft",
    "unsold": "Beendet",
    "unknown": "Unbekannt",
}

LISTING_TYPE_LABELS = {
    "fixed": "Festpreis",
    "fixed_vb": "Festpreis + VB",
    "vb": "Verhandlungsbasis",
    "auction": "Auktion",
    "combo": "Auktion + Sofortkauf",
    "unknown": "Unbekannt",
}

PRICE_ORIGIN_LABELS = {
    "live": "Aktives Angebot",
    "sold_final": "Verkauft (eBay)",
    "reserved_estimate": "Reserviert (Kleinanzeigen)",
    "reserved_unknown": "Reserviert (Preis unbekannt)",
    "unsold": "Beendet ohne Verkauf",
}

SNAPSHOT_SOURCE_LABELS = {
    "scrape": "Suche",
    "revisit": "Revisit",
}


DEFAULT_LISTING_FILTERS = {
    "q": "",
    "provider": "",
    "availability": "",
    "set_nr": "",
    "show_hidden": False,
    "without_set": False,
    "limit": 50,
}
FILTER_COOKIE_NAME = "listing_filters"
FILTER_COOKIE_MAX_AGE = 60 * 60 * 24 * 30


SET_NR_RE = re.compile(r"\b\d{5,6}\b")
PARTS_RE = re.compile(r"(\d{2,4})\s*(teile|piece|pieces|pcs|stk|stück)", re.IGNORECASE)
TRANSLATION_TABLE = str.maketrans({
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
})
FUZZY_STOPWORDS = {"bluebrixx", "blue", "brixx", "star", "trek", "pro"}


def _normalize_text(value: str) -> str:
    lowered = value.lower().translate(TRANSLATION_TABLE)
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    normalized = re.sub(r"\s+", " ", lowered).strip()
    if not normalized:
        return normalized
    tokens = [token for token in normalized.split() if token not in FUZZY_STOPWORDS]
    return " ".join(tokens)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _coerce_str(value: Optional[str]) -> str:
    return (value or "").strip()


def _coerce_limit(value: Any) -> int:
    if value is None:
        return DEFAULT_LISTING_FILTERS["limit"]
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        return DEFAULT_LISTING_FILTERS["limit"]
    return max(1, min(200, ivalue))


def _extract_set_numbers(text: str) -> List[str]:
    return list({match.group(0) for match in SET_NR_RE.finditer(text)})


def _extract_parts_count(text: str) -> Optional[int]:
    match = PARTS_RE.search(text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _score_set_candidates(
    title: str,
    sets: Dict[str, Tuple[str, Optional[int]]],
    *,
    threshold: float = 0.2,
) -> Optional[Dict[str, Any]]:
    if not title or not sets:
        return None

    normalized_title = _normalize_text(title)
    numbers = _extract_set_numbers(title)
    parts_in_title = _extract_parts_count(title)

    normalized_names = {
        set_nr: _normalize_text(name or "")
        for set_nr, (name, _parts) in sets.items()
    }

    candidate_nrs = {nr for nr in numbers if nr in sets}

    if len(candidate_nrs) < 3:
        name_matches = process.extract(
            normalized_title,
            {set_nr: normalized_names[set_nr] for set_nr in sets},
            scorer=fuzz.token_set_ratio,
            limit=5,
        )
        for matched_name, score, set_nr in name_matches:
            if score >= 50:
                candidate_nrs.add(set_nr)

    best_score = 0.0
    best_payload: Optional[Dict[str, Any]] = None

    for set_nr in candidate_nrs:
        name, parts = sets[set_nr]
        matched_number = set_nr in numbers
        name_ratio = fuzz.token_set_ratio(normalized_title, normalized_names[set_nr]) / 100.0

        parts_difference: Optional[int] = None
        if parts_in_title is not None and parts is not None:
            parts_difference = abs(parts - parts_in_title)

        score = 0.0
        if matched_number:
            score += 0.6
        score += 0.5 * name_ratio
        if parts_difference is not None:
            if parts_difference == 0:
                score += 0.1
            elif parts_difference <= 5:
                score += 0.05

        score = min(score, 1.0)

        if score > best_score:
            best_score = score
            best_payload = {
                "set_nr": set_nr,
                "name": name,
                "score": score,
                "matched_number": matched_number,
                "name_score": name_ratio,
                "parts_difference": parts_difference,
            }

    if not best_payload or best_score < threshold:
        return None

    return best_payload


def _cents_to_eur(cents: Optional[int]) -> str:
    if cents is None:
        return "-"
    value = cents / 100
    return f"{value:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")


def _format_date(value: Optional[Any]) -> str:
    if not value:
        return "-"
    return value.strftime("%Y-%m-%d")


def _availability_label(value: Optional[str]) -> str:
    normalized = value or DEFAULT_AVAILABILITY
    return AVAILABILITY_LABELS.get(normalized, normalized.replace("_", " ").title())


def _format_shipping(cents: Optional[int], note: Optional[str]) -> str:
    if cents is None and not note:
        return "-"
    parts: List[str] = []
    if cents is not None:
        parts.append("Kostenlos" if cents == 0 else _cents_to_eur(cents))
    if note:
        clean = note.strip()
        if clean:
            parts.append(clean)
    return " | ".join(parts) if parts else "-"


def _snapshot_source_label(value: Optional[str]) -> str:
    normalized = (value or "scrape").strip().lower()
    return SNAPSHOT_SOURCE_LABELS.get(normalized, normalized.replace("_", " ").title())


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if database is None:
        raise HTTPException(status_code=503, detail="database connection is not ready")
    async with database.session_factory() as session:
        yield session


@app.on_event("startup")
async def on_startup() -> None:
    global database
    cfg = load_config()
    db_cfg = cfg.get("database") or {}
    db_url = db_cfg.get("url")
    if not db_url:
        raise RuntimeError("config.yml missing database.url for Postgres connection")

    database = Database(db_url, echo=bool(db_cfg.get("echo", False)))
    await database.init_models()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if database is not None:
        await database.dispose()


def _base_items_query() -> Select[Any]:
    return (
        select(
            Item,
            Provider.label.label("provider_label"),
            ItemSetMatch.set_nr.label("set_nr"),
            ItemSetMatch.score.label("set_score"),
            ItemSetMatch.name_score.label("set_name_score"),
            ItemSetMatch.matched_number.label("set_matched_number"),
            ItemSetMatch.parts_difference.label("set_parts_difference"),
            ItemSetMatch.manual_override.label("set_manual_override"),
            ItemSetMatch.original_score.label("set_original_score"),
            Set.name.label("set_name"),
            Item.hidden.label("hidden"),
        )
        .join(Provider, Provider.key == Item.provider_key)
        .outerjoin(ItemSetMatch, ItemSetMatch.item_id == Item.id)
        .outerjoin(Set, Set.set_nr == ItemSetMatch.set_nr)
    )


async def _fetch_providers(session: AsyncSession) -> List[Provider]:
    result = await session.execute(select(Provider).order_by(Provider.label))
    return [row[0] for row in result.all()]


def _apply_item_filters(
    stmt: Select[Any],
    *,
    query: Optional[str],
    provider: Optional[str],
    availability: Optional[str],
    set_nr: Optional[str],
    include_hidden: bool,
    only_unassigned: bool,
) -> Select[Any]:
    if query:
        term = query.strip()
        like_term = f"%{term}%"
        filters = [
            Item.title.ilike(like_term),
            Item.provider_item_id.ilike(like_term),
        ]
        if term.isdigit():
            try:
                filters.append(Item.id == int(term))
            except ValueError:
                pass
        stmt = stmt.where(or_(*filters))
    if provider:
        stmt = stmt.where(Item.provider_key == provider)
    if availability:
        if availability == DEFAULT_AVAILABILITY:
            stmt = stmt.where(or_(Item.availability == availability, Item.availability.is_(None)))
        else:
            stmt = stmt.where(Item.availability == availability)
    if only_unassigned:
        stmt = stmt.where(ItemSetMatch.item_id.is_(None))
    elif set_nr:
        stmt = stmt.where(ItemSetMatch.set_nr == set_nr)
    if not include_hidden:
        stmt = stmt.where(Item.hidden.is_(False))
    return stmt


def _apply_sort(stmt: Select[Any], sort_by: Optional[str], sort_dir: Optional[str]) -> Select[Any]:
    if sort_by in SORT_CONFIGS and sort_dir in {"asc", "desc"}:
        return stmt.order_by(*SORT_CONFIGS[sort_by][sort_dir])
    default = SORT_CONFIGS["last_seen"]
    return stmt.order_by(*default["desc"])


def _best_price(item: Item) -> Optional[int]:
    prices = [p for p in (item.current_buyout_cents, item.current_bid_cents) if p is not None]
    return min(prices) if prices else None


def _round_avg_to_cents(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    return int(round(float(value)))


def _median_cents(values: List[int]) -> Optional[int]:
    if not values:
        return None
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2 == 1:
        return sorted_vals[mid]
    return int(round((sorted_vals[mid - 1] + sorted_vals[mid]) / 2))


_SPARKLINE_BLOCKS = ["▁", "▂", "▃", "▅", "▇"]


def _sparkline_from_prices(values: List[int]) -> str:
    if not values:
        return ""
    sorted_vals = sorted(values)
    sample = sorted_vals
    if len(sorted_vals) > 20:
        step = (len(sorted_vals) - 1) / 19
        sample = [sorted_vals[int(round(i * step))] for i in range(20)]
    min_val = sample[0]
    max_val = sample[-1]
    if min_val == max_val:
        return _SPARKLINE_BLOCKS[-1] * min(len(sample), 10)
    rng = max_val - min_val
    if rng == 0:
        return _SPARKLINE_BLOCKS[-1] * len(sample)
    blocks = []
    for val in sample:
        idx = int(((val - min_val) / rng) * 4)
        idx = max(0, min(idx, 4))
        blocks.append(_SPARKLINE_BLOCKS[idx])
    return "".join(blocks)


def _build_sort_state(
    sort_configs: Dict[str, Dict[str, Any]],
    current_by: Optional[str],
    current_dir: Optional[str],
    request: Request,
) -> Dict[str, Dict[str, Optional[str]]]:
    base_params = [
        (k, v)
        for k, v in request.query_params.multi_items()
        if k not in {"sort_by", "sort_dir"}
    ]

    sort_state: Dict[str, Dict[str, Optional[str]]] = {}
    for key, config in sort_configs.items():
        current = current_dir if current_by == key else None
        if current_by == key:
            if current_dir == "asc":
                next_dir = "desc"
            elif current_dir == "desc":
                next_dir = None
            else:
                next_dir = "asc"
        else:
            next_dir = "asc"

        params = list(base_params)
        if next_dir:
            params.extend([("sort_by", key), ("sort_dir", next_dir)])
        query_str = urlencode(params)
        url = request.url.path if not query_str else f"{request.url.path}?{query_str}"

        sort_state[key] = {
            "current": current,
            "next": next_dir,
            "url": url,
            "label": config["label"],
        }

    return sort_state


def _display_snapshot_field(field: str, snapshot: Optional[Snapshot]) -> str:
    if not snapshot:
        return "-"
    value = getattr(snapshot, field, None)
    if field == "listing_type":
        return LISTING_TYPE_LABELS.get(value or "unknown", (value or "unknown").title())
    if field == "price_origin":
        return PRICE_ORIGIN_LABELS.get(value or "live", (value or "live").replace("_", " ").title())
    if field == "shipping_cents":
        return _format_shipping(value, getattr(snapshot, "shipping_note", None))
    if field == "shipping_note":
        text = value or getattr(snapshot, "shipping_note", None)
        if isinstance(text, str) and text.strip():
            return text.strip()
        return "-"
    if field == "availability":
        return _availability_label(value)
    if field.endswith("_cents"):
        return _cents_to_eur(value)
    if field in {"direct_buy", "vb_flag"}:
        return "Ja" if value else "Nein"
    if field == "note":
        return value.strip() if isinstance(value, str) and value.strip() else "-"
    if value is None or value == "":
        return "-"
    return str(value)


def _snapshot_changes(current: Snapshot, previous: Optional[Snapshot]) -> List[Dict[str, str]]:
    changes: List[Dict[str, str]] = []
    if not previous:
        return changes
    for field, label in SNAPSHOT_FIELDS:
        current_val = _display_snapshot_field(field, current)
        previous_val = _display_snapshot_field(field, previous)
        if current_val != previous_val:
            changes.append({"label": label, "previous": previous_val, "current": current_val})
    return changes


def _serialize_item(row: Any) -> Dict[str, Any]:
    item: Item = row.Item
    best_price_cents = _best_price(item)
    availability_value = item.availability or DEFAULT_AVAILABILITY
    completed_flag = bool(item.completed) or (availability_value in COMPLETED_AVAILABILITIES)
    set_info = None
    if row.set_nr:
        set_info = {
            "nr": row.set_nr,
            "name": row.set_name,
            "score": row.set_score,
            "name_score": row.set_name_score,
            "matched_number": row.set_matched_number,
            "parts_difference": row.set_parts_difference,
            "manual_override": row.set_manual_override,
            "original_score": row.set_original_score,
        }

    data = {
        "id": item.id,
        "provider_item_id": item.provider_item_id,
        "provider": row.provider_label,
        "provider_key": item.provider_key,
        "title": item.title,
        "href": item.href,
        "direct_buy": bool(item.direct_buy),
        "vb_flag": bool(item.vb_flag),
        "current_bid_cents": item.current_bid_cents,
        "current_bid": _cents_to_eur(item.current_bid_cents),
        "current_buyout_cents": item.current_buyout_cents,
        "current_buyout": _cents_to_eur(item.current_buyout_cents),
        "best_price_cents": best_price_cents,
        "best_price": _cents_to_eur(best_price_cents),
        "currency": item.currency,
        "first_seen_ts": item.first_seen_ts,
        "last_seen_ts": item.last_seen_ts,
        "availability": availability_value,
        "availability_label": _availability_label(availability_value),
        "completed": completed_flag,
        "completed_ts": item.completed_ts if completed_flag else None,
        "last_revisit_ts": item.last_revisit_ts,
        "price_origin": item.price_origin,
        "price_origin_label": PRICE_ORIGIN_LABELS.get(item.price_origin, item.price_origin.replace("_", " ").title()),
        "settled_price_cents": item.settled_price_cents,
        "settled_price": _cents_to_eur(item.settled_price_cents),
        "settled_ts": item.settled_ts,
        "listing_type": item.listing_type or "unknown",
        "listing_type_label": LISTING_TYPE_LABELS.get(item.listing_type or "unknown", (item.listing_type or "unknown").title()),
        "shipping_cents": item.shipping_cents,
        "shipping_note": item.shipping_note,
        "shipping": _format_shipping(item.shipping_cents, item.shipping_note),
        "hidden": bool(getattr(row, "hidden", False)),
        "set": set_info,
    }

    listing_type = data["listing_type"]
    display_price = "-"
    if listing_type in {"fixed", "fixed_vb"} and data["current_buyout"] != "-":
        display_price = data["current_buyout"]
    elif listing_type == "auction" and data["current_bid"] != "-":
        display_price = data["current_bid"]
    elif "vb" in listing_type and data["best_price"] != "-":
        display_price = data["best_price"]
    elif data["best_price"] != "-":
        display_price = data["best_price"]
    if "vb" in listing_type and display_price != "-" and "VB" not in display_price:
        display_price = f"{display_price} (VB)"
    data["display_price"] = display_price
    data["is_vb"] = "vb" in listing_type
    data["has_auction"] = listing_type in {"auction", "combo"}
    return data


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    q: Optional[str] = Query(default=None, description="Suche im Titel"),
    provider: Optional[str] = Query(default=None),
    availability: Optional[str] = Query(default=None),
    set_nr: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    show_hidden: Optional[bool] = Query(default=None),
    without_set: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    reset_filters: Optional[bool] = Query(default=None, alias="reset"),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    if sort_by not in SORT_CONFIGS:
        sort_by = None
        sort_dir = None
    elif sort_dir not in {"asc", "desc"}:
        sort_dir = "asc"

    cookie_filters: Dict[str, Any] = {}
    cookie_payload_raw = request.cookies.get(FILTER_COOKIE_NAME)
    if cookie_payload_raw and not reset_filters:
        try:
            cookie_filters = json.loads(cookie_payload_raw)
        except json.JSONDecodeError:
            cookie_filters = {}

    filters = DEFAULT_LISTING_FILTERS.copy()

    if q is not None:
        filters["q"] = _coerce_str(q)
    elif "q" in cookie_filters:
        filters["q"] = _coerce_str(cookie_filters.get("q"))

    if provider is not None:
        filters["provider"] = _coerce_str(provider)
    elif "provider" in cookie_filters:
        filters["provider"] = _coerce_str(cookie_filters.get("provider"))

    if availability is not None:
        filters["availability"] = _coerce_str(availability)
    elif "availability" in cookie_filters:
        filters["availability"] = _coerce_str(cookie_filters.get("availability"))

    if filters["availability"] and filters["availability"] not in AVAILABILITY_LABELS:
        filters["availability"] = ""

    if set_nr is not None:
        filters["set_nr"] = _coerce_str(set_nr)
    elif "set_nr" in cookie_filters:
        filters["set_nr"] = _coerce_str(cookie_filters.get("set_nr"))

    if show_hidden is not None:
        filters["show_hidden"] = _coerce_bool(show_hidden)
    elif "show_hidden" in cookie_filters:
        filters["show_hidden"] = _coerce_bool(cookie_filters.get("show_hidden"))

    if without_set is not None:
        filters["without_set"] = _coerce_bool(without_set)
    elif "without_set" in cookie_filters:
        filters["without_set"] = _coerce_bool(cookie_filters.get("without_set"))

    if limit is not None:
        filters["limit"] = _coerce_limit(limit)
    elif "limit" in cookie_filters:
        filters["limit"] = _coerce_limit(cookie_filters.get("limit"))

    if filters["without_set"]:
        filters["set_nr"] = ""

    stmt = _apply_item_filters(
        _base_items_query(),
        query=filters["q"] or None,
        provider=filters["provider"] or None,
        availability=filters["availability"] or None,
        set_nr=filters["set_nr"] or None,
        include_hidden=filters["show_hidden"],
        only_unassigned=filters["without_set"],
    )
    stmt = _apply_sort(stmt, sort_by, sort_dir)
    stmt = stmt.limit(filters["limit"])

    result = await session.execute(stmt)
    items = [_serialize_item(row) for row in result.all()]

    providers = await _fetch_providers(session)

    availability_options = [
        {"value": key, "label": label}
        for key, label in AVAILABILITY_LABELS.items()
    ]

    set_stmt = (
        select(Set.set_nr, Set.name)
        .join(ItemSetMatch, ItemSetMatch.set_nr == Set.set_nr)
        .distinct()
        .order_by(Set.set_nr)
    )
    set_result = await session.execute(set_stmt)
    set_options = [
        {"nr": row[0], "name": row[1]}
        for row in set_result.fetchall()
    ]

    sort_state = _build_sort_state(SORT_CONFIGS, sort_by, sort_dir, request)

    set_filter_value = filters["set_nr"] if (filters["set_nr"] and not filters["without_set"]) else ""

    context = {
        "request": request,
        "items": items,
        "query": filters["q"],
        "providers": providers,
        "provider_filter": filters["provider"],
        "availability_options": availability_options,
        "availability_filter": filters["availability"],
        "set_options": set_options,
        "set_filter": set_filter_value,
        "limit": filters["limit"],
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "sort_state": sort_state,
        "show_hidden": filters["show_hidden"],
        "without_set": filters["without_set"],
        "now": datetime.now(timezone.utc),
    }
    response = templates.TemplateResponse("index.html", context)
    cookie_payload = {
        "q": filters["q"],
        "provider": filters["provider"],
        "availability": filters["availability"],
        "set_nr": filters["set_nr"],
        "show_hidden": filters["show_hidden"],
        "without_set": filters["without_set"],
        "limit": filters["limit"],
    }
    response.set_cookie(
        FILTER_COOKIE_NAME,
        json.dumps(cookie_payload, ensure_ascii=False),
        max_age=FILTER_COOKIE_MAX_AGE,
        samesite="Lax",
    )
    return response


@app.get("/sets", response_class=HTMLResponse)
async def sets_view(
    request: Request,
    provider: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    price_expr = func.coalesce(Item.current_buyout_cents, Item.current_bid_cents)
    stmt = (
        select(
            Set.set_nr,
            Set.name,
            func.count(Item.id).label("item_count"),
            func.min(price_expr).label("min_price"),
            func.max(price_expr).label("max_price"),
            func.avg(price_expr).label("avg_price"),
            func.percentile_cont(0.5).within_group(price_expr).label("median_price"),
            func.avg(ItemSetMatch.score).label("avg_match_score"),
        )
        .join(ItemSetMatch, ItemSetMatch.set_nr == Set.set_nr)
        .join(Item, Item.id == ItemSetMatch.item_id)
        .join(Provider, Provider.key == Item.provider_key)
        .where(price_expr.isnot(None))
        .group_by(Set.set_nr, Set.name)
        .order_by(func.count(Item.id).desc())
        .limit(limit)
    )
    if provider:
        stmt = stmt.where(Provider.key == provider)

    result = await session.execute(stmt)
    rows = result.all()

    sets = [
        {
            "set_nr": row.set_nr,
            "name": row.name,
            "item_count": row.item_count,
            "min_price": _cents_to_eur(row.min_price),
            "median_price": _cents_to_eur(row.median_price if row.median_price is not None else None),
            "avg_price": _cents_to_eur(_round_avg_to_cents(row.avg_price) if row.avg_price is not None else None),
            "max_price": _cents_to_eur(row.max_price),
            "avg_match_score": round(float(row.avg_match_score), 2) if row.avg_match_score is not None else None,
        }
        for row in rows
    ]

    providers = await _fetch_providers(session)

    context = {
        "request": request,
        "sets": sets,
        "providers": providers,
        "provider_filter": provider or "",
        "limit": limit,
        "now": datetime.now(timezone.utc),
    }
    return templates.TemplateResponse("sets.html", context)


@app.get("/sets/catalog", response_class=HTMLResponse)
async def set_catalog(
    request: Request,
    era: Optional[str] = Query(default=None),
    series: Optional[str] = Query(default=None),
    parts_min: Optional[str] = Query(default=None),
    parts_max: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    catalog_sort_configs = {
        "set_nr": {"label": "Set-Nr", "asc": [Set.set_nr.asc()], "desc": [Set.set_nr.desc()]},
        "name": {"label": "Name", "asc": [Set.name.asc()], "desc": [Set.name.desc()]},
        "uvp": {"label": "UVP", "asc": [Set.uvp_cents.asc()], "desc": [Set.uvp_cents.desc()]},
        "release_date": {"label": "Erscheinung", "asc": [Set.release_date.asc()], "desc": [Set.release_date.desc()]},
        "parts": {"label": "Teile", "asc": [Set.parts.asc()], "desc": [Set.parts.desc()]},
        "category": {"label": "Kategorie", "asc": [Set.category.asc()], "desc": [Set.category.desc()]},
        "era": {"label": "Ära", "asc": [Set.era.asc()], "desc": [Set.era.desc()]},
        "series": {"label": "Serie/Film", "asc": [Set.series.asc()], "desc": [Set.series.desc()]},
        "listings": {"label": "Listings", "asc": [func.count(ItemSetMatch.item_id).asc()], "desc": [func.count(ItemSetMatch.item_id).desc()]},
    }

    if sort_by not in catalog_sort_configs:
        sort_by = "set_nr"
        sort_dir = "asc"
    elif sort_dir not in {"asc", "desc"}:
        sort_dir = "asc"

    base_query = (
        select(
            Set,
            func.count(ItemSetMatch.item_id).label("listing_count"),
        )
        .select_from(Set)
        .outerjoin(ItemSetMatch, ItemSetMatch.set_nr == Set.set_nr)
        .group_by(Set)
    )

    if era:
        base_query = base_query.where(Set.era == era)
    if series:
        base_query = base_query.where(Set.series == series)
    try:
        parts_min_int = int(parts_min) if parts_min not in (None, "") else None
    except ValueError:
        parts_min_int = None
    try:
        parts_max_int = int(parts_max) if parts_max not in (None, "") else None
    except ValueError:
        parts_max_int = None

    if parts_min_int is not None:
        base_query = base_query.where(Set.parts.isnot(None), Set.parts >= parts_min_int)
    if parts_max_int is not None:
        base_query = base_query.where(Set.parts.isnot(None), Set.parts <= parts_max_int)

    order_clause = catalog_sort_configs[sort_by][sort_dir]
    base_query = base_query.order_by(*order_clause).limit(limit)

    result = await session.execute(base_query)
    rows = result.all()

    sets = []
    for row in rows:
        set_obj: Set = row.Set
        sets.append(
            {
                "set_nr": set_obj.set_nr,
                "name": set_obj.name,
                "uvp": _cents_to_eur(set_obj.uvp_cents),
                "release_date": _format_date(set_obj.release_date),
                "parts": set_obj.parts or "-",
                "category": set_obj.category or "-",
                "era": set_obj.era or "-",
                "series": set_obj.series or "-",
                "features": set_obj.features or "-",
                "listing_count": row.listing_count,
            }
        )

    era_result = await session.execute(select(Set.era).distinct().order_by(Set.era))
    era_options = [value for (value,) in era_result.fetchall() if value]

    series_result = await session.execute(select(Set.series).distinct().order_by(Set.series))
    series_options = [value for (value,) in series_result.fetchall() if value]

    sort_state = _build_sort_state(catalog_sort_configs, sort_by, sort_dir, request)

    context = {
        "request": request,
        "sets": sets,
        "era_options": era_options,
        "series_options": series_options,
        "filters": {
            "era": era or "",
            "series": series or "",
            "parts_min": parts_min if parts_min not in (None, "") else "",
            "parts_max": parts_max if parts_max not in (None, "") else "",
        },
        "sort_state": sort_state,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "limit": limit,
        "now": datetime.now(timezone.utc),
    }
    return templates.TemplateResponse("set_catalog.html", context)


@app.get("/sets/{set_nr}", response_class=HTMLResponse)
async def set_detail(
    request: Request,
    set_nr: str,
    provider: Optional[str] = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    set_obj = await session.get(Set, set_nr)
    if not set_obj:
        raise HTTPException(status_code=404, detail="Set nicht gefunden")

    price_expr = func.coalesce(Item.current_buyout_cents, Item.current_bid_cents)
    stats_stmt = (
        select(
            func.count(Item.id).label("item_count"),
            func.min(price_expr).label("min_price"),
            func.max(price_expr).label("max_price"),
            func.avg(price_expr).label("avg_price"),
            func.percentile_cont(0.5).within_group(price_expr).label("median_price"),
            func.avg(ItemSetMatch.score).label("avg_match_score"),
        )
        .select_from(ItemSetMatch)
        .join(Item, Item.id == ItemSetMatch.item_id)
        .where(ItemSetMatch.set_nr == set_nr, Item.hidden.is_(False))
    )
    if provider:
        stats_stmt = stats_stmt.join(Provider, Provider.key == Item.provider_key).where(Provider.key == provider)

    stats_result = await session.execute(stats_stmt)
    stats_row = stats_result.first()

    items_stmt = (
        _base_items_query()
        .where(ItemSetMatch.set_nr == set_nr, Item.hidden.is_(False))
        .order_by(ItemSetMatch.score.desc(), Item.last_seen_ts.desc())
    )
    if provider:
        items_stmt = items_stmt.where(Item.provider_key == provider)

    result = await session.execute(items_stmt)
    items = [_serialize_item(row) for row in result.all()]

    providers = await _fetch_providers(session)
    provider_lookup = {prov.key: prov.label for prov in providers}

    active_listings = [item for item in items if not item["completed"]]
    completed_listings = [item for item in items if item["completed"]]

    active_groups_map: Dict[str, Dict[str, Any]] = {}
    for listing in active_listings:
        group = active_groups_map.setdefault(
            listing["provider_key"],
            {"label": listing["provider"], "listings": []},
        )
        group["listings"].append(listing)

    active_stats: Dict[str, Dict[str, Any]] = {}
    active_groups: List[Dict[str, Any]] = []
    for key, group in active_groups_map.items():
        group_label = group.get("label") or provider_lookup.get(key, key)
        items_sorted = sorted(
            group["listings"],
            key=lambda l: (l["best_price_cents"] is None, l["best_price_cents"] or 0),
        )
        price_values = [l["best_price_cents"] for l in group["listings"] if l["best_price_cents"] is not None]
        avg_price = _cents_to_eur(_round_avg_to_cents(statistics.mean(price_values))) if price_values else "-"
        min_price = _cents_to_eur(min(price_values)) if price_values else "-"
        max_price = _cents_to_eur(max(price_values)) if price_values else "-"
        median_price = _cents_to_eur(_median_cents(price_values)) if price_values else "-"
        sparkline = _sparkline_from_prices(price_values)
        vb_count = sum(1 for l in group["listings"] if l.get("is_vb"))
        active_stats[key] = {
            "label": group_label,
            "count": len(group["listings"]),
            "avg_price": avg_price,
            "min_price": min_price,
            "max_price": max_price,
            "median_price": median_price,
            "sparkline": sparkline,
            "vb_count": vb_count,
        }
        active_groups.append({
            "key": key,
            "label": group_label,
            "listings": items_sorted,
            "stats": active_stats[key],
        })

    active_groups.sort(key=lambda g: g["label"])

    market_stats_map: Dict[str, Dict[str, Any]] = {}
    for listing in completed_listings:
        key = listing["provider_key"]
        entry = market_stats_map.setdefault(
            key,
            {
                "label": listing["provider"],
                "values": [],
                "count": 0,
                "priced_count": 0,
                "price_origin_counts": {},
            },
        )
        entry["count"] += 1
        origin = listing.get("price_origin") or "live"
        entry["price_origin_counts"][origin] = entry["price_origin_counts"].get(origin, 0) + 1
        if listing.get("settled_price_cents") is not None:
            entry["values"].append(listing["settled_price_cents"])
            entry["priced_count"] += 1

    market_stats: List[Dict[str, Any]] = []
    for key, entry in market_stats_map.items():
        values = entry.pop("values")
        if values:
            entry["avg_price"] = _cents_to_eur(_round_avg_to_cents(statistics.mean(values)))
            entry["min_price"] = _cents_to_eur(min(values))
            entry["max_price"] = _cents_to_eur(max(values))
        else:
            entry["avg_price"] = entry["min_price"] = entry["max_price"] = "-"
        priced_count = entry["priced_count"]
        if key == "ebay":
            if priced_count:
                entry["note"] = f"basierend auf {priced_count} echten Verkäufen"
            else:
                entry["note"] = "Keine echten Verkäufe erfasst"
        elif key == "kleinanzeigen":
            without_price = entry["count"] - priced_count
            if priced_count:
                note = f"basierend auf {priced_count} reservierten Preisen"
            else:
                note = "Keine reservierten Preise erfasst"
            if without_price:
                note += f" • {without_price} Reservierungen ohne Preis"
            entry["note"] = note
        else:
            entry["note"] = ""
        entry["provider_key"] = key
        entry["label"] = entry.get("label") or provider_lookup.get(key, key)
        market_stats.append(entry)

    market_stats.sort(key=lambda s: s["label"])

    comparison_stats = [
        {
            "key": key,
            "label": stats["label"],
            "count": stats["count"],
            "avg_price": stats["avg_price"],
            "min_price": stats["min_price"],
            "median_price": stats["median_price"],
            "max_price": stats["max_price"],
            "sparkline": stats["sparkline"],
        }
        for key, stats in active_stats.items()
    ]
    comparison_stats.sort(key=lambda s: s["label"])

    context = {
        "request": request,
        "set": {
            "set_nr": set_obj.set_nr,
            "name": set_obj.name,
            "release_date": set_obj.release_date,
            "parts": set_obj.parts,
            "category": set_obj.category,
            "era": set_obj.era,
            "series": set_obj.series,
            "features": set_obj.features,
        },
        "stats": {
            "item_count": stats_row.item_count if stats_row else 0,
            "min_price": _cents_to_eur(stats_row.min_price if stats_row else None),
            "median_price": _cents_to_eur(stats_row.median_price if stats_row and stats_row.median_price is not None else None),
            "avg_price": _cents_to_eur(_round_avg_to_cents(stats_row.avg_price) if stats_row and stats_row.avg_price is not None else None),
            "max_price": _cents_to_eur(stats_row.max_price if stats_row else None),
            "avg_match_score": float(stats_row.avg_match_score) if stats_row and stats_row.avg_match_score is not None else None,
        },
        "items": items,
        "active_groups": active_groups,
        "market_stats": market_stats,
        "comparison_stats": comparison_stats,
        "active_total": len(active_listings),
        "sales_total": sum(stat["count"] for stat in market_stats),
        "providers": providers,
        "provider_filter": provider or "",
        "now": datetime.now(timezone.utc),
    }
    return templates.TemplateResponse("set_detail.html", context)


@app.get("/items/{item_id}", response_class=HTMLResponse)
async def item_detail(
    request: Request,
    item_id: int,
    snapshot_id: Optional[int] = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    stmt = (
        select(Item, Provider.label.label("provider_label"))
        .options(selectinload(Item.set_match).selectinload(ItemSetMatch.set))
        .join(Provider, Provider.key == Item.provider_key)
        .where(Item.id == item_id)
    )
    result = await session.execute(stmt)
    row = result.first()
    if row is None:
        raise HTTPException(status_code=404, detail="Item nicht gefunden")

    item_obj: Item = row.Item
    provider_label = row.provider_label

    snapshot_stmt = (
        select(Snapshot)
        .where(Snapshot.item_id == item_id)
        .order_by(Snapshot.observed_ts.desc())
    )
    snapshot_rows = await session.execute(snapshot_stmt)
    snapshots = snapshot_rows.scalars().all()
    if not snapshots:
        raise HTTPException(status_code=404, detail="Keine Historie vorhanden")

    selected_snapshot = None
    if snapshot_id is not None:
        for snap in snapshots:
            if snap.id == snapshot_id:
                selected_snapshot = snap
                break
    if selected_snapshot is None:
        selected_snapshot = snapshots[0]

    selected_index = snapshots.index(selected_snapshot)
    previous_snapshot = snapshots[selected_index + 1] if selected_index + 1 < len(snapshots) else None

    def serialize_snapshot(snap: Snapshot, prev: Optional[Snapshot]) -> Dict[str, Any]:
        return {
            "id": snap.id,
            "observed_ts": snap.observed_ts,
            "buyout": _cents_to_eur(snap.buyout_cents),
            "bid": _cents_to_eur(snap.bid_cents),
            "availability": snap.availability,
            "availability_label": _availability_label(snap.availability),
            "note": snap.note,
            "source": snap.source,
            "source_label": _snapshot_source_label(snap.source),
            "changes": _snapshot_changes(snap, prev),
            "is_selected": snap.id == selected_snapshot.id,
        }

    snapshot_view_models = []
    for idx, snap in enumerate(snapshots):
        prev = snapshots[idx + 1] if idx + 1 < len(snapshots) else None
        snapshot_view_models.append(serialize_snapshot(snap, prev))

    detail_rows = []
    for field, label in SNAPSHOT_FIELDS:
        current_value = _display_snapshot_field(field, selected_snapshot)
        previous_value = _display_snapshot_field(field, previous_snapshot)
        changed = current_value != previous_value
        detail_rows.append(
            {
                "field": field,
                "label": label,
                "current": current_value,
                "previous": previous_value,
                "changed": changed,
            }
        )

    set_match = None
    if item_obj.set_match:
        set_record = item_obj.set_match
        set_obj = set_record.set
        set_match = {
            "nr": set_record.set_nr,
            "name": set_obj.name if set_obj else None,
            "score": set_record.score,
            "matched_number": set_record.matched_number,
            "name_score": set_record.name_score,
            "parts_difference": set_record.parts_difference,
            "manual_override": set_record.manual_override,
            "original_score": set_record.original_score,
        }

    set_rows = await session.execute(select(Set.set_nr, Set.name).order_by(Set.set_nr))
    set_options = [
        {"nr": row[0], "name": row[1]}
        for row in set_rows.fetchall()
    ]

    context = {
        "request": request,
        "item": _serialize_item_from_detail(item_obj, provider_label, set_match),
        "snapshots": snapshot_view_models,
        "selected_snapshot": {
            "id": selected_snapshot.id,
            "observed_ts": selected_snapshot.observed_ts,
        },
        "detail_rows": detail_rows,
        "set_options": set_options,
        "now": datetime.now(timezone.utc),
    }

    return templates.TemplateResponse("item_detail.html", context)


@app.post("/items/{item_id}/suggest_set", response_class=JSONResponse)
async def suggest_set(
    item_id: int,
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    item_title = await session.scalar(select(Item.title).where(Item.id == item_id))
    if not item_title:
        raise HTTPException(status_code=404, detail="Item nicht gefunden")

    set_rows = await session.execute(select(Set.set_nr, Set.name, Set.parts))
    set_records = {
        row.set_nr: (row.name or "", row.parts)
        for row in set_rows.all()
    }

    match = _score_set_candidates(item_title, set_records)

    if not match:
        return JSONResponse({"status": "no_match"})

    return JSONResponse({
        "status": "ok",
        "match": {
            "set_nr": match["set_nr"],
            "name": match["name"],
            "score": match["score"],
            "matched_number": match["matched_number"],
            "name_score": match["name_score"],
            "parts_difference": match["parts_difference"],
        },
    })


def _serialize_item_from_detail(item: Item, provider_label: str, set_match: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    best_price_cents = _best_price(item)
    availability_value = item.availability or DEFAULT_AVAILABILITY
    completed_flag = bool(item.completed) or (availability_value in COMPLETED_AVAILABILITIES)
    return {
        "id": item.id,
        "provider": provider_label,
        "provider_item_id": item.provider_item_id,
        "title": item.title,
        "href": item.href,
        "first_seen_ts": item.first_seen_ts,
        "last_seen_ts": item.last_seen_ts,
        "best_price": _cents_to_eur(best_price_cents),
        "listing_type": item.listing_type or "unknown",
        "listing_type_label": LISTING_TYPE_LABELS.get(item.listing_type or "unknown", (item.listing_type or "unknown").title()),
        "availability": availability_value,
        "availability_label": _availability_label(availability_value),
        "completed": completed_flag,
        "completed_ts": item.completed_ts if completed_flag else None,
        "last_revisit_ts": item.last_revisit_ts,
        "price_origin": item.price_origin,
        "price_origin_label": PRICE_ORIGIN_LABELS.get(item.price_origin, item.price_origin.replace("_", " ").title()),
        "settled_price": _cents_to_eur(item.settled_price_cents),
        "settled_price_cents": item.settled_price_cents,
        "settled_ts": item.settled_ts,
        "shipping": _format_shipping(item.shipping_cents, item.shipping_note),
        "shipping_note": item.shipping_note,
        "set": set_match,
        "hidden": item.hidden,
    }


@app.post("/items/{item_id}/assign_set")
async def assign_set_manual(item_id: int, set_nr: str = Form(...)) -> RedirectResponse:
    if database is None:
        raise HTTPException(status_code=503, detail="database connection is not ready")
    try:
        await database.assign_manual_set(item_id, set_nr)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return RedirectResponse(url=f"/items/{item_id}", status_code=303)


@app.post("/items/{item_id}/clear_set")
async def clear_set_manual(item_id: int) -> RedirectResponse:
    if database is None:
        raise HTTPException(status_code=503, detail="database connection is not ready")
    await database.clear_set_match(item_id)
    return RedirectResponse(url=f"/items/{item_id}", status_code=303)


@app.post("/items/{item_id}/toggle_hidden")
async def toggle_item_hidden(item_id: int, hide: str = Form(...)) -> RedirectResponse:
    if database is None:
        raise HTTPException(status_code=503, detail="database connection is not ready")
    hidden = hide == "1"
    await database.set_item_hidden(item_id, hidden)
    return RedirectResponse(url=f"/items/{item_id}", status_code=303)


@app.get("/api/items", response_class=JSONResponse)
async def api_items(
    q: Optional[str] = Query(default=None),
    provider: Optional[str] = Query(default=None),
    set_nr: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    include_hidden: bool = Query(default=False),
    without_set: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    stmt = _apply_item_filters(
        _base_items_query(),
        query=q,
        provider=provider,
        set_nr=set_nr,
        include_hidden=include_hidden,
        only_unassigned=without_set,
    )
    stmt = _apply_sort(stmt, sort_by, sort_dir)
    stmt = stmt.limit(limit)
    result = await session.execute(stmt)
    items = [_serialize_item(row) for row in result.all()]
    return JSONResponse({"items": items, "count": len(items)})


@app.get("/api/sets", response_class=JSONResponse)
async def api_sets(
    provider: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    price_expr = func.coalesce(Item.current_buyout_cents, Item.current_bid_cents)
    stmt = (
        select(
            Set.set_nr,
            Set.name,
            func.count(Item.id).label("item_count"),
            func.min(price_expr).label("min_price"),
            func.max(price_expr).label("max_price"),
            func.avg(price_expr).label("avg_price"),
            func.percentile_cont(0.5).within_group(price_expr).label("median_price"),
            func.avg(ItemSetMatch.score).label("avg_match_score"),
        )
        .join(ItemSetMatch, ItemSetMatch.set_nr == Set.set_nr)
        .join(Item, Item.id == ItemSetMatch.item_id)
        .join(Provider, Provider.key == Item.provider_key)
        .group_by(Set.set_nr, Set.name)
        .order_by(func.count(Item.id).desc())
        .limit(limit)
    )
    if provider:
        stmt = stmt.where(Provider.key == provider)

    result = await session.execute(stmt)
    sets = []
    for row in result.all():
        sets.append(
            {
                "set_nr": row.set_nr,
                "name": row.name,
                "item_count": row.item_count,
                "min_price_cents": row.min_price,
                "min_price": _cents_to_eur(row.min_price),
                "median_price_cents": row.median_price,
                "median_price": _cents_to_eur(row.median_price if row.median_price is not None else None),
                "avg_price_cents": _round_avg_to_cents(row.avg_price) if row.avg_price is not None else None,
                "avg_price": _cents_to_eur(_round_avg_to_cents(row.avg_price) if row.avg_price is not None else None),
                "max_price_cents": row.max_price,
                "max_price": _cents_to_eur(row.max_price),
                "avg_match_score": round(float(row.avg_match_score), 2) if row.avg_match_score is not None else None,
            }
        )

    return JSONResponse({"sets": sets, "count": len(sets)})
SNAPSHOT_FIELDS = [
    ("title", "Titel"),
    ("listing_type", "Listing-Typ"),
    ("availability", "Verfügbarkeit"),
    ("price_origin", "Preisquelle"),
    ("settled_price_cents", "Abschlusspreis"),
    ("shipping_cents", "Versand"),
    ("direct_buy", "Direkt kaufen"),
    ("vb_flag", "Verhandlung"),
    ("buyout_cents", "Sofortpreis"),
    ("bid_cents", "Gebotspreis"),
    ("note", "Notiz"),
]
