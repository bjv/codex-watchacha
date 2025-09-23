from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from typing import Any, AsyncGenerator, Dict, List, Optional

import yaml
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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

SNAPSHOT_SOURCE_LABELS = {
    "scrape": "Suche",
    "revisit": "Revisit",
}


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
    set_nr: Optional[str],
    include_hidden: bool,
) -> Select[Any]:
    if query:
        like_term = f"%{query.strip()}%"
        stmt = stmt.where(Item.title.ilike(like_term))
    if provider:
        stmt = stmt.where(Item.provider_key == provider)
    if set_nr:
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

    return {
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
        "hidden": bool(getattr(row, "hidden", False)),
        "set": set_info,
    }


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    q: Optional[str] = Query(default=None, description="Suche im Titel"),
    provider: Optional[str] = Query(default=None),
    set_nr: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    show_hidden: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    if sort_by not in SORT_CONFIGS:
        sort_by = None
        sort_dir = None
    elif sort_dir not in {"asc", "desc"}:
        sort_dir = "asc"

    stmt = _apply_item_filters(
        _base_items_query(),
        query=q,
        provider=provider,
        set_nr=set_nr,
        include_hidden=show_hidden,
    )
    stmt = _apply_sort(stmt, sort_by, sort_dir)
    stmt = stmt.limit(limit)

    result = await session.execute(stmt)
    items = [_serialize_item(row) for row in result.all()]

    providers = await _fetch_providers(session)

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

    context = {
        "request": request,
        "items": items,
        "query": q or "",
        "providers": providers,
        "provider_filter": provider or "",
        "set_options": set_options,
        "set_filter": set_nr or "",
        "limit": limit,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "sort_state": sort_state,
        "show_hidden": show_hidden,
        "now": datetime.now(timezone.utc),
    }
    return templates.TemplateResponse("index.html", context)


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
        "availability": availability_value,
        "availability_label": _availability_label(availability_value),
        "completed": completed_flag,
        "completed_ts": item.completed_ts if completed_flag else None,
        "last_revisit_ts": item.last_revisit_ts,
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
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    stmt = _apply_item_filters(
        _base_items_query(),
        query=q,
        provider=provider,
        set_nr=set_nr,
        include_hidden=include_hidden,
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
    ("availability", "Verfügbarkeit"),
    ("direct_buy", "Direkt kaufen"),
    ("vb_flag", "Verhandlung"),
    ("buyout_cents", "Sofortpreis"),
    ("bid_cents", "Gebotspreis"),
    ("note", "Notiz"),
]
