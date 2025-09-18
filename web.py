from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import yaml
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db import Database, Item, Provider, Snapshot


CONFIG_PATH = Path("config.yml")
templates = Jinja2Templates(directory="templates")


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise RuntimeError("config.yml missing – create it from config.yml.sample")
    return yaml.safe_load(CONFIG_PATH.read_text()) or {}


database: Optional[Database] = None

app = FastAPI(title="Watchacha Dashboard")


def _cents_to_eur(cents: Optional[int]) -> str:
    if cents is None:
        return "-"
    value = cents / 100
    return f"{value:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")


def _normalize_numeric(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    return int(round(float(value)))


SNAPSHOT_FIELDS = [
    ("title", "Titel"),
    ("direct_buy", "Direkt kaufen"),
    ("vb_flag", "Verhandlung"),
    ("buyout_cents", "Sofortpreis"),
    ("bid_cents", "Gebotspreis"),
]


def _display_snapshot_field(field: str, snapshot: Optional[Snapshot]) -> str:
    if snapshot is None:
        return "–"
    value = getattr(snapshot, field, None)
    if field.endswith("_cents"):
        return _cents_to_eur(value)
    if field in {"direct_buy", "vb_flag"}:
        return "Ja" if value else "Nein"
    if value is None or value == "":
        return "–"
    return str(value)


def _snapshot_changes(current: Snapshot, previous: Optional[Snapshot]):
    changes = []
    if not previous:
        return changes
    for field, label in SNAPSHOT_FIELDS:
        current_val = getattr(current, field, None)
        previous_val = getattr(previous, field, None)
        if current_val != previous_val:
            changes.append(
                {
                    "field": field,
                    "label": label,
                    "previous": _display_snapshot_field(field, previous),
                    "current": _display_snapshot_field(field, current),
                }
            )
    return changes


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
        )
        .join(Provider, Provider.key == Item.provider_key)
        .order_by(Item.last_seen_ts.desc())
    )


async def _fetch_providers(session: AsyncSession) -> List[Provider]:
    result = await session.execute(select(Provider).order_by(Provider.label))
    return [row[0] for row in result.all()]


def _apply_item_filters(
    stmt: Select[Any],
    *,
    query: Optional[str],
    provider: Optional[str],
    cluster: Optional[str],
) -> Select[Any]:
    if query:
        like_term = f"%{query.strip()}%"
        stmt = stmt.where(Item.title.ilike(like_term))
    if provider:
        stmt = stmt.where(Item.provider_key == provider)
    if cluster:
        stmt = stmt.where(Item.cluster_key == cluster)
    return stmt


def _serialize_item(item: Item, provider_label: str) -> Dict[str, Any]:
    return {
        "id": item.id,
        "provider_item_id": item.provider_item_id,
        "provider": provider_label,
        "provider_key": item.provider_key,
        "title": item.title,
        "href": item.href,
        "cluster_key": item.cluster_key,
        "direct_buy": bool(item.direct_buy),
        "vb_flag": bool(item.vb_flag),
        "current_bid_cents": item.current_bid_cents,
        "current_bid": _cents_to_eur(item.current_bid_cents),
        "current_buyout_cents": item.current_buyout_cents,
        "current_buyout": _cents_to_eur(item.current_buyout_cents),
        "currency": item.currency,
        "first_seen_ts": item.first_seen_ts,
        "last_seen_ts": item.last_seen_ts,
    }


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    q: Optional[str] = Query(default=None, description="Suche im Titel"),
    provider: Optional[str] = Query(default=None),
    cluster: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    stmt = _apply_item_filters(_base_items_query(), query=q, provider=provider, cluster=cluster)
    stmt = stmt.limit(limit)
    result = await session.execute(stmt)
    rows = result.all()
    items = [_serialize_item(row.Item, row.provider_label) for row in rows]

    providers = await _fetch_providers(session)

    cluster_stmt = select(Item.cluster_key).where(Item.cluster_key.isnot(None)).distinct().order_by(Item.cluster_key)
    cluster_result = await session.execute(cluster_stmt)
    clusters = [row[0] for row in cluster_result.all()]

    context = {
        "request": request,
        "items": items,
        "query": q or "",
        "providers": providers,
        "provider_filter": provider or "",
        "clusters": clusters,
        "cluster_filter": cluster or "",
        "limit": limit,
        "now": datetime.now(timezone.utc),
    }
    return templates.TemplateResponse("index.html", context)


@app.get("/clusters", response_class=HTMLResponse)
async def clusters_view(
    request: Request,
    provider: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    price_expr = func.coalesce(Item.current_buyout_cents, Item.current_bid_cents)
    stmt = (
        select(
            Item.cluster_key,
            func.count(Item.id).label("item_count"),
            func.min(price_expr).label("min_price"),
            func.max(price_expr).label("max_price"),
            func.avg(price_expr).label("avg_price"),
            func.percentile_cont(0.5).within_group(price_expr).label("median_price"),
        )
        .where(Item.cluster_key.isnot(None), price_expr.isnot(None))
        .group_by(Item.cluster_key)
        .order_by(func.count(Item.id).desc())
        .limit(limit)
    )
    if provider:
        stmt = stmt.where(Item.provider_key == provider)

    result = await session.execute(stmt)
    rows = result.all()

    clusters = []
    for row in rows:
        clusters.append(
            {
                "cluster_key": row.cluster_key,
                "item_count": row.item_count,
                "min_price": _cents_to_eur(row.min_price),
                "max_price": _cents_to_eur(row.max_price),
                "avg_price": _cents_to_eur(_normalize_numeric(row.avg_price)),
                "median_price": _cents_to_eur(_normalize_numeric(row.median_price)),
            }
        )

    providers = await _fetch_providers(session)

    context = {
        "request": request,
        "clusters": clusters,
        "providers": providers,
        "provider_filter": provider or "",
        "limit": limit,
        "now": datetime.now(timezone.utc),
    }
    return templates.TemplateResponse("clusters.html", context)


@app.get("/items/{item_id}", response_class=HTMLResponse)
async def item_detail(
    request: Request,
    item_id: int,
    snapshot_id: Optional[int] = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    stmt = (
        select(
            Item,
            Provider.label.label("provider_label"),
        )
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

    snapshot_view_models = []
    for idx, snap in enumerate(snapshots):
        prev = snapshots[idx + 1] if idx + 1 < len(snapshots) else None
        changes = _snapshot_changes(snap, prev)
        snapshot_view_models.append(
            {
                "id": snap.id,
                "observed_ts": snap.observed_ts,
                "buyout": _cents_to_eur(snap.buyout_cents),
                "bid": _cents_to_eur(snap.bid_cents),
                "changes": changes,
                "is_selected": snap.id == selected_snapshot.id,
            }
        )

    selected_changes = _snapshot_changes(selected_snapshot, previous_snapshot)

    detail_rows = []
    for field, label in SNAPSHOT_FIELDS:
        current_value = _display_snapshot_field(field, selected_snapshot)
        previous_value = _display_snapshot_field(field, previous_snapshot) if previous_snapshot else "–"
        change_entry = next((c for c in selected_changes if c["field"] == field), None)
        detail_rows.append(
            {
                "field": field,
                "label": label,
                "current": current_value,
                "previous": change_entry["previous"] if change_entry else previous_value,
                "changed": change_entry is not None,
            }
        )

    item_payload = _serialize_item(item_obj, provider_label)

    context = {
        "request": request,
        "item": item_payload,
        "snapshots": snapshot_view_models,
        "selected_snapshot": {
            "id": selected_snapshot.id,
            "observed_ts": selected_snapshot.observed_ts,
        },
        "detail_rows": detail_rows,
        "selected_changes": selected_changes,
        "now": datetime.now(timezone.utc),
    }

    return templates.TemplateResponse("item_detail.html", context)


@app.get("/api/items", response_class=JSONResponse)
async def api_items(
    q: Optional[str] = Query(default=None),
    provider: Optional[str] = Query(default=None),
    cluster: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    stmt = _apply_item_filters(_base_items_query(), query=q, provider=provider, cluster=cluster)
    stmt = stmt.limit(limit)
    result = await session.execute(stmt)
    items = [_serialize_item(row.Item, row.provider_label) for row in result.all()]
    return JSONResponse({"items": items, "count": len(items)})


@app.get("/api/clusters", response_class=JSONResponse)
async def api_clusters(
    provider: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    price_expr = func.coalesce(Item.current_buyout_cents, Item.current_bid_cents)
    stmt = (
        select(
            Item.cluster_key.label("cluster_key"),
            func.count(Item.id).label("item_count"),
            func.min(price_expr).label("min_price"),
            func.max(price_expr).label("max_price"),
            func.avg(price_expr).label("avg_price"),
            func.percentile_cont(0.5).within_group(price_expr).label("median_price"),
        )
        .where(Item.cluster_key.isnot(None), price_expr.isnot(None))
        .group_by(Item.cluster_key)
        .order_by(func.count(Item.id).desc())
        .limit(limit)
    )
    if provider:
        stmt = stmt.where(Item.provider_key == provider)

    result = await session.execute(stmt)
    clusters = []
    for row in result.all():
        clusters.append(
            {
                "cluster_key": row.cluster_key,
                "item_count": row.item_count,
                "min_price_cents": row.min_price,
                "min_price": _cents_to_eur(row.min_price),
                "max_price_cents": row.max_price,
                "max_price": _cents_to_eur(row.max_price),
                "avg_price_cents": _normalize_numeric(row.avg_price),
                "avg_price": _cents_to_eur(_normalize_numeric(row.avg_price)),
                "median_price_cents": _normalize_numeric(row.median_price),
                "median_price": _cents_to_eur(_normalize_numeric(row.median_price)),
            }
        )
    return JSONResponse({"clusters": clusters, "count": len(clusters)})
