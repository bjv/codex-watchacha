import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    case,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, relationship, selectinload
from sqlalchemy.exc import ProgrammingError


Base = declarative_base()

PROJECT_ROOT = Path(__file__).resolve().parent
SETS_CSV_PATH = PROJECT_ROOT / "artifacts" / "sets.csv"

DEFAULT_AVAILABILITY = "active"
COMPLETED_AVAILABILITIES = {"deleted", "sold", "unsold"}


class Provider(Base):
    __tablename__ = "providers"

    key = Column(String(64), primary_key=True)
    label = Column(String(255), nullable=False)

    items = relationship("Item", back_populates="provider", cascade="all, delete-orphan")


class Item(Base):
    __tablename__ = "items"

    id = Column(BigInteger, primary_key=True)
    provider_key = Column(String(64), ForeignKey("providers.key", ondelete="CASCADE"), nullable=False)
    provider_item_id = Column(String(128), nullable=False)
    title = Column(Text, nullable=False)
    href = Column(Text, nullable=False)
    direct_buy = Column(Boolean, nullable=False, default=False)
    vb_flag = Column(Boolean, nullable=False, default=False)
    current_bid_cents = Column(Integer, nullable=True)
    current_buyout_cents = Column(Integer, nullable=True)
    currency = Column(String(8), nullable=False, default="EUR")
    availability = Column(String(32), nullable=False, default="active")
    completed = Column(Boolean, nullable=False, default=False)
    completed_ts = Column(DateTime(timezone=True), nullable=True)
    first_seen_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    last_seen_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    last_revisit_ts = Column(DateTime(timezone=True), nullable=True)
    listing_type = Column(String(32), nullable=False, default="unknown")
    shipping_cents = Column(Integer, nullable=True)
    shipping_note = Column(Text, nullable=True)
    price_origin = Column(String(32), nullable=False, default="live")
    settled_price_cents = Column(Integer, nullable=True)
    settled_ts = Column(DateTime(timezone=True), nullable=True)
    hidden = Column(Boolean, nullable=False, default=False)

    provider = relationship("Provider", back_populates="items")
    snapshots = relationship("Snapshot", back_populates="item", cascade="all, delete-orphan", order_by="Snapshot.observed_ts.desc()")
    set_match = relationship("ItemSetMatch", back_populates="item", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("provider_key", "provider_item_id", name="uq_items_provider_item"),
    )


class Snapshot(Base):
    __tablename__ = "snapshots"

    id = Column(BigInteger, primary_key=True)
    item_id = Column(BigInteger, ForeignKey("items.id", ondelete="CASCADE"), nullable=False)
    observed_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    bid_cents = Column(Integer, nullable=True)
    buyout_cents = Column(Integer, nullable=True)
    direct_buy = Column(Boolean, nullable=False, default=False)
    vb_flag = Column(Boolean, nullable=False, default=False)
    listing_type = Column(String(32), nullable=False, default="unknown")
    shipping_cents = Column(Integer, nullable=True)
    shipping_note = Column(Text, nullable=True)
    price_origin = Column(String(32), nullable=False, default="live")
    settled_price_cents = Column(Integer, nullable=True)
    availability = Column(String(32), nullable=False, default="active")
    source = Column(String(32), nullable=False, default="scrape")
    note = Column(Text, nullable=True)
    title = Column(Text, nullable=False)
    fingerprint = Column(String(64), nullable=False)

    item = relationship("Item", back_populates="snapshots")

    __table_args__ = (
        UniqueConstraint("item_id", "fingerprint", name="uq_snapshots_item_fingerprint"),
    )


class Set(Base):
    __tablename__ = "sets"

    set_nr = Column(String(32), primary_key=True)
    name = Column(Text, nullable=False)
    uvp_cents = Column(Integer, nullable=True)
    release_date = Column(Date, nullable=True)
    parts = Column(Integer, nullable=True)
    category = Column(String(128), nullable=True)
    era = Column(String(128), nullable=True)
    series = Column(String(255), nullable=True)
    features = Column(Text, nullable=True)
    created_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    matches = relationship("ItemSetMatch", back_populates="set", cascade="all, delete-orphan")


class ItemSetMatch(Base):
    __tablename__ = "item_set_matches"

    id = Column(BigInteger, primary_key=True)
    item_id = Column(BigInteger, ForeignKey("items.id", ondelete="CASCADE"), nullable=False, unique=True)
    set_nr = Column(String(32), ForeignKey("sets.set_nr", ondelete="CASCADE"), nullable=False)
    score = Column(Float, nullable=False)
    name_score = Column(Float, nullable=True)
    matched_number = Column(Boolean, nullable=False, default=False)
    parts_difference = Column(Integer, nullable=True)
    manual_override = Column(Boolean, nullable=False, default=False)
    original_score = Column(Float, nullable=True)
    created_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    item = relationship("Item", back_populates="set_match")
    set = relationship("Set", back_populates="matches")


@dataclass(slots=True)
class UpsertResult:
    item_id: int
    is_new_item: bool
    snapshot_created: bool
    changed_fields: Dict[str, Tuple[Any, Any]]


@dataclass(slots=True)
class SetMatchPayload:
    set_nr: str
    score: float
    matched_number: bool
    name_score: float
    parts_difference: Optional[int]
    above_threshold: bool


@dataclass(slots=True)
class SetRecord:
    set_nr: str
    name: str
    uvp_cents: Optional[int]
    release_date: Optional[date]
    parts: Optional[int]
    category: Optional[str]
    era: Optional[str]
    series: Optional[str]
    features: Optional[str]


@dataclass(slots=True)
class RevisitCandidate:
    item_id: int
    provider_key: str
    provider_label: Optional[str]
    provider_item_id: str
    href: str
    title: str
    availability: str
    last_seen_ts: datetime
    last_revisit_ts: Optional[datetime]
    current_bid_cents: Optional[int]
    current_buyout_cents: Optional[int]
    listing_type: str
    shipping_cents: Optional[int]
    shipping_note: Optional[str]
    price_origin: str
    settled_price_cents: Optional[int]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint_payload(data: Dict[str, Any]) -> str:
    fingerprint_fields = {
        "bid": data.get("bid_cents"),
        "buyout": data.get("buyout_cents"),
        "direct_buy": bool(data.get("direct_buy")),
        "vb_flag": bool(data.get("vb_flag")),
        "availability": (data.get("availability") or DEFAULT_AVAILABILITY),
        "listing_type": (data.get("listing_type") or "unknown"),
        "shipping_cents": data.get("shipping_cents"),
        "shipping_note": (data.get("shipping_note") or "").strip().casefold(),
        "price_origin": (data.get("price_origin") or "live"),
        "settled_price": data.get("settled_price_cents"),
        "title": (data.get("title") or "").strip().casefold(),
    }
    encoded = json.dumps(fingerprint_fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class Database:
    def __init__(self, url: str, *, echo: bool = False) -> None:
        self.url = url
        self.engine: AsyncEngine = create_async_engine(url, echo=echo, future=True)
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine, expire_on_commit=False
        )
        self._provider_cache: Dict[str, str] = {}

    async def init_models(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await self._ensure_schema_updates()
        await self._ensure_sets_seeded()

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def upsert_item_and_snapshot(
        self,
        provider_key: str,
        provider_label: Optional[str],
        payload: Dict[str, Any],
    ) -> UpsertResult:
        now = _now_utc()
        provider_label = provider_label or provider_key
        payload = dict(payload)
        availability = payload.get("availability", DEFAULT_AVAILABILITY)
        payload.setdefault("availability", availability)
        completed_flag = bool(payload.get("completed", availability in COMPLETED_AVAILABILITIES))
        completed_ts = payload.get("completed_ts")
        if completed_flag and completed_ts is None:
            completed_ts = now
        snapshot_note = payload.get("snapshot_note")
        snapshot_source = payload.get("snapshot_source", "scrape")
        listing_type = payload.get("listing_type") or "unknown"
        shipping_cents = payload.get("shipping_cents")
        shipping_note = payload.get("shipping_note")
        price_origin_payload = payload.get("price_origin", "live")
        payload.setdefault("price_origin", price_origin_payload)
        payload.setdefault("listing_type", listing_type)
        payload.setdefault("shipping_cents", shipping_cents)
        payload.setdefault("shipping_note", shipping_note)
        settled_price_cents_payload = payload.get("settled_price_cents")
        payload.setdefault("settled_price_cents", settled_price_cents_payload)
        settled_ts_payload = payload.get("settled_ts")
        fp = _fingerprint_payload(payload)

        async with self.session_factory() as session:
            async with session.begin():
                await self._ensure_provider(session, provider_key, provider_label)

                item = await self._get_item(session, provider_key, payload["provider_item_id"])
                changed_fields: Dict[str, Tuple[Any, Any]] = {}
                is_new = item is None

                if item is None:
                    item = Item(
                        provider_key=provider_key,
                        provider_item_id=payload["provider_item_id"],
                        title=payload["title"],
                        href=payload["href"],
                        direct_buy=bool(payload.get("direct_buy")),
                        vb_flag=bool(payload.get("vb_flag")),
                        current_bid_cents=payload.get("bid_cents"),
                        current_buyout_cents=payload.get("buyout_cents"),
                        currency=payload.get("currency", "EUR"),
                        availability=availability,
                        completed=completed_flag,
                        completed_ts=completed_ts,
                        first_seen_ts=now,
                        last_seen_ts=now,
                        listing_type=listing_type,
                        shipping_cents=shipping_cents,
                        shipping_note=shipping_note,
                        price_origin=price_origin_payload,
                        settled_price_cents=settled_price_cents_payload,
                        settled_ts=settled_ts_payload,
                    )
                    session.add(item)
                    await session.flush([item])

                    changed_fields = {
                        "title": (None, item.title),
                        "direct_buy": (None, item.direct_buy),
                        "vb_flag": (None, item.vb_flag),
                        "current_bid_cents": (None, item.current_bid_cents),
                        "current_buyout_cents": (None, item.current_buyout_cents),
                        "availability": (None, item.availability),
                        "completed": (None, item.completed),
                        "listing_type": (None, item.listing_type),
                        "shipping_cents": (None, item.shipping_cents),
                        "shipping_note": (None, item.shipping_note),
                        "price_origin": (None, item.price_origin),
                        "settled_price_cents": (None, item.settled_price_cents),
                    }
                else:
                    updates = {
                        "title": payload["title"],
                        "href": payload["href"],
                        "direct_buy": bool(payload.get("direct_buy")),
                        "vb_flag": bool(payload.get("vb_flag")),
                        "current_bid_cents": payload.get("bid_cents"),
                        "current_buyout_cents": payload.get("buyout_cents"),
                        "currency": payload.get("currency", "EUR"),
                        "availability": availability,
                    }
                    if listing_type and listing_type != item.listing_type:
                        updates["listing_type"] = listing_type
                    if shipping_cents is not None and shipping_cents != item.shipping_cents:
                        updates["shipping_cents"] = shipping_cents
                    if shipping_note is not None and (shipping_note or item.shipping_note):
                        if shipping_note != item.shipping_note:
                            updates["shipping_note"] = shipping_note
                    if price_origin_payload and price_origin_payload != item.price_origin:
                        updates["price_origin"] = price_origin_payload
                    for attr, new_val in updates.items():
                        old_val = getattr(item, attr)
                        if old_val != new_val:
                            setattr(item, attr, new_val)
                            if attr in {"title", "direct_buy", "vb_flag", "current_bid_cents", "current_buyout_cents", "availability", "listing_type", "shipping_cents", "shipping_note", "price_origin"}:
                                changed_fields[attr] = (old_val, new_val)

                    if item.completed != completed_flag or (
                        completed_flag and completed_ts is not None and item.completed_ts != completed_ts
                    ):
                        changed_fields["completed"] = (item.completed, completed_flag)
                        item.completed = completed_flag
                        item.completed_ts = completed_ts

                    if settled_price_cents_payload is not None and item.settled_price_cents != settled_price_cents_payload:
                        changed_fields["settled_price_cents"] = (
                            item.settled_price_cents,
                            settled_price_cents_payload,
                        )
                        item.settled_price_cents = settled_price_cents_payload
                        item.settled_ts = settled_ts_payload or now

                    item.last_seen_ts = now

                await self._upsert_item_set_match(session, item, payload.get("set_match"))

                last_fingerprint = None
                if not is_new:
                    stmt = (
                        select(Snapshot.fingerprint)
                        .where(Snapshot.item_id == item.id)
                        .order_by(Snapshot.observed_ts.desc())
                        .limit(1)
                    )
                    last_fingerprint = await session.scalar(stmt)

                snapshot_created = is_new or (last_fingerprint != fp and bool(changed_fields))

                if snapshot_created:
                    snapshot = Snapshot(
                        item_id=item.id,
                        observed_ts=now,
                        bid_cents=payload.get("bid_cents"),
                        buyout_cents=payload.get("buyout_cents"),
                        direct_buy=bool(payload.get("direct_buy")),
                        vb_flag=bool(payload.get("vb_flag")),
                        listing_type=listing_type,
                        shipping_cents=shipping_cents,
                        shipping_note=shipping_note,
                        price_origin=price_origin_payload,
                        settled_price_cents=settled_price_cents_payload,
                        availability=availability,
                        source=snapshot_source,
                        note=snapshot_note,
                        title=payload["title"],
                        fingerprint=fp,
                    )
                    session.add(snapshot)

            # session.commit() handled by context manager

        return UpsertResult(
            item_id=int(item.id),
            is_new_item=is_new,
            snapshot_created=snapshot_created,
            changed_fields=changed_fields,
        )

    async def fetch_set_records(self) -> List[SetRecord]:
        async with self.session_factory() as session:
            result = await session.execute(select(Set))
            sets = result.scalars().all()
            return [
                SetRecord(
                    set_nr=s.set_nr,
                    name=s.name,
                    uvp_cents=s.uvp_cents,
                    release_date=s.release_date,
                    parts=s.parts,
                    category=s.category,
                    era=s.era,
                    series=s.series,
                    features=s.features,
                )
                for s in sets
            ]

    async def fetch_revisit_candidates(
        self,
        *,
        providers: Sequence[str],
        stale_before: datetime,
        limit: int,
    ) -> List[RevisitCandidate]:
        if limit <= 0:
            return []

        async with self.session_factory() as session:
            stmt = (
                select(Item, Provider.label.label("provider_label"))
                .join(Provider, Provider.key == Item.provider_key)
                .where(Item.last_seen_ts <= stale_before, Item.completed.is_(False))
            )
            if providers:
                stmt = stmt.where(Item.provider_key.in_(providers))

            nulls_first = case((Item.last_revisit_ts.is_(None), 0), else_=1)
            stmt = (
                stmt.order_by(
                    nulls_first,
                    Item.last_revisit_ts.asc(),
                    Item.last_seen_ts.asc(),
                    Item.id.asc(),
                )
                .limit(limit)
            )

            result = await session.execute(stmt)
            candidates: List[RevisitCandidate] = []
            for row in result.all():
                item: Item = row[0]
                provider_label = row[1]
                candidates.append(
                    RevisitCandidate(
                        item_id=int(item.id),
                        provider_key=item.provider_key,
                        provider_label=provider_label,
                        provider_item_id=item.provider_item_id,
                        href=item.href,
                        title=item.title,
                        availability=item.availability,
                        last_seen_ts=item.last_seen_ts,
                        last_revisit_ts=item.last_revisit_ts,
                        current_bid_cents=item.current_bid_cents,
                        current_buyout_cents=item.current_buyout_cents,
                        listing_type=item.listing_type,
                        shipping_cents=item.shipping_cents,
                        shipping_note=item.shipping_note,
                        price_origin=item.price_origin,
                        settled_price_cents=item.settled_price_cents,
                    )
                )
            return candidates

    async def _ensure_provider(self, session: AsyncSession, provider_key: str, label: str) -> None:
        if provider_key in self._provider_cache:
            return

        provider = await session.get(Provider, provider_key)
        if provider is None:
            session.add(Provider(key=provider_key, label=label))
        else:
            provider.label = label

        self._provider_cache[provider_key] = label

    async def _get_item(self, session: AsyncSession, provider_key: str, provider_item_id: str) -> Optional[Item]:
        stmt = (
            select(Item)
            .options(selectinload(Item.set_match).selectinload(ItemSetMatch.set))
            .where(Item.provider_key == provider_key, Item.provider_item_id == provider_item_id)
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def _upsert_item_set_match(
        self,
        session: AsyncSession,
        item: Item,
        match_payload: Optional[SetMatchPayload],
    ) -> None:
        existing = await session.scalar(
            select(ItemSetMatch).where(ItemSetMatch.item_id == item.id)
        )
        if match_payload is None or not match_payload.above_threshold:
            if existing is not None and not existing.manual_override:
                await session.delete(existing)
            return

        set_obj = await session.get(Set, match_payload.set_nr)
        if not set_obj:
            return

        if existing is None:
            match = ItemSetMatch(
                item=item,
                set=set_obj,
                score=match_payload.score,
                name_score=match_payload.name_score,
                matched_number=match_payload.matched_number,
                parts_difference=match_payload.parts_difference,
                manual_override=False,
                original_score=None,
            )
            session.add(match)
        else:
            if existing.manual_override:
                return
            existing.set = set_obj
            existing.score = match_payload.score
            existing.name_score = match_payload.name_score
            existing.matched_number = match_payload.matched_number
            existing.parts_difference = match_payload.parts_difference
            existing.manual_override = False
            existing.original_score = None

    async def record_revisit_outcome(
        self,
        item_id: int,
        *,
        availability: str,
        note: Optional[str] = None,
        source: str = "revisit",
        observed_ts: Optional[datetime] = None,
        settled_price_cents: Optional[int] = None,
        price_origin: Optional[str] = None,
        listing_type: Optional[str] = None,
        shipping_cents: Optional[int] = None,
        shipping_note: Optional[str] = None,
        settled_ts: Optional[datetime] = None,
    ) -> bool:
        observed = observed_ts or _now_utc()
        normalized_availability = availability or DEFAULT_AVAILABILITY

        async with self.session_factory() as session:
            async with session.begin():
                item = await session.get(Item, item_id)
                if item is None:
                    raise ValueError("Item not found")

                item.availability = normalized_availability
                item.last_revisit_ts = observed

                if price_origin:
                    item.price_origin = price_origin

                if listing_type and listing_type != "unknown":
                    item.listing_type = listing_type

                if shipping_cents is not None:
                    item.shipping_cents = shipping_cents

                if shipping_note is not None:
                    item.shipping_note = shipping_note

                if settled_price_cents is not None:
                    item.settled_price_cents = settled_price_cents
                    item.settled_ts = settled_ts or observed

                if normalized_availability in COMPLETED_AVAILABILITIES:
                    if not item.completed:
                        item.completed = True
                        item.completed_ts = observed
                    elif item.completed_ts is None:
                        item.completed_ts = observed
                else:
                    if item.completed:
                        item.completed = False
                        item.completed_ts = None

                payload = {
                    "title": item.title,
                    "direct_buy": item.direct_buy,
                    "vb_flag": item.vb_flag,
                    "bid_cents": item.current_bid_cents,
                    "buyout_cents": item.current_buyout_cents,
                    "availability": normalized_availability,
                    "listing_type": item.listing_type,
                    "shipping_cents": item.shipping_cents,
                    "shipping_note": item.shipping_note,
                    "price_origin": item.price_origin,
                    "settled_price_cents": item.settled_price_cents,
                }
                fp = _fingerprint_payload(payload)

                last_fingerprint = await session.scalar(
                    select(Snapshot.fingerprint)
                    .where(Snapshot.item_id == item.id)
                    .order_by(Snapshot.observed_ts.desc())
                    .limit(1)
                )

                if last_fingerprint == fp:
                    return False

                snapshot = Snapshot(
                    item_id=item.id,
                    observed_ts=observed,
                    bid_cents=item.current_bid_cents,
                    buyout_cents=item.current_buyout_cents,
                    direct_buy=item.direct_buy,
                    vb_flag=item.vb_flag,
                    listing_type=item.listing_type,
                    shipping_cents=item.shipping_cents,
                    shipping_note=item.shipping_note,
                    price_origin=item.price_origin,
                    settled_price_cents=item.settled_price_cents,
                    availability=normalized_availability,
                    source=source,
                    note=note,
                    title=item.title,
                    fingerprint=fp,
                )
                session.add(snapshot)
                return True

    async def _ensure_sets_seeded(self) -> None:
        if not SETS_CSV_PATH.exists():
            return

        async with self.session_factory() as session:
            async with session.begin():
                total_sets = await session.scalar(select(func.count(Set.set_nr)))
                if total_sets and total_sets > 0:
                    return
                await self._import_sets_from_csv(session, SETS_CSV_PATH)

    async def _import_sets_from_csv(self, session: AsyncSession, path: Path) -> None:
        with path.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter=";")
            rows = list(reader)

        for row in rows:
            set_nr = (row.get("SetNr") or "").strip()
            if not set_nr:
                continue

            name = (row.get("Name") or "").strip()
            uvp_cents = _parse_price_to_cents(row.get("UVP_EUR"))
            release_date = _parse_release_date(row.get("Erscheinungsdatum"))
            parts = _parse_int(row.get("Teile"))
            category = _clean_text(row.get("Kategorie"))
            era = _clean_text(row.get("Ära"))
            series = _clean_text(row.get("Serie_Film"))
            features = _clean_text(row.get("Besonderheiten"))

            session.add(
                Set(
                    set_nr=set_nr,
                    name=name,
                    uvp_cents=uvp_cents,
                    release_date=release_date,
                    parts=parts,
                    category=category,
                    era=era,
                    series=series,
                    features=features,
                )
            )

    async def assign_manual_set(self, item_id: int, set_nr: str) -> None:
        async with self.session_factory() as session:
            async with session.begin():
                item = await session.get(Item, item_id, options=[selectinload(Item.set_match)])
                if item is None:
                    raise ValueError("Item not found")
                set_obj = await session.get(Set, set_nr)
                if set_obj is None:
                    raise ValueError("Set not found")

                match = item.set_match
                if match is None:
                    match = ItemSetMatch(
                        item=item,
                        set=set_obj,
                        score=1.0,
                        name_score=1.0,
                        matched_number=True,
                        parts_difference=0,
                        manual_override=True,
                        original_score=None,
                    )
                    session.add(match)
                else:
                    if not match.manual_override and match.original_score is None:
                        match.original_score = match.score
                    match.set = set_obj
                    match.score = 1.0
                    match.name_score = 1.0
                    match.matched_number = True
                    match.parts_difference = 0
                    match.manual_override = True

    async def clear_set_match(self, item_id: int) -> None:
        async with self.session_factory() as session:
            async with session.begin():
                match = await session.scalar(select(ItemSetMatch).where(ItemSetMatch.item_id == item_id))
                if match is not None:
                    await session.delete(match)

    async def set_item_hidden(self, item_id: int, hidden: bool) -> None:
        async with self.session_factory() as session:
            async with session.begin():
                item = await session.get(Item, item_id)
                if item is None:
                    raise ValueError("Item not found")
                item.hidden = hidden

    async def _ensure_schema_updates(self) -> None:
        statements = [
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS hidden BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS availability VARCHAR(32) NOT NULL DEFAULT 'active'",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS completed BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS completed_ts TIMESTAMPTZ",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS first_seen_ts TIMESTAMPTZ",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS last_revisit_ts TIMESTAMPTZ",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS listing_type VARCHAR(32) NOT NULL DEFAULT 'unknown'",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS shipping_cents INTEGER",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS shipping_note TEXT",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS price_origin VARCHAR(32) NOT NULL DEFAULT 'live'",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS settled_price_cents INTEGER",
            "ALTER TABLE items ADD COLUMN IF NOT EXISTS settled_ts TIMESTAMPTZ",
            "ALTER TABLE item_set_matches ADD COLUMN IF NOT EXISTS manual_override BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE item_set_matches ADD COLUMN IF NOT EXISTS original_score DOUBLE PRECISION",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS availability VARCHAR(32) NOT NULL DEFAULT 'active'",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS source VARCHAR(32) NOT NULL DEFAULT 'scrape'",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS note TEXT",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS listing_type VARCHAR(32) NOT NULL DEFAULT 'unknown'",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS shipping_cents INTEGER",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS shipping_note TEXT",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS price_origin VARCHAR(32) NOT NULL DEFAULT 'live'",
            "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS settled_price_cents INTEGER",
        ]
        async with self.engine.begin() as conn:
            for stmt in statements:
                try:
                    await conn.execute(text(stmt))
                except Exception:
                    pass


def _parse_price_to_cents(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        return int((Decimal(str(value).replace(",", ".")) * 100).to_integral_value())
    except (InvalidOperation, ValueError):
        return None


def _parse_release_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m", "%Y-%m-%d", "%Y"):
        try:
            dt = datetime.strptime(text, fmt)
            if fmt == "%Y":
                return date(dt.year, 1, 1)
            return dt.date()
        except ValueError:
            continue
    return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _clean_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.strip()
    return text or None
