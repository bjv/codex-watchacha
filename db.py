import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from typing import Pattern

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    delete,
    insert,
    select,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class Provider(Base):
    __tablename__ = "providers"

    key = Column(String(64), primary_key=True)
    label = Column(String(255), nullable=False)

    items = relationship("Item", back_populates="provider", cascade="all, delete-orphan")


item_clusters_table = Table(
    "item_clusters",
    Base.metadata,
    Column("item_id", BigInteger, ForeignKey("items.id", ondelete="CASCADE"), primary_key=True),
    Column("cluster_key", String(128), ForeignKey("clusters.cluster_key", ondelete="CASCADE"), primary_key=True),
    Column("created_ts", DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)),
)


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
    first_seen_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    last_seen_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    provider = relationship("Provider", back_populates="items")
    snapshots = relationship("Snapshot", back_populates="item", cascade="all, delete-orphan", order_by="Snapshot.observed_ts.desc()")
    clusters = relationship("Cluster", secondary=item_clusters_table, back_populates="items")

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
    title = Column(Text, nullable=False)
    fingerprint = Column(String(64), nullable=False)

    item = relationship("Item", back_populates="snapshots")

    __table_args__ = (
        UniqueConstraint("item_id", "fingerprint", name="uq_snapshots_item_fingerprint"),
    )


class Cluster(Base):
    __tablename__ = "clusters"

    cluster_key = Column(String(128), primary_key=True)
    created_ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    items = relationship("Item", secondary=item_clusters_table, back_populates="clusters")


@dataclass(slots=True)
class UpsertResult:
    item_id: int
    is_new_item: bool
    snapshot_created: bool
    changed_fields: Dict[str, Tuple[Any, Any]]


DEFAULT_CLUSTER_PATTERNS: List[Pattern[str]] = [re.compile(r"\b\d{6}\b")]


def compile_cluster_patterns(patterns: Optional[Sequence[str]]) -> List[Pattern[str]]:
    compiled: List[Pattern[str]] = []
    if patterns:
        for pattern in patterns:
            try:
                compiled.append(re.compile(pattern))
            except re.error:
                continue
    if not compiled:
        compiled = DEFAULT_CLUSTER_PATTERNS.copy()
    return compiled


def extract_cluster_keys(title: str, patterns: Sequence[Pattern[str]]) -> List[str]:
    if not title:
        return []
    values: List[str] = []
    seen = set()
    for pattern in patterns:
        for match in pattern.finditer(title):
            key = match.group(0)
            if key not in seen:
                seen.add(key)
                values.append(key)
    return values


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint_payload(data: Dict[str, Any]) -> str:
    fingerprint_fields = {
        "bid": data.get("bid_cents"),
        "buyout": data.get("buyout_cents"),
        "direct_buy": bool(data.get("direct_buy")),
        "vb_flag": bool(data.get("vb_flag")),
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
        self._cluster_cache: set[str] = set()

    async def init_models(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

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
        fp = _fingerprint_payload(payload)

        async with self.session_factory() as session:
            async with session.begin():
                await self._ensure_provider(session, provider_key, provider_label)

                item = await self._get_item(session, provider_key, payload["provider_item_id"])
                changed_fields: Dict[str, Tuple[Any, Any]] = {}
                is_new = item is None

                cluster_keys_normalized = self._normalize_cluster_keys(payload.get("cluster_keys"))

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
                        first_seen_ts=now,
                        last_seen_ts=now,
                    )
                    session.add(item)
                    await session.flush()

                    changed_fields = {
                        "title": (None, item.title),
                        "direct_buy": (None, item.direct_buy),
                        "vb_flag": (None, item.vb_flag),
                        "current_bid_cents": (None, item.current_bid_cents),
                        "current_buyout_cents": (None, item.current_buyout_cents),
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
                    }
                    for attr, new_val in updates.items():
                        old_val = getattr(item, attr)
                        if old_val != new_val:
                            setattr(item, attr, new_val)
                            if attr in {"title", "direct_buy", "vb_flag", "current_bid_cents", "current_buyout_cents"}:
                                changed_fields[attr] = (old_val, new_val)

                    item.last_seen_ts = now

                await self._sync_item_clusters(session, item, cluster_keys_normalized)

                last_fingerprint = None
                if not is_new:
                    stmt = select(Snapshot.fingerprint).where(Snapshot.item_id == item.id).order_by(Snapshot.observed_ts.desc()).limit(1)
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
            .where(Item.provider_key == provider_key, Item.provider_item_id == provider_item_id)
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    def _normalize_cluster_keys(self, keys: Optional[Sequence[str]]) -> List[str]:
        if not keys:
            return []
        normalized: List[str] = []
        seen = set()
        for key in keys:
            if not key:
                continue
            val = key.strip()
            if not val or val in seen:
                continue
            seen.add(val)
            normalized.append(val)
        return normalized

    async def _sync_item_clusters(self, session: AsyncSession, item: Item, cluster_keys: List[str]) -> None:
        if cluster_keys is None:
            return

        stmt = select(item_clusters_table.c.cluster_key).where(item_clusters_table.c.item_id == item.id)
        result = await session.execute(stmt)
        existing_keys = {row[0] for row in result.fetchall()}
        desired_keys = set(cluster_keys)

        to_add = desired_keys - existing_keys
        to_remove = existing_keys - desired_keys

        if to_add:
            for key in to_add:
                await self._ensure_cluster(session, key)
                await session.execute(
                    insert(item_clusters_table).values(item_id=item.id, cluster_key=key)
                )

        if to_remove:
            await session.execute(
                delete(item_clusters_table)
                .where(item_clusters_table.c.item_id == item.id)
                .where(item_clusters_table.c.cluster_key.in_(to_remove))
            )

    async def _ensure_cluster(self, session: AsyncSession, cluster_key: str) -> Cluster:
        cluster = await session.get(Cluster, cluster_key)
        if cluster is None:
            cluster = Cluster(cluster_key=cluster_key)
            session.add(cluster)
            await session.flush([cluster])
        self._cluster_cache.add(cluster_key)
        return cluster
