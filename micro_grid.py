
from __future__ import annotations

import asyncio
import uuid
import weakref
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl
from app.logs.logging import log

# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class MicroGridConfig:
    """Immutable definition of a single micro-grid."""
    name: str                                       # e.g. "hot_tickers"
    table_name: str                                 # DB table, e.g. "micro_hot_tickers"
    primary_keys: Tuple[str, ...]                   # e.g. ("id",)
    columns: Dict[str, Any]                         # {col_name: default_value} — schema
    column_types: Dict[str, pl.DataType]            # polars types per column
    rules_enabled: bool = True
    persist: bool = True

    @property
    def room(self) -> str:
        return f"MICRO.{self.name.upper()}"

    @property
    def grid_id(self) -> str:
        return f"micro_{self.name}"

    @property
    def schema(self) -> Dict[str, pl.DataType]:
        return dict(self.column_types)


@dataclass(frozen=True)
class MicroGridGroup:
    """Groups multiple micro-grids as tabs in a single modal."""
    name: str                                       # e.g. "pt_tools"
    display_name: str                               # e.g. "Portfolio Tools"
    grids: Tuple[str, ...]                          # ordered micro-grid names

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "grids": list(self.grids),
        }


# =============================================================================
# Registry — singleton
# =============================================================================

class MicroGridRegistry:
    """Central registry for micro-grid configs and groups."""

    def __init__(self) -> None:
        self._configs: Dict[str, MicroGridConfig] = {}
        self._groups: Dict[str, MicroGridGroup] = {}
        self._actors: Dict[str, MicroGridActor] = {}

    # -- configs --

    def register(self, config: MicroGridConfig) -> None:
        if config.name in self._configs:
            raise ValueError(f"Micro-grid already registered: {config.name}")
        self._configs[config.name] = config

    def get_config(self, name: str) -> MicroGridConfig:
        cfg = self._configs.get(name)
        if cfg is None:
            raise KeyError(f"Unknown micro-grid: {name}")
        return cfg

    def list_configs(self) -> List[MicroGridConfig]:
        return list(self._configs.values())

    # -- groups --

    def register_group(self, group: MicroGridGroup) -> None:
        if group.name in self._groups:
            raise ValueError(f"Micro-grid group already registered: {group.name}")
        for g in group.grids:
            if g not in self._configs:
                raise ValueError(f"Micro-grid group '{group.name}' references unknown grid: {g}")
        self._groups[group.name] = group

    def get_group(self, name: str) -> MicroGridGroup:
        grp = self._groups.get(name)
        if grp is None:
            raise KeyError(f"Unknown micro-grid group: {name}")
        return grp

    def list_groups(self) -> List[MicroGridGroup]:
        return list(self._groups.values())

    # -- actors --

    def get_actor(self, name: str) -> MicroGridActor:
        actor = self._actors.get(name)
        if actor is None:
            raise KeyError(f"Micro-grid actor not initialized: {name}")
        return actor

    def set_actor(self, name: str, actor: MicroGridActor) -> None:
        self._actors[name] = actor

    async def shutdown(self) -> None:
        for actor in self._actors.values():
            try:
                if actor._dirty:
                    await actor.persist()
            except Exception as e:
                await log.error(f"[MicroGrid] Error persisting {actor.config.name} on shutdown: {e}")
        self._actors.clear()


# Module-level singleton
_registry = MicroGridRegistry()


def get_micro_registry() -> MicroGridRegistry:
    return _registry


def register_micro_grid(config: MicroGridConfig) -> None:
    _registry.register(config)


def register_micro_grid_group(group: MicroGridGroup) -> None:
    _registry.register_group(group)


def get_micro_actor(name: str) -> MicroGridActor:
    return _registry.get_actor(name)


def get_micro_group(name: str) -> MicroGridGroup:
    return _registry.get_group(name)


# =============================================================================
# MicroGridActor — lightweight in-memory actor
# =============================================================================

class MicroGridActor:
    """
    Simplified actor for a single global micro-grid.
    No MVCC, no slices, no filters, no hibernation.
    Uses an asyncio.Lock to serialize mutations.
    """

    __slots__ = (
        "config", "_data", "_subscribers", "_dirty",
        "_lock", "_loaded", "_persist_dirty_ids",
        "_persist_deleted_ids",
    )

    def __init__(self, config: MicroGridConfig) -> None:
        self.config = config
        self._data: pl.DataFrame = pl.DataFrame(
            schema={col: config.column_types[col] for col in config.columns}
        )
        self._subscribers: Dict[str, weakref.ref] = {}
        self._dirty: bool = False
        self._lock = asyncio.Lock()
        self._loaded: bool = False
        self._persist_dirty_ids: Set[str] = set()
        self._persist_deleted_ids: Set[str] = set()

    # -- DB lifecycle --

    async def load_from_db(self) -> None:
        from app.server import get_db
        db = get_db()
        table_name = self.config.table_name

        exists = await db._table_exists(table_name)
        if not exists:
            if self.config.persist:
                await self._create_table(db)
            self._loaded = True
            return

        try:
            df = await db.select(table_name)
            if df is not None and len(df) > 0:
                # Align schema — only keep columns we know about
                known = set(self.config.columns.keys())
                keep = [c for c in df.columns if c in known]
                if keep:
                    df = df.select(keep)
                    # Cast to expected types
                    casts = {}
                    for col in df.columns:
                        expected = self.config.column_types.get(col)
                        if expected and df[col].dtype != expected:
                            casts[col] = expected
                    if casts:
                        df = df.cast(casts, strict=False)
                    self._data = df
                await log.info(f"[MicroGrid] Loaded {len(self._data)} rows from {table_name}")
            else:
                await log.info(f"[MicroGrid] Table {table_name} exists but is empty")
        except Exception as e:
            await log.error(f"[MicroGrid] Failed to load from {table_name}: {e}")

        self._loaded = True

    async def _create_table(self, db) -> None:
        """Create the backing table with the configured schema."""
        schema_df = pl.DataFrame(
            schema={col: self.config.column_types[col] for col in self.config.columns}
        )
        try:
            await db.upsert(self.config.table_name, schema_df, primary_keys=list(self.config.primary_keys))
            await log.info(f"[MicroGrid] Created table: {self.config.table_name}")
        except Exception as e:
            await log.error(f"[MicroGrid] Failed to create table {self.config.table_name}: {e}")

    async def persist(self) -> None:
        """Flush dirty rows to the database."""
        if not self.config.persist:
            return
        from app.server import get_db
        db = get_db()

        dirty_ids = set()
        deleted_ids = set()
        data_snapshot = None

        try:
            async with self._lock:
                if not self._dirty:
                    return
                dirty_ids = set(self._persist_dirty_ids)
                deleted_ids = set(self._persist_deleted_ids)
                self._persist_dirty_ids.clear()
                self._persist_deleted_ids.clear()
                # _dirty stays True until DB ops succeed
                data_snapshot = self._data.clone()

            # Delete removed rows
            if deleted_ids:
                pk_col = self.config.primary_keys[0]
                for rid in deleted_ids:
                    try:
                        await db.delete(
                            self.config.table_name,
                            filters={pk_col: rid},
                        )
                    except Exception as e:
                        await log.error(f"[MicroGrid] Delete failed for {rid}: {e}")

            # Upsert dirty rows
            if dirty_ids:
                pk_col = self.config.primary_keys[0]
                dirty_df = data_snapshot.filter(pl.col(pk_col).is_in(list(dirty_ids)))
                if len(dirty_df) > 0:
                    await db.upsert(
                        self.config.table_name,
                        dirty_df,
                        primary_keys=list(self.config.primary_keys),
                    )

            # Success — mark clean only if no new dirty state accumulated
            async with self._lock:
                self._dirty = bool(self._persist_dirty_ids or self._persist_deleted_ids)

            await log.info(f"[MicroGrid] Persisted {self.config.name}: "
                           f"{len(dirty_ids)} upserted, {len(deleted_ids)} deleted")
        except Exception as e:
            await log.error(f"[MicroGrid] Persist failed for {self.config.name}: {e}")
            # Merge back for retry
            async with self._lock:
                self._persist_dirty_ids.update(dirty_ids)
                self._persist_deleted_ids.update(deleted_ids)
                self._dirty = True

    # -- Subscriber management --

    def add_subscriber(self, ws) -> str:
        token = str(uuid.uuid4())
        self._subscribers[token] = weakref.ref(ws)
        # Store token on ws for removal lookup
        if not hasattr(ws, '_micro_sub_tokens'):
            ws._micro_sub_tokens = {}
        ws._micro_sub_tokens[self.config.name] = token
        return token

    def remove_subscriber(self, ws) -> None:
        token = getattr(ws, '_micro_sub_tokens', {}).get(self.config.name)
        if token:
            self._subscribers.pop(token, None)
            ws._micro_sub_tokens.pop(self.config.name, None)
        else:
            # Fallback: search for matching ws ref
            to_remove = [t for t, ref in self._subscribers.items() if ref() is ws]
            for t in to_remove:
                del self._subscribers[t]

    def subscriber_count(self) -> int:
        dead = [t for t, ref in self._subscribers.items() if ref() is None]
        for t in dead:
            del self._subscribers[t]
        return len(self._subscribers)

    def get_live_subscribers(self) -> list:
        """Return list of live websocket references, pruning dead ones."""
        live = []
        dead = []
        for token, ref in self._subscribers.items():
            ws = ref()
            if ws is None:
                dead.append(token)
            else:
                live.append(ws)
        for t in dead:
            del self._subscribers[t]
        return live

    # -- Data operations --

    def snapshot(self) -> pl.DataFrame:
        """Return a copy of current data."""
        return self._data.clone()

    def snapshot_as_rows(self) -> List[Dict[str, Any]]:
        """Return data as list of dicts (for JSON serialization)."""
        return self._data.to_dicts()

    async def apply_edit(
        self,
        payloads: Dict[str, Any],
        user: str = "unknown",
    ) -> Optional[pl.DataFrame]:
        """
        Apply add/update/remove payloads to the in-memory data.

        payloads format:
            { "add": [...rows], "update": [...rows], "remove": [...rows] }

        Returns: DataFrame of changed rows (the delta), or None if no changes.
        """
        pk_col = self.config.primary_keys[0]
        add_delta_rows: List[Dict[str, Any]] = []
        update_delta_rows: List[Dict[str, Any]] = []
        has_removes = False

        async with self._lock:
            current = self._data

            # --- Add ---
            add_rows = payloads.get("add") or []
            if add_rows:
                new_rows = []
                for row in add_rows:
                    # Auto-generate ID if needed
                    if pk_col not in row or not row[pk_col]:
                        row = dict(row)
                        row[pk_col] = str(uuid.uuid4())
                    # Fill defaults for missing columns
                    filled = {}
                    for col, default in self.config.columns.items():
                        filled[col] = row.get(col, default)
                    new_rows.append(filled)
                    add_delta_rows.append(filled)

                if new_rows:
                    new_df = pl.DataFrame(
                        new_rows,
                        schema={c: self.config.column_types[c] for c in self.config.columns},
                    )
                    # Deduplicate: remove existing rows with same PK before adding
                    new_ids = [str(r[pk_col]) for r in new_rows]
                    dedup_mask = current[pk_col].cast(pl.String).is_in(new_ids)
                    if dedup_mask.sum() > 0:
                        current = current.filter(~dedup_mask)
                    current = pl.concat([current, new_df], how="diagonal_relaxed")
                    for r in new_rows:
                        self._persist_dirty_ids.add(str(r[pk_col]))

            # --- Update ---
            update_rows = payloads.get("update") or []
            if update_rows:
                for row in update_rows:
                    rid = row.get(pk_col)
                    if rid is None:
                        continue
                    rid_str = str(rid)
                    # Find and update the row
                    mask = current[pk_col].cast(pl.String) == rid_str
                    if mask.sum() == 0:
                        continue
                    for col, val in row.items():
                        if col == pk_col:
                            continue
                        if col not in current.columns:
                            continue
                        expected_type = self.config.column_types.get(col, pl.String)
                        current = current.with_columns(
                            pl.when(mask)
                            .then(pl.lit(val, dtype=expected_type))
                            .otherwise(pl.col(col))
                            .alias(col)
                        )
                    # Materialize the updated row for delta
                    updated = current.filter(mask)
                    if len(updated) > 0:
                        update_delta_rows.extend(updated.to_dicts())
                    self._persist_dirty_ids.add(rid_str)

            # --- Remove ---
            remove_rows = payloads.get("remove") or []
            if remove_rows:
                remove_id_set = set()
                for row in remove_rows:
                    rid = row.get(pk_col)
                    if rid is not None:
                        remove_id_set.add(str(rid))
                if remove_id_set:
                    remove_mask = current[pk_col].cast(pl.String).is_in(list(remove_id_set))
                    if remove_mask.sum() > 0:
                        has_removes = True
                    current = current.filter(~remove_mask)
                    for rid in remove_id_set:
                        self._persist_deleted_ids.add(rid)
                        self._persist_dirty_ids.discard(rid)

            self._data = current
            all_delta_rows = add_delta_rows + update_delta_rows
            if all_delta_rows or has_removes:
                self._dirty = True

            # Build delta DataFrame inside the lock to avoid races
            if all_delta_rows:
                delta_df = pl.DataFrame(
                    all_delta_rows,
                    schema={c: self.config.column_types[c] for c in self.config.columns},
                    strict=False,
                )
                # Attach counts so callers can slice add vs update deltas
                delta_df._micro_add_count = len(add_delta_rows)
            elif has_removes:
                delta_df = pl.DataFrame(
                    schema={c: self.config.column_types[c] for c in self.config.columns},
                )
                delta_df._micro_add_count = 0
            else:
                return None

        return delta_df


# =============================================================================
# Initialization
# =============================================================================

async def init_micro_grids() -> None:
    """Load all registered micro-grids from DB. Call at startup."""
    registry = _registry
    for config in registry.list_configs():
        actor = MicroGridActor(config)
        await actor.load_from_db()
        registry.set_actor(config.name, actor)
        await log.info(f"[MicroGrid] Initialized: {config.name} ({len(actor._data)} rows)")


async def shutdown_micro_grids() -> None:
    """Persist and clean up all micro-grid actors."""
    await _registry.shutdown()
