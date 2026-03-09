
from __future__ import annotations

import asyncio
import atexit
import logging
import os
import secrets
import threading
import time
import weakref
from collections import OrderedDict, defaultdict
from queue import PriorityQueue
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import pprint
from app.helpers.type_helpers import ensure_list
from msgspec.json import encode

import apsw
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
from app.helpers.pandas_helpers import pd
from app.helpers.polars_hyper_plugin import *

try:
    import connectorx as cx

    _HAS_CONNECTORX = True
except ImportError:
    cx = None  # type: ignore[assignment]
    _HAS_CONNECTORX = False

from app.logs.logging import log
from app.helpers.loop_helpers import set_uvloop

_fallback_logger = logging.getLogger("cadesql.connectorx_fallback")

# ---------- Core constants / tunables ----------

CLEAR_SENTINEL = -999999

DEFAULT_QUERY_TIMEOUT_MS = 30_000
READ_WORKERS = min(8, (os.cpu_count() or 4) + 1)
WRITE_BATCH_SIZE = 1_000
BUSY_TIMEOUT_MS = 5_000

CACHE_KIB = 64 * 1024                  # 64 MB page cache
MMAP_BYTES = 268_435_456               # 256 MB mmap
WAL_AUTOCHECKPOINT = 1_000

SCHEMA_CACHE_SIZE = 256
QUERY_CACHE_SIZE = 256
QUERY_CACHE_TTL_MS = 1_000

AUTO_INDEX_HIT_THRESHOLD = 64        # threshold of filter hits before adding index
AUTO_INDEX_REMOVE_THRESHOLD = 10     # unused filters below this after idle window are candidates for removal
AUTO_INDEX_CHECK_INTERVAL_SEC = 60.0
AUTO_INDEX_STALE_SECONDS = 3_600     # 1 hour since last use before considering index removal

PARTITION_COLUMN_LIMIT = 200  # max non-PK columns per vertical partition

# ---------- Exceptions ----------
class CadeSQLError(Exception): pass
class CadeSQLConnectionError(CadeSQLError): pass
class CadeSQLSchemaError(CadeSQLError): pass
class CadeSQLQueryError(CadeSQLError): pass
class CadeSQLQueryTimeout(CadeSQLQueryError): pass

# ---------- DataFrame adapter (Polars-centric, columnar-first) ----------
class DataFrameAdapter:
    @staticmethod
    def to_polars(df: Any, columns: Optional[Sequence[str]] = None) -> pl.DataFrame:
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        elif isinstance(df, pl.DataFrame):
            pass
        elif isinstance(df, pa.Table):
            df = pl.from_arrow(df)
        elif isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
        else:
            raise CadeSQLError(f"Unsupported dataframe type: {type(df)}")
        if columns is not None:
            df = df.select(list(columns))
        return df

    @staticmethod
    def to_records(df: Any, columns: Optional[Sequence[str]] = None) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        pdf = DataFrameAdapter.to_polars(df, columns=columns)
        cols = pdf.columns
        rows = [tuple(row) for row in pdf.iter_rows()]
        return cols, rows

    @staticmethod
    def empty_like(schema_cols: Sequence[str], schema_types: Dict[str, str]) -> pl.DataFrame:
        series_map = {}
        for col in schema_cols:
            sql_type = (schema_types.get(col) or "").upper()
            if "INT" in sql_type:
                dt = pl.Int64
            elif "REAL" in sql_type or "FLOA" in sql_type or "DOUB" in sql_type:
                dt = pl.Float64
            elif "BOOL" in sql_type:
                dt = pl.Boolean
            elif "DATE" in sql_type or "TIME" in sql_type:
                dt = pl.Datetime
            else:
                dt = pl.String
            series_map[col] = pl.Series(name=col, values=[], dtype=dt)
        return pl.DataFrame(series_map, strict=False, infer_schema_length=None)

    @staticmethod
    def from_records(
            rows: Sequence[Sequence[Any]],
            columns: Sequence[str],
            as_format: Optional[str],
    ):
        # Fast path for simple scalar outputs
        if len(columns) == 1 and as_format in ("item", "list", "set"):
            values = [row[0] for row in rows]
            if as_format == "item":
                return values[0] if values else None
            if as_format == "list":
                return values
            return set(values)

        df = pl.DataFrame(rows, schema=list(columns), orient="row", infer_schema_length=None)

        if as_format is None or as_format == "polars":
            return df
        if as_format == "polars_lazy":
            return df.lazy()
        if as_format == "pandas":
            return df.to_pandas()
        if as_format == "arrow":
            return df.to_arrow()
        if as_format in ("item", "list", "set"):
            values = df.select(columns[0]).to_series().to_list()
            if as_format == "item":
                return values[0] if values else None
            if as_format == "list":
                return values
            return set(values)
        raise CadeSQLError(f"Unsupported output format: {as_format}")


# ---------- SQLite type helpers ----------

def _sqlite_affinity(sql_type: str) -> str:
    s = (sql_type or "").upper()
    if "INT" in s:
        return "INTEGER"
    if "CHAR" in s or "CLOB" in s or "TEXT" in s:
        return "TEXT"
    if "BLOB" in s:
        return "BLOB"
    if "REAL" in s or "FLOA" in s or "DOUB" in s:
        return "REAL"
    return "NUMERIC"


def _polars_dtype_for_sqlite(sql_type: str):
    a = _sqlite_affinity(sql_type)
    if a == "INTEGER":
        return pl.Int64
    if a == "REAL":
        return pl.Float64
    if a == "BLOB":
        return pl.Binary
    return pl.String


def _get_sqlite_type(dtype: Union[str, pl.DataType, pa.DataType]) -> str:
    if isinstance(dtype, str):
        return dtype
    if isinstance(dtype, pa.DataType):
        if pa.types.is_integer(dtype):
            return "INTEGER"
        if pa.types.is_floating(dtype):
            return "REAL"
        if pa.types.is_boolean(dtype):
            return "INTEGER"
        if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
            return "BLOB"
        if pa.types.is_date(dtype) or pa.types.is_timestamp(dtype) or pa.types.is_time(dtype):
            return "TEXT"
        return "TEXT"
    name = str(dtype).lower()
    if "int" in name:
        return "INTEGER"
    if "float" in name or "double" in name or "decimal" in name:
        return "REAL"
    if "bool" in name:
        return "INTEGER"
    if "date" in name or "time" in name:
        return "TEXT"
    if "binary" in name or "blob" in name:
        return "BLOB"
    return "TEXT"


def _coerce_frame_to_sqlite_schema(
        df: pl.DataFrame,
        schema_sql: Dict[str, str],
        drop_on_fail: bool,
        warn_cb,
) -> Tuple[pl.DataFrame, List[str]]:
    if df.is_empty() or not schema_sql:
        return df, []
    keep_cols: List[str] = []
    drop_cols: List[str] = []
    cast_exprs: List[pl.Expr] = []
    for c in df.columns:
        if c not in schema_sql:
            keep_cols.append(c)
            continue
        target = _polars_dtype_for_sqlite(schema_sql[c])
        try:
            if df[c].dtype != target:
                cast_exprs.append(pl.col(c).cast(target, strict=False).alias(c))
            else:
                keep_cols.append(c)
        except Exception as e:
            drop_cols.append(c)
            if warn_cb:
                warn_cb(f"[SQL] Coerce failed for column '{c}' to {target}: {e}. Dropping from insert.")
    if cast_exprs:
        try:
            df = df.with_columns(cast_exprs)
        except Exception as e:
            for expr in cast_exprs:
                cname = expr.meta.output_name()
                if cname not in drop_cols:
                    drop_cols.append(cname)
                    if warn_cb:
                        warn_cb(f"[SQL] Vectorized coercion failed for '{cname}', dropping. {e}")
    if drop_on_fail and drop_cols:
        df = df.drop(drop_cols)
    return df, drop_cols


# ---------- Caches ----------

class AsyncLRUCache:
    def __init__(self, maxsize: int = 128):
        self.maxsize = maxsize
        self.cache: "OrderedDict[Any, Any]" = OrderedDict()
        self.lock = asyncio.Lock()
        self._pending: Dict[Any, asyncio.Future] = {}

    async def get(self, key: Any, factory):
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            fut = self._pending.get(key)
            if fut is None:
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                self._pending[key] = fut
                creator = True
            else:
                creator = False

        if not creator:
            return await fut

        try:
            val = await factory()
            async with self.lock:
                self.cache[key] = val
                if len(self.cache) > self.maxsize:
                    self.cache.popitem(last=False)
            fut.set_result(val)
            return val
        except Exception as e:
            fut.set_exception(e)
            raise
        finally:
            async with self.lock:
                fut = self._pending.pop(key, None)

    async def clear(self):
        async with self.lock:
            self.cache.clear()
            self._pending.clear()


class AsyncTTLCache:
    def __init__(self, maxsize: int, ttl_ms: int):
        self.maxsize = maxsize
        self.ttl_sec = ttl_ms / 1000.0
        self.data: "OrderedDict[Any, Tuple[float, Any]]" = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, key: Any):
        async with self.lock:
            item = self.data.get(key)
            if item is None:
                return None
            expires, value = item
            if expires < time.monotonic():
                self.data.pop(key, None)
                return None
            self.data.move_to_end(key)
            return value

    async def set(self, key: Any, value: Any, ttl_ms: Optional[int] = None):
        ttl_sec = self.ttl_sec if ttl_ms is None else ttl_ms / 1000.0
        async with self.lock:
            self.data[key] = (time.monotonic() + ttl_sec, value)
            self.data.move_to_end(key)
            while len(self.data) > self.maxsize:
                self.data.popitem(last=False)

    async def clear(self):
        async with self.lock:
            self.data.clear()


# ---------- Schema / partition metadata ----------

@dataclass
class TableSchema:
    table: str
    columns: List[str]
    column_types: Dict[str, str]
    primary_keys: List[str]
    is_partitioned: bool = False
    partitions: List["PartitionInfo"] = field(default_factory=list)

    def __repr__(self):
        return str(pprint.pformat(self.column_types, indent=2, sort_dicts=False, compact=True))


@dataclass
class PartitionInfo:
    table: str
    column: str
    partition: str


# ---------- Auto-index metadata ----------

@dataclass(slots=True)
class IndexStats:
    column: str
    table: str
    query_count: int = 0
    filter_count: int = 0
    last_used: float = field(default_factory=time.monotonic)
    has_index: bool = False


class AutoIndexer:
    def __init__(
            self,
            index_threshold: int = AUTO_INDEX_HIT_THRESHOLD,
            remove_threshold: int = AUTO_INDEX_REMOVE_THRESHOLD,
            check_interval: float = AUTO_INDEX_CHECK_INTERVAL_SEC,
            auto_add: bool = True,
            auto_remove: bool = True,
            max_columns_per_table: int = 256,
    ):
        self._stats: Dict[str, IndexStats] = {}
        self._lock = asyncio.Lock()
        self._index_threshold = index_threshold
        self._remove_threshold = remove_threshold
        self._check_interval = check_interval
        self._enabled = False
        self._db_ref: Optional[weakref.ReferenceType["CadeSQL"]] = None
        self._task: Optional[asyncio.Task] = None
        self._auto_add = auto_add
        self._auto_remove = auto_remove
        self._max_columns_per_table = max_columns_per_table

    def attach(self, db: "CadeSQL") -> None:
        """Attach to CadeSQL via weakref to avoid reference cycles."""
        self._db_ref = weakref.ref(db)

    async def start(self) -> None:
        """Start background indexing task."""
        if self._enabled:
            return
        if self._db_ref is None or self._db_ref() is None:
            return
        self._enabled = True
        self._task = asyncio.create_task(self._background_indexer(), name="CadeSQL.AutoIndexer")

    async def stop(self) -> None:
        """Stop background indexing."""
        self._enabled = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def record_query(self, table: str, columns: List[str]) -> None:
        """Record columns touched by a query."""
        if not self._enabled:
            return
        key_prefix = f"{table}."
        async with self._lock:
            # Cap per-table distinct column count to avoid unbounded growth.
            existing_cols = {
                k.split(".", 1)[1]
                for k in self._stats.keys()
                if k.startswith(key_prefix)
            }
            for col in columns:
                if col in existing_cols:
                    stat = self._stats[f"{table}.{col}"]
                    stat.query_count += 1
                    stat.last_used = time.monotonic()
                    continue
                if self._max_columns_per_table and (len(existing_cols) >= self._max_columns_per_table):
                    continue
                key = f"{table}.{col}"
                stat = self._stats.get(key)
                if stat is None:
                    stat = IndexStats(column=col, table=table)
                    self._stats[key] = stat
                stat.query_count += 1
                stat.last_used = time.monotonic()
                existing_cols.add(col)

    async def record_filter(self, table: str, columns: List[str]) -> None:
        """Record columns actually used in WHERE filters."""
        if not self._enabled:
            return
        key_prefix = f"{table}."
        async with self._lock:
            existing_cols = {
                k.split(".", 1)[1]
                for k in self._stats.keys()
                if k.startswith(key_prefix)
            }
            for col in columns:
                if col in existing_cols:
                    stat = self._stats[f"{table}.{col}"]
                else:
                    if len(existing_cols) >= self._max_columns_per_table:
                        continue
                    key = f"{table}.{col}"
                    stat = self._stats.get(key)
                    if stat is None:
                        stat = IndexStats(column=col, table=table)
                        self._stats[key] = stat
                    existing_cols.add(col)
                stat.filter_count += 1
                stat.last_used = time.monotonic()

    async def _background_indexer(self) -> None:
        """Background loop to adjust indices."""
        while self._enabled:
            try:
                await asyncio.sleep(self._check_interval)
                await self._analyze_and_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await log.warning("Auto-indexer error", error=str(e))

    async def _analyze_and_update(self) -> None:
        """Analyze collected stats and add/remove indices as needed."""
        db = self._db_ref() if self._db_ref is not None else None
        if db is None:
            return

        now = time.monotonic()
        to_delete: List[str] = []

        async with self._lock:
            stats_items = list(self._stats.items())

        for key, stat in stats_items:
            # Add index if heavily filtered and enabled
            if self._auto_add and not stat.has_index and stat.filter_count >= self._index_threshold:
                try:
                    await db.add_indices(stat.table, [stat.column])
                    stat.has_index = True
                    stat.last_used = now
                    #await log.debug("Auto index added", table=stat.table, column=stat.column)
                except Exception as exc:
                    await log.warning("Auto index add failed", table=stat.table, column=stat.column, error=str(exc))

            elif self._auto_remove and stat.has_index:
                age = now - stat.last_used
                if age > AUTO_INDEX_STALE_SECONDS and stat.filter_count < self._remove_threshold:
                    try:
                        # CadeSQL.remove_indices expects index names; auto indices are named auto_idx_<table>_<col>.
                        idx_name = f"auto_idx_{stat.table}_{stat.column}"
                        await db.remove_indices(stat.table, [idx_name])
                        stat.has_index = False
                        #await log.debug("Auto index removed", table=stat.table, column=stat.column)
                    except Exception as exc:
                        await log.warning("Auto index remove failed", table=stat.table, column=stat.column, error=str(exc))

            # Prune stale stats entirely to bound memory
            if not stat.has_index and (now - stat.last_used) > (AUTO_INDEX_STALE_SECONDS * 4):
                to_delete.append(key)

        if to_delete:
            async with self._lock:
                for key in to_delete:
                    self._stats.pop(key, None)


# ---------- Filter parser ----------

class FilterParser:
    """
    Filter normalization with looser semantics:

    - {col: value} is treated as equality regardless of type.
    - IN / BETWEEN only when explicitly requested via "op".
    - Supports boolean combinators:
        * {"OR": [expr1, expr2, ...]}  -> OR between sub-expressions
        * {"AND": [expr1, expr2, ...]} -> AND between sub-expressions
      where each exprX can itself be a mapping, list, or another OR/AND tree.
    - Unknown columns can be ignored (loose mode) or raise (strict mode).
    """

    @staticmethod
    def _is_date_like(sql_type: Optional[str]) -> bool:
        if not sql_type:
            return False
        t = sql_type.upper()
        return "DATE" in t or "TIME" in t

    @staticmethod
    def _normalize_column_name(
            column: str,
            schema: TableSchema,
    ) -> Tuple[str, bool]:
        negated = False
        if column.startswith("~"):
            negated = True
            column = column[1:]
        col = column
        if schema is not None:
            if col not in schema.columns:
                raise CadeSQLSchemaError(f"Unknown column in filters: {column}")
        return col, negated

    @staticmethod
    def _infer_op(
            column: str,
            value: Any,
            sql_type: Optional[str],
            negated: bool,
    ) -> str:
        # Looser semantics: mapping {col: value} is always equality (or !=) except for NULL.
        if value is None:
            return "is_not_null" if negated else "is_null"
        elif isinstance(value, (list, tuple, set)):
            return "not_in" if negated else "in"
        return "ne" if negated else "eq"

    @staticmethod
    def normalize_filters(
            filters: Any,
            schema: TableSchema = None,
            ignore_unknown: bool = False,
    ) -> List[List[Dict[str, Any]]]:
        """
        Normalize arbitrary filter expressions into DNF-like structure:
        List[Group], where each Group is List[Condition], and:
            - Groups are OR-ed together
            - Conditions inside a group are AND-ed together

        Supported inputs:
        - None
        - {column: value, ...}
        - {"column": ..., "op": ..., "value": ...}
        - [ {"column":..., "op":..., "value":...}, ... ]  (AND)
        - [ {col: val}, {col2: val2}, ... ]               (OR of AND-mappings)
        - {"OR": [expr1, expr2, ...]}
        - {"AND": [expr1, expr2, ...]}
        """
        if filters is None: return []
        groups: List[List[Dict[str, Any]]] = []

        def mapping_to_group(mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
            group: List[Dict[str, Any]] = []
            for col_key, val in mapping.items():
                try:
                    col, neg = FilterParser._normalize_column_name(col_key, schema)
                except CadeSQLSchemaError:
                    if ignore_unknown:
                        continue
                    raise
                sql_type = schema.column_types.get(col)
                op = FilterParser._infer_op(col, val, sql_type, neg)
                group.append({"column": col, "op": op, "value": val})
            return group

        # ----- Dict branch -----
        if isinstance(filters, dict):
            # Boolean combinators: {"OR": [...]} or {"AND": [...]}
            upper_keys = {str(k).upper() for k in filters.keys()}
            if "OR" in upper_keys or "AND" in upper_keys:
                or_key = next((k for k in filters.keys() if str(k).upper() == "OR"), None)
                and_key = next((k for k in filters.keys() if str(k).upper() == "AND"), None)

                if or_key is not None and and_key is not None:
                    raise CadeSQLSchemaError("Filter dict cannot contain both AND and OR at the same level.")

                key = or_key or and_key
                sub_filters = filters[key]
                if not isinstance(sub_filters, list):
                    raise CadeSQLSchemaError(f"{key} value must be a list of filter expressions.")

                # OR: union of groups from each sub-expression
                if or_key is not None:
                    for sub in sub_filters:
                        sub_groups = FilterParser.normalize_filters(
                            sub, schema, ignore_unknown=ignore_unknown
                        )
                        groups.extend(sub_groups)
                    return groups

                # AND: distributive combination of groups from each sub-expression
                current: List[List[Dict[str, Any]]] = [[]]
                for sub in sub_filters:
                    sub_groups = FilterParser.normalize_filters(
                        sub, schema, ignore_unknown=ignore_unknown
                    )
                    if not sub_groups:
                        continue
                    new_current: List[List[Dict[str, Any]]] = []
                    for base in current:
                        for g in sub_groups:
                            new_current.append(base + g)
                    current = new_current
                groups.extend(current)
                return groups

            # Explicit single condition with {column/op/value}
            if "column" in filters or "op" in filters or 'field' in filters:
                col_raw = filters.get("column", filters.get('field'))
                if col_raw is None:
                    raise CadeSQLSchemaError("Filter dict with op/column requires 'column'.")
                try:
                    col, neg = FilterParser._normalize_column_name(col_raw, schema)
                except CadeSQLSchemaError:
                    if ignore_unknown:
                        return []
                    raise
                val = filters.get("value")
                op = filters.get("op")
                if not op:
                    sql_type = schema.column_types.get(col)
                    op = FilterParser._infer_op(col, val, sql_type, neg)
                groups.append([{"column": col, "op": op, "value": val}])
            else:
                g = mapping_to_group(filters)
                if g:
                    groups.append(g)
            return groups

        # ----- List branch -----
        if isinstance(filters, list):
            if not filters:
                return []

            # List of explicit conditions -> single AND group
            if all(isinstance(item, dict) and ("column" in item or "op" in item) for item in filters):
                group: List[Dict[str, Any]] = []
                for f in filters:
                    col_raw = f.get("column", f.get('field'))
                    if col_raw is None:
                        raise CadeSQLSchemaError("Filter dict with op/column requires 'column'.")
                    try:
                        col, neg = FilterParser._normalize_column_name(col_raw, schema)
                    except CadeSQLSchemaError:
                        if ignore_unknown:
                            continue
                        raise
                    val = f.get("value")
                    op = f.get("op")
                    if not op:
                        sql_type = schema.column_types.get(col)
                        op = FilterParser._infer_op(col, val, sql_type, neg)
                    group.append({"column": col, "op": op, "value": val})
                if group:
                    groups.append(group)
            else:
                # List of mapping groups -> OR between groups, AND within each mapping
                for mapping in filters:
                    if not isinstance(mapping, dict):
                        raise CadeSQLSchemaError("List filters must be list of dicts.")
                    g = mapping_to_group(mapping)
                    if g:
                        groups.append(g)
            return groups

        raise CadeSQLSchemaError("Unsupported filters type; must be dict or list of dicts.")

    @staticmethod
    def build_where_clause(
            filters: Any,
            schema: TableSchema,
            param_style: str,
            supports_ilike: bool,
            strict: bool,
    ) -> Tuple[str, List[Any]]:
        groups = FilterParser.normalize_filters(filters, schema, ignore_unknown=not strict)
        if not groups:
            return "", []

        placeholder = "?" if param_style == "qmark" else "%s"
        params: List[Any] = []
        clauses: List[str] = []

        for group in groups:
            parts: List[str] = []
            for cond in group:
                col = cond["column"]
                op = str(cond["op"]).lower()
                val = cond.get("value")

                if op in ("is_null", "is null"):
                    parts.append(f"`{col}` IS NULL")
                    continue
                if op in ("is_not_null", "is not null", "not_null"):
                    parts.append(f"`{col}` IS NOT NULL")
                    continue

                if op in ("between", "not_between"):
                    if not isinstance(val, (list, tuple)) or len(val) != 2:
                        raise CadeSQLSchemaError(f"BETWEEN requires list/tuple of length 2 for column {col}")
                    op_sql = "NOT BETWEEN" if "not" in op else "BETWEEN"
                    parts.append(f"`{col}` {op_sql} {placeholder} AND {placeholder}")
                    params.extend([val[0], val[1]])
                    continue

                if op in ("in", "not_in"):
                    if not isinstance(val, (list, tuple, set)):
                        raise CadeSQLSchemaError(f"IN requires list/tuple/set for column {col}")
                    if not val:
                        continue
                    vals_list = list(val)
                    ph = ",".join(placeholder for _ in vals_list)
                    op_sql = "NOT IN" if "not" in op else "IN"
                    parts.append(f"`{col}` {op_sql} ({ph})")
                    params.extend(vals_list)
                    continue

                if op in ("like", "ilike", "not_like", "not_ilike"):
                    not_flag = "not" in op
                    is_ilike = "ilike" in op
                    if is_ilike and not supports_ilike:
                        expr = f"LOWER(`{col}`) LIKE LOWER({placeholder})"
                    else:
                        expr = f"`{col}` LIKE {placeholder}"
                    if not_flag:
                        expr = f"NOT ({expr})"
                    parts.append(expr)
                    params.append(val)
                    continue

                # Canonical comparison ops
                if op in ("eq", "=", "=="):
                    op_sql = "="
                elif op in ("ne", "!=", "<>"):
                    op_sql = "!="
                elif op in ("lt", "<"):
                    op_sql = "<"
                elif op in ("lte", "<="):
                    op_sql = "<="
                elif op in ("gt", ">"):
                    op_sql = ">"
                elif op in ("gte", ">="):
                    op_sql = ">="
                else:
                    raise CadeSQLSchemaError(f"Unsupported filter operation: {op}")

                parts.append(f"`{col}` {op_sql} {placeholder}")
                params.append(val)

            if parts:
                clauses.append("(" + " AND ".join(parts) + ")")

        if not clauses:
            return "", []
        where_sql = "WHERE " + " OR ".join(clauses)
        return where_sql, params

    @staticmethod
    def denormalize_filters(groups: Any) -> Any:
        """
        Reverse of normalize_filters for the OR-of-AND normal form.

        Input:
            - groups == None
            - groups == List[List[Condition]] (normalized form)
            - groups == List[Condition]
            - groups == Condition dict
        Output:
            - None
            - single Condition dict
            - {"AND": [cond1, cond2, ...]}
            - {"OR": [expr1, expr2, ...]}
              where each exprX is either a Condition dict or {"AND": [...]}.

        Examples:
            [[c1], [c2], [c3]] ->
                {"OR": [c1, c2, c3]}

            [[c1, c2]] ->
                {"AND": [c1, c2]}

            [[c1, c2], [c3]] ->
                {"OR": [ {"AND": [c1, c2]}, c3 ]}
        """
        if groups is None:
            return None

        # Already a logical dict -> return as-is
        if isinstance(groups, dict):
            return groups

        if isinstance(groups, list):
            if not groups:
                return None

            # Normalized form: list of list-of-dicts
            if all(isinstance(g, list) for g in groups):
                or_terms: List[Any] = []
                for g in groups:
                    if not g:
                        continue
                    if len(g) == 1:
                        or_terms.append(g[0])
                    else:
                        or_terms.append({"AND": g})
                if not or_terms:
                    return None
                if len(or_terms) == 1:
                    return or_terms[0]
                return {"OR": or_terms}

            # Single AND-group: list of condition dicts
            if all(isinstance(c, dict) for c in groups):
                if len(groups) == 1:
                    return groups[0]
                return {"AND": groups}

        # Fallback: unknown structure, return unchanged
        return groups


# ---------- SQLite writer actor (APSW, adapted) ----------

@dataclass
class _Job:
    kind: str
    sql: Optional[str] = None
    params: Optional[List[Any]] = None
    seq_params: Optional[List[Tuple[Any, ...]]] = None
    txid: Optional[int] = None
    loop: Optional[asyncio.AbstractEventLoop] = None
    fut: Optional[asyncio.Future] = None
    priority: int = 0


class _WriterActor:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self.q: "PriorityQueue[Tuple[int, int, _Job]]" = PriorityQueue()
        self.thread = threading.Thread(target=self._run, name="SQL-Writer", daemon=True)
        self.stop_evt = threading.Event()
        self.conn: Optional[apsw.Connection] = None
        self.current_txid: Optional[int] = None
        self._tx_counter = 0
        self._enqueue_counter = 0
        self.grouped_commit_wait_ms = 5
        self.running = False

    def configure_grouped_commit(self, wait_ms: int):
        self.grouped_commit_wait_ms = max(0, int(wait_ms))

    def _setup_conn(self) -> apsw.Connection:
        conn = apsw.Connection(self.db_path)
        conn.setbusytimeout(BUSY_TIMEOUT_MS)
        cur = conn.cursor()
        # APSW requires consuming cursor results for PRAGMAs to take effect
        list(cur.execute("PRAGMA journal_mode=WAL"))
        list(cur.execute("PRAGMA synchronous=NORMAL"))
        list(cur.execute(f"PRAGMA cache_size=-{int(CACHE_KIB)}"))
        list(cur.execute("PRAGMA temp_store=MEMORY"))
        list(cur.execute(f"PRAGMA mmap_size={int(MMAP_BYTES)}"))
        list(cur.execute("PRAGMA foreign_keys=ON"))
        list(cur.execute(f"PRAGMA wal_autocheckpoint={int(WAL_AUTOCHECKPOINT)}"))
        conn.pragma('optimize')
        return conn

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread.start()

    def stop(self):
        if not self.running:
            return
        self.running = False

        self.stop_evt.set()
        self._put_job(_Job(kind="STOP", priority=-10))

        try:
            if self.conn is not None:
                self.conn.interrupt()
        except Exception:
            pass

        self.thread.join()

    def _put_job(self, job: _Job):
        self._enqueue_counter += 1
        self.q.put((job.priority, self._enqueue_counter, job))

    def next_txid(self) -> int:
        self._tx_counter += 1
        return self._tx_counter

    def _set_result(self, job: _Job, val: Any):
        if job.loop and job.fut and not job.fut.done():
            job.loop.call_soon_threadsafe(job.fut.set_result, val)

    def _set_exception(self, job: _Job, exc: BaseException):
        if job.loop and job.fut and not job.fut.done():
            job.loop.call_soon_threadsafe(job.fut.set_exception, exc)

    def _drain_for_grouped_commit(self, first_job: _Job) -> List[_Job]:
        if self.grouped_commit_wait_ms <= 0:
            return [first_job]
        batch = [first_job]
        t0 = time.monotonic()
        while (time.monotonic() - t0) * 1000.0 < self.grouped_commit_wait_ms:
            try:
                pri, _, peek = self.q.get_nowait()
            except __import__("queue").Empty:
                break
            if peek.kind not in ("SQL_AUTO", "SQLM_AUTO"):
                self.q.put((pri, self._enqueue_counter, peek))
                self._enqueue_counter += 1
                break
            batch.append(peek)
            time.sleep(0.0005)
        return batch

    def _run(self):

        try:
            self.conn = self._setup_conn()
            cur = self.conn.cursor()
            while not self.stop_evt.is_set():
                try:
                    _, _, job = self.q.get()
                except Exception:
                    continue
                if job.kind == "STOP":
                    break
                try:
                    if job.kind in ("SQL_AUTO", "SQLM_AUTO") and self.current_txid is None:
                        batch = self._drain_for_grouped_commit(job)
                        cur.execute("BEGIN IMMEDIATE")
                        try:
                            for j in batch:
                                if j.kind == "SQL_AUTO":
                                    cur.execute(j.sql or "", tuple(j.params or ()))
                                else:
                                    cur.executemany(j.sql or "", list(j.seq_params or []))
                            cur.execute("COMMIT")
                            for j in batch:
                                self._set_result(j, True)
                        except BaseException as e:
                            cur.execute("ROLLBACK")
                            for j in batch:
                                self._set_exception(j, e)
                        continue

                    if job.kind == "SQL_AUTO":
                        cur.execute("BEGIN IMMEDIATE")
                        try:
                            cur.execute(job.sql or "", tuple(job.params or ()))
                            cur.execute("COMMIT")
                            self._set_result(job, True)
                        except BaseException as e:
                            cur.execute("ROLLBACK")
                            self._set_exception(job, e)

                    elif job.kind == "SQLM_AUTO":
                        cur.execute("BEGIN IMMEDIATE")
                        try:
                            cur.executemany(job.sql or "", list(job.seq_params or []))
                            cur.execute("COMMIT")
                            self._set_result(job, True)
                        except BaseException as e:
                            cur.execute("ROLLBACK")
                            self._set_exception(job, e)

                    elif job.kind == "TX_BEGIN":
                        if self.current_txid is not None:
                            raise RuntimeError("Writer already in transaction")
                        cur.execute("BEGIN IMMEDIATE")
                        self.current_txid = job.txid
                        self._set_result(job, job.txid)

                    elif job.kind == "TX_EXECUTE":
                        if self.current_txid != job.txid:
                            raise RuntimeError("TX_EXECUTE for non-current tx")

                        cols: List[str] = []
                        def exectrace(cursor, sql, bindings):
                            desc = cursor.getdescription()
                            if desc: cols[:] = [d[0] for d in desc]
                            return True

                        cur.setexectrace(exectrace)
                        try:
                            cur.execute(job.sql or "", tuple(job.params or ()))
                            # desc = cur.getdescription() or []
                            rows = list(cur) if cols else []
                            # cols = [d[0] for d in desc] if desc else []
                            self._set_result(job, (rows, cols))
                        finally:
                            cur.setexectrace(None)

                    elif job.kind == "TX_EXECUTEMANY":
                        if self.current_txid != job.txid:
                            raise RuntimeError("TX_EXECUTEMANY for non-current tx")
                        cur.executemany(job.sql or "", list(job.seq_params or []))
                        self._set_result(job, True)

                    elif job.kind == "TX_COMMIT":
                        if self.current_txid != job.txid:
                            raise RuntimeError("TX_COMMIT for non-current tx")
                        cur.execute("COMMIT")
                        self.current_txid = None
                        self._set_result(job, True)

                    elif job.kind == "TX_ROLLBACK":
                        if self.current_txid != job.txid:
                            raise RuntimeError("TX_ROLLBACK for non-current tx")
                        cur.execute("ROLLBACK")
                        self.current_txid = None
                        self._set_result(job, True)

                    else:
                        raise ValueError(f"Unknown job kind: {job.kind}")
                except BaseException as e:
                    self._set_exception(job, e)
        finally:
            try:
                if self.conn is not None:
                    self.conn.close()
            except Exception:
                pass

    async def submit(self, job: _Job):
        loop = asyncio.get_running_loop()
        job.loop = loop
        job.fut = loop.create_future()
        self._put_job(job)
        return await job.fut


# ---------- SQLite reader pool with timeouts ----------

class _ReaderPool:
    def __init__(self, db_path: str, workers: int):
        self.db_path = str(db_path)
        self.exec = ThreadPoolExecutor(max_workers=max(1, workers), thread_name_prefix="SQL-Reader")
        self.tlocal = threading.local()
        self._conns = weakref.WeakSet()

    def _get_conn(self) -> apsw.Connection:
        conn = getattr(self.tlocal, "conn", None)
        if conn is not None:
            return conn
        conn = apsw.Connection(self.db_path)
        conn.setbusytimeout(BUSY_TIMEOUT_MS)
        cur = conn.cursor()
        # APSW requires consuming cursor results for PRAGMAs to take effect
        list(cur.execute("PRAGMA journal_mode=WAL"))
        list(cur.execute("PRAGMA synchronous=NORMAL"))
        list(cur.execute(f"PRAGMA cache_size=-{int(CACHE_KIB)}"))
        list(cur.execute("PRAGMA temp_store=MEMORY"))
        list(cur.execute(f"PRAGMA mmap_size={int(MMAP_BYTES)}"))
        list(cur.execute("PRAGMA foreign_keys=ON"))
        list(cur.execute(f"PRAGMA wal_autocheckpoint={int(WAL_AUTOCHECKPOINT)}"))
        list(cur.execute("PRAGMA query_only=ON"))
        setattr(self.tlocal, "conn", conn)
        self._conns.add(conn)
        return conn

    def _exec_read_with_progress(
            self,
            sql: str,
            params: Optional[Tuple[Any, ...]],
            deadline_ms: Optional[int],
            cancel_event: Optional[threading.Event],
    ) -> Tuple[List[Tuple[Any, ...]], List[str]]:
        conn = self._get_conn()
        start = time.monotonic()

        def progress() -> bool:
            if cancel_event and cancel_event.is_set():
                return True
            if deadline_ms is not None and (time.monotonic() - start) * 1000.0 > deadline_ms:
                return True
            return False

        cols: List[str] = []
        def exectrace(cursor, sql, bindings):
            desc = cursor.getdescription()
            if desc:
                cols[:] = [d[0] for d in desc]
            return True

        conn.setprogresshandler(progress, 1000)
        try:
            cur = conn.cursor()
            cur.setexectrace(exectrace)
            try:
                cur.execute(sql, tuple(params or ()))
            finally:
                cur.setexectrace(None)
            rows = list(cur)
            return rows, cols
        except Exception as e:
            log.error(f"SQL ERROR: {e}", query=sql)
            raise e
        finally:
            conn.setprogresshandler(None, 0)

    async def exec_read(
            self,
            sql: str,
            params: Optional[Tuple[Any, ...]],
            deadline_ms: Optional[int],
            cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[Tuple[Any, ...]], List[str]]:
        loop = asyncio.get_running_loop()
        t_event:threading.Event = None
        mirror_task = None

        if cancel_event is not None:
            t_event = threading.Event()

            async def mirror():
                try:
                    await cancel_event.wait()
                    t_event.set()
                except asyncio.CancelledError:
                    # expected on cleanup
                    pass

            mirror_task = asyncio.create_task(mirror())
        try:
            return await loop.run_in_executor(
                self.exec,
                self._exec_read_with_progress,
                sql,
                params,
                deadline_ms,
                t_event,
            )
        finally:
            if mirror_task is not None:
                mirror_task.cancel()
                try:
                    await mirror_task
                except asyncio.CancelledError:
                    pass

    def shutdown(self):
        conns = list(self._conns)
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass
        self._conns.clear()
        try:
            self.exec.shutdown(wait=True, cancel_futures=False)
        except TypeError:
            self.exec.shutdown(wait=True)


# ---------- SQLite transaction façade ----------

class _Tx:
    def __init__(self, actor: _WriterActor):
        self.actor = actor
        self.txid: Optional[int] = None
        self._closed = False

    async def __aenter__(self):
        self.txid = self.actor.next_txid()
        await self.actor.submit(_Job(kind="TX_BEGIN", txid=self.txid))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._closed:
            return
        if exc:
            await self.rollback()
        else:
            await self.commit()

    async def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        if self.txid is None:
            raise RuntimeError("Transaction not started")
        rows, cols = await self.actor.submit(
            _Job(kind="TX_EXECUTE", txid=self.txid, sql=sql, params=list(params or ()))
        )
        return rows, cols

    async def executemany(self, sql: str, seq_params: Iterable[Sequence[Any]]):
        if self.txid is None:
            raise RuntimeError("Transaction not started")
        await self.actor.submit(
            _Job(kind="TX_EXECUTEMANY", txid=self.txid, sql=sql, seq_params=[tuple(p) for p in seq_params])
        )

    async def commit(self):
        if self.txid is not None and not self._closed:
            await self.actor.submit(_Job(kind="TX_COMMIT", txid=self.txid))
            self._closed = True

    async def rollback(self):
        if self.txid is not None and not self._closed:
            await self.actor.submit(_Job(kind="TX_ROLLBACK", txid=self.txid))
            self._closed = True


# ---------- Async SQLite engine (ConnectorX reads + APSW writes) ----------

class AsyncSQLiteEngine:
    """
    High-performance async SQLite engine.
    Single writer (APSW) + concurrent readers (ConnectorX -> Arrow/Polars).

    Architecture:
    - WAL journal mode for concurrent reads/writes.
    - Writes serialized through _WriterActor (asyncio-bridged single thread)
      which supports grouped commits and explicit transactions.
    - Reads dispatched to a ThreadPoolExecutor via ConnectorX (zero-copy Arrow).
    - APSW fallback for parameterized queries or when ConnectorX is unavailable.
    """

    def __init__(self, db_path: str, read_workers: int | None = None, create_db: bool = True):
        p = Path(db_path).resolve()
        if create_db:
            p.parent.mkdir(parents=True, exist_ok=True)
            if not p.exists():
                p.touch()
        self.db_path = str(p)
        self._cx_uri = f"sqlite://{self.db_path}"

        # Write path: WriterActor provides serialised writes + transactions
        self._writer = _WriterActor(self.db_path)
        # Additional lock for write_and_read atomicity
        self._write_lock = asyncio.Lock()

        # Read path
        n = read_workers or READ_WORKERS
        self._read_executor = ThreadPoolExecutor(
            max_workers=n, thread_name_prefix="CX-Reader",
        )
        # APSW reader pool (fallback for parameterised queries + CX errors)
        self._apsw_readers = _ReaderPool(self.db_path, n)

        self._initialized = False

    # ------ lifecycle ------

    async def initialize(self) -> None:
        """Set WAL mode, APSW pragmas, warm ConnectorX."""
        if self._initialized:
            return
        self._writer.start()
        # Wait for the writer to be fully ready (connection + pragmas set)
        # before allowing other connections to open the DB file.
        await self._writer.submit(
            _Job(kind="SQL_AUTO", sql="SELECT 1", params=[]),
        )
        # NOTE: ConnectorX warmup is intentionally skipped.
        # ConnectorX's bundled SQLite may reset WAL mode when opening a
        # connection, which deletes the WAL/SHM files.  The first real CX
        # read will pay the one-time connection cost instead.
        self._initialized = True

    async def close(self) -> None:
        """Shut down thread pool, close APSW connections, run WAL checkpoint."""
        if not self._initialized:
            return
        # Checkpoint WAL before shutdown
        try:
            async with self.transaction() as tx:
                await tx.execute("PRAGMA wal_checkpoint(TRUNCATE)", ())
        except Exception:
            pass
        self._apsw_readers.shutdown()
        self._writer.stop()
        self._read_executor.shutdown(wait=False)
        self._initialized = False

    # ------ ConnectorX helpers ------

    def _cx_read_raw(self, query: str) -> pa.Table:
        """
        Synchronous ConnectorX read — runs inside the thread pool.

        WARNING: ConnectorX's bundled SQLite may reset WAL journal mode,
        deleting WAL/SHM files.  Only use this after a WAL checkpoint or
        when the database is quiescent.  In general, prefer the APSW
        reader pool for WAL-safe concurrent reads.
        """
        return cx.read_sql(self._cx_uri, query, return_type="arrow")

    # ------ APSW fallback helpers ------

    async def _apsw_read_as_arrow(
            self,
            query: str,
            params: Sequence[Any] | None,
            timeout_ms: int | None = None,
    ) -> pa.Table:
        """
        Execute a read via the APSW reader pool and return a PyArrow Table.
        Used as fallback when ConnectorX cannot handle a query
        (e.g. parameter binding).
        """
        _fallback_logger.debug("APSW fallback read: %s", query[:120])
        deadline = timeout_ms if timeout_ms and timeout_ms > 0 else None
        rows, cols = await self._apsw_readers.exec_read(
            query, tuple(params or ()), deadline,
        )
        if not cols:
            return pa.table({})
        # Build columnar arrays
        arrays: dict[str, list[Any]] = {c: [] for c in cols}
        for row in rows:
            for i, c in enumerate(cols):
                arrays[c].append(row[i])
        return pa.table(arrays)

    # ------ Public read methods ------

    async def read_arrow(
            self,
            query: str,
            params: dict | Sequence[Any] | None = None,
            timeout_ms: int | None = None,
    ) -> pa.Table:
        """
        Execute a read query and return a PyArrow Table.

        Uses the APSW reader pool by default to preserve WAL-mode
        compatibility.  ConnectorX's bundled SQLite can reset WAL mode,
        so it is NOT used here.  For bulk exports on a quiescent database
        (after checkpoint), use ``read_arrow_cx`` instead.
        """
        if params is not None:
            p = tuple(params.values()) if isinstance(params, dict) else tuple(params)
            return await self._apsw_read_as_arrow(query, p, timeout_ms)
        return await self._apsw_read_as_arrow(query, None, timeout_ms)

    async def read_arrow_cx(
            self,
            query: str,
            timeout_ms: int | None = None,
    ) -> pa.Table:
        """
        Execute a parameter-free read via ConnectorX (zero-copy Arrow).

        WARNING: ConnectorX may reset SQLite WAL mode. Only call this
        after ``PRAGMA wal_checkpoint(TRUNCATE)`` or on a read-only /
        quiescent database.  For general use, prefer ``read_arrow``.
        """
        if not _HAS_CONNECTORX:
            return await self._apsw_read_as_arrow(query, None, timeout_ms)
        loop = asyncio.get_running_loop()
        try:
            if timeout_ms and timeout_ms > 0:
                return await asyncio.wait_for(
                    loop.run_in_executor(self._read_executor, self._cx_read_raw, query),
                    timeout=timeout_ms / 1000.0,
                )
            return await loop.run_in_executor(
                self._read_executor, self._cx_read_raw, query,
            )
        except Exception as exc:
            _fallback_logger.warning(
                "ConnectorX read failed (%s), falling back to APSW: %s",
                exc, query[:120],
            )
            return await self._apsw_read_as_arrow(query, None, timeout_ms)

    async def read_polars(
            self,
            query: str,
            params: dict | Sequence[Any] | None = None,
            timeout_ms: int | None = None,
    ) -> pl.DataFrame:
        """
        Execute a read query and return a Polars DataFrame.
        Built on top of read_arrow (pl.from_arrow — zero copy).
        """
        table = await self.read_arrow(query, params, timeout_ms)
        return pl.from_arrow(table)

    async def read_dicts(
            self,
            query: str,
            params: dict | Sequence[Any] | None = None,
            timeout_ms: int | None = None,
    ) -> list[dict]:
        """
        BACKWARD COMPATIBILITY SHIM.
        Returns list of dicts like the old APSW pattern.

        .. deprecated::
            Use ``read_arrow`` or ``read_polars`` for new code.
        """
        table = await self.read_arrow(query, params, timeout_ms)
        cols = table.column_names
        if not cols:
            return []
        pydict = table.to_pydict()
        n = table.num_rows
        return [{c: pydict[c][i] for c in cols} for i in range(n)]

    async def read_tuples(
            self,
            query: str,
            params: Sequence[Any] | None,
            timeout_ms: int | None = None,
            cancel_event: asyncio.Event | None = None,
    ) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """
        Backward-compat shim returning (columns, rows) like the old APSW
        reader pool.  Used internally by SQLiteBackend.fetch_all.
        """
        # For parameterised queries or when cancel_event is set, use APSW
        # reader pool directly (it supports progress-handler cancellation).
        if params or cancel_event is not None:
            deadline = timeout_ms if timeout_ms and timeout_ms > 0 else None
            rows, cols = await self._apsw_readers.exec_read(
                query, tuple(params or ()), deadline, cancel_event,
            )
            return cols, rows

        # Fast path: ConnectorX -> Arrow -> tuples
        try:
            table = await self.read_arrow(query, None, timeout_ms)
        except Exception:
            # Ultimate fallback
            deadline = timeout_ms if timeout_ms and timeout_ms > 0 else None
            rows, cols = await self._apsw_readers.exec_read(
                query, (), deadline, cancel_event,
            )
            return cols, rows

        cols = table.column_names
        if not cols:
            return [], []
        pydict = table.to_pydict()
        n = table.num_rows
        rows = [tuple(pydict[c][i] for c in cols) for i in range(n)]
        return cols, rows

    # ------ Public write methods ------

    async def execute(
            self,
            sql: str,
            params: dict | tuple | Sequence[Any] | None = None,
    ) -> None:
        """
        Single write operation (INSERT, UPDATE, DELETE, DDL).
        Serialized through the WriterActor.
        """
        p = list(params or ())
        await self._writer.submit(_Job(kind="SQL_AUTO", sql=sql, params=p))

    async def execute_many(
            self,
            sql: str,
            params_seq: Sequence[tuple | Sequence[Any]],
    ) -> None:
        """Batch write using APSW executemany inside a transaction."""
        seq = [tuple(p) for p in params_seq]
        await self._writer.submit(_Job(kind="SQLM_AUTO", sql=sql, seq_params=seq))

    async def execute_script(self, sql: str) -> None:
        """
        Execute raw multi-statement SQL (migrations, schema setup).
        Each statement is submitted individually through the writer.
        """
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                await self.execute(stmt)

    async def write_and_read(
            self,
            write_sql: str,
            write_params: dict | tuple | Sequence[Any] | None,
            read_sql: str,
            read_params: dict | Sequence[Any] | None = None,
    ) -> pa.Table:
        """
        Atomic write-then-read.  Acquires write lock, performs write via APSW,
        then reads via ConnectorX within the same lock scope (WAL guarantees
        the read sees the write).
        """
        async with self._write_lock:
            await self.execute(write_sql, write_params)
            return await self.read_arrow(read_sql, read_params)

    # ------ Serialization ------

    @staticmethod
    def serialize_arrow_ipc(
            table: pa.Table,
            compression: str = "lz4_frame",
    ) -> bytes:
        """
        Serialize a PyArrow Table to Arrow IPC streaming format with
        LZ4 compression.  Synchronous — called after await read_arrow().
        """
        sink = pa.BufferOutputStream()
        opts = ipc.IpcWriteOptions(compression=compression)
        with ipc.new_stream(sink, table.schema, options=opts) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()

    # ------ Transaction support ------

    def transaction(self):
        """Return a _Tx context manager backed by this engine's WriterActor."""
        return _TxContextHelper(self._writer)


class _TxContextHelper:
    """Thin wrapper to make ``async with engine.transaction()`` work."""

    def __init__(self, writer: _WriterActor):
        self._writer = writer
        self._tx: _Tx | None = None

    async def __aenter__(self) -> _Tx:
        self._tx = _Tx(self._writer)
        await self._tx.__aenter__()
        return self._tx

    async def __aexit__(self, exc_type, exc, tb):
        if self._tx is not None:
            await self._tx.__aexit__(exc_type, exc, tb)


# ---------- SQLite backend wrapper ----------

class SQLiteBackend:
    """
    SQLite backend wrapper — now delegates to AsyncSQLiteEngine for
    ConnectorX-accelerated reads while preserving the original interface.
    """

    def __init__(self, db_path: str, create_db: bool):
        self.db_path = Path(db_path)
        if create_db:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.db_path.exists():
                self.db_path.touch()
        elif not self.db_path.exists():
            raise FileNotFoundError(str(self.db_path))
        self.engine = AsyncSQLiteEngine(str(self.db_path), create_db=False)
        # Expose writer directly for _Tx / transaction() usage
        self.writer = self.engine._writer
        self.connected = False

    async def connect(self):
        if self.connected:
            return
        await self.engine.initialize()
        self.connected = True

    async def disconnect(self):
        if not self.connected:
            return
        await self.engine.close()
        self.connected = False

    # ------ write path (delegates to engine) ------

    async def execute_write(self, sql: str, params: Optional[Sequence[Any]] = None):
        if not self.connected:
            raise CadeSQLConnectionError("SQLite backend not connected")
        await self.engine.execute(sql, params)

    async def executemany_write(self, sql: str, seq_params: List[Tuple[Any, ...]]):
        if not self.connected:
            raise CadeSQLConnectionError("SQLite backend not connected")
        await self.engine.execute_many(sql, seq_params)

    # ------ read path (ConnectorX primary, APSW fallback) ------

    async def fetch_all(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
            cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        if not self.connected:
            raise CadeSQLConnectionError("SQLite backend not connected")
        return await self.engine.read_tuples(sql, params, timeout_ms, cancel_event)

    async def fetch_one(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
            cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[str], Optional[Tuple[Any, ...]]]:
        cols, rows = await self.fetch_all(sql, params, timeout_ms, cancel_event)
        return cols, rows[0] if rows else None

    # ------ new Arrow / Polars accessors ------

    async def read_arrow(
            self, sql: str, params: Optional[Sequence[Any]] = None,
            timeout_ms: Optional[int] = None,
    ) -> pa.Table:
        if not self.connected:
            raise CadeSQLConnectionError("SQLite backend not connected")
        return await self.engine.read_arrow(sql, params, timeout_ms)

    async def read_polars(
            self, sql: str, params: Optional[Sequence[Any]] = None,
            timeout_ms: Optional[int] = None,
    ) -> pl.DataFrame:
        if not self.connected:
            raise CadeSQLConnectionError("SQLite backend not connected")
        return await self.engine.read_polars(sql, params, timeout_ms)

    async def read_dicts(
            self, sql: str, params: Optional[Sequence[Any]] = None,
            timeout_ms: Optional[int] = None,
    ) -> list[dict]:
        """Backward compatibility shim. Deprecated — use read_arrow."""
        if not self.connected:
            raise CadeSQLConnectionError("SQLite backend not connected")
        return await self.engine.read_dicts(sql, params, timeout_ms)

    # ------ metadata helpers (use APSW fallback — PRAGMAs unsupported by CX) ------

    async def table_exists(self, table: str) -> bool:
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        _, row = await self.fetch_one(sql, [table], timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
        return row is not None

    async def list_tables(self):
        sql = "SELECT name FROM sqlite_master WHERE type='table'"
        _, row = await self.fetch_all(sql, None, timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
        return [item for sublist in row for item in sublist]

    async def get_table_schema(self, table: str) -> TableSchema:
        sql = f"PRAGMA table_info(`{table}`)"
        cols, rows = await self.fetch_all(sql, None, timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
        columns: List[str] = []
        column_types: Dict[str, str] = {}
        pks: List[str] = []
        for r in rows:
            name = r[1]
            col_type = r[2] or ""
            pk_flag = r[5]
            columns.append(name)
            column_types[name] = col_type
            if pk_flag:
                pks.append(name)
        return TableSchema(table=table, columns=columns, column_types=column_types, primary_keys=pks)

    async def get_indices(self, table: str) -> List[Tuple[str, str]]:
        sql = f"PRAGMA index_list(`{table}`)"
        _, rows = await self.fetch_all(sql, None, timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
        result: List[Tuple[str, str]] = []
        for row in rows:
            index_name = row[1]
            info_sql = f"PRAGMA index_info(`{index_name}`)"
            _, info_rows = await self.fetch_all(info_sql, None, timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
            for info in info_rows:
                result.append((index_name, info[2]))
        return result

    async def add_index(self, table: str, column: str):
        name = f"auto_idx_{table}_{column}"
        sql = f"CREATE INDEX IF NOT EXISTS `{name}` ON `{table}`(`{column}`)"
        await self.execute_write(sql)

    async def drop_index(self, index_name: str):
        sql = f"DROP INDEX IF EXISTS `{index_name}`"
        await self.execute_write(sql)


# ---------- MySQL backend wrapper ----------

class MySQLBackend:
    def __init__(
            self,
            host: str,
            port: int,
            user: str,
            password: str,
            database: str,
            max_connections: int,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.max_connections = max_connections
        self.pool = None

    async def connect(self):
        import asyncmy  # type: ignore

        if self.pool is not None:
            return
        self.pool = await asyncmy.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            minsize=1,
            maxsize=self.max_connections,
            autocommit=True,
            charset="utf8mb4",
        )

    async def disconnect(self):
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def execute_write(self, sql: str, params: Optional[Sequence[Any]] = None, timeout_ms: Optional[int] = None):
        if self.pool is None:
            raise CadeSQLConnectionError("MySQL backend not connected")
        timeout = timeout_ms / 1000.0 if timeout_ms and timeout_ms > 0 else None
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                coro = cur.execute(sql, params or [])
                if timeout is not None:
                    await asyncio.wait_for(coro, timeout=timeout)
                else:
                    await coro

    async def executemany_write(self, sql: str, seq_params: List[Tuple[Any, ...]], timeout_ms: Optional[int] = None):
        if self.pool is None:
            raise CadeSQLConnectionError("MySQL backend not connected")
        timeout = timeout_ms / 1000.0 if timeout_ms and timeout_ms > 0 else None
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                coro = cur.executemany(sql, seq_params)
                if timeout is not None:
                    await asyncio.wait_for(coro, timeout=timeout)
                else:
                    await coro

    async def fetch_all(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
            cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        if self.pool is None:
            raise CadeSQLConnectionError("MySQL backend not connected")
        timeout = timeout_ms / 1000.0 if timeout_ms and timeout_ms > 0 else None

        async def _run():
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params or [])
                    rows = await cur.fetchall()
                    desc = cur.description or []
                    cols = [d[0] for d in desc]
                    return cols, rows

        if timeout is None and cancel_event is None:
            return await _run()

        task = asyncio.create_task(_run())
        if timeout is not None:
            try:
                return await asyncio.wait_for(task, timeout)
            except asyncio.TimeoutError:
                task.cancel()
                raise CadeSQLQueryTimeout(f"MySQL query timed out after {timeout_ms} ms")
        if cancel_event is not None:
            done, pending = await asyncio.wait({task, asyncio.create_task(cancel_event.wait())}, return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                return await task
            for p in pending:
                p.cancel()
            task.cancel()
            raise CadeSQLQueryTimeout("MySQL query cancelled")
        return await task

    async def fetch_one(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
            cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[str], Optional[Tuple[Any, ...]]]:
        cols, rows = await self.fetch_all(sql, params, timeout_ms, cancel_event)
        return cols, rows[0] if rows else None

    async def table_exists(self, table: str) -> bool:
        sql = "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s LIMIT 1"
        _, row = await self.fetch_one(sql, [self.database, table], timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
        return row is not None

    async def get_table_schema(self, table: str) -> TableSchema:
        sql = """
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        ORDER BY ORDINAL_POSITION
        """
        cols, rows = await self.fetch_all(sql, [self.database, table], timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
        columns: List[str] = []
        column_types: Dict[str, str] = {}
        pks: List[str] = []
        for row in rows:
            name = row[0]
            col_type = row[1] or ""
            key = row[2] or ""
            columns.append(name)
            column_types[name] = col_type
            if "PRI" in key:
                pks.append(name)
        return TableSchema(table=table, columns=columns, column_types=column_types, primary_keys=pks)

    async def get_indices(self, table: str) -> List[Tuple[str, str]]:
        sql = f"SHOW INDEX FROM `{table}`"
        cols, rows = await self.fetch_all(sql, None, timeout_ms=DEFAULT_QUERY_TIMEOUT_MS)
        result: List[Tuple[str, str]] = []
        for row in rows:
            result.append((row[2], row[4]))  # (index_name, column_name)
        return result

    async def add_index(self, table: str, column: str):
        name = f"auto_idx_{table}_{column}"
        sql = f"CREATE INDEX `{name}` ON `{table}`(`{column}`)"
        await self.execute_write(sql)

    async def drop_index(self, index_name: str, table: str):
        sql = f"DROP INDEX `{index_name}` ON `{table}`"
        await self.execute_write(sql)


# ---------- Main CadeSQL wrapper ----------

class CadeSQL:
    def __init__(
            self,
            driver: str,
            database: str,
            host: Optional[str] = None,
            port: Optional[int] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            create_db: bool = True,
            auto_connect: bool = True,
            default_query_timeout_ms: int = DEFAULT_QUERY_TIMEOUT_MS,
            enable_cache: bool = False,
            cache_on_miss: bool = False,
            cache_ttl_ms: int = QUERY_CACHE_TTL_MS,
            enable_clear_sentinel: bool = False,
            auto_index: bool = False,
            auto_index_add: bool = True,
            auto_index_remove: bool = True,
            auto_index_check_interval: float = AUTO_INDEX_CHECK_INTERVAL_SEC,
            enable_partitions: Union[bool, Sequence[str]] = False,
            strict_filters: bool = False,
            default_format: Optional[str] = None
    ):
        self.driver_type = driver.lower()
        if self.driver_type not in ("sqlite", "mysql"):
            raise CadeSQLError("driver must be 'sqlite' or 'mysql'")

        self.database = database
        self.host = host or "localhost"
        self.port = port or (3306 if self.driver_type == "mysql" else 0)
        self.user = user or ""
        self.password = password or ""

        self.create_db = create_db
        self.auto_connect = auto_connect
        self.default_query_timeout_ms = default_query_timeout_ms
        self.enable_cache = enable_cache
        self.cache_on_miss = cache_on_miss
        self.cache_ttl_ms = cache_ttl_ms
        self.enable_clear_sentinel = enable_clear_sentinel

        # Auto Index Controls
        self.auto_index = auto_index
        self.auto_index_add = auto_index_add
        self.auto_index_remove = auto_index_remove
        self.auto_index_check_interval = auto_index_check_interval
        self._auto_indexer: Optional[AutoIndexer] = None

        # Partition controls
        self.enable_partitions = enable_partitions

        # Filters: if False, unknown columns in filters are ignored for SELECTs.
        self.strict_filters = strict_filters

        self.default_format = default_format

        self._sqlite_backend: Optional[SQLiteBackend] = None
        self._mysql_backend: Optional[MySQLBackend] = None
        self._connected = False

        self._schema_cache = AsyncLRUCache(SCHEMA_CACHE_SIZE)
        self._query_cache = AsyncTTLCache(QUERY_CACHE_SIZE, cache_ttl_ms) if enable_cache else None
        atexit.register(self._atexit)

    def _atexit(self):
        try:
            loop = asyncio.get_event_loop()
            loop = set_uvloop(loop=loop)
        except RuntimeError:
            loop = set_uvloop()
        try:
            loop.run_until_complete(self.disconnect())
        except Exception:
            asyncio.run(self.disconnect())

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    async def connect(self):
        if self._connected: return
        if self.driver_type == "sqlite":
            self._sqlite_backend = SQLiteBackend(self.database, create_db=self.create_db)
            await self._sqlite_backend.connect()
        else:
            self._mysql_backend = MySQLBackend(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                max_connections=READ_WORKERS * 2,
            )
            await self._mysql_backend.connect()
        self._connected = True

        # Ensure meta tables for partitioning exist on both backends
        await self._ensure_meta_tables()

        # Initialize and start auto-indexer if enabled
        if self.auto_index and self._auto_indexer is None:
            self._auto_indexer = AutoIndexer(
                index_threshold=AUTO_INDEX_HIT_THRESHOLD,
                remove_threshold=AUTO_INDEX_REMOVE_THRESHOLD,
                check_interval=self.auto_index_check_interval,
                auto_add=self.auto_index_add,
                auto_remove=self.auto_index_remove,
            )
        if self.auto_index and self._auto_indexer is not None:
            self._auto_indexer.attach(self)
            await self._auto_indexer.start()

        await log.success("CadeSQL connected", driver=self.driver_type, database=self.database)

    async def disconnect(self, truncate_wal=True):
        if not self._connected:
            return

        # Stop auto-indexer first so it doesn't race with shutdown
        if self._auto_indexer is not None:
            await self._auto_indexer.stop()

        if self._sqlite_backend:
            await log.notify("Disconnecting backend")
            # WAL checkpoint is handled inside AsyncSQLiteEngine.close()
            await self._sqlite_backend.disconnect()

        if self._mysql_backend:
            await self._mysql_backend.disconnect()

        self._sqlite_backend = None
        self._mysql_backend = None
        self._connected = False
        await self._schema_cache.clear()
        if self._query_cache:
            await self._query_cache.clear()
        await log.success("CadeSQL disconnected", driver=self.driver_type, database=self.database)

    async def is_connected(self):
        return self._connected

    async def _ensure_connected(self):
        if not self._connected:
            if self.auto_connect:
                await self.connect()
            else:
                raise CadeSQLConnectionError("CadeSQL is not connected")

    def _param_style(self) -> str:
        return "qmark" if self.driver_type == "sqlite" else "format"

    async def _run_with_timeout(self, coro, timeout_ms: Optional[int]):
        if timeout_ms is None or timeout_ms <= 0:
            return await coro
        try:
            return await asyncio.wait_for(coro, timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            await log.warning("Query timeout", timeout_ms=timeout_ms)
            raise CadeSQLQueryTimeout(f"Query timed out after {timeout_ms} ms")

    async def _ensure_meta_tables(self):
        """
        Create partitioning metadata tables on both SQLite and MySQL.

        _partitions: one row per (logical_table, column) -> physical partition table.
        _admin: one row per logical_table with partitioned flag + primary key list (CSV).
        """
        await self._ensure_connected()

        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            sql_partitions = """
            CREATE TABLE IF NOT EXISTS "_partitions" (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                partition_table TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name)
            )
            """
            await backend.execute_write(sql_partitions)
            sql_admin = """
            CREATE TABLE IF NOT EXISTS "_admin" (
                table_name TEXT PRIMARY KEY,
                partitioned INTEGER NOT NULL,
                primary_key TEXT
            )
            """
            await backend.execute_write(sql_admin)
        else:
            backend = self._mysql_backend
            assert backend is not None
            sql_partitions = """
            CREATE TABLE IF NOT EXISTS `_partitions` (
                table_name VARCHAR(255) NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                partition_table VARCHAR(255) NOT NULL,
                PRIMARY KEY (table_name, column_name)
            ) ENGINE=InnoDB
            """
            await backend.execute_write(sql_partitions)
            sql_admin = """
            CREATE TABLE IF NOT EXISTS `_admin` (
                table_name VARCHAR(255) PRIMARY KEY,
                partitioned TINYINT(1) NOT NULL,
                primary_key TEXT NULL
            ) ENGINE=InnoDB
            """
            await backend.execute_write(sql_admin)

    async def _backend_fetch_all(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
            cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            return await backend.fetch_all(sql, params, timeout_ms, cancel_event)
        backend = self._mysql_backend
        assert backend is not None
        return await backend.fetch_all(sql, params, timeout_ms, cancel_event)

    async def _backend_fetch_one(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
            cancel_event: Optional[asyncio.Event] = None,
    ) -> Tuple[List[str], Optional[Tuple[Any, ...]]]:
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            return await backend.fetch_one(sql, params, timeout_ms, cancel_event)
        backend = self._mysql_backend
        assert backend is not None
        return await backend.fetch_one(sql, params, timeout_ms, cancel_event)

    async def _backend_read_arrow(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
    ) -> pa.Table:
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            return await backend.read_arrow(sql, params, timeout_ms)

        # MySQL backend has no native Arrow path yet; preserve behavior
        # by materializing rows and converting to Arrow table.
        out_cols, rows = await self._backend_fetch_all(sql, params, timeout_ms)
        if not out_cols:
            return pa.table({})
        arrays: Dict[str, List[Any]] = {c: [] for c in out_cols}
        for row in rows:
            for i, c in enumerate(out_cols):
                arrays[c].append(row[i])
        return pa.table(arrays)

    async def _backend_read_polars(
            self,
            sql: str,
            params: Optional[Sequence[Any]],
            timeout_ms: Optional[int],
    ) -> pl.DataFrame:
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            return await backend.read_polars(sql, params, timeout_ms)
        out_cols, rows = await self._backend_fetch_all(sql, params, timeout_ms)
        return DataFrameAdapter.from_records(rows, out_cols, as_format="polars")

    async def _backend_execute_write(
            self,
            sql: str,
            params: Optional[Sequence[Any]] = None,
            timeout_ms: Optional[int] = None,
    ):
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            await backend.execute_write(sql, params)
        else:
            backend = self._mysql_backend
            assert backend is not None
            await backend.execute_write(sql, params, timeout_ms=timeout_ms)

    async def _backend_executemany_write(
            self,
            sql: str,
            seq_params: List[Tuple[Any, ...]],
            timeout_ms: Optional[int] = None,
    ):
        if not seq_params:
            return
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            await backend.executemany_write(sql, seq_params)
        else:
            backend = self._mysql_backend
            assert backend is not None
            await backend.executemany_write(sql, seq_params, timeout_ms=timeout_ms)

    # ---------- Schema APIs ----------


    async def list_tables(self) -> List[str]:
        await self._ensure_connected()
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            return await backend.list_tables()
        raise NotImplementedError("Havent implement this for MYSQL")

    async def _table_exists(self, table_name: str) -> bool:
        await self._ensure_connected()
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            return await backend.table_exists(table_name)
        backend = self._mysql_backend
        assert backend is not None
        return await backend.table_exists(table_name)

    async def query_schema(self, table_name: str, refresh: bool = False) -> TableSchema:
        await self._ensure_connected()

        async def factory():
            if self.driver_type == "sqlite":
                backend = self._sqlite_backend
            else:
                backend = self._mysql_backend
            assert backend is not None

            # Read partition metadata
            if self.driver_type == "sqlite":
                admin_sql = 'SELECT partitioned, primary_key FROM "_admin" WHERE table_name=?'
                parts_sql = 'SELECT table_name, column_name, partition_table FROM "_partitions" WHERE table_name=?'
                params = [table_name]
            else:
                admin_sql = "SELECT partitioned, primary_key FROM `_admin` WHERE table_name=%s"
                parts_sql = "SELECT table_name, column_name, partition_table FROM `_partitions` WHERE table_name=%s"
                params = [table_name]

            _, row = await backend.fetch_one(admin_sql, params, timeout_ms=self.default_query_timeout_ms)
            is_part = False
            pk_meta = None
            if row:
                is_part = bool(row[0])
                pk_meta = row[1]

            if is_part:
                _, rows = await backend.fetch_all(parts_sql, params, timeout_ms=self.default_query_timeout_ms)
                partitions: List[PartitionInfo] = []
                for r in rows:
                    partitions.append(PartitionInfo(table=r[0], column=r[1], partition=r[2]))
                physical_tables = sorted({p.partition for p in partitions})
                if not physical_tables:
                    # Metadata present but no physical partitions; fall back to base table
                    return await backend.get_table_schema(table_name)

                merged_cols: List[str] = []
                merged_types: Dict[str, str] = {}
                pks: List[str] = []

                for pt in physical_tables:
                    ts = await backend.get_table_schema(pt)
                    for c in ts.columns:
                        if c not in merged_cols:
                            merged_cols.append(c)
                            merged_types[c] = ts.column_types.get(c, "")
                    for pk in ts.primary_keys:
                        if pk not in pks:
                            pks.append(pk)

                if not pks and pk_meta:
                    if isinstance(pk_meta, str):
                        pks = [c.strip() for c in pk_meta.split(",") if c and c.strip()]
                    else:
                        pks = [str(pk_meta)]

                return TableSchema(
                    table=table_name,
                    columns=merged_cols,
                    column_types=merged_types,
                    primary_keys=pks,
                    is_partitioned=True,
                    partitions=partitions,
                )

            # Not partitioned; read physical table schema
            return await backend.get_table_schema(table_name)

        key = (table_name, self.driver_type)
        if refresh:
            return await factory()
        return await self._schema_cache.get(key, factory)

    async def query_schema_types(self, table_name, refresh: bool = False):
        s = await self.query_schema(table_name, refresh)
        return s.column_types

    async def query_empty(
            self,
            table_name: str,
            columns: Optional[Sequence[str]] = None,
            as_format: Optional[str] = None,
    ):
        schema = await self.query_schema(table_name)
        cols = list(columns) if columns is not None else schema.columns
        types = {c: schema.column_types.get(c, "") for c in cols}
        df = DataFrameAdapter.empty_like(cols, types)
        if as_format is None or as_format == "polars":
            return df
        if as_format == "polars_lazy":
            return df.lazy()
        if as_format == "pandas":
            return df.to_pandas()
        if as_format == "arrow":
            return df.to_arrow()
        if as_format in ("item", "list", "set"):
            return None
        raise CadeSQLError(f"Unsupported output format for empty: {as_format}")

    async def search_schema(self, table_name: str, stub: str = None, limit: int = 5, threshold: float = 0.6):
        import difflib

        schema = await self.query_schema(table_name)
        stub_lower = stub.lower()
        scores: List[Tuple[str, float]] = []
        for col in schema.columns:
            col_lower = col.lower()
            if stub_lower in col_lower:
                score = 1.0
            else:
                score = difflib.SequenceMatcher(a=stub_lower, b=col_lower).ratio()
            if score >= threshold:
                scores.append((col, score))
        scores.sort(key=lambda x: x[1], reverse=True)
        return [c for c, _ in scores[:limit]]

    async def schema_has(self, table_name: str, column: str):
        schema = await self.query_schema(table_name)
        return column in schema.columns

    async def schema_add(self, table_name: str, column: Union[List[str],str], sql_type: str = "TEXT", default: Any = None):
        await self._ensure_connected()
        schema = await self.query_schema(table_name)
        schema_types = schema.column_types
        columns = ensure_list(column)
        new_columns = {col for col in columns if not col in schema_types}
        if not new_columns: return

        _sql = []
        for col in new_columns:
            default_sql = ""
            params: List[Any] = []
            if default is not None:
                if self.driver_type == "sqlite":
                    default_sql = " DEFAULT ?"
                else:
                    default_sql = " DEFAULT %s"

                if isinstance(default, dict):
                    params.append(default.get(col, None))
                else:
                    params.append(default)
            _sql.append(
                (f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {sql_type}{default_sql}", params)
            )

        for sql, params in _sql:
            await self._backend_execute_write(sql, params)
        await self._schema_cache.clear()

    async def schema_remove(self, table_name: str, column: Union[List[str],str]):
        await self._ensure_connected()
        schema = await self.query_schema(table_name)
        schema_types = schema.column_types
        columns = ensure_list(column)
        existing_columns = {col for col in columns if col in schema_types}
        if not existing_columns: raise CadeSQLSchemaError("No existing columns match to remove")

        if self.driver_type == "mysql":
            for col in existing_columns:
                sql = f"ALTER TABLE `{table_name}` DROP COLUMN `{col}`"
                await self._backend_execute_write(sql)
        else:

            # SQLite: rebuild table
            backend = self._sqlite_backend
            assert backend is not None

            schema = await backend.get_table_schema(table_name)
            new_schema_sql = {
                c: schema.column_types[c]
                for c in schema.columns
                if c not in existing_columns
            }
            await self._rebuild_sqlite_table_with_schema(table_name, new_schema_sql)

        await self._schema_cache.clear()

    async def schema_alter(self, table_name: str, column: Union[List[str],str], new_sql_type: Union[List[str],str, Dict]):
        await self._ensure_connected()
        columns = ensure_list(column)

        if isinstance(new_sql_type, str):
            new_sql_type = {c:new_sql_type for c in columns}
        elif isinstance(new_sql_type, list):
            if len(columns) != len(new_sql_type): raise CadeSQLError("Columns and Type lengths do not match")
            new_sql_type = {c: dtype for c, dtype in zip(columns, new_sql_type)}

        if self.driver_type == "mysql":
            for col in columns:
                sql = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{col}` {new_sql_type.get(col, 'TEXT')}"
                await self._backend_execute_write(sql)
        else:
            backend = self._sqlite_backend
            assert backend is not None
            schema = await backend.get_table_schema(table_name)
            new_schema_sql = dict(schema.column_types)
            new_schema_sql = {**new_schema_sql, **new_sql_type}
            await self._rebuild_sqlite_table_with_schema(table_name, new_schema_sql)
        await self._schema_cache.clear()

    async def _rebuild_sqlite_table_with_schema(self, table_name: str, new_schema_sql: Dict[str, str]):
        backend = self._sqlite_backend
        assert backend is not None
        info_sql = f"PRAGMA table_info(`{table_name}`)"
        _, rows = await backend.fetch_all(info_sql, None, timeout_ms=self.default_query_timeout_ms)
        old_cols_order = [r[1] for r in sorted(rows, key=lambda r: r[0])]
        pk_cols = [r[1] for r in rows if r[5]]
        notnull = {r[1]: bool(r[3]) for r in rows}
        dfltval = {r[1]: r[4] for r in rows}
        create_sql_row, row = await backend.fetch_one(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            [table_name],
            timeout_ms=self.default_query_timeout_ms,
        )
        base_create_sql = row[0] if row and row[0] else ""
        upper = base_create_sql.upper()
        has_without_rowid = "WITHOUT ROWID" in upper
        has_strict = " STRICT" in upper

        idx_rows_sql = f"PRAGMA index_list(`{table_name}`)"
        _, idx_rows = await backend.fetch_all(idx_rows_sql, None, timeout_ms=self.default_query_timeout_ms)
        idx_sqls: List[str] = []
        for idx in idx_rows:
            iname = idx[1]
            origin = str(idx[3])
            if origin != "c":
                continue
            _, row_sql = await backend.fetch_one(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                [iname],
                timeout_ms=self.default_query_timeout_ms,
            )
            if row_sql and row_sql[0]:
                idx_sqls.append(row_sql[0])
        trg_rows_sql = "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?"
        _, trg_rows = await backend.fetch_all(trg_rows_sql, [table_name], timeout_ms=self.default_query_timeout_ms)
        trg_sqls = [r[1] for r in trg_rows if r[1]]

        keep_cols = [c for c in old_cols_order if c in new_schema_sql]
        new_only = [c for c in new_schema_sql.keys() if c not in keep_cols]
        ordered_cols = keep_cols + new_only

        col_defs: List[str] = []
        for c in ordered_cols:
            ct = new_schema_sql[c]
            frag = f"`{c}` {ct}"
            if c in notnull and notnull[c]:
                frag += " NOT NULL"
            dv = dfltval.get(c, None)
            if dv is not None:
                frag += f" DEFAULT {dv}"
            col_defs.append(frag)
        if pk_cols:
            col_defs.append("PRIMARY KEY (" + ", ".join(f"`{pk}`" for pk in pk_cols) + ")")
        tail = ""
        if has_without_rowid:
            tail += " WITHOUT ROWID"
        if has_strict:
            tail += " STRICT"
        create_sql = f"CREATE TABLE `{table_name}` (" + ", ".join(col_defs) + ")" + tail

        temp_old = f"__old_{table_name}_{secrets.token_hex(6)}"
        async with self.transaction() as tx:
            await tx.execute("PRAGMA foreign_keys=OFF", ())
            await tx.execute(f"ALTER TABLE `{table_name}` RENAME TO `{temp_old}`", ())
            await tx.execute(create_sql, ())
            dst_cols_q = ", ".join(f"`{c}`" for c in ordered_cols)
            select_terms: List[str] = []
            for c in ordered_cols:
                if c in old_cols_order:
                    cast_to = new_schema_sql[c]
                    select_terms.append(f"CAST(`{c}` AS {cast_to}) AS `{c}`")
                else:
                    select_terms.append(f"NULL AS `{c}`")
            select_sql = ", ".join(select_terms)
            await tx.execute(
                f"INSERT INTO `{table_name}` ({dst_cols_q}) SELECT {select_sql} FROM `{temp_old}`",
                (),
            )
            await tx.execute(f"DROP TABLE `{temp_old}`", ())
            await tx.execute("PRAGMA foreign_keys=ON", ())
            for s in idx_sqls:
                await tx.execute(s, ())
            for s in trg_sqls:
                await tx.execute(s, ())

    # ---------- Transactions (SQLite only) ----------

    @asynccontextmanager
    async def transaction(self):
        if self.driver_type != "sqlite":
            raise CadeSQLError("transaction() is SQLite-only in this wrapper")
        backend = self._sqlite_backend
        assert backend is not None
        tx = _Tx(backend.writer)
        await tx.__aenter__()
        try:
            yield tx
        except Exception as e:
            await tx.__aexit__(type(e), e, e.__traceback__)
            raise
        else:
            await tx.__aexit__(None, None, None)

    # ---------- Index APIs ----------

    async def get_indices(self, table_name: str) -> List[Tuple[str, str]]:
        """
        Return list of (index_name, column_name) pairs.
        """
        await self._ensure_connected()
        if self.driver_type == "sqlite":
            backend = self._sqlite_backend
            assert backend is not None
            return await backend.get_indices(table_name)
        backend = self._mysql_backend
        assert backend is not None
        return await backend.get_indices(table_name)

    async def add_indices(self, table_name: str, columns: Sequence[str]):
        """
        Add auto indices for the given columns (idempotent).
        """
        await self._ensure_connected()
        for col in columns:
            if self.driver_type == "sqlite":
                backend = self._sqlite_backend
                assert backend is not None
                await backend.add_index(table_name, col)
            else:
                backend = self._mysql_backend
                assert backend is not None
                await backend.add_index(table_name, col)

    async def remove_indices(self, table_name: str, index_names: Sequence[str]):
        """
        Remove indices by name.
        """
        await self._ensure_connected()
        for idx in index_names:
            if self.driver_type == "sqlite":
                backend = self._sqlite_backend
                assert backend is not None
                await backend.drop_index(idx)
            else:
                backend = self._mysql_backend
                assert backend is not None
                await backend.drop_index(idx, table_name)

    # ---------- Table management ----------

    async def create(
            self,
            table_name: str,
            dataframe: Any,
            on_exists: str = "replace",
            columns: Optional[Sequence[str]] = None,
            primary_keys: Optional[Sequence[str]] = None,
    ):
        await self._ensure_connected()
        df = DataFrameAdapter.to_polars(dataframe, columns=columns)
        exists = await self._table_exists(table_name)
        if exists:
            if on_exists == "raise":
                raise CadeSQLSchemaError(f"Table {table_name} already exists")
            if on_exists == "replace":
                await self.drop_table(table_name)
            elif on_exists == "append":
                await self.upsert(table_name, df)
                return
            else:
                raise CadeSQLSchemaError(f"Unsupported on_exists mode: {on_exists}")

        cols = df.columns
        dtypes = df.dtypes
        col_defs: List[str] = []
        pk_cols = list(primary_keys or [])
        if not pk_cols:
            pk_col = "index"
            pk_cols.append(pk_col)
            if self.driver_type == "sqlite":
                col_defs.append(f"`{pk_col}` INTEGER PRIMARY KEY AUTOINCREMENT")
            else:
                col_defs.append(f"`{pk_col}` BIGINT PRIMARY KEY AUTO_INCREMENT")
        for name, dtype in zip(cols, dtypes):
            if name in pk_cols:
                continue
            sql_type = "TEXT"
            if dtype.is_integer():
                sql_type = "INTEGER"
            elif dtype.is_float():
                sql_type = "REAL"
            elif dtype == pl.Duration:
                sql_type = "REAL"
            elif dtype == pl.Boolean:
                sql_type = "BOOLEAN"
            elif dtype in (pl.Date, pl.Datetime):
                sql_type = "DATETIME"
            col_defs.append(f"`{name}` {sql_type}")
        if pk_cols and pk_cols[0] not in ("index",):
            pk_sql = ", ".join(f"`{c}`" for c in pk_cols)
            col_defs.append(f"PRIMARY KEY ({pk_sql})")
        create_sql = f"CREATE TABLE `{table_name}` (" + ", ".join(col_defs) + ")"
        await self._backend_execute_write(create_sql)
        await self._schema_cache.clear()
        if df.height:
            await self.upsert(table_name, df)

    async def drop_table(self, table_name: str):
        await self._ensure_connected()
        sql = f"DROP TABLE IF EXISTS `{table_name}`"
        await self._backend_execute_write(sql)
        await self._schema_cache.clear()

    async def rename_table(self, old_name: str, new_name: str):
        await self._ensure_connected()
        # NOTE: MySQL supports both `ALTER TABLE old RENAME TO new` (8.0+) and `RENAME TABLE`.
        if self.driver_type == "mysql":
            sql = f"RENAME TABLE `{old_name}` TO `{new_name}`"
        else:
            sql = f"ALTER TABLE `{old_name}` RENAME TO `{new_name}`"
        await self._backend_execute_write(sql)
        await self._schema_cache.clear()

    async def copy_table(self, source_table: str, target_table: str, drop_target: bool = False):
        await self._ensure_connected()
        if drop_target:
            await self.drop_table(target_table)
        schema = await self.query_schema(source_table)
        if not await self._table_exists(target_table):
            sample = await self.select(source_table, limit=1, as_format="polars")
            await self.create(target_table, sample, on_exists="raise", primary_keys=schema.primary_keys or None)
        cols_sql = ", ".join(f"`{c}`" for c in schema.columns)
        sql = f"INSERT INTO `{target_table}` ({cols_sql}) SELECT {cols_sql} FROM `{source_table}`"
        await self._backend_execute_write(sql)

    # ---------- Partitioning (column-based / vertical) ----------

    async def partition_table(self, table_name: str, as_copy: bool = True):
        """
        Vertically partition a logical table into multiple physical tables by columns.
        """
        await self._ensure_connected()

        schema = await self.query_schema(table_name, refresh=True)
        if schema.is_partitioned: return

        pk_cols = schema.primary_keys or ["Index"]
        if not pk_cols:
            raise CadeSQLSchemaError("partition_table requires at least one primary key column")
        non_pk = [c for c in schema.columns if c not in pk_cols]
        if not non_pk:  return

        # Slice non-PK columns into vertical chunks
        chunks: List[List[str]] = []
        for i in range(0, len(non_pk), PARTITION_COLUMN_LIMIT):
            chunks.append(non_pk[i : i + PARTITION_COLUMN_LIMIT])

        # Clear existing metadata for this table
        param_style = self._param_style()
        placeholder = "?" if param_style == "qmark" else "%s"
        if self.driver_type == "sqlite":
            del_part_sql = 'DELETE FROM "_partitions" WHERE table_name=?'
            del_admin_sql = 'DELETE FROM "_admin" WHERE table_name=?'
        else:
            del_part_sql = "DELETE FROM `_partitions` WHERE table_name=%s"
            del_admin_sql = "DELETE FROM `_admin` WHERE table_name=%s"
        await self._backend_execute_write(del_part_sql, [table_name])
        await self._backend_execute_write(del_admin_sql, [table_name])

        # Create / fill partition tables
        for idx, chunk in enumerate(chunks):
            part_name = f"{table_name}__p{idx}"

            # Drop any existing partition table to avoid duplicates
            drop_sql = f"DROP TABLE IF EXISTS `{part_name}`"
            await self._backend_execute_write(drop_sql)

            # Column definitions: all PKs + this chunk
            col_defs: List[str] = []
            for pk in pk_cols:
                col_type = schema.column_types.get(pk, "TEXT")
                col_defs.append(f"`{pk}` {col_type}")
            for c in chunk:
                col_type = schema.column_types.get(c, "TEXT")
                col_defs.append(f"`{c}` {col_type}")
            pk_sql = ", ".join(f"`{c}`" for c in pk_cols)
            col_defs.append(f"PRIMARY KEY ({pk_sql})")

            create_sql = f"CREATE TABLE `{part_name}` (" + ", ".join(col_defs) + ")"
            await self._backend_execute_write(create_sql)

            cols_for_copy = pk_cols + chunk
            cols_sql = ", ".join(f"`{c}`" for c in cols_for_copy)
            insert_sql = f"INSERT INTO `{part_name}` ({cols_sql}) SELECT {cols_sql} FROM `{table_name}`"
            await self._backend_execute_write(insert_sql)

            # Metadata: one row per (logical_table, column_name)
            if self.driver_type == "sqlite":
                meta_sql = 'INSERT INTO "_partitions"(table_name, column_name, partition_table) VALUES (?, ?, ?)'
            else:
                meta_sql = "INSERT INTO `_partitions`(table_name, column_name, partition_table) VALUES (%s, %s, %s)"
            for c in chunk:
                await self._backend_execute_write(meta_sql, [table_name, c, part_name])

        # Admin row with composite PK as CSV
        pk_meta = ",".join(pk_cols)
        if self.driver_type == "sqlite":
            admin_sql = 'INSERT INTO "_admin"(table_name, partitioned, primary_key) VALUES (?, ?, ?)'
        else:
            admin_sql = "INSERT INTO `_admin`(table_name, partitioned, primary_key) VALUES (%s, %s, %s)"
        await self._backend_execute_write(admin_sql, [table_name, 1, pk_meta])

        if not as_copy:
            await self.drop_table(table_name)

        await self._schema_cache.clear()

    async def unpartition_table(self, table_name: str):
        """
        Merge vertical partitions back into a single physical table.
        """
        await self._ensure_connected()
        schema = await self.query_schema(table_name, refresh=True)
        if not schema.is_partitioned or not schema.partitions:
            return

        pk_cols = schema.primary_keys or ["Index"]
        if not pk_cols:
            raise CadeSQLSchemaError("unpartition_table requires at least one primary key column")

        cols = list(pk_cols) + [c for c in schema.columns if c not in pk_cols]
        cols_sql = ", ".join(f"`{c}`" for c in cols)
        temp_table = f"{table_name}__unpart"

        # Create destination table with same types and composite PK
        col_defs: List[str] = []
        for c in cols:
            col_type = schema.column_types.get(c, "TEXT")
            col_defs.append(f"`{c}` {col_type}")
        pk_sql = ", ".join(f"`{c}`" for c in pk_cols)
        col_defs.append(f"PRIMARY KEY ({pk_sql})")
        create_sql = f"CREATE TABLE `{temp_table}` (" + ", ".join(col_defs) + ")"
        await self._backend_execute_write(create_sql)

        # JOIN all partition tables on composite PK
        partitions_by_col: Dict[str, str] = {p.column: p.partition for p in schema.partitions}
        partition_tables = sorted({p.partition for p in schema.partitions})
        if not partition_tables:
            return

        alias_by_table: Dict[str, str] = {}
        base = partition_tables[0]
        alias_by_table[base] = "p0"

        joins = [f"`{base}` p0"]
        for idx, pt in enumerate(partition_tables[1:], start=1):
            alias = f"p{idx}"
            alias_by_table[pt] = alias
            join_conds = " AND ".join(f"p0.`{pk}` = {alias}.`{pk}`" for pk in pk_cols)
            joins.append(f"LEFT JOIN `{pt}` {alias} ON {join_conds}")
        from_clause = " FROM " + " ".join(joins)

        select_cols: List[str] = []
        # Primary keys from base alias
        for pk in pk_cols:
            select_cols.append(f"p0.`{pk}` AS `{pk}`")
        # Non-PK columns from their partition tables
        for c in cols:
            if c in pk_cols:
                continue
            part = partitions_by_col.get(c)
            if part is None:
                # Column not explicitly partitioned, fallback to base
                alias = "p0"
            else:
                alias = alias_by_table.get(part, "p0")
            select_cols.append(f"{alias}.`{c}` AS `{c}`")

        select_sql = "SELECT " + ", ".join(select_cols) + from_clause
        insert_sql = f"INSERT INTO `{temp_table}` ({cols_sql}) " + select_sql
        await self._backend_execute_write(insert_sql)

        # Replace logical table with merged physical table
        await self.drop_table(table_name)
        await self.rename_table(temp_table, table_name)

        # Clean metadata
        param_style = self._param_style()
        if self.driver_type == "sqlite":
            del_part_sql = 'DELETE FROM "_partitions" WHERE table_name=?'
            upd_admin_sql = 'UPDATE "_admin" SET partitioned=0 WHERE table_name=?'
        else:
            del_part_sql = "DELETE FROM `_partitions` WHERE table_name=%s"
            upd_admin_sql = "UPDATE `_admin` SET partitioned=0 WHERE table_name=%s"
        await self._backend_execute_write(del_part_sql, [table_name])
        await self._backend_execute_write(upd_admin_sql, [table_name])
        await self._schema_cache.clear()

    # ---------- Select ----------

    async def _normalize_columns(
            self,
            table_name: str,
            columns: Optional[Sequence[str]],
    ) -> Tuple[TableSchema, List[str]]:
        schema = await self.query_schema(table_name)
        if columns is None:
            return schema, schema.columns
        normalized: List[str] = []
        for col in columns:
            if col in schema.columns:
                normalized.append(col)
            else:
                raise CadeSQLSchemaError(f"Unknown column: {col}")
        return schema, normalized

    async def _build_select_sql(
            self,
            table_name: str,
            filters: Any,
            columns: Optional[Sequence[str]],
            distinct: Union[bool, Sequence[str]],
            limit: Optional[int],
            group_by: Optional[Sequence[str]],
            order_by: Optional[Sequence[Union[str, Tuple[str, bool]]]],
    ) -> Tuple[str, List[Any], TableSchema]:
        schema, cols = await self._normalize_columns(table_name, columns)

        order_by= ensure_list(order_by) if order_by is not None else order_by

        if schema.is_partitioned and self.driver_type == "sqlite":
            # Reuse partitioned path for SQLite; for MySQL we can also use this path if needed.
            return await self._build_partitioned_select_sql(
                table_name,
                filters,
                columns,
                distinct,
                limit,
                group_by,
                order_by,
                schema,
                cols,
            )

        select_clause = "SELECT "
        if isinstance(distinct, bool):
            if distinct:
                select_clause += "DISTINCT "
        else:
            select_clause += "DISTINCT " + ", ".join(f"`{c}`" for c in distinct) + ", "
        select_clause += ", ".join(f"`{c}`" for c in cols)

        param_style = self._param_style()
        where_sql, params = FilterParser.build_where_clause(
            filters,
            schema,
            param_style=param_style,
            supports_ilike=(self.driver_type == "mysql"),
            strict=self.strict_filters,
        )

        group_sql = ""
        if group_by:
            group_cols: List[str] = []
            for col in group_by:
                group_cols.append(col)
            group_sql = " GROUP BY " + ", ".join(f"`{c}`" for c in group_cols)

        order_sql = ""
        if order_by:
            order_cols: List[str] = []
            for item in order_by:
                if isinstance(item, str):
                    col_name = item
                    desc = False
                else:
                    col_name, desc = item
                order_cols.append(f"`{col_name}`{' DESC' if desc else ''}")
            order_sql = " ORDER BY " + ", ".join(order_cols)

        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        sql = select_clause + f" FROM `{table_name}`" + (" " + where_sql if where_sql else "") + group_sql + order_sql + limit_sql
        return sql, params, schema

    async def _build_partitioned_select_sql(
            self,
            table_name: str,
            filters: Any,
            columns: Optional[Sequence[str]],
            distinct: Union[bool, Sequence[str]],
            limit: Optional[int],
            group_by: Optional[Sequence[str]],
            order_by: Optional[Sequence[Union[str, Tuple[str, bool]]]],
            schema: TableSchema,
            cols: Sequence[str],
    ) -> Tuple[str, List[Any], TableSchema]:
        """
        Partition-aware SELECT builder for column-based partitions.

        Joins all partition tables on composite primary key and projects the requested columns.
        """
        pk_cols = schema.primary_keys or ["Index"]
        if not pk_cols:
            raise CadeSQLSchemaError("partitioned SELECT requires a primary key")

        partitions_by_col: Dict[str, str] = {p.column: p.partition for p in schema.partitions}
        partition_tables = sorted({p.partition for p in schema.partitions})
        if not partition_tables:
            raise CadeSQLSchemaError("partitioned table has no physical partitions")

        alias_by_table: Dict[str, str] = {}
        joins: List[str] = []

        base = partition_tables[0]
        alias_by_table[base] = "p0"
        joins.append(f"`{base}` p0")
        for idx, pt in enumerate(partition_tables[1:], start=1):
            alias = f"p{idx}"
            alias_by_table[pt] = alias
            join_conds = " AND ".join(f"p0.`{pk}` = {alias}.`{pk}`" for pk in pk_cols)
            joins.append(f"LEFT JOIN `{pt}` {alias} ON {join_conds}")
        from_sql = " FROM " + " ".join(joins)

        # Build SELECT clause
        select_cols: List[str] = []
        selected_cols = list(cols)
        for pk in pk_cols:
            if pk not in selected_cols:
                selected_cols.insert(0, pk)

        column_alias: Dict[str, str] = {}
        for c in selected_cols:
            if c in pk_cols:
                alias = "p0"
            else:
                part = partitions_by_col.get(c)
                alias = alias_by_table.get(part or base, "p0")
            select_cols.append(f"{alias}.`{c}` AS `{c}`")
            column_alias[c] = alias

        select_clause = "SELECT "
        if isinstance(distinct, bool):
            if distinct:
                select_clause += "DISTINCT "
        else:
            # DISTINCT over explicit columns, then project rest
            distinct_cols: List[str] = []
            for col in distinct:
                alias = column_alias.get(col, "p0")
                distinct_cols.append(f"{alias}.`{col}`")
            select_clause += "DISTINCT " + ", ".join(distinct_cols) + ", "

        select_clause += ", ".join(select_cols)

        # WHERE, GROUP BY, ORDER BY
        groups = FilterParser.normalize_filters(
            filters,
            schema,
            ignore_unknown=not self.strict_filters,
        ) if filters is not None else []
        where_sql, params = await self._build_partitioned_where_clause(schema, groups, column_alias)

        group_sql = ""
        if group_by:
            group_cols: List[str] = []
            for col in group_by:
                alias = column_alias.get(col, "p0")
                group_cols.append(f"{alias}.`{col}`")
            group_sql = " GROUP BY " + ", ".join(group_cols)

        order_sql = ""
        if order_by:
            order_cols: List[str] = []
            for item in order_by:
                if isinstance(item, str):
                    col_name = item
                    desc = False
                else:
                    col_name, desc = item
                alias = column_alias.get(col_name, "p0")
                order_cols.append(f"{alias}.`{col_name}`{' DESC' if desc else ''}")
            order_sql = " ORDER BY " + ", ".join(order_cols)

        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        sql = select_clause + from_sql + (" " + where_sql if where_sql else "") + group_sql + order_sql + limit_sql
        return sql, params, schema

    async def _build_partitioned_where_clause(
            self,
            schema: TableSchema,
            groups: List[List[Dict[str, Any]]],
            column_alias: Dict[str, str],
    ) -> Tuple[str, List[Any]]:
        if not groups:
            return "", []
        param_style = self._param_style()
        placeholder = "?" if param_style == "qmark" else "%s"
        params: List[Any] = []
        clauses: List[str] = []
        for group in groups:
            parts: List[str] = []
            for cond in group:
                col = cond["column"]
                op = str(cond["op"]).lower()
                val = cond.get("value")
                alias = column_alias.get(col, "p0")
                qualified = f"{alias}.`{col}`"
                if op in ("is_null", "is null"):
                    parts.append(f"{qualified} IS NULL")
                    continue
                if op in ("is_not_null", "is not null", "not_null"):
                    parts.append(f"{qualified} IS NOT NULL")
                    continue
                if op in ("between", "not_between"):
                    if not isinstance(val, (list, tuple)) or len(val) != 2:
                        raise CadeSQLSchemaError(f"BETWEEN requires list/tuple of length 2 for column {col}")
                    op_sql = "NOT BETWEEN" if "not" in op else "BETWEEN"
                    parts.append(f"{qualified} {op_sql} {placeholder} AND {placeholder}")
                    params.extend([val[0], val[1]])
                    continue
                if op in ("in", "not_in"):
                    if not isinstance(val, (list, tuple, set)):
                        raise CadeSQLSchemaError(f"IN requires list/tuple/set for column {col}")
                    if not val:
                        continue
                    vals_list = list(val)
                    ph = ",".join(placeholder for _ in vals_list)
                    op_sql = "NOT IN" if "not" in op else "IN"
                    parts.append(f"{qualified} {op_sql} ({ph})")
                    params.extend(vals_list)
                    continue
                if op in ("like", "ilike", "not_like", "not_ilike"):
                    not_flag = "not" in op
                    is_ilike = "ilike" in op
                    if is_ilike and self.driver_type != "mysql":
                        expr = f"LOWER({qualified}) LIKE LOWER({placeholder})"
                    else:
                        expr = f"{qualified} LIKE {placeholder}"
                    if not_flag:
                        expr = f"NOT ({expr})"
                    parts.append(expr)
                    params.append(val)
                    continue
                if op in ("eq", "=", "=="):
                    op_sql = "="
                elif op in ("ne", "!=", "<>"):
                    op_sql = "!="
                elif op in ("lt", "<"):
                    op_sql = "<"
                elif op in ("lte", "<="):
                    op_sql = "<="
                elif op in ("gt", ">"):
                    op_sql = ">"
                elif op in ("gte", ">="):
                    op_sql = ">="
                else:
                    raise CadeSQLSchemaError(f"Unsupported filter operation: {op}")
                parts.append(f"{qualified} {op_sql} {placeholder}")
                params.append(val)
            if parts:
                clauses.append("(" + " AND ".join(parts) + ")")
        if not clauses:
            return "", []
        return "WHERE " + " OR ".join(clauses), params

    async def select(
            self,
            table_name: str,
            filters: Any = None,
            columns: Optional[Sequence[str]] = None,
            distinct: Union[bool, Sequence[str]] = False,
            limit: Optional[int] = None,
            group_by: Optional[Sequence[str]] = None,
            order_by: Optional[Sequence[Union[str, Tuple[str, bool]]]] = None,
            as_format: Optional[str] = None,
            timeout_ms: Optional[int] = None,
            enable_cache: Optional[bool] = None,
            cache_on_miss: Optional[bool] = None,
            cache_ttl_ms: Optional[int] = None,
            align_types: Optional[bool] = True,
            *,
            where: Any = None #Backwards compatibility
    ):
        await self._ensure_connected()
        if where is not None: filters = where
        if columns is not None:
            columns = [columns] if not isinstance(columns, (list, tuple, set)) else list(columns)
        use_cache = self.enable_cache if enable_cache is None else enable_cache
        cache_miss_ok = self.cache_on_miss if cache_on_miss is None else cache_on_miss
        ttl_ms = self.cache_ttl_ms if cache_ttl_ms is None else cache_ttl_ms
        timeout = timeout_ms if timeout_ms is not None else self.default_query_timeout_ms

        if as_format is None:
            as_format = self.default_format

        cache_key = None
        if use_cache and self._query_cache is not None:
            cache_key = (
                table_name,
                repr(filters),
                tuple(columns) if columns is not None else None,
                tuple(distinct) if isinstance(distinct, (list, tuple)) else distinct,
                limit,
                tuple(group_by) if group_by is not None else None,
                tuple(order_by) if order_by is not None else None,
                as_format,
            )
            cached = await self._query_cache.get(cache_key)
            if cached is not None:
                return cached

        sql, params, schema = await self._build_select_sql(
            table_name,
            filters=filters,
            columns=columns,
            distinct=distinct,
            limit=limit,
            group_by=group_by,
            order_by=order_by,
        )

        # Auto-indexer integration: record query + filter usage (normalized).
        if self.auto_index and self._auto_indexer is not None:
            # Query columns: normalize if specified; else all schema columns.
            query_cols: List[str] = []
            if columns is None:
                query_cols = list(schema.columns)
            else:
                for col in columns:
                    if col in schema.columns:
                        query_cols.append(str(col))
            try:
                if query_cols:
                    await self._auto_indexer.record_query(table_name, query_cols)
                if filters is not None:
                    groups = FilterParser.normalize_filters(
                        filters,
                        schema,
                        ignore_unknown=not self.strict_filters,
                    )
                    filter_cols: List[str] = []
                    for g in groups:
                        for cond in g:
                            c = cond["column"]
                            if c not in filter_cols:
                                filter_cols.append(c)
                    if filter_cols:
                        await self._auto_indexer.record_filter(table_name, filter_cols)
            except Exception as exc:
                # Indexer failure must not break main query path.
                await log.warning("Auto-index bookkeeping failed", table=table_name, error=str(exc))

        if as_format == "arrow":
            result = await self._backend_read_arrow(sql, params, timeout_ms=timeout)
            has_rows = (result is not None) and (result.num_rows > 0)
        elif as_format in (None, "polars", "polars_lazy") and self.driver_type == "sqlite":
            result = await self._backend_read_polars(sql, params, timeout_ms=timeout)
            has_rows = (result is not None) and (not result.hyper.is_empty())
            if as_format == "polars_lazy":
                result = result.lazy()
        else:
            out_cols, rows = await self._backend_fetch_all(sql, params, timeout_ms=timeout)
            result = DataFrameAdapter.from_records(rows, out_cols, as_format)
            if isinstance(result, (pl.DataFrame, pl.LazyFrame)):
                has_rows = (result is not None) and (not result.hyper.is_empty())
            elif isinstance(result, pa.Table):
                has_rows = (result is not None) and (result.num_rows > 0)
            else:
                has_rows = (result is not None) and bool(result)

        if align_types and ( (as_format is None) or (as_format in ('polars', 'polars_lazy'))):
            result = await self._coerce_nulls_to_schema(table_name, result)

        if use_cache and (self._query_cache is not None) and (has_rows or cache_miss_ok):
            await self._query_cache.set(cache_key, result, ttl_ms=ttl_ms)
        return result

    # ---------- Remove / overlaps ----------

    async def remove(self, table_name: str, filters: Any, timeout_ms: Optional[int] = None):
        await self._ensure_connected()
        schema = await self.query_schema(table_name)
        param_style = self._param_style()
        # Deletion is dangerous; keep filters strict regardless of global setting.
        where_sql, params = FilterParser.build_where_clause(
            filters,
            schema,
            param_style=param_style,
            supports_ilike=(self.driver_type == "mysql"),
            strict=True,
        )
        if not where_sql:
            raise CadeSQLQueryError("Refusing to DELETE without filters")
        sql = f"DELETE FROM `{table_name}` {where_sql}"
        timeout = timeout_ms if timeout_ms is not None else self.default_query_timeout_ms
        await self._backend_execute_write(sql, params, timeout_ms=timeout)

    async def remove_overlaps(
            self,
            table_name: str,
            dataframe: Any,
            key_columns: Optional[Sequence[str]] = None,
            timeout_ms: Optional[int] = None,
    ):
        await self._ensure_connected()
        df = DataFrameAdapter.to_polars(dataframe)
        schema = await self.query_schema(table_name)
        keys = list(key_columns or (schema.primary_keys or []))
        if not keys:
            raise CadeSQLSchemaError("remove_overlaps requires key_columns or primary keys")
        _, rows = DataFrameAdapter.to_records(df, columns=keys)
        if not rows:
            return
        param_style = self._param_style()
        placeholder = "?" if param_style == "qmark" else "%s"
        if len(keys) == 1:
            key = keys[0]
            values = [r[0] for r in rows]
            in_clause = ",".join(placeholder for _ in values)
            where_sql = f"`{key}` IN ({in_clause})"
            params = values
        else:
            tuple_place = "(" + ",".join(placeholder for _ in keys) + ")"
            tuples_clause = ",".join(tuple_place for _ in rows)
            where_sql = "(" + ", ".join(f"`{k}`" for k in keys) + f") IN ({tuples_clause})"
            params = [v for row in rows for v in row]
        sql = f"DELETE FROM `{table_name}` WHERE {where_sql}"
        timeout = timeout_ms if timeout_ms is not None else self.default_query_timeout_ms
        await self._backend_execute_write(sql, params, timeout_ms=timeout)

    async def add_columns(self, table_name: str, columns: Dict[str, str]):
        await self._ensure_connected()
        if not columns: return

        exists = await self._table_exists(table_name)
        if not exists:
            raise CadeSQLError('Cant add columns to a table that doesnt exist')

        existing = await self.query_schema_types(table_name)
        new_columns = {k:v for k,v in columns.items() if k not in existing.keys()}
        if not new_columns: return

        for col, dtype in new_columns.items():
            await self.schema_add(table_name, col, dtype)
        await self._schema_cache.clear()


    async def upsert(
            self,
            table_name: str,
            dataframe: Any,
            use_sentinel: bool = False,
            enable_schema_match: bool = True,
            on_schema_mismatch: str = "alter",
            enable_schema_overrides: bool = False,
            enable_new_columns: bool = True,
            new_column_limit: Optional[int] = 50,
            default_fill: Any = None,
            drop_on_failed_coerce: bool = False,
    ):
        await self._ensure_connected()
        df = DataFrameAdapter.to_polars(dataframe)
        if df.is_empty(): return
        exists = await self._table_exists(table_name)
        if not exists:
            await self.create(table_name, df, on_exists="raise")
            return

        schema = await self.query_schema_types(table_name)
        df = await self._coerce_nulls_to_schema(table_name, df)
        pks = await self.list_primary_keys(table_name)
        if not pks: raise ValueError("Upsert requires primary keys")

        if enable_new_columns:
            incoming_types = {c: _get_sqlite_type(t) for c, t in df.schema.items()}
            to_add = {c: t for c, t in incoming_types.items() if c not in schema.keys()}
            if to_add and (new_column_limit is not None) and (len(to_add) > new_column_limit):
                await log.erorr(f"Refusing to add {len(to_add)} new column(s) to '{table_name}' for {df.height} rows; safeguard={new_column_limit}")
                df = df.drop(list(to_add.keys()), strict=False)
            elif len(to_add):
                await log.sql(f"adding {len(to_add)} columns")
                await self.add_columns(table_name, to_add)
                schema = await self.query_schema_types(table_name)

        if enable_schema_overrides:
            existing = await self.query_schema_types(table_name)
            desired = {c: _get_sqlite_type(t) for c, t in df.schema.items() if c in existing.keys()}
            change_map = {c: desired[c] for c, dbt in existing.items() if c in desired and _sqlite_affinity(dbt) != _sqlite_affinity(desired[c])}
            if change_map:
                await self.schema_alter(table_name, list(change_map.keys()), change_map)

        if enable_schema_match:
            warn_cb = (lambda m: log.warning(m))
            df, dropped = _coerce_frame_to_sqlite_schema(df, schema, drop_on_fail=drop_on_failed_coerce, warn_cb=warn_cb)
            if not df.columns:
                warn_cb(f"[SQL] No columns left after coercion for '{table_name}'. Nothing upserted.")
                return

        table_cols = set(schema.keys())
        _df_cols = df.columns
        df_cols = [c for c in _df_cols if c in table_cols]
        if not df_cols: return
        df = df.select(df_cols)
        if df.is_empty(): return

        update_cols = {c for c in df_cols if c not in pks}
        if not update_cols: return

        if 'safeguard' in update_cols:
            if df.select(pl.col("safeguard").is_null().all()).item():
                raise CadeSQLError('SAFEGAURD TRIPPED')

        cols, rows = DataFrameAdapter.to_records(df)
        if use_sentinel or self.enable_clear_sentinel:
            if len(pks) != 1: raise CadeSQLSchemaError("CLEAR_SENTINEL upsert only supports single PK column")
            await self._rowwise_upsert_with_sentinel(table_name, cols, rows, pks[0])
        else:
            await self._bulk_upsert(table_name, cols, rows, pks)

    async def _coerce_nulls_to_schema(self, table_name: str, df: pl.DataFrame) -> pl.DataFrame:

        casts: List[pl.Expr] = []
        null_cols = set()

        for col, src_dt in df.schema.items():
            if src_dt == pl.Null:
                null_cols.add(col)
        if null_cols:
            schema = await self.query_schema(table_name)
            for col in null_cols:
                tgt_sql = schema.column_types.get(col)
                if tgt_sql:
                    tgt_dt = _polars_dtype_for_sqlite(tgt_sql)
                    if tgt_dt != pl.Null:
                        casts.append(pl.col(col).cast(tgt_dt, strict=False))

        return df.with_columns(casts) if casts else df

    async def _bulk_upsert(self, table_name: str, columns: Sequence[str], rows: List[Tuple[Any, ...]], pk_cols: Sequence[str]):
        if not rows:
            return
        param_style = self._param_style()
        placeholder = "?" if param_style == "qmark" else "%s"
        cols_sql = ", ".join(f"`{c}`" for c in columns)
        values_sql = ", ".join(placeholder for _ in columns)
        non_pk = [c for c in columns if c not in pk_cols]
        if self.driver_type == "sqlite":
            conflict_cols = ", ".join(f"`{c}`" for c in pk_cols)
            update_assignments = ", ".join(f'"{col}"=excluded."{col}"' for col in non_pk)
            sql = f"INSERT OR IGNORE INTO `{table_name}` ({cols_sql}) VALUES ({values_sql}) ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_assignments}"
        else:
            update_assignments = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in non_pk)
            sql = f"INSERT OR IGNORE INTO `{table_name}` ({cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_assignments}"
        batch: List[Tuple[Any, ...]] = []
        for row in rows:
            batch.append(row)
            if len(batch) >= WRITE_BATCH_SIZE:
                await self._backend_executemany_write(sql, batch, timeout_ms=self.default_query_timeout_ms)
                batch = []
        if batch:
            await self._backend_executemany_write(sql, batch, timeout_ms=self.default_query_timeout_ms)

    async def _rowwise_upsert_with_sentinel(
            self,
            table_name: str,
            columns: Sequence[str],
            rows: List[Tuple[Any, ...]],
            pk_col: str,
    ):
        if not rows:
            return
        if pk_col not in columns:
            raise CadeSQLSchemaError("Primary key column not in dataframe for sentinel upsert")
        pk_idx = columns.index(pk_col)
        param_style = self._param_style()
        placeholder = "?" if param_style == "qmark" else "%s"
        for row in rows:
            pk_val = row[pk_idx]
            update_assignments: List[str] = []
            update_params: List[Any] = []
            insert_values: List[Any] = []
            for idx, col in enumerate(columns):
                val = row[idx]
                ins_val = val
                if val == CLEAR_SENTINEL:
                    ins_val = None
                if col != pk_col:
                    if val is None:
                        # Skip NULL in update to preserve existing value
                        pass
                    elif val == CLEAR_SENTINEL:
                        update_assignments.append(f"`{col}`=NULL")
                    else:
                        update_assignments.append(f"`{col}`={placeholder}")
                        update_params.append(val)
                insert_values.append(ins_val)
            if update_assignments:
                sql_update = f"UPDATE `{table_name}` SET " + ", ".join(update_assignments) + f" WHERE `{pk_col}`={placeholder}"
                params_update = list(update_params) + [pk_val]
                await self._backend_execute_write(sql_update, params_update, timeout_ms=self.default_query_timeout_ms)
            cols_sql = ", ".join(f"`{c}`" for c in columns)
            placeholders = ", ".join(placeholder for _ in columns)
            if self.driver_type == "sqlite":
                sql_insert = f"INSERT OR IGNORE INTO `{table_name}` ({cols_sql}) VALUES ({placeholders})"
            else:
                sql_insert = f"INSERT IGNORE INTO `{table_name}` ({cols_sql}) VALUES ({placeholders})"
            await self._backend_execute_write(sql_insert, insert_values, timeout_ms=self.default_query_timeout_ms)

    async def upsert_if_exists(self, table_name: str, dataframe: Any, **kwargs):
        await self._ensure_connected()
        df = DataFrameAdapter.to_polars(dataframe)
        schema = await self.query_schema(table_name)
        if not schema.primary_keys:
            raise CadeSQLSchemaError("upsert_if_exists requires primary key")
        pk = schema.primary_keys[0]
        pk_values = df.select([pk]).to_series().to_list()
        if not pk_values:
            return
        param_style = self._param_style()
        placeholder = "?" if param_style == "qmark" else "%s"
        in_clause = ",".join(placeholder for _ in pk_values)
        sql = f"SELECT `{pk}` FROM `{table_name}` WHERE `{pk}` IN ({in_clause})"
        _, rows = await self._backend_fetch_all(sql, pk_values, timeout_ms=self.default_query_timeout_ms)
        existing = {r[0] for r in rows}
        if not existing:
            return
        mask = df[pk].is_in(existing)
        df_filtered = df.filter(mask)
        await self.upsert(table_name, df_filtered, **kwargs)

    async def upsert_if_new(self, table_name: str, dataframe: Any, **kwargs):
        await self._ensure_connected()
        df = DataFrameAdapter.to_polars(dataframe)
        schema = await self.query_schema(table_name)
        if not schema.primary_keys:
            raise CadeSQLSchemaError("upsert_if_new requires primary key")
        pk = schema.primary_keys[0]
        pk_values = df.select([pk]).to_series().to_list()
        if not pk_values:
            return
        param_style = self._param_style()
        placeholder = "?" if param_style == "qmark" else "%s"
        in_clause = ",".join(placeholder for _ in pk_values)
        sql = f"SELECT `{pk}` FROM `{table_name}` WHERE `{pk}` IN ({in_clause})"
        _, rows = await self._backend_fetch_all(sql, pk_values, timeout_ms=self.default_query_timeout_ms)
        existing = {r[0] for r in rows}
        mask = ~df[pk].is_in(existing)
        df_filtered = df.filter(mask)
        await self.upsert(table_name, df_filtered, **kwargs)

    # ---------- Duplicate ----------

    async def duplicate(
            self,
            table_name: str,
            filters: Any,
            columns: Optional[Sequence[str]] = None,
            pk_mutator=None,
    ):
        await self._ensure_connected()
        schema, cols = await self._normalize_columns(table_name, columns)
        sql, params, _ = await self._build_select_sql(
            table_name,
            filters=filters,
            columns=cols,
            distinct=False,
            limit=None,
            group_by=None,
            order_by=None,
        )
        out_cols, rows = await self._backend_fetch_all(sql, params, timeout_ms=self.default_query_timeout_ms)
        if not rows:
            return DataFrameAdapter.from_records([], out_cols, as_format="polars")
        if not schema.primary_keys:
            raise CadeSQLSchemaError("duplicate requires primary key")
        pk = schema.primary_keys[0]
        pk_idx = out_cols.index(pk)
        new_rows: List[Tuple[Any, ...]] = []
        for row in rows:
            row_list = list(row)
            old_pk = row_list[pk_idx]
            if pk_mutator:
                new_pk = pk_mutator(old_pk)
            else:
                new_pk = f"{old_pk}-2"
            row_list[pk_idx] = new_pk
            new_rows.append(tuple(row_list))
        df_new = DataFrameAdapter.from_records(new_rows, out_cols, as_format="polars")
        await self.upsert(table_name, df_new)
        return df_new

    async def list_primary_keys(self, table_name: str) -> List[str]:
        s = await self.query_schema(table_name, True)
        return s.primary_keys if s is not None else None

    # ---------- Direct Arrow / Polars accessors (SQLite only) ----------

    async def read_arrow(
            self,
            query: str,
            params: Optional[Sequence[Any]] = None,
            timeout_ms: Optional[int] = None,
    ) -> pa.Table:
        """Execute a read query via ConnectorX and return a PyArrow Table."""
        await self._ensure_connected()
        if self.driver_type != "sqlite":
            raise CadeSQLError("read_arrow is only available for the SQLite driver")
        backend = self._sqlite_backend
        assert backend is not None
        return await backend.read_arrow(query, params, timeout_ms)

    async def read_polars(
            self,
            query: str,
            params: Optional[Sequence[Any]] = None,
            timeout_ms: Optional[int] = None,
    ) -> pl.DataFrame:
        """Execute a read query via ConnectorX and return a Polars DataFrame."""
        await self._ensure_connected()
        if self.driver_type != "sqlite":
            raise CadeSQLError("read_polars is only available for the SQLite driver")
        backend = self._sqlite_backend
        assert backend is not None
        return await backend.read_polars(query, params, timeout_ms)

    async def read_dicts(
            self,
            query: str,
            params: Optional[Sequence[Any]] = None,
            timeout_ms: Optional[int] = None,
    ) -> list[dict]:
        """
        Backward-compat shim returning list[dict].

        .. deprecated:: Use ``read_arrow`` or ``read_polars`` for new code.
        """
        await self._ensure_connected()
        if self.driver_type != "sqlite":
            raise CadeSQLError("read_dicts is only available for the SQLite driver")
        backend = self._sqlite_backend
        assert backend is not None
        return await backend.read_dicts(query, params, timeout_ms)

    @property
    def sqlite_engine(self) -> Optional[AsyncSQLiteEngine]:
        """Direct access to the underlying AsyncSQLiteEngine (SQLite only)."""
        if self._sqlite_backend is not None:
            return self._sqlite_backend.engine
        return None

# ------ Arrow IPC helpers ------

def arrow_ipc_response(
        table: pa.Table,
        compression: str = "lz4_frame",
):
    """
    Build an HTTP-response-ready tuple for Arrow IPC streaming data.

    Returns ``(body_bytes, headers_dict)`` so the caller can construct a
    framework-specific ``Response`` object (Starlette, FastAPI, Litestar, …).

    Usage with Starlette/FastAPI::

        body, headers = arrow_ipc_response(table)
        return Response(content=body, media_type="application/vnd.apache.arrow.stream", headers=headers)
    """
    body = AsyncSQLiteEngine.serialize_arrow_ipc(table, compression=compression)
    headers = {
        "Content-Length": str(len(body)),
        "X-Arrow-Compression": compression,
        "X-Row-Count": str(table.num_rows),
        "X-Col-Count": str(table.num_columns),
    }
    return body, headers


# ------ Legacy ------

_encode_meta = lambda v: encode(v)
def _package_arrow_metadata(tbl: pa.Table, meta: Dict[str, Any]):
    metadata_bytes = {
        str(key).encode(): (_encode_meta(value) if isinstance(value, (dict, list, tuple)) else str(value).encode())
        for key, value in meta.items()
    }
    return tbl.replace_schema_metadata(metadata_bytes)

def _arrow_ipc_from_arrow(arr: pa.Table) -> bytes:
    """Legacy IPC serialiser — prefer ``AsyncSQLiteEngine.serialize_arrow_ipc``."""
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, arr.schema) as writer:
        writer.write_table(arr)
    return sink.getvalue().to_pybytes()

if __name__ == "__main__2":
    db = CadeSQL(
        driver="sqlite",  # or "mysql"
        database=r"C:\Users\zawackic\CADE_GIT\portfolio-tool - Copy (5)\app\data\portfolioTool.db",
        enable_cache=False,
        auto_index=True,
        enable_clear_sentinel=False,
    )
