
import asyncio
import contextlib
import time
import weakref
import uuid
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from collections import defaultdict

import polars as pl
import msgspec.json

from app.logs.logging import log
from app.services.payload.columnar_codec import OptimizedColumnarCodec
from app.services.payload.payloadV4 import (
    BroadcastMessage, Publish, Toast, Message, PayloadOptions, RoomContext, Delta, INDEX_COL_NAME
)

# ======================================================================================
# Tunables
# ======================================================================================

MAX_BATCH_SIZE = 500                                # max grouped rows per key before immediate flush
MAX_PAYLOAD_ROWS = 25_000                           # Safegaurd
FLUSH_INTERVAL_MS = 200                             # periodic flush cadence in milliseconds
MAX_AUDIT_TRAIL = 64                                # cap per-pk audit records
MAX_AUDIT_USER_KEYS = 8                             # cap user_data keys we retain in audit
FLATTEN = False                                     # True -> single combined publish per flush; False -> chunk by action/columns
MAX_PAYLOAD_BYTES = 0                               # Safegaurd, not using right now
MAX_UPDATE_BUCKETS = 64                             # Guard against bucket explosion
_DEFAULT_OPTIONS_DICT = PayloadOptions().to_dict()  # cached

# ======================================================================================
# Utilities
# ======================================================================================

_json_enc = msgspec.json.Encoder()
_codec = OptimizedColumnarCodec()

def _now() -> float:
    return time.time()

def _to_pk_tuple(row: Dict[str, Any], pk_cols: List[str]) -> Tuple[Any, ...]:
    if not pk_cols:
        return tuple()
    if len(pk_cols) == 1:
        return (row.get(pk_cols[0]),)
    return tuple(row.get(c) for c in pk_cols)

def _pk_dict(pk_t: Tuple[Any, ...], pk_cols: List[str]) -> Dict[str, Any]:
    if not pk_cols:
        return {}
    if len(pk_cols) == 1:
        return {pk_cols[0]: pk_t[0] if pk_t else None}
    return dict(zip(pk_cols, pk_t))


@lru_cache(maxsize=4096)
def _stable_filters_bytes(s: str) -> bytes:
    return s.encode("utf-8")


def _group_key_from_context(ctx: RoomContext) -> Tuple[str, str, bytes]:
    room = (ctx.room or "")
    grid_id = (ctx.grid_id or "")
    filters = ctx.grid_filters or {}
    try:
        # msgspec is ~5-10x faster than stdlib json.dumps; keys are pre-sorted
        # by RoomContext.__post_init__ via _sort_dict
        b = msgspec.json.encode(filters)
    except Exception:
        b = _stable_filters_bytes(repr(sorted(filters.items(), key=lambda kv: str(kv[0]))))
    return room, grid_id, b


def _rows_from_frame(df: Any) -> List[Dict[str, Any]]:
    """
    Decode an incoming frame (encoded columnar dict or pl.DataFrame) into
    a list of row dicts WITHOUT introducing false nulls.

    Key guarantee:
      - If a cell was *missing* in the sparse payload, it remains absent
        in the row dict (no synthetic None).
      - If a cell was explicitly null, it is preserved as None.
    """
    if df is None:
        return []
    if isinstance(df, pl.DataFrame):
        return df.to_dicts()
    if isinstance(df, dict):  # encoded or column-oriented
        # 1) Prefer presence-partition decode to preserve sparsity
        try:
            parts = _codec.decode_to_polars_partitions(df)
            if parts:
                out: List[Dict[str, Any]] = []
                for pdf in parts:
                    out.extend(pdf.to_dicts())
                return out
        except Exception as e1:
            log.debug(f"[Batcher] partition decode failed: {e1}")
        # 2) Fallback: dense decode (best-effort) — may be legacy/unknown format
        try:
            pdf = _codec.decode_to_polars(df)
            return pdf.to_dicts()
        except Exception as e2:
            log.debug(f"[Batcher] dense decode failed: {e2}")
        # 3) Column-oriented dict → DataFrame
        try:
            pdf = pl.DataFrame(df)
            return pdf.to_dicts()
        except Exception as e3:
            log.debug(f"[Batcher] dict->DataFrame failed: {e3}")
        # 4) Sparse rows fallback
        data = df.get("_data")
        return data if isinstance(data, list) else []
    # unsupported frame type is ignored to keep hot path exception-free
    return []


def _encode_sparse(obj: Union[pl.DataFrame, List[Dict[str, Any]]]) -> Dict[str, Any]:
    if isinstance(obj, pl.DataFrame):
        return _codec.encode_dataframe(obj, include_schema=True)
    if isinstance(obj, list):
        if not obj:
            return {"_format": "empty", "_count": 0, "_version": _codec.ENCODING_VERSION}
        try:
            df = pl.DataFrame(obj, infer_schema_length=min(len(obj), 1000))
            return _codec.encode_dataframe(df, include_schema=True)
        except Exception:
            return {"_format": "rows", "_data": obj, "_count": len(obj)}
    return {"_format": "unknown"}


def _ensure_pk_columns(payload: Publish, fallback: Optional[Iterable[str]] = None) -> List[str]:
    try:
        pls = payload.data.payloads if payload and payload.data else None
        if pls and getattr(pls, "_pk_columns", None):
            return list(pls._pk_columns)
        ctx_pks = getattr(getattr(payload, "context", None), "primary_keys", None)
        if ctx_pks:
            return list(ctx_pks)
        if fallback:
            return list(fallback)
        return []
    except Exception:
        return list(fallback or [])


# ======================================================================================
# Audit + Batched item
# ======================================================================================

@dataclass
class AuditInfo:
    websocket: Optional[Any] = None
    user_data: Optional[Dict[str, Any]] = None
    timestamp: float = field(default_factory=_now)
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    origin_rule: Optional[str] = None
    ws_ref: Optional[weakref.ReferenceType] = field(default=None, repr=False)

    def __post_init__(self):
        if self.websocket is not None:
            try:
                self.ws_ref = weakref.ref(self.websocket)
            except TypeError:
                self.ws_ref = None
            self.websocket = None
        if isinstance(self.user_data, dict):
            keys = sorted(list(self.user_data.keys()))[:MAX_AUDIT_USER_KEYS]
            self.user_data = {k: self.user_data.get(k, None) for k in keys}


@dataclass
class _BatchedItem:
    payload: BroadcastMessage | Publish
    audit: AuditInfo


# ======================================================================================
# PayloadBatcher (v4)
# ======================================================================================

class PayloadBatcher:
    def __init__(
            self,
            max_batch_size: int = MAX_BATCH_SIZE,
            flush_interval_ms: int = FLUSH_INTERVAL_MS,
            flatten: bool = FLATTEN,
            max_audit_trail: int = MAX_AUDIT_TRAIL,
    ):
        self.max_batch_size = int(max_batch_size)
        self.flush_interval = float(flush_interval_ms) / 1000.0
        self.flatten = bool(flatten)
        self.max_audit_trail = int(max_audit_trail)

        self._in_q: asyncio.Queue[_BatchedItem] = asyncio.Queue(maxsize=80192)
        self._out_q: asyncio.Queue[Union[BroadcastMessage, Dict[str, Any]]] = asyncio.Queue(maxsize=80192)
        self._lock = asyncio.Lock()
        self._running = True

        # groups: (room, grid_id, filters_bytes) -> state
        self._groups: Dict[
            Tuple[str, str, bytes],
            Dict[str, Any],
        ] = defaultdict(lambda: {
            "context": None,  # RoomContext (dict form)
            "pk": None,  # List[str]
            "options": None,  # Dict (PayloadOptions-like)
            "adds": {},  # pk_tuple -> (row_dict, [AuditInfo])
            "updates": {},  # pk_tuple -> (partial_row_dict, [AuditInfo])
            "removes": set(),  # set[pk_tuple]
            "toasts": [],
            "count": 0,  # aggregated row count
            "based_on": -1,
            "action_seq": -1,
        })

        self._synthetic_pk_counter: int = 0
        self._consumer: Optional[asyncio.Task] = None
        self._flusher: Optional[asyncio.Task] = None

    # ---------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------

    async def init(self):
        self._consumer = asyncio.create_task(self._consume_loop(), name="payloadbatcher-consume-v4")
        self._flusher = asyncio.create_task(self._periodic_flush(), name="payloadbatcher-flush-v4")

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._running and self._out_q.empty():
            raise StopAsyncIteration
        item = await self._out_q.get()
        return item

    async def shutdown(self, timeout=5.0):
        await self._shutdown()
        await self.wait_drained(timeout=timeout)

    async def _shutdown(self):
        self._running = False
        # Drain any remaining items from _in_q into groups before flushing
        while not self._in_q.empty():
            try:
                item = self._in_q.get_nowait()
                key = self._group_key(item.payload)
                async with self._lock:
                    self._ingest_into_group(key, item)
            except Exception:
                break
        await self._emergency_flush_all()
        if self._consumer:
            self._consumer.cancel()
        if self._flusher:
            self._flusher.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*(t for t in (self._consumer, self._flusher) if t))
        self._consumer = None
        self._flusher = None

    # ---------------------------------------------------------------------
    # Public API (backward-compatible)
    # ---------------------------------------------------------------------

    async def add_message(
            self,
            payload: Union[Publish, Toast, BroadcastMessage, Dict[str, Any]],
            websocket: Optional[Any] = None,
            user_data: Optional[Dict[str, Any]] = None,
            origin_rule: Optional[str] = None,
    ):
        if not self._running:
            await log.critical("BATCHER IS NOT RUNNING")
            return
        try:
            obj: BroadcastMessage
            if isinstance(payload, BroadcastMessage):
                obj = payload
            elif isinstance(payload, dict):
                obj = Message.construct(payload)  # Publish/Toast/etc.
            else:
                obj = Message.construct(payload)  # may raise
            action = obj.str  # v4 action string
            if action == "publish":
                audit = AuditInfo(websocket=websocket, user_data=user_data, origin_rule=origin_rule)
                await self._in_q.put(_BatchedItem(obj, audit))
            else:
                # Non-publish messages passthrough (e.g., Toast)
                await self._out_q.put(obj)
        except Exception as e:
            await log.error(f"[Batcher] add_message error: {e}")

    # ---------------------------------------------------------------------
    # Internal loops
    # ---------------------------------------------------------------------

    async def _consume_loop(self):
        try:
            while self._running or (not self._in_q.empty()):
                try:
                    item = await self._in_q.get()
                except asyncio.CancelledError:
                    break
                except Exception:
                    continue

                needs_flush = None
                try:
                    key = self._group_key(item.payload)
                    async with self._lock:
                        self._ingest_into_group(key, item)
                        g = self._groups[key]
                        if g["count"] >= self.max_batch_size:
                            needs_flush = key
                except Exception as e:
                    await log.error(f"[Batcher] consume error: {e}")
                    continue

                if needs_flush is not None:
                    await self._flush(needs_flush)
        except asyncio.CancelledError:
            pass

    async def _periodic_flush(self):
        try:
            while self._running:
                await asyncio.sleep(self.flush_interval)
                messages: List[Union[BroadcastMessage, Dict[str, Any]]] = []
                async with self._lock:
                    for k, g in list(self._groups.items()):
                        if g["count"] and (g["adds"] or g["updates"] or g["removes"]):
                            try:
                                messages.extend(self._prepare_flush_locked(k))
                            except Exception as e:
                                await log.error(f"[Batcher] periodic flush prep error for {k}: {e}")
                for msg in messages:
                    await self._out_q.put(msg)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await log.error(f"[Batcher] periodic flush error: {e}")

    async def _emergency_flush_all(self):
        messages: List[Union[BroadcastMessage, Dict[str, Any]]] = []
        async with self._lock:
            for k, g in list(self._groups.items()):
                if g["count"] > 0:
                    try:
                        messages.extend(self._prepare_flush_locked(k))
                    except Exception:
                        pass
        for msg in messages:
            await self._out_q.put(msg)

    async def wait_drained(self, timeout: Optional[float] = None) -> bool:
        start = time.time()
        while not self._out_q.empty() or not self._in_q.empty():
            await asyncio.sleep(0.01)
            if (timeout is not None) and (time.time() - start > timeout):
                return False
        return True

    # ---------------------------------------------------------------------
    # Grouping + ingestion
    # ---------------------------------------------------------------------

    def _group_key(self, payload: Publish) -> Tuple[str, str, bytes]:
        return _group_key_from_context(payload.context)

    def _ingest_into_group(self, key: Tuple[str, str, bytes], item: _BatchedItem):
        p: Publish = item.payload  # Publish (v4)
        g = self._groups[key]

        if g["context"] is None:
            try:
                g["context"] = p.context.to_dict()
            except Exception:
                g["context"] = {
                    "room": p.context.room,
                    "grid_id": p.context.grid_id,
                    "grid_filters": p.context.grid_filters,
                    "primary_keys": p.context.primary_keys,
                    "matched_pattern": p.context.matched_pattern
                }
        try:
            opts = p.options.to_dict() if hasattr(p.options, "to_dict") else {}
        except Exception:
            opts = {}
        g["options"] = opts or g["options"] or _DEFAULT_OPTIONS_DICT
        if p.data and p.data.toast is not None:
            if len(g['toasts']) < MAX_AUDIT_TRAIL:
                g['toasts'].append(p.data.toast)

        pls = p.data.payloads if p.data else None
        if pls:
            try:
                if pls.based_on and pls.based_on > g["based_on"]:
                    g["based_on"] = int(pls.based_on)
            except Exception:
                pass
            try:
                if pls.action_seq and pls.action_seq > g["action_seq"]:
                    g["action_seq"] = int(pls.action_seq)
            except Exception:
                pass

        # Derive PK columns once
        if g["pk"] is None:
            g["pk"] = _ensure_pk_columns(p, fallback=[])

        pk_cols: List[str] = g["pk"] or []
        if not pls: return
        ap_count = 0

        def maybe_adopt_row_index_pk(row: Dict[str, Any]):
            nonlocal pk_cols
            if not pk_cols and (INDEX_COL_NAME in row):
                pk_cols = [INDEX_COL_NAME]
                g["pk"] = pk_cols

        # ADD
        for d in (pls.add or []):
            rows = _rows_from_frame(d.frame)
            if not rows:
                continue
            for row in rows:
                maybe_adopt_row_index_pk(row)
                if pk_cols:
                    pk_t = _to_pk_tuple(row, pk_cols)
                    cur = g["adds"].get(pk_t)
                    if cur is None:
                        g["adds"][pk_t] = (row, [item.audit])
                    else:
                        merged = {**cur[0], **row}
                        audits = (cur[1] + [item.audit])[-self.max_audit_trail:]
                        g["adds"][pk_t] = (merged, audits)
                    g["removes"].discard(pk_t)
                    g["updates"].pop(pk_t, None)
                else:
                    # No PK available: append as unique entry using a stable monotonic key
                    self._synthetic_pk_counter += 1
                    g["adds"][("_syn", self._synthetic_pk_counter)] = (row, [item.audit])
                ap_count += 1

        # UPDATE
        for d in (pls.update or []):
            rows = _rows_from_frame(d.frame)
            if not rows:
                continue
            for row in rows:
                maybe_adopt_row_index_pk(row)
                if pk_cols:
                    pk_t = _to_pk_tuple(row, pk_cols)
                    cur = g["updates"].get(pk_t, (None, []))
                    base = cur[0] if isinstance(cur[0], dict) else {}
                    merged = base if not row else ({**base, **row})
                    audits = (cur[1] + [item.audit])[-self.max_audit_trail:]
                    g["updates"][pk_t] = (merged, audits)
                    g["removes"].discard(pk_t)
                else:
                    self._synthetic_pk_counter += 1
                    g["updates"][("_syn", self._synthetic_pk_counter)] = (row, [item.audit])
                ap_count += 1

        # REMOVE
        for d in (pls.remove or []):
            rows = _rows_from_frame(d.frame)
            if not rows:
                continue
            for row in rows:
                maybe_adopt_row_index_pk(row)
                if pk_cols:
                    pk_t = _to_pk_tuple(row, pk_cols)
                    g["adds"].pop(pk_t, None)
                    g["updates"].pop(pk_t, None)
                    g["removes"].add(pk_t)
                    ap_count += 1
                else:
                    # Without PK, a remove cannot be meaningfully batched; skip
                    continue

        g["count"] += ap_count

    # ---------------------------------------------------------------------
    # Flush
    # ---------------------------------------------------------------------

    def _iter_row_chunks(self, rows, *, max_rows: int = MAX_PAYLOAD_ROWS, max_bytes: int = 0):
        if not rows: return
        if max_bytes > 0:
            start = 0
            n = len(rows)
            while start < n:
                size = 0
                end = start
                while (end < n) and (end - start < max_rows):
                    r = rows[end]
                    size += 50 + sum((len(str(k)) + len(str(v)) + 4) for k, v in r.items())
                    if size > max_bytes and end > start: break
                    end += 1
                yield rows[start:end]
                start = end
        else:
            for i in range(0, len(rows), max_rows):
                yield rows[i:i + max_rows]

    def _build_chunks(self, context_dict, action, rows, pk_cols, opts, based_on, action_seq, toasts=None):
        out = []
        for chunk in self._iter_row_chunks(rows):
            payloads = {action: [{"frame": _encode_sparse(chunk)}]}
            out.append(self._make_publish_dict(context_dict, payloads, opts, based_on, action_seq, toast=toasts))
        return out

    async def _flush(self, key: Tuple[str, str, bytes]):
        messages: List[Union[BroadcastMessage, Dict[str, Any]]] = []
        async with self._lock:
            messages = self._prepare_flush_locked(key)
        for msg in messages:
            await self._out_q.put(msg)

    def _prepare_flush_locked(self, key: Tuple[str, str, bytes]) -> List[Dict[str, Any]]:
        g = self._groups.pop(key, None)
        if not g or not (g["adds"] or g["updates"] or g["removes"]):
            return []

        context_dict = g["context"] or {}
        pk = g["pk"] or []
        opts = g["options"] or PayloadOptions().to_dict()
        based_on = int(g["based_on"] or -1)
        action_seq = int(g["action_seq"] or -1)
        toasts = g['toasts'] or {}

        add_rows, rem_rows, upd_rows = [], [], []

        for _, (row, _) in g["adds"].items():
            add_rows.append(row)
        for pk_t in g["removes"]:
            rem_rows.append(_pk_dict(pk_t, pk))
        for _, (row, _) in g["updates"].items():
            if all(k in row for k in pk):
                upd_rows.append(row)
            else:
                if pk:
                    full = {**_pk_dict(_to_pk_tuple(row, pk), pk), **row}
                    upd_rows.append(full)
                else:
                    upd_rows.append(row)

        out: List[Dict[str, Any]] = []
        if self.flatten:
            for action, rows in (("add", add_rows), ("remove", rem_rows), ("update", upd_rows)):
                if not rows:
                    continue
                for chunk in self._iter_row_chunks(rows):
                    payloads = {action: [{"frame": _encode_sparse(chunk)}]}
                    out.append(self._make_publish_dict(context_dict, payloads, opts, based_on, action_seq, toast=toasts))
            return out

        if add_rows:
            out.extend(self._build_chunks(context_dict, "add", add_rows, pk, opts, based_on, action_seq, toasts=toasts))
        if rem_rows:
            out.extend(self._build_chunks(context_dict, "remove", rem_rows, pk, opts, based_on, action_seq, toasts=toasts))
        if upd_rows:
            for _, group in self._cluster_updates(upd_rows, pk).items():
                out.extend(self._build_chunks(context_dict, "update", group, pk, opts, based_on, action_seq, toasts=toasts))
        return out


    # ---------------------------------------------------------------------
    # Builders
    # ---------------------------------------------------------------------

    def _make_publish_dict(self, context_dict: Dict[str, Any], payloads: Dict[str, Any], opts: Dict[str, Any], based_on: int, action_seq: int, toast: Optional[list | dict] = None):
        data = {
            "payloads": {
                **payloads,
                "based_on": based_on,
                "action_seq": action_seq,
            },
        }
        if toast:
            data["toast"] = toast
        return {
            "action": "publish",
            "context": context_dict,
            "options": opts,
            "data": data,
        }

    # ---------------------------------------------------------------------
    # Update clustering
    # ---------------------------------------------------------------------

    def _cluster_updates(self, rows: List[Dict[str, Any]], pk_cols: Union[str, List[str]]):
        """Group update rows by their changed-column signature, dedup by PK (last-wins).

        Uses a single-pass grouping (O(n)) instead of the O(n^3) iterative merge
        which also risked duplicating rows across buckets.
        """
        pk_list = [pk_cols] if isinstance(pk_cols, str) else list(pk_cols or [])
        buckets: Dict[frozenset, Dict[Tuple[Any, ...], Dict[str, Any]]] = defaultdict(dict)
        syn_counter = 0

        for r in rows:
            cols = frozenset(c for c in r.keys() if (not pk_list) or c not in pk_list)
            if pk_list:
                pk_t = tuple(r.get(c) for c in pk_list)
            else:
                syn_counter += 1
                pk_t = ("_syn", syn_counter)
            buckets[cols][pk_t] = r

            # Safety: if too many distinct column-sets, stop splitting further
            if len(buckets) > MAX_UPDATE_BUCKETS:
                break

        return {cols: list(by_pk.values()) for cols, by_pk in buckets.items()}

