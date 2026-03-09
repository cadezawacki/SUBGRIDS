
import asyncio
import atexit
import threading
from typing import Optional, Callable, Any, Union

from app.helpers.pandas_helpers import pd
import polars as pl
from app.helpers.polars_hyper_plugin import *

from app.helpers.string_helpers import clean_camel
from app.services.kdb.kdbManager import KDBClient
from app.services.kdb.kdb import extract_table_name
from app.helpers.taskContext import TaskContext
from app.services.portfolio.kdb_sync_v3 import KdbSyncManager
from app.helpers.asyncThreadExecutor import AsyncThreadExecutor
import uuid
from app.logs.logging import log, quick_log, big_log
import weakref
import signal
import sys, time
from uuid import uuid4
from app.helpers.loop_helpers import set_uvloop
from app.helpers.regex_helpers import hyper_match


# ----- Tunables --------
START_TIMEOUT = 10.0
SHUTDOWN_TIMEOUT = 10.0
JOIN_TIMEOUT = 2.0
EXECUTOR_MAX_WORKERS = 50
QUERY_LOG_LENGTH_CAP = 5000 # safeguard
#---------------------------

STRING_TYPES = {pl.String, pl.Categorical}

TRUTHY_STRINGS = {
    "true", "t", "yes", "y", "1", "+1",
    "on", "enabled", "enable", "active", "valid",
    "✓", "✔", "✅", "+", "＋"
}

FALSY_STRINGS = {
    "false", "f", "no", "n", "nope", "negative", "0", "00", "000", "-1",
    "off", "disabled", "disable", "inactive", "invalid", "x",
    "✗", "✘", "❌", "x", "×",
    "-", "−", "none", "null", "nil", "nan", "na", "n/a", "not applicable", "",
    "undef", "undefined", "no data", "no_data", "blank"
}

# String match -----------------------------


def _normalize_expr(col: pl.Expr) -> pl.Expr:
    return (
        col
        .cast(pl.String, strict=False)
        .str.to_lowercase()
        .str.strip_chars()
    )

def is_boolean_like_string(
        col: str,
        *,
        threshold: float = 0.95,
        present: float = 0.5,
) -> pl.Expr:
    s = _normalize_expr(pl.col(col))

    is_truthy = s.is_in(TRUTHY_STRINGS)
    is_falsy = s.is_in(FALSY_STRINGS)

    recognized = is_truthy | is_falsy

    non_null_n = pl.col(col).is_not_null().sum()
    recognized_n = recognized.sum()
    total_n = pl.len()

    present_ratio = non_null_n / total_n
    recognized_ratio = recognized_n / non_null_n

    return (present_ratio >= present) & (recognized_ratio >= threshold)




def _id():
    return str(uuid4())[:5]

def _kdb_derive_pool_name(host: str, port: int, base_name: str, table_name: str | None):
    if host and str(host).startswith("kdb-gateway") and table_name:
        safe_table = str(table_name)
        if len(safe_table) > 128:
            safe_table = safe_table[:128]
        if base_name.endswith(f":{safe_table}"):
            return base_name
        return f"{base_name}:{safe_table}"
    return base_name


def _is_list_dtype(dtype) -> bool:
    # Polars versions differ slightly; keep this robust and cheap (column-level only).
    try:
        return isinstance(dtype, pl.List)
    except Exception:
        return str(dtype).lower().startswith("list")

class ThreadedServer(object):
    _instances = weakref.WeakSet()
    _registered = False

    def __init__(
            self,
            onStartFunc: Optional[Callable] = None,
            onStopFunc: Optional[Callable] = None,
            *,
            name: Optional[str] = None,
            auto_start: bool = True,
            loop: Any = None
    ):
        self.server = None
        self.name = name or f"Server-{uuid.uuid4().hex[:5]}"
        self._server_loop: Optional[asyncio.AbstractEventLoop] = None
        self._server_thread: Optional[threading.Thread] = None
        self._should_exit_event: Optional[asyncio.Event] = None
        self._is_serving_event = threading.Event()
        self._shutdown_complete = threading.Event()
        self.auto_start = auto_start
        self._blocking_executor = AsyncThreadExecutor(max_workers=EXECUTOR_MAX_WORKERS, name=f"{self.name}-executor")
        self._shutdown_timeout = SHUTDOWN_TIMEOUT
        self._start_timeout = START_TIMEOUT
        self._is_stopping = threading.Lock()
        self._stop_called = False

        ThreadedServer._instances.add(self)
        if not ThreadedServer._registered:
            ThreadedServer._registered = True
            # Register class-wide shutdown to avoid capturing any instance strongly
            atexit.register(ThreadedServer.shutdown_all)

        self.onStartFunc = onStartFunc
        self.onStopFunc = onStopFunc
        log.register(item=self.name, color='cyan')

    @classmethod
    def shutdown_all(cls):
        instances = list(cls._instances)
        for instance in instances:
            try:
                instance.stop()
            except Exception as e:
                log.error(f"Error shutting down {instance.name}: {e}")

    def _run_server_loop(self):
        try:
            self._server_loop = set_uvloop()
            asyncio.set_event_loop(self._server_loop)
            self._should_exit_event = asyncio.Event()
            log.info(f"[{self.name}] Event loop created and set.")
            self._server_loop.run_until_complete(self._start_server_coroutine())
            log.app(f"[{self.name}] Server initialized and serving.")
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Loop task cancelled.")
        except Exception as e:
            log.error(f"[{self.name}] Unexpected error in loop: {e}")
        finally:
            self._cleanup_loop()

    def _cleanup_loop(self):
        try:
            loop = self._server_loop
            if loop and not loop.is_closed():
                async def _cancel_all():
                    # Phase 1: cancel everything and gather
                    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
                    if tasks:
                        for t in tasks:
                            if not t.done(): t.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                    # Phase 2: flush any callbacks spawned by cancellations
                    await asyncio.sleep(0)
                    tasks2 = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
                    if tasks2:
                        for t in tasks2:
                            if not t.done(): t.cancel()
                        await asyncio.gather(*tasks2, return_exceptions=True)

                loop.run_until_complete(_cancel_all())
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
        except BaseException as e:
            log.error(f"[{self.name}] Error during loop cleanup: {e}")
        finally:
            try:
                self._blocking_executor.shutdown(cancel_pending=True)
            except Exception as e:
                log.error(e)
            self._shutdown_complete.set()

    async def onStart(self):
        if not self.onStartFunc:
            return
        try:
            fut = self._blocking_executor.submit(self.onStartFunc)
            res = await asyncio.wrap_future(fut)
            if asyncio.iscoroutine(res):
                res = await res
            return res
        except Exception as e:
            log.error(f"[{self.name}] Error in onStart: {e}")
            raise

    async def onStop(self):
        if not self.onStopFunc:
            return
        try:
            fut = self._blocking_executor.submit(self.onStopFunc)
            res = await asyncio.wrap_future(fut)
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            log.error(f"[{self.name}] Error in onStop: {e}")

    async def _start_server_coroutine(self):
        try:
            self.server = await self.onStart()
            self._is_serving_event.set()
            await self._should_exit_event.wait()
        except asyncio.CancelledError:
            await log.app(f"[{self.name}] Server coroutine cancelled.")
        except Exception as e:
            await log.error(f"[{self.name}] Error in server coroutine: {e}")
        finally:
            await self._stop_server_coroutine()

    async def _stop_server_coroutine(self):
        try:
            await log.app(f"[{self.name}] Stopping server...")
            self._is_serving_event.clear()
            await self.onStop()
            await log.app(f"[{self.name}] Server shutdown complete.")
        except Exception as e:
            log.error(f"[{self.name}] Error during server shutdown: {e}")

    def start(self):
        with self._is_stopping:
            if self._stop_called: return
            if self._server_thread and self._server_thread.is_alive(): return

            # Recreate executor if it was shut down in a previous stop().
            try:
                if self._blocking_executor is None or getattr(self._blocking_executor, "_shutdown", False):
                    self._blocking_executor = AsyncThreadExecutor(
                        max_workers=EXECUTOR_MAX_WORKERS,
                        name=f"{self.name}-executor"
                    )
            except Exception:
                # If executor recreation fails, fail fast rather than starting a broken server loop.
                raise

            self._blocking_executor.start()
            self._is_serving_event.clear()
            self._shutdown_complete.clear()

            log.info(f"starting....{self.name}-thread")
            if self._server_thread is None:
                self._server_thread = threading.Thread(
                    target=self._run_server_loop,
                    daemon=True,
                    name=f"{self.name}-thread"
                )
            if not self._server_thread.is_alive():
                self._server_thread.start()

        if self._is_serving_event.wait(timeout=self._start_timeout):
            log.success(f"[{self.name}] KDB server started and is serving.")
        else:
            self.stop()

    def stop(self):
        with self._is_stopping:
            if self._stop_called: return
            self._stop_called = True
        try:
            if not self._server_thread or not self._server_thread.is_alive():
                return
            if self._server_loop and not self._server_loop.is_closed() and self._should_exit_event:
                try:
                    self._server_loop.call_soon_threadsafe(self._should_exit_event.set)
                except RuntimeError as e:
                    log.warning(f"[{self.name}] Could not signal shutdown: {e}")
            if self._shutdown_complete.wait(timeout=self._shutdown_timeout):
                log.success(f"[{self.name}] Server shutdown gracefully.")
            else:
                log.warning(f"[{self.name}] Server shutdown timeout.")
            if self._server_thread and self._server_thread.is_alive():
                if threading.current_thread() is not self._server_thread:
                    self._server_thread.join(timeout=JOIN_TIMEOUT)
                    if self._server_thread.is_alive():
                        log.warning(f"[{self.name}] Server thread did not terminate within timeout.")
        except Exception as e:
            log.error(f"[{self.name}] Error during stop: {e}")
        finally:
            self._server_thread = None
            self.server = None
            self._server_loop = None
            self._should_exit_event = None
            self._is_serving_event.clear()
            with self._is_stopping:
                self._stop_called = False
            log.success(f"[{self.name}] Server instance stopped.")

    def is_running(self):
        return bool(
            self._server_thread and
            self._server_thread.is_alive() and
            self._is_serving_event.is_set() and
            not self._stop_called
        )

def _signal_handler(signum, frame):
    log.app(f"Received signal {signum}, shutting down all servers...")
    ThreadedServer.shutdown_all()
    sys.exit(0)

def _maybe_register_signal_handlers():
    if threading.current_thread() is threading.main_thread():
        for _sig in (signal.SIGINT, signal.SIGTERM):
            try:
                current = signal.getsignal(_sig)
                if current in (signal.SIG_DFL, None):
                    signal.signal(_sig, _signal_handler)
            except Exception:
                pass

_maybe_register_signal_handlers()


class KDBServer(ThreadedServer):
    def __init__(self, kdb: Optional[KDBClient] = None, auto_start: bool = True, loop: Any = None,
                 maintenance_interval: int = 20):
        self.kdb = kdb if kdb is not None else KDBClient(maintenance_interval=maintenance_interval)
        self.ctx = TaskContext()
        on_start_func = self.kdb.initialize
        on_stop_func = self.kdb.shutdown
        super().__init__(name="KDBServer", onStartFunc=on_start_func, onStopFunc=on_stop_func, auto_start=auto_start, loop=loop)

    async def stats(self):
        return await self.kdb.get_pool_stats()

    async def _wrap_execution(self, cour):
        if not self._server_loop or not self._server_loop.is_running():
            raise RuntimeError("Server's asyncio loop is not running.")

        cfut = asyncio.run_coroutine_threadsafe(cour, self._server_loop)
        afut = asyncio.wrap_future(cfut)

        try:
            return await afut
        except asyncio.CancelledError:
            KDBServer._cancel(afut)
            KDBServer._cancel(cfut)
            raise

    @staticmethod
    def _cancel(f):
        try:
            # Only cancel if the future is still pending.
            # Original: `not f.cancelled() or f.done()` always evaluates True
            # when f.done() is True (including successful completion), which
            # would attempt to cancel already-finished futures.
            if not f.done():
                f.cancel()
            return True
        except Exception:
            return False

    async def execute_query(
            self,
            host: str,
            port: int,
            q: str,
            usr: Optional[str] = None,
            pwd: Optional[str] = None,
            name: Optional[str] = None,
            timeout: float = 10,
            pool_settings: Optional[dict] = None,
            pooled: bool = True,
            cast: bool = True,
            cast_bool_strings: bool = True,
            clean_columns: bool = True,
            coalesce_cleaned_columns: bool = True,
            allow_objects: bool = True,
            coerce_nulls: bool = True,
            convert_null: bool = True,
            convert_bools: bool = True,
            categorical_symbols: bool = False,
            lazy: bool = True,
            null_list=('', 'na', 'nan', 'null', 'none'),
            suppress_log = False,
            **kwargs
    ):

        if not self.is_running():
            if self.auto_start:
                try:
                    self.start()
                    if not self._is_serving_event.wait(timeout=self._start_timeout):
                        raise RuntimeError("KDBServer failed to start in time.")
                except Exception as e:
                    raise RuntimeError(f"KDBServer failed to start in time: {e}")
            else:
                raise RuntimeError("KDBServer is not running, auto_start is False")

        start = time.monotonic()
        pool_settings_local = dict(pool_settings or {})
        pool_settings_local['pooled'] = pooled

        error = None
        table_name = extract_table_name(q)
        base_name = str(name or f"{host}:{int(port)}")
        effective_name = _kdb_derive_pool_name(host, int(port), base_name, table_name)

        try:
            cour = self.kdb.execute_query(
                host=host,
                port=port,
                query=q,
                usr=usr,
                pwd=pwd,
                name=effective_name,
                timeout=timeout,
                allow_objects=allow_objects,
                pool_settings=pool_settings_local,
                **kwargs
            )
            result = await asyncio.wrap_future(self._blocking_executor.submit(cour))
            # result = await self._wrap_execution(cour)

            if isinstance(result, pd.DataFrame):
                result = await asyncio.to_thread(pl.from_pandas, result)
            if not isinstance(result, (pl.DataFrame, pl.LazyFrame)): return result

            schema = result.hyper.schema()
            if not (cast or clean_columns or coerce_nulls or convert_null or cast_bool_strings):
                result_lazy = result
            else:
                result_lazy = result.lazy() if isinstance(result, pl.DataFrame) else result

            if cast or convert_null or cast_bool_strings:
                recasts = []
                for col, dtype in schema.items():
                    if cast and (dtype == pl.Datetime):
                        if dtype.time_zone is None:
                            recasts.append(pl.col(col).cast(pl.Datetime, strict=False).dt.replace_time_zone("UTC", non_existent='null', ambiguous="earliest").alias(col))
                    elif cast and (dtype == pl.Duration):
                        recasts.append(pl.col(col).cast(pl.Duration, strict=False).dt.total_nanoseconds().alias(col))
                    elif cast and (dtype == pl.Unknown):
                        recasts.append(pl.col(col).cast(pl.String, strict=False).alias(col))
                    elif cast and (not allow_objects) and (dtype == pl.Object):
                        recasts.append(pl.col(col).cast(pl.String, strict=False).alias(col))
                    elif cast and (not allow_objects) and (dtype == pl.List):
                        inner = getattr(dtype, "inner", None)
                        if inner == pl.String:
                            recasts.append(pl.col(col).list.join(",").alias(col))
                        else:
                            recasts.append(
                                pl.col(col)
                                .list.eval(pl.element().cast(pl.String))
                                .list.join(",")
                                .alias(col)
                            )
                    elif cast and convert_bools and (dtype == pl.Boolean):
                        recasts.append(pl.col(col).cast(pl.Int8, strict=False).alias(col))
                    elif convert_null and (dtype==pl.Null):
                        recasts.append(pl.col(col).cast(pl.String, strict=False).alias(col))
                    elif cast_bool_strings and (dtype in STRING_TYPES) and (hyper_match('^[iI]{1}s[A-Z0-9]', col, case_sensitive=True)):
                        recasts.append(
                            pl.col(col).cast(pl.String, strict=False).str.to_lowercase().is_in(TRUTHY_STRINGS).cast(pl.Int8, strict=False).alias(col)
                        )

                if recasts:
                    result_lazy = (await result_lazy.with_columns(recasts).collect_async()).lazy()
                schema = result_lazy.hyper.schema()

            exprs = []
            if coerce_nulls and null_list:
                null_set = set(null_list)
                for col, dtype in schema.items():
                    if dtype in STRING_TYPES:
                        new_dtype = pl.String if not categorical_symbols else dtype
                        exprs.append(
                            pl.when(
                                pl.col(col).cast(pl.String, strict=False)
                                .str.strip_chars()
                                .str.to_lowercase()
                                .is_in(null_set)
                            ).then(pl.lit(None, dtype=new_dtype)).otherwise(
                                pl.col(col)
                            ).cast(new_dtype, strict=False).alias(col)
                        )
            elif not categorical_symbols:
                for col, dtype in schema.items():
                    if dtype == pl.Categorical:
                        exprs.append(
                            pl.col(col).cast(pl.String, strict=False).alias(col)
                        )
            if exprs:
                result_lazy = result_lazy.with_columns(exprs)

            if clean_columns:
                col_map = {}
                if coalesce_cleaned_columns:
                    coalesce_cols = {}
                    coalesce_exprs = []
                    drop_cols = []
                    for c in schema.keys():
                        new_col = clean_camel(c)
                        if new_col in coalesce_cols:
                            coalesce_cols[new_col].append(c)
                        else:
                            coalesce_cols[new_col] = [c]
                    for new_col, overlap_cols in coalesce_cols.items():
                        if len(overlap_cols) == 1:
                            col_map[overlap_cols[0]] = new_col
                        else:
                            drop_cols.extend([d for d in overlap_cols if d!=new_col])
                            coalesce_exprs.append(pl.coalesce([
                                pl.col(o) for o in overlap_cols
                            ]).alias(new_col))

                    if coalesce_exprs:
                        result_lazy = (await result_lazy.with_columns(coalesce_exprs).drop(drop_cols, strict=False).collect_async()).lazy()

                else:
                    used = set()
                    for c in schema.keys():
                        new_col = clean_camel(c)
                        i = 1
                        new_col_candidate = new_col
                        while new_col in used:
                            i += 1
                            new_col = f"{new_col_candidate}_{i}"
                        col_map[c] = new_col
                        used.add(new_col)

                if col_map:
                    result_lazy = result_lazy.rename(col_map, strict=False)

            if cast or clean_columns or coerce_nulls or convert_null:
                result_lazy = await result_lazy.collect_async() if isinstance(result_lazy, pl.LazyFrame) else result_lazy

            return result_lazy.lazy() if lazy else result_lazy

        except asyncio.CancelledError:
            raise # Dont log this
        except Exception as e:
            error = e
            raise
        finally:
            if not suppress_log:
                duration = time.monotonic() - start
                trace = kwargs.pop('trace_id', _id())
                basic_log_details = {
                    "trace": trace,
                    "state": "ERROR" if error else "success",
                    'server': f"{host}:{port}",
                    "pool": effective_name,
                    "table": table_name,
                    "duration_ms": round(duration * 1000, 2),
                    "timeout": timeout
                }
                extended_log_details = {
                    'pool_settings': pool_settings_local,
                    'q': q[:QUERY_LOG_LENGTH_CAP] # capped
                }

                if error is not None:
                    basic_log_details['error'] = str(error)
                self.ctx.spawn(quick_log(**basic_log_details))
                self.ctx.spawn(big_log(**{**basic_log_details, **extended_log_details}))


class SyncServer(ThreadedServer):
    def __init__(self, auto_start: bool = True, loop: Any = None, maintenance_interval: int = 10):
        self.ksm = KdbSyncManager(sync_interval=maintenance_interval)
        self.ctx = TaskContext()
        on_start_func = self.ksm.initialize
        on_stop_func = self.ksm.close
        super().__init__(
            name="SyncServer",
            onStartFunc=on_start_func,
            onStopFunc=on_stop_func,
            auto_start=auto_start,
            loop=loop
        )

    async def full_sync_with_kdb(self):
        if not self.is_running():
            if self.auto_start:
                self.start()
                if not self._is_serving_event.wait(timeout=self._start_timeout):
                    raise RuntimeError("SyncServer failed to start in time.")
            else:
                raise RuntimeError("SyncServer is not running.")

        if not self._server_loop or not self._server_loop.is_running():
            raise RuntimeError("Server's asyncio loop is not running.")

        async def _op():
            if not self.ksm.is_syncing:
                await self.ksm.full_sync_with_kdb()

        fut = asyncio.run_coroutine_threadsafe(_op(), self._server_loop)
        await asyncio.wrap_future(fut)

    async def sync_n_with_kdb(self, n: int = 1, suppressLoad=False, suppressCompare=False, dates=None):
        if not self.is_running():
            if self.auto_start:
                self.start()
                if not self._is_serving_event.wait(timeout=self._start_timeout):
                    raise RuntimeError("SyncServer failed to start in time.")
            else:
                raise RuntimeError("SyncServer is not running.")

        if not self._server_loop or not self._server_loop.is_running():
            raise RuntimeError("Server's asyncio loop is not running.")

        async def _op():
            if not self.ksm.is_syncing:
                await self.ksm.sync_n_with_kdb(n, suppressLoad, suppressCompare, dates)

        fut = asyncio.run_coroutine_threadsafe(_op(), self._server_loop)
        await asyncio.wrap_future(fut)

    async def refresh_quotes(self, portfolio_key):
        if not self.is_running():
            if self.auto_start:
                self.start()
                if not self._is_serving_event.wait(timeout=self._start_timeout):
                    raise RuntimeError("SyncServer failed to start in time.")
            else:
                raise RuntimeError("SyncServer is not running.")

        if not self._server_loop or not self._server_loop.is_running():
            raise RuntimeError("Server's asyncio loop is not running.")

        async def _op():
            if not self.ksm.is_refreshing:
                await self.ksm.refresh_quotes(portfolio_key)
            else:
                await log.error("Already refreshing")

        fut = asyncio.run_coroutine_threadsafe(_op(), self._server_loop)
        await asyncio.wrap_future(fut)
