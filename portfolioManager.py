
import asyncio

from app.services.storage.sqlManagerV2 import CadeSQL
from app.helpers.date_helpers import parse_date, next_biz_date
from app.helpers.type_helpers import ensure_list
from app.logs.logging import log
from typing import Optional, Sequence, Union, Tuple, Any
import polars as pl

try:
    import re2 as re
except ImportError:
    import re

# Implicit Non-Null checks
PROTECTED_COLUMNS = {
    "portfolio": {'quoteType'},
    'meta': {'state'}
}

MARKET_COLS = re.compile(r"^([a-zA-Z0-9]+)(Bid|Mid|Ask)(Px|Spd|Yld|Mmy|Dm|Ytm|Ytw|Ytc)$")

class PortfolioManager(CadeSQL):
    def __init__(self, db_path: str, driver="sqlite", auto_index=True, enable_cache=False):
        super().__init__(
            database=db_path,
            driver=driver,
            create_db=True,
            auto_connect=True,
            auto_index=auto_index,
            enable_cache=enable_cache,
            default_format = "polars"
        )

    async def market_columns(self, refresh=False, suppress_benchmarks=False):
        s = await self.query_schema('portfolio', refresh=refresh)
        r = set()
        for k in s.columns:
            if MARKET_COLS.search(k) and ((not suppress_benchmarks) or ("benchmark" not in k.lower())):
                r.add(k)
        return r

    async def drop_null_entries(self):
        keys = await self.select('portfolio', filters={'quoteType':None}, columns=['portfolioKey'], as_format="list", distinct=True)
        if keys:
            await log.sql(f'Removing {len(keys)} for lacking containing nulls')
            await self.remove_pt_by_keys(keys)
            from app.server import get_mm
            await get_mm().rebuild_all_caches()

    async def purge_bad_data(self):
        pt = await self.select('portfolio', columns='portfolioKey', as_format="set", distinct=True)
        m = await self.select('meta', columns='portfolioKey', as_format="set", distinct=True)
        b1 = list(pt.difference(m))
        if b1:
            await log.sql(f'Removing {len(b1)} for lacking meta')
            await self.remove_pt_by_keys(b1)
        b2 = list(m.difference(pt))
        if b2:
            await log.sql(f'Removing {len(b2)} for lacking pt')
            await self.remove_pt_by_keys(b2)
        await self.drop_null_entries()

    async def remove_pt_by_keys(self, keys):
        keys = [keys] if isinstance(keys, str) else list(keys)
        m1 = await self.select("meta", filters={'portfolioKey': keys})
        p1 = await self.select("portfolio", filters={'portfolioKey': keys})
        if not m1.is_empty(): await self.remove('meta', filters={'portfolioKey': keys})
        if not p1.is_empty(): await self.remove('portfolio', filters={'portfolioKey': keys})
        return m1.height + p1.height

    async def remove_pt_by_dates(self, dates):
        dates = [dates] if isinstance(dates, str) else dates
        keys = await self.select("meta", filters={'date': dates}, columns='portfolioKey', as_format='set')
        return await self.remove_pt_by_keys(keys)

    async def select_today(self, **kwargs):
        return await self.select_at_date(dt="T", **kwargs)

    async def select_yesterday(self, **kwargs):
        return await self.select_at_date(dt="T-1", **kwargs)

    async def select_at_date(self, dt, **kwargs):
        dt = ensure_list(dt)
        dts = [parse_date(d, return_format="%Y-%m-%d") for d in dt]
        filters = {**{'date': dts}, **kwargs.pop('filters', {})}
        return await self.select('meta', filters=filters, **kwargs)

    async def select_random_key(self, dt="T"):
        pts = await self.select_at_date(dt)
        if pts.is_empty(): return
        return pts.sample(n=1, shuffle=True).select('portfolioKey').item(0, 0)

    async def select_random_rfq(self, dates="T"):
        pts = await self.select_at_date(dates, columns=['rfqListId', 'datetime'], filters={'!rfqListId':None})
        if pts.hyper.is_empty():
            prev_date = next_biz_date(dates, -1)
            await log.notify(f'Date {dates} was empty, trying {prev_date}')
            return await self.select_random_rfq(dates=prev_date)
        pt = pts.hyper.sample(1)
        return pt.hyper.peek('rfqListId'), pt.hyper.peek('datetime')

    async def debug_unsync_meta(self, key):
        meta = await self.select_meta(key)
        meta = meta.with_columns(pl.lit(None).alias('syncedTime'), pl.lit('PENDING').alias('state'))
        await self.upsert('meta', meta)

    async def select_random_pt(self, dt=None, lazy=True, **kwargs):
        auto = False
        if dt is None: auto = True; dt = "T"
        key = await self.select_random_key(dt)
        if key is None:
            if auto:
                await log.notify("Nothing found for date T, trying T-1")
                pt = await self.select_random_pt(dt="T-1")
                return pt.lazy() if lazy else pt
            raise KeyError(f"No keys returned for date: {dt}")
        pt = await self.select('portfolio', filters={'portfolioKey': key}, **kwargs)
        return pt.lazy() if lazy else pt

    async def select_random_meta(self, dt="T", **kwargs):
        auto = False
        if dt is None: auto = True; dt = "T"
        key = await self.select_random_key(dt)
        if key is None:
            if auto:
                await log.notify("Nothing found for date T, trying T-1")
                return await self.select_random_meta(dt="T-1")
            raise KeyError(f"No keys returned for date: {dt}")
        return await self.select('meta', filters={'portfolioKey': key}, **kwargs)

    async def search_schema(self, table_name: str, stub: str = None, limit: int = 5, threshold: float = 0.6):
        if (stub is None) and (not (await self._table_exists(table_name))):
            stub = table_name
            table_name = 'portfolio'
        return await super().search_schema(table_name=table_name, stub=stub, limit=limit, threshold=threshold)

    async def select_meta(self, key, **kwargs):
        return await self.select("meta", filters={'portfolioKey': key}, **kwargs)

    async def select_pt(self, key, **kwargs):
        return await self.select("portfolio", filters={'portfolioKey': key}, **kwargs)

    async def base_pt(self, key, bond_id='isin'):
        return await self.select("portfolio", filters={'portfolioKey': key}, columns=['tnum', 'portfolioKey', bond_id])

    async def override_pt(self, key, df, bond_id='isin'):
        base = await self.base_pt(key, bond_id)
        return await self.upsert('portfolio', df.join(base, on=bond_id, how='left'))

    async def fake_pt_with_bond(self, isin, fake=False, n=1, **kwargs):
        res = None
        if not fake:
            t = await self.select('portfolio', where={'isin': isin}, limit=1)
            if t.hyper.is_empty():
                res = t
        if res is None:
            await log.warning('BOND NOT FOUND - please pass static if needed.')
            t = (await self.select_random_pt(lazy=False)).head(n).with_columns(pl.lit(isin, pl.String).alias('isin'))
        exprs = []
        for k, v in kwargs.items():
            exprs.append(pl.lit(v).alias(k))
        return t.with_columns(exprs)
    
    async def replace_from_clipboard(self, portfolioKey=None, columns=None):
        clip_data = pl.read_clipboard()
        clip_schema = set(clip_data.hyper.schema().keys())
        
        if portfolioKey is None:
            await log.please_enter_portfolio_key("")
            portfolioKey = input("")
            
        if (not portfolioKey) or (portfolioKey == ''): 
            raise Exception("portfolioKey is required.")
        
        my_pt = await self.select_pt(portfolioKey)
        if my_pt.hyper.is_empty():
            raise ValueError(f"PortfolioKey is unknown in database: {portfolioKey}")
        
        if not columns:
        
            db_isin_col = 'isin'
            clip_isin_col = {'ISIN', 'isin', 'Isin'}.intersection(clip_schema)
            if clip_isin_col: 
                clip_isin_col = list(clip_isin_col)[0]
                columns[db_isin_col] = clip_isin_col

            db_cusip_col = 'cusip'
            clip_cusip_col = {'CUSIP', 'cusip', 'Cusip'}.intersection(clip_schema)
            if clip_cusip_col:
                clip_cusip_col = list(clip_cusip_col)[0]
                columns[db_cusip_col] = clip_cusip_col
    
            db_qt_col = 'quoteType'
            clip_qt_col = {'QuoteType', 'QT','quotetype', 'quoteType', 'QUOTE_TYPE', 'QUOTETYPE'}.intersection(clip_schema)
            if clip_qt_col: 
                clip_qt_col = list(clip_qt_col)[0]
                columns[db_qt_col] = clip_qt_col
    
            db_quote_col = 'newLevel'
            clip_quote_col = {'Quote', 'quote', 'newLevel', 'newlevel', 'NEWLEVEL', 'QUOTE', 'level', 'Level'}.intersection(clip_schema)
            if clip_quote_col: 
                clip_quote_col = list(clip_quote_col)[0]
                columns[db_quote_col] = clip_quote_col

            db_tnum_col = 'tnum'
            clip_tnum_col = {'tnum', 'TNUM', 'index', 'INDEX', 'idx', 'IDX', 'uidx', 'UIDX'}.intersection(clip_schema)
            if clip_tnum_col: 
                clip_tnum_col = list(clip_tnum_col)[0]
                columns[db_tnum_col] = clip_tnum_col
        
        if (columns.get('tnum') is None) and (columns.get('isin') is None):
            pass
            
        
        

    # ----- UTILITIES -----
    async def rfq_to_key(self, rfqListId, force_storage=False):
        if not force_storage:
            from app.services.loaders.kdb_queries_dev import generate_portfolio_key
            return generate_portfolio_key(rfqListId)
        return await self.select("meta", filters={'rfqListId': rfqListId}, columns=['portfolioKey'], as_format='item')

    async def key_to_rfq(self, portfolio_key):
        return await self.select("meta", filters={"portfolioKey":portfolio_key}, columns="rfqListId", as_format="item")

if __name__ == "__main__2":
    # Example bootstrap; replace with your app config if desired
    pass

    # async with PortfolioManager(r'Y:\NYK\HG Trading\Zawacki\Repo\PACT\Systematic Tools\_PT\portfolioTool.db') as db:
    #     await db.remove_pt_by_keys(['8250d1bb2fc3e755a4e40a988499c540'])


