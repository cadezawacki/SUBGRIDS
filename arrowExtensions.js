

import Fuse from "fuse.js";
import {asArray, hash64Any, capitalize, measureText, asyncZipArray} from '@/utils/helpers.js';
import {NumberFormatter} from '@/utils/NumberFormatter.js';
import * as recursiveMerge from 'deepmerge';
import {BiMap} from "@/utils/bidirectionalMap.js";
// import {coerceToNumber, roundTo, coerceToBool} from "@/utils/NumberFormatter.js";
import {coerceToNumber, coerceToBool, formatNumber, CLEAR_SENTINEL, coerceToDate} from '@/utils/typeHelpers.js';
import {LRUCache} from 'mnemonist';
import memoize from 'fast-memoize';
import {FlagCellRenderer} from "@/grids/js/flagCellRenderer.js";
import {_inferSigFigs} from "@/pt/js/grids/portfolio/portfolioColumns.js";
import * as arrow from "apache-arrow";

window.coerceToDate = coerceToDate
function _toUint32(n) { return n >>> 0; }
function _toInt32(n) { return n | 0; }


const FILTER_TYPES = {
    TEXT: 'text',
    NUMBER: 'number',
    DATE: 'date',
    SET: 'set',
    SELECT: 'select',
}

function nullishFilter(x) {
    return x == null || x === '' || x === '(Blanks)'
}

export class GlobalSearchManager {
    constructor(adapter, { scanThreshold = 50_000, fuseMinLen = 2, useProjection=false, useCache=true} = {}) {
        this.adapter = adapter;
        this.engine = adapter.engine;
        this.scanThreshold = scanThreshold;
        this.fuseMinLen = fuseMinLen;
        this.useProjection = useProjection;

        this.useCache = useCache;
        this._cacheKey = null;
        this._cacheHits = null; // Int32Array of matching source row indices
        this._colsUsed = new Set();
        this._cache = new LRUCache(16);

        // invalidate cache on epoch-change touching used columns or rows
        this._off = this.engine.onEpochChange?.((p) => {
            if (!this._cacheKey) return;
            if (p?.global) {
                this._cacheKey = null;
                this._cacheHits = null;
                this._colsUsed.clear();
                this._cache.clear();
                return;
            }
            if (p?.colsChanged === true || p?.rowsChanged) {
                this._cacheKey = null;
                this._cacheHits = null;
                this._colsUsed.clear();
                this._cache.clear();
                return;
            }
            if (Array.isArray(p?.colsChanged) && this._colsUsed) {
                for (let i = 0; i < p.colsChanged.length; i++) {
                    if (this._colsUsed.has(p.colsChanged[i])) {
                        this._cacheKey = null;
                        this._cacheHits = null;
                        this._colsUsed.clear();
                        this._cache.clear();
                        return;
                    }
                }
            }
        }) || (() => {});
    }

    clearCache() {
        this._cacheKey = null;
        this._cacheHits = null;
        this._colsUsed.clear();
        this._cache.clear();
    }

    getSearch() {return this.adapter.context.page.quickSearch$.get('quickSearch') || ''}
    setSearch(q) {this.adapter.context.page.page$.set('quickSearch', q || '')}
    clearSearch() {this.adapter.context.page.page$.set('quickSearch', '')}

    getProjections() {
        return this.adapter._projection || this.engine._normalizeColumnSelector(null);
    }

    getAllColumns() {
        const phys = this.engine._normalizeColumnSelector(null);
        const der = this.engine.listDerivedColumns();
        const seen = new Set(phys);
        for (let i = 0; i < der.length; i++) if (!seen.has(der[i])) phys.push(der[i]);
        return phys;
    }

    getSearchableColumns(){
        return Array.from(this.adapter.columnRegistry.columns.values())
                     .filter(colDef=>colDef?.context?.isSearchable ?? false)
                     .map(col=>col.field)
    }

    getVisibleColumns() {
        const api = this.adapter?.api;
        try {
            const cols = (typeof api.getAllDisplayedColumns === 'function')
                ? api.getAllDisplayedColumns().map(c => c.getColId?.() || c.colId || c.colDef?.field).filter(Boolean)
                : (api?.columnModel?.getAllDisplayedColumns?.() || []).map(c => c.colDef?.field).filter(Boolean);
            return cols;
        } catch { return []; }
    }

    colsForSearch({includeAll=false, includeProjection=false, includeSearchable=true, includeVisible=true}={}) {
        const cols = new Set();
        if (includeAll) return this.getAllColumns();
        if (includeProjection) this.getProjections().forEach(col=>cols.add(col));
        if (includeSearchable) this.getSearchableColumns().forEach(col=>cols.add(col));
        if (includeVisible) this.getVisibleColumns().forEach(col=>cols.add(col));
        return Array.from(cols);
    }

    dispose() {
        try { this._off?.(); } catch {}
        this.clearCache();
        this._cache = null;
        this.adapter = null;
        this.engine = null;
    }

    // returns Int32Array of source indices passing the search
    filter(baseIndex, allColumns, query) {
        const q = String(query ?? '').trim();
        if (!q) return baseIndex;

        const eng = this.engine;
        const depCols = eng.getDependenciesClosure ? eng.getDependenciesClosure(allColumns) : allColumns;
        const colSig = eng.hashDepsFor ? eng.hashDepsFor(depCols) : 0n;
        const rowSig = eng.hashRows ? eng.hashRows(baseIndex) : 0n;

        const key = `${q}::${rowSig.toString(36)}::${colSig.toString(36)}`;
        // if (this._cache.has(key)) return this._cache.get(key);
        // if (this._cacheKey === key && this._cacheHits) return this._cacheHits;

        // Track columns used for invalidation
        this._colsUsed = new Set([ ...(this._colsUsed || []), ...depCols ]);

        // Prebind value getters once for the scan path
        const getters = new Array(allColumns.length);
        for (let i = 0; i < allColumns.length; i++) getters[i] = eng._getValueGetter(allColumns[i]);

        const n = baseIndex.length | 0;
        if (n <= this.scanThreshold || q.length < this.fuseMinLen) {
            const qi = q.toLowerCase();
            const hits = new Int32Array(n);
            let m = 0;
            for (let i = 0; i < n; i++) {
                const ri = baseIndex[i] | 0;
                if (this._rowContains(ri, getters, qi)) hits[m++] = ri;
            }
            const out = hits.subarray(0, m);
            // this._cacheKey = key; this._cacheHits = out; this._cache.set(key, out);
            return out;
        }

        // Large set → Fuse; build corpus once
        const { corpus, map } = this._buildCorpus(baseIndex, allColumns);
        const fuse = new Fuse(corpus, {
            includeScore: false, threshold: 0.35, ignoreLocation: true,
            minMatchCharLength: Math.min(3, q.length), useExtendedSearch: false
        });
        const res = fuse.search(q);
        const hits = new Int32Array(res.length);
        for (let i = 0; i < res.length; i++) hits[i] = map[res[i].refIndex | 0] | 0;
        // this._cacheKey = key; this._cacheHits = hits;
        return hits;
    }

    _rowContains(ri, getters, qi) {
        for (let c = 0; c < getters.length; c++) {
            const v = getters[c](ri);
            if (v == null) continue;
            if (typeof v === 'number') {
                // numeric contains only if query numeric-like; avoid toLowerCase cost
                if (!Number.isNaN(+qi) && String(v).indexOf(qi) !== -1) return true;
                continue;
            }
            if (String(v).toLowerCase().indexOf(qi) !== -1) return true;
        }
        return false;
    }

    _buildCorpus(baseIndex, cols) {
        const eng = this.engine;
        const n = baseIndex.length | 0;
        const map = new Int32Array(n);
        const corpus = new Array(n);

        for (let i = 0; i < n; i++) {
            const ri = baseIndex[i]|0;
            map[i] = ri;
            let s = '';
            for (let c = 0; c < cols.length; c++) {
                const v = eng.getCell(ri, cols[c]);
                if (v == null) continue;
                if (s) s += ' ';
                s += typeof v === 'string' ? v : String(v);
            }
            corpus[i] = s;
        }
        return { corpus, map };
    }
}

export class RecomputeBatcher {
    constructor({ debounceMs = 40, maxDelayMs = 120, useRaf = true } = {}) {
        this.debounceMs = debounceMs;
        this.maxDelayMs = maxDelayMs;
        this.useRaf = useRaf;

        this._tDebounce = 0;
        this._tMax = 0;
        this._scheduled = false;
        this._inFlight = false;
        this._rerun = false;
        this._lastRequestTs = 0;

        this._cb = null;
        this._boundRun = this._run.bind(this);
    }

    schedule(cb) {
        this._cb = cb;
        this._lastRequestTs = performance.now();
        if (this._inFlight) { this._rerun = true; return; }
        if (this._scheduled) return;

        this._scheduled = true;
        this._armDebounce();
        this._armMax();
    }

    cancel() {
        this._scheduled = false;
        if (this._tDebounce) { clearTimeout(this._tDebounce); this._tDebounce = 0; }
        if (this._tMax) { clearTimeout(this._tMax); this._tMax = 0; }
        this._rerun = false;
    }

    _armDebounce() {
        if (this._tDebounce) clearTimeout(this._tDebounce);
        this._tDebounce = setTimeout(() => this._trigger(), this.debounceMs);
    }

    _armMax() {
        if (this._tMax) return;
        this._tMax = setTimeout(() => this._trigger(), this.maxDelayMs);
    }

    _trigger() {
        this._tDebounce = 0;
        this._tMax = 0;
        if (this.useRaf && typeof requestAnimationFrame === 'function') {
            requestAnimationFrame(this._boundRun);
        } else {
            this._run();
        }
    }

    async _run() {
        if (!this._scheduled || this._inFlight) return;
        this._scheduled = false;
        this._inFlight = true;
        try {
            if (typeof this._cb === 'function') await this._cb();
        } finally {
            this._inFlight = false;
            if (this._rerun) {
                this._rerun = false;
                this.schedule(this._cb);
            }
        }
    }
}

function _extractId(params, guess=false, idxProperty='__srcIndex', pivotIdxProperty='__pid') {
    const data = params?.data ?? params;
    if (!data || typeof data !== 'object') return null;
    let id = data?.[idxProperty] ?? data?.[pivotIdxProperty];
    if (id == null && guess) id = data?.[Object.keys(data).filter(x=>x.startsWith('__'))[0]];
    return id != null ? id : null;
}

function mapFromArrowType(typeId) {
    // Arrow Type enum values
    switch(typeId) {
        case arrow.Type.Int: return "integer"; // or Int64, Int16, etc.
        case arrow.Type.Float: return "float"; // or Float32
        case arrow.Type.Utf8: return "text";
        case arrow.Type.Bool: return "boolean";
        case arrow.Type.Date: return "date"
        case arrow.Type.Null: return "text";
        default: return "text"
    }
}

export class ArrowAgGridFilter {
    constructor(adapter, defaultOpts={}) {
        this.adapter = adapter;
        this.engine = adapter?.engine;
        if (this.engine) {
            this.fields = new Set(this.engine.fields());
        }
        this._unifiedFilterModel = {};
        this._unifiedQuickSearch = '';
        this.defaultOpts = {
            returnType: 'indices',
            columns: null,
            fromIndex: null,
            caseSensitiveTextDefault: false,
            treatWhitespaceAsBlank:false,
            treatPlainNumberAsPercent: true,
            defaultType: 'select',
            cacheSize:10_000,
            ...defaultOpts
        }
        this.quoteTypeColumn = this.defaultOpts.quoteTypeColumn || 'QT';
        this.filterConfigs = new Map();
        this.filterValueCache = null; // new LRUCache(this.defaultOpts?.cacheSize ?? 1000);
        this.treatPlainNumberAsPercent = this.defaultOpts.treatPlainNumberAsPercent;
        this.defaultType = this.defaultOpts.defaultType;
        this.dataTypeFilters = this._initializeDataTypeFilters();

        if (this.adapter) {
            this.filterModel$ = this.adapter.context.page.createStore('filterModel', {
                _unifiedFilterModel: {},
                _unifiedQuickSearch: ''
            });
        }

        // this.mem_inferSigFigs = memoize(this._mem_inferSigFigs.bind(this))
    }

    clearAllFilters() {
        if (this?.adapter?.api) this.adapter.api.filterManager.setFilterModel([]);
    }

    _isInteger(x) {
        const v = coerceToNumber(x);
        return v === parseInt(v?.toString())
    }

    _getSigFigsByQt(qt) {
        const qtMap = this.adapter.context.page.page$.get('sigFigs');
        return qtMap[qt] ?? (qtMap?.default ?? 3);
    }

    _normalizeQt(header) {
        if (!header) return 'default';
        header = String(header).toUpperCase();
        if (header.includes('PX')) return 'PX'
        if (header.includes('SPD')) return 'SPD'
        if (header.includes('MMY')
            || header.includes('YLD')
            || header.includes('YTC')
            || header.includes('YTW')
            || header.includes('YTM')
        ) return 'YLD'
        if (header.includes('CASH')
            || header.includes('PROCEEDS')
            || header.includes('DIRTY')
        ) return 'CASH'
        if (header.includes('DM')) return 'DM'
        if (header.includes('BPS'))  return "BPS"
        return null;
    }

    _guess_sigfigs(col) {
        const headerQt = this._normalizeQt(col);
        if (headerQt != null) return this._getSigFigsByQt(headerQt);
        const qtMap = this.adapter.context.page.page$.get('sigFigs');
        return qtMap?.default ?? 3
    }

    _initializeDataTypeFilters() {
        const self = this;
        const engine = this.engine;
        return {
            number: {
                filter: 'agNumberColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                headerClass: ['ag-numeric-header'],
                cellClass: ['ag-right-aligned-cell', 'ag-numeric-cell', 'ag-number-cell'],
                filterParams: {
                    convertValuesToStrings: false,
                    refreshValuesOnOpen: true,
                    caseSensitive: false,
                    numberParser: (text) => {
                        return text == null ? null : coerceToNumber(text, {onNaN:null});
                    },
                    values: (params) => {
                        const f = this._getFilterValues(params);
                        return params.success(f)
                    },
                    valueFormatter: (params) => {
                        return this._getFilterFormats(params)
                    },
                    comparator: (a, b) => {
                        const valA = nullishFilter(a) ? 0 : coerceToNumber(a, {onNaN:null});
                        const valB = nullishFilter(b) ? 0 : coerceToNumber(b, {onNaN:null});
                        if (valA === valB) return 0;
                        return valA > valB ? 1 : -1;
                    },
                },
                valueFormatter: (params, engine, formatting) => {
                    const v = params?.value;
                    // if (params.colDef?.field === 'refLevel') {
                    //     console.log("FORMATTTING", params, params.value, formatting)
                    // }

                    if (v === CLEAR_SENTINEL) return 'CLEAR';
                    return formatNumber(v, formatting)
                },
                valueParser: (params) => {
                    return coerceToNumber(params.value, {onNaN:null});
                },
                context: {
                    formatting: (params) => {
                        return {
                            divisor: false,
                            showCommas: true,
                            onNaN: null,
                            onZero: null,
                            trimSigFigs: false,
                            currencySpace: true,
                        }
                    }
                }
            },
            integer: {
                filter: 'agNumberColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                headerClass: ['ag-numeric-header'],
                cellClass: ['ag-right-aligned-cell', 'ag-numeric-cell', 'ag-integer-cell'],
                filterParams: {
                    numberParser: (text) => {
                        return text == null ? null : parseInt(coerceToNumber(text, {onNaN:null}));
                    },
                    comparator: (a, b) => {
                        const valA = a == null ? 0 : coerceToNumber(a, {onNaN:null});
                        const valB = b == null ? 0 : coerceToNumber(b, {onNaN:null});
                        if (valA === valB) return 0;
                        return valA > valB ? 1 : -1;
                    },
                },
                valueFormatter: (params, engine, formatting) => {
                    const v = params?.value;
                    if (v === CLEAR_SENTINEL) return 'CLEAR';
                    return formatNumber(v, formatting)
                },
                valueParser: (params) => {
                    return params.value == null ? null : parseInt(coerceToNumber(params.value, {onNaN:null}));
                },
                context: {
                    formatting: (params) => {
                        return {
                            divisor: false,
                            showCommas: true,
                            onNaN: null,
                            onZero: null,
                            currencySpace: true,
                            sigFigs: 0
                        }
                    }
                }
            },
            currency: {
                filter: 'agNumberColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                headerClass: ['ag-numeric-header'],
                cellClass: ['ag-right-aligned-cell', 'ag-numeric-cell', 'ag-integer-cell'],
                filterParams: {
                    numberParser: (text) => {
                        return text == null ? null : parseInt(coerceToNumber(text, {onNaN:null}));
                    },
                    comparator: (a, b) => {
                        const valA = a == null ? 0 : coerceToNumber(a, {onNaN:null});
                        const valB = b == null ? 0 : coerceToNumber(b, {onNaN:null});
                        if (valA === valB) return 0;
                        return valA > valB ? 1 : -1;
                    },
                },
                valueFormatter: (params, engine, formatting) => {
                    const v = params?.value;
                    if (v === CLEAR_SENTINEL) return 'CLEAR';
                    const ri = _extractId(params);
                    const fmtSettings = {...formatting};
                    let currencyCol;
                    if (fmtSettings.asCurrency === null) {
                        currencyCol = 'currency';
                    } else if (fmtSettings.asCurrency && fmtSettings.asCurrency.toString().length > 1) {
                        currencyCol = fmtSettings.asCurrency;
                    }
                    let currency = fmtSettings.asCurrency;
                    if (currencyCol) {
                        currency = engine ? engine.getCell(ri, currencyCol) : currency;
                    }
                    fmtSettings.asCurrency = currency;

                    return formatNumber(v, fmtSettings)
                },
                valueParser: (params) => {
                    return params.value == null ? null : parseInt(coerceToNumber(params.value, {onNaN:null}));
                },
                context: {
                    formatting: (params) => {
                        return {
                            divisor: false,
                            showCommas: true,
                            onNaN: null,
                            onZero: null,
                            asCurrency: null,
                            currencySpace: true,
                        }
                    }
                }
            },
            float: {
                filter: 'agNumberColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                headerClass: ['ag-numeric-header'],
                cellClass: ['ag-right-aligned-cell', 'ag-numeric-cell', 'ag-float-cell'],
                filterParams: {
                    numberParser: (text) => {
                        return text == null ? null : coerceToNumber(text, {onNaN:null});
                    },
                    comparator: (a, b) => {
                        const valA = a == null ? 0 : coerceToNumber(a, {onNaN:null});
                        const valB = b == null ? 0 : coerceToNumber(b, {onNaN:null});
                        if (valA === valB) return 0;
                        return valA > valB ? 1 : -1;
                    },
                },
                valueFormatter: (params, engine, formatting) => {
                    const v = params?.value;
                    if (v === CLEAR_SENTINEL) return 'CLEAR';
                    return formatNumber(v, formatting)
                },
                valueParser: (params) => {
                    return params.value == null ? null : coerceToNumber(params.value, {onNaN:null});
                },
                context: {
                    formatting: (params) => {
                        return {
                            divisor: false,
                            showCommas: true,
                            onNaN: null,
                            onZero: null,
                            currencySpace: true,
                        }
                    }
                }
            },
            date: {
                filter: 'agDateColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                cellClass: ['ag-date-cell'],
                filterParams: {
                    comparator: (filterDate, cellValue) => {
                        const toUTCDateOnly = (d) => {
                            const t = new Date(d);
                            return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate()));
                        };
                        const f = toUTCDateOnly(filterDate).getTime();
                        const c = toUTCDateOnly(cellValue).getTime();
                        if (c === f) return 0;
                        return c < f ? -1 : 1;
                    }
                },
                valueParser: (params) => {
                    return coerceToDate(params.value)
                }
            },

            datetime: {
                filter: 'agDateColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                cellClass: ['ag-datetime-cell'],
                filterParams: {
                    comparator: (filterDate, cellValue) => {
                        const f = new Date(filterDate).getTime();
                        const c = new Date(cellValue).getTime();
                        if (isNaN(c)) return -1;
                        if (c === f) return 0;
                        return c < f ? -1 : 1;
                    }
                },
                valueParser: (params) => {
                    return coerceToDate(params.value)
                }
            },

            boolean: {
                filter: 'agSetColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                cellClass: ['ag-boolean-cell'],
                cellEditor: 'agRichSelectCellEditor',
                cellEditorParams: {
                    values: [true, false]
                },
                filterParams: {
                    values: (params) => {
                        return params.success([true, false])
                    },
                    valueFormatter: (params) =>  params.value ? '✓' : '✗',
                    refreshValuesOnOpen: true,
                },
                valueParser: (params) => coerceToBool(params.value),
                valueFormatter: (params) =>  coerceToBool(params.value) ? '✓' : '✗'
            },
            flag: {
                filter: 'agSetColumnFilter',
                width: 90,
                sortingOrder: ['desc', 'asc', null],
                cellClass: ['ag-boolean-cell', 'center-cell'],
                headerClass: ['text-center'],
                cellRenderer: FlagCellRenderer,
                cellEditor: 'agRichSelectCellEditor',
                cellEditorParams: {
                    values: [true, false]
                },
                context: {
                    triggerRedraw: true,
                },
                filterParams: {
                    values: (params) => {
                        return params.success([true, false])
                    },
                    refreshValuesOnOpen: true,
                    // valueFormatter: (params) =>  params.value ? '✓' : '✗',
                },
                valueParser: (params) => {
                    return coerceToBool(params.value)
                },
                valueFormatter: (params) =>  coerceToBool(params.value) ? '✓' : '✗'
            },
            text: {
                filter: 'agTextColumnFilter',
                sortingOrder: ['asc', 'desc', null],
                cellClass: ['ag-text-cell'],
                filterParams: {
                    filterOptions: ['contains', 'notContains', 'equals', 'notEqual', 'startsWith', 'endsWith'],
                    defaultOption: 'contains'
                },
                valueParser: (params) => String(params.value).trim() === '' ? null : params.value
            },
            select: {
                filter: 'agSetColumnFilter',
                sortingOrder: ['asc', 'desc', null],
                cellClass: ['ag-select-cell'],
                filterParams: {
                    values: (params) => {
                        const f = self._getFilterValues(params);
                        return params.success(f)
                    },
                    convertValuesToStrings: false,
                    refreshValuesOnOpen: true,
                    caseSensitive: false,
                    valueFormatter: (params) => {
                        return self._getFilterFormats(params)
                    }
                },
                valueParser: (params) => String(params.value).trim() === '' ? null : params.value

            },
            object: {
                filter: 'agSetColumnFilter',
                sortingOrder: ['asc', 'desc', null],
                cellClass: ['ag-select-cell'],
                filterParams: {
                    values: (params) => params.success(this._getFilterValues(params)),
                    convertValuesToStrings: true,
                    refreshValuesOnOpen: true,
                },
                valueFormatter: (params) => {
                    return asArray(params.value).join(',')
                }
            },
            percentage: {
                filter: 'agNumberColumnFilter',
                sortingOrder: ['desc', 'asc', null],
                headerClass: ['ag-numeric-header'],
                cellClass: ['ag-right-aligned-cell','ag-numeric-cell','ag-percentage-cell'],
                filterParams: {
                    numberParser: (text) => {
                        if (text == null) return null;
                        const raw = String(text).trim();
                        if (raw.endsWith('%')) {
                            const v = parseFloat(raw.slice(0, -1));
                            return isNaN(v) ? null : v / 100;
                        }
                        const v = parseFloat(raw);
                        if (isNaN(v)) return null;
                        if (this.treatPlainNumberAsPercent) {
                            return v / 100;
                        } else {
                            return v > 1 ? v / 100 : v;
                        }
                    },
                    comparator: (a, b) => {
                        const valA = a == null ? 0 : coerceToNumber(a, {onNaN:null});
                        const valB = b == null ? 0 : coerceToNumber(b, {onNaN:null});
                        if (valA === valB) return 0;
                        return valA > valB ? 1 : -1;
                    },
                },
                valueFormatter: (params, engine, formatting) => {
                    const v = params?.value;
                    if (v === CLEAR_SENTINEL) return 'CLEAR';
                    return formatNumber(v, formatting)
                },
                context: {
                    formatting: (params) => {
                        return {
                            divisor: false,
                            showCommas: true,
                            onNaN: null,
                            onZero: 0,
                            sigFigs: {'0':0, '>0': 1},
                            asPercent: true,
                        }
                    }
                }
            }
        };
    }

    mapArrowToColumnType(arrowType) {

    }

    configureFilter(colDef) {
        const engine = this.engine;
        const field = colDef.field || colDef.colId;
        const dataType = colDef?.context?.dataType || this.defaultType;
        let baseConfig = this.dataTypeFilters[dataType] || this.dataTypeFilters[this.defaultType];
        let secondaryType = colDef?.context?.secondaryType;
        if ((dataType === "select") && (secondaryType == null)) {
            if (this.engine && !this.engine?._isDerived(field)) {
                const vec = this.engine._getVector(field);
                if (this.engine._isNumericArrowType(vec)) {
                    secondaryType = 'number';
                } else if (this.engine._detectTimeUnitScale(vec) !== 1) {
                    secondaryType = 'date'; // Timestamps are numeric-like dates
                } else {
                    secondaryType = "text"
                }
            }
            if (secondaryType == null && colDef?.context?.allowAggregation) {
                secondaryType = 'number'
            }

            const secondConfig = this.dataTypeFilters?.[secondaryType] //agSetColumnFilter
            if (secondConfig) {
                const newConfig = {...secondConfig};
                newConfig['filter'] = 'agSetColumnFilter'
                newConfig['filterParams'] = baseConfig['filterParams'] || {};
                newConfig['filterParams'] = {...newConfig['filterParams'], ...secondConfig?.filterParams};
                baseConfig = newConfig;
            }
        }

        const filterConfig =  recursiveMerge.all([baseConfig, colDef]);
        filterConfig.context = filterConfig.context ?? {};

        if (typeof colDef?.cellClass === 'function') {
            const baseClass = baseConfig?.cellClass || [];
            const cs = colDef.context?.cellClassfn;
            if (cs) {
                filterConfig.cellClass = (params) => cs(params, engine, [...baseClass])
            }
        }

        const os = colDef.context?.formatting ?? {};
        const bs = baseConfig.context?.formatting ?? {};

        if ((typeof os === 'function') || (typeof bs === 'function')) {
            filterConfig.context.formatting = (params) => {
                let bc = bs;
                if (typeof bs === 'function') bc = bs(params);
                let oc = os;
                if (typeof os === 'function') oc = os(params, engine);

                const ff = {...bc, ...oc};
                // if (params?.colDef?.field === 'duration') {
                //     if (!window.tester) {
                //         console.log("DURATION: ---> ", ff)
                //         window.tester = 1
                //     }
                // }
                return ff
            }
        } else {
            filterConfig.context.formatting = (params) => filterConfig.context.formatting ?? {}
        }

        const fos = colDef?.valueFormatter ?? {};
        const fbs = baseConfig?.valueFormatter ?? {};
        if ((typeof fos === 'function') || (typeof fbs === 'function')) {
            filterConfig.valueFormatter = (params) => {
                if (params == null) return;
                let my_params = params || {};
                if (typeof my_params !== 'object') {
                    my_params = {value: my_params};
                } else {
                    my_params = {...my_params};
                }
                // console.log('running with', params)
                const formatting = filterConfig.context.formatting(my_params);
                if (formatting?.sigFigs && (typeof formatting.sigFigs === 'function')) {
                    formatting.sigFigs = formatting.sigFigs(my_params, engine)
                }

                my_params.colDef = my_params.colDef || {};
                my_params.colDef.context = my_params.colDef?.context || {}

                my_params.colDef.context._formatting = formatting;

                let v = my_params.value;
                if (typeof fos === 'function') v = fos(my_params, engine, formatting);
                else if (typeof fbs === 'function') v = fbs(my_params, engine, formatting);

                // if (key && col_map) col_map.set(key, v);
                return v
            }
        }

        this.filterConfigs.set(field, filterConfig);
        return filterConfig;
    }

    setFilterValues(field, values) {
        this.filterValueCache.set(field, values);
        return this;
    }

    _getFilterFormats(params) {
        if (params?.colDef?.valueFormatter) return params.colDef.valueFormatter(params.value)
        return params.value
    }

    _getFilterValues(params) {
        if (params?.colDef?.context?.filterValues) {
            return params.colDef.context.filterValues(params, this.engine);
        }

        const field = params.colDef.field;
        let raw = this.engine.getDistinctColumnValues(field, true);
        if (raw == null) return [];

        const vp = params?.colDef?.valueParser;
        const vf = params?.colDef?.valueFormatter;

        if (vp) raw = raw.map(r => vp({...params, value: r}));
        if (vf) raw = raw.map(r => vf({...params, value: r}));

        const distinct = new Set();
        const pushVal = (v) => {
            const k = (v == null) ? '(Blanks)' : v;
            if (!distinct.has(k)) {
                distinct.add(k);
            }
        };

        if (Array.isArray(raw)) {
            // Could be flat or nested
            for (let i = 0; i < raw.length; i++) {
                const v = raw[i];
                if (Array.isArray(v) || (v && typeof v.length === 'number' && typeof v !== 'string')) {
                    for (let j = 0; j < v.length; j++) {
                        pushVal(v[j]);
                    }
                } else {
                    pushVal(v);
                }
            }
        }

        return Array.from(distinct).toSorted((a,b) => a-b)
    }

    _normalizeQuickSearch(x) {
        return x === '' ? null : x
    }

    // Heuristic: infer AG Grid filterType from vector
    _inferFilterType(vector) {
        const t = vector && vector.type;
        if (!t) return FILTER_TYPES.TEXT
        if (t && t.unit) return FILTER_TYPES.DATE
        const sample = vector.get(0);
        if (typeof sample === 'number') return FILTER_TYPES.NUMBER
        return FILTER_TYPES.TEXT
    }

    _parseDateMs(x) {
        if (x == null) return null;
        if (typeof x === 'number' && Number.isFinite(x)) return x;
        if (typeof x !== 'string') return null;
        if (/^\d{4}-\d{2}-\d{2}$/.test(x)) {
            const [Y, M, D] = x.split('-').map(Number);
            return new Date(Y, M - 1, D, 0, 0, 0, 0).getTime();
        }
        const t = Date.parse(x);
        return Number.isFinite(t) ? t : null;
    }

    _isBlank(val, treatWhitespaceAsBlank) {
        if (val == null) return true;
        if (typeof val === 'string') return treatWhitespaceAsBlank ? val.trim().length === 0 : val.length === 0;
        return false;
    }

    _inferFilterTypeFromSamples(colName) {
        const get = this.engine._getValueGetter(colName);
        const n = this.engine.table.numRows | 0;
        if (n === 0) return FILTER_TYPES.TEXT;

        const p0 = 0, p1 = (n >>> 1), p2 = n ? (n - 1) : 0;
        const v0 = get(p0), v1 = get(p1), v2 = get(p2);

        const isNum = (v) => typeof v === 'number' && Number.isFinite(v);
        const isDateLike = (v) => v instanceof Date || (typeof v === 'number' && Number.isFinite(v) && v > 0 && v < 8.22e13);

        if (isNum(v0) || isNum(v1) || isNum(v2)) return FILTER_TYPES.NUMBER;
        if (isDateLike(v0) || isDateLike(v1) || isDateLike(v2)) return FILTER_TYPES.DATE;

        // Heuristic: small cardinality across samples → SET
        const s0 = v0 == null ? '∅' : String(v0);
        const s1 = v1 == null ? '∅' : String(v1);
        const s2 = v2 == null ? '∅' : String(v2);
        const distinct = ((s0 !== s1) + (s0 !== s2) + (s1 !== s2)) + 1; // 1..3
        if (distinct <= 2) return FILTER_TYPES.SET;

        return FILTER_TYPES.TEXT;
    }

    _buildPredicateForFilter(colName, def, opts) {
        const columnDef = this.adapter.columnRegistry.columns.get(colName) || {};

        // 1) explicit from colDef context, if provided
        let filterType = columnDef?.context?.dataType;

        // 2) Arrow physical vectors → infer
        if (!filterType && !this.engine._isDerived(colName)) {
            const vec = this.engine._getVector(colName);
            if (vec) filterType = this._inferFilterType(vec);
        }

        // 3) Derived columns → use registered meta.kind if available
        if (!filterType && this.engine._isDerived(colName)) {
            const meta = this.engine._getDerived(colName);
            // Normalize kind -> FILTER_TYPES
            const kind = meta?.kind && String(meta.kind).toLowerCase();
            if (kind === 'number' || kind === 'numeric' || kind === 'float' || kind === 'int') filterType = FILTER_TYPES.NUMBER;
            else if (kind === 'string' || kind === 'text') filterType = FILTER_TYPES.TEXT;
            else if (kind === 'date' || kind === 'datetime' || kind === 'timestamp') filterType = FILTER_TYPES.DATE;
        }

        // 4) Final fallback for anything (derived or odd physical): sample the value getter
        if (!filterType) {
            filterType = this._inferFilterTypeFromSamples(colName);
        }

        switch (filterType) {
            case FILTER_TYPES.TEXT:
                return this._buildTextPredicate(colName, def, opts);
            case FILTER_TYPES.NUMBER:
                return this._buildNumberPredicate(colName, def, opts);
            case FILTER_TYPES.DATE:
                return this._buildDatePredicate(colName, def, opts);
            case FILTER_TYPES.SET:
            case FILTER_TYPES.SELECT:
                return this._buildSetPredicate(colName, def, opts);
            default:
                // safe default: pass-through (or throw if you prefer strict)
                return () => true;
        }
    }

    _buildTextPredicate(colName, def, opts) {
        const op = def.type || 'contains';
        const caseSensitive = !!def.caseSensitive || opts?.caseSensitiveTextDefault;
        const ftext = (def.filter != null) ? String(def.filter) : '';
        const query = caseSensitive ? ftext : ftext.toLowerCase();
        if (op === 'blank') {
            return (ri) => this._isBlank(this.engine.getCell(ri, colName), def.treatWhitespaceAsBlank);
        }
        if (op === 'notBlank') {
            return (ri) => !this._isBlank(this.engine.getCell(ri, colName), def.treatWhitespaceAsBlank);
        }
        return (ri) => {
            const raw = this.engine.getCell(ri, colName);
            if (raw == null) return false;
            let s = String(raw);
            if (!caseSensitive) s = s.toLowerCase();
            switch (op) {
                case 'equals': return s === query;
                case 'notEqual': return s !== query;
                case 'contains': return s.indexOf(query) !== -1;
                case 'notContains': return s.indexOf(query) === -1;
                case 'startsWith': return s.startsWith(query);
                case 'endsWith': return s.endsWith(query);
                default: return s.indexOf(query) !== -1;
            }
        };
    }

    _buildNumberPredicate(colName, def, opts) {
        const op = def.type || 'equals';
        const a = def.filter != null ? Number(def.filter) : null;
        const b = def.filterTo != null ? Number(def.filterTo) : null;
        if (op === 'blank') {
            return (ri) => (this.engine.getCell(ri, colName) == null);
        }
        if (op === 'notBlank') {
            return (ri) => (this.engine.getCell(ri, colName) != null);
        }
        if (op === 'inRange') {
            if (!Number.isFinite(a) || !Number.isFinite(b)) return () => false;
            return (ri) => {
                const v = this.engine.getCell(ri, colName);
                if (v == null) return false;
                return v >= a && v <= b;
            };
        }
        return (ri) => {
            const v = this.engine.getCell(ri, colName);
            if (v == null) return false;
            switch (op) {
                case 'equals': return v === a;
                case 'notEqual': return v !== a;
                case 'lessThan': return v < a;
                case 'lessThanOrEqual': return v <= a;
                case 'greaterThan': return v > a;
                case 'greaterThanOrEqual': return v >= a;
                default: return v === a;
            }
        };
    }

    _buildDatePredicate(colName, def, opts) {
        const op = def.type || 'equals';
        const df = this._parseDateMs(def.dateFrom ?? def.filter ?? null);
        const dt = this._parseDateMs(def.dateTo ?? null);
        if (op === 'blank') {
            return (ri) => {
                const raw = this.engine.getCell(ri, colName);
                return raw == null || raw === '';
            };
        }
        if (op === 'notBlank') {
            return (ri) => {
                const raw = this.engine.getCell(ri, colName);
                return !(raw == null || raw === '');
            };
        }
        const normalizeCellMs = (ri) => {
            const v = this.engine.getCell(ri, colName);
            if (v == null) return null;
            const vNum = typeof v === 'number' ? v : this._parseDateMs(String(v));
            return Number.isFinite(vNum) ? vNum : null;
        };
        if (op === 'inRange') {
            if (df == null || dt == null) return () => false;
            return (ri) => {
                const v = normalizeCellMs(ri);
                if (v == null) return false;
                return v >= df && v <= dt;
            };
        }
        return (ri) => {
            const v = normalizeCellMs(ri);
            if (v == null || df == null) return false;
            switch (op) {
                case 'equals': return v === df;
                case 'notEqual': return v !== df;
                case 'lessThan': return v < df;
                case 'lessThanOrEqual': return v <= df;
                case 'greaterThan': return v > df;
                case 'greaterThanOrEqual': return v >= df;
                default: return v === df;
            }
        };
    }

    _buildSetPredicate(colName, def, opts) {
        const vals = Array.isArray(def.values) ? def.values : [];
        if (!vals.length) return () => false;

        // Pre-normalize: keep a string set and a numeric set if applicable
        let hasNumeric = false;
        for (let i = 0; i < vals.length; i++) {
            const v = vals[i];
            if (typeof v === 'number') { hasNumeric = true; break; }
            // treat numeric-looking strings as numeric candidates too
            if (typeof v === 'string' && v.trim() && !Number.isNaN(+v)) { hasNumeric = true; break; }
        }

        const strSet = new Set(vals.map(v => (v == null ? '∅' : String(v))));
        const numSet = hasNumeric
            ? (() => {
                const s = new Set();
                for (let i = 0; i < vals.length; i++) {
                    const v = vals[i];
                    const n = (typeof v === 'number') ? v : (typeof v === 'string' ? +v : NaN);
                    if (Number.isFinite(n)) s.add(n);
                }
                return s;
            })()
            : null;

        const get = this.engine._getValueGetter(colName);
        return (ri) => {
            const raw = get(ri);
            if (raw == null) return strSet.has('∅');
            if (numSet && typeof raw === 'number' && Number.isFinite(raw) && numSet.has(raw)) return true;
            return strSet.has(String(raw));
        };
    }

    applyAgGridFilterModel(filterModel, opts = {}) {
        opts = { ...this.defaultOpts, ...opts };
        const { returnType, columns, fromIndex } = opts;

        const entries = (filterModel && typeof filterModel === 'object') ? Object.entries(filterModel) : [];
        const totalRows = (this.engine.table?.numRows | 0);
        const base = (fromIndex && fromIndex.length) ? fromIndex : null;
        const baseLen = base ? base.length : totalRows;

        if (entries.length === 0) {
            if (returnType === 'indices') {
                if (base) return base;
                const idx = new Int32Array(totalRows);
                for (let i = 0; i < totalRows; i++) idx[i] = i;
                return idx;
            } else {
                const cols = this.engine._normalizeColumnSelector(columns ?? (this.adapter?._projection || this.engine._fieldNames));
                const idx = base ? base : (() => { const a = new Int32Array(totalRows); for (let i = 0; i < totalRows; i++) a[i] = i; return a; })();
                return this.engine.getDisjointRowObjects(idx, cols);
            }
        }

        const normalizeLeaf = (parentType, leaf) => {
            if (!leaf) return leaf;
            if (!leaf.filterType && parentType) return Object.assign({ filterType: parentType }, leaf);
            return leaf;
        };

        const buildColumnPredicate = (colName, colModel) => {
            if (colModel && colModel.filterType === 'multi' && Array.isArray(colModel.filterModels)) {
                const op = (colModel.operator || 'AND').toUpperCase();
                const parts = colModel.filterModels.map(m => buildColumnPredicate(colName, m));
                if (op === 'OR') return (ri) => { for (let i = 0; i < parts.length; i++) if (parts[i](ri)) return true; return false; };
                return (ri) => { for (let i = 0; i < parts.length; i++) if (!parts[i](ri)) return false; return true; };
            }
            if (colModel && (colModel.condition1 || colModel.condition2)) {
                const op = (colModel.operator || 'AND').toUpperCase();
                const c1 = colModel.condition1 ? this._buildPredicateForFilter(colName, normalizeLeaf(colModel.filterType, colModel.condition1), opts) : null;
                const c2 = colModel.condition2 ? this._buildPredicateForFilter(colName, normalizeLeaf(colModel.filterType, colModel.condition2), opts) : null;
                if (op === 'OR') return (ri) => ((c1 ? c1(ri) : false) || (c2 ? c2(ri) : false));
                return (ri) => ((c1 ? c1(ri) : true) && (c2 ? c2(ri) : true));
            }
            return this._buildPredicateForFilter(colName, normalizeLeaf(colModel.filterType, colModel), opts);
        };

        const preds = [];
        for (let i = 0; i < entries.length; i++) {
            const colId = entries[i][0];
            preds.push(buildColumnPredicate(colId, entries[i][1]));
        }

        const tmp = new Int32Array(baseLen);
        let outCount = 0;
        if (base) {
            for (let i = 0; i < baseLen; i++) {
                const ri = base[i] | 0;
                let pass = true;
                for (let p = 0; p < preds.length; p++) { if (!preds[p](ri)) { pass = false; break; } }
                if (pass) tmp[outCount++] = ri;
            }
        } else {
            for (let ri = 0; ri < totalRows; ri++) {
                let pass = true;
                for (let p = 0; p < preds.length; p++) { if (!preds[p](ri)) { pass = false; break; } }
                if (pass) tmp[outCount++] = ri;
            }
        }

        const indices = tmp.subarray(0, outCount);
        if (returnType === 'indices') return indices;
        const cols = this.engine._normalizeColumnSelector(columns ?? (this.adapter?._projection || this.engine._fieldNames));
        return this.engine.getDisjointRowObjects(indices, cols);
    }

    // Build once; reuse across repeated passes
    compileAgGridFilterModel(filterModel, opts = {}) {
        const entries = (filterModel && typeof filterModel === 'object') ? Object.entries(filterModel) : [];
        if (!entries.length) return () => true;

        const normalizeLeaf = (parentType, leaf) => {
            if (!leaf) return leaf;
            if (!leaf.filterType && parentType) return Object.assign({ filterType: parentType }, leaf);
            return leaf;
        };

        const buildColumnPredicate = (colName, colModel) => {
            if (colModel && colModel.filterType === 'multi' && Array.isArray(colModel.filterModels)) {
                const op = (colModel.operator || 'AND').toUpperCase();
                const parts = colModel.filterModels.map(m => buildColumnPredicate(colName, m));
                if (op === 'OR') return (ri) => { for (let i = 0; i < parts.length; i++) if (parts[i](ri)) return true; return false; };
                return (ri) => { for (let i = 0; i < parts.length; i++) if (!parts[i](ri)) return false; return true; };
            }
            if (colModel && (colModel.condition1 || colModel.condition2)) {
                const op = (colModel.operator || 'AND').toUpperCase();
                const c1 = colModel.condition1 ? this._buildPredicateForFilter(colName, normalizeLeaf(colModel.filterType, colModel.condition1), opts) : null;
                const c2 = colModel.condition2 ? this._buildPredicateForFilter(colName, normalizeLeaf(colModel.filterType, colModel.condition2), opts) : null;
                if (op === 'OR') return (ri) => ((c1 ? c1(ri) : false) || (c2 ? c2(ri) : false));
                return (ri) => ((c1 ? c1(ri) : true) && (c2 ? c2(ri) : true));
            }
            return this._buildPredicateForFilter(colName, normalizeLeaf(colModel.filterType, colModel), opts);
        };

        const preds = entries.map(([colId, model]) => buildColumnPredicate(colId, model));
        return (ri) => { for (let i = 0; i < preds.length; i++) if (!preds[i](ri)) return false; return true; };
    }

}

export class AgColumnRegistry {
    constructor(adapter, fuseOptions={}) {
        this.adapter = adapter;
        this.engine = adapter.engine;
        this.columns = new Map(); // name -> columnDef
        this.fuseData = new Map();
        this.columnIndexMap = new BiMap();
        this.fuseOptions = {
            keys: [
                { name: "field", weight: 0.5 },
                { name: "headerName", weight: 0.75 },
                { name: "metaTags", weight: 0.25 },
                { name: "autoTags", weight: 0.05 },
            ],
            includeScore: true,
            isCaseSensitive: false,
            shouldSort: false,
            threshold: 0.25,
            distance: 50,
            location: 0,
            minMatchCharLength: 1,
            findAllMatches: false
        };
        this.fuseOptions = {...this.fuseOptions, ...fuseOptions}
        this.fuse = new Fuse([], this.fuseOptions);
    }

    mapColumnIndices() {
        if (!this?.adapter?.api) return
        this.columnIndexMap.delete();
        const idxMap = this.columnIndexMap;
        this.adapter.api.getAllGridColumns().forEach((col, idx) => {
            idxMap.set(col.getColId(), idx)
        });
        return this.columnIndexMap;
    }

    _getFieldFromDef(columnDef) {
        return typeof columnDef === 'string' ? columnDef : (columnDef?.field || columnDef?.colId);
    }

    register(columnDef) {
        const fieldKey = this._getFieldFromDef(columnDef);
        if (!fieldKey) throw new Error('Cannot register a column without a field', columnDef);
        const fieldData = {
            field: fieldKey,
            headerName: columnDef?.headerName,
            metaTags: columnDef?.context?.metaTags || []
        }
        this.columns.set(fieldKey, columnDef);
        this.fuseData.set(fieldKey, fieldData);
        this.fuse.add(fieldData);
    }

    unregister(columnDef) {
        const fieldKey = this._getFieldFromDef(columnDef);
        if (!fieldKey) return
        if (this.fuseData.has(fieldKey)) {
            this.fuseData.delete(fieldKey);
            this.fuse.remove((doc) => {
                return doc?.field === fieldKey
            });
        }
        if (this.columns.has(fieldKey)) {
            this.columns.delete(fieldKey);
        }
    }

    rebuildFuseIndex() {
        const fuseData = Array.from(this.fuseData.values());
        this.fuse.setCollection(fuseData);
    }

    getColumnDef(field) {return this.columns.get(field)}
    hasColumnDef(field) {return this.columns.has(field)}

    registerBatchColumns(colDefArray) {
        colDefArray = asArray(colDefArray);
        colDefArray.forEach(colDef => {
            this.register(colDef);
        })
    }
}

export class EngineAgGridRefreshBridge {
    constructor({ engine, api, debounceMs = 40, maxDelayMs = 120, suppressDuringEdit = true } = {}) {
        if (!engine || !api) throw new Error('EngineAgGridRefreshBridge: engine and api are required');
        this.engine = engine;
        this.api = api;
        this.suppressDuringEdit = suppressDuringEdit;

        this._pending = { global: false, rows: false, cols: new Set() };
        this._disposed = false;

        this._batcher = new RecomputeBatcher({ debounceMs, maxDelayMs, useRaf: true });
        this._flush = this._flush.bind(this);

        this._off = this.engine.onEpochChange?.((p) => {
            if (this._disposed) return;
            if (!p || (p.global && p.global === true)) {
                this._pending.global = true;
                this._pending.rows = true;
                this._pending.cols.clear();
                this._schedule();
                return;
            }
            if (p.rowsChanged) this._pending.rows = true;
            if (p.colsChanged === true) {
                this._pending.cols.clear();
            } else if (Array.isArray(p.colsChanged) && p.colsChanged.length) {
                for (let i = 0; i < p.colsChanged.length; i++) {
                    const v = p.colsChanged[i];
                    if (typeof v === 'number') {
                        // convert column index to field name safely
                        const names = this.engine?._fieldNames;
                        if (names && names[v] != null) this._pending.cols.add(names[v]);
                    } else if (typeof v === 'string') {
                        this._pending.cols.add(v);
                    }
                }
            }
            this._schedule();
        }) || (() => {});
    }

    _schedule() {
        this._batcher.schedule(this._flush);
    }

    _isEditing() {
        try {
            const cells = typeof this.api.getEditingCells === 'function'
                ? this.api.getEditingCells()
                : [];
            return Array.isArray(cells) && cells.length > 0;
        } catch { return false; }
    }

    async _flush() {
        if (this._disposed) return;
        const api = this.api;
        if (!api) return;

        if (this.suppressDuringEdit && this._isEditing()) {
            // reschedule until edit stops
            this._schedule();
            return;
        }

        const needRows = this._pending.global || this._pending.rows;
        const colIds = (this._pending.cols.size && !this._pending.global)
            ? Array.from(this._pending.cols)
            : null;

        // Prefer the lightest ops first; force recompute of valueGetters
        try {
            api.refreshCells({
                force: true,
                suppressFlash: true,
                columns: colIds || undefined
            });
        } catch {}

        if (needRows) {
            try {
                api.redrawRows(); // safe no-op for server/client models
            } catch {}
        }

        // clear pending
        this._pending.global = false;
        this._pending.rows = false;
        this._pending.cols.clear();
    }

    dispose() {
        if (this._disposed) return;
        this._disposed = true;
        try { this._off?.(); } catch {}
        try { this._batcher.cancel(); } catch {}
    }
}

// ---------- NumericOverlay (dense/sparse with row blocks) ----------
const NULL_SENTINEL = Symbol('overlay-null');
const MASK_NONE = 0;   // no overlay
const MASK_VALUE = 1;  // overlay has a concrete value
const MASK_NULL = 2;   // overlay is explicit null

export class NumericOverlay {
    constructor(nRows, { blockSize = 2048, denseThreshold = 0.20, sparseThreshold = 0.05 } = {}) {
        this.n = nRows|0;
        this.blockSize = blockSize|0;
        this.blocks = null;      // sparse: Array<Map<int,value|NULL_SENTINEL>>
        this.mask = null;        // dense: Uint8Array
        this.values = null;      // dense: Float64Array
        this.size = 0;
        this.mode = 'sparse';
        this.denseThreshold = denseThreshold;
        this.sparseThreshold = sparseThreshold;
        this._ensureSparse();
    }
    _ensureSparse() {
        if (this.blocks) return;
        const nb = ((this.n + this.blockSize - 1) / this.blockSize) | 0;
        this.blocks = new Array(nb);
        for (let i = 0; i < nb; i++) this.blocks[i] = new Map();
        this.mask = null; this.values = null; this.mode = 'sparse';
    }
    _ensureDense() {
        if (this.mask && this.values) return;
        this.mask = new Uint8Array(this.n);         // uses MASK_* constants
        this.values = new Float64Array(this.n);
        this.blocks = null; this.mode = 'dense';
    }
    _density(){ return this.size / (this.n || 1); }
    _maybeRebalance() {
        const d = this._density();
        if (this.mode === 'sparse' && d >= this.denseThreshold) this._sparseToDense();
        else if (this.mode === 'dense' && d <= this.sparseThreshold) this._denseToSparse();
    }
    _sparseToDense() {
        this._ensureDense();
        if (!this.blocks) return;
        for (let b = 0; b < this.blocks.length; b++) {
            const base = b * this.blockSize;
            for (const [off, v] of this.blocks[b].entries()) {
                const ri = base + (off|0);
                if (v === NULL_SENTINEL) { this.mask[ri] = MASK_NULL; }
                else { this.mask[ri] = MASK_VALUE; this.values[ri] = v; }
            }
        }
        this.blocks = null;
    }
    _denseToSparse() {
        this._ensureSparse();
        for (let i = 0; i < this.n; i++) {
            const m = this.mask[i];
            if (m === MASK_NONE) continue;
            const b = (i / this.blockSize) | 0;
            const off = i - b*this.blockSize;
            if (m === MASK_NULL) this.blocks[b].set(off, NULL_SENTINEL);
            else this.blocks[b].set(off, this.values[i]);
        }
        this.mask = null; this.values = null;
    }
    clear() {
        this.size = 0;
        if (this.mode === 'dense') this.mask.fill(MASK_NONE);
        else { for (let b = 0; b < this.blocks.length; b++) this.blocks[b].clear(); }
    }
    has(ri) {
        ri|=0;
        if (this.mode === 'dense') return this.mask[ri] !== MASK_NONE;
        const b = (ri / this.blockSize)|0, off = ri - b*this.blockSize;
        return this.blocks[b].has(off);
    }
    get(ri) {
        ri|=0;
        if (this.mode === 'dense') {
            const m = this.mask[ri];
            if (m === MASK_NONE) return undefined;
            if (m === MASK_NULL) return null;
            return this.values[ri];
        }
        const b = (ri / this.blockSize)|0, off = ri - b*this.blockSize;
        if (!this.blocks[b].has(off)) return undefined;
        const v = this.blocks[b].get(off);
        return v === NULL_SENTINEL ? null : v;
    }
    set(ri, value) {
        ri|=0;
        if (this.mode === 'dense') {
            const prev = this.mask[ri];
            if (value === undefined) { if (prev !== MASK_NONE) { this.mask[ri] = MASK_NONE; this.size--; } return; }
            if (value === null) { if (prev === MASK_NONE) this.size++; this.mask[ri] = MASK_NULL; return; }
            // number
            if (prev === MASK_NONE) this.size++;
            this.mask[ri] = MASK_VALUE; this.values[ri] = value;
            this._maybeRebalance(); // stays dense
            return;
        }
        // sparse
        const b = (ri / this.blockSize)|0, off = ri - b*this.blockSize;
        const blk = this.blocks[b];
        if (value === undefined) { if (blk.delete(off)) this.size--; return; }
        if (value === null) {
            if (!blk.has(off)) this.size++;
            blk.set(off, NULL_SENTINEL);
            this._maybeRebalance();
            return;
        }
        if (!blk.has(off)) this.size++;
        blk.set(off, value);
        this._maybeRebalance();
    }
    setMany(rowIndices, values) {
        const n = rowIndices?.length | 0;
        if (!n) return;
        for (let i = 0; i < n; i++) this.set(rowIndices[i] | 0, values[i]);
    }
}

export class TypedArrayPool {
    constructor(bucketCaps) {
        this.buckets = {
            float64: new Map(),
            float32: new Map(),
            int32:   new Map(),
            uint32:  new Map(),
            uint8:   new Map()
        };

        const defaults = {
            float64: 8,
            float32: 8,
            int32: 16,
            uint32: 16,
            uint8: 32
        };

        this._caps = Object.assign(defaults, bucketCaps || {});
    }

    _nextPow2(n) {
        n |= 0;
        if (n <= 1) return 1;
        n -= 1;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        return (n + 1) >>> 0;
    }

    _getBucket(kind) {
        const bucket = this.buckets[kind];
        if (!bucket) throw new Error('TypedArrayPool: unknown kind ' + kind);
        return bucket;
    }

    acquire(kind, length) {
        const size = this._nextPow2(length | 0);
        const bucket = this._getBucket(kind);
        const stack = bucket.get(size);

        if (stack && stack.length) {
            return stack.pop();
        }

        switch (kind) {
            case 'float64': return new Float64Array(size);
            case 'float32': return new Float32Array(size);
            case 'int32':   return new Int32Array(size);
            case 'uint32':  return new Uint32Array(size);
            case 'uint8':   return new Uint8Array(size);
            default:
                throw new Error('TypedArrayPool: unsupported kind ' + kind);
        }
    }

    release(kind, arr) {
        if (!arr || typeof arr.length !== 'number') return;

        const size = arr.length | 0;
        const bucket = this._getBucket(kind);

        let stack = bucket.get(size);
        if (!stack) {
            stack = [];
            bucket.set(size, stack);
        }

        const cap = this._caps[kind] == null ? 16 : this._caps[kind];
        if (cap <= 0) return;

        if (stack.length < cap) {
            stack.push(arr);
        }
        // else: drop it on the floor, let GC reclaim
    }

    clear() {
        const kinds = Object.keys(this.buckets);
        for (let i = 0; i < kinds.length; i++) {
            this.buckets[kinds[i]].clear();
        }
    }
}

