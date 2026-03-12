import {CustomCellEditor} from "@/grids/js/genericCellEditor.js";
import {PivotColumnChooser} from "@/grids/js/newTree.js";
import { ArrowAgGridAdapter } from "@/grids/js/arrow/arrowEngine.js";
import { createGrid } from 'ag-grid-enterprise';
import {hash64Any, asArray, zipArray, debounce, debouncePerArgs, measureText} from '@/utils/helpers.js';
import { PivotEngine } from '@/grids/js/arrow/smartPivot.js';
import {coerceToNumeric} from "@/utils/NumberFormatter.js";
import {clearFormatCache, coerceToNumber, CLEAR_SENTINEL, coerceToBool} from '@/utils/typeHelpers.js';
import {getRefLevelRaw} from '@/pt/js/grids/portfolio/portfolioColumns.js';
import * as recursiveMerge from 'deepmerge';
import {CadesEmitter} from "@/utils/cades-emitter.js";

import memoize from "fast-memoize";
import {isNumber} from "lodash";
import {writeObjectToClipboard} from "@/utils/clipboardHelpers.js";
import {mean, median, quantileSeq} from "mathjs";

export const ARROW_COALESCE_KEYS = Object.freeze(['ArrowUp', 'ArrowDown', 'Home', 'End']);
const GUESS_MIN_W = 75;
const GUESS_MAX_W = 240;
const THROTTLE_MS = 100;
let lastCopyAt = 0;

export class ArrowAgPivotAdapter {
    constructor(context, sourceAdapter, opts = {}) {
        if (!sourceAdapter || !sourceAdapter.engine) throw new Error('ArrowAgPivotAdapter: sourceAdapter with engine is required.');

        this.context = context;
        this.source = sourceAdapter;
        this.engine = sourceAdapter.engine;
        this.grid$ = this.source.grid$;
        this.element = null;

        this.opts = Object.assign({
            refreshDebounceMs: 80,
            suppressTree: false,
            gridOptions: {},
            columnOverrides: {},                 // { colId: partialDef }
            customDefs: [],
            initialGroups: [],
            requiredGroups: [],                  // always include in groupBy
            requiredAggregations: [],
            enableGrandTotal: true, // always include in aggregations (format: [{ col: { func, name, weight? } }])
        }, opts || {});

        this.pivotEngine = new PivotEngine(this.engine);
        this.enableGrandTotal = this.opts.enableGrandTotal;
        this._defaultSort = false;
        this.domTimeout = false;

        this.pivotConfig = {
            groupBy: Array.from(new Set([...asArray(this.opts.requiredGroups), ...this.opts.initialGroups])),
            aggregations: this._normalizeAggList(this.opts.requiredAggregations || []),
        };

        this.req_aggs = new Set(this.pivotConfig.aggregations.map(x=>Object.keys(x)[0]));
        this.req_aggsOuts = new Set(this.pivotConfig.aggregations.map(x => Object.values(x)[0]?.name || Object.keys(x)[0]));
        this.req_gps = new Set(this.pivotConfig.groupBy);

        this._grid = null;
        this.api = null;
        this.gridOptions = null;

        this._projection = [];                 // pivot grid visible columns
        this._aggByOutputName = new Map();     // output name -> { input, spec }
        this._groupSet = new Set();
        this._aggInputSet = new Set();
        this.customDefs = this.opts.customDefs;
        this.totalRows = [];

        this._rows = [];                       // current pivot rows
        this._rowMap = new Map();              // id -> row
        this._pendingHard = false;
        this._pendingSoft = false;
        this._inHardSwap = false;
        this._debounceTimer = null;
        this._locked = false;

        this._respectSourceFilters = false;
        this._lockGridTotals = false;
        this._em = this.context.page.emitter;

        this._memo = memoize;
        this._guessWidth = this._memo(this._guessWidthRaw.bind(this));

        // Colorization state
        this._color = {
            enabled: false,
            metric: null,
            paletteSize: 64,
            scheme: 'RdYlGn',
            reverse: false,
            palette: null,
            ifNull: {'background-color':'transparent'},
            min: 0,
            max: 1
        };

        // Skew controls
        this._skew = {
            enabled: true,                    // disable wholesale
            refAggName: 'current_skew',               // aggregator output name the pivot engine returns
            sourceSkewColumn: 'refSkew', // base column in the SOURCE grid to mutate on APPLY
            weightColumn: 'grossSize',        // used by wavg agg; only for reference
        };

        // Skew mode: 'outright' (default) or 'percent'
        // Percent mode interprets input as % of bid-mid or ask-mid width
        this._skewMode = 'outright';
        this._skewPctMarket = null; // market key for percent width source (null = use active ref market)

        // pid -> { mode: 'input'|'proposed', input: number|null, proposed: number|null }
        this._bucketState = new Map();
        this._inBulkApply = false;     // guard to suppress epoch noise during apply()

        this._onEpoch = this._handleEpoch.bind(this);
        this._offEpoch = this.engine.onEpochChange(this._onEpoch);

        this._onDerivedDirty = this._handleDerivedDirty.bind(this);
        this._offDerived = this.engine.onDerivedDirty(this._onDerivedDirty)

        this._updateGrandTotalSkews = debounce(this._debounced_updateGrandTotalSkews.bind(this), 100);
        this._rebuildSets = debouncePerArgs(this.raw_rebuildSets.bind(this), 150, {trailing:true});
        this._setupReactions();

    }

    /* -------------------------------- DOM ----------------------------- */

    _normalizeCssInput(size) {
        if (isNumber(size)) {
            if (size <= 1) size = `${size*100}%`;
            else size = `${size}px`;
        }
        return size
    }

    setStyle(key, value) {
        if (!this.element) return;
        const eGridDiv = this.element;
        value = this._normalizeCssInput(value);
        eGridDiv.style.setProperty(key, value);
    }

    setWidth(maxWidth) {
        this.setStyle('width', maxWidth);
        this.setStyle('max-width', maxWidth);
    }

    _ensureWeightState() {
        if (this._weight && this._weight.field) return;
        this._weight = { mode: 'notional', field: '__wavg__' };
    }

    // Todo fix count / others to account for isreal
    async setWavgWeightMode(mode = 'notional') {
        this._ensureWeightState();
        const v = (mode === 'dv01' || mode === 'count') ? mode : 'notional';
        if (this._weight.mode === v) {
            // this.hardRefresh({force:true});
            return;
        }
        this._weight.mode = v;
        this._skew.weightColumn = (v === 'dv01') ? 'grossDv01' : (v === 'count' ? '__count__' : 'grossSize');
        this.grid$.set('weight', this._skew.weightColumn);
        this.hardRefresh({ force: false });
        try { this._updateGrandTotalSkews(); } catch {}
    }

    getPendingBucketCount() { return this._bucketState ? this._bucketState.size : 0; }
    onBucketChange(fn) { return this._em.on('bucket-change', fn); }
    _emitBucketChange() { try { this._em.emitAsync('bucket-change', { size: this.getPendingBucketCount() }); } catch {} }
    onGroupChange(fn) { return this._em.on('group-change', fn); }
    _emitGroupChange() { try { this._em.emitAsync('group-change', { groups: this._groupSet }); } catch {} }

    _computeWeightForIndex(rowIdx) {
        this._ensureWeightState();
        const m = this._weight.mode;
        if (m === 'dv01') {
            const v = this.engine.getCell(rowIdx, 'grossDv01');
            const n = Number(v);
            return Number.isFinite(n) ? n : 0;
        }
        if (m === 'count') return 1;
        const v = this.engine.getCell(rowIdx, 'grossSize');
        const n = Number(v);
        return Number.isFinite(n) ? n : 0;
    }

    _rowWeight(row) {
        this._ensureWeightState();
        if (row && Number.isFinite(+row.__w_sum__)) return +row.__w_sum__;
        const m = this._weight.mode;
        if (m === 'dv01') return Number(row?.grossDv01) || 0;
        if (m === 'count') return Number(row?.count) || 0;
        // notional
        return Number(row?.grossSize) || 0;
    }

    /* -------------------------------- grid events ----------------------------- */

    _notifyGeneric(key, ...args){this._em.emitAsync(key, ...args)};
    _notify = debouncePerArgs((...args) => this._notifyGeneric(...args), 275, {leading:true, trailing:true, keyIdx: 0});

    onComputed(fn){ return this._em.on('computed-totals', fn); }
    _emComputed(totals){ this._em.emitAsync('computed-totals', totals); }
    onComputedFull(fn){ return this._em.on('computed-totals-full', fn); }
    _emComputedFull(totals){ this._em.emitAsync('computed-totals-full', totals); }

    _setupReactions(){
        const adapter = this;
        this.source.grid$.onValueChanged('lastIdx', () => {
            if (adapter._respectSourceFilters) {
                adapter.hardRefresh();
            }
        });

        // this.context.page.page$.onValueChanged('linkedPivotFilters', (link) => {
        //     adapter.setRespectSourceFilters(link.newValue);
        // })

        this._setupRowTintBuckets();
        this.context.page.page$.onValueChanged('colorizePivot', (colorize) => {
            if (colorize) {
                adapter.enableColorBy('skewScore');
            } else {
                adapter.disableColor();
            }
        });
    }

    // Helpers

    _guessWidthRaw(colId, defaultMin = 50, force = false) {
        try {
            const col = this.api.getColumn(colId);
            const colDef = col && col.colDef;
            if (colDef?.width && !force) {
                return colDef.width;
            }

            const mapVal = v => (colDef?.valueFormatter ? colDef.valueFormatter({ value: v }) : v);
            const n = this.engine.numRows() | 0;
            const min = colDef?.minWidth || defaultMin;
            if (n === 0) return min;

            // Sample up to 200 evenly-spaced rows instead of reading ALL values
            const sampleSize = Math.min(n, 200);
            const step = n / sampleSize;
            const widths = [];
            for (let s = 0; s < sampleSize; s++) {
                const ri = Math.min((s * step) | 0, n - 1);
                const raw = this.engine.getCell(ri, colId);
                if (raw == null) continue;
                const formatted = mapVal(raw);
                if (formatted == null) continue;
                const w = measureText(String(formatted)).width;
                if (w > 0) widths.push(w);
            }

            if (!widths.length) return min;

            const l = quantileSeq(widths, 0.25);
            const u = quantileSeq(widths, 0.75);
            if (widths.length) {
                let ww = widths.filter(w => (l <= w) && (w <= u));
                if (ww.length < 2) return median(widths) || defaultMin;
                return mean(ww) || defaultMin;
            }
            return defaultMin;
        } catch {
            return defaultMin;
        }
    }

    /* ------------------------------ public API ------------------------------ */

    mount(elementOrSelector) {
        const el = (typeof elementOrSelector === 'string')
            ? (typeof document !== 'undefined' ? document.querySelector(elementOrSelector) : null)
            : elementOrSelector;
        if (!el) throw new Error('ArrowAgPivotAdapter.mount: target element not found');

        this.element = el;
        const adapter = this;
        const columnDefs = this._buildPivotColumnDefs();
        this._rebuildSets();

        const treePanel = this.opts.suppressTree ? {} : {
            id: 'treePivotChooser',
            labelDefault: 'Customize Aggregations',
            labelKey: 'treePivotChooser',
            iconKey: 'columns',
            toolPanel: PivotColumnChooser, // Use the new class name
            toolPanelParams: {
                context: this.context,
                adapter: this,
                toolbarId: 'treeColumnChooser-pivot',
                gridName: 'portfolio-pivot',
                globalPresets: this.getGlobalPresets()
            }
        };

        const baseGridOptions = Object.assign({
            rowModelType: 'clientSide',
            columnDefs,
            defaultColDef: {
                hide: false,
                sortingOrder: ['desc', 'asc', null],
                menuTabs: ["filterMenuTab"],
                resizable: true,
                sortable: true,
                filter: true,
                editable: false,
                lockPinned: true,
                floatingFilter: false,
                autoHeight: false,
                wrapText: false,
                suppressMovable: false,
                suppressSizeToFit: true,
                suppressColumnsToolPanel: false,
                suppressSpanHeaderHeight: true,
                cellEditor: CustomCellEditor,
                // suppressKeyboardEvent: (params) => {
                //     return isArrowKey(params.event);
                // },
            },
            animateRows: false,
            suppressColumnVirtualisation: false,
            suppressChangeDetection: false,
            suppressAggFuncInHeader: true,
            suppressMaintainUnsortedOrder: true,
            suppressDragLeaveHidesColumns: true,
            enableRangeSelection: true,
            deltaSort: true,
            suppressAnimationFrame: false,
            suppressAutoSize: true,
            suppressTouch: true,
            suppressColumnMoveAnimation: true,
            suppressRowHoverHighlight: false,
            pagination: false,
            enterNavigatesVerticallyAfterEdit:true,
            suppressMenuHide: false,
            suppressScrollOnNewData: true,
            stopEditingWhenCellsLoseFocus: true,
            suppressClipboardPaste: true,
            suppressCutToClipboard: true,
            copyHeadersToClipboard: false,
            rowHeight: 28,
            headerHeight: 32,
            autoSizeStrategy: {
                type: 'fitCellContents'
            },
            getRowId: (params) => {
                if (!params.data) throw new Error('getRowId: params.data is missing')
                if (params.data.__pid) return String(params.data.__pid);
                const _gk = this.pivotConfig.groupBy;
                const r = {};
                for (let _i = 0; _i < _gk.length; _i++) { if (_gk[_i] in params.data) r[_gk[_i]] = params.data[_gk[_i]]; }
                return this._computePivotRowId(r);
            },
            onDisplayedColumnsChanged: (e) => {
                if (!this.domTimeout) return
                try { this._updateProjection(); } catch (error) {console.error(error)}
                this._schedule('hard');
            },
            onColumnHeaderContextMenu: (e) => {
                e.api.showColumnFilter(e.column.colId);
            },
            onCellKeyDown: async (e) => {
                await adapter.onKeyDown(e)
            },
            onSortChanged: async (e) => {
                const sortModel = e.api.sortController.getSortModel();
                if (sortModel.length === 0) {
                    e.api.applyColumnState({
                        state: [
                            {colId: 'userSide', sort: 'asc', sortIndex: 1},
                            {colId: 'QT', sort: 'desc', sortIndex: 2},
                            {colId: 'skewOrder', sort: 'desc', sortIndex: 3},
                            {colId: 'grossSize', sort: 'desc', sortIndex: 4},
                        ],
                        applyOrder: false // column order
                    })
                }
            },
            onColumnVisible: (e) => {
                try { this._processColumnVisibleEvent(e) } catch {}
            },
            processUnpinnedColumns: (params) => {},
            onToolPanelVisibleChanged: (params) => {
                // params?.api?.treeService?.refresh();
                params?.api?.treeService?.clearSearch();
            },
            getRowClass: (p) => {
                if (p?.node?.rowPinned === 'bottom') {
                    return ['ag-pinned-grand-total-skews']
                }
            },
            onGridReady: (params) => {
                adapter.grid$.set('pivotInitialized', true);
                adapter._em.emitSync("pivot-initialized")
            },
            onFirstDataRendered: (params) => {
                params?.api?.treeService?._initializeStateManagement();
                params?.api?.treeService?.completeInitialization();

                params?.api?.treeService?.setRequiredColumns(
                    [...adapter.opts.requiredGroups, ...adapter.opts.requiredAggregations]
                );

                setTimeout(() => {
                    try { this._updateProjection(); this.hardRefresh(); } catch (error) {console.error(error)}
                    this.domTimeout = true;
                    adapter._notify(ArrowAgGridAdapter.FIRST_EVENT, params);
                }, ArrowAgGridAdapter.LOAD_DELAY)

            },
            getRowStyle: (p) => {
                if (!this._color.enabled) return null;
                const idx = p?.data?.__colorIdx;
                if (idx === -1) return this._color.ifNull;
                if (idx==null) return null;
                const pal = this._color.palette;
                return pal && pal[idx] ? pal[idx] : null;
            },
            sideBar:  this.opts.suppressTree ? {} : {
                toolPanels: [
                    treePanel
                ],
                position: "right",
            },

            icons: {
                columns:'<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" style="opacity:0.75;transform:rotate(90deg)"><g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><rect width="18" height="18" x="3" y="3" rx="2" ry="2"/><path d="M9 17c2 0 2.8-1 2.8-2.8V10c0-2 1-3.3 3.2-3m-6 4.2h5.7"/></g></svg>'
            },
        }, this.opts.gridOptions || {});

        this._grid = createGrid(el, baseGridOptions);
        this.api = this._grid;
        this.gridOptions = baseGridOptions;
        this._setupGridLinks();
        this.hardRefresh({ force: true });
        this.afterMount();

        return this;
    }

    afterMount() {
        this._headerContextMenuController = new AbortController();
        const header = this.element.querySelector('.ag-root .ag-header');
        if (header) {
            header.addEventListener('contextmenu', (e) => {
                e.preventDefault();
            }, { signal: this._headerContextMenuController.signal });
        }
    }

    async onKeyDown(e) {
        try {
            const event = e.event;
            const api = e.api;
            const key = event.key;
            const copyPressed = (event.ctrlKey || event.metaKey) && (key === 'c' || key === 'C') && (!event.shiftKey);
            if (!copyPressed) return;

            const now = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            if (now - lastCopyAt < THROTTLE_MS) return;
            lastCopyAt = now;

            event.preventDefault();

            const ranges = api.getCellRanges ? api.getCellRanges() : null;
            if (!ranges || ranges.length === 0) return;

            const displayedCount = this.engine.numRows();
            if (!displayedCount) return;

            const rowToColPos = new Map();    // ri -> Set(pos)
            const riToNode = new Map();       // ri -> RowNode
            const riVisualIdx = new Map();    // ri -> first-seen visual order index
            const visualOrder = [];           // array of ri in visual encounter order

            const colIdToPos = new Map();     // fallback position map if display order not available
            const posToColId = [];

            const displayCols = typeof api.getAllDisplayedColumns === 'function' ? api.getAllDisplayedColumns() : null;
            const colIdToDisplayPos = new Map();
            if (displayCols && displayCols.length) {
                for (let i = 0; i < displayCols.length; i++) {
                    const c = displayCols[i];
                    const colId = (c && (c.colId || (c.getColId && c.getColId()))) || c;
                    if (colId != null && !colIdToDisplayPos.has(colId)) colIdToDisplayPos.set(colId, i);
                }
            }

            for (let r = 0; r < ranges.length; r++) {
                const rng = ranges[r];
                if (!rng || !rng.columns || rng.columns.length === 0) continue;

                let startIdx = (rng.startRow && typeof rng.startRow.rowIndex === 'number') ? rng.startRow.rowIndex : 0;
                let endIdx = (rng.endRow && typeof rng.endRow.rowIndex === 'number') ? rng.endRow.rowIndex : (displayedCount - 1);
                if (startIdx > endIdx) {
                    const t = startIdx;
                    startIdx = endIdx;
                    endIdx = t;
                }
                if (startIdx < 0) startIdx = 0;
                if (endIdx >= displayedCount) endIdx = displayedCount - 1;

                const cols = rng.columns;
                const posList = new Array(cols.length);
                let posCnt = 0;

                for (let j = 0; j < cols.length; j++) {
                    const c = cols[j];
                    const colId = (c && (c.colId || (c.getColId && c.getColId()))) || c;
                    let pos;
                    if (colIdToDisplayPos.size && colIdToDisplayPos.has(colId)) {
                        pos = colIdToDisplayPos.get(colId);
                        if (posToColId[pos] === undefined) posToColId[pos] = colId;
                    } else {
                        pos = colIdToPos.get(colId);
                        if (pos === undefined) {
                            pos = posToColId.length;
                            colIdToPos.set(colId, pos);
                            posToColId.push(colId);
                        }
                    }
                    posList[posCnt++] = pos;
                }

                for (let i = startIdx; i <= endIdx; i++) {
                    const node = api.getDisplayedRowAtIndex(i);
                    if (!node || !node.data) continue;

                    const ri = (node.data.__srcIndex != null) ? node.data.__srcIndex :
                        (typeof node.rowIndex === 'number' ? node.rowIndex : i);

                    if (!riToNode.has(ri)) riToNode.set(ri, node);
                    if (!riVisualIdx.has(ri)) {
                        riVisualIdx.set(ri, visualOrder.length);
                        visualOrder.push(ri);
                    }

                    let set = rowToColPos.get(ri);
                    if (!set) {
                        set = new Set();
                        rowToColPos.set(ri, set);
                    }
                    for (let p = 0; p < posCnt; p++) set.add(posList[p]);
                }
            }
            if (rowToColPos.size === 0) return;

            const _rowKeys = new Array(rowToColPos.size);
            {
                let k = 0;
                for (const ri of rowToColPos.keys()) _rowKeys[k++] = ri;
            }
            const rowKeys = _rowKeys.toSorted((a, b) => a - b);

            const setsEqual = (a, b) => {
                if (a === b) return true;
                if (!a || !b || a.size !== b.size) return false;
                for (const v of a) if (!b.has(v)) return false;
                return true;
            };

            const groups = [];
            let prevSet = rowToColPos.get(rowKeys[0]);
            let running = [];
            for (let idx = 0; idx < rowKeys.length; idx++) {
                const ri = rowKeys[idx];
                const s = rowToColPos.get(ri);
                if (setsEqual(prevSet, s)) {
                    running.push(ri);
                } else {
                    const minIdx = running.length ? Math.min(...running.map(x => riVisualIdx.get(x))) : Number.MAX_SAFE_INTEGER;
                    groups.push({rows: running.slice(), set: prevSet, orderKey: minIdx});
                    running.length = 0;
                    running.push(ri);
                    prevSet = s;
                }
            }
            if (running.length) {
                const minIdx = Math.min(...running.map(x => riVisualIdx.get(x)));
                groups.push({rows: running.slice(), set: prevSet, orderKey: minIdx});
            }

            groups.sort((a, b) => a.orderKey - b.orderKey);

            const comps = new Array(groups.length);
            for (let g = 0; g < groups.length; g++) {
                const rowsArr = groups[g].rows.slice().sort((a, b) => (riVisualIdx.get(a) - riVisualIdx.get(b)));
                const gSet = groups[g].set;

                const posArr = new Array(gSet.size);
                {
                    let i = 0;
                    for (const p of gSet) posArr[i++] = p;
                }
                posArr.sort((a, b) => a - b);

                const colIds = [];
                for (let i = 0; i < posArr.length; i++) {
                    const cid = posToColId[posArr[i]];
                    if (cid !== undefined) colIds.push(cid);
                }

                comps[g] = this.engine.getDisjointRowObjects(rowsArr, colIds, {
                    includeId: false, useFormats: false, gridApi: api, useHeaders: true
                });
            }

            if (!comps.length) return;

            let h = new HyperTable(comps[0]);
            for (let i = 1; i < comps.length; i++) h = h.bulkOperations({add: comps[i]});

            const headers = (h.rowCount() > 1) || (h.fields().size > 1);
            const obj = h.toArray().map(x => x?.toJSON ? x.toJSON() : x);
            await writeObjectToClipboard(obj, {headers, addCommaToNumerics: true});
            flashSelection(this.selector, this.api);
        } catch (err) {
            console.error('Optimized copy failed:', err);
        }
    }

    _setupGridLinks() {
        // try { this.source.api?.addEventListener?.('filterChanged', this._onSourceFilterChanged); } catch {}
    }

    dispose() {
        try { this._offEpoch && this._offEpoch(); } catch {}
        this._offEpoch = null;
        try { this._offDerived && this._offDerived(); } catch {}
        this._offDerived = null;
        try { this._headerContextMenuController?.abort(); } catch {}
        if (this._debounceTimer) { clearTimeout(this._debounceTimer); this._debounceTimer = null; }
        this._rowMap.clear();
        this._rows = [];
        this._aggByOutputName.clear();
        this._groupSet.clear();
        this._aggInputSet.clear();
        // try { this.source.api?.removeEventListener?.('filterChanged', this._onSourceFilterChanged); } catch {}
    }

    softRefresh({ force = false } = {}) {
        if (!this.api) return;
        if (force) { this._computeAndApply('soft'); return; }
        this._schedule('soft');
    }

    hardRefresh({ force = false } = {}) {
        if (!this.api) return;
        if (force) {
            this._cancelPendingWork();
            this._drainAsyncTx();
            this._computeAndApply('hard');
            return;
        }
        this._schedule('hard');
    }

    updateGroups(groups = [], { hard = true } = {}) {
        this.pivotConfig.groupBy  = Array.from(new Set([...(this.opts.requiredGroups || []), ...asArray(groups)]));
        this._rebuildSets();
        if (hard) this.hardRefresh(); else this.softRefresh();
    }

    addGroups(groups = [], { hard = true } = {}) {
        const current = this.pivotConfig.groupBy;
        this.pivotConfig.groupBy = Array.from(new Set([...current, ...asArray(groups)]));
        this._rebuildSets();
        if (hard) this.hardRefresh(); else this.softRefresh();
    }

    removeGroups(groups = [], { hard = true } = {}) {
        const current = this.pivotConfig.groupBy;
        const rems = new Set(asArray(groups));
        const req = new Set(asArray(this.opts.requiredGroups));
        this.pivotConfig.groupBy = current.filter((x) => (!rems.has(x) || req.has(x)));
        this._rebuildSets();
        if (hard) this.hardRefresh(); else this.softRefresh();
    }

    updateAggregations(aggs = [], { hard = false } = {}) {
        const norm = this._normalizeAggList([...(this.opts.requiredAggregations || []), ...aggs]);
        this.pivotConfig.aggregations = norm;
        this._rebuildSets();
        if (hard) this.hardRefresh(); else this.softRefresh();
    }

    updateColumnDefs(overrides = {}) {
        this.opts.columnOverrides = Object.assign({}, this.opts.columnOverrides || {}, overrides || {});
        if (!this.api) return;
        const defs = this._buildPivotColumnDefs();
        try { this.api.setGridOption('columnDefs', defs); } catch {}
        this.hardRefresh();
    }

    setRequired(groups = [], aggs = []) {
        this.opts.requiredGroups = asArray(groups);
        this.opts.requiredAggregations = this._normalizeAggList(aggs);
        this.updateGroups(this.pivotConfig.groupBy, { hard: false });
        this.updateAggregations(this.pivotConfig.aggregations, { hard: false });
    }

    getState() {
        return {
            groupBy: this.pivotConfig.groupBy.slice(),
            aggregations: this.pivotConfig.aggregations.slice(),
            projection: this._projection.slice(),
        };
    }

    getGlobalPresets() {
        return [
            {
                name: "Default View",
                version: '3.0.0', // Version for cache-busting
                columnState: [
                    { colId: 'isPriced', hide: false, width: 45, pinned: 'left'},
                    { colId: 'userSide', hide: false, width: 80, pinned: 'left'},
                    { colId: 'QT', hide: false, width: 50, pinned: 'left'},
                    { colId: 'grossSize', hide: false, width: 85 },
                    { colId: 'grossPct', hide: false, width: 45 },
                    { colId: 'grossDv01', hide: false, width: 75 },
                    { colId: 'count', hide: false, width: 37 },
                    { colId: 'liqScoreCombined', hide: false, width: 35 },
                    { colId: 'duration', hide: false, width: 45 },
                    { colId: 'signalTotal', hide: false, width: 70 },
                    { colId: 'signalLiveStats', hide: false, width: 70 },

                    { colId: 'TRACESkewAgg', hide: false, width: 75 },
                    { colId: 'BVALSkewAgg', hide: false, width: 75 },
                    { colId: 'MACPSkewAgg', hide: false, width: 75 },
                    { colId: 'ALLQSkewAgg', hide: false, width: 75 },

                    { colId: 'refLevel', hide: false, width: 80, pinned: 'right'},
                    { colId: 'newLevelDisplay', hide: false, width: 80, pinned: 'right' },
                    { colId: 'refSkew', hide: false, width: 100, pinned: 'right' },
                ],
                metaData: {
                    isMutable: false,
                    isTemporary: false,
                    isGlobal: true,
                    isDefault: false,
                    owner: 'Cade Zawacki',
                    lastModified: '2025-11-19T10:29:12.796Z',
                }
            }
        ];
    }
    /* ------------------------------- helpers ------------------------------- */

    _mapDefToField(defs) {return asArray(defs).map(def=>def.field)}
    getAllValidGroupDefs() {
        return this.source.getAllColumnDefs().filter(col=>col?.context?.allowGrouping === true)
    }
    getAllValidAggregationDefs() {
        const a = this.customDefs;
        const f = new Set(); a.forEach(aa => f.add(aa.field));
        const b = this.source.getAllColumnDefs().filter(col => {
            return (col?.context?.allowAggregation === true) && (!f.has(col?.field))
        })
        return [...a, ...b]
    }
    getAllValidGroupFields(){return new Set(this._mapDefToField(this.getAllValidGroupDefs()))}
    getAllValidAggregationFields(){return new Set(this._mapDefToField(this.getAllValidAggregationDefs()))}

    getColumnDef(name) {
        return this.source.api.getColumnDef(name);
    }

    getGridState() {
        return {
            columnState: this.api?.getColumnState?.() || [],
            filterModel: this.api?.getFilterModel?.() || {},
            sortModel:   this.api?.getColumnState?.()
            ?.filter(c => c.sort)
                              ?.map(c => ({ colId: c.colId, sort: c.sort })) || []
        };
    }

    async loadGridState(state) {
        if (!this.api || !state) return;
        if (state.columnState) this.applyColumnState(state.columnState, true);
        if (state.filterModel) this.api.setFilterModel(state.filterModel);
        this.api.refreshHeader();
    }

    applyColumnState(state, applyOrder = true) {
        if (!this.api || !state) return;
        this.api.applyColumnState({ state, applyOrder });
    }

    _processColumnVisibleEvent(e) {
        if (!this.domTimeout) return
        if (!e?.api.treeService?.initialized) return;
        if (e.columns && e.columns.length) {
            e.columns.forEach(col => { this._processColumnVisible(col); })
        } else if (e.column) { this._processColumnVisible(e.column) }
    }

    _processColumnVisible(column, { suppressFlash = false } = {}) {
        const name = column.colId || column?.getColId?.() || column?.colDef?.field || column?.colDef?.colId;
        if (!name) return;

        this._updateProjection();

        if (column.visible) {
            // Move to rightmost unpinned spot & scroll into view (unchanged)
            if (this?.api?.treeService?._filterSelected !== true) {
                this.moveColumn(name, Number.MAX_SAFE_INTEGER, { breakPins: false, toRight: true });
            }
            const api = this.api;
            if (api && typeof api.ensureColumnVisible === 'function') {
                requestAnimationFrame(() => api.ensureColumnVisible(name, 'end'));
            }

            // Visual affordance (unchanged)
            if (!suppressFlash && this.grid$.get('presetLoaded')) this.flashColumnHeader(column);

            try {
                const w = (this._guessWidth(name) ?? 51) * 1.6;
                const guessed = Math.min(Math.max(GUESS_MIN_W, Math.round(w)), GUESS_MAX_W);
                this.api.setColumnWidths([{ key: name, newWidth: guessed }], false);
            } catch {}
        }
    }

    moveColumn(key, index, opts = { breakPins: false, toRight: true, pinned: null, ensureScroll: null }) {
        const api = this.api;
        if (!api) return 0;

        const { pinned = null, ensureScroll = null } = opts || {};

        const toIndex = this._getNewColumnIndex(key, index, opts);
        api.moveColumns([key], toIndex);

        // Re-assert pinning in case grid options (e.g. lockPinned) changed it during move.
        if (pinned === 'left' || pinned === 'right') {
            try { api.setColumnsPinned([key], pinned); } catch {}
        } else {
            try { api.setColumnsPinned([key], null); } catch {}
        }

        if (ensureScroll !== null) {
            // Valid positions: 'start' | 'middle' | 'end' | 'auto' (default).
            try { api.ensureColumnVisible(key, ensureScroll === true ? undefined : ensureScroll); } catch {}
        }

        return toIndex;
    }

    _getNewColumnIndex(key, _index, { toRight = true, breakPins = false, pinned = null } = {}) {
        const api = this.api;
        if (!api) return 0;

        const col = api.getColumn(key);
        if (!col) return 0;

        const side = pinned === 'left' || pinned === 'right' ? pinned : null;

        try {
            api.setColumnsPinned([key], side);
        } catch {}

        const all = api.getAllGridColumns() || [];
        if (!all.length) return 0;
        const after = [];
        for (let i = 0; i < all.length; i++) {
            if (all[i] !== col) after.push(all[i]);
        }
        if (!after.length) return 0;
        let leftCount = 0, centerCount = 0, rightCount = 0;
        for (let i = 0; i < after.length; i++) {
            const p = typeof after[i].getPinned === 'function' ? after[i].getPinned() : null;
            if (p === 'left') leftCount++;
            else if (p === 'right') rightCount++;
            else centerCount++;
        }

        // Compute insertion index within 'after' (i.e., after the removal step AG Grid does internally).
        let toIndex;
        if (side === 'left') {
            toIndex = toRight ? leftCount : 0;
        } else if (side === 'right') {
            toIndex = toRight ? after.length : (leftCount + centerCount);
        } else if (breakPins) {
            toIndex = toRight ? after.length : 0;
        } else {
            // Center only: just after left pins or just before right pins.
            toIndex = toRight ? (leftCount + centerCount) : leftCount;
        }

        if (toIndex < 0) toIndex = 0;
        if (toIndex > after.length) toIndex = after.length;

        return toIndex;
    }

    _processColumnVisible(column, { suppressFlash = false } = {}) {
        const name = column.colId || column?.getColId?.() || column?.colDef?.field || column?.colDef?.colId;
        if (!name) return;

        this._updateProjection();

        if (column.visible) {
            if (column?.colDef?.lockPosition === undefined) {
                let dr;
                if (this.getAllValidAggregationFields().has(name)) {
                    this.moveColumn(name, Number.MAX_SAFE_INTEGER, { breakPins: false, toRight: true });
                    dr = 'end';
                } else if (this.getAllValidGroupFields().has(name)) {
                    this.api.setColumnsPinned([name], 'left');
                    dr = 'start';
                }

                const api = this.api;
                if (api && typeof api.ensureColumnVisible === 'function') {
                    requestAnimationFrame(() => {
                        api.ensureColumnVisible(name, dr)
                    });
                }
            }

            try {
                const actual = column?.colDef?.width;
                const hasWidth = Number.isFinite(actual) && actual > 0;
                if (!hasWidth && name) {
                    const w = (this._guessWidth(name, true) ?? 51) * 1.6;
                    const guessed = Math.min(Math.max(GUESS_MIN_W, Math.round(w)), GUESS_MAX_W);
                    this.api.setColumnWidths([{ key: name, newWidth: guessed }], false);
                }
            } catch {}


        }
    }

    /* ------------------------------- Rxns       ------------------------------- */
    setRespectSourceFilters(on=true){
        this._respectSourceFilters = !!on;
        this.hardRefresh();
    }

    /* ------------------------------- internals ------------------------------- */

    _cancelPendingWork() {
        if (this._debounceTimer) { clearTimeout(this._debounceTimer); this._debounceTimer = null; }
        this._pendingHard = false;
        this._pendingSoft = false;
    }

    _drainAsyncTx() {
        try { this.api?.flushAsyncTransactions?.(); } catch {}
    }

    _isTotalsRow(p) {
        const n = p?.node;
        return !!(n && n.rowPinned === 'bottom');
    }

    _ensureStableTotalIds(rows) {
        const keys = this._grandTotalConfig().groups;
        for (let i = 0; i < rows.length; i++) {
            const r = rows[i];
            if (r.__gid == null) {
                const vals = new Array(keys.length);
                for (let k = 0; k < keys.length; k++) vals[k] = r[keys[k]];
                r.__gid = String(hash64Any(vals));
            }
        }
        return rows;
    }

    _ensureStableRowIds(rows) {
        const keys = this.pivotConfig.groupBy || [];
        for (let i = 0; i < rows.length; i++) {
            const r = rows[i];
            if (r.__pid == null) {
                const vals = new Array(keys.length);
                for (let k = 0; k < keys.length; k++) vals[k] = r[keys[k]];
                r.__pid = String(hash64Any(vals));
            }
        }
        return rows;
    }

    _normalizeAggList(list) {
        const out = [];
        const seenNames = new Set();
        const arr = Array.isArray(list) ? list : (list ? [list] : []);
        for (let i = 0; i < arr.length; i++) {
            const entry = arr[i];
            if (!entry || typeof entry !== 'object') continue;
            const keys = Object.keys(entry);
            if (keys.length !== 1) continue;
            const input = keys[0];
            const spec = Object.assign({}, entry[input] || {});
            spec['inField'] = input;
            const name = spec.name || `${spec.func || 'agg'}_${input}`;
            spec.name = name;
            if (seenNames.has(name)) continue;
            seenNames.add(name);
            out.push({ [input]: spec });
        }
        return out;
    }

    raw_rebuildSets() {
        //console.log('rebuilding sets...')
        this._groupSet = new Set(this.pivotConfig.groupBy || []);
        this._emitGroupChange();
        this._aggByOutputName.clear();
        this._aggInputSet = new Set();
        const aggs = this.pivotConfig.aggregations || [];
        for (let i = 0; i < aggs.length; i++) {
            const entry = aggs[i];
            const input = Object.keys(entry)[0];
            const spec = Object.assign({}, entry[input]);
            const outName = spec.name;
            this._aggByOutputName.set(outName, { input, spec });
            this._aggInputSet.add(input);
            if (spec.weight) this._aggInputSet.add(spec.weight);
        }
    }

    _getPivotNodesMatchingTotalRow(totalRow) {
        const keys = (this._grandTotalConfig().groups || []).slice();
        const nodes = this._getAllClientNodes();
        const out = [];
        outer: for (let i = 0; i < nodes.length; i++) {
            const n = nodes[i];
            if (!n || n.rowPinned) continue;
            const r = n.data;
            for (let k = 0; k < keys.length; k++) {
                const key = keys[k];
                let a = r[key];
                let b = totalRow[key];
                if (a === '' || a === ' ') a = null;
                if (b === '' || b === ' ') b = null;
                if (a === b) continue;
                if (a == null && b == null) continue;
                if (typeof a === 'number' && typeof b === 'number' && Number.isNaN(a) && Number.isNaN(b)) continue;
                if (a instanceof Date || b instanceof Date) {
                    const ax = a instanceof Date ? a.getTime() : +a;
                    const bx = b instanceof Date ? b.getTime() : +b;
                    if (ax === bx) continue;
                }
                continue outer;
            }
            out.push(n);
        }
        return out;
    }

    _distributeGrandTotalEdit(totalRow, mode, rawValue) {
        const nodes = this._getPivotNodesMatchingTotalRow(totalRow);
        const isClear = rawValue === CLEAR_SENTINEL;
        const val = isClear ? CLEAR_SENTINEL : coerceToNumber(rawValue, { onNaN: null });

        const refKey = this._skew.refAggName;
        for (let i = 0; i < nodes.length; i++) {
            const n = nodes[i];
            const r = n.data;
            let pid = r?.__pid;
            if (!pid) continue;
            pid = String(pid);

            const st = this._bucketState.get(pid) || { mode: mode, input: null, proposed: null };
            st.mode = mode;

            if (isClear) {
                st.input = CLEAR_SENTINEL;
                st.proposed = CLEAR_SENTINEL;
                r.input_skew = CLEAR_SENTINEL;
                r.proposed_skew = CLEAR_SENTINEL;
            } else if (val == null) {
                this._bucketState.delete(pid);
                r.input_skew = null;
                r.proposed_skew = null;
                r.sticky = null;
                continue;
            } else if (mode === 'input') {
                st.input = val;
                r.input_skew = val;
                const ref = coerceToNumber(r[refKey] ?? r.current_skew, { onNaN: null });
                r.proposed_skew = ref == null ? null : (ref + val);
                st.proposed = r.proposed_skew;
            } else {
                st.proposed = val;
                r.proposed_skew = val;
                const ref = coerceToNumber(r[refKey] ?? r.current_skew, { onNaN: null });
                const delta = ref == null ? null : (val - ref);
                r.input_skew = delta;
                st.input = delta;
            }

            r.sticky = st.mode;
            this._bucketState.set(pid, st);
        }

        if (nodes.length) {
            try { this.api.refreshCells({ rowNodes: nodes, force: true }); } catch {}
        }
        this._updateGrandTotalSkews();
        this._emitBucketChange();
        return true;
    }

    _getVisibleAggNames() {
        if (!this.api) return Array.from(this._aggByOutputName.keys());
        try {
            const cols = this.api.getAllDisplayedColumns?.() || [];
            const names = [];
            for (let i = 0; i < cols.length; i++) {
                const id = cols[i].getColId?.() || cols[i].colId || cols[i].field;
                if (!id) continue;
                if (!this._groupSet.has(id) && this._aggByOutputName.has(id)) names.push(id);
            }
            return names.length ? names : Array.from(this._aggByOutputName.keys());
        } catch {
            return Array.from(this._aggByOutputName.keys());
        }
    }

    _requiredInputColumns() {
        const cols = new Set(this.pivotConfig.groupBy || []);
        // Only compute aggregations that are currently visible
        const visibleAggs = new Set([
            ...this._getVisibleAggNames(),
            ...this.req_aggs
        ]);
        for (const [name, meta] of this._aggByOutputName.entries()) {
            if (!visibleAggs.has(name)) continue;
            cols.add(meta.input);
            if (meta.spec.weight) cols.add(meta.spec.weight);
        }
        return Array.from(cols);
    }

    _collectRowsForCompute(respect_filters=false) {
        const colNames = this._requiredInputColumns();
        const n = this.engine.numRows()|0;

        let idx = null;
        if (respect_filters) idx = this.source.grid$.get('lastIdx');
        if (!idx) {
            const all = new Int32Array(n);
            for (let i=0;i<n;i++) all[i]=i;
            idx = all;
        }

        // Return columnar format directly — avoids O(n) intermediate object allocations.
        // PivotEngine.toCols() accepts {colName: Array} natively.
        const len = idx.length;
        const result = {};
        for (let c = 0; c < colNames.length; c++) {
            const k = colNames[c];
            const arr = new Array(len);
            for (let i = 0; i < len; i++) arr[i] = this.engine.getCell(idx[i]|0, k);
            result[k] = arr;
        }
        const wfield = (this._ensureWeightState(), this._weight.field);
        const warr = new Array(len);
        for (let i = 0; i < len; i++) warr[i] = this._computeWeightForIndex(idx[i]|0);
        result[wfield] = warr;
        return result;
    }

    // Evaluate current source grid filterModel into an Int32Array of row indices
    _collectFilteredRowIndices() {
        const filterModel = this.source.api?.getFilterModel?.() || {};
        const keys = Object.keys(filterModel);
        const n = this.engine.numRows() | 0;
        if (!keys.length || n === 0) return null;

        let candidateIndices = null;

        // Fast path: Build intersections for number filters we can push down
        for (const col of keys) {
            const model = filterModel[col] || {};
            const filterType = String(model.filterType || model.type || '').toLowerCase();
            if (filterType === 'number' || filterType === 'agnumbercolumnfilter') {
                const opMap = { lessthan:'<', lessthanorequal:'<=', greaterthan:'>', greaterthanorequal:'>=', equals:'==', inrange:'between' };
                const op = opMap[String(model.type || '').toLowerCase()];
                if (op) {
                    let next = null;
                    if (op === 'between') {
                        const lo = this._coerceNumber(model.filter);
                        const hi = this._coerceNumber(model.filterTo);
                        if (lo != null && hi != null) next = this.engine.queryRange({ column: col, op:'between', lo, hi });
                    } else {
                        const v = this._coerceNumber(model.filter);
                        if (v != null) next = this.engine.queryRange({ column: col, op, value: v });
                    }
                    if (next && next.length) {
                        candidateIndices = candidateIndices ? this._intersectIdx(candidateIndices, next) : next;
                        if (candidateIndices.length === 0) return candidateIndices;
                    }
                }
            }
        }

        // Build remaining predicates (text/set/date/number not pushed down)
        const predicates = [];
        for (const col of keys) {
            const model = filterModel[col] || {};
            const p = this._buildFilterPredicate(col, model);
            if (p) predicates.push(p);
        }
        if (!predicates.length) return candidateIndices || null;

        const base = candidateIndices || (() => { const a = new Int32Array(n); for (let i=0; i<n; i++) a[i]=i; return a; })();
        const kept = new Int32Array(base.length);
        let k = 0;

        // Optimized evaluation loop
        for (let i = 0; i < base.length; i++) {
            const ri = base[i] | 0;
            let passesAll = true;
            for (const pred of predicates) {
                // Get value once, pass to predicate
                const value = this.engine.getCell(ri, pred.column);
                if (!pred.test(value)) {
                    passesAll = false;
                    break;
                }
            }
            if (passesAll) {
                kept[k++] = ri;
            }
        }
        return kept.subarray(0, k);
    }

    _intersectIdx(a, b) {
        // Both Int32Array, assume sorted? engine.queryRange returns ascending; intersect in O(n).
        let i=0,j=0,k=0;
        const out = new Int32Array(Math.min(a.length,b.length));
        while(i<a.length && j<b.length){
            const x=a[i]|0, y=b[j]|0;
            if (x===y){ out[k++]=x; i++; j++; }
            else if (x<y) i++; else j++;
        }
        return out.subarray(0,k);
    }

    _buildFilterPredicate(col, model) {
        if (!model || typeof model !== 'object') return null;
        const ft = String(model.filterType || '').toLowerCase();

        // Handle compound (AND/OR) recursively
        if (model.operator && (model.condition1 || model.condition2)) {
            const pred1 = this._buildFilterPredicate(col, model.condition1);
            const pred2 = this._buildFilterPredicate(col, model.condition2);
            if (!pred1 && !pred2) return null;
            const op = String(model.operator).toUpperCase() === 'OR' ? 'OR' : 'AND';
            if (op === 'OR') {
                return { column: col, test: (v) => (pred1?.test(v) || false) || (pred2?.test(v) || false) };
            }
            return { column: col, test: (v) => (pred1 ? pred1.test(v) : true) && (pred2 ? pred2.test(v) : true) };
        }

        // Text Filter
        if (ft === 'text' || ft === 'agtextcolumnfilter') {
            const type = String(model.type || 'contains').toLowerCase();
            if (type === 'blank') return { column: col, test: (v) => v == null || v === '' };
            if (type === 'notblank') return { column: col, test: (v) => !(v == null || v === '') };

            const query = String(model.filter ?? '').toLowerCase();
            if (!query) return null;

            const testFn = (cellValue) => {
                const v = String(cellValue ?? '').toLowerCase();
                switch (type) {
                    case 'equals': return v === query;
                    case 'notequals': return v !== query;
                    case 'startswith': return v.startsWith(query);
                    case 'endswith': return v.endsWith(query);
                    case 'notcontains': return v.indexOf(query) === -1;
                    default: return v.indexOf(query) !== -1;
                }
            };
            return { column: col, test: testFn };
        }

        // Set Filter
        if (ft === 'set' || ft === 'agsetcolumnfilter') {
            const valueSet = Array.isArray(model.values) ? new Set(model.values.map(String)) : null;
            if (!valueSet) return null;
            return { column: col, test: (v) => valueSet.has(String(v)) };
        }

        // Date Filter
        if (ft === 'date' || ft === 'agdatecolumnfilter') {
            const type = String(model.type || 'equals').toLowerCase();
            const toMs = (x) => (x instanceof Date) ? x.getTime() : (x ? new Date(x).getTime() : NaN);
            const v1 = toMs(model.dateFrom), v2 = toMs(model.dateTo);

            const getCellMs = (cellValue) => {
                if (cellValue == null) return NaN;
                if (cellValue instanceof Date) return cellValue.getTime();
                const n = Number(cellValue);
                return Number.isFinite(n) && n > 0 ? n : toMs(cellValue);
            };

            const testFn = (cellValue) => {
                const t = getCellMs(cellValue);
                if (!Number.isFinite(t)) return false;
                switch (type) {
                    case 'equals': return this._sameDay(t, v1);
                    case 'inrange': return t >= v1 && t <= v2;
                    case 'lessthan': return t < v1;
                    case 'greaterthan': return t > v1;
                    default: return false;
                }
            };
            return { column: col, test: testFn };
        }

        // Number Filter
        if (ft === 'number' || ft === 'agnumbercolumnfilter') {
            const type = String(model.type || 'equals').toLowerCase();
            const f = this._coerceNumber(model.filter);
            const t = this._coerceNumber(model.filterTo);
            const getCellNumber = (v) => {
                const n = typeof v === 'number' ? v : Number(v);
                return Number.isFinite(n) ? n : NaN;
            };

            const testFn = (cellValue) => {
                const x = getCellNumber(cellValue);
                if (isNaN(x)) return false;
                switch (type) {
                    case 'equals': return x === f;
                    case 'notequals': return x !== f;
                    case 'lessthan': return x < f;
                    case 'lessthanorequal': return x <= f;
                    case 'greaterthan': return x > f;
                    case 'greaterthanorequal': return x >= f;
                    case 'inrange': return x >= f && x <= t;
                    default: return false;
                }
            };
            return { column: col, test: testFn };
        }

        return null;
    }

    _sameDay(aMs, bMs) {
        if (!Number.isFinite(aMs) || !Number.isFinite(bMs)) return false;
        const A = new Date(aMs), B = new Date(bMs);
        return A.getFullYear() === B.getFullYear() && A.getMonth() === B.getMonth() && A.getDate() === B.getDate();
    }

    _coerceNumber(x) {
        return coerceToNumeric(x)
    }


    _activeAggregationsForCompute() {
        this._ensureWeightState();
        const names = new Set([
            ...this._getVisibleAggNames(),
            ...this.req_aggsOuts
        ]);
        names.add(this._skew.refAggName);

        const out = [];
        for (const [name, meta] of this._aggByOutputName.entries()) {
            if (!names.has(name)) continue;
            const spec0 = meta?.spec || {};
            const spec = Object.assign({}, spec0);
            if (String(spec.func).toLowerCase() === 'wavg') {
                spec.weight = this._weight.field; // pivot engine will read this property from row objects
            }
            out.push({ [meta.input || spec.inField || name]: spec });
        }

        // Ensure a sum over the virtual weight exists for robust GRAND TOTALS weighting
        out.push({ [this._weight.field]: { inField: this._weight.field, func: 'sum', name: '__w_sum__' } });
        return out;
    }

    _computePivotRowId(row, keys=null) {
        keys = keys ?? (this.pivotConfig.groupBy || []);
        const vals = new Array(keys.length);
        for (let i = 0; i < keys.length; i++) vals[i] = row[keys[i]];
        return String(hash64Any(vals));
    }

    _indexRows(rows) {
        const map = new Map();
        for (let i = 0; i < rows.length; i++) {
            const r = rows[i];
            const id = r && r.__pid != null ? String(r.__pid) : '';
            if (id) map.set(id, r);
        }
        return map;
    }

    _buildPivotColumnDefs() {
        let srcDefs = this.source.getAllColumnDefs() || [];
        srcDefs = srcDefs.concat(asArray(this.customDefs).map(def=>this.source.filterManager.configureFilter(def)));
        this.srcById = new Map();

        for (let i = 0; i < srcDefs.length; i++) {
            let d = srcDefs[i] || {};
            const id = d.colId || d.field;
            const o = this.opts.columnOverrides?.[id];
            if (o) {
                d = recursiveMerge.all([d, o]);
            }
            if (id) this.srcById.set(id, d);
        }

        const adapter = this;

        const groupDefs = [];
        const g = Array.from(this.getAllValidGroupFields());
        for (let i = 0; i < g.length; i++) {
            const id = g[i];
            const base = Object.assign({}, this.srcById.get(id) || { colId: id, field: id, headerName: id });
            base.colId = id;
            base.field = id;
            base.hide = !this.req_gps.has(id);
            base.sortable = true;
            base.editable = false;
            base.pinned = 'left';
            base.lockPinned = false;
            base.context = base?.context || {};
            base.context.customColumnGroup = 'Groups/' + (base ?.context?.customColumnGroup ?? '')
            groupDefs.push(base);

            if (id === 'refLevel') base.pinned = 'right'
            if (id === 'refSkew') base.pinned = 'right'
            if (id === 'newLevelDisplay') base.pinned = 'right'

        }

        const aggDefs = [];
        for (const { input, spec } of this._iterAggs()) {
            const outName = spec.name;
            const inName = spec.inField || spec.name;
            const base = this.srcById.get(outName) || this.srcById.get(inName) || {};
            const col = {
                ...base,
                editable: false,
                colId: outName,
                field: outName,
                headerName: spec.headerName || outName,
                hide: true,
                sortable: true,
            };
            col.context = col?.context || {};
            col.context.customColumnGroup = 'Aggregations/' + (col ?.context?.customColumnGroup ?? '')

            if (outName === 'input_skew') {
                col.valueSetter = (p) => adapter._onEditInputSkew(p);
                col.editable = true;
                col.pinned = 'left';
                col.lockPinned = true;
                const cc = col.cellStyle;
                if (typeof cc === 'function') {
                    col.cellStyle = (params) => {
                        return cc(params, this)
                    }
                }

            } else if (outName === 'proposed_skew') {
                col.valueSetter = (p) => adapter._onEditProposedSkew(p);
                col.editable = true;
                col.pinned = 'left';
                col.lockPinned = true;
                const cc = col.cellStyle;
                if (typeof cc === 'function') {
                    col.cellStyle = (params) => {
                        return cc(params, this)
                    }
                }
            } else if (outName === 'current_skew') {
                col.pinned = 'left';
                col.lockPinned = true;
            } else {
                col.cellClass = ['ag-numeric-cell'];
                col.pinned = null;

            }
            aggDefs.push(col);
        }
        return groupDefs.concat(aggDefs);
    }

    _applyGrandTotalEdit(p, mode /* 'input' | 'proposed' */) {
        try {
            const d = p?.data; if (!d) return false;
            let val = p.newValue;
            // Strip trailing % sign
            if (val != null && typeof val === 'string') {
                val = val.replace(/%\s*$/, '').trim();
            }
            const isClear = val === CLEAR_SENTINEL;
            if (!isClear) {
                val = coerceToNumber(val, { onNaN: null });
                if (val == null) return false;
            }

            const side = d.userSide;
            const qt   = d.QT;
            const useFull = this._lockGridTotals || (!this._respectSourceFilters);

            // Pick universe of target rows
            let targets = [];
            if (useFull) {
                const all = Array.isArray(this._pivotResultFull) ? this._pivotResultFull : [];
                for (let i = 0; i < all.length; i++) {
                    const r = all[i]; if (!r || r.__pid == null) continue;
                    if (r.userSide === side && r.QT === qt) targets.push(r);
                }
            } else {
                const nodes = this._getAllClientNodes();
                for (let i = 0; i < nodes.length; i++) {
                    const n = nodes[i]; const r = n?.data; if (!r || r.__pid == null) continue;
                    if (n?.rowPinned) continue;
                    if (r.userSide === side && r.QT === qt) targets.push(r);
                }
            }
            if (!targets.length) return true;

            // Apply to bucket state + mutate visible row objects when present
            const refKey = this._skew.refAggName;
            for (let i = 0; i < targets.length; i++) {
                const row = targets[i];
                const pid = String(row.__pid);
                const ref = coerceToNumber(row[refKey] ?? row.current_skew, { onNaN: null });

                if (isClear) {
                    this._bucketState.set(pid, { mode: mode, input: CLEAR_SENTINEL, proposed: CLEAR_SENTINEL });
                    // mutate when this row object is on-screen
                    row.input_skew = CLEAR_SENTINEL;
                    row.proposed_skew = CLEAR_SENTINEL;
                    row.sticky = mode;
                    continue;
                }

                if (mode === 'input') {
                    const proposed = ref == null ? null : (ref + val);
                    this._bucketState.set(pid, { mode:'input', input: val, proposed });
                    row.input_skew = val;
                    row.proposed_skew = proposed;
                    row.sticky = 'input';
                } else {
                    const implied = ref == null ? null : (val - ref);
                    this._bucketState.set(pid, { mode:'proposed', input: implied, proposed: val });
                    row.proposed_skew = val;
                    row.input_skew = implied;
                    row.sticky = 'proposed';
                }
            }

            this.api.applyTransaction({update:targets});

            // If we edited across FULL set, mirror state into visible rows quickly
            if (useFull && Array.isArray(this._rows) && this._rows.length) {
                this._applyInteractiveSkewColumns(this._rows);
            }

            // Recompute pinned totals for correct universe and repaint
            try { this.api.refreshCells({ force: true, columns: ['input_skew','proposed_skew'] }); } catch {};
            this._updateGrandTotalSkews()
            return true;
        } catch {
            return false;
        }
    }

    _onEditInputSkew(p) {
        if (this._isPinnedTotalsEdit(p)) {
            this._applyGrandTotalEdit(p, 'input');
            this._emitBucketChange();
            return
        }
        try {
            const d = p?.data; if (!d) return false;
            let pid = d.__pid; if (!pid) return false;
            pid = String(pid);

            // Check for 'x' / CLEAR_SENTINEL before coercing to number
            if (p.newValue != null && String(p.newValue).toLowerCase() === 'x') {
                p.newValue = CLEAR_SENTINEL;
            }

            // Strip trailing % sign (percent mode: user may type "50%")
            if (p.newValue != null && typeof p.newValue === 'string') {
                p.newValue = p.newValue.replace(/%\s*$/, '').trim();
            }

            if (p.newValue === CLEAR_SENTINEL) {
                const st = this._bucketState.get(pid) || { mode: 'input', input: 0, proposed: null };
                st.mode = 'input';
                st.input = CLEAR_SENTINEL;
                st.proposed = CLEAR_SENTINEL;
                d.input_skew = CLEAR_SENTINEL;
                d.proposed_skew = CLEAR_SENTINEL;
                d.sticky = st.mode;
                this._bucketState.set(pid, st);
                return true;
            }

            const v = coerceToNumber(p.newValue, {onNaN:null});

            if (v == null) {
                this.resetBucketState(d);
                return
            }

            const st = this._bucketState.get(pid) || { mode: 'input', input: 0, proposed: null };
            st.mode = 'input';

            st.input = v;
            d.input_skew = st.input;
            const ref = coerceToNumber(d[this._skew.refAggName] ?? d.current_skew, {onNaN:null});
            d.proposed_skew = (ref == null) ? null : (ref + st.input);
            st.proposed = d.proposed_skew;
            d.sticky = st.mode;
            this._bucketState.set(pid, st);
            return true;
        } catch { return false; }
        finally {
            this._updateGrandTotalSkews();
            this._emitBucketChange();
        }
    }

    _onEditProposedSkew(p) {
        if (this._isPinnedTotalsEdit(p)) {
            this._applyGrandTotalEdit(p, 'proposed');
            this._emitBucketChange();
            return
        }
        try {
            const d = p?.data; if (!d) return false;
            let pid = d.__pid; if (!pid) return false;
            pid = String(pid);

            // Check for 'x' / CLEAR_SENTINEL before coercing to number
            if (p.newValue != null && String(p.newValue).toLowerCase() === 'x') {
                p.newValue = CLEAR_SENTINEL;
            }

            // Strip trailing % sign
            if (p.newValue != null && typeof p.newValue === 'string') {
                p.newValue = p.newValue.replace(/%\s*$/, '').trim();
            }

            if (p.newValue === CLEAR_SENTINEL) {
                const st = this._bucketState.get(pid) || { mode: 'proposed', input: 0, proposed: null };
                st.mode = 'proposed';
                st.input = CLEAR_SENTINEL;
                st.proposed = CLEAR_SENTINEL;
                d.input_skew = CLEAR_SENTINEL;
                d.proposed_skew = CLEAR_SENTINEL;
                d.sticky = st.mode;
                this._bucketState.set(pid, st);
                return true;
            }

            const v = coerceToNumber(p.newValue, {onNaN:null});
            if (v == null) {
                this.resetBucketState(d);
                return
            }

            const st = this._bucketState.get(pid) || { mode: 'proposed', input: 0, proposed: null };
            st.mode = 'proposed';

            st.proposed = v;
            d.proposed_skew = st.proposed;
            const ref = coerceToNumber(d[this._skew.refAggName] ?? d.current_skew, {onNaN:null});
            st.input  = (ref == null) ? null : (v - ref);
            d.input_skew = st.input;
            d.sticky = st.mode;
            this._bucketState.set(pid, st);
            return true;
        } catch { return false; }
        finally {
            this._updateGrandTotalSkews();
            this._emitBucketChange();
        }
    }

    _isPinnedTotalsEdit(p) {
        if (!p) return false;
        if (p?.node?.rowPinned === 'bottom') return true;
        const d = p.data || {};
        return d.__gid != null && d.__pid == null;
    }

    async _compute_updateGrandTotalSkews(rows, totalRows, sort=true) {
        this._ensureWeightState();
        this._ensureStableTotalIds(rows);

        const input = new Map();
        const prop = new Map();
        const sizes = new Map();
        const nulls = new Map();
        const keys = new Set();

        for (let ri = 0; ri < rows.length; ri++) {
            const row = rows[ri];
            const k = row.__gid;
            keys.add(k);

            const s = coerceToNumber(this._rowWeight(row), {onNaN:0});
            const c = coerceToNumber(row?.current_skew, {onNaN:0});
            let i = coerceToNumber(row?.input_skew, {onNaN:null});
            const pt =  coerceToNumber(row?.proposed_skew, {onNaN:null});

            let p;
            if (i === CLEAR_SENTINEL || pt === CLEAR_SENTINEL) {
                i = 0; p = 0;
            } else {
                if (i == null) {
                    if (pt == null) {
                        nulls.set(k, (nulls.get(k)??0) + s);
                    }
                    i = 0;
                }
                p = c + i;
            }

            sizes.set(k, (sizes.get(k)??0) + s);
            input.set(k, (input.get(k)??0) + (i*s));
            prop.set(k, (prop.get(k)??0) + (p*s));
        }

        const totals = totalRows.map(row => {
            row.input_skew = null;
            row.proposed_skew = null;
            if (!keys.has(row.__gid)) return row;
            const s = sizes.get(row.__gid);
            if (!s) return row;
            const n = nulls.get(row.__gid);
            if (n && n === s) {
                row.input_skew = null;
                row.proposed_skew = null;
            } else {
                const c = row.current_skew;
                const i = input.get(row.__gid);
                const p = prop.get(row.__gid);
                let ic = coerceToNumber(i, {onNaN:null});
                let pc = coerceToNumber(p, {onNaN:null});
                if (ic !== null) {
                    if (pc == null) pc = ic + c;
                } else if (pc !== null) {
                    ic = pc - c;
                } else {
                    row.input_skew = null;
                    row.proposed_skew = null;
                    return row;
                }

                row.input_skew = ic / s;
                row.proposed_skew = pc / s;
            }
            return row;
        });
        if (!sort) return totals;
        return totals.toSorted((a,b) => {
            if (a?.skewScore !== b?.skewScore) {
                if (a?.skewScore == null) return 1;
                if (b?.skewScore == null) return -1;
                return a.skewScore - b.skewScore;
            }
            if (a?.userSide !== b?.userSide) return (a?.userSide?.toString() ?? '').localeCompare(b?.userSide?.toString() ?? '');
            if (a?.QT !== b?.QT) return (b?.QT?.toString() ?? '').localeCompare(a?.QT?.toString() ?? '');
            return 0;
        });
    }

    async _debounced_updateGrandTotalSkews() {
        const useFull = !!(this._respectSourceFilters && this._lockGridTotals);

        if (useFull) {
            // Work from full pivot result, but inject interactive skews first so totals reflect edits.
            const rows = Array.isArray(this._pivotResultFull) ? this._pivotResultFull.map(r => ({...r})) : [];
            // this._applyInteractiveSkewColumns(rows);
            const totalsIn = Array.isArray(this.totalRowsFull) ? this.totalRowsFull : [];
            this.totalRowsFull = await this._compute_updateGrandTotalSkews(rows, totalsIn, true);
            try { this._emComputedFull(this.totalRowsFull); } catch {}
            requestAnimationFrame(()=> {
                try { this.api.setGridOption('pinnedBottomRowData', this.totalRowsFull); } catch {}
            });
            this._emitBucketChange();
            return;
        }

        // Default: use visible grid rows (filtered) for totals
        const nodes = this._getAllClientNodes();
        const rows = nodes.map(n => ({...n.data}));
        this.totalRows = await this._compute_updateGrandTotalSkews(rows, this.totalRows, true);
        try { this._emComputed(this.totalRows); } catch {}
        requestAnimationFrame(()=> {
            try { this.api.setGridOption('pinnedBottomRowData', this.totalRows); } catch {}
            this._emitBucketChange();
        });
    }

    async applyBucketSkew(pivotRow, { respectFilters = this._respectSourceFilters } = {}) {
        if (!pivotRow) return []

        let pid = pivotRow.__pid;
        if (!pid) return [];
        pid = String(pid);
        const st = this._bucketState.get(pid) || null;
        if (st == null) return

        const baseRef = coerceToNumber(pivotRow.current_skew, {onNaN:null});
        const i = coerceToNumber(st?.input, {onNaN:null});
        const p = coerceToNumber(st?.proposed, {onNaN:null});

        const ref = baseRef === null ? 0 : baseRef;

        const isPercent = this._skewMode === 'percent';

        // Determine delta to add per source row
        let delta;
        let pctInput = null; // raw percentage value for percent mode
        if (st && (st?.proposed === CLEAR_SENTINEL || st?.input === CLEAR_SENTINEL)) {
            delta = CLEAR_SENTINEL;
        } else if (st && st.mode === 'proposed' && p != null && ref != null) {
            delta = p - ref;
        } else if (st && st.mode === 'input' && i != null) {
            if (isPercent) {
                pctInput = i; // keep raw pct; convert per row below
                delta = i;   // placeholder; will be overridden per row
            } else {
                delta = i;
            }
        }
        if (delta == null) return []
        if (delta !== CLEAR_SENTINEL && !Number.isFinite(delta)) return [];
        this._inBulkApply = true;
        try {
            let idx = this.getSourceRowIndicesForPivotRow(pivotRow);
            if (idx && respectFilters) {
                const filteredRows = new Set(this.source.grid$.get('lastIdx'));
                const idxFiltered = [];
                for (let fi = 0; fi < idx.length; fi++) { if (filteredRows.has(idx[fi])) idxFiltered.push(idx[fi]); }
                idx = idxFiltered;
            }
            const col = this._skew.sourceSkewColumn;
            const tasks = [];
            let j = 0;
            while (j < idx.length) {
                const ri = idx[j++] | 0;
                const cur = this.engine.getCell(ri, col);
                const id = this.engine.getRowIdByIndex(ri);
                let val;
                if (delta === CLEAR_SENTINEL) {
                    val = null;
                } else if (isPercent && st.mode === 'input' && pctInput != null) {
                    // Percent mode: convert pct to absolute skew per row
                    const width = this._getWidthForPercentSkew(ri);
                    if (width != null && Number.isFinite(width)) {
                        const rowDelta = (pctInput / 100) * width;
                        val = Number.isFinite(+cur) ? (+cur + rowDelta) : rowDelta;
                    } else {
                        // No width data; skip this row
                        continue;
                    }
                } else if (st.mode === 'proposed') {
                    val = p;
                } else {
                    val = Number.isFinite(+cur) ? (+cur + delta) : delta;
                }

                // Build per-row update payload, respecting input vs proposed
                // market preservation rules.
                const task = { [this.engine._idProperty]: id };
                const existingTargetMkt = this.engine.getCell(ri, 'relativeSkewTargetMkt');

                if (delta === CLEAR_SENTINEL || st.mode === 'proposed' || !existingTargetMkt) {
                    // Proposed mode, clear, or no existing target: use refSkew
                    // inverse which resolves market from current active settings.
                    task[col] = val;
                } else {
                    // Input mode with existing relativeSkewTargetMkt: preserve
                    // the existing market/side/qt, only adjust the skew value.
                    // Compute new relativeSkewValue = existing + delta, then
                    // let the engine recompute newLevel* from the pinned target.
                    const existingSkewVal = coerceToNumber(
                        this.engine.getCell(ri, 'relativeSkewValue'), {onNaN: 0}
                    );
                    let rowDelta;
                    if (isPercent && pctInput != null) {
                        const width = this._getWidthForPercentSkew(ri);
                        rowDelta = (width != null && Number.isFinite(width))
                            ? (pctInput / 100) * width : null;
                    } else {
                        rowDelta = delta;
                    }
                    if (rowDelta == null) { j; continue; }
                    task['relativeSkewValue'] = existingSkewVal + rowDelta;
                }
                tasks.push(task);
            }
            return tasks;
        } finally {
            this._inBulkApply = false;
        }
    }

    async applyAllBuckets({ respectFilters = this._respectSourceFilters } = {}) {
        const nodes = this._getAllClientNodes();
        const updates = [];
        for (let k=0;k<nodes.length;k++){
            const row = nodes[k]?.data; if (!row) continue;
            let pid = row.__pid; if (!pid) continue;
            const update = await this.applyBucketSkew(row, { respectFilters });
            if (update) { for (let u=0;u<update.length;u++) updates.push(update[u]); }
        }
        //console.log('UPDATES', updates)
        // try { this.source.api?.refreshServerSide?.({ purge:false }); } catch {}
        if (updates.length) {
            await this.source.applyServerUpdateTransaction(updates, {emitAsEdit:true})
            this.resetAllBuckets();
        }
    }

    resetBucketState(pivotRow){
        let pid = pivotRow?.__pid; if (!pid) return;
        pid = String(pid);
        this._bucketState.delete(pid);
        this.hardRefresh({force:true});
        this._updateGrandTotalSkews();
        this._emitBucketChange();
    }

    _getAllClientNodes() {
        const nodes = [];
        if (this?.api) this?.api.forEachNode((x)=>nodes.push(x));
        return nodes;
    }

    input_clear_on_each_row() {
        const nodes = this._getAllClientNodes();
        nodes.forEach(node => {
            node.setDataValue('input_skew', CLEAR_SENTINEL)
        });
    }

    resetAllBuckets() {
        const nodes = this._getAllClientNodes();
        for (let k=0;k<nodes.length;k++){
            const row = nodes[k]?.data; if (!row) continue;
            const pid = row?.__pid; if (!pid) continue;
            this._bucketState.delete(String(pid));
        }
        this.hardRefresh({force:true});
        this._updateGrandTotalSkews();
        this._emitBucketChange();
    }

    _applyInteractiveSkewColumns(rows) {
        const refKey = this._skew.refAggName; // e.g., 'refSkew'
        for (let i=0;i<rows.length;i++){
            const r = rows[i];
            const pid = String(r.__pid);
            const ref = r[refKey]; // computed wavg
            const state = this._bucketState.get(pid) || null;

            // current_skew mirrors refSkew live
            r.current_skew = coerceToNumber(ref, {onNaN: 0});

            if (!state) {
                r.input_skew = null;
                r.proposed_skew = null
                continue;
            }

            if (
                (r.input_skew === CLEAR_SENTINEL) ||
                (r.proposed_skew === CLEAR_SENTINEL) ||
                (state.input === CLEAR_SENTINEL) ||
                (state.proposed === CLEAR_SENTINEL)
            ) {
                r.input_skew = CLEAR_SENTINEL;
                r.proposed_skew = CLEAR_SENTINEL;
                continue
            }

            if (state.mode === 'input') {
                const delta = Number.isFinite(+state.input) ? +state.input : 0;
                r.input_skew = delta;
                r.proposed_skew = (ref == null || !Number.isFinite(+ref)) ? null : (+ref + delta);
            } else if (state.mode === 'proposed') {
                // proposed is sticky; compute implied input from current ref
                const proposed = Number.isFinite(+state.proposed) ? +state.proposed : null;
                r.proposed_skew = proposed;
                const delta = (proposed == null || ref == null || !Number.isFinite(+ref))
                    ? null
                    : (proposed - (+ref));
                r.input_skew = (delta == null || !Number.isFinite(delta)) ? 0 : delta;
                // store the implied input so UI shows consistent value
                state.input = r.input_skew;
            } else {
                r.input_skew = 0;
                r.proposed_skew = (ref == null || !Number.isFinite(+ref)) ? null : (+ref);
            }
        }

    }

    *_iterAggs() {
        const seen = new Set();
        const a = this.pivotConfig.aggregations || [];
        for (let i = 0; i < a.length; i++) {
            const input = Object.keys(a[i])[0];
            const spec = a[i][input];
            seen.add(spec.name ?? input);
            yield { input, spec };
        }
        const b = this.getAllValidAggregationDefs() || [];
        for (let i = 0; i < b.length; i++) {
            const entry = b[i];
            const input = this._tieredColName(entry);
            const inField = entry?.context?.aggregationField || input;
            if (input && !seen.has(input)) {
                const spec = {
                    name: input,
                    inField: inField,
                    headerName: entry?.context?.aggregationName || entry?.headerName || input,
                    oHeader: entry?.headerName || inField || input,
                    func:  entry?.context?.aggregationFunction || 'count',
                    weight: entry?.context?.weightField || 'grossSize',
                    DROP_NULLS: entry?.context?.DROP_NULLS,
                    FILL_NULL: entry?.context?.FILL_NULL,
                    ZERO_AS_NULL: entry?.context?.ZERO_AS_NULL,
                }
                yield { inField, spec };
            }
        }
    }

    setLockGridTotals(on = false) {
        this._lockGridTotals = !!on;
        this.element.classList.toggle('totals-locked', !!on);
        try {
            this._updateGrandTotalSkews();
            this._emitBucketChange();
        } catch {}
    }

    setSkewMode(mode) {
        this._skewMode = (mode === 'percent') ? 'percent' : 'outright';
    }

    getSkewMode() { return this._skewMode; }

    setSkewPctMarket(mkt) {
        this._skewPctMarket = mkt || null;
    }

    getSkewPctMarket() { return this._skewPctMarket; }

    /**
     * For a given source row, compute the bid-mid or ask-mid width from the
     * percent-width market.  Returns the signed width value such that
     *   skew = (pct / 100) * width
     *
     * For BID side:  width = bid - mid   (positive when bid > mid — normal)
     * For OFFER side: width = ask - mid  (positive when ask > mid — normal)
     * Sign-flip for negative percentages is handled by the caller.
     */
    _getWidthForPercentSkew(ri) {
        const engine = this.engine;
        const mkt = this._skewPctMarket || this.context.page.page$.get('activeMarket') || 'Dynamic';
        const qt = engine.getCell(ri, 'QT');
        const userSide = engine.getCell(ri, 'userSide');

        // Determine bid and ask values for the width market (with waterfall)
        const bidRaw = getRefLevelRaw(ri, mkt, 'Bid', qt, engine, { waterfall: true });
        const askRaw = getRefLevelRaw(ri, mkt, 'Ask', qt, engine, { waterfall: true });
        const midRaw = getRefLevelRaw(ri, mkt, 'Mid', qt, engine, { waterfall: true });

        const bid = bidRaw?.value != null ? +bidRaw.value : null;
        const ask = askRaw?.value != null ? +askRaw.value : null;
        let mid = midRaw?.value != null ? +midRaw.value : null;

        // If mid is missing, try to compute from bid+ask
        if (mid == null && bid != null && ask != null) mid = (bid + ask) / 2;
        if (mid == null) return null;

        // BID side: use bid-mid width. OFFER side: use ask-mid width.
        const isBid = String(userSide).toUpperCase() === 'BID';
        if (isBid) {
            return bid != null ? (bid - mid) : null;
        } else {
            return ask != null ? (ask - mid) : null;
        }
    }

    _makeSpecFromDef(def) {
        const input = this._tieredColName(def);
        const inField = def?.context?.aggregationField || input;
        return {
            name: input,
            inField: inField,
            headerName: def?.context?.aggregationName || def?.headerName || input,
            func:  def?.context?.aggregationFunction || 'count',
            weight: def?.context?.weightField || 'grossSize',
            oHeader: def?.headerName || inField || input,
            DROP_NULLS: def?.context?.DROP_NULLS,
            FILL_NULL: def?.context?.FILL_NULL,
            ZERO_AS_NULL: def?.context?.ZERO_AS_NULL,
        }
    }

    _tieredColName(def) {
        return def?.field || def?.colId;
    }

    _mergeOverride(def) {
        const id = def.colId || def.field;
        const o = (this.opts.columnOverrides || {})[id];
        return o ? Object.assign({}, def, o) : def;
    }

    _updateProjection() {
        if (!this.api || !this.api.getAllDisplayedColumns) return;
        const cols = this.api.getAllDisplayedColumns();
        const names = new Array(cols.length);
        const aggs = []; const groups = [];
        for (let i = 0; i < cols.length; i++) {
            const col = cols[i].getColId();
            names[i] = col;
            const colDef = cols[i].getColDef();
            if (this.getAllValidAggregationFields().has(col)) {
                const spec = this._makeSpecFromDef(colDef);
                const inField = spec?.inField || spec.name;
                if (spec) aggs.push({[inField]:spec})
            } else if (this.getAllValidGroupFields().has(col)) {
                groups.push(col);
            } else {
                console.error(`Unknown column: ${col}`)
            }

        }
        this.updateAggregations(aggs)
        this.updateGroups(groups);
        this._projection = names;
    }

    async addPivotGroup(colId, override = {}) {
        this.api?.flushAsyncTransactions?.();
        const { groupBy } = this.getState();
        const next = Array.from(new Set([...groupBy, colId]));
        this.updateGroups(next, { hard: false });
        this.api.setColumnsVisible(next, true);
        this.hardRefresh({ force: true });
    }

    async removePivotGroup(colId) {
        this.api?.flushAsyncTransactions?.();
        const { groupBy } = this.getState();
        const next = groupBy.filter(g => g !== colId);
        this.updateGroups(next, { hard: false });
        this.api.setColumnsVisible([colId], false);
        this.hardRefresh({ force: true });
    }

    async addRemovePivotGroups({added=[], removed=[]}={}) {
        try { this.api?.flushAsyncTransactions?.(); } catch {};
        const { groupBy } = this.getState();
        const _removedSet = new Set(removed);
        let next = [...new Set([...groupBy, ...added])].filter(x => !_removedSet.has(x));
        this.updateGroups(next, { hard: false });
        if (added.length) this.api.setColumnsVisible(added, true);
        if (removed.length) this.api.setColumnsVisible(removed, false);
        this.hardRefresh({ force: true });
    }

    _schedule(kind) {
        if (this._locked) return;
        if (kind === 'hard') this._pendingHard = true;
        if (kind === 'soft') this._pendingSoft = true;
        if (this._debounceTimer) return;

        const delay = Math.max(1, this.opts.refreshDebounceMs | 0);
        this._debounceTimer = setTimeout(() => {
            this._debounceTimer = null;
            const doHard = this._pendingHard;
            this._pendingHard = false;
            this._pendingSoft = false;
            this._computeAndApply(doHard ? 'hard' : 'soft');
        }, delay);
    }

    _transformPivotResult(rows, cols) {
        if (!cols || !rows || !cols.length || !rows.length) return []
        return zipArray(rows, cols);
    }

    _grandTotalConfig() {
        return {
            enabled: this.enableGrandTotal,
            groups: ['QT', 'userSide']
        }
    }

    _deduplicateByPrimaryKey(rows, primaryKeyColumn) {
        const pkCols = Array.isArray(primaryKeyColumn) ? primaryKeyColumn : [primaryKeyColumn];
        const seen = new Map();
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const key = pkCols.map(col => row[col]).join('::');
            seen.set(key, row);
        }
        return Array.from(seen.values());
    }

    _computeAndApply(mode) {
        try {

            if (mode === 'hard') {
                this._inHardSwap = true;
                this._cancelPendingWork();
                this._drainAsyncTx();

                if (this._rowMap) {
                    this._rowMap.clear();
                    this._rowMap = null;
                }
                this._rows = null;
            }

            // this._rebuildSets();
            const respect_filters = this._respectSourceFilters
            const rowsIn = this._collectRowsForCompute(respect_filters);
            const aggSpec = this._activeAggregationsForCompute();
            const grandCfg = this._grandTotalConfig();

            const _result = this.pivotEngine.compute(rowsIn, {
                groupBy: this.pivotConfig.groupBy,
                aggregations: aggSpec,
                force: mode === 'hard',
                GRAND_TOTAL: grandCfg?.enabled !== false,
                GRAND_TOTAL_GROUPS: grandCfg?.groups,
            }) || [];

            let _resultFull;
            if (respect_filters) {
                const rowsInFull = this._collectRowsForCompute(false);
                _resultFull = this.pivotEngine.compute(rowsInFull, {
                    groupBy: this.pivotConfig.groupBy,
                    aggregations: aggSpec,
                    force: mode === 'hard',
                    GRAND_TOTAL: grandCfg?.enabled !== false,
                    GRAND_TOTAL_GROUPS: grandCfg?.groups,
                }) || [];
            }

            this._pivotResultRaw = _result;
            let result = this._transformPivotResult(_result.rows, _result.columns);

            if (this?.api) {
                const bools = this.pivotConfig.groupBy.map(col => this.api.getColumnDef(col))
                .filter(col=>["boolean", "flag"]
                .includes(col?.context?.dataType))
                                   .map(col=>col.field);
                if (bools.length) {
                    result.forEach(row => {
                        bools.forEach(boolCol => {
                            row[boolCol] = coerceToBool(row[boolCol])
                        })
                    })
                }
            }

            this._ensureStableRowIds(result);
            result = this._deduplicateByPrimaryKey(result, '__pid')
            this._applyInteractiveSkewColumns(result);
            this._applyColorization(result);
            this._pivotResult = result;

            if (respect_filters) {
                let resultFull = this._transformPivotResult(_resultFull.rows, _resultFull.columns);
                this._ensureStableRowIds(resultFull);
                resultFull = this._deduplicateByPrimaryKey(resultFull, '__pid');

                if (_resultFull && _resultFull.totals && _resultFull.totals.length) {
                    const totalsFull = this._transformPivotResult(_resultFull.totals, _resultFull.columns);
                    this._ensureStableTotalIds(totalsFull);
                    // this.totalRowsFull = totalsFull;
                    this._compute_updateGrandTotalSkews(_resultFull.rows, totalsFull, false).then((result) => {
                        this.totalRowsFull = result
                        try { this._emComputedFull(this.totalRowsFull); } catch {}
                    })
                }
                this._pivotResultFull = resultFull;
            } else {
                this._pivotResultFull = result;
            }

            if (_result && _result.totals && _result.totals.length) {
                const totals = this._transformPivotResult(_result.totals, _result.columns);
                this._ensureStableTotalIds(totals);
                this.totalRows = totals;
                try { this._updateGrandTotalSkews() } catch {}
            }

            if (mode === 'hard' || !this._rows.length) {

                const oldRows = this._rows;
                const oldMap = this._rowMap;

                this._rows = result;
                this._rowMap = this._indexRows(this._rows);

                if (oldRows) oldRows.length = 0;
                if (oldMap) oldMap.clear();

                try { this.api.setGridOption('rowData', this._rows); } catch {}
                return;
            }

            // soft: update deltas only (assumes group membership unchanged)
            const nextMap = this._indexRows(result);
            const updates = [];
            for (const [id, newRow] of nextMap.entries()) {
                const prev = this._rowMap.get(id);
                if (!prev) continue; // group membership drifted -> ignore in soft path
                if (this._shallowDiff(prev, newRow)) updates.push(newRow);
            }

            if (updates.length) {
                try {
                    this.api.applyTransactionAsync({ update: updates });
                } catch {}
                // update local cache immediately
                const oldRows = this._rows;
                const oldMap = this._rowMap;

                this._rows = result;
                this._rowMap = nextMap;

                if (oldRows) oldRows.length = 0;
                if (oldMap) oldMap.clear();
            } else {
                // no changes
                const oldRows = this._rows;
                const oldMap = this._rowMap;

                this._rows = result;
                this._rowMap = nextMap;

                if (oldRows) oldRows.length = 0;
                if (oldMap) oldMap.clear();
            }

        } catch (e) {
            console.error('ArrowAgPivotAdapter compute/apply error:', e);
        } finally {
            if (mode === 'hard') this._inHardSwap = false;
        }
    }

    _shallowDiff(a, b) {
        // Compare only keys present in b; treat group keys as stable
        const keys = Object.keys(b);
        for (let i = 0; i < keys.length; i++) {
            const k = keys[i];
            if (this._groupSet.has(k)) continue;
            if (a[k] !== b[k]) return true;
        }
        return false;
    }
    /* --------------------------- colors ---------------------------------------- */
    _applyColorization(rows){
        if (!this._color.enabled || !this._color.metric) {
            for (let i=0;i<rows.length;i++) rows[i].__colorIdx = -1;
            return;
        }
        const key = this._color.metric;
        let min=Number.POSITIVE_INFINITY, max=Number.NEGATIVE_INFINITY;
        let minGood=0, maxGood=0;
        let minBad=0, maxBad=0;
        for (let i=0;i<rows.length;i++){
            const v = rows[i][key];
            if (v==null || !Number.isFinite(+v) || !v) continue;
            const x = +v;
            if (x<min) min=x; if (x>max) max=x;
            const flip = rows[i]?.userSide === (this._color.reverse ? 'BID' : 'OFFER');
            const isGood = flip ? v < 0 : v > 0;
            let y = Math.abs(x);
            let z = -1*y;
            if (isGood) {
                if (z<minGood) minGood = z;
                if (y>maxGood) maxGood = y;
            } else {
                if (z<minBad) minBad = z;
                if (y>maxBad) maxBad = y;
            }
        }
        if (!(min<max)) { // flat or no data
            this._ensurePalette(); // still ensure
            for (let i=0;i<rows.length;i++) {
                const v = rows[i][key];
                if (!v) {
                    rows[i].__colorIdx = -1;
                } else {
                    const flip = rows[i]?.userSide === (this._color.reverse ? 'BID' : 'OFFER');
                    rows[i].__colorIdx = flip ? (this._color.paletteSize-1) : 0;
                }
            }
            return;
        }
        this._color.min = min; this._color.max = max;
        this._ensurePalette();
        const span = max - min || 1;
        const K = this._color.paletteSize - 1;
        const T = Math.floor(this._color.paletteSize / 2);
        for (let i=0;i<rows.length;i++){
            const v = rows[i][key];
            const flip = rows[i]?.userSide === (this._color.reverse ? 'BID' : 'OFFER');
            const isGood = flip ? v < 0 : v > 0;
            if (v==null || !Number.isFinite(+v) || !v) { rows[i].__colorIdx = -1; continue; }

            let t = -1;

            if (isGood) {
                if (!(maxGood - minGood)) {
                    t = 1;
                } else {
                    t = (+v - minGood) / (maxGood - minGood);
                }
            } else {
                if (!(maxBad - minBad)) {
                    t = 0;
                } else {
                    t = (+v - minBad) / (maxBad - minBad);
                }
            }

            //t = (+v - min) / span;
            if (t<0) t=0; else if (t>1) t=1;
            let idx = Math.floor(t * K);
            if (flip) idx = K - idx;
            rows[i].__colorIdx = idx
        }
    }

    _ensurePalette() {
        if (this._color.palette && this._color.palette.length===this._color.paletteSize) return;
        if (!this._rowStyleBuckets) this._setupRowTintBuckets();
        const N = this._color.paletteSize|0;
        const arr = new Array(N);
        const pal = this._rowStyleBuckets.neg.toReversed().concat(this._rowStyleBuckets.pos);
        for (let i=0;i<N;i++){
            arr[i] = pal[i];
        }
        this._color.palette = arr;
        this._color.posOutlier = this._rowStyleBuckets.posOutlier;
        this._color.negOutlier = this._rowStyleBuckets.negOutlier;
    }

    _setupRowTintBuckets() {
        const N = Math.floor(this._color.paletteSize / 2);
        if (this._rowStyleBuckets && this._rowStyleBuckets.length === N) return;
        const cs = { alphaMin:0.1, alphaMax:0.32, gamma:0.4, hoverDarken:0.08 };
        const POS = '#2dc262';  // GREEN
        const NEG = '#e05757';  // RED

        const lerp = (a, b, t) => a + (b - a) * t;
        const hexToRgb = (h) => {
            const x = h.replace('#','');
            const n = x.length === 3
                ? x.split('').map(c => parseInt(c + c, 16))
                : [parseInt(x.slice(0,2),16), parseInt(x.slice(2,4),16), parseInt(x.slice(4,6),16)];
            return n;
        };
        const rgbToHex = (r,g,b) => '#' + [r,g,b].map(v => {
            const s = (v|0).toString(16); return s.length===1?'0'+s:s;
        }).join('');

        const pos = hexToRgb(POS), neg = hexToRgb(NEG);

        // Build positive and negative buckets, index 0..N-1 for magnitude
        const posBuckets = new Array(N);
        const negBuckets = new Array(N);
        for (let i = 0; i < N; i++) {
            const t = i / (N - 1);
            const eased = Math.pow(t, cs.gamma);
            const alpha = cs.alphaMin + (cs.alphaMax - cs.alphaMin) * eased;

            const pr = lerp(255, pos[0], eased);
            const pg = lerp(255, pos[1], eased);
            const pb = lerp(255, pos[2], eased);
            const nr = lerp(255, neg[0], eased);
            const ng = lerp(255, neg[1], eased);
            const nb = lerp(255, neg[2], eased);

            posBuckets[i] = {
                'background-color': `rgba(${pr}, ${pg}, ${pb}, ${alpha})`,
                'border': 'inherit',
            };
            negBuckets[i] = {
                'background-color': `rgba(${nr}, ${ng}, ${nb}, ${alpha})`,
                'border': 'inherit',
            };
        }

        const posOutlier = {
            'border': `2px dashed color-mix(in srgb, ${posBuckets[N-1]['background-color']} 50%, limegreen 50%)`,
            'filter': 'contrast(150%) hue-rotate(290deg)',
        };
        const negOutlier = {
            'border': `2px dashed color-mix(in srgb, ${negBuckets[N-1]['background-color']} 50%, hotpink 50%)`,
            'filter': 'contrast(150%) hue-rotate(290deg)'
        };

        this._rowStyleBuckets = { N, pos: posBuckets, neg: negBuckets, posOutlier: posOutlier, negOutlier: negOutlier};
    }

    // Public toggles
    enableColorBy(metricName, { scheme='RdYlGn', reverse=false, paletteSize=64 } = {}){
        this._color.enabled = true;
        this._color.metric = metricName;
        this._color.scheme = scheme;
        this._color.reverse = !!reverse;
        this._color.paletteSize = Math.max(8, paletteSize|0);
        this._color.palette = null;
        this.hardRefresh();
    }
    disableColor(){
        this._color.enabled = false;
        setTimeout(()=>this.api.redrawRows(),0);
        this.hardRefresh();
    }
    /* --------------------------- apply ----------------------------------------- */
    // Returns Int32Array of source row indices that belong to the pivot row's bucket
    getSourceRowIndicesForPivotRow(pivotRow){
        //console.log(pivotRow)
        const g = this.pivotConfig.groupBy || [];
        if (!g.length || !pivotRow) return new Int32Array(0);
        const n = this.engine.numRows()|0;
        const out = new Int32Array(n); let k=0;
        outer: for (let ri=0; ri<n; ri++){
            for (let j=0;j<g.length;j++){
                const col = g[j];
                let a = this.engine.getCell(ri, col);
                let b = pivotRow[col];
                if ((a==='') || (a===' ')) a = null;
                if ((b==='') || (b===' ')) b = null;
                if (a===b) continue;
                if (a==null && b==null) continue;
                // NaN equality and Date equality
                if (typeof a==='number' && typeof b==='number' && Number.isNaN(a) && Number.isNaN(b)) continue;
                if (a instanceof Date || b instanceof Date){
                    const ax = a instanceof Date ? a.getTime() : +a;
                    const bx = b instanceof Date ? b.getTime() : +b;
                    if (ax===bx) continue;
                }
                continue outer;
            }
            out[k++] = ri;
        }
        return out.subarray(0,k);
    }

    getSourceRowIdsForPivotRow(pivotRow){
        const idx = this.getSourceRowIndicesForPivotRow(pivotRow);
        const ids = new Array(idx.length);
        for (let i=0;i<idx.length;i++) ids[i] = this.engine.getRowIdByIndex(idx[i]|0);
        return ids;
    }

    // Bulk apply an edit to all source rows in the bucket
    async applyEditToBucket(pivotRow, columnName, newValue, { concurrency=256 } = {}){
        const idx = this.getSourceRowIndicesForPivotRow(pivotRow);
        if (!idx.length) return { applied: 0, total: 0 };

        const tasks = [];
        let inFlight = 0, i = 0, applied = 0;
        const colDef = this?.api?.getColumnDef(columnName);

        const next = async ()=>{
            while (i < idx.length && inFlight < concurrency){
                const ri = idx[i++] | 0;
                inFlight++;
                const p = this.engine.applyValue(ri, columnName, newValue, colDef)
                .then(ok => { if (ok) applied++; })
                               .catch(()=>{})
                               .finally(()=>{ inFlight--; next(); });
                tasks.push(p);
            }
        };
        await next();
        await Promise.allSettled(tasks);

        // Coalesce repaint
        try { this.source.api?.refreshServerSide?.({ purge:false }); } catch {}
        return { applied, total: idx.length };
    }

    /* --------------------------- engine epoch wiring -------------------------- */
    _handleDerivedDirty(columns) {
        const payload = {colsChanged: columns};
        return this._handleEpoch(payload);
    }

    _handleEpoch(payload) {
        try {
            if (this._inHardSwap) return; // ignore noise during atomic swap
            if (this._inBulkApply) return;
            const cols = payload?.colsChanged;

            if (!cols) return;
            if (cols === true) { this.hardRefresh(); return; }

            // Normalize: cols may be an Array, Set, or a single string name.
            const changed = typeof cols === 'string' ? [cols]
                : (cols instanceof Set ? cols : Array.isArray(cols) ? cols : [cols]);
            let groupHit = false, aggHit = false;

            changed.forEach((c) => {
                if (this._groupSet.has(c)) { groupHit = true; return; }
            });

            if (!groupHit) {
                for (const c of changed) {
                    if (this._aggInputSet.has(c)) { aggHit = true; break; }
                }
            }

            if (groupHit) { this.hardRefresh(); return; }
            if (aggHit) { this.softRefresh(); }
        } catch (e) {
            console.error('ArrowAgPivotAdapter epoch handler error:', e);
            this.hardRefresh();
        }
    }
}

/* ----------------------------- usage example -----------------------------
import { ArrowAgPivotAdapter } from '@/grids/js/arrow/arrowPivotAdapter.js';

// required sets from your snippet:
const required_groups = ['userSide', 'QT'];
const required_aggs = [
    { isin: { func: 'count', name: '#' } },
    { grossSize: { func: 'sum', name: 'grossSize' } },
    { BVAL_Level: { func: 'mean', name: 'BVAL_Level' } },
    { relativeSkewValue: { func: 'wavg', weight: 'grossSize', name: 'current_skew' } },
    { input_skew_input: { func: 'wavg', weight: 'grossSize', name: 'input_skew_input' } },
    { proposed_skew: { func: 'wavg', weight: 'grossSize', name: 'proposed_skew' } },
    { grossSize: { func: 'SUM', name: '__w_sum__' } },
];

const pivot = new ArrowAgPivotAdapter(sourceAdapter, {
    refreshDebounceMs: 80,
    columnOverrides: {
        '#': { width: 72 },
        current_skew: { width: 112 },
        proposed_skew: { width: 112 },
    },
    requiredGroups: required_groups,
    requiredAggregations: required_aggs,
});

// Configure:
pivot.updateGroups([...required_groups, /* additional group columns ]);
pivot.updateAggregations([...required_aggs , more ]);

// Mount:
pivot.mount('#pivot-grid');

// Public API:
pivot.softRefresh();
pivot.hardRefresh();
pivot.updateColumnDefs({ BVAL_Level: { width: 90 } });
----------------------------------------------------------------------------- */
