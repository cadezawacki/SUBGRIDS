

// pt/js/widgets/pivotWidget.js

import {BaseWidget} from './baseWidget.js';
import {combineLatest, mergeAll} from 'rxjs';
import {debounceTime, filter, map} from 'rxjs/operators';
import '../../css/pivot.css';
import * as utils from '@stdlib/utils';
import {pivotColumns} from "@/pt/js/grids/portfolio/portfolioColumns.js";
import {writeObjectToClipboard, getVenueCopyColumns, mapVenueFromRfq} from '@/utils/clipboardHelpers.js';
import {ArrowAgPivotAdapter} from '@/grids/js/arrow/arrowPivotEngine.js';
import {asArray, wait} from "@/utils/helpers.js";
import {ArrowAgGridAdapter} from "@/grids/js/arrow/arrowEngine.js";
import {ACTION_MAP} from "@/actionMap.js";
import interact from "interactjs";

function _escHtml(v) {
    if (v == null) return '';
    const s = typeof v === 'number' ? v.toLocaleString(undefined, {maximumFractionDigits: 4}) : String(v);
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

//context, widgetId, manager, feederId, selector, config = {}
export class PivotWidget extends BaseWidget {
    constructor(context, widgetId, adapter, selector, config = {}) {
        super(context, widgetId, adapter, selector, config);

        // Debug
        this.tableId = config.tableId || "pivotWidget-table";
        this.pivotSelector = config.pivotSelector || '#pivot-grid-container';
        this.ptPivot = null;

        this.currentGroups = [];
        this.required_groups = [];
        this._applyRafId = 0;
        this._applyTries = 0;
        this._current_active_presets = new Set();
        this._em = this.context.page.emitter || this.context.emitter;

        // Subscription disposers drained in onCleanup
        this._disposers = [];
        // In-flight guard for applyAllBuckets
        this._applyInFlight = false;
        // RAF id for coalesced alignSkewButton
        this._alignRafId = 0;
        // Tracked timeout for onPresetLoaded
        this._presetTimeoutId = 0;

    }

    // --- Widget Lifecycle ---

    async onInit() {

        const required_groups = ['userSide', 'QT'];
        this.required_groups = required_groups;
        const required_aggs = [
            {isPriced: {func: 'PERCENT_OF_ROW_COUNT', name: '%'}},
            {refSkew: {func: 'wavg', name: 'Skew', weight: 'grossSize'}},
            {grossSize: {func: 'sum', name: 'Gross'}},
            {refSkewPx: {func: 'wavg', name: 'refSkewPx', weight: 'grossSize', DROP_NULLS: false, FILL_NULL: 0, ZERO_AS_NULL: false}},
            {refSkewSpd: {func: 'wavg', name: 'refSkewSpd', weight: 'grossSize', DROP_NULLS: false, FILL_NULL: 0, ZERO_AS_NULL: false}},
            {skewScore: {func: 'wavg', name: 'skewScore', weight: 'grossSize', DROP_NULLS: true, ZERO_AS_NULL: true}},

            {current_skew: {func: 'wavg', name: 'current_skew', weight: 'grossSize', headerName: 'Current', DROP_NULLS: false, FILL_NULL: 0, ZERO_AS_NULL: false}},
            {input_skew: {func: 'wavg', name: 'input_skew', weight: 'grossSize', headerName: '∆'}},
            {proposed_skew: {func: 'wavg', name: 'proposed_skew', weight: 'grossSize', headerName: 'Prop'}},
        ];

        this.ptPivot = new ArrowAgPivotAdapter(this.context, this.adapter, {
            requiredGroups: required_groups,
            requiredAggregations: required_aggs,
            customDefs: pivotColumns
        });
        this.ptPivot.mount(this.pivotSelector);
        this.currentGroups = [];

        this._initControls();
        this._cacheDom();
        await this._setupReactions();
        this._setupHotkeys();
        this._setupResizer();
        this.setupTooltips();
    }

    flipLock(force = null) {
        let v;
        if (force != null) {
            v = force
        } else {
            v = !this.context.page.page$.get('proceedsLocked');
        }
        if (this.tt) {
            if (v) {
                this.tt.enable();
                this.tt.flash();
            } else {
                this.tt.stopFlash();
                this.tt.disable();
            }
        }

        this.context.page.page$.set('proceedsLocked', v);
        return v
    }

    async _redistributeProceeds() {
        if (this._redistInFlight) return;
        this._redistInFlight = true;
        this._redistStale = false;

        const page = this.context.page;
        const toast = page.toastManager();
        const mm = page.modalManager();
        const sm = page.subscriptionManager();
        const socketManager = page.socketManager();

        const roomContext = page._ptRaw?.context;
        if (!roomContext) {
            toast.error('Redistribute', 'No active portfolio context.');
            this._redistInFlight = false;
            return;
        }

        const portfolioRoom = roomContext.room;

        // Track portfolio publishes to detect stale data
        const onPortfolioPublish = () => { this._redistStale = true; };
        sm.registerMessageHandler(portfolioRoom, 'publish', onPortfolioPublish);

        const cleanup = () => {
            sm.unregisterMessageHandler(portfolioRoom, 'publish', onPortfolioPublish);
            this._redistInFlight = false;
        };

        toast.info('Proceeds Solver', 'Computing redistribution...');

        try {
            const result = await this._fetchRedistribution(socketManager, roomContext);
            await this._showRedistributeModal(result, roomContext, {mm, sm, toast, esc: _escHtml, cleanup, onPortfolioPublish});
        } catch (err) {
            console.error('[redistribute]', err);
            toast.error('Redistribute', `Error: ${err.message || err}`);
            cleanup();
        }
    }

    async _fetchRedistribution(socketManager, roomContext) {
        const response = await socketManager._sendWebSocketMessage({
            action: ACTION_MAP.get('redistribute'),
            context: {room: roomContext.room, grid_id: roomContext.grid_id},
            data: {params: {}},
        }, {wait: true, timeout: 15000});
        return response?.data;
    }

    async _showRedistributeModal(result, roomContext, {mm, sm, toast, esc, cleanup, onPortfolioPublish}) {
        // Handle error responses in a modal rather than just a toast
        if (!result || result.error) {
            cleanup();
            const errorMsg = result?.error || 'Failed to compute redistribution.';
            await mm.show({
                title: 'Redistribute Proceeds — Error',
                body: `<div style="padding:12px;color:var(--danger, #e74c3c);">${esc(errorMsg)}</div>`,
                fields: null,
                buttons: [
                    {text: 'Close', value: 'close'},
                    {text: 'Re-run', value: 'rerun', class: 'btn-primary'}
                ],
                modalBoxClass: 'pt-redistribute-modal'
            }).then(action => {
                if (action === 'rerun') this._redistributeProceeds();
            });
            return;
        }

        const {summary, updates, stats, constraints_applied, constraints_relaxed} = result;
        if (!summary || summary.length === 0) {
            toast.info('Redistribute', 'No eligible rows for redistribution.');
            cleanup();
            return;
        }

        // Build table HTML
        const displayCols = ['ticker', 'isin', 'description', 'grossSize', 'weight_pct', 'current_skew', 'proposed_skew', 'delta'];
        const colLabels = {
            ticker: 'Ticker', isin: 'ISIN', description: 'Description',
            grossSize: 'Gross Size', weight_pct: 'Weight %',
            current_skew: 'Current Skew', proposed_skew: 'Proposed Skew', delta: 'Delta'
        };
        const cols = displayCols.filter(c => summary[0]?.hasOwnProperty(c));

        const headerRow = cols.map(c => `<th>${colLabels[c] || esc(c)}</th>`).join('');
        const bodyRows = summary.map(row => {
            const cells = cols.map(c => `<td>${esc(row[c])}</td>`).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        let infoHtml = '';
        if (constraints_applied) {
            infoHtml += `<div style="margin-bottom:6px;padding:4px 8px;background:var(--info-bg, #1a3a4a);border-radius:4px;font-size:11px;color:var(--info-text, #8ad);">Filtered by AI Tickers microgrid patterns</div>`;
        }
        if (constraints_relaxed?.length) {
            infoHtml += constraints_relaxed.map(msg =>
                `<div style="margin-bottom:6px;padding:4px 8px;background:var(--warning-bg, #4a3a1a);border-radius:4px;font-size:11px;color:var(--warning-text, #da8);">${esc(msg)}</div>`
            ).join('');
        }

        const statsHtml = `<div class="redist-stats" style="margin-bottom:10px;font-size:12px;color:var(--text-secondary, #aaa);">
            <span><b>Rows:</b> ${esc(stats.row_count)}</span> &middot;
            <span><b>Total Gross:</b> ${esc(stats.total_gross)}</span> &middot;
            <span><b>Uniform Skew:</b> ${esc(stats.uniform_skew)}</span> &middot;
            <span><b>Column:</b> ${esc(stats.skew_column)}</span>
        </div>`;

        const staleId = `redist-stale-${Date.now()}`;
        const tableHtml = `
            <div class="redist-modal-body" style="max-height:60vh;overflow:auto;">
                ${infoHtml}
                ${statsHtml}
                <div id="${staleId}" style="display:none;margin-bottom:8px;padding:6px 10px;background:var(--danger-bg, #4a1a1a);border:1px solid var(--danger, #e74c3c);border-radius:4px;font-size:12px;color:var(--danger, #e74c3c);font-weight:600;">
                    STALE — Portfolio data has changed since this was computed. Re-run recommended.
                </div>
                <table class="redist-summary-table" style="width:100%;border-collapse:collapse;font-size:12px;">
                    <thead><tr style="border-bottom:1px solid var(--border-color, #444);text-align:left;">${headerRow}</tr></thead>
                    <tbody>${bodyRows}</tbody>
                </table>
            </div>`;

        // Show modal — poll stale state to update banner while open
        let staleCheckInterval = null;
        const startStaleCheck = () => {
            staleCheckInterval = setInterval(() => {
                const el = document.getElementById(staleId);
                if (el && this._redistStale) el.style.display = 'block';
            }, 500);
        };
        startStaleCheck();

        const action = await mm.show({
            title: 'Redistribute Proceeds',
            body: tableHtml,
            fields: null,
            buttons: [
                {text: 'Cancel', value: 'cancel'},
                {text: 'Re-run', value: 'rerun'},
                {text: 'Apply', value: 'apply', class: 'btn-primary'}
            ],
            modalBoxClass: 'pt-redistribute-modal'
        });

        clearInterval(staleCheckInterval);

        if (action === 'rerun') {
            cleanup();
            return this._redistributeProceeds();
        }

        if (action !== 'apply') {
            cleanup();
            return;
        }

        // If stale, require double confirmation
        if (this._redistStale) {
            const confirm = await mm.show({
                title: 'Stale Data Warning',
                body: `<div style="padding:12px;">
                    <p style="color:var(--danger, #e74c3c);font-weight:600;margin-bottom:8px;">Portfolio data has changed since this redistribution was computed.</p>
                    <p>Applying may produce unexpected results. Are you sure?</p>
                </div>`,
                fields: null,
                buttons: [
                    {text: 'Cancel', value: 'cancel'},
                    {text: 'Re-run Instead', value: 'rerun'},
                    {text: 'Apply Anyway', value: 'apply', class: 'btn-danger'}
                ],
                modalBoxClass: 'pt-redistribute-modal'
            });

            if (confirm === 'rerun') {
                cleanup();
                return this._redistributeProceeds();
            }
            if (confirm !== 'apply') {
                cleanup();
                return;
            }
        }

        // Apply: publish the updates to the portfolio grid
        const room = roomContext.room;
        const meta = sm.buildPayloadContext(room, roomContext);
        const data = sm.buildPayloadData(room, roomContext, updates);
        await sm.publishMessage(room, meta, data);
        toast.success('Proceeds Solver', `Applied redistribution to ${updates.length} rows.`);
        cleanup();
    }

    setupTooltips() {
        const flipLock = this.flipLock.bind(this);
        const lockState = () => this.context.page.page$.get('proceedsLocked');

        const toast = this.context.page.toastManager();
        this.context.page.tooltipManager().registerContextMenuItem({
            id: 'lock-proceeds',
            label: 'Lock Proceeds',
            icon: '🔒',
            handler({tooltip}) {
                flipLock(true);
            }
        });

        this.context.page.tooltipManager().registerContextMenuItem({
            id: 'unlock-proceeds',
            label: 'Unlock Proceeds',
            icon: '🔓',
            handler({tooltip}) {
                flipLock(false);
            }
        });

        const widget = this;
        this.context.page.tooltipManager().registerContextMenuItem({
            id: 'redistribute-proceeds',
            label: 'Redistribute Proceeds',
            icon: '',
            handler({tooltip}) {
                widget._redistributeProceeds();
            }
        });

        this.tt = this.context.page.tooltipManager().add({
            id: 'proceed-lock-tooltip',
            title: null,
            target: this.topLock,
            padding: 2,
            singleton: true,
            contextMenu: {enabled: true, items: ['lock-proceeds', 'unlock-proceeds']},
            showDelay: 200,
            hideDelay: 0,
            interactive: true,
            fitContent: false,
            offset: 10,
            flash:{
                enabled: false,
                color: "var(--purple-400)",
                opacity: 0.6,
                spread: 6,
                interval: 10_000,
                duration: 2000,
                once: false
            },
            className: ['pivot-proceeds-modal'],
            html: true,
            content: `<div class="pivot-proceeds-title">
                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 20 20"><path fill="currentColor" d="M8.693 2.058c.404-.058.77-.065 1.13.006c.28.057.529.157.765.282a1.42 1.42 0 0 0-.566 1.401l-.008-.004a1.5 1.5 0 0 0-.487-.207c-.14-.028-.322-.036-.612.005a1.25 1.25 0 0 0-.861.537c-.21.301-.359.75-.405 1.357c-.003.16-.028.647-.065 1.315H9.3a.75.75 0 0 1 0 1.5H7.498a506 506 0 0 1-.35 5.607v.005l-.01.12c-.091 1.115-.226 2.772-1.76 3.666l-.006.003c-.997.57-2.18.384-3.107-.08a.751.751 0 0 1 .67-1.341c.671.335 1.287.349 1.69.12c.807-.472.92-1.326 1.027-2.61c.05-.645.174-2.635.286-4.522l.058-.968H4.801a.75.75 0 0 1 0-1.5h1.28l.006-.093a169 169 0 0 0 .062-1.224v-.025L6.15 5.4l.001-.053c.056-.785.257-1.529.67-2.124A2.75 2.75 0 0 1 8.677 2.06zm1.264 7.101c.31-.132.635-.173.957-.1c.312.07.555.235.736.402c.277.256.505.622.657.867l.074.116a36 36 0 0 0 .76 1.354l1.985-1.985l.53.174a.6.6 0 0 1 .219.14c.07.06.11.14.14.22l.17.527l-2.147 2.15a46 46 0 0 1 1.02 1.798l.061.103c.053.09.102.171.158.258q.116.176.202.257q.043.04.064.05q.019.011.02.01h.006l.033-.006c.018-.01.026-.019.033-.025a1 1 0 0 0 .127-.176a.75.75 0 0 1 1.279.783c-.227.37-.448.601-.804.78a1.6 1.6 0 0 1-1.056.096a1.7 1.7 0 0 1-.733-.423c-.29-.275-.51-.65-.64-.872l-.05-.087l-.028-.05a38 38 0 0 0-.788-1.4l-2.662 2.66a.75.75 0 0 1-1.06-1.06l2.825-2.825a46 46 0 0 1-.947-1.671l-.087-.14c-.058-.091-.108-.172-.167-.259a1.6 1.6 0 0 0-.212-.262l-.042-.035l-.012.01l-.066.076l-.022.025l-.174.202a.75.75 0 1 1-1.133-.983l.164-.19l.022-.026a1.8 1.8 0 0 1 .415-.38l.032-.024a1 1 0 0 1 .14-.079M17.484 6a.3.3 0 0 1 .285.201l.25.766a1.58 1.58 0 0 0 .999.998l.765.248l.016.004a.302.302 0 0 1 0 .57l-.766.248a1.58 1.58 0 0 0-.999.998l-.249.766a.3.3 0 0 1-.46.145a.3.3 0 0 1-.11-.145l-.25-.766a1.58 1.58 0 0 0-.997-1.002l-.766-.248A.3.3 0 0 1 15 8.498a.3.3 0 0 1 .202-.285l.766-.248a1.58 1.58 0 0 0 .983-.998l.248-.766A.3.3 0 0 1 17.484 6m-3.006-6a.43.43 0 0 1 .4.282l.348 1.072a2.2 2.2 0 0 0 1.399 1.396l1.071.349l.022.005a.426.426 0 0 1 .205.643a.42.42 0 0 1-.205.154l-1.072.349a2.21 2.21 0 0 0-1.398 1.396l-.349 1.072a.424.424 0 0 1-.643.204l-.02-.015a.43.43 0 0 1-.136-.19l-.347-1.07a2.2 2.2 0 0 0-1.398-1.402l-1.073-.349a.424.424 0 0 1 0-.797l1.072-.349a2.21 2.21 0 0 0 1.377-1.396L14.08.282a.42.42 0 0 1 .4-.282"/></svg>
                <span>Proceeds Solver</span>
            </div>`,
            actions: [
                {id: 'btn-unlock-proceeds', label: 'Unlock', variant: 'portfolio', onClick: () => flipLock(false), closeOnClick: false},
                {
                    id: 'btn-redist-proceeds', label: 'Redistribute', variant: 'portfolio', onClick: () => {
                        this._redistributeProceeds();
                    }, closeOnClick: false
                },
            ],
        });
        this.flipLock(false);
        this.context.page.addEventListener(this.topLock, 'click', () => {
            if (!lockState()) {
                flipLock()
            } else {
                this.tt.show()
            }
        })

        this._disposers.push(this.context.page.page$.onValueChanged('proceedsLocked', (cur) => {
            if (cur) {
                this.lockIcon.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 48 48"><defs><mask id="SVGtDIhpJdd"><g fill="none" stroke-linejoin="round" stroke-width="4"><rect width="36" height="22" x="6" y="22" fill="#fff" stroke="#fff" rx="2"/><path stroke="#fff" stroke-linecap="round" d="M14 22v-8c0-5.523 4.477-10 10-10s10 4.477 10 10v8"/><path stroke="#000" stroke-linecap="round" d="M24 30v6"/></g></mask></defs><path fill="currentColor" d="M0 0h48v48H0z" mask="url(#SVGtDIhpJdd)"/></svg>`;
                this.topLock.classList.add('locked')
            } else {
                this.lockIcon.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 48 48"><g fill="none" stroke="currentColor" stroke-linejoin="round" stroke-width="4"><rect width="34" height="22" x="7" y="22.048" rx="2"/><path stroke-linecap="round" d="M14 22v-7.995c-.005-5.135 3.923-9.438 9.086-9.954S32.967 6.974 34 12.006M24 30v6"/></g></svg>`;
                this.topLock.classList.remove('locked')
            }
        }));
    }

    _cacheDom() {
        this.filterToggle = document.getElementById('filter-pivot-by-grid');
        this.colorizeToggle = document.getElementById('pivot-colorize-toggle');
        this.bulkSkewMode = document.getElementById('pivot-bulkskew-toggle');
        this.lockTotalsToggle = document.getElementById('pivot-locktotals-toggle');
        this.gridDiv = document.querySelector(this.pivotSelector);
        this.bulkSkewClear = document.getElementById('pivot-bulk-skew-clear');
        this.bulkSkewApply = document.getElementById('pivot-bulk-skew-apply');
        this.venueCopy = document.getElementById('venue-copy-btn');
        this.venueDropdown = document.querySelector('#venue-caret');
        this.venueOptions = this.venueDropdown ? this.venueDropdown.querySelectorAll('.quote-types') : [];
        this.widgetDom = document.getElementById('pivotWidget');
        this.bulkBar = document.querySelector('.pt-left');
        this.controls = document.querySelector('.pivot-controls');
        this.presetButtons = document.getElementById("pivot-quick-presets");
        this.contentRow = document.getElementById('content-row');
        this.refSection = document.querySelector('.upper-ref-market-section');
        this.topBar = document.getElementById('pivot-topbar');
        this.pivotWeight = document.getElementById('pivot-weight-group');
        this.weightNotional = document.getElementById('weightNotional');
        this.weightDv01 = document.getElementById('weightDv01');
        this.weightCount = document.getElementById('weightCount');
        this.topApply = document.getElementById('pivot-top-apply');
        this.topLock = document.getElementById('pivot-top-lock');
        this.lockIcon = document.querySelector('.icon-wrap');
        this.topBtnWrapper = document.getElementById('pivot-top-btn-wrapper')
        this.topDiscard = document.getElementById('pivot-top-discard');

        // Skew mode elements
        this.skewModeGroup = document.getElementById('pivot-skew-mode');
        this.pctMarketWrap = document.getElementById('pivot-pct-market-wrap');
        this.pctMarketSelect = document.getElementById('pivot-pct-market');
    }

    _updatePendingBar(count = 0) {
        const n = Number(count) | 0;
        const has = n > 0;
        if (this.topApply) this.topApply.disabled = !has;
        // if (this.topDiscard) this.topDiscard.disabled = !has;

        if (this.topApply) {
            if (has) {
                this.topApply.textContent = `Apply ${n} Pending Skew${n > 1 ? 's' : ''}`;
                if (this.topDiscard) this.topDiscard.textContent = 'Discard';
            } else {
                this.topApply.textContent = `No Pending Skews`
                if (this.topDiscard) this.topDiscard.textContent = 'Clear';
            }
        }
    }

    currentWidth() {
        return this.ptPivot.element.getBoundingClientRect().width
    }

    maxWidth() {
        return this.contentRow.getBoundingClientRect().width - (this.controls.getBoundingClientRect().width + this.refSection.getBoundingClientRect().width)
    }

    proposeWidth() {
        if (!this.ptPivot) return
        let headerWidth = Array.from(this.ptPivot.element.querySelectorAll('.ag-header-cell')).reduce((acc, dom) => {
            acc += (dom.getBoundingClientRect().width ?? 0)
            return acc;
        }, 0);
        if (!headerWidth) return;
        headerWidth += 30 // sidebar + small buffer
        const maxWidth = this.maxWidth();
        return Math.max(400, Math.min(headerWidth, maxWidth));
    }

    async alignWidthIfNeeded() {
        const c = this.currentWidth()
        const p = this.proposeWidth();
        if (c && p && (p > c)) this.ptPivot.setWidth(p);
    }

    alignWidth() {
        const w = this.proposeWidth();
        if (w != null) this.ptPivot.setWidth(w);
    }

    alignWidthOnTap() {
        const c = this.currentWidth()
        const p = this.proposeWidth();
        const m = this.maxWidth();
        if (Math.abs(c - p) <= 50) {
            this.ptPivot.setWidth(m);
        } else {
            this.ptPivot.setWidth(p);
        }
    }

    _setupResizer() {
        const target = document.querySelector(".pivot-controls");
        const grid = this.ptPivot.element;
        const tbl = this.ptPivot;
        this.interactable = interact(target).resizable({
            edges: {top: false, left: true, bottom: false, right: true},
            listeners: {
                move: function (event) {
                    let base = event.client.x - grid.getBoundingClientRect().left;
                    tbl.setWidth(base)
                }
            }
        });

        // Expand pivot grid to fill available width when the container resizes
        const widget = this;
        let resizeRafId = 0;
        this._resizeObserver = new ResizeObserver(() => {
            if (resizeRafId) return;
            resizeRafId = requestAnimationFrame(() => {
                resizeRafId = 0;
                const maxW = widget.maxWidth();
                if (maxW > 0) tbl.setWidth(maxW);
            });
        });
        if (this.contentRow) this._resizeObserver.observe(this.contentRow);
    }


    /* ------------------------ GroupBy persistence helpers ------------------------ */
    _groupByStorageKey() {
        const k = (this.context?.portfolio_key || this.context?.url_context || 'default').toUpperCase();
        return `pt:pivotGroupBy:${k}`;
    }

    _readGroupByCache() {
        try {
            const raw = localStorage.getItem(this._groupByStorageKey());
            const v = raw ? JSON.parse(raw) : [];
            return Array.isArray(v) ? v : [];
        } catch {
            return [];
        }
    }

    _writeGroupByCache(values) {
        try {
            const dedup = Array.from(new Set((values || []).filter(Boolean)));
            localStorage.setItem(this._groupByStorageKey(), JSON.stringify(dedup));
        } catch {
        }
    }

    _validGroupFields() {
        // Mirrors your existing getAllValidGroupDefs filter.  [oai_citation:0‡Pivot.txt](sediment://file_00000000215061f6be6fe4f880c65325)
        const defs = this.ptPivot.getAllValidGroupDefs().filter(cd => !(cd?.context?.suppressColumnMenu && cd?.context?.suppressColumnMenu.includes('pivotWidget-table')));
        const fields = new Set(defs.map(cd => cd.field));
        return {defs, fields};
    }

    // async _applyGroupByFromCache() {
    //     const pivot = this.ptPivot;    //     if (!pivot?.api) return false;    //    //     const { fields } = this._validGroupFields();    //     const saved = this._readGroupByCache().filter(v => fields.has(v));    //     if (!saved.length) return true; // nothing to do, but grid is ready    //    //     const MAX_GROUPS = 4;    //     const next = saved.slice(0, MAX_GROUPS);    //    //     const curSet = new Set(this.currentGroups || []);    //     const nextSet = new Set(next);    //     const removed = Array.from(curSet).filter(x => !nextSet.has(x));    //     const added = Array.from(nextSet).filter(x => !curSet.has(x));    //    //     if (added.length || removed.length) {    //         await pivot.addRemovePivotGroups({ added, removed });    //         this.currentGroups = next;    //     }    //     if (this.groupBySelect?.addSelections) this.groupBySelect.addSelections(next);    //     return true;    // }
    // _scheduleApplyGroupByFromCache() {    //     // Bounded RAF retry in case API becomes ready a tick later.    //     const MAX_TRIES = 60; // ~1s worst-case    //     const tick = async () => {    //         this._applyTries++;    //         const ok = await this._applyGroupByFromCache();    //         if (ok || this._applyTries >= MAX_TRIES) { this._applyRafId = 0; return; }    //         this._applyRafId = requestAnimationFrame(tick);    //     };    //     this._applyRafId = requestAnimationFrame(tick);    // }
    async _setupReactions() {
        const pivot = this.ptPivot;
        const page$ = this.context.page.page$;
        const widget = this;

        console.log('setting up reactions')

        this._disposers.push(this.engine.onColumnEvent('columnResized', () => widget._scheduleAlignSkewButton()));
        const isInitialized = pivot.grid$.pick('pivotInitialized');
        if (isInitialized && pivot.api) {
            widget.api = pivot.api;
            await this.onPresetLoaded()
        } else {
            this._disposers.push(this._em.on('pivot-initialized', async () => {
                widget.api = pivot.api;
                await this.onPresetLoaded()
            }));
        }

        this._disposers.push(this._em.on(ArrowAgGridAdapter.COLUMNS_EVENT, () => {
            widget._scheduleAlignSkewButton()
        }));

        // this.context.page.addEventListener(this.filterToggle, 'input', (v) => {
        //     page$.set('linkedPivotFilters', v?.target?.checked ?? false)        // }, {passive: true});
        // Link toggles
        this.context.page.linkStoreToInput(this.filterToggle, 'linkedPivotFilters', page$, {persist:true, storageKey:'linkedPivotFilters'});
        this.context.page.linkStoreToInput(this.lockTotalsToggle, 'lockPivotTotals', page$, {persist: true, storageKey: 'lockPivotTotals'});
        this.context.page.linkStoreToInput(this.colorizeToggle, 'colorizePivot', page$, {persist: true, storageKey: 'colorizePivot'});

        // React to changes
        this._disposers.push(this.context.page.page$.onValueChanged('linkedPivotFilters', (link) => pivot.setRespectSourceFilters(link)));
        this._disposers.push(this.context.page.page$.onValueChanged('lockPivotTotals', (ev) => pivot.setLockGridTotals(ev)));

        // Skew mode: outright vs percent
        this._setupSkewModeControls(pivot);

        // initialize adapter with current persisted values
        pivot.setRespectSourceFilters(!!page$.get('linkedPivotFilters'));
        pivot.setLockGridTotals(!!page$.get('lockPivotTotals'));
        this.context.page.addEventListener(this.bulkSkewClear, 'click', (v) => {
            if (pivot._bucketState.size === 0) return pivot.input_clear_on_each_row();
            pivot.resetAllBuckets();
        });

        this.context.page.addEventListener(this.bulkSkewApply, 'click', async () => {
            if (this._applyInFlight) return;
            this._applyInFlight = true;
            try {
                await pivot.applyAllBuckets();
            } finally {
                this._applyInFlight = false;
            }
        });

        if (this.venueOptions) {
            const pg = this.context.page
            this.venueOptions.forEach(item => {
                this.context.page.addEventListener(item, 'mousedown', async (e) => {
                    const venue = e.currentTarget.getAttribute('data-venue');
                    if (venue) {
                        await widget.writeVenueToClipboard(venue);
                        await pg.send_full_push("Submission", true);
                    } else {
                        widget.context.page.toastManager().error('Submission Copy', `Failed to write to clipboard.`);
                    }
                });
            });
        }

        if (this.topApply) {
            this.context.page.addEventListener(this.topApply, 'click', async () => {
                if (this._applyInFlight) return;
                this._applyInFlight = true;
                try {
                    await pivot.applyAllBuckets();
                    this._updatePendingBar(0);
                } finally {
                    this._applyInFlight = false;
                }
            });
        }
        if (this.topDiscard) {
            this.context.page.addEventListener(this.topDiscard, 'click', () => {
                if (pivot._bucketState.size === 0) {
                    return pivot.input_clear_on_each_row()
                }
                pivot.resetAllBuckets();
                this._updatePendingBar(0);
            });
        }

        this.context.page.addEventListener(this.presetButtons, 'click', async (e) => {
            const nearestBtn = e.target.closest('.pill-button');
            if (nearestBtn) {
                const groupsThen = [...(pivot.pivotConfig.groupBy || [])];
                const group = nearestBtn.getAttribute('data-column');

                if (group === 'CLEAR') {
                    widget.clearActiveQuickPreset();
                    await pivot.updateGroups([], {hard: true});
                } else {
                    if (!e.ctrlKey) {
                        widget.clearActiveQuickPreset();
                        this._current_active_presets.add(nearestBtn);
                        nearestBtn.classList.toggle('active', true);
                        await pivot.updateGroups([group], {hard: true});
                    } else {
                        if (this._current_active_presets.has(nearestBtn)) {
                            this._current_active_presets.delete(nearestBtn);
                            await pivot.removeGroups([group], {hard: true});
                            nearestBtn.classList.toggle('active', false);
                        } else {
                            this._current_active_presets.add(nearestBtn);
                            await pivot.addGroups([group], {hard: true});
                            nearestBtn.classList.toggle('active', true);
                        }
                    }
                }
                const groupsNow = pivot.pivotConfig.groupBy;
                const groupsSet = new Set(groupsNow);
                const removals = groupsThen.filter(x => !groupsSet.has(x))
                pivot.api.setColumnsVisible(removals, false);
                pivot.api.setColumnsVisible(groupsNow, true);
            }
        });

        this.context.page.addEventListener(this.venueCopy, 'click', async () => {
            if (this._applyInFlight) return;
            this._applyInFlight = true;
            try {
                const venue = widget.context.page.portfolioMeta$.get("venue") || 'ib';
                await widget.writeVenueToClipboard(venue);
                await widget.context.page.send_full_push("Submission", true);
            } finally {
                this._applyInFlight = false;
            }
        });

        const applyBtn = this.bulkSkewApply;
        this._disposers.push(this.ptPivot.onComputed(() => {
            applyBtn.classList.toggle('dirtySkews', pivot._bucketState.size > 0);
        }));

        const node_event = 'treeColumnChooser-pivot-NODE_TOGGLE';
        this._disposers.push(this._em.on(node_event, () => {
            widget.clearActiveQuickPreset()
        }))

        this._disposers.push(this.ptPivot.onBucketChange((info) => {
            widget._updatePendingBar(info?.size || 0);
        }));

        this._disposers.push(this.ptPivot.onGroupChange((info) => {
            widget.clearSpan();
            widget._scheduleAlignSkewButton();
        }));
        // initialize banner from current state
        widget._updatePendingBar(this.ptPivot.getPendingBucketCount ? this.ptPivot.getPendingBucketCount() : 0);

    }

    clearSpan() {
        this.ptPivot.resetAllBuckets();
        this._updatePendingBar(0);
    }

    clearActiveQuickPreset() {
        const a = this.presetButtons.querySelectorAll('.active');
        if (a) a.forEach(aa => aa.classList.toggle('active', false));
        this._current_active_presets.clear();
        this.clearSpan();
    }

    async onPresetLoaded() {
        const page$ = this.context.page.page$;
        const pivot = this.ptPivot;
        console.log("HERE!")
        // this.groupBySelect.addSelections(this.required_groups);
        // this.api.setColumnsVisible(['isPriced','userSide','QT'], true);        // this.api.setColumnsPinned(['isPriced','userSide','QT'], 'left');
        const bs_cb = async (v, realign = true) => {
            this.ptPivot.domTimeout = false;
            pivot?.api.setColumnsVisible(['current_skew', 'input_skew', 'proposed_skew'], v);
            pivot.api.setColumnsPinned(['current_skew', 'input_skew', 'proposed_skew'], 'left');
            pivot.api.moveColumns(['current_skew', 'input_skew', 'proposed_skew'], pivot.api.getColumnDefs().length - 3);
            if (v && realign) await this.alignWidthIfNeeded();

            this.topApply.classList.toggle('disabled', !v);
            this.topDiscard.classList.toggle('disabled', !v);
            this.topLock.classList.toggle('disabled', !v);

            this._presetTimeoutId = setTimeout(() => {
                this._presetTimeoutId = 0;
                this.alignSkewButton();
                this.ptPivot.domTimeout = true
            }, 0);
        }
        this.context.page.linkStoreToInput(this.bulkSkewMode, 'bulkSkewMode', page$, {persist: true, storageKey: 'bulkSkewMode', cb: bs_cb});
        await bs_cb(this.context.page.page$.get('bulkSkewMode'), false);
        // this.alignWidth();
    }

    async writeVenueToClipboard(venue) {
        let trueVenue = this.context.page.portfolioMeta$.get("venue") || 'ib';
        if (trueVenue !== 'ib') {
            trueVenue = mapVenueFromRfq(trueVenue)
        }
        const clean_venue = mapVenueFromRfq(venue)
        const cols_needed = {...getVenueCopyColumns(clean_venue, false)};
        if (cols_needed) {
            if (this.context.page.page$.get('activeQuoteType') === 'client') {
                if ('newLevel' in cols_needed) {
                    cols_needed['newLevelDisplay'] = cols_needed.newLevel;
                    delete cols_needed.newLevel
                }
            }
            const fields = Object.keys(cols_needed);
            fields.push('isReal', 'assignedTrader');
            if (!fields.includes('grossSize')) {
                fields.push('grossSize')
            }

            const data = this.adapter.engine.getColumns(fields)
            const headers = {};

            Object.entries(cols_needed).forEach(([k, v]) => {
                if (!Array.isArray(v)) {
                    headers[k] = v;
                } else {
                    headers[k] = v[0];
                }
            });
            const reformat = data.filter(row => {
                return !(
                    (row.isReal == null) ||
                    (row.isReal === 0) ||
                    (row.grossSize == null) ||
                    (row.grossSize === 0) ||
                    (row.assignedTrader === "REMOVED")
                )
            }).map(row => {
                const clean = {};
                Object.entries(row).forEach(([k, v]) => {
                    if (!(k in cols_needed)) return;
                    if (!Array.isArray(cols_needed[k])) {
                        clean[k] = v;
                    } else {
                        clean[k] = cols_needed[k][1](v) || '';
                    }
                })
                return clean
            });
            const removedCount = data.length - reformat.length;
            await writeObjectToClipboard(reformat, {headerOverride: headers, addCommaToNumerics: clean_venue === 'ib'});

            const mismatchVenue = (clean_venue !== trueVenue) && (clean_venue !== "ib")
            let msg = `Copied data for: ${clean_venue.toUpperCase()}`;
            if (mismatchVenue) {
                msg = msg + `<br>Venue Expected: ${trueVenue.toUpperCase()}`
            }
            if (removedCount > 0) {
                msg = msg + `<br>**REMOVED ${removedCount} bonds**`
            }
            if (!mismatchVenue) {
                this.context.page.toastManager().success('Submission Copy', msg)
            } else {
                this.context.page.toastManager().warning('Submission Copy - Mismatched Venue', msg)
            }
        } else {
            this.context.page.toastManager().error('Submission Copy', `Failed to write to clipboard.`)
        }
    }

    _setupSkewModeControls(pivot) {
        if (!this.skewModeGroup) return;
        const widget = this;

        // Populate percent market select with available markets
        this._populatePctMarketOptions();

        // Radio change: outright vs percent
        this.context.page.addEventListener(this.skewModeGroup, 'change', (e) => {
            const mode = e.target.value;
            pivot.setSkewMode(mode);
            if (widget.pctMarketWrap) {
                widget.pctMarketWrap.style.display = mode === 'percent' ? '' : 'none';
            }
            // Clear pending skews when switching mode to avoid confusion
            pivot.resetAllBuckets();
            widget._updatePendingBar(0);
        });

        // Market select for percent width source
        if (this.pctMarketSelect) {
            this.context.page.addEventListener(this.pctMarketSelect, 'change', (e) => {
                pivot.setSkewPctMarket(e.target.value || null);
            });
        }
    }

    _populatePctMarketOptions() {
        if (!this.pctMarketSelect) return;
        const page = this.context.page;
        const marketMap = page.marketDataMap;
        if (!marketMap || !marketMap.size) return;
        // Keep the default "(Ref Mkt)" option, then add available markets
        const frag = document.createDocumentFragment();
        const def = document.createElement('option');
        def.value = '';
        def.textContent = '(Ref Mkt)';
        frag.appendChild(def);
        for (const [key, meta] of marketMap.entries()) {
            const opt = document.createElement('option');
            opt.value = key;
            opt.textContent = meta?.abbr || meta?.label || key;
            frag.appendChild(opt);
        }
        this.pctMarketSelect.innerHTML = '';
        this.pctMarketSelect.appendChild(frag);
    }

    _setupHotkeys() {
        const page$ = this.context.page.page$;
        const widget = this;
        this.context.page.addEventListener(document, 'keydown', async (event) => {

            // Ctrl+Shift+f to toggle grid link
            if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === 'f') {
                event.preventDefault();
                if (this.filterToggle) {
                    const newValue = !this.filterToggle.checked;
                    this.filterToggle.checked = newValue;
                    page$.set('linkedPivotFilters', newValue)
                }
            } else if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === 'b') {
                event.preventDefault();
                if (this.bulkSkewMode) {
                    const newValue = !this.bulkSkewMode.checked;
                    this.bulkSkewMode.checked = newValue;
                    page$.set('bulkSkewMode', newValue)
                }
            }

            // Ctrl+shift+c to copy
            else if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === 'c') {
                event.preventDefault();
                event.stopPropagation();
                if (widget._applyInFlight) return;
                widget._applyInFlight = true;
                try {
                    const venue = widget.context.page.portfolioMeta$.get("venue") || 'ib';
                    await widget.writeVenueToClipboard(venue);
                    await widget.context.page.send_full_push("Submission", true);
                } finally {
                    widget._applyInFlight = false;
                }
            }

        });
    }


    async onActivate() {
        console.log('[PivotWidget] activated');
        this.ptPivot._locked = false;
        this.ptPivot.isActive = true;
        await this.alignSkewButton();
    }

    _scheduleAlignSkewButton() {
        if (this._alignRafId) return;
        this._alignRafId = requestAnimationFrame(async () => {
            this._alignRafId = 0;
            await this.alignSkewButton();
        });
    }

    async alignSkewButton() {

        if (!this.ptPivot?.api) return;
        const current_skew = this.ptPivot.api.getColumn('current_skew');
        const input_skew = this.ptPivot.api.getColumn('input_skew');
        const prop_skew = this.ptPivot.api.getColumn('proposed_skew');
        if (!current_skew || !input_skew || !prop_skew) return;

        const barrier = this.bulkBar.getBoundingClientRect().width || 0
        const cost = this.topBtnWrapper.getBoundingClientRect().width || 0;

        const max_skew = Math.max(0, (barrier || 0) - (cost || 0));

        const minLeft = Math.max(0, Math.min(current_skew.left, input_skew.left, prop_skew.left));
        this.topBtnWrapper.style.transform = `translateX(${Math.min(max_skew, minLeft - 10)}px)`;

        const w = current_skew.getActualWidth() + input_skew.getActualWidth() + prop_skew.getActualWidth();
        this.topBtnWrapper.style.width = `${w - 65}px`;

    }

    onResumeSubscriptions() {
        this.ptPivot.hardRefresh({force: true})
    }

    onDeactivate() {
        // console.log('[PivotWidget] deactivated');
        // this.ptPivot._locked = true;        // this.ptPivot.isActive = false;
    }

    async onCleanup() {
        // Drain subscription disposers
        for (const dispose of this._disposers) {
            try {
                if (typeof dispose === 'function') dispose();
            } catch (_) {
            }
        }
        this._disposers.length = 0;

        if (this._alignRafId) {
            cancelAnimationFrame(this._alignRafId);
            this._alignRafId = 0;
        }
        if (this._presetTimeoutId) {
            clearTimeout(this._presetTimeoutId);
            this._presetTimeoutId = 0;
        }
        if (this._resizeObserver) {
            try { this._resizeObserver.disconnect(); } catch(_) {}
            this._resizeObserver = null;
        }

        try {
            this.api?.destroy();
        } catch (_) {
        }
        try {
            this.ptPivot.dispose();
        } catch (_) {
        }


        if (this.interactable) {
            try {
                this.interactable.unset();
            } catch (_) {
            }
            this.interactable = null;
        }
        if (this.tt) {
            try {
                this.context.page.tooltipManager()?.remove?.('proceed-lock-tooltip');
            } catch (_) {
            }
            this.tt = null;
        }
        try {
            if (this._applyRafId) cancelAnimationFrame(this._applyRafId);
            this._applyRafId = 0;
            this._applyTries = 0;
        } finally {
            if (this.dynamicController) this.dynamicController.abort();
        }
        if (this.widgetDom) {
            this.widgetDom.innerHTML = '';
        }
    }

    // --- UI & Controls ---

    onRender() {
        this.widgetDiv.innerHTML = `
        <div class="pivot-controls">
            <div class="pivot-controls-top">
                <div class="pivot-control-group">
                    <label>
                        <div class="piv-left">
                            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"><path fill="currentColor" d="M10 8V3h9q.825 0 1.413.588T21 5v3zM5 21q-.825 0-1.412-.587T3 19v-9h5v11zM3 8V5q0-.825.588-1.412T5 3h3v5zm9.85 11l.875.9q.275.275.275.688t-.3.712q-.275.275-.7.275t-.7-.275l-2.6-2.6q-.3-.3-.3-.7t.3-.7l2.6-2.6q.275-.275.688-.287t.712.287q.275.275.288.688t-.263.712l-.875.9H15q.825 0 1.413-.587T17 15v-2.2l-.9.9q-.275.275-.7.275t-.7-.275t-.275-.7t.275-.7l2.6-2.6q.3-.3.7-.3t.7.3l2.6 2.6q.275.275.275.7t-.275.7t-.7.275t-.7-.275l-.9-.9V15q0 1.65-1.175 2.825T15 19z"/></svg>
                            <p>Pivot Portfolio</p>
                        </div>
                        <div class="piv-right">
                            <button class="tooltip-left" id="pivotSettings" data-tooltip="Settings">
                                <lord-icon src="/assets/lottie/cog-hover-2.json" trigger="hover" class="current-color" stroke="currentColor" style="width: 16px; height: 16px;"></lord-icon>
                            </button>
                        </div>
                    </label>
                </div>
                <div id="pivot-quick-presets">
                    <div id="pivot-quick-main">
                        <div class="colored-pill indigo-pill pill-button btn" data-column="assignedTrader">Assigned</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="desigName">Desig</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="lastEditUser">LastEdit</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="isDnt">DNT</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="bvalSubAssetClass">Asset</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="hasPriced">Priced</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="liqScoreCombinedGroup">Liq</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="firmPositionBucket">Firm</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="algoPositionBucket">Algo</div>
                    </div>
                    <div id="pivot-quick-clear">
                        <div class="colored-pill gold-pill pill-button btn" data-column="CLEAR">Clear Groups</div>
                        <div class="pivot-quick-groups-title">Quick Groups</div>
                    </div>
                </div>
            </div>
            <div class="pivot-controls-middle">
                <div class="pivot-control-group pivot-skew-mode-group">
                    <span class="label-text skew-mode-label">Skew Mode</span>
                    <div id="pivot-skew-mode" class="pivot-skew-mode-radios">
                        <label class="skew-mode-option">
                            <input type="radio" name="pivotSkewMode" value="outright" checked />
                            <span>Outright</span>
                        </label>
                        <label class="skew-mode-option">
                            <input type="radio" name="pivotSkewMode" value="percent" />
                            <span>%&nbsp;Width</span>
                        </label>
                    </div>
                    <div id="pivot-pct-market-wrap" class="pivot-pct-market-wrap" style="display:none;">
                        <span class="label-text pct-mkt-label">of</span>
                        <select id="pivot-pct-market" class="pivot-pct-market-select">
                            <option value="">(Ref Mkt)</option>
                        </select>
                    </div>
                </div>
            </div>
            <div class="pivot-controls-bottom">
                <div class="pivot-control-group middle-wrapper">
                                        
                    <div class="pivot-control-group pivot-toggle">
                        <label class="label">
                            <span class="label-text">Use grid filters?</span>
                            <input id="filter-pivot-by-grid" type="checkbox" class="checkbox"/>
                        </label>
                        <label class="label">
                            <span class="label-text">Colorize</span>
                            <input id="pivot-colorize-toggle" type="checkbox" class="checkbox"/>
                        </label>
                        <label class="label">
                            <span class="label-text tooltip tooltip-top" data-tooltip="Grand totals ignore filters; top rows still follow them.">Guard grid totals?</span>
                            <input id="pivot-locktotals-toggle" type="checkbox" class="checkbox"/>
                        </label>
                    </div>
                </div>
                <div class="pivot-control-group bottom-wrapper">
                    <div class="pivot-control-group bulkskew-wrapper">
                        <div class="pivot-control-group pivot-toggle pivot-switch" id="pivot-bulkskew-toggle-outer">
                            <label class="label">
                                <input id="pivot-bulkskew-toggle" type="checkbox" class="toggle"/>
                                <span class="label-text">Bulk Skew Mode</span>
                            </label>
                        </div>
                        <div id="bulk-skew-apply-btn" style="display:none;">
                            <button class="btn btn-primary btn-sm" id="pivot-bulk-skew-apply">APPLY</button>
                            <button class="btn btn-primary btn-sm" id="pivot-bulk-skew-clear">CLEAR</button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div id="pivot-grid-container" class="ag-theme-alpine fill-parent">
            <div id="pivot-topbar" class="pivot-topbar">
                <div class="pt-left">
                    <div id="pivot-top-btn-wrapper">
                        <button id="pivot-top-discard" class="btn btn-xs disabled">Discard</button>
                        <button id="pivot-top-apply" class="btn btn-xs disabled" disabled>Apply Pending Skews</button>
                        <button id="pivot-top-lock" class="btn btn-xs">
                             <span class="icon-wrap">
                                 <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 48 48"><g fill="none" stroke="currentColor" stroke-linejoin="round" stroke-width="4"><rect width="34" height="22" x="7" y="22.048" rx="2"/><path stroke-linecap="round" d="M14 22v-7.995c-.005-5.135 3.923-9.438 9.086-9.954S32.967 6.974 34 12.006M24 30v6"/></g></svg>
                            </span>
                           
                        </button>
                    </div>
                    <span id="pivot-pending-count" class="pending-indicator"></span>
                </div>
            </div>
        </div>`;
    }


    _initControls() {
        const {defs} = this._validGroupFields();
        const groupByOptions = defs.map(cd => ({
            label: cd?.context?.aggregationName ?? cd.headerName ?? cd.field,
            value: cd.field,
            group: cd.context?.customColumnGroup || 'General'
        }));

        const t2 = document.getElementById('portfolio-top-two-rows'); // z-index shim if present
        const pivot = this.ptPivot;
        const widget = this;

        const vc = document.getElementById('venue-caret');
        const gg = document.getElementById('portfolio-pricing-grid');
        if (vc && this.context?.page?.addEventListener) {
            this.context.page.addEventListener(vc, 'mousedown', () => {
                if (t2) t2.style.zIndex = "2";
                if (gg) gg.style.zIndex = "0";
            });
            const vcb = vc.querySelector('*');
            if (vcb) {
                this.context.page.addEventListener(vcb, 'blur', () => {
                    if (t2) t2.style.zIndex = "0";
                    if (gg) gg.style.zIndex = "auto";
                });
            }
        }
    }

}
