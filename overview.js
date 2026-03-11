
import {BaseWidget} from "@/pt/js/widgets/baseWidget.js";
import interact from 'interactjs';
import {generateGradientChroma} from '@/utils/colorHelpers.js';
import {writeObjectToClipboard,} from "@/utils/clipboardHelpers.js";
import {QT_MAP} from '@/pt/js/grids/portfolio/portfolioColumns.js';
import {coerceToNumeric, roundToNumeric, coerceToBool} from "@/utils/NumberFormatter.js";
import { ENumberFlow } from '@/utils/eNumberFlow.js';
import {debounce, asArray} from '@/utils/helpers.js';
import {InfoCardStack} from '@/global/js/cardStack.js';
import {sort_dictionary_by_value} from "@/utils/utility.js";
import {NumberFormatter} from "@/utils/NumberFormatter.js";
import {StringFormatter} from "@/utils/StringFormatter.js";

const clean_camel = StringFormatter.clean_camel;

function buildTable(dataArray) {
    const table = document.createElement('table');
    table.classList.add('overview-modal-table');
    table.style.borderCollapse = 'collapse'; // For a cleaner look
    table.style.width = '100%';

    // Create table header
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    dataArray[0].forEach(headerText => {
        const th = document.createElement('th');
        th.textContent = headerText;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Create table body
    const tbody = document.createElement('tbody');
    for (let i = 1; i < dataArray.length; i++) { // Start from index 1 to skip header
        const rowData = dataArray[i];
        const tr = document.createElement('tr');
        rowData.forEach(cellData => {
            const td = document.createElement('td');
            td.textContent = cellData;
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    return table
}

function slugify(str) {
    str = str.toLowerCase();
    str = str.replace(/[^a-z0-9 -]/g, ''); // remove non-alphanumeric characters
    str = str.replace(/\s+/g, '-'); // replace spaces with hyphens
    str = str.replace(/-+/g, '-'); // replace consecutive hyphens with a single hyphen
    return str.trim(); // trim leading/trailing whitespace
}
function ensure_list(x) { return Array.isArray(x) ? x : [x]; }

export class OverviewWidget extends BaseWidget {
    constructor(context, widgetId, managerId, selector, config={}) {
        super(context, widgetId, managerId, selector, config);
        this.bidSkew = null;
        this.askSkew = null;
        this.resizer = null;
        this.pillManager = this.context.page.pillManager;
    }

    async onInit() {
        this.cacheDom();
        this.bindEvents();
        this.setupResizer();
        await this.setupFlags();
        this.setupReactions();
        this.pivotEngine = this.context.page.getWidget('pivotWidget').ptPivot.pivotEngine;
    }

    async afterMount() {
        if (this?.manager?.grid) {
            this.manager.grid.grid$.pick('weight').onChanges(async (ch) => {
                const w = ch.current.get('weight');
                // console.log('weight changed to:', w)
                this.kpi_elements.forEach(elem => {
                    if (elem.getAttribute('data-weight')) {
                        elem.setAttribute('data-weight', w)
                    }
                });
                await this.refreshOverview();
            });
        }
    }

    cacheDom() {
        this.pillContent = document.getElementById('overview-pill-section');
        this.flagContent = document.getElementById('overview-flag-section');
        this.currentSkew = document.getElementById('kpi-current-skew');
        this.kpiGrid = document.getElementById('kpi-metrics-grid');
        this.kpi_elements = this.kpiGrid.querySelectorAll('.kpi-metric');
    }

    getActiveWeightKey() {
        return this.manager.grid.grid$.get('weight')
    }


    async updateWeightMode(mode) {
        if (mode === this.weightMode) return;
        this.weightMode = mode;
        await this.refreshOverview()
    }

    async getData(cols) {
        if (!this.manager.grid) return
        return await this.manager.grid.engine.getAllRows({columns:cols});
    }

    // Columns the overview widget always needs for KPI computation,
    // distribution bars, liquidity cards, and pivot breakdowns.
    static REQUIRED_COLUMNS = [
        // KPI top-line + net
        'grossSize', 'grossDv01', 'netSize', 'netDv01',
        // KPI secondary metrics
        'duration', 'liqScoreCombined', 'signalLiveStats',
        // Axed / anti-axed computation
        'axeDirection', 'direction', 'isAxeOrMkt', 'isAntiAxe',
        // BSR / Yield / Algo KPIs
        'firmAggBsrSize', 'unitDv01', 'bvalMidYld', 'isInAlgoUniverse',
        // Right-panel chart pivots
        'emProductType', 'yieldCurvePosition',
        // Liquidity charges
        'QT',
        // Distribution bar dropdown options (all categorical columns)
        'deskAsset', 'regionCountry', 'desigName', 'maturityBucket',
        'ratingCombined', 'ratingMnemonic', 'industrySector',
        // Pill flags
        'description', 'isin', 'currency', 'isPriced', 'claimed',
        'isMuni', 'isNewIssue', 'isInDefault', 'isDnt', 'restrictedCode',
    ];

    /** Columns needed for this refresh (static + current distribution selection). */
    _getColumnsToEnsure(columns=null) {
        columns = columns ?? [];
        const cols = [...OverviewWidget.REQUIRED_COLUMNS, ...columns];
        const distCol = document.getElementById('distribution-metric-select')?.value;
        if (distCol) cols.push(distCol);
        return Array.from(new Set(cols))
    }

    async refreshOverview() {
        if (!this.manager.grid) return;
        if (!this.isActive) return;

        const engine = this.manager.grid.engine;
        // await engine.ensureColumns(this._getColumnsToEnsure());
        const portfolio = await this.getData(this._getColumnsToEnsure());
        await this.updateWidget(portfolio);
    }


    setupFlags() {
        const pills = this.context.page.pillManager;
        const DETAILS = this.pillContent;
        const FLAGS = this.flagContent;

        // DETAILS --------------------------------------

        pills.createPill('meta', {
            id: 'algoPct',
            columns: 'algoPct',
            label: 'Algo Eligible:',
            color: 'gray',
            valueFormatter: (v) => v ? this.formatNumber(v * 100, false, true, 0) : null,
            tooltip: null,
        }).then(p => p.mount(DETAILS));


        // FLAGS --------------------------------------

        pills.createPill('notDistinct', {
            id: 'quoteType',
            columns: 'quoteType',
            type: 'warning',
            label: 'Multi QT:',
            valueFormatter: (val, pill) => Array.from(val?.seenValues || []).toSorted().join(", "),
            nullPolicy: 'hide'
        }).then(p => p.mount(FLAGS));

        ["inEtfAgg","inEtfLqd","inEtfHyg","inEtfJnk","inEtfEmb","inEtfSpsb","inEtfSpib","inEtfSplb","inEtfVcst","inEtfVcit","inEtfVclt","inEtfSpab","inEtfIgib","inEtfIglb","inEtfIgsb","inEtfIemb","inEtfUshy","inEtfSjnk"].forEach((etf) => {
            pills.createPill('custom', {
                id: `pctInEtf-${etf}`,
                columns: (pill) => {
                    const w = pill.mgr.context.page.getWidget('overviewWidget').getActiveWeightKey();
                    return [etf, w]
                },
                type: 'status',
                valueGetter: (data, pill) => {
                    const wc = Object.keys(data).filter(x=>!x.startsWith('inEtf'));
                    const ws = data[wc];
                    const es = data[etf];
                    const r = [0, 0];
                    es.forEach((e, idx) => {
                        const w = ws[idx];
                        r[1] += w;
                        if ((e != null) && (e !== 0)) r[0] += w;
                    });
                    if (r[1] !== 0) {
                        return r[0]/r[1]
                    }
                    return null
                },
                valueFormatter: (val, pill) => {
                    const ticker = etf.replace('inEtf','').toUpperCase();
                    let label = `${ticker} Overlap:`;
                    if (val < 0.25) {
                        label = `LOW ${ticker} Overlap:`;
                        pill.root.classList.add('pill-warning');
                        pill.root.classList.remove('pill-success');
                    } else if (val > 0.9) {
                        label = `HIGH ${ticker} Overlap:`;
                        pill.root.classList.remove('pill-warning');
                        pill.root.classList.add('pill-success');
                    }
                    return `${label} ${Math.round(val * 100)}%`
                },
                label: null,
                condition: (v, pill) => {
                    const a = (pill.mgr.context.page._metaStore.get('assetClass'))?.split(",");
                    if (a == null) return;
                    return (
                        (a.includes("IG") && (etf === 'inEtfLqd')) ||
                        (a.includes("HY") && (etf === 'inEtfHyg')) ||
                        (a.includes("EM") && (etf === 'inEtfEmb')) ||
                        (v && v > 0.75)
                    )
                },
                tooltip: null,
            }).then(p => p.mount(DETAILS));
        })

        try {
            pills.createPill('meta', {
                id: 'bsrPct',
                columns: 'bsrPct',
                label: null,
                type: 'status',
                valueFormatter: (v) => v && (v > 0.25)
                    ? `Balance Sheet Reducing: ${this.formatNumber(v * 100, false, true, 0)}`
                    : null,
                tooltip: null,
            }).then(p => p.mount(DETAILS));
        } catch {}

        try {
            pills.createPill('meta', {
                id: 'axePct',
                columns: 'axePct',
                label: null,
                type: 'status',
                valueFormatter: (v) => v && (v > 0.15) ? `Axed: ${this.formatNumber(v * 100, false, true, 0)}` : null,
                tooltip: null,
            }).then(p => p.mount(DETAILS));
        } catch {}

        const maturityBucket = (y) => {
            if (y == null || isNaN(+y)) return null;
            const v = +y;
            if (v <= 2) return 'Front-end';
            if (v <= 5) return 'Belly';
            if (v <= 10) return 'Intermediate';
            return 'Long-End';
        };

        const hhiClass = (v) => (v == null ? null : (v > 0.7 ? 'Highly Concentrated' : (v < 0.2 ? 'Diversified' : null)));

        try {
            pills.createPill('meta', {
                id: 'isCrossed',
                columns: 'isCrossed',
                type: 'warning',
                valueFormatter: (v) => v ? 'TSY CROSSED' : null
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('distinct', {
                id: 'currency',
                columns: 'currency',
                type: 'warning',
                valueFormatter: (val) => {
                    if (val.distinct > 1) return `Multi Currency: ${Array.from(val.seenValues).toSorted().join(", ")}`;
                    if (!val.seenValues.includes('USD')) return `Non-USD: ${Array.from(val.seenValues).toSorted().join(", ")}`;
                    return null;
                }
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('meta', {
                id: 'creditRating',
                columns: 'creditRating',
                label: 'Credit Rating',
                type: 'portfolio',
                nullPolicy: 'showX'
            }).then(p => p.mount(DETAILS));
        } catch (e) {}

        try {
            pills.createPill('meta', {
                id: 'removedCount',
                columns: 'removedCount',
                type: 'warning',
                valueFormatter: (v) => (v > 0 ? `Removed bonds: ${v | 0}` : null)
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('custom', {
                id: 'clientSellingBucket',
                type: 'portfolio',
                valueGetter: async (data, pill) => {
                    const m = pill.mgr.metaStore; if (!m) return null;
                    const side = m.get('rfqSide');
                    if (side !== 'BWIC' && side !== 'BOWIC') return null;
                    let y = m.get('bwicYrsToMaturity'); if (y == null) y = m.get('yrsToMaturity');
                    const b = maturityBucket(y);
                    return b ? `Client Selling: ${b}` : null;
                }
            }).then(p => p.mount(DETAILS));
        } catch (e) {}

        try {
            pills.createPill('custom', {
                id: 'clientBuyingBucket',
                type: 'portfolio',
                valueGetter: async (data, pill) => {
                    const m = pill.mgr.metaStore; if (!m) return null;
                    const side = m.get('rfqSide');
                    if (side !== 'OWIC' && side !== 'BOWIC') return null;
                    let y = m.get('owicYrsToMaturity'); if (y == null) y = m.get('yrsToMaturity');
                    const b = maturityBucket(y);
                    return b ? `Client Buying: ${b}` : null;
                }
            }).then(p => p.mount(DETAILS));
        } catch (e) {}

        try {
            pills.createPill('meta', {
                id: 'hhiDesigId',
                columns: 'hhiDesigId',
                label: 'Desig',
                type: 'portfolio',
                valueFormatter: (v) => hhiClass(v),
                styleRules: [
                    { gt: 0.7, color: 'red', type: 'warning' },
                    { lt: 0.2, color: 'green', type: 'portfolio' }
                ]
            }).then(p => p.mount(DETAILS));
        } catch (e) {}

        try {
            pills.createPill('meta', {
                id: 'hhiMaturityBucket',
                columns: 'hhiMaturityBucket',
                label: 'Maturity Bucket',
                type: 'portfolio',
                valueFormatter: (v) => hhiClass(v),
                styleRules: [
                    { gt: 0.7, color: 'red', type: 'warning' },
                    { lt: 0.2, color: 'green', type: 'portfolio' }
                ]
            }).then(p => p.mount(DETAILS));
        } catch (e) {}

        try {
            pills.createPill('meta', {
                id: 'hhiRatingAssetClass',
                columns: 'hhiRatingAssetClass',
                label: 'Asset Class',
                type: 'portfolio',
                valueFormatter: (v) => hhiClass(v),
                styleRules: [
                    { gt: 0.7, color: 'red', type: 'warning' },
                    { lt: 0.2, color: 'green', type: 'portfolio' }
                ]
            }).then(p => p.mount(DETAILS));
        } catch (e) {}

        try {
            pills.createPill('custom', {
                id: 'liquidityBasket', type: 'portfolio',
                valueGetter: async (data, pill) => {
                    const m = pill.mgr.metaStore; if (!m) return null;
                    return m.get('liqScoreCombined');
                },
                valueFormatter: (score) => {
                    if (score == null) return null;
                    if (score > 7) return 'Liquid Basket';
                    if (score < 4) return 'Illiquid Basket';
                    return null;
                },
                styleRules: [
                    { gt: 7, color: 'green', type: 'status' },
                    { lt: 4, color: 'red', type: 'warning' }
                ]
            }).then(p => p.mount(DETAILS));
        } catch (e) {}

        const tooltipFromLines = (lines, max = 8) => {
            if (!lines || !lines.length) return '';
            const out = lines.length > max ? [...lines.slice(0, max), `+${lines.length - max} more`] : lines;
            return out.join('\n');
        };

        try {
            pills.createPill('count', {
                id: 'pill_missing_descriptions',
                columns: 'description',
                equals: (v) => v == null || String(v).trim() === '',
                min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `Missing Descriptions: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'description', (v) => v == null || String(v).trim() === '')),
                modalFn: mkModal('Missing Descriptions', (eng) => maskByPredicate(eng, 'description', (v) => v == null || String(v).trim() === ''), 'info'),
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_missing_dv01',
                columns: 'grossDv01',
                equals: (v) => v == null, min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `Missing DV01: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'grossDv01', (v) => v == null)),
                modalFn: mkModal('Missing DV01', (eng) => maskByPredicate(eng, 'grossDv01', (v) => v == null), 'info', ['description', 'isin', 'grossSize']),
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_contains_muni',
                columns: 'isMuni',
                equals: (v) => +v === 1, min: 1, type: 'warning',
                valueFormatter: (count) => count ? 'Contains Muni' : null,
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isMuni', (v) => +v === 1)),
                modalFn: mkModal('Contains Muni', (eng) => maskByPredicate(eng, 'isMuni', (v) => +v === 1))
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_contains_new_issue',
                columns: 'isNewIssue',
                equals: (v) => +v === 1, min: 1, type: 'warning',
                valueFormatter: (count) => count ? 'Contains New Issue' : null,
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isNewIssue', (v) => +v === 1)),
                modalFn: mkModal('Contains New Issue', (eng) => maskByPredicate(eng, 'isNewIssue', (v) => +v === 1), 'info', ['description', 'isin', 'issueDate', 'isWhenIssued', 'announcementDate'])
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_contains_default',
                columns: 'isInDefault',
                equals: (v) => +v === 1, min: 1, type: 'error',
                valueFormatter: (count) => count ? 'Contains Default' : null,
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isInDefault', (v) => +v === 1)),
                modalFn: mkModal('Contains Default', (eng) => maskByPredicate(eng, 'isInDefault', (v) => +v === 1), 'info')
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_non_standard_settle',
                columns: 'daysToSettle',
                equals: (v) => (+v || 0) > 2, min: 1, type: 'warning',
                valueFormatter: (count) => count ? `Non-Standard Settle: ${count}` : null,
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'daysToSettle', (v) => (+v || 0) > 2), ['description', 'daysToSettle']),
                modalFn: mkModal('Non-Standard Settle', (eng) => maskByPredicate(eng, 'daysToSettle', (v) => (+v || 0) > 2), 'info', ['description', 'isin', 'daysToSettle', 'isNewIssue'])
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_blocks_over_10m',
                columns: 'grossSize',
                equals: (v) => Math.abs(+v || 0) > 10_000_000, min: 1, type: 'warning',
                valueFormatter: (count) => count ? `Blocks: ${count}` : null,
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'grossSize', (v) => Math.abs(+v || 0) > 10_000_000)),
                modalFn: mkModal('Blocks > 10mm', (eng) => maskByPredicate(eng, 'grossSize', (v) => Math.abs(+v || 0) > 10_000_000), 'info', ['description', 'isin', 'userSide', 'grossSize'])
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_rfq_bmk_mismatch',
                columns: 'isRfqBenchmarkMismatch',
                equals: (v) => +v === 1, min: 1, type: 'error',
                valueFormatter: (count) => count ? 'RFQ Benchmark Mismatch' : null,
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isRfqBenchmarkMismatch', (v) => +v === 1)),
                modalFn: mkModal('RFQ Benchmark Mismatch', (eng) => maskByPredicate(eng, 'isRfqBenchmarkMismatch', (v) => +v === 1), 'error')
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_missing_desig', columns: ['desigName'],
                equals: (v) => v == null || String(v).trim() === '', min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `Missing Desig: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'desigName', (v) => v == null || String(v).trim() === ''), ['description']),
                modalFn: mkModal('Missing Desig', (eng) => maskByPredicate(eng, 'desigName', (v) => v == null || String(v).trim() === ''))
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_missing_assigned', columns: ['assignedTrader'],
                equals: (v) => v == null || String(v).trim() === '', min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `Unassigned: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'assignedTrader', (v) => v == null || String(v).trim() === ''), ['description']),
                modalFn: mkModal('Unassigned Bonds', (eng) => maskByPredicate(eng, 'assignedTrader', (v) => v == null || String(v).trim() === ''))
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_bval_mismatch', columns: ['isBvalBenchmarkMismatch'],
                equals: (v) => v === 1, min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `BVAL Mismatch: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isBvalBenchmarkMismatch', (v) => v === 1), ['description']),
                modalFn: mkModal('BVAL Benchmark Mismatch', (eng) => maskByPredicate(eng, 'isBvalBenchmarkMismatch', (v) => v === 1), 'info', {
                    'isin': 'isin', 'description': 'description', 'bvalBenchmarkTenor': 'BVAL BM', 'bvalBenchmarkIsin': 'BVAL ISIN', 'benchmarkName': 'PT BM', 'benchmarkIsin': 'PT Bench ISIN', 'bvalBenchYldDiff': 'Approx. BPS Diff'
                }, 'Note: [approx bps diff] = ( [live yld of BVAL tenor] - [live yld of PT tenor] ) * 100', ['benchmarkName', 'description'])
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_macp_mismatch', columns: ['isMacpBenchmarkMismatch'],
                equals: (v) => v === 1, min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `CP+ Mismatch: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isMacpBenchmarkMismatch', (v) => v === 1), ['description']),
                modalFn: mkModal('CP+ Benchmark Mismatch', (eng) => maskByPredicate(eng, 'isMacpBenchmarkMismatch', (v) => v === 1), 'info', {
                    'isin': 'isin', 'description': 'description', 'macpBenchmarkTenor': 'CP+ BM', 'macpBenchmarkIsin': 'CP+ ISIN', 'benchmarkName': 'PT BM', 'benchmarkIsin': 'PT Bench ISIN'
                }, null, ['benchmarkName', 'description'])
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_stub', columns: ['isStub'],
                equals: (v) => v === 1, min: 1, type: 'warning',
                valueFormatter: (count) => (count > 0 ? `Stub Sizes: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isStub', (v) => v === 1), ['description']),
                modalFn: mkModal('Stub Sizes', (eng) => maskByPredicate(eng, 'isStub', (v) => v === 1), 'info', ['isin', 'description', 'grossSize'])
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('custom', {
                id: 'pill_claimed_bonds_sum', columns: ['claimed'], type: 'portfolio',
                valueGetter: async (data, pill) => {
                    let sum = 0, any = false;
                    const mask = new Array((data || []).length);
                    for (let i = 0; i < (data || []).length; i++) {
                        const v = data[i];
                        const num = (v == null || v === '') ? 0 : +(v ? 1 : 0);
                        if (num > 0) {
                            any = true;
                            sum += num; mask[i] = true;
                        } else mask[i] = false;
                    }
                    return any ? { sum, mask } : null;
                },
                valueFormatter: (obj) => obj ? `Claimed Bonds: ${Math.round(obj.sum)}` : null,
                tooltipFn: (_disp, pill) => {
                    const raw = pill._lastRaw;
                    if (!raw || !raw.mask) return '';
                    const lines = buildLines(pill.mgr.engine, raw.mask);
                    return tooltipFromLines(lines);
                },
                modalFn: (pill) => {
                    const raw = pill._lastRaw;
                    const lines = raw && raw.mask ? buildLines(pill.mgr.engine, raw.mask) : [];
                    return modalPayload(`Claimed Bonds: ${raw ? Math.round(raw.sum) : 0}`, lines, ['description', 'isin'], 'portfolio');
                }
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_dnt_count', columns: ['isDnt'],
                equals: (v) => coerceToBool(v), min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `DNT Bonds: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'isDnt', (v) => coerceToBool(v)), ['description']),
                modalFn: mkModal('DNT Bonds', (eng) => maskByPredicate(eng, 'isDnt', (v) => coerceToBool(v)), 'info', {
                    'isin': 'isin', 'description': 'Description', 'userSide': 'Side', 'grossSize': 'Size', 'desigName': 'Desig', 'assignedTrader': 'Assigned', 'dntComment': 'DNT', 'dntEventEnd': 'End Time', 'dntModifiedBy': 'Modified By'
                }, null)
            }).then(p => p.mount(FLAGS));
        } catch (e) {}

        try {
            pills.createPill('count', {
                id: 'pill_restrictedCode_count', columns: ['restrictedCode'],
                equals: (v) => v != null, min: 1, type: 'error',
                valueFormatter: (count) => (count > 0 ? `RESTRICTED: ${count}` : null),
                tooltipFn: mkTooltip((eng) => maskByPredicate(eng, 'restrictedCode', (v) => v != null), ['description']),
                modalFn: mkModal('Restricted Bonds', (eng) => maskByPredicate(eng, 'restrictedCode', (v) => v != null), 'info', {
                    'isin': 'isin', 'description': 'Description', 'userSide': 'Side', 'grossSize': 'Size', 'desigName': 'Desig', 'assignedTrader': 'Assigned', 'restrictedCode': 'Code', 'restrictionTier': 'Tier', 'restrictionTime': 'Time'
                }, null)
            }).then(p => p.mount(FLAGS));
        } catch (e) {}
    }


    setupResizer() {
        const target = document.querySelector('.right-side');
        this.resizer = interact(target)
        .resizable({
            edges: {top: false, left: true, bottom: false, right: false},
            listeners: {
                move: function (event) {
                    const p = event.target.parentElement.getBoundingClientRect();
                    let xPos = 1 - (event.client.x - p.left) / (p.right - p.left);
                    xPos = Math.max(0.3, Math.min(0.7, xPos));
                    event.target.style.width = `${xPos * 100}%`;
                }
            }
        });
    }

    getShortString(withLink=true) {
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML  = formatPortfolioSummary(this.context.page.portfolioMeta$.asObject());
        const txt = (tempDiv.textContent || tempDiv.innerText || '').replace(/ +/g, ' ');
        return `${txt}\n${this.getLinkString()}`
    }

    getLinkString() {
        const client = clean_camel(this.context.page._metaStore.get('client') ?? '').toUpperCase();
        let sub_name = client.substring(0,10);
        if (sub_name.endsWith("ASS")) sub_name += client.substring(10) // Happens more often than youd think
        return window.location.href.split("?")[0] + `?name=${sub_name}`
    }

    getKeyString() {
        return window.location.href.split("/pt/")[1].split("?")[0]
    }


    bindEvents() {
        // Dropdown for distribution bar
        const dropdown = document.getElementById('distribution-metric-select');
        this.context.page.addEventListener(dropdown, 'change', async () => {
            await this.refreshOverview();
        });

        this.context.page.portfolioMeta$.onChanges(debounce(async ()=>{
            await this.refreshOverview();
        }, 500))

        // Jump to Pivot button
        const jumpBtn = document.querySelector('#jump-to-pivot');
        const pivotTab = document.querySelector('.toolbar-button[data-tooltip="Pivot"]');
        this.context.page.addEventListener(jumpBtn, 'click', () => pivotTab.click());

    }

    onRender() {
        this.widgetDiv.innerHTML = `
        <div class="overview-widget">
            <div class="widget-body">
                <div class="kpi-section-top">
                    <div class="kpi-main-row">
                        <div class="kpi-large"><span class="label">Notional</span><span class="value" data-agg="sumMetricWeight" data-metric="grossSize"></span></div>
                        <div class="kpi-large kpi-net"><span class="label">Net</span><span class="value" data-agg="sumMetricWeight" data-metric="netSize"></span></div>
                        <div class="kpi-large kpi-not-net"><span class="label">DV01</span><span class="value" data-agg="sumMetricWeight" data-metric="grossDv01"></span></div>
                        <div class="kpi-large kpi-net"><span class="label">Net DV01</span><span class="value" data-agg="sumMetricWeight" data-metric="netDv01"></span></div>
                        <div class="kpi-large"><span class="label">Count</span><span class="value" data-agg="count" data-metric="tnum"></span></div>
                        <div class="kpi-large"><span class="label">Current Skew</span>
                            <number-flow-group class="value" id="kpi-current-skew"></number-flow-group>
                        </div>
                    </div>
                    <div id="kpi-metrics-grid">
                        <div class="kpi-secondary-row kpi-row">
                            <span>Dur: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="duration" data-weight="grossSize" data-sigfigs="2"></strong></span>
                            <span>Liq: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="liqScoreCombined" data-weight="grossSize" data-sigfigs="1"></strong></span>
                            <span>Signal: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="signalLiveStats" data-weight="grossSize" data-sigfigs="2"></strong></span>
                        </div>
                        <div class="kpi-tertiary-row kpi-row">
                            <span>Axed: <strong class="kpi-metric" data-metric="isAxed" data-filter="MKT,AXED" data-agg="wavg" data-weight="grossSize" id="kpi-axed-pct">--</strong></span>
                            <span>Anti: <strong class="kpi-metric" data-metric="isAxed" data-filter="ANTI" data-agg="wavg" data-weight="grossSize" id="kpi-anti-axed-pct">--</strong></span>
                            <span>BSR: <strong class="kpi-metric" data-agg="percent" data-metric="firmAggBsrSize" data-weight="grossSize" data-sigfigs="0" data-format="percent" data-fill=0></strong></span>
                        </div>
                        <div class="bs-breakdown kpi-quad-row kpi-row">
                            <span>Yld: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="bvalMidYld" data-weight="grossSize" data-sigfigs="2" data-format="percent"></strong></span>
                            <span>BSIFR: <strong class="kpi-metric" data-agg="percentOfWeight" data-metric="firmAggBsifrSize" data-weight="grossSize" data-sigfigs="0" data-format="NA" data-fill=0></strong></span>
                            <span>Algo: <strong class="kpi-metric" data-agg="percentOfWeight" data-metric="isInAlgoUniverse" data-weight="grossSize" data-sigfigs="0" data-format="percent" data-fill=0></strong></span>
                        </div>
                    </div>
                </div>
                <div class="distribution-section">
                    <div class="distribution-controls">
                        <span class="label">Distribution by (% of Total)</span>
                        <select id="distribution-metric-select">
                            <option value="deskAsset">Asset Class</option><option value="regionCountry">Country</option><option value="emProductType">Corp/Quasi/Sov</option><option value="yieldCurvePosition">Curve</option><option value="desigName">Desig</option><option value="maturityBucket">Maturity</option><option value="ratingCombined" selected>Rating</option><option value="ratingMnemonic">Rating Bucket</option><option value="quoteType">Req. QuoteType</option><option value="industrySector">Sector</option>
                        </select>
                    </div>
                    <div class="rating-distribution-bar" id="distribution-bar-container"></div>
                    <div class="dynamic-legend" id="distribution-legend-container"></div>
                </div>
                <div id="overview-pill-section">
            
                </div>
            </div>
            <div class="right-side">
                <div class="right-side-upper">
                    <div class="right-side-content" id="portfolio-breakdown-container"></div>
                </div>
                <div class="right-side-middle">
                    <div id="overview-string-section" class="overview-string-wrapper">
                        <div id="overview-pill-content"></div>
                        <div id="overview-flag-section"></div>
                    </div>
                </div>
            
                <div class="right-side-lower">

                    <span> </span>

                
                    <button class="btn btn-ghost btn-sm" id="jump-to-pivot">Pivot<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"><path fill="currentColor" d="M2 12a10 10 0 0 0 10 10a10 10 0 0 0 10-10A10 10 0 0 0 12 2A10 10 0 0 0 2 12m2 0a8 8 0 0 1 8-8a8 8 0 0 1 8 8a8 8 0 0 1-8 8a8 8 0 0 1-8-8m6 5l5-5l-5-5z"/></svg></button>
                </div>
            
            </div>
        </div>
        `;

        const s = this.context.page._metaStore.get('rfqSide');
        if (s === 'BWIC' || s === 'OWIC') {
            document.querySelectorAll('.kpi-net').forEach(e=>e.style.display = 'none');
        } else {
            document.querySelectorAll('.kpi-not-net').forEach(e=>e.style.display = 'none');
        }
    }

    async updateWidget(portfolioData) {
        if (!this.isActive) return;

        const portfolio = Object.values(portfolioData);
        if (portfolio.length === 0) {
            this.clearWidget();
            return;
        }

        // Single pass to compute totals
        let totalGrossSize = 0, totalDv01 = 0;
        for (let i = 0; i < portfolio.length; i++) {
            totalGrossSize += Math.abs(portfolio[i].grossSize || 0);
            totalDv01 += Math.abs(portfolio[i].grossDv01 || 0);
        }
        const totalCount = portfolio.length;

        const weightKey = this.getActiveWeightKey();
        const totalWeight = weightKey === 'grossSize'
            ? totalGrossSize
            : (weightKey === 'grossDv01' ? totalDv01 : totalCount);

        const totals = { totalGrossSize, totalDv01, totalCount, totalWeight, weightKey };

        await this.updateAllKpis(portfolio, totals);
        await this.renderDistributionBar(portfolio, totals);
        await this.renderRightPanel(portfolio, totals);
    }

    clearWidget() {
        this.widgetDiv.querySelectorAll('[data-agg], .value, strong').forEach(el => el.textContent = '--');
        document.getElementById('distribution-bar-container').innerHTML = '';
        document.getElementById('distribution-legend-container').innerHTML = '';
        document.getElementById('portfolio-breakdown-container').innerHTML = '<div class="empty-state">No data in portfolio</div>';
    }

    async updateAllKpis(portfolio, totals) {
        const { totalGrossSize } = totals;
        const kpiElements = Array.from(this.widgetDiv.querySelectorAll('[data-agg]'));

        // 1) Collect all columns needed across every KPI in a single set
        const colSet = new Set();
        const kpiSpecs = [];
        for (const el of kpiElements) {
            const { metric, agg, sigfigs = 1, format = 'number', fill, weight } = el.dataset;
            if (format === 'NA' || !metric) {
                kpiSpecs.push({ el, value: null, format, sigfigs, agg });
                continue;
            }
            colSet.add(metric);
            const passWeight = weight || null;
            if (passWeight && passWeight !== '__count__') colSet.add(passWeight);
            if (passWeight === 'grossDv01' && metric === 'firmAggBsrSize') colSet.add('unitDv01');
            kpiSpecs.push({ el, metric, agg, passWeight, format, sigfigs, fill });
        }
        // axed metrics need these
        colSet.add('axeDirection'); colSet.add('direction'); colSet.add('grossSize');

        // 2) Single engine fetch for all KPI columns
        const allRows = await this.engine.getAllRows({ columns: [...colSet] });

        // 3) Compute each KPI from the shared row set
        for (const spec of kpiSpecs) {
            if (spec.value === null && !spec.metric) {
                spec.el.textContent = '--';
                continue;
            }
            const missing = {};
            if (spec.fill) missing.value = spec.fill;
            const value = this._computeFromRows(allRows, spec.metric, spec.passWeight, spec.agg, missing);
            const isPct = (spec.format === 'percent') || (spec.agg === 'percent');
            spec.el.textContent = (value !== null && !isNaN(value))
                ? this.formatNumber(value, !isPct, isPct, parseInt(spec.sigfigs))
                : '--';
        }

        // Axed/anti-axed from the same row set
        this._updateAxedFromRows(allRows, totalGrossSize);
    }

    _updateAxedFromRows(rows, totalGrossSize) {
        let axedSize = 0;
        let antiAxedSize = 0;

        for (const bond of rows) {
            if (bond.axeDirection && bond.direction) {
                const size = Math.abs(bond.grossSize || 0);
                if (bond.direction.toLowerCase() === bond.axeDirection.toLowerCase()) {
                    axedSize += size;
                } else {
                    antiAxedSize += size;
                }
            }
        }

        const axedPct = totalGrossSize > 0 ? (axedSize / totalGrossSize) * 100 : 0;
        const antiAxedPct = totalGrossSize > 0 ? (antiAxedSize / totalGrossSize) * 100 : 0;

        document.getElementById('kpi-axed-pct').textContent = this.formatNumber(axedPct, false, true, 0);
        document.getElementById('kpi-anti-axed-pct').textContent = this.formatNumber(antiAxedPct, false, true, 0);
    }

    getTopConcentrations(portfolio, key, totalSize, count) {
        if (portfolio.length === 0 || totalSize === 0) return [];

        const groups = portfolio.reduce((acc, bond) => {
            const groupKey = bond[key] || 'N/A';
            acc[groupKey] = (acc[groupKey] || 0) + Math.abs(bond.grossSize || 0);
            return acc;
        }, {});

        return Object.entries(groups)
                      .sort(([, a], [, b]) => b - a)
                      .slice(0, count)
                      .map(([name, value]) => ({
            name,
            value,
            pct: (value / totalSize) * 100
        }));
    }

    async pivotData(groups, weightKey = this.getActiveWeightKey(), aggs = ['SUM', 'PERCENT_OF_COL_SUM']) {
        const pe = this.pivotEngine;
        groups = Array.isArray(groups) ? groups : [groups];
        aggs = Array.isArray(aggs) ? aggs : [aggs];
        const cols = [...groups, ...aggs];
        // await this.engine.ensureColumns({columns:cols});

        const arrowTbl = this.engine.asTable();
        return pe.compute(arrowTbl, {
            groupBy: groups,
            aggregations: aggs.map(a => ({ [weightKey]: a }))
        });
    }


    // { refSkewPx: { func: 'wavg', weight:weight, DROP_NULLS:false, FILL_NULL:0, ZERO_AS_NULL:false} },
    async computeAllData() {

        const arrowTbl = this.engine.asTable();
        const pe = this.pivotEngine;
        const groups = []
        let aggs = [];

        const weight = this.getActiveWeightKey()
        if (weight !== '__count__') {
            aggs = [
                { duration: { func: 'wavg', weight:weight, DROP_NULLS:true, ZERO_AS_NULL: true} },
                { liqScoreCombined: { func: 'wavg', weight:weight, DROP_NULLS:true, ZERO_AS_NULL: true} },
                { signalLiveStats: { func: 'wavg', weight:weight, DROP_NULLS:true, ZERO_AS_NULL: false} },
                { isAxeOrMkt: { func: 'PERCENT_OF_COL_WEIGHT', weight:weight, DROP_NULLS:false, FILL_NULL:0, ZERO_AS_NULL:false} },
                { isAntiAxe: { func: 'PERCENT_OF_COL_WEIGHT', weight:weight, DROP_NULLS:false, FILL_NULL:0, ZERO_AS_NULL:false} },
            ]
        }
        const data = await this.engine.getColumns([...groups, ...aggs.map(x=>Object.keys(x)[0])]);
        return pe.compute(data, {
            groupBy: groups,
            aggregations: aggs
        });
    }


    /** Register a card if it doesn't exist yet, otherwise update its data in-place. */
    _upsertCard(name, title, type, data, options) {
        const existing = this._stack.cards.find(c => c.name === name);
        if (existing) {
            existing.update(data, options);
        } else {
            this._stack.registerCard(name, title, type, data, undefined, '', options);
        }
    }

    async renderRightPanel(portfolio, totals) {
        const container = document.getElementById('portfolio-breakdown-container');
        if (!this._stack) {
            container.innerHTML = '';
            this._stack = new InfoCardStack(container, { showNav: true, pauseOnHover: true, autoCycleMs: false});
        }

        // 1) Liquidity recs + skew header strip
        const row = document.createElement('div');
        row.className = 'breakdown-row-1';
        const charges = await this.calculateLiquidityCharges();
        row.appendChild(this.createLiquidityCard(charges));
        row.appendChild(this.createSkewCard(0));

        if (!this._headerRow) {
            container.parentElement?.insertBefore(row, container);
            this._headerRow = row;
        } else {
            this._headerRow.replaceWith(row);
            this._headerRow = row;
        }

        // 2) Stack cards — all weight-aware
        const weightKey = this.getActiveWeightKey();

        // Pivot-based breakdown
        const em = await this.pivotData('emProductType', weightKey, ['SUM','PERCENT_OF_COL_SUM']);
        const emCols = ['Asset Type', `${weightKey}_SUM`, `${weightKey}_PERCENT_OF_COL_SUM`];
        const emCardEl = this.createBreakdownCard('Asset Type Breakdown', em.rows || [], emCols, { limit: 8 });
        this._upsertCard('bdn_emProductType', 'Asset Type Breakdown', 'raw_html', emCardEl);

        window.pivotData = this.pivotData;

        // Single engine call for histogram + scatter (both need liqScoreCombined + weight)
        const colsNeeded = new Set(['liqScoreCombined', 'duration']);
        if (weightKey !== '__count__') colsNeeded.add(weightKey);
        const rows = await this.engine.getColumns([...colsNeeded]);

        // Histogram: Liquidity Score 1..10
        {
            const buckets = new Array(10).fill(0);
            for (const r of rows) {
                const s = Math.min(10, Math.max(1, Math.round(+r.liqScoreCombined || 0)));
                const w = weightKey === '__count__' ? 1 : Math.abs(+r[weightKey] || 0);
                if (s >= 1) buckets[s - 1] += w;
            }
            const labels = Array.from({ length: 10 }, (_, i) => `${i + 1}`);
            this._upsertCard('hist_liq', 'Liquidity Score Histogram', 'histogram', { labels, values: buckets });
        }

        // Scatter: Liq vs Duration (radius by weight)
        {
            let maxW = 0;
            const points = [];
            for (const r of rows) {
                const x = +r.duration; const y = +r.liqScoreCombined;
                if (!isFinite(x) || !isFinite(y)) continue;
                const w = weightKey === '__count__' ? 1 : Math.abs(+r[weightKey] || 0);
                maxW = Math.max(maxW, w);
                points.push({ x, y, _w: w });
            }
            const data = points.map(p => ({ x: p.x, y: p.y, r: 2 + (maxW ? (6 * Math.sqrt(p._w / maxW)) : 0) }));
            this._upsertCard('scatter_liq_dur', 'Liq vs Duration', 'scatter', { points: data });
        }

        // Bar: exposures by Curve
        {
            const res = await this.pivotData('yieldCurvePosition', weightKey, ['SUM']);
            const curveRows = res.rows || [];
            const labels = curveRows.map(r => r[0] == null ? 'N/A' : String(r[0]));
            const values = curveRows.map(r => +r[1] || 0);
            this._upsertCard('bar_curve', 'Exposures by Curve', 'vertical-bar', { labels, values });
        }
    }

    async calculateLiquidityCharges() {
        let pxChargeTotal = 0, pxDv01Total = 0;
        let bpsChargeTotal = 0, bpsDv01Total = 0;
        const portfolio = await this.engine.getColumns(['liqScoreCombined','QT', 'grossDv01']);
        for (const bond of portfolio) {
            const score = bond.liqScoreCombined;
            const quoteType = bond.QT;
            const dv01 = Math.abs(bond.grossDv01 || 0);
            let charge = 0;

            if (score <= 4) { // Low Liquidity
                charge = (quoteType === 'PX') ? 0.50 : 3.0; // 50c or 3bps
            } else if (score <= 6) { // Medium Liquidity
                charge = (quoteType === 'PX') ? 0.25 : 1.0; // 25c or 1bps
            }

            if (charge > 0) {
                if (quoteType === 'PX') {
                    pxChargeTotal += charge * dv01;
                    pxDv01Total += dv01;
                } else {
                    bpsChargeTotal += charge * dv01;
                    bpsDv01Total += dv01;
                }
            }
        }

        return {
            pxCharge: pxDv01Total > 0 ? pxChargeTotal / pxDv01Total : 0,
            bpsCharge: bpsDv01Total > 0 ? bpsChargeTotal / bpsDv01Total : 0,
            pxDv01Total,
            pxChargeTotal,
            bpsChargeTotal,
            bpsDv01Total
        };
    }

    createLiquidityCard(liqCharges) {
        const pxHas = (liqCharges?.pxDv01Total || 0) > 0;
        const bpsHas = (liqCharges?.bpsDv01Total || 0) > 0;

        const wrapper = document.createElement('div');
        wrapper.className = 'info-card';

        const title = document.createElement('div');
        title.className = 'info-card-title';
        title.textContent = 'Liquidity Cost Recs';
        wrapper.appendChild(title);

        if (pxHas) {
            const r = document.createElement('div');
            r.className = 'liq-cost-row';
            const l = document.createElement('span'); l.textContent = '$PX (c)';
            const v = document.createElement('span'); v.className = 'liq-cost-value'; v.textContent = (liqCharges.pxCharge || 0).toFixed(2);
            r.appendChild(l); r.appendChild(v);
            wrapper.appendChild(r);
        }

        if (bpsHas) {
            const r = document.createElement('div');
            r.className = 'liq-cost-row';
            const l = document.createElement('span'); l.textContent = 'SPD (bps)';
            const v = document.createElement('span'); v.className = 'liq-cost-value'; v.textContent = (liqCharges.bpsCharge || 0).toFixed(1);
            r.appendChild(l); r.appendChild(v);
            wrapper.appendChild(r);
        }

        // fall back: nothing present
        if (!pxHas && !bpsHas) {
            const r = document.createElement('div');
            r.className = 'liq-cost-row';
            const l = document.createElement('span'); l.textContent = 'No eligible quotes';
            const v = document.createElement('span'); v.className = 'liq-cost-value'; v.textContent = '--';
            r.appendChild(l); r.appendChild(v);
            wrapper.appendChild(r);
        }
        return wrapper;
    }

    createSkewCard(skew) {
        const wrapper = document.createElement('div');
        wrapper.className = 'info-card';
        wrapper.innerHTML = `
            <div class="info-card-title">Recommended Skew</div>
            <div class="skew-row">
                <span>---</span>
            </div>
        `;
        return wrapper;
    }

    createBreakdownCard(title, rows, columns, opts = {}) {
        const {
            limit = 7,
            gradient = true,
            gradientStart = '#8bc1bb',
            gradientEnd = '#3e3649',
            percentIndex = Math.max(0, columns.findIndex(c => /PERCENT/i.test(c))),
            rawIndex = Math.max(0, columns.findIndex(c => /_SUM$|_COUNT$|_MEAN$|_MEDIAN$/i.test(c))),
            labelIndex = 0
        } = opts;

        const list = document.createElement('div');
        list.className = 'breakdown-list';

        const sorted = rows
        .map(r => ({ label: r[labelIndex], pct: +r[percentIndex] || 0, raw: +r[rawIndex] || 0 }))
         .sort((a, b) => b.pct - a.pct)
         .slice(0, limit);

        const colors = gradient
            ? generateGradientChroma(gradientStart, gradientEnd, Math.max(1, sorted.length))
            : Array(sorted.length).fill('var(--teal-500)');

        const maxPct = Math.max(...sorted.map(d => d.pct), 0.000001);
        sorted.forEach((item, i) => {
            const pct100 = item.pct * 100;
            const row = document.createElement('div');
            row.className = 'breakdown-row tooltip tooltip-top';

            const text = document.createElement('div');
            text.className = 'breakdown-text';

            const label = document.createElement('div');
            label.className = 'breakdown-label';
            label.textContent = item.label == null ? 'N/A' : String(item.label);

            const value = document.createElement('div');
            value.className = 'breakdown-value';
            value.textContent = `${pct100.toFixed(1)}%`;

            const barBg = document.createElement('div');
            barBg.className = 'breakdown-bar-bg';

            const barFg = document.createElement('div');
            barFg.className = 'breakdown-bar-fg';
            const width = Math.max(2, (item.pct / maxPct) * 100);
            barFg.style.width = `${width}%`;
            barFg.style.backgroundColor = colors[i];

            const rawStr = NumberFormatter.formatNumber(item.raw, { sigFigs: { global: 2 } });
            row.setAttribute('data-tooltip', `${rawStr} • ${pct100.toFixed(1)}%`);

            barBg.appendChild(barFg);
            text.appendChild(label);
            text.appendChild(value);
            row.append(text)
            row.appendChild(barBg);
            list.appendChild(row);
        });

        return list;
    }


    async generateSummaryString() {

        const portfolio = await this.getData(OverviewWidget.REQUIRED_COLUMNS);
        if (portfolio.length === 0) return "Empty portfolio.";
        const weightKey = this.getActiveWeightKey();

        const portfolioName = this.context.page.page$.get('name') || 'Portfolio Summary';
        const fNum = (val, dec = 0, prefix = '', postfix = '') => this.formatNumber(val, true, false, dec, prefix, postfix);
        const fPct = (val, dec = 0) => this.formatNumber(val, false, true, dec);

        const grossSize = await this.computeValue(portfolio, 'grossSize', null, 'sumMetricWeight');
        const netDv01 = await this.computeValue(portfolio, 'netDv01', null, 'sumMetricWeight');
        const waDur = await this.computeValue(portfolio, 'duration', weightKey, 'weightedAvg');
        const waYld = await this.computeValue(portfolio, 'bvalMidYld', weightKey, 'weightedAvg');
        const waLiq = await this.computeValue(portfolio, 'liqScoreCombined', weightKey, 'weightedAvg');

        const topSectors = this.getTopConcentrations(portfolio, 'industrySector', grossSize, 3);
        const topCountries = this.getTopConcentrations(portfolio, 'regionCountry', grossSize, 3);
        const bsrPct = await this.computeValue(portfolio, 'firmAggBsrSize', weightKey, 'percent');
        const bsifrPct = ""; //await this.computeValue(portfolio, 'firmAggBsifrSize', 'grossSize', 'NA');
        const wwPct = await this.computeValue(portfolio, 'firmAggBsiSize', weightKey, 'percent');
        const liqCharges = await this.calculateLiquidityCharges();

        const formatTopItems = (items) => items.map(i => `${i.name} ${fPct(i.pct, 0)}`).join(' | ');

        const topline = `${fNum(grossSize)} Gross | ${portfolio.length} Lines`;
        const riskline = `DV01: ${fNum(netDv01)} | W.A. Dur: ${fNum(waDur, 2)} | W.A. Yld: ${fPct(waYld, 2)} | W.A. Liq: ${fNum(waLiq, 1)}`;
        const concentrationline = `Top Sectors: ${formatTopItems(topSectors)}\nTop Countries: ${formatTopItems(topCountries)}`;
        const factorline = `BSR: ${fPct(bsrPct, 0)} | BSIFR: ${fPct(bsifrPct, 0)} | WW: ${fPct(wwPct, 0)}`;
        const liqChargeLine = `Liq. Cost Recs: ${fNum(liqCharges.pxCharge, 2, '', 'c')} (PX) | ${fNum(liqCharges.bpsCharge, 1, '', 'bps')} (Other)`;

        return `${portfolioName}
-------------------
${topline}
${riskline}
${concentrationline}
${factorline}
${liqChargeLine}
`;
    }

    formatNumber(num, isCurrency = true, isPercent = false, decimals = 1, prefix = '', postfix = '') {
        if (num === null || typeof num === 'undefined' || isNaN(num)) return '--';
        const finalPostfix = isPercent ? '%' : postfix;
        return NumberFormatter.formatNumber(num, {
            prefix: prefix,
            postfix: finalPostfix,
            sigFigs: {global: decimals},
        });
    }

    /**
     * Compute a KPI from pre-fetched rows. All needed columns must already be
     * present in each row object.
     */
    _computeFromRows(rows, metric, weight = null, output = 'weightedAvg', missing = {}) {
        let totalWeight = 0, totalMetricWeight = 0, count = 0, totalMetric = 0, totalItems = 0;
        const hasWeight = (weight != null) && (weight !== '__count__');

        for (const row of rows) {
            try {
                let m = coerceToNumeric(row?.[metric], {onNaN: missing?.value});
                if ((m == null) || isNaN(m)) continue;

                if ((weight === 'grossDv01') && (metric === 'firmAggBsrSize')) {
                    m = row['unitDv01'] / 10_000 * m;
                }

                const w = coerceToNumeric(hasWeight ? row[weight] : 1, {onNaN: null, emptyStringIsZero: false});
                if ((w == null) || isNaN(w)) continue;

                totalItems++;
                totalWeight += w;
                totalMetricWeight += m * w;
                totalMetric += m;
                count++;
            } catch (e) {
                console.error(e);
            }
        }

        switch (output) {
            case 'percentOfWeight':
                return totalWeight > 0 ? (totalMetricWeight / totalWeight) * 100 : 0;
            case 'percent':
                return totalWeight > 0 ? (totalMetric / totalWeight) * 100 : 0;
            case 'sumWeight':
                return totalWeight;
            case 'count':
                return totalItems;
            case 'countNoNan':
                return count;
            case 'weightedAvg':
                return totalWeight ? (totalMetricWeight / totalWeight) : null;
            case 'sumMetricWeight':
                return totalMetric;
            default:
                return null;
        }
    }

    /** Public API kept for callers outside updateAllKpis (e.g. generateSummaryString). */
    async computeValue(portfolio, metric, weight = null, output = 'weightedAvg', missing = {}) {
        const cols = [metric];
        const hasWeight = (weight != null) && (weight !== '__count__');
        if (hasWeight) cols.push(weight);
        if (weight === 'grossDv01') cols.push('unitDv01');
        const rows = await this.engine.getAllRows({ columns: cols });
        return this._computeFromRows(rows, metric, weight, output, missing);
    }


    renderDistributionBar(portfolioData, totals) {
        const metricKey = document.getElementById('distribution-metric-select')?.value;
        if (!metricKey) return;

        const { totalWeight, weightKey } = totals;
        const barContainer = document.getElementById('distribution-bar-container');
        const legendContainer = document.getElementById('distribution-legend-container');
        barContainer.innerHTML = '';
        legendContainer.innerHTML = '';
        if (totalWeight === 0) return;

        const distribution = portfolioData.reduce((acc, bond) => {
            const key = bond[metricKey] || 'N/A';
            let w = 0;
            if (weightKey === '__count__') w = 1;
            else if (weightKey === 'grossSize') w = Math.abs(bond.grossSize || 0);
            else if (weightKey === 'grossDv01') w = Math.abs(bond.grossDv01 || 0);
            acc[key] = (acc[key] || 0) + w;
            return acc;
        }, {});

        const sortOrders = {
            ratingCombined: ['AAA','AA+','AA','AA-','A+','A','A-','BBB+','BBB','BBB-','BB+','BB','BB-','B+','B','B-','CCC+','CCC','CCC-','CC','C','D','NR','N/A'],
            maturityBucket: ['<2Y','2-5Y','5-10Y','10-20Y','20Y+'],
            yieldCurvePosition: ['Front-end','Belly','Intermediate','Long-end'],
            ratingMnemonic: ['PRIME','IG_HIGH_GRADE','IG_MEDIUM_GRADE','IG_LOW_GRADE','HY_UPPER_GRADE','HY_LOWER_GRADE','JUNK_UPPER_GRADE','JUNK_MEDIUM_GRADE','JUNK_LOW_GRADE','IN_DEFAULT'],
        };

        let sorted = Object.entries(distribution);
        const custom = sortOrders[metricKey];
        if (custom) sorted.sort(([a], [b]) => custom.indexOf(a) - custom.indexOf(b));
        else sorted.sort(([,a],[,b]) => b - a);

        const colors = generateGradientChroma("#8bc1bb", "#3e3649", sorted.length);
        const colorMap = new Map();

        sorted.forEach(([category, value], i) => {
            const pct = (value / totalWeight) * 100;
            if (pct < 0.1) return;
            const color = colors[i % colors.length];
            colorMap.set(category, color);

            const segment = document.createElement('div');
            segment.className = 'rating-segment tooltip tooltip-top';
            segment.style.width = `${pct}%`;
            segment.style.backgroundColor = color;
            segment.setAttribute('data-tooltip', `${category}: ${pct.toFixed(1)}%`);
            barContainer.appendChild(segment);
        });

        const legendItems = sorted.slice(0, 5);
        legendItems.forEach(([category, value]) => {
            const pct = (value / totalWeight) * 100;
            if (pct < 0.1) return;
            const color = colorMap.get(category) || 'gray';
            this.addLegendItem(legendContainer, category, pct, color);
        });
    }

    addLegendItem(container, name, percentage, color) {
        const legendItem = document.createElement('div');
        legendItem.className = 'legend-item';
        legendItem.innerHTML = `
            <div class="legend-color-box" style="background-color: ${color};"></div>
            <span>${name}: ${percentage.toFixed(1)}%</span>
        `;
        container.appendChild(legendItem);
    }

    onResumeSubscriptions() {
    }


    updateSkew(s) {
        if (this?.askSkew) {
            const unit = s?.ask?.unit;
            if (this.askSkew.config.suffix !== unit) {
                this.askSkew.setConfig({'suffix': unit});
            }
            const v = roundToNumeric(s?.ask?.value, 2);
            if (this.askSkew.currentValue !== v) {
                this.askSkew.update(v);
            }
        } else {
            const unit = s?.bid?.unit;
            if (this.bidSkew && this.bidSkew.config.suffix !== unit) {
                this.bidSkew.setConfig({'suffix': unit});
            }
        }
        if (this?.bidSkew) {
            const v = roundToNumeric(s?.bid?.value, 2);
            if (this.bidSkew.currentValue !== v) {
                this.bidSkew.update(v);
            }
        }
    }


    async onActivate() {
        await this.refreshOverview()
    }

    setupReactions() {

        const side = this.context.page._metaStore.get('rfqSide')

        if (side === 'BOWIC' || side === 'BWIC') {
            this.bidSkew = new ENumberFlow(this.currentSkew, {
                displayMode:'number',
                showSign: 'always',
                duration: 200,
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            });
        }

        const s = document.createElement('span')
        s.innerText = '/'
        this.currentSkew.appendChild(s);

        if (side === 'BOWIC' || side === 'OWIC') {
            this.askSkew = new ENumberFlow(this.currentSkew, {
                displayMode: 'number',
                showSign: 'always',
                duration: 200,
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            });
        }

        const ov = this;
        this.context.page.overallSkew$.onChanges(()=> {
            ov.updateSkew(ov.context.page.overallSkew$.asObject());
        })

    }

    async onCleanup() {
        try { this.dynamicController.abort(); } catch(_){}
        try { if (this.bidSkew) this.bidSkew?.destroy(); } catch(_){}
        try { if (this.askSkew) this.askSkew?.destroy(); } catch(_){}
        try { this._stack?.destroy(); } catch(_){}
        try { this._headerRow?.remove(); } catch(_){}
        try { this.resizer.unset(); } catch(_){}
    }
}

function formatPortfolioSummary(portfolioMeta$) {
    const meta = normalizeMeta(portfolioMeta$);

    const side = (meta.rfqSide || '').toUpperCase();
    const nowMs = Date.now();
    const status = pickStatus(meta);
    const dueIso = meta.dueTimeEt || meta.dueTime || null;

    const clientName = nonEmpty(meta.client) || '-';
    const traderName = nonEmpty(meta.clientTraderName) || (nonEmpty(meta.clientTraderUsername) || '-');
    const salesName = nonEmpty(meta.barcSalesName) || (nonEmpty(meta.barcSalesUsername) || '-');
    const dirFlags = buildDirFlags(meta);
    const liveStates = new Set(['LIVE', 'PENDING', 'NEW']);
    const statusOrDue = liveStates.has(status) && dueIso
        ? `[Due ${fmtDue(nowMs, dueIso)}]`
        : `[${status || '-'}]`;

    const overall = {
        gs: num(meta.grossSize),
        ns: num(meta.netSize),
        gdv: num(meta.grossDv01),
        ndv: num(meta.netDv01),
        ct: int(meta.count),
        liq: preferNumber(meta.liqScoreCombined, meta.liqScoreBase),
        rating: nonEmpty(meta.creditRating) || '-',
        dur: preferNumber(meta.duration, 0),
        signal: num(meta.signalLiveStats),
    };

    const bid = {
        gs: num(coalesce(meta.bwicGrossSize, meta.grossSize)),
        gdv: num(coalesce(meta.bwicGrossDv01, meta.grossDv01)),
        ct: int(coalesce(meta.bwicCount, meta.count)),
        liq: preferNumber(meta.bwicLiqScoreCombined, meta.bwicLiqScoreBase),
        rating: nonEmpty(meta.bwicCreditRating) || nonEmpty(meta.creditRating) || '-',
        dur: preferNumber(meta.bwicDuration, meta.duration),
        signal: num(meta.bwicSignalLiveStats),
    };

    const ask = {
        gs: num(coalesce(meta.owicGrossSize, meta.grossSize)),
        gdv: num(coalesce(meta.owicGrossDv01, meta.grossDv01)),
        ct: int(coalesce(meta.owicCount, meta.count)),
        liq: preferNumber(meta.owicLiqScoreCombined, meta.owicLiqScoreBase),
        rating: nonEmpty(meta.owicCreditRating) || nonEmpty(meta.creditRating) || '-',
        dur: preferNumber(meta.owicDuration, meta.duration),
        signal: num(meta.owicSignalLiveStats),
    };

    const headerParts = [
        clientName,
        `${truncateName(traderName)}`,
        side || '-',
        // dirFlags,
        statusOrDue
    ].filter(Boolean);

    // BOWIC => 3 lines (overall + bid + ask). BWIC/OWIC => 2 lines (header + that side).
    if (side === 'BOWIC') {
        const line1Metrics = [
            kv('Gross', fmtSize(overall.gs)),
            kv('Net', fmtSize(overall.ns)),
            kv('DV01', fmtK(overall.gdv)),
            // kv('Ndv01', fmtK(overall.ndv)),
            kv(fmtInt(overall.ct), 'bonds'),
            kv('Liq', fmtLiq(overall.liq)),
            overall.rating || '',
            kv('Dur', fmtDur(overall.dur)),
            kv('Sig', fmtDur(overall.signal)),
        ].join(', ');

        const line1 = `<div class="overview-string-line" id=overview-string-line-1-a>${headerParts.join(' | ')}</div>
                        <div class="overview-string-line" id=overview-string-line-1-b>${line1Metrics}</div>`;
        const line2 = sideLine('Bid Side', bid);
        const line3 = sideLine('Offer Side', ask);
        return `<div class="overview-string-line" id='overview-string-line-1'>${line1.trim()}</div>
                <div class="overview-string-line" id='overview-string-line-2'>${line2.trim()}</div>
                <div class="overview-string-line" id='overview-string-line-3'>${line3.trim()}</div>`;
    }

    if (side === 'BWIC') {
        const line1 = `<div class="overview-string-line" id=overview-string-line-1-a>${headerParts.join(' | ')}</div>`
        const line2 = sideLine('Bid Side', bid);
        return `<div class="overview-string-line" id='overview-string-line-1'>${line1.trim()}</div>
                <div class="overview-string-line" id='overview-string-line-2'>${line2.trim()}</div>`;
    }

    if (side === 'OWIC') {
        const line1 = `<div class="overview-string-line" id=overview-string-line-1-a>${headerParts.join(' | ')}</div>`
        const line2 = sideLine('Offer Side', ask);
        return `<div class="overview-string-line" id='overview-string-line-1'>${line1.trim()}</div>
                <div class="overview-string-line" id='overview-string-line-2'>${line2.trim()}</div>`;
    }

    // Fallback if rfqSide missing: decide by counts; else minimal 2-line.
    const hasBid = int(meta.bwicCount) > 0 || int(meta.count) > 0;
    const hasAsk = int(meta.owicCount) > 0;
    if (hasBid && hasAsk) {
        const line1 = `${headerParts.join(' | ')} | ${kv('Gsz', fmtSize(overall.gs))} | ${kv('Nsz', fmtSize(overall.ns))}`;
        return `${line1}\n${sideLine('BID Side:', bid)}\n${sideLine('OFFER Side:', ask)}`;
    }
    if (hasBid) {
        const line1 = headerParts.join(' | ');
        return `${line1}\n${sideLine('BID Side:', bid)}`;
    }
    if (hasAsk) {
        const line1 = headerParts.join(' | ');
        return `${line1}\n${sideLine('OFFER Side:', ask)}`;
    }
    return headerParts.join(' | ');
}

// ---------- helpers ----------
function normalizeMeta(src) {
    if (!src) return {};
    if (Array.isArray(src)) {
        try { return Object.fromEntries(src); } catch { return arrayPairsToObject(src); }
    }
    if (src instanceof Map) return Object.fromEntries(src);
    if (typeof src === 'object') return src;
    return {};
}

function arrayPairsToObject(pairs) {
    const o = {};
    for (let i = 0; i < pairs.length; i++) {
        const p = pairs[i];
        if (p && typeof p[0] === 'string') o[p[0]] = p[1];
    }
    return o;
}

function nonEmpty(v) {
    if (v === null || v === undefined) return '';
    const s = String(v).trim();
    return s.length ? s : '';
}

function num(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

function int(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return 0;
    return n < 0 ? Math.ceil(n) : Math.floor(n);
}

function coalesce(...vals) {
    for (let i = 0; i < vals.length; i++) {
        const v = vals[i];
        if (v !== undefined && v !== null) return v;
    }
    return undefined;
}

function preferNumber(...vals) {
    for (let i = 0; i < vals.length; i++) {
        const n = Number(vals[i]);
        if (Number.isFinite(n)) return n;
    }
    return undefined;
}

function kv(k, v) {
    if (v === undefined || v === '' || v === null) return `${k} -`;
    return `${k} ${v}`;
}

function fmtInt(n) {
    const v = int(n);
    return v.toString();
}

function fmtDur(n) {
    if (!Number.isFinite(n)) return '-';
    const r = round1(n);
    return stripTrailingZero(r);
}

function fmtLiq(n) {
    if (!Number.isFinite(n)) return '-';
    const r = round1(n);
    return stripTrailingZero(r);
}

function fmtSize(n) {
    const a = Math.abs(n);
    if (a >= 1e9) return trimDecimals(n / 1e9, 0) + 'b';
    if (a >= 1e6) return trimDecimals(n / 1e6, 0) + 'm';
    if (a >= 1e3) return trimDecimals(n / 1e3, 0) + 'k';
    return String(int(n));
}

function fmtK(n) {
    const a = Math.abs(n);
    if (!Number.isFinite(a)) return '-';
    if (a >= 1e3) return trimDecimals(n / 1e3, 1) + 'k';
    return trimDecimals(n, 0);
}

function trimDecimals(v, dp) {
    const s = v.toFixed(dp);
    return stripTrailingZero(s);
}

function round1(v) { return Number.isFinite(v) ? Math.round(v * 10) / 10 : v; }
function round2(v) { return Number.isFinite(v) ? Math.round(v * 100) / 100 : v; }

function stripTrailingZero(s) {
    if (typeof s !== 'string') s = String(s);
    if (s.indexOf('.') === -1) return s;
    s = s.replace(/\.0+$/, '');
    s = s.replace(/(\.\d*?[1-9])0+$/, '$1');
    return s;
}

function truncateName(name) {
    const s = nonEmpty(name);
    if (!s) return '-';
    if (s.length <= 18) return s;
    return s.slice(0, 17) + '…';
}

function buildDirFlags(meta) {
    const flags = [];
    if (int(meta.isAon) === 1) flags.push('AON');
    if (int(meta.isCrossed) === 1) flags.push('Xed');
    if (nonEmpty(meta.inquiryType)) flags.push(meta.inquiryType);
    if (nonEmpty(meta.venue)) flags.push(meta.venue);
    if (nonEmpty(meta.assetClass)) flags.push(meta.assetClass);
    if (nonEmpty(meta.region)) flags.push(meta.region);
    return flags.join(' ');
}

function sideLine(label, s) {
    const parts = [
        kv('Ntl', fmtSize(s.gs)),
        kv('DV01', fmtK(s.gdv)),
        kv(fmtInt(s.ct), 'bonds'),
        kv('Liq', fmtLiq(s.liq)),
        s.rating || '',
        kv('Dur', fmtDur(s.dur)),
        kv('Sig', fmtDur(s.signal)),
    ];
    return `${label}: ${parts.join(', ')}`;
}

function pickStatus(meta) {
    const a = (meta.state || '').toString().trim().toUpperCase();
    const b = (meta.rfqState || '').toString().trim().toUpperCase();
    return a || b || '';
}

function fmtDue(nowMs, dueIso) {
    const dueMs = Date.parse(dueIso);
    if (!Number.isFinite(dueMs)) return '-';
    let delta = Math.round((dueMs - nowMs) / 1000);
    const sign = delta >= 0 ? 1 : -1;
    delta = Math.abs(delta);
    const h = Math.floor(delta / 3600); delta -= h * 3600;
    const m = Math.floor(delta / 60);
    const s = delta - m * 60;
    if (h > 0 && m > 0) return sign > 0 ? `${h}h ${m}m` : `-${h}h ${m}m`;
    if (h > 0) return sign > 0 ? `${h}h` : `-${h}h`;
    if (m > 0) return sign > 0 ? `${m}m` : `-${m}m`;
    return sign > 0 ? `${s}s` : `-${s}s`;
}
