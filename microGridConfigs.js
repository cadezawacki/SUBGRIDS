
/**
 * Micro-Grid configuration declarations.
 *
 * Each config defines:
 *   - name:         unique identifier matching the backend MicroGridConfig.name
 *   - displayName:  human-readable title for the tab header
 *   - primaryKeys:  array of PK column names
 *   - columns:      AG Grid column definitions
 *   - addRowDefaults: function that fills default values for new rows
 *
 * Groups bundle multiple micro-grids into a single tabbed modal.
 */

export const SEVERITY_COLOR_MAP = {
    low:   '#FFFF00',   // yellow
    med:   '#FFBF00',   // amber
    high:  '#FF0000',   // red
    other: '#800080',   // purple
};

export const MICRO_GRID_CONFIGS = {

    hot_tickers: {
        name: 'hot_tickers',
        displayName: 'Hot Tickers',
        primaryKeys: ['id'],
        columns: [
            {
                field: 'id',
                hide: true,
                suppressColumnsToolPanel: true,
            },
            {
                field: 'column',
                headerName: 'Column',
                editable: true,
                width: 120,
                cellEditor: 'agSelectCellEditor',
                cellEditorParams: {
                    values: ['ticker', 'isin', 'cusip', 'sedol', 'name'],
                },
            },
            {
                field: 'pattern',
                headerName: 'Pattern',
                editable: true,
                flex: 1,
                minWidth: 140,
                cellEditor: 'agTextCellEditor',
            },
            {
                field: 'severity',
                headerName: 'Severity',
                editable: true,
                width: 110,
                cellEditor: 'agSelectCellEditor',
                cellEditorParams: {
                    values: ['low', 'med', 'high', 'other'],
                },
                cellStyle: (params) => {
                    const color = SEVERITY_COLOR_MAP[params.value];
                    return color
                        ? { backgroundColor: color + '30', borderLeft: `3px solid ${color}` }
                        : null;
                },
            },
            {
                field: 'color',
                headerName: 'Color',
                editable: true,
                width: 90,
                cellRenderer: (params) => {
                    if (!params.value) return '';
                    return `<span style="display:inline-block;width:16px;height:16px;border-radius:3px;background:${params.value};vertical-align:middle;margin-right:6px;border:1px solid rgba(255,255,255,0.2)"></span>${params.value}`;
                },
                cellEditor: 'agTextCellEditor',
            },
        ],
        addRowDefaults: (row = {}) => ({
            id: crypto.randomUUID(),
            column: 'ticker',
            pattern: '',
            severity: 'low',
            color: SEVERITY_COLOR_MAP['low'],
            ...row,
        }),
    },
};

export const MICRO_GRID_GROUPS = {

    pt_tools: {
        name: 'pt_tools',
        displayName: 'Portfolio Tools',
        grids: ['hot_tickers'],
    },
};
