
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

// ============================================================================
// Custom AG Grid cell editor: native color picker
// ============================================================================

export class ColorPickerEditor {
    init(params) {
        this._value = params.value || '#FFFFFF';
        this._el = document.createElement('div');
        this._el.style.cssText = 'display:flex;align-items:center;gap:6px;padding:2px 4px;height:100%;';

        this._input = document.createElement('input');
        this._input.type = 'color';
        this._input.value = this._value;
        this._input.style.cssText = 'width:32px;height:26px;border:none;padding:0;cursor:pointer;background:transparent;';

        this._label = document.createElement('span');
        this._label.style.cssText = 'font-size:12px;color:#ccc;';
        this._label.textContent = this._value;

        this._input.addEventListener('input', () => {
            this._value = this._input.value;
            this._label.textContent = this._value;
        });

        this._el.appendChild(this._input);
        this._el.appendChild(this._label);
    }
    getGui() { return this._el; }
    afterGuiAttached() { this._input.focus(); this._input.click(); }
    getValue() { return this._value; }
    isPopup() { return false; }
    destroy() {}
}

// ============================================================================
// Custom AG Grid cell editor: tags pill editor
// ============================================================================

const ALL_KNOWN_TAGS = new Set();

export class TagsCellEditor {
    init(params) {
        this._params = params;
        this._tags = _parseTags(params.value);
        this._el = document.createElement('div');
        this._el.className = 'micro-tags-editor';
        this._el.style.cssText = 'display:flex;flex-wrap:wrap;gap:4px;align-items:center;padding:4px 6px;min-height:28px;background:#1e1e2e;';
        this._render();
    }

    _render() {
        this._el.innerHTML = '';
        this._tags.forEach((tag, i) => {
            const pill = _createPillEl(tag, true, () => {
                this._tags.splice(i, 1);
                this._render();
            });
            this._el.appendChild(pill);
        });

        // Add-tag input
        const input = document.createElement('input');
        input.type = 'text';
        input.placeholder = '+ tag';
        input.style.cssText = 'border:none;outline:none;background:transparent;color:#ccc;font-size:11px;width:60px;';
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ',') {
                e.preventDefault();
                e.stopPropagation();
                const val = input.value.trim();
                if (val && !this._tags.includes(val)) {
                    this._tags.push(val);
                    ALL_KNOWN_TAGS.add(val);
                    this._render();
                }
            }
        });
        this._el.appendChild(input);

        // Suggestions dropdown from known tags
        if (ALL_KNOWN_TAGS.size > 0) {
            const missing = [...ALL_KNOWN_TAGS].filter(t => !this._tags.includes(t));
            if (missing.length > 0) {
                const wrap = document.createElement('div');
                wrap.style.cssText = 'width:100%;display:flex;flex-wrap:wrap;gap:3px;margin-top:2px;opacity:0.5;';
                missing.slice(0, 10).forEach(tag => {
                    const sug = _createPillEl(tag, false);
                    sug.style.cursor = 'pointer';
                    sug.style.opacity = '0.6';
                    sug.addEventListener('click', () => {
                        if (!this._tags.includes(tag)) {
                            this._tags.push(tag);
                            this._render();
                        }
                    });
                    wrap.appendChild(sug);
                });
                this._el.appendChild(wrap);
            }
        }

        // Refocus the input after re-render
        requestAnimationFrame(() => {
            const inp = this._el.querySelector('input');
            if (inp) inp.focus();
        });
    }

    getGui() { return this._el; }
    afterGuiAttached() {
        const inp = this._el.querySelector('input');
        if (inp) inp.focus();
    }
    getValue() { return this._tags.join(','); }
    isPopup() { return true; }
    destroy() {}
}

// ============================================================================
// Tags helper functions (exported for use by microGridManager)
// ============================================================================

export function _parseTags(val) {
    if (!val || typeof val !== 'string') return [];
    return val.split(',').map(s => s.trim()).filter(Boolean);
}

export function _createPillEl(tag, showRemove = false, onRemove = null) {
    const pill = document.createElement('span');
    pill.className = 'micro-tag-pill';
    const hue = _tagHue(tag);
    pill.style.cssText = `display:inline-flex;align-items:center;gap:3px;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:500;background:hsl(${hue},55%,25%);color:hsl(${hue},80%,80%);border:1px solid hsl(${hue},50%,35%);white-space:nowrap;user-select:none;`;
    pill.textContent = tag;
    if (showRemove && onRemove) {
        const x = document.createElement('span');
        x.textContent = '×';
        x.style.cssText = 'cursor:pointer;margin-left:2px;font-size:13px;line-height:1;';
        x.addEventListener('click', (e) => { e.stopPropagation(); onRemove(); });
        pill.appendChild(x);
    }
    return pill;
}

function _tagHue(tag) {
    let h = 0;
    for (let i = 0; i < tag.length; i++) h = (h * 31 + tag.charCodeAt(i)) & 0xFFFF;
    return h % 360;
}

export function getAllKnownTags() { return ALL_KNOWN_TAGS; }

export function seedKnownTags(rows) {
    for (const row of rows) {
        if (row.tags) {
            for (const t of _parseTags(row.tags)) {
                ALL_KNOWN_TAGS.add(t);
            }
        }
    }
}

// ============================================================================
// Tags cell renderer (pills display)
// ============================================================================

export function tagsCellRenderer(params) {
    if (!params.value) return '';
    const tags = _parseTags(params.value);
    return tags.map(tag => {
        const hue = _tagHue(tag);
        return `<span style="display:inline-flex;align-items:center;padding:1px 7px;border-radius:10px;font-size:10px;font-weight:500;background:hsl(${hue},55%,25%);color:hsl(${hue},80%,80%);border:1px solid hsl(${hue},50%,35%);margin-right:3px;">${_escHtml(tag)}</span>`;
    }).join('');
}

function _escHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ============================================================================
// Tags filter (custom AG Grid filter)
// ============================================================================

export class TagsFilter {
    init(params) {
        this._params = params;
        this._filterTags = new Set();
        this._el = document.createElement('div');
        this._el.style.cssText = 'padding:8px;min-width:180px;max-height:250px;overflow-y:auto;';
        this._render();
    }

    _render() {
        this._el.innerHTML = '';
        const tags = [...ALL_KNOWN_TAGS].sort();

        if (tags.length === 0) {
            this._el.textContent = 'No tags yet';
            return;
        }

        // Clear button
        const clearBtn = document.createElement('button');
        clearBtn.textContent = 'Clear';
        clearBtn.style.cssText = 'font-size:11px;margin-bottom:6px;cursor:pointer;background:#333;color:#ccc;border:1px solid #555;border-radius:3px;padding:2px 8px;';
        clearBtn.addEventListener('click', () => {
            this._filterTags.clear();
            this._render();
            this._params.filterChangedCallback();
        });
        this._el.appendChild(clearBtn);

        tags.forEach(tag => {
            const row = document.createElement('label');
            row.style.cssText = 'display:flex;align-items:center;gap:6px;padding:2px 0;cursor:pointer;font-size:12px;color:#ccc;';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = this._filterTags.has(tag);
            cb.addEventListener('change', () => {
                if (cb.checked) this._filterTags.add(tag);
                else this._filterTags.delete(tag);
                this._params.filterChangedCallback();
            });
            row.appendChild(cb);
            const pill = _createPillEl(tag, false);
            row.appendChild(pill);
            this._el.appendChild(row);
        });
    }

    getGui() { return this._el; }
    doesFilterPass(params) {
        if (this._filterTags.size === 0) return true;
        const rowTags = _parseTags(params.data?.tags);
        return rowTags.some(t => this._filterTags.has(t));
    }
    isFilterActive() { return this._filterTags.size > 0; }
    getModel() {
        if (this._filterTags.size === 0) return null;
        return { tags: [...this._filterTags] };
    }
    setModel(model) {
        this._filterTags = new Set(model?.tags || []);
        this._render();
    }
    afterGuiAttached() { this._render(); }
    destroy() {}
}

// ============================================================================
// Column definitions: hot_tickers
// ============================================================================

export const MICRO_GRID_CONFIGS = {

    hot_tickers: {
        name: 'hot_tickers',
        displayName: 'Hot Tickers',
        primaryKeys: ['id'],
        columns: [
            {
                headerCheckboxSelection: true,
                headerCheckboxSelectionFilteredOnly: true,
                checkboxSelection: true,
                width: 42,
                maxWidth: 42,
                pinned: 'left',
                suppressMenu: true,
                sortable: false,
                filter: false,
                resizable: false,
                editable: false,
                suppressColumnsToolPanel: true,
                headerName: '',
            },
            {
                field: 'id',
                hide: true,
                suppressColumnsToolPanel: true,
            },
            {
                field: 'column',
                headerName: 'Column',
                editable: true,
                width: 110,
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
                minWidth: 120,
                cellEditor: 'agTextCellEditor',
            },
            {
                field: 'severity',
                headerName: 'Severity',
                editable: true,
                width: 100,
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
                    return `<span style="display:inline-block;width:16px;height:16px;border-radius:3px;background:${params.value};vertical-align:middle;margin-right:6px;border:1px solid rgba(255,255,255,0.2)"></span>${_escHtml(params.value)}`;
                },
                cellEditor: ColorPickerEditor,
            },
            {
                field: 'tags',
                headerName: 'Tags',
                editable: true,
                width: 160,
                cellRenderer: tagsCellRenderer,
                cellEditor: TagsCellEditor,
                filter: TagsFilter,
            },
            {
                field: 'notes',
                headerName: 'Notes',
                editable: true,
                width: 150,
                cellEditor: 'agTextCellEditor',
            },
            {
                field: 'updated_at',
                headerName: 'Updated',
                editable: false,
                width: 140,
                sort: 'desc',
            },
            {
                field: 'updated_by',
                headerName: 'By',
                editable: false,
                width: 80,
            },
        ],
        addRowDefaults: (row = {}) => ({
            id: crypto.randomUUID(),
            column: 'ticker',
            pattern: '',
            severity: 'low',
            color: SEVERITY_COLOR_MAP['low'],
            tags: '',
            notes: '',
            updated_at: '',
            updated_by: '',
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
