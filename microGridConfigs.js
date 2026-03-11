

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
// Regex / glob auto-detection
// ============================================================================

const REGEX_INDICATORS = /[\\^$*+?.()|[\]{}]/;

export function detectMatchMode(pattern) {
    if (!pattern || typeof pattern !== 'string') return 'literal';
    if (REGEX_INDICATORS.test(pattern)) return 'regex';
    return 'literal';
}

export function isValidRegex(pattern) {
    try { new RegExp(pattern); return true; } catch { return false; }
}

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
// Tags system — hierarchical with custom colors
// ============================================================================

const ALL_KNOWN_TAGS = new Set();
const TAG_CUSTOM_COLORS = new Map();   // tag -> {bg, fg, border}

export function _parseTags(val) {
    if (!val || typeof val !== 'string') return [];
    return val.split(',').map(s => s.trim()).filter(Boolean);
}

export function _parseTagCategory(tag) {
    const idx = tag.indexOf(':');
    if (idx > 0) return { category: tag.slice(0, idx), label: tag.slice(idx + 1) };
    return { category: null, label: tag };
}

function _tagHue(tag) {
    const { label } = _parseTagCategory(tag);
    let h = 0;
    for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) & 0xFFFF;
    return h % 360;
}

function _tagColors(tag) {
    const custom = TAG_CUSTOM_COLORS.get(tag);
    if (custom) return custom;
    const hue = _tagHue(tag);
    return {
        bg: `hsl(${hue},55%,25%)`,
        fg: `hsl(${hue},80%,80%)`,
        border: `hsl(${hue},50%,35%)`,
    };
}

export function _createPillEl(tag, showRemove = false, onRemove = null) {
    const pill = document.createElement('span');
    pill.className = 'micro-tag-pill';
    const c = _tagColors(tag);
    pill.style.cssText = `display:inline-flex;align-items:center;gap:3px;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:500;background:${c.bg};color:${c.fg};border:1px solid ${c.border};white-space:nowrap;user-select:none;`;

    const { category, label } = _parseTagCategory(tag);
    if (category) {
        const catSpan = document.createElement('span');
        catSpan.style.cssText = 'opacity:0.6;font-size:10px;margin-right:1px;';
        catSpan.textContent = category + ':';
        pill.appendChild(catSpan);
        pill.appendChild(document.createTextNode(label));
    } else {
        pill.textContent = tag;
    }

    if (showRemove && onRemove) {
        const x = document.createElement('span');
        x.textContent = '×';
        x.style.cssText = 'cursor:pointer;margin-left:2px;font-size:13px;line-height:1;';
        x.addEventListener('click', (e) => { e.stopPropagation(); onRemove(); });
        pill.appendChild(x);
    }
    return pill;
}

export function getAllKnownTags() { return ALL_KNOWN_TAGS; }
export function getTagCustomColors() { return TAG_CUSTOM_COLORS; }

export function setTagColor(tag, bg, fg, border) {
    TAG_CUSTOM_COLORS.set(tag, { bg, fg, border });
}

export function removeTagColor(tag) {
    TAG_CUSTOM_COLORS.delete(tag);
}

export function seedKnownTags(rows) {
    for (const row of rows) {
        if (row.tags) {
            for (const t of _parseTags(row.tags)) ALL_KNOWN_TAGS.add(t);
        }
    }
}

/**
 * Group known tags by category. Returns Map<category|'', string[]>.
 */
export function getTagsByCategory() {
    const cats = new Map();
    for (const tag of ALL_KNOWN_TAGS) {
        const { category } = _parseTagCategory(tag);
        const key = category || '';
        if (!cats.has(key)) cats.set(key, []);
        cats.get(key).push(tag);
    }
    for (const arr of cats.values()) arr.sort();
    return cats;
}

// ============================================================================
// Tags cell renderer (pills display)
// ============================================================================

export function tagsCellRenderer(params) {
    if (!params.value) return '';
    const tags = _parseTags(params.value);
    return tags.map(tag => {
        const c = _tagColors(tag);
        const { category, label } = _parseTagCategory(tag);
        const catHtml = category ? `<span style="opacity:0.6;font-size:9px;">${_escHtml(category)}:</span>` : '';
        return `<span style="display:inline-flex;align-items:center;gap:1px;padding:1px 7px;border-radius:10px;font-size:10px;font-weight:500;background:${c.bg};color:${c.fg};border:1px solid ${c.border};margin-right:3px;">${catHtml}${_escHtml(label)}</span>`;
    }).join('');
}

function _escHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ============================================================================
// Custom AG Grid cell editor: tags pill editor
// ============================================================================

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

        const input = document.createElement('input');
        input.type = 'text';
        input.placeholder = '+ tag (category:name)';
        input.style.cssText = 'border:none;outline:none;background:transparent;color:#ccc;font-size:11px;width:80px;';
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

        // Suggestions grouped by category
        if (ALL_KNOWN_TAGS.size > 0) {
            const missing = [...ALL_KNOWN_TAGS].filter(t => !this._tags.includes(t));
            if (missing.length > 0) {
                const wrap = document.createElement('div');
                wrap.style.cssText = 'width:100%;display:flex;flex-wrap:wrap;gap:3px;margin-top:2px;opacity:0.5;';
                missing.slice(0, 12).forEach(tag => {
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
// Tags filter — hierarchical grouping by category
// ============================================================================

export class TagsFilter {
    init(params) {
        this._params = params;
        this._filterTags = new Set();
        this._el = document.createElement('div');
        this._el.style.cssText = 'padding:8px;min-width:200px;max-height:280px;overflow-y:auto;';
        this._render();
    }

    _render() {
        this._el.innerHTML = '';
        const cats = getTagsByCategory();

        if (cats.size === 0) {
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

        // Render each category
        const sortedCats = [...cats.keys()].sort((a, b) => {
            if (a === '') return 1;
            if (b === '') return -1;
            return a.localeCompare(b);
        });

        for (const cat of sortedCats) {
            const tags = cats.get(cat);
            if (cat) {
                const catHeader = document.createElement('div');
                catHeader.style.cssText = 'font-size:10px;font-weight:600;color:#888;text-transform:uppercase;margin:6px 0 2px;letter-spacing:0.5px;';
                catHeader.textContent = cat;
                this._el.appendChild(catHeader);
            }
            for (const tag of tags) {
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
                row.appendChild(_createPillEl(tag, false));
                this._el.appendChild(row);
            }
        }
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
// Pattern cell renderer — teal highlight for regex patterns
// ============================================================================

function patternCellRenderer(params) {
    const val = params.value;
    if (!val) return '';
    const mode = detectMatchMode(val);
    if (mode === 'regex') {
        const valid = isValidRegex(val);
        const color = valid ? '#2dd4bf' : '#f87171';  // teal or red for invalid
        const title = valid ? 'Regex pattern' : 'Invalid regex';
        return `<span style="color:${color};font-family:monospace;font-size:12px;" title="${title}">${_escHtml(val)}</span>`;
    }
    return _escHtml(val);
}

// ============================================================================
// Column definitions: hot_tickers
// ============================================================================

export const MICRO_GRID_CONFIGS = {

    hot_tickers: {
        name: 'hot_tickers',
        displayName: `<div class="hot-ticker-modal">
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"><path fill="currentColor" d="M22 14h-1c0-3.87-3.13-7-7-7h-1V5.73A2 2 0 1 0 10 4c0 .74.4 1.39 1 1.73V7h-1c-3.87 0-7 3.13-7 7H2c-.55 0-1 .45-1 1v3c0 .55.45 1 1 1h1v1a2 2 0 0 0 2 2h14c1.11 0 2-.89 2-2v-1h1c.55 0 1-.45 1-1v-3c0-.55-.45-1-1-1M9.86 16.68l-1.18 1.18l-1.18-1.18l-1.18 1.18l-1.18-1.18l1.18-1.18l-1.18-1.18l1.18-1.18l1.18 1.18l1.18-1.18l1.18 1.18l-1.18 1.18zm9 0l-1.18 1.18l-1.18-1.18l-1.18 1.18l-1.18-1.18l1.18-1.18l-1.18-1.18l1.18-1.18l1.18 1.18l1.18-1.18l1.18 1.18l-1.18 1.18z"/></svg>
            <span>AI Tickers</span>
            </div>`,
        primaryKeys: ['id'],
        columns: [
            {
                headerCheckboxSelection: true,
                headerCheckboxSelectionFilteredOnly: true,
                checkboxSelection: true,
                width: 42,
                maxWidth: 42,
                pinned: 'left',
                suppressHeaderMenuButton: true,
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
                width: 120,
                cellEditor: 'agSelectCellEditor',
                cellEditorParams: {
                    values: ['ticker', 'isin', 'cusip', 'description'],
                },
            },
            {
                field: 'pattern',
                headerName: 'Pattern',
                editable: true,
                flex: 1,
                minWidth: 140,
                cellEditor: 'agTextCellEditor',
                cellRenderer: patternCellRenderer,
            },
            {
                field: 'match_mode',
                headerName: 'Match Mode',
                editable: false,
                width: 100,
                hide: true,
                suppressColumnsToolPanel: false,
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
            match_mode: 'literal',
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
        displayName: 'Bond Flags',
        grids: ['hot_tickers'],
    },
};
