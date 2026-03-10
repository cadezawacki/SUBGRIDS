
/**
 * MicroGridManager — central frontend manager for micro-grid modals.
 *
 * Handles:
 *   - WebSocket subscribe/unsubscribe/publish for micro-grids
 *   - Tabbed modal lifecycle (open, tab switch, close)
 *   - AG Grid instance creation and data management per micro-grid
 *   - Real-time delta application from other users
 *   - Bulk add (paste tickers/ISINs), bulk tag add/remove
 *   - Cleanup of all resources on close/destroy
 */

import {
    MICRO_GRID_CONFIGS, MICRO_GRID_GROUPS, SEVERITY_COLOR_MAP,
    _parseTags, seedKnownTags, getAllKnownTags, _createPillEl,
} from './microGridConfigs.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function _upper(s) { return (s || '').toUpperCase(); }

function _topic(microName) { return `MICRO.${_upper(microName)}`; }

// Pattern matchers for bulk add
const ISIN_RE = /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/;            // 12 chars, starts with 2-letter country
const TICKER_RE = /^[A-Z]{1,5}(?:\.[A-Z]{1,2})?$/;        // 1-5 uppercase letters, optional .XX suffix

function _extractIdentifiers(text) {
    // Split on common delimiters: newlines, commas, semicolons, tabs, pipes, spaces
    const tokens = text.split(/[\n\r,;\t|]+/).map(s => s.trim()).filter(Boolean);
    const results = [];
    const seen = new Set();

    for (const raw of tokens) {
        // Further split on spaces for multi-word lines
        const words = raw.split(/\s+/);
        for (const w of words) {
            const cleaned = w.replace(/[^A-Za-z0-9.]/g, '');
            if (!cleaned) continue;
            const upper = cleaned.toUpperCase();
            if (seen.has(upper)) continue;

            // 12 chars → ISIN
            if (upper.length === 12 && ISIN_RE.test(upper)) {
                seen.add(upper);
                results.push({ column: 'isin', pattern: upper });
            }
            // All caps, 1-5 letters → ticker
            else if (TICKER_RE.test(upper) && upper.length >= 1) {
                seen.add(upper);
                results.push({ column: 'ticker', pattern: upper });
            }
            // else: ignore noise
        }
    }
    return results;
}

// ---------------------------------------------------------------------------
// MicroGridManager
// ---------------------------------------------------------------------------

export class MicroGridManager {

    constructor(page) {
        /** @type {import('./pageBaseV2.js').PageBase} */
        this._page = page;

        /** In-memory row data keyed by micro-grid name. @type {Map<string, Array<object>>} */
        this._data = new Map();

        /** AG Grid API instances keyed by micro-grid name. @type {Map<string, object>} */
        this._gridApis = new Map();

        /** Currently open modal dialogs keyed by group name. @type {Map<string, HTMLDialogElement>} */
        this._openModals = new Map();

        /** Active tab per open group. @type {Map<string, string>} */
        this._activeTabs = new Map();

        /** Set of micro-grid names currently subscribed. @type {Set<string>} */
        this._subscribed = new Set();

        /** AbortControllers for modal event listeners keyed by group name. @type {Map<string, AbortController>} */
        this._abortControllers = new Map();

        /** Bound handler references for cleanup. @type {Map<string, object>} */
        this._handlers = new Map();
    }

    // =========================================================================
    // Subscription
    // =========================================================================

    async subscribe(microName) {
        if (this._subscribed.has(microName)) return;

        const sm = this._page.subscriptionManager();
        const topic = _topic(microName);

        const handlers = {
            subscribe: this._onSnapshot.bind(this, microName),
            publish: this._onDelta.bind(this, microName),
        };
        this._handlers.set(microName, handlers);

        sm.registerMessageHandler(topic, 'micro_subscribe', handlers.subscribe);
        sm.registerMessageHandler(topic, 'micro_publish', handlers.publish);

        this._subscribed.add(microName);
        await sm.microSubscribe(microName);
    }

    async unsubscribe(microName) {
        if (!this._subscribed.has(microName)) return;

        const sm = this._page.subscriptionManager();
        const topic = _topic(microName);
        const handlers = this._handlers.get(microName);

        if (handlers) {
            sm.unregisterMessageHandler(topic, 'micro_subscribe', handlers.subscribe);
            sm.unregisterMessageHandler(topic, 'micro_publish', handlers.publish);
            this._handlers.delete(microName);
        }

        this._subscribed.delete(microName);
        await sm.microUnsubscribe(microName).catch(() => {});
    }

    // =========================================================================
    // Message handlers
    // =========================================================================

    _onSnapshot(microName, message) {
        const snapshot = message?.data?.snapshot;
        if (!Array.isArray(snapshot)) return;

        this._data.set(microName, [...snapshot]);
        seedKnownTags(snapshot);
        this._refreshGrid(microName);
    }

    _onDelta(microName, message) {
        const payloads = message?.data?.payloads;
        if (!payloads) return;

        const config = MICRO_GRID_CONFIGS[microName];
        if (!config) return;

        const pkCol = config.primaryKeys[0];
        let rows = this._data.get(microName) || [];

        const removeRows = payloads.remove || [];
        if (removeRows.length > 0) {
            const removeIds = new Set(removeRows.map(r => String(r[pkCol])));
            rows = rows.filter(r => !removeIds.has(String(r[pkCol])));
        }

        const addRows = payloads.add || [];
        if (addRows.length > 0) {
            rows = [...rows, ...addRows];
            seedKnownTags(addRows);
        }

        const updateRows = payloads.update || [];
        if (updateRows.length > 0) {
            const updateMap = new Map(updateRows.map(r => [String(r[pkCol]), r]));
            rows = rows.map(r => {
                const upd = updateMap.get(String(r[pkCol]));
                return upd ? { ...r, ...upd } : r;
            });
            seedKnownTags(updateRows);
        }

        this._data.set(microName, rows);
        this._refreshGrid(microName);
    }

    _refreshGrid(microName) {
        const api = this._gridApis.get(microName);
        if (!api) return;

        const rows = this._data.get(microName) || [];
        api.setGridOption('rowData', rows);
    }

    // =========================================================================
    // Publish edits
    // =========================================================================

    async publishAdd(microName, row) {
        const config = MICRO_GRID_CONFIGS[microName];
        if (!config) return;
        const filled = config.addRowDefaults ? config.addRowDefaults(row) : row;
        const sm = this._page.subscriptionManager();
        await sm.microPublish(microName, { add: [filled] });
    }

    async publishBulkAdd(microName, rows) {
        const config = MICRO_GRID_CONFIGS[microName];
        if (!config) return;
        const filled = rows.map(r => config.addRowDefaults ? config.addRowDefaults(r) : r);
        if (filled.length === 0) return;
        const sm = this._page.subscriptionManager();
        await sm.microPublish(microName, { add: filled });
    }

    async publishUpdate(microName, row) {
        const sm = this._page.subscriptionManager();
        await sm.microPublish(microName, { update: [row] });
    }

    async publishRemove(microName, row) {
        const config = MICRO_GRID_CONFIGS[microName];
        if (!config) return;
        const pkCol = config.primaryKeys[0];
        const sm = this._page.subscriptionManager();
        await sm.microPublish(microName, { remove: [{ [pkCol]: row[pkCol] }] });
    }

    async publishBulkRemove(microName, rowArr) {
        const config = MICRO_GRID_CONFIGS[microName];
        if (!config) return;
        const pkCol = config.primaryKeys[0];
        const sm = this._page.subscriptionManager();
        await sm.microPublish(microName, { remove: rowArr.map(r => ({ [pkCol]: r[pkCol] })) });
    }

    async publishBulkUpdate(microName, rowArr) {
        if (rowArr.length === 0) return;
        const sm = this._page.subscriptionManager();
        await sm.microPublish(microName, { update: rowArr });
    }

    // =========================================================================
    // Modal / Tab UI
    // =========================================================================

    async openGroup(groupNameOrConfig) {
        const groupConfig = typeof groupNameOrConfig === 'string'
            ? MICRO_GRID_GROUPS[groupNameOrConfig]
            : groupNameOrConfig;

        if (!groupConfig) {
            console.error('[MicroGrid] Unknown group:', groupNameOrConfig);
            return;
        }

        const groupName = groupConfig.name;

        const existing = this._openModals.get(groupName);
        if (existing && existing.open) {
            return;
        }

        for (const gridName of groupConfig.grids) {
            await this.subscribe(gridName);
        }

        const mm = this._page.modalManager();
        const mgr = this;

        return mm.showCustom({
            title: groupConfig.displayName,
            modalBoxClass: 'micro-grid-modal-box',
            preventBackdropClick: false,
            setupContent(contentArea, dialog, closeWithValue) {
                mgr._openModals.set(groupName, dialog);
                mgr._buildTabbedUI(groupConfig, contentArea, dialog, closeWithValue);
            },
        }).then(result => {
            this._cleanupGroup(groupName);
            return result;
        });
    }

    _buildTabbedUI(groupConfig, contentArea, dialog, closeWithValue) {
        const groupName = groupConfig.name;
        const grids = groupConfig.grids;
        const ac = new AbortController();
        this._abortControllers.set(groupName, ac);
        const signal = ac.signal;

        // --- Style injection (idempotent) ---
        if (!document.getElementById('micro-grid-styles')) {
            const style = document.createElement('style');
            style.id = 'micro-grid-styles';
            style.textContent = `
                .micro-grid-modal-box {
                    width: 900px;
                    max-width: 95vw;
                    max-height: 85vh;
                    padding: 0;
                    overflow: hidden;
                    display: flex;
                    flex-direction: column;
                }
                .micro-grid-modal-box .modal-title {
                    padding: 12px 16px 0;
                }
                .micro-grid-modal-box .modal-custom-content {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    overflow: hidden;
                }
                .micro-tab-bar {
                    display: flex;
                    gap: 0;
                    border-bottom: 1px solid rgba(255,255,255,0.1);
                    padding: 0 16px;
                    flex-shrink: 0;
                }
                .micro-tab {
                    padding: 8px 16px;
                    cursor: pointer;
                    border-bottom: 2px solid transparent;
                    color: rgba(255,255,255,0.6);
                    font-size: 13px;
                    transition: color 0.15s, border-color 0.15s;
                    user-select: none;
                }
                .micro-tab:hover {
                    color: rgba(255,255,255,0.85);
                }
                .micro-tab.active {
                    color: #fff;
                    border-bottom-color: #4764f5;
                }
                .micro-grid-container {
                    flex: 1;
                    min-height: 300px;
                    padding: 0;
                    overflow: hidden;
                }
                .micro-grid-container .ag-root-wrapper {
                    border: none;
                }
                .micro-toolbar {
                    display: flex;
                    gap: 6px;
                    padding: 8px 16px;
                    border-top: 1px solid rgba(255,255,255,0.1);
                    flex-shrink: 0;
                    flex-wrap: wrap;
                    align-items: center;
                }
                .micro-toolbar .btn {
                    font-size: 12px;
                }
                .micro-toolbar .toolbar-spacer {
                    flex: 1;
                }
                .micro-toolbar .toolbar-sep {
                    width: 1px;
                    height: 20px;
                    background: rgba(255,255,255,0.15);
                    margin: 0 2px;
                }
                .micro-bulk-overlay {
                    position: absolute;
                    inset: 0;
                    background: rgba(0,0,0,0.85);
                    z-index: 10;
                    display: flex;
                    flex-direction: column;
                    padding: 16px;
                    gap: 10px;
                }
                .micro-bulk-overlay textarea {
                    flex: 1;
                    background: #1a1a2e;
                    color: #e0e0e0;
                    border: 1px solid #444;
                    border-radius: 4px;
                    padding: 10px;
                    font-family: monospace;
                    font-size: 13px;
                    resize: none;
                }
                .micro-bulk-overlay .bulk-preview {
                    font-size: 12px;
                    color: #aaa;
                    max-height: 60px;
                    overflow-y: auto;
                }
                .micro-bulk-overlay .bulk-actions {
                    display: flex;
                    gap: 8px;
                    justify-content: flex-end;
                }
                .micro-tag-bulk-overlay {
                    position: absolute;
                    inset: 0;
                    background: rgba(0,0,0,0.85);
                    z-index: 10;
                    display: flex;
                    flex-direction: column;
                    padding: 16px;
                    gap: 10px;
                }
                .micro-tag-bulk-overlay .tag-input-row {
                    display: flex;
                    gap: 8px;
                    align-items: center;
                }
                .micro-tag-bulk-overlay .tag-input-row input {
                    flex: 1;
                    background: #1a1a2e;
                    color: #e0e0e0;
                    border: 1px solid #444;
                    border-radius: 4px;
                    padding: 6px 10px;
                    font-size: 13px;
                }
                .micro-tag-bulk-overlay .tag-suggestions {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 4px;
                }
                .micro-tag-bulk-overlay .bulk-actions {
                    display: flex;
                    gap: 8px;
                    justify-content: flex-end;
                }
            `;
            document.head.appendChild(style);
        }

        // --- Tab bar ---
        const tabBar = document.createElement('div');
        tabBar.className = 'micro-tab-bar';

        grids.forEach((gridName, idx) => {
            const config = MICRO_GRID_CONFIGS[gridName];
            const tab = document.createElement('div');
            tab.className = 'micro-tab' + (idx === 0 ? ' active' : '');
            tab.textContent = config?.displayName || gridName;
            tab.dataset.grid = gridName;
            tab.addEventListener('click', () => {
                tabBar.querySelectorAll('.micro-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                this._switchTab(groupName, gridName, gridContainer);
            }, { signal });
            tabBar.appendChild(tab);
        });

        contentArea.appendChild(tabBar);

        // --- Grid container (position:relative for overlays) ---
        const gridContainer = document.createElement('div');
        gridContainer.className = 'micro-grid-container';
        gridContainer.style.position = 'relative';
        contentArea.appendChild(gridContainer);

        // --- Toolbar ---
        const toolbar = document.createElement('div');
        toolbar.className = 'micro-toolbar';

        const addBtn = document.createElement('button');
        addBtn.className = 'btn btn-sm btn-primary';
        addBtn.textContent = '+ Add Row';
        addBtn.addEventListener('click', () => {
            const activeGrid = this._activeTabs.get(groupName);
            if (activeGrid) this.publishAdd(activeGrid, {});
        }, { signal });

        const bulkAddBtn = document.createElement('button');
        bulkAddBtn.className = 'btn btn-sm btn-outline';
        bulkAddBtn.textContent = 'Bulk Add';
        bulkAddBtn.addEventListener('click', () => {
            const activeGrid = this._activeTabs.get(groupName);
            if (activeGrid) this._showBulkAddOverlay(activeGrid, gridContainer);
        }, { signal });

        const sep1 = document.createElement('span');
        sep1.className = 'toolbar-sep';

        const addTagBtn = document.createElement('button');
        addTagBtn.className = 'btn btn-sm btn-outline';
        addTagBtn.textContent = '+ Tag Selected';
        addTagBtn.addEventListener('click', () => {
            const activeGrid = this._activeTabs.get(groupName);
            if (activeGrid) this._showTagBulkOverlay(activeGrid, gridContainer, 'add');
        }, { signal });

        const removeTagBtn = document.createElement('button');
        removeTagBtn.className = 'btn btn-sm btn-outline';
        removeTagBtn.textContent = '− Tag Selected';
        removeTagBtn.addEventListener('click', () => {
            const activeGrid = this._activeTabs.get(groupName);
            if (activeGrid) this._showTagBulkOverlay(activeGrid, gridContainer, 'remove');
        }, { signal });

        const sep2 = document.createElement('span');
        sep2.className = 'toolbar-sep';

        const removeBtn = document.createElement('button');
        removeBtn.className = 'btn btn-sm btn-error btn-outline';
        removeBtn.textContent = 'Remove Selected';
        removeBtn.addEventListener('click', () => {
            const activeGrid = this._activeTabs.get(groupName);
            if (!activeGrid) return;
            const api = this._gridApis.get(activeGrid);
            if (!api) return;
            const selected = api.getSelectedRows();
            if (selected.length === 0) return;
            this.publishBulkRemove(activeGrid, selected);
        }, { signal });

        const spacer = document.createElement('span');
        spacer.className = 'toolbar-spacer';

        const closeBtn = document.createElement('button');
        closeBtn.className = 'btn btn-sm';
        closeBtn.textContent = 'Close';
        closeBtn.addEventListener('click', () => closeWithValue('close'), { signal });

        toolbar.appendChild(addBtn);
        toolbar.appendChild(bulkAddBtn);
        toolbar.appendChild(sep1);
        toolbar.appendChild(addTagBtn);
        toolbar.appendChild(removeTagBtn);
        toolbar.appendChild(sep2);
        toolbar.appendChild(removeBtn);
        toolbar.appendChild(spacer);
        toolbar.appendChild(closeBtn);

        contentArea.appendChild(toolbar);

        // --- Render first tab ---
        const firstGrid = grids[0];
        this._activeTabs.set(groupName, firstGrid);
        this._renderGrid(firstGrid, gridContainer);
    }

    // =========================================================================
    // Bulk Add overlay
    // =========================================================================

    _showBulkAddOverlay(microName, container) {
        // Prevent double overlay
        if (container.querySelector('.micro-bulk-overlay')) return;

        const config = MICRO_GRID_CONFIGS[microName];
        const existingRows = this._data.get(microName) || [];
        const existingPatterns = new Set(existingRows.map(r => _upper(r.pattern || '')));

        const overlay = document.createElement('div');
        overlay.className = 'micro-bulk-overlay';

        const title = document.createElement('div');
        title.style.cssText = 'font-size:14px;font-weight:600;color:#fff;';
        title.textContent = 'Bulk Add — paste tickers / ISINs';

        const subtitle = document.createElement('div');
        subtitle.style.cssText = 'font-size:12px;color:#888;';
        subtitle.textContent = 'Comma/newline/tab separated. ISINs (12 chars) and ALL-CAPS tickers auto-detected. Duplicates ignored.';

        const textarea = document.createElement('textarea');
        textarea.placeholder = 'AAPL, MSFT, GOOGL\nUS0378331005\nTSLA NVDA';

        const preview = document.createElement('div');
        preview.className = 'bulk-preview';

        textarea.addEventListener('input', () => {
            const items = _extractIdentifiers(textarea.value);
            const deduped = items.filter(i => !existingPatterns.has(_upper(i.pattern)));
            preview.textContent = deduped.length
                ? `Will add ${deduped.length} item(s): ${deduped.map(i => i.pattern).join(', ')}`
                : items.length ? 'All items already exist.' : '';
        });

        const actions = document.createElement('div');
        actions.className = 'bulk-actions';

        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'btn btn-sm';
        cancelBtn.textContent = 'Cancel';
        cancelBtn.addEventListener('click', () => overlay.remove());

        const importBtn = document.createElement('button');
        importBtn.className = 'btn btn-sm btn-primary';
        importBtn.textContent = 'Import';
        importBtn.addEventListener('click', () => {
            const items = _extractIdentifiers(textarea.value);
            const deduped = items.filter(i => !existingPatterns.has(_upper(i.pattern)));
            if (deduped.length > 0) {
                this.publishBulkAdd(microName, deduped);
            }
            overlay.remove();
        });

        actions.appendChild(cancelBtn);
        actions.appendChild(importBtn);

        overlay.appendChild(title);
        overlay.appendChild(subtitle);
        overlay.appendChild(textarea);
        overlay.appendChild(preview);
        overlay.appendChild(actions);

        container.appendChild(overlay);
        textarea.focus();
    }

    // =========================================================================
    // Bulk Tag overlay (add or remove tags to/from selected rows)
    // =========================================================================

    _showTagBulkOverlay(microName, container, mode) {
        if (container.querySelector('.micro-tag-bulk-overlay')) return;

        const api = this._gridApis.get(microName);
        if (!api) return;

        const selected = api.getSelectedRows();
        if (selected.length === 0) {
            // Brief visual hint — no rows selected
            return;
        }

        const config = MICRO_GRID_CONFIGS[microName];
        const pkCol = config.primaryKeys[0];

        const overlay = document.createElement('div');
        overlay.className = 'micro-tag-bulk-overlay';

        const title = document.createElement('div');
        title.style.cssText = 'font-size:14px;font-weight:600;color:#fff;';
        title.textContent = mode === 'add'
            ? `Add tag to ${selected.length} selected row(s)`
            : `Remove tag from ${selected.length} selected row(s)`;

        // Input row
        const inputRow = document.createElement('div');
        inputRow.className = 'tag-input-row';
        const tagInput = document.createElement('input');
        tagInput.type = 'text';
        tagInput.placeholder = 'Type tag name and press Enter';
        inputRow.appendChild(tagInput);

        // Tag suggestions
        const sugContainer = document.createElement('div');
        sugContainer.className = 'tag-suggestions';
        const allTags = [...getAllKnownTags()].sort();
        let chosenTag = '';

        const renderSuggestions = () => {
            sugContainer.innerHTML = '';
            const tags = mode === 'remove'
                ? _getTagsFromRows(selected)
                : allTags;
            tags.forEach(tag => {
                const pill = _createPillEl(tag, false);
                pill.style.cursor = 'pointer';
                pill.addEventListener('click', () => {
                    tagInput.value = tag;
                    chosenTag = tag;
                });
                sugContainer.appendChild(pill);
            });
        };
        renderSuggestions();

        tagInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                chosenTag = tagInput.value.trim();
                if (chosenTag) _applyTag();
            }
        });

        const actions = document.createElement('div');
        actions.className = 'bulk-actions';

        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'btn btn-sm';
        cancelBtn.textContent = 'Cancel';
        cancelBtn.addEventListener('click', () => overlay.remove());

        const applyBtn = document.createElement('button');
        applyBtn.className = 'btn btn-sm btn-primary';
        applyBtn.textContent = mode === 'add' ? 'Add Tag' : 'Remove Tag';
        applyBtn.addEventListener('click', () => {
            chosenTag = tagInput.value.trim();
            if (chosenTag) _applyTag();
        });

        const _applyTag = () => {
            if (!chosenTag) return;
            const updates = [];
            for (const row of selected) {
                const currentTags = _parseTags(row.tags);
                let newTags;
                if (mode === 'add') {
                    if (currentTags.includes(chosenTag)) continue;
                    newTags = [...currentTags, chosenTag];
                } else {
                    if (!currentTags.includes(chosenTag)) continue;
                    newTags = currentTags.filter(t => t !== chosenTag);
                }
                updates.push({ [pkCol]: row[pkCol], tags: newTags.join(',') });
            }
            if (updates.length > 0) {
                this.publishBulkUpdate(microName, updates);
                getAllKnownTags().add(chosenTag);
            }
            overlay.remove();
        };

        actions.appendChild(cancelBtn);
        actions.appendChild(applyBtn);

        overlay.appendChild(title);
        overlay.appendChild(inputRow);
        overlay.appendChild(sugContainer);
        overlay.appendChild(actions);

        container.appendChild(overlay);
        tagInput.focus();
    }

    // =========================================================================
    // Grid rendering
    // =========================================================================

    _switchTab(groupName, gridName, container) {
        const currentGrid = this._activeTabs.get(groupName);
        if (currentGrid) {
            this._destroyGrid(currentGrid);
        }

        this._activeTabs.set(groupName, gridName);
        container.innerHTML = '';
        this._renderGrid(gridName, container);
    }

    _renderGrid(microName, container) {
        const config = MICRO_GRID_CONFIGS[microName];
        if (!config) {
            console.error('[MicroGrid] No config for:', microName);
            return;
        }

        const gridDiv = document.createElement('div');
        gridDiv.className = 'ag-theme-balham-dark';
        gridDiv.style.width = '100%';
        gridDiv.style.height = '100%';
        container.appendChild(gridDiv);

        const rows = this._data.get(microName) || [];
        seedKnownTags(rows);
        const mgr = this;

        const gridOptions = {
            columnDefs: config.columns,
            rowData: rows,
            rowSelection: 'multiple',
            suppressRowClickSelection: true,
            animateRows: false,
            getRowId: (params) => String(params.data[config.primaryKeys[0]]),
            defaultColDef: {
                resizable: true,
                sortable: true,
                filter: true,
                suppressMovable: true,
            },
            singleClickEdit: true,
            stopEditingWhenCellsLoseFocus: true,
            onCellValueChanged: (event) => {
                const pkCol = config.primaryKeys[0];
                const row = { [pkCol]: event.data[pkCol] };
                row[event.colDef.field] = event.newValue;

                // Auto-set color when severity changes
                if (event.colDef.field === 'severity' && SEVERITY_COLOR_MAP[event.newValue]) {
                    row.color = SEVERITY_COLOR_MAP[event.newValue];
                }

                mgr.publishUpdate(microName, row);
            },
        };

        const api = agGrid.createGrid(gridDiv, gridOptions);
        this._gridApis.set(microName, api);
    }

    _destroyGrid(microName) {
        const api = this._gridApis.get(microName);
        if (api) {
            try { api.destroy(); } catch (e) { /* ignore */ }
            this._gridApis.delete(microName);
        }
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    async _cleanupGroup(groupName) {
        const groupConfig = MICRO_GRID_GROUPS[groupName];
        if (!groupConfig) return;

        const ac = this._abortControllers.get(groupName);
        if (ac) {
            ac.abort();
            this._abortControllers.delete(groupName);
        }

        for (const gridName of groupConfig.grids) {
            this._destroyGrid(gridName);
            await this.unsubscribe(gridName);
        }

        this._openModals.delete(groupName);
        this._activeTabs.delete(groupName);
    }

    async destroy() {
        for (const [, ac] of this._abortControllers) {
            try { ac.abort(); } catch (e) { /* ignore */ }
        }
        this._abortControllers.clear();

        for (const [groupName, dialog] of this._openModals) {
            try { if (dialog?.open) dialog.close(); } catch (e) { /* ignore */ }
        }

        for (const [name] of this._gridApis) {
            this._destroyGrid(name);
        }

        for (const microName of new Set(this._subscribed)) {
            await this.unsubscribe(microName).catch(() => {});
        }

        this._data.clear();
        this._gridApis.clear();
        this._openModals.clear();
        this._activeTabs.clear();
        this._handlers.clear();
        this._subscribed.clear();
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function _getTagsFromRows(rows) {
    const tagSet = new Set();
    for (const row of rows) {
        for (const t of _parseTags(row.tags)) {
            tagSet.add(t);
        }
    }
    return [...tagSet].sort();
}
