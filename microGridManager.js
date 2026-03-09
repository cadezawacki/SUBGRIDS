
/**
 * MicroGridManager — central frontend manager for micro-grid modals.
 *
 * Handles:
 *   - WebSocket subscribe/unsubscribe/publish for micro-grids
 *   - Tabbed modal lifecycle (open, tab switch, close)
 *   - AG Grid instance creation and data management per micro-grid
 *   - Real-time delta application from other users
 *   - Cleanup of all resources on close/destroy
 */

import { MICRO_GRID_CONFIGS, MICRO_GRID_GROUPS, SEVERITY_COLOR_MAP } from './microGridConfigs.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function _upper(s) { return (s || '').toUpperCase(); }

function _topic(microName) { return `MICRO.${_upper(microName)}`; }

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

    /**
     * Subscribe to a micro-grid's real-time updates.
     * Registers message handlers and sends the subscribe message.
     */
    async subscribe(microName) {
        if (this._subscribed.has(microName)) return;

        const sm = this._page.subscriptionManager();
        const topic = _topic(microName);

        // Register handlers before subscribing so we catch the initial snapshot
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

    /**
     * Unsubscribe from a micro-grid.
     */
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
        this._refreshGrid(microName);
    }

    _onDelta(microName, message) {
        const payloads = message?.data?.payloads;
        if (!payloads) return;

        const config = MICRO_GRID_CONFIGS[microName];
        if (!config) return;

        const pkCol = config.primaryKeys[0];
        let rows = this._data.get(microName) || [];

        // Apply removes
        const removeRows = payloads.remove || [];
        if (removeRows.length > 0) {
            const removeIds = new Set(removeRows.map(r => String(r[pkCol])));
            rows = rows.filter(r => !removeIds.has(String(r[pkCol])));
        }

        // Apply adds
        const addRows = payloads.add || [];
        if (addRows.length > 0) {
            rows = [...rows, ...addRows];
        }

        // Apply updates
        const updateRows = payloads.update || [];
        if (updateRows.length > 0) {
            const updateMap = new Map(updateRows.map(r => [String(r[pkCol]), r]));
            rows = rows.map(r => {
                const upd = updateMap.get(String(r[pkCol]));
                return upd ? { ...r, ...upd } : r;
            });
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

    // =========================================================================
    // Modal / Tab UI
    // =========================================================================

    /**
     * Open a tabbed modal for a micro-grid group.
     * Returns a Promise that resolves when the modal is closed.
     */
    async openGroup(groupNameOrConfig) {
        const groupConfig = typeof groupNameOrConfig === 'string'
            ? MICRO_GRID_GROUPS[groupNameOrConfig]
            : groupNameOrConfig;

        if (!groupConfig) {
            console.error('[MicroGrid] Unknown group:', groupNameOrConfig);
            return;
        }

        const groupName = groupConfig.name;

        // If already open, bring to front
        const existing = this._openModals.get(groupName);
        if (existing && existing.open) {
            return;
        }

        // Subscribe to all grids in the group
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
                    width: 700px;
                    max-width: 90vw;
                    max-height: 80vh;
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
                    min-height: 280px;
                    padding: 0;
                    overflow: hidden;
                }
                .micro-grid-container .ag-root-wrapper {
                    border: none;
                }
                .micro-toolbar {
                    display: flex;
                    gap: 8px;
                    padding: 8px 16px;
                    border-top: 1px solid rgba(255,255,255,0.1);
                    flex-shrink: 0;
                }
                .micro-toolbar .btn {
                    font-size: 12px;
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

        // --- Grid container ---
        const gridContainer = document.createElement('div');
        gridContainer.className = 'micro-grid-container';
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
            selected.forEach(row => this.publishRemove(activeGrid, row));
        }, { signal });

        const closeBtn = document.createElement('button');
        closeBtn.className = 'btn btn-sm';
        closeBtn.textContent = 'Close';
        closeBtn.addEventListener('click', () => closeWithValue('close'), { signal });

        toolbar.appendChild(addBtn);
        toolbar.appendChild(removeBtn);
        toolbar.appendChild(document.createElement('span'));
        toolbar.lastChild.style.flex = '1';
        toolbar.appendChild(closeBtn);

        contentArea.appendChild(toolbar);

        // --- Render first tab ---
        const firstGrid = grids[0];
        this._activeTabs.set(groupName, firstGrid);
        this._renderGrid(firstGrid, gridContainer);
    }

    _switchTab(groupName, gridName, container) {
        // Destroy current grid
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
        const mgr = this;

        const gridOptions = {
            columnDefs: config.columns,
            rowData: rows,
            rowSelection: 'multiple',
            suppressRowClickSelection: false,
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

        // Use agGrid.createGrid (AG Grid 31+)
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

        // Abort all modal event listeners
        const ac = this._abortControllers.get(groupName);
        if (ac) {
            ac.abort();
            this._abortControllers.delete(groupName);
        }

        // Destroy all grids in the group
        for (const gridName of groupConfig.grids) {
            this._destroyGrid(gridName);
            await this.unsubscribe(gridName);
        }

        this._openModals.delete(groupName);
        this._activeTabs.delete(groupName);
    }

    /**
     * Full cleanup — call on page destroy.
     */
    async destroy() {
        // Abort all event listeners
        for (const [, ac] of this._abortControllers) {
            try { ac.abort(); } catch (e) { /* ignore */ }
        }
        this._abortControllers.clear();

        // Close all open modals
        for (const [groupName, dialog] of this._openModals) {
            try { if (dialog?.open) dialog.close(); } catch (e) { /* ignore */ }
        }

        // Destroy all grids
        for (const [name] of this._gridApis) {
            this._destroyGrid(name);
        }

        // Unsubscribe from all micro-grids
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
