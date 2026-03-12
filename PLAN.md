# Micro-Grids Implementation Plan

## Overview

Add a generalized "micro-grid" subsystem: lightweight, globally-shared sub-grids that appear as tabbed modal dialogs on the frontend, persist to the database, update in real-time across all users, and flow edits through the existing Rules Engine for cross-grid writes.

**First use-case:** "Hot Tickers" on pt.js — a shared watchlist with columns: `column` (default "ticker"), `pattern` (regex/literal), `severity` (low/med/high/other), `color` (auto-mapped from severity, user-overridable).

---

## Architecture Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Backend actor model | Single global `MicroGridActor` per micro-grid (no slices/filters) | Requirement: one global actor, not per-room like GridActor |
| Action event | New `"micro_publish"` action (ID 22 in ActionMap) | Avoid overlap with existing `"publish"` (ID 3) |
| Frontend rendering | Reuse AG Grid via `ArrowAgGridAdapter` inside modal | Consistent with existing grid infrastructure |
| Modal system | Extend `ModalManager.showCustom()` with tab support | Existing modal system is solid; tabs are additive |
| Persistence | `get_db().upsert()` / `get_db().select()` with table-existence guard | Existing DB layer handles schema evolution safely |
| Rules integration | Micro-grid edits feed into `RulesEngine.run_ingress()` with `source_task="micro_publish"` | Enables cross-grid writes per requirement |
| Real-time sync | PubSubRouter topic `MICRO.{grid_name}` — all subscribers get all updates | Global actor = single topic, no filter variants |
| Grouping | `MicroGridGroup` config object maps group name → ordered list of micro-grid names (tabs) | Declarative grouping, rendered as tabs in modal |

---

## File-by-File Implementation

### Phase 1: Backend — MicroGridActor + Persistence

#### 1.1 New file: `micro_grid.py`

This is the core backend module. It contains:

**`MicroGridConfig` dataclass** (~line 1)
```python
@dataclass(frozen=True)
class MicroGridConfig:
    name: str                          # e.g. "hot_tickers"
    table_name: str                    # DB table, e.g. "micro_hot_tickers"
    primary_keys: Tuple[str, ...]      # e.g. ("id",)
    columns: Dict[str, Any]            # {col_name: default_value} — defines schema
    column_types: Dict[str, pl.DataType]  # polars types for each column
    rules_enabled: bool = True         # whether edits flow through rules engine
    persist: bool = True               # whether to persist to disk
```

**`MicroGridRegistry`** class (~line 30)
- Singleton registry of all configured micro-grids
- `register(config: MicroGridConfig)` — registers a micro-grid definition
- `get(name: str) -> MicroGridConfig` — lookup by name
- `list_configs() -> List[MicroGridConfig]`
- Validates table existence on registration via `get_db()._table_exists()`
- If table doesn't exist and `persist=True`, creates it with schema from `columns` + `column_types`
- **Safety guard**: if table exists, does NOT add new columns automatically (requirement)

**`MicroGridActor`** class (~line 80)
- Modeled after `GridActor` but simplified — no filters, no slices, no MVCC version chains
- Uses a simple `pl.DataFrame` as in-memory state (micro-grids are small)
- Key attributes:
  - `config: MicroGridConfig`
  - `_data: pl.DataFrame` — current state
  - `_subscribers: Dict[str, weakref.ref]` — token → websocket weakref
  - `_dirty: bool` — whether pending DB write
  - `_lock: asyncio.Lock` — serialize mutations
- Methods:
  - `async load_from_db()` — initial load via `get_db().select(config.table_name)`
  - `async apply_edit(patch: Dict, user: str, ingress_id: int) -> pl.DataFrame` — apply add/update/remove, return delta
  - `async persist()` — `get_db().upsert(config.table_name, self._data)` for dirty rows
  - `add_subscriber(ws)` / `remove_subscriber(ws)` — weakref-based, same pattern as GridActor
  - `snapshot() -> pl.DataFrame` — return copy of current data
  - `subscriber_count() -> int`
- **No hibernation** — micro-grids are small, always in memory
- **Auto-generate row IDs** — if PK is "id", auto-assign UUID on add

**`MicroGridGroup` dataclass** (~line 160)
```python
@dataclass(frozen=True)
class MicroGridGroup:
    name: str                          # e.g. "pt_tools"
    display_name: str                  # e.g. "Portfolio Tools"
    grids: Tuple[str, ...]             # ordered micro-grid names (tab order)
```

**Module-level functions** (~line 180)
- `register_micro_grid(config)` — convenience wrapper
- `register_micro_grid_group(group)` — convenience wrapper
- `get_micro_actor(name) -> MicroGridActor`
- `get_micro_group(name) -> MicroGridGroup`
- `async init_micro_grids()` — called at startup, loads all registered grids from DB

#### 1.2 Modify: `grid_system_v4.py`

**In `GridSystem.__init__`** (~line 3111):
- Add `self._micro_registry: MicroGridRegistry` attribute
- Import and store reference to micro-grid registry

**New method: `GridSystem.ingest_micro_publish()`** (~line 3500):
```python
async def ingest_micro_publish(self, micro_name, payloads, user, trace):
    """Process a micro-grid edit, persist, run rules, yield outbound Publish messages."""
    actor = get_micro_actor(micro_name)
    config = actor.config

    # 1. Assign global clock
    based_on = await self.clock.next()
    action_seq = await self.action_clock.next()
    ingress_id = action_seq

    # 2. Apply edit to micro-grid actor
    async with actor._lock:
        delta_df = await actor.apply_edit(payloads, user, ingress_id)

    # 3. Build outbound Publish for micro-grid subscribers
    if delta_df is not None and len(delta_df) > 0:
        # Build a Publish-like message for MICRO.{name} topic
        micro_pub = MicroPublish(
            micro_name=micro_name,
            delta=delta_df,
            based_on=based_on,
            action_seq=action_seq,
            pk_columns=config.primary_keys,
        )
        yield micro_pub

    # 4. Optionally run rules engine
    if config.rules_enabled and delta_df is not None:
        # Build RoomContext pointing to micro-grid's virtual room
        micro_context = RoomContext(
            room=f"MICRO.{micro_name.upper()}",
            grid_id=f"micro_{micro_name}",
            primary_keys=config.primary_keys,
        )
        # Create publish_delta_by_room for rules engine
        publish_delta = {micro_context.room: delta_df}
        leases = SnapshotLeases(self)

        async for rule_pub in self.rules.run_ingress(
            system=self,
            actor_context=micro_context,
            ingress_id=ingress_id,
            based_on=based_on,
            publish_delta_by_room=publish_delta,
            primary_keys=config.primary_keys,
            leases=leases,
            trace=trace,
            emit_options=None,
            trigger_rules=True,
        ):
            yield rule_pub

        leases.release_all()

    # 5. Persist
    if config.persist:
        await actor.persist()
```

#### 1.3 Modify: `connectionManager.py`

**Add to `_DISPATCH` dict** (~line 691):
```python
"micro_publish": self._handle_micro_publish,
"micro_subscribe": self._handle_micro_subscribe,
"micro_unsubscribe": self._handle_micro_unsubscribe,
```

**New handler: `_handle_micro_publish()`** (~line 926):
```python
async def _handle_micro_publish(self, websocket, d):
    trace = self._extract_trace(d)
    micro_name = d.get("micro_name")
    payloads = d.get("data", {}).get("payloads", {})
    user = self._get_user(websocket)

    outbounds = []
    async for outbound in self.grid_system.ingest_micro_publish(
        micro_name, payloads, user, trace
    ):
        outbounds.append(
            self.ctx.spawn(self.broadcast_micro(outbound))
        )
    await asyncio.gather(*outbounds, return_exceptions=True)
```

**New handler: `_handle_micro_subscribe()`**:
```python
async def _handle_micro_subscribe(self, websocket, d):
    micro_name = d.get("micro_name")
    actor = get_micro_actor(micro_name)
    actor.add_subscriber(websocket)

    # Subscribe to MICRO.{name} topic in router
    topic = f"MICRO.{micro_name.upper()}"
    self.router.subscribe(websocket, topic)

    # Send initial snapshot
    snapshot = actor.snapshot()
    await self._send_direct_message(websocket, {
        "action": "micro_subscribe",
        "micro_name": micro_name,
        "data": snapshot_to_payload(snapshot, actor.config),
        "trace": d.get("trace"),
    })
```

**New handler: `_handle_micro_unsubscribe()`**:
- Remove subscriber from actor
- Unsubscribe from router topic

**New method: `broadcast_micro()`**:
- Encode delta and send to all subscribers of `MICRO.{name}` topic via router

#### 1.4 Modify: `payloadV4.py`

**New class: `MicroPublish`** (~after Publish class):
```python
@dataclass
class MicroPublish:
    micro_name: str
    delta: pl.DataFrame          # The changed rows
    based_on: int
    action_seq: int
    pk_columns: Tuple[str, ...]
    add: List[Delta] = None
    update: List[Delta] = None
    remove: List[Delta] = None
    trace: str = None
```

#### 1.5 Modify: `actionMap.js`

Add new action IDs:
```javascript
22: 'micro_publish',
23: 'micro_subscribe',
24: 'micro_unsubscribe',
```

---

### Phase 2: Frontend — MicroGridManager + Modal UI

#### 2.1 New file: `microGridManager.js`

Central frontend manager for all micro-grid interactions.

**`MicroGridManager`** class:

**Constructor:**
```javascript
constructor(subscriptionManager, modalManager, emitter) {
    this._sm = subscriptionManager;
    this._modal = modalManager;
    this._emitter = emitter;
    this._activeGrids = new Map();    // micro_name → { engine, adapter, data, config }
    this._openModals = new Map();     // group_name → { dialog, tabs, activeTab }
    this._subscriptions = new Set();  // micro_names currently subscribed
}
```

**Subscription Methods:**
- `async subscribe(microName)` — sends `micro_subscribe` via WebSocket, registers message handlers for `micro_subscribe` (initial snapshot) and `micro_publish` (deltas)
- `async unsubscribe(microName)` — sends `micro_unsubscribe`, cleans up handlers
- `_handleSnapshot(microName, data)` — stores initial data, refreshes grid if modal is open
- `_handleDelta(microName, data)` — applies incremental update to in-memory data, refreshes grid

**Modal/Tab Methods:**
- `async openGroup(groupConfig)` — opens a tabbed modal for a `MicroGridGroup`
  - Creates `<dialog>` with tab bar + content area
  - Subscribes to all grids in group
  - Renders first tab's grid
  - Returns Promise that resolves on close
- `_renderTab(microName, contentArea)` — creates AG Grid in the content area for the given micro-grid
- `_switchTab(microName)` — destroys current grid, renders new tab's grid
- `closeGroup(groupName)` — closes modal, unsubscribes all grids, destroys adapters

**Edit Methods:**
- `async publishEdit(microName, payloads)` — sends `micro_publish` message via WebSocket
  - payloads format: `{ add: [...], update: [...], remove: [...] }`
- `_setupCellEditHandler(microName, adapter)` — wires AG Grid `cell-edit` events to `publishEdit()`

**Grid Factory:**
- `_createGridAdapter(microName, data, container)` — creates `ArrowAgGridAdapter` with column defs from config, mounts into container DOM element

**Cleanup:**
- `destroy()` — unsubscribes all, closes all modals, destroys all adapters

#### 2.2 New file: `microGridConfigs.js`

Declarative configuration for all micro-grids and groups.

```javascript
export const MICRO_GRID_CONFIGS = {
    hot_tickers: {
        name: 'hot_tickers',
        displayName: 'Hot Tickers',
        primaryKeys: ['id'],
        columns: [
            { field: 'id', hide: true },
            {
                field: 'column',
                headerName: 'Column',
                editable: true,
                cellEditor: 'agSelectCellEditor',
                cellEditorParams: { values: ['ticker', 'isin', 'cusip', 'sedol', 'name'] },
                defaultValue: 'ticker',
            },
            {
                field: 'pattern',
                headerName: 'Pattern',
                editable: true,
                cellEditor: 'agTextCellEditor',
            },
            {
                field: 'severity',
                headerName: 'Severity',
                editable: true,
                cellEditor: 'agSelectCellEditor',
                cellEditorParams: { values: ['low', 'med', 'high', 'other'] },
                defaultValue: 'low',
            },
            {
                field: 'color',
                headerName: 'Color',
                editable: true,
                cellRenderer: 'colorCellRenderer',   // custom: shows color swatch
                cellEditor: 'colorCellEditor',        // custom: color picker
            },
        ],
        // Auto-map severity → default color
        severityColorMap: {
            low: '#FFFF00',      // yellow
            med: '#FFBF00',      // amber
            high: '#FF0000',     // red
            other: '#800080',    // purple
        },
        addRowDefaults: (row) => ({
            id: crypto.randomUUID(),
            column: 'ticker',
            severity: 'low',
            color: '#FFFF00',
            ...row,
        }),
    },
};

export const MICRO_GRID_GROUPS = {
    pt_tools: {
        name: 'pt_tools',
        displayName: 'Portfolio Tools',
        grids: ['hot_tickers'],   // expandable — add more micro-grids here later
    },
};
```

#### 2.3 Modify: `enhancedSubscriptionManager.js`

**In `messageRouter()`** (~line 133):
Add routing for micro-grid actions:
```javascript
case 'micro_subscribe':
case 'micro_publish':
    this._emitter.emit(`micro:${message.micro_name}:${action}`, message);
    break;
```

This uses the existing emitter to dispatch micro-grid messages to whoever is listening (the `MicroGridManager`).

#### 2.4 Modify: `modalManager.js`

No structural changes needed — `showCustom()` already supports arbitrary content via `setupContent(contentArea, dialog, closeWithValue)`. The `MicroGridManager` will build tabbed UI inside `contentArea`.

#### 2.5 Modify: `pt.js`

**In constructor** (~line 87):
```javascript
this._microGridManager = null;
```

**New method: `_initMicroGrids()`** (~after `_subscribeToMeta`):
```javascript
_initMicroGrids() {
    const { MicroGridManager } = await import('./microGridManager.js');
    this._microGridManager = new MicroGridManager(
        this.subscriptionManager(),
        this.modalManager(),
        this.emitter,
    );
}
```

**New method: `openHotTickers()`**:
```javascript
async openHotTickers() {
    await this._microGridManager.openGroup(MICRO_GRID_GROUPS.pt_tools);
}
```

**Wire up trigger** — add a button/menu item in the pt.js toolbar area that calls `openHotTickers()`. Likely add to the existing header buttons (~line 326-390):
```javascript
// In _setupToolbarButtons() or equivalent:
this.els.hotTickersBtn = document.querySelector('#hot-tickers-btn');
this.els.hotTickersBtn?.addEventListener('click', () => this.openHotTickers());
```

**In `destroy()`** — add cleanup:
```javascript
this._microGridManager?.destroy();
```

---

### Phase 3: Backend Rules Integration

#### 3.1 New file: `micro_grid_rules.py`

Define rules that can be triggered by micro-grid edits and/or trigger micro-grid writes.

**`register_micro_grid_rules(engine)`** function:
```python
def register_micro_grid_rules(engine: RulesEngine):
    """Register rules that involve micro-grids."""

    # Example: When hot_tickers is edited, update severity color if not manually set
    @rule(
        name="micro_severity_color_default",
        room_pattern="MICRO.HOT_TICKERS",
        column_triggers_any=("severity",),
        priority=PRIORITY_LOW,
        emit_mode=EmitMode.IMMEDIATE,
    )
    async def severity_color_default(ctx: RuleContext):
        delta = ctx.triggering_delta
        severity_map = {
            "low": "#FFFF00",
            "med": "#FFBF00",
            "high": "#FF0000",
            "other": "#800080",
        }
        # Only set color for rows where severity changed but color wasn't explicitly set
        if "color" not in delta.columns:
            rows = []
            for row in delta.iter_rows(named=True):
                if row.get("severity") in severity_map:
                    rows.append({
                        "id": row["id"],
                        "color": severity_map[row["severity"]],
                    })
            if rows:
                return pl.DataFrame(rows)
        return None

    engine.register(severity_color_default)
```

#### 3.2 Modify: `portfolio_rules_v4.py`

In `register_portfolio_rules()` (~line 742), add:
```python
from app.helpers.micro_grid_rules import register_micro_grid_rules
register_micro_grid_rules(engine)
```

#### 3.3 Rules Engine Compatibility

The existing `RulesEngine.run_ingress()` already supports:
- `room_pattern` matching via fnmatch — `"MICRO.HOT_TICKERS"` will match
- `column_triggers_any/all` — works with micro-grid columns
- Cross-grid writes via `target_room` — a micro-grid rule could write to a main grid and vice versa
- `publish_delta_by_room` dict — micro-grid delta keyed by `"MICRO.{NAME}"`

**No changes needed to the core rules engine.** The `ingest_micro_publish()` method in GridSystem already feeds deltas into `run_ingress()` with the correct room context.

---

### Phase 4: Wire Everything Together at Startup

#### 4.1 Modify: `connectionManager.py` or startup module

In the application startup sequence (where `GridSystem` is initialized), add:
```python
from app.helpers.micro_grid import (
    MicroGridConfig, register_micro_grid, register_micro_grid_group,
    MicroGridGroup, init_micro_grids
)

# Register micro-grid configs
register_micro_grid(MicroGridConfig(
    name="hot_tickers",
    table_name="micro_hot_tickers",
    primary_keys=("id",),
    columns={"id": "", "column": "ticker", "pattern": "", "severity": "low", "color": "#FFFF00"},
    column_types={
        "id": pl.String,
        "column": pl.String,
        "pattern": pl.String,
        "severity": pl.String,
        "color": pl.String,
    },
))

register_micro_grid_group(MicroGridGroup(
    name="pt_tools",
    display_name="Portfolio Tools",
    grids=("hot_tickers",),
))

# Initialize (load from DB)
await init_micro_grids()
```

---

## Data Flow Summary

```
User edits cell in micro-grid modal (frontend)
  │
  ▼
MicroGridManager.publishEdit("hot_tickers", {update: [{id, severity: "high"}]})
  │
  ▼
WebSocket sends: {action: "micro_publish", micro_name: "hot_tickers", data: {payloads: {update: [...]}}}
  │
  ▼
connectionManager._handle_micro_publish()
  │
  ▼
grid_system.ingest_micro_publish("hot_tickers", payloads, user, trace)
  ├── actor.apply_edit() → delta_df
  ├── yield MicroPublish (broadcast to all MICRO.HOT_TICKERS subscribers)
  ├── rules.run_ingress() → may trigger cross-grid writes
  └── actor.persist() → get_db().upsert("micro_hot_tickers", dirty_rows)
  │
  ▼
connectionManager.broadcast_micro(MicroPublish)
  │
  ▼
router.publish("MICRO.HOT_TICKERS", encoded_delta)
  │
  ▼
All subscribed WebSockets receive delta
  │
  ▼
MicroGridManager._handleDelta("hot_tickers", delta) → updates AG Grid in-place
```

---

## Files Changed/Created Summary

| File | Action | Description |
|---|---|---|
| `micro_grid.py` | **NEW** | MicroGridConfig, MicroGridRegistry, MicroGridActor, MicroGridGroup |
| `micro_grid_rules.py` | **NEW** | Rules for micro-grid edits (severity→color default) |
| `microGridManager.js` | **NEW** | Frontend manager: subscribe, edit, modal/tabs, grid lifecycle |
| `microGridConfigs.js` | **NEW** | Declarative micro-grid and group configs for frontend |
| `grid_system_v4.py` | **MODIFY** | Add `ingest_micro_publish()`, micro registry ref |
| `connectionManager.py` | **MODIFY** | Add dispatch entries + handlers for micro_publish/subscribe/unsubscribe |
| `payloadV4.py` | **MODIFY** | Add `MicroPublish` dataclass |
| `actionMap.js` | **MODIFY** | Add action IDs 22-24 |
| `enhancedSubscriptionManager.js` | **MODIFY** | Route micro-grid messages to emitter |
| `pt.js` | **MODIFY** | Init MicroGridManager, add toolbar button, wire openHotTickers() |
| `portfolio_rules_v4.py` | **MODIFY** | Register micro-grid rules at startup |

---

## Risk Mitigations

1. **No breaking changes to existing publish flow** — micro_publish is an entirely separate action with separate handlers. The existing `"publish"` action ID 3 is untouched.

2. **No new columns on existing tables** — `MicroGridRegistry` validates table existence and schema on startup. If the table exists, it only uses existing columns. New tables are created only for newly registered micro-grids.

3. **Memory safety** — Weakref subscribers (same pattern as GridActor), `asyncio.Lock` for mutation serialization, `destroy()` cleanup on modal close.

4. **No memory leaks** — AG Grid adapters are destroyed when tabs switch or modal closes. WebSocket subscriptions are cleaned up on unsubscribe/disconnect. No global event listeners without cleanup.

5. **Rules engine isolation** — Micro-grid edits use their own room namespace (`MICRO.*`), so existing room patterns in portfolio rules won't accidentally match unless explicitly configured.

6. **Graceful degradation** — If micro-grid table doesn't exist yet, it's created on first startup. If DB is unavailable, in-memory state still works for the session.

---

## Implementation Order

1. `micro_grid.py` — backend core (can be tested independently)
2. `payloadV4.py` — MicroPublish dataclass
3. `grid_system_v4.py` — ingest_micro_publish
4. `connectionManager.py` — dispatch + handlers
5. `actionMap.js` — frontend action IDs
6. `enhancedSubscriptionManager.js` — message routing
7. `microGridConfigs.js` — config declarations
8. `microGridManager.js` — frontend manager
9. `pt.js` — integration + toolbar button
10. `micro_grid_rules.py` — rules (severity→color)
11. `portfolio_rules_v4.py` — register rules
12. Startup wiring
