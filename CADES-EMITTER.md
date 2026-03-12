# CadesEmitter Documentation

`CadesEmitter` is a flexible event emitter wrapper built on top of `CozyEvent`. It adds token-based unsubscription, scoped listeners, bound contexts, batch registration, sync/async emit modes, custom registrar backends, and full introspection.

## Table of Contents

- [Basic Usage](#basic-usage)
- [Unsubscribing / Turning Off Listeners](#unsubscribing--turning-off-listeners)
  - [Token-based with offToken()](#1-token-based-with-offtoken)
  - [Reference-based with off()](#2-reference-based-with-off)
  - [Passing a token to off()](#3-passing-a-token-to-off)
  - [AbortController signals](#4-abortcontroller-signals)
  - [Bulk removal with clear()](#5-bulk-removal-with-clear)
- [Features](#features)
  - [Debounce](#debounce)
  - [Scoped Subscriptions](#scoped-subscriptions-with-createscope)
  - [Bound Context Subscriptions](#bound-context-subscriptions)
  - [Batch Registration with onMany()](#batch-registration-with-onmany)
  - [Sync vs Async Emit](#sync-vs-async-emit)
  - [Introspection](#introspection)
  - [Custom Registrars](#custom-registrars)
  - [Destroy](#destroy)

---

## Basic Usage

```js
import { CadesEmitter } from './cades-emitter.js';

const emitter = new CadesEmitter();

emitter.on('player:move', (x, y) => {
    console.log(`Moved to ${x}, ${y}`);
});

emitter.emit('player:move', 10, 20);
```

### Constructor Options

```js
const emitter = new CadesEmitter({
    emitter: existingCozyEvent,     // optional — provide your own CozyEvent instance
    defaultRegistrar: 'cozy-sync',  // 'cozy-sync' (default) or 'cozy-async'
    maxListeners: 0,                // 0 = unlimited
});
```

---

## Unsubscribing / Turning Off Listeners

There are five ways to stop listening to events.

### 1. Token-based with `offToken()`

Every call to `.on()` or `.once()` returns a frozen token object. This is the most direct and reliable way to unsubscribe:

```js
const token = emitter.on('player:move', (x, y) => {
    console.log(`Moved to ${x}, ${y}`);
});

// Later, unsubscribe:
emitter.offToken(token);
```

### 2. Reference-based with `off()`

Pass the original function reference, just like a standard EventEmitter:

```js
function onDamage(amount) {
    console.log(`Took ${amount} damage`);
}

emitter.on('player:damage', onDamage);

// Later:
emitter.off('player:damage', onDamage);
```

### 3. Passing a token to `off()`

The `.off()` method detects if you pass a token and routes to `offToken` automatically:

```js
const token = emitter.on('chat:message', handleMsg);
emitter.off('chat:message', token); // works!
```

### 4. AbortController signals

Pass an `AbortSignal` via options and the listener auto-removes when the signal aborts. Great for lifecycle management:

```js
const controller = new AbortController();

emitter.on('enemy:spawn', handleSpawn, undefined, {
    signal: controller.signal,
});

// Auto-unsubscribe by aborting:
controller.abort();
```

### 5. Bulk removal with `clear()`

```js
emitter.clear('player:move');           // All listeners for one event
emitter.clear();                        // All listeners for all events
emitter.clear(null, 'cozy-async');      // All listeners on a specific registrar
emitter.clear('player:move', 'cozy-sync'); // One event on one registrar
```

---

## Features

### Debounce

Rate-limit how often a listener fires by passing a `debounce` option. The handler will only execute after emissions have settled for the given number of milliseconds:

```js
// Only process after mouse stops moving for 150ms
emitter.on('mouse:move', updateTooltip, undefined, { debounce: 150 });
```

**Leading edge** — fire immediately on the first call, then lock out until emissions settle:

```js
// Respond to the first click instantly, ignore spam for 300ms
emitter.on('ui:click', handleClick, undefined, {
    debounce: { wait: 300, leading: true },
});
```

**Leading-only** (throttle-style) — fire on the leading edge, skip the trailing call:

```js
emitter.on('player:input', processInput, undefined, {
    debounce: { wait: 100, leading: true, trailing: false },
});
```

**Flush and cancel** — the returned token exposes control methods for pending debounced calls:

```js
const token = emitter.on('search:type', runQuery, undefined, { debounce: 250 });

// Force the pending call to execute right now (e.g. user pressed Enter)
token.flush();

// Discard the pending call entirely (e.g. component unmounting)
token.cancel();
```

Pending debounced calls are automatically cancelled when a listener is unsubscribed via `offToken`, `off`, `clear`, or `destroy`.

Works with all subscription features — contexts, `once`, `onMany`, scopes, and `AbortController` signals:

```js
const controller = new AbortController();

emitter.on('resize', obj, 'onResize', {
    debounce: 200,
    signal: controller.signal,
});

// Abort auto-unsubscribes AND cancels the pending debounce
controller.abort();
```

### Scoped Subscriptions with `createScope()`

Creates an isolated group of listeners that can be torn down all at once. Useful for UI components, game phases, or any temporary listener group:

```js
const scope = emitter.createScope();

scope.on('ui:click', handleClick);
scope.on('ui:hover', handleHover);
scope.once('ui:init', handleInit);

// You can emit through the scope too:
scope.emit('ui:click', { x: 100, y: 200 });

// Tear down everything in this scope at once:
scope.clear();
```

The scope tracks all tokens internally and cleans them up in one `clear()` call.

### Bound Context Subscriptions

Bind a `this` context directly when subscribing — no manual `.bind()` needed. You can pass a method reference or a method name as a string:

```js
class Player {
    constructor() { this.hp = 100; }
    onHit(damage) { this.hp -= damage; }
}

const player = new Player();

// Context + method reference:
emitter.on('player:hit', player, player.onHit);

// Context + method name string:
emitter.on('player:hit', player, 'onHit');
```

When unsubscribing with `off()`, pass the same context to match:

```js
emitter.off('player:hit', player.onHit, player);
```

### Batch Registration with `onMany()`

Register many listeners at once. Returns an array of tokens.

**Object shorthand** — keys are event names:

```js
const tokens = emitter.onMany({
    'game:start': handleStart,
    'game:over': handleOver,
    'game:pause': handlePause,
});
```

**Array of tuples:**

```js
const tokens = emitter.onMany([
    ['player:move', onMove],
    ['player:jump', onJump],
]);
```

**Array of descriptors** — supports `once`, `context`, and `method`:

```js
const tokens = emitter.onMany([
    { event: 'player:spawn', handler: onSpawn, once: true },
    { event: 'player:move', handler: onMove },
    { event: 'player:hit', context: player, method: 'onHit' },
]);
```

### Sync vs Async Emit

```js
emitter.emit('data:ready', payload);       // Uses the default registrar
emitter.emitSync('data:ready', payload);   // Forces synchronous dispatch
emitter.emitAsync('data:ready', payload);  // Returns a Promise (Promise.all of all handlers)
```

`emitAsync` is useful when handlers do async work and you need to wait for all of them to finish.

### Introspection

```js
emitter.listenerCount('player:move');   // Number of listeners on this event
emitter.listenerCount();                // Total listeners across all events

emitter.events();                       // ['player:move', 'player:hit', ...]

emitter.has('player:move', onMove);     // true if this handler is registered
emitter.has('player:hit', player.onHit, player); // checks with context

emitter.lookup('player:move');          // Array of listener details for one event
emitter.lookup();                       // Map of all events to their listener details
```

Each entry from `lookup()` contains:

```js
{
    event: 'player:move',
    registrar: 'cozy-sync',
    once: false,
    handler: [Function],
    context: null,
}
```

### Custom Registrars

Plug in entirely custom event backends:

```js
emitter.useRegistrar('my-bus', {
    on(evt, fn) { /* ... */ },
    once(evt, fn) { /* ... */ },
    off(evt, fn) { /* ... */ },
    emit(evt, args) { /* ... */ },
    removeAll(evt) { /* ... */ },
});

// Use it for specific subscriptions:
emitter.on('custom:event', handler, undefined, { registrar: 'my-bus' });

// Or set it as the default:
emitter.setDefaultRegistrar('my-bus');
```

All five methods (`on`, `once`, `off`, `emit`, `removeAll`) are required.

### Destroy

Full teardown that clears all listeners and prevents further use:

```js
emitter.destroy();
// Any subsequent call throws: "CadesEmitter is destroyed"
```

Calling `destroy()` multiple times is safe — subsequent calls are no-ops.
