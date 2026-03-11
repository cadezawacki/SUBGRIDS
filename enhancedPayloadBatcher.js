



// enhancedPayloadBatcher.js
import { OptimizedColumnarCodec } from '@/global/optimizedColumnarCodec.js';
import { asArray } from '@/utils/helpers.js';

const DEBUG_MODE = false;

export class EnhancedPayloadBatcher {
    constructor(options = {}) {
        this.batchWindow = Number.isFinite(options.batchWindow) ? options.batchWindow : 250;
        this.maxBatchSize = Number.isFinite(options.maxBatchSize) ? options.maxBatchSize : 500;
        this.maxBatchBytes = Number.isFinite(options.maxBatchBytes) ? options.maxBatchBytes : 512_000;
        this.onFlush = typeof options.onFlush === 'function' ? options.onFlush : async () => {};
        this.maxPendingBehindSize = Number.isFinite(options.maxPendingBehindSize) ? options.maxPendingBehindSize : 1000;

        this._codec = new OptimizedColumnarCodec();

        // Primary batch accumulation: key -> { context, options, payloads, size, bytes, createdAt, promises[] }
        this._batches = new Map();

        // Sliding-window timers: key -> timer id
        this._timers = new Map();

        // Backpressure tracking: key -> Promise of in-flight flush
        this._inFlight = new Map();

        // Pending-behind buffer: key -> batch (queued while in-flight flush is running)
        this._pendingBehind = new Map();

        this._isDestroyed = false;

        this._stats = {
            batchesSent: 0,
            payloadsBatched: 0,
            bytesSaved: 0,
            compressionRatio: 1
        };
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Add payloads to a batch. Returns a Promise that resolves when the batch
     * containing these payloads is flushed.
     *
     * @param {object} context  - Must contain at least `grid_id`. May contain `room`, `primary_keys`, etc.
     * @param {object} data     - { payloads: { add?: [], update?: [], remove?: [] } }
     * @param {object} options  - Pass-through options forwarded to onFlush.
     * @returns {Promise}
     */
    add(context, data, options = {}) {
        if (this._isDestroyed) {
            return Promise.reject(new Error('PayloadBatcher has been destroyed.'));
        }

        const batchKey = this._computeBatchKey(context);

        // If there is an in-flight flush for this key, accumulate into the
        // pending-behind buffer instead of the primary batch.
        const targetMap = this._inFlight.has(batchKey) ? this._pendingBehind : this._batches;

        let batch = targetMap.get(batchKey);
        if (!batch) {
            batch = this._createBatch(context, options);
            targetMap.set(batchKey, batch);
        }

        if (targetMap === this._pendingBehind && this.maxPendingBehindSize > 0 && batch.size >= this.maxPendingBehindSize) {
            return Promise.reject(new Error(
                `Pending-behind buffer for "${batchKey}" exceeded limit of ${this.maxPendingBehindSize} entries. ` +
                'Flush is still in-flight; apply backpressure upstream.'
            ));
        }

        const payloads = data?.payloads || {};
        let appended = 0;

        for (const action of ['add', 'update', 'remove']) {
            const p = payloads[action];
            if (!p) continue;

            let rows;
            if (this._isColumnarPayload(p)) {
                // Decode columnar to rows -- preserve nulls (no stripping)
                rows = this._codec.decodeToRows(p);
            } else {
                rows = asArray(p);
            }

            if (!rows || rows.length === 0) continue;

            const target = batch.payloads[action];
            for (let i = 0; i < rows.length; i++) {
                target.push(rows[i]);
            }
            appended += rows.length;
        }

        if (appended === 0) return Promise.resolve(null);

        batch.size += appended;
        batch.bytes += this._estimateSize(payloads);

        // Create a deferred promise for this add() caller
        const deferred = this._createDeferred();
        batch.promises.push(deferred);

        // --- Size-based flush triggers ---
        const sizeExceeded = this.maxBatchSize > 0 && batch.size >= this.maxBatchSize;
        const bytesExceeded = this.maxBatchBytes > 0 && batch.bytes >= this.maxBatchBytes;

        if (sizeExceeded || bytesExceeded) {
            if (targetMap === this._batches) {
                this._flush(batchKey).catch(e => {
                    console.error('Flush error:', e);
                });
            }
        } else {
            // --- Sliding-window timer: reset on every add ---
            if (targetMap === this._batches) {
                this._resetTimer(batchKey);
            }
        }

        return deferred.promise;
    }

    /**
     * Flush all pending batches immediately.
     * @returns {Promise<Array>}
     */
    async flushAll() {
        const keys = Array.from(this._batches.keys());
        if (keys.length === 0) return [];
        const tasks = new Array(keys.length);
        for (let i = 0; i < keys.length; i++) {
            tasks[i] = this._flush(keys[i]);
        }
        return Promise.all(tasks);
    }

    /**
     * Destroy the batcher. Rejects all outstanding promises.
     */
    async destroy() {
        this._isDestroyed = true;

        for (const t of this._timers.values()) clearTimeout(t);
        this._timers.clear();

        const inFlightPromises = [...this._inFlight.values()].map(p => p.catch(() => {}));
        if (inFlightPromises.length > 0) {
            await Promise.all(inFlightPromises);
        }

        const destroyError = new Error('PayloadBatcher destroyed');

        for (const batch of this._batches.values()) {
            this._rejectBatchPromises(batch, destroyError);
        }
        this._batches.clear();

        for (const batch of this._pendingBehind.values()) {
            this._rejectBatchPromises(batch, destroyError);
        }
        this._pendingBehind.clear();

        this._inFlight.clear();
    }

    /**
     * Decode a columnar payload back to rows.
     * @param {object} columnarData
     * @returns {object[]|object}
     */
    static fromColumnar(columnarData) {
        if (!columnarData || typeof columnarData !== 'object') return columnarData;
        const fmt = columnarData._format;
        if (fmt === 'empty') return [];
        if (fmt === 'single') return [columnarData._data];
        if (fmt === 'columnar') {
            const codec = new OptimizedColumnarCodec();
            return codec.decodeToRows(columnarData);
        }
        return columnarData;
    }

    getStats() {
        return { ...this._stats };
    }

    // -------------------------------------------------------------------------
    // Internal: Timer management
    // -------------------------------------------------------------------------

    /**
     * Sliding-window timer: clear any existing timer for this key and set a new
     * one. Every call to add() resets the window so rapid bursts are fully
     * coalesced.
     */
    _resetTimer(batchKey) {
        const existing = this._timers.get(batchKey);
        if (existing !== undefined) {
            clearTimeout(existing);
        }
        const t = setTimeout(() => {
            this._timers.delete(batchKey);
            this._flush(batchKey).catch(e => {
                console.error('Flush error:', e);
            });
        }, this.batchWindow);
        this._timers.set(batchKey, t);
    }

    // -------------------------------------------------------------------------
    // Internal: Flush pipeline
    // -------------------------------------------------------------------------

    /**
     * Flush the primary batch for `batchKey`.
     *
     * Backpressure: if there is already an in-flight flush for this key, we do
     * NOT start another one. Instead, the current items sit in _pendingBehind
     * and will be promoted once the in-flight flush completes.
     */
    async _flush(batchKey) {
        if (this._isDestroyed) return null;

        let lastResult = null;
        let continueFlush = true;

        while (continueFlush) {
            continueFlush = false;

            if (this._isDestroyed) return lastResult;

            const timer = this._timers.get(batchKey);
            if (timer !== undefined) {
                clearTimeout(timer);
                this._timers.delete(batchKey);
            }

            const batch = this._batches.get(batchKey);
            if (!batch) return lastResult;

            this._batches.delete(batchKey);

            if (batch.size === 0) {
                this._resolveBatchPromises(batch, null);
                return lastResult;
            }

            const flightPromise = this._doFlush(batchKey, batch);
            this._inFlight.set(batchKey, flightPromise);

            try {
                lastResult = await flightPromise;
            } finally {
                this._inFlight.delete(batchKey);
            }

            if (this._pendingBehind.has(batchKey)) {
                const behind = this._pendingBehind.get(batchKey);
                this._pendingBehind.delete(batchKey);
                this._batches.set(batchKey, behind);

                const sizeExceeded = this.maxBatchSize > 0 && behind.size >= this.maxBatchSize;
                const bytesExceeded = this.maxBatchBytes > 0 && behind.bytes >= this.maxBatchBytes;
                if (sizeExceeded || bytesExceeded) {
                    continueFlush = true;
                } else {
                    this._resetTimer(batchKey);
                }
            }
        }

        return lastResult;
    }

    /**
     * Perform the actual flush: optimize the batch and call onFlush.
     * Resolves or rejects all promises associated with the batch.
     */
    async _doFlush(batchKey, batch) {
        try {
            if (this._isDestroyed) {
                throw new Error('PayloadBatcher destroyed before flush');
            }
            const optimizedPayload = this._optimizeBatch(batch);

            if (DEBUG_MODE) this._updateStats(batch, optimizedPayload);

            const result = await this.onFlush(optimizedPayload);
            // Check again after the async gap -- destroy() may have been called
            if (this._isDestroyed) {
                this._rejectBatchPromises(batch, new Error('PayloadBatcher destroyed during flush'));
                return;
            }
            this._resolveBatchPromises(batch, result);
            return result;
        } catch (err) {
            this._rejectBatchPromises(batch, err);
            throw err;
        }
    }

    // -------------------------------------------------------------------------
    // Internal: Batch optimization (dedup + columnar encode)
    // -------------------------------------------------------------------------

    /**
     * Dedup by primary key (if available) and convert to columnar.
     * Dedup is done ONCE here, not on every add().
     */
    _optimizeBatch(batch) {
        const { payloads } = batch;
        const out = {};
        const pk = batch.context.primary_keys;

        for (const action of ['add', 'update', 'remove']) {
            let rows = payloads[action];
            if (!rows || rows.length === 0) continue;

            // Dedup by primary key at flush time
            if (pk && (!Array.isArray(pk) || pk.length > 0)) {
                rows = this._deduplicateByPrimaryKey(rows, pk);
            }

            out[action] = this._toColumnar(rows);
        }

        return {
            context: batch.context,
            data: { payloads: out },
            options: { ...batch.options }
        };
    }

    _deduplicateByPrimaryKey(rows, primaryKeys) {
        const pkCols = Array.isArray(primaryKeys) ? primaryKeys : [primaryKeys];
        const seen = new Map();
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const key = JSON.stringify(pkCols.map(col => row[col]));
            const existing = seen.get(key);
            if (existing) {
                // Merge: later writes win per-column, but earlier columns are preserved
                Object.assign(existing, row);
            } else {
                seen.set(key, { ...row });
            }
        }
        return Array.from(seen.values());
    }

    // -------------------------------------------------------------------------
    // Internal: Helpers
    // -------------------------------------------------------------------------

    _computeBatchKey(context) {
        const room = context?.room || 'default';
        const gridId = context?.grid_id || 'default';
        return `${room}:${gridId}-batch`;
    }

    _createBatch(context, options) {
        return {
            context: { ...context },
            options: { ...options },
            payloads: { add: [], update: [], remove: [] },
            size: 0,
            bytes: 0,
            createdAt: Date.now(),
            promises: []       // Array of { resolve, reject, promise }
        };
    }

    _createDeferred() {
        let resolve, reject;
        const promise = new Promise((res, rej) => {
            resolve = res;
            reject = rej;
        });
        return { resolve, reject, promise };
    }

    _resolveBatchPromises(batch, value) {
        const promises = batch.promises;
        for (let i = 0; i < promises.length; i++) {
            promises[i].resolve(value);
        }
        batch.promises = [];
    }

    _rejectBatchPromises(batch, error) {
        const promises = batch.promises;
        for (let i = 0; i < promises.length; i++) {
            promises[i].reject(error);
        }
        batch.promises = [];
    }

    _isColumnarPayload(payload) {
        return payload && typeof payload === 'object' && (
            payload?._format ||
            payload?.add?._format || payload?.update?._format || payload?.remove?._format
        );
    }

    /**
     * Lightweight byte-size heuristic. Avoids JSON.stringify on every add().
     * Walks the payload tree shallowly and estimates based on value types.
     * No artificial cap (the old Math.min(2048, ...) is removed).
     */
    _estimateSize(obj) {
        if (obj === null || obj === undefined) return 0;
        const type = typeof obj;
        if (type === 'string') return obj.length * 2; // ~2 bytes per char (UTF-16 in JS)
        if (type === 'number') return 8;
        if (type === 'boolean') return 4;
        if (Array.isArray(obj)) {
            let total = 16; // array overhead
            // Sample-based estimation for large arrays
            const len = obj.length;
            if (len <= 32) {
                for (let i = 0; i < len; i++) {
                    total += this._estimateSize(obj[i]);
                }
            } else {
                // Sample first 16 + last 8 elements, then extrapolate
                let sampleTotal = 0;
                const sampleCount = 24;
                for (let i = 0; i < 16; i++) {
                    sampleTotal += this._estimateSize(obj[i]);
                }
                for (let i = len - 8; i < len; i++) {
                    sampleTotal += this._estimateSize(obj[i]);
                }
                total += (sampleTotal / sampleCount) * len;
            }
            return total;
        }
        if (type === 'object') {
            let total = 32; // object overhead
            const keys = Object.keys(obj);
            for (let i = 0; i < keys.length; i++) {
                total += keys[i].length * 2 + 8; // key name + separator overhead
                total += this._estimateSize(obj[keys[i]]);
            }
            return total;
        }
        return 16; // fallback for symbols, functions, etc.
    }

    _toColumnar(rows) {
        return this._codec._toColumnar(rows);
    }

    _updateStats(batch, optimizedPayload) {
        this._stats.batchesSent++;
        this._stats.payloadsBatched += batch.size;

        try {
            const originalSize = JSON.stringify(batch.payloads).length;
            const optimizedSize = JSON.stringify(optimizedPayload?.data?.payloads).length;
            const saved = Math.max(0, originalSize - optimizedSize);
            this._stats.bytesSaved += saved;
            this._stats.compressionRatio = originalSize > 0 ? optimizedSize / originalSize : 1;
        } catch {
            // stats are best-effort
        }
    }
}
