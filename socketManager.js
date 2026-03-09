



import { webSocket } from 'rxjs/webSocket';
import {
    Subject, first, retry, delay, map, takeUntil,
    share, tap, switchMap, of, catchError, from, take, EMPTY,
    BehaviorSubject, filter, timer, distinctUntilChanged, combineLatest,
} from 'rxjs';
import {v4 as uuidv4} from "uuid";
import { ACTION_MAP } from '@/global/actionMap.js';
import { DisconnectOverlay } from '@/global/disconnectOverlay.js';
import { InactivityMonitor } from '@/global/inactivityMonitor.js';


const MAX_MESSAGE_RETRIES = 3;

/**
 * OfflineEditQueue – bounded, deduplicated queue for offline edits.
 *
 * - Per-cell last-write-wins deduplication for publish/update payloads
 *   (keyed by `action + ":" + payload.data?.cell_id` when present).
 * - Bounded to `maxSize` entries; when full the oldest non-transactional
 *   message is evicted (backpressure).
 * - Persisted to localStorage under `__offline_edit_queue` so edits
 *   survive page refreshes while offline.
 */
class OfflineEditQueue {
    constructor(maxSize = 2000) {
        this.maxSize = maxSize;
        /** @type {Map<string, object>} insertion-ordered, dedup key -> payload */
        this._queue = new Map();
        this._persistTimer = null;
        this._uniqueCounter = 0;
        this._storageKey = '__offline_edit_queue';
        this._restore();
    }

    /**
     * Build a dedup key.  For cell-level publish/update messages we key
     * on action + cell_id so only the latest edit per cell is kept.
     * Everything else gets a unique key (no dedup).
     */
    _keyFor(payload) {
        if (payload?._offlineKey) {
            return payload._offlineKey;
        }
        const cellId = payload?.data?.cell_id;
        const action = payload?.action;
        const isEditAction =
            typeof action === 'string'
                ? ['publish', 'update'].includes(action)
                : typeof action === 'number'
                    ? ['publish', 'update'].includes(ACTION_MAP.get(action))
                    : false;
        if (cellId && isEditAction) {
            return `${action}:${cellId}`;
        }
        return `__unique_${++this._uniqueCounter}`;
    }

    /** Returns true when the payload must not be evicted for backpressure. */
    _isTransactional(payload) {
        const action = payload?.action;
        if (typeof action === 'string') {
            return ['identify', 'subscribe', 'unsubscribe', 'disconnect'].includes(action);
        }
        if (typeof action === 'number') {
            const name = ACTION_MAP.get(action);
            return ['identify', 'subscribe', 'unsubscribe', 'disconnect'].includes(name);
        }
        return false;
    }

    /**
     * Enqueue a payload.  Deduplicates cell-level edits (last-write-wins)
     * and enforces the bounded size limit by dropping the oldest
     * non-transactional message when the queue is full.
     */
    enqueue(payload) {
        const key = this._keyFor(payload);
        payload._offlineKey = key;

        if (!payload._retryCount) {
            payload._retryCount = 0;
        }

        if (this._queue.has(key)) {
            this._queue.delete(key);
        }

        while (this._queue.size >= this.maxSize) {
            let evicted = false;
            for (const [k, v] of this._queue) {
                if (!this._isTransactional(v)) {
                    this._queue.delete(k);
                    evicted = true;
                    break;
                }
            }
            if (!evicted) {
                const firstKey = this._queue.keys().next().value;
                this._queue.delete(firstKey);
            }
        }

        this._queue.set(key, payload);
        this._schedulePersist();
    }

    /** Peek at the first queued payload without removing it. */
    peek() {
        const first = this._queue.values().next();
        return first.done ? undefined : first.value;
    }

    /** Remove and return the first queued payload. */
    dequeue() {
        const firstEntry = this._queue.entries().next();
        if (firstEntry.done) return undefined;
        const [key, value] = firstEntry.value;
        this._queue.delete(key);
        this._schedulePersist();
        return value;
    }

    /** Number of queued payloads. */
    size() {
        return this._queue.size;
    }

    /** Remove all entries and clear storage. */
    clear() {
        this._queue.clear();
        this._clearStorage();
    }

    /** True when there is at least one queued edit. */
    hasEdits() {
        return this._queue.size > 0;
    }

    _schedulePersist() {
        if (this._persistTimer) return;
        this._persistTimer = setTimeout(() => {
            this._persistTimer = null;
            this.persist();
        }, 100);
    }

    /** Write current queue to localStorage (call directly for immediate flush). */
    persist() {
        if (this._persistTimer) {
            clearTimeout(this._persistTimer);
            this._persistTimer = null;
        }
        try {
            const serializable = Array.from(this._queue.entries());
            localStorage.setItem(this._storageKey, JSON.stringify(serializable));
        } catch (e) {
            console.error('OfflineEditQueue persist failed:', e);
        }
    }

    /** Restore queue from localStorage (called on construction). */
    _restore() {
        try {
            const raw = localStorage.getItem(this._storageKey);
            if (raw) {
                const entries = JSON.parse(raw);
                if (Array.isArray(entries)) {
                    for (const [key, value] of entries) {
                        this._queue.set(key, value);
                    }
                }
            }
        } catch (e) {
            console.error('Corrupt offline queue:', e);
            localStorage.removeItem(this._storageKey);
        }
    }

    _clearStorage() {
        try {
            localStorage.removeItem(this._storageKey);
        } catch (_) {}
    }
}

// -----------------------------------------------------------

export class SocketManager {
    constructor(context, options = {}) {
        this.context = context;

        this.wsUrl = '';
        this.WEBSOCKET_ENDPOINT = options.endpoint || "/ws/";
        this.MAX_CONNECTION_RETRIES = options.maxRetries || 15;
        this.RECONNECT_BASE_DELAY = options.reconnectDelay || 2000;
        this.HEARTBEAT_INTERVAL = options.heartbeatInterval || 30000;
        this.LONG_RECONNECT_DELAY = options.longReconnectDelay || 30000;

        this.offlineQueue = new OfflineEditQueue();
        this.isProcessingBuffer = false;
        this._drainAgain = false;
        this.connection$ = this.context.page.createSharedStore('connection', {
            status: false,
            mnemonic: 'disconnected',
            latency: 0
        });

        this.userProfile$ = this.context.page.createSharedStore('userProfile', {},
            {persist: 'local', storageKey: 'userProfile'});

        this.socket$ = null;
        this.messages$ = null;
        this._transportReady = false;
        this._messagesSubscription = null;
        this._connectPromise = null;
        this._heartbeatTimer = null;
        this._lastHeartbeat = Date.now();

        this.connectionRetryCount = 0;
        this._hasEverDisconnected = false;

        this.toastElement = null;
        this.currentToastType = null;

        this._disconnectOverlay = new DisconnectOverlay();
        this._inactiveDisconnected = false;
        this._inactivityMonitor = new InactivityMonitor({
            timeout: options.inactivityTimeout ?? (60 * 60 * 1000),
            actionTypes: options.inactivityActionTypes ?? null,
            onInactive: () => this._onInactivityTimeout()
        });

        this.pendingRequests = new Map();

        this.onMessageError = null;

        this.metrics = {
            messagesSent: 0,
            messagesReceived: 0,
            bytesOut: 0,
            bytesIn: 0,
            latencyHistory: []
        };
        this.init();
    }

    init() {
        this._setupConnectionMonitoring();
        this._setupSubscriptions();
    }

    async cleanup(include_overlay=false) {
        this._rejectAllPendingRequests('SocketManager cleanup');
        if (this._messagesSubscription) {
            this._messagesSubscription.unsubscribe();
            this._messagesSubscription = null;
        }
        if (this._connectionStatusSubscription) {
            this._connectionStatusSubscription.unsubscribe();
            this._connectionStatusSubscription = null;
        }
        await this.disconnectWebSocket(1001, "Client Initiated Cleanup");
        if (this._inactivityMonitor) {
            this._inactivityMonitor.destroy();
            this._inactivityMonitor = null;
        }
        if (include_overlay && this._disconnectOverlay) {
            this._disconnectOverlay.destroy();
            this._disconnectOverlay = null;
        }
    }

    /**
     * Reject every in-flight sendRequest promise and clear their timeouts.
     */
    _rejectAllPendingRequests(reason) {
        for (const [id, entry] of this.pendingRequests) {
            clearTimeout(entry.request_timeout);
            entry.reject(new Error(reason));
        }
        this.pendingRequests.clear();
    }

    // --- Connection Lifecycle ---
    connectWebSocket() {
        if (this._connectPromise) return this._connectPromise;
        this._connectPromise = this._doConnect().finally(() => {
            this._connectPromise = null;
        });
        return this._connectPromise;
    }

    async _doConnect() {
        try {
            const userManager = this.context.page.userManager();
            if (!userManager) throw new Error('No user manager found.');
            if (this.socket$) {
                try { this.socket$.complete({code: 1001, reason: "Duplicate instances, reconnecting"}); } catch (e) {}
                this.socket$ = null;
                this.messages$ = null;
            }

            const fingerprint = userManager.fingerprint;
            const sessionFingerprint = userManager.session_fp;

            if (!fingerprint || !sessionFingerprint) {
                this.connection$.merge({
                    'status': false,
                    'mnemonic': 'disconnected',
                    'error': 'Missing user identity'
                });
                return;
            }

            this.wsUrl = this._buildWebSocketUrl(fingerprint, sessionFingerprint);
            this.connection$.set('mnemonic', 'connecting');

            this.socket$ = webSocket({
                url: this.wsUrl,
                binaryType: "arraybuffer",

                openObserver: {
                    next: () => {
                        this.connectionRetryCount = 0;
                        this._transportReady = true;
                        this.connection$.merge({
                            'status': true,
                            'mnemonic': 'connected',
                            'connectedAt': Date.now()
                        });
                        this._onConnected();
                    }
                },

                closeObserver: {
                    next: (event) => {
                        this._transportReady = false;
                        this.connection$.merge({
                            'status': false,
                            'mnemonic': 'disconnected',
                            'disconnectedAt': Date.now(),
                            'closeCode': event.code,
                            'closeReason': event.reason
                        });
                        this._rejectAllPendingRequests(
                            `WebSocket closed: ${event.code} ${event.reason || ''}`
                        );
                    }
                },

                serializer: payload => this.context.page.serialManager().serializeMessage(payload),
                deserializer: payload => this.context.page.serialManager().deserializeMessage(payload),

            });

            this.messages$ = this.socket$.pipe(
                tap({
                    error: err => this._handleConnectionError(err),
                    complete: () => {
                        this.socket$ = null;
                        this.messages$ = null;
                    }
                }),
                retry({
                    count: this.MAX_CONNECTION_RETRIES,
                    delay: (error, retryCount) => this._calculateRetryDelay(retryCount)
                }),
                share()
            );

            if (this._messagesSubscription) {
                this._messagesSubscription.unsubscribe();
            }

            this._messagesSubscription = this.messages$.subscribe({
                next: (msg) => {
                    this.handleWebSocketMessage(msg).catch(err => {
                        console.error('Error handling WebSocket message:', err);
                        if (this.onMessageError) {
                            this.onMessageError(err, msg);
                        }
                    });
                },
                error: (err) => {
                    this._transportReady = false;
                    this.connection$.merge({
                        'status': false,
                        'mnemonic': 'disconnected',
                        'error': err.message
                    });
                    this.socket$ = null;
                    this.messages$ = null;

                    this._scheduleLongReconnect();
                }
            });

        } catch (error) {
            this._transportReady = false;
            this.socket$ = null;
            this.messages$ = null;
            this.connection$.merge({
                'status': false,
                'mnemonic': 'disconnected',
                'error': error.message
            });
            throw error;
        }
    }

    async disconnectWebSocket(code=1001, reason="Client Initiated") {

        if (this.socket$) {
            try {
                await this._sendWebSocketMessage({
                    action: 'disconnect',
                    reason: reason,
                    code: code
                }, {wait: true});
            } catch (e) {}

            this._transportReady = false;
            this.socket$.complete({code: code, reason: reason});
            this.socket$ = null;
            this.messages$ = null;
        }

        this._rejectAllPendingRequests('WebSocket disconnected');

        if (this._messagesSubscription) {
            this._messagesSubscription.unsubscribe();
            this._messagesSubscription = null;
        }

        this.offlineQueue.persist();
    }

    getConnectionStatus() {
        return this.connection$.getValue().get('status');
    }

    // --- Message Handling ---
    _isSocketOpen() {
        return !!(this.socket$ && this._transportReady);
    }

    async _sendWebSocketMessage(payload, {wait=false, timeout=5000}={}) {
        if (this._inactivityMonitor) {
            const actionName = typeof payload.action === 'number'
                ? ACTION_MAP.get(payload.action)
                : payload.action;
            this._inactivityMonitor.recordOutboundMessage(actionName);
        }

        if (!this._isSocketOpen()) {
            this.offlineQueue.enqueue(payload);
            return;
        }
        try {
            payload.trace = payload.traceId ? payload.traceId : this._generateTraceId();
            payload.context = payload?.context ? payload.context : {};
            payload.user = payload.user ? payload.user : this.userProfile$.asObject();

            if (wait) {
                const message = await this.sendRequest(payload, timeout);
                if (!this?.context?.page?.subscriptionManager()) {
                    throw new Error("MISSING SUBSCRIPTION MANAGER");
                }
                await this.context.page.subscriptionManager().messageRouter(message);
                return message;
            }

            if (!this._isSocketOpen()) {
                this.offlineQueue.enqueue(payload);
                return;
            }

            try {
                this.socket$.next(payload);
            } catch (e) {
                this.offlineQueue.enqueue(payload);
            }

            return;
        } catch (error) {
            console.error("Error sending message:", error);
            throw error;
        }
    }

    sendRequest(payload, timeout=5000) {
        if (!this._isSocketOpen()) {
            return Promise.reject(new Error('Cannot send request: socket is disconnected'));
        }

        const pendingRequests = this.pendingRequests;
        const id = payload?.trace ?? this._generateTraceId();

        return new Promise((resolve, reject) => {

            const request_timeout = setTimeout(() => {
                if (pendingRequests.has(id)) {
                    pendingRequests.delete(id);
                    reject(new Error(`Request timed out: ${id}`));
                }
            }, timeout);

            pendingRequests.set(id, { resolve, reject, request_timeout });

            try {
                this.socket$.next(payload);
            } catch (e) {
                pendingRequests.delete(id);
                clearTimeout(request_timeout);
                reject(e);
            }
        });
    }

    // ═══════════════════════════════════════════════════════════════
    // Connection Helpers
    // ═══════════════════════════════════════════════════════════════

    _buildWebSocketUrl(fingerprint, sessionFingerprint) {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.host;
        const params = new URLSearchParams({
            fp: fingerprint,
            sfp: sessionFingerprint
        });

        return `${protocol}//${host}${this.WEBSOCKET_ENDPOINT}?${params}`;
    }

    async _onConnected() {
        try {
            await this.identifyUser();
        } catch (err) {
            console.error("identifyUser failed, forcing reconnect:", err);
            try { this.socket$.error(err); } catch (e) {
                console.error(e)
            }
            return;
        }

        try {
            await this.context.page.subscriptionManager().replayKnownRooms({wait:true, rateLimitMs: 200});
        } catch (err) {
            console.error("replayKnownRooms failed:", err);
        }

        try {
            await this._processMessageBuffer(100);
        } catch (err) {
            console.error("processMessageBuffer failed:", err);
        }
    }

    async identifyUser() {

        if (this.userProfile$.size() === 0) {
            throw new Error("Cannot identify: empty user profile");
        }

        await this._sendWebSocketMessage({
            action: ACTION_MAP.get("identify"),
            user: this.userProfile$.asObject()
        }, {wait:true});
    }

    async _processMessageBuffer(rateLimit=0) {
        if (this.isProcessingBuffer || this.offlineQueue.size() === 0) return;
        this.isProcessingBuffer = true;
        try {
            while (this.offlineQueue.size() > 0) {
                const msg = this.offlineQueue.dequeue();
                if (!msg) break;
                try {
                    await this._sendWebSocketMessage(msg);
                    if (rateLimit > 0) {
                        await new Promise(r => setTimeout(r, rateLimit));
                    }
                } catch (e) {
                    console.error("Failed to send buffered message:", e);
                }
            }
        } finally {
            this.isProcessingBuffer = false;
        }
    }

    _scheduleLongReconnect() {
        setTimeout(() => {
            this.connectionRetryCount = 0;
            this.connectWebSocket().catch(err => {
                console.error('Long-delay reconnect failed:', err);
            });
        }, this.LONG_RECONNECT_DELAY);
    }

    _calculateRetryDelay(retryCount) {
        if (this.connectionRetryCount === 0) {
            this.context.page.subscriptionManager?.()?.captureKnownRooms?.();
        }

        this.connectionRetryCount = retryCount + 1;

        this.connection$.merge({
            'status': false,
            'mnemonic': 'reconnecting',
            'retryCount': this.connectionRetryCount
        });

        if (this.connectionRetryCount >= this.MAX_CONNECTION_RETRIES) {
            this.connection$.merge({
                'status': false,
                'mnemonic': 'disconnected',
                'error': 'Max retries exceeded'
            });
            this.messages$ = null;

            this._scheduleLongReconnect();

            return EMPTY;
        }

        const baseDelay = Math.min(30000, this.RECONNECT_BASE_DELAY * Math.pow(2, Math.floor(retryCount / 3)));
        const jitter = Math.random() * 0.3 * baseDelay;
        const delayMs = baseDelay + jitter;

        return timer(delayMs);
    }

    async handleWebSocketMessage(message) {
        message = await message;
        message.status = message?.status ?? {code: 200};
        message.status.success = message?.status?.code === 200;
        const id = message?.trace;
        if (id && this.pendingRequests.has(id)) {
            const entry = this.pendingRequests.get(id);
            if (!entry) return;
            const { resolve, reject, request_timeout } = entry;
            clearTimeout(request_timeout);
            this.pendingRequests.delete(id);

            if (message.error || (message.status?.code >= 400)) {
                reject(new Error(message.error || `Server error: ${message.status.code}`));
            } else {
                resolve(message);
            }
            return;
        }
        return await this.context.page.subscriptionManager().messageRouter(message);
    }

    _handleConnectionError(error) {
        console.error("WebSocket error:", error);

        if (error.code === 1006) {
            this.connection$.merge({
                'error': 'Connection lost unexpectedly'
            });
        } else if (error.code === 1008) {
            this.connection$.merge({
                'error': 'Authentication failed'
            });
        }
    }


    // ═══════════════════════════════════════════════════════════════
    // Inactivity Handling
    // ═══════════════════════════════════════════════════════════════

    async _onInactivityTimeout() {
        this._inactiveDisconnected = true;
        this._inactivityMonitor.stop();

        try {
            const subManager = this.context.page.subscriptionManager();
            if (subManager) {
                const rooms = Array.from(subManager.rooms$.get('subscribed') || new Set());
                for (const room of rooms) {
                    try { await subManager.unsubscribeFromRoom(room); } catch (_) {}
                }
            }
        } catch (_) {}

        try { await this.disconnectWebSocket(1000, "Inactive Closure"); } catch (_) {}

        try {
            if (this.context?.page?.cleanup) {
                await this.context.page.cleanup();
            }
        } catch (_) {}
        try {
            if (this.context?.frame?.cleanup) {
                await this.context.frame.cleanup();
            }
        } catch (_) {}

        this._disconnectOverlay.showInactivityModal();
    }

    // ═══════════════════════════════════════════════════════════════
    // Utility Methods
    // ═══════════════════════════════════════════════════════════════

    _generateTraceId() {
        return uuidv4();
    }

    // ═══════════════════════════════════════════════════════════════
    // Toast & UI Notifications
    // ═══════════════════════════════════════════════════════════════

    _setupSubscriptions() {
        this._connectionStatusSubscription = this.connection$.subject.pipe(
            map(socket => socket.get('status')),
            takeUntil(this.context.destroy$)
        ).subscribe(v => this.isConnected = v);
    }

    _setupConnectionMonitoring() {
        this.connection$.onValueAddedOrChanged('mnemonic', (newValue, oldValue) => {
            const value = newValue;

            if (value === 'connected') {
                this._disconnectOverlay.hideAll();
                this._inactiveDisconnected = false;

                if (this._inactivityMonitor && !this._inactivityMonitor._active) {
                    this._inactivityMonitor.start();
                }

                if (this._hasEverDisconnected) {
                    if (this.offlineQueue.hasEdits()) {
                        this._showConnectionToast('connected',
                            `Reconnected. ${this.offlineQueue.size()} pending edit${this.offlineQueue.size() === 1 ? '' : 's'} will be replayed.`);
                    } else {
                        this._showConnectionToast('connected');
                    }
                    this.connectionRetryCount = 0;
                    this._hasEverDisconnected = false;
                } else {
                    this.connectionRetryCount = 0;
                }
            } else if (value === 'reconnecting') {
                this._hasEverDisconnected = true;
                if (this.connectionRetryCount < this.MAX_CONNECTION_RETRIES) {
                    this._showConnectionToast('reconnecting');
                }
            } else if (value === 'disconnected') {
                this._hasEverDisconnected = true;
                if (this.connectionRetryCount >= this.MAX_CONNECTION_RETRIES) {
                    this._showConnectionToast('disconnected');
                    this._disconnectOverlay.showOverlay(false);
                }
            }
        });
    }

    _showConnectionToast(type, customMessage) {
        if (this.currentToastType === type) return;

        if (this.toastElement) {
            try {
                this.toastElement.remove();
            } catch (e) {}
            this.toastElement = null;
        }

        switch (type) {
            case 'reconnecting':
                this.context.page.toastManager().create(
                    'warning',
                    'Reconnecting',
                    'Attempting to reconnect... Note: Offline edits MAY re-sync, please double check once back online!',
                    {
                        toastId: 'broadcast-connection',
                        permanent: true,
                        updateOnExist: true
                    }
                );
                break;

            case 'disconnected':
                this.context.page.toastManager().create(
                    'error',
                    'Disconnected',
                    'Connection lost. Please refresh the page.',
                    {
                        toastId: 'broadcast-connection',
                        permanent: true,
                        updateOnExist: true
                    }
                );
                break;

            case 'connected':
                this.context.page.toastManager().create(
                    'success',
                    'Connected',
                    customMessage || 'Successfully reconnected!',
                    {
                        toastId: 'broadcast-connection',
                        persist: false,
                        permanent: false,
                        updateOnExist: true,
                        options: {
                            timeOut: 2500,
                            extendedTimeout: 500,
                            tapToDismiss: true
                        }
                    }
                );
                break;
        }

        this.currentToastType = type;
    }

    _formatBytes(bytes) {
        if (bytes < 1024) return `${bytes} B`;
        if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
        return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
    }
}
