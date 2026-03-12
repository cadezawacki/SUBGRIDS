


import { v4 as uuidv4 } from 'uuid';
import { EnhancedPayloadBatcher } from '@/global/enhancedPayloadBatcher.js';
import * as ht from "@/utils/hyperTable.js";
import {asArray} from '@/utils/helpers.js';
window.HyperTable = ht.HyperTable;
import { Table } from 'apache-arrow';
import { ACTION_MAP } from '@/global/actionMap.js';

export class SubscriptionManager {
    constructor(context, options = {}) {
        this.context = context;

        this.rooms$ = this.context.page.createSharedStore('rooms');
        this.rooms$.set('subscribed', new Set());
        this._room_filters = new Map();
        this._knownRoomsOnReconnect = [];
        this._isReplaying = false;
        this._replayAgain = false;

        this._pendingSubscriptions = new Set();
        this._onReconnectCallbacks = [];

        this.messageHandlers = new Map();
        this.onHandlerError = options.onHandlerError || null;

        this.batchingEnabled = true;
        this.columnarEnabled = true;

        this.batchWindow = 250;

        if (this.batchingEnabled) {
            this.payloadBatcher = new EnhancedPayloadBatcher({
                batchWindow: this.batchWindow,
                maxBatchSize: 200,
                maxBatchBytes: options.maxBatchBytes || 1024 * 1024,
                compressionThreshold: options.compressionThreshold || 1024,
                enableColumnar: this.columnarEnabled,
                onFlush: this._sendBatchedPayload.bind(this)
            });
        } else {
            this.payloadBatcher = null;
        }

        this.socketManager = null;
        this.toastManager = this.context.toastManager;
    }

    getSocketManager() {
        if (this.socketManager) return this.socketManager;
        this.socketManager = this.context.page.socketManager();
        return this.socketManager;
    }

    _mergeRoomMeta(room, patch = {}) {
        const upperRoom = String(room || '').toUpperCase();
        if (!upperRoom) return null;

        const current = this._room_filters.get(upperRoom) || {};
        const next = { ...current };

        for (const [k, v] of Object.entries(patch || {})) {
            if (v !== undefined) next[k] = v;
        }

        this._room_filters.set(upperRoom, next);
        return next;
    }

    _cleanSendOptions(options = {}) {
        const out = { ...(options || {}) };
        delete out.force;
        delete out.resubscribe;
        return out;
    }

    captureKnownRooms() {
        const subs = this.rooms$.get('subscribed') || new Set();
        this._knownRoomsOnReconnect = Array.from(subs || []);
    }

    async replayKnownRooms(options = { wait: true, rateLimitMs: 40 }) {
        if (this._isReplaying) {
            this._replayAgain = true;
            return;
        }

        this._isReplaying = true;
        try {
            await this._doReplayKnownRooms(options);
        } finally {
            this._isReplaying = false;
            if (this._replayAgain) {
                this._replayAgain = false;
                await this.replayKnownRooms(options);
            }
        }
    }

    async _doReplayKnownRooms(options = { wait: true, rateLimitMs: 40 }) {
        const rateLimitMs = Number.isFinite(options?.rateLimitMs) ? options.rateLimitMs : 40;

        const desired = this.rooms$.get('subscribed') || new Set();
        const list = (Array.isArray(this._knownRoomsOnReconnect) && this._knownRoomsOnReconnect.length)
            ? this._knownRoomsOnReconnect
            : Array.from(desired);

        const seen = new Set();

        for (const room of list) {
            const upperRoom = String(room || '').toUpperCase();
            if (!upperRoom || seen.has(upperRoom)) continue;
            seen.add(upperRoom);

            if (!desired.has(upperRoom)) continue;

            try {
                const meta = this._room_filters.get(upperRoom) || {};
                const ctx = { ...(meta || {}) };

                console.debug('[reconnect] re-subscribing to', upperRoom, ctx);
                await this.subscribeToRoom(upperRoom, ctx, {force: true}, {wait: options.wait});
                if (rateLimitMs > 0) await new Promise(r => setTimeout(r, rateLimitMs));
            } catch (err) {
                console.error(`Failed to replay room ${upperRoom}:`, err);
            }
        }

        this._knownRoomsOnReconnect = [];

        // Fire reconnect callbacks (e.g. micro-grid resubscribe)
        for (const cb of this._onReconnectCallbacks) {
            try { await cb(); } catch (e) { console.error('[SubscriptionManager] reconnect callback error:', e); }
        }
    }

    /** Register a callback to be invoked after WebSocket reconnect replay. */
    onReconnect(callback) {
        this._onReconnectCallbacks.push(callback);
    }

    /** Remove a reconnect callback. */
    offReconnect(callback) {
        this._onReconnectCallbacks = this._onReconnectCallbacks.filter(cb => cb !== callback);
    }

    async messageRouter(message) {
        try {
            if (message?.action !== undefined) return await this.handleMessage(message);
            if (message instanceof Table) return await this.handleTable(message);
        } catch (e) {
            const action = message?.action ?? 'unknown';
            const room = message?.context?.room ?? 'unknown';
            console.error(`messageRouter error [action=${action}, room=${room}]:`, e);
            this.onHandlerError?.('messageRouter', room, action, e);
        }
    }

    async handleMessage(message) {
        const action = message?.action;

        switch (action) {
            case 'identify': await this._handleIdentifyResponse(message); break;
            case 'subscribe': await this._handleSubscribeResponse(message); break;
            case 'unsubscribe': await this._handleUnsubscribeResponse(message); break;
            case 'update_filter': await this._handleFilterUpdateResponse(message); break;
            case 'toast': await this._handleToastResponse(message); break;
            case 'error': await this._handleErrorMessage(message); break;
            case 'publish': await this._handlePublishMessage(message); break;
            case 'fetch_columns': break;
            case 'fetch_schema': break;
            case 'control': await this._handleControlMessage(message); break;
            case 'ack': this._handleAcknowledgment(message); break;
            case 'micro_publish':
            case 'micro_subscribe':
            case 'micro_unsubscribe':
                await this._handleMicroGridMessage(message);
                break;
            case 'redistribute': break;
            default: console.warn("Unknown action:", action, message);
        }

        const room = message?.context?.matched_pattern ?? message?.context?.room;
        if (room) await this._dispatchToHandlers(room, action, message);

        const toasts = asArray(message?.data?.toast);
        toasts.forEach(toast => {
            if (toast && toast?.message != null) {
                const opts = toast?.options ?? {};
                this.toastManager.create(
                    toast?.toastType || 'info',
                    toast?.title || '',
                    toast?.message,
                    {
                        toastId: toast?.toastId,
                        permanent: toast?.permanent ?? opts?.permanent ?? false,
                        persist: toast?.persist ?? opts?.persist ?? false,
                        updateOnExist: toast?.updateOnExist ?? opts?.updateOnExist ?? true,
                        link: toast?.link,
                        options: {
                            timeOut: toast?.options?.timeOut ?? message?.options?.timeOut ?? 2500,
                            extendedTimeout: toast?.options?.extendedTimeout ?? message?.options?.timeOut ?? 500,
                            tapToDismiss: toast?.options?.tapToDismiss ?? message?.options?.timeOut ?? true
                        }
                    }
                );
            }
        });
    }

    _handleErrorMessage(message) {
        console.error('Server error:', message);
        if (!message?.data?.toast) {
            const error_reason = message?.status?.reason ?? 'An error occurred'
            const error_code = message?.status?.code ?? 400;
            this.toastManager.create('error', `Server Error (${error_code})`, error_reason);
        }
    }

    _handleControlMessage(_message) {
        console.warn('CONTROL MESSAGE NOT IMPLEMENTED:', _message?.action);
    }

    async handleTable(_message) {
        console.warn('TABLE MESSAGE NOT IMPLEMENTED:', _message?.action);
    }

    async _dispatchToHandlers(room, action, message) {
        const upperRoom = String(room || '').toUpperCase();
        const lowerAction = typeof action === 'string' ? action.toLowerCase() : '';
        const roomHandlers = this.messageHandlers.get(upperRoom);
        if (!roomHandlers || !lowerAction || !roomHandlers.has(lowerAction)) {
            return;
        }

        const handlers = new Set(roomHandlers.get(lowerAction));
        for (const handler of handlers) {
            queueMicrotask(() => {
                try {
                    const result = handler(message);
                    if (result && typeof result.catch === 'function') {
                        result.catch(err => {
                            console.error(`Async handler error for ${upperRoom}:${lowerAction} [${handler.name || 'anonymous'}]:`, err);
                            this.onHandlerError?.(handler, upperRoom, lowerAction, err);
                        });
                    }
                } catch (err) {
                    console.error(`Handler error for ${upperRoom}:${lowerAction} [${handler.name || 'anonymous'}]:`, err);
                    this.onHandlerError?.(handler, upperRoom, lowerAction, err);
                }
            });
        }
    }

    registerMessageHandler(room, type, handler) {

        const upperRoom = room.toUpperCase();
        const lowerType = type.toLowerCase();

        if (!upperRoom || !lowerType || typeof handler !== 'function') {
            console.error(`Cannot register handler for ${room}:${type}`)
            return;
        }

        if (!this.messageHandlers.has(upperRoom)) {
            this.messageHandlers.set(upperRoom, new Map());
        }
        const roomHandlers = this.messageHandlers.get(upperRoom);

        if (!roomHandlers.has(lowerType)) {
            roomHandlers.set(lowerType, new Set());
        }
        const typeHandlers = roomHandlers.get(lowerType);
        if (typeHandlers.has(handler)) {
            console.warn(`overriding message handler: ${room}, ${type}`)
        }
        typeHandlers.add(handler);
    }

    unregisterMessageHandler(room, type, handler) {
        const upperRoom = room?.toUpperCase();
        const lowerType = type?.toLowerCase();
        if (!upperRoom || !lowerType || typeof handler !== 'function') return;

        if (!this.messageHandlers.has(upperRoom)) return;
        const roomHandlers = this.messageHandlers.get(upperRoom);
        if (!roomHandlers.has(lowerType)) return;

        const typeHandlers = roomHandlers.get(lowerType);
        if (typeHandlers.has(handler)) {
            typeHandlers.delete(handler);
            if (typeHandlers.size === 0) {
                roomHandlers.delete(lowerType);
                if (roomHandlers.size === 0) this.messageHandlers.delete(upperRoom);
            }
        }
    }

    removeAllHandlersForRoom(room) {
        const upperRoom = room?.toUpperCase();
        if (upperRoom && this.messageHandlers.has(upperRoom)) {
            this.messageHandlers.delete(upperRoom);
        }
    }

    async updateSubscriptionFilter(room, context, options = {}, {wait=false}={}) {
        const upperRoom = String(room || '').toUpperCase();
        const socketManager = this.getSocketManager();
        if (!upperRoom) return Promise.reject('Invalid room name');
        if (!socketManager) return Promise.reject('SocketManager not available');

        this._mergeRoomMeta(upperRoom, context);

        const new_meta = { ...(context || {}), room: upperRoom };
        await socketManager._sendWebSocketMessage({
            action: ACTION_MAP.get("update_filter"),
            context: new_meta,
            options: this._cleanSendOptions(options)
        }, {wait:wait});
    }

    async subscribeToRoom(room, context = {}, options = {}, {wait=false}={}) {
        const upperRoom = String(room || '').toUpperCase();
        const socketManager = this.getSocketManager();
        if (!upperRoom) return Promise.reject('Invalid room name');
        if (!socketManager) return Promise.reject('SocketManager not available');

        this._mergeRoomMeta(upperRoom, context);
        const currentSubs = this.rooms$.get("subscribed") || new Set();
        const force = (options?.force === true) || (options?.resubscribe === true);

        if (currentSubs.has(upperRoom) && !force) {
            return Promise.resolve(true);
        }

        if (this._pendingSubscriptions.has(upperRoom) && !force) {
            return Promise.resolve(true);
        }

        this._pendingSubscriptions.add(upperRoom);

        const subscriptionTimeout = 30000;
        const timeoutId = setTimeout(() => {
            this._pendingSubscriptions.delete(upperRoom);
        }, subscriptionTimeout);

        try {
            await socketManager._sendWebSocketMessage({
                action: ACTION_MAP.get("subscribe"),
                context: { ...(context || {}), room: upperRoom },
                options: this._cleanSendOptions(options)
            }, {wait: wait});
        } catch (err) {
            clearTimeout(timeoutId);
            this._pendingSubscriptions.delete(upperRoom);
            throw err;
        }

        clearTimeout(timeoutId);
        return Promise.resolve();
    }

    async unsubscribeFromRoom(room, context = {}, options = {}) {
        const upperRoom = String(room || context?.room || '').toUpperCase();
        const socketManager = this.getSocketManager();
        if (!upperRoom) return Promise.reject('Invalid room name');
        if (!socketManager) return Promise.reject('SocketManager not available');

        const meta = this._room_filters.get(upperRoom) || {};
        const ctx = { ...(meta || {}), ...(context || {}), room: upperRoom };

        this.removeAllHandlersForRoom(upperRoom);
        this._pendingSubscriptions.delete(upperRoom);

        const subs = this.rooms$.get("subscribed") || new Set();
        if (subs.has(upperRoom)) {
            const nextSubs = new Set(subs);
            nextSubs.delete(upperRoom);
            this.rooms$.set('subscribed', nextSubs);
        }
        this._room_filters.delete(upperRoom);

        await socketManager._sendWebSocketMessage({
            action: ACTION_MAP.get("unsubscribe"),
            context: ctx,
            options: {}
        }, { wait: true });

        return Promise.resolve();
    }

    _handleToastResponse(message) {
    }

    _handleAcknowledgment(message) {

    }

    async _handleIdentifyResponse(message) {
        if (message?.status?.success) {
            const user = message.user;
            if (user == null) {
                console.error(`Identification failed: no user in message`);
                await this.context.page.socketManager().disconnectWebSocket(3000, "Identification Failure");
                return
            }
            const serverUser = new Map(Object.entries(user));
            serverUser.delete("sessionFingerprint");

            if (window.frame?.context?.pageInitializing) {
                window.frame.context.pageInitializing = false;
                requestAnimationFrame(() => {
                    window.request_page(window.page_name || 'homepage').catch(err => {
                        console.error("Error loading initial page:", err);
                        window.request_page('error');
                    });
                });
            }

        } else {
            console.error(`Identification failed: ${message?.status?.reason || 'Unknown reason'}`);
            await this.context.page.socketManager().disconnectWebSocket(3000, "Identification Failure");
        }
    }

    async _handleSubscribeResponse(message) {
        const room = message?.context?.room;
        if (!room) return;
        const upperRoom = room.toUpperCase();

        this._pendingSubscriptions.delete(upperRoom);

        if (!message?.status?.success) return;

        const currentSubs = this.rooms$.get("subscribed") || new Set();

        if (!currentSubs.has(upperRoom)) {
            const nextSubs = new Set(currentSubs);
            nextSubs.add(upperRoom);
            this.rooms$.set('subscribed', nextSubs);
        }

        this._mergeRoomMeta(upperRoom, {
            grid_id: message?.context?.grid_id,
            grid_filters: message?.context?.grid_filters,
            primary_keys: message?.context?.primary_keys,
            columns: message?.context?.columns,
        });
    }

    async _handleUnsubscribeResponse(message) {
        const room = message?.context?.room;
        if (!room) return;
        const upperRoom = room.toUpperCase();
        const currentSubs = this.rooms$.get("subscribed") || new Set();
        if (currentSubs.has(upperRoom)) {
            const nextSubs = new Set(currentSubs);
            nextSubs.delete(upperRoom);
            this.rooms$.set('subscribed', nextSubs);
            this._room_filters.delete(upperRoom);
        }
    }

    async _handleFilterUpdateResponse(message) {
        if (message?.status?.success) {
            const room = message?.context?.room?.toUpperCase();
            if (!room) return;

            this._mergeRoomMeta(room, {
                grid_id: message?.context?.grid_id,
                grid_filters: message?.context?.grid_filters,
                primary_keys: message?.context?.primary_keys,
                columns: message?.context?.columns,
            });
        }
    }

    buildPayloadContext(room, hyperMeta) {
        return {
            room:room,
            grid_id: hyperMeta.grid_id ?? room,
            grid_filters: hyperMeta.grid_filters ?? {},
            primary_keys: hyperMeta.primary_keys ?? [],
        };
    }

    buildPayloadData(room, hyperMeta, update) {
        return {
            payloads: { update: update }
        };
    }

    async publishMessage(room, context = {}, data = {}, options = {}) {
        await this.genericMessage(room, 'publish', context, data, options)
    }

    async genericMessage(room, action, context = {}, data = {}, options = {}) {
        const upperRoom = room?.toUpperCase();
        if (!upperRoom) return Promise.reject('Invalid room name');
        const socketManager = this.getSocketManager();

        const ctx = {...context, room: upperRoom};

        if (action !== 'publish') {
            const out = {
                action: ACTION_MAP.get(action),
                context: ctx,
                data: data,
                options: options,
                traceId: uuidv4()
            };
            const result = await socketManager._sendWebSocketMessage(out);
            if (result === false) {
                return Promise.reject(new Error(`Failed to send ${action} message to ${upperRoom}`));
            }
            return result;
        }

        return this.payloadBatcher.add(ctx, data, options);
    }

    _sendBatchedPayload(batchedPayload) {
        const socketManager = this.getSocketManager();
        const payload = {
            action: ACTION_MAP.get("publish"),
            context: batchedPayload.context,
            data: batchedPayload.data,
            options: batchedPayload.options,
            traceId: batchedPayload?.traceId ?? uuidv4(),
            user: this.context.page.userManager().user_data()
        };
        return socketManager._sendWebSocketMessage(payload)
                             .catch(e => {
            console.error('Batched payload send failed:', e);
            throw e;
        });
    }

    async _handlePublishMessage(message) {
        if (message?.status?.code === 400) return;
    }

    // -------------------------------------------------------------------------
    // Micro-grid message handling
    // -------------------------------------------------------------------------

    async _handleMicroGridMessage(message) {
        const action = message?.action;
        const microName = message?.micro_name;
        if (!microName) return;

        // Dispatch to registered room handlers using the micro topic as the room key
        const topic = `MICRO.${microName.toUpperCase()}`;
        const roomHandlers = this.messageHandlers.get(topic);
        if (roomHandlers) {
            const lowerAction = action.toLowerCase();
            const handlers = roomHandlers.get(lowerAction);
            if (handlers) {
                for (const handler of new Set(handlers)) {
                    queueMicrotask(() => {
                        try { handler(message); }
                        catch (e) { console.error(`[MicroGrid] Handler error (${microName}/${action}):`, e); }
                    });
                }
            }
        }
    }

    async microSubscribe(microName, {wait = false} = {}) {
        const socketManager = this.getSocketManager();
        if (!socketManager) return Promise.reject('SocketManager not available');
        return socketManager._sendWebSocketMessage({
            action: ACTION_MAP.get("micro_subscribe"),
            micro_name: microName,
        }, {wait});
    }

    async microUnsubscribe(microName) {
        const socketManager = this.getSocketManager();
        if (!socketManager) return Promise.reject('SocketManager not available');
        // Remove handlers for this micro-grid topic
        const topic = `MICRO.${microName.toUpperCase()}`;
        this.removeAllHandlersForRoom(topic);
        return socketManager._sendWebSocketMessage({
            action: ACTION_MAP.get("micro_unsubscribe"),
            micro_name: microName,
        }, {wait: false});
    }

    async microPublish(microName, payloads, {trace} = {}) {
        const socketManager = this.getSocketManager();
        if (!socketManager) return Promise.reject('SocketManager not available');
        return socketManager._sendWebSocketMessage({
            action: ACTION_MAP.get("micro_publish"),
            micro_name: microName,
            data: { payloads },
            trace: trace,
        }, {wait: false});
    }

    async flushAllBatches() {
        if (!this.payloadBatcher) return [];
        return this.payloadBatcher.flushAll();
    }

    async cleanup() {
        if (this.payloadBatcher) {
            await this.payloadBatcher.flushAll?.();
            await this.payloadBatcher.destroy();
            this.payloadBatcher = null;
        }
    }
}
