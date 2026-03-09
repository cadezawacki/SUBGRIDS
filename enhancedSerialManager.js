

import { OptimizedColumnarCodec } from '@/global/optimizedColumnarCodec.js';
import {ZstdInit} from "@oneidentity/zstd-js";
import {pack, unpack} from 'msgpackr';
import {HyperTable} from "@/utils/HyperTable";
import {isNumeric} from '@/utils/utility.js';
import {ACTION_MAP} from '@/global/actionMap.js';
import * as arrow from "apache-arrow";


export class SerialManager {
    constructor(context, options = {}) {
        this.context = context;
        this.compressLevel = options.compressLevel || 3;
        this.compression_threshold = options.compression_threshold || 1024; // 1KB
        this.ZstdSimple = null;
        this.ZstdStream = null;
        this._zstdReady = false;
        this._zstdPromise = this.initCompressionLibrary();
        this._columnarCodec = new OptimizedColumnarCodec();
        this._columnarThreshold = options.columnarThreshold || 10;
        this.textDecoder = new TextDecoder();

        this.init();
    }

    init() {}
    cleanup() {}

    async initCompressionLibrary() {
        const {ZstdSimple, ZstdStream} = await new Promise((resolve) => resolve(ZstdInit()))
        this.ZstdSimple = ZstdSimple;
        this.ZstdStream = ZstdStream;
        this._zstdReady = true;
    }


    async _ensureZstdReady() {
        if (!this._zstdReady) {
            await this._zstdPromise;
            return true
        }
        return false
    }

    serializeMessage(message) {
        const enc = this.encode(message);
        try {
            if (!this._zstdReady) {
                console.error('zstd NOT READY.')
                return enc;
            }
            const comp = this.ZstdStream.compress(enc, this.compressLevel);
            return comp
        } catch(e) {
            console.error(e)
            return enc;
        }
    }

    deserializeMessage(message) {
        return Promise.resolve(message).then(async (message) => {
            let payload;
            payload = this.isMessageEvent(message) ? message.data : message;
            if (!payload) throw Error(`Invalid message: ${message}`);
            const decompressed = await this.decompress(payload);
            return await this.decode(decompressed);
        }).then(msg=>{console.log(msg); return msg}).catch((e) => {console.error(e)});
    }

    async decompress(payload) {
        if (!this._zstdReady) {
            await this._zstdPromise;
        }
        const isZ = this.isZstd(payload);

        if (!isZ && this.isArrayBuffer(payload)) {
            const _payload =  new Uint8Array(payload)
            if (this.isZstd(_payload)) return this.ZstdStream.decompress(_payload)
        }

        return isZ ? this.ZstdStream.decompress(payload) : payload
    }

    compress(payload, level=this.compressLevel) {
        if (payload.length < 100 || !this._zstdReady) return payload;
        return this.ZstdStream.compress(payload, level);
    }

    encode(payload) {
        return this.isArrowTable(payload) ? arrow.tableToIPC(payload) : pack(payload);
    }

    async loadArrowTable(message, bytes) {
        bytes = await this.decompress(bytes)
        const ht = new HyperTable(bytes);
        const meta = ht.metaData;
        const grid_filters = meta.grid_filters ? meta.grid_filters : message?.context?.grid_filters;
        const primary_keys = meta.primary_keys ? meta.primary_keys : message?.context?.primary_keys;
        const grid_id = meta.grid_id ?? message?.context?.grid_id;
        const room = meta.room ?? message?.context?.room;
        return {
            action: message?.action ?? "subscribe",
            status: { code: message?.status?.code ?? 200 },
            context: { room, grid_id, grid_filters, primary_keys },
            data: ht,
            trace: message?.trace
        };
    }

    async decode(payload) {
        let message;
        if (this.isMsgPack(payload)) {
            message = unpack(payload);
            // Arrow blob nested in MessagePack?
            if (message?.data && (message.data instanceof Uint8Array)) {
                message = await this.loadArrowTable(message, message.data);
            }
            // Flexible payloads: array rows, direct columnar, or { frame }
            else if (message?.data?.payloads) {
                const { payloads } = message.data;
                const toRows = (delta) => {
                    if (!delta) return [];
                    if (Array.isArray(delta)) return delta;                           // already rows
                    if (delta._format) return this._columnarCodec.decodeToRows(delta); // direct columnar
                    if (delta.frame?._format) return this._columnarCodec.decodeToRows(delta.frame); // {frame}
                    if (typeof delta === 'object') return [delta]
                    return []
                };
                for (const action of ['add', 'update', 'remove']) {
                    const chunk = payloads[action];
                    const parts = Array.isArray(chunk) ? chunk : [chunk];
                    const out = [];
                    for (let i = 0; i < parts.length; i++) out.push(...toRows(parts[i]));
                    payloads[action] = out;
                }
            }
        } else if (this.isArrayBuffer(payload) && this.isArrow(payload)) {
            message = await this.loadArrowTable({}, payload);
        } else {
            let text = payload;
            if (payload instanceof Uint8Array || this.isArrayBuffer(payload)) {
                const bytes = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
                text = this.textDecoder.decode(bytes);
            }
            console.warn('JSON fallback decode:', text);
            message = JSON.parse(text);
        }

        if (message.action != null) {
            message.action = isNumeric(message.action) ? ACTION_MAP.get(Number(message.action)) : message.action;
        }
        return message;
    }

    isArrayBuffer(value) {
        return value instanceof ArrayBuffer;
    }

    isArrowTable(value) {
        return value instanceof arrow.Table;
    }

    isMessageEvent(value) {
        return value instanceof MessageEvent;
    }

    isZstd(uint8Array) {
        if (uint8Array.length < 4) {
            return false;
        }
        return (
            uint8Array[0] === 0x28 &&
            uint8Array[1] === 0xB5 &&
            uint8Array[2] === 0x2F &&
            uint8Array[3] === 0xFD
        );
    }

    isMsgPack(arrayBuffer) {
        const a = this.isArrayBuffer(arrayBuffer) ? new Uint8Array(arrayBuffer) : arrayBuffer;
        const firstByte = a[0];
        return (
            (firstByte >= 0x80 && firstByte <= 0x8f) ||
            (firstByte >= 0x90 && firstByte <= 0x9f) ||
            (firstByte === 0xde || firstByte === 0xdf) ||
            (firstByte === 0xdc || firstByte === 0xdd)
        )
    }

    isArrow(arrayBuffer) {
        const bytes = this.isArrayBuffer(arrayBuffer) ? new Uint8Array(arrayBuffer) : arrayBuffer;
        if (bytes.length < 6) return false;
        const arrowMagic = new TextDecoder().decode(bytes.slice(0, 6));
        if (arrowMagic === 'ARROW1') return true;
        if (bytes.length < 4) return false;
        return 0xFFFFFFFF === new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true);
    }

    static detectFormat(bytes) {
        if (bytes.length < 8) return 'unknown';

        // Check for Arrow File format
        const arrowMagic = new TextDecoder().decode(bytes.slice(0, 6));
        if (arrowMagic === 'ARROW1') {
            return 'arrow-file';
        }

        const first4Bytes = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true);
        if (first4Bytes === 0xFFFFFFFF) {
            return 'arrow-stream';
        }

        // Check for MessagePack patterns
        const firstByte = bytes[0];
        if (
            (firstByte >= 0x80 && firstByte <= 0x8f) ||
            (firstByte >= 0x90 && firstByte <= 0x9f) ||
            (firstByte === 0xde || firstByte === 0xdf) ||
            (firstByte === 0xdc || firstByte === 0xdd)
        ) {
            return 'messagepack';
        }
        return null;
    }
}
