
import { BehaviorSubject, Subject, combineLatest, Subscription } from 'rxjs';
import {
    map,
    filter,
    shareReplay,
    debounceTime,
    throttleTime,
    bufferTime,
    bufferCount,
    distinctUntilChanged,
} from 'rxjs/operators';
import deepEqual from 'fast-deep-equal';

const _FROZEN_EMPTY_MAP = Object.freeze(new Map());
const _EMPTY_ARR = Object.freeze([]);
const EMPTY_CHANGE = Object.freeze({
    keys: _EMPTY_ARR,
    keySet: Object.freeze(new Set()),
    values: _EMPTY_ARR,
    entries: _EMPTY_ARR,
    map: _FROZEN_EMPTY_MAP
});

const viewSubscriptionRegistry = typeof FinalizationRegistry !== 'undefined'
    ? new FinalizationRegistry((sub) => {
        if (sub && typeof sub.unsubscribe === 'function') sub.unsubscribe();
    })
    : null;

export class ObservableDictionary {
    constructor(initialData = null, options = {}) {
        this._options = options;
        this._isDerivedView = options?.isDerivedView || options?.isMergedView || options?.isCombinedView || false;
        this._isMergedView = options?.isMergedView || false;
        this._isCombinedView = options?.isCombinedView || false;
        let _version;

        let internalValue = initialData;
        if (!this._isCombinedView) {
            internalValue = initialData instanceof Map
                ? initialData
                : new Map(Object.entries(initialData || {}));
            if (internalValue.has('_version_')) {
                _version = Number(internalValue.get('_version_'));
            }
        } else {
            internalValue = initialData || {};
        }

        if (!this._isDerivedView && options.persist && options.storageKey) {
            let storageData = new Map();
            try {
                let raw = null;
                try {
                    raw = (options.persist === 'local'
                            ? localStorage.getItem(options.storageKey)
                            : sessionStorage.getItem(options.storageKey)
                    );
                } catch (_) {}
                if (raw) {
                    const obj = JSON.parse(raw);
                    storageData = new Map(Object.entries(obj));
                } else {
                    storageData = internalValue;
                }
                const sv = storageData.get('_version_');
                if ((_version != null) && ((sv == null) || (Number(sv) < Number(_version)))) {
                    if (!this._isCombinedView && options.coalesceStorage) {
                        internalValue = new Map([...storageData, ...internalValue]);
                        ObservableDictionary.setStorageFromMap(internalValue, options);
                    } else {
                        ObservableDictionary.setStorageFromMap(internalValue, options);
                    }
                } else if (!this._isCombinedView && options.coalesceStorage) {
                    internalValue = new Map([...internalValue, ...storageData]);
                    ObservableDictionary.setStorageFromMap(internalValue, options);
                } else {
                    internalValue = storageData;
                }
            } catch (e) {
                console.warn('ObservableDictionary: failed to load persistence', e);
            }
        }

        this.subject = new BehaviorSubject(internalValue);
        this.dictionary$ = this.subject.asObservable();
        this._storageBindings = new Set();


        this._keyListeners = new Map();
        this._changeSubject = new Subject();

        this.rawChanges$ = this._changeSubject.asObservable();
        this.added$ = this.rawChanges$.pipe(
            filter(x => x.added.keys.length > 0),
            map(x => x.added)
        );
        this.removed$ = this.rawChanges$.pipe(
            filter(x => x.removed.keys.length > 0),
            map(x => x.removed)
        );
        this.modified$ = this.rawChanges$.pipe(
            filter(x => x.modified.keys.length > 0),
            map(x => x.modified)
        );
        this.changes$ = this.rawChanges$.pipe(
            filter(x =>
                x.added.keys.length > 0 ||
                x.removed.keys.length > 0 ||
                x.modified.keys.length > 0
            )
        );

        this._subs = new Set();
        this._persistSubs = new Set();
        this._sourceSubscription = null;

        this._batchDepth = 0;
        this._batchChanges = null;
        this._batchPrevious = null;
        this._isEmitting = false;
        this._emitQueue = [];

        this._persistSubscription = null;
        if (!this._isDerivedView && options.persist && options.storageKey) {
            const target = options.persist === 'local' ? localStorage : sessionStorage;
            const key = options.storageKey;
            this._persistSubscription = this.dictionary$.pipe(debounceTime(100)).subscribe(m => {
                if (m instanceof Map) {
                    try { target.setItem(key, JSON.stringify(Object.fromEntries(m))); } catch (e) { console.warn('ObservableDictionary: save failed', e); }
                } else {
                    console.warn("ObservableDictionary: Attempted to persist non-Map value. Skipping.", m);
                }
            });
            this._persistSubs.add(this._persistSubscription);
        }
    }

    static setStorageFromMap(m, options) {
        const target = options.persist === 'local' ? localStorage : sessionStorage;
        const key = options.storageKey;
        try { target.setItem(key, JSON.stringify(Object.fromEntries(m))); } catch (_) {}
    }

    static mergeFrom(sources, options = {}) {
        if (!Array.isArray(sources) || sources.length === 0) return new ObservableDictionary(new Map(), { ...options, isMergedView: true });
        if (!sources.every(s => s instanceof ObservableDictionary)) throw new Error("ObservableDictionary.mergeFrom: All sources must be instances of ObservableDictionary.");

        const sourceObservables = sources.map(s => s.dictionary$);
        const mergedObservable = combineLatest(sourceObservables).pipe(
            map(latestMapsArray => {
                const mergedMap = new Map();
                for (const currentMap of latestMapsArray) {
                    if (currentMap instanceof Map) {
                        for (const [key, value] of currentMap.entries()) mergedMap.set(key, value);
                    }
                }
                return mergedMap;
            }),
            distinctUntilChanged(areMapsEqual),
            shareReplay({ bufferSize: 1, refCount: true })
        );

        const mergedDict = new ObservableDictionary(new Map(), { ...options, isMergedView: true });
        const subscription = mergedObservable.subscribe(mergedMap => { mergedDict._emitDerivedUpdate(mergedMap); });

        if (typeof mergedDict._sourceSubscription !== 'undefined') {
            mergedDict._sourceSubscription = subscription;
        } else {
            mergedDict._subs.add(subscription);
        }
        return mergedDict;
    };

    static combineSources(sourcesObject, options = {}) {
        if (!sourcesObject || typeof sourcesObject !== 'object' || Object.keys(sourcesObject).length === 0) return new ObservableDictionary({}, { ...options, isDerivedView: true, isCombinedView: true });

        const sourceNames = Object.keys(sourcesObject);
        const sourceDictionaries = Object.values(sourcesObject);
        if (!sourceDictionaries.every(s => s instanceof ObservableDictionary)) throw new Error("ObservableDictionary.combineSources: All values in sourcesObject must be instances of ObservableDictionary.");

        const sourceObservables = sourceDictionaries.map(s => s.dictionary$);
        const combinedObservable = combineLatest(sourceObservables).pipe(
            map(latestMapsArray => {
                const combinedObject = {};
                sourceNames.forEach((name, index) => {
                    combinedObject[name] = latestMapsArray[index];
                });
                return combinedObject;
            }),
            shareReplay({ bufferSize: 1, refCount: true })
        );

        const combinedDict = new ObservableDictionary({}, { ...options, isDerivedView: true, isCombinedView: true });
        combinedDict._sourceSubscription = combinedObservable.subscribe(combinedObject => { combinedDict._emitDerivedUpdate(combinedObject); });
        return combinedDict;
    }

    get size$() {
        if (!this._size$) {
            this._size$ = this.dictionary$.pipe(
                map(m => m instanceof Map ? m.size : Object.keys(m).length),
                distinctUntilChanged(),
                shareReplay({ bufferSize: 1, refCount: true })
            );
        }
        return this._size$;
    }

    get isNonEmpty$() {
        if (!this._isNonEmpty$) {
            this._isNonEmpty$ = this.size$.pipe(
                map(size => size > 0),
                distinctUntilChanged(),
                shareReplay({ bufferSize: 1, refCount: true })
            );
        }
        return this._isNonEmpty$;
    }

    _empty() { return EMPTY_CHANGE; }

    _expand(m) {
        const source = m;
        let _keys, _values, _entries, _keySet;
        return {
            get keys() { if (_keys === undefined) _keys = Array.from(source.keys()); return _keys; },
            get keySet() { if (_keySet === undefined) _keySet = new Set(this.keys); return _keySet; },
            get values() { if (_values === undefined) _values = Array.from(source.values()); return _values; },
            get entries() { if (_entries === undefined) _entries = Array.from(source.entries()); return _entries; },
            map: source
        };
    }

    _emitChange(added, removed, modified, previous) {
        const current = this.getValue();
        this._changeSubject.next({
            added: added.size > 0 ? this._expand(added) : this._empty(),
            removed: removed.size > 0 ? this._expand(removed) : this._empty(),
            modified: modified.size > 0 ? this._expand(modified) : this._empty(),
            current,
            previous
        });
    }

    _emitSingleKey(k, v, oldVal, currentMap, type) {
        const singleMap = {
            get(key) { return key === k ? v : undefined; },
            has(key) { return key === k; },
            size: 1,
            keys() { return [k][Symbol.iterator](); },
            values() { return [v][Symbol.iterator](); },
            entries() { return [[k, v]][Symbol.iterator](); },
            forEach(fn) { fn(v, k, this); },
            [Symbol.iterator]() { return [[k, v]][Symbol.iterator](); }
        };
        const change = {
            keys: [k], keySet: new Set([k]), values: [v], entries: [[k, v]], map: singleMap
        };
        const previous = {
            get(key) { return key === k ? oldVal : currentMap.get(key); },
            has(key) { return key === k ? (oldVal !== undefined) : currentMap.has(key); },
            keys() { return currentMap.keys(); }
        };

        const listeners = this._keyListeners.get(k);
        if (listeners) {
            listeners.forEach(fn => fn(v, oldVal, type));
        }

        this._changeSubject.next({
            added:    type === 'add'    ? change : EMPTY_CHANGE,
            removed:  type === 'remove' ? change : EMPTY_CHANGE,
            modified: type === 'modify' ? change : EMPTY_CHANGE,
            current: currentMap,
            previous
        });
    }

    _emitDerivedUpdate(newMap) {
        const prev = this.subject.getValue();
        this.subject.next(newMap);

        if (!this._changeSubject.observed) return;

        if ((prev instanceof Map) && (newMap instanceof Map)) {
            const added = new Map();
            const removed = new Map();
            const modified = new Map();

            for (const [k, v] of newMap.entries()) {
                if (!prev.has(k)) added.set(k, v);
                else if (!deepEqual(prev.get(k), v)) modified.set(k, v);
            }
            for (const k of prev.keys()) {
                if (!newMap.has(k)) removed.set(k, prev.get(k));
            }

            if (added.size > 0 || removed.size > 0 || modified.size > 0) {
                this._changeSubject.next({
                    added: added.size > 0 ? this._expand(added) : EMPTY_CHANGE,
                    removed: removed.size > 0 ? this._expand(removed) : EMPTY_CHANGE,
                    modified: modified.size > 0 ? this._expand(modified) : EMPTY_CHANGE,
                    current: newMap,
                    previous: prev
                });
            }
        } else {
            this._changeSubject.next({ added: EMPTY_CHANGE, removed: EMPTY_CHANGE, modified: EMPTY_CHANGE, current: newMap, previous: prev });
        }
    }

    _accumulateChange(key, type, value) {
        if (!this._batchChanges) this._batchChanges = { added: new Map(), removed: new Map(), modified: new Map() };
        const bc = this._batchChanges;
        if (type === 'add') {
            if (bc.removed.has(key)) { bc.removed.delete(key); bc.modified.set(key, value); }
            else { bc.added.set(key, value); }
        } else if (type === 'modify') {
            if (bc.added.has(key)) { bc.added.set(key, value); }
            else { bc.modified.set(key, value); }
        } else if (type === 'remove') {
            if (bc.added.has(key)) { bc.added.delete(key); }
            else { bc.modified.delete(key); bc.removed.set(key, value); }
        }
    }

    _composeStorageKey(namespace, key) { return namespace ? `${namespace}:${key}` : String(key); }
    _getStorageDriver(storage) { return storage === 'session' ? sessionStorage : localStorage; }

    _storagePack(value, ttlSeconds) {
        const e = Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? Date.now() + ttlSeconds * 1000 : 0;
        return JSON.stringify({ e, v: value });
    }

    _storageUnpack(raw) {
        try {
            const parsed = JSON.parse(raw);
            if (!parsed || typeof parsed !== 'object') return { expired:false, value:null };
            if (parsed.e && parsed.e > 0 && Date.now() > parsed.e) return { expired:true, value:null };
            return { expired:false, value:parsed.v };
        } catch (_) { return { expired:false, value:null }; }
    }

    _storageRead(driver, ns, key) {
        const k = this._composeStorageKey(ns, key);
        try {
            const raw = driver.getItem(k);
            if (!raw) return null;
            const { expired, value } = this._storageUnpack(raw);
            if (expired) { try { driver.removeItem(k); } catch (_) {} return null; }
            return value;
        } catch (_) { return null; }
    }

    _storageWrite(driver, ns, key, value, ttlSeconds) {
        const k = this._composeStorageKey(ns, key);
        try { driver.setItem(k, this._storagePack(value, ttlSeconds)); return true; }
        catch (e) { console.warn('ObservableDictionary: storage write failed', e); return false; }
    }

    bindPersistentKeys(keys, {
        namespace, key, storage = 'local', ttlSeconds, debounceMs = 80, pruneNullish = true, reconcile = 'replace', listen = true
    } = {}) {
        if (!Array.isArray(keys) || keys.length === 0) throw new Error('bindPersistentKeys: keys[] required.');
        if (!namespace || !key) throw new Error('bindPersistentKeys: namespace and key are required.');
        if (this._isDerivedView) throw new Error('bindPersistentKeys: cannot attach to a derived view.');

        const driver = this._getStorageDriver(storage);
        const ns = namespace;
        const storageKey = key;
        const keySet = new Set(keys);

        let lastPersistedStr = '';
        let applyingFromStorage = false;

        const collectSubset = (map) => {
            const out = {};
            if (!(map instanceof Map)) return out;
            for (const [k, v] of map.entries()) {
                if (!keySet.has(k)) continue;
                if (pruneNullish && (v === null || v === undefined)) continue;
                out[k] = v;
            }
            return out;
        };

        const applyToSource = (obj) => {
            if (!obj || typeof obj !== 'object') return;
            if (reconcile === 'replace') {
                for (const k of keySet) {
                    if (!(k in obj)) {
                        const cur = this.getValue();
                        if (cur instanceof Map && cur.has(k)) this.delete(k);
                    }
                }
            }
            for (const k of Object.keys(obj)) {
                if (!keySet.has(k)) continue;
                this.set(k, obj[k]);
            }
        };

        const pushNow = () => {
            if (applyingFromStorage) return true;
            const snapshot = collectSubset(this.getValue());
            const str = JSON.stringify(snapshot);
            if (str === lastPersistedStr) return true;
            const ok = this._storageWrite(driver, ns, storageKey, snapshot, ttlSeconds);
            if (ok) lastPersistedStr = str;
            return ok;
        };

        const stored = this._storageRead(driver, ns, storageKey);
        if (stored) {
            applyingFromStorage = true;
            try { applyToSource(stored); } finally { applyingFromStorage = false; }
            lastPersistedStr = JSON.stringify(collectSubset(this.getValue()));
        } else {
            lastPersistedStr = JSON.stringify(collectSubset(this.getValue()));
            try { this._storageWrite(driver, ns, storageKey, JSON.parse(lastPersistedStr), ttlSeconds); } catch (_) {}
        }

        let t = null;
        const scheduleWrite = () => {
            if (t) clearTimeout(t);
            t = setTimeout(() => { t = null; try { pushNow(); } catch (_) {} }, debounceMs);
        };

        const sub = this.rawChanges$.subscribe(({ added, removed, modified }) => {
            if (applyingFromStorage) return;

            let relevant = false;
            for (const k of added.keys) { if (keySet.has(k)) { relevant = true; break; } }
            if (!relevant) for (const k of removed.keys) { if (keySet.has(k)) { relevant = true; break; } }
            if (!relevant) for (const k of modified.keys) { if (keySet.has(k)) { relevant = true; break; } }
            if (!relevant) return;
            scheduleWrite();
        });

        let removeStorageListener = null;
        if (listen && typeof window !== 'undefined') {
            const composedKey = this._composeStorageKey(ns, storageKey);
            const handler = (ev) => {
                if (ev.storageArea !== driver) return;
                if (ev.key !== composedKey) return;
                const next = this._storageRead(driver, ns, storageKey);
                applyingFromStorage = true;
                try { applyToSource(next || {}); } finally { applyingFromStorage = false; }
                lastPersistedStr = JSON.stringify(collectSubset(this.getValue()));
            };
            window.addEventListener('storage', handler);
            removeStorageListener = () => window.removeEventListener('storage', handler);
        }

        const handle = {
            namespace: ns, key: storageKey, keys: Object.freeze([...keySet]),
            dispose: () => {
                clearTimeout(t);
                try { sub.unsubscribe(); } catch (_) {}
                if (removeStorageListener) { try { removeStorageListener(); } catch (_) {} }
                this._storageBindings.delete(handle);
            },
            pullFromStorage: () => {
                const next = this._storageRead(driver, ns, storageKey);
                applyingFromStorage = true;
                try { applyToSource(next || {}); } finally { applyingFromStorage = false; }
                lastPersistedStr = JSON.stringify(collectSubset(this.getValue()));
                return true;
            },
            pushToStorage: () => pushNow()
        };
        this._storageBindings.add(handle);
        return handle;
    }

    getValue() { return this.subject.getValue(); }
    get(k) { return this.getValue().get(k); }
    has(k) { return this.getValue().has(k); }
    keys() { return this.getValue().keys(); }
    values() { return this.getValue().values(); }
    entries() { return this.getValue().entries(); }
    forEach(callbackFn) { this.getValue().forEach(callbackFn); }

    update(m) {
        if (this._isDerivedView) return console.warn("ObservableDictionary: Cannot directly 'update' a merged view.");
        if (m == null) return;
        const nm = m instanceof Map ? m : new Map(Object.entries(m));
        this.batch(() => {
            for (const k of this.getValue().keys()) if (!nm.has(k)) this.delete(k);
            for (const [k, v] of nm.entries()) this.set(k, v);
        });
    }

    updateFromList(l, key) {
        l.forEach((r) => { this.set(r[key], r); });
    }

    set(k, v) {
        if (this._isDerivedView) return console.warn("ObservableDictionary: Cannot directly 'set' on a merged view.");
        const currentMap = this.getValue();
        const existed = currentMap.has(k);
        const oldVal = existed ? currentMap.get(k) : undefined;

        if (existed && (oldVal === v || (typeof oldVal === 'object' && oldVal !== null && deepEqual(oldVal, v)))) {
            return this;
        }

        currentMap.set(k, v);
        if (this._batchDepth > 0) {
            if (!this._batchPrevious.has(k)) this._batchPrevious.set(k, existed ? oldVal : undefined);
            this._accumulateChange(k, existed ? 'modify' : 'add', v);
            return this;
        }

        if (this._isEmitting) {
            const type = existed ? 'modify' : 'add';
            this._emitQueue.push(() => {
                this.subject.next(new Map(this.getValue()));
                this._emitSingleKey(k, v, existed ? oldVal : undefined, this.getValue(), type);
            });
            return this;
        }

        this._isEmitting = true;
        try {
            this.subject.next(new Map(currentMap));
            this._emitSingleKey(k, v, existed ? oldVal : undefined, currentMap, existed ? 'modify' : 'add');
        } finally {
            this._isEmitting = false;
        }

        while (this._emitQueue.length > 0) {
            const queued = this._emitQueue.shift();
            this._isEmitting = true;
            try { queued(); } finally { this._isEmitting = false; }
        }

        return this;
    }

    add(k, v) {
        if (this._isDerivedView) return console.warn("ObservableDictionary: Cannot directly 'add' to a merged view.");
        if (v !== undefined) return this.set(k, v);
        if (typeof k === 'object') {
            this.batch(() => { Object.entries(k).forEach(([kk, vv]) => this.set(kk, vv)); });
        }
    }

    delete(k) {
        if (this._isDerivedView) return console.warn("ObservableDictionary: Cannot directly 'delete' from a merged view.");
        const currentMap = this.getValue();
        if (!currentMap.has(k)) return false;

        const oldVal = currentMap.get(k);
        currentMap.delete(k);

        if (this._batchDepth > 0) {
            if (!this._batchPrevious.has(k)) this._batchPrevious.set(k, oldVal);
            this._accumulateChange(k, 'remove', oldVal);
            return true;
        }

        if (this._isEmitting) {
            this._emitQueue.push(() => {
                this.subject.next(new Map(this.getValue()));
                this._emitSingleKey(k, oldVal, oldVal, this.getValue(), 'remove');
            });
            return true;
        }

        this._isEmitting = true;
        try {
            this.subject.next(new Map(currentMap));
            this._emitSingleKey(k, oldVal, oldVal, currentMap, 'remove');
        } finally {
            this._isEmitting = false;
        }

        while (this._emitQueue.length > 0) {
            const queued = this._emitQueue.shift();
            this._isEmitting = true;
            try { queued(); } finally { this._isEmitting = false; }
        }

        return true;
    }

    remove(k) { return this.delete(k); }

    clear() {
        if (this._isDerivedView) return console.warn("ObservableDictionary: Cannot directly 'clear' a merged view.");
        const currentMap = this.getValue();
        if (currentMap.size === 0) return;

        if (this._batchDepth > 0) {
            for (const [k, v] of currentMap.entries()) {
                if (!this._batchPrevious.has(k)) this._batchPrevious.set(k, v);
                this._accumulateChange(k, 'remove', v);
            }
            currentMap.clear();
            return;
        }

        const prev = new Map(currentMap);
        const removed = new Map(currentMap);
        currentMap.clear();
        this.subject.next(new Map(currentMap));
        this._emitChange(new Map(), removed, new Map(), prev);
    }

    size(){ return this.getValue().size; }
    asObject(){ return Object.fromEntries(this.getValue()); }
    asObservable() { return this.dictionary$; }

    batch(fn) {
        if (this._isDerivedView) {
            console.warn("ObservableDictionary: Cannot use 'batch' on a merged view.");
            try { fn(); } catch(e) { console.error("Error during non-batched execution on merged view:", e)}
            return;
        }

        const isOuterBatch = this._batchDepth === 0;
        if (isOuterBatch) {
            this._batchPrevious = new Map();
            this._batchChanges = { added: new Map(), removed: new Map(), modified: new Map() };
        }

        this._batchDepth++;
        try {
            fn();
        } finally {
            this._batchDepth--;
            if (this._batchDepth === 0) {
                const bc = this._batchChanges;
                const prev = this._batchPrevious;
                this._batchChanges = null;
                this._batchPrevious = null;

                if (bc && (bc.added.size > 0 || bc.removed.size > 0 || bc.modified.size > 0)) {
                    const currentMap = this.getValue();
                    const previousSnapshot = new Map(currentMap);

                    for (const [k, v] of prev.entries()) {
                        if (v === undefined) {
                            previousSnapshot.delete(k);
                        } else {
                            previousSnapshot.set(k, v);
                        }
                    }

                    for (const [k, v] of bc.added.entries()) {
                        const l = this._keyListeners.get(k);
                        if (l) l.forEach(fn => fn(v, undefined, 'add'));
                    }
                    for (const [k, v] of bc.modified.entries()) {
                        const l = this._keyListeners.get(k);
                        if (l) l.forEach(fn => fn(v, prev.get(k), 'modify'));
                    }
                    for (const [k, v] of bc.removed.entries()) {
                        const l = this._keyListeners.get(k);
                        if (l) l.forEach(fn => fn(undefined, v, 'remove'));
                    }

                    this._isEmitting = true;
                    try {
                        this.subject.next(new Map(currentMap));
                        this._emitChange(bc.added, bc.removed, bc.modified, previousSnapshot);
                    } finally {
                        this._isEmitting = false;
                    }

                    while (this._emitQueue.length > 0) {
                        const queued = this._emitQueue.shift();
                        this._isEmitting = true;
                        try { queued(); } finally { this._isEmitting = false; }
                    }
                }
            }
        }
    }

    merge(data) {
        if (this._isDerivedView) return console.warn("ObservableDictionary: Cannot directly 'merge' into a merged view.");
        if (data == null) return;
        this.batch(() => {
            const entries = data instanceof Map ? data.entries() : Object.entries(data);
            for (const [k, v] of entries) this.set(k, v);
        });
    }

    _fastTrackSubscription(key, handler) {
        let specific = this._keyListeners.get(key);
        if (!specific) {
            specific = new Set();
            this._keyListeners.set(key, specific);
        }
        specific.add(handler);

        const sub = {
            closed: false,
            unsubscribe: () => {
                if (sub.closed) return;
                sub.closed = true;
                specific.delete(handler);
                if (specific.size === 0) this._keyListeners.delete(key);
                this._subs.delete(sub);
            }
        };
        this._subs.add(sub);
        return sub;
    }

    onValueConfirmed(key, fn) {
        if (this._keyListeners.has(key)) {
            if (this.getValue() === true) {
                return fn(true);
            }
        }
        return this.onValueChanged(key, fn)
    }

    onValueChanged(key, fn) {
        if (fn.constructor.name === 'AsyncFunction') {
            return this.onValueChangedAsync(key, fn)
        }
        return this.onValueChangedSync(key, fn)
    }

    onValueChangedSync(key, fn) {
        return this._fastTrackSubscription(key, (cur, prev, type) => {
            if (type === 'modify') {
                return fn(cur, prev);
            }
        });
    }

    onValueChangedAsync(key, fn) {
        return this._fastTrackSubscription(key, async (cur, prev, type) => {
            if (type === 'modify') {
                return await fn(cur, prev);
            }
        });
    }

    onValueAddedOrChanged(key, fn) {
        return this._fastTrackSubscription(key, (cur, prev, type) => {
            if (type === 'modify' || type === 'add') {
                return fn(cur, prev);
            }
        });
    }

    onKeyAdded(key, fn) {
        return this._fastTrackSubscription(key, (cur, prev, type) => {
            if (type === 'add') {
                return fn(cur);
            }
        });
    }

    onKeyRemoved(key, fn) {
        return this._fastTrackSubscription(key, (cur, prev, type) => {
            if (type === 'remove') {
                return fn();
            }
        });
    }

    onAny(fn){ return this.onChanges(fn); }
    onChanges(fn){ const s=this.changes$.subscribe(fn); this._subs.add(s); return s; }
    onAdd(fn){ const s=this.added$.subscribe(fn); this._subs.add(s); return s; }
    onRemove(fn){ const s=this.removed$.subscribe(fn); this._subs.add(s); return s; }
    onModified(fn){ const s=this.modified$.subscribe(fn); this._subs.add(s); return s; }
    onRawChanges(fn){ const s=this.rawChanges$.subscribe(fn); this._subs.add(s); return s; }

    pipe(...operators) {
        let source$ = this.dictionary$;
        source$ = source$.pipe(...operators);
        const derived = new ObservableDictionary(new Map(), { isMergedView: true });

        const sub = source$.subscribe(map => {
            if (map instanceof Map) derived._emitDerivedUpdate(map);
        });

        derived._sourceSubscription = sub;

        if (viewSubscriptionRegistry) {
            viewSubscriptionRegistry.register(derived, sub);
        }

        return derived;
    }

    pick(keys) {
        return this.pipe(map(map =>
            new Map(Array.from(map.entries()).filter(([k]) => keys.includes(k)))
        ));
    }

    selectKeys(keys){
        const initialSelected = new Map(
            Array.from(this.getValue().entries()).filter(([k]) => keys.includes(k))
        );
        const view = new ObservableDictionary(initialSelected); // Not a merged view

        const sub = this.rawChanges$.subscribe(({ added, removed, modified }) => {
            view.batch(() => { // Batch updates to the view
                // added
                added.entries.forEach(([k, v]) => {
                    if (keys.includes(k)) view.set(k, v);
                });
                // removed
                removed.keys.forEach(k => {
                    if (keys.includes(k)) view.delete(k);
                });
                // modified
                modified.entries.forEach(([k, v]) => {
                    if (keys.includes(k)) view.set(k, v);
                });
            });
        });
        view._subs.add(sub); // Add to the *view's* subscriptions
        return view;
    }


    omit(keys) {
        const keySet = new Set(keys);
        return this.pipe(map(sourceMap => {
            const result = new Map();
            for (const [k, v] of sourceMap) { if (!keySet.has(k)) result.set(k, v); }
            return result;
        }));
    }

    unsubscribe(sub){
        if (sub && typeof sub.unsubscribe === 'function' && !sub.closed){
            sub.unsubscribe();
            this._subs.delete(sub);
            this._persistSubs.delete(sub);
            return true;
        }
        return false;
    }

    dispose(){
        if (this._sourceSubscription && typeof this._sourceSubscription.unsubscribe === 'function') {
            this._sourceSubscription.unsubscribe();
            this._sourceSubscription = null;
        }

        this._subs.forEach(s => s.unsubscribe());
        this._subs.clear();

        this._persistSubs.forEach(s => s.unsubscribe());
        this._persistSubs.clear();

        if (this._persistSubscription && !this._persistSubscription.closed) {
            this._persistSubscription.unsubscribe();
            this._persistSubscription = null;
        }

        if (this._storageBindings && this._storageBindings.size) {
            const bindings = Array.from(this._storageBindings);
            this._storageBindings.clear();
            for (let i = 0; i < bindings.length; i++) {
                try { bindings[i].dispose(); } catch (_) {}
            }
        }

        this._changeSubject.complete();
        this.subject.complete();
        this._keyListeners.clear();
    }
}

function areMapsEqual(map1, map2) {
    if (!(map1 instanceof Map) || !(map2 instanceof Map)) return false;
    if (map1.size !== map2.size) return false;
    for (const [key, value] of map1.entries()) {
        if (!map2.has(key) || !deepEqual(map2.get(key), value)) return false;
    }
    return true;
}

function areCombinedObjectsEqual(obj1, obj2) {
    return deepEqual(obj1, obj2)
}
