


/**
 * OptimizedColumnarCodec - Encodes/decodes data between row-oriented JS objects
 * and a sparse columnar format optimized for network transmission.
 * Version 2 introduces a sparse envelope format and RLE index encoding.
 */
export class OptimizedColumnarCodec {
    static ENCODING_VERSION = 2;
    static NULL_MARKER = -1; // Kept for potential legacy compatibility if needed

    constructor() {
        // Encoders/Decoders are typically needed for more complex binary formats,
        // but this implementation primarily deals with JSON-compatible structures.
        // Kept for potential future use or consistency if extended.
        this.textEncoder = new TextEncoder();
        this.textDecoder = new TextDecoder();
    }

    //--------------------------------------------------------------------------
    // RLE (Run-Length Encoding) Helpers
    //--------------------------------------------------------------------------

    /**
     * Decodes a Run-Length Encoded list of indices.
     * The RLE format is [start1, count1, start2, count2, ...].
     * @param {number[]} rle - The RLE encoded array.
     * @returns {number[]} - An array of decoded indices.
     */
        // BUG-15 FIX: cap maximum decoded size to prevent OOM from malicious payloads
    static MAX_RLE_DECODED_SIZE = 10_000_000;

    _rleDecodeIndices(rle) {
        const out = [];
        if (!rle || !Array.isArray(rle) || rle.length % 2 !== 0) {
            console.warn("Invalid RLE data received:", rle);
            return out;
        }
        for (let i = 0; i < rle.length; i += 2) {
            const start = parseInt(rle[i], 10);
            const count = parseInt(rle[i + 1], 10);
            if (!isNaN(start) && !isNaN(count) && count > 0) {
                if (out.length + count > OptimizedColumnarCodec.MAX_RLE_DECODED_SIZE) {
                    console.error(`RLE expansion exceeds safety limit (${OptimizedColumnarCodec.MAX_RLE_DECODED_SIZE}), truncating`);
                    const allowed = OptimizedColumnarCodec.MAX_RLE_DECODED_SIZE - out.length;
                    for (let j = 0; j < allowed; j++) out.push(start + j);
                    return out;
                }
                for (let j = 0; j < count; j++) {
                    out.push(start + j);
                }
            } else if(count < 0){
                console.warn(`Invalid RLE count (${count}) at index ${i+1}`);
            }
        }
        return out;
    }


    //--------------------------------------------------------------------------
    // Decoding (Sparse Payload -> Rows)
    //--------------------------------------------------------------------------

    /**
     * Decodes a payload (potentially sparse columnar) into an array of row objects.
     * @param {object} payload - The encoded payload object.
     * @returns {object[]} - An array of decoded row objects.
     */
    decodeToRows(payload) {
        // --- Basic Format Handling ---
        if (!payload || typeof payload !== 'object') {
            console.warn("Received invalid payload:", payload);
            return [];
        }
        const format = payload._format;
        if (format === 'empty') {
            return [];
        }
        if (format === 'single') {
            return payload._data ? [payload._data] : [];
        }
        // Assume non-columnar formats are just single rows (or handle as error)
        if (format !== 'columnar') {
            console.warn(`Received unknown or legacy payload format: '${format}'. Treating as single row.`);
            return [payload];
        }

        // --- Columnar Decoding ---
        const numRows = payload._count;
        const columnsData = payload._columns;
        if (typeof numRows !== 'number' || numRows <= 0 || !columnsData) {
            if (numRows === 0) return []; // Valid empty columnar
            console.warn("Invalid columnar payload structure:", payload);
            return [];
        }

        // Initialize rows as empty objects
        const rows = new Array(numRows);
        for(let i=0; i<numRows; ++i) rows[i] = {};

        // Decode each column and populate the rows
        for (const colName in columnsData) {
            if (Object.prototype.hasOwnProperty.call(columnsData, colName)) {
                const colData = columnsData[colName];
                const values = this._decodeColumn(colData, numRows);

                // Apply decoded values to the rows array
                // Important: Only set the property if the value is not 'undefined'.
                // 'undefined' signifies the column was missing for that row in the sparse payload.
                // 'null' signifies the column was explicitly set to null.
                for (let i = 0; i < numRows; i++) {
                    if (values[i] !== undefined) {
                        rows[i][colName] = values[i];
                    }
                    // else: Do nothing, leave the property absent for this row object
                }
            }
        }
        return rows;
    }

    /**
     * Decodes the data for a single column based on its encoded format.
     * Handles both legacy dense formats and the new sparse format.
     * @param {object} colData - The encoded data for the column.
     * @param {number} numRows - The total number of rows expected.
     * @returns {Array<any>} - An array of decoded values (length numRows),
     * using 'undefined' for missing values and 'null' for explicit nulls.
     */
    _decodeColumn(colData, numRows) {
        const colType = colData?._type;

        // --- Handle Sparse Format (Version 2+) ---
        if (colType === 'sparse') {
            return this._decodeSparseColumn(colData, numRows);
        }

        // --- Handle Compact Null Format ---
        if (colType === 'null') {
            // All rows where this column appears are null. Other rows are 'missing' (undefined).
            const out = new Array(numRows).fill(undefined); // Default to missing
            const nulls = new Set(colData._nulls || []); // If _nulls is absent, means ALL are null
            if (!colData._nulls) { // If _nulls isn't specified, all rows are null
                for(let i=0; i<numRows; ++i) out[i] = null;
            } else { // Only specified rows are null
                nulls.forEach(i => {
                    if (i >= 0 && i < numRows) { // Basic bounds check
                        out[i] = null;
                    }
                });
            }
            return out;
        }

        // --- Handle Legacy Dense Formats (Kept for compatibility) ---
        // These assume the column is present in *all* rows, except where explicitly nulled.
        const out = new Array(numRows); // Initialize with implicit empty slots
        const nulls = new Set(colData?._nulls || []);

        switch (colType) {
            case 'boolean': { // Legacy boolean uses _bits for all rows
                const bits = colData._bits || [];
                for (let i = 0; i < numRows; i++) {
                    if (nulls.has(i)) {
                        out[i] = null;
                        continue;
                    }
                    // Avoid error if bits array is too short (shouldn't happen in valid payload)
                    const byteIndex = Math.floor(i / 8);
                    if (byteIndex < bits.length) {
                        const bit = i % 8;
                        out[i] = (bits[byteIndex] & (1 << bit)) !== 0;
                    } else {
                        out[i] = undefined; // Treat as missing if data is short
                    }
                }
                return out;
            }
            case 'int':
            case 'float':
            case 'string': // Legacy numeric/string uses _values for all rows
            {
                const vals = colData._values || [];
                for (let i = 0; i < numRows; i++) {
                    // Check nulls first
                    if (nulls.has(i)) {
                        out[i] = null;
                    } else if (i < vals.length) { // Check if value exists
                        out[i] = vals[i];
                    } else {
                        out[i] = undefined; // Treat as missing if data is short
                    }
                }
                return out;
            }
            case 'dict': // Legacy dict uses _indices for all rows
            {
                const dict = colData._dict || [];
                const indices = colData._indices || [];
                const dLen = dict.length;
                for (let i = 0; i < numRows; i++) {
                    if (nulls.has(i)) {
                        out[i] = null;
                        continue;
                    }
                    const code = indices[i];
                    // Check if index exists and is valid
                    if (code === OptimizedColumnarCodec.NULL_MARKER || code === undefined || code < 0 || code >= dLen) {
                        // Treat explicit null marker OR invalid index as null for legacy compatibility
                        out[i] = null;
                    } else {
                        out[i] = dict[code];
                    }
                }
                return out;
            }
            // Handle potential numeric base64 legacy format (rare)
            case 'numeric':
                return this._decodeNumericBase64Legacy(colData, numRows, nulls);

            default:
                console.warn(`Encountered unknown or unsupported legacy column type: '${colType}'`);
                // Fallback: Treat as missing for all rows unless explicitly nulled
                for (let i = 0; i < numRows; i++) {
                    out[i] = nulls.has(i) ? null : undefined;
                }
                return out;
        }
    }

    /**
     * Decodes a column specifically marked with _type: 'sparse'.
     * @param {object} colData - The sparse column data object.
     * @param {number} numRows - The total number of rows.
     * @returns {Array<any>} - Decoded values array.
     */
    _decodeSparseColumn(colData, numRows) {
        // 1. Initialize output array with 'undefined' (missing)
        const out = new Array(numRows).fill(undefined);

        // 2. Decode and combine present indices
        const presentRaw = colData._present || [];
        const presentRle = colData._present_rle || [];
        let presentIndices = [];
        if (presentRle.length > 0) {
            presentIndices = this._rleDecodeIndices(presentRle);
            // If raw indices also exist (for compat), merge them (less common)
            if (presentRaw.length > 0) {
                presentIndices = Array.from(new Set([...presentIndices, ...presentRaw])).sort((a, b) => a - b);
            }
        } else {
            presentIndices = presentRaw; // Assume already sorted integers
        }

        // 3. Decode and combine null indices
        const nullsRaw = colData._nulls || [];
        const nullsRle = colData._nulls_rle || [];
        let nullIndicesSet = new Set();
        if (nullsRle.length > 0) {
            this._rleDecodeIndices(nullsRle).forEach(idx => nullIndicesSet.add(idx));
        }
        nullsRaw.forEach(idx => nullIndicesSet.add(idx)); // Add raw nulls

        // 4. Apply explicit nulls to the output array (bounds check included)
        nullIndicesSet.forEach(i => {
            if (i >= 0 && i < numRows) {
                out[i] = null;
            }
        });

        // 5. Apply present values based on subtype
        const subtype = colData._subtype;
        const presentValuesData = colData._values; // Can be values array or dict indices
        const presentBitsData = colData._bits;     // For boolean
        const dictData = colData._dict;           // For dict

        let valueIdx = 0; // Tracks position in the _values/_bits arrays

        for (const rowIndex of presentIndices) {
            // Ensure rowIndex is valid and *not* explicitly nulled
            if (rowIndex >= 0 && rowIndex < numRows && !nullIndicesSet.has(rowIndex)) {
                try {
                    switch (subtype) {
                        case 'boolean':
                            if (presentBitsData && Math.floor(valueIdx / 8) < presentBitsData.length) {
                                const byte = presentBitsData[Math.floor(valueIdx / 8)];
                                const bit = valueIdx % 8;
                                out[rowIndex] = (byte & (1 << bit)) !== 0;
                            } else {
                                // Data mismatch, treat as missing
                                console.warn(`Boolean data missing for present index ${rowIndex} (value index ${valueIdx})`);
                                out[rowIndex] = undefined;
                            }
                            break;
                        case 'int': // Assume already numbers
                        case 'float': // Assume already numbers
                        case 'string': // Assume already strings
                            if (presentValuesData && valueIdx < presentValuesData.length) {
                                out[rowIndex] = presentValuesData[valueIdx];
                            } else {
                                // Data mismatch
                                console.warn(`${subtype} data missing for present index ${rowIndex} (value index ${valueIdx})`);
                                out[rowIndex] = undefined;
                            }
                            break;
                        case 'dict':
                            if (presentValuesData && dictData && valueIdx < presentValuesData.length) {
                                const dictIndex = presentValuesData[valueIdx];
                                if (dictIndex >= 0 && dictIndex < dictData.length) {
                                    out[rowIndex] = dictData[dictIndex];
                                } else {
                                    // Invalid dict index, treat as null or missing? Let's use null.
                                    console.warn(`Invalid dict index ${dictIndex} for present index ${rowIndex} (value index ${valueIdx})`);
                                    out[rowIndex] = null;
                                }
                            } else {
                                // Data mismatch
                                console.warn(`Dict index data missing for present index ${rowIndex} (value index ${valueIdx})`);
                                out[rowIndex] = undefined;
                            }
                            break;
                        default:
                            console.warn(`Unknown sparse subtype '${subtype}' for index ${rowIndex}`);
                            out[rowIndex] = undefined; // Treat unknown subtype as missing
                            break;
                    }
                } catch (e) {
                    console.error(`Error decoding sparse subtype '${subtype}' at row ${rowIndex} (value index ${valueIdx}):`, e);
                    out[rowIndex] = undefined; // Error -> missing
                }
            } // End if valid index and not null
            valueIdx++; // Increment index into value/bit array
        } // End for presentIndices

        return out;
    }

    /**
     * Decodes legacy numeric base64 format (rare).
     * @param {object} colData Column data.
     * @param {number} numRows Total rows.
     * @param {Set<number>} nulls Set of null indices.
     * @returns {Array<any>} Decoded values.
     */
    _decodeNumericBase64Legacy(colData, numRows, nulls) {
        const out = new Array(numRows);
        try {
            const base64Str = colData._values || '';
            const dtypeStr = colData._dtype || 'f64'; // Default to float64
            const buffer = this._base64ToArrayBuffer(base64Str);

            let typedArray;
            switch (dtypeStr) {
                case 'i8': typedArray = new Int8Array(buffer); break;
                case 'u8': typedArray = new Uint8Array(buffer); break;
                case 'i16': typedArray = new Int16Array(buffer); break;
                case 'u16': typedArray = new Uint16Array(buffer); break;
                case 'i32': typedArray = new Int32Array(buffer); break;
                case 'u32': typedArray = new Uint32Array(buffer); break;
                case 'f32': typedArray = new Float32Array(buffer); break;
                case 'i64': // BigInt64Array might not be supported everywhere, fall back
                case 'u64': // BigUint64Array might not be supported everywhere, fall back
                    console.warn(`Legacy ${dtypeStr} not fully supported, decoding as float64.`);
                    typedArray = new Float64Array(buffer); break;
                case 'f64':
                default: typedArray = new Float64Array(buffer); break;
            }

            const numValues = typedArray.length;
            for (let i = 0; i < numRows; i++) {
                if (nulls.has(i)) {
                    out[i] = null;
                } else if (i < numValues) {
                    out[i] = Number(typedArray[i]); // Convert BigInt back to Number if needed
                } else {
                    out[i] = undefined; // Missing
                }
            }
        } catch (e) {
            console.error("Error decoding numeric base64 legacy column:", e);
            // Fallback: Treat as missing unless explicitly nulled
            for (let i = 0; i < numRows; i++) {
                out[i] = nulls.has(i) ? null : undefined;
            }
        }
        return out;
    }


    //--------------------------------------------------------------------------
    // Encoding (Rows -> Sparse Payload) - Less commonly needed on frontend
    //--------------------------------------------------------------------------
    // Note: Encoding methods (_toColumnar, _encode*Sparse) are kept from the
    // original file but are less likely to be used directly in a typical
    // frontend scenario receiving data. They could be useful for local
    // transformations or sending data *back* in the same format.

    _toColumnar(rows) {
        if (!rows?.length) return { _format: 'empty', _count: 0, _columns: {} };
        if (rows.length === 1) return { _format: 'single', _data: rows[0], _columns: {} }; // Adjusted slightly

        const columns = {};
        const numRows = rows.length;
        const allKeys = new Set();
        for (let i = 0; i < numRows; i++) {
            const r = rows[i];
            // Ensure r is an object before trying to get keys
            if (r && typeof r === 'object') {
                for (const k in r) allKeys.add(k);
            }
        }

        for (const key of allKeys) {
            const presentIndices = [];
            const presentValues = [];
            const nullIndices = [];

            for (let i = 0; i < numRows; i++) {
                const row = rows[i];
                // Check if row exists and is an object
                if (!row || typeof row !== 'object') continue;

                const hasKey = Object.prototype.hasOwnProperty.call(row, key);
                if (!hasKey) continue;
                const val = row[key];
                if (val === null) {
                    nullIndices.push(i);
                } else if (val !== undefined) {
                    presentIndices.push(i);
                    presentValues.push(val);
                }
            }

            if (presentIndices.length === 0 && nullIndices.length === 0) {
                continue; // Column never appears
            }

            // Compact null encoding
            if (presentIndices.length === 0 && nullIndices.length > 0) {
                columns[key] = { _type: 'null', _count: numRows, _nulls: nullIndices };
                continue;
            }

            // Determine subtype and encode present values
            let payload;
            const valueType = presentValues.length > 0 ? typeof presentValues[0] : 'undefined';
            let allSameType = true;
            let allNumeric = true;
            let allBoolean = true;
            let allString = true; // Assume string initially

            for(const val of presentValues) {
                const currentType = typeof val;
                if (currentType !== valueType) allSameType = false; // Only matters if needed
                if (currentType !== 'number') allNumeric = false;
                if (currentType !== 'boolean') allBoolean = false;
                if (currentType === 'object' || currentType === 'function' || currentType === 'symbol' || currentType === 'bigint') {
                    allString = false; // Cannot reliably coerce these to string for dict encoding
                }
            }

            if (allBoolean) {
                payload = this._encodeBooleanColumnSparse(presentIndices, presentValues);
            } else if (allNumeric) {
                payload = this._encodeNumericColumnSparse(presentIndices, presentValues);
            } else if (allString) { // Fallback to string/dict if can be stringified
                payload = this._encodeStringColumnSparse(presentIndices, presentValues);
            } else { // Mixed or complex types - force simple string encoding
                console.warn(`Column "${key}" has mixed or complex types, encoding as plain strings.`);
                payload = {
                    _type: 'string',
                    _present: presentIndices.slice(),
                    _values: presentValues.map(v => String(v)), // Force string conversion
                    _bits: undefined,
                    _dict: undefined
                };
            }


            // Sparse envelope
            columns[key] = {
                _type: 'sparse',
                _subtype: payload._type,
                _present: payload._present,
                _values: payload._values,
                _bits: payload._bits,
                _dict: payload._dict,
                _nulls: nullIndices.length ? nullIndices : undefined
            };
        }

        return {
            _format: 'columnar', // Added format here for consistency
            _count: numRows,
            _columns: columns
        };
    }

    _encodeBooleanColumnSparse(presentIndices, presentValues) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        const n = presentValues.length;
        const bits = new Uint8Array(Math.ceil(n / 8));
        for (let i = 0; i < n; i++) {
            if (presentValues[i] === true) {
                bits[Math.floor(i / 8)] |= (1 << (i % 8));
            }
        }
        return {
            _type: 'boolean',
            _present: presentIndices.slice(),
            _bits: Array.from(bits), // Convert Uint8Array to plain array for JSON
            _values: undefined,
            _dict: undefined
        };
    }

    _encodeNumericColumnSparse(presentIndices, presentValues) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        let isInteger = true;
        for (let i = 0; i < presentValues.length; i++) {
            if (!Number.isInteger(presentValues[i])) { isInteger = false; break; }
        }
        return {
            _type: isInteger ? 'int' : 'float',
            _present: presentIndices.slice(),
            _values: presentValues.slice(), // Ensure a copy
            _bits: undefined,
            _dict: undefined
        };
    }

    _encodeStringColumnSparse(presentIndices, presentValues) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        const uniq = new Map();
        let next = 0;
        const idx = new Array(presentValues.length);
        const stringValues = presentValues.map(v => String(v)); // Ensure strings

        for (let i = 0; i < stringValues.length; i++) {
            const s = stringValues[i];
            let code = uniq.get(s);
            if (code === undefined) { code = next++; uniq.set(s, code); }
            idx[i] = code;
        }
        // Heuristic: Use dictionary if size is less than half the original values count
        const useDict = uniq.size < presentValues.length * 0.5;

        if (useDict) {
            return {
                _type: 'dict',
                _present: presentIndices.slice(),
                _values: idx, // indices into dict
                _dict: Array.from(uniq.keys()),
                _bits: undefined
            };
        }
        return {
            _type: 'string',
            _present: presentIndices.slice(),
            _values: stringValues, // Use the stringified values
            _dict: undefined,
            _bits: undefined
        };
    }

    /**
     * Encodes an array of row objects into the sparse columnar format.
     * @param {object[]} rows - Array of row objects.
     * @param {boolean} [includeSchema=false] - Whether to include a basic inferred schema.
     * @returns {object} - The encoded payload.
     */
    encodeDataFrame(rows, includeSchema = false) {
        if (!rows || rows.length === 0) {
            return { _format: 'empty', _count: 0, _version: OptimizedColumnarCodec.ENCODING_VERSION };
        }
        if (rows.length === 1) {
            return {
                _format: 'single',
                _data: rows[0], // Assuming row[0] is not null/undefined
                _version: OptimizedColumnarCodec.ENCODING_VERSION
            };
        }

        const columnarData = this._toColumnar(rows);

        // BUG-2 FIX: _toColumnar returns `_columns`, not `columns`
        const result = {
            _format: 'columnar',
            _count: columnarData._count,
            _columns: columnarData._columns,
            _version: OptimizedColumnarCodec.ENCODING_VERSION
        };

        if (includeSchema) {
            const keys = columnarData._columns ? Object.keys(columnarData._columns) : [];
            if (keys.length > 0) {
                result._schema = this._inferSchema(rows, keys);
                // Optionally add detailed schema if needed
                // result._schema_detail = ... ;
            } else {
                result._schema = {}; // Handle case where no columns were encoded
            }
        }

        return result;
    }


    // --- Other helper methods (like _inferSchema, _analyzeColumnType, etc.) ---
    // Keep these as they are, they are mostly used by the encoding part or
    // potentially for schema information if needed on the client.
    _analyzeColumnType(values, nullCount) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        let isBoolean = true, isInteger = true, isFloat = true, isDateTime = true;
        let minInt = Infinity, maxInt = -Infinity;

        for (const val of values) {
            if (val === null || val === undefined) continue;
            const type = typeof val;
            if (type !== 'boolean') isBoolean = false;
            // Check if it's a number, excluding NaN and Infinity
            if (type === 'number' && Number.isFinite(val)) {
                if (!Number.isInteger(val)) isInteger = false;
                if (isInteger) {
                    minInt = Math.min(minInt, val);
                    maxInt = Math.max(maxInt, val);
                }
            } else { // Not a finite number
                isInteger = false;
                isFloat = false; // Also mark float as false if not a finite number
            }
            // Check for Date objects or valid date strings
            if (!(val instanceof Date) && !this._isDateString(val)) {
                isDateTime = false;
            }
        }

        const nonNullCount = values.length; // Already filtered for null/undefined

        // Prioritize types: boolean -> integer -> float -> datetime -> string
        if (isBoolean && nonNullCount > 0) return { type: 'boolean' };
        if (isInteger && nonNullCount > 0) return { type: 'integer', min: minInt, max: maxInt };
        // Check isFloat *after* integer, as integers are also floats conceptually
        if (isFloat && nonNullCount > 0) return { type: 'float' };
        if (isDateTime && nonNullCount > 0) return { type: 'datetime' };
        // Default to string if no other type fits or if values are empty
        return { type: 'string' };
    }

    _isDateString(val) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        if (typeof val !== 'string' || val.length < 8) return false; // Basic length check
        // Try parsing - robust but potentially slow for many checks
        const date = new Date(val);
        // Check if the parsed date is valid AND if the original string likely represented a date
        // (e.g., avoids parsing "12345678" as a date)
        return !isNaN(date.getTime()) && (val.includes('-') || val.includes('/') || val.includes(':') || val.toUpperCase().includes('T'));
    }

    _inferSchema(rows, keys) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        const schema = {};
        // Take a larger sample if possible, up to 100 rows
        const sampleSize = Math.min(100, rows.length);
        const sample = rows.slice(0, sampleSize);

        for (const key of keys) {
            // Extract values *only from the sample* for performance
            const values = [];
            for(let i=0; i<sampleSize; ++i) {
                const row = sample[i];
                if (row && typeof row === 'object' && Object.prototype.hasOwnProperty.call(row, key)) {
                    const val = row[key];
                    // Filter out only null/undefined for type analysis
                    if (val !== null && val !== undefined) {
                        values.push(val);
                    }
                }
            }

            if (values.length === 0) {
                // If no non-null values in sample, try full scan? Or default?
                // Let's default to 'unknown' for performance.
                // A full scan could be added if more accuracy is needed.
                schema[key] = 'unknown';
                continue;
            }
            const typeInfo = this._analyzeColumnType(values, 0); // nullCount is 0 as we pre-filtered
            schema[key] = typeInfo.type;
        }
        return schema;
    }

    // --- Base64 helpers (if needed for legacy or binary subtypes) ---
    _arrayBufferToBase64(buffer) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        const bytes = new Uint8Array(buffer);
        let binary = '';
        const len = bytes.byteLength;
        for (let i = 0; i < len; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        // Check if btoa is available (might not be in Node.js without polyfill/module)
        if (typeof btoa === 'function') {
            return btoa(binary);
        } else if (typeof Buffer === 'function') { // Node.js environment
            return Buffer.from(binary, 'binary').toString('base64');
        } else {
            console.error("btoa function not available. Cannot encode ArrayBuffer to Base64.");
            return ""; // Or throw error
        }
    }

    _base64ToArrayBuffer(base64) {
        // ... (Keep implementation from original optimizedColumnarCodec.txt) ...
        let binary;
        // Check if atob is available
        if (typeof atob === 'function') {
            binary = atob(base64);
        } else if (typeof Buffer === 'function') { // Node.js environment
            binary = Buffer.from(base64, 'base64').toString('binary');
        } else {
            console.error("atob function not available. Cannot decode Base64 to ArrayBuffer.");
            return new ArrayBuffer(0); // Or throw error
        }

        const len = binary.length;
        const bytes = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        return bytes.buffer;
    }

} // End of class OptimizedColumnarCodec


