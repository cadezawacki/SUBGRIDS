
import polars as pl
import numpy as np
import msgspec
import zstandard as zstd
from dataclasses import dataclass
import base64
from typing import Any, Dict, List, Optional, Iterable, Tuple
from app.helpers.type_helpers import ensure_list
from app.logs.logging import log

_NP_BOOL_TYPES = (bool, np.bool_)
_MAX_RLE_TOTAL_ELEMENTS = 10_000_000

def _series_from_numeric(col_name: str, values: List[Any], prefer_int: bool):
    if not values:
        return pl.Series(col_name, [], dtype=pl.Float64 if not prefer_int else pl.Int32, strict=False)

    def _finite_nums(seq):
        for v in seq:
            if v is None:
                continue
            yield v

    if prefer_int:
        if any(isinstance(v, (float, np.floating)) for v in values if v is not None):
            if all((isinstance(v, (int, np.integer)) or (isinstance(v, (float, np.floating)) and float(v).is_integer()))
                   for v in values if v is not None):
                coerced = [int(v) if isinstance(v, (float, np.floating)) else v for v in values]
                try:
                    vmin = min(_finite_nums(coerced)); vmax = max(_finite_nums(coerced))
                    if (vmin >= 0) and (vmax > np.iinfo(np.int64).max) and (vmax <= np.iinfo(np.uint64).max):
                        return pl.Series(col_name, coerced, dtype=pl.UInt64, strict=False)
                except ValueError:
                    pass
                return pl.Series(col_name, coerced, dtype=pl.Int64, strict=False)
            return pl.Series(col_name, values, dtype=pl.Float64, strict=False)

        try:
            vmin = min(_finite_nums(values)); vmax = max(_finite_nums(values))
            if (vmin >= 0) and (np.iinfo(np.int64).max < vmax <= np.iinfo(np.uint64).max):
                return pl.Series(col_name, values, dtype=pl.UInt64, strict=False)
        except ValueError:
            pass
        return pl.Series(col_name, values, dtype=pl.Int64, strict=False)

    if any(isinstance(v, (float, np.floating)) for v in values if v is not None):
        return pl.Series(col_name, values, dtype=pl.Float64, strict=False)
    return pl.Series(col_name, values, dtype=pl.Int32, strict=False)

def _rle_encode_indices(indices: List[int]) -> List[int]:
    if not indices:
        return []
    start = last = indices[0]
    out: List[int] = []
    for idx in indices[1:]:
        if idx == last + 1:
            last = idx
        else:
            out.extend([start, last - start + 1])
            start = last = idx
    out.extend([start, last - start + 1])
    return out

def _rle_decode_indices(rle: List[int]) -> List[int]:
    out: List[int] = []
    if not rle or (len(rle) % 2 != 0):
        return out
    for i in range(0, len(rle), 2):
        start = int(rle[i]); count = int(rle[i + 1])
        if count > 0:
            if len(out) + count > _MAX_RLE_TOTAL_ELEMENTS:
                log.warning(f"RLE decode capped at {_MAX_RLE_TOTAL_ELEMENTS} elements (requested +{count})")
                break
            out.extend(range(start, start + count))
    return out

@dataclass
class ColumnarMetadata:
    row_count: int
    schema_hash: Optional[int] = None
    compression_ratio: Optional[float] = None
    encoding_version: int = 2


class OptimizedColumnarCodec:
    ENCODING_VERSION = 2
    NULL_MARKER = -1

    def __init__(self, compression_level: int = 3):
        self.compressor = zstd.ZstdCompressor(level=compression_level)
        self.decompressor = zstd.ZstdDecompressor()

    # ---------------------------
    # Public API
    # ---------------------------

    def encode_payload(self, payload: dict, include_schema: bool = True):
        out = {}
        for action, value in payload.items():
            if isinstance(value, pl.DataFrame):
                out[action] = self.encode_dataframe(value, include_schema=include_schema)
            else:
                out[action] = value
        return out

    def encode_dataframe(self, df: pl.DataFrame, include_schema: bool = True) -> Dict[str, Any]:
        if isinstance(df, pl.LazyFrame): df = df.collect()
        if isinstance(df, pl.DataFrame) and df.is_empty():
            return {'_format': 'empty', '_count': 0, '_version': self.ENCODING_VERSION}

        if df.height == 1:
            result = {
                '_format': 'single',
                '_data': df.to_dicts()[0],
                '_version': self.ENCODING_VERSION
            }
            if include_schema:
                result['_schema'] = self._encode_schema(df.schema)
            return result

        metadata = ColumnarMetadata(row_count=df.height)
        columns_data: Dict[str, Any] = {}

        for col_name in df.columns:
            series = df[col_name]
            col_data = self._encode_column_sparse(series)
            if col_data is not None:
                columns_data[col_name] = col_data

        result = {
            '_format': 'columnar',
            '_count': metadata.row_count,
            '_columns': columns_data,
            '_version': self.ENCODING_VERSION
        }

        if include_schema:
            result['_schema'] = self._encode_schema(df.schema)
            result['_schema_detail'] = {name: str(dtype) for name, dtype in df.schema.items()}
        return result

    def decode_to_polars_partitions(self, payload: Dict[str, Any]) -> List[pl.DataFrame]:
        if not payload or payload.get('_format') != 'columnar':
            df = self.decode_to_polars(payload)
            return [] if df.is_empty() else [df]

        num_rows = int(payload.get('_count', 0))
        if num_rows <= 0:
            return []

        columns_data = payload.get('_columns', {})
        if not columns_data:
            return [pl.DataFrame([], strict=False)]

        def _norm_list(x):
            if x is None or isinstance(x, msgspec.msgpack.Ext): return []
            return list(x) if not isinstance(x, list) else x

        col_presence: Dict[str, np.ndarray] = {}
        col_types: Dict[str, str] = {}

        for col_name, col in columns_data.items():
            ctype = str(col.get('_type'))
            col_types[col_name] = ctype

            mask = np.zeros(num_rows, dtype=bool)

            if ctype == 'sparse':
                # Normalize all list fields to handle msgspec.msgpack.Ext
                present = _norm_list(col.get('_present'))
                present_rle = _norm_list(col.get('_present_rle'))
                nulls = _norm_list(col.get('_nulls'))
                nulls_rle = _norm_list(col.get('_nulls_rle'))

                # Decode RLEs then mark mask
                if present_rle:
                    present = list(present) + _rle_decode_indices(present_rle)
                if nulls_rle:
                    nulls = list(nulls) + _rle_decode_indices(nulls_rle)

                if present:
                    idx = np.fromiter((int(i) for i in present), count=len(present), dtype=np.int64)
                    mask[idx] = True
                if nulls:
                    idx = np.fromiter((int(i) for i in nulls), count=len(nulls), dtype=np.int64)
                    mask[idx] = True

            elif ctype == 'null':
                # If explicit _nulls present, mark only those; else all rows are null -> all present
                nz = _norm_list(col.get('_nulls'))
                if nz:
                    idx = np.fromiter((int(i) for i in nz), count=len(nz), dtype=np.int64)
                    mask[idx] = True
                else:
                    mask[:] = True

            else:
                # Dense/legacy: treat as fully present
                mask[:] = True

            col_presence[col_name] = mask

        # -----------------------------------------
        # Partition rows by presence signature (set)
        # Vectorized: pack per-column masks into a byte signature per row
        # -----------------------------------------
        col_names = list(columns_data.keys())
        num_cols = len(col_names)
        # Stack all presence masks into a (num_cols, num_rows) bool matrix, then
        # compute a hashable signature per row via column-index tuple.
        if num_cols > 0:
            mask_matrix = np.stack([col_presence[c] for c in col_names], axis=0)  # (num_cols, num_rows)
            # Pack bits along axis=0, then transpose to C-contiguous (row-major)
            # so that row[i].tobytes() is a contiguous memcpy, not a strided gather
            sig_per_row = np.packbits(mask_matrix, axis=0, bitorder='little')  # (ceil(num_cols/8), num_rows)
            sig_row_major = np.ascontiguousarray(sig_per_row.T)  # (num_rows, ceil(num_cols/8))
            sig_keys = [sig_row_major[i].tobytes() for i in range(num_rows)]
        else:
            sig_keys = [b''] * num_rows

        # Group rows by their packed signature
        raw_sig_to_rows: Dict[bytes, List[int]] = {}
        for i, sk in enumerate(sig_keys):
            raw_sig_to_rows.setdefault(sk, []).append(i)

        # Map packed signatures back to frozensets of column names for downstream use
        signature_to_rows: Dict[frozenset, List[int]] = {}
        for sk, row_list in raw_sig_to_rows.items():
            # Recover which columns are present from the packed bits
            bits = np.unpackbits(np.frombuffer(sk, dtype=np.uint8), bitorder='little')[:num_cols]
            present_cols = frozenset(col_names[j] for j in range(num_cols) if bits[j])
            signature_to_rows[present_cols] = row_list

        # ---------------------------------
        # Decode columns once into row-wise
        # ---------------------------------
        decoded_cache: Dict[str, List[Any]] = {}
        for col_name, col in columns_data.items():
            if isinstance(col, msgspec.msgpack.Ext):
                decoded_cache[col_name] = []
                continue
            s = self._decode_column(col_name, col, num_rows)
            decoded_cache[col_name] = s.to_list() if isinstance(s, pl.Series) else []

        def _build_df_from_parts(parts: Dict[str, List[Any]]) -> pl.DataFrame:
            series_map: Dict[str, pl.Series] = {}
            for name, values in parts.items():
                # Numeric vs non-numeric fast path (NumPy 2.0-safe)
                has_num = False
                has_non_num = False
                for v in values:
                    if v is None:
                        continue
                    if isinstance(v, (int, float, np.integer, np.floating)):
                        has_num = True
                    elif isinstance(v, _NP_BOOL_TYPES):
                        has_non_num = True
                    else:
                        has_non_num = True
                    if has_num and has_non_num:
                        break

                if has_num and not has_non_num:
                    series_map[name] = _series_from_numeric(name, values, prefer_int=False)
                else:
                    series_map[name] = pl.Series(name, values, strict=False)
            return pl.DataFrame(series_map, strict=False)

        partitions: List[pl.DataFrame] = []
        for sig, row_idx in signature_to_rows.items():
            if not sig:
                # No present columns in this group; construct empty DF with correct row count
                partitions.append(
                    pl.DataFrame({}).with_row_index(name="_r").slice(0, len(row_idx)).drop("_r")
                )
                continue
            part_cols: Dict[str, List[Any]] = {}
            # Pull only the columns that are present in this signature
            for col_name in sig:
                col_vals = decoded_cache[col_name]
                # Fast gather by Python list indexing since row_idx is small for each subgroup in practice
                part_cols[col_name] = [col_vals[i] for i in row_idx]
            partitions.append(_build_df_from_parts(part_cols))

        return partitions

    def decode_to_polars(self, payload: Dict[str, Any]) -> pl.DataFrame:
        if not payload or not isinstance(payload, dict):
            return pl.DataFrame()

        fmt = payload.get('_format')
        if fmt == 'empty':
            return pl.DataFrame()
        if fmt == 'single':
            return pl.DataFrame([payload.get('_data', {})], strict=False)
        if fmt != 'columnar':
            log.warning(f"Unknown columnar payload format: {fmt!r}")
            return pl.DataFrame([payload], strict=False)

        num_rows = payload.get('_count', 0)
        columns_data = payload.get('_columns', {})
        if num_rows == 0 or not columns_data:
            return pl.DataFrame()

        decoded: Dict[str, pl.Series] = {}
        for col_name, col_data in columns_data.items():
            local = {k: (None if isinstance(v, msgspec.msgpack.Ext) else v) for k, v in col_data.items()}
            s = self._decode_column(col_name, local, num_rows)
            if s is not None:
                decoded[col_name] = s

        return pl.DataFrame(decoded, strict=False)

    # ---------------------------
    # Encoding
    # ---------------------------

    def _encode_column_sparse(self, series: pl.Series) -> Optional[Dict[str, Any]]:
        n = len(series)
        null_mask = series.is_null()
        null_count = int(null_mask.sum())

        # Compact all-null: _count encodes the range implicitly — no indices needed
        if null_count == n:
            return {'_type': 'null', '_count': n}

        # Build present/null indices with minimal allocations via np.nonzero
        null_np = null_mask.to_numpy()
        present_indices: List[int] = np.flatnonzero(~null_np).tolist()
        null_indices: List[int] = np.flatnonzero(null_np).tolist() if null_count > 0 else []

        # Extract only non-null values (single materialization, not per-element)
        if null_count == 0:
            present_values: List[Any] = series.to_list()
        else:
            present_values = series.filter(~null_mask).to_list()

        if not present_indices and not null_indices:
            return None  # Column never appears (shouldn't happen for DataFrame)

        # Decide subtype based on present_values
        subtype, payload = self._encode_sparse_payload(present_values)

        if subtype is None:
            # Fallback generic
            return {
                '_type': 'sparse',
                '_subtype': 'string',
                '_present': present_indices,
                '_values': [None if v is None else str(v) for v in present_values],
                '_nulls': null_indices or None
            }

        payload.update({
            '_type': 'sparse',
            '_subtype': subtype,
            '_present': present_indices,
        })
        if null_indices:
            payload['_nulls'] = null_indices
        return payload

    def _encode_sparse_payload(self, present_values: List[Any]) -> Tuple[Optional[str], Dict[str, Any]]:
        if not present_values: return 'string', {'_values': []}

        # Booleans
        if all(isinstance(v, _NP_BOOL_TYPES) for v in present_values):
            arr = np.array([bool(v) for v in present_values], dtype=np.uint8)
            packed = np.packbits(arr, bitorder='little')
            return 'boolean', {'_bits': packed.tolist()}

        # Pure numeric (excluding bools)
        all_num = True
        is_int = True
        for v in present_values:
            if isinstance(v, _NP_BOOL_TYPES):
                all_num = False
                break
            if not isinstance(v, (int, float, np.integer, np.floating)):
                all_num = False
                break
            if is_int and not float(v).is_integer():
                is_int = False
        if all_num:
            return ('int' if is_int else 'float',
                    {'_values': [int(v) if is_int else float(v) for v in present_values]})

        # Strings / mixed -> try dictionary encoding
        as_str = ["" if v is None else str(v) for v in present_values]
        uniq: Dict[str, int] = {}
        idx: List[int] = []
        for s in as_str:
            idx.append(uniq.setdefault(s, len(uniq)))
        if len(uniq) < (len(as_str) * 0.5):
            return 'dict', {'_values': idx, '_dict': list(uniq.keys())}
        return 'string', {'_values': as_str}

    # ---------------------------
    # Decoding
    # ---------------------------

    def _decode_column(self, col_name: str, col_data: Dict[str, Any], num_rows: int) -> Optional[pl.Series]:
        ctype = col_data.get('_type')

        # Compact/legacy all-null
        if ctype == 'null':
            nulls = col_data.get('_nulls')
            if isinstance(nulls, msgspec.msgpack.Ext): return None
            # No _nulls field (or _count == num_rows) → all rows are null
            if nulls is None:
                return pl.Series(col_name, [None] * num_rows, dtype=pl.Null, strict=False)
            if isinstance(nulls, list) and len(nulls) != num_rows:
                # Mixed: those indices are null; others "missing" => None in DF
                out = [None] * num_rows
                for i in nulls:
                    if 0 <= i < num_rows:
                        out[i] = None
                return pl.Series(col_name, out, strict=False)
            return pl.Series(col_name, [None] * num_rows, dtype=pl.Null, strict=False)

        # New sparse envelope
        if ctype == 'sparse':
            return self._decode_sparse(col_name, col_data, num_rows)

        # Legacy dense types
        raw_nulls = col_data.get('_nulls')
        if isinstance(raw_nulls, msgspec.msgpack.Ext): return None
        null_indices = set(raw_nulls) if isinstance(raw_nulls, list) else set()

        if ctype == 'boolean':
            return self._decode_legacy_boolean(col_name, col_data, num_rows, null_indices)
        if ctype in ('int', 'float'):
            return self._decode_legacy_numeric(col_name, col_data, num_rows, null_indices)
        if ctype == 'dict':
            return self._decode_legacy_dict(col_name, col_data, num_rows, null_indices)
        if ctype == 'string':
            return self._decode_legacy_string(col_name, col_data, num_rows, null_indices)
        if ctype == 'numeric':
            return self._decode_numeric_base64(col_name, col_data, num_rows, null_indices)
        if ctype == 'categorical':
            return self._decode_categorical_legacy(col_name, col_data, num_rows, null_indices)
        if ctype == 'generic':
            return self._decode_generic_legacy(col_name, col_data, num_rows, null_indices)
        log.warning(f"Unknown column type {ctype!r} for column {col_name!r}, dropping")
        return None

    def _decode_sparse(self, col_name: str, col_data: Dict[str, Any], num_rows: int) -> pl.Series:
        subtype = col_data.get('_subtype')
        present = col_data.get('_present')
        present_rle = col_data.get('_present_rle')
        raw_nulls = col_data.get('_nulls')
        nulls_rle = col_data.get('_nulls_rle')

        # Filter out msgpack.Ext and normalize
        subtype = [] if isinstance(subtype, msgspec.msgpack.Ext) else subtype

        def _norm(x):
            if isinstance(x, msgspec.msgpack.Ext) or x is None:
                return []
            return x

        present = _norm(present)
        raw_nulls = _norm(raw_nulls)
        if present_rle:
            present = _rle_decode_indices(_norm(present_rle))
        if nulls_rle:
            raw_nulls = _rle_decode_indices(_norm(nulls_rle))

        nulls = set(raw_nulls)
        out: List[Any] = [None] * num_rows

        if subtype == 'boolean':
            bits = col_data.get('_bits', [])
            bits = [] if isinstance(bits, msgspec.msgpack.Ext) else bits
            bits = np.frombuffer(np.asarray(bits, dtype=np.uint8).tobytes(), dtype=np.uint8)
            unpacked = np.unpackbits(bits, bitorder='little')[:len(present)].astype(bool)
            for j, row_idx in enumerate(present):
                if 0 <= row_idx < num_rows and row_idx not in nulls:
                    out[row_idx] = bool(unpacked[j])
            return pl.Series(col_name, out, strict=False)

        if subtype in ('int', 'float', 'string'):
            vals = col_data.get('_values', [])
            vals = [] if isinstance(vals, msgspec.msgpack.Ext) else vals
            for j, row_idx in enumerate(present):
                if 0 <= row_idx < num_rows and row_idx not in nulls:
                    out[row_idx] = vals[j] if j < len(vals) else None
            return _series_from_numeric(col_name, out, prefer_int=(subtype == 'int')) if subtype in ('int', 'float') else pl.Series(col_name, out, strict=False)

        if subtype == 'dict':
            vals = col_data.get('_values', [])
            vals = [] if isinstance(vals, msgspec.msgpack.Ext) else vals
            dictionary = col_data.get('_dict', [])
            dictionary = [] if isinstance(dictionary, msgspec.msgpack.Ext) else dictionary
            dlen = len(dictionary)
            for j, row_idx in enumerate(present):
                if 0 <= row_idx < num_rows and row_idx not in nulls:
                    code = vals[j] if j < len(vals) else self.NULL_MARKER
                    out[row_idx] = dictionary[code] if (isinstance(code, int) and 0 <= code < dlen) else None
            return pl.Series(col_name, out, strict=False)
        return pl.Series(col_name, out, strict=False)

    # ---------------------------
    # Legacy decoders (kept)
    # ---------------------------

    def _decode_legacy_boolean(self, col_name: str, col_data: Dict[str, Any], num_rows: int, null_indices: set) -> pl.Series:
        bits_bytes_b64 = col_data.get('_bits', '')
        if isinstance(bits_bytes_b64, str):
            bits_bytes = base64.b64decode(bits_bytes_b64) if bits_bytes_b64 else b''
        else:
            # Some older payloads might already be an array of ints
            bits_bytes = np.asarray(bits_bytes_b64, dtype=np.uint8).tobytes()
        bits = np.frombuffer(bits_bytes, dtype=np.uint8)
        values = np.unpackbits(bits, bitorder='little')[:num_rows].astype(bool)
        out = [None if i in null_indices else bool(values[i]) for i in range(num_rows)]
        return pl.Series(col_name, out, strict=False)

    def _decode_legacy_numeric(self, col_name: str, col_data: Dict[str, Any], num_rows: int,
                               null_indices: set) -> pl.Series:
        vals = col_data.get('_values') or []
        out: List[Any] = [None] * num_rows
        n = min(len(vals), num_rows)
        for i in range(n):
            if i in null_indices:
                out[i] = None
            else:
                out[i] = vals[i]
        # Many legacy payloads imply int; only keep Int64 if safe
        return _series_from_numeric(col_name, out, prefer_int=True)

    def _decode_legacy_dict(self, col_name: str, col_data: Dict[str, Any], num_rows: int, null_indices: set) -> pl.Series:
        dictionary = col_data.get('_dict', [])
        indices = col_data.get('_indices', [])
        out: List[Any] = []
        for i in range(num_rows):
            if i in null_indices or (i < len(indices) and indices[i] == self.NULL_MARKER):
                out.append(None)
            else:
                idx = indices[i] if i < len(indices) else self.NULL_MARKER
                out.append(None if idx == self.NULL_MARKER else dictionary[idx])
        return pl.Series(col_name, out, strict=False)

    def _decode_legacy_string(self, col_name: str, col_data: Dict[str, Any], num_rows: int, null_indices: set) -> pl.Series:
        vals = col_data.get('_values', [])
        out = [None if i in null_indices else (vals[i] if i < len(vals) else None) for i in range(num_rows)]
        return pl.Series(col_name, out, strict=False)

    def _decode_numeric_base64(self, col_name: str, col_data: Dict[str, Any], num_rows: int,
                               null_indices: set) -> pl.Series:
        dtype_str = col_data.get('_dtype', 'f64')
        base64_str = col_data.get('_values', '')
        values_bytes = base64.b64decode(base64_str) if base64_str else b''
        dtype_map = {
            'i8': np.int8, 'i16': np.int16, 'i32': np.int32, 'i64': np.int64,
            'u8': np.uint8, 'u16': np.uint16, 'u32': np.uint32, 'u64': np.uint64,
            'f32': np.float32, 'f64': np.float64
        }
        np_dtype = dtype_map.get(dtype_str, np.float64)
        arr = np.frombuffer(values_bytes, dtype=np_dtype)

        out = arr.astype(object).tolist()
        if len(out) < num_rows:
            out += [None] * (num_rows - len(out))
        else:
            out = out[:num_rows]

        if null_indices:
            for i in null_indices:
                if 0 <= i < num_rows:
                    out[i] = None

        # If dtype was integer-like, prefer_int=True, else allow float
        prefer_int = dtype_str in ('i8', 'i16', 'i32', 'u8', 'u16', 'u32') #'i64','u64'
        return _series_from_numeric(col_name, out, prefer_int=prefer_int)

    def _decode_categorical_legacy(self, col_name: str, col_data: Dict[str, Any], num_rows: int, null_indices: set) -> pl.Series:
        categories = col_data.get('_categories', [])
        codes_bytes_b64 = col_data.get('_codes', '')
        codes_bytes = base64.b64decode(codes_bytes_b64) if isinstance(codes_bytes_b64, str) else b''
        codes = np.frombuffer(codes_bytes, dtype=np.int32)
        out = []
        for i in range(num_rows):
            if i in null_indices:
                out.append(None)
            else:
                code = int(codes[i]) if i < len(codes) else self.NULL_MARKER
                out.append(None if code == self.NULL_MARKER else categories[code])
        return pl.Series(col_name, out, strict=False)

    def _decode_generic_legacy(self, col_name: str, col_data: Dict[str, Any], num_rows: int, null_indices: set) -> pl.Series:
        vals = col_data.get('_values', [])
        out = [None if i in null_indices else (vals[i] if i < len(vals) else None) for i in range(num_rows)]
        return pl.Series(col_name, out, strict=False)

    # ---------------------------
    # Schema helpers
    # ---------------------------

    def _encode_schema(self, schema: Dict[str, pl.DataType]) -> Dict[str, Any]:
        return {col: self._encode_dtype(dtype) for col, dtype in schema.items()}

    def _encode_dtype(self, dtype: pl.DataType) -> str:
        dtype_map = {
            pl.Int8: 'i8', pl.Int16: 'i16', pl.Int32: 'i32',
            pl.Int64: 'i64',
            pl.UInt8: 'u8', pl.UInt16: 'u16', pl.UInt32: 'u32',
            pl.UInt64: 'u64',
            pl.Float32: 'f32', pl.Float64: 'f64',
            pl.Boolean: 'bool', pl.String: 'str',
            pl.Date: 'date', pl.Datetime: 'datetime',
            pl.Categorical: 'cat'
        }
        return dtype_map.get(dtype, 'unknown')

