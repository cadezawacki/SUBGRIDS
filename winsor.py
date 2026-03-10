from __future__ import annotations

import concurrent.futures
from enum import Enum
from typing import List, Optional, Union

import numpy as np
import polars as pl
from scipy import stats as scipy_stats


class OutlierMethod(Enum):
    IQR = "iqr"
    ZSCORE = "zscore"
    MAD = "mad"
    PERCENTILE = "percentile"
    GRUBBS = "grubbs"


_DEFAULT_METHOD = OutlierMethod.MAD
_DEFAULT_SENSITIVITY = 2.5
_DEFAULT_PERCENTILE_BOUNDS = (0.05, 0.95)
_DEFAULT_SYMMETRIC = True
_DEFAULT_NULLS_AS_ZERO = False
_DEFAULT_ZEROS_AS_NULL = False
_DEFAULT_FILTER_NULLS = True
_DEFAULT_AUTO_RESCALE = False
_DEFAULT_WEIGHT_COL: Optional[str] = None

_EXECUTOR = concurrent.futures.ThreadPoolExecutor()

FrameType = Union[pl.DataFrame, pl.LazyFrame]


def _ensure_lazy(df: FrameType) -> pl.LazyFrame:
    if isinstance(df, pl.DataFrame):
        return df.lazy()
    return df


def _preprocess_columns(
    lf: pl.LazyFrame,
    columns: List[str],
    *,
    nulls_as_zero: bool,
    zeros_as_null: bool,
    filter_nulls: bool,
) -> pl.LazyFrame:
    exprs = []
    for c in columns:
        col_expr = pl.col(c).cast(pl.Float64)
        if zeros_as_null:
            col_expr = pl.when(col_expr == 0.0).then(None).otherwise(col_expr)
        if nulls_as_zero and (not zeros_as_null):
            col_expr = col_expr.fill_null(0.0)
        exprs.append(col_expr.alias(c))
    return lf.with_columns(exprs)


def _detect_outliers_1d(
    arr: np.ndarray,
    *,
    method: OutlierMethod,
    sensitivity: float,
    percentile_bounds: tuple[float, float],
    symmetric: bool,
) -> np.ndarray:
    mask = np.isnan(arr)
    valid = arr[~mask]
    n = valid.size
    if n < 3:
        return np.zeros(arr.shape, dtype=np.bool_)

    if method == OutlierMethod.IQR:
        q1, q3 = np.nanpercentile(valid, [25, 75])
        iqr = q3 - q1
        lower = q1 - (sensitivity * iqr)
        upper = q3 + (sensitivity * iqr)
        return (~mask) & ((arr < lower) | (arr > upper))

    elif method == OutlierMethod.ZSCORE:
        mu = np.nanmean(valid)
        sigma = np.nanstd(valid, ddof=1) if n > 1 else 1.0
        if sigma < 1e-15:
            return np.zeros(arr.shape, dtype=np.bool_)
        z = np.abs(arr - mu) / sigma
        return (~mask) & (z > sensitivity)

    elif method == OutlierMethod.MAD:
        med = np.nanmedian(valid)
        abs_dev = np.abs(valid - med)
        mad = np.nanmedian(abs_dev)
        if mad < 1e-15:
            mad = np.nanmean(abs_dev) * 1.2533
            if mad < 1e-15:
                return np.zeros(arr.shape, dtype=np.bool_)
        modified_z = 0.6745 * np.abs(arr - med) / mad
        return (~mask) & (modified_z > sensitivity)

    elif method == OutlierMethod.PERCENTILE:
        lo, hi = percentile_bounds
        lower = np.nanpercentile(valid, lo * 100)
        upper = np.nanpercentile(valid, hi * 100)
        return (~mask) & ((arr < lower) | (arr > upper))

    elif method == OutlierMethod.GRUBBS:
        if n < 6:
            return _detect_outliers_1d(
                arr, method=OutlierMethod.MAD, sensitivity=sensitivity,
                percentile_bounds=percentile_bounds, symmetric=symmetric,
            )
        mu = np.nanmean(valid)
        sigma = np.nanstd(valid, ddof=1)
        if sigma < 1e-15:
            return np.zeros(arr.shape, dtype=np.bool_)
        g_scores = np.abs(arr - mu) / sigma
        t_crit_sq = scipy_stats.t.ppf(1 - (0.05 / (2 * n)), n - 2) ** 2
        g_crit = ((n - 1) / np.sqrt(n)) * np.sqrt(t_crit_sq / ((n - 2) + t_crit_sq))
        return (~mask) & (g_scores > g_crit)

    return np.zeros(arr.shape, dtype=np.bool_)


def _detect_outliers_2d_rowwise(
    data: np.ndarray,
    *,
    method: OutlierMethod,
    sensitivity: float,
    percentile_bounds: tuple[float, float],
    symmetric: bool,
) -> np.ndarray:
    n_rows, n_cols = data.shape
    nan_mask = np.isnan(data)
    valid_count = np.sum(~nan_mask, axis=1)
    outlier_mask = np.zeros_like(data, dtype=np.bool_)
    enough_data = valid_count >= 3

    if not np.any(enough_data):
        return outlier_mask

    if method == OutlierMethod.MAD:
        med = np.nanmedian(data, axis=1, keepdims=True)
        abs_dev_all = np.where(nan_mask, np.nan, np.abs(data - med))
        mad = np.nanmedian(abs_dev_all, axis=1, keepdims=True)
        zero_mad = (mad < 1e-15).ravel()
        if np.any(zero_mad & enough_data):
            mean_dev = np.nanmean(abs_dev_all, axis=1, keepdims=True) * 1.2533
            mad = np.where(mad < 1e-15, mean_dev, mad)
        safe_mad = np.where(mad < 1e-15, 1.0, mad)
        modified_z = 0.6745 * np.abs(data - med) / safe_mad
        is_outlier = modified_z > sensitivity
        still_zero = (mad < 1e-15).ravel()
        if np.any(still_zero):
            is_outlier[still_zero, :] = False
        outlier_mask = (~nan_mask) & is_outlier & enough_data[:, np.newaxis]

    elif method == OutlierMethod.IQR:
        q1 = np.nanpercentile(data, 25, axis=1, keepdims=True)
        q3 = np.nanpercentile(data, 75, axis=1, keepdims=True)
        iqr = q3 - q1
        lower = q1 - (sensitivity * iqr)
        upper = q3 + (sensitivity * iqr)
        outlier_mask = (~nan_mask) & ((data < lower) | (data > upper)) & enough_data[:, np.newaxis]

    elif method == OutlierMethod.ZSCORE:
        mu = np.nanmean(data, axis=1, keepdims=True)
        with np.errstate(invalid="ignore"):
            sigma = np.nanstd(data, axis=1, ddof=1, keepdims=True)
        safe_sigma = np.where(sigma < 1e-15, 1.0, sigma)
        z = np.abs(data - mu) / safe_sigma
        zero_sigma = (sigma < 1e-15).ravel()
        is_outlier = z > sensitivity
        if np.any(zero_sigma):
            is_outlier[zero_sigma, :] = False
        outlier_mask = (~nan_mask) & is_outlier & enough_data[:, np.newaxis]

    elif method == OutlierMethod.PERCENTILE:
        lo, hi = percentile_bounds
        lower = np.nanpercentile(data, lo * 100, axis=1, keepdims=True)
        upper = np.nanpercentile(data, hi * 100, axis=1, keepdims=True)
        outlier_mask = (~nan_mask) & ((data < lower) | (data > upper)) & enough_data[:, np.newaxis]

    elif method == OutlierMethod.GRUBBS:
        need_fallback = enough_data & (valid_count < 6)
        can_grubbs = enough_data & (valid_count >= 6)

        if np.any(need_fallback):
            fallback = _detect_outliers_2d_rowwise(
                data[need_fallback],
                method=OutlierMethod.MAD, sensitivity=sensitivity,
                percentile_bounds=percentile_bounds, symmetric=symmetric,
            )
            outlier_mask[need_fallback] = fallback

        if np.any(can_grubbs):
            sub = data[can_grubbs]
            sub_nan = np.isnan(sub)
            mu = np.nanmean(sub, axis=1, keepdims=True)
            sigma = np.nanstd(sub, axis=1, ddof=1, keepdims=True)
            safe_sigma = np.where(sigma < 1e-15, 1.0, sigma)
            g_scores = np.abs(sub - mu) / safe_sigma
            ns = np.sum(~sub_nan, axis=1)
            g_crits = np.empty(ns.shape)
            for idx, ni in enumerate(ns):
                if ni < 3 or sigma.ravel()[idx] < 1e-15:
                    g_crits[idx] = np.inf
                else:
                    t_sq = scipy_stats.t.ppf(1 - (0.05 / (2 * ni)), ni - 2) ** 2
                    g_crits[idx] = ((ni - 1) / np.sqrt(ni)) * np.sqrt(t_sq / ((ni - 2) + t_sq))
            is_outlier = g_scores > g_crits[:, np.newaxis]
            zero_sig = (sigma < 1e-15).ravel()
            if np.any(zero_sig):
                is_outlier[zero_sig, :] = False
            outlier_mask[can_grubbs] = (~sub_nan) & is_outlier

    return outlier_mask


def _try_rescale_value(
    val: float,
    reference_median: float,
    reference_mad: float,
) -> float:
    if (np.isnan(val)) or (np.isnan(reference_median)) or (reference_median == 0.0):
        return val
    ratio = val / reference_median
    if (80 < ratio < 120) or (-120 < ratio < -80):
        candidate = val / 100.0
        if reference_mad > 1e-15:
            if abs(candidate - reference_median) / reference_mad < 3.5:
                return candidate
        elif abs(candidate - reference_median) < (abs(reference_median) * 0.5):
            return candidate
    elif (0.008 < ratio < 0.012) or (-0.012 < ratio < -0.008):
        candidate = val * 100.0
        if reference_mad > 1e-15:
            if abs(candidate - reference_median) / reference_mad < 3.5:
                return candidate
        elif abs(candidate - reference_median) < (abs(reference_median) * 0.5):
            return candidate
    return val


def _auto_rescale_array(arr: np.ndarray) -> np.ndarray:
    valid = arr[~np.isnan(arr)]
    if valid.size < 3:
        return arr.copy()
    med = np.nanmedian(valid)
    abs_dev = np.abs(valid - med)
    mad = np.nanmedian(abs_dev)
    if mad < 1e-15:
        mad = np.nanmean(abs_dev) * 1.2533
    result = arr.copy()
    for i in range(result.shape[0]):
        if not np.isnan(result[i]):
            result[i] = _try_rescale_value(result[i], med, mad)
    return result


def _auto_rescale_2d_rowwise(data: np.ndarray) -> np.ndarray:
    result = data.copy()
    nan_mask = np.isnan(data)
    valid_count = np.sum(~nan_mask, axis=1)
    needs_rescale = valid_count >= 3
    if not np.any(needs_rescale):
        return result
    for i in np.where(needs_rescale)[0]:
        result[i] = _auto_rescale_array(result[i])
    return result


def _winsorize_clamp_2d(
    data: np.ndarray,
    outlier_mask: np.ndarray,
) -> np.ndarray:
    nan_mask = np.isnan(data)
    safe_data = np.where(nan_mask | outlier_mask, np.nan, data)
    import warnings
    with warnings.catch_warnings(), np.errstate(invalid="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        row_min = np.nanmin(safe_data, axis=1, keepdims=True)
        row_max = np.nanmax(safe_data, axis=1, keepdims=True)
    all_nan_rows = np.all(np.isnan(safe_data), axis=1)
    row_min[all_nan_rows, :] = np.nan
    row_max[all_nan_rows, :] = np.nan
    clamped = np.where(
        outlier_mask & (~nan_mask),
        np.clip(data, row_min, row_max),
        data,
    )
    return clamped


def _weighted_nanmean_rows(
    data: np.ndarray,
    weights: Optional[np.ndarray],
) -> np.ndarray:
    nan_mask = np.isnan(data)
    if weights is None:
        import warnings
        with warnings.catch_warnings(), np.errstate(invalid="ignore"):
            warnings.simplefilter("ignore", RuntimeWarning)
            return np.nanmean(data, axis=1)
    w = np.where(nan_mask | np.isnan(weights), 0.0, weights)
    vals = np.where(nan_mask, 0.0, data)
    w_sum = np.sum(w, axis=1)
    safe_w_sum = np.where(w_sum < 1e-15, np.nan, w_sum)
    return np.sum(vals * w, axis=1) / safe_w_sum


def horizontal_winsor(
    df: FrameType,
    columns: List[str],
    *,
    method: OutlierMethod = _DEFAULT_METHOD,
    sensitivity: float = _DEFAULT_SENSITIVITY,
    percentile_bounds: tuple[float, float] = _DEFAULT_PERCENTILE_BOUNDS,
    symmetric: bool = _DEFAULT_SYMMETRIC,
    auto_rescale: bool = _DEFAULT_AUTO_RESCALE,
    weight_col: Optional[str] = _DEFAULT_WEIGHT_COL,
    nulls_as_zero: bool = _DEFAULT_NULLS_AS_ZERO,
    zeros_as_null: bool = _DEFAULT_ZEROS_AS_NULL,
    filter_nulls: bool = _DEFAULT_FILTER_NULLS,
    result_alias: str = "h_winsor_mean",
    n_threads: int = 2,
):
    lf = _ensure_lazy(df)
    lf = _preprocess_columns(
        lf, columns,
        nulls_as_zero=nulls_as_zero,
        zeros_as_null=zeros_as_null,
        filter_nulls=filter_nulls,
    )
    collected = lf.select(columns).collect()
    data = collected.to_numpy(writable=True).astype(np.float64)

    weight_data = None
    if weight_col is not None:
        w_series = _ensure_lazy(df).select(pl.col(weight_col).cast(pl.Float64)).collect()
        w_arr = w_series.to_numpy(writable=True).astype(np.float64).ravel()
        weight_data = np.tile(w_arr[:, np.newaxis], (1, data.shape[1]))

    if auto_rescale:
        data = _auto_rescale_2d_rowwise(data)

    outlier_mask = _detect_outliers_2d_rowwise(
        data, method=method, sensitivity=sensitivity,
        percentile_bounds=percentile_bounds, symmetric=symmetric,
    )
    clamped = _winsorize_clamp_2d(data, outlier_mask)
    results = _weighted_nanmean_rows(clamped, weight_data)

    result_series = pl.Series(result_alias, results)
    return _ensure_lazy(df).with_columns(result_series.alias(result_alias))


def _winsorize_column_1d(
    arr: np.ndarray,
    *,
    method: OutlierMethod,
    sensitivity: float,
    percentile_bounds: tuple[float, float],
    symmetric: bool,
    auto_rescale: bool,
) -> tuple[float, np.ndarray]:
    if auto_rescale:
        arr = _auto_rescale_array(arr)
    outlier_mask = _detect_outliers_1d(
        arr, method=method, sensitivity=sensitivity,
        percentile_bounds=percentile_bounds, symmetric=symmetric,
    )
    valid_mask = (~np.isnan(arr)) & (~outlier_mask)
    valid_vals = arr[valid_mask]
    if valid_vals.size == 0:
        return np.nan, outlier_mask
    lower_bound = np.min(valid_vals)
    upper_bound = np.max(valid_vals)
    clipped = np.where(
        outlier_mask & (~np.isnan(arr)),
        np.clip(arr, lower_bound, upper_bound),
        arr,
    )
    final_vals = clipped[~np.isnan(clipped)]
    if final_vals.size == 0:
        return np.nan, outlier_mask
    return float(np.nanmean(final_vals)), outlier_mask


def vertical_winsor(
    df: FrameType,
    columns: List[str],
    *,
    method: OutlierMethod = _DEFAULT_METHOD,
    sensitivity: float = _DEFAULT_SENSITIVITY,
    percentile_bounds: tuple[float, float] = _DEFAULT_PERCENTILE_BOUNDS,
    symmetric: bool = _DEFAULT_SYMMETRIC,
    auto_rescale: bool = _DEFAULT_AUTO_RESCALE,
    nulls_as_zero: bool = _DEFAULT_NULLS_AS_ZERO,
    zeros_as_null: bool = _DEFAULT_ZEROS_AS_NULL,
    filter_nulls: bool = _DEFAULT_FILTER_NULLS,
    result_suffix: str = "_v_winsor",
    n_threads: int = 2,
):
    lf = _ensure_lazy(df)
    lf = _preprocess_columns(
        lf, columns,
        nulls_as_zero=nulls_as_zero,
        zeros_as_null=zeros_as_null,
        filter_nulls=filter_nulls,
    )
    collected = lf.select(columns).collect()

    def _process_col(col_name: str) -> tuple[str, float]:
        arr = collected.get_column(col_name).to_numpy(writable=True).astype(np.float64)
        mean_val, _ = _winsorize_column_1d(
            arr, method=method, sensitivity=sensitivity,
            percentile_bounds=percentile_bounds, symmetric=symmetric,
            auto_rescale=auto_rescale,
        )
        return col_name, mean_val

    if len(columns) > 2 and n_threads > 1:
        futures = {_EXECUTOR.submit(_process_col, c): c for c in columns}
        results_map = {}
        for f in concurrent.futures.as_completed(futures):
            col_name, mean_val = f.result()
            results_map[col_name] = mean_val
    else:
        results_map = {}
        for c in columns:
            col_name, mean_val = _process_col(c)
            results_map[col_name] = mean_val

    new_cols = [
        pl.lit(results_map[c]).cast(pl.Float64).alias(f"{c}{result_suffix}")
        for c in columns
    ]
    return _ensure_lazy(df).with_columns(new_cols)


def vertical_winsor_w_neighbors(
    df: FrameType,
    target_columns: List[str],
    neighbor_columns: List[str],
    *,
    method: OutlierMethod = _DEFAULT_METHOD,
    sensitivity: float = _DEFAULT_SENSITIVITY,
    percentile_bounds: tuple[float, float] = _DEFAULT_PERCENTILE_BOUNDS,
    symmetric: bool = _DEFAULT_SYMMETRIC,
    auto_rescale: bool = _DEFAULT_AUTO_RESCALE,
    nulls_as_zero: bool = _DEFAULT_NULLS_AS_ZERO,
    zeros_as_null: bool = _DEFAULT_ZEROS_AS_NULL,
    filter_nulls: bool = _DEFAULT_FILTER_NULLS,
    result_suffix: str = "_vn_winsor",
    n_threads: int = 2,
):
    all_cols = list(dict.fromkeys(target_columns + neighbor_columns))
    lf = _ensure_lazy(df)
    lf = _preprocess_columns(
        lf, all_cols,
        nulls_as_zero=nulls_as_zero,
        zeros_as_null=zeros_as_null,
        filter_nulls=filter_nulls,
    )
    collected = lf.select(all_cols).collect()
    neighbor_arrays = {
        c: collected.get_column(c).to_numpy(writable=True).astype(np.float64)
        for c in neighbor_columns
    }

    def _process_target(col_name: str) -> tuple[str, float]:
        target_arr = collected.get_column(col_name).to_numpy(writable=True).astype(np.float64)
        unique_neighbors = [c for c in neighbor_columns if c != col_name]
        if unique_neighbors:
            combined = np.column_stack([target_arr] + [neighbor_arrays[c] for c in unique_neighbors])
        else:
            combined = target_arr.copy()
        flat = combined.ravel() if combined.ndim > 1 else combined
        if auto_rescale:
            flat = _auto_rescale_array(flat)
        outlier_in_combined = _detect_outliers_1d(
            flat, method=method, sensitivity=sensitivity,
            percentile_bounds=percentile_bounds, symmetric=symmetric,
        )
        n_rows = target_arr.shape[0]
        if combined.ndim == 1:
            target_outlier_mask = outlier_in_combined[:n_rows]
        else:
            outlier_2d = outlier_in_combined.reshape(combined.shape)
            target_outlier_mask = outlier_2d[:, 0]
        if auto_rescale:
            target_arr = _auto_rescale_array(target_arr)
        valid_mask = (~np.isnan(target_arr)) & (~target_outlier_mask)
        valid_vals = target_arr[valid_mask]
        if valid_vals.size == 0:
            return col_name, np.nan
        lower_bound = np.min(valid_vals)
        upper_bound = np.max(valid_vals)
        clipped = np.where(
            target_outlier_mask & (~np.isnan(target_arr)),
            np.clip(target_arr, lower_bound, upper_bound),
            target_arr,
        )
        final_vals = clipped[~np.isnan(clipped)]
        if final_vals.size == 0:
            return col_name, np.nan
        return col_name, float(np.nanmean(final_vals))

    if len(target_columns) > 2 and n_threads > 1:
        futures = {_EXECUTOR.submit(_process_target, c): c for c in target_columns}
        results_map = {}
        for f in concurrent.futures.as_completed(futures):
            col_name, mean_val = f.result()
            results_map[col_name] = mean_val
    else:
        results_map = {}
        for c in target_columns:
            col_name, mean_val = _process_target(c)
            results_map[col_name] = mean_val

    new_cols = [
        pl.lit(results_map[c]).cast(pl.Float64).alias(f"{c}{result_suffix}")
        for c in target_columns
    ]
    return _ensure_lazy(df).with_columns(new_cols)


def horizontal_winsor_w_neighbors(
    df: FrameType,
    target_columns: List[str],
    neighbor_columns: List[str],
    *,
    method: OutlierMethod = _DEFAULT_METHOD,
    sensitivity: float = _DEFAULT_SENSITIVITY,
    percentile_bounds: tuple[float, float] = _DEFAULT_PERCENTILE_BOUNDS,
    symmetric: bool = _DEFAULT_SYMMETRIC,
    auto_rescale: bool = _DEFAULT_AUTO_RESCALE,
    weight_col: Optional[str] = _DEFAULT_WEIGHT_COL,
    nulls_as_zero: bool = _DEFAULT_NULLS_AS_ZERO,
    zeros_as_null: bool = _DEFAULT_ZEROS_AS_NULL,
    filter_nulls: bool = _DEFAULT_FILTER_NULLS,
    result_alias: str = "h_winsor_neighbor_mean",
    n_threads: int = 2,
):
    all_cols = list(dict.fromkeys(target_columns + neighbor_columns))
    lf = _ensure_lazy(df)
    lf = _preprocess_columns(
        lf, all_cols,
        nulls_as_zero=nulls_as_zero,
        zeros_as_null=zeros_as_null,
        filter_nulls=filter_nulls,
    )
    collected = lf.select(all_cols).collect()
    target_data = collected.select(target_columns).to_numpy(writable=True).astype(np.float64)
    neighbor_only = [c for c in neighbor_columns if c not in target_columns]
    if neighbor_only:
        neighbor_data = collected.select(neighbor_only).to_numpy(writable=True).astype(np.float64)
        combined_data = np.hstack([target_data, neighbor_data])
    else:
        combined_data = target_data.copy()

    weight_data = None
    if weight_col is not None:
        w_series = _ensure_lazy(df).select(pl.col(weight_col).cast(pl.Float64)).collect()
        w_arr = w_series.to_numpy(writable=True).astype(np.float64).ravel()
        weight_data = np.tile(w_arr[:, np.newaxis], (1, target_data.shape[1]))

    if auto_rescale:
        combined_data = _auto_rescale_2d_rowwise(combined_data)
        target_data = combined_data[:, :len(target_columns)]

    outlier_mask_combined = _detect_outliers_2d_rowwise(
        combined_data, method=method, sensitivity=sensitivity,
        percentile_bounds=percentile_bounds, symmetric=symmetric,
    )
    target_outlier = outlier_mask_combined[:, :len(target_columns)]

    nan_mask_combined = np.isnan(combined_data)
    safe_combined = np.where(nan_mask_combined | outlier_mask_combined, np.nan, combined_data)
    with np.errstate(invalid="ignore"):
        row_min = np.nanmin(safe_combined, axis=1, keepdims=True)
        row_max = np.nanmax(safe_combined, axis=1, keepdims=True)
    all_nan = np.all(np.isnan(safe_combined), axis=1)
    row_min[all_nan, :] = np.nan
    row_max[all_nan, :] = np.nan

    nan_mask_target = np.isnan(target_data)
    clamped_target = np.where(
        target_outlier & (~nan_mask_target),
        np.clip(target_data, row_min, row_max),
        target_data,
    )
    results = _weighted_nanmean_rows(clamped_target, weight_data)
    result_series = pl.Series(result_alias, results)
    return _ensure_lazy(df).with_columns(result_series.alias(result_alias))


def horizontal_outlier_mask(
    df: FrameType,
    columns: List[str],
    *,
    method: OutlierMethod = _DEFAULT_METHOD,
    sensitivity: float = _DEFAULT_SENSITIVITY,
    percentile_bounds: tuple[float, float] = _DEFAULT_PERCENTILE_BOUNDS,
    symmetric: bool = _DEFAULT_SYMMETRIC,
    auto_rescale: bool = _DEFAULT_AUTO_RESCALE,
    nulls_as_zero: bool = _DEFAULT_NULLS_AS_ZERO,
    zeros_as_null: bool = _DEFAULT_ZEROS_AS_NULL,
    filter_nulls: bool = _DEFAULT_FILTER_NULLS,
    result_suffix: str = "_h_outlier",
    n_threads: int = 2,
):
    lf = _ensure_lazy(df)
    lf = _preprocess_columns(
        lf, columns,
        nulls_as_zero=nulls_as_zero,
        zeros_as_null=zeros_as_null,
        filter_nulls=filter_nulls,
    )
    collected = lf.select(columns).collect()
    data = collected.to_numpy(writable=True).astype(np.float64)

    if auto_rescale:
        data = _auto_rescale_2d_rowwise(data)

    outlier_matrix = _detect_outliers_2d_rowwise(
        data, method=method, sensitivity=sensitivity,
        percentile_bounds=percentile_bounds, symmetric=symmetric,
    ).astype(np.int8)

    mask_series = [
        pl.Series(f"{columns[j]}{result_suffix}", outlier_matrix[:, j]).cast(pl.Int8)
        for j in range(len(columns))
    ]
    result_lf = _ensure_lazy(df)
    for s in mask_series:
        result_lf = result_lf.with_columns(s.alias(s.name))
    return result_lf


def vertical_outlier_mask(
    df: FrameType,
    columns: List[str],
    *,
    method: OutlierMethod = _DEFAULT_METHOD,
    sensitivity: float = _DEFAULT_SENSITIVITY,
    percentile_bounds: tuple[float, float] = _DEFAULT_PERCENTILE_BOUNDS,
    symmetric: bool = _DEFAULT_SYMMETRIC,
    auto_rescale: bool = _DEFAULT_AUTO_RESCALE,
    nulls_as_zero: bool = _DEFAULT_NULLS_AS_ZERO,
    zeros_as_null: bool = _DEFAULT_ZEROS_AS_NULL,
    filter_nulls: bool = _DEFAULT_FILTER_NULLS,
    result_suffix: str = "_v_outlier",
    n_threads: int = 2,
):
    lf = _ensure_lazy(df)
    lf = _preprocess_columns(
        lf, columns,
        nulls_as_zero=nulls_as_zero,
        zeros_as_null=zeros_as_null,
        filter_nulls=filter_nulls,
    )
    collected = lf.select(columns).collect()

    def _process_col(col_name: str) -> tuple[str, np.ndarray]:
        arr = collected.get_column(col_name).to_numpy(writable=True).astype(np.float64)
        if auto_rescale:
            arr = _auto_rescale_array(arr)
        mask = _detect_outliers_1d(
            arr, method=method, sensitivity=sensitivity,
            percentile_bounds=percentile_bounds, symmetric=symmetric,
        )
        return col_name, mask.astype(np.int8)

    if len(columns) > 2 and n_threads > 1:
        futures = {_EXECUTOR.submit(_process_col, c): c for c in columns}
        results_map = {}
        for f in concurrent.futures.as_completed(futures):
            col_name, mask = f.result()
            results_map[col_name] = mask
    else:
        results_map = {}
        for c in columns:
            col_name, mask = _process_col(c)
            results_map[col_name] = mask

    result_lf = _ensure_lazy(df)
    for c in columns:
        s = pl.Series(f"{c}{result_suffix}", results_map[c]).cast(pl.Int8)
        result_lf = result_lf.with_columns(s.alias(s.name))
    return result_lf
