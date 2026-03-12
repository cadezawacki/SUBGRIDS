"""
processRedistributeProceeds.py

Computes a proposed redistribution of refSkew values across portfolio rows,
weighted by grossSize. Optionally constrained by microgrid ticker patterns
(hot_tickers). Returns a summary table of proposed adjustments that the
frontend can present for user approval before applying.
"""

import polars as pl
from typing import Dict, Any, List, Optional
from app.helpers.polars_hyper_plugin import *


_ID_DISPLAY_COLS = ["ticker", "isin", "cusip", "description", "userSide", "QT"]


def process(
    grid_frame: pl.DataFrame,
    primary_keys: List[str],
    params: Optional[Dict[str, Any]] = None,
    micro_constraints: Optional[pl.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Compute proposed refSkew redistribution.

    Parameters
    ----------
    grid_frame : pl.DataFrame
        Current portfolio snapshot.
    primary_keys : list[str]
        Primary key column names for the grid.
    params : dict, optional
        Additional parameters from the frontend (e.g. target_total, bucket).
    micro_constraints : pl.DataFrame, optional
        Live hot_tickers microgrid data. If provided, only rows matching
        these patterns participate in redistribution.

    Returns
    -------
    dict with:
        - "summary": list[dict]  — rows for the modal summary table
        - "updates": list[dict]  — row updates to apply if user confirms
        - "stats": dict          — aggregate stats
        - "constraints_applied": bool — whether microgrid patterns were used
        - "constraints_relaxed": list[str] — patterns that were relaxed (no matches)
        - "error": str           — only present on failure
    """
    params = params or {}

    available_cols = set(grid_frame.columns)
    pk_cols = [pk for pk in primary_keys if pk in available_cols]

    if not pk_cols:
        return {"error": "No primary key columns found in grid data"}

    # Determine which skew bucket to use
    bucket = params.get("bucket")
    skew_col = f"refSkew{bucket}" if bucket and f"refSkew{bucket}" in available_cols else "refSkew"

    if skew_col not in available_cols:
        return {"error": f"Column '{skew_col}' not found in grid data"}
    if "grossSize" not in available_cols:
        return {"error": "Column 'grossSize' not found in grid data"}

    # Identify display columns that exist
    display_cols = [c for c in _ID_DISPLAY_COLS if c in available_cols]

    # Deduplicated column selection
    select_cols = pk_cols + display_cols + ["grossSize", skew_col]
    seen = set()
    unique_cols = []
    for c in select_cols:
        if c not in seen and c in available_cols:
            seen.add(c)
            unique_cols.append(c)

    df = grid_frame.select(unique_cols)

    # Filter out rows with null/zero grossSize
    df = df.filter(
        pl.col("grossSize").is_not_null()
        & (pl.col("grossSize").abs() > 1e-12)
    )

    if df.hyper.is_empty():
        return {
            "summary": [], "updates": [],
            "stats": {"total_gross": 0, "total_skew_product": 0, "row_count": 0},
            "constraints_applied": False, "constraints_relaxed": [],
        }

    # --- Apply microgrid constraints if present ---
    constraints_applied = False
    constraints_relaxed = []

    if micro_constraints is not None and not micro_constraints.hyper.is_empty():
        mask = _build_constraint_mask(df, micro_constraints, available_cols)
        if mask is not None:
            constrained = df.filter(mask)
            if constrained.hyper.is_empty():
                # All constraints matched zero rows — relax and use full set
                constraints_relaxed.append("All patterns matched zero rows — using full portfolio")
            else:
                df = constrained
                constraints_applied = True

    # Compute weighted skew totals
    total_gross = df["grossSize"].sum()
    current_skew_product = (df["grossSize"] * df[skew_col].fill_null(0)).sum()

    target_total = params.get("target_total", current_skew_product)

    # Uniform redistribution: every row gets the same refSkew = wavg
    uniform_skew = 0.0 if abs(total_gross) < 1e-12 else target_total / total_gross

    # Single with_columns for all computed fields
    df = df.with_columns(
        pl.col(skew_col).fill_null(0).round(6).alias("current_skew"),
        pl.lit(uniform_skew).round(6).alias("proposed_skew"),
        (pl.lit(uniform_skew) - pl.col(skew_col).fill_null(0)).round(6).alias("delta"),
        (pl.col("grossSize") / total_gross * 100).round(2).alias("weight_pct"),
    )

    # Build summary rows
    summary_cols = pk_cols + display_cols + ["grossSize", "weight_pct", "current_skew", "proposed_skew", "delta"]
    summary_cols = [c for c in summary_cols if c in df.columns]
    summary_df = df.select(summary_cols)

    # Build update rows vectorially (PK + new skew value only)
    update_records = df.select(pk_cols + ["proposed_skew"]).rename({"proposed_skew": skew_col}).to_dicts()

    stats = {
        "total_gross": round(float(total_gross), 2),
        "total_skew_product": round(float(current_skew_product), 6),
        "uniform_skew": round(float(uniform_skew), 6),
        "row_count": df.hyper.height(),
        "skew_column": skew_col,
    }

    return {
        "summary": summary_df.to_dicts(),
        "updates": update_records,
        "stats": stats,
        "constraints_applied": constraints_applied,
        "constraints_relaxed": constraints_relaxed,
    }


def _build_constraint_mask(
    df: pl.DataFrame,
    micro: pl.DataFrame,
    available_cols: set,
) -> Optional[pl.Expr]:
    """
    Build a boolean filter expression from hot_tickers microgrid patterns.
    Each micro row has: column (ticker/isin/cusip/description), pattern, match_mode.
    Returns a combined OR mask, or None if no valid constraints exist.
    """
    import re

    exprs = []
    for row in micro.to_dicts():
        col_name = row.get("column", "ticker")
        pattern = row.get("pattern", "")
        match_mode = row.get("match_mode", "literal")

        if not pattern or col_name not in available_cols:
            continue

        if match_mode == "regex":
            try:
                re.compile(pattern)
                exprs.append(pl.col(col_name).cast(pl.String).str.contains(pattern))
            except re.error:
                continue
        else:
            # Literal match (case-insensitive)
            upper_pat = pattern.upper()
            exprs.append(pl.col(col_name).cast(pl.String).str.to_uppercase().str.contains(pl.lit(upper_pat), literal=True))

    if not exprs:
        return None

    # Combine all patterns with OR
    combined = exprs[0]
    for e in exprs[1:]:
        combined = combined | e
    return combined
