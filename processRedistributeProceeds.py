"""
processRedistributeProceeds.py

Computes a proposed redistribution of refSkew values across portfolio rows,
weighted by grossSize. Returns a summary table of proposed adjustments
that the frontend can present for user approval before applying.
"""

import polars as pl
from typing import Dict, Any, List, Optional


# Columns we need from the portfolio grid
_REQUIRED_COLS = ["grossSize", "refSkew"]
_SKEW_BUCKET_COLS = ["refSkewPx", "refSkewSpd"]
_ID_DISPLAY_COLS = ["ticker", "isin", "cusip", "description", "userSide", "QT"]


def process(grid_frame: pl.DataFrame, primary_keys: List[str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Compute proposed refSkew redistribution.

    Parameters
    ----------
    grid_frame : pl.DataFrame
        Current portfolio snapshot with at least grossSize and refSkew columns.
    primary_keys : list[str]
        Primary key column names for the grid.
    params : dict, optional
        Additional parameters from the frontend (e.g. target_total, bucket).

    Returns
    -------
    dict with:
        - "summary": list[dict]  — rows for the modal summary table
        - "updates": list[dict]  — row updates to apply if user confirms
        - "stats": dict          — aggregate stats (total_gross, total_skew, etc.)
    """
    params = params or {}

    # Determine which columns are available
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

    # Select relevant columns
    select_cols = pk_cols + display_cols + ["grossSize", skew_col]
    # Deduplicate while preserving order
    seen = set()
    unique_cols = []
    for c in select_cols:
        if c not in seen and c in available_cols:
            seen.add(c)
            unique_cols.append(c)

    df = grid_frame.select(unique_cols)

    # Filter out rows with null/zero grossSize (they can't participate in redistribution)
    df = df.filter(
        pl.col("grossSize").is_not_null()
        & (pl.col("grossSize").abs() > 1e-12)
    )

    if df.is_empty():
        return {
            "summary": [],
            "updates": [],
            "stats": {"total_gross": 0, "total_skew_product": 0, "row_count": 0},
        }

    # Current weighted skew total (sum of grossSize * refSkew)
    total_gross = df["grossSize"].sum()
    current_skew_product = (df["grossSize"] * df[skew_col].fill_null(0)).sum()

    # Target total — either provided or use the current total (preserve overall skew)
    target_total = params.get("target_total", current_skew_product)

    # Compute new refSkew for each row: proportional to its weight share
    # new_refSkew_i = target_total / total_gross  (uniform redistribution)
    # This gives every row the same refSkew equal to the portfolio-level wavg
    if abs(total_gross) < 1e-12:
        uniform_skew = 0.0
    else:
        uniform_skew = target_total / total_gross

    # Compute per-row proposed values and deltas
    df = df.with_columns([
        pl.col(skew_col).fill_null(0).alias("current_skew"),
        pl.lit(uniform_skew).alias("proposed_skew"),
        (pl.lit(uniform_skew) - pl.col(skew_col).fill_null(0)).alias("delta"),
        (pl.col("grossSize") / total_gross * 100).round(2).alias("weight_pct"),
    ])

    # Round for display
    df = df.with_columns([
        pl.col("current_skew").round(6),
        pl.col("proposed_skew").round(6),
        pl.col("delta").round(6),
    ])

    # Build summary rows (for the modal table)
    summary_cols = pk_cols + display_cols + ["grossSize", "weight_pct", "current_skew", "proposed_skew", "delta"]
    summary_cols = [c for c in summary_cols if c in df.columns]
    summary_df = df.select(summary_cols)

    # Build update rows vectorially (PK + new skew value only)
    update_records = df.select(pk_cols + ["proposed_skew"]).rename({"proposed_skew": skew_col}).to_dicts()

    stats = {
        "total_gross": round(total_gross, 2),
        "total_skew_product": round(current_skew_product, 6),
        "uniform_skew": round(uniform_skew, 6),
        "row_count": df.shape[0],
        "skew_column": skew_col,
    }

    return {
        "summary": summary_df.to_dicts(),
        "updates": update_records,
        "stats": stats,
    }
