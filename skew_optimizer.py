"""
Bond Skew Redistribution Optimizer
===================================
Redistributes proceeds across a basket of bonds to align with per-bond
riskiness while preserving bucket-level (side × quote_type) proceeds totals.

Solves a convex QP:
    min  λ₁‖p − p*‖² + λ₂‖Δσ‖²
    s.t. bucket conservation, mid-line bounds, optional trader hard constraints

Two-tier fallback:
    1) Try with all hard constraints
    2) If infeasible → relax trader constraints to soft penalties

Author: Claude
Python 3.11+ / Polars / CVXPY
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import cvxpy as cp
import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Column Mapping
# ──────────────────────────────────────────────────────────────────────────────
# Replace the *values* with your actual column names.  The keys are the
# internal canonical names used throughout the optimizer.

COLUMN_MAP: dict[str, str] = {
    # identifiers
    "bond_id":          "bond_id",          # unique bond identifier
    "trader":           "trader",           # trader name / desk
    "side":             "side",             # "BUY" or "SELL"
    "quote_type":       "quote_type",       # "PX" or "SPD"
    # numeric fields
    "proceeds":         "proceeds",         # current proceeds amount
    "skew":             "skew",             # current skew / charge (bps or price‑equiv)
    "size":             "size",             # notional / face
    "dv01":             "dv01",             # dollar value of 1 bp
    "liquidity_score":  "liquidity_score",  # higher = more liquid
}


def _c(canonical: str) -> str:
    """Resolve a canonical column name via the mapping."""
    return COLUMN_MAP[canonical]


# ──────────────────────────────────────────────────────────────────────────────
# Enums & Config
# ──────────────────────────────────────────────────────────────────────────────

class TraderConstraintMode(str, Enum):
    HARD = "hard"
    SOFT = "soft"


@dataclass
class OptimizerConfig:
    """All tuneable knobs live here."""

    # objective weights
    lambda_alignment: float = 1.0       # λ₁  – weight on proceeds→target alignment
    lambda_stability: float = 0.1       # λ₂  – weight on minimising |Δσ|
    lambda_trader_soft: float = 10.0    # λ₃  – penalty when trader constraint is SOFT

    # trader constraint mode  (switch on the fly)
    trader_constraint: TraderConstraintMode = TraderConstraintMode.HARD

    # If True, automatically fall back to SOFT when HARD is infeasible
    auto_fallback: bool = True

    # max absolute skew move per bond (None = unbounded)
    max_skew_delta: float | None = None

    # risk weight formula: w = dv01^a * size^b / liquidity^c
    risk_exp_dv01: float = 1.0
    risk_exp_size: float = 1.0
    risk_exp_liq:  float = 1.0

    # solver settings
    solver: str = "SCS"
    solver_verbose: bool = False
    solver_kwargs: dict[str, Any] = field(default_factory=lambda: {
        "max_iters": 50_000,
    })


# ──────────────────────────────────────────────────────────────────────────────
# Sensitivity helper  κ — maps Δskew → Δproceeds
# ──────────────────────────────────────────────────────────────────────────────

def _compute_kappa(dv01: np.ndarray, size: np.ndarray, qt: list[str]) -> np.ndarray:
    """
    κ_i  =  proceeds sensitivity to a 1‑unit skew change.

    For PX bonds :  Δproceeds ≈ Δprice × size / 100   → κ = size / 100
    For SPD bonds:  Δproceeds ≈ Δspread × DV01 × size → κ = dv01 * size

    Adjust these formulae to match your actual proceeds model.
    """
    kappa = np.empty_like(dv01)
    for i, q in enumerate(qt):
        if q == "PX":
            kappa[i] = size[i] / 100.0
        else:  # SPD
            kappa[i] = dv01[i] * size[i]
    return kappa


# ──────────────────────────────────────────────────────────────────────────────
# Risk weight computation
# ──────────────────────────────────────────────────────────────────────────────

def _compute_risk_weights(
    dv01: np.ndarray,
    size: np.ndarray,
    liq: np.ndarray,
    cfg: OptimizerConfig,
) -> np.ndarray:
    """w_i = dv01^a × size^b / liquidity^c   (element‑wise)."""
    w = (
        np.power(dv01, cfg.risk_exp_dv01)
        * np.power(size, cfg.risk_exp_size)
        / np.power(np.clip(liq, 1e-12, None), cfg.risk_exp_liq)
    )
    return w


# ──────────────────────────────────────────────────────────────────────────────
# Mid‑line sign helper
# ──────────────────────────────────────────────────────────────────────────────

def _skew_sign_bounds(
    side: str, qt: str
) -> tuple[float | None, float | None]:
    """
    Return (lb, ub) on the final skew σ_i to keep it on the correct side
    of mid.

    BUY  + PX  → skew ≤ 0   (buying cheap → negative charge)
    BUY  + SPD → skew ≥ 0   (buying wide  → positive charge)
    SELL + PX  → skew ≥ 0   (selling rich → positive charge)
    SELL + SPD → skew ≤ 0   (selling tight→ negative charge)
    """
    if (side, qt) == ("BUY", "PX"):
        return (None, 0.0)
    elif (side, qt) == ("BUY", "SPD"):
        return (0.0, None)
    elif (side, qt) == ("SELL", "PX"):
        return (0.0, None)
    elif (side, qt) == ("SELL", "SPD"):
        return (None, 0.0)
    else:
        raise ValueError(f"Unknown side/qt combo: {side}/{qt}")


# ──────────────────────────────────────────────────────────────────────────────
# Core solver
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class OptimizationResult:
    status: str
    optimal: bool
    new_skews: np.ndarray
    skew_deltas: np.ndarray
    new_proceeds: np.ndarray
    proceeds_deltas: np.ndarray
    target_proceeds: np.ndarray
    risk_weights: np.ndarray
    risk_shares: np.ndarray
    objective_value: float
    trader_mode_used: TraderConstraintMode
    fell_back: bool


def _build_and_solve(
    *,
    N: int,
    p0: np.ndarray,
    sigma0: np.ndarray,
    kappa: np.ndarray,
    p_star: np.ndarray,
    sides: list[str],
    qts: list[str],
    bucket_indices: dict[tuple[str, str], list[int]],
    trader_bucket_indices: dict[tuple[str, str, str], list[int]],
    A_tb: dict[tuple[str, str, str], float],
    trader_mode: TraderConstraintMode,
    cfg: OptimizerConfig,
) -> tuple[str, float | None, np.ndarray | None]:
    """
    Build the CVXPY problem for a given trader_mode, solve,
    return (status, obj_value, delta_sigma_value).
    """
    delta_sigma = cp.Variable(N, name="delta_sigma")
    p_new = p0 + cp.multiply(kappa, delta_sigma)

    # Normalise alignment term so λ scales are comparable across datasets
    kappa_sq_mean = float(np.mean(kappa ** 2)) + 1e-12

    obj = (
        (cfg.lambda_alignment / kappa_sq_mean) * cp.sum_squares(p_new - p_star)
        + cfg.lambda_stability * cp.sum_squares(delta_sigma)
    )

    constraints: list[cp.Constraint] = []

    # 1) Bucket‑level proceeds conservation  ∀ (s,q)
    #    When trader constraints are HARD these are redundant (they're implied)
    #    but we only skip them in HARD mode to keep the problem smaller.
    if trader_mode != TraderConstraintMode.HARD:
        for bk, idxs in bucket_indices.items():
            idx = np.array(idxs)
            constraints.append(kappa[idx] @ delta_sigma[idx] == 0)

    # 2) Trader‑level  ∀ (t,s,q)
    for tbk, idxs in trader_bucket_indices.items():
        idx = np.array(idxs)
        rhs = A_tb[tbk] - float(np.sum(p0[idx]))

        if trader_mode == TraderConstraintMode.HARD:
            constraints.append(kappa[idx] @ delta_sigma[idx] == rhs)
        else:
            slack = kappa[idx] @ delta_sigma[idx] - rhs
            obj += (cfg.lambda_trader_soft / kappa_sq_mean) * cp.square(slack)

    # 3) Mid‑line bounds  ∀ i
    for i in range(N):
        lb, ub = _skew_sign_bounds(sides[i], qts[i])
        if lb is not None:
            constraints.append(sigma0[i] + delta_sigma[i] >= lb)
        if ub is not None:
            constraints.append(sigma0[i] + delta_sigma[i] <= ub)

    # 4) Max absolute skew delta
    if cfg.max_skew_delta is not None:
        constraints.append(delta_sigma >= -cfg.max_skew_delta)
        constraints.append(delta_sigma <=  cfg.max_skew_delta)

    prob = cp.Problem(cp.Minimize(obj), constraints)
    try:
        prob.solve(solver=cfg.solver, verbose=cfg.solver_verbose, **cfg.solver_kwargs)
    except cp.SolverError:
        for backup in ("SCS", "ECOS", "OSQP"):
            if backup == cfg.solver:
                continue
            try:
                prob.solve(solver=backup, verbose=cfg.solver_verbose)
                if prob.status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE):
                    break
            except cp.SolverError:
                continue

    return prob.status, prob.value, delta_sigma.value


def solve(
    df: pl.DataFrame,
    cfg: OptimizerConfig | None = None,
) -> tuple[pl.DataFrame, OptimizationResult]:
    """
    Main entry point.

    Parameters
    ----------
    df : pl.DataFrame
        Portfolio with columns matching COLUMN_MAP values.
    cfg : OptimizerConfig, optional
        Solver configuration.

    Returns
    -------
    df_result : pl.DataFrame
        Original frame augmented with solution columns.
    result : OptimizationResult
        Structured result object for programmatic access.
    """
    cfg = cfg or OptimizerConfig()
    N = len(df)

    # ── extract numpy arrays ──────────────────────────────────────────────
    traders   = df[_c("trader")].to_list()
    sides     = df[_c("side")].to_list()
    qts       = df[_c("quote_type")].to_list()
    p0        = df[_c("proceeds")].to_numpy().astype(float)
    sigma0    = df[_c("skew")].to_numpy().astype(float)
    sizes     = df[_c("size")].to_numpy().astype(float)
    dv01s     = df[_c("dv01")].to_numpy().astype(float)
    liqs      = df[_c("liquidity_score")].to_numpy().astype(float)

    kappa = _compute_kappa(dv01s, sizes, qts)
    w = _compute_risk_weights(dv01s, sizes, liqs, cfg)

    # ── group indices ─────────────────────────────────────────────────────
    bucket_indices: dict[tuple[str, str], list[int]] = {}
    trader_bucket_indices: dict[tuple[str, str, str], list[int]] = {}

    for i in range(N):
        bk = (sides[i], qts[i])
        bucket_indices.setdefault(bk, []).append(i)
        tbk = (traders[i], sides[i], qts[i])
        trader_bucket_indices.setdefault(tbk, []).append(i)

    # ── risk shares & targets ─────────────────────────────────────────────
    P0_bucket: dict[tuple[str, str], float] = {}
    W_bucket: dict[tuple[str, str], float] = {}
    for bk, idxs in bucket_indices.items():
        P0_bucket[bk] = float(np.sum(p0[idxs]))
        W_bucket[bk] = float(np.sum(w[idxs]))

    rho: dict[tuple[str, str, str], float] = {}
    for tbk, idxs in trader_bucket_indices.items():
        bk = (tbk[1], tbk[2])
        w_total = W_bucket[bk]
        rho[tbk] = float(np.sum(w[idxs])) / w_total if w_total > 0 else 0.0

    A_tb: dict[tuple[str, str, str], float] = {}
    for tbk in trader_bucket_indices:
        bk = (tbk[1], tbk[2])
        A_tb[tbk] = rho[tbk] * P0_bucket[bk]

    p_star = np.zeros(N)
    risk_shares_per_bond = np.zeros(N)
    for tbk, idxs in trader_bucket_indices.items():
        w_trader_sum = float(np.sum(w[idxs]))
        for i in idxs:
            frac = w[i] / w_trader_sum if w_trader_sum > 0 else 1.0 / len(idxs)
            p_star[i] = frac * A_tb[tbk]
            risk_shares_per_bond[i] = rho[tbk]

    # ── solve with optional fallback ──────────────────────────────────────
    common = dict(
        N=N, p0=p0, sigma0=sigma0, kappa=kappa, p_star=p_star,
        sides=sides, qts=qts, bucket_indices=bucket_indices,
        trader_bucket_indices=trader_bucket_indices, A_tb=A_tb, cfg=cfg,
    )

    fell_back = False
    mode_used = cfg.trader_constraint

    status, obj_val, d_sigma = _build_and_solve(trader_mode=cfg.trader_constraint, **common)
    is_ok = status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE)

    if (not is_ok
            and cfg.auto_fallback
            and cfg.trader_constraint == TraderConstraintMode.HARD):
        logger.warning(
            "HARD trader constraints infeasible (status=%s). Falling back to SOFT.",
            status,
        )
        fell_back = True
        mode_used = TraderConstraintMode.SOFT
        status, obj_val, d_sigma = _build_and_solve(
            trader_mode=TraderConstraintMode.SOFT, **common,
        )
        is_ok = status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE)

    if not is_ok:
        logger.error("Solver status: %s", status)

    d_sigma = d_sigma if d_sigma is not None else np.zeros(N)

    new_sigma = sigma0 + d_sigma
    new_p = p0 + kappa * d_sigma
    delta_p = new_p - p0

    result = OptimizationResult(
        status=status,
        optimal=is_ok,
        new_skews=new_sigma,
        skew_deltas=d_sigma,
        new_proceeds=new_p,
        proceeds_deltas=delta_p,
        target_proceeds=p_star,
        risk_weights=w,
        risk_shares=risk_shares_per_bond,
        objective_value=obj_val if obj_val is not None else float("nan"),
        trader_mode_used=mode_used,
        fell_back=fell_back,
    )

    df_result = df.with_columns(
        pl.Series("opt_skew", new_sigma),
        pl.Series("opt_skew_delta", d_sigma),
        pl.Series("opt_proceeds", new_p),
        pl.Series("opt_proceeds_delta", delta_p),
        pl.Series("opt_target_proceeds", p_star),
        pl.Series("risk_weight", w),
        pl.Series("risk_share", risk_shares_per_bond),
        pl.Series("kappa", kappa),
    )

    return df_result, result


# ──────────────────────────────────────────────────────────────────────────────
# Summary reporting
# ──────────────────────────────────────────────────────────────────────────────

def summary_by_trader_side_qt(df_result: pl.DataFrame) -> pl.DataFrame:
    """
    Grouped summary: trader × side × quote_type

    Returns trader, side, quote_type, bond_count, total_dv01,
    starting/final proceeds, wavg starting/final/delta skew, risk_share.
    """
    side_col, qt_col, trader_col = _c("side"), _c("quote_type"), _c("trader")
    dv01_col, proceeds_col, skew_col, size_col = (
        _c("dv01"), _c("proceeds"), _c("skew"), _c("size")
    )

    grp = (
        df_result
        .group_by([trader_col, side_col, qt_col])
        .agg(
            pl.count().alias("bond_count"),
            pl.col(dv01_col).sum().alias("total_dv01"),
            pl.col(size_col).sum().alias("total_size"),
            pl.col(proceeds_col).sum().alias("starting_proceeds"),
            pl.col("opt_proceeds").sum().alias("final_proceeds"),
            pl.col("opt_proceeds_delta").sum().alias("proceeds_delta"),
            (pl.col(skew_col) * pl.col(dv01_col)).sum().alias("_wn_start"),
            (pl.col("opt_skew") * pl.col(dv01_col)).sum().alias("_wn_final"),
            pl.col("risk_share").first().alias("risk_share"),
        )
        .with_columns(
            (pl.col("_wn_start") / pl.col("total_dv01")).alias("wavg_starting_skew"),
            (pl.col("_wn_final") / pl.col("total_dv01")).alias("wavg_final_skew"),
        )
        .with_columns(
            (pl.col("wavg_final_skew") - pl.col("wavg_starting_skew")).alias("wavg_skew_delta"),
        )
        .drop("_wn_start", "_wn_final")
    )
    return grp.sort([side_col, qt_col, trader_col])


def summary_by_side_qt(df_result: pl.DataFrame) -> pl.DataFrame:
    """Overall bucket summary: side × quote_type."""
    side_col, qt_col = _c("side"), _c("quote_type")
    dv01_col, proceeds_col, skew_col, size_col = (
        _c("dv01"), _c("proceeds"), _c("skew"), _c("size")
    )

    grp = (
        df_result
        .group_by([side_col, qt_col])
        .agg(
            pl.count().alias("bond_count"),
            pl.col(dv01_col).sum().alias("total_dv01"),
            pl.col(size_col).sum().alias("total_size"),
            pl.col(proceeds_col).sum().alias("starting_proceeds"),
            pl.col("opt_proceeds").sum().alias("final_proceeds"),
            pl.col("opt_proceeds_delta").sum().alias("proceeds_delta"),
            (pl.col(skew_col) * pl.col(dv01_col)).sum().alias("_s"),
            (pl.col("opt_skew") * pl.col(dv01_col)).sum().alias("_f"),
        )
        .with_columns(
            (pl.col("_s") / pl.col("total_dv01")).alias("wavg_starting_skew"),
            (pl.col("_f") / pl.col("total_dv01")).alias("wavg_final_skew"),
        )
        .with_columns(
            (pl.col("wavg_final_skew") - pl.col("wavg_starting_skew")).alias("wavg_skew_delta"),
        )
        .drop("_s", "_f")
    )
    return grp.sort([side_col, qt_col])


# ──────────────────────────────────────────────────────────────────────────────
# Pretty‑print
# ──────────────────────────────────────────────────────────────────────────────

def print_report(df_result: pl.DataFrame, result: OptimizationResult) -> None:
    """Print a formatted console report."""
    fb = "  (auto‑fallback from HARD)" if result.fell_back else ""
    print("=" * 80)
    print(f"  SKEW OPTIMIZATION REPORT")
    print(f"  status          : {result.status}")
    print(f"  trader mode used: {result.trader_mode_used.value}{fb}")
    print(f"  objective value : {result.objective_value:.6f}")
    print("=" * 80)

    print("\n── Overall by Side × Quote Type ─────────────────────────────────")
    _pp(summary_by_side_qt(df_result))

    print("\n── By Trader × Side × Quote Type ────────────────────────────────")
    _pp(summary_by_trader_side_qt(df_result))

    print("\n── Bond‑Level Detail (first 30) ─────────────────────────────────")
    cols = [
        _c("bond_id"), _c("trader"), _c("side"), _c("quote_type"),
        _c("skew"), "opt_skew", "opt_skew_delta",
        _c("proceeds"), "opt_proceeds", "opt_proceeds_delta",
    ]
    _pp(df_result.select([c for c in cols if c in df_result.columns]).head(30))


def _pp(df: pl.DataFrame) -> None:
    with pl.Config(tbl_cols=-1, tbl_rows=50, tbl_width_chars=160, float_precision=4):
        print(df)
