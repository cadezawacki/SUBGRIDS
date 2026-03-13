"""
Bond Charge Optimization (LP)
==============================
Decision variables:
    X[side, qt]           — 4 scalars (bucket-level constant)
    Y[trader, side, qt]   — M scalars (trader-level multiplier)

Per unlocked bond i:
    final_charge_i  = Y[t,s,q] × blended_i  −  X[s,q]       (in charge-bps)
    kappa_i         = DV01 (SPD) or notional/10_000 (PX)
    sign_i          = +1 (SELL+PX, BUY+SPD), −1 (SELL+SPD, BUY+PX)
    proceeds_i      = base_i + sign_i × final_charge_i × kappa_i

Objective:  MAXIMIZE Σ proceeds_i
Constraints:
    1. Side floors     (≥ pct of starting)
    2. Trader band     (charge-$ near risk-weighted target ± buffer)
    3. Mid-line        (charge sign convention)
    4. Max charge Δ    (no single bond moves more than max_charge_delta)

Python 3.11+ / Polars / CVXPY
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Any

import cvxpy as cp
import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Column mapping — swap VALUES with your actual column names
# ══════════════════════════════════════════════════════════════════════════════
COLUMN_MAP: dict[str, str] = {
    "bond_id":        "id",
    "trader":         "desigName",
    "side":           "side",
    "quote_type":     "quoteType",
    "ref_mid_px":     "refMidPx",
    "ref_mid_spd":    "refMidSpd",
    "quote_px":       "quotePx",
    "quote_spd":      "quoteSpd",
    "skew":           "skew",
    "proceeds":       "proceeds",
    "size":           "size",
    "dv01":           "dv01",
    "liq_score":      "macpLiqScore",
    "bsr_notional":   "bsr_notional",
    "bsi_notional":   "bsi_notional",
    "locked":         "locked",
}

def _c(k: str) -> str:
    return COLUMN_MAP[k]

# ══════════════════════════════════════════════════════════════════════════════
# BSR / BSI weight matrix  (liq → (bsr_w, bsi_w))
# ══════════════════════════════════════════════════════════════════════════════
DEFAULT_BSR_BSI_MATRIX: dict[int, tuple[float, float]] = {
    1: (5.00, 4.00), 2: (4.50, 3.50), 3: (4.00, 3.00), 4: (3.50, 2.50),
    5: (3.00, 2.00), 6: (2.50, 1.50), 7: (2.00, 1.00), 8: (1.50, 0.75),
    9: (1.00, 0.50), 10: (0.50, 0.25),
}

SIGN_MAP = {
    ("SELL", "PX"): +1.0, ("SELL", "SPD"): -1.0,
    ("BUY",  "PX"): -1.0, ("BUY",  "SPD"): +1.0,
}

# Solver-specific kwargs
_SOLVER_KW: dict[str, dict] = {
    "SCS": {"max_iters": 100_000}, "ECOS": {}, "OSQP": {"max_iter": 100_000}, "CLARABEL": {},
}

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class OptimizerConfig:
    side_floor_pct: float = 0.95
    buffer: float = 100.0
    risk_weights: tuple[float, float, float, float] = (0.50, 0.25, 0.15, 0.10)
    bsr_bsi_matrix: dict[int, tuple[float, float]] = field(default_factory=lambda: DEFAULT_BSR_BSI_MATRIX)

    # ── NEW: max per-bond charge movement (in charge-bps) ──
    # For SPD bonds 1 charge-bp = 1 spread-bp.
    # For PX bonds 1 charge-bp = 0.01 price points.
    # Set to None to disable.
    max_charge_delta: float | None = 1.5  # default: limit to 1.5 bps

    solver: str = "SCS"
    solver_verbose: bool = False

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
def _parse_num(s) -> float:
    return float(s.replace(",", "").strip()) if isinstance(s, str) else float(s)

def _derive_dv01(qt, size, ref_mid_px, ref_mid_spd, quote_px, quote_spd):
    """Derive DV01 for ALL bonds from spread/price relationship."""
    N = len(qt)
    dv01 = np.zeros(N)
    for i in range(N):
        d_spd = quote_spd[i] - ref_mid_spd[i]
        d_px = quote_px[i] - ref_mid_px[i]
        if abs(d_spd) < 1e-8:
            dv01[i] = 0.08 * size[i] / 100.0
        else:
            dv01[i] = abs(d_px / d_spd) * size[i] / 100.0
    return dv01

def _compute_kappa(qt, size, dv01):
    """κ = DV01 for SPD, size/10000 for PX."""
    return np.array([dv01[i] if qt[i] == "SPD" else size[i] / 10_000.0 for i in range(len(qt))])

def _compute_trader_risk(side, dv01, liq, mid_spd):
    return np.array([
        (1.0 if side[i] == "BUY" else -1.0) * dv01[i] * (np.sqrt(11) - np.sqrt(liq[i])) * mid_spd[i] / 100.0
        for i in range(len(side))
    ])

def _compute_blended_charge(liq, pct_bsr, pct_bsi, matrix):
    return np.array([
        pct_bsr[i] * matrix[int(np.clip(liq[i], 1, 10))][0] +
        pct_bsi[i] * matrix[int(np.clip(liq[i], 1, 10))][1]
        for i in range(len(liq))
    ])

def _compute_wavg_risk_pct(t_risk, pct_bsi, traders, sides, qts, locked, cfg):
    w_risk = cfg.risk_weights[0] + cfg.risk_weights[1]  # 0.75
    w_bsi = cfg.risk_weights[2] + cfg.risk_weights[3]    # 0.25
    N = len(traders)

    bucket_bonds: dict[tuple[str, str], list[int]] = {}
    for i in range(N):
        bk = (sides[i], qts[i])
        bucket_bonds.setdefault(bk, []).append(i)

    result: dict[tuple[str, str, str], float] = {}
    for bk, idxs in bucket_bonds.items():
        t_abs: dict[str, float] = {}
        t_bsi: dict[str, float] = {}
        for i in idxs:
            t = traders[i]
            ar = abs(t_risk[i])
            t_abs[t] = t_abs.get(t, 0) + ar
            t_bsi[t] = t_bsi.get(t, 0) + ar * pct_bsi[i]
        tot_abs = sum(t_abs.values()) or 1e-12
        tot_bsi = sum(t_bsi.values()) or 1e-12
        for t in t_abs:
            result[(t, bk[0], bk[1])] = w_risk * t_abs[t] / tot_abs + w_bsi * t_bsi[t] / tot_bsi
    return result

# ══════════════════════════════════════════════════════════════════════════════
# Result
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class OptimizationResult:
    status: str
    optimal: bool
    objective_value: float
    X_values: dict[tuple[str, str], float]
    Y_values: dict[tuple[str, str, str], float]
    final_charges: np.ndarray
    final_proceeds: np.ndarray
    starting_proceeds_by_side: dict[str, float]
    final_proceeds_by_side: dict[str, float]
    risk_pct: dict[tuple[str, str, str], float]

# ══════════════════════════════════════════════════════════════════════════════
# Solver
# ══════════════════════════════════════════════════════════════════════════════
def solve(df: pl.DataFrame, cfg: OptimizerConfig | None = None) -> tuple[pl.DataFrame, OptimizationResult]:
    cfg = cfg or OptimizerConfig()
    N = len(df)

    # ── Extract ───────────────────────────────────────────────────────────
    bond_ids    = df[_c("bond_id")].to_list()
    traders     = df[_c("trader")].to_list()
    sides       = df[_c("side")].to_list()
    qts         = df[_c("quote_type")].to_list()
    ref_mid_px  = df[_c("ref_mid_px")].to_numpy().astype(float)
    ref_mid_spd = df[_c("ref_mid_spd")].to_numpy().astype(float)
    quote_px    = df[_c("quote_px")].to_numpy().astype(float)
    quote_spd   = df[_c("quote_spd")].to_numpy().astype(float)
    skew        = df[_c("skew")].to_numpy().astype(float)
    proceeds    = df[_c("proceeds")].to_numpy().astype(float)
    liq         = df[_c("liq_score")].to_numpy().astype(float)

    sc = _c("size")
    sizes = np.array([_parse_num(s) for s in df[sc].to_list()]) if df[sc].dtype == pl.Utf8 else df[sc].to_numpy().astype(float)

    # DV01: use column if present, else derive
    if _c("dv01") in df.columns:
        dv01 = df[_c("dv01")].to_numpy().astype(float)
    else:
        dv01 = _derive_dv01(qts, sizes, ref_mid_px, ref_mid_spd, quote_px, quote_spd)

    # BSR/BSI
    bsr_n = df[_c("bsr_notional")].to_numpy().astype(float) if _c("bsr_notional") in df.columns else sizes.copy()
    bsi_n = df[_c("bsi_notional")].to_numpy().astype(float) if _c("bsi_notional") in df.columns else np.zeros(N)
    locked = df[_c("locked")].to_numpy() if _c("locked") in df.columns else np.zeros(N, dtype=bool)

    # ── Derived ───────────────────────────────────────────────────────────
    kappa = _compute_kappa(qts, sizes, dv01)
    sign = np.array([SIGN_MAP[(sides[i], qts[i])] for i in range(N)])
    base_proceeds = ref_mid_px / 100.0 * sizes

    tot_n = bsr_n + bsi_n
    tot_n = np.where(tot_n > 0, tot_n, 1.0)
    pct_bsr, pct_bsi = bsr_n / tot_n, bsi_n / tot_n

    blended = _compute_blended_charge(liq, pct_bsr, pct_bsi, cfg.bsr_bsi_matrix)
    t_risk = _compute_trader_risk(sides, dv01, liq, ref_mid_spd)

    # Starting charge in bps: PX skew is in price pts → ×100, SPD is already bps
    is_px = np.array([q == "PX" for q in qts])
    starting_charge_bps = np.where(is_px, skew * 100.0, skew)

    risk_pct = _compute_wavg_risk_pct(t_risk, pct_bsi, traders, sides, qts, locked, cfg)

    # ── Groups ────────────────────────────────────────────────────────────
    bucket_keys_s: set[tuple[str, str]] = set()
    tbk_keys_s: set[tuple[str, str, str]] = set()
    bond_bk: list[tuple[str, str]] = []
    bond_tbk: list[tuple[str, str, str]] = []
    for i in range(N):
        bk = (sides[i], qts[i]); tbk = (traders[i], sides[i], qts[i])
        bucket_keys_s.add(bk); tbk_keys_s.add(tbk)
        bond_bk.append(bk); bond_tbk.append(tbk)

    bucket_keys = sorted(bucket_keys_s)
    tbk_keys = sorted(tbk_keys_s)
    bk_idx = {bk: j for j, bk in enumerate(bucket_keys)}
    tbk_idx = {tbk: j for j, tbk in enumerate(tbk_keys)}
    n_bk, n_tbk = len(bucket_keys), len(tbk_keys)

    unlocked = ~locked
    ul_idx = np.where(unlocked)[0]
    lk_idx = np.where(locked)[0]
    n_ul = len(ul_idx)

    # Starting proceeds by side / bucket
    start_by_side: dict[str, float] = {}
    for s in ("BUY", "SELL"):
        m = np.array([sides[i] == s for i in range(N)])
        start_by_side[s] = float(np.sum(proceeds[m]))

    start_charge_by_bk: dict[tuple[str, str], float] = {}
    for bk in bucket_keys:
        m = np.array([bond_bk[i] == bk for i in range(N)])
        start_charge_by_bk[bk] = float(np.sum(sign[m] * starting_charge_bps[m] * kappa[m]))

    # ══════════════════════════════════════════════════════════════════════
    # BUILD LP
    # ══════════════════════════════════════════════════════════════════════
    X = cp.Variable(n_bk, name="X")
    Y = cp.Variable(n_tbk, name="Y")

    # Per unlocked bond: final_charge_bps = Y[tbk]*blended - X[bk]
    # proceeds = base + sign * final_charge * kappa
    # We build the per-bond final_charge as CVXPY expressions to constrain them individually.

    # Coefficient matrices for vectorised proceeds
    A_Y = np.zeros((n_ul, n_tbk))
    A_X = np.zeros((n_ul, n_bk))
    const = np.zeros(n_ul)
    y_per = np.zeros(n_ul, dtype=int)
    x_per = np.zeros(n_ul, dtype=int)

    for j, i in enumerate(ul_idx):
        A_Y[j, tbk_idx[bond_tbk[i]]] = sign[i] * kappa[i] * blended[i]
        A_X[j, bk_idx[bond_bk[i]]] = -sign[i] * kappa[i]
        const[j] = base_proceeds[i]
        y_per[j] = tbk_idx[bond_tbk[i]]
        x_per[j] = bk_idx[bond_bk[i]]

    ul_proceeds = const + A_Y @ Y + A_X @ X
    locked_total = float(np.sum(proceeds[lk_idx])) if len(lk_idx) else 0.0

    objective = cp.Maximize(cp.sum(ul_proceeds) + locked_total)
    constraints: list[cp.Constraint] = []

    # 1) Side floors
    for s in ("BUY", "SELL"):
        if start_by_side.get(s, 0) == 0:
            continue
        sm = np.array([sides[ul_idx[j]] == s for j in range(n_ul)])
        lk_s = float(np.sum(proceeds[lk_idx[np.array([sides[i] == s for i in lk_idx])]])) if len(lk_idx) else 0.0
        side_expr = (
            float(np.sum(const[sm]))
            + (A_Y[sm].sum(0) if sm.any() else np.zeros(n_tbk)) @ Y
            + (A_X[sm].sum(0) if sm.any() else np.zeros(n_bk)) @ X
            + lk_s
        )
        constraints.append(side_expr >= cfg.side_floor_pct * start_by_side[s])

    # 2) Trader band (on charge proceeds)
    for tbk in tbk_keys:
        t, s, q = tbk; bk = (s, q)
        rp = risk_pct.get(tbk, 0.0)
        target = rp * start_charge_by_bk.get(bk, 0.0)
        tm = np.array([bond_tbk[ul_idx[j]] == tbk for j in range(n_ul)])
        lk_tbk = [i for i in lk_idx if bond_tbk[i] == tbk]
        lk_chg = float(np.sum(sign[lk_tbk] * starting_charge_bps[lk_tbk] * kappa[lk_tbk])) if lk_tbk else 0.0
        tbk_expr = (
            (A_Y[tm].sum(0) if tm.any() else np.zeros(n_tbk)) @ Y
            + (A_X[tm].sum(0) if tm.any() else np.zeros(n_bk)) @ X
            + lk_chg
        )
        constraints.append(tbk_expr <= target)
        constraints.append(tbk_expr >= target - cfg.buffer)

    # 3) Mid-line  AND  4) Max charge delta
    def _add_bond_constraints(constraints_list, max_delta):
        for j, i in enumerate(ul_idx):
            fc = Y[tbk_idx[bond_tbk[i]]] * blended[i] - X[bk_idx[bond_bk[i]]]
            if bond_bk[i] in (("SELL", "PX"), ("BUY", "SPD")):
                constraints_list.append(fc >= 0)
            else:
                constraints_list.append(fc <= 0)
            if max_delta is not None:
                sc_bps = starting_charge_bps[i]
                constraints_list.append(fc >= sc_bps - max_delta)
                constraints_list.append(fc <= sc_bps + max_delta)

    _add_bond_constraints(constraints, cfg.max_charge_delta)

    # ── Solve with auto-relaxation ────────────────────────────────────────
    def _try_solve(prob):
        for slv in [cfg.solver, "SCS", "ECOS", "CLARABEL"]:
            kw = _SOLVER_KW.get(slv, {})
            try:
                prob.solve(solver=slv, verbose=cfg.solver_verbose, **kw)
                if prob.status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE):
                    return True
            except Exception as e:
                logger.warning("Solver %s failed: %s", slv, e)
        return False

    prob = cp.Problem(objective, constraints)
    is_ok = _try_solve(prob)
    actual_max_delta = cfg.max_charge_delta
    fell_back = False

    # Auto-relax: double max_charge_delta up to 3 times, then remove it
    if not is_ok and cfg.max_charge_delta is not None:
        for multiplier in [2.0, 4.0, None]:
            new_delta = cfg.max_charge_delta * multiplier if multiplier else None
            logger.warning(
                "Infeasible with max_charge_delta=%.2f. Relaxing to %s...",
                actual_max_delta if actual_max_delta else 0,
                f"{new_delta:.2f}" if new_delta else "uncapped",
            )
            constraints_retry: list[cp.Constraint] = []
            # Rebuild side + trader constraints (same as before)
            for s in ("BUY", "SELL"):
                if start_by_side.get(s, 0) == 0: continue
                sm = np.array([sides[ul_idx[j]] == s for j in range(n_ul)])
                lk_s = float(np.sum(proceeds[lk_idx[np.array([sides[i] == s for i in lk_idx])]])) if len(lk_idx) else 0.0
                side_expr = float(np.sum(const[sm])) + (A_Y[sm].sum(0) if sm.any() else np.zeros(n_tbk)) @ Y + (A_X[sm].sum(0) if sm.any() else np.zeros(n_bk)) @ X + lk_s
                constraints_retry.append(side_expr >= cfg.side_floor_pct * start_by_side[s])
            for tbk in tbk_keys:
                t2, s2, q2 = tbk; bk2 = (s2, q2)
                rp2 = risk_pct.get(tbk, 0.0)
                target2 = rp2 * start_charge_by_bk.get(bk2, 0.0)
                tm2 = np.array([bond_tbk[ul_idx[j]] == tbk for j in range(n_ul)])
                lk_tbk2 = [i for i in lk_idx if bond_tbk[i] == tbk]
                lk_chg2 = float(np.sum(sign[lk_tbk2] * starting_charge_bps[lk_tbk2] * kappa[lk_tbk2])) if lk_tbk2 else 0.0
                tbk_expr2 = (A_Y[tm2].sum(0) if tm2.any() else np.zeros(n_tbk)) @ Y + (A_X[tm2].sum(0) if tm2.any() else np.zeros(n_bk)) @ X + lk_chg2
                constraints_retry.append(tbk_expr2 <= target2)
                constraints_retry.append(tbk_expr2 >= target2 - cfg.buffer)
            _add_bond_constraints(constraints_retry, new_delta)
            prob = cp.Problem(objective, constraints_retry)
            is_ok = _try_solve(prob)
            actual_max_delta = new_delta
            fell_back = True
            if is_ok:
                break

    is_ok_final = prob.status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE)
    if not is_ok_final:
        logger.error("Optimization failed even after relaxation: %s", prob.status)

    X_val = X.value if X.value is not None else np.zeros(n_bk)
    Y_val = Y.value if Y.value is not None else np.zeros(n_tbk)

    # ── Extract results (fall back to STARTING values if infeasible) ──────
    if not is_ok_final:
        # Keep everything at starting values
        final_charge_bps = np.copy(starting_charge_bps)
        final_proceeds_arr = proceeds.copy()
        logger.warning("Using starting values as fallback (no solution found).")
    else:
        final_charge_bps = np.copy(starting_charge_bps)
        final_proceeds_arr = proceeds.copy()
        for j, i in enumerate(ul_idx):
            fc = Y_val[tbk_idx[bond_tbk[i]]] * blended[i] - X_val[bk_idx[bond_bk[i]]]
            final_charge_bps[i] = fc
            final_proceeds_arr[i] = base_proceeds[i] + sign[i] * fc * kappa[i]

    # ── Implied prices, spreads, skews ────────────────────────────────────
    implied_px = final_proceeds_arr / sizes * 100.0
    implied_spd = np.where(~is_px, ref_mid_spd + final_charge_bps, np.nan)
    implied_skew = np.where(is_px, final_charge_bps / 100.0, final_charge_bps)
    charge_delta_display = implied_skew - skew

    # Build result
    X_dict = {bk: float(X_val[bk_idx[bk]]) for bk in bucket_keys}
    Y_dict = {tbk: float(Y_val[tbk_idx[tbk]]) for tbk in tbk_keys}
    final_by_side: dict[str, float] = {}
    for s in ("BUY", "SELL"):
        m = np.array([sides[i] == s for i in range(N)])
        final_by_side[s] = float(np.sum(final_proceeds_arr[m]))

    result = OptimizationResult(
        status=prob.status, optimal=is_ok_final,
        objective_value=prob.value if prob.value is not None else float("nan"),
        X_values=X_dict, Y_values=Y_dict,
        final_charges=final_charge_bps, final_proceeds=final_proceeds_arr,
        starting_proceeds_by_side=start_by_side, final_proceeds_by_side=final_by_side,
        risk_pct=risk_pct,
    )
    result._fell_back = fell_back if 'fell_back' in dir() else False
    result._actual_max_delta = actual_max_delta if 'actual_max_delta' in dir() else cfg.max_charge_delta

    df_result = df.with_columns(
        pl.Series("base_proceeds", np.round(base_proceeds, 0)),
        pl.Series("blended_charge", np.round(blended, 4)),
        pl.Series("kappa", np.round(kappa, 2)),
        pl.Series("sign", sign),
        pl.Series("trader_risk", np.round(t_risk, 2)),
        pl.Series("risk_share", [round(risk_pct.get((traders[i], sides[i], qts[i]), 0), 4) for i in range(N)]),
        pl.Series("final_charge_bps", np.round(final_charge_bps, 4)),
        pl.Series("final_skew", np.round(implied_skew, 4)),
        pl.Series("skew_delta", np.round(charge_delta_display, 4)),
        pl.Series("final_proceeds", np.round(final_proceeds_arr, 0)),
        pl.Series("proceeds_delta", np.round(final_proceeds_arr - proceeds, 0)),
        pl.Series("implied_px", np.round(implied_px, 6)),
        pl.Series("implied_spd", np.round(np.nan_to_num(implied_spd, nan=0), 4)),
        pl.Series("spd_delta", np.round(np.where(~is_px, implied_spd - quote_spd, 0), 4)),
    )

    return df_result, result

# ══════════════════════════════════════════════════════════════════════════════
# Summaries
# ══════════════════════════════════════════════════════════════════════════════
def summary_by_trader_side_qt(df_result: pl.DataFrame) -> pl.DataFrame:
    s, q, t = _c("side"), _c("quote_type"), _c("trader")
    return (
        df_result.group_by([t, s, q]).agg(
            pl.count().alias("bond_count"),
            pl.col(_c("proceeds")).sum().alias("starting_proceeds"),
            pl.col("final_proceeds").sum().alias("final_proceeds"),
            pl.col("proceeds_delta").sum().alias("proceeds_delta"),
            (pl.col(_c("skew")) * pl.col("kappa").abs()).sum().alias("_ws"),
            (pl.col("final_skew") * pl.col("kappa").abs()).sum().alias("_wf"),
            pl.col("kappa").abs().sum().alias("_wk"),
            pl.col("risk_share").first().alias("risk_share"),
        )
        .with_columns(
            (pl.col("_ws") / pl.col("_wk")).alias("wavg_start_skew"),
            (pl.col("_wf") / pl.col("_wk")).alias("wavg_final_skew"),
        )
        .with_columns((pl.col("wavg_final_skew") - pl.col("wavg_start_skew")).alias("wavg_skew_delta"))
        .drop("_ws", "_wf", "_wk")
        .sort([s, q, t])
    )

def summary_by_side_qt(df_result: pl.DataFrame) -> pl.DataFrame:
    s, q = _c("side"), _c("quote_type")
    return (
        df_result.group_by([s, q]).agg(
            pl.count().alias("bond_count"),
            pl.col(_c("proceeds")).sum().alias("starting_proceeds"),
            pl.col("final_proceeds").sum().alias("final_proceeds"),
            pl.col("proceeds_delta").sum().alias("proceeds_delta"),
            (pl.col(_c("skew")) * pl.col("kappa").abs()).sum().alias("_ws"),
            (pl.col("final_skew") * pl.col("kappa").abs()).sum().alias("_wf"),
            pl.col("kappa").abs().sum().alias("_wk"),
        )
        .with_columns(
            (pl.col("_ws") / pl.col("_wk")).alias("wavg_start_skew"),
            (pl.col("_wf") / pl.col("_wk")).alias("wavg_final_skew"),
        )
        .with_columns((pl.col("wavg_final_skew") - pl.col("wavg_start_skew")).alias("wavg_skew_delta"))
        .drop("_ws", "_wf", "_wk")
        .sort([s, q])
    )

def print_report(df_result: pl.DataFrame, result: OptimizationResult, cfg: OptimizerConfig | None = None) -> None:
    cfg = cfg or OptimizerConfig()
    fb_note = ""
    if hasattr(result, '_fell_back') and result._fell_back:
        fb_note = f"  (relaxed from {cfg.max_charge_delta} → {result._actual_max_delta})"
    print("=" * 110)
    print(f"  CHARGE OPTIMIZATION REPORT")
    print(f"  status        : {result.status}")
    print(f"  objective     : ${result.objective_value:,.0f}")
    print(f"  max_charge_Δ  : {cfg.max_charge_delta}{fb_note}")
    print("=" * 110)

    print("\n── X / Y values ──")
    for k, v in sorted(result.X_values.items()):
        print(f"  X[{k[0]:4s},{k[1]:3s}] = {v:+.6f}")
    for k, v in sorted(result.Y_values.items()):
        print(f"  Y[{k[0]:20s},{k[1]:4s},{k[2]:3s}] = {v:+.6f}")

    print(f"\n── Side proceeds ──")
    for s in ("BUY", "SELL"):
        st = result.starting_proceeds_by_side.get(s, 0)
        fi = result.final_proceeds_by_side.get(s, 0)
        if st > 0:
            print(f"  {s:4s}: start=${st:>15,.0f}  final=${fi:>15,.0f}  Δ=${fi-st:>+12,.0f}  ({fi/st*100:.2f}%)")

    print(f"\n── Overall ──")
    _pp(summary_by_side_qt(df_result))
    print(f"\n── By Trader ──")
    _pp(summary_by_trader_side_qt(df_result))

    print(f"\n── Bond Detail ──")
    cols = [
        _c("bond_id"), _c("trader"), _c("quote_type"),
        _c("skew"), "final_skew", "skew_delta",
        _c("quote_spd"), "implied_spd", "spd_delta",
        _c("quote_px"), "implied_px",
        _c("proceeds"), "final_proceeds", "proceeds_delta", "risk_share",
    ]
    _pp(df_result.select([c for c in cols if c in df_result.columns]))

def _pp(df):
    with pl.Config(tbl_cols=-1, tbl_rows=50, tbl_width_chars=220, float_precision=4):
        print(df)
