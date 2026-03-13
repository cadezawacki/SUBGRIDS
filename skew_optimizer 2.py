"""
Bond Charge Optimization (LP Formulation)
==========================================

Decision variables:
    X[side, qt]          — 4 scalars (one per bucket)
    Y[trader, side, qt]  — M scalars (one per trader-bucket group)

Per unlocked bond i  (trader=t, side=s, qt=q):
    final_charge_i  = Y[t,s,q] × blended_charge_i  −  X[s,q]
    kappa_i         = DV01 (SPD) or notional/10_000 (PX)
    sign_i          = +1 for (SELL,PX)/(BUY,SPD), −1 for (SELL,SPD)/(BUY,PX)
    proceeds_i      = base_i + sign_i × final_charge_i × kappa_i

Objective:
    MAXIMIZE  Σ proceeds_i   (= base + charge component)

Constraints:
    1. Side floors:  Σ proceeds (BUY)  ≥ 0.95 × starting_bid_proceeds
                     Σ proceeds (SELL) ≥ 0.95 × starting_offer_proceeds
    2. Trader band:  For each (t,s,q):
         target − buffer  ≤  Σ proceeds_i  ≤  target
         where target = wavg_risk_pct[t,s,q] × starting_proceeds[s,q]
    3. Mid-line:     final_charge respects sign convention per bucket

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
# Column Mapping — swap VALUES with your actual column names
# ══════════════════════════════════════════════════════════════════════════════

COLUMN_MAP: dict[str, str] = {
    "bond_id":        "id",
    "trader":         "desigName",
    "side":           "side",             # "BUY" or "SELL"
    "quote_type":     "quoteType",        # "PX" or "SPD"
    "ref_mid_px":     "refMidPx",
    "ref_mid_spd":    "refMidSpd",
    "quote_px":       "quotePx",
    "quote_spd":      "quoteSpd",
    "skew":           "skew",             # current charge (price pts PX / spread bps SPD)
    "proceeds":       "proceeds",         # current total dollar proceeds
    "size":           "size",             # notional face value
    "liq_score":      "macpLiqScore",     # 1–10
    # optional — defaults applied if missing
    "dv01":           "dv01",             # spread DV01 ($ per 1bp). Derived if absent.
    "bsr_notional":   "bsr_notional",     # BSR notional per line
    "bsi_notional":   "bsi_notional",     # BSI notional per line
    "locked":         "locked",           # bool — True = frozen, excluded from solve
}

_C = COLUMN_MAP  # shorthand


def _c(k: str) -> str:
    return COLUMN_MAP[k]


# ══════════════════════════════════════════════════════════════════════════════
# BSR / BSI Weight Matrix  (liq_score → (bsr_weight, bsi_weight))
# Replace with your actual matrix.
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_BSR_BSI_MATRIX: dict[int, tuple[float, float]] = {
    1:  (5.00, 4.00),
    2:  (4.50, 3.50),
    3:  (4.00, 3.00),
    4:  (3.50, 2.50),
    5:  (3.00, 2.00),
    6:  (2.50, 1.50),
    7:  (2.00, 1.00),
    8:  (1.50, 0.75),
    9:  (1.00, 0.50),
    10: (0.50, 0.25),
}


# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class OptimizerConfig:
    # side-level floor
    side_floor_pct: float = 0.95

    # trader-level band
    buffer: float = 100.0  # small tolerance ($) below target

    # wavg risk weights:  net, gross, net_bsi, gross_bsi
    # Within a same-side bucket net==gross, so effectively:
    #   (0.5+0.25)*risk_share + (0.15+0.10)*bsi_risk_share = 0.75*risk + 0.25*bsi
    risk_weights: tuple[float, float, float, float] = (0.50, 0.25, 0.15, 0.10)

    bsr_bsi_matrix: dict[int, tuple[float, float]] = field(
        default_factory=lambda: DEFAULT_BSR_BSI_MATRIX
    )

    # solver
    solver: str = "SCS"
    solver_verbose: bool = False
    solver_kwargs: dict[str, Any] = field(default_factory=lambda: {"max_iters": 100_000})


# Solver-specific kwargs (don't pass incompatible args)
_SOLVER_KWARGS: dict[str, dict] = {
    "SCS":     {"max_iters": 100_000},
    "ECOS":    {},
    "OSQP":    {"max_iter": 100_000},
    "CLARABEL": {},
}


# ══════════════════════════════════════════════════════════════════════════════
# Derived quantities
# ══════════════════════════════════════════════════════════════════════════════

SIGN_MAP: dict[tuple[str, str], float] = {
    ("SELL", "PX"):  +1.0,
    ("SELL", "SPD"): -1.0,
    ("BUY",  "PX"):  -1.0,
    ("BUY",  "SPD"): +1.0,
}


def _derive_dv01(
    qt: list[str],
    size: np.ndarray,
    ref_mid_px: np.ndarray,
    ref_mid_spd: np.ndarray,
    quote_px: np.ndarray,
    quote_spd: np.ndarray,
) -> np.ndarray:
    """
    Derive spread DV01 for ALL bonds from the two known price/spread points.
    DV01 = |dPx/dSpd| × size / 100   (always positive, units = $/bp)

    Both PX and SPD bonds carry spread and price data, so DV01 is computed
    for every bond regardless of quote type.  The quote type only determines
    whether κ uses DV01 (SPD) or notional/10_000 (PX).
    """
    N = len(qt)
    dv01 = np.zeros(N)
    for i in range(N):
        d_spd = quote_spd[i] - ref_mid_spd[i]
        d_px = quote_px[i] - ref_mid_px[i]
        if abs(d_spd) < 1e-8:
            dv01[i] = 0.08 * size[i] / 100.0  # fallback
        else:
            dv01[i] = abs(d_px / d_spd) * size[i] / 100.0
    return dv01


def _compute_kappa(qt: list[str], size: np.ndarray, dv01: np.ndarray) -> np.ndarray:
    """
    κ_i = DV01  for SPD bonds   ($ per 1bp spread change)
    κ_i = size / 10_000  for PX bonds  ($ per 1bp price change)
    """
    N = len(qt)
    kappa = np.empty(N)
    for i in range(N):
        kappa[i] = dv01[i] if qt[i] == "SPD" else size[i] / 10_000.0
    return kappa


def _compute_trader_risk(
    side: list[str],
    dv01: np.ndarray,
    liq: np.ndarray,
    mid_spd: np.ndarray,
) -> np.ndarray:
    """
    Per-bond trader risk measure:
        risk_i = (±1 for side) × DV01_i × (√11 − √liq_i) × mid_spread_i / 100
    """
    N = len(side)
    risk = np.empty(N)
    for i in range(N):
        s = 1.0 if side[i] == "BUY" else -1.0
        risk[i] = s * dv01[i] * (np.sqrt(11.0) - np.sqrt(liq[i])) * mid_spd[i] / 100.0
    return risk


def _compute_blended_charge(
    liq: np.ndarray,
    pct_bsr: np.ndarray,
    pct_bsi: np.ndarray,
    matrix: dict[int, tuple[float, float]],
) -> np.ndarray:
    """
    blended_charge_i = %BSR_i × BSR_WEIGHT(liq) + %BSI_i × BSI_WEIGHT(liq)
    """
    N = len(liq)
    bc = np.empty(N)
    for i in range(N):
        ls = int(np.clip(liq[i], 1, 10))
        bsr_w, bsi_w = matrix[ls]
        bc[i] = pct_bsr[i] * bsr_w + pct_bsi[i] * bsi_w
    return bc


def _compute_wavg_risk_pct(
    trader_risk: np.ndarray,
    pct_bsi: np.ndarray,
    traders: list[str],
    sides: list[str],
    qts: list[str],
    locked: np.ndarray,
    cfg: OptimizerConfig,
) -> dict[tuple[str, str, str], float]:
    """
    For each (side, qt) bucket compute the wavg risk percent per trader.

    wavg_risk_pct = w_net × net_pct + w_gross × gross_pct
                  + w_net_bsi × net_bsi_pct + w_gross_bsi × gross_bsi_pct

    Within a same-side bucket net_pct == gross_pct (all same sign), so:
        = (w_net + w_gross) × risk_share + (w_net_bsi + w_gross_bsi) × bsi_risk_share

    Returns dict  (trader, side, qt) → fraction  [sums to 1.0 per (side,qt)]
    """
    w_net, w_gross, w_net_bsi, w_gross_bsi = cfg.risk_weights
    w_risk = w_net + w_gross        # 0.75
    w_bsi = w_net_bsi + w_gross_bsi # 0.25

    N = len(traders)

    # group by (side, qt)
    bucket_bonds: dict[tuple[str, str], list[int]] = {}
    for i in range(N):
        if locked[i]:
            continue
        bk = (sides[i], qts[i])
        bucket_bonds.setdefault(bk, []).append(i)

    # also include locked bonds in risk calculation (they still carry risk)
    bucket_bonds_all: dict[tuple[str, str], list[int]] = {}
    for i in range(N):
        bk = (sides[i], qts[i])
        bucket_bonds_all.setdefault(bk, []).append(i)

    result: dict[tuple[str, str, str], float] = {}

    for bk, idxs in bucket_bonds_all.items():
        # per trader: abs risk and bsi-weighted risk
        trader_abs_risk: dict[str, float] = {}
        trader_bsi_risk: dict[str, float] = {}

        for i in idxs:
            t = traders[i]
            ar = abs(trader_risk[i])
            br = abs(trader_risk[i]) * pct_bsi[i]
            trader_abs_risk[t] = trader_abs_risk.get(t, 0.0) + ar
            trader_bsi_risk[t] = trader_bsi_risk.get(t, 0.0) + br

        total_abs = sum(trader_abs_risk.values()) or 1e-12
        total_bsi = sum(trader_bsi_risk.values()) or 1e-12

        for t in trader_abs_risk:
            risk_share = trader_abs_risk[t] / total_abs
            bsi_share = trader_bsi_risk[t] / total_bsi
            result[(t, bk[0], bk[1])] = w_risk * risk_share + w_bsi * bsi_share

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Result
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class OptimizationResult:
    status: str
    optimal: bool
    objective_value: float
    X_values: dict[tuple[str, str], float]     # X[side, qt]
    Y_values: dict[tuple[str, str, str], float] # Y[trader, side, qt]
    final_charges: np.ndarray
    final_proceeds: np.ndarray
    starting_proceeds_by_side: dict[str, float]
    final_proceeds_by_side: dict[str, float]
    risk_pct: dict[tuple[str, str, str], float]


# ══════════════════════════════════════════════════════════════════════════════
# Solver
# ══════════════════════════════════════════════════════════════════════════════

def _parse_num(s) -> float:
    if isinstance(s, str):
        return float(s.replace(",", "").strip())
    return float(s)


def solve(
    df: pl.DataFrame,
    cfg: OptimizerConfig | None = None,
) -> tuple[pl.DataFrame, OptimizationResult]:
    cfg = cfg or OptimizerConfig()
    N = len(df)

    # ── extract arrays ────────────────────────────────────────────────────
    bond_ids  = df[_c("bond_id")].to_list()
    traders   = df[_c("trader")].to_list()
    sides     = df[_c("side")].to_list()
    qts       = df[_c("quote_type")].to_list()
    ref_mid_px  = df[_c("ref_mid_px")].to_numpy().astype(float)
    ref_mid_spd = df[_c("ref_mid_spd")].to_numpy().astype(float)
    quote_px    = df[_c("quote_px")].to_numpy().astype(float)
    quote_spd   = df[_c("quote_spd")].to_numpy().astype(float)
    skew        = df[_c("skew")].to_numpy().astype(float)
    proceeds    = df[_c("proceeds")].to_numpy().astype(float)
    liq         = df[_c("liq_score")].to_numpy().astype(float)

    # size (may be string)
    sc = _c("size")
    if df[sc].dtype == pl.Utf8:
        sizes = np.array([_parse_num(s) for s in df[sc].to_list()])
    else:
        sizes = df[sc].to_numpy().astype(float)

    # optional: dv01 — derive if missing
    dv01_col = _c("dv01")
    if dv01_col in df.columns:
        dv01 = df[dv01_col].to_numpy().astype(float)
    else:
        dv01 = _derive_dv01(qts, sizes, ref_mid_px, ref_mid_spd, quote_px, quote_spd)

    # optional: bsr/bsi notional — default all BSR
    if _c("bsr_notional") in df.columns:
        bsr_n = df[_c("bsr_notional")].to_numpy().astype(float)
    else:
        bsr_n = sizes.copy()

    if _c("bsi_notional") in df.columns:
        bsi_n = df[_c("bsi_notional")].to_numpy().astype(float)
    else:
        bsi_n = np.zeros(N)

    # optional: locked
    if _c("locked") in df.columns:
        locked = df[_c("locked")].to_numpy()
    else:
        locked = np.zeros(N, dtype=bool)

    # ── derived quantities ────────────────────────────────────────────────
    kappa = _compute_kappa(qts, sizes, dv01)
    sign = np.array([SIGN_MAP[(sides[i], qts[i])] for i in range(N)])
    base_proceeds = ref_mid_px / 100.0 * sizes

    total_notional = bsr_n + bsi_n
    total_notional = np.where(total_notional > 0, total_notional, 1.0)
    pct_bsr = bsr_n / total_notional
    pct_bsi = bsi_n / total_notional

    blended = _compute_blended_charge(liq, pct_bsr, pct_bsi, cfg.bsr_bsi_matrix)
    t_risk = _compute_trader_risk(sides, dv01, liq, ref_mid_spd)

    # For PX bonds, the skew in the data is in price points.
    # Our kappa_px = size/10000 expects charge in price basis points.
    # Convert: starting_charge_bps = skew * 100 for PX, skew for SPD.
    starting_charge_bps = np.where(
        np.array([q == "PX" for q in qts]), skew * 100.0, skew
    )

    # Verify: base + sign * starting_charge_bps * kappa ≈ proceeds
    recon = base_proceeds + sign * starting_charge_bps * kappa
    for i in range(N):
        err = abs(recon[i] - proceeds[i])
        if err > 500:
            logger.warning(
                "Proceeds reconstruction mismatch for %s: recon=%.0f vs data=%.0f (err=%.0f)",
                bond_ids[i], recon[i], proceeds[i], err,
            )

    # ── risk percents ─────────────────────────────────────────────────────
    risk_pct = _compute_wavg_risk_pct(t_risk, pct_bsi, traders, sides, qts, locked, cfg)

    # ── group indices ─────────────────────────────────────────────────────
    bucket_keys: set[tuple[str, str]] = set()
    trader_bucket_keys: set[tuple[str, str, str]] = set()
    bond_bucket: list[tuple[str, str]] = []
    bond_trader_bucket: list[tuple[str, str, str]] = []

    for i in range(N):
        bk = (sides[i], qts[i])
        tbk = (traders[i], sides[i], qts[i])
        bucket_keys.add(bk)
        trader_bucket_keys.add(tbk)
        bond_bucket.append(bk)
        bond_trader_bucket.append(tbk)

    bucket_keys = sorted(bucket_keys)
    trader_bucket_keys = sorted(trader_bucket_keys)

    # Maps for variable indexing
    bk_idx = {bk: j for j, bk in enumerate(bucket_keys)}
    tbk_idx = {tbk: j for j, tbk in enumerate(trader_bucket_keys)}

    n_bk = len(bucket_keys)
    n_tbk = len(trader_bucket_keys)

    # Unlocked mask
    unlocked = ~locked
    unlocked_idx = np.where(unlocked)[0]
    locked_idx = np.where(locked)[0]

    # ── starting proceeds by side (from data) ─────────────────────────────
    starting_by_side: dict[str, float] = {}
    for s_val in ("BUY", "SELL"):
        mask = np.array([sides[i] == s_val for i in range(N)])
        starting_by_side[s_val] = float(np.sum(proceeds[mask]))

    # starting proceeds by bucket  (side, qt) — total and charge-only
    starting_by_bucket: dict[tuple[str, str], float] = {}
    starting_charge_by_bucket: dict[tuple[str, str], float] = {}
    for bk in bucket_keys:
        mask = np.array([bond_bucket[i] == bk for i in range(N)])
        starting_by_bucket[bk] = float(np.sum(proceeds[mask]))
        # charge proceeds = sign * starting_charge_bps * kappa  (always positive)
        starting_charge_by_bucket[bk] = float(np.sum(
            sign[mask] * starting_charge_bps[mask] * kappa[mask]
        ))

    # ══════════════════════════════════════════════════════════════════════
    #  BUILD LP
    # ══════════════════════════════════════════════════════════════════════

    X = cp.Variable(n_bk, name="X")       # one per (side, qt)
    Y = cp.Variable(n_tbk, name="Y")      # one per (trader, side, qt)

    # For each unlocked bond i:
    #   final_charge_i = Y[tbk(i)] * blended[i] − X[bk(i)]
    #   total_proceeds_i = base[i] + sign[i] * final_charge_i * kappa[i]
    #
    # Rewrite as:
    #   total_proceeds_i = base[i] + sign[i]*kappa[i]*blended[i]*Y[tbk(i)]
    #                              − sign[i]*kappa[i]*X[bk(i)]

    # Build proceeds expression for UNLOCKED bonds (linear in X, Y)
    # We'll sum them into various groupings.

    # Coefficient arrays for unlocked bonds
    n_ul = len(unlocked_idx)
    coeff_Y_per_bond = np.zeros(n_ul)  # sign*kappa*blended → multiplied by Y[tbk]
    coeff_X_per_bond = np.zeros(n_ul)  # −sign*kappa        → multiplied by X[bk]
    const_per_bond = np.zeros(n_ul)    # base

    y_idx_per_bond = np.zeros(n_ul, dtype=int)
    x_idx_per_bond = np.zeros(n_ul, dtype=int)

    for j, i in enumerate(unlocked_idx):
        coeff_Y_per_bond[j] = sign[i] * kappa[i] * blended[i]
        coeff_X_per_bond[j] = -sign[i] * kappa[i]
        const_per_bond[j] = base_proceeds[i]
        y_idx_per_bond[j] = tbk_idx[bond_trader_bucket[i]]
        x_idx_per_bond[j] = bk_idx[bond_bucket[i]]

    # proceeds_i = const + coeff_Y * Y[y_idx] + coeff_X * X[x_idx]
    # We can express this via sparse matrix multiply:

    # Build Y coefficient matrix: (n_ul × n_tbk)
    A_Y = np.zeros((n_ul, n_tbk))
    for j in range(n_ul):
        A_Y[j, y_idx_per_bond[j]] = coeff_Y_per_bond[j]

    # Build X coefficient matrix: (n_ul × n_bk)
    A_X = np.zeros((n_ul, n_bk))
    for j in range(n_ul):
        A_X[j, x_idx_per_bond[j]] = coeff_X_per_bond[j]

    # Per-bond proceeds (CVXPY expression, length n_ul)
    ul_proceeds = const_per_bond + A_Y @ Y + A_X @ X

    # Total proceeds for all bonds (unlocked + locked)
    locked_total = float(np.sum(proceeds[locked_idx])) if len(locked_idx) > 0 else 0.0
    total_proceeds_expr = cp.sum(ul_proceeds) + locked_total

    # ── Objective: MAXIMIZE total proceeds ────────────────────────────────
    objective = cp.Maximize(total_proceeds_expr)

    constraints: list[cp.Constraint] = []

    # ── Constraint 1: Side floors ─────────────────────────────────────────
    for s_val in ("BUY", "SELL"):
        if starting_by_side.get(s_val, 0) == 0:
            continue

        # Mask of unlocked bonds on this side
        side_mask_ul = np.array([sides[unlocked_idx[j]] == s_val for j in range(n_ul)])

        # locked bonds on this side
        locked_side = float(np.sum(
            proceeds[locked_idx[np.array([sides[i] == s_val for i in locked_idx])]]
        )) if len(locked_idx) > 0 else 0.0

        # Sum of unlocked proceeds on this side
        side_ul_proceeds = (
            float(np.sum(const_per_bond[side_mask_ul]))
            + (A_Y[side_mask_ul, :].sum(axis=0) if side_mask_ul.any() else np.zeros(n_tbk)) @ Y
            + (A_X[side_mask_ul, :].sum(axis=0) if side_mask_ul.any() else np.zeros(n_bk)) @ X
        )

        side_total = side_ul_proceeds + locked_side
        floor = cfg.side_floor_pct * starting_by_side[s_val]
        constraints.append(side_total >= floor)

    # ── Constraint 2: Trader-level band (on CHARGE proceeds) ────────────
    #    The trader's share of charge-$ should match their risk share.
    #    target = risk_pct × starting_charge_proceeds_per_bucket
    for tbk in trader_bucket_keys:
        t, s, q = tbk
        bk = (s, q)

        rp = risk_pct.get(tbk, 0.0)
        target = rp * starting_charge_by_bucket.get(bk, 0.0)

        # Mask of unlocked bonds in this trader-bucket
        tbk_mask_ul = np.array([
            bond_trader_bucket[unlocked_idx[j]] == tbk for j in range(n_ul)
        ])

        # Locked bonds in this trader-bucket — their charge proceeds
        locked_tbk_ids = [i for i in locked_idx if bond_trader_bucket[i] == tbk]
        locked_tbk_charge = float(np.sum(
            sign[locked_tbk_ids] * starting_charge_bps[locked_tbk_ids] * kappa[locked_tbk_ids]
        )) if locked_tbk_ids else 0.0

        # Unlocked charge proceeds (no base — just the A_Y + A_X terms)
        tbk_ul_charge = (
            (A_Y[tbk_mask_ul, :].sum(axis=0) if tbk_mask_ul.any() else np.zeros(n_tbk)) @ Y
            + (A_X[tbk_mask_ul, :].sum(axis=0) if tbk_mask_ul.any() else np.zeros(n_bk)) @ X
        )

        tbk_charge_total = tbk_ul_charge + locked_tbk_charge

        constraints.append(tbk_charge_total <= target)
        constraints.append(tbk_charge_total >= target - cfg.buffer)

    # ── Constraint 3: Mid-line on final_charge ────────────────────────────
    # final_charge_i = Y[tbk] * blended[i] - X[bk]
    # (SELL,PX)  / (BUY,SPD):  charge ≥ 0
    # (SELL,SPD) / (BUY,PX):   charge ≤ 0
    for j, i in enumerate(unlocked_idx):
        bk = bond_bucket[i]
        tbk = bond_trader_bucket[i]
        # final_charge = Y[tbk_idx] * blended[i] - X[bk_idx]
        fc_expr = Y[tbk_idx[tbk]] * blended[i] - X[bk_idx[bk]]

        if bk in (("SELL", "PX"), ("BUY", "SPD")):
            constraints.append(fc_expr >= 0)
        else:
            constraints.append(fc_expr <= 0)

    # ── Solve ─────────────────────────────────────────────────────────────
    prob = cp.Problem(objective, constraints)
    for slv in [cfg.solver, "SCS", "ECOS", "CLARABEL"]:
        kw = _SOLVER_KWARGS.get(slv, {})
        try:
            prob.solve(solver=slv, verbose=cfg.solver_verbose, **kw)
            if prob.status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE):
                break
        except (cp.SolverError, Exception) as e:
            logger.warning("Solver %s failed: %s", slv, e)
            continue

    is_ok = prob.status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE)
    if not is_ok:
        logger.error("Optimization failed: %s", prob.status)

    X_val = X.value if X.value is not None else np.zeros(n_bk)
    Y_val = Y.value if Y.value is not None else np.zeros(n_tbk)

    # ── Extract results ───────────────────────────────────────────────────
    final_charges = np.zeros(N)
    final_proceeds_arr = proceeds.copy()  # start from actual

    for j, i in enumerate(unlocked_idx):
        tbk = bond_trader_bucket[i]
        bk = bond_bucket[i]
        fc = Y_val[tbk_idx[tbk]] * blended[i] - X_val[bk_idx[bk]]
        final_charges[i] = fc
        final_proceeds_arr[i] = base_proceeds[i] + sign[i] * fc * kappa[i]

    for i in locked_idx:
        final_charges[i] = starting_charge_bps[i]  # keep original
        final_proceeds_arr[i] = proceeds[i]

    # X and Y as dicts
    X_dict = {bk: float(X_val[bk_idx[bk]]) for bk in bucket_keys}
    Y_dict = {tbk: float(Y_val[tbk_idx[tbk]]) for tbk in trader_bucket_keys}

    # Final proceeds by side
    final_by_side: dict[str, float] = {}
    for s_val in ("BUY", "SELL"):
        mask = np.array([sides[i] == s_val for i in range(N)])
        final_by_side[s_val] = float(np.sum(final_proceeds_arr[mask]))

    result = OptimizationResult(
        status=prob.status,
        optimal=is_ok,
        objective_value=prob.value if prob.value is not None else float("nan"),
        X_values=X_dict,
        Y_values=Y_dict,
        final_charges=final_charges,
        final_proceeds=final_proceeds_arr,
        starting_proceeds_by_side=starting_by_side,
        final_proceeds_by_side=final_by_side,
        risk_pct=risk_pct,
    )

    # ── Compute implied prices ────────────────────────────────────────────
    implied_px = final_proceeds_arr / sizes * 100.0

    # For PX bonds: the final charge is in price bps → convert to price pts
    final_charge_display = np.where(
        np.array([q == "PX" for q in qts]),
        final_charges / 100.0,   # price bps → price points
        final_charges,            # spread bps — keep as-is
    )
    charge_delta = final_charge_display - skew

    df_result = df.with_columns(
        pl.Series("base_proceeds", np.round(base_proceeds, 0)),
        pl.Series("blended_charge", np.round(blended, 4)),
        pl.Series("kappa", np.round(kappa, 2)),
        pl.Series("sign", sign),
        pl.Series("trader_risk", np.round(t_risk, 2)),
        pl.Series("risk_share", [
            round(risk_pct.get((traders[i], sides[i], qts[i]), 0.0), 4) for i in range(N)
        ]),
        pl.Series("final_charge_bps", np.round(final_charges, 4)),
        pl.Series("final_charge", np.round(final_charge_display, 4)),
        pl.Series("charge_delta", np.round(charge_delta, 4)),
        pl.Series("final_proceeds", np.round(final_proceeds_arr, 0)),
        pl.Series("proceeds_delta", np.round(final_proceeds_arr - proceeds, 0)),
        pl.Series("implied_px", np.round(implied_px, 6)),
    )

    return df_result, result


# ══════════════════════════════════════════════════════════════════════════════
# Summary reporting
# ══════════════════════════════════════════════════════════════════════════════

def summary_by_trader_side_qt(df_result: pl.DataFrame) -> pl.DataFrame:
    side_col, qt_col, trader_col = _c("side"), _c("quote_type"), _c("trader")
    proceeds_col = _c("proceeds")

    grp = (
        df_result
        .group_by([trader_col, side_col, qt_col])
        .agg(
            pl.count().alias("bond_count"),
            pl.col(proceeds_col).sum().alias("starting_proceeds"),
            pl.col("final_proceeds").sum().alias("final_proceeds"),
            pl.col("proceeds_delta").sum().alias("proceeds_delta"),
            # kappa-weighted average charge
            (pl.col(_c("skew")) * pl.col("kappa").abs()).sum().alias("_ws"),
            (pl.col("final_charge") * pl.col("kappa").abs()).sum().alias("_wf"),
            pl.col("kappa").abs().sum().alias("_wk"),
            pl.col("risk_share").first().alias("risk_share"),
        )
        .with_columns(
            (pl.col("_ws") / pl.col("_wk")).alias("wavg_start_charge"),
            (pl.col("_wf") / pl.col("_wk")).alias("wavg_final_charge"),
        )
        .with_columns(
            (pl.col("wavg_final_charge") - pl.col("wavg_start_charge")).alias("wavg_charge_delta"),
        )
        .drop("_ws", "_wf", "_wk")
    )
    return grp.sort([side_col, qt_col, trader_col])


def summary_by_side_qt(df_result: pl.DataFrame) -> pl.DataFrame:
    side_col, qt_col = _c("side"), _c("quote_type")
    proceeds_col = _c("proceeds")

    grp = (
        df_result
        .group_by([side_col, qt_col])
        .agg(
            pl.count().alias("bond_count"),
            pl.col(proceeds_col).sum().alias("starting_proceeds"),
            pl.col("final_proceeds").sum().alias("final_proceeds"),
            pl.col("proceeds_delta").sum().alias("proceeds_delta"),
            (pl.col(_c("skew")) * pl.col("kappa").abs()).sum().alias("_ws"),
            (pl.col("final_charge") * pl.col("kappa").abs()).sum().alias("_wf"),
            pl.col("kappa").abs().sum().alias("_wk"),
        )
        .with_columns(
            (pl.col("_ws") / pl.col("_wk")).alias("wavg_start_charge"),
            (pl.col("_wf") / pl.col("_wk")).alias("wavg_final_charge"),
        )
        .with_columns(
            (pl.col("wavg_final_charge") - pl.col("wavg_start_charge")).alias("wavg_charge_delta"),
        )
        .drop("_ws", "_wf", "_wk")
    )
    return grp.sort([side_col, qt_col])


def print_report(df_result: pl.DataFrame, result: OptimizationResult) -> None:
    print("=" * 100)
    print("  CHARGE OPTIMIZATION REPORT (LP)")
    print(f"  status    : {result.status}")
    print(f"  objective : ${result.objective_value:,.0f}  (total proceeds)")
    print("=" * 100)

    print("\n── X values (per side × qt) ──────────────────────────────────────")
    for k, v in sorted(result.X_values.items()):
        print(f"    X[{k[0]:4s}, {k[1]:3s}] = {v:+.6f}")

    print("\n── Y values (per trader × side × qt) ─────────────────────────────")
    for k, v in sorted(result.Y_values.items()):
        print(f"    Y[{k[0]:20s}, {k[1]:4s}, {k[2]:3s}] = {v:+.6f}")

    print(f"\n── Side proceeds ─────────────────────────────────────────────────")
    for s in ("BUY", "SELL"):
        st = result.starting_proceeds_by_side.get(s, 0)
        fi = result.final_proceeds_by_side.get(s, 0)
        if st > 0:
            print(f"    {s:4s}:  start=${st:>15,.0f}   final=${fi:>15,.0f}   "
                  f"delta=${fi - st:>+12,.0f}   ({fi / st * 100:.2f}%)")

    print(f"\n── Overall by Side × QuoteType ───────────────────────────────────")
    _pp(summary_by_side_qt(df_result))

    print(f"\n── By Trader × Side × QuoteType ──────────────────────────────────")
    _pp(summary_by_trader_side_qt(df_result))

    print(f"\n── Bond-Level Detail ──────────────────────────────────────────────")
    cols = [
        _c("bond_id"), _c("trader"), _c("side"), _c("quote_type"),
        _c("skew"), "final_charge", "charge_delta",
        _c("proceeds"), "final_proceeds", "proceeds_delta",
        "implied_px", _c("quote_px"), "risk_share",
    ]
    _pp(df_result.select([c for c in cols if c in df_result.columns]))


def _pp(df: pl.DataFrame) -> None:
    with pl.Config(tbl_cols=-1, tbl_rows=50, tbl_width_chars=210, float_precision=4):
        print(df)
