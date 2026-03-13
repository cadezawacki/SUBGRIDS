"""
Demo / smoke test for skew_optimizer
=====================================
Generates a synthetic portfolio, runs the optimizer in both HARD and SOFT
modes, demonstrates auto-fallback, and prints reports.
"""

import numpy as np
import polars as pl

from skew_optimizer import (
    COLUMN_MAP,
    OptimizerConfig,
    TraderConstraintMode,
    print_report,
    solve,
    summary_by_side_qt,
    summary_by_trader_side_qt,
)

np.random.seed(42)

# ── synthetic portfolio ───────────────────────────────────────────────────────
N = 40
traders = np.random.choice(["TraderA", "TraderB", "TraderC"], size=N)
sides   = np.random.choice(["BUY", "SELL"], size=N)
qts     = np.random.choice(["PX", "SPD"], size=N)

sizes = np.random.uniform(5_000_000, 50_000_000, size=N).round(-3)
dv01s = np.random.uniform(200, 3000, size=N).round(2)
liqs  = np.random.uniform(1, 10, size=N).round(2)

# Skews well away from zero to give solver room to manoeuvre
skews = np.zeros(N)
for i in range(N):
    mag = np.random.uniform(1.0, 5.0)
    if (sides[i], qts[i]) in (("BUY", "PX"), ("SELL", "SPD")):
        skews[i] = -mag
    else:
        skews[i] = mag

proceeds = sizes * np.random.uniform(0.98, 1.02, size=N)

df = pl.DataFrame({
    "bond_id":         [f"BOND_{i:03d}" for i in range(N)],
    "trader":          traders.tolist(),
    "side":            sides.tolist(),
    "quote_type":      qts.tolist(),
    "proceeds":        proceeds,
    "skew":            skews.round(4),
    "size":            sizes,
    "dv01":            dv01s,
    "liquidity_score": liqs,
})

print("Input portfolio shape:", df.shape)
print(df.head(10))

# ══════════════════════════════════════════════════════════════════════════════
# RUN 1 — HARD trader constraints (with auto-fallback enabled)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "▓" * 80)
print("  RUN 1 — HARD trader constraints (auto_fallback=True)")
print("▓" * 80)

cfg_hard = OptimizerConfig(
    lambda_alignment=1.0,
    lambda_stability=0.05,
    trader_constraint=TraderConstraintMode.HARD,
    auto_fallback=True,
    max_skew_delta=10.0,
)

df_hard, res_hard = solve(df, cfg_hard)
print_report(df_hard, res_hard)

# ══════════════════════════════════════════════════════════════════════════════
# RUN 2 — SOFT trader constraints (explicit)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "▓" * 80)
print("  RUN 2 — SOFT trader constraints (λ_trader=10)")
print("▓" * 80)

cfg_soft = OptimizerConfig(
    lambda_alignment=1.0,
    lambda_stability=0.05,
    lambda_trader_soft=10.0,
    trader_constraint=TraderConstraintMode.SOFT,
    max_skew_delta=10.0,
)

df_soft, res_soft = solve(df, cfg_soft)
print_report(df_soft, res_soft)

# ══════════════════════════════════════════════════════════════════════════════
# Side-by-side comparison
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  COMPARISON: RUN 1 vs RUN 2 — Overall by Side × QuoteType")
print("=" * 80)

r1 = summary_by_side_qt(df_hard).select([
    "side", "quote_type",
    pl.col("wavg_starting_skew").alias("start_skew"),
    pl.col("wavg_final_skew").alias("r1_final_skew"),
    pl.col("wavg_skew_delta").alias("r1_delta"),
    pl.col("starting_proceeds").alias("start_proceeds"),
    pl.col("final_proceeds").alias("r1_final_proceeds"),
])
r2 = summary_by_side_qt(df_soft).select([
    "side", "quote_type",
    pl.col("wavg_final_skew").alias("r2_final_skew"),
    pl.col("wavg_skew_delta").alias("r2_delta"),
    pl.col("final_proceeds").alias("r2_final_proceeds"),
])

comp = r1.join(r2, on=["side", "quote_type"])
with pl.Config(tbl_cols=-1, tbl_width_chars=160, float_precision=6):
    print(comp)

# ══════════════════════════════════════════════════════════════════════════════
# Trader-level comparison
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  COMPARISON: RUN 1 vs RUN 2 — By Trader × Side × QuoteType")
print("=" * 80)

t1 = summary_by_trader_side_qt(df_hard).select([
    "trader", "side", "quote_type",
    pl.col("wavg_starting_skew").alias("start_skew"),
    pl.col("wavg_final_skew").alias("r1_final_skew"),
    pl.col("wavg_skew_delta").alias("r1_skew_delta"),
    pl.col("starting_proceeds").alias("start_proceeds"),
    pl.col("proceeds_delta").alias("r1_proceeds_delta"),
    "risk_share",
])
t2 = summary_by_trader_side_qt(df_soft).select([
    "trader", "side", "quote_type",
    pl.col("wavg_final_skew").alias("r2_final_skew"),
    pl.col("wavg_skew_delta").alias("r2_skew_delta"),
    pl.col("proceeds_delta").alias("r2_proceeds_delta"),
])

tcomp = t1.join(t2, on=["trader", "side", "quote_type"])
with pl.Config(tbl_cols=-1, tbl_rows=30, tbl_width_chars=180, float_precision=4):
    print(tcomp)
