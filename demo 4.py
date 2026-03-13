"""
Demo with real data — LP charge optimization with max_charge_delta.
"""
import numpy as np
import polars as pl
from skew_optimizer import OptimizerConfig, print_report, solve

# ── Real data with DV01 derived from spread/price relationship ────────────────
# DV01 = |ΔPx / ΔSpd| × size / 100
raw = [
    {'id': 'US29250NBT19', 'description': 'ENBCN 8 1/2 01/15/2084', 'refMidSpd': 184.2068, 'refMidPx': 114.424062, 'macpLiqScore': 7, 'desigName': 'David Yoder', 'side': 'SELL', 'size': 10500000, 'quoteType': 'PX', 'quotePx': 114.74, 'quoteSpd': 179.341893, 'skew': 0.32, 'proceeds': 12047700},
    {'id': 'US002824BN93', 'description': 'ABT 4 3/4 04/15/43', 'refMidSpd': 60.8554, 'refMidPx': 92.044039, 'macpLiqScore': 4, 'desigName': 'Samuel Selarnick', 'side': 'SELL', 'size': 16300000, 'quoteType': 'SPD', 'quotePx': 92.286369, 'quoteSpd': 58.0554, 'skew': -2.8, 'proceeds': 15042678},
    {'id': 'US191216CW80', 'description': 'KO 2 1/2 06/01/40', 'refMidSpd': 90.2773, 'refMidPx': 73.478326, 'macpLiqScore': 7, 'desigName': 'Charles Li', 'side': 'SELL', 'size': 19000000, 'quoteType': 'SPD', 'quotePx': 73.711287, 'quoteSpd': 87.4421, 'skew': -2.84, 'proceeds': 14005145},
    {'id': 'US25746UDV89', 'description': 'D 6 5/8 05/15/55', 'refMidSpd': 201.45, 'refMidPx': 102.410707, 'macpLiqScore': 7, 'desigName': 'David Yoder', 'side': 'SELL', 'size': 11700000, 'quoteType': 'PX', 'quotePx': 102.47, 'quoteSpd': 200.552934, 'skew': 0.06, 'proceeds': 11988990},
    {'id': 'US744320BL59', 'description': 'PRU 6 3/4 03/01/53', 'refMidSpd': 201.4961, 'refMidPx': 104.89093, 'macpLiqScore': 4, 'desigName': 'David Yoder', 'side': 'SELL', 'size': 11500000, 'quoteType': 'PX', 'quotePx': 105.19, 'quoteSpd': 196.201317, 'skew': 0.3, 'proceeds': 12096850},
    {'id': 'US26441CCG87', 'description': 'DUK 6.45 09/01/54', 'refMidSpd': 152.3856, 'refMidPx': 104.355993, 'macpLiqScore': 8, 'desigName': 'David Yoder', 'side': 'SELL', 'size': 11500000, 'quoteType': 'PX', 'quotePx': 104.62, 'quoteSpd': 148.38041, 'skew': 0.26, 'proceeds': 12031300},
    {'id': 'US38145GAS93', 'description': 'GS 5.065 01/21/37', 'refMidSpd': 107.4052, 'refMidPx': 98.006365, 'macpLiqScore': 10, 'desigName': 'Devan Cross', 'side': 'SELL', 'size': 15300000, 'quoteType': 'SPD', 'quotePx': 98.20692, 'quoteSpd': 104.7, 'skew': -2.71, 'proceeds': 15025659},
    {'id': 'US06051GJE08', 'description': 'BAC 2.676 06/19/41', 'refMidSpd': 134.8495, 'refMidPx': 71.52751, 'macpLiqScore': 8, 'desigName': 'Devan Cross', 'side': 'SELL', 'size': 20900000, 'quoteType': 'SPD', 'quotePx': 71.695741, 'quoteSpd': 132.7, 'skew': -2.15, 'proceeds': 14984410},
    {'id': 'US46647PBV76', 'description': 'JPM 2.525 11/19/41', 'refMidSpd': 129.0736, 'refMidPx': 69.95155, 'macpLiqScore': 6, 'desigName': 'Devan Cross', 'side': 'SELL', 'size': 21400000, 'quoteType': 'SPD', 'quotePx': 70.179612, 'quoteSpd': 126.2, 'skew': -2.87, 'proceeds': 15018437},
    {'id': 'US30231GBF81', 'description': 'XOM 4.227 03/19/40', 'refMidSpd': 95.9801, 'refMidPx': 90.287222, 'macpLiqScore': 9, 'desigName': 'Dan Krasner', 'side': 'SELL', 'size': 13300000, 'quoteType': 'SPD', 'quotePx': 90.58981, 'quoteSpd': 92.6801, 'skew': -3.3, 'proceeds': 12048445},
    {'id': 'US907818FD57', 'description': 'UNP 3.55 08/15/39', 'refMidSpd': 98.9033, 'refMidPx': 83.843799, 'macpLiqScore': 5, 'desigName': 'Charles Li', 'side': 'SELL', 'size': 11900000, 'quoteType': 'SPD', 'quotePx': 84.029828, 'quoteSpd': 96.7033, 'skew': -2.2, 'proceeds': 9999550},
    {'id': 'US126408GU17', 'description': 'CSX 5 1/2 04/15/41', 'refMidSpd': 119.2537, 'refMidPx': 100.540025, 'macpLiqScore': 5, 'desigName': 'Charles Li', 'side': 'SELL', 'size': 9900000, 'quoteType': 'SPD', 'quotePx': 100.757246, 'quoteSpd': 117.0537, 'skew': -2.2, 'proceeds': 9974967},
    {'id': 'US037833EE62', 'description': 'AAPL 2 3/8 02/08/41', 'refMidSpd': 100.9217, 'refMidPx': 70.443806, 'macpLiqScore': 7, 'desigName': 'David Holliday', 'side': 'SELL', 'size': 17000000, 'quoteType': 'SPD', 'quotePx': 70.608652, 'quoteSpd': 98.9028, 'skew': -2.02, 'proceeds': 12003471},
    {'id': 'US437076AV48', 'description': 'HD 5.95 04/01/41', 'refMidSpd': 113.9315, 'refMidPx': 105.570541, 'macpLiqScore': 6, 'desigName': 'Charles Li', 'side': 'SELL', 'size': 11400000, 'quoteType': 'SPD', 'quotePx': 105.846875, 'quoteSpd': 111.2315, 'skew': -2.7, 'proceeds': 12066544},
    {'id': 'US717081EZ22', 'description': 'PFE 2.55 05/28/40', 'refMidSpd': 110.3175, 'refMidPx': 72.339989, 'macpLiqScore': 6, 'desigName': 'Samuel Selarnick', 'side': 'SELL', 'size': 13800000, 'quoteType': 'SPD', 'quotePx': 72.5078, 'quoteSpd': 108.2175, 'skew': -2.1, 'proceeds': 10006076},
    {'id': 'US91324PDY51', 'description': 'UNH 2 3/4 05/15/40', 'refMidSpd': 130.4332, 'refMidPx': 72.721992, 'macpLiqScore': 6, 'desigName': 'Samuel Selarnick', 'side': 'SELL', 'size': 13700000, 'quoteType': 'SPD', 'quotePx': 72.903712, 'quoteSpd': 128.1332, 'skew': -2.3, 'proceeds': 9987809},
    {'id': 'US254687FY73', 'description': 'DIS 3 1/2 05/13/40', 'refMidSpd': 109.0371, 'refMidPx': 81.849863, 'macpLiqScore': 7, 'desigName': 'David Holliday', 'side': 'SELL', 'size': 14600000, 'quoteType': 'SPD', 'quotePx': 82.090875, 'quoteSpd': 106.2371, 'skew': -2.8, 'proceeds': 11985268},
    {'id': 'US58933YBA29', 'description': 'MRK 2.35 06/24/40', 'refMidSpd': 101.9553, 'refMidPx': 70.941726, 'macpLiqScore': 4, 'desigName': 'Samuel Selarnick', 'side': 'SELL', 'size': 14100000, 'quoteType': 'SPD', 'quotePx': 71.165916, 'quoteSpd': 99.1553, 'skew': -2.8, 'proceeds': 10034394},
    {'id': 'US375558BS17', 'description': 'GILD 2.6 10/01/40', 'refMidSpd': 109.088, 'refMidPx': 72.501982, 'macpLiqScore': 6, 'desigName': 'Samuel Selarnick', 'side': 'SELL', 'size': 13800000, 'quoteType': 'SPD', 'quotePx': 72.813748, 'quoteSpd': 105.288, 'skew': -3.8, 'proceeds': 10048297},
    {'id': 'US23291KAJ43', 'description': 'DHR 3 1/4 11/15/39', 'refMidSpd': 106.2054, 'refMidPx': 80.117221, 'macpLiqScore': 5, 'desigName': 'Samuel Selarnick', 'side': 'SELL', 'size': 12500000, 'quoteType': 'SPD', 'quotePx': 80.308007, 'quoteSpd': 103.9054, 'skew': -2.3, 'proceeds': 10038501},
    {'id': 'US110122DR63', 'description': 'BMY 2.35 11/13/40', 'refMidSpd': 109.3021, 'refMidPx': 69.817674, 'macpLiqScore': 5, 'desigName': 'Samuel Selarnick', 'side': 'SELL', 'size': 14300000, 'quoteType': 'SPD', 'quotePx': 70.124266, 'quoteSpd': 105.5021, 'skew': -3.8, 'proceeds': 10027770},
    {'id': 'US001192AH64', 'description': 'SO 5 7/8 03/15/41', 'refMidSpd': 142.2262, 'refMidPx': 101.957473, 'macpLiqScore': 5, 'desigName': 'Dan Krasner', 'side': 'SELL', 'size': 9800000, 'quoteType': 'SPD', 'quotePx': 102.033499, 'quoteSpd': 141.4262, 'skew': -0.8, 'proceeds': 9999283},
    {'id': 'US341081FD42', 'description': 'NEE 5 1/4 02/01/41', 'refMidSpd': 113.4422, 'refMidPx': 98.601415, 'macpLiqScore': 4, 'desigName': 'Dan Krasner', 'side': 'SELL', 'size': 10100000, 'quoteType': 'SPD', 'quotePx': 98.930456, 'quoteSpd': 110.1422, 'skew': -3.3, 'proceeds': 9991976},
]

# Derive DV01 from spread/price data (same approach as before, but adding it as a column)
for r in raw:
    d_spd = r['quoteSpd'] - r['refMidSpd']
    d_px = r['quotePx'] - r['refMidPx']
    if abs(d_spd) < 1e-8:
        r['dv01'] = round(0.08 * r['size'] / 100.0, 2)
    else:
        r['dv01'] = round(abs(d_px / d_spd) * r['size'] / 100.0, 2)

df = pl.DataFrame(raw)

# ══════════════════════════════════════════════════════════════════════════════
# RUN 1 — max_charge_delta = 1.5, auto_relax=True (will relax if infeasible)
# ══════════════════════════════════════════════════════════════════════════════
print("▓" * 110)
print("  RUN 1 — max_charge_delta=1.5, auto_relax=True")
print("▓" * 110)

cfg1 = OptimizerConfig(side_floor_pct=0.95, buffer=200.0, max_charge_delta=1.5, auto_relax=True)
df1, res1 = solve(df, cfg1)
print_report(df1, res1, cfg1)

# ══════════════════════════════════════════════════════════════════════════════
# RUN 2 — max_charge_delta = 0.75, auto_relax=FALSE (hard cap — will fail)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "▓" * 110)
print("  RUN 2 — max_charge_delta=0.75, auto_relax=FALSE (hard cap)")
print("▓" * 110)

cfg2 = OptimizerConfig(side_floor_pct=0.95, buffer=500.0, max_charge_delta=0.75, auto_relax=False)
df2, res2 = solve(df, cfg2)
print_report(df2, res2, cfg2)

# ══════════════════════════════════════════════════════════════════════════════
# RUN 3 — uncapped (baseline)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "▓" * 110)
print("  RUN 3 — max_charge_delta=None (uncapped)")
print("▓" * 110)

cfg3 = OptimizerConfig(side_floor_pct=0.95, buffer=200.0, max_charge_delta=None)
df3, res3 = solve(df, cfg3)
print_report(df3, res3, cfg3)

# ══════════════════════════════════════════════════════════════════════════════
# Comparison
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 110)
print("  COMPARISON")
print("=" * 110)
for label, dfr, res in [
    ("Run1 (Δ=1.5, relax)", df1, res1),
    ("Run2 (Δ=0.75, hard)", df2, res2),
    ("Run3 (uncapped)",      df3, res3),
]:
    spd_d = dfr.filter(pl.col("quoteType") == "SPD")["spd_delta"].to_numpy()
    skw_d = dfr["skew_delta"].to_numpy()
    actual = res.actual_max_delta
    status = "OPTIMAL" if res.optimal else "INFEASIBLE"
    print(f"  {label:25s}  status={status:10s}  actual_Δ_cap={str(actual):>6s}  "
          f"max|spd_Δ|={np.max(np.abs(spd_d)):.4f}bp  "
          f"max|skew_Δ|={np.max(np.abs(skw_d)):.4f}  "
          f"proceeds=${dfr['final_proceeds'].sum():,.0f}")
