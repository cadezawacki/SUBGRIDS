"""
Amended functions for skew_optimizer.py
=======================================
Three changes to align with the Excel implementation:

1. BSR/BSI MATRIX — exponential scaling: BSR = 1.2^(10−liq), BSI = 1.25^(10−liq)
   instead of the hand-coded linear lookup.

2. CHARGE FORMULA SWAP — per-bond charge is now:
       final_charge_i = X[side,qt] × blended_i  −  Y[trader,side,qt]
   (X is the blended multiplier, Y is the flat trader offset.)
   The original had these roles reversed.

3. X[PX BUCKETS] = 0 — the bucket-level blended multiplier is pinned to zero
   for PX-quoted bonds, so every PX bond under the same trader gets an
   identical charge (= −Y[trader]).  This preserves the DV01-weighted
   average skew for PX bonds.

4. SIDE FLOOR ON CHARGE PROCEEDS — the ≥ 95 % floor is now applied to the
   charge-dollar component  (Σ sign × charge × κ) rather than to total
   proceeds  (base + charge).
"""

# ══════════════════════════════════════════════════════════════════════════════
# BSR / BSI weight matrix  (liq → (bsr_w, bsi_w))
# Exponential scaling matching Excel: BSR = 1.2^(10-liq), BSI = 1.25^(10-liq)
# ══════════════════════════════════════════════════════════════════════════════
DEFAULT_BSR_BSI_MATRIX: dict[int, tuple[float, float]] = {
    liq: (round(1.2 ** (10 - liq), 6), round(1.25 ** (10 - liq), 6))
    for liq in range(1, 11)
}


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

    # ── Starting charge proceeds (sign × charge_bps × kappa) ─────────────
    starting_charge_proceeds = sign * starting_charge_bps * kappa

    # Charge proceeds by side (for the amended side-floor constraint)
    start_chgproc_by_side: dict[str, float] = {}
    for s in ("BUY", "SELL"):
        m = np.array([sides[i] == s for i in range(N)])
        start_chgproc_by_side[s] = float(np.sum(starting_charge_proceeds[m]))

    # Total proceeds by side (kept for the result object)
    start_by_side: dict[str, float] = {}
    for s in ("BUY", "SELL"):
        m = np.array([sides[i] == s for i in range(N)])
        start_by_side[s] = float(np.sum(proceeds[m]))

    # Charge proceeds by bucket
    start_charge_by_bk: dict[tuple[str, str], float] = {}
    for bk in bucket_keys:
        m = np.array([bond_bk[i] == bk for i in range(N)])
        start_charge_by_bk[bk] = float(np.sum(starting_charge_proceeds[m]))

    # ══════════════════════════════════════════════════════════════════════
    # BUILD LP — amended formula:
    #   final_charge_i = X[s,q] × blended_i  −  Y[t,s,q]
    #   proceeds_i     = base_i + sign_i × final_charge_i × kappa_i
    # ══════════════════════════════════════════════════════════════════════
    X = cp.Variable(n_bk, name="X")
    Y = cp.Variable(n_tbk, name="Y")

    # Coefficient matrices for vectorised proceeds
    A_X = np.zeros((n_ul, n_bk))
    A_Y = np.zeros((n_ul, n_tbk))
    const = np.zeros(n_ul)

    for j, i in enumerate(ul_idx):
        A_X[j, bk_idx[bond_bk[i]]]   =  sign[i] * kappa[i] * blended[i]
        A_Y[j, tbk_idx[bond_tbk[i]]] = -sign[i] * kappa[i]
        const[j] = base_proceeds[i]

    ul_proceeds = const + A_X @ X + A_Y @ Y
    locked_total = float(np.sum(proceeds[lk_idx])) if len(lk_idx) else 0.0

    objective = cp.Maximize(cp.sum(ul_proceeds) + locked_total)
    constraints: list[cp.Constraint] = []

    # 0) Pin X = 0 for PX buckets
    for bk in bucket_keys:
        if bk[1] == "PX":
            constraints.append(X[bk_idx[bk]] == 0)

    # 1) Side floors — on charge proceeds, not total proceeds
    for s in ("BUY", "SELL"):
        if start_chgproc_by_side.get(s, 0) == 0:
            continue
        sm = np.array([sides[ul_idx[j]] == s for j in range(n_ul)])
        lk_s_chg = 0.0
        if len(lk_idx):
            lk_s_mask = np.array([sides[i] == s for i in lk_idx])
            lk_s_chg = float(np.sum(starting_charge_proceeds[lk_idx[lk_s_mask]]))
        chg_expr = (
            (A_X[sm].sum(0) if sm.any() else np.zeros(n_bk)) @ X
            + (A_Y[sm].sum(0) if sm.any() else np.zeros(n_tbk)) @ Y
            + lk_s_chg
        )
        constraints.append(chg_expr >= cfg.side_floor_pct * start_chgproc_by_side[s])

    # 2) Trader band (on charge proceeds)
    for tbk in tbk_keys:
        t, s, q = tbk; bk = (s, q)
        rp = risk_pct.get(tbk, 0.0)
        target = rp * start_charge_by_bk.get(bk, 0.0)
        tm = np.array([bond_tbk[ul_idx[j]] == tbk for j in range(n_ul)])
        lk_tbk = [i for i in lk_idx if bond_tbk[i] == tbk]
        lk_chg = float(np.sum(starting_charge_proceeds[lk_tbk])) if lk_tbk else 0.0
        tbk_expr = (
            (A_X[tm].sum(0) if tm.any() else np.zeros(n_bk)) @ X
            + (A_Y[tm].sum(0) if tm.any() else np.zeros(n_tbk)) @ Y
            + lk_chg
        )
        constraints.append(tbk_expr <= target)
        constraints.append(tbk_expr >= target - cfg.buffer)

    # 3) Mid-line  AND  4) Max charge delta — amended formula
    def _add_bond_constraints(constraints_list, max_delta):
        for j, i in enumerate(ul_idx):
            fc = X[bk_idx[bond_bk[i]]] * blended[i] - Y[tbk_idx[bond_tbk[i]]]
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

    if not is_ok and cfg.auto_relax and cfg.max_charge_delta is not None:
        for multiplier in [2.0, 4.0, None]:
            new_delta = cfg.max_charge_delta * multiplier if multiplier else None
            logger.warning(
                "Infeasible with max_charge_delta=%.2f. Relaxing to %s...",
                actual_max_delta if actual_max_delta else 0,
                f"{new_delta:.2f}" if new_delta else "uncapped",
            )
            constraints_retry: list[cp.Constraint] = []

            # Pin X = 0 for PX buckets
            for bk in bucket_keys:
                if bk[1] == "PX":
                    constraints_retry.append(X[bk_idx[bk]] == 0)

            # Side floor on charge proceeds
            for s in ("BUY", "SELL"):
                if start_chgproc_by_side.get(s, 0) == 0:
                    continue
                sm = np.array([sides[ul_idx[j]] == s for j in range(n_ul)])
                lk_s_chg = 0.0
                if len(lk_idx):
                    lk_s_mask = np.array([sides[i] == s for i in lk_idx])
                    lk_s_chg = float(np.sum(starting_charge_proceeds[lk_idx[lk_s_mask]]))
                chg_expr = (
                    (A_X[sm].sum(0) if sm.any() else np.zeros(n_bk)) @ X
                    + (A_Y[sm].sum(0) if sm.any() else np.zeros(n_tbk)) @ Y
                    + lk_s_chg
                )
                constraints_retry.append(chg_expr >= cfg.side_floor_pct * start_chgproc_by_side[s])

            # Trader band
            for tbk in tbk_keys:
                t2, s2, q2 = tbk; bk2 = (s2, q2)
                rp2 = risk_pct.get(tbk, 0.0)
                target2 = rp2 * start_charge_by_bk.get(bk2, 0.0)
                tm2 = np.array([bond_tbk[ul_idx[j]] == tbk for j in range(n_ul)])
                lk_tbk2 = [i for i in lk_idx if bond_tbk[i] == tbk]
                lk_chg2 = float(np.sum(starting_charge_proceeds[lk_tbk2])) if lk_tbk2 else 0.0
                tbk_expr2 = (
                    (A_X[tm2].sum(0) if tm2.any() else np.zeros(n_bk)) @ X
                    + (A_Y[tm2].sum(0) if tm2.any() else np.zeros(n_tbk)) @ Y
                    + lk_chg2
                )
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

    # ── Extract results ───────────────────────────────────────────────────
    if not is_ok_final:
        final_charge_bps = np.copy(starting_charge_bps)
        final_proceeds_arr = proceeds.copy()
        logger.warning("Using starting values as fallback (no solution found).")
    else:
        final_charge_bps = np.copy(starting_charge_bps)
        final_proceeds_arr = proceeds.copy()
        for j, i in enumerate(ul_idx):
            fc = X_val[bk_idx[bond_bk[i]]] * blended[i] - Y_val[tbk_idx[bond_tbk[i]]]
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
        requested_max_delta=cfg.max_charge_delta,
        actual_max_delta=actual_max_delta,
        fell_back=fell_back,
    )

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
