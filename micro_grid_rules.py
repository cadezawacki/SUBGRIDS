
from __future__ import annotations

import polars as pl
from app.logs.logging import log
from app.services.redux.grid_system_v4 import rule, RulesEngine, RuleContext, Priority, EmitMode


# =============================================================================
# Severity -> Color auto-mapping rule
# =============================================================================
#
# When a user edits the 'severity' column of the hot_tickers micro-grid,
# this rule automatically sets the 'color' column to the default color for
# that severity level — but only if the user did not also explicitly set
# a color in the same edit.
#

SEVERITY_COLOR_MAP = {
    "low":   "#FFFF00",   # yellow
    "med":   "#FFBF00",   # amber
    "high":  "#FF0000",   # red
    "other": "#800080",   # purple
}


@rule(
    name="micro_severity_color_default",
    room_pattern="MICRO.HOT_TICKERS",
    column_triggers_any=("severity",),
    priority=Priority.LOW,
    emit_mode=EmitMode.IMMEDIATE,
    suppress_cascade=True,
    declared_column_outputs=("color",),
)
async def micro_severity_color_default(ctx: RuleContext):
    """Auto-set color when severity changes (unless color was also explicitly set)."""
    try:
        delta = ctx.triggering_delta
        if delta is None or len(delta) == 0:
            return None

        # If the user explicitly set 'color' in this same edit, don't override
        if "color" in delta.columns:
            # Check if any color values are non-null (explicitly set)
            color_series = delta["color"]
            if color_series.null_count() < len(color_series):
                return None

        rows = []
        for row in delta.iter_rows(named=True):
            severity = row.get("severity")
            default_color = SEVERITY_COLOR_MAP.get(severity)
            if default_color:
                rows.append({
                    "id": row["id"],
                    "color": default_color,
                })

        if rows:
            return pl.DataFrame(rows)
        return None

    except Exception as e:
        await log.error(f"[MicroGrid] severity_color_default rule error: {e}")
        return None


# =============================================================================
# Registration
# =============================================================================

def register_micro_grid_rules(engine: RulesEngine):
    """Register all micro-grid rules with the rules engine."""
    engine.register(micro_severity_color_default)
