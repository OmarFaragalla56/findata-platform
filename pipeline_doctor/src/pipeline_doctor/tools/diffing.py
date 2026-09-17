"""Compares a current data_profiling profile against a stored baseline profile
and turns the deltas into drift flags, using threshold config from
pipeline_doctor.config.settings, optionally overridden per column.

Pure function, independently unit-testable — no LangGraph/LLM dependency.

Flag kinds, in the order they are checked:

    schema_column_removed   a baseline column is gone
    schema_column_added     a column appeared
    schema_dtype_changed    same name, different type
    row_count               total rows moved more than the threshold
    null_rate               a column's null rate moved
    categorical             a category distribution shifted (chi-square)
    numeric                 a value distribution shifted (two-sample KS)

Schema is checked first because it explains the rest: if a column is gone,
every value-level comparison for it is meaningless, and a dtype change can move
a distribution without any real-world change at all.
"""

from __future__ import annotations

from scipy import stats

from pipeline_doctor.config.settings import Settings

# Flags that describe the table's *shape* rather than its values. A human
# almost always wants to see these, so graph.route_after_triage sends them to
# human_gate whatever severity triage assigned.
STRUCTURAL_FLAG_KINDS = ("schema_column_removed", "schema_dtype_changed")


def _is_ignored(column_config: dict | None, column: str | None) -> bool:
    """Columns the step's config says not to test. Applies to every check,
    schema included: a column deliberately excluded from monitoring shouldn't
    raise a structural alarm when it changes either."""
    if not column_config or column is None:
        return False
    if column in (column_config.get("ignore") or []):
        return True
    return bool((column_config.get("columns") or {}).get(column, {}).get("ignore"))


def _threshold(column_config: dict | None, column: str | None, key: str, default):
    """Per-column override > per-step override > global setting.

    A single global threshold can't fit every column: a near-unique free-text
    column and a 10-value category column drift in completely different ways.
    """
    if not column_config:
        return default
    if column:
        per_column = (column_config.get("columns") or {}).get(column) or {}
        if key in per_column:
            return per_column[key]
    if key in column_config:
        return column_config[key]
    return default


def _schema_flags(baseline: dict, current: dict, column_config: dict | None = None) -> list[dict]:
    baseline_schema = baseline.get("schema")
    current_schema = current.get("schema")
    if not baseline_schema or not current_schema:
        # A baseline written before schema was profiled has nothing to compare
        # against. Reporting every column as "added" on the first run after an
        # upgrade would be noise, so treat it as not-yet-baselined instead.
        return []

    inferred = "inferred" in (baseline.get("schema_source"), current.get("schema_source"))
    flags = []

    for column, baseline_type in baseline_schema.items():
        if _is_ignored(column_config, column):
            continue
        if column not in current_schema:
            flags.append({
                "kind": "schema_column_removed",
                "column": column,
                "detail": f"column '{column}' ({baseline_type}) is missing from this run",
            })
            continue
        current_type = current_schema[column]
        if current_type != baseline_type:
            flag = {
                "kind": "schema_dtype_changed",
                "column": column,
                "detail": f"type changed from {baseline_type} to {current_type}",
            }
            if inferred:
                # Not suppressed, but labelled: over CSV the type is a guess,
                # so this may say more about this run's values than the column.
                flag["inferred_types"] = True
                flag["detail"] += " (types inferred, not declared by the source)"
            flags.append(flag)

    for column, current_type in current_schema.items():
        if column not in baseline_schema and not _is_ignored(column_config, column):
            flags.append({
                "kind": "schema_column_added",
                "column": column,
                "detail": f"new column '{column}' ({current_type}) not present in the baseline",
            })

    return flags


def _row_count_flag(baseline: dict, current: dict, settings: Settings,
                    column_config: dict | None = None) -> dict | None:
    baseline_n, current_n = baseline["row_count"], current["row_count"]
    if baseline_n == 0:
        return None
    limit = _threshold(column_config, None, "row_count_drift_pct", settings.row_count_drift_pct)
    pct_change = abs(current_n - baseline_n) / baseline_n
    if pct_change > limit:
        return {
            "kind": "row_count",
            "column": None,
            "detail": f"row count changed {pct_change:.1%} (baseline={baseline_n}, current={current_n})",
        }
    return None


def _null_rate_flags(baseline: dict, current: dict, settings: Settings,
                     column_config: dict | None = None) -> list[dict]:
    flags = []
    for column, current_rate in current["null_rates"].items():
        if _is_ignored(column_config, column):
            continue
        baseline_rate = baseline["null_rates"].get(column, 0.0)
        limit = _threshold(column_config, column, "null_rate_drift_pct", settings.null_rate_drift_pct)
        if abs(current_rate - baseline_rate) > limit:
            flags.append({
                "kind": "null_rate",
                "column": column,
                "detail": f"null rate moved from {baseline_rate:.1%} to {current_rate:.1%}",
            })
    return flags


def _categorical_flags(baseline: dict, current: dict, settings: Settings,
                       column_config: dict | None = None) -> list[dict]:
    flags = []
    for column, current_freq in current.get("categorical", {}).items():
        if _is_ignored(column_config, column):
            continue
        baseline_freq = baseline.get("categorical", {}).get(column)
        if not baseline_freq:
            continue  # new column with no baseline to compare against yet
                      # (its arrival is reported by _schema_flags instead)

        categories = sorted(set(baseline_freq) | set(current_freq))
        row_n = current["row_count"]
        # Additive smoothing (floor of 0.5 expected count) avoids a category
        # unseen in the baseline producing a zero expected count, which would
        # blow up the chi-square statistic to infinity.
        epsilon = 0.5
        f_obs = [current_freq.get(cat, 0.0) * row_n for cat in categories]
        f_exp = [max(baseline_freq.get(cat, 0.0) * row_n, epsilon) for cat in categories]
        # chisquare requires both arrays to sum to the same total; rescale
        # f_exp (already perturbed by the smoothing above) to match f_obs's sum.
        f_obs_total, f_exp_total = sum(f_obs), sum(f_exp)
        f_exp = [v * (f_obs_total / f_exp_total) for v in f_exp]

        _, p_value = stats.chisquare(f_obs=f_obs, f_exp=f_exp)
        p_value = float(p_value)  # scipy returns numpy.float64, not msgpack-serializable by the checkpointer
        limit = _threshold(column_config, column, "categorical_drift_pvalue",
                           settings.categorical_drift_pvalue)
        if p_value < limit:
            flags.append({
                "kind": "categorical",
                "column": column,
                "p_value": p_value,
                "detail": f"chi-squared p={p_value:.4f} vs baseline distribution",
            })
    return flags


def _numeric_flags(baseline: dict, current: dict, settings: Settings,
                   column_config: dict | None = None) -> list[dict]:
    flags = []
    for column, current_sample in current.get("numeric", {}).items():
        if _is_ignored(column_config, column):
            continue
        baseline_sample = baseline.get("numeric", {}).get(column)
        if not baseline_sample or not current_sample:
            continue
        _, p_value = stats.ks_2samp(baseline_sample, current_sample)
        p_value = float(p_value)  # scipy returns numpy.float64, not msgpack-serializable by the checkpointer
        limit = _threshold(column_config, column, "ks_pvalue_threshold", settings.ks_pvalue_threshold)
        if p_value < limit:
            flags.append({
                "kind": "numeric",
                "column": column,
                "p_value": p_value,
                "detail": f"KS-test p={p_value:.4f} vs baseline distribution",
            })
    return flags


def diff_profiles(baseline: dict, current: dict, settings: Settings,
                  column_config: dict | None = None) -> dict:
    """Returns {"has_drift": bool, "flags": [...]} comparing `current` against
    `baseline` (both shaped like tools.data_profiling.profile_dataframe's output)."""
    flags = []
    flags += _schema_flags(baseline, current, column_config)
    row_count_flag = _row_count_flag(baseline, current, settings, column_config)
    if row_count_flag:
        flags.append(row_count_flag)
    flags += _null_rate_flags(baseline, current, settings, column_config)
    flags += _categorical_flags(baseline, current, settings, column_config)
    flags += _numeric_flags(baseline, current, settings, column_config)
    return {"has_drift": len(flags) > 0, "flags": flags}


def has_structural_drift(drift_report: dict) -> bool:
    """True when the run found a shape change (column removed or retyped) — the
    kind of drift that should reach a human regardless of severity scoring."""
    return any(f.get("kind") in STRUCTURAL_FLAG_KINDS for f in drift_report.get("flags", []))
