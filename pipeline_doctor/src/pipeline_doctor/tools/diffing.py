"""Compares a current data_profiling profile against a stored baseline profile
and turns the deltas into drift flags, using threshold config from
pipeline_doctor.config.settings.

Pure function, independently unit-testable — no LangGraph/LLM dependency.
"""

from __future__ import annotations

from scipy import stats

from pipeline_doctor.config.settings import Settings


def _row_count_flag(baseline: dict, current: dict, settings: Settings) -> dict | None:
    baseline_n, current_n = baseline["row_count"], current["row_count"]
    if baseline_n == 0:
        return None
    pct_change = abs(current_n - baseline_n) / baseline_n
    if pct_change > settings.row_count_drift_pct:
        return {
            "kind": "row_count",
            "column": None,
            "detail": f"row count changed {pct_change:.1%} (baseline={baseline_n}, current={current_n})",
        }
    return None


def _null_rate_flags(baseline: dict, current: dict, settings: Settings) -> list[dict]:
    flags = []
    for column, current_rate in current["null_rates"].items():
        baseline_rate = baseline["null_rates"].get(column, 0.0)
        if abs(current_rate - baseline_rate) > settings.null_rate_drift_pct:
            flags.append({
                "kind": "null_rate",
                "column": column,
                "detail": f"null rate moved from {baseline_rate:.1%} to {current_rate:.1%}",
            })
    return flags


def _categorical_flags(baseline: dict, current: dict, settings: Settings) -> list[dict]:
    flags = []
    for column, current_freq in current.get("categorical", {}).items():
        baseline_freq = baseline.get("categorical", {}).get(column)
        if not baseline_freq:
            continue  # new column with no baseline to compare against yet

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
        if p_value < settings.categorical_drift_pvalue:
            flags.append({
                "kind": "categorical",
                "column": column,
                "p_value": p_value,
                "detail": f"chi-squared p={p_value:.4f} vs baseline distribution",
            })
    return flags


def _numeric_flags(baseline: dict, current: dict, settings: Settings) -> list[dict]:
    flags = []
    for column, current_sample in current.get("numeric", {}).items():
        baseline_sample = baseline.get("numeric", {}).get(column)
        if not baseline_sample or not current_sample:
            continue
        _, p_value = stats.ks_2samp(baseline_sample, current_sample)
        p_value = float(p_value)  # scipy returns numpy.float64, not msgpack-serializable by the checkpointer
        if p_value < settings.ks_pvalue_threshold:
            flags.append({
                "kind": "numeric",
                "column": column,
                "p_value": p_value,
                "detail": f"KS-test p={p_value:.4f} vs baseline distribution",
            })
    return flags


def diff_profiles(baseline: dict, current: dict, settings: Settings) -> dict:
    """Returns {"has_drift": bool, "flags": [...]} comparing `current` against
    `baseline` (both shaped like tools.data_profiling.profile_dataframe's output)."""
    flags = []
    row_count_flag = _row_count_flag(baseline, current, settings)
    if row_count_flag:
        flags.append(row_count_flag)
    flags += _null_rate_flags(baseline, current, settings)
    flags += _categorical_flags(baseline, current, settings)
    flags += _numeric_flags(baseline, current, settings)
    return {"has_drift": len(flags) > 0, "flags": flags}
