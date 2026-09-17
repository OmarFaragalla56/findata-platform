"""Unit tests for persistence.column_config and the per-column thresholds it
feeds into diffing.

The case that motivated this: infer_column_roles uses a hardcoded "<= 20
distinct values" cut for categoricals, which excluded account_id (25 accounts,
a string) from drift testing entirely — the one dimension the findata pipeline
is actually about."""

import pandas as pd

from pipeline_doctor.config.settings import Settings
from pipeline_doctor.nodes.quality_check import infer_column_roles
from pipeline_doctor.persistence.column_config import resolve_column_roles
from pipeline_doctor.tools.diffing import diff_profiles

SETTINGS = Settings()


def _df_with_25_accounts():
    return pd.DataFrame({
        "account_id": [f"acct_{i % 25:04d}" for i in range(200)],
        "amount": [float(i % 97) for i in range(200)],
        "merchant": [f"merchant_{i}" for i in range(200)],
    })


def test_account_id_is_invisible_to_the_heuristic():
    categorical, numeric = infer_column_roles(_df_with_25_accounts())
    assert "account_id" not in categorical   # 25 distinct > the cut of 20
    assert "account_id" not in numeric       # and it's a string
    assert "amount" in numeric


def test_config_can_force_a_column_to_categorical():
    df = _df_with_25_accounts()
    config = {"categorical": ["account_id"]}

    categorical, numeric = resolve_column_roles(df, config, infer_column_roles(df))

    assert categorical == ["account_id"]
    assert "amount" in numeric  # untouched roles keep their inferred value


def test_ignore_removes_a_column_from_both_roles():
    df = _df_with_25_accounts()
    config = {"categorical": ["account_id"], "numeric": ["amount"], "ignore": ["amount"]}

    categorical, numeric = resolve_column_roles(df, config, infer_column_roles(df))

    assert categorical == ["account_id"]
    assert numeric == []


def test_columns_named_in_config_but_absent_are_dropped():
    df = _df_with_25_accounts()
    config = {"categorical": ["account_id", "gone"]}

    categorical, _ = resolve_column_roles(df, config, infer_column_roles(df))

    assert categorical == ["account_id"]


def _profiles(baseline_rows, current_rows):
    def p(n):
        return {"row_count": n, "schema": {"a": "int4"}, "schema_source": "declared",
                "null_rates": {"a": 0.0}, "categorical": {}, "numeric": {}}
    return p(baseline_rows), p(current_rows)


def test_per_step_threshold_override_beats_the_global_setting():
    baseline, current = _profiles(100, 130)   # 30% change, global threshold is 20%

    assert diff_profiles(baseline, current, SETTINGS)["has_drift"] is True
    assert diff_profiles(baseline, current, SETTINGS, {"row_count_drift_pct": 0.50})["has_drift"] is False


def test_per_column_threshold_override():
    baseline = {"row_count": 100, "schema": {"a": "int4"}, "schema_source": "declared",
                "null_rates": {"a": 0.0}, "categorical": {}, "numeric": {}}
    current = {"row_count": 100, "schema": {"a": "int4"}, "schema_source": "declared",
               "null_rates": {"a": 0.15}, "categorical": {}, "numeric": {}}

    # 15% null-rate move: over the global 10% threshold, under a per-column 20%.
    assert diff_profiles(baseline, current, SETTINGS)["has_drift"] is True
    config = {"columns": {"a": {"null_rate_drift_pct": 0.20}}}
    assert diff_profiles(baseline, current, SETTINGS, config)["has_drift"] is False
