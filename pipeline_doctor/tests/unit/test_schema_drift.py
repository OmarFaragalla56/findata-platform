"""Unit tests for schema-drift detection in tools.diffing.

This is the gap these tests exist for: before the schema block, every check in
diffing iterated over the *current* profile, so a column present in the
baseline and missing now was never looked at, and dropping a column produced
zero flags."""

import pytest

from pipeline_doctor.config.settings import Settings
from pipeline_doctor.tools.diffing import diff_profiles, has_structural_drift

SETTINGS = Settings()


def _profile(schema, source="declared", row_count=100):
    return {
        "profile_version": 2,
        "row_count": row_count,
        "schema": schema,
        "schema_source": source,
        "null_rates": {c: 0.0 for c in schema},
        "categorical": {},
        "numeric": {},
        "numeric_meta": {},
    }


def test_removed_column_is_flagged():
    baseline = _profile({"a": "int4", "b": "varchar"})
    current = _profile({"a": "int4"})

    flags = diff_profiles(baseline, current, SETTINGS)["flags"]

    kinds = [(f["kind"], f["column"]) for f in flags]
    assert ("schema_column_removed", "b") in kinds


def test_added_column_is_flagged():
    flags = diff_profiles(_profile({"a": "int4"}), _profile({"a": "int4", "c": "bool"}), SETTINGS)["flags"]
    assert ("schema_column_added", "c") in [(f["kind"], f["column"]) for f in flags]


def test_dtype_change_is_flagged():
    flags = diff_profiles(_profile({"a": "int4"}), _profile({"a": "varchar"}), SETTINGS)["flags"]
    flag = next(f for f in flags if f["kind"] == "schema_dtype_changed")
    assert flag["column"] == "a"
    assert "int4" in flag["detail"] and "varchar" in flag["detail"]
    assert "inferred_types" not in flag


def test_dtype_change_over_inferred_types_is_marked():
    # Over CSV pandas guesses the type, so an all-null column reads as float64
    # and looks like a type change. Still reported, but labelled, so triage and
    # a human can weigh it differently.
    flags = diff_profiles(
        _profile({"a": "object"}, source="inferred"),
        _profile({"a": "float64"}, source="inferred"),
        SETTINGS,
    )["flags"]
    flag = next(f for f in flags if f["kind"] == "schema_dtype_changed")
    assert flag["inferred_types"] is True
    assert "inferred" in flag["detail"]


def test_identical_schema_produces_no_schema_flags():
    flags = diff_profiles(_profile({"a": "int4"}), _profile({"a": "int4"}), SETTINGS)["flags"]
    assert [f for f in flags if f["kind"].startswith("schema_")] == []


def test_baseline_without_schema_block_is_not_all_added():
    """Baselines written before schema profiling have no "schema" key. Treating
    that as "every column is new" would flag every column once, on everyone's
    first run after upgrading."""
    old_baseline = {"row_count": 100, "null_rates": {"a": 0.0}, "categorical": {}, "numeric": {}}
    current = _profile({"a": "int4", "b": "varchar"})

    flags = diff_profiles(old_baseline, current, SETTINGS)["flags"]

    assert [f for f in flags if f["kind"].startswith("schema_")] == []


def test_ignored_columns_raise_no_schema_flags():
    config = {"ignore": ["b"]}
    flags = diff_profiles(_profile({"a": "int4", "b": "varchar"}), _profile({"a": "int4"}),
                          SETTINGS, config)["flags"]
    assert [f for f in flags if f["kind"].startswith("schema_")] == []


@pytest.mark.parametrize("kind,expected", [
    ("schema_column_removed", True),
    ("schema_dtype_changed", True),
    ("schema_column_added", False),
    ("numeric", False),
])
def test_has_structural_drift(kind, expected):
    assert has_structural_drift({"flags": [{"kind": kind}]}) is expected
