"""Unit tests for tools.diffing: fixture-based end-to-end drift detection plus
small synthetic-dict cases pinning down threshold boundary behavior."""

import os

import pandas as pd
import pytest

from pipeline_doctor.config.settings import Settings
from pipeline_doctor.tools.data_profiling import profile_dataframe
from pipeline_doctor.tools.diffing import diff_profiles

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures")
CATEGORICAL_COLUMNS = ["signup_channel"]
NUMERIC_COLUMNS = ["age"]


def _profile(filename):
    df = pd.read_csv(os.path.join(FIXTURES, filename))
    return profile_dataframe(df, categorical_columns=CATEGORICAL_COLUMNS, numeric_columns=NUMERIC_COLUMNS)


@pytest.fixture
def settings():
    return Settings()


def test_no_drift_against_self(settings):
    profile = _profile("sample_data.csv")
    report = diff_profiles(profile, profile, settings)
    assert report == {"has_drift": False, "flags": []}


def test_drift_detected_across_all_dimensions(settings):
    baseline = _profile("sample_data.csv")
    current = _profile("sample_data_drifted.csv")  # drift-kind=all
    report = diff_profiles(baseline, current, settings)

    assert report["has_drift"] is True
    kinds = {flag["kind"] for flag in report["flags"]}
    assert kinds == {"row_count", "null_rate", "categorical", "numeric"}


def test_row_count_flag_respects_threshold(settings):
    baseline = {"row_count": 1000, "null_rates": {}}
    within_threshold = {"row_count": 1100, "null_rates": {}}  # 10% change; threshold is 20%
    over_threshold = {"row_count": 1300, "null_rates": {}}    # 30% change

    assert diff_profiles(baseline, within_threshold, settings)["has_drift"] is False
    report = diff_profiles(baseline, over_threshold, settings)
    assert report["has_drift"] is True
    assert report["flags"][0]["kind"] == "row_count"


def test_null_rate_flag_respects_threshold(settings):
    baseline = {"row_count": 1000, "null_rates": {"email": 0.05}}
    within_threshold = {"row_count": 1000, "null_rates": {"email": 0.10}}  # +5pp; threshold is 10pp
    over_threshold = {"row_count": 1000, "null_rates": {"email": 0.20}}    # +15pp

    assert diff_profiles(baseline, within_threshold, settings)["has_drift"] is False
    report = diff_profiles(baseline, over_threshold, settings)
    assert report["has_drift"] is True
    assert report["flags"][0]["kind"] == "null_rate"


def test_categorical_flag_ignores_column_with_no_baseline(settings):
    baseline = {"row_count": 100, "null_rates": {}, "categorical": {}}
    current = {"row_count": 100, "null_rates": {}, "categorical": {"new_col": {"a": 1.0}}}
    report = diff_profiles(baseline, current, settings)
    assert report == {"has_drift": False, "flags": []}
