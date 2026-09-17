"""Unit tests for tools.data_profiling, against the fixture CSVs (generated
from pipelines/example_step.py with a fixed seed — see tests/fixtures/)."""

import os

import pandas as pd
import pytest

from pipeline_doctor.tools.data_profiling import (
    categorical_frequencies,
    null_rates,
    numeric_sample,
    profile_dataframe,
    row_count,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures")


@pytest.fixture
def baseline_df():
    return pd.read_csv(os.path.join(FIXTURES, "sample_data.csv"))


def test_row_count(baseline_df):
    assert row_count(baseline_df) == 1000


def test_null_rates(baseline_df):
    rates = null_rates(baseline_df)
    assert rates["id"] == 0.0
    assert rates["age"] == 0.0
    # example_step.py's baseline null rate for `email` is ~5% (seeded, so this
    # should land close but isn't necessarily bit-exact).
    assert 0.02 < rates["email"] < 0.09


def test_categorical_frequencies(baseline_df):
    freqs = categorical_frequencies(baseline_df, "signup_channel")
    assert set(freqs) == {"web", "mobile", "referral"}
    assert abs(sum(freqs.values()) - 1.0) < 1e-9
    # baseline weights are .50/.35/.15 - web should clearly be the largest.
    assert freqs["web"] > freqs["mobile"] > freqs["referral"]


def test_numeric_sample(baseline_df):
    sample = numeric_sample(baseline_df, "age")
    assert len(sample) == 1000
    assert all(isinstance(v, float) for v in sample)


def test_profile_dataframe_shape(baseline_df):
    profile = profile_dataframe(
        baseline_df, categorical_columns=["signup_channel"], numeric_columns=["age"]
    )
    assert profile["row_count"] == 1000
    assert "email" in profile["null_rates"]
    assert "signup_channel" in profile["categorical"]
    assert "age" in profile["numeric"]
    assert len(profile["numeric"]["age"]) == 1000
