"""Unit tests for nodes.quality_check, using the fixture CSVs as stand-ins for
a run_step output and an isolated tmp baseline directory per test."""

import os

import pandas as pd
import pytest

from pipeline_doctor.nodes.quality_check import infer_column_roles, quality_check
from pipeline_doctor.persistence.baseline_store import load_baseline

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures")
STEP_PATH = "pipelines/example_step.py"


@pytest.fixture(autouse=True)
def isolated_baseline_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_BASELINE_DIR", str(tmp_path))


def _state(output_filename):
    return {"step_path": STEP_PATH, "output_path": os.path.join(FIXTURES, output_filename)}


def test_bootstraps_baseline_on_first_run():
    assert load_baseline(STEP_PATH) is None

    result = quality_check(_state("sample_data.csv"))

    assert result["status"] == "succeeded"
    assert result["drift_report"] == {"has_drift": False, "flags": []}
    assert load_baseline(STEP_PATH) is not None


def test_no_drift_on_repeat_of_same_data():
    quality_check(_state("sample_data.csv"))  # bootstrap
    result = quality_check(_state("sample_data.csv"))  # same data again

    assert result["status"] == "succeeded"
    assert result["drift_report"]["has_drift"] is False


def test_flags_drift_against_baseline():
    quality_check(_state("sample_data.csv"))  # bootstrap from the clean baseline
    result = quality_check(_state("sample_data_drifted.csv"))

    assert result["status"] == "quality_flagged"
    assert result["drift_report"]["has_drift"] is True
    kinds = {flag["kind"] for flag in result["drift_report"]["flags"]}
    assert "row_count" in kinds


def test_infer_column_roles_excludes_identifier_and_high_cardinality():
    df = pd.read_csv(os.path.join(FIXTURES, "sample_data.csv"))
    categorical, numeric = infer_column_roles(df)

    assert categorical == ["signup_channel"]
    assert numeric == ["age"]  # `id` excluded (all-unique); `email` isn't numeric dtype
