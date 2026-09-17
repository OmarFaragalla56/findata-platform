"""Unit tests for persistence.baseline_store, isolated to a tmp directory per
test via the PIPELINE_DOCTOR_BASELINE_DIR env var (get_settings() re-reads the
environment fresh each call — see config/settings.py)."""

import pytest

from pipeline_doctor.persistence.baseline_store import (
    baseline_path,
    load_baseline,
    save_baseline,
    step_name,
)


@pytest.fixture(autouse=True)
def isolated_baseline_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_BASELINE_DIR", str(tmp_path))


def test_step_name_from_path():
    assert step_name("pipelines/example_step.py") == "example_step"
    assert step_name("/abs/path/to/other_step.py") == "other_step"


def test_load_returns_none_when_missing():
    assert load_baseline("pipelines/example_step.py") is None


def test_save_then_load_roundtrip():
    profile = {"row_count": 100, "null_rates": {"a": 0.1}}
    save_baseline("pipelines/example_step.py", profile)
    assert load_baseline("pipelines/example_step.py") == profile


def test_save_overwrites_existing():
    save_baseline("pipelines/example_step.py", {"row_count": 100, "null_rates": {}})
    save_baseline("pipelines/example_step.py", {"row_count": 200, "null_rates": {}})
    assert load_baseline("pipelines/example_step.py")["row_count"] == 200


def test_baseline_path_uses_configured_dir(tmp_path):
    path = baseline_path("pipelines/example_step.py")
    assert path.startswith(str(tmp_path))
    assert path.endswith("example_step.json")
