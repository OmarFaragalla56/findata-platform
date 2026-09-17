"""Shared fixtures for the whole test suite.

Autouse + session-independent: every test gets its own throwaway baseline
directory and checkpoint db path, so nothing in tests/ can ever read or write
the real project-level data/baselines/ or checkpoints/ — even a test that
doesn't itself expect to touch quality_check (e.g. a self_heal test whose
"success" path now also flows through quality_check/triage since the graph is
unified) is protected by default, rather than relying on every test file to
remember to isolate these paths itself.

A test that wants its own more specific isolation (e.g. sharing one baseline
across two calls within itself) can still override the env var again.
"""

import pytest


@pytest.fixture(autouse=True)
def isolated_data_dirs(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_BASELINE_DIR", str(tmp_path / "baselines"))
    monkeypatch.setenv("PIPELINE_DOCTOR_CHECKPOINT_DB", str(tmp_path / "checkpoints" / "test.sqlite"))
