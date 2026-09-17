"""Tests for the operability fixes: run reports, the step timeout, baseline
identity, and bounded numeric samples."""

import json
import os
import subprocess
import sys

import pandas as pd
import pytest

from pipeline_doctor.nodes.finalize import finalize, report_path
from pipeline_doctor.persistence.baseline_store import run_key
from pipeline_doctor.tools.data_profiling import numeric_sample, profile_dataframe
from pipeline_doctor.tools.pipeline_runner import run_step


# --- run reports (finalize used to be `return {}`) ---------------------------

def test_finalize_writes_a_run_report():
    state = {
        "step_path": "pipelines/example_step.py",
        "status": "quality_flagged",
        "drift_report": {"has_drift": True,
                         "flags": [{"kind": "numeric", "column": "age", "detail": "KS p=0.001"}]},
        "triage_result": {"severity": "medium", "explanation": "shifted", "source": "rules"},
        "data_profile": {"row_count": 200},
        "baseline_profile": {"row_count": 1000},
    }

    finalize(state)

    with open(report_path("example_step")) as f:
        report = json.loads(f.read().strip())
    assert report["status"] == "quality_flagged"
    assert report["has_drift"] is True
    assert report["severity"] == "medium"
    assert report["row_count"] == 200
    assert report["baseline_row_count"] == 1000
    assert report["flags"][0]["column"] == "age"
    assert report["finished_at"].endswith("+00:00")


def test_finalize_appends_one_line_per_run():
    state = {"step_path": "pipelines/example_step.py", "status": "succeeded"}
    finalize(state)
    finalize(state)

    with open(report_path("example_step")) as f:
        assert len([line for line in f if line.strip()]) == 2


def test_finalize_never_fails_the_run(monkeypatch):
    """The work is already done by the time finalize runs; losing the report is
    strictly better than losing the outcome."""
    monkeypatch.setenv("PIPELINE_DOCTOR_RUN_REPORT_DIR", "/proc/cannot/write/here")
    assert finalize({"step_path": "pipelines/example_step.py", "status": "succeeded"}) == {}


# --- step timeout ------------------------------------------------------------

def test_step_timeout_is_reported_as_a_failure(tmp_path):
    """A hung step used to hang the graph forever, with nothing in state to say
    why. It should look like any other failed step instead."""
    hanging = tmp_path / "hangs.py"
    hanging.write_text("import time, argparse\n"
                       "p = argparse.ArgumentParser(); p.add_argument('--output'); p.parse_args()\n"
                       "time.sleep(30)\n")

    result = run_step(str(hanging), str(tmp_path / "out.csv"), timeout_sec=2)

    assert result.success is False
    assert result.returncode == 124
    assert "timeout" in result.stderr


# --- baseline identity -------------------------------------------------------

def test_run_key_prefers_explicit_name():
    assert run_key({"name": "findata_fct", "step_path": "pipelines/example_step.py"}) == "findata_fct"


def test_run_key_from_step_path():
    assert run_key({"step_path": "pipelines/example_step.py"}) == "example_step"


def test_run_key_from_postgres_table_keeps_the_schema():
    # "." would be eaten by splitext later, so schema and table are joined with
    # an underscore rather than truncated to just the schema.
    assert run_key({"source": {"kind": "postgres", "table": "analytics_marts.fct_transactions"}}) == (
        "analytics_marts_fct_transactions"
    )


def test_run_key_from_query_is_stable_and_specific():
    a = run_key({"source": {"kind": "postgres", "query": "select 1"}})
    b = run_key({"source": {"kind": "postgres", "query": "select 1"}})
    c = run_key({"source": {"kind": "postgres", "query": "select 2"}})
    assert a == b and a != c and a.startswith("query_")


def test_run_key_without_any_identity_is_an_error():
    with pytest.raises(ValueError, match="cannot derive a baseline identity"):
        run_key({})


# --- bounded profiles --------------------------------------------------------

def test_numeric_sample_is_capped():
    df = pd.DataFrame({"a": range(10_000)})
    assert len(numeric_sample(df, "a", max_values=500)) == 500


def test_numeric_sample_is_deterministic():
    df = pd.DataFrame({"a": range(10_000)})
    assert numeric_sample(df, "a", max_values=500) == numeric_sample(df, "a", max_values=500)


def test_numeric_sample_uncapped_by_default():
    df = pd.DataFrame({"a": range(1_000)})
    assert len(numeric_sample(df, "a")) == 1_000


def test_profile_records_how_much_was_sampled():
    df = pd.DataFrame({"a": range(10_000)})
    profile = profile_dataframe(df, [], ["a"], numeric_sample_size=500)
    assert profile["numeric_meta"]["a"] == {"sampled": 500, "total": 10_000}
    assert profile["profile_version"] == 2


def test_profile_records_schema_and_its_provenance():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    inferred = profile_dataframe(df, [], [])
    assert inferred["schema_source"] == "inferred"
    assert set(inferred["schema"]) == {"a", "b"}

    declared = profile_dataframe(df, [], [], declared_schema={"a": "int4", "b": "varchar"})
    assert declared["schema"] == {"a": "int4", "b": "varchar"}
    assert declared["schema_source"] == "declared"
