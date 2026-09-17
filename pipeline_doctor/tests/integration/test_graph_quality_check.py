"""Integration test: full graph through quality_check (M5). run_step actually
executes the real example_step.py subprocess; quality_check profiles its
output against an isolated tmp baseline directory.

Since M6, a flagged drift also routes through triage before finalize (see
graph.route_after_quality_check) — these tests only care about quality_check's
own output, so triage's LLM is mocked with a fixed response rather than
re-asserting on it (test_graph_triage.py owns that behavior).
"""

import os
from unittest.mock import MagicMock

import pytest
from langgraph.checkpoint.memory import MemorySaver

from pipeline_doctor.graph import build_graph
from pipeline_doctor.nodes.triage import TriageResult

EXAMPLE_STEP = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "pipelines", "example_step.py")
)


@pytest.fixture(autouse=True)
def isolated_baseline_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_BASELINE_DIR", str(tmp_path / "baselines"))


def _invoke(tmp_path, thread_id, scenario_args, output_name):
    graph = build_graph(MemorySaver())
    return graph.invoke(
        {
            "step_path": EXAMPLE_STEP,
            "output_path": str(tmp_path / output_name),
            "step_args": scenario_args,
            "status": "pending",
            "retry_count": 0,
            "max_retries": 3,
        },
        config={"configurable": {"thread_id": thread_id}},
    )


def test_first_run_bootstraps_baseline_with_no_drift(tmp_path):
    result = _invoke(tmp_path, "t1", ["--scenario", "success"], "out1.csv")

    assert result["status"] == "succeeded"
    assert result["drift_report"]["has_drift"] is False


def test_second_drifted_run_against_shared_baseline_gets_flagged(tmp_path, monkeypatch):
    stub = MagicMock()
    stub.invoke.return_value = TriageResult(severity="medium", explanation="test explanation")
    monkeypatch.setattr("pipeline_doctor.nodes.triage.get_llm", lambda schema: stub)

    _invoke(tmp_path, "t1", ["--scenario", "success"], "out1.csv")
    result = _invoke(tmp_path, "t2", ["--scenario", "drift", "--drift-kind", "all"], "out2.csv")

    assert result["status"] == "quality_flagged"
    assert result["drift_report"]["has_drift"] is True
