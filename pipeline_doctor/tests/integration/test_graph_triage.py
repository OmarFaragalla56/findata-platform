"""Integration test: full graph through triage (M6). Confirms
route_after_quality_check only sends a run to triage when drift was actually
found, and that triage's (mocked) result lands in state."""

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


def test_no_drift_skips_triage_entirely(tmp_path, monkeypatch):
    stub = MagicMock()
    monkeypatch.setattr("pipeline_doctor.nodes.triage.get_llm", lambda schema: stub)

    result = _invoke(tmp_path, "t1", ["--scenario", "success"], "out1.csv")

    assert result["status"] == "succeeded"
    assert "triage_result" not in result or result.get("triage_result") is None
    stub.invoke.assert_not_called()


def test_drift_routes_through_triage(tmp_path, monkeypatch):
    stub = MagicMock()
    stub.invoke.return_value = TriageResult(severity="medium", explanation="test explanation")
    monkeypatch.setattr("pipeline_doctor.nodes.triage.get_llm", lambda schema: stub)

    _invoke(tmp_path, "t1", ["--scenario", "success"], "out1.csv")  # bootstrap baseline
    result = _invoke(tmp_path, "t2", ["--scenario", "drift", "--drift-kind", "all"], "out2.csv")

    assert result["status"] == "quality_flagged"
    assert result["triage_result"]["severity"] == "medium"
    assert result["triage_result"]["explanation"] == "test explanation"
    stub.invoke.assert_called_once()
