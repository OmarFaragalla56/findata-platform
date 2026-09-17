"""Integration test: the real interrupt() / Command(resume=...) cycle (M7),
end to end through the compiled graph — the highest-risk path in the system,
so this drives it programmatically for both approve and reject rather than
relying on a manual CLI walkthrough alone.
"""

import os
from unittest.mock import MagicMock

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from pipeline_doctor.graph import build_graph
from pipeline_doctor.nodes.triage import TriageResult
from pipeline_doctor.persistence.baseline_store import load_baseline

EXAMPLE_STEP = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "pipelines", "example_step.py")
)


@pytest.fixture(autouse=True)
def isolated_baseline_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_BASELINE_DIR", str(tmp_path / "baselines"))


@pytest.fixture
def mock_triage_llm(monkeypatch):
    stub = MagicMock()
    stub.invoke.return_value = TriageResult(severity="high", explanation="test: severe drift")
    monkeypatch.setattr("pipeline_doctor.nodes.triage.get_llm", lambda schema: stub)
    return stub


def _run(graph, thread_id, tmp_path, scenario_args, output_name):
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


def test_high_severity_drift_pauses_at_human_gate(tmp_path, mock_triage_llm):
    graph = build_graph(MemorySaver())
    _run(graph, "bootstrap", tmp_path, ["--scenario", "success"], "out1.csv")

    result = _run(graph, "drifted-run", tmp_path, ["--scenario", "drift", "--drift-kind", "all"], "out2.csv")

    assert "__interrupt__" in result
    payload = result["__interrupt__"][0].value
    assert payload["triage_result"]["severity"] == "high"
    assert payload["drift_report"]["has_drift"] is True


def test_approve_resumes_and_updates_baseline(tmp_path, mock_triage_llm):
    graph = build_graph(MemorySaver())
    _run(graph, "bootstrap", tmp_path, ["--scenario", "success"], "out1.csv")
    baseline_before = load_baseline(EXAMPLE_STEP)

    config = {"configurable": {"thread_id": "drifted-run"}}
    interrupted = _run(graph, "drifted-run", tmp_path, ["--scenario", "drift", "--drift-kind", "all"], "out2.csv")
    assert "__interrupt__" in interrupted

    resumed = graph.invoke(Command(resume="approve"), config=config)

    assert resumed["status"] == "approved"
    baseline_after = load_baseline(EXAMPLE_STEP)
    assert baseline_after != baseline_before
    assert baseline_after["row_count"] == 200  # the drifted run's row count is now the baseline


def test_reject_resumes_and_leaves_baseline_untouched(tmp_path, mock_triage_llm):
    graph = build_graph(MemorySaver())
    _run(graph, "bootstrap", tmp_path, ["--scenario", "success"], "out1.csv")
    baseline_before = load_baseline(EXAMPLE_STEP)

    config = {"configurable": {"thread_id": "drifted-run"}}
    interrupted = _run(graph, "drifted-run", tmp_path, ["--scenario", "drift", "--drift-kind", "all"], "out2.csv")
    assert "__interrupt__" in interrupted

    resumed = graph.invoke(Command(resume="reject"), config=config)

    assert resumed["status"] == "rejected"
    assert load_baseline(EXAMPLE_STEP) == baseline_before


def test_low_severity_drift_skips_human_gate(tmp_path, monkeypatch):
    stub = MagicMock()
    stub.invoke.return_value = TriageResult(severity="low", explanation="minor, probably fine")
    monkeypatch.setattr("pipeline_doctor.nodes.triage.get_llm", lambda schema: stub)

    graph = build_graph(MemorySaver())
    _run(graph, "bootstrap", tmp_path, ["--scenario", "success"], "out1.csv")
    result = _run(graph, "drifted-run", tmp_path, ["--scenario", "drift", "--drift-kind", "all"], "out2.csv")

    assert "__interrupt__" not in result
    assert result["status"] == "quality_flagged"
