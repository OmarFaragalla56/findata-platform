"""Integration test: the full compiled graph with self_heal's LLM mocked,
confirming M3's wiring (still run_step <-> self_heal <-> finalize, unchanged
from M2 — see graph.py's routing functions) behaves correctly end-to-end when
the mocked LLM says "retry". Uses an in-memory checkpointer, not the real
sqlite file, to keep tests isolated from dev/manual-run state.
"""

import os
from unittest.mock import MagicMock

from langgraph.checkpoint.memory import MemorySaver

from pipeline_doctor.graph import build_graph
from pipeline_doctor.nodes.self_heal import SelfHealDecision

EXAMPLE_STEP = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "pipelines", "example_step.py")
)


def _invoke(tmp_path, monkeypatch, fail_times: str, llm_action: str):
    stub = MagicMock()
    stub.invoke.return_value = SelfHealDecision(action=llm_action, reasoning="test")
    monkeypatch.setattr("pipeline_doctor.nodes.self_heal.get_llm", lambda schema: stub)

    output = str(tmp_path / "out.csv")
    graph = build_graph(MemorySaver())
    result = graph.invoke(
        {
            "step_path": EXAMPLE_STEP,
            "output_path": output,
            "step_args": ["--scenario", "crash", "--fail-times", fail_times],
            "status": "pending",
            "retry_count": 0,
            "max_retries": 3,
        },
        config={"configurable": {"thread_id": "test-thread"}},
    )
    return result, stub


def test_llm_retry_recovers_flaky_step(tmp_path, monkeypatch):
    result, stub = _invoke(tmp_path, monkeypatch, fail_times="2", llm_action="retry")

    assert result["status"] == "succeeded"
    assert result["retry_count"] == 2
    assert stub.invoke.call_count == 2


def test_llm_abort_stops_before_exhausting_retries(tmp_path, monkeypatch):
    # fail_times="-1" means the step always fails; with the LLM immediately
    # saying "abort", the graph should stop after just one self_heal call
    # rather than spending all 3 max_retries attempts.
    result, stub = _invoke(tmp_path, monkeypatch, fail_times="-1", llm_action="abort")

    assert result["status"] == "aborted"
    assert result["retry_count"] == 1
    assert stub.invoke.call_count == 1
