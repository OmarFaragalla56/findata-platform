"""Unit tests for human_gate's branch logic (approve saves the baseline,
reject doesn't), with langgraph's interrupt() itself monkeypatched to return a
fixed value instead of actually pausing — that's not something a plain
function call can exercise outside a running graph. The real interrupt() /
Command(resume=...) mechanism is covered separately by
tests/integration/test_graph_human_gate.py.
"""

import pytest

from pipeline_doctor.nodes.human_gate import human_gate
from pipeline_doctor.persistence.baseline_store import load_baseline

STEP_PATH = "pipelines/example_step.py"


@pytest.fixture(autouse=True)
def isolated_baseline_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_BASELINE_DIR", str(tmp_path))


def _state(**overrides):
    state = {
        "step_path": STEP_PATH,
        "data_profile": {"row_count": 200, "null_rates": {}},
        "drift_report": {"has_drift": True, "flags": []},
        "triage_result": {"severity": "high", "explanation": "test"},
    }
    state.update(overrides)
    return state


def test_approve_saves_new_baseline_and_sets_status(monkeypatch):
    monkeypatch.setattr("pipeline_doctor.nodes.human_gate.interrupt", lambda payload: "approve")

    result = human_gate(_state())

    assert result == {"human_decision": "approve", "status": "approved"}
    assert load_baseline(STEP_PATH) == {"row_count": 200, "null_rates": {}}


def test_reject_leaves_baseline_untouched(monkeypatch):
    monkeypatch.setattr("pipeline_doctor.nodes.human_gate.interrupt", lambda payload: "reject")

    result = human_gate(_state())

    assert result == {"human_decision": "reject", "status": "rejected"}
    assert load_baseline(STEP_PATH) is None
