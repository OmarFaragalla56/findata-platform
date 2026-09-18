"""triage and self_heal must degrade, not crash, without ANTHROPIC_API_KEY.

Before this, both nodes constructed a ChatAnthropic client unconditionally, so
a run with no key died at the first sign of drift — which is exactly when it
had something useful to say. That's survivable for a developer at a terminal
and not survivable for a scheduled task."""

from unittest.mock import MagicMock

import pytest

from pipeline_doctor.nodes.self_heal import self_heal
from pipeline_doctor.nodes.triage import rule_based_triage, triage


@pytest.fixture
def no_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


def _drift_state(flags, baseline_rows=1000, current_rows=1000):
    return {
        "step_path": "pipelines/example_step.py",
        "drift_report": {"has_drift": True, "flags": flags},
        "baseline_profile": {"row_count": baseline_rows},
        "data_profile": {"row_count": current_rows},
    }


def test_triage_without_key_uses_rules(no_api_key):
    state = _drift_state([{"kind": "numeric", "column": "age", "detail": "KS p=0.001"}])

    result = triage(state)["triage_result"]

    assert result["source"] == "rules"
    assert result["severity"] == "medium"
    assert "no ANTHROPIC_API_KEY" in result["explanation"]


def test_removed_column_is_high_severity_by_rule(no_api_key):
    state = _drift_state([{"kind": "schema_column_removed", "column": "b", "detail": "gone"}])
    assert triage(state)["triage_result"]["severity"] == "high"


def test_large_row_count_drop_is_high_severity_by_rule(no_api_key):
    state = _drift_state(
        [{"kind": "row_count", "column": None, "detail": "80%"}],
        baseline_rows=1000, current_rows=200,
    )
    assert triage(state)["triage_result"]["severity"] == "high"


def test_small_row_count_move_is_medium_by_rule(no_api_key):
    state = _drift_state(
        [{"kind": "row_count", "column": None, "detail": "25%"}],
        baseline_rows=1000, current_rows=750,
    )
    assert triage(state)["triage_result"]["severity"] == "medium"


def test_inferred_dtype_change_is_not_treated_as_a_break(no_api_key):
    state = _drift_state([
        {"kind": "schema_dtype_changed", "column": "a", "detail": "object -> float64",
         "inferred_types": True}
    ])
    assert triage(state)["triage_result"]["severity"] == "medium"


def test_rule_based_triage_with_no_flags_is_low():
    assert rule_based_triage({"drift_report": {"flags": []}}).severity == "low"


def test_self_heal_without_key_falls_back_to_fixed_retries(no_api_key):
    state = {"step_path": "pipelines/example_step.py", "retry_count": 0, "max_retries": 3,
             "error": "boom", "traceback": "Traceback..."}

    result = self_heal(state)

    assert result["status"] == "running"
    assert result["self_heal_decision"]["source"] == "rules"
    assert result["retry_count"] == 1


def test_self_heal_without_key_still_respects_the_ceiling(no_api_key):
    state = {"step_path": "pipelines/example_step.py", "retry_count": 3, "max_retries": 3}
    assert self_heal(state)["status"] == "aborted"


# --- a key is present but the call fails --------------------------------------
# Found live: Gemini returned 503 "model experiencing high demand" and the whole
# run crashed with exit 1. Having a key doesn't mean the call works.

def test_triage_falls_back_to_rules_when_the_llm_call_fails(monkeypatch):
    failing = MagicMock()
    failing.invoke.side_effect = RuntimeError("503 UNAVAILABLE: high demand")
    monkeypatch.setattr("pipeline_doctor.nodes.triage.get_llm", lambda schema: failing)
    state = _drift_state([{"kind": "schema_column_removed", "column": "b", "detail": "gone"}])

    result = triage(state)["triage_result"]

    assert result["source"] == "rules_after_llm_error"
    assert result["severity"] == "high"          # the finding survives the outage
    assert "RuntimeError" in result["explanation"]
    assert "503" in result["llm_error"]


def test_self_heal_falls_back_when_the_llm_call_fails(monkeypatch):
    failing = MagicMock()
    failing.invoke.side_effect = RuntimeError("429 rate limited")
    monkeypatch.setattr("pipeline_doctor.nodes.self_heal.get_llm", lambda schema: failing)
    state = {"step_path": "pipelines/example_step.py", "retry_count": 0, "max_retries": 3,
             "error": "boom", "traceback": "Traceback..."}

    result = self_heal(state)

    assert result["status"] == "running"
    assert result["self_heal_decision"]["source"] == "rules_after_llm_error"
