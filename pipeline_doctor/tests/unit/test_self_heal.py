"""Unit tests for self_heal's M3 LLM-driven decision. The LLM client is
monkeypatched out entirely, so these run with no network access and no API
cost — see the build plan's note on checking before spending paid tokens.
"""

from unittest.mock import MagicMock

import pytest

from pipeline_doctor.nodes.self_heal import SelfHealDecision, self_heal


@pytest.fixture
def mock_llm(monkeypatch):
    """Patches get_llm (as imported into nodes.self_heal) to return a stub
    whose .invoke() return value each test sets directly."""
    stub = MagicMock()
    monkeypatch.setattr("pipeline_doctor.nodes.self_heal.get_llm", lambda schema: stub)
    return stub


def _base_state(**overrides):
    state = {
        "step_path": "pipelines/example_step.py",
        "status": "failed",
        "error": "RuntimeError: boom",
        "traceback": "Traceback...\nRuntimeError: boom",
        "retry_count": 0,
        "max_retries": 3,
    }
    state.update(overrides)
    return state


def test_retries_when_llm_says_retry(mock_llm):
    mock_llm.invoke.return_value = SelfHealDecision(action="retry", reasoning="looks transient")

    result = self_heal(_base_state())

    assert result["status"] == "running"
    assert result["retry_count"] == 1
    assert result["self_heal_decision"]["action"] == "retry"


def test_aborts_when_llm_says_abort(mock_llm):
    mock_llm.invoke.return_value = SelfHealDecision(action="abort", reasoning="config error, not transient")

    result = self_heal(_base_state())

    assert result["status"] == "aborted"
    assert result["self_heal_decision"]["action"] == "abort"


def test_aborts_when_retries_exhausted_without_calling_llm(mock_llm):
    result = self_heal(_base_state(retry_count=3, max_retries=3))

    assert result["status"] == "aborted"
    assert result["retry_count"] == 4
    mock_llm.invoke.assert_not_called()
