"""Unit tests for nodes.triage's M6 LLM-driven classification. The LLM client
is monkeypatched out entirely — no network access, no API cost."""

from unittest.mock import MagicMock

import pytest

from pipeline_doctor.nodes.triage import TriageResult, triage


@pytest.fixture
def mock_llm(monkeypatch):
    stub = MagicMock()
    monkeypatch.setattr("pipeline_doctor.nodes.triage.get_llm", lambda schema: stub)
    return stub


def test_triage_returns_structured_result(mock_llm):
    mock_llm.invoke.return_value = TriageResult(severity="high", explanation="looks like a schema break")

    result = triage({
        "step_path": "pipelines/example_step.py",
        "drift_report": {
            "has_drift": True,
            "flags": [{"kind": "row_count", "column": None, "detail": "row count changed 80%"}],
        },
    })

    assert result["triage_result"]["severity"] == "high"
    assert result["triage_result"]["explanation"] == "looks like a schema break"
    assert result["triage_result"]["source"] == "llm"
    mock_llm.invoke.assert_called_once()
