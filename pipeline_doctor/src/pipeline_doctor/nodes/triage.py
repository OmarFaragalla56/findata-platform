"""Classifies quality_check's drift report into a severity + explanation. Only
reached when quality_check actually found drift — see graph's
route_after_quality_check.

The severity produced here is what the human_gate conditional edge reads to
decide whether the drift needs a human (with one exception: structural drift
goes to a human regardless — see graph.route_after_triage).

Two implementations:

- **With an API key**, the LLM classifies, as before. Weighing whether an 80%
  row drop is a breakage or a known seasonal dip is judgement, which is the
  thing a model is actually for.
- **Without one**, deterministic rules assign the severity instead. They are
  cruder — a fixed severity per flag kind, with a large row-count change
  treated as high — but they let an unattended run reach a sensible decision
  instead of failing outright.

Structural drift does not depend on either: a removed or retyped column goes to
a human whatever severity is assigned here (see graph.route_after_triage), so
this node is never the only thing standing between a dropped column and
someone noticing.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from pipeline_doctor.llm.client import get_llm, llm_available
from pipeline_doctor.llm.prompts import build_triage_prompt
from pipeline_doctor.persistence.baseline_store import run_key
from pipeline_doctor.state import PipelineDoctorState

_SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2}

# Row-count change above this is treated as high on its own: losing or gaining
# half the data is not a judgement call.
_ROW_COUNT_HIGH_PCT = 0.50

_FLAG_SEVERITY = {
    "schema_column_removed": "high",
    "schema_dtype_changed": "high",
    "schema_column_added": "low",
    "null_rate": "medium",
    "categorical": "medium",
    "numeric": "medium",
    "row_count": "medium",   # raised to high by _row_count_severity
}


class TriageResult(BaseModel):
    severity: Literal["low", "medium", "high"] = Field(
        description=(
            "'low' if the drift is minor/likely benign (e.g. a small seasonal "
            "shift); 'medium' if it's notable and worth a human's attention "
            "but not obviously broken; 'high' if it looks like a real data "
            "quality break (e.g. a pipeline bug, an upstream schema change, a "
            "large chunk of missing data)."
        )
    )
    explanation: str = Field(description="A few sentences on the likely cause and why this severity.")


def _row_count_severity(state: PipelineDoctorState) -> str:
    baseline = (state.get("baseline_profile") or {}).get("row_count")
    current = (state.get("data_profile") or {}).get("row_count")
    if not baseline:
        return "medium"
    pct = abs((current or 0) - baseline) / baseline
    return "high" if pct >= _ROW_COUNT_HIGH_PCT else "medium"


def _flag_severity(flag: dict, state: PipelineDoctorState) -> str:
    kind = flag.get("kind")
    if kind == "row_count":
        return _row_count_severity(state)
    if kind == "schema_dtype_changed" and flag.get("inferred_types"):
        # The source didn't declare its types, so pandas guessed them; an
        # all-null column alone can produce this. Worth reporting, not worth
        # calling a break.
        return "medium"
    return _FLAG_SEVERITY.get(kind, "medium")


def rule_based_triage(state: PipelineDoctorState) -> TriageResult:
    """Severity from the flags alone, no model involved."""
    flags = (state.get("drift_report") or {}).get("flags", [])
    if not flags:
        return TriageResult(severity="low", explanation="No drift flags were raised.")

    severities = [_flag_severity(flag, state) for flag in flags]
    worst = max(severities, key=lambda s: _SEVERITY_ORDER[s])
    summary = "; ".join(f"{f.get('kind')}({f.get('column')}): {f.get('detail')}" for f in flags)
    return TriageResult(
        severity=worst,
        explanation=(
            f"{len(flags)} drift flag(s) raised, worst severity {worst} by rule. {summary}"
        ),
    )


def triage(state: PipelineDoctorState) -> dict:
    if not llm_available():
        result = rule_based_triage(state).model_dump()
        result["explanation"] += " (rule-based: no ANTHROPIC_API_KEY configured)"
        result["source"] = "rules"
        return {"triage_result": result}

    llm = get_llm(TriageResult)
    try:
        llm_result: TriageResult = llm.invoke(
            build_triage_prompt(step_path=run_key(state), drift_report=state["drift_report"])
        )
    except Exception as exc:
        # Having a key is not the same as the call working: a provider outage,
        # a 503 "high demand", a rate limit or an expired key all surface here.
        # Found live - a Gemini 503 crashed the whole run with exit 1, which is
        # exactly the unattended failure the rules fallback exists to prevent.
        # The drift itself is still real and still needs routing, so degrade
        # to the rules rather than lose the finding.
        result = rule_based_triage(state).model_dump()
        result["explanation"] += f" (rule-based: LLM call failed with {type(exc).__name__})"
        result["source"] = "rules_after_llm_error"
        result["llm_error"] = str(exc)[:500]
        return {"triage_result": result}
    return {"triage_result": {**llm_result.model_dump(), "source": "llm"}}
