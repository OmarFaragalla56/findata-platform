"""M6: classifies quality_check's drift report via an LLM call, returning a
structured severity + explanation. Only reached when quality_check actually
found drift — see graph.py's route_after_quality_check.

The severity this produces is what M7's human_gate conditional edge reads to
decide whether the drift needs a human, or can pass through on its own.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from pipeline_doctor.llm.client import get_llm
from pipeline_doctor.llm.prompts import build_triage_prompt
from pipeline_doctor.state import PipelineDoctorState


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


def triage(state: PipelineDoctorState) -> dict:
    llm = get_llm(TriageResult)
    result: TriageResult = llm.invoke(
        build_triage_prompt(step_path=state["step_path"], drift_report=state["drift_report"])
    )
    return {"triage_result": result.model_dump()}
