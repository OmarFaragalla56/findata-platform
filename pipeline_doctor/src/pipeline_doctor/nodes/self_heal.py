"""M3: LLM-driven retry/abort decision, replacing M2's fixed retry-N policy.

The target pipeline step is an opaque subprocess (see tools.pipeline_runner) —
pipeline_doctor has no generic way to change its parameters, so the LLM's only
real levers are "retry" (it judges the traceback looks transient) or "abort"
(it judges the error is deterministic/config-related and retrying is
pointless). This still beats M2's fixed policy: it can abort early on an
obviously-unrecoverable error instead of burning every max_retries attempt,
and its `reasoning` is preserved in state for later inspection/logging.

max_retries is still a hard ceiling regardless of what the LLM decides, and is
checked *before* calling the LLM so an already-exhausted run doesn't spend a
token on a decision that's moot.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from pipeline_doctor.llm.client import get_llm
from pipeline_doctor.llm.prompts import build_self_heal_prompt
from pipeline_doctor.state import PipelineDoctorState


class SelfHealDecision(BaseModel):
    action: Literal["retry", "abort"] = Field(
        description=(
            "'retry' if the traceback looks like a transient/recoverable "
            "failure worth attempting again; 'abort' if it looks "
            "deterministic (e.g. bad config, missing file) and retrying "
            "would just fail the same way."
        )
    )
    reasoning: str = Field(description="One or two sentences explaining the decision.")


def self_heal(state: PipelineDoctorState) -> dict:
    retry_count = state.get("retry_count", 0) + 1
    max_retries = state.get("max_retries", 3)

    if retry_count > max_retries:
        return {"retry_count": retry_count, "status": "aborted"}

    llm = get_llm(SelfHealDecision)
    decision: SelfHealDecision = llm.invoke(
        build_self_heal_prompt(
            step_path=state["step_path"],
            error=state.get("error"),
            traceback=state.get("traceback"),
            retry_count=retry_count,
            max_retries=max_retries,
        )
    )

    status = "running" if decision.action == "retry" else "aborted"
    return {
        "retry_count": retry_count,
        "status": status,
        "self_heal_decision": decision.model_dump(),
    }
