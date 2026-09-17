"""Executes the target pipeline step via tools.pipeline_runner, capturing success
or the failure's error/traceback into state.

M1 only wires the happy path into the graph (run_step -> finalize); the failure
fields are already captured here so M2 can add the conditional edge to self_heal
without touching this node again.
"""

from __future__ import annotations

from pipeline_doctor.state import PipelineDoctorState
from pipeline_doctor.tools.pipeline_runner import run_step as _run_step


def run_step(state: PipelineDoctorState) -> dict:
    result = _run_step(
        step_path=state["step_path"],
        output_path=state["output_path"],
        extra_args=state.get("step_args"),
    )
    if result.success:
        return {"status": "succeeded", "error": None, "traceback": None}
    return {
        "status": "failed",
        "error": result.error,
        "traceback": result.traceback,
    }
