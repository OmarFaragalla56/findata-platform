"""M7: the human-in-the-loop gate. Reached only when triage's severity meets
or exceeds settings.human_gate_min_severity (see graph.route_after_triage).

Calls interrupt() with the drift report + triage result as payload, pausing
the graph until the CLI resumes it with Command(resume="approve"|"reject")
against the *same* thread_id (see persistence.checkpointer's thread-id note
and cli.py's `resume` subcommand) — LangGraph re-runs this node function from
the top on resume, at which point interrupt() returns the resumed value
instead of pausing again.

- approve: the drift becomes the new normal — overwrite the baseline with the
  run's observed profile so the same drift doesn't re-trigger this gate again.
- reject: leave the baseline untouched.
"""

from __future__ import annotations

from langgraph.types import interrupt

from pipeline_doctor.persistence.baseline_store import save_baseline
from pipeline_doctor.state import PipelineDoctorState


def human_gate(state: PipelineDoctorState) -> dict:
    decision = interrupt({
        "step_path": state["step_path"],
        "drift_report": state["drift_report"],
        "triage_result": state["triage_result"],
    })

    if decision == "approve":
        save_baseline(state["step_path"], state["data_profile"])
        return {"human_decision": "approve", "status": "approved"}

    return {"human_decision": "reject", "status": "rejected"}
