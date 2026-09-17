"""Assembles the StateGraph: nodes, conditional edges, and compilation with the
SqliteSaver checkpointer.

Topology:

    START --(step_path given)--> run_step
      |--(source given, no step)--> quality_check

    run_step --(failed)--> self_heal --(status=="running")--> run_step
                |                        |--(status=="aborted")-----------|
                |--(succeeded)--> quality_check --(no drift)--------------|
                                        |--(has_drift)--> triage          |
                                                              |--(severity < threshold)--|
                                                              |--(severity >= threshold)--> human_gate
                                                                                                |
                                                                                                v
                                                                                            finalize --> END

human_gate calls interrupt() and only resolves via a separate
Command(resume=...) invocation against the same thread_id (see cli.py).
"""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from pipeline_doctor.config.settings import get_settings
from pipeline_doctor.nodes.finalize import finalize
from pipeline_doctor.nodes.human_gate import human_gate
from pipeline_doctor.nodes.quality_check import quality_check
from pipeline_doctor.nodes.run_step import run_step
from pipeline_doctor.nodes.self_heal import self_heal
from pipeline_doctor.nodes.triage import triage
from pipeline_doctor.state import PipelineDoctorState
from pipeline_doctor.tools.diffing import has_structural_drift

_SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2}


def route_entry(state: PipelineDoctorState) -> str:
    """Run a step first, or go straight to checking a source.

    A run that names a `source` has nothing to execute — an orchestrator
    already produced the data and this is a check on the result. Forcing it
    through run_step would mean inventing a script whose only purpose is to
    copy the data somewhere Pipeline Doctor can read it, which is exactly the
    wrapper that tools.readers removed.
    """
    return "run_step" if state.get("step_path") else "quality_check"


def route_after_run_step(state: PipelineDoctorState) -> str:
    return "self_heal" if state["status"] == "failed" else "quality_check"


def route_after_self_heal(state: PipelineDoctorState) -> str:
    return "run_step" if state["status"] == "running" else "finalize"


def route_after_quality_check(state: PipelineDoctorState) -> str:
    return "triage" if state["drift_report"]["has_drift"] else "finalize"


def route_after_triage(state: PipelineDoctorState) -> str:
    # Structural drift (a column removed or retyped) goes to a human whatever
    # severity was assigned. Everything else in a profile describes values,
    # which can legitimately shift; a column that stopped existing cannot, and
    # whether someone is told about it shouldn't depend on a severity score.
    if has_structural_drift(state.get("drift_report") or {}):
        return "human_gate"
    severity = state["triage_result"]["severity"]
    threshold = get_settings().human_gate_min_severity
    return "human_gate" if _SEVERITY_ORDER[severity] >= _SEVERITY_ORDER[threshold] else "finalize"


def build_graph(checkpointer):
    """Compile and return the pipeline-doctor graph. Caller owns the
    checkpointer's lifecycle (see persistence.checkpointer.get_checkpointer)."""
    builder = StateGraph(PipelineDoctorState)
    builder.add_node("run_step", run_step)
    builder.add_node("self_heal", self_heal)
    builder.add_node("quality_check", quality_check)
    builder.add_node("triage", triage)
    builder.add_node("human_gate", human_gate)
    builder.add_node("finalize", finalize)

    builder.add_conditional_edges(
        START, route_entry, {"run_step": "run_step", "quality_check": "quality_check"}
    )
    builder.add_conditional_edges(
        "run_step", route_after_run_step, {"self_heal": "self_heal", "quality_check": "quality_check"}
    )
    builder.add_conditional_edges(
        "self_heal", route_after_self_heal, {"run_step": "run_step", "finalize": "finalize"}
    )
    builder.add_conditional_edges(
        "quality_check", route_after_quality_check, {"triage": "triage", "finalize": "finalize"}
    )
    builder.add_conditional_edges(
        "triage", route_after_triage, {"human_gate": "human_gate", "finalize": "finalize"}
    )
    builder.add_edge("human_gate", "finalize")
    builder.add_edge("finalize", END)
    return builder.compile(checkpointer=checkpointer)
