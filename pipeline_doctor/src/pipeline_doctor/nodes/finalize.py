"""Terminal node: no-op pass-through for M1 (the node that routed here already
set `status`). Exists as a stable graph sink so later milestones (M2 abort, M7
rejected) have a single place to add summary logging without changing the graph
topology.
"""

from __future__ import annotations

from pipeline_doctor.state import PipelineDoctorState


def finalize(state: PipelineDoctorState) -> dict:
    return {}
