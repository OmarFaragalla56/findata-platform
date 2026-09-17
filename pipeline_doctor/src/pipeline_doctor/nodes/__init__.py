"""Graph nodes: run_step, self_heal, quality_check, triage, human_gate, finalize.

Each module exposes a single node function with the signature
`(state: PipelineDoctorState) -> dict` (a partial state update), matching what
`graph.py` wires into the compiled StateGraph.
"""
