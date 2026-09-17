"""Terminal node: records what the run did, then ends the graph.

This used to be `return {}` — a stable sink for the graph's topology and
nothing else. That left Pipeline Doctor with no memory of its own work outside
LangGraph's checkpoint database, which stores its state in an internal
serialization format and can't answer "what drifted last week?".

Each run appends one JSON object to data/runs/<run_key>.jsonl. JSONL because
runs are append-only and a partially written file still parses line by line,
and one file per step so a single step's history is readable on its own.
Report writing must never fail a run: the work is already done by this point,
and losing the report is strictly better than losing the outcome.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pipeline_doctor.config.settings import get_settings
from pipeline_doctor.persistence.baseline_store import run_key
from pipeline_doctor.state import PipelineDoctorState


def report_path(key: str) -> str:
    return os.path.join(get_settings().run_report_dir, f"{key}.jsonl")


def build_report(state: PipelineDoctorState) -> dict:
    drift_report = state.get("drift_report") or {}
    triage_result = state.get("triage_result") or {}
    data_profile = state.get("data_profile") or {}
    baseline_profile = state.get("baseline_profile") or {}
    return {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": state.get("status"),
        "step_path": state.get("step_path"),
        "source": state.get("source"),
        "retry_count": state.get("retry_count", 0),
        "error": state.get("error"),
        "row_count": data_profile.get("row_count"),
        "baseline_row_count": baseline_profile.get("row_count"),
        "has_drift": drift_report.get("has_drift", False),
        "flags": [
            {"kind": f.get("kind"), "column": f.get("column"), "detail": f.get("detail")}
            for f in drift_report.get("flags", [])
        ],
        "severity": triage_result.get("severity"),
        "triage_source": triage_result.get("source"),
        "explanation": triage_result.get("explanation"),
        "human_decision": state.get("human_decision"),
    }


def finalize(state: PipelineDoctorState) -> dict:
    try:
        path = report_path(run_key(state))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(build_report(state)) + "\n")
    except Exception as exc:  # never fail a completed run over its own bookkeeping
        print(f"[pipeline-doctor] could not write run report: {exc!r}")
    return {}
