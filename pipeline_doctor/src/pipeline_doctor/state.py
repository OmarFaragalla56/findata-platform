"""The graph's state schema.

Grouped by which milestone populates it. M1 only reads/writes the "identity" and
"run_step / self_heal" groups (minus fields explicitly marked for later
milestones); later milestones add nodes that populate the rest without needing to
touch this file again.
"""

from __future__ import annotations

from typing import Literal, NotRequired, TypedDict


class PipelineDoctorState(TypedDict):
    # --- identity ---
    # A run is either "run this step, then check the file it wrote" (step_path +
    # output_path) or "check this source directly" (source). Both are optional
    # because neither shape needs the other's fields; persistence.baseline_store
    # .run_key() works out the baseline identity from whichever is present.
    step_path: NotRequired[str]        # filesystem path to the target pipeline step script
    output_path: NotRequired[str]      # where the step's output is written (CSV, historically)
    step_args: NotRequired[list[str]]  # extra CLI args passed through to the step (e.g. ["--scenario", "drift"])
    source: NotRequired[dict]          # where to read data from: {"kind": "postgres"|"parquet"|"csv", ...}
    name: NotRequired[str]             # explicit identity; overrides whatever run_key would derive

    # --- run_step / self_heal (M1-M3) ---
    status: Literal[
        "pending", "running", "succeeded", "failed", "healed", "aborted",
        "quality_flagged", "approved", "rejected",
    ]
    error: NotRequired[str | None]        # exception message from the last attempt
    traceback: NotRequired[str | None]    # full stderr/traceback text from the last attempt
    retry_count: NotRequired[int]         # number of self_heal-triggered retries so far (M2)
    max_retries: NotRequired[int]         # M2
    self_heal_decision: NotRequired[dict] # structured LLM decision, M3

    # --- quality_check / diffing (M4-M5) ---
    data_profile: NotRequired[dict]       # current run's profiling stats
    baseline_profile: NotRequired[dict]   # loaded baseline for comparison
    drift_report: NotRequired[dict]       # diffing.py output: flags + magnitudes

    # --- triage (M6) ---
    triage_result: NotRequired[dict]      # {"severity": ..., "explanation": ...}

    # --- human_gate (M7) ---
    human_decision: NotRequired[Literal["approve", "reject"] | None]
