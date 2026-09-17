"""After a successful run_step: profiles the output data, compares it against
the stored baseline, and produces a drift report.

Categorical vs. numeric column roles are inferred automatically from dtype and
cardinality (infer_column_roles) rather than configured per-step. This is a
reasonable generic default for an arbitrary wrapped pipeline step, but it's a
heuristic: it correctly excludes obvious identifier columns (every value
unique) from numeric drift-testing, but a real per-step config could still be
needed later to override a case the heuristic gets wrong.

If no baseline exists yet for this step, bootstraps one from the current run
and reports "no drift" rather than comparing against nothing. Never mutates an
*existing* baseline on a normal run — see human_gate (M7) for when it's updated.
"""

from __future__ import annotations

import pandas as pd

from pipeline_doctor.config.settings import get_settings
from pipeline_doctor.persistence.baseline_store import load_baseline, save_baseline
from pipeline_doctor.state import PipelineDoctorState
from pipeline_doctor.tools.data_profiling import profile_dataframe
from pipeline_doctor.tools.diffing import diff_profiles


def infer_column_roles(df: pd.DataFrame, categorical_max_unique: int = 20) -> tuple[list[str], list[str]]:
    """numeric dtype + not all-unique -> numeric (drops likely-identifier
    columns like a sequential/UUID id); object/category/bool dtype with low
    cardinality -> categorical. Everything else (e.g. high-cardinality free
    text) still gets row_count/null_rate coverage, just no drift test."""
    numeric_columns = [
        col for col in df.select_dtypes(include="number").columns
        if df[col].nunique(dropna=True) < len(df)
    ]
    categorical_columns = [
        col for col in df.select_dtypes(include=["object", "str", "category", "bool"]).columns
        if df[col].nunique(dropna=True) <= categorical_max_unique
    ]
    return categorical_columns, numeric_columns


def quality_check(state: PipelineDoctorState) -> dict:
    settings = get_settings()
    df = pd.read_csv(state["output_path"])
    categorical_columns, numeric_columns = infer_column_roles(df)
    current_profile = profile_dataframe(df, categorical_columns, numeric_columns)

    baseline_profile = load_baseline(state["step_path"])
    if baseline_profile is None:
        save_baseline(state["step_path"], current_profile)
        return {
            "data_profile": current_profile,
            "baseline_profile": current_profile,
            "drift_report": {"has_drift": False, "flags": []},
            "status": "succeeded",
        }

    drift_report = diff_profiles(baseline_profile, current_profile, settings)
    status = "quality_flagged" if drift_report["has_drift"] else "succeeded"
    return {
        "data_profile": current_profile,
        "baseline_profile": baseline_profile,
        "drift_report": drift_report,
        "status": status,
    }
