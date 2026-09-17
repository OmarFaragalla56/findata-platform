"""Profiles this run's data, compares it against the stored baseline, and
produces a drift report.

The data can come from a file a wrapped step produced, or straight from a
source (a Postgres table, a Parquet file) with no step involved — see
tools.readers. Reading a source directly is what lets this run as a check
*after* an orchestrator has already done the work, rather than insisting on
running the work itself.

Categorical vs. numeric column roles are inferred from dtype and cardinality
(infer_column_roles) unless a per-step config overrides them (see
persistence.column_config). The heuristic is a reasonable default for an
arbitrary table; the config exists because a default cannot know, for example,
that a 25-value string column is the key dimension of the pipeline rather than
a high-cardinality label.

If no baseline exists yet, bootstraps one from this run and reports "no drift"
rather than comparing against nothing. Never mutates an *existing* baseline on
a normal run — see human_gate for when it's updated.
"""

from __future__ import annotations

import pandas as pd

from pipeline_doctor.config.settings import get_settings
from pipeline_doctor.persistence.baseline_store import load_baseline, run_key, save_baseline
from pipeline_doctor.persistence.column_config import load_column_config, resolve_column_roles
from pipeline_doctor.state import PipelineDoctorState
from pipeline_doctor.tools.data_profiling import profile_dataframe
from pipeline_doctor.tools.diffing import diff_profiles
from pipeline_doctor.tools.readers import read_source


def infer_column_roles(df: pd.DataFrame, categorical_max_unique: int = 20) -> tuple[list[str], list[str]]:
    """numeric dtype + not all-unique -> numeric (drops likely-identifier
    columns like a sequential/UUID id); object/category/bool dtype with low
    cardinality -> categorical. Everything else (e.g. high-cardinality free
    text) still gets row_count/null_rate/schema coverage, just no drift test."""
    numeric_columns = [
        col for col in df.select_dtypes(include="number").columns
        if df[col].nunique(dropna=True) < len(df)
    ]
    categorical_columns = [
        col for col in df.select_dtypes(include=["object", "str", "category", "bool"]).columns
        if df[col].nunique(dropna=True) <= categorical_max_unique
    ]
    return categorical_columns, numeric_columns


def _read(state: PipelineDoctorState) -> tuple[pd.DataFrame, dict[str, str] | None]:
    """A `source` wins when present; otherwise fall back to the step-output
    path, which is always a CSV (that was the only shape before readers/)."""
    source = state.get("source")
    if source:
        return read_source(source)

    output_path = state.get("output_path")
    if not output_path:
        raise ValueError("quality_check needs either a 'source' or an 'output_path'")
    return read_source({"kind": "csv", "path": output_path})


def quality_check(state: PipelineDoctorState) -> dict:
    settings = get_settings()
    key = run_key(state)

    df, declared_schema = _read(state)
    column_config = load_column_config(key)
    categorical_columns, numeric_columns = resolve_column_roles(
        df, column_config, infer_column_roles(df)
    )
    current_profile = profile_dataframe(
        df,
        categorical_columns,
        numeric_columns,
        declared_schema=declared_schema,
        numeric_sample_size=settings.numeric_sample_size,
    )

    baseline_profile = load_baseline(key)
    if baseline_profile is None:
        save_baseline(key, current_profile)
        return {
            "data_profile": current_profile,
            "baseline_profile": current_profile,
            "drift_report": {"has_drift": False, "flags": []},
            "status": "succeeded",
        }

    drift_report = diff_profiles(baseline_profile, current_profile, settings, column_config)
    status = "quality_flagged" if drift_report["has_drift"] else "succeeded"
    return {
        "data_profile": current_profile,
        "baseline_profile": baseline_profile,
        "drift_report": drift_report,
        "status": status,
    }
