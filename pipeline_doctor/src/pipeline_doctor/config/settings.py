"""Central config: retry limits, drift thresholds, LLM model name, and storage
paths — all overridable via environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """Field values here are the code-level defaults — used as-is by a plain
    `Settings()` (handy in tests that want deterministic thresholds regardless
    of the environment). Real code should go through get_settings() instead,
    which layers environment variables on top of these.
    """

    llm_model: str = "claude-opus-5"
    max_retries: int = 3
    row_count_drift_pct: float = 0.20
    null_rate_drift_pct: float = 0.10
    categorical_drift_pvalue: float = 0.05
    ks_pvalue_threshold: float = 0.05
    checkpoint_db: str = "checkpoints/pipeline_doctor.sqlite"
    baseline_dir: str = "data/baselines"
    human_gate_min_severity: str = "medium"  # triage severities >= this reach human_gate (low < medium < high)


def get_settings() -> Settings:
    """Builds a Settings instance from the current environment (python-dotenv's
    load_dotenv(), then os.getenv), falling back to each field's default above
    when a variable isn't set.

    Deliberately reads the environment fresh on every call rather than caching
    a singleton or relying on dataclass field defaults evaluated once at
    import time (a default like `os.getenv(...)` written directly on a field
    is baked in when the module is first imported, not re-read later) — this
    way both a real .env edit and, in tests, a monkeypatched env var are always
    picked up on the next call.
    """
    load_dotenv()
    d = Settings()
    return Settings(
        llm_model=os.getenv("PIPELINE_DOCTOR_LLM_MODEL", d.llm_model),
        max_retries=int(os.getenv("PIPELINE_DOCTOR_MAX_RETRIES", str(d.max_retries))),
        row_count_drift_pct=float(
            os.getenv("PIPELINE_DOCTOR_ROW_COUNT_DRIFT_PCT", str(d.row_count_drift_pct))
        ),
        null_rate_drift_pct=float(
            os.getenv("PIPELINE_DOCTOR_NULL_RATE_DRIFT_PCT", str(d.null_rate_drift_pct))
        ),
        categorical_drift_pvalue=float(
            os.getenv("PIPELINE_DOCTOR_CATEGORICAL_DRIFT_PVALUE", str(d.categorical_drift_pvalue))
        ),
        ks_pvalue_threshold=float(
            os.getenv("PIPELINE_DOCTOR_KS_PVALUE_THRESHOLD", str(d.ks_pvalue_threshold))
        ),
        checkpoint_db=os.getenv("PIPELINE_DOCTOR_CHECKPOINT_DB", d.checkpoint_db),
        baseline_dir=os.getenv("PIPELINE_DOCTOR_BASELINE_DIR", d.baseline_dir),
        human_gate_min_severity=os.getenv(
            "PIPELINE_DOCTOR_HUMAN_GATE_MIN_SEVERITY", d.human_gate_min_severity
        ),
    )
