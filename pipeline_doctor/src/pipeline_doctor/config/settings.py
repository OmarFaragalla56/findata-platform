"""Central config: retry limits, drift thresholds, LLM model name, storage
paths, and the Postgres connection — all overridable via environment variables
(see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Default model per provider, used when PIPELINE_DOCTOR_LLM_MODEL isn't set.
# Keyed by provider so switching providers doesn't leave a Claude model name
# pointed at Gemini's API (or the reverse).
DEFAULT_MODELS = {
    "anthropic": "claude-opus-5",
    "google": "gemini-2.5-flash",
}


@dataclass(frozen=True)
class Settings:
    """Field values here are the code-level defaults — used as-is by a plain
    `Settings()` (handy in tests that want deterministic thresholds regardless
    of the environment). Real code should go through get_settings() instead,
    which layers environment variables on top of these.
    """

    llm_provider: str = "anthropic"          # "anthropic" | "google"
    llm_model: str = DEFAULT_MODELS["anthropic"]
    max_retries: int = 3
    row_count_drift_pct: float = 0.20
    null_rate_drift_pct: float = 0.10
    categorical_drift_pvalue: float = 0.05
    ks_pvalue_threshold: float = 0.05
    checkpoint_db: str = "checkpoints/pipeline_doctor.sqlite"
    baseline_dir: str = "data/baselines"
    human_gate_min_severity: str = "medium"  # triage severities >= this reach human_gate (low < medium < high)

    # Per-step column config (which columns are categorical/numeric/ignored,
    # plus per-column threshold overrides). One JSON file per step name.
    column_config_dir: str = "data/configs"

    # Where finalize writes its run reports (one JSONL file per step name).
    run_report_dir: str = "data/runs"

    # Cap on how many values per numeric column go into a profile. The KS test
    # in diffing works on a sample; storing the whole column made a 3k-row
    # profile 39.7 KB, and that profile is held twice in graph state (current +
    # baseline) and serialized into every checkpoint.
    numeric_sample_size: int = 5000

    # Hard limit on a wrapped step's runtime. Without it a hung step hangs the
    # whole graph forever with no diagnostic.
    step_timeout_sec: int = 600

    # Postgres source connection. Deliberately the same POSTGRES_* variables the
    # rest of the stack uses, so wiring this into an existing pipeline's compose
    # stack introduces no new secrets.
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "findata"
    postgres_user: str = "findata"
    postgres_password: str = "findata_pw"


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
    provider = os.getenv("PIPELINE_DOCTOR_LLM_PROVIDER", d.llm_provider).lower()
    return Settings(
        llm_provider=provider,
        llm_model=os.getenv("PIPELINE_DOCTOR_LLM_MODEL", DEFAULT_MODELS.get(provider, d.llm_model)),
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
        column_config_dir=os.getenv("PIPELINE_DOCTOR_COLUMN_CONFIG_DIR", d.column_config_dir),
        run_report_dir=os.getenv("PIPELINE_DOCTOR_RUN_REPORT_DIR", d.run_report_dir),
        numeric_sample_size=int(
            os.getenv("PIPELINE_DOCTOR_NUMERIC_SAMPLE_SIZE", str(d.numeric_sample_size))
        ),
        step_timeout_sec=int(
            os.getenv("PIPELINE_DOCTOR_STEP_TIMEOUT_SEC", str(d.step_timeout_sec))
        ),
        postgres_host=os.getenv("POSTGRES_HOST", d.postgres_host),
        postgres_port=int(os.getenv("POSTGRES_PORT", str(d.postgres_port))),
        postgres_db=os.getenv("POSTGRES_DB", d.postgres_db),
        postgres_user=os.getenv("POSTGRES_USER", d.postgres_user),
        postgres_password=os.getenv("POSTGRES_PASSWORD", d.postgres_password),
    )
