"""Prompt templates for self_heal (M3) and triage (M6)."""

from __future__ import annotations

SELF_HEAL_SYSTEM_PROMPT = (
    "You are a pipeline reliability engineer deciding whether a failed data "
    "pipeline step is worth retrying. The step runs as an opaque subprocess: "
    "you cannot change its parameters, only decide whether to run the exact "
    "same command again or give up. Judge from the traceback whether the "
    "failure looks transient (e.g. a network blip, a timeout, a race "
    "condition) or deterministic (e.g. a missing file, a bad config value, a "
    "type error in the step's own code) — only the former is worth retrying."
)


def build_self_heal_prompt(
    step_path: str,
    error: str | None,
    traceback: str | None,
    retry_count: int,
    max_retries: int,
) -> list[dict]:
    user_content = (
        f"Step: {step_path}\n"
        f"This will be attempt {retry_count} of {max_retries} allowed retries.\n"
        f"Error: {error}\n\n"
        f"Traceback:\n{traceback}"
    )
    return [
        {"role": "system", "content": SELF_HEAL_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]


TRIAGE_SYSTEM_PROMPT = (
    "You are a data quality analyst triaging an automated drift report from a "
    "data pipeline's output. Classify how severe the drift looks and briefly "
    "explain the likely cause, so a human can decide quickly whether it needs "
    "their attention."
)


def build_triage_prompt(step_path: str, drift_report: dict) -> list[dict]:
    flags_text = "\n".join(
        f"- {flag['kind']} on column {flag.get('column')}: {flag['detail']}"
        for flag in drift_report["flags"]
    )
    user_content = f"Step: {step_path}\n\nDetected drift:\n{flags_text}"
    return [
        {"role": "system", "content": TRIAGE_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
