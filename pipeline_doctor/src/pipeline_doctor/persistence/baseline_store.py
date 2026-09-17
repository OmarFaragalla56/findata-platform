"""Reads and writes per-step baseline profiles as JSON under data/baselines/
(one file per step). This is the only module that touches that directory —
callers never build the path themselves.

Lifecycle (see build plan): quality_check only reads (bootstrapping a new
baseline file if one doesn't exist yet); human_gate is the only writer of an
*update* to an existing baseline, and only on a human "approve" decision.

Identity: a baseline file is named after the *run key* (see run_key). A run that
wraps a step script is keyed by that script's name, as it always was. A run that
reads a source directly has no script, so it is keyed by an explicit --name, or
failing that by the table it reads. Getting this wrong is quietly destructive —
two different sources sharing one key would each overwrite the other's idea of
"normal", and drift detection would go on reporting green while comparing
against the wrong thing — so derivation is deliberately boring and explicit.
"""

from __future__ import annotations

import hashlib
import json
import os
import re

from pipeline_doctor.config.settings import get_settings

_UNSAFE = re.compile(r"[^A-Za-z0-9_-]+")


def _safe(value: str) -> str:
    """Filesystem-safe, and free of '.' specifically: step_name() runs
    splitext() on whatever it is given, so a dotted key like
    "analytics_marts.fct_transactions" would otherwise be truncated to
    "analytics_marts"."""
    return _UNSAFE.sub("_", value).strip("_")


def step_name(step_path: str) -> str:
    """A filesystem-safe, stable identifier for a step, derived from its
    script's basename (e.g. "pipelines/example_step.py" -> "example_step")."""
    return os.path.splitext(os.path.basename(step_path))[0]


def run_key(state: dict) -> str:
    """The identity a baseline (and a run report) is filed under.

    Precedence: explicit name > step script > source. The source fallbacks
    exist so a one-off `check` still works without --name, but an explicit name
    is the honest choice for anything recurring."""
    name = state.get("name")
    if name:
        return _safe(name)

    step_path = state.get("step_path")
    if step_path:
        return step_name(step_path)

    source = state.get("source") or {}
    kind = (source.get("kind") or "").lower()
    if kind == "postgres":
        if source.get("table"):
            return _safe(source["table"])
        if source.get("query"):
            # No table to name it after. Hash the query so the same query keeps
            # the same baseline, and a different one gets its own rather than
            # silently inheriting.
            digest = hashlib.sha256(source["query"].encode()).hexdigest()[:12]
            return f"query_{digest}"
    if source.get("path"):
        return step_name(source["path"])

    raise ValueError("cannot derive a baseline identity: pass --name, a step_path, or a source")


def baseline_path(step_path: str) -> str:
    return os.path.join(get_settings().baseline_dir, f"{step_name(step_path)}.json")


def load_baseline(step_path: str) -> dict | None:
    """Returns the stored baseline profile for this step, or None if it
    hasn't been bootstrapped yet."""
    path = baseline_path(step_path)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def save_baseline(step_path: str, profile: dict) -> None:
    """Overwrites the stored baseline for this step with `profile`. Used both
    to bootstrap a first-ever baseline (quality_check) and to update one after
    a human approves a drift (human_gate, M7)."""
    path = baseline_path(step_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(profile, f, indent=2)
