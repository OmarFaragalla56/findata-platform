"""Reads and writes per-step baseline profiles as JSON under data/baselines/
(one file per step). This is the only module that touches that directory —
callers never build the path themselves.

Lifecycle (see build plan): quality_check only reads (bootstrapping a new
baseline file if one doesn't exist yet); human_gate is the only writer of an
*update* to an existing baseline, and only on a human "approve" decision.
"""

from __future__ import annotations

import json
import os

from pipeline_doctor.config.settings import get_settings


def step_name(step_path: str) -> str:
    """A filesystem-safe, stable identifier for a step, derived from its
    script's basename (e.g. "pipelines/example_step.py" -> "example_step")."""
    return os.path.splitext(os.path.basename(step_path))[0]


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
