"""Wraps invocation of an arbitrary target pipeline step (by file path) as a
subprocess, normalizing the result into a plain shape that nodes.run_step stores
into state.

The step is expected to follow the same CLI contract as pipelines/example_step.py:
accept `--output <path>`, write its result there on success, and exit non-zero
with a traceback on stderr on failure. Any additional scenario-specific flags
(e.g. example_step.py's `--scenario`) are passed through via `extra_args`.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass


@dataclass
class StepResult:
    success: bool
    stdout: str
    stderr: str
    returncode: int

    @property
    def traceback(self) -> str | None:
        return self.stderr if not self.success else None

    @property
    def error(self) -> str | None:
        """Best-effort one-line summary of the failure, from the last non-empty
        stderr line (typically the exception's repr)."""
        if self.success or not self.stderr:
            return None
        lines = [line for line in self.stderr.strip().splitlines() if line]
        return lines[-1] if lines else "unknown error"


def run_step(step_path: str, output_path: str, extra_args: list[str] | None = None) -> StepResult:
    """Runs `step_path` as a subprocess: `python <step_path> --output <output_path>
    [extra_args...]`. Captures stdout/stderr; success is `returncode == 0`."""
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    cmd = [sys.executable, step_path, "--output", output_path, *(extra_args or [])]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return StepResult(
        success=proc.returncode == 0,
        stdout=proc.stdout,
        stderr=proc.stderr,
        returncode=proc.returncode,
    )
