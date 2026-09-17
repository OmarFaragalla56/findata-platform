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

from pipeline_doctor.config.settings import get_settings


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


def run_step(
    step_path: str,
    output_path: str,
    extra_args: list[str] | None = None,
    timeout_sec: int | None = None,
) -> StepResult:
    """Runs `step_path` as a subprocess: `python <step_path> --output <output_path>
    [extra_args...]`. Captures stdout/stderr; success is `returncode == 0`.

    A timeout is enforced (settings.step_timeout_sec by default). Without one, a
    step that hangs — a query waiting on a lock, a request with no timeout of
    its own — hangs the whole graph indefinitely, with no error and nothing in
    state to explain it. A timeout turns that into an ordinary failure, which
    self_heal can then decide to retry or abort."""
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if timeout_sec is None:
        timeout_sec = get_settings().step_timeout_sec

    cmd = [sys.executable, step_path, "--output", output_path, *(extra_args or [])]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired as exc:
        # Report it the way a crashed step is reported, so the graph's existing
        # failure path handles it. 124 is the conventional timeout exit code.
        return StepResult(
            success=False,
            stdout=exc.stdout or "",
            stderr=f"step exceeded its {timeout_sec}s timeout and was killed",
            returncode=124,
        )
    return StepResult(
        success=proc.returncode == 0,
        stdout=proc.stdout,
        stderr=proc.stderr,
        returncode=proc.returncode,
    )
