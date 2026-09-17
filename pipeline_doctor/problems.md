# Known Problems / Gaps

Compiled during a full code walkthrough (2026-09-04). These are the things that make
Pipeline Doctor a solid *learning project* but not yet a production-ready tool. None of
these are fixed yet — this is a punch list for a future session.

The underlying pattern (separate "crashed" from "silently drifted," gate risky changes
behind a resumable human review) is real and used by real data-quality tools. The gaps
below are about this specific implementation, not the idea.

## Correctness / robustness gaps

- **No timeout on the pipeline subprocess.** `tools/pipeline_runner.py::run_step` calls
  `subprocess.run(cmd, capture_output=True, text=True)` with no `timeout=`. A step that
  hangs (e.g. a network call that never returns) hangs the node — and the whole graph —
  forever.
- **No schema-drift detection.** `tools/diffing.py` only compares *value distributions*
  (row count, null rate, categorical frequencies, numeric distribution) for columns that
  already exist in both baseline and current profile. A column being silently dropped,
  renamed, or changing dtype is never flagged — arguably the most common and most
  dangerous real-world pipeline failure.
- **Self-heal's "transient vs deterministic" judgment is under-tested against real
  failure modes.** `pipelines/example_step.py`'s crash scenario is only "transient"
  because it's told to be, via a hardcoded `--fail-times` sidecar counter. Most real
  crashes (bad config, bad code, missing file/column) are deterministic — retrying the
  identical subprocess with identical args fails identically, and the LLM call is wasted
  latency/cost. The pattern is genuinely useful only for a narrower slice of real
  failures (flaky network calls, rate limits, race conditions) than the demo implies.
- **Crude categorical/numeric role inference.** `nodes/quality_check.py::infer_column_roles`
  uses a flat `nunique <= 20` cutoff to decide "categorical." Easy to get wrong on real
  datasets (a genuinely categorical column with 25 distinct values gets excluded
  entirely; a low-cardinality numeric column gets treated as categorical).

## Scale gaps

- **Full numeric column stored in every profile and baseline.**
  `tools/data_profiling.py::numeric_sample` keeps the *entire* column (not a sample or
  histogram) so `diffing.py` can run a real two-sample KS-test. Fine at the toy scale
  (1000 rows); at real data volumes this bloats the LangGraph checkpoint DB (msgpack
  serialization) and the baseline JSON files, and slows every run. The module's own
  docstring already flags this as a "would need bounded sample/histogram" gap.
- **CSV-only.** `nodes/quality_check.py` reads the output path as a CSV via pandas. No
  Parquet, no database table, no streaming source.
- **Single-step scope, not a pipeline.** This wraps exactly one script (`step_path`).
  Real pipelines are DAGs of many steps (Airflow/Dagster/dbt-style). Pipeline Doctor is
  a monitor *around one step*, not an orchestrator *of a pipeline*.

## Observability / operability gaps

- **`nodes/finalize.py` is a literal no-op** (`def finalize(state): return {}`). No
  summary logging, no run report, nothing written anywhere describing what happened.
- **No notification mechanism.** When `human_gate` calls `interrupt()` and the run
  pauses, nothing tells anyone — no Slack ping, no email, no dashboard entry. The only
  way to know a run is waiting is to already be watching the terminal or to manually
  poll `pipeline-doctor resume <thread_id> ...` against a thread id you happened to save.
- **No queryable run history / audit trail.** Everything lives inside the LangGraph
  SQLite checkpoint DB, in LangGraph's own serialization format — not human-readable,
  not easily reportable on without going back through LangGraph's APIs.

## Data-quality-model gaps

- **Baseline is just "the last approved snapshot," no trend or seasonality
  awareness.** `persistence/baseline_store.py` overwrites the baseline wholesale on
  approval. A metric that's naturally higher every Monday, or trending upward over
  weeks, would trip the drift thresholds constantly with no way to express "this
  variation is expected."
- **Fixed global thresholds, not per-column.** `config/settings.py` has one
  `row_count_drift_pct`, one `null_rate_drift_pct`, etc. applied uniformly to every
  column of every step. A column that's naturally noisy and one that's naturally
  stable get the same tolerance.

## Housekeeping (unrelated to production-readiness, but noted)

- `README.md` "Status" section says "currently at M0 — scaffolding"; the code actually
  implements M1–M7 and all 35 tests pass. Stale doc.
- `docs/` directory is empty; README promises architecture notes and a generated
  mermaid diagram of the graph that don't exist yet.
- No build-plan document exists anywhere, despite being referenced in code/docstrings
  (`__init__.py`, `persistence/__init__.py`, etc.) — presumed lost to a Claude Code
  session that was never saved to a file.
- `llm/client.py` hardcodes `ChatAnthropic`. Swapping to another provider (e.g. Gemini)
  needs a new client class and a settings/model-id change; `with_structured_output` is
  supported by both so the node call sites shouldn't need to change.

## Not a problem, for the record

- Attribution was checked line-by-line: Claude is not credited anywhere as author,
  co-author, or contributor (no `authors` field in `pyproject.toml`, no AUTHORS/
  CONTRIBUTORS/LICENSE file, no attribution docstrings, no git history at all — this
  directory isn't a git repo).
