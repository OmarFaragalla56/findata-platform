# Plan: findata pipeline + Pipeline Doctor, integrated

Written 2026-09-06, after running Pipeline Doctor against the live findata
pipeline as a one-off experiment (see "What the experiment established" below).

**Living location:** the combined repo root, moved here during Phase 0
(2026-09-17).

---

## The decision

**Option C: Airflow orchestrates, Pipeline Doctor gates.**

```
Airflow DAG:  dbt_source_freshness >> dbt_build >> pipeline_doctor_check
```

Airflow stays the scheduler. Pipeline Doctor becomes a quality gate on the
output of a step Airflow just ran — not a competing orchestrator.

Rejected alternatives, for the record:

- **A — Observer** (what the experiment actually did): PD runs a read-only
  export and drift-checks it. Works, but PD's entire `self_heal` half is
  structurally inert — retrying a `SELECT` cannot fix a pipeline. Half the
  tool is dead weight.
- **B — PD wraps the dbt build**: makes `self_heal` meaningful, but now PD and
  Airflow both want to run dbt on a schedule. Two competing orchestrators
  racing on the same `target/` directory.

Option C also fixes one of PD's own listed gaps for free: "no notification
mechanism — when `human_gate` calls `interrupt()`, nothing tells anyone." Under
C, a paused run is a visible red task in the Airflow UI.

---

## What the experiment established

Facts to build on, not assumptions:

- PD's drift detection **works on real pipeline data**. It correctly passed two
  disjoint 10-minute windows (0 shared transaction IDs, 0.8% row delta vs a 20%
  threshold) and correctly flagged a forced 79.8% row drop.
- `account_id` — the dimension this whole pipeline is *about* — is **invisible
  to drift detection**. 25 distinct values, and `infer_column_roles` uses a
  hardcoded `nunique <= 20` cutoff for categoricals. It's a string, so it isn't
  numeric either. It falls through both.
- A 2,958-row profile produces a **39.7 KB baseline**, because `numeric_sample`
  stores the entire column verbatim. The full 75,700-row mart extrapolates to
  ~1 MB per baseline, plus the same again in every LangGraph checkpoint.
- `triage` and `self_heal` **hard-fail without `ANTHROPIC_API_KEY`**. There is
  no `.env` in this project. Drift detection reached `triage` and the run died.
- dbt marts lag live data by up to the rebuild interval (observed: 15 min).
  Anything monitoring a mart inherits that lag.

---

## Phase 0 — Combine the two projects into one repo

**Status (2026-09-17): structure done, verification pending.** Done: repo
created at `findata-platform/` (copied, originals untouched), `bigdata/` nested
under `pipeline/`, `airflow/` + `docker-compose.yml` at the root, every compose
path rewritten, `.env` merged + `.env.example` created, `.gitignore` merged
(now covers `.user.yml`), `plan.md`/`notes.md`/`exp.md` at the root, `git init`.
Remaining: bring the stack up and confirm the volumes reattached, DAG green,
then the first commit.

**Goal:** one folder, one stack, one `docker compose up`.

Proposed structure:

```
findata-platform/              <- new root
├── README.md
├── plan.md                    <- this file, moved here
├── docker-compose.yml         <- the single stack definition
├── .env / .env.example
├── pipeline/                  <- was bigdata/
│   ├── producer/
│   ├── spark_jobs/
│   ├── dbt_project/
│   └── postgres_init/
├── pipeline_doctor/           <- was doc/pipeline_doctor/
│   ├── pyproject.toml
│   ├── src/pipeline_doctor/
│   └── tests/
└── airflow/
    ├── Dockerfile             <- installs dbt AND pipeline_doctor
    └── dags/
```

**Decisions to make here:**

- **Keep PD as its own installable package.** It already has a `pyproject.toml`
  and a console entry point. Do not merge the two codebases into one package —
  PD should stay usable against pipelines other than this one.
- **`git init`.** Neither project is currently a git repo. Both have a
  `.gitignore` and no history. This is the moment. Check the merged
  `.gitignore` covers: `.env`, `checkpoints/`, `data/outputs/`,
  `data/baselines/` (decide: regenerable, but arguably worth versioning as a
  record of approved state), `dbt_project/target/`, `dbt_project/logs/`,
  `.venv/`, `__pycache__/`.
- **Single `.env`.** Merge PD's `.env.example` vars into the pipeline's. Both
  already use `POSTGRES_*` with the same values, so PD's future Postgres reader
  gets its connection for free.
- **Delete `pipelines/findata_mart_export.py`** once Phase 2 lands — the
  Postgres reader makes the CSV export adapter unnecessary. Keep it until then.

**Risk:** the `bigdata` stack currently has ~12h of live data in named Docker
volumes and a working Airflow metadata DB. Moving directories changes Compose's
project name, which **orphans those volumes**. Either set `name:` explicitly in
`docker-compose.yml` (it's already `findata-pipeline` — keep that string
unchanged) or accept a clean rebuild. Verify before moving, not after.

**Done when:** `docker compose up -d` from the new root brings up the full
stack, `pytest` passes PD's 35 tests from the new location, and the Airflow DAG
still runs green.

---

## Phase 1 — Schema drift detection

**Priority: highest.** This is the most dangerous real-world pipeline failure
and PD is currently blind to it. `diffing.py` only compares value distributions
for columns that already exist in *both* profiles — a dropped, renamed, or
retyped column is silently ignored.

**What changes:**

- `tools/data_profiling.py::profile_dataframe` — add a `schema` block to the
  profile: ordered `{column_name: dtype_string}`.
- `tools/diffing.py::diff_profiles` — add schema comparison ahead of the
  existing distribution checks. New flag kinds:
  - `schema_column_removed` — a baseline column is gone
  - `schema_column_added` — a new column appeared
  - `schema_dtype_changed` — same name, different type
- `nodes/triage.py` / prompts — schema flags need to reach the LLM with enough
  context to reason about them.

**Design questions to settle:**

- **Should schema drift bypass the severity threshold?** A dropped column is
  almost always worth a human's eyes. Consider making
  `schema_column_removed` and `schema_dtype_changed` route to `human_gate`
  regardless of `human_gate_min_severity`.
- **Column order.** Probably ignore — reordering is harmless and would generate
  noise.
- **Backward compatibility.** Existing baselines have no `schema` key. Treat a
  missing schema block as "not yet baselined" and bootstrap it, rather than
  reporting every column as newly added.

**Known limitation to accept going in:** over CSV, pandas *infers* dtypes.
A column that happens to be all-null in one run reads as `float64` instead of
`object` and will produce a false `schema_dtype_changed`. This is inherent to
the CSV round-trip, not a bug in the detection logic. **Phase 2's Postgres
reader is what makes schema drift genuinely reliable**, because the database
reports declared types. Doing Phase 1 first is fine — just expect false
positives on nullable columns until Phase 2 lands, and don't tune the logic
around them.

**Done when:** unit tests in `tests/unit/test_diffing.py` cover all three flag
kinds, and dropping a column from `fct_transactions.sql` is caught end-to-end.

---

## Phase 2 — Read from somewhere other than CSV

**Priority: high, and it unblocks Phase 1's reliability.**

Currently `nodes/quality_check.py` does exactly one thing:
`pd.read_csv(state["output_path"])`. Everything else follows from that.

**What changes:**

- New `tools/readers/` module with a common interface returning
  `(DataFrame, declared_schema)` — the second element being real source types
  where the source has them, `None` where it doesn't (CSV).
- Readers, in order of value to this project:
  1. **Postgres table or query** — the big one. Kills the export adapter,
     gives real declared dtypes, removes the `docker exec` dependency, and
     removes the mart-lag workaround from the step script.
  2. **Parquet** — cheap (pandas native), preserves dtypes.
  3. **CSV** — keep the existing path.
- `state.py` — `output_path` becomes a source spec (path + kind, or a URI).
  Keep the current CLI working; add the new form alongside.
- `cli.py` — `--output` stays for step-produced files; add a way to point at a
  table directly.

**Consequence worth stating plainly:** with a Postgres reader, Pipeline Doctor
stops needing a "step" at all for this use case. It reads
`analytics_marts.fct_transactions` directly after Airflow's `dbt_build` task.
`run_step` and `self_heal` become optional parts of the graph rather than
mandatory ones — which is the honest shape for a monitoring tool.

**Done when:** PD can drift-check a mart with no intermediate CSV, and
`pipelines/findata_mart_export.py` is deleted.

---

## Phase 3 — Bounded profiles

**Why now:** blocks pointing PD at anything real-sized. 39.7 KB for 2,958 rows
does not scale, and it inflates every checkpoint too.

- Replace `tools/data_profiling.py::numeric_sample`'s full-column capture with
  either a bounded reservoir sample (~5,000 values) or a histogram plus summary
  stats.
- The KS test still works on a bounded sample — that is what sampling is for.
  Note the sample size in the profile so the test is reproducible.
- Baselines written before this change have full columns; handle both shapes or
  version the profile format.

---

## Phase 4 — Per-step, per-column configuration

**Why:** directly fixes the `account_id` blind spot the experiment found, plus
the "fixed global thresholds" gap.

- A per-step config (YAML/JSON alongside the baseline) declaring: which columns
  are categorical / numeric / ignored, and per-column threshold overrides.
- `infer_column_roles` stays as the default when no config exists — it's a
  reasonable heuristic, it just needs to be overridable.
- For findata specifically: force `account_id` to categorical. Consider whether
  `merchant` and `location` want a cardinality check rather than a frequency
  test (they're near-unique by construction, since the producer generates a
  fresh Faker value per row).

---

## Phase 5 — Make the LLM optional

**Why:** PD currently hard-fails without `ANTHROPIC_API_KEY`. Under Option C
this runs unattended on a schedule, so a missing/expired key would break the
DAG rather than degrade it.

- Add a deterministic severity fallback in `nodes/triage.py` (e.g. row-count
  drift >50% → high, schema column removed → high, single categorical shift →
  medium) used when no key is configured.
- Keep the LLM for the *explanation* text and for judgment calls the rules
  can't make. Rules decide severity; the LLM makes it readable.
- Same for `self_heal`: without a key, fall back to "retry N times, then abort"
  rather than crashing.

---

## Phase 6 — Operability

Smaller items, mostly from `problems.md`:

- **`nodes/finalize.py` is a literal no-op** (`return {}`). Make it write a run
  report — what ran, what drifted, what was decided.
- **No subprocess timeout** in `tools/pipeline_runner.py::run_step`. A hanging
  step hangs the graph forever. (The findata export step defends itself with
  `timeout=120`, but every other step is exposed.)
- **No queryable run history** — everything is inside LangGraph's checkpoint DB
  in its own serialization format. A simple runs table would make this
  reportable.
- **Stale README** — the "Status" section says M0/scaffolding; M1–M7 are
  implemented and 35 tests pass.
- **Empty `docs/`** — README promises architecture notes and a generated
  mermaid graph diagram that don't exist.

---

## Wiring details for Option C (needs Phase 2 first)

**Correction to the original sequencing:** the Airflow task cannot be added
"alongside Phase 0". `pipelines/findata_mart_export.py` reads the mart by
shelling out to `docker exec findata-postgres psql`, and there is no Docker
CLI (or socket) inside the Airflow container. So `pipeline_doctor_check`
only becomes possible once Phase 2's Postgres reader exists — or a throwaway
psycopg-based export is written first, which Phase 2 would then delete anyway.
Phase 2 is therefore a hard prerequisite for the wiring below.


These are the concrete integration mechanics, separate from the feature work:

- **Install PD into the Airflow image.** `airflow/Dockerfile` already installs
  dbt into an isolated venv (`/home/airflow/dbt_venv`) because Airflow and
  dbt-core pin conflicting jinja2/click versions. PD brings langgraph +
  langchain + pydantic v2 and will have its own conflicts — use the same
  pattern, a third venv, rather than fighting the resolver.
- **Persist PD's state.** `data/baselines/` and `checkpoints/` must be Docker
  volumes (or host mounts) on the Airflow containers, or every container
  recreate silently re-bootstraps every baseline — which would mean PD reports
  "no drift" forever and looks healthy while checking nothing. This is the same
  class of bug as the Spark checkpoint volume gap already fixed in the pipeline.
- **Decide exit-code semantics for a paused run.** PD's CLI currently prints
  "PAUSED - awaiting human review" and exits 0. As an Airflow task that reads
  as success, so a pending review would look green and the DAG would carry on.
  Proposal: PD exits `0` clean / `2` paused-awaiting-review / `1` error, and
  the DAG treats `2` as a stop. Then `pipeline-doctor resume <thread_id>
  <approve|reject>` stays a deliberate human action.
- **Connection config.** PD reads the same `POSTGRES_*` env vars the rest of
  the stack uses — no new secrets once the `.env` files are merged.

---

## Sequencing

Phase 0 first (nothing else is comfortable until the repos are one). Then
1 → 2, accepting the CSV dtype false positives in between, or swap them if the
false positives prove more annoying than waiting. 3 before pointing at anything
larger than a rolling window. 4 whenever the `account_id` blind spot starts
mattering. 5 before this runs unattended on a schedule for real. 6 is
opportunistic.

One phase at a time, each landing green with tests, before starting the next.
