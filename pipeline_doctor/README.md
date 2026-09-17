# Pipeline Doctor

A LangGraph agent that runs a data pipeline step, self-heals on crashes, and
separately checks output data quality (row count drift, null drift, categorical
drift, numeric distribution shift via KS-test) even when the code succeeds.
Anomalies are triaged by an LLM and gated behind a human-in-the-loop `interrupt()`
before the graph proceeds. State is checkpointed via LangGraph's `SqliteSaver`, so a
run can be interrupted and resumed later with `Command(resume=...)`.

## Status

Feature-complete for its original scope and wired into the findata pipeline as
a scheduled quality gate. 84 tests pass (`pytest`).

What it does today:

- Reads from **Postgres, Parquet or CSV** (`tools/readers/`). A Postgres source
  reports its own declared column types, and needs no step script at all.
- Detects **schema drift** (column removed / added / retyped) as well as row
  count, null rate, categorical and numeric distribution drift.
- **Bounded profiles**: numeric columns are sampled, not stored whole.
- **Per-step column config** (`data/configs/<name>.json`) overrides the
  role-inference heuristic and any threshold, per column.
- **Runs without an API key**: deterministic rules assign severity when no LLM
  is configured, instead of failing the run.
- Structural drift (a column removed or retyped) always reaches a human,
  whatever severity was assigned.
- Exits **0** clean / **2** awaiting human review / **1** error, so a scheduler
  can tell the three apart.

See `docs/architecture.md` for the graph and `../plan.md` for the roadmap this
followed.

## Project layout

- `src/pipeline_doctor/` — the installable package: graph, nodes, tools, LLM
  wrapper, and persistence.
- `pipelines/` — example/target pipeline steps the agent wraps and runs.
- `data/baselines/` — JSON snapshots of each step's reference data profile, used to
  detect drift. Regenerable; not meant to be hand-authored (though it's plain JSON,
  so it's inspectable and git-diffable).
- `checkpoints/` — the SqliteSaver checkpoint database (gitignored, regenerable).
- `tests/` — unit tests (pure functions, no LangGraph/LLM) and integration tests
  (full graph runs, including the interrupt/resume cycle).
- `docs/` — architecture notes and a mermaid diagram of the graph.
- `data/configs/` — optional per-step column config (checked in: it's source, not state).
- `data/runs/` — one JSONL run report per step, appended by `finalize`.

## Setup (once dependencies land in later milestones)

```bash
cp .env.example .env   # fill in ANTHROPIC_API_KEY etc.
pip install -e ".[dev]"
```

## Running

```bash
pipeline-doctor run <step-name>            # starts a new run (fresh thread_id)
pipeline-doctor resume <thread_id> <approve|reject>   # resumes after a human_gate interrupt
```
