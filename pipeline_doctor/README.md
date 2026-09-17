# Pipeline Doctor

A LangGraph agent that runs a data pipeline step, self-heals on crashes, and
separately checks output data quality (row count drift, null drift, categorical
drift, numeric distribution shift via KS-test) even when the code succeeds.
Anomalies are triaged by an LLM and gated behind a human-in-the-loop `interrupt()`
before the graph proceeds. State is checkpointed via LangGraph's `SqliteSaver`, so a
run can be interrupted and resumed later with `Command(resume=...)`.

## Status

Under active build, milestone by milestone. See the build plan for the full
directory rationale and milestone breakdown (currently at M0 — scaffolding).

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
- `docs/` — architecture notes and a generated mermaid diagram of the graph.

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
