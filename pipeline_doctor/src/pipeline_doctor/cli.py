"""Entry point.

  pipeline-doctor run <step_path> --output <path> [-- <extra args for the step>]
      Runs a step script, then quality-checks the file it wrote.

  pipeline-doctor check --source postgres --table <schema.table> [--name <id>]
  pipeline-doctor check --source postgres --query "select ..." --name <id>
  pipeline-doctor check --source parquet --path <file.parquet> [--name <id>]
  pipeline-doctor check --source csv --path <file.csv> [--name <id>]
      Quality-checks a source directly, with no step to run. This is the shape
      for monitoring something an orchestrator already built.

  pipeline-doctor resume <thread_id> <approve|reject>
      Continues a run that paused at human_gate, via Command(resume=...)
      against that same thread_id — never a new one, or the checkpointer
      won't find the paused state.

Exit codes (these matter when this runs as a scheduled task):

  0  clean — no drift, or drift below the human-review threshold
  2  paused — drift is awaiting human review
  1  error — the step failed, the run aborted, or a human rejected the drift

A paused run used to exit 0, which as an Airflow task reads as success: the DAG
would go green and carry on while a review sat unanswered forever. Exiting 2
makes the task fail, so the pending review is visible in the place people
already look.
"""

from __future__ import annotations

import argparse
import sys

from langgraph.types import Command

from pipeline_doctor.config.settings import get_settings
from pipeline_doctor.graph import build_graph
from pipeline_doctor.persistence.checkpointer import (
    get_checkpointer,
    new_thread_id,
    thread_config,
)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_PAUSED = 2

_FAILED_STATUSES = {"failed", "aborted", "rejected"}


def _invoke(initial_state: dict) -> tuple[str, dict]:
    thread_id = new_thread_id()
    with get_checkpointer() as checkpointer:
        graph = build_graph(checkpointer)
        result = graph.invoke(initial_state, config=thread_config(thread_id))
    return thread_id, result


def run(step_path: str, output_path: str, step_args: list[str] | None = None) -> int:
    thread_id, result = _invoke({
        "step_path": step_path,
        "output_path": output_path,
        "step_args": step_args or [],
        "status": "pending",
        "retry_count": 0,
        "max_retries": get_settings().max_retries,
    })
    return _print_result(thread_id, result)


def check(source: dict, name: str | None = None) -> int:
    """Quality-check a source with no step to run."""
    state = {
        "source": source,
        "status": "pending",
        "retry_count": 0,
        "max_retries": get_settings().max_retries,
    }
    if name:
        state["name"] = name
    thread_id, result = _invoke(state)
    return _print_result(thread_id, result)


def resume(thread_id: str, decision: str) -> int:
    with get_checkpointer() as checkpointer:
        graph = build_graph(checkpointer)
        result = graph.invoke(Command(resume=decision), config=thread_config(thread_id))
    return _print_result(thread_id, result)


def _print_result(thread_id: str, result: dict) -> int:
    print(f"thread_id: {thread_id}")
    if "__interrupt__" in result:
        payload = result["__interrupt__"][0].value
        triage = payload["triage_result"]
        print("status: PAUSED - awaiting human review")
        print(f"  step: {payload['step_path']}")
        print(f"  triage severity: {triage['severity']}")
        print(f"  triage explanation: {triage['explanation']}")
        print("  drift flags:")
        for flag in payload["drift_report"]["flags"]:
            print(f"    - [{flag['kind']}] {flag['detail']}")
        print(f"\nResume with: pipeline-doctor resume {thread_id} <approve|reject>")
        return EXIT_PAUSED

    status = result.get("status")
    print(f"status: {status}")
    print(f"retry_count: {result.get('retry_count', 0)}")
    drift_report = result.get("drift_report") or {}
    for flag in drift_report.get("flags", []):
        print(f"  - [{flag['kind']}] {flag['detail']}")
    return EXIT_ERROR if status in _FAILED_STATUSES else EXIT_OK


def _source_from_args(args) -> dict:
    kind = args.source
    if kind == "postgres":
        if not (args.table or args.query):
            raise SystemExit("check --source postgres needs --table or --query")
        source = {"kind": "postgres"}
        if args.table:
            source["table"] = args.table
        if args.query:
            source["query"] = args.query
        if args.limit is not None:
            source["limit"] = args.limit
        return source

    if not args.path:
        raise SystemExit(f"check --source {kind} needs --path")
    return {"kind": kind, "path": args.path}


def main() -> None:
    # Split on a literal "--" ourselves rather than using argparse.REMAINDER:
    # REMAINDER starts consuming immediately after the first positional it's
    # attached to, regardless of order, so a later `--output` on the command
    # line gets swallowed into it instead of being parsed as an option.
    argv = sys.argv[1:]
    if "--" in argv:
        sep = argv.index("--")
        own_argv, step_args = argv[:sep], argv[sep + 1:]
    else:
        own_argv, step_args = argv, []

    parser = argparse.ArgumentParser(prog="pipeline-doctor", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="run a step, then quality-check its output")
    run_parser.add_argument("step_path")
    run_parser.add_argument("--output", required=True)

    check_parser = sub.add_parser("check", help="quality-check a source directly (no step)")
    check_parser.add_argument("--source", choices=["postgres", "parquet", "csv"], default="postgres")
    check_parser.add_argument("--table", help="postgres: schema-qualified table name")
    check_parser.add_argument("--query", help="postgres: a SELECT to profile")
    check_parser.add_argument("--path", help="parquet/csv: file to profile")
    check_parser.add_argument("--limit", type=int, help="postgres: cap rows read from --table")
    check_parser.add_argument(
        "--name",
        help="baseline identity. Defaults to the table name (or a hash of the query); "
             "set it explicitly for anything that runs on a schedule.",
    )

    resume_parser = sub.add_parser("resume", help="resume a run paused at human_gate")
    resume_parser.add_argument("thread_id")
    resume_parser.add_argument("decision", choices=["approve", "reject"])

    args = parser.parse_args(own_argv)
    if args.command == "run":
        code = run(args.step_path, args.output, step_args)
    elif args.command == "check":
        code = check(_source_from_args(args), args.name)
    else:
        code = resume(args.thread_id, args.decision)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
