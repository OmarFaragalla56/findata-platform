"""Entry point.

  pipeline-doctor run <step_path> --output <path> [-- <extra args for the step>]
      Starts a new run with a fresh thread_id, invokes the graph, and either
      prints the final status or, if it paused at human_gate, prints the
      triage/drift summary and the thread_id to resume with.

  pipeline-doctor resume <thread_id> <approve|reject>
      Continues a run that paused at human_gate, via Command(resume=...)
      against that same thread_id — never a new one, or the checkpointer
      won't find the paused state.
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


def run(step_path: str, output_path: str, step_args: list[str] | None = None) -> dict:
    thread_id = new_thread_id()
    with get_checkpointer() as checkpointer:
        graph = build_graph(checkpointer)
        result = graph.invoke(
            {
                "step_path": step_path,
                "output_path": output_path,
                "step_args": step_args or [],
                "status": "pending",
                "retry_count": 0,
                "max_retries": get_settings().max_retries,
            },
            config=thread_config(thread_id),
        )
    _print_result(thread_id, result)
    return result


def resume(thread_id: str, decision: str) -> dict:
    with get_checkpointer() as checkpointer:
        graph = build_graph(checkpointer)
        result = graph.invoke(Command(resume=decision), config=thread_config(thread_id))
    _print_result(thread_id, result)
    return result


def _print_result(thread_id: str, result: dict) -> None:
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
    else:
        print(f"status: {result['status']}")
        print(f"retry_count: {result.get('retry_count', 0)}")


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

    run_parser = sub.add_parser("run", help="start a new pipeline-doctor run")
    run_parser.add_argument("step_path")
    run_parser.add_argument("--output", required=True)

    resume_parser = sub.add_parser("resume", help="resume a run paused at human_gate")
    resume_parser.add_argument("thread_id")
    resume_parser.add_argument("decision", choices=["approve", "reject"])

    args = parser.parse_args(own_argv)
    if args.command == "run":
        run(args.step_path, args.output, step_args)
    elif args.command == "resume":
        resume(args.thread_id, args.decision)


if __name__ == "__main__":
    main()
