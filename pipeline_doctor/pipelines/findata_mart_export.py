"""Pipeline Doctor step that exports a slice of the findata pipeline's dbt marts
to CSV, so Pipeline Doctor can profile and drift-check real streaming data
instead of the synthetic example_step dataset.

Target pipeline: ../../pipeline (Kafka -> Spark Structured Streaming -> Postgres
-> dbt). This script is the adapter between the two projects; it owns no
pipeline logic of its own, it just materializes a mart as CSV.

Invoked by pipeline_doctor.tools.pipeline_runner as a subprocess:

    python pipelines/findata_mart_export.py --output out.csv
    python pipelines/findata_mart_export.py --output out.csv --minutes 30

Reads via `docker exec findata-postgres psql --csv` rather than a Python
Postgres driver, so this adds no dependency to pipeline_doctor's venv and uses
the same access path as the rest of the findata stack's tooling.

Why a bounded time window (--minutes) rather than the whole mart:
data_profiling.numeric_sample stores the *entire* numeric column in the profile
and hence in the baseline JSON and the LangGraph checkpoint. fct_transactions
grows by ~300 rows/minute indefinitely, so exporting all of it would bloat both
and get slower every run. A rolling window keeps the profile a fixed size and
also makes drift meaningful: it compares like-for-like periods of activity
rather than an ever-growing cumulative total whose row count only ever rises.

Why the window is anchored to the mart's own max(event_time), not now():
fct_transactions is a dbt table rebuilt on a schedule (hourly, via Airflow), so
its newest row is as old as the last build - up to an hour behind the live raw
stream. A window measured against wall-clock `now()` therefore comes back empty
most of the time. Anchoring to the data's own latest timestamp makes the export
depend on mart contents rather than on when dbt last happened to run.
"""

import argparse
import shutil
import subprocess
import sys

CONTAINER = "findata-postgres"
DB_USER = "findata"
DB_NAME = "findata"

# Column list is explicit rather than `select *` so a schema change upstream
# surfaces as a psql error here, rather than silently changing the CSV's shape
# (Pipeline Doctor has no schema-drift detection of its own - see problems.md).
QUERY_TEMPLATE = """
select
    transaction_id,
    account_id,
    event_time,
    event_date,
    amount,
    merchant,
    category,
    location,
    card_present,
    is_large_transaction
from analytics_marts.fct_transactions
where event_time >= (
    select max(event_time) - interval '{minutes} minutes'
    from analytics_marts.fct_transactions
)
"""


def export(output: str, minutes: int) -> int:
    if shutil.which("docker") is None:
        raise RuntimeError("docker not found on PATH; is the findata stack reachable?")

    query = QUERY_TEMPLATE.format(minutes=minutes)
    # --csv only: no -t/-A. `-A` selects psql's unaligned format and overrides
    # --csv, which silently yields pipe-delimited output; letting psql emit the
    # header itself also avoids maintaining a second copy of the column list
    # that could drift out of step with QUERY_TEMPLATE.
    proc = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB_NAME,
         "--csv", "-c", query],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"psql export failed (exit {proc.returncode}): {proc.stderr.strip()}"
        )

    lines = proc.stdout.strip().splitlines()
    if len(lines) <= 1:  # header only, or nothing at all
        raise RuntimeError(
            f"no rows in the {minutes}-minute window - has dbt ever built the mart?"
        )

    with open(output, "w", newline="") as f:
        f.write("\n".join(lines) + "\n")

    return len(lines) - 1  # exclude header


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="CSV path to write on success")
    parser.add_argument("--minutes", type=int, default=10,
                        help="export transactions from the last N minutes (default 10)")
    args = parser.parse_args()

    n = export(args.output, args.minutes)
    print(f"wrote {n} rows to {args.output} (last {args.minutes} min of fct_transactions)")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
