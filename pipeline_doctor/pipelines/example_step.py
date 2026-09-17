"""Toy pipeline step used to develop and test pipeline_doctor against, without
needing a real data pipeline. Generates a synthetic "customers" dataset and can be
told, via CLI flags, to succeed, crash (optionally recovering after N attempts, to
exercise the self_heal retry loop), or succeed while drifting its output
distribution away from the "success" baseline (to exercise quality_check).

Invoked by pipeline_doctor.tools.pipeline_runner as a subprocess:

    python pipelines/example_step.py --scenario success --output out.csv
    python pipelines/example_step.py --scenario crash --output out.csv --fail-times 2
    python pipelines/example_step.py --scenario drift --output out.csv --drift-kind categorical

Exit code 0 + CSV written = success. Non-zero exit + traceback on stderr = failure.

Scenarios:
  success   Writes ~1000 rows matching the reference distribution:
              age ~ Normal(35, 8), signup_channel in {web: .50, mobile: .35,
              referral: .15}, ~5% nulls in `email`.
  crash     Raises an exception. With --fail-times N, tracks attempts in a sidecar
            "<output>.attempts" counter file and only raises while the counter is
            below N, then succeeds (like `success`) and removes the counter file —
            this simulates a transient failure that self_heal's retry can recover
            from. Omit --fail-times (or pass -1) to always fail.
  drift     Writes output like `success` but with one aspect of the distribution
            shifted, chosen via --drift-kind:
              row_count    ~200 rows instead of ~1000
              null         ~30% nulls in `email` instead of ~5%
              categorical  signup_channel shifted to {web: .10, mobile: .20,
                           referral: .70}
              numeric      age ~ Normal(50, 8) instead of Normal(35, 8)
              all          applies every shift above at once (default)
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

N_ROWS_DEFAULT = 1000
N_ROWS_DRIFTED = 200
CHANNELS = ["web", "mobile", "referral"]
CHANNEL_WEIGHTS_BASELINE = [0.50, 0.35, 0.15]
CHANNEL_WEIGHTS_DRIFTED = [0.10, 0.20, 0.70]
AGE_MEAN_BASELINE, AGE_MEAN_DRIFTED = 35, 50
AGE_STD = 8
NULL_RATE_BASELINE, NULL_RATE_DRIFTED = 0.05, 0.30


def _generate(n_rows: int, channel_weights: list[float], age_mean: float,
              null_rate: float, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ages = rng.normal(loc=age_mean, scale=AGE_STD, size=n_rows).round(1)
    channels = rng.choice(CHANNELS, size=n_rows, p=channel_weights)
    emails = [f"user{i}@example.com" for i in range(n_rows)]
    null_mask = rng.random(n_rows) < null_rate
    emails = [None if is_null else email for is_null, email in zip(null_mask, emails)]
    return pd.DataFrame({
        "id": range(n_rows),
        "age": ages,
        "signup_channel": channels,
        "email": emails,
    })


def _write_success(output: str, drift_kind: str | None, seed: int) -> None:
    n_rows = N_ROWS_DEFAULT
    channel_weights = CHANNEL_WEIGHTS_BASELINE
    age_mean = AGE_MEAN_BASELINE
    null_rate = NULL_RATE_BASELINE

    if drift_kind in ("row_count", "all"):
        n_rows = N_ROWS_DRIFTED
    if drift_kind in ("categorical", "all"):
        channel_weights = CHANNEL_WEIGHTS_DRIFTED
    if drift_kind in ("numeric", "all"):
        age_mean = AGE_MEAN_DRIFTED
    if drift_kind in ("null", "all"):
        null_rate = NULL_RATE_DRIFTED

    df = _generate(n_rows, channel_weights, age_mean, null_rate, seed)
    df.to_csv(output, index=False)
    print(f"wrote {len(df)} rows to {output}")


def _run_crash(output: str, fail_times: int, seed: int) -> None:
    counter_path = f"{output}.attempts"
    attempts = 0
    if os.path.exists(counter_path):
        attempts = int(open(counter_path).read().strip() or 0)

    if fail_times < 0 or attempts < fail_times:
        attempts += 1
        with open(counter_path, "w") as f:
            f.write(str(attempts))
        raise RuntimeError(
            f"simulated crash on attempt {attempts}"
            + ("" if fail_times < 0 else f" (recovers after {fail_times})")
        )

    # Recovered: succeed like a normal run and clear the counter.
    if os.path.exists(counter_path):
        os.remove(counter_path)
    _write_success(output, drift_kind=None, seed=seed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=["success", "crash", "drift"], required=True)
    parser.add_argument("--output", required=True, help="CSV path to write on success")
    parser.add_argument("--drift-kind",
                         choices=["row_count", "null", "categorical", "numeric", "all"],
                         default="all", help="only used when --scenario drift")
    parser.add_argument("--fail-times", type=int, default=-1,
                         help="only used when --scenario crash; -1 = always fail, "
                              "N = fail N times then succeed")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.scenario == "success":
        _write_success(args.output, drift_kind=None, seed=args.seed)
    elif args.scenario == "drift":
        _write_success(args.output, drift_kind=args.drift_kind, seed=args.seed)
    elif args.scenario == "crash":
        _run_crash(args.output, fail_times=args.fail_times, seed=args.seed)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
