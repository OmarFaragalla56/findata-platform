"""Optional per-step column config, sitting next to the baselines.

    data/configs/<run_key>.json

    {
      "categorical": ["account_id", "category"],
      "numeric": ["amount"],
      "ignore": ["merchant", "location"],
      "row_count_drift_pct": 0.30,
      "columns": {
        "amount": {"ks_pvalue_threshold": 0.01},
        "category": {"categorical_drift_pvalue": 0.01}
      }
    }

Why this exists: nodes.quality_check.infer_column_roles decides what to test
from dtype and cardinality alone, with a hardcoded "<= 20 distinct values" cut
for categoricals. That is a fine default for an unknown table and wrong for a
specific one. In the findata pipeline it silently excluded `account_id` — 25
accounts, one over the cut, and a string so not numeric either — which is the
one dimension the whole pipeline is about.

Absent config, nothing changes: the heuristic still applies.
"""

from __future__ import annotations

import json
import os

from pipeline_doctor.config.settings import get_settings


def config_path(run_key: str) -> str:
    return os.path.join(get_settings().column_config_dir, f"{run_key}.json")


def load_column_config(run_key: str) -> dict | None:
    path = config_path(run_key)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def resolve_column_roles(
    df,
    config: dict | None,
    inferred: tuple[list[str], list[str]],
) -> tuple[list[str], list[str]]:
    """(categorical, numeric) after applying config on top of the inferred
    roles. A declared list replaces inference for that role; `ignore` removes
    columns from both. Columns named in config but absent from the data are
    dropped rather than raising — their absence is already reported as
    schema_column_removed, which is the clearer signal."""
    categorical, numeric = inferred
    if not config:
        return categorical, numeric

    present = set(map(str, df.columns))
    if "categorical" in config:
        categorical = [c for c in config["categorical"] if c in present]
    if "numeric" in config:
        numeric = [c for c in config["numeric"] if c in present]

    ignored = set(config.get("ignore") or [])
    ignored |= {c for c, spec in (config.get("columns") or {}).items() if spec.get("ignore")}
    categorical = [c for c in categorical if c not in ignored]
    numeric = [c for c in numeric if c not in ignored]
    return categorical, numeric
