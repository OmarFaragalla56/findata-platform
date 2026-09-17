"""Pure, independently unit-testable profiling functions over a pandas
DataFrame.

Produces a JSON-serializable "profile" dict:

    {
        "row_count": int,
        "null_rates": {column: float, ...},            # all columns
        "categorical": {column: {category: freq, ...}, ...},
        "numeric": {column: [values...], ...},          # full sample, for KS-test
    }

Storing the full numeric sample (rather than just summary stats) is what lets
diffing.py run a real two-sample KS-test against the baseline later; at the
toy/dev scale this project runs at that's fine, but a production-scale version
would want to store a bounded sample or histogram instead of the full column.
"""

from __future__ import annotations

import pandas as pd


def row_count(df: pd.DataFrame) -> int:
    return len(df)


def null_rates(df: pd.DataFrame) -> dict[str, float]:
    # Explicit float(...) casts below: pandas/numpy scalars aren't guaranteed
    # to be native Python types, and this profile ends up in graph state,
    # which the checkpointer msgpack-serializes — a stray numpy.float64
    # raises TypeError there even though everything looks fine in-process.
    return {str(col): float(v) for col, v in df.isna().mean().to_dict().items()}


def categorical_frequencies(df: pd.DataFrame, column: str) -> dict[str, float]:
    counts = df[column].value_counts(normalize=True, dropna=True).to_dict()
    return {str(k): float(v) for k, v in counts.items()}


def numeric_sample(df: pd.DataFrame, column: str) -> list[float]:
    return df[column].dropna().astype(float).tolist()


def profile_dataframe(
    df: pd.DataFrame,
    categorical_columns: list[str],
    numeric_columns: list[str],
) -> dict:
    """Builds the full profile dict for `df`, given which columns to treat as
    categorical vs. numeric (every column still gets row_count/null_rates
    coverage; categorical/numeric drift tests only run on the columns named
    here)."""
    return {
        "row_count": row_count(df),
        "null_rates": null_rates(df),
        "categorical": {col: categorical_frequencies(df, col) for col in categorical_columns},
        "numeric": {col: numeric_sample(df, col) for col in numeric_columns},
    }
