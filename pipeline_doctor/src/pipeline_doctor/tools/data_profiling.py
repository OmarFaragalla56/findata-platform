"""Pure, independently unit-testable profiling functions over a pandas
DataFrame.

Produces a JSON-serializable "profile" dict:

    {
        "profile_version": 2,
        "row_count": int,
        "schema": {column: type_string, ...},           # ordered, all columns
        "null_rates": {column: float, ...},             # all columns
        "categorical": {column: {category: freq, ...}, ...},
        "numeric": {column: [values...], ...},          # bounded sample, for KS-test
        "numeric_meta": {column: {"sampled": int, "total": int}, ...},
    }

Two things about this shape are worth knowing.

**schema** is what makes a dropped, added or retyped column detectable at all.
Without it, diffing only ever compared value distributions for columns present
in *both* profiles, so a column that vanished was simply never iterated over.
Where the source declares its own types (Postgres, Parquet) those are recorded;
over CSV pandas' inferred dtypes are used instead, and those are not reliable
enough to alarm on — see readers/.

**numeric is a bounded sample.** It used to be the entire column, which made a
3k-row profile 39.7 KB and, since state holds the current *and* baseline
profile, put two copies of it into every checkpoint. The KS test is a
two-sample test, so a sample is what it wants anyway; `numeric_meta` records
how much of the column the sample represents, so a result can be interpreted
(and reproduced) later.
"""

from __future__ import annotations

import pandas as pd

PROFILE_VERSION = 2

# Sampling is deterministic so that profiling the same data twice produces the
# same profile - otherwise re-running a check could flip a borderline KS result
# purely on the draw, and "did the data change?" would be unanswerable.
_SAMPLE_SEED = 20260917


def row_count(df: pd.DataFrame) -> int:
    return len(df)


def null_rates(df: pd.DataFrame) -> dict[str, float]:
    # Explicit float(...) casts below: pandas/numpy scalars aren't guaranteed
    # to be native Python types, and this profile ends up in graph state,
    # which the checkpointer msgpack-serializes — a stray numpy.float64
    # raises TypeError there even though everything looks fine in-process.
    return {str(col): float(v) for col, v in df.isna().mean().to_dict().items()}


def schema_of(df: pd.DataFrame, declared_schema: dict[str, str] | None = None) -> dict[str, str]:
    """{column: type_string} in column order. Prefers the source's declared
    types; falls back to pandas' inferred dtypes when the source has none."""
    if declared_schema:
        # Keep the DataFrame's column order, and don't invent entries for
        # columns the source declared but the query didn't return.
        return {str(col): str(declared_schema.get(str(col), df[col].dtype)) for col in df.columns}
    return {str(col): str(dtype) for col, dtype in df.dtypes.items()}


def categorical_frequencies(df: pd.DataFrame, column: str) -> dict[str, float]:
    counts = df[column].value_counts(normalize=True, dropna=True).to_dict()
    return {str(k): float(v) for k, v in counts.items()}


def numeric_sample(df: pd.DataFrame, column: str, max_values: int | None = None) -> list[float]:
    """Values of `column`, capped at `max_values` (no cap when None).

    Sampling (rather than taking the first N) matters: the head of a table is
    often sorted, so the first N rows are a biased slice and a KS test against
    them would flag ordering as distribution drift."""
    values = df[column].dropna().astype(float)
    if max_values is not None and len(values) > max_values:
        values = values.sample(n=max_values, random_state=_SAMPLE_SEED)
    return values.tolist()


def profile_dataframe(
    df: pd.DataFrame,
    categorical_columns: list[str],
    numeric_columns: list[str],
    declared_schema: dict[str, str] | None = None,
    numeric_sample_size: int | None = None,
) -> dict:
    """Builds the full profile dict for `df`, given which columns to treat as
    categorical vs. numeric (every column still gets row_count/null_rates/schema
    coverage; categorical/numeric drift tests only run on the columns named
    here)."""
    numeric = {
        col: numeric_sample(df, col, max_values=numeric_sample_size) for col in numeric_columns
    }
    numeric_meta = {
        col: {"sampled": len(numeric[col]), "total": int(df[col].notna().sum())}
        for col in numeric_columns
    }
    return {
        "profile_version": PROFILE_VERSION,
        "row_count": row_count(df),
        "schema": schema_of(df, declared_schema),
        # Whether those types came from the source or were guessed by pandas.
        # diffing marks dtype-change flags raised against inferred types, since
        # an all-null CSV column reads as float64 and looks like a type change
        # when nothing about the column actually changed.
        "schema_source": "declared" if declared_schema else "inferred",
        "null_rates": null_rates(df),
        "categorical": {col: categorical_frequencies(df, col) for col in categorical_columns},
        "numeric": numeric,
        "numeric_meta": numeric_meta,
    }
