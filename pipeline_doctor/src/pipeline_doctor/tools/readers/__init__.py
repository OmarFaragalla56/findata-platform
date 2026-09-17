"""Source readers: everything that turns "where the data is" into a DataFrame.

Before this module, quality_check did exactly one thing - `pd.read_csv(output_path)` -
which forced every monitored thing to be a *file produced by a step script*. A
database table had to be exported to CSV first, by a wrapper script whose only
job was to materialize it.

Every reader returns the same pair:

    (dataframe, declared_schema)

`declared_schema` is `{column_name: type_string}` **as the source itself
declares it**, or None where the source has no type information of its own.
That second element is what makes schema-drift detection trustworthy: over CSV
pandas has to *infer* types, so a column that happens to be all-null in one run
reads as float64 instead of object and looks like a type change. Postgres and
Parquet report the types they actually store, so they can't produce that false
positive.

    kind        declared_schema   notes
    ----------  ----------------  --------------------------------------------
    postgres    yes               real column types, straight from the server
    parquet     yes               types are stored in the file
    csv         None              pandas guesses; drift on dtypes is unreliable
"""

from __future__ import annotations

import pandas as pd

SourceSpec = dict
ReadResult = tuple[pd.DataFrame, dict[str, str] | None]

_KINDS = ("csv", "parquet", "postgres")


def read_source(spec: SourceSpec) -> ReadResult:
    """Dispatch on spec["kind"]. Readers are imported lazily so that an optional
    dependency (psycopg for postgres, pyarrow for parquet) is only required by
    the people who actually use that source kind."""
    kind = (spec.get("kind") or "csv").lower()

    if kind == "csv":
        from pipeline_doctor.tools.readers.csv_reader import read_csv_source
        return read_csv_source(spec)
    if kind == "parquet":
        from pipeline_doctor.tools.readers.parquet_reader import read_parquet_source
        return read_parquet_source(spec)
    if kind == "postgres":
        from pipeline_doctor.tools.readers.postgres_reader import read_postgres_source
        return read_postgres_source(spec)

    raise ValueError(f"unknown source kind {kind!r}; expected one of {_KINDS}")
