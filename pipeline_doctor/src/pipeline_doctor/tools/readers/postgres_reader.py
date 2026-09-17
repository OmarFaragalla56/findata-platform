"""Postgres reader: query a table (or an arbitrary SELECT) directly, with the
column types the server declares.

This is what removes the need for a "step" at all when monitoring a warehouse
table. Before it, reading analytics_marts.fct_transactions meant a wrapper
script shelling out to `docker exec ... psql --csv`, which also meant Pipeline
Doctor could only run somewhere with a Docker socket - ruling out the Airflow
container it is meant to run in.

Connection details come from the same POSTGRES_* environment variables the rest
of the stack uses, so integrating adds no new secrets.
"""

from __future__ import annotations

import re

import pandas as pd

from pipeline_doctor.config.settings import get_settings
from pipeline_doctor.tools.readers import ReadResult, SourceSpec

# schema.table / table, letters digits underscore only. Used to reject anything
# that isn't a plain identifier before it reaches an f-string: `table` names a
# relation and cannot be passed as a query parameter, so it has to be validated
# rather than bound.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")

# Postgres type names whose values psycopg hands back as decimal.Decimal or as
# Python ints/floats. Decimal lands in pandas as dtype=object, which would make
# a money column invisible to numeric drift testing, so these are coerced.
_NUMERIC_TYPES = {
    "numeric", "decimal", "money",
    "int2", "int4", "int8", "smallint", "integer", "bigint",
    "float4", "float8", "real", "double precision",
}


def _connection_string() -> str:
    s = get_settings()
    return (
        f"host={s.postgres_host} port={s.postgres_port} dbname={s.postgres_db} "
        f"user={s.postgres_user} password={s.postgres_password}"
    )


def _build_query(spec: SourceSpec) -> str:
    query, table = spec.get("query"), spec.get("table")
    if query and table:
        raise ValueError("postgres source takes 'query' or 'table', not both")
    if query:
        return query
    if not table:
        raise ValueError("postgres source requires 'query' or 'table'")
    if not _IDENTIFIER.match(table):
        raise ValueError(f"not a valid table identifier: {table!r}")
    sql = f"select * from {table}"
    limit = spec.get("limit")
    if limit is not None:
        sql += f" limit {int(limit)}"       # int() is the validation
    return sql


def read_postgres_source(spec: SourceSpec) -> ReadResult:
    import psycopg  # lazy: only postgres sources need the driver

    sql = _build_query(spec)
    with psycopg.connect(_connection_string()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                raise ValueError("query returned no result set (did you pass a non-SELECT?)")
            columns = [d.name for d in cur.description]
            declared = {d.name: _type_name(d.type_code) for d in cur.description}
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=columns)
    for column, type_name in declared.items():
        if type_name in _NUMERIC_TYPES:
            # errors="coerce" rather than raise: a value that won't convert
            # becomes NaN and shows up as null-rate drift, which is a far more
            # useful signal than the monitoring run itself crashing.
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df, declared


def _type_name(oid: int) -> str:
    """OID -> Postgres type name ("numeric", "timestamp", "varchar"...).
    Falls back to the raw oid when the type isn't in psycopg's registry (a
    user-defined type, say) so the profile still records *something* stable."""
    import psycopg

    try:
        type_info = psycopg.postgres.types.get(oid)
    except Exception:  # pragma: no cover - registry lookup should not raise
        type_info = None
    return type_info.name if type_info is not None else f"oid:{oid}"
