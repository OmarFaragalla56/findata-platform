"""Unit tests for tools.readers — the dispatcher and the specs each reader
accepts. The postgres reader's query building and identifier validation are
tested without a database; reading from a real one is covered by the live
check against the findata stack."""

import pandas as pd
import pytest

from pipeline_doctor.tools.readers import read_source
from pipeline_doctor.tools.readers.postgres_reader import _build_query


def test_csv_reader_returns_no_declared_schema(tmp_path):
    path = tmp_path / "d.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(path, index=False)

    df, declared = read_source({"kind": "csv", "path": str(path)})

    assert list(df.columns) == ["a", "b"]
    # CSV types are inferred by pandas, never declared by the file.
    assert declared is None


def test_parquet_reader_declares_types(tmp_path):
    path = tmp_path / "d.parquet"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_parquet(path)

    df, declared = read_source({"kind": "parquet", "path": str(path)})

    assert len(df) == 2
    assert declared["a"].startswith("int")
    assert declared["b"] in ("object", "string", "str")


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="unknown source kind"):
        read_source({"kind": "carrier-pigeon", "path": "x"})


def test_missing_path_is_rejected():
    with pytest.raises(ValueError, match="requires 'path'"):
        read_source({"kind": "csv"})


def test_postgres_query_passthrough():
    assert _build_query({"query": "select 1"}) == "select 1"


def test_postgres_table_builds_select():
    assert _build_query({"table": "analytics_marts.fct_transactions"}) == (
        "select * from analytics_marts.fct_transactions"
    )


def test_postgres_limit_is_applied():
    assert _build_query({"table": "raw.transactions", "limit": 10}).endswith("limit 10")


def test_postgres_rejects_non_identifier_table():
    # A table name can't be a bound parameter, so it has to be validated
    # instead — this is the check that keeps it from reaching an f-string.
    with pytest.raises(ValueError, match="not a valid table identifier"):
        _build_query({"table": "raw.transactions; drop table raw.transactions"})


def test_postgres_rejects_both_query_and_table():
    with pytest.raises(ValueError, match="not both"):
        _build_query({"table": "raw.transactions", "query": "select 1"})


def test_postgres_requires_one_of_them():
    with pytest.raises(ValueError, match="requires 'query' or 'table'"):
        _build_query({})
