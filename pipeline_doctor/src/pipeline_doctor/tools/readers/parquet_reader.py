"""Parquet reader. Types are stored in the file, so they are reported as
declared rather than inferred."""

from __future__ import annotations

import pandas as pd

from pipeline_doctor.tools.readers import ReadResult, SourceSpec


def read_parquet_source(spec: SourceSpec) -> ReadResult:
    path = spec.get("path")
    if not path:
        raise ValueError("parquet source requires 'path'")
    # pandas needs pyarrow (or fastparquet) installed; the import error it
    # raises names the missing package clearly enough to pass through.
    df = pd.read_parquet(path, columns=spec.get("columns"))
    declared = {str(col): str(dtype) for col, dtype in df.dtypes.items()}
    return df, declared
