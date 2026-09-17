"""CSV reader - the original behaviour, kept as one source kind among several.

Returns declared_schema=None on purpose. pandas *infers* dtypes from the text
it finds, so the types it reports describe this run's values rather than the
column's real type. Reporting them as "declared" would make schema drift fire
on data that never changed shape.
"""

from __future__ import annotations

import pandas as pd

from pipeline_doctor.tools.readers import ReadResult, SourceSpec


def read_csv_source(spec: SourceSpec) -> ReadResult:
    path = spec.get("path")
    if not path:
        raise ValueError("csv source requires 'path'")
    return pd.read_csv(path), None
