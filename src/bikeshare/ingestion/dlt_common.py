"""dlt destination shared by all pipelines: the Postgres warehouse."""

from __future__ import annotations

import os

import dlt

from bikeshare.config import pg_dsn

# Keep dlt's local working state inside the container (pipelines are re-runnable;
# incremental state is also stored in the destination and restored on demand).
os.environ.setdefault("DLT_DATA_DIR", "/tmp/dlt")


def postgres_destination():
    return dlt.destinations.postgres(credentials=pg_dsn())


def run_pipeline(name: str, dataset: str, data, **run_kwargs):
    pipeline = dlt.pipeline(
        pipeline_name=name,
        destination=postgres_destination(),
        dataset_name=dataset,
        progress="log",
    )
    info = pipeline.run(data, **run_kwargs)
    print(pipeline.last_trace.last_normalize_info.asstr())
    print(info)
    return info
