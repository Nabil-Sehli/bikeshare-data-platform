"""Lake -> warehouse: streaming Parquet files on MinIO -> raw_stream.station_status.

Uses dlt's filesystem source with an incremental cursor on the file
modification date, so each run only loads files the Kafka lake consumer wrote
since the previous run.

Usage:
    python -m bikeshare.ingestion.dlt_lake_status
"""

from __future__ import annotations

import dlt
from dlt.common.configuration.specs import AwsCredentials
from dlt.sources.filesystem import filesystem, read_parquet

from bikeshare.config import env, lake_bucket
from bikeshare.ingestion.dlt_common import run_pipeline


def lake_files():
    credentials = AwsCredentials(
        aws_access_key_id=env("S3_ACCESS_KEY"),
        aws_secret_access_key=env("S3_SECRET_KEY"),
        endpoint_url=env("S3_ENDPOINT", "http://minio:9000"),
        region_name="us-east-1",
    )
    files = filesystem(
        bucket_url=f"s3://{lake_bucket()}/streaming/station_status",
        credentials=credentials,
        file_glob="**/*.parquet",
    )
    files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    return (files | read_parquet()).with_name("station_status")


def main() -> None:
    run_pipeline("lake_station_status", "raw_stream", lake_files(), write_disposition="append")


if __name__ == "__main__":
    main()
