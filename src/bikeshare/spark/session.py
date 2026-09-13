"""SparkSession wired to the MinIO lake (s3a://) and the Postgres warehouse (JDBC)."""

from __future__ import annotations

from pyspark.sql import SparkSession

from bikeshare.config import env


def build_spark(app_name: str) -> SparkSession:
    endpoint = env("S3_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName(app_name)
        .master(env("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.ui.showConsoleProgress", "false")
        # --- S3A -> MinIO
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", env("S3_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", env("S3_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # S3A magic committer: tasks upload directly, the job commit completes the
        # multipart uploads. No _temporary dirs and no renames on the object store.
        .config("spark.hadoop.fs.s3a.committer.name", "magic")
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        .config(
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
        )
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        .config(
            "spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
        )
        .getOrCreate()
    )
