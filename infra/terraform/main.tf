locals {
  # Warehouse layers (the BigQuery "datasets" of this project)
  schemas = {
    raw          = "Spark-cleaned trips and station flows"
    raw_weather  = "dlt: Open-Meteo hourly weather"
    raw_gbfs     = "dlt: GBFS station reference data"
    raw_stream   = "dlt: streaming station status loaded from the lake"
    live         = "Kafka consumer: latest status per station"
    staging      = "dbt staging views"
    intermediate = "dbt intermediate models"
    marts        = "dbt marts consumed by the dashboard"
    bruin_raw    = "Bruin ingestion assets"
    bruin_mart   = "Bruin transformation assets"
  }

  # Schemas the read-only dashboard role may query
  reader_schemas = ["marts", "live", "bruin_mart"]
}

# ---------------------------------------------------------------- data lake
resource "minio_s3_bucket" "lake" {
  bucket        = var.lake_bucket
  force_destroy = true
}

resource "minio_s3_bucket_versioning" "lake" {
  bucket = minio_s3_bucket.lake.bucket

  versioning_configuration {
    status = "Suspended"
  }
}

resource "minio_ilm_policy" "lake" {
  bucket = minio_s3_bucket.lake.bucket

  rule {
    id         = "expire-raw-streaming-files"
    status     = "Enabled"
    filter     = "streaming/"
    expiration = "${var.streaming_retention_days}d"
  }
}

# ---------------------------------------------------------------- warehouse
resource "postgresql_schema" "layer" {
  for_each = local.schemas

  name     = each.key
  database = var.pg_database
  owner    = var.pg_admin_user
}

resource "postgresql_role" "reader" {
  name     = var.pg_reader_user
  login    = true
  password = var.pg_reader_password
}

resource "postgresql_grant" "reader_connect" {
  database    = var.pg_database
  role        = postgresql_role.reader.name
  object_type = "database"
  privileges  = ["CONNECT"]
}

resource "postgresql_grant" "reader_schema_usage" {
  for_each = toset(local.reader_schemas)

  database    = var.pg_database
  role        = postgresql_role.reader.name
  schema      = postgresql_schema.layer[each.key].name
  object_type = "schema"
  privileges  = ["USAGE"]
}

# Tables that already exist...
resource "postgresql_grant" "reader_tables" {
  for_each = toset(local.reader_schemas)

  database    = var.pg_database
  role        = postgresql_role.reader.name
  schema      = postgresql_schema.layer[each.key].name
  object_type = "table"
  privileges  = ["SELECT"]
}

# ...and tables dbt / consumers create later.
resource "postgresql_default_privileges" "reader_tables" {
  for_each = toset(local.reader_schemas)

  database    = var.pg_database
  role        = postgresql_role.reader.name
  schema      = postgresql_schema.layer[each.key].name
  owner       = var.pg_admin_user
  object_type = "table"
  privileges  = ["SELECT"]
}
