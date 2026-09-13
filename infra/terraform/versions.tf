terraform {
  required_version = ">= 1.6"

  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "~> 3.42"
    }
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.27"
    }
  }
}

# Local-cloud equivalents of the GCP resources used in the Zoomcamp:
#   GCS bucket        -> MinIO bucket (S3 API)
#   BigQuery datasets -> Postgres schemas
#   IAM / service acc -> Postgres roles + grants
provider "minio" {
  minio_server   = var.minio_server
  minio_user     = var.minio_user
  minio_password = var.minio_password
  minio_ssl      = false
}

provider "postgresql" {
  host            = var.pg_host
  port            = var.pg_port
  database        = var.pg_database
  username        = var.pg_admin_user
  password        = var.pg_admin_password
  sslmode         = "disable"
  connect_timeout = 15
}
