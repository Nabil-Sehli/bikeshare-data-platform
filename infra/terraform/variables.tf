variable "pg_host" {
  type    = string
  default = "postgres"
}

variable "pg_port" {
  type    = number
  default = 5432
}

variable "pg_database" {
  type    = string
  default = "bikeshare"
}

variable "pg_admin_user" {
  type = string
}

variable "pg_admin_password" {
  type      = string
  sensitive = true
}

variable "pg_reader_user" {
  type    = string
  default = "dashboard_reader"
}

variable "pg_reader_password" {
  type      = string
  sensitive = true
}

variable "minio_server" {
  type    = string
  default = "minio:9000"
}

variable "minio_user" {
  type = string
}

variable "minio_password" {
  type      = string
  sensitive = true
}

variable "lake_bucket" {
  type    = string
  default = "bikeshare-lake"
}

variable "streaming_retention_days" {
  description = "Days to keep raw streaming Parquet files in the lake before MinIO expires them."
  type        = number
  default     = 30
}
