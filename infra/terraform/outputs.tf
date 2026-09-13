output "lake_bucket" {
  value = minio_s3_bucket.lake.bucket
}

output "warehouse_schemas" {
  value = sort(keys(postgresql_schema.layer))
}

output "dashboard_role" {
  value = postgresql_role.reader.name
}
