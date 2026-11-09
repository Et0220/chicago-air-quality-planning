# S13: Snowflake generates this identity when AQ_GCS_READ is created.
variable "snowflake_storage_service_account" {
  type        = string
  default     = null
  description = "STORAGE_GCP_SERVICE_ACCOUNT from DESC STORAGE INTEGRATION AQ_GCS_READ."
  validation {
    condition = var.snowflake_storage_service_account == null ? true : can(regex(
      "^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\\.gserviceaccount\\.com$",
      var.snowflake_storage_service_account
    ))
    error_message = "Supply the generated Google service-account email, not a user email."
  }
}

resource "google_project_iam_custom_role" "snowflake_reader" {
  count       = var.snowflake_storage_service_account == null ? 0 : 1
  role_id     = "airQualitySnowflakeReader"
  title       = "Air Quality Snowflake Reader"
  description = "Read-only external stage access; cannot purge or overwrite objects."
  permissions = ["storage.buckets.get", "storage.objects.get", "storage.objects.list"]
}

resource "google_storage_bucket_iam_member" "snowflake_reader" {
  count  = var.snowflake_storage_service_account == null ? 0 : 1
  bucket = google_storage_bucket.data.name
  role   = google_project_iam_custom_role.snowflake_reader[0].name
  member = "serviceAccount:${var.snowflake_storage_service_account}"
  condition {
    title       = "snowflake_curated_read"
    description = "Bucket metadata/listing plus object reads restricted to curated/."
    expression  = "resource.name == 'projects/_/buckets/${google_storage_bucket.data.name}' || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/curated/')"
  }
}
