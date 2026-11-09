resource "google_sql_database_instance" "operational" {
  name                = "aq-operational"
  database_version    = "POSTGRES_16"
  region              = var.region
  deletion_protection = true
  settings {
    edition               = "ENTERPRISE"
    tier                  = "db-f1-micro"
    availability_type     = "ZONAL"
    disk_type             = "PD_SSD"
    disk_size             = 10
    disk_autoresize       = false
    user_labels           = local.labels
    connector_enforcement = "REQUIRED"
    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }
    ip_configuration {
      ipv4_enabled = true
      ssl_mode     = "ENCRYPTED_ONLY"
      # No authorized networks: connections require the Cloud SQL connector.
    }
    backup_configuration {
      enabled                        = true
      start_time                     = "08:00"
      point_in_time_recovery_enabled = false
      backup_retention_settings { retained_backups = 3 }
    }
  }
  depends_on = [google_project_service.api]
}

resource "google_sql_database" "operational" {
  name     = "air_quality"
  instance = google_sql_database_instance.operational.name
}

resource "google_project_iam_member" "sql_connect" {
  for_each = local.db_workers
  project  = var.project_id
  role     = "roles/cloudsql.client"
  member   = "serviceAccount:${google_service_account.worker[each.key].email}"
}

resource "google_project_iam_member" "sql_login" {
  for_each = local.db_workers
  project  = var.project_id
  role     = "roles/cloudsql.instanceUser"
  member   = "serviceAccount:${google_service_account.worker[each.key].email}"
}

resource "google_sql_user" "worker" {
  for_each = local.db_workers
  name     = trimsuffix(google_service_account.worker[each.key].email, ".gserviceaccount.com")
  instance = google_sql_database_instance.operational.name
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}
