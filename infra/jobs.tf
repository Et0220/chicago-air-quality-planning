locals {
  jobs = {
    "epa-download"   = "epa"
    "epa-stage"      = "epa"
    "purpleair-poll" = "purpleair"
    "airnow-collect" = "airnow"
    "live-sync"      = "sync"
  }
  # Deployed images; null keeps the S02 placeholder for that job.
  images = {
    "epa-download"   = var.epa_download_image
    "epa-stage"      = var.epa_stage_image
    "purpleair-poll" = var.purpleair_poll_image
  }
  deployed = { for k, _ in local.jobs : k => lookup(local.images, k, null) != null }
  commands = {
    "epa-download"   = ["python", "-m", "ingestion.epa_download"]
    "epa-stage"      = ["python", "-m", "ingestion.epa_stage"]
    "purpleair-poll" = ["python", "-m", "ingestion.purpleair_poll"]
  }
  command_args = {
    "epa-download"   = []
    "epa-stage"      = []
    "purpleair-poll" = ["--db", "--raw-store", "gcs"]
  }
  schedule_enabled = {
    "purpleair-poll" = var.purpleair_poll_enabled
  }
  schedules = {
    "purpleair-poll" = "0 * * * *" # hourly, on the hour (UTC)
    "airnow-collect" = "40 * * * *"
    "live-sync"      = "*/5 * * * *"
  }
}

resource "google_cloud_run_v2_job" "placeholder" {
  for_each            = local.jobs
  name                = "aq-${each.key}"
  location            = var.region
  deletion_protection = false
  labels              = local.labels
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.worker[each.value].email
      timeout         = each.key == "epa-stage" && local.deployed[each.key] ? "14400s" : each.key == "epa-download" && local.deployed[each.key] ? "7200s" : "300s"
      max_retries     = contains(["epa-download", "epa-stage"], each.key) && local.deployed[each.key] ? 1 : 0
      containers {
        image   = local.deployed[each.key] ? local.images[each.key] : "docker.io/library/python:3.15-slim"
        command = local.deployed[each.key] ? local.commands[each.key] : ["python"]
        args    = local.deployed[each.key] ? local.command_args[each.key] : ["-c", "print('S02 placeholder: ${each.key}; replace image and command in its owning segment')"]
        resources { limits = { cpu = each.key == "epa-stage" && local.deployed[each.key] ? "2" : "1", memory = each.key == "epa-stage" && local.deployed[each.key] ? "2Gi" : each.key == "epa-download" && local.deployed[each.key] ? "1Gi" : "512Mi" } }
        env {
          name  = "AIR_QUALITY_GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "AIR_QUALITY_GCS_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "AIR_QUALITY_CLOUDSQL_CONNECTION_NAME"
          value = google_sql_database_instance.operational.connection_name
        }
        dynamic "env" {
          for_each = local.deployed[each.key] ? {
            AIR_QUALITY_CLOUDSQL_IAM_USER = trimsuffix(google_service_account.worker[each.value].email, ".gserviceaccount.com")
            AIR_QUALITY_CLOUDSQL_DB       = google_sql_database.operational.name
          } : {}
          content {
            name  = env.key
            value = env.value
          }
        }
        # S08: the read key stays in Secret Manager and is injected at start-up.
        dynamic "env" {
          for_each = each.key == "purpleair-poll" && local.deployed[each.key] ? [1] : []
          content {
            name = "AIR_QUALITY_PURPLEAIR_READ_KEY"
            value_source {
              secret_key_ref {
                secret  = google_secret_manager_secret.credential["purpleair-read-key"].secret_id
                version = "latest"
              }
            }
          }
        }
        dynamic "env" {
          for_each = each.key == "purpleair-poll" && local.deployed[each.key] && var.purpleair_poll_stop_after_utc != null ? [var.purpleair_poll_stop_after_utc] : []
          content {
            name  = "AIR_QUALITY_PURPLEAIR_STOP_AFTER_UTC"
            value = env.value
          }
        }
        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }
      }
      volumes {
        name = "cloudsql"
        cloud_sql_instance { instances = [google_sql_database_instance.operational.connection_name] }
      }
    }
  }
  depends_on = [google_project_iam_member.sql_connect, google_project_iam_member.sql_login]
}

resource "google_cloud_run_v2_job_iam_member" "scheduler" {
  for_each = local.schedules
  location = var.region
  name     = google_cloud_run_v2_job.placeholder[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.worker["scheduler"].email}"
}

resource "google_cloud_scheduler_job" "collector" {
  for_each         = local.schedules
  name             = "aq-${each.key}"
  region           = var.region
  schedule         = each.value
  time_zone        = "Etc/UTC"
  paused           = !lookup(local.schedule_enabled, each.key, false)
  attempt_deadline = "180s"
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.placeholder[each.key].name}:run"
    headers     = { "Content-Type" = "application/json" }
    body        = base64encode("{}")
    oauth_token {
      service_account_email = google_service_account.worker["scheduler"].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
  depends_on = [google_cloud_run_v2_job_iam_member.scheduler]
}
