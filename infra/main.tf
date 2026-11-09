locals {
  labels = { project = "air-quality", managed_by = "terraform", segment = "s02" }
  bucket = coalesce(var.bucket_name, "${var.project_id}-air-quality")
  apis = toset([
    "serviceusage.googleapis.com", "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com", "iamcredentials.googleapis.com", "storage.googleapis.com",
    "secretmanager.googleapis.com", "sqladmin.googleapis.com",
    "artifactregistry.googleapis.com", "run.googleapis.com", "cloudscheduler.googleapis.com",
    "dataproc.googleapis.com", "compute.googleapis.com", "logging.googleapis.com",
    "monitoring.googleapis.com", "billingbudgets.googleapis.com", "cloudbilling.googleapis.com",
    "cloudbuild.googleapis.com",
  ])
  # Each worker can modify only its own object prefixes. Bucket-level matching
  # permits object listing; GCS cannot hide individual object names in a listing.
  write_prefixes = {
    epa       = ["raw/epa/", "raw/aqs/", "staging/epa/"]
    purpleair = ["raw/purpleair/"]
    airnow    = ["raw/airnow/"]
    sync      = ["staging/live/"]
    spark     = ["curated/epa_hourly/", "checks/s02/"]
  }
  db_workers = toset(["epa", "purpleair", "airnow", "sync"])
  secret_consumers = {
    "purpleair-read-key" = "purpleair"
    "airnow-api-key"     = "airnow"
    "snowflake-creds"    = "sync"
  }
}

data "google_project" "current" {
  project_id = var.project_id
}

resource "google_project_service" "api" {
  for_each           = local.apis
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "data" {
  name                        = local.bucket
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels
  versioning { enabled = true }
  depends_on = [google_project_service.api]
}

resource "google_storage_bucket" "scratch" {
  name                        = "${var.project_id}-aq-spark-scratch"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels
  lifecycle_rule {
    condition { age = 7 }
    action { type = "Delete" }
  }
  depends_on = [google_project_service.api]
}

resource "google_service_account" "worker" {
  for_each     = toset(concat(keys(local.write_prefixes), ["scheduler"]))
  account_id   = "aq-${each.key}"
  display_name = "Air quality ${each.key}"
  depends_on   = [google_project_service.api]
}

resource "google_storage_bucket_iam_member" "writer" {
  for_each = local.write_prefixes
  bucket   = google_storage_bucket.data.name
  role     = "roles/storage.objectUser"
  member   = "serviceAccount:${google_service_account.worker[each.key].email}"
  condition {
    title = "${each.key}-prefixes"
    expression = join(" || ", concat(
      ["resource.name == 'projects/_/buckets/${google_storage_bucket.data.name}'"],
      [for prefix in each.value : "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/${prefix}')"]
    ))
  }
}

resource "google_storage_bucket_iam_member" "spark_input" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker["spark"].email}"
  condition {
    title      = "spark-staged-input"
    expression = "resource.name == 'projects/_/buckets/${google_storage_bucket.data.name}' || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/staging/epa/')"
  }
}

resource "google_storage_bucket_iam_member" "spark_scratch" {
  bucket = google_storage_bucket.scratch.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.worker["spark"].email}"
}

resource "google_secret_manager_secret" "credential" {
  for_each  = local.secret_consumers
  secret_id = "aq-${each.key}"
  labels    = local.labels
  replication {
    user_managed {
      replicas { location = var.region }
    }
  }
  depends_on = [google_project_service.api]
}

# Only containers and IAM are managed here; secret payloads never enter state.
resource "google_secret_manager_secret_iam_member" "consumer" {
  for_each  = local.secret_consumers
  secret_id = google_secret_manager_secret.credential[each.key].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker[each.value].email}"
}

resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "air-quality"
  format        = "DOCKER"
  description   = "Air quality collector and serving images"
  labels        = local.labels
  depends_on    = [google_project_service.api]
}
