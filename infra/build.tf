# S06: build identity can read staged source and push only to this repository.
resource "google_service_account" "image_builder" {
  account_id   = "aq-build"
  display_name = "Air quality container builder"
  depends_on   = [google_project_service.api]
}

resource "google_project_iam_member" "build_logs" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.image_builder.email}"
}

resource "google_artifact_registry_repository_iam_member" "build_push" {
  location   = var.region
  repository = google_artifact_registry_repository.images.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.image_builder.email}"
}

resource "google_storage_bucket_iam_member" "build_source" {
  bucket = google_storage_bucket.scratch.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.image_builder.email}"
  condition {
    title      = "build-source-only"
    expression = "resource.name == 'projects/_/buckets/${google_storage_bucket.scratch.name}' || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.scratch.name}/objects/build-source/')"
  }
}
