resource "google_compute_network" "spark" {
  name                    = "aq-spark"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.api]
}

resource "google_compute_subnetwork" "spark" {
  name                     = "aq-spark"
  region                   = var.region
  ip_cidr_range            = "10.42.0.0/24"
  network                  = google_compute_network.spark.id
  private_ip_google_access = true
}

resource "google_compute_firewall" "spark_internal" {
  name          = "aq-spark-internal"
  network       = google_compute_network.spark.name
  direction     = "INGRESS"
  source_ranges = [google_compute_subnetwork.spark.ip_cidr_range]
  target_tags   = ["aq-spark"]
  allow { protocol = "all" }
}

data "google_iam_role" "dataproc_worker" {
  name = "roles/dataproc.worker"
}

# The predefined Worker role grants project-wide object writes. Preserve its
# compute/logging permissions while granting storage separately per bucket.
resource "google_project_iam_custom_role" "spark_worker" {
  role_id = "airQualitySparkWorker"
  title   = "Air quality Spark worker without project storage access"
  permissions = [
    for permission in data.google_iam_role.dataproc_worker.included_permissions :
    permission if !startswith(permission, "storage.")
  ]
  depends_on = [google_project_service.api]
}

resource "google_project_iam_member" "spark_worker" {
  project = var.project_id
  role    = google_project_iam_custom_role.spark_worker.name
  member  = "serviceAccount:${google_service_account.worker["spark"].email}"
}

resource "google_storage_bucket_iam_member" "spark_bucket_metadata" {
  for_each = { data = google_storage_bucket.data.name, scratch = google_storage_bucket.scratch.name }
  bucket   = each.value
  role     = "roles/storage.legacyBucketReader"
  member   = "serviceAccount:${google_service_account.worker["spark"].email}"
}

resource "google_compute_subnetwork_iam_member" "spark_network" {
  subnetwork = google_compute_subnetwork.spark.name
  region     = var.region
  role       = "roles/compute.networkUser"
  member     = "serviceAccount:${google_service_account.worker["spark"].email}"
}

resource "google_project_iam_custom_role" "spark_submit" {
  role_id = "airQualitySparkSubmitter"
  title   = "Air quality Spark batch submitter"
  permissions = [
    "dataproc.batches.create", "dataproc.batches.get", "dataproc.batches.list",
    "dataproc.batches.cancel", "dataproc.operations.get", "dataproc.operations.cancel",
    "serviceusage.services.use",
  ]
  depends_on = [google_project_service.api]
}

resource "google_project_iam_member" "spark_submit" {
  for_each = var.spark_submitter_members
  project  = var.project_id
  role     = google_project_iam_custom_role.spark_submit.name
  member   = each.value
}

resource "google_service_account_iam_member" "spark_act_as" {
  for_each           = var.spark_submitter_members
  service_account_id = google_service_account.worker["spark"].name
  role               = "roles/iam.serviceAccountUser"
  member             = each.value
}

resource "google_storage_bucket_iam_member" "spark_submit_upload" {
  for_each = var.spark_submitter_members
  bucket   = google_storage_bucket.scratch.name
  role     = "roles/storage.objectUser"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "spark_submit_metadata" {
  for_each = var.spark_submitter_members
  bucket   = google_storage_bucket.scratch.name
  role     = "roles/storage.legacyBucketReader"
  member   = each.value
}

resource "google_compute_subnetwork_iam_member" "spark_submit_network" {
  for_each   = var.spark_submitter_members
  subnetwork = google_compute_subnetwork.spark.name
  region     = var.region
  role       = "roles/compute.networkUser"
  member     = each.value
}
