output "project_id" { value = var.project_id }
output "region" { value = var.region }
output "data_bucket" { value = google_storage_bucket.data.name }
output "scratch_bucket" { value = google_storage_bucket.scratch.name }
output "cloudsql_connection_name" { value = google_sql_database_instance.operational.connection_name }
output "cloudsql_database" { value = google_sql_database.operational.name }
output "service_accounts" { value = { for key, sa in google_service_account.worker : key => sa.email } }
output "secret_ids" { value = { for key, secret in google_secret_manager_secret.credential : key => secret.secret_id } }
output "artifact_repository" { value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}" }
output "spark_subnet" { value = google_compute_subnetwork.spark.id }
output "jobs" { value = { for key, job in google_cloud_run_v2_job.placeholder : key => job.name } }
