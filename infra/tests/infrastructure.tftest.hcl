# These use a simulated provider: no cloud authentication or resources.
mock_provider "google" {
  mock_data "google_project" {
    defaults = { number = "123456789012", billing_account = "ABCDEF-123456-ABCDEF" }
  }
  mock_data "google_billing_account" {
    defaults = { open = true, project_ids = ["air-quality-test"] }
  }
  mock_data "google_iam_role" {
    defaults = { included_permissions = ["dataproc.agents.create", "logging.logEntries.create", "storage.objects.delete", "storage.buckets.get"] }
  }
}

run "snowflake_read_only_stage" {
  command = plan
  variables {
    snowflake_storage_service_account = "snowflake-stage@example-project.iam.gserviceaccount.com"
  }
  assert {
    condition = (
      toset(google_project_iam_custom_role.snowflake_reader[0].permissions) ==
      toset(["storage.buckets.get", "storage.objects.get", "storage.objects.list"])
    )
    error_message = "The Snowflake stage may read only; purge and writes are forbidden."
  }
  assert {
    condition = (
      google_storage_bucket_iam_member.snowflake_reader[0].bucket == google_storage_bucket.data.name &&
      strcontains(google_storage_bucket_iam_member.snowflake_reader[0].condition[0].expression, "/objects/curated/")
    )
    error_message = "Snowflake object reads must be restricted to curated data."
  }
}

override_resource {
  target = google_service_account.worker["spark"]
  values = {
    name  = "projects/air-quality-test/serviceAccounts/aq-spark@air-quality-test.iam.gserviceaccount.com"
    email = "aq-spark@air-quality-test.iam.gserviceaccount.com"
  }
}

variables {
  project_id              = "air-quality-test"
  billing_account_id      = "ABCDEF-123456-ABCDEF"
  trial_confirmed         = true
  region                  = "us-central1"
  bucket_name             = null
  monthly_budget_usd      = 50
  spark_submitter_members = ["user:operator@example.com"]
  epa_download_image      = null
  airnow_collect_image    = null
  airnow_schedule_enabled = false
  epa_stage_image         = null
  purpleair_poll_image    = null
  purpleair_poll_enabled  = false
}

run "default_infrastructure" {
  command = apply
  assert {
    condition     = length(google_storage_bucket_iam_member.snowflake_reader) == 0
    error_message = "Snowflake access must wait for its actual generated service identity."
  }
  assert {
    condition = (
      google_artifact_registry_repository_iam_member.spark_image_reader.role == "roles/artifactregistry.reader" &&
      google_artifact_registry_repository_iam_member.spark_image_reader.repository == google_artifact_registry_repository.images.repository_id &&
      google_artifact_registry_repository_iam_member.spark_image_reader.member == "serviceAccount:${google_service_account.worker["spark"].email}"
    )
    error_message = "Spark needs repository-scoped read access for its Python 3.15 image."
  }
  assert {
    condition = alltrue([
      for permission in google_project_iam_custom_role.spark_worker.permissions :
      !startswith(permission, "storage.")
    ]) && contains(google_project_iam_custom_role.spark_worker.permissions, "dataproc.agents.create")
    error_message = "The Spark project role must not bypass bucket-scoped storage grants."
  }
  assert {
    condition = (
      google_storage_bucket.data.versioning[0].enabled &&
      google_storage_bucket.data.uniform_bucket_level_access &&
      google_storage_bucket.data.public_access_prevention == "enforced" &&
      !google_storage_bucket.data.force_destroy
    )
    error_message = "Source storage must retain versions, reject public access, and resist accidental deletion."
  }
  assert {
    condition = (
      google_sql_database_instance.operational.settings[0].tier == "db-f1-micro" &&
      google_sql_database_instance.operational.settings[0].disk_size == 10 &&
      !google_sql_database_instance.operational.settings[0].disk_autoresize &&
      google_sql_database_instance.operational.settings[0].connector_enforcement == "REQUIRED" &&
      length(google_sql_database_instance.operational.settings[0].ip_configuration[0].authorized_networks) == 0
    )
    error_message = "SQL must use the small fixed-size tier with connector-only access."
  }
  assert {
    condition = alltrue([
      for schedule in google_cloud_scheduler_job.collector :
      schedule.paused && schedule.time_zone == "Etc/UTC" &&
      schedule.http_target[0].oauth_token[0].service_account_email == google_service_account.worker["scheduler"].email
    ])
    error_message = "Placeholder schedules must be paused and authenticate as the scheduler identity."
  }
  assert {
    condition = alltrue([
      for key, consumer in google_secret_manager_secret_iam_member.consumer :
      consumer.member == "serviceAccount:${google_service_account.worker[local.secret_consumers[key]].email}"
    ]) && length(google_secret_manager_secret.credential) == 3
    error_message = "Each source credential must have only its intended worker grant."
  }
  assert {
    condition = alltrue([
      for secret in google_secret_manager_secret.credential :
      secret.replication[0].user_managed[0].replicas[0].location == var.region
    ])
    error_message = "Secret replicas must stay in the selected region."
  }
  assert {
    condition = (
      google_compute_subnetwork.spark.private_ip_google_access &&
      google_compute_firewall.spark_internal.source_ranges == toset(["10.42.0.0/24"]) &&
      google_compute_firewall.spark_internal.target_tags == toset(["aq-spark"])
    )
    error_message = "Spark must reach Google APIs and accept internal traffic only within its subnet."
  }
  assert {
    condition = (
      google_billing_budget.project.budget_filter[0].projects == toset(["projects/123456789012"]) &&
      google_billing_budget.project.budget_filter[0].credit_types_treatment == "EXCLUDE_ALL_CREDITS" &&
      length(google_billing_budget.project.all_updates_rule) == 0
    )
    error_message = "The budget must count this project's gross spend and notify billing recipients."
  }
  assert {
    condition = alltrue([
      for key, writer in google_storage_bucket_iam_member.writer :
      writer.condition[0].expression == join(" || ", concat(
        ["resource.name == 'projects/_/buckets/${google_storage_bucket.data.name}'"],
        [for prefix in local.write_prefixes[key] : "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.data.name}/objects/${prefix}')"]
      ))
    ])
    error_message = "Writes must remain confined to worker prefixes."
  }
}

run "trial_required" {
  command = plan
  variables { trial_confirmed = false }
  expect_failures = [var.trial_confirmed]
}

run "airnow_hourly_deployed" {
  command = apply
  variables {
    airnow_collect_image    = "us-central1-docker.pkg.dev/air-quality-test/air-quality/airnow-collect@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    airnow_schedule_enabled = true
  }
  assert {
    condition = (
      google_cloud_run_v2_job.placeholder["airnow-collect"].template[0].template[0].containers[0].image == var.airnow_collect_image &&
      google_cloud_run_v2_job.placeholder["airnow-collect"].template[0].template[0].timeout == "1800s" &&
      google_cloud_run_v2_job.placeholder["airnow-collect"].template[0].template[0].max_retries == 1 &&
      google_cloud_run_v2_job.placeholder["airnow-collect"].template[0].template[0].service_account == google_service_account.worker["airnow"].email &&
      !google_cloud_scheduler_job.collector["airnow-collect"].paused &&
      google_cloud_scheduler_job.collector["airnow-collect"].schedule == "40 * * * *" &&
      google_cloud_scheduler_job.collector["live-sync"].paused
    )
    error_message = "S09 must run the pinned image hourly as its own IAM identity."
  }
  assert {
    condition = alltrue([
      for e in google_cloud_run_v2_job.placeholder["airnow-collect"].template[0].template[0].containers[0].env :
      e.name != "AIR_QUALITY_CLOUDSQL_IAM_USER" || e.value == trimsuffix(google_service_account.worker["airnow"].email, ".gserviceaccount.com")
    ])
    error_message = "AirNow must use its dedicated SQL IAM account."
  }
}

run "airnow_enable_requires_image" {
  command = plan
  variables { airnow_schedule_enabled = true }
  expect_failures = [var.airnow_schedule_enabled]
}

run "airnow_image_must_be_pinned" {
  command = plan
  variables { airnow_collect_image = "us-central1-docker.pkg.dev/air-quality-test/air-quality/airnow-collect:latest" }
  expect_failures = [var.airnow_collect_image]
}

run "epa_download_deployed" {
  command = apply
  variables {
    epa_download_image = "us-central1-docker.pkg.dev/air-quality-test/air-quality/epa-download@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
  assert {
    condition = (
      google_cloud_run_v2_job.placeholder["epa-download"].template[0].template[0].containers[0].image == var.epa_download_image &&
      google_cloud_run_v2_job.placeholder["epa-download"].template[0].template[0].timeout == "7200s" &&
      google_cloud_run_v2_job.placeholder["epa-download"].template[0].parallelism == 1 &&
      google_cloud_run_v2_job.placeholder["epa-download"].template[0].template[0].service_account == google_service_account.worker["epa"].email &&
      google_cloud_run_v2_job.placeholder["epa-stage"].template[0].template[0].containers[0].image == "docker.io/library/python:3.15-slim"
    )
    error_message = "S06 must deploy the pinned image using the EPA worker and keep unrelated placeholders unchanged."
  }
}

run "epa_image_must_be_pinned" {
  command = plan
  variables { epa_download_image = "us-central1-docker.pkg.dev/air-quality-test/air-quality/epa-download:latest" }
  expect_failures = [var.epa_download_image]
}

run "epa_stage_deployed" {
  command = apply
  variables {
    epa_stage_image = "us-central1-docker.pkg.dev/air-quality-test/air-quality/epa-stage@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  assert {
    condition = (
      google_cloud_run_v2_job.placeholder["epa-stage"].template[0].template[0].containers[0].image == var.epa_stage_image &&
      google_cloud_run_v2_job.placeholder["epa-stage"].template[0].template[0].timeout == "14400s" &&
      google_cloud_run_v2_job.placeholder["epa-stage"].template[0].template[0].containers[0].resources[0].limits["cpu"] == "2" &&
      google_cloud_run_v2_job.placeholder["epa-stage"].template[0].template[0].service_account == google_service_account.worker["epa"].email
    )
    error_message = "S10 must deploy a pinned stage image with the EPA worker and a four-hour timeout."
  }
}

run "epa_stage_image_must_be_pinned" {
  command = plan
  variables { epa_stage_image = "us-central1-docker.pkg.dev/air-quality-test/air-quality/epa-stage:latest" }
  expect_failures = [var.epa_stage_image]
}

run "purpleair_poll_deployed_hourly" {
  command = apply
  variables {
    purpleair_poll_image          = "us-central1-docker.pkg.dev/air-quality-test/air-quality/purpleair-poll@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    purpleair_poll_enabled        = true
    purpleair_poll_stop_after_utc = "2026-11-12T06:00:00Z"
  }
  assert {
    condition = (
      google_cloud_run_v2_job.placeholder["purpleair-poll"].template[0].template[0].containers[0].image == var.purpleair_poll_image &&
      google_cloud_run_v2_job.placeholder["purpleair-poll"].template[0].template[0].max_retries == 0 &&
      google_cloud_run_v2_job.placeholder["purpleair-poll"].template[0].template[0].service_account == google_service_account.worker["purpleair"].email &&
      google_cloud_run_v2_job.placeholder["airnow-collect"].template[0].template[0].containers[0].image == "docker.io/library/python:3.15-slim"
    )
    error_message = "S08 must deploy the pinned image with the PurpleAir worker, no retries, and leave other placeholders unchanged."
  }
  assert {
    condition = (
      google_cloud_scheduler_job.collector["purpleair-poll"].schedule == "0 * * * *" &&
      !google_cloud_scheduler_job.collector["purpleair-poll"].paused &&
      google_cloud_scheduler_job.collector["airnow-collect"].paused &&
      google_cloud_scheduler_job.collector["live-sync"].paused
    )
    error_message = "Only the PurpleAir schedule may be unpaused, and it must run hourly."
  }
  assert {
    condition = anytrue([
      for e in google_cloud_run_v2_job.placeholder["purpleair-poll"].template[0].template[0].containers[0].env :
      e.name == "AIR_QUALITY_PURPLEAIR_READ_KEY" && length(e.value_source) == 1 && e.value == null
    ])
    error_message = "The PurpleAir key must come from Secret Manager, never a literal value."
  }
}

run "purpleair_enable_requires_image" {
  command = plan
  variables { purpleair_poll_enabled = true }
  expect_failures = [var.purpleair_poll_enabled]
}

run "purpleair_image_must_be_pinned" {
  command = plan
  variables { purpleair_poll_image = "us-central1-docker.pkg.dev/air-quality-test/air-quality/purpleair-poll:latest" }
  expect_failures = [var.purpleair_poll_image]
}

run "budget_must_be_positive" {
  command = plan
  variables { monthly_budget_usd = 0 }
  expect_failures = [var.monthly_budget_usd]
}

run "billing_account_must_match" {
  command = plan
  variables { billing_account_id = "000000-000000-000000" }
  override_data {
    target = data.google_billing_account.trial
    values = { open = true, project_ids = [] }
  }
  expect_failures = [google_billing_budget.project]
}
