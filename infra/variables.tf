variable "project_id" {
  description = "Existing GCP project attached to the team's free trial. No project or billing account is created."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "Supply a valid existing GCP project ID."
  }
}

variable "billing_account_id" {
  description = "Existing trial billing account ID, used only to create a project-filtered budget."
  type        = string
  validation {
    condition     = can(regex("^[A-Fa-f0-9]{6}-[A-Fa-f0-9]{6}-[A-Fa-f0-9]{6}$", var.billing_account_id))
    error_message = "Use the billing account ID format XXXXXX-XXXXXX-XXXXXX."
  }
}

variable "trial_confirmed" {
  description = "Set true only after confirming available trial credits and expiry through submission. Never upgrade to paid billing."
  type        = bool
  default     = false
  validation {
    condition     = var.trial_confirmed
    error_message = "The course requires an eligible free trial with available credits. Confirm that before planning cloud resources."
  }
}

variable "region" {
  description = "Keep storage and compute in one region. The documented estimate assumes us-central1."
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Optional globally unique data bucket name; otherwise <project_id>-air-quality."
  type        = string
  default     = null
}

variable "monthly_budget_usd" {
  description = "Gross monthly project spend alert before credits; an alert is not a spending cap."
  type        = number
  default     = 50
  validation {
    condition     = var.monthly_budget_usd > 0 && floor(var.monthly_budget_usd) == var.monthly_budget_usd
    error_message = "Budget must be a positive whole number of US dollars."
  }
}

variable "spark_submitter_members" {
  description = "IAM principals allowed to submit smoke batches using the Spark worker, e.g. user:name@example.com."
  type        = set(string)
  default     = []
}

variable "epa_download_image" {
  description = "S06 image pinned by sha256 digest; null keeps the S02 placeholder."
  type        = string
  default     = null
  validation {
    condition     = var.epa_download_image == null ? true : can(regex("@sha256:[a-f0-9]{64}$", var.epa_download_image))
    error_message = "Pin the deployed EPA downloader to an immutable image digest."
  }
}

variable "airnow_collect_image" {
  description = "S09 image pinned by sha256 digest; null keeps the S02 placeholder."
  type        = string
  default     = null
  validation {
    condition     = var.airnow_collect_image == null ? true : can(regex("@sha256:[a-f0-9]{64}$", var.airnow_collect_image))
    error_message = "Pin the deployed AirNow collector to an immutable image digest."
  }
}

variable "airnow_schedule_enabled" {
  description = "Enable the hourly S09 collector after its backfill and rerun pass."
  type        = bool
  default     = false
  validation {
    condition     = !var.airnow_schedule_enabled || var.airnow_collect_image != null
    error_message = "Deploy the AirNow collector image before enabling its schedule."
  }
}

variable "epa_stage_image" {
  description = "S10 image pinned by sha256 digest; null keeps the S02 placeholder."
  type        = string
  default     = null
  validation {
    condition     = var.epa_stage_image == null ? true : can(regex("@sha256:[a-f0-9]{64}$", var.epa_stage_image))
    error_message = "Pin the deployed EPA stager to an immutable image digest."
  }
}

variable "purpleair_poll_image" {
  description = "S08 image pinned by sha256 digest; null keeps the S02 placeholder."
  type        = string
  default     = null
  validation {
    condition     = var.purpleair_poll_image == null ? true : can(regex("@sha256:[a-f0-9]{64}$", var.purpleair_poll_image))
    error_message = "Pin the deployed PurpleAir poller to an immutable image digest."
  }
}

variable "purpleair_poll_enabled" {
  description = "Unpause the hourly PurpleAir schedule. Requires purpleair_poll_image."
  type        = bool
  default     = false
  validation {
    condition     = !var.purpleair_poll_enabled || var.purpleair_poll_image != null
    error_message = "Set purpleair_poll_image before enabling the PurpleAir schedule."
  }
}

variable "purpleair_poll_stop_after_utc" {
  description = "ISO 8601 cutoff with Z or offset; the job exits without an API call after it."
  type        = string
  default     = null
  validation {
    condition     = var.purpleair_poll_stop_after_utc == null ? true : can(regex("(Z|[+-][0-9]{2}:[0-9]{2})$", var.purpleair_poll_stop_after_utc))
    error_message = "The stop time must include Z or a UTC offset."
  }
}
