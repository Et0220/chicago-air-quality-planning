data "google_billing_account" "trial" {
  billing_account = var.billing_account_id
  lookup_projects = true
  depends_on      = [google_project_service.api["cloudbilling.googleapis.com"]]
}

resource "google_billing_budget" "project" {
  billing_account = var.billing_account_id
  display_name    = "Air quality monthly gross spend"
  budget_filter {
    projects               = ["projects/${data.google_project.current.number}"]
    calendar_period        = "MONTH"
    credit_types_treatment = "EXCLUDE_ALL_CREDITS"
  }
  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_usd)
    }
  }
  dynamic "threshold_rules" {
    for_each = [0.5, 0.9, 1.0]
    content {
      threshold_percent = threshold_rules.value
      spend_basis       = "CURRENT_SPEND"
    }
  }
  # Omitting all_updates_rule retains the default billing-admin/user emails.
  lifecycle {
    precondition {
      condition     = data.google_billing_account.trial.open && contains(data.google_billing_account.trial.project_ids, var.project_id)
      error_message = "The existing project must already be linked to the specified trial billing account."
    }
  }
  depends_on = [google_project_service.api]
}
