# S08: PurpleAir poller (3-day hourly test)

**Status: deployed 2026-11-09 UTC; hourly schedule enabled for a 72-hour test.**

**Follow-up, November 16 (Chicago time):** the trial finished at its cutoff.
All 71 eligible scheduled calls and three manual calls succeeded; raw records
reconcile to 9,672 inserted reports and 29 skipped duplicates in job logs.
The scheduler remains enabled but the stop guard prevents new sensor calls.
See [the complete review and proposed next campaign](collection_review.md).
The proposed 20-sensor ten-minute campaign has not been deployed.

The deployed trial image makes one `/v1/sensors` call per run for the whole
Chicago bounding box (outdoor sensors, 7 requested fields plus sensor ID), stores the response unchanged
under `raw/purpleair/dt=YYYY-MM-DD/hh=HH/`, upserts `pa_sensor`,
`pa_sensor_history` and `pa_reading`, and writes one `pa_poll_log` row.

**Prepared locally, not deployed:** the current source also accepts an exact
fixed roster with `--sensor-ids` / `AIR_QUALITY_PURPLEAIR_SENSOR_IDS`. Fixed-roster
requests use `show_only`, omit the discovery bounding box, and cap freshness at
180 seconds relative to response `time_stamp`. Missing/indoor status, invalid
humidity, negative/missing PM channels and provisional A/B disagreement are
flagged. Raw responses and flagged readings are retained; they do not count
toward accepted analytical coverage. Unexpected IDs fail before SQL writes.
Quality details are stored in `ingest_run.details.quality` and emitted as
structured Cloud Logging records. See [deployment and review steps](collection_campaign.md).
Rows with unknown indoor/outdoor status or future report timestamps stay raw-only,
with explicit flags, so they cannot overwrite sensor metadata/history with a
guessed status or advance the operational last-seen timestamp into the future.

## Test configuration

| Item | Value |
| --- | --- |
| Cloud Run job | `aq-purpleair-poll` (service account `aq-purpleair`, no retries) |
| Schedule | `0 * * * *` UTC, Cloud Scheduler `aq-purpleair-poll` |
| Stop time | `2026-11-12T07:00:00Z` (job exits without an API call after this) |
| Image | `purpleair-poll@sha256:3a9a8ae90fde05c4cbc5cf6cd7c3876b9b82fa4f445347e72aff6121e4a3e040` |
| Key | Secret Manager `aq-purpleair-read-key` (version 1), injected at start |
| Cost | about 1,053 points per call, about 76,000 points for 72 calls |

## Verified on the deployed job

| Run | Result |
| --- | --- |
| First manual execution | 131 sensors, 131 inserted, 0 duplicates, HTTP 200 |
| Second manual execution | 131 sensors, 102 inserted, 29 duplicates, 0 late |

The second run shows reruns do not create duplicate rows: sensors that had not
reported again were counted as duplicates (primary key `sensor_id, report_ts_utc`).
Hourly polling gives about one reading per sensor-hour, so S16 must define hourly
completeness for this cadence. This is a discovery test, not the final sensor set.

## Acceptance evidence (2026-11-09)

Source: Cloud Run execution list for `aq-purpleair-poll` and the raw responses
in GCS. Configured interval: 60 min.

| Check | Result |
| --- | --- |
| Expected polls | one per hour; 14 scheduled executions 08:00 to 21:00 UTC |
| Observed | 14 of 14 started within 1 to 40 s of the hour; all succeeded, none failed |
| Unattended | yes, Cloud Scheduler only; no manual runs between 08:00 and 21:00 |
| Duplicates | none across the 14 scheduled captures (no repeated sensor and report time). The 29 repeats seen overall come from two manual calls 93 s apart, and are the duplicates the poller counted and skipped |
| Sensors per poll | 130 to 131, 0 stale to 1 stale |
| Fix check | after the 21:29 deploy, a manual run succeeded: 131 sensors, 131 inserted, 0 duplicates, 0 late |

The first 13 scheduled captures (09:00 to 21:00) were the evidence cited at
validation; the 08:00 execution, the first Scheduler fire, also succeeded.
Duplicate counts here come from the raw responses and the poller's own logs;
the `pa_reading` table was not queried for this note, but its primary key
(`sensor_id, report_ts_utc`) rejects duplicates.

## Outage recovery (not exercised live)

The S08 acceptance asks for recovery from a logged simulated outage. This was
verified only in the PostgreSQL integration test
(`test_poll_dedupes_flags_late_tracks_history_and_recovers`): an HTTP 403 is
logged as `http_error` with no readings inserted, and the next poll succeeds.
It was **not** simulated on the deployed job: Cloud Run cannot override the
secret-injected `AIR_QUALITY_PURPLEAIR_READ_KEY` with a literal for one
execution. A live drill would need a throwaway secret holding an invalid key,
passed with `--update-secrets` for a single execution, then deleted.

## Overrides

`AIR_QUALITY_PURPLEAIR_MAX_AGE_SECONDS`, `..._STALE_AFTER_SECONDS` and
`..._TARGET_POLL_SECONDS` override `config/settings.yaml` (CLI flags win over
both). `TARGET_POLL_SECONDS` is only recorded in the poll details; the actual
cadence is the Cloud Scheduler cron (`0 * * * *`).

The new `purpleair_poll_schedule` Terraform variable can switch that cron to
`5,15,25,35,45,55 * * * *`. This requires a fixed roster and cutoff. Use
`AIR_QUALITY_PURPLEAIR_NOT_BEFORE_UTC` to prevent calls before activation and
`--run-kind preflight` (or `AIR_QUALITY_PURPLEAIR_RUN_KIND=preflight`) to exclude
manual checks from scheduled-window acceptance. The historical trial remains
the deployed behavior until the image is rebuilt and the reviewed plan applied.

## Operate

```sh
# Pause now (also set purpleair_poll_enabled = false in infra/terraform.tfvars):
gcloud scheduler jobs pause aq-purpleair-poll --location=us-central1
# Run once by hand:
gcloud run jobs execute aq-purpleair-poll --region=us-central1 --wait
```

Extend or end the test by changing `purpleair_poll_stop_after_utc` in the
ignored `infra/terraform.tfvars` and applying; do not edit the job with gcloud,
or the next Terraform apply will revert it.
