# Fixed-cohort collection: deployment and review

Status: source code and offline checks prepared; **no new image built or cloud
deployment performed in this review**. Python is 3.15. The source of the
20-device roster is [collection_strategy.yaml](../config/collection_strategy.yaml).
The former November 17 00:05 CST start is not an activation deadline.

## 1. Review the cohort and prerequisites

Read [the review](collection_review.md), including its coverage gaps and
[per-device replacement rules](../output/collection_review/replacement_plan.json).
The four checked backups serve only CHI_SP and Joliet, and two are exploratory
distance pairs. Other failed slots are held unless a new backup is qualified.
Confirm the December 10 06:00 UTC final cutoff is still appropriate before
starting; an already-expired cutoff must not be extended automatically.

From the repository root, prepare local evidence:

```sh
.venv/bin/python --version
.venv/bin/python -m analysis.collection_plan_review
.venv/bin/python -m pytest -q tests/test_purpleair_poll.py tests/test_collection_gate.py
make lint
```

The generated `output/collection_review/campaign.tfvars.json` contains exactly
the reviewed roster, ten-minute cadence, thresholds and cutoff. It **sets
`purpleair_poll_enabled=false`** and leaves activation time unset. It intentionally
does not supply an image digest: use the newly built image in step 3.

The following commands are an operator runbook, **not commands executed by this
review**. Keep credentials in local authentication/Secret Manager. If `gcloud`
reports expired interactive login, run `gcloud auth login` and complete its
browser sign-in; do not send credentials to chat.

## 2. Pause the old scheduler and build the new image

Use the existing job and scheduler; do not create a second recurring collector.
Set `purpleair_poll_enabled=false` in the ignored `infra/terraform.tfvars` as
well, so later applies cannot silently re-enable it.

```sh
campaign_project=final-team-514205
campaign_region=us-central1
campaign_bucket=final-team-514205-air-quality
campaign_tag="${campaign_region}-docker.pkg.dev/${campaign_project}/air-quality/purpleair-poll:cohort-$(date -u +%Y%m%d%H%M%S)"

gcloud scheduler jobs pause aq-purpleair-poll \
  --project="$campaign_project" --location="$campaign_region"
gcloud builds submit . --project="$campaign_project" \
  --config=ingestion/cloudbuild-purpleair.yaml \
  --ignore-file=.gcloudignore.purpleair \
  --substitutions="_IMAGE=$campaign_tag"
campaign_digest=$(gcloud artifacts docker images describe "$campaign_tag" \
  --project="$campaign_project" --format='value(image_summary.digest)')
campaign_image="${campaign_tag%:*}@${campaign_digest}"
```

The image uses Python 3.15. A failed build or absent digest is not a reason to
reuse the old bounding-box image with the new cron.

## 3. Apply a paused deployment and run one preflight

Use the installed Terraform CLI (the review used a checksum-verified 1.13.5
binary under `tmp/collection_review/terraform/terraform`). Inspect the complete
plan; resolve unrelated changes before applying.

```sh
mkdir -p tmp/campaign
terraform -chdir=infra plan \
  -var-file=../output/collection_review/campaign.tfvars.json \
  -var="purpleair_poll_image=$campaign_image" \
  -out=../tmp/campaign/paused.tfplan
terraform -chdir=infra apply ../tmp/campaign/paused.tfplan
```

Read the PurpleAir organization balance immediately before the single manual
execution, using the existing local key without displaying it:

```sh
.venv/bin/python - <<'PY'
import os
from ingestion import purpleair_check as pa
pa.load_env()
print(pa.remaining_points(os.environ[pa.KEY_VAR]))
PY
gcloud run jobs execute aq-purpleair-poll --project="$campaign_project" \
  --region="$campaign_region" \
  --update-env-vars=AIR_QUALITY_PURPLEAIR_RUN_KIND=preflight --wait
```

Repeat the balance-only command until billing settles. Expected consumption
is **165 points when all twenty rows return**; a smaller result can cost less
and must not be mistaken for a complete roster. The new cycle must have:

- Twenty expected IDs, no unexpected/missing IDs, and twenty valid IDs in
  the structured `purpleair_poll` event. A failed quality check is investigated
  before enabling the schedule. Do not automatically retry a potentially
  billable request.
- An unchanged raw response in GCS and a successful committed SQL poll record.
  New rows plus duplicates plus explicitly raw-only/unusable rows must account
  for all response rows. Unknown indoor/outdoor status and future timestamps
  are raw-only so they cannot corrupt metadata history. Raw QC flags
  stay visible in `ingest_run.details.quality`; SQL row insertion is not itself
  proof of an acceptable analytical observation.
- The expected image digest, fixed allowlist, 180-second client cap and
  `max_retries=0`. Verify runtime Python 3.15 in the built image if changed.

Using an existing authenticated operational database connection, substitute
the actual logged poll ID in these read-only queries:

```sql
SELECT p.poll_id, p.outcome, p.sensors_seen, p.rows_inserted,
       p.duplicate_reports, p.stale_reports, r.details
FROM pa_poll_log p JOIN ingest_run r USING (run_id)
WHERE p.poll_id = 'ACTUAL_PREFLIGHT_POLL_ID';
```

## 4. Enable at a future slot, then verify the first scheduled cycle

After all preflight checks pass, set the next feasible **future** :05/:15/:25/
:35/:45/:55 UTC slot as `purpleair_poll_not_before_utc`. Update the reviewed
overlay and ignored local tfvars with that activation time, the pinned image
and `purpleair_poll_enabled=true`; inspect and apply a new plan. Do not
regenerate the overlay afterward without reviewing it: generation deliberately
returns it to the paused state.

Verify the existing scheduler now has the ten-minute cron and the first
scheduled event is successful. Record the **first fully sampled UTC hour** as
`campaign_review_start`, for example `2026-11-18T07:00:00Z` if activation was
07:05 UTC. An activation halfway through an hour starts quality acceptance
at the following full hour; record partial-hour observations separately.
Recalculate the remaining duration and points at 23,760/day plus 10% allowance.
The final cutoff stays fixed. No automatic points purchase or quota expansion.

## 5. Evaluate the first 72 hours, then daily

Retrieve the new structured poll events and raw responses. These example
commands are read-only; the local data directories are ignored by Git.

```sh
campaign_review_start='ACTUAL_FIRST_FULL_HOUR_UTC'
gcloud logging read \
  "resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"aq-purpleair-poll\" AND jsonPayload.event=\"purpleair_poll\" AND timestamp>=\"${campaign_review_start}\"" \
  --project="$campaign_project" --limit=20000 --format=json \
  > tmp/campaign/polls.json
gcloud storage rsync --recursive \
  "gs://${campaign_bucket}/raw/purpleair" tmp/campaign/raw
.venv/bin/python -m analysis.collection_gate \
  --poll-log tmp/campaign/polls.json --raw-dir tmp/campaign/raw \
  --start-utc "$campaign_review_start" --hours 72 \
  --out output/collection_review/campaign_gate.json
```

Run at least five minutes after the window ends. Exit 0 = pass, 1 = review
required, 2 = window incomplete. The first gate requires **411/432** successful
scheduled slots, **411/432 usable reports per retained sensor**, and **65/72**
accepted hours per sensor. An accepted hour needs five distinct event-time
reports in five distinct ten-minute bins spanning at least 35 minutes, all
passing freshness, outdoor, humidity and provisional A/B checks. Missing calls
and hours count against the denominator. Preflights and duplicate log entries
do not improve coverage. Missing raw evidence or a changed roster prevents a
clean acceptance claim.

The gate includes separate ≤5 km and 5–10 km coverage summaries. Update the
strategy's `exploratory_sensor_ids` and frozen per-window roster if a replacement
changes distance class. These sensor-hour counts do not imply independent
spatial replication and must not be pooled into one validation score.

For daily reviews, use `--hours 24` and the completed day's UTC start: minimum
137/144 successful/usable calls and 22/24 accepted hours. Check both the JSON
details and exit status. A per-sensor three-poll unusable streak is reported
by the gate when invoked; **no live alert delivery service is deployed** by
this change. Review logs promptly when a missing/stale-device issue is seen.
The gate evaluates coverage/QC only. It does not calculate corrected PM2.5,
fit calibration, or replace the S16/S17 analyses.

If a device fails the gate, recheck its named backup before substituting it.
Apply one replacement per backup, maintain at most twenty active devices,
and log the effective time and reason. Hold slots with no qualified backup.
A >5 km replacement requires exploratory reclassification; never combine
primary and exploratory validation metrics. Freeze each reviewed roster and
evaluate changes in separate windows.

At 09:00 Chicago time also check organization balance and daily consumption.
The 610,000-point campaign cap, 250,000-point reserve and >28,512/day burn alert
are **operator-enforced budget rules**, not automated account billing controls.
Pause and investigate before breaching them. At December 10 06:00 UTC, pause
both PurpleAir and AirNow schedulers, record final balance/counts, and retain
raw versions plus the cohort-change log for the December 10 analysis day.
