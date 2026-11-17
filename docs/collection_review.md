# Three-day collection review and next campaign

Reviewed on November 16, 2026, Chicago time. The next campaign below is
**proposed, not deployed**. Python remains 3.15.

## What the three-day trial achieved

The configured trial window was November 9 07:00 UTC to November 12 07:00 UTC
(01:00 to 01:00 Chicago time). The first scheduler fire after deployment was
November 9 08:00 UTC; the last eligible fire was November 12 06:00 UTC.
There were therefore **71 eligible scheduled calls**, plus three documented
manual calls, rather than 72 scheduled calls.

| Measure | Observed result |
| --- | --- |
| Scheduled captures | **71 / 71**, no missing or duplicate scheduled hour |
| Total successful captures | 74, including 3 manual checks |
| Sensors | 134 distinct; 128–134 returned per capture |
| Raw sensor rows | 9,701 |
| Distinct sensor/report-time records | **9,672** |
| Repeated reports | 29, all from the closely spaced manual checks; logs show they were skipped |
| Late reports | 0 in the successful poll logs |
| Response age | Median snapshot age 64 s; response-time age p95 140 s |
| Stale by the original 600 s rule | 8 rows |
| Missing humidity | 709 rows, **7.31%**; nine sensors always lacked humidity |
| Missing channel A / B | 74 / 0 rows |
| Provisional stricter QC | 7,567 / 9,701 rows (78.0%) pass; 72 sensors pass the historical selection rule |
| Poll request delay after the hour | Median 110 s, maximum 210 s |
| Readings belonging to the previous UTC hour | **2,650 / 9,308 scheduled rows (28.47%)** |
| Estimated trial API cost | **77,978 points**, using measured field cost and actual row counts |

The raw deduplication count exactly matches the Cloud Run successful-commit
summaries: 9,672 inserted, 29 duplicates. Evidence is GCS raw generations plus
Cloud Run logs, **not a fresh direct Cloud SQL table query**. The existing
read-only sync identity could not be impersonated; no IAM grant was changed
just to bypass that limitation.

Nine always-missing-humidity sensors are `132993, 175419, 185093, 185283,
193676, 193801, 193807, 226529, 249451`. Another sensor, `169713`, lacked it in
43 captures. The proposed correction-ready cohort excludes these. The
provisional A/B screen flags 1,511 rows; these flags overlap missing-field
flags and must not be summed as independent rejection categories.

**Interpretation:** the unattended collection and deduplication worked. The
trial is useful for availability, geographic selection, and quality screening.
One snapshot per hour does not establish a continuous hourly mean. In
particular, assigning the request hour would mislabel 28.47% of scheduled
readings. The next campaign must use `last_seen` for event time.

## AirNow overlap and current operation

For the exact 72-hour trial window, the latest saved revisions cover **all 72
hours**, with **2,401 observation rows across 24 sites**. These include **1,510
PM2.5 rows across 21 PM2.5 sites**. Nineteen PM2.5 sites in the seven project
counties have at least 65 valid nonnegative hourly concentrations. The cohort
below is paired only to those eligible sites; Ogden Dunes in Porter County is
excluded from the final study cohort.

AirNow has continued running after the PurpleAir trial. At inventory time the
bucket contained 1,331 raw AirNow versions (about 1.33 GB), including observations
through November 17 02:00 UTC. Revisions are retained by content hash.

PurpleAir stopped making sensor requests at the configured November 12 07:00
UTC cutoff. The scheduler is still ENABLED, however: **117 later executions
only printed the stop message**. They did not consume PurpleAir sensor points.
The old schedule should be replaced as part of the next deployment; disabling
an expired schedule also removes unnecessary empty Cloud Run executions.

## Specific recommendation

Use **20 fixed outdoor sensors every 10 minutes**, at UTC minutes
**05, 15, 25, 35, 45, 55**. Keep all eight returned columns: sensor ID,
last_seen, latitude, longitude, location type, PM2.5 cf_1 A/B, and humidity.
Keeping location fields on every call preserves movement/outdoor history
without adding a separate metadata collector.

Use one `/v1/sensors` request with the exact `show_only` allowlist. Request
`max_age=300`, and independently require response-time age at most **180 s**
for analytical use. A current check found sensor `279109` almost 23 hours
stale **despite an explicit-ID request with max_age=300**; it was replaced.
All 20 final sensors passed the current checks. Never infer online status
merely from an ID appearing in the response.

The three-day evidence supports initial selection only; it is **not a forecast
of 23-day reliability**. Weather, humidity and device availability may change.
The first 72-hour gate and daily reviews below are required to retain devices.

Selection rules were fixed before ranking: at least 95% presence in the 71
scheduled calls, at least 95% provisional valid-row rate, no position shift
over 200 m, and an eligible nearby AirNow reference. Cover distinct reference
sites first, then add a second sensor per site. The final 20 all appeared in
**71/71** scheduled captures, with trial QC pass rates at least **95.8%**.
They cover **14 reference sites** and yielded **1,245 paired QC sensor-hours**
in the trial. Seventeen are within 5 km; three at 5–7.89 km are exploratory
spatial comparisons. These distances support neighborhood comparisons, not
claims of instrument collocation or a newly validated calibration model.

The actual reference coverage is concentrated in Cook, DuPage, Will, Kane,
and Lake County IN. This does not claim balanced coverage of all seven study
counties; Lake County IL and McHenry need additional eligible pairs if venues
there become mandatory.

### Follow-up review: coverage versus redundancy

The 20 IDs are retained for a **reference-neighborhood longitudinal study**.
The current evidence does not support a balanced seven-county claim. Selection
already covers distinct nearest eligible references before filling second
sensors; unused reference names do not necessarily have a usable nearby device.

There are five uncovered eligible references: **CARY, Elgin_DMV, Summit,
Kingery Near-road #1, and Hammond-167th St**. Gary-IITRI has only **61/72** valid
hours and does not meet the 65-hour reference threshold; CARY has **72/72**.

| Uncovered reference | What the saved sensor data allow |
| --- | --- |
| CARY | The nearest devices at 1.68 and 4.67 km pass QC only 63.4% and 22.5% of the time. The nearest historically qualified device is 13.31 km away. |
| Elgin_DMV | The nearest historically qualified device is 16.03 km away. |
| Kingery Near-road #1 | The nearest historically qualified device is 10.53 km away and is already paired with East Chicago–Marina. |
| Hammond-167th St | The only qualified device within 10 km is existing device 185079, 5.89 km away; its closer primary reference is East Chicago–Marina. |
| Summit | Existing devices 254189 and 195219 are 5.44 and 8.25 km away. Unselected device 269072 is 8.79 km away, has no current health check, and is closer to McCook. |

Reusing an existing device against a second reference is a possible **secondary
exploratory comparison**, not new geographic sensor coverage. Adding 269072
would increase a reference-site count by accepting another distant pairing,
rather than resolve the northern/northwestern gaps. We therefore do not swap
two or three devices or relax the 95% QC / 10 km limits without new evidence.
If seven-county coverage becomes a required conclusion, a targeted discovery
and a fresh qualification period must precede the roster change.

The Mayfair pair **101643 / 271809** is **0.722 km** apart; the Joliet pair
**287190 / 288452** is **1.894 km** apart. These are retained as within-area
redundancy, not independent geographic replicates. Summaries should first
aggregate within reference site and hour, then compare areas; a site with two
devices must not automatically receive double regional weight. A future
replacement can favor wider or opposite-side separation only after satisfying
quality, distance and health requirements.

The reproducible [coverage audit](../output/collection_review/coverage_gaps.json)
records all five gaps, the ineligible Gary reference, nearest candidates and
the six same-reference pair distances.

| Sensor ID | Nearest AirNow PM2.5 site | Distance km | Trial QC pass | Paired QC hours |
| --- | --- | ---: | ---: | ---: |
| 164317 | Naperville (`170434002`) | 0.724 | 100.0% | 56 |
| 175417 | SCHILPRK (`170313103`) | 1.007 | 100.0% | 67 |
| 287190 | Joliet (`171971002`) | 1.368 | 100.0% | 60 |
| 192837 | CHI_SP (`170310057`) | 2.262 | 98.6% | 59 |
| 185079 | East Chicago - Marina (`840180890034`) | 2.630 | 98.6% | 57 |
| 101643 | CHI_MAYFAIR (`840170310052`) | 3.175 | 95.8% | 62 |
| 310731 | DESPLNS (`170314007`) | 3.417 | 98.6% | 65 |
| 195219 | Cicero Liberty (`170316005`) | 3.418 | 98.6% | 65 |
| 175455 | CHI_WASH (`170310022`) | 3.593 | 100.0% | 65 |
| 254189 | McCook (`840170311016`) | 3.713 | 95.8% | 64 |
| 312091 | Aurora (`840170890007`) | 4.571 | 100.0% | 67 |
| 304088 | NORTHBRK (`170314201`) | 5.152 | 97.2% | 64 |
| 244039 | CHI_COM (`170310076`) | 5.595 | 95.8% | 59 |
| 325302 | ALSIP (`170310001`) | 7.888 | 97.2% | 58 |
| 288452 | Joliet (`171971002`) | 1.645 | 100.0% | 64 |
| 314513 | Naperville (`170434002`) | 2.217 | 97.2% | 61 |
| 271809 | CHI_MAYFAIR (`840170310052`) | 3.309 | 100.0% | 65 |
| 87741 | CHI_SP (`170310057`) | 3.711 | 98.6% | 65 |
| 247067 | DESPLNS (`170314007`) | 3.857 | 95.8% | 56 |
| 280642 | Cicero Liberty (`170316005`) | 4.954 | 98.6% | 66 |

Healthy checked backups: **185237, 249139, 182281, 309644**. A replacement
must match the intended area/reference as closely as possible; adding every
backup to the active roster would change the budget and study design.

The [replacement map](../output/collection_review/replacement_plan.json) now
contains a rule for **every active device**. For CHI_SP, **192837 or 87741 →
185237** after a fresh check. For Joliet, **287190 or 288452 → 249139** after a
fresh check. A shared backup can replace only one active sensor at a time.
CHI_SP alternatives **182281** and **309644** are respectively 5.61 and 6.01 km
from the reference; using them requires explicit exploratory reclassification.
They cannot silently replace a ≤5 km primary pair.

The other 16 active devices have **no verified same-stratum backup** in the
saved checked pool. Their explicit fallback is to hold the failed slot, retain
other working sensors and report reduced coverage. This is not a claim that
four backups can replace any of the twenty devices. Log old/new sensor IDs,
reference, UTC effective time, reason and distance stratum before changing the
roster. Re-evaluate changed cohorts in separate windows.

## Dates and points budget

If started November 17, collect through **December 9 23:59 Chicago time**.
The hard cutoff is `2026-12-10T06:00:00Z`. This gives a maximum **23 days**
before the December 11 submission date, with up to 66,240 sampled readings.
A later start shortens the run; do not automatically extend the end date.
Reserve December 10 for final reconciliation, figures, and the demo. Review
health and points daily at 09:00 Chicago time.

The former `2026-11-17T06:05:00Z` first-poll target is withdrawn as an automatic
deadline. `planned_first_poll_utc` is intentionally unset. Choose a **future**
cron slot only after the new image, paused deployment and manual preflight
have passed. Recompute the duration/budget for that actual start; the 23-day
figures below are a maximum planning allowance, not a committed start or spend.

Two small current-health requests used **218 points** in total. The current
balance is **919,698**. The 20-sensor request cost was directly measured at
**165 points**; the six-candidate follow-up cost 53 points.

| Budget item | Points |
| --- | ---: |
| 144 calls/day × 165 points | **23,760/day** |
| First 72 hours | **71,280** |
| Maximum 23-day run | **546,480** |
| Additional 10% contingency | **54,648** |
| Total planning allowance | **601,128** |
| Balance remaining after that allowance | **318,570** |

Set a campaign cap of **610,000 points** and a reserve floor of **250,000**.
Review organization balance daily and stop or investigate if the balance/cap
would be breached. An unexpectedly high daily burn (>28,512 points, 20% over
plan) triggers investigation, not automatic quota expansion. Keep Cloud Run
retries at zero; retry only after checking whether an API request already ran.
Compared with the trial's roughly 25,290 points/day, this improves sampling
frequency sixfold for the selected sensors while slightly reducing daily cost.

This is a points budget, not a GCP compute/storage bill. Field prices can
change; repeat the measured-cost check after changing fields or roster size.
No extra points purchase is required by this plan.

## Hourly quality and review gates

- Keep every raw response. Deduplicate by `(sensor_id, last_seen)`.
- Group by the UTC hour of `last_seen`, with Chicago display derived afterward.
- An accepted sampled hour needs **at least 5 of the expected 6 distinct
  reports**, in five distinct ten-minute bins, spanning at least **35 minutes**.
- Require both nonnegative A/B values, humidity between 0 and 100, outdoor
  status, and response age at most 180 seconds. Preserve failed observations
  with flags rather than deleting them from raw storage.
- Provisional A/B tolerance: `abs(A-B) <= max(5, 0.20 * mean(A,B))` in µg/m³.
  This is a project screening rule, not an EPA regulatory acceptance standard.
- Label the output a **sampled hourly estimate**. AirNow's reference value is
  a raw hourly concentration; do not compare it with PM2.5 NowCast AQI.
- Three consecutive unusable polls (about 30 minutes) trigger a health check.
  Replace persistently failing devices only after checking a nearby backup;
  keep 20 active sensors and record the change's effective time.
- After the first 72 complete UTC hours, require at least **411/432 successful
  scheduled slots**, and **65/72 accepted sampled hours per retained sensor**.
  Each retained sensor must also have usable reports in at least 411 slots.
  Daily windows require at least 137/144 successful/usable slots and 22/24
  accepted hours. Failed/missing calls remain in the denominator; duplicate
  log entries and preflight calls cannot fill extra slots. Investigate poor
  sensors individually. Do not equate a successful API call with twenty valid
  observations.
  Report performance for ≤5 km and 5–10 km pairs separately; do not tune the
  cohort using an in-sample correlation score.

Continue AirNow at **minute 40 of each hour**, including its **72-hour revision
lookback**. Its product specification describes hourly updates and revisions
of the preceding 72 hours. Preserve the preliminary-data label. End the
collector with the campaign and pause both schedulers after the cutoff.

## Deployment sequence (not executed in this review)

1. **Implemented locally:** `ingestion/purpleair_poll.py` supports exact
   `show_only` IDs, rejects empty/duplicate/oversized rosters, and caps
   fixed-cohort response-time age at 180 seconds. It retains raw bytes,
   records missing IDs and QC reasons, and rejects unexpected IDs before SQL
   writes. This code is not yet in the deployed image.
2. **Implemented locally:** Terraform accepts the roster, cron, server age,
   client threshold, activation time and cutoff. Prepare the overlay with
   the existing scheduler **paused**; keep the same `aq-purpleair-poll`
   scheduler resource. The proposed YAML is not automatically read by the
   deployed job.
3. Build a new Python 3.15 image and pin its digest. Apply the reviewed paused
   configuration, replacing the old hourly schedule. Manually run one cycle
   marked `preflight` and verify all 20 IDs, quality details, raw output,
   committed SQL writes, and settled point cost (expected 165 for all 20).
   Resolve failures before enabling recurring requests.
4. Set a future activation slot and enable only this scheduler. Verify the
   first scheduled cycle; record the first complete UTC hour as the review
   start. Keep the December 10 cutoff even if activation is delayed.
5. Run the offline gate below after the first 72 hours and for daily windows.
   Resolve failures using the replacement map; there is no automatic swap or
   automatic success declaration. Pause schedulers at the final cutoff and
   reconcile counts and points.

Detailed operator commands are in [the campaign runbook](collection_campaign.md).
The gate is runnable with Python 3.15:

```sh
.venv/bin/python -m analysis.collection_gate \
  --poll-log tmp/campaign/polls.json \
  --raw-dir tmp/campaign/raw \
  --start-utc ACTUAL_FIRST_FULL_HOUR_UTC --hours 72
```

Exit code 0 means pass, 1 means review required, and 2 means the requested
window is not complete. Run five minutes after the final hour to allow the
last scheduled attempt to complete. The gate groups by report time and
deduplicates sensor/report timestamps; it is a coverage/QC check, not an EPA
calibration or pollution-effect test.

No schedule, job image, Cloud SQL grant, or production collection configuration
was changed in this review. No human login is needed to read the evidence:
Application Default Credentials worked even though the gcloud CLI login
needs renewal before future CLI operations.

## Reproduction and sources

Run `.venv/bin/python -m analysis.collection_review` against the downloaded
trial cache. The [source manifest](../output/collection_review/source_manifest.json)
records exact GCS generations and local content hashes; the
[summary](../output/collection_review/summary.json),
[per-sensor metrics](../output/collection_review/sensor_quality.json), and
[commit logs](../output/collection_review/log_evidence.json) support the findings.
The current health checks are separate from trial statistics.

Run `.venv/bin/python -m analysis.collection_plan_review` to reproduce the
coverage-gap audit and replacement map from those saved metrics. No additional
PurpleAir requests were made for this follow-up review.

Follow-up verification: **53 Python tests passed**, with two opt-in local
PostgreSQL integration tests skipped because no test DSN was configured;
Terraform validation and **18 mocked infrastructure tests** passed. Lint,
format and whitespace checks passed. This is local verification, not proof of
a rebuilt image or a live SQL preflight. See
[verification evidence](../output/collection_review/followup_verification.json).

[PurpleAir points calculation](https://community.purpleair.com/t/api-pricing/4523)
explains base plus per-field/per-row cost; our estimates use measured calls.
[AirNow HourlyAQObs specification](https://docs.airnowapi.org/docs/HourlyAQObsFactSheet.pdf)
defines the 9/12-digit site IDs, raw hourly concentrations, UTC begin-hour,
72-hour revisions, and preliminary status. For S16, retain A/B and humidity
needed by the [published EPA correction research](https://amt.copernicus.org/articles/14/4617/2021/);
this review does not fit or validate a new correction formula.
