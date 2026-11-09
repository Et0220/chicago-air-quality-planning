# Air Quality Planning for Outdoor Events — AI-Executable Segments

Source of truth: `output/pdf/Air_Quality_Project_Proposal_Revised.pdf` (nationwide 1990–latest backfill, PurpleAir + AirNow live, Chicago study).

Each segment is scoped so that one AI session can complete it given the listed inputs. Segments marked **[HUMAN]** need a person to supply credentials, approve spending, or make a judgement call before the AI starts.

---

## Project question and reference

**Operator question:** Which Chicago neighborhood venue and recurring class window should an outdoor fitness operator schedule, given historical PM2.5 patterns and the latest local conditions?

[Surviving Chicago](https://github.com/asingh49-cmd/Surviving-Chicago-Data-Engineering-and-Analytics-Pipeline) is the reference for a decision-focused project: a Chicago question, a Snowflake star schema, Streamlit visual findings, and Claude over MCP. Its README describes approximately 195,000 crime records from one year and 49,000 active business licenses. This project demonstrates scale with the nationwide EPA hourly archive from 1990 onward processed in cloud PySpark, and live ingestion with successive PurpleAir reports. AirNow provides preliminary hourly context.

The final README and demo follow one operator decision through the system. Compare two candidate venue and session-window combinations using paired historical observations and coverage. Check current conditions and freshness on the map. Ask Claude the same historical comparison through a read-only MCP tool and verify the result against the dashboard's Snowflake serving view. Show the source-to-warehouse reconciliation and a newly ingested sensor report to establish where the displayed numbers came from.

## 0. Shared conventions (read before every segment)

**Repo layout**

```
infra/          # Terraform / gcloud scripts
ingestion/      # manifest, downloaders, pollers (Python)
spark/          # PySpark jobs + schemas
cloudsql/       # migrations (SQL)
snowflake/      # DDL, stages, tasks, views
analysis/       # SQL + notebooks for Chicago study
app/            # Streamlit
mcp/            # MCP server
tests/          # pytest, small sample fixtures
docs/           # data contracts, runbooks, reports
config/         # settings.yaml, .env.example
```

**GCS layout**: `gs://<bucket>/raw/epa/{year}/{file}.zip`, `staging/epa/{param}/{year}/*.csv`, `curated/epa_hourly/year=/pollutant=/*.parquet`, `raw/purpleair/dt=YYYY-MM-DD/hh=HH/*.json`, `raw/airnow/dt=/hh=/*.dat`.

**Project language**: Write all project changes and inputs in English, including code comments, documentation, configuration descriptions, and presentation materials.

**Rules**: Python 3.11, `pyproject.toml`, config via env vars + `config/settings.yaml`, secrets only from Secret Manager / `.env` (never committed), all timestamps stored in UTC with Chicago local time derived, every job idempotent and re-runnable, each segment ships tests + a short README section.

**Prompt template to hand a segment to an AI**

> You are working in the repo described in `plan/PROJECT_SEGMENTS.md`. Complete segment **SXX** exactly as specified. Read Section 0 conventions and the outputs of its dependencies first. Deliver the listed files, make the acceptance checks pass, and summarize anything you could not verify.

---

## Segment order and dependencies

The rows below follow the order in which segments typically hand off their outputs. Start a segment as soon as its prerequisites are available. A segment can use an early deliverable from another segment while that segment continues. The dependency column names the specific output needed to finish.

| ID | Segment | Owner | Start and parallel work | Required outputs for completion |
|----|---------|-------|-------------------------|---------------------------------|
| S01 | Repo scaffold and data contracts | Tongyan | Start immediately. Share draft contracts with all owners. | Agreed schemas, keys, configuration conventions, and repo layout. |
| S02 | GCP infrastructure | Yujie | Start account and trial checks in parallel with S01. Provision once the relevant names and configuration are agreed. | S01 infrastructure conventions and human access to an eligible GCP project. Verify a small cloud Spark job and the services needed by the collectors. |
| S03 | EPA AirData manifest | Yujie | Explore the public catalog in parallel with S01 and S02. Implement the manifest after S01 defines its schema. | S01 manifest contract and a catalog snapshot with file counts, sizes, and empty or missing entries. |
| S04 | PurpleAir feasibility check | Yujie | Start credential setup and exploratory requests in parallel with S01–S03. | S01 sensor field contract, a PurpleAir key, and a short authenticated pilot that establishes reporting cadence, usable fields, coverage, and points cost. |
| S05 | Cloud SQL operational schema | Breanna | After S01, develop and test migrations locally while S02 provisions GCP. | S01 contracts and S02 Cloud SQL access to apply the migrations in the cloud. |
| S06 | EPA ZIP downloader to GCS | Yujie | Develop against S03 sample manifest entries as soon as S01 defines their format. Run a sample download before starting the rest. | S02 GCS, S03 manifest, and S05 ingestion-state tables. Publish completed ZIPs incrementally for S10. |
| S07 | AQS site and monitor listings | Yujie | Develop in parallel with S06 and S08. The national backfill is independent of this load. | S02 GCS and S05 reference tables. Publish Chicago site IDs, coordinates, and available activity dates for S17 and S18. |
| S08 | PurpleAir poller | Yujie | Draft against S01 fixtures while S04 runs. Start collecting as soon as the prerequisites are ready. | S02 runtime and storage, S04 approved sensor set and interval, and S05 sensor tables. Publish readings for S14 and early S25. |
| S09 | AirNow hourly collector | Yujie | Develop in parallel with S08. Start hourly collection as soon as its cloud prerequisites are ready. | S02 runtime and storage and S05 observation tables. Publish raw hourly concentrations and AQI for S14 and S17. Revision tracking is optional. |
| S10 | ZIP-to-CSV staging | Yujie | Stage the first ZIPs as S06 publishes them. Continue in parallel with the remaining downloads. | Verified S06 ZIPs and recorded CSV row counts for each staged file. |
| S11 | PySpark normalization | Breanna | Start on S01 contracts and fixtures while S06 and S10 run. | Representative S10 CSVs for validation, including older and recent observations. |
| S12 | Cloud PySpark run and scale report | Breanna | Start with one representative year after S11 passes. Publish its Parquet and metrics before running the remaining partitions. | S02 Spark access, S10 staged inputs for each batch, and S11 tested transformations. Reconcile all processed partitions and document any shortfall. |
| S13 | Snowflake warehouse schema and load | Bravo | Design the star schema after S01. Create live landing tables and test GCS integration before the full S12 run. Load the first S12 Parquet output as soon as it is ready. | Human access to a Snowflake trial, S02 GCS access, S07 reference data, and S12 outputs for the historical fact load. Live landing tables can be handed to S14 earlier. |
| S14 | Live data sync to Snowflake | Bravo | Start once the S13 live landing tables and first S08/S09 rows exist. Run independently of the national backfill and historical fact load. | S08 and S09 source tables and S13 live landing tables. Verify incremental delivery and idempotency. |
| S15 | Data quality and reconciliation | Breanna | Develop assertions after S01. Run historical checks against each S12/S13 batch and live checks as S14 delivers data. | S12 processing counts, S13 warehouse counts for the loaded scope, and S14 live sync results. Publish Chicago coverage and quality results early for S19. |
| S25 | Exploratory data analysis | Tongyan | Profile early S08/S09 rows and the first S12 output in parallel. Share initial coverage and sensor quality findings before S16 and S18 finalize their choices. | S12 historical outputs, S13 warehouse data, and S14 live data for final reproducible figures. Use early cross-source overlap checks without waiting for S17's final validation. |
| S16 | PurpleAir QC, hourly aggregation, and EPA correction | Bravo | Implement on fixtures while S14 is being wired. Use preliminary S25 findings to assess data completeness and the documented EPA method for correction. | S14 PurpleAir rows and S04 measured cadence to verify hourly completeness. Publish raw and corrected hourly values for S17. |
| S17 | Sensor-to-monitor matching and validation | Bravo | Design geographic matching after S07 while S16 is underway. | S07 reference metadata, S09 hourly concentrations through S14, and S16 sensor hours. Use eligible S12 AQS observations when they overlap. Report the available matched hours and validation metrics. |
| S18 | Chicago venues and session windows | Bravo | Choose provisional venues and windows early with the team. Start mapping after S07 in parallel with S17. | S07 reference data, preliminary S25 coverage results, human decisions on venues and thresholds, and S13 venue dimension for the final load. |
| S19 | Historical scheduling analysis | Bravo | Draft and test queries on fixtures or the first Chicago warehouse slice while S15 and S18 are underway. | S13 Chicago historical facts, S15 Chicago quality checks, S18 final venue mapping, and S25 findings relevant to the study. National reconciliation can continue separately. |
| S20 | Curated serving views | Bravo | Define view names and schemas after S01 so S21 and S22 can start on fixtures. Publish historical and live views as each branch is ready. | S19 historical results, S16 corrected readings, S17 match and validation results, and S14 AirNow data for the complete view set. |
| S21 | Streamlit application | Tongyan | Build against the S20 view contracts while the views are being implemented. Run in parallel with S22. | Real S20 serving views, S25 EDA highlights, and S02 Cloud Run access for end-to-end verification. |
| S22 | Claude MCP server | Tongyan | Build against the same S20 contracts in parallel with S21. | Real S20 views and the read-only warehouse role for Claude integration tests. |
| S23 | Orchestration, monitoring, and runbook | Breanna | Document restart and recovery as jobs are built. Add optional alerts and cross-job orchestration once job interfaces stabilize. | Working S06–S14 jobs for integrated checks. Individual collectors own their minimum scheduling and restart behavior. |
| S24 | Submission materials and recording | All | Start the README, architecture diagram, and check-in deck as soon as design and sample-run evidence are available. Update them as results come in. | Verified scale, EDA, analysis, and calibration results plus working S21/S22 demos. Rehearse with all four members, scan for secrets, then package and record. S23 stretch work does not gate submission. |

### Scope decisions

- **Historical backfill.** Prioritize the full 1990-to-latest run. If the measured cloud budget or throughput makes that infeasible, prioritize 2010 onward using the same partitioned pipeline. Report the full manifest separately from the rows actually processed and state the shortfall.
- **PurpleAir.** Target two-minute polling. If the authenticated pilot shows it exceeds available points, narrow the sensor set near eligible monitors first. If needed, use the shortest affordable interval that provides enough hourly coverage and report the measured cadence. If credentials or coverage fail, document the missing live sensor component explicitly.
- **Optional work.** Drop additional orchestration alerts, AirNow revision tracking, and a locally fitted calibration adjustment before cutting core ingestion, EDA, warehouse validation, or the demo.

---

## S01 — Repo scaffold & data contracts

- **Goal**: Create the repo skeleton and the written contracts every other segment codes against.
- **Deliverables**: directory tree above; `pyproject.toml` (ruff, pytest); `config/settings.yaml`, `.env.example`; `docs/data_contracts.md` defining: pollutant codes (44201 ozone, 42401 SO2, 42101 CO, 42602 NO2, 88101 PM2.5 FRM/FEM, 88502 PM2.5 non-FRM/FEM, 81102 PM10, lead), canonical column names/types for EPA hourly, PurpleAir, AirNow; monitor key `state-county-site-param-poc`; GCS paths; `Makefile` with `lint`, `test`.
- **Acceptance**: `make lint test` passes on empty test suite; contracts reviewed by all four owners.

## S02 — GCP infrastructure **[HUMAN: project ID, billing, credits]**

- **Goal**: Provision cloud resources reproducibly.
- **Deliverables**: `infra/` Terraform (or idempotent gcloud scripts) for: GCS bucket (object versioning on), service accounts with least privilege, Secret Manager entries (PurpleAir key, AirNow key, Snowflake creds), Cloud SQL Postgres (smallest tier), Artifact Registry, Cloud Run jobs placeholders, Cloud Scheduler, Dataproc Serverless permissions; budget alert.
- **Acceptance**: `terraform plan` clean; `docs/infra.md` lists every resource and monthly cost estimate.

## S03 — EPA AirData file manifest

- **Goal**: Enumerate every hourly file 1990→latest for the 8 pollutant groups with listed rows and bytes.
- **Deliverables**: `ingestion/epa_manifest.py` scraping the AirData download catalog (`aqs.epa.gov/aqsweb/airdata/download_files.html`); output `manifest.csv` (file name, pollutant, year, url, listed_rows, listed_bytes, last_modified, status); flags empty/missing files and pollutants (e.g. lead) only measured on longer schedules; summary of total rows/bytes.
- **Acceptance**: manifest covers every year × pollutant cell or explicitly marks it absent; totals printed; unit test on a saved HTML fixture.

## S04 — PurpleAir feasibility check **[HUMAN: API key]**

- **Goal**: Establish key access, quota, fields, reporting cadence, and Chicago coverage before deploying S08.
- **Deliverables**: `ingestion/purpleair_check.py` querying sensors in a Chicago metro bounding box (outdoor only); report `docs/purpleair_feasibility.md`: sensor count, fields available (sensor_index, last_seen, lat/lon, pm2.5_cf_1 A/B, humidity), points cost per call, projected monthly points at 2-min polling, recommendation (poll interval / field set) to stay within budget.
- **Acceptance**: report contains live pilot measurements for distinct report timestamps, usable fields, sensor coverage, and points cost at the selected polling interval.

## S05 — Cloud SQL operational schema

- **Goal**: Normalized store for operational records and ingestion state.
- **Deliverables**: `cloudsql/migrations/*.sql` (Alembic or plain SQL) for tables: `ingest_file` (manifest + download/stage/process state, checksum, version), `ingest_run`, `pa_sensor`, `pa_reading` (sensor_id, report_ts, arrival_ts, pm25_a, pm25_b, humidity, raw_uri; PK sensor_id+report_ts), `pa_poll_log` (outages, latency), `airnow_obs` (with revision history), `aqs_site`, `aqs_monitor`; `ingestion/db.py` connection helper.
- **Acceptance**: migrations apply on local Postgres via docker; pytest inserts/reads sample rows; duplicate insert is a no-op.

## S06 — EPA ZIP downloader → GCS

- **Goal**: Land every manifest file in GCS, versioned and verifiable.
- **Deliverables**: `ingestion/epa_download.py` (Cloud Run job): streams ZIP to `raw/epa/`, records md5/size/generation in `ingest_file`, retries with backoff, skips unchanged files, concurrency limit, `--years --pollutants` filters; Dockerfile.
- **Acceptance**: test downloads of representative nonempty ZIPs succeed and an empty lead manifest entry is skipped; rerun is a no-op; state table matches GCS listing.

## S07 — AQS site & monitor listings ingest

- **Deliverables**: `ingestion/aqs_listings.py` loading `aqs_sites.zip` and `aqs_monitors.zip` into `aqs_site`/`aqs_monitor` and GCS; note EPA PM2.5 data advisory filters as flags in `docs/data_contracts.md`.
- **Acceptance**: Chicago-area (Cook, DuPage, Lake, Will, Kane, McHenry, Lake IN) PM2.5 monitors list printed with coordinates and active dates.

## S08 — PurpleAir poller

- **Goal**: Continuous capture of Chicago outdoor sensors at the interval S04 shows is affordable (target 2 min, expect 10–15 min).
- **Deliverables**: `ingestion/purpleair_poll.py` (Cloud Run job + Scheduler); writes raw JSON to GCS; upserts `pa_reading` keyed on sensor_id+report_ts; logs arrival_ts, duplicates, late arrivals (report_ts older than last seen), staleness, HTTP failures to `pa_poll_log`; respects S04 budget.
- **Acceptance**: one hour of unattended running shows the expected poll count for the configured interval, no duplicate rows, and recovery from a logged simulated outage.

## S09 — AirNow hourly collector

- **Deliverables**: `ingestion/airnow_collect.py` hourly job fetching AirNow hourly data files (public archive) for the Chicago area; stores raw; upserts `airnow_obs`; optional revision tracking keeps prior values when a reading within the preceding 72 h is revised; label all as preliminary.
- **Acceptance**: backfill the last 72 hours and rerun without creating duplicates. If revision tracking is implemented, report the revision count and test a revised value.

## S10 — ZIP → CSV staging

- **Deliverables**: `ingestion/epa_stage.py` decompressing `raw/epa` ZIPs to `staging/epa/{param}/{year}/`, updating `ingest_file` state, recording CSV row counts (for reconciliation vs listed_rows).
- **Acceptance**: staged row counts recorded for every downloaded file; mismatches listed.

## S11 — PySpark normalization job (local)

- **Goal**: Correct, tested transform logic before spending cloud money.
- **Deliverables**: `spark/schemas.py` explicit StructTypes; `spark/normalize_epa.py`: parse local date/time + GMT fields to UTC timestamp, derive Chicago local time; normalize units per pollutant (ppm/ppb/µg/m³ documented); preserve monitor ids, POC, method code, qualifier, MDL, uncertainty; dedupe on (monitor key, utc hour) keeping latest revision by date_of_last_change; write Parquet partitioned by `year`,`pollutant`; `tests/fixtures/` small CSVs incl. duplicates/revisions/DST edge.
- **Acceptance**: pytest with local Spark passes; output schema documented.

## S12 — Full PySpark run on cloud + scale report

- **Deliverables**: Dataproc Serverless (or cluster) submit script; run on one year → all years; `docs/scale_report.md`: actual rows and bytes in/out by year × pollutant, reconciliation vs manifest listed_rows and staged counts, runtime, executor/worker config, cost.
- **Acceptance**: every year × pollutant reconciled or discrepancy explained.

## S13 — Snowflake star schema (SCD2 dims) & load

- **Goal**: Build the OLAP dimensional model from the normalized sources (Cloud SQL `aqs_site`/`aqs_monitor`/`pa_sensor` + curated Parquet), with defensible grain and history-aware dimensions.
- **Deliverables**: `snowflake/ddl/`: storage integration + external stage on `curated/`; COPY/Snowpipe or tasks for loading; clustering on date/pollutant; Chicago study subset views.
  - **Facts and stated grain** (documented in `docs/data_contracts.md`): `fact_epa_hourly` = one row per monitor key (`state-county-site-param-poc`) × UTC hour; `fact_pa_hourly` = one row per sensor × UTC hour (after S16 QC); `fact_airnow_hourly` = one row per AirNow monitor × UTC hour (latest value, revisions in a history table).
  - **Type-1 / static dims**: `dim_pollutant`, `dim_date`, `dim_hour` (UTC + Chicago local, DST-aware).
  - **SCD Type 2 dims** (columns `valid_from_utc`, `valid_to_utc` as a half-open `[from, to)` interval per `docs/data_contracts.md`, `is_current`, surrogate key, `row_hash`): `dim_monitor` (location, status, method code, monitoring objective, activity dates), `dim_site` (address, lat/lon, county), `dim_sensor` (lat/lon, indoor/outdoor flag, device model — sensors can be moved or re-flagged), `dim_venue` (venue → nearest monitor mapping, which changes as monitors open or close). Facts join to the dimension version valid at the observation hour. Create historical versions only where effective dates are supported by source records. Resolve observations outside documented validity to an unknown dimension member and report those counts.
  - **Idempotent SCD2 loader**: a MERGE-based task/stored procedure that closes the old version and inserts the new one only when `row_hash` changes; re-running with unchanged input creates no new versions.
  - `docs/warehouse_model.md`: star-schema diagram, grain per fact, which dims are SCD2 and why (and why the others are not).
- **Acceptance**: EPA fact counts reconcile to S12 by year and pollutant for the declared warehouse load scope; sample analytical query (Chicago PM2.5 monthly mean) runs < 10 s on XS warehouse; tests with synthetic changes (monitor moves, method code changes, monitor closes) produce exactly the expected dimension versions; re-running the load is a no-op; a point-in-time join returns the historical attribute, not the current one.

## S14 — Live data sync Cloud SQL → Snowflake

- **Deliverables**: incremental export job (watermark on arrival_ts / revision ts) from `pa_reading`, `airnow_obs` to Parquet in GCS → Snowflake MERGE task; freshness metadata table.
- **Acceptance**: new PurpleAir readings visible in Snowflake within ≤15 min; re-run creates no duplicates.

## S15 — Data quality & reconciliation suite

- **Deliverables**: `tests/dq/` (Great Expectations, dbt tests or SQL assertions): uniqueness, null/range checks per pollutant, duplicate and revision counts, coverage/missingness by monitor-year, AirNow-vs-AQS reconciliation when AQS catches up; `docs/dq_report.md` generator.
- **Acceptance**: suite produces a reconciliation report with catalog, staged CSV, curated Parquet, and Snowflake fact counts by year and pollutant. Account for rejected rows, deduplication, and partitions outside the warehouse load scope. Reconcile live facts at their respective source and aggregation grains.

## S25 — Exploratory data analysis (EDA)

- **Goal**: Profile every data source before analysis and record the findings that change the design. The write-up must state "we found X, so we changed Y" for at least three findings. (Rubric: EDA, 20% of the grade.)
- **Deliverables**: `analysis/eda/` notebook(s) + reusable SQL, run on Snowflake (and Spark output for the pre-warehouse parts), and `docs/eda_findings.md` with embedded charts. Sections:
  1. **EPA hourly (national → Chicago)**: shape (rows, bytes, monitors, sites by year × pollutant); distributions per pollutant (histogram, log scale, percentiles); missing values (null rates, missing hours per monitor-year, monitors with long gaps); outliers (negative values, values above plausible ranges, qualifier-flagged readings, method-code changes); coverage over time (monitors reporting per year, start of usable hourly PM2.5); Chicago subset: which PM2.5 monitors have hourly data and how many hours per season.
  2. **PurpleAir live**: sensors found vs. active, report interval and latency, staleness, duplicate and late-arrival rates, A/B disagreement distribution, humidity range, missing-humidity share, spatial coverage map against the venues.
  3. **AirNow**: availability by monitor and overlap with AQS; include revision rate and magnitude if revision tracking is implemented.
  4. **Cross-source**: PurpleAir vs. nearest reference monitor on overlapping hours (scatter, bias by humidity), distance to nearest monitor per candidate venue.
  5. **"What surprised us"** list, each item linked to a design decision (e.g. QC threshold chosen from the A/B distribution, analysis restricted to years with hourly PM2.5, venue dropped for coverage gap, elevated-threshold sensitivity levels chosen from the observed PM2.5 percentiles).
- **Acceptance**: each source has shape, distribution, missingness and outlier tables/charts; ≥ 3 findings explicitly mapped to design changes in `docs/eda_findings.md`; numbers reproducible from the notebook/SQL in the repo; headline figures reused in the Streamlit "Data quality & coverage" page (S21) and the slides (S24).

## S16 — PurpleAir QC, hourly aggregation, EPA correction

- **Deliverables**: Snowflake SQL or PySpark: drop readings with missing humidity, stale reports, A/B disagreement (documented EPA thresholds); aggregate to UTC hour with minimum completeness rule; apply the EPA humidity-based PurpleAir correction for hourly averages (cite Fire & Smoke Map methods doc); keep raw and corrected columns; `docs/purpleair_methods.md`.
- **Acceptance**: unit tests on synthetic readings for each screen; share of readings dropped per rule reported.

## S17 — Sensor-to-monitor matching & calibration validation

- **Deliverables**: match each sensor to nearest eligible AQS/AirNow PM2.5 monitor within a documented distance limit (prefer collocations, record distance, leave others unmatched); join on overlapping UTC hours; date-based train/holdout split; raw vs corrected bias and MAE on holdout; optional local adjustment fit on train only; `docs/calibration_report.md` with tables/plots.
- **Acceptance**: no holdout leakage (test); metrics reproducible from one command.

## S18 — Chicago venues & session windows **[HUMAN: confirm venues, windows, threshold]**

- **Deliverables**: `analysis/venues.csv` (8–15 candidate park venues across neighborhoods with lat/lon); session windows (e.g. morning 06–09, afternoon 16–19 local); analytical elevated threshold (e.g. hourly PM2.5 > 35 µg/m³, stated as analytical, not regulatory); venue → nearest monitor(s) mapping with distance; load `dim_venue`.
- **Acceptance**: every venue has a mapped monitor or is flagged as a coverage gap.

## S19 — Historical scheduling analysis

- **Deliverables**: `analysis/` SQL + notebook: seasonal PM2.5 distributions per venue/window; paired morning vs afternoon differences within same monitor and date; episode detection (consecutive elevated hours, missing hour breaks run) with counts and durations; sample counts, monitor proximity, coverage gaps beside every comparison; findings summary `docs/analysis_findings.md`.
- **Acceptance**: episode logic unit-tested on synthetic series; results reproducible.

## S20 — Curated serving views

- **Deliverables**: `snowflake/views/serving_*.sql`: `venue_window_summary`, `venue_season_distribution`, `paired_window_diff`, `episodes`, `live_sensor_latest` (corrected PM2.5, freshness), `airnow_latest` (preliminary AQI), `coverage`; read-only role `APP_READER`.
- **Acceptance**: each view documented (columns, grain) in `docs/serving_views.md`; queries < 5 s.

## S21 — Streamlit application

- **Deliverables**: `app/` multi-page Streamlit: (1) Venue & session comparison (filters season/years, distributions, paired diffs, episode counts, coverage notes), (2) Live conditions map (corrected PurpleAir clearly labeled separate from preliminary AirNow AQI, freshness/staleness badges), (3) Data quality, EDA highlights & coverage (from S25), (4) About/methods & attributions; cached Snowflake queries; Dockerfile; deploy to Cloud Run.
- **Acceptance**: app loads against `APP_READER`; live page updates on refresh; screenshot set in `docs/app/`.

## S22 — MCP server for Claude

- **Deliverables**: `mcp/server.py` (Python MCP SDK) exposing read-only tools: `list_venues`, `compare_venues(season, window)`, `get_episodes(venue, years)`, `live_conditions(venue)`, `coverage(venue)`, plus a guarded `run_sql` limited to serving views with row limit; config for Claude Desktop; `docs/mcp.md` with example questions.
- **Acceptance**: Claude answers 5 scripted operator questions correctly via the tools; non-SELECT SQL rejected (test).

## S23 — Orchestration, monitoring & runbook

- **Deliverables**: Cloud Scheduler/Workflows wiring for S06–S14; alerting (poller outage, stale data > 30 min, failed jobs) via Cloud Monitoring; `docs/runbook.md` (restart, backfill, key rotation, cost controls).
- **Acceptance**: simulated failure triggers an alert; runbook steps verified once.

## S24 — Final report, demo script & slides

- **Deliverables**: `docs/final_report.md` (architecture diagram, scale metrics from S12, DQ, calibration, findings, limitations); `docs/demo_script.md` (each owner presents their stage; live sensor updates, correction + matched-hour validation, operator comparing venues/windows, Claude via MCP); slide deck.
- **Acceptance**: dry-run demo within time limit; the same venue and session comparison agrees across Streamlit and Claude; show a new sensor report progressing from ingestion to the serving view and display its observation and warehouse arrival times; all numbers trace to a report file.

---

## Human decisions before dependent work

1. GCP project on free trial credits and Snowflake trial account (S02, S13). **Course rule: do not upgrade to a paid billing account.** Confirm that the GCP services and Snowflake trial will remain available through rehearsal, recording, and submission. Enable Snowflake in time to verify the first real warehouse path.
2. PurpleAir API key and points budget (S04).
3. Venue list, session windows, elevated threshold, sensor–monitor distance limit (S17, S18).
4. Spark execution choice: Dataproc Serverless vs. small cluster (S12) — cost vs. speed.
