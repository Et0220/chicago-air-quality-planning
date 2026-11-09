# S05: Cloud SQL operational schema

Cloud status updated by S06 on November 9, 2026 UTC: migrations
`001_operational.sql` and `002_aqs_listing_fields.sql` were applied to
`final-team-514205:us-central1:aq-operational`, database `air_quality`, and the
four worker identities received their table grants. A replay applied no
migrations. Tables are owned by `cloudsqlsuperuser`; the temporary bootstrap
user and its in-memory credential were removed without resetting `postgres`.
S06 also passed its state/version/locking integration test in a disposable
schema, which was removed afterward.

S07 subsequently initialized a `postgres` password with the project owner's
authorization to load the AQS listings. A later administrator user update
invalidated that temporary local credential during the S07 rerun. The stale
local copy was removed. The reference-table load and its first read-back
succeeded; see `docs/aqs_listings.md`. Obtain current administrator access
from the database owner for future migrations.

## Design and handoffs

The numbered migration in `cloudsql/migrations/` creates the operational
tables. `ingest_file` has one current row per EPA catalog group and year, with
separate catalog, download, stage, and process states. `ingest_file_version`
retains each downloaded GCS generation after a source file is reissued.
`ingest_run` records job outcome and counts. The `raw_md5`, exact downloaded
byte size, GCS generation, and run ID make S06/S10 state auditable; the catalog
byte count remains an estimate.

S07 adds migration `002_aqs_listing_fields.sql` for listing-specific dates and
monitor coordinates. It also admits EPA's `CC` code on Canadian site rows.
Run `python -m ingestion.db migrate` again before loading those listings.

PurpleAir readings use `(sensor_id, report_ts_utc)` as the primary key.
`pa_poll_log` records HTTP failures, latency, duplicate, late, and stale
counts. `pa_sensor_history` gives S13 a place to read known metadata changes;
S08 must create and close intervals when reported metadata changes. It must
not invent historical effective dates before the first observation.

AirNow's current row uses `(airnow_site_id, pollutant, observed_at_utc)`.
An update that changes concentration, unit, or AQI automatically archives the
previous value in `airnow_obs_revision`. A repeated insert with `ON CONFLICT
DO NOTHING` creates no second row. S09 should only update current rows when
the newly retrieved file is newer; the revision trigger does not choose the
latest source by itself. Both tables keep preliminary values and raw lineage.

AQS sites and monitors preserve leading-zero component keys. The database
checks that `site_key` and `monitor_key` match their components. Coordinates,
source URI/generation, activity dates, and the PM2.5 advisory flag are stored
for S07/S13. Extra listing fields can remain in `metadata` JSONB until their
downstream use is settled.

## Local verification

Use Python 3.15 and install `.[dev]`. Docker must be available for the
PostgreSQL 16 integration check. The compose service is a disposable database
published on loopback only and uses trust authentication, so no password or
credentialed connection string is stored in the repository.

```sh
docker compose -f cloudsql/compose.yaml up -d --wait
export AIR_QUALITY_TEST_POSTGRES_DSN='postgresql://air_quality_dev@127.0.0.1:5433/air_quality_test'
export AIR_QUALITY_CLOUDSQL_DSN="$AIR_QUALITY_TEST_POSTGRES_DSN"
python -m ingestion.db migrate
python -m ingestion.db check
make lint test
docker compose -f cloudsql/compose.yaml down
```

The integration test creates a random schema in the disposable database,
applies the migration twice, inserts sample records, checks duplicate inserts,
and verifies that an AirNow change archives the old value. It drops its test
schema afterward. Without `AIR_QUALITY_TEST_POSTGRES_DSN`, only that database
test is skipped.

## Cloud application

S02 created PostgreSQL 16 instance `aq-operational`, database `air_quality`,
and IAM database users. It did not create an administrator password or SQL
table grants. An authorized project operator must set a temporary password
for the built-in `postgres` account through the Cloud SQL console, keep it in
an ignored local `.env` or a secure secret store, then run the following with
project access and Application Default Credentials. Do not place the password
on a command line, in Git, or in a Terraform variable. Rotate it after schema
bootstrap. The Python connector handles the encrypted instance connection.

```sh
python3.15 -m pip install -e '.[cloudsql]'
unset AIR_QUALITY_CLOUDSQL_DSN AIR_QUALITY_CLOUDSQL_IAM_USER
export AIR_QUALITY_CLOUDSQL_CONNECTION_NAME='final-team-514205:us-central1:aq-operational'
export AIR_QUALITY_CLOUDSQL_DB='air_quality'
export AIR_QUALITY_GCP_PROJECT_ID='final-team-514205'
# Set AIR_QUALITY_CLOUDSQL_USER=postgres and
# AIR_QUALITY_CLOUDSQL_PASSWORD through an ignored local environment source.
python3.15 -m ingestion.db migrate
python3.15 -m ingestion.db grant-workers
python3.15 -m ingestion.db check
```

`grant-workers` grants each S02 IAM SQL user access only to its operational
tables: EPA can write ingestion state and AQS listings; PurpleAir can write
sensors, readings, and poll logs; AirNow can write observations and revision
history; sync can read live data and write its run log. The command fails if a
Terraform-created IAM database role is absent. Granting privileges is
idempotent. On Cloud Run, set `AIR_QUALITY_CLOUDSQL_IAM_USER` to the service
account email without `.gserviceaccount.com`, and leave the built-in user and
password unset. The connector then uses automatic IAM database authentication.
The service account needs S02's Cloud SQL Client and Instance User IAM roles.

The database is small (`db-f1-micro`, 10 GiB fixed SSD). Monitor table growth
and retention before starting high-cadence collectors. No production rows or
cloud migration should be claimed for a new environment until these commands
complete.

### Application record

For project `final-team-514205`, instance `aq-operational`, database
`air_quality`: migrations `001_operational.sql` and
`002_aqs_listing_fields.sql` are recorded in `schema_migration` (applied
2026-11-09 06:04 UTC, 12 tables in `public`). On 2026-11-09 a repeat `migrate`
reported the schema as current, `grant-workers` completed, and `check` listed
both migrations. No production rows have been loaded by this step.
