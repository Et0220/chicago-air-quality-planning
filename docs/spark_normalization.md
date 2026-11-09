# S11/S12: EPA normalization and cloud execution

## Progress and prerequisites

S01–S10 supply a reconciled nationwide archive: 233 staged CSVs, 907,024,502
records, and 218,661,382,349 bytes. The November 9 validation report identified
S08 late-metadata/configuration defects and S10's custom raw-prefix defect.
The current working tree already contains fixes and regression tests for all
three; this S11/S12 work did not author those fixes. The user confirmed that
the corrected images were rebuilt and deployed. The November 9 follow-up
checked the live Cloud Run image digests against the configured images and
ran a full Terraform plan: **No changes**. See the deployment details in
[validation evidence](../output/spark/verification.json). Pending contract reviews and provisional
PurpleAir cadence decisions remain separate acceptance items. See
[the previous review](../output/validation/s01_s10_report.txt).

S11 now has explicit raw/curated schemas, native Spark SQL transformations,
old and recent real EPA fixtures, and local Spark tests. S12 has a versioned
input snapshot, an asynchronous Dataproc submitter, row reconciliation,
generation-checked publication, and a generated scale report. See
[scale_report.md](scale_report.md) for actual processed scope; planned rows
are never reported as processed.

The user selected this session's scope on November 9: finish S11, the 2026
pilot, the report, and step-by-step instructions. The 1990–2025 cloud backfill
is deliberately deferred; no all-year run is launched by this session.

**Pilot verified:** batch `aq-s12-2026-20261109221230` published 6,988,009 rows
across 50 Parquet files (514,136,327 bytes), with zero rejects or duplicates.
The identical-input submission returned `unchanged` after checking all
committed generations/checksums, without creating another Spark batch. See
[replay evidence](../output/spark/replay_verification.json) and
[validation evidence](../output/spark/verification.json).

## Python and local verification

**All project Python code runs on Python 3.15.** `pyproject.toml` restricts the
minor version. Make defaults to the existing venv or `python3.15`; the cloud
driver and smoke job reject any other minor version. Dataproc's stock Python
is replaced by our custom Python 3.15 image. The project uses native SQL
expressions, without Python UDFs or distributed Python serialization.

1. Create or activate a Python 3.15 environment:

   ```sh
   python3.15 -m venv .venv
   .venv/bin/python -m pip install -e '.[dev,spark,gcp]'
   ```

2. Install Java 17 in a persistent location. Java is separate from Python.
   This machine now retains Temurin 17.0.21 at
   `~/Library/Java/JavaVirtualMachines/temurin-17.0.21.jdk`. On macOS:

   ```sh
   export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
   export SPARK_LOCAL_IP=127.0.0.1
   ```

   On another machine, install a retained [Temurin 17 JDK](https://adoptium.net/temurin/releases/?version=17)
   for that operating system and set `JAVA_HOME` to its home directory.
   Do not use an OS temporary directory for the installed JDK.

3. Run local checks:

   ```sh
   make lint test
   ```

   Spark tests run when both the optional `spark` dependency and working
   Java 17 are available. `tests/conftest.py` probes `java -version`, detects
   macOS launcher stubs, and skips only JVM-dependent tests if Java is absent
   or unusable. With no `JAVA_HOME`, it discovers the macOS Java 17 installation
   or Java on `PATH`. An explicitly invalid `JAVA_HOME` is reported as a skip.
   Spark startup/execution failures with a usable JDK still fail the tests.
   The tests exercise actual JVM execution, not mocks. PostgreSQL tests retain
   their existing opt-in integration flags. Java needs loopback networking, so
   a sandbox that blocks local sockets must allow the test process.

## Transform contract

The 24 EPA source columns are explicit strings on read, preserving identifiers
and bad source values for validation. Headers must match positionally, CSV
decoding is UTF-8, quoted newlines are supported, and malformed CSV fails the
batch. `spark/schemas.py` defines the typed curated StructType.

- `observed_at_utc` comes exclusively from `Date GMT` and `Time GMT`. Missing
  GMT, invalid dates, or malformed `HH:mm` times are exceptions. Valid nonzero
  minutes are preserved: the 2026 pilot found 3,579 samples beginning at `:26`
  at monitor `06-073-0077-88101-03`. `observed_hour_utc` is a separate explicit
  hour bucket; `is_hour_aligned=false` flags these rows for downstream window
  comparisons. Original
  local-standard fields are retained; they are never interpreted as DST-aware
  Chicago time. Chicago county rows derive civil date/hour and UTC offset from
  `America/Chicago`. Both fall-back 01:00 hours remain distinct UTC keys.
- Identifiers are validated before padding. POC has at most two digits.
  Canadian state code `CC` is retained. Site and monitor keys follow S01.
- Gas ppm is multiplied by 1,000; ppb is unchanged. Recognized particulate
  microgram labels retain their numeric values. Original unit labels preserve
  local versus standard conditions; MDL and uncertainty retain source units.
  Unsupported parameter/unit combinations go to exceptions. No physical
  conversion between gas units and mass concentration is inferred.
- Empty concentrations become null; negative values remain. Invalid numeric
  strings, NaN/infinity, bad identifiers/dates, and missing lineage are
  exceptions with reasons, source URI/generation, and a serialized raw record.
- Parameter 88101 with method 236/238 is advisory-superseded. For each
  `(monitor_key, observed_hour_utc)`, eligible methods precede superseded ones,
  then latest revision date, numerically greatest source generation, stable
  SHA-256 row hash, and URI. A superseded row with no eligible replacement is
  retained and flagged. Raw sources retain all versions.
- Parquet uses UTC `TIMESTAMP_MICROS`; no shifted local timestamp is stored.
  Python `collect()` may display a naive datetime in the host's timezone;
  tests verify timestamps through Spark SQL rather than treating that display
  as UTC. Chicago display columns are DATE/SMALLINT.
- **Partition `year` is the EPA archive/local-standard year.** A December 31
  local-standard sample can have a January 1 UTC timestamp in that partition.
  This permits a complete archive-year replacement and avoids overwriting the
  next year's partial UTC partition. Filter event time with `observed_at_utc`.
  `pollutant` is the observed five-digit parameter **STRING**, never a file-group
  label. `LEAD` is the catalog file group; lead observations retain codes such
  as `14129` or `12128`. Partition columns are encoded in directory names and
  omitted from physical Parquet payloads. Spark can infer a numeric-looking
  partition as INT if no schema is supplied. Readers must use `CURATED_SCHEMA`
  and the curated root as `basePath` when reading exact file URIs; this keeps
  `year` INT and `pollutant` STRING. The lead regression test covers this path.

Additional lineage fields beyond S01 are `raw_source_uri`,
`raw_source_generation`, `source_file_group`, and `row_hash`.
`source_uri/source_generation` identify the staged CSV. `ingested_at_utc`
is fixed for a batch. `pm25_advisory_superseded` exposes the advisory.

Sources: [Spark partition discovery](https://spark.apache.org/docs/3.5.7/sql-data-sources-parquet.html#partition-discovery),
[EPA field semantics](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html),
[EPA PM2.5 advisory](https://aqs.epa.gov/aqsweb/airdata/Data_Advisory_PM25_88101_Pregen_Files.pdf),
[Dataproc custom Python environments](https://docs.cloud.google.com/managed-spark/docs/guides/custom-containers).

The base image in `spark/Dockerfile` is pinned to
`python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928`,
the digest recorded by the original successful Cloud Build
`1e7a033e-7338-4cf1-85f9-77574c5af3bf`. The deployed runtime digest remains
unchanged. This pins the base layer; `apt-get` package repositories are still
mutable, so future builds are not guaranteed byte-identical. Review base
updates explicitly, following [Docker digest pinning guidance](https://docs.docker.com/build/building/best-practices/#pin-base-image-versions).

## Cloud run: step by step

Run these commands from the repository root, the folder containing
`pyproject.toml`. Exported values below are resource names, not credentials.

The existing project is reused. No API keys or SQL passwords are needed.
S12 adds repository-scoped Artifact Registry Reader for `aq-spark`, because
the S02 stock-image smoke test did not need private image pulls. Terraform
records that grant in `infra/spark.tf`. The submitter needs the S02 roles,
read access to staged data, and write access to scratch and curated objects.

1. Set nonsecret resource names and the digest-pinned Python 3.15 image:

   ```sh
   export AIR_QUALITY_GCP_PROJECT_ID=final-team-514205
   export AIR_QUALITY_GCS_BUCKET=final-team-514205-air-quality
   export AIR_QUALITY_SPARK_IMAGE=us-central1-docker.pkg.dev/final-team-514205/air-quality/spark-python315@sha256:22bfeeadd8de25752a45e9617e3e4820b73870835e573f3d921af24efb96103b
   ```

   Existing credentials work on the current machine. If a future session
   reports expired credentials, run `gcloud auth login`, then
   `gcloud auth application-default login` locally. Do not paste tokens into
   chat or commit credential files.

2. Preview a complete source year without cloud writes or compute:

   ```sh
   .venv/bin/python -m spark.cloud_epa plan --year 2026
   ```

3. Submit the year (cloud compute is billable):

   ```sh
   .venv/bin/python -m spark.cloud_epa submit --year 2026 \
     --image "$AIR_QUALITY_SPARK_IMAGE"
   ```

   The command prints a batch ID. Fixed resources: two 4-core executors,
   one 4-core driver, 64 shuffle partitions, 30-minute TTL. At the default
   12 DCUs and $0.06/DCU-hour, that is approximately $0.36 compute per batch
   if it reaches the TTL; shuffle/storage/build/operations are additional.
   A trial budget alert is not a spending cap.

4. Inspect the returned batch until `SUCCEEDED`:

   ```sh
   gcloud dataproc batches describe BATCH_ID \
     --project="$AIR_QUALITY_GCP_PROJECT_ID" --region=us-central1 \
     --format='yaml(state,stateMessage,runtimeInfo)'
   ```

5. Publish only after success, then regenerate the report:

   ```sh
   .venv/bin/python -m spark.cloud_epa finalize --batch BATCH_ID
   .venv/bin/python -m spark.scale_report
   ```

   Dataproc can report `SUCCEEDED` shortly before final usage metrics arrive.
   If `finalize` says final usage is unavailable, retry after about a minute;
   it has not changed the current published dataset.

6. Repeat `submit` with the same year/image/code. It verifies committed
   object generations and returns `unchanged`, without a new Spark job.
   For a remaining year, replace `2026` with that year and repeat steps 2–5.
   Complete each batch before advancing; do not run concurrent jobs for the
   same year. Use the measured pilot cost/throughput to budget the full run.

   Larger historical years contain over 33 million rows, versus 7 million in
   the partial 2026 pilot. A linear extrapolation can exceed the default
   30-minute TTL. Before a future full backfill, benchmark a larger year and
   optimize or explicitly raise its bound, for example
   `plan --year 2022 --ttl-minutes 60` followed by the matching `submit`.
   The allowed range is 10–120 minutes. A 60-minute bound doubles the default
   compute allowance to about $0.72 per batch, excluding other charges.

## Publication and recovery

Runtime evidence uses an explicit allowlist of CPU, memory, and SQL settings.
Generated Spark authentication fields are excluded. Earlier diagnostic
metadata was sanitized before handoff; this does not change source or
measurement data.

The submitter validates each S10 object's generation, byte size, MD5, source
ZIP lineage, and row count. It copies that exact generation server-side into
scratch at a generation-named path. Spark reads this fixed snapshot, avoiding
mutable-path reads. The source fingerprint includes source evidence, image,
runtime, and transformation source hashes.

A generation-protected year lock prevents overlapping publication. Outputs
remain under the scratch attempt until the job succeeds and counts reconcile.
The publisher copies verified Parquet into `curated/epa_hourly/year=.../pollutant=.../`
with batch-specific filenames, retains exceptions and run evidence under
hidden control directories, and atomically updates `_commits/{year}.json`.

**S13 must load the exact file list in the commit, not recursively glob the
curated root.** Old attempts may remain for audit and never belong to the
current dataset unless the current commit references them. An interrupted
publication leaves the previous commit intact and can be finalized again.
The `_runs/` evidence and `_exceptions/` files survive the seven-day scratch
lifecycle. Source CSV snapshots and temporary results expire in scratch.

### Retiring superseded files

The November 9 follow-up **already retired** the 50 live Parquet objects
(514,171,574 bytes) from `aq-s12-2026-20261109220005`. The live 2026 folder
now contains exactly the current commit's 50 files (514,136,327 bytes).
The current commit generation is unchanged. Old generations remain available
in versioned storage. See [the cleanup record](../output/spark/aq-s12-2026-20261109220005/cleanup.json)
and [the post-cleanup verification](../output/spark/review_verification.json).
No manual cleanup is needed now; the commands below document the procedure.

Use `spark.curated_cleanup` to retire one known superseded published batch.
It defaults to a read-only preview, checks the durable `_runs/` file list,
rejects currently referenced or out-of-scope paths, and verifies every live
generation/size/MD5 before the first deletion. Applying it acquires the same
year lock used by publication. Each deletion has a generation precondition.
Versioning is required: deletion removes the live name while retaining the
old generation for recovery. This prevents wildcard duplication but does
not immediately reclaim the retained version's storage charges.

1. Preview the exact batch and inspect the resulting `cleanup_plan.json`:

   ```sh
   .venv/bin/python -m spark.curated_cleanup \
     --project final-team-514205 --bucket final-team-514205-air-quality \
     --year 2026 --batch aq-s12-2026-20261109220005
   ```

2. Apply that specific batch cleanup:

   ```sh
   .venv/bin/python -m spark.curated_cleanup \
     --project final-team-514205 --bucket final-team-514205-air-quality \
     --year 2026 --batch aq-s12-2026-20261109220005 --apply
   ```

   The local `output/spark/BATCH_ID/cleanup.json` and durable
   `_runs/BATCH_ID/cleanup.json` record the outcome. The command is restartable:
   files already absent from the live listing are skipped; a replacement
   generation causes it to stop. If interrupted with a year lock remaining,
   inspect the cleanup process and lock owner before releasing that exact
   lock generation. Never remove a lock belonging to an active publication.

3. Keep the S13 loading rule even after cleanup. Resolve the current commit,
   verify its listed object generations/checksums, then read exactly its
   `files[].uri`. A Spark reader must preserve partition types explicitly:

   ```python
   from spark.schemas import CURATED_SCHEMA

   # commit is the validated current _commits/{year}.json document.
   curated_root = "gs://final-team-514205-air-quality/curated/epa_hourly"
   frame = (
       spark.read.schema(CURATED_SCHEMA)
       .option("basePath", curated_root)
       .parquet(*[item["uri"] for item in commit["files"]])
   )
   ```

   Do not infer `pollutant` from Parquet types or substitute catalog `LEAD`.
   The S13 acceptance checks must include exact committed row counts and
   a superseded-file case, even when the live folder currently looks clean.

If a run fails, inspect the Dataproc `stateMessage` and driver logs. Do not
delete its year lock while the batch is PENDING, RUNNING, or CANCELLING.
For a FAILED or CANCELLED run, release only its recorded lock generation:

```sh
.venv/bin/python -m spark.cloud_epa release-failed --batch BATCH_ID
```

This command checks the actual terminal state and matching owner/generation
before deletion. Then resubmit the year. Keep any prior
commit and output files. If failure happened during publication after a
successful batch, retry `finalize` first. Do not rerun a full year merely to
recover an interrupted copy.

The S12 ledger is GCS commits plus checked-in metrics. Cloud SQL
`ingest_file.process_state` remains unchanged because the Spark service
account deliberately has no SQL writer grant. Do not interpret pending SQL
processing state as evidence that a committed S12 output is missing.
