# S10: EPA ZIP to CSV staging

**Status: complete and verified on November 9, 2026 UTC.**

All **233 downloaded, nonempty EPA ZIPs** from 1990–2026 have a staged CSV.
The staged objects total **218,661,382,349 bytes (203.6 GiB)** and
**907,024,502 CSV rows**, exactly matching the S03 listed-row total. A fresh
GCS listing found 233 current objects and 233 total object generations, so
the final rerun did not create duplicate versions. The deployed read-only
verification matched all 233 files across Cloud SQL, GCS metadata, and the
manifest with **zero mismatches**. Per-file row counts, object sizes,
generations, and checksums are in
[`output/epa/stage_verification.json`](../output/epa/stage_verification.json).

`ingestion/epa_stage.py` reads the downloaded, nonempty S03 manifest entries
from Cloud SQL. It opens each raw object at the GCS generation recorded by S06,
checks its size and MD5, then validates the ZIP while streaming its single CSV
member to `staging/epa/{parameter}/{year}/hourly_{parameter}_{year}__g{raw_generation}.csv`.
The generation suffix keeps staged files tied to the raw version that produced
them. `staged_rows` counts CSV records after the header, including records with
quoted newlines. The job compares that count with `listed_rows` and reports
every difference.

The stage writer checks ZIP CRC, CSV decoding and row width, extracted size and
MD5, and the final GCS object's size and MD5. It stores source URI, generation,
MD5, producer run ID, CSV row count, column count, and CSV MD5 as object
metadata. A per-file PostgreSQL advisory lock and an update conditioned on the
raw generation keep S06 and S10 from publishing inconsistent current state.
Reruns reuse a staged object only when its source lineage and CSV checksum
metadata agree; a completed rerun reports `unchanged`. `--verify-only` compares
the current SQL state, stage objects, and catalog row counts without writes.

## Deployment and commands

The Cloud Run job is `aq-epa-stage` in `us-central1`. Its image is built from
`ingestion/Dockerfile.epa_stage` and a small source allowlist in
`.gcloudignore.epa-stage`, then pinned by digest with Terraform's
`epa_stage_image` variable. The job runs as the existing `aq-epa` service
account with IAM Cloud SQL authentication. It uses one task, two CSV workers,
2 vCPU, 2 GiB memory, a four-hour task timeout, and one retry. It has no
schedule.

The deployed image is
`us-central1-docker.pkg.dev/final-team-514205/air-quality/epa-stage@sha256:db7d193627f41d542eea2348c920fcbdbe87f4206ff227461bb1119d2311a240`
from Cloud Build `9b57e6ae-511e-4ba3-8d7d-a6b7a920741e`.

```sh
python -m ingestion.epa_stage --dry-run
python -m ingestion.epa_stage --years 2026 \
  --pollutants carbon_monoxide,pm25_non_frm_fem
python -m ingestion.epa_stage --verify-only

gcloud run jobs execute aq-epa-stage --project=final-team-514205 \
  --region=us-central1 --wait
gcloud run jobs execute aq-epa-stage --project=final-team-514205 \
  --region=us-central1 --args=--verify-only --wait
```

Local runs require the GCS project/bucket and Cloud SQL environment described
in [cloudsql.md](cloudsql.md). `--years` accepts comma-separated years and
ranges; `--pollutants` accepts configured group names. `--concurrency` accepts
one to four workers and defaults to two. ZIPs are downloaded to temporary
local storage one per worker; the extracted CSV is streamed to GCS and is
never held in full in memory or on local disk. An interrupted run can be
restarted: published stage objects are reconciled with SQL on rerun.

## Verification

The offline tests in `tests/test_epa_stage.py` cover exact byte preservation,
logical CSV row counting, invalid ZIP members, generation-specific reuse, and
catalog mismatch reporting. Results from the deployed full run are recorded
in [EPA stage evidence](epa_stage_evidence.json) and
[`output/epa/stage_verification.json`](../output/epa/stage_verification.json).

| Execution | Scope | Result |
| --- | --- | --- |
| `aq-epa-stage-f5f6q` | 2026 CO and PM2.5 non-FRM/FEM sample | 2 staged; 703,729 rows |
| `aq-epa-stage-l4zd9` | Sample verification | 2/2 matched; zero mismatches |
| `aq-epa-stage-2x27f` | 1990–1994 | 25/25 files covered; succeeded |
| `aq-epa-stage-hg42g` | 1995–2004 | 57/57 files covered; succeeded after one task retry |
| `aq-epa-stage-6mvcs` | 2005–2015 | 74/74 files covered; succeeded |
| `aq-epa-stage-lh5mf` | 2016–2026 | 77/77 files covered; succeeded |
| `aq-epa-stage-6g8j8` | Full rerun | 233 unchanged; no problems |
| `aq-epa-stage-8tgzm` | Final read-only verification | 233/233 matched; 907,024,502 rows; zero mismatches |

An initial all-years execution (`aq-epa-stage-sjttc`) overlapped the
year-bounded workers and was cancelled after transient advisory-lock
conflicts. The 1995–2004 task retried once. Every year range subsequently
completed successfully, and the uncontended all-years rerun confirmed all
233 files unchanged. These coordination errors did not produce row-count or
content mismatches.

The Python repository checks passed with 71 tests and four opt-in tests
skipped; Terraform's simulated infrastructure suite passed 14/14 tests.
