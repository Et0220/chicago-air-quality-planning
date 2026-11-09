# S03: EPA AirData hourly manifest

S03 is complete. The manifest covers all **37 years × 8 groups = 296 cells**
from 1990 through the latest hourly catalog year, 2026. There are **233
nonempty files, 63 listed empty files, and no missing cells**. National source
totals are **907,024,502 rows** and **6,716,687,360 approximate compressed
bytes** (6.717 GB / 6.255 GiB). These are catalog counts before deduplication
or Chicago filtering, not measured downloads or processed records.

## Evidence and freshness

- Source: [EPA AirData download catalog](https://aqs.epa.gov/aqsweb/airdata/download_files.html).
- Retrieved: **2026-11-09 05:39:49 UTC** (November 8 in Chicago).
- Every selected cell says **As of 2026-06-25**. The latest-year data is partial;
  fetching the page today does not make these counts current through today.
- The page's separate HTTP `Last-Modified` is **2026-07-13 15:51:18 GMT**.
- Snapshot SHA-256:
  `815afb5445d66d1762caecfc5a5c4072640d7d2548638bb3c90c0ad43bdb814b`.
- Files: [manifest CSV](../output/epa/manifest.csv),
  [summary JSON](../output/epa/summary.json),
  [source metadata](../output/epa/catalog_source.json), and
  [saved catalog HTML](../output/epa/catalog.html).

## Coverage and sizes

Every group has 37 entries and zero missing years. `listed_bytes` uses the
explicit project convention **catalog KB × 1,024**. EPA does not define KB on
this page; the displayed sizes are rounded estimates. They must not be used
as exact byte counts or integrity checks. S06 will record measured ZIP sizes.

| Pollutant | Nonempty | Empty | Listed rows | Approximate bytes |
| --- | ---: | ---: | ---: | ---: |
| `carbon_monoxide` | 37 | 0 | 111,915,207 | 777,751,552 |
| `lead` | 0 | 37 | 0 | 0 |
| `nitrogen_dioxide` | 37 | 0 | 118,702,199 | 885,823,488 |
| `ozone` | 37 | 0 | 296,543,739 | 2,240,696,320 |
| `pm10` | 37 | 0 | 68,408,453 | 529,607,680 |
| `pm25_frm_fem` | 19 | 18 | 87,419,505 | 697,812,992 |
| `pm25_non_frm_fem` | 29 | 8 | 65,403,498 | 519,116,800 |
| `sulfur_dioxide` | 37 | 0 | 158,631,901 | 1,065,878,528 |
| **Total** | **233** | **63** | **907,024,502** | **6,716,687,360** |

The 63 empty entries comprise lead in **1990–2026**, PM2.5 FRM/FEM (`88101`)
in **1990–2007**, and PM2.5 non-FRM/FEM (`88502`) in **1990–1997**.
The summary explicitly flags lead with `no_hourly_rows_listed` and
`longer_duration_samples_excluded`. EPA's hourly files exclude measurements
collected over longer durations, so zero hourly rows do not establish an
absence of measurements. Lead observations would require an appropriate
longer-duration source rather than fabricated hourly values.
[EPA file documentation, section 6](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html)

## Run and reproduce

Use the repository's Python 3.15 environment and dependencies:

```sh
# Refresh the catalog; no ZIP downloads, authentication, or cloud writes:
python -m ingestion.epa_manifest

# Rebuild from the saved HTML into a separate directory:
python -m ingestion.epa_manifest --catalog-file output/epa/catalog.html \
  --output-dir tmp/epa-replay
cmp output/epa/manifest.csv tmp/epa-replay/manifest.csv
cmp output/epa/summary.json tmp/epa-replay/summary.json

make lint test
```

`--settings` selects a YAML configuration. `AIR_QUALITY_EPA_CATALOG_URL` and
`AIR_QUALITY_PROJECT_HISTORY_START_YEAR` override the corresponding scalar
settings; `--catalog-url` and `--start-year` take precedence over those.
`--end-year` optionally limits the range; its default is the latest year
found in any hourly catalog link. Group mappings come from YAML.
Environment variables must be exported; this public-data script does not
read secrets or automatically load `.env`.

The CSV preserves the eight-column S01 contract. It is ordered by ascending
year and pollutant name. A `missing` cell has blank nullable fields, while
a listed empty file retains its URL, filename, catalog date, and zero count.
Zero rows determine emptiness even if a ZIP header has a nonzero size.
Missing row/byte counts are excluded from sums and remain separately counted.
`last_modified` stores the cell's catalog date, not an HTTP header.

Repeated input produces identical CSV and summary bytes. Online retrieval
time and HTTP headers live separately in `catalog_source.json`; offline
runs record `input_mode: saved_html` without inventing a retrieval time.
Each output is replaced atomically. Parsing or configuration errors fail
before output is written, with a nonzero exit code. Conflicting duplicate
entries and malformed metadata fail rather than becoming zero or missing.

## Validation and downstream handoff

The 19 S03 tests use saved EPA HTML cells, with one deliberately omitted
entry. They verify grid coverage, empty versus missing, comma-separated
counts, dates and URL resolution, exclusion of daily files, latest-year
discovery, malformed metadata, conflicting duplicates, configuration,
and an offline rerun that preserves the prior valid output on parsing failure.
The full catalog was also parsed independently to compare all 296 source
cells against the CSV, including dates, row counts, sizes, totals, and hash.
An offline replay of the full snapshot reproduces both manifest and summary.

S05/S06 can load all 296 manifest records, retain the 63 empty states, and
select the **233 `listed_nonempty`** entries for ZIP download. Catalog status
is separate from download/stage/process state. S06 must verify actual sizes,
checksums, and GCS generations; S03 made no ZIP downloads. Preserve `LEAD` as
a file group and actual parameter codes on any later observations.

For S02 budgeting, one retained raw ZIP generation is approximately
**6.255 GiB × $0.020/GiB-month = $0.13/month gross** in `us-central1` Standard
storage. This estimates raw ZIP storage only. Staged CSV, curated Parquet,
older versions, scratch data, operations, and compute remain additional;
measure expansion in S10–S12. See the updated [infrastructure estimate](infra.md)
and [Cloud Storage rates](https://cloud.google.com/storage/pricing).
