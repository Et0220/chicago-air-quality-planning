# S12: Cloud PySpark scale report

Updated 2026-11-09T22:22:28.313931+00:00 from verified batch results.

**Processed 6,988,009 of 907,024,502 catalog rows (0.77%).** Curated rows: **6,988,009**; duplicates removed: **0**; exception rows: **0**.

**Remaining shortfall: 900,036,493 rows.** Unprocessed inputs are not counted as normalized output. Full historical S12 acceptance remains open. This delivery intentionally covers the 2026 pilot; the 1990–2025 backfill is deferred.

The saved EPA catalog covers 1990–2026 and was published in June 2026; 2026 is a partial year. There are 296 year × file-group cells: 233 nonempty and 63 empty. Empty hourly cells do not imply no longer-duration observations exist.

[All manifest cells](../output/spark/reconciliation.csv). Identity: input = curated + removed duplicates + exceptions. Each batch first checks actual CSV record counts against S10 and the catalog, then reads the written Parquet back. Input bytes are staged CSV sizes; output bytes are actual Parquet object lengths.

## Verified batches

| Archive year | Input rows | Input bytes | Curated rows | Duplicates | Exceptions | Output bytes | Transform seconds | DCU hours | Compute estimate USD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026 | 6,988,009 | 1,832,308,769 | 6,988,009 | 0 | 0 | 514,136,327 | 388.6 | 1.4008 | 0.0840 |

## Output by observed parameter

| Archive year | Parameter code | Curated rows | Parquet bytes | Advisory flagged | Null measurements | Negative measurements | Nonaligned |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026 | 42101 | 368,027 | 27,314,234 | 0 | 0 | 4,658 | 0 |
| 2026 | 42401 | 741,160 | 54,262,653 | 0 | 0 | 58,664 | 0 |
| 2026 | 42602 | 805,803 | 59,695,880 | 0 | 0 | 7,597 | 0 |
| 2026 | 44201 | 1,906,626 | 138,863,227 | 0 | 0 | 251 | 0 |
| 2026 | 81102 | 850,829 | 63,158,323 | 0 | 0 | 6,668 | 0 |
| 2026 | 88101 | 1,979,862 | 145,791,422 | 14,137 | 0 | 50,243 | 3,579 |
| 2026 | 88502 | 335,702 | 25,050,588 | 0 | 0 | 9,700 | 0 |

## Runtime, cost, and evidence

Each batch uses two fixed executors with four cores each, a four-core driver, 64 shuffle partitions, adaptive SQL execution, and a 30-minute TTL. Python 3.15 is mandatory. The Docker image is pinned by digest; actual Python/Spark versions and properties are preserved in each commit.

Transform duration excludes cloud startup and publication. DCU usage comes from Dataproc approximateUsage; estimated compute cost is milliDcuSeconds / 3,600,000 × $0.06. Shuffle estimate is reported shuffleStorageGbSeconds / 3,600 × $0.000054795; both components are retained in commit evidence. This is a compute estimate, not a billing invoice or total cost. Build time, retained GCS data, operations, and credits are separate. [Google Cloud pricing](https://cloud.google.com/products/managed-service-for-apache-spark/pricing).

- 2026: `aq-s12-2026-20261109221230`; Python 3.15.2, Spark 3.5.3; [metrics](../output/spark/aq-s12-2026-20261109221230/commit.json).

## All attempts, including failures

| Batch | State | Compute USD | Shuffle USD |
| --- | --- | ---: | ---: |
| aq-s12-2026-20261109214434 | FAILED (failed_lock_released) | 0.0119 | 0.0011 |
| aq-s12-2026-20261109214818 | SUCCEEDED (diagnostic_retired) | 0.0908 | 0.0083 |
| aq-s12-2026-20261109220005 | SUCCEEDED (published) | 0.0819 | 0.0075 |
| aq-s12-2026-20261109221230 | SUCCEEDED (published) | 0.0840 | 0.0077 |

All-attempt compute + shuffle estimate: $0.2933.
Missing usage is not counted as zero-cost evidence. The first pilot failed while pulling the private image (403); the repository-only Spark read grant was then added through Terraform before retry.
A successful diagnostic run then exposed an overly strict zero-minute timestamp check. Its output was not published: 3,579 valid `:26` samples would have been quarantined. The corrected job preserves exact sampling time, adds an explicit UTC-hour key/alignment flag, and successfully reran the year.

## Interpretation and handoff

Partition `year` is the EPA archive/local-standard calendar year. New Year UTC spillover remains in the same archive partition. Use observed_at_utc for event-time filtering. Input files preserve parameter codes, units/bases, and method metadata. Superseded PM2.5 methods are flagged and lose same-hour ties, but otherwise remain visible. Null/negative concentrations remain in curated data.

S13 must consume exact file URIs/generations from `curated/epa_hourly/_commits/{year}.json`; recursive wildcard loading can include old or interrupted attempts. Only a verified successful batch can publish a commit. Raw inputs and prior generations remain recoverable. Cloud SQL process_state has not been updated: the Spark identity has no SQL write grants. These commit records are the S12 processing ledger.

S15 owns national cross-year key validation and warehouse reconciliation. [Run and recovery instructions](spark_normalization.md).
