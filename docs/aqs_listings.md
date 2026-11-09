# S07: AQS site and monitor listings

## Source and validation

The loader fetched EPA's public [site](https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip)
and [monitor](https://aqs.epa.gov/aqsweb/airdata/aqs_monitors.zip) listings.
Both CSVs report extraction date **2026-06-26** on every row. This is the
source's extraction date, not today's monitoring status. The full ZIPs passed
CSV/key validation: **20,994 sites**, **370,090 monitors**, no duplicate keys,
and no monitor without a listed site. EPA includes four Canadian sites and 28
Canadian monitors under state code `CC`; migration 002 retains them.

| Source | ZIP bytes | SHA-256 | GCS generation |
| --- | ---: | --- | --- |
| `aqs_sites.zip` | 1,018,314 | `56b322f6a3d5368d055bce53d0988275c0b27fbcced3d57a6ce8f2e2b539e15a` | `1794204166286269` |
| `aqs_monitors.zip` | 6,919,360 | `e8325216d4ec5df462a23c201e6ffa1176b2fce363c9f6b0e3f18ce6ad958525` | `1794204166716088` |

The versioned GCS objects were uploaded and read back with matching sizes and
SHA-256 metadata. A second upload attempt reused both objects; their GCS
generations remained unchanged:

- `gs://final-team-514205-air-quality/raw/aqs/aqs_sites/56b322f6a3d5368d055bce53d0988275c0b27fbcced3d57a6ce8f2e2b539e15a.zip`
- `gs://final-team-514205-air-quality/raw/aqs/aqs_monitors/e8325216d4ec5df462a23c201e6ffa1176b2fce363c9f6b0e3f18ce6ad958525.zip`

## Chicago-area PM2.5 reference list

The [100-row CSV](../output/aqs/chicago_pm25_monitors.csv) prints monitor key,
parameter, coordinates, first year of data, last sample date, latest method,
and the PM2.5 advisory flag. It covers parameter `88101` (80 monitors) and
`88502` (20 monitors). All 100 have coordinates and those two activity fields.

| County | PM2.5 monitors |
| --- | ---: |
| Cook, IL | 51 |
| DuPage, IL | 5 |
| Lake, IL | 1 |
| Will, IL | 7 |
| Kane, IL | 6 |
| McHenry, IL | 4 |
| Lake, IN | 26 |
| **Total** | **100** |

No Chicago row's **last listed method** is 236/238. Nationally, 33 of the
4,454 `88101` listing rows have one of those last methods. The listing does
not provide method history, so S11 must flag historical hourly rows by their
own method code. `Last Sample Date` is a latest recorded sample, not a
guarantee of current operation; EPA notes reporting lag in its
[file format documentation](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html).

## Run and database handoff

The loader uses Python 3.15. Install `.[dev,gcp,cloudsql]` and set the
non-secret project and bucket environment variables, plus a Cloud SQL
connection according to `docs/cloudsql.md`. Then:

```sh
python -m ingestion.db migrate
python -m ingestion.db grant-workers
python -m ingestion.aqs_listings
```

For an offline report from downloaded ZIPs, add `--site-zip PATH
--monitor-zip PATH --no-gcs --no-db`. The normal run uploads each ZIP once
under its content hash and upserts batches into Cloud SQL. A rerun with the
same ZIP generation leaves existing rows unchanged.

**Cloud SQL load completed.** Migrations 001 and 002 were recorded in the
database, and the EPA worker grants were applied. The first load completed
with 20,994 sites and 370,090 monitors. Read-back queries found exactly those
counts, all rows carrying the expected GCS generation, 100 Chicago-area PM2.5
monitors, and 33 nationally flagged latest-method 88101 monitors. A privilege
check confirmed the EPA worker may insert into `aqs_monitor`.

A second full load completed without error. Its conflict clause updates a row
only when its source generation differs, so the same ZIP generation is a no-op
for existing rows. The GCS rerun left both object generations unchanged. A
separate administrator user update occurred during the SQL rerun; new
connections using the earlier password then failed. The rerun's existing
connection completed, but a fresh SQL read-back afterward was unavailable.
The stale local password file was removed. Coordinate current administrator
access with the database owner for future SQL checks or loads.
