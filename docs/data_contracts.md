# Shared data contracts (S01 draft)

This document fixes the interfaces used by S02-S05 and the later pipeline.
Yujie, Breanna, Bravo, and Tongyan should review changes to keys, grains,
timestamps, units, and object paths before dependent jobs are finalized.

## Conventions

- Python runtime: 3.11. Configuration starts with `config/settings.yaml`;
  `AIR_QUALITY_*` environment variables override corresponding values.
  `AIR_QUALITY_GCP_PROJECT_ID` and `AIR_QUALITY_GCS_BUCKET` name cloud resources.
  For a YAML scalar, the override name is
  `AIR_QUALITY_<SECTION>_<KEY>` (for example,
  `AIR_QUALITY_PURPLEAIR_TARGET_POLL_SECONDS`). Lists and mappings are edited
  in YAML. Cloud resource IDs and the secret-only variables in `.env.example`
  have no committed defaults.
  A deployed job reads secrets from Google Secret Manager. Local development
  may load them from ignored `.env`. No key or connection string belongs in
  committed files, logs, slides, or sample data.
- Lowercase `snake_case` names are used in Python, SQL, Parquet, and CSV outputs.
  Identifiers are strings unless explicitly typed otherwise. Empty source
  strings become `NULL`; no identifier is converted to a number if doing so
  would remove leading zeros.
- All event, arrival, revision, and load timestamps use UTC. SQL uses
  `TIMESTAMPTZ` in Cloud SQL and `TIMESTAMP_TZ` or documented UTC
  `TIMESTAMP_NTZ` in Snowflake. Parquet timestamps are UTC, not local wall time.
  Timestamp columns end in `_utc`. Dates without times are `DATE`.
- Chicago display fields derive from the event timestamp with the IANA zone
  `America/Chicago`. Keep the UTC timestamp and offset alongside a local
  hour: the fall DST hour occurs twice and a local date/hour alone is not a
  unique key. EPA `Date Local`/`Time Local` are *local standard time* fields;
  do not use them as Chicago civil time or to reconstruct UTC when valid GMT
  fields exist.
- Every batch is idempotent. Keep raw source objects and source version or
  generation so a normalized row can be traced back. Re-running the same
  input must preserve keys and row counts. Null concentrations and negative
  raw values remain visible for quality analysis; exclusion happens in
  explicit downstream filters, not silently at ingestion.

## Pollutant groups and units

The **file group** controls EPA catalog discovery; the **parameter code**
identifies an individual measurement. Never replace an observed parameter
code with a group label. The lead catalog group is `LEAD`, not a five-digit
parameter code; preserve the actual code on any rows it contains (for
example, 14129 Lead TSP local conditions or 12128 Lead TSP standard
conditions). Empty lead hourly files are expected and must be recorded.

| Group name | AirData hourly ZIP suffix | Parameter / intended content | Normalized unit |
| --- | --- | --- | --- |
| `ozone` | `44201` | 44201 ozone | ppb |
| `sulfur_dioxide` | `42401` | 42401 SO2 | ppb |
| `carbon_monoxide` | `42101` | 42101 CO | ppb |
| `nitrogen_dioxide` | `42602` | 42602 NO2 | ppb |
| `pm25_frm_fem` | `88101` | 88101 PM2.5 FRM/FEM | µg/m³ |
| `pm25_non_frm_fem` | `88502` | 88502 PM2.5 non-FRM/FEM | µg/m³ |
| `pm10` | `81102` | 81102 PM10 | µg/m³ |
| `lead` | `LEAD` | observed lead parameter code | µg/m³ |

Store the EPA-provided `sample_measurement` and `units_of_measure` unchanged.
Set `measurement_standard` and `unit_standard` only for recognized units:
ppm × 1,000 = ppb, ppb unchanged, and micrograms per cubic meter unchanged.
Reject unknown unit labels into a documented exception set. Retain the raw
unit text because EPA distinguishes local-conditions and standard-conditions
particulate values; a shared display unit alone does not make those bases
equivalent. Do not mix unlike lead parameter codes in one statistic.

## EPA AirData hourly

**Source grain:** one reported sample at a monitor and GMT begin-hour. Raw
rows and file versions are immutable. **Curated grain:** one row per
`(monitor_key, observed_at_utc)` after the documented revision rule. `monitor_key`
is the five-part string `SS-CCC-NNNN-PPPPP-OO`, where `SS` is two-digit state
FIPS, `CCC` three-digit county FIPS, `NNNN` four-digit site number, `PPPPP`
five-digit parameter code, and `OO` two-digit POC. Example:
`17-031-0001-88101-01`. Keep the five components in separate columns too.
The site key is `SS-CCC-NNNN`. Preserve the raw POC and validate its width
before padding. AQS site and monitor listings join by these component keys.

| Canonical column | Type | Source / rule |
| --- | --- | --- |
| `state_code`, `county_code`, `site_num`, `parameter_code`, `poc` | STRING | Zero-padded source identifiers |
| `site_key`, `monitor_key` | STRING | Composed as above |
| `observed_at_utc` | TIMESTAMP UTC | `Date GMT` + `Time GMT`, the sample begin-hour |
| `date_local_standard`, `time_local_standard` | DATE, STRING | Original `Date Local` + `Time Local` for audit |
| `sample_measurement`, `measurement_standard` | DOUBLE nullable | Original value; unit conversion where recognized |
| `units_of_measure`, `unit_standard` | STRING | Original unit; documented target unit |
| `latitude`, `longitude` | DOUBLE nullable | EPA coordinates, WGS84 when datum supports it |
| `datum`, `parameter_name` | STRING nullable | Source descriptors |
| `mdl`, `uncertainty` | DOUBLE nullable | Method detection limit and uncertainty |
| `qualifier`, `method_type`, `method_code`, `method_name` | STRING nullable | Preserve source text/codes |
| `state_name`, `county_name` | STRING nullable | Source descriptors |
| `date_of_last_change` | DATE nullable | EPA revision date; date only |
| `source_uri`, `source_generation`, `ingested_at_utc` | STRING, STRING, TIMESTAMP UTC | Lineage |
| `chicago_local_date`, `chicago_local_hour`, `chicago_utc_offset_minutes` | DATE, SMALLINT, SMALLINT nullable | Derived for Chicago study rows only |

When rows share a curated key, first apply the EPA PM2.5 advisory preference
described below, then prefer the greatest `date_of_last_change`, newest
immutable source generation, and a stable row hash. Count and report rows
removed. A later source correction must update the curated value on rerun,
while the raw version remains. `date_of_last_change` cannot order changes
within a day, hence the version tie-breaker. S11 must test this rule on
representative source rows.

**EPA manifest:** `manifest.csv` has one record for every requested
`(file_group, year)` from 1990 through the catalog's latest year, including
empty or absent cells. Its columns are `file_name` STRING nullable,
`pollutant` STRING (group name above), `year` INT, `url` STRING nullable,
`listed_rows` BIGINT nullable, `listed_bytes` BIGINT nullable,
`last_modified` DATE nullable, `status` STRING. Status is one of
`listed_nonempty`, `listed_empty`, or `missing`. `listed_bytes` converts the
catalog's displayed KB to approximate bytes, so it is an estimate, not the
downloaded ZIP size. `last_modified` means the catalog's "As of" date, not an
HTTP `Last-Modified` header. S05 tracks download and staging state separately
from this catalog status.

EPA's hourly ZIPs exclude measurements taken at longer durations. A zero-row
hourly file does not prove that a pollutant was never measured. The PM2.5
advisory requires method-aware filtering. For parameter 88101, flag original
Teledyne T640/T640X methods 236/238 as superseded and prefer corrected methods
736/738 or updated FEM methods 636/638 when they represent the same monitor
hour. Keep all versions in raw data and record the selected method in curated
data. S07/S11 should expose the advisory flag, not silently remove records.

## PurpleAir current readings

**Raw grain:** one API response per successful poll, stored unchanged.
**Operational grain:** one row per `(sensor_id, report_ts_utc)`, with the
latest arrival used for duplicate detection. `sensor_id` is PurpleAir's
integer `sensor_index`, separate from AQS identifiers. PurpleAir includes it
by default; do not add it to the requested field list. S04 must verify that
`last_seen` (server receipt time) is a usable report timestamp and that
the requested A/B fields are returned at its measured cadence. If the pilot
shows a different event-time field is needed, update this contract before S08.

| Canonical column | Type | PurpleAir field / rule |
| --- | --- | --- |
| `sensor_id` | BIGINT | `sensor_index` |
| `report_ts_utc` | TIMESTAMP UTC | `last_seen` Unix seconds, subject to S04 pilot |
| `arrival_ts_utc` | TIMESTAMP UTC | Collector receipt time |
| `latitude`, `longitude` | DOUBLE nullable | `latitude`, `longitude` |
| `is_outdoor` | BOOLEAN | `location_type = 0`; S04 verifies the endpoint/filter |
| `pm25_cf1_a`, `pm25_cf1_b` | DOUBLE nullable | `pm2.5_cf_1_a`, `pm2.5_cf_1_b`, µg/m³ |
| `humidity_pct` | DOUBLE nullable | `humidity`, percent relative humidity |
| `raw_uri`, `source_request_id` | STRING | Raw GCS object and poll identifier |

`pa_sensor` holds the latest reported coordinates and metadata with change
history available to S13's SCD2 loader. Preserve null channel/humidity
values for QC. Log duplicate reports, late arrivals, source staleness, and
HTTP failures in `pa_poll_log`. No PM correction is applied in the raw or
operational reading table.

## AirNow hourly observations

Use the public **HourlyAQObs** CSV product (`HourlyAQObs_yyyymmddhh.dat`),
which includes both raw concentrations and separate hourly AQI fields.
Its filename and `ValidDate`/`ValidTime` are UTC begin-hours. Its files for
the preceding 72 hours are updated, so refetch that window and retain the
retrieval timestamp and source object version. This product is available
from May 2019 onward. AirNow values are preliminary.

**Logical observation grain:** `(airnow_site_id, pollutant, observed_at_utc)`;
the latest version is served and older values may be retained in a revision
table. `airnow_site_id` is the source `AQSID` as text. It is not automatically
an EPA `monitor_key`; S17 must validate any crosswalk. The source may provide
either a nine-digit site code or a twelve-digit code with a country prefix.

| Canonical column | Type | HourlyAQObs field / rule |
| --- | --- | --- |
| `airnow_site_id`, `site_name` | STRING | `AQSID`, `SiteName` |
| `observed_at_utc` | TIMESTAMP UTC | `ValidDate` + `ValidTime` |
| `pollutant` | STRING | Unpivot `PM25`, `OZONE`, `NO2`, `CO`, `SO2`, `PM10` |
| `concentration`, `concentration_unit` | DOUBLE nullable, STRING nullable | Pollutant value and `_Unit` field |
| `aqi` | SMALLINT nullable | Corresponding `_AQI` where provided; PM2.5/ozone/PM10 are NowCast AQI |
| `latitude`, `longitude` | DOUBLE nullable | Site coordinates |
| `site_status`, `data_source` | STRING nullable | `Status`, `DataSource` |
| `retrieved_at_utc`, `raw_uri`, `source_generation` | TIMESTAMP UTC, STRING, STRING | Retrieval and lineage |
| `is_preliminary` | BOOLEAN | Always true at ingestion |

Do not derive AQI directly from a single hourly concentration. A missing AQI
stays null. A changed concentration or AQI for the same logical key is a
revision, not a second current observation. Keep negative raw concentrations
for audit, with downstream quality flags.

## Storage paths and handoffs

Object names are relative to `gs://<AIR_QUALITY_GCS_BUCKET>/`. Raw paths are
immutable after publication; if a source is reissued, use object versioning
and record its GCS generation. Use UTC dates and hours in live prefixes.

| Layer | Object path | Producer -> consumer |
| --- | --- | --- |
| EPA raw | `raw/epa/{year}/{file}.zip` (`{file}` excludes `.zip`) | S06 -> S10 |
| EPA staging | `staging/epa/{param}/{year}/*.csv` | S10 -> S11/S12; `{param}` is ZIP group suffix |
| EPA curated | `curated/epa_hourly/year={YYYY}/pollutant={code}/*.parquet` | S12 -> S13; pollutant is observed parameter code |
| PurpleAir raw | `raw/purpleair/dt=YYYY-MM-DD/hh=HH/*.json` | S08 -> S14/S16 |
| AirNow raw | `raw/airnow/dt=YYYY-MM-DD/hh=HH/*.dat` | S09 -> S14 |

Raw, staging, and curated objects should carry producer run ID, source URI,
source version, UTC ingestion time, row count, and checksum in an ingestion
manifest or operational state table. Historical warehouse facts use
`monitor_key × UTC hour`; live PurpleAir and AirNow facts use the grains above.
SCD2 dimension rows use half-open `[valid_from_utc, valid_to_utc)` intervals;
facts resolve to the version valid at their observation hour. If an AQS
listing only supplies current metadata, do not invent earlier effective
dates; mark unknown history and state the limit of point-in-time joins.

## Source specifications

- [EPA AirData download catalog](https://aqs.epa.gov/aqsweb/airdata/download_files.html)
  and [pre-generated file formats](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html)
- [EPA PM2.5 data advisory](https://aqs.epa.gov/aqsweb/airdata/Data_Advisory_PM25_88101_Pregen_Files.pdf)
- [AirNow HourlyAQObs specification](https://docs.airnowapi.org/docs/HourlyAQObsFactSheet.pdf)
- [PurpleAir field descriptions](https://community.purpleair.com/t/api-fields-descriptions/4652)

The source specifications establish field meanings; the keys, target units,
and tie-breaking rules above are project decisions to validate in S03/S04 and
with sample files in S11.

## Review record

S01's written contract is ready for review. Each owner should record any
change request before implementing a dependent schema or job. No review is
claimed on another person's behalf.

| Owner | Handoff area | Status |
| --- | --- | --- |
| Yujie | Source fields, manifest, GCS paths | Pending |
| Breanna | Cloud SQL keys, EPA normalization, data quality | Pending |
| Bravo | Warehouse grains, SCD2, source crosswalks | Pending |
| Tongyan | Serving fields, configuration, application needs | Pending |
