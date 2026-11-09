# PurpleAir feasibility (S04)

Generated 2026-11-09 05:38 UTC from one live `/v1/sensors` call.

## Request

- Bounding box: NW (42.5, -88.5) to SE (41.3, -87.0) (Chicago metro incl. NW Indiana)
- Outdoor sensors only (`location_type=0`)
- `max_age`: 600 s (only sensors seen recently)
- Fields: sensor_index, last_seen, location_type, latitude, longitude, humidity, pm2.5_cf_1_a, pm2.5_cf_1_b
- API version: V1.2.3-1.1.45

## Coverage and data quality (one snapshot)

- Sensors returned: **131**
- Report age at snapshot: median 65 s, p90 114 s
- A/B channel |gap| (µg/m³): median 0.85, p90 4.60

| Field | Null rate |
| --- | --- |
| `sensor_index` | 0.0% |
| `last_seen` | 0.0% |
| `location_type` | 0.0% |
| `latitude` | 0.0% |
| `longitude` | 0.0% |
| `humidity` | 6.9% |
| `pm2.5_cf_1_a` | 0.8% |
| `pm2.5_cf_1_b` | 0.0% |

## Points cost

- Remaining points before the call: 998,947
- **Measured cost of one call: 1,053 points** (8.04 per sensor row, 1.00 per row x column cell)
- Planning horizon: 30 days (through the Dec 11 deadline); budget rule: use at most 80% of remaining points
- Cost model (estimate extrapolated from this one call): cost scales with sensors returned x columns returned. The reduced-column scenario below is a projection, not a measurement.

### Scenario: as tested (8 columns)

Estimated cost per call: 1,053 points

| Interval | Calls/day | Points/day | Points/30 days | Fits budget |
| --- | --- | --- | --- | --- |
| 2 min | 720 | 758,160 | 22,744,800 | no |
| 10 min | 144 | 151,632 | 4,548,960 | no |
| 15 min | 96 | 101,088 | 3,032,640 | no |
| 30 min | 48 | 50,544 | 1,516,320 | no |
| 60 min | 24 | 25,272 | 758,160 | yes |
| 120 min | 12 | 12,636 | 379,080 | yes |

### Scenario: lean polling (5 columns: sensor_index, last_seen, pm2.5_cf_1_a, pm2.5_cf_1_b, humidity); static fields (lat/lon, location_type) fetched once per day

Estimated cost per call: 658 points

| Interval | Calls/day | Points/day | Points/30 days | Fits budget |
| --- | --- | --- | --- | --- |
| 2 min | 720 | 473,850 | 14,215,500 | no |
| 10 min | 144 | 94,770 | 2,843,100 | no |
| 15 min | 96 | 63,180 | 1,895,400 | no |
| 30 min | 48 | 31,590 | 947,700 | no |
| 60 min | 24 | 15,795 | 473,850 | yes |
| 120 min | 12 | 7,898 | 236,925 | yes |

## Recommendation

- as tested: Poll every **60 min** (~758,160 points over the planning horizon).
- lean polling: Poll every **60 min** (~473,850 points over the planning horizon).

Cost scales with the number of sensors returned. If the affordable cadence is too coarse for the hourly aggregation (S16), the lever is the sensor count, not just the interval:

1. Run this discovery call (whole bounding box) once, then choose the sensors near the candidate venues and eligible reference monitors (S17/S18).
2. Poll only those sensors (`show_only` list) with the lean column set.
3. Re-measure the balance after the first day of polling and adjust.

Caveat for S16: polling hourly yields about one reading per sensor-hour, so a minimum-completeness rule for hourly averages (e.g. a share of the hour's expected readings) cannot be met. Either define completeness for the chosen cadence (e.g. at least 2 of 4 readings at 15 min) or cut the sensor count to afford a 10-15 min cadence. Decide this before S08.

## Live pilot across repeated captures (added by hand, 2026-11-09)

The sections above come from one call. The S08 poller has since stored the raw
response of every call under `raw/purpleair/`; this section measures those 17
captures (07:39 to 21:30 UTC, same bounding box, 8 columns, `max_age` 900 s).
It is not produced by `purpleair_check.py`: regenerating that report replaces
the file, so re-add this section afterwards.

| Measure | Result |
| --- | --- |
| Captures analysed | 17 (3 manual, 14 scheduled hourly) |
| Sensors per capture | 130 to 131; 132 distinct overall, 130 present in every capture |
| Distinct `last_seen` values in one capture | 76 to 87 across about 130 sensors (sensors do not report in lockstep) |
| Report age at snapshot | median 52 to 97 s; p90 106 to 139 s; max 119 to 829 s |
| Rows with a null `humidity` | 7.1% |
| Rows with a null `pm2.5_cf_1_a` | 0.8% (`pm2.5_cf_1_b` and location fields never null) |
| Distinct (sensor, report time) pairs | 2,189 of 2,218 rows |

**Reporting cadence.** Two manual calls 93 s apart (07:39:30 and 07:41:03)
returned 102 sensors with a new report time and 29 repeats, so most sensors
report about every 2 minutes. Across the 14 hourly captures no (sensor,
report time) pair repeats; the 29 repeats all come from that 93 s pair.

**Points cost at the selected interval.** Measured 1,053 points per call
(first section). The selected interval is 60 min (`target_poll_seconds: 3600`):
24 calls/day = 25,272 points/day = 758,160 points per 30 days, inside the
80% budget rule. The 72-hour test costs about 76,000 points.

**Limit of this evidence.** Hourly polling samples one report per sensor per
hour. The 2-minute and 10 to 15 minute cadences do not fit the points budget
for the whole box, as the scenario tables show.
