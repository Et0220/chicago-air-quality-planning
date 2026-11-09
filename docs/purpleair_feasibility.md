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
