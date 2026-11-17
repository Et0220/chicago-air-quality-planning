"""Review saved trial captures and rank a concrete follow-up sensor cohort."""

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median

from ingestion.airnow_collect import Bounds, parse_file
from ingestion.aqs_listings import CHICAGO_COUNTIES

START = datetime(2026, 11, 9, 7, tzinfo=UTC)
STOP = datetime(2026, 11, 12, 7, tzinfo=UTC)
MANUAL = {"20261109T073930Z", "20261109T074103Z", "20261109T213028Z"}


def distance(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 6371.0088 * 2 * math.asin(min(1, math.sqrt(h)))


def percentile(values, fraction):
    values = sorted(values)
    return values[round((len(values) - 1) * fraction)] if values else None


def quality(row):
    a, b, rh = (row.get(k) for k in ("pm2.5_cf_1_a", "pm2.5_cf_1_b", "humidity"))
    return (
        a is not None
        and b is not None
        and rh is not None
        and all(math.isfinite(x) for x in (a, b, rh))
        and a >= 0
        and b >= 0
        and 0 <= rh <= 100
        and row.get("location_type") == 0
        and 0 <= row.get("response_age_seconds", row["age_seconds"]) <= 180
        and abs(a - b) <= max(5, 0.2 * ((a + b) / 2))
    )


def review(root, output):
    captures, rows, per_sensor = [], [], defaultdict(list)
    for path in sorted((root / "purpleair").glob("*.json")):
        tag = re.search(r"purpleair_(\d{8}T\d{6}Z)_", path.name)[1]
        requested = datetime.strptime(tag, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        payload = json.loads(path.read_text())
        snapshot = payload.get("data_time_stamp") or payload["time_stamp"]
        manual = tag in MANUAL
        captures.append(
            {
                "name": path.name,
                "requested_at": requested.isoformat(),
                "manual": manual,
                "sensors": len(payload["data"]),
                "delay_seconds": requested.minute * 60 + requested.second,
            }
        )
        for raw in payload["data"]:
            row = dict(zip(payload["fields"], raw, strict=True))
            row.update(
                age_seconds=snapshot - row["last_seen"],
                response_age_seconds=payload["time_stamp"] - row["last_seen"],
                manual=manual,
                requested_at=requested.isoformat(),
            )
            row["hour"] = (
                datetime.fromtimestamp(row["last_seen"], UTC)
                .replace(minute=0, second=0, microsecond=0)
                .isoformat()
            )
            row["qc_pass"] = quality(row)
            rows.append(row)
            if not manual:
                per_sensor[row["sensor_index"]].append(row)
    scheduled = [c for c in captures if not c["manual"]]
    expected = [START + timedelta(hours=i) for i in range(1, 72)]
    actual_hours = [
        datetime.fromisoformat(c["requested_at"]).replace(
            minute=0, second=0, microsecond=0
        )
        for c in scheduled
    ]
    airnow, refs, by_hour = [], defaultdict(list), defaultdict(list)
    for item in json.loads((root / "airnow_selected.json").read_text()):
        hour = datetime.fromisoformat(item["hour"])
        if not START <= hour < STOP:
            continue
        observations = parse_file(Path(item["path"]).read_bytes(), hour, Bounds())
        airnow.extend(observations)
        for obs in observations:
            by_hour[hour.isoformat()].append(obs)
            if (
                obs.pollutant == "PM25"
                and obs.concentration is not None
                and obs.concentration >= 0
            ):
                refs[obs.airnow_site_id].append(obs)
    references = []
    for key, observations in sorted(refs.items()):
        obs = observations[-1]
        hours = {o.observed_at_utc.isoformat() for o in observations}
        references.append(
            {
                "site_id": key,
                "in_study_counties": (key[-9:][:2], key[-9:][2:5]) in CHICAGO_COUNTIES
                and (len(key) == 9 or key.startswith("840")),
                "site_name": obs.site_name,
                "latitude": obs.latitude,
                "longitude": obs.longitude,
                "valid_pm25_hours": len(hours),
                "hours": sorted(hours),
            }
        )
    active_refs = [
        r for r in references if r["valid_pm25_hours"] >= 65 and r["in_study_counties"]
    ]
    current_health = {}
    for filename in ("cohort_check.json", "cohort_backups_check.json"):
        path = root / filename
        if not path.exists():
            continue
        payload = json.loads(path.read_text())
        for raw in payload["data"]:
            row = dict(zip(payload["fields"], raw, strict=True))
            row["age_seconds"] = payload["data_time_stamp"] - row["last_seen"]
            row["response_age_seconds"] = payload["time_stamp"] - row["last_seen"]
            current_health[row["sensor_index"]] = row
    sensors = []
    for sensor, values in sorted(per_sensor.items()):
        coords = [
            (v["latitude"], v["longitude"])
            for v in values
            if v["latitude"] is not None and v["longitude"] is not None
        ]
        position = coords[-1]
        nearest = min(
            active_refs,
            key=lambda ref: distance(position, (ref["latitude"], ref["longitude"])),
        )
        available = len(values) / len(expected)
        passing = sum(v["qc_pass"] for v in values)
        report_hours = {v["hour"] for v in values}
        good_hours = {v["hour"] for v in values if v["qc_pass"]}
        moved = max(distance(position, c) for c in coords)
        item = {
            "sensor_id": sensor,
            "latitude": position[0],
            "longitude": position[1],
            "scheduled_captures": len(values),
            "availability": round(available, 5),
            "qc_pass_rows": passing,
            "qc_pass_fraction": round(passing / len(values), 5),
            "report_hours": len(report_hours),
            "qc_hours": len(good_hours),
            "humidity_missing": sum(v["humidity"] is None for v in values),
            "channel_a_missing": sum(v["pm2.5_cf_1_a"] is None for v in values),
            "channel_b_missing": sum(v["pm2.5_cf_1_b"] is None for v in values),
            "age_p95_seconds": percentile([v["age_seconds"] for v in values], 0.95),
            "position_change_km": round(moved, 4),
            "nearest_airnow_site": nearest["site_id"],
            "nearest_airnow_name": nearest["site_name"],
            "reference_distance_km": round(
                distance(position, (nearest["latitude"], nearest["longitude"])), 3
            ),
            "paired_qc_hours": len(good_hours.intersection(nearest["hours"])),
        }
        item["eligible"] = (
            available >= 0.95 and passing / len(values) >= 0.95 and moved <= 0.2
        )
        current = current_health.get(sensor)
        item["current_health_pass"] = (
            quality(current)
            and distance(position, (current["latitude"], current["longitude"])) <= 0.2
            if current
            else None
        )
        item["current_response_age_seconds"] = (
            current["response_age_seconds"] if current else None
        )
        sensors.append(item)
    eligible = sorted(
        [s for s in sensors if s["eligible"] and s["current_health_pass"] is not False],
        key=lambda s: (s["reference_distance_km"], -s["qc_hours"], s["sensor_id"]),
    )
    selected = []
    per_reference = Counter()
    # Cover reference sites first, then add at most a second sensor per site.
    for cap in (1, 2):
        for sensor in eligible:
            if len(selected) >= 20:
                break
            if sensor in selected or sensor["reference_distance_km"] > 10:
                continue
            if per_reference[sensor["nearest_airnow_site"]] >= cap:
                continue
            selected.append(sensor)
            per_reference[sensor["nearest_airnow_site"]] += 1
    # Fill only if necessary, retaining the same 10 km proximity constraint.
    for sensor in eligible:
        if len(selected) >= 20:
            break
        if sensor not in selected and sensor["reference_distance_km"] <= 10:
            selected.append(sensor)
    backups = [
        s
        for s in eligible
        if s not in selected
        and s["reference_distance_km"] <= 10
        and (not current_health or s["current_health_pass"])
    ][:5]
    unique = {}
    for row in rows:
        unique.setdefault((row["sensor_index"], row["last_seen"]), row)
    scheduled_rows = [r for r in rows if not r["manual"]]
    nulls = {
        k: sum(r[k] is None for r in rows)
        for k in ("humidity", "pm2.5_cf_1_a", "pm2.5_cf_1_b")
    }
    logs = json.loads((root / "pa_logs.json").read_text())
    success = []
    for entry in logs:
        message = entry.get("textPayload", "")
        if "outcome=success" in message:
            success.append(
                {
                    k: int(v)
                    for k, v in re.findall(
                        r"(sensors|inserted|duplicates|late|stale)=(\d+)", message
                    )
                }
            )
    summary = {
        "trial_start_utc": START.isoformat(),
        "trial_stop_utc": STOP.isoformat(),
        "evidence": "GCS raw versions and Cloud Run commit logs; no direct SQL read",
        "captures": len(captures),
        "scheduled_captures": len(scheduled),
        "manual_captures": len(captures) - len(scheduled),
        "expected_scheduled_captures": len(expected),
        "missing_schedule_hours": [
            h.isoformat() for h in expected if h not in actual_hours
        ],
        "duplicate_schedule_hours": len(actual_hours) - len(set(actual_hours)),
        "raw_rows": len(rows),
        "distinct_sensor_reports": len(unique),
        "duplicate_raw_reports": len(rows) - len(unique),
        "sensors": len({r["sensor_index"] for r in rows}),
        "sensors_per_capture_min": min(c["sensors"] for c in captures),
        "sensors_per_capture_max": max(c["sensors"] for c in captures),
        "null_rows": nulls,
        "stale_over_600_seconds": sum(r["age_seconds"] > 600 for r in rows),
        "age_median_seconds": median(r["age_seconds"] for r in rows),
        "age_p95_seconds": percentile([r["age_seconds"] for r in rows], 0.95),
        "response_age_p95_seconds": percentile(
            [r["response_age_seconds"] for r in rows], 0.95
        ),
        "scheduled_request_delay_median_seconds": median(
            c["delay_seconds"] for c in scheduled
        ),
        "scheduled_request_delay_max_seconds": max(
            c["delay_seconds"] for c in scheduled
        ),
        "scheduled_prior_hour_reports": sum(
            r["hour"]
            < datetime.fromisoformat(r["requested_at"])
            .replace(minute=0, second=0, microsecond=0)
            .isoformat()
            for r in scheduled_rows
        ),
        "scheduled_rows": len(scheduled_rows),
        "historically_quality_eligible_sensors": sum(s["eligible"] for s in sensors),
        "currently_disqualified": [
            s["sensor_id"] for s in sensors if s["current_health_pass"] is False
        ],
        "provisional_qc_pass_rows": sum(r["qc_pass"] for r in rows),
        "successful_poll_logs": len(success),
        "error_log_entries": sum(
            e.get("severity") in ("ERROR", "CRITICAL", "ALERT", "EMERGENCY")
            for e in logs
        ),
        "log_totals": dict(sum((Counter(x) for x in success), Counter())),
        "post_cutoff_skipped_executions": sum(
            "poll skipped" in x.get("textPayload", "") for x in logs
        ),
        "remaining_points": json.loads((root / "points.json").read_text())[
            "remaining_points"
        ],
        "remaining_points_before_health_checks": json.loads(
            (root / "points.json").read_text()
        ).get("before_health_checks"),
        "health_check_points": json.loads((root / "points.json").read_text()).get(
            "health_check_cost"
        ),
        "estimated_trial_points": 5 * len(captures) + 8 * len(rows),
        "airnow": {
            "hours": len(by_hour),
            "rows": len(airnow),
            "sites": len({o.airnow_site_id for o in airnow}),
            "pollutants": dict(Counter(o.pollutant for o in airnow)),
            "pm25_stations": len(refs),
            "study_pm25_stations_at_least_65_hours": len(active_refs),
        },
        "selected_sensor_ids": [s["sensor_id"] for s in selected],
        "backup_sensor_ids": [s["sensor_id"] for s in backups],
        "selected_reference_sites": len({s["nearest_airnow_site"] for s in selected}),
        "selected_max_distance_km": max(
            (s["reference_distance_km"] for s in selected), default=None
        ),
        "selected_paired_qc_hours": sum(s["paired_qc_hours"] for s in selected),
        "selected_all_pass_current_health": all(
            s["current_health_pass"] for s in selected
        ),
    }
    output.mkdir(parents=True, exist_ok=True)
    for name, data in [
        ("summary", summary),
        ("sensor_quality", sensors),
        ("selected_sensors", selected),
        ("backup_sensors", backups),
        ("airnow_reference_sites", references),
        ("captures", captures),
    ]:
        (output / (name + ".json")).write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("tmp/collection_review"))
    parser.add_argument("--output", type=Path, default=Path("output/collection_review"))
    args = parser.parse_args()
    review(args.root, args.output)
