"""Preview the morning-versus-afternoon PM2.5 comparison for the Chicago area.

Reads one EPA hourly PM2.5 (parameter 88101) archive ZIP, keeps the Chicago-area
monitors listed by S07, and compares the planned session windows (morning 06-09,
afternoon 16-19, Chicago clock time). Hours are bucketed by UTC and converted to
`America/Chicago`, so daylight-saving changes move no observation. This is a
preliminary look at one partial archive year, not the S19 warehouse analysis.
"""

import argparse
import csv
import io
import json
import statistics
import zipfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ingestion.aqs_listings import CHICAGO_COUNTIES, SUPERSEDED_METHODS

PARAMETER = "88101"
CHICAGO = ZoneInfo("America/Chicago")
MORNING = (6, 7, 8)
AFTERNOON = (16, 17, 18)
MIN_WINDOW_HOURS = 2
MIN_REGION_MONITORS = 3
ELEVATED_UG_M3 = 35.0
MONITORS_CSV = Path("output/aqs/chicago_pm25_monitors.csv")
OUT_DIR = Path("output/analysis")


def monitor_key(row: dict[str, str]) -> str:
    parts = (row["State Code"], row["County Code"], row["Site Num"])
    return "-".join((*parts, row["Parameter Code"], row["POC"].zfill(2)))


def read_hours(zip_path: Path) -> tuple[dict, dict]:
    """Return one value per (monitor, UTC hour) and the row accounting."""
    best: dict[tuple[str, datetime], tuple[tuple, float]] = {}
    counts = {"rows_read": 0, "chicago_rows": 0, "null_values": 0, "replaced": 0}
    with zipfile.ZipFile(zip_path) as archive:
        (member,) = archive.namelist()
        with archive.open(member) as raw:
            for row in csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8")):
                counts["rows_read"] += 1
                if (row["State Code"], row["County Code"]) not in CHICAGO_COUNTIES:
                    continue
                counts["chicago_rows"] += 1
                if not row["Sample Measurement"]:
                    counts["null_values"] += 1
                    continue
                hour = datetime.strptime(
                    f"{row['Date GMT']} {row['Time GMT'][:2]}", "%Y-%m-%d %H"
                ).replace(tzinfo=UTC)
                # EPA advisory: eligible methods precede superseded ones, then the
                # latest revision wins. This mirrors spark.normalize_epa.
                rank = (
                    row["Method Code"] not in SUPERSEDED_METHODS,
                    row["Date of Last Change"],
                )
                key = (monitor_key(row), hour)
                if key in best:
                    counts["replaced"] += 1
                if key not in best or rank > best[key][0]:
                    best[key] = (rank, float(row["Sample Measurement"]))
    return {key: value for key, (_, value) in best.items()}, counts


def window_means(values: dict) -> dict:
    """Group hourly values into (monitor, local date, window) means."""
    groups: dict[tuple[str, object, str], list[float]] = defaultdict(list)
    profile: dict[int, list[float]] = defaultdict(list)
    for (monitor, hour), value in values.items():
        local = hour.astimezone(CHICAGO)
        profile[local.hour].append(value)
        if local.hour in MORNING:
            groups[(monitor, local.date(), "morning")].append(value)
        elif local.hour in AFTERNOON:
            groups[(monitor, local.date(), "afternoon")].append(value)
    return {"groups": groups, "profile": profile}


def paired_days(groups: dict) -> list[tuple[str, object, float, float]]:
    """Return (monitor, date, morning mean, afternoon mean) for complete pairs."""
    pairs = []
    for monitor, day, window in list(groups):
        if window != "morning":
            continue
        morning = groups[(monitor, day, "morning")]
        afternoon = groups.get((monitor, day, "afternoon"), [])
        if min(len(morning), len(afternoon)) >= MIN_WINDOW_HOURS:
            pairs.append(
                (monitor, day, statistics.fmean(morning), statistics.fmean(afternoon))
            )
    return sorted(pairs, key=lambda pair: (pair[0], pair[1]))


def share(count: int, total: int) -> float | None:
    return round(count / total, 4) if total else None


def rounded(function, items: list[float], minimum: int = 1) -> float | None:
    return round(function(items), 3) if len(items) >= minimum else None


def summarize(values: dict, counts: dict) -> dict:
    grouped = window_means(values)
    groups, profile = grouped["groups"], grouped["profile"]
    pairs = paired_days(groups)
    diffs = [after - before for _, _, before, after in pairs]
    by_day: dict[object, list[float]] = defaultdict(list)
    for _, day, before, after in pairs:
        by_day[day].append(after - before)
    regional = [
        statistics.fmean(items)
        for items in by_day.values()
        if len(items) >= MIN_REGION_MONITORS
    ]
    elevated = {}
    for name, hours in (("morning", MORNING), ("afternoon", AFTERNOON)):
        window = [
            value
            for (_, hour), value in values.items()
            if hour.astimezone(CHICAGO).hour in hours
        ]
        elevated[name] = {
            "valid_hours": len(window),
            "elevated_hours": sum(value > ELEVATED_UG_M3 for value in window),
            "elevated_share": share(
                sum(value > ELEVATED_UG_M3 for value in window), len(window)
            ),
            "mean": round(statistics.fmean(window), 3),
        }
    local_days = sorted({hour.astimezone(CHICAGO).date() for _, hour in values})
    return {
        "scope": "preliminary; one partial archive year; not the S19 analysis",
        "parameter": PARAMETER,
        "windows_local": {"morning": "06-09", "afternoon": "16-19"},
        "elevated_threshold_ug_m3": ELEVATED_UG_M3,
        "min_window_hours": MIN_WINDOW_HOURS,
        "min_region_monitors": MIN_REGION_MONITORS,
        "rows": counts,
        "monitor_hours": len(values),
        "monitors": len({monitor for monitor, _ in values}),
        "first_local_date": local_days[0].isoformat(),
        "last_local_date": local_days[-1].isoformat(),
        "paired_monitor_days": len(pairs),
        "paired_monitors": len({pair[0] for pair in pairs}),
        "monitor_day_diff_median": rounded(statistics.median, diffs),
        "monitor_day_diff_mean": rounded(statistics.fmean, diffs),
        "monitor_day_afternoon_lower": share(sum(d < 0 for d in diffs), len(diffs)),
        "regional_days": len(regional),
        "regional_diff_median": rounded(statistics.median, regional),
        "regional_diff_mean": rounded(statistics.fmean, regional),
        "regional_diff_stdev": rounded(statistics.stdev, regional, 2),
        "regional_afternoon_lower": share(sum(d < 0 for d in regional), len(regional)),
        "windows": elevated,
        "profile_mean_morning": round(
            statistics.fmean(v for h in MORNING for v in profile[h]), 3
        ),
        "profile_mean_afternoon": round(
            statistics.fmean(v for h in AFTERNOON for v in profile[h]), 3
        ),
    }


def write_profile(path: Path, values: dict) -> None:
    profile = window_means(values)["profile"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["local_hour", "window", "monitor_hours", "mean", "median"])
        for hour in range(24):
            items = profile[hour]
            window = (
                "morning"
                if hour in MORNING
                else "afternoon"
                if hour in AFTERNOON
                else ""
            )
            writer.writerow(
                [
                    hour,
                    window,
                    len(items),
                    round(statistics.fmean(items), 3),
                    round(statistics.median(items), 3),
                ]
            )


def write_monitors(path: Path, values: dict, monitors_csv: Path) -> None:
    listing = {}
    with monitors_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            listing[row["monitor_key"]] = row
    pairs = defaultdict(list)
    for monitor, _, before, after in paired_days(window_means(values)["groups"]):
        pairs[monitor].append((before, after))
    hours = defaultdict(list)
    for (monitor, _), value in values.items():
        hours[monitor].append(value)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "monitor_key",
                "county",
                "site_name",
                "latitude",
                "longitude",
                "hours",
                "mean_all_hours",
                "paired_days",
                "morning_mean",
                "afternoon_mean",
                "median_diff",
            ]
        )
        for monitor in sorted(hours):
            info = listing.get(monitor, {})
            rows = pairs.get(monitor, [])
            writer.writerow(
                [
                    monitor,
                    info.get("county", ""),
                    info.get("site_name", ""),
                    info.get("latitude", ""),
                    info.get("longitude", ""),
                    len(hours[monitor]),
                    round(statistics.fmean(hours[monitor]), 3),
                    len(rows),
                    round(statistics.fmean(b for b, _ in rows), 3) if rows else "",
                    round(statistics.fmean(a for _, a in rows), 3) if rows else "",
                    round(statistics.median(a - b for b, a in rows), 3) if rows else "",
                ]
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("zip_path", type=Path, help="hourly_88101_<year>.zip")
    parser.add_argument("--monitors", type=Path, default=MONITORS_CSV)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()
    values, counts = read_hours(args.zip_path)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize(values, counts)
    (args.out_dir / "prelim_window_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    write_profile(args.out_dir / "prelim_hourly_profile.csv", values)
    write_monitors(args.out_dir / "prelim_monitor_windows.csv", values, args.monitors)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
