"""S04: PurpleAir feasibility check for the Chicago metro.

Makes ONE live /v1/sensors call for outdoor sensors in a bounding box, measures
the points it consumed, and writes ``docs/purpleair_feasibility.md`` with real
numbers and a monthly-cost projection per polling interval.

The API key is read from ``AIR_QUALITY_PURPLEAIR_READ_KEY`` (environment or an
ignored ``.env``). It is never printed, logged, or written to any output file.

Usage:
    python -m ingestion.purpleair_check
    python -m ingestion.purpleair_check --fixture tmp/purpleair_sample.json \
        --measured-cost 1234          # offline: rebuild report from a saved call
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
API = "https://api.purpleair.com/v1"
KEY_VAR = "AIR_QUALITY_PURPLEAIR_READ_KEY"

# Chicago metro incl. NW Indiana: north-west corner, south-east corner.
BBOX = {"nwlng": -88.5, "nwlat": 42.5, "selng": -87.0, "selat": 41.3}
INTERVALS_MIN = [2, 10, 15, 30, 60, 120]
SAFETY = 0.8  # plan to use at most 80% of the remaining points
# Per-poll columns actually needed; lat/lon/location_type change rarely.
LEAN_FIELDS = ["sensor_index", "last_seen", "pm2.5_cf_1_a", "pm2.5_cf_1_b", "humidity"]
LEAN_COLUMNS = len(LEAN_FIELDS)


def load_env(path: Path = ROOT / ".env") -> None:
    """Load KEY=VALUE lines from .env without overriding the real environment."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            name, _, value = line.partition("=")
            os.environ.setdefault(name.strip(), value.strip().strip("'\""))


def api_get(path: str, key: str, params: dict | None = None) -> dict:
    url = f"{API}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"X-API-Key": key})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def remaining_points(key: str) -> int | None:
    """Points left on the organization, or None if the endpoint is unavailable."""
    try:
        value = api_get("/organization", key).get("remaining_points")
    except urllib.error.URLError, ValueError, OSError:
        return None
    return int(value) if value is not None else None


def settled_remaining(
    key: str, before: int | None, tries: int = 9, wait: float = 10
) -> int | None:
    """Re-read the balance until it changes: the portal balance updates with a lag
    (observed: an immediate re-read after a call showed no change)."""
    after = remaining_points(key)
    for _ in range(tries):
        if before is None or after is None or after != before:
            break
        time.sleep(wait)
        after = remaining_points(key)
    return after


def fetch_sensors(key: str, fields: list[str], max_age: int) -> dict:
    params = {
        "fields": ",".join(fields),
        "location_type": 0,  # outdoor only
        "max_age": max_age,
        **BBOX,
    }
    return api_get("/sensors", key, params)


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1)))]


def summarize(payload: dict) -> dict:
    """Profile one /v1/sensors response: counts, null rates, staleness, A/B gap."""
    columns = payload["fields"]
    rows = [dict(zip(columns, row, strict=False)) for row in payload["data"]]
    n = len(rows)
    null_rate = {
        name: (sum(r.get(name) is None for r in rows) / n if n else None)
        for name in columns
    }
    snapshot = payload.get("data_time_stamp") or payload.get("time_stamp")
    ages = [
        snapshot - r["last_seen"]
        for r in rows
        if snapshot and r.get("last_seen") is not None
    ]
    gaps = [
        abs(r["pm2.5_cf_1_a"] - r["pm2.5_cf_1_b"])
        for r in rows
        if r.get("pm2.5_cf_1_a") is not None and r.get("pm2.5_cf_1_b") is not None
    ]
    return {
        "sensor_count": n,
        "fields": columns,
        "max_age_seconds": payload.get("max_age"),
        "api_version": payload.get("api_version"),
        "null_rate": null_rate,
        "age_median_s": statistics.median(ages) if ages else None,
        "age_p90_s": _percentile(ages, 90),
        "ab_gap_median": statistics.median(gaps) if gaps else None,
        "ab_gap_p90": _percentile(gaps, 90),
    }


def project(cost_per_call: float, remaining: int | None, horizon_days: int) -> list:
    """Monthly and horizon cost per polling interval, and whether it fits."""
    table = []
    for minutes in INTERVALS_MIN:
        calls_per_day = 24 * 60 / minutes
        horizon = cost_per_call * calls_per_day * horizon_days
        table.append(
            {
                "interval_min": minutes,
                "calls_per_day": calls_per_day,
                "points_per_day": cost_per_call * calls_per_day,
                "points_30d": cost_per_call * calls_per_day * 30,
                "points_horizon": horizon,
                "fits": remaining is not None and horizon <= SAFETY * remaining,
            }
        )
    return table


def recommend(table: list) -> str:
    for row in table:
        if row["fits"]:
            return (
                f"Poll every **{row['interval_min']} min** "
                f"(~{row['points_horizon']:,.0f} points over the planning horizon)."
            )
    return (
        "No tested interval fits the budget. Reduce the field set or the "
        "bounding box, or lengthen the interval beyond 30 min."
    )


def _r(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1%}"


def render_report(
    summary: dict,
    cost_per_call: float | None,
    remaining_before: int | None,
    horizon_days: int,
    captured_at: str,
) -> str:
    lines = [
        "# PurpleAir feasibility (S04)",
        "",
        f"Generated {captured_at} from one live `/v1/sensors` call.",
        "",
        "## Request",
        "",
        f"- Bounding box: NW ({BBOX['nwlat']}, {BBOX['nwlng']}) to "
        f"SE ({BBOX['selat']}, {BBOX['selng']}) (Chicago metro incl. NW Indiana)",
        "- Outdoor sensors only (`location_type=0`)",
        f"- `max_age`: {summary['max_age_seconds']} s (only sensors seen recently)",
        f"- Fields: {', '.join(summary['fields'])}",
        f"- API version: {summary['api_version']}",
        "",
        "## Coverage and data quality (one snapshot)",
        "",
        f"- Sensors returned: **{summary['sensor_count']}**",
        f"- Report age at snapshot: median {summary['age_median_s']} s, "
        f"p90 {summary['age_p90_s']} s",
        f"- A/B channel |gap| (µg/m³): median {_r(summary['ab_gap_median'])}, "
        f"p90 {_r(summary['ab_gap_p90'])}",
        "",
        "| Field | Null rate |",
        "| --- | --- |",
        *[f"| `{k}` | {_pct(v)} |" for k, v in summary["null_rate"].items()],
        "",
        "## Points cost",
        "",
    ]
    if cost_per_call is None:
        lines += [
            "Points per call could not be measured (the organization endpoint did",
            "not return `remaining_points`). Read the balance in the PurpleAir",
            "developer portal before and after one call, then rerun with",
            "`--measured-cost <points>`.",
        ]
        return "\n".join(lines) + "\n"
    rows = max(summary["sensor_count"], 1)
    n_cols = len(summary["fields"])
    per_sensor = cost_per_call / rows
    per_cell = cost_per_call / (rows * n_cols)
    lines += [
        f"- Remaining points before the call: {remaining_before:,}"
        if remaining_before is not None
        else "- Remaining points before the call: unknown",
        f"- **Measured cost of one call: {cost_per_call:,.0f} points** "
        f"({per_sensor:.2f} per sensor row, {per_cell:.2f} per row x column cell)",
        f"- Planning horizon: {horizon_days} days (through the Dec 11 deadline); "
        f"budget rule: use at most {SAFETY:.0%} of remaining points",
        "- Cost model (estimate extrapolated from this one call): cost scales "
        "with sensors returned x columns returned. The reduced-column scenario "
        "below is a projection, not a measurement.",
        "",
    ]
    scenarios = [
        (f"as tested ({n_cols} columns)", cost_per_call),
        (
            f"lean polling ({LEAN_COLUMNS} columns: {', '.join(LEAN_FIELDS)}); "
            "static fields (lat/lon, location_type) fetched once per day",
            per_cell * rows * LEAN_COLUMNS,
        ),
    ]
    picks = []
    for label, cost in scenarios:
        table = project(cost, remaining_before, horizon_days)
        extra = horizon_days != 30
        header = ["Interval", "Calls/day", "Points/day", "Points/30 days"]
        header += [f"Points/{horizon_days} days"] if extra else []
        lines += [
            f"### Scenario: {label}",
            "",
            f"Estimated cost per call: {cost:,.0f} points",
            "",
            "| " + " | ".join([*header, "Fits budget"]) + " |",
            "|" + " --- |" * (len(header) + 1),
        ]
        for r in table:
            cells = [
                f"{r['interval_min']} min",
                f"{r['calls_per_day']:.0f}",
                f"{r['points_per_day']:,.0f}",
                f"{r['points_30d']:,.0f}",
            ]
            cells += [f"{r['points_horizon']:,.0f}"] if extra else []
            cells.append("yes" if r["fits"] else "no")
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
        picks.append((label, recommend(table)))
    lines += ["## Recommendation", ""]
    lines += [f"- {label.split(' (')[0]}: {text}" for label, text in picks]
    lines += [
        "",
        "Cost scales with the number of sensors returned. If the affordable "
        "cadence is too coarse for the hourly aggregation (S16), the lever is "
        "the sensor count, not just the interval:",
        "",
        "1. Run this discovery call (whole bounding box) once, then choose the "
        "sensors near the candidate venues and eligible reference monitors "
        "(S17/S18).",
        "2. Poll only those sensors (`show_only` list) with the lean column set.",
        "3. Re-measure the balance after the first day of polling and adjust.",
        "",
        "Caveat for S16: polling hourly yields about one reading per sensor-hour, "
        "so a minimum-completeness rule for hourly averages (e.g. a share of the "
        "hour's expected readings) cannot be met. Either define completeness for "
        "the chosen cadence (e.g. at least 2 of 4 readings at 15 min) or cut the "
        "sensor count to afford a 10-15 min cadence. Decide this before S08.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--max-age", type=int, default=600)
    parser.add_argument("--horizon-days", type=int, default=30)
    parser.add_argument("--fixture", help="saved /v1/sensors JSON; skips the live call")
    parser.add_argument("--measured-cost", type=float, help="points for one call")
    parser.add_argument("--remaining", type=int, help="points left before the call")
    parser.add_argument("--save-raw", default="tmp/purpleair_sample.json")
    parser.add_argument("--out", default="docs/purpleair_feasibility.md")
    args = parser.parse_args()

    settings = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
    fields = settings["purpleair"]["fields"]
    before = args.remaining
    after = None
    if args.fixture:
        payload = json.loads(Path(args.fixture).read_text())
    else:
        load_env()
        key = os.environ.get(KEY_VAR)
        if not key:
            raise SystemExit(f"{KEY_VAR} is not set (put it in .env).")
        before = remaining_points(key)
        payload = fetch_sensors(key, fields, args.max_age)
        after = settled_remaining(key, before)
        raw = ROOT / args.save_raw
        raw.parent.mkdir(parents=True, exist_ok=True)
        raw.write_text(json.dumps(payload))

    cost = args.measured_cost
    if cost is None and before is not None and after is not None:
        # The two organization calls are assumed free; verify in the portal.
        cost = float(before - after)
    summary = summarize(payload)
    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    report = render_report(summary, cost, before, args.horizon_days, stamp)
    out = ROOT / args.out
    out.write_text(report)
    print(f"sensors={summary['sensor_count']} cost={cost} report={out}")


if __name__ == "__main__":
    main()
