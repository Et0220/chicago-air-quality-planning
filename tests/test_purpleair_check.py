"""Offline tests for the S04 feasibility helpers (no network, no key)."""

from ingestion import purpleair_check as pc

PAYLOAD = {
    "api_version": "V1.0.test",
    "time_stamp": 1000,
    "data_time_stamp": 1000,
    "max_age": 600,
    "fields": [
        "sensor_index",
        "last_seen",
        "latitude",
        "longitude",
        "location_type",
        "pm2.5_cf_1_a",
        "pm2.5_cf_1_b",
        "humidity",
    ],
    "data": [
        [1, 990, 41.9, -87.6, 0, 10.0, 12.0, 50],
        [2, 900, 41.8, -87.7, 0, 20.0, 20.0, None],
        [3, 940, 41.7, -87.5, 0, None, 5.0, 40],
    ],
}


def test_summarize_counts_nulls_age_and_ab_gap() -> None:
    s = pc.summarize(PAYLOAD)
    assert s["sensor_count"] == 3
    assert s["null_rate"]["humidity"] == 1 / 3
    assert s["null_rate"]["pm2.5_cf_1_a"] == 1 / 3
    assert s["age_median_s"] == 60  # ages 10, 100, 60
    assert s["ab_gap_median"] == 1.0  # gaps 2.0 and 0.0 -> median 1.0


def test_project_scales_inversely_with_interval_and_checks_budget() -> None:
    table = pc.project(cost_per_call=1000, remaining=1_000_000, horizon_days=30)
    by_interval = {row["interval_min"]: row for row in table}
    assert by_interval[2]["calls_per_day"] == 720
    assert by_interval[10]["points_per_day"] == 144_000
    # 10 min: 144k/day * 30 = 4.32M > 800k budget; 30 min: 48k * 30 = 1.44M > 800k
    assert not by_interval[10]["fits"]
    cheap = pc.project(cost_per_call=10, remaining=1_000_000, horizon_days=30)
    assert all(row["fits"] for row in cheap)  # 10*720*30 = 216k <= 800k


def test_recommend_picks_smallest_fitting_interval() -> None:
    table = pc.project(cost_per_call=200, remaining=1_000_000, horizon_days=30)
    # 2 min: 200*720*30 = 4.3M (no); 10 min: 864k (no); 15 min: 576k (yes)
    assert "15 min" in pc.recommend(table)


def test_report_has_no_duplicate_columns_and_covers_hourly() -> None:
    report = pc.render_report(
        pc.summarize(PAYLOAD), 500, 1_000_000, 30, "2026-11-09 00:00 UTC"
    )
    assert report.count("Points/30 days") == 2  # once per scenario table
    assert "60 min" in report and "120 min" in report
    assert "lean polling" in report


def test_report_never_contains_the_key(monkeypatch) -> None:
    monkeypatch.setenv(pc.KEY_VAR, "SECRET-KEY-VALUE")
    report = pc.render_report(
        pc.summarize(PAYLOAD), 500, 1_000_000, 30, "2026-11-09 00:00 UTC"
    )
    assert "SECRET-KEY-VALUE" not in report
    assert "Measured cost of one call: 500 points" in report


def test_report_handles_unmeasured_cost() -> None:
    report = pc.render_report(pc.summarize(PAYLOAD), None, None, 30, "now")
    assert "--measured-cost" in report
