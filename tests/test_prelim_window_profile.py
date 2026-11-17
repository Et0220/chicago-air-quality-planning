import csv
import io
import zipfile

from analysis import prelim_window_profile as prelim

COLUMNS = [
    "State Code",
    "County Code",
    "Site Num",
    "Parameter Code",
    "POC",
    "Date GMT",
    "Time GMT",
    "Sample Measurement",
    "Method Code",
    "Date of Last Change",
]


def row(date, hour, value, *, method="636", changed="2026-05-01", state="17"):
    return dict(
        zip(
            COLUMNS,
            [
                state,
                "031",
                "0001",
                "88101",
                "3",
                date,
                f"{hour:02d}:00",
                str(value),
                method,
                changed,
            ],
            strict=True,
        )
    )


def make_zip(tmp_path, rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    path = tmp_path / "hourly_88101_2026.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("hourly_88101_2026.csv", buffer.getvalue())
    return path


def window_rows(date, utc_morning, utc_afternoon, morning, afternoon):
    rows = [row(date, hour, morning) for hour in utc_morning]
    return rows + [row(date, hour, afternoon) for hour in utc_afternoon]


def test_summary_converts_utc_to_chicago_time_across_dst(tmp_path):
    rows = (
        # July is CDT (UTC-5): local 06-08 is 11-13 UTC, local 16-18 is 21-23 UTC.
        window_rows("2026-07-01", (11, 12, 13), (21, 22, 23), 10, 7)
        # January is CST (UTC-6): local 06-08 is 12-14 UTC, local 16-18 is 22-24.
        + window_rows("2026-01-15", (12, 13, 14), (22, 23), 5, 6)
        + [row("2026-01-16", 0, 6)]
        + [row("2026-07-01", 12, 500, state="06")]
    )
    values, counts = prelim.read_hours(make_zip(tmp_path, rows))
    summary = prelim.summarize(values, counts)

    assert counts["rows_read"] == len(rows)
    assert counts["chicago_rows"] == len(rows) - 1
    assert summary["paired_monitor_days"] == 2
    assert summary["monitor_day_diff_mean"] == -1.0
    assert summary["monitor_day_afternoon_lower"] == 0.5
    assert summary["regional_days"] == 0
    assert summary["regional_diff_median"] is None


def test_eligible_method_beats_superseded_method(tmp_path):
    rows = [
        row("2026-07-01", 11, 100, method="236", changed="2026-06-30"),
        row("2026-07-01", 11, 10, method="636", changed="2026-01-01"),
        row("2026-07-01", 12, 10),
        row("2026-07-01", 21, 7),
        row("2026-07-01", 22, 7),
    ]
    values, counts = prelim.read_hours(make_zip(tmp_path, rows))

    assert counts["replaced"] == 1
    assert sorted(values.values()) == [7, 7, 10, 10]


def test_window_needs_two_valid_hours(tmp_path):
    rows = [row("2026-07-01", 11, 10), row("2026-07-01", 21, 7)]
    rows += [row("2026-07-01", 22, 7)]
    values, counts = prelim.read_hours(make_zip(tmp_path, rows))

    assert prelim.paired_days(prelim.window_means(values)["groups"]) == []
