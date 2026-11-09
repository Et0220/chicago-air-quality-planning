"""S03 acceptance tests against saved EPA cells; no network or ZIP downloads."""

import csv
import json
from dataclasses import asdict
from pathlib import Path

import pytest
import yaml

from ingestion import epa_manifest as epa

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/epa_catalog_excerpt.html"
URL = "https://aqs.epa.gov/aqsweb/airdata/download_files.html"
GROUPS = yaml.safe_load((ROOT / "config/settings.yaml").read_text())["epa"][
    "hourly_groups"
]


@pytest.fixture
def html():
    return FIXTURE.read_text()


def test_saved_catalog_covers_every_year_group_and_keeps_empty_distinct(html):
    entries = epa.build_manifest(html, URL, GROUPS)
    assert len(entries) == 37 * 8
    assert {(entry.pollutant, entry.year) for entry in entries} == {
        (name, year) for name in GROUPS for year in range(1990, 2027)
    }
    lookup = {(entry.pollutant, entry.year): entry for entry in entries}
    ozone = lookup["ozone", 2026]
    assert asdict(ozone) == {
        "file_name": "hourly_44201_2026.zip",
        "pollutant": "ozone",
        "year": 2026,
        "url": "https://aqs.epa.gov/aqsweb/airdata/hourly_44201_2026.zip",
        "listed_rows": 1_906_626,
        "listed_bytes": 14_116 * 1024,
        "last_modified": "2026-06-25",
        "status": "listed_nonempty",
    }
    lead = lookup["lead", 2026]
    assert lead.status == "listed_empty"
    assert lead.file_name == "hourly_LEAD_2026.zip"
    assert lead.listed_rows == lead.listed_bytes == 0
    assert lookup["pm25_frm_fem", 1990].status == "listed_empty"
    missing = lookup["nitrogen_dioxide", 2025]
    assert missing.status == "missing"
    assert all(
        getattr(missing, field) is None
        for field in (
            "file_name",
            "url",
            "listed_rows",
            "listed_bytes",
            "last_modified",
        )
    )


def test_summary_totals_exclude_daily_and_out_of_range_files(html):
    entries = epa.build_manifest(html, URL, GROUPS, 2025)
    summary = epa.summarize(entries, GROUPS)
    expected_rows = sum(
        (
            1_906_626,
            741_160,
            368_027,
            805_803,
            8_867_266,
            2_940_386,
            1_674_907,
            1_979_862,
            335_702,
            850_829,
            8_367_660,
            1_599_992,
            3_949_057,
        )
    )
    expected_kb = sum(
        (
            14_116,
            4_928,
            2_788,
            6_621,
            65_874,
            19_295,
            12_406,
            15_989,
            2_670,
            7_098,
            66_951,
            12_485,
            32_188,
        )
    )
    assert summary["totals"] == {
        "cells": 16,
        "listed_nonempty": 13,
        "listed_empty": 2,
        "missing": 1,
        "listed_rows": expected_rows,
        "listed_bytes": expected_kb * 1024,
    }
    assert summary["by_pollutant"]["lead"]["flags"] == [
        "no_hourly_rows_listed",
        "longer_duration_samples_excluded",
    ]
    assert summary["by_pollutant"]["nitrogen_dioxide"]["missing_years"] == [2025]


def test_latest_hourly_year_extends_missing_grid_but_daily_does_not(html):
    html += '<td><a href="daily_44201_2030.zip">daily</a></td>'
    assert epa.build_manifest(html, URL, GROUPS)[-1].year == 2026
    # Another hourly group advances the catalog year even if requested groups lag.
    html += '<td><a href="hourly_WIND_2027.zip">wind</a></td>'
    entries = epa.build_manifest(html, URL, GROUPS, 2026)
    assert len(entries) == 16
    assert all(entry.status == "missing" for entry in entries if entry.year == 2027)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("1,906,626 Rows", "unknown Rows"),
        ("1,906,626 Rows", "-1 Rows"),
        ("1,906,626 Rows", "1,90,626 Rows"),
        ("14,116 KB", "14 MB"),
        ("14,116 KB", "14.5 KB"),
        ("As of 2026-06-25", "As of 2026-02-30"),
        ("As of 2026-06-25", "unknown date"),
    ],
)
def test_malformed_metadata_fails_instead_of_becoming_zero(html, old, new):
    with pytest.raises(ValueError):
        epa.build_manifest(html.replace(old, new), URL, GROUPS, 2025)


def test_identical_duplicate_links_are_idempotent_but_conflicts_fail(html):
    expected = epa.build_manifest(html, URL, GROUPS, 2025)
    assert epa.build_manifest(html + html, URL, GROUPS, 2025) == expected
    with pytest.raises(ValueError, match="Conflicting duplicate"):
        epa.build_manifest(html + html.replace("1,906,626 Rows", "1 Rows"), URL, GROUPS)


def test_absolute_urls_and_implicit_cell_end_are_supported():
    html = (
        '<table><tr><td><a href="https://aqs.epa.gov/aqsweb/airdata/'
        'hourly_LEAD_2026.zip">lead</a><br>0 Rows<br>1 KB<br>As of 2026-06-25'
        "<td>next cell</tr></table>"
    )
    entry = epa.build_manifest(html, URL, {"lead": "LEAD"}, 2026)[0]
    assert entry.status == "listed_empty"  # ZIP headers can still occupy bytes.
    assert entry.listed_bytes == 1024
    assert entry.url.endswith("/hourly_LEAD_2026.zip")


@pytest.mark.parametrize(
    "html",
    [
        "<html>Service unavailable</html>",
        '<a href="hourly_LEAD_2026.zip">link outside a table cell</a>',
    ],
)
def test_invalid_catalog_fails_instead_of_emitting_an_all_missing_manifest(html):
    with pytest.raises(ValueError):
        epa.build_manifest(html, URL, GROUPS)


@pytest.mark.parametrize(("start", "end"), [(2027, None), (2026, 2025), (1990, 2030)])
def test_invalid_ranges_fail(html, start, end):
    with pytest.raises(ValueError, match="Year range"):
        epa.build_manifest(html, URL, GROUPS, start, end)


def test_cli_offline_outputs_are_reproducible_and_print_totals(
    tmp_path, monkeypatch, capsys
):
    def unexpected_network(*args, **kwargs):
        pytest.fail("Offline mode must not use the network")

    monkeypatch.setattr(epa, "urlopen", unexpected_network)
    monkeypatch.setenv("AIR_QUALITY_PROJECT_HISTORY_START_YEAR", "2025")
    monkeypatch.setenv("AIR_QUALITY_EPA_CATALOG_URL", URL)
    args = ["--catalog-file", str(FIXTURE), "--output-dir", str(tmp_path)]
    assert epa.main(args) == 0
    first = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    assert epa.main(args) == 0
    assert first == {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    with (tmp_path / "manifest.csv").open(newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == list(epa.FIELDS)
        rows = list(reader)
    assert len(rows) == 16
    missing = next(row for row in rows if row["status"] == "missing")
    assert missing["listed_rows"] == missing["url"] == ""
    summary = json.loads(first["summary.json"])
    source = json.loads(first["catalog_source.json"])
    assert summary["catalog_sha256"] == source["catalog_sha256"]
    assert source["input_mode"] == "saved_html"
    assert "Listed rows:" in capsys.readouterr().out
    # A failed refresh must not replace a previously valid manifest.
    bad = tmp_path / "bad.html"
    bad.write_text("<html>error</html>")
    assert epa.main(["--catalog-file", str(bad), "--output-dir", str(tmp_path)]) == 1
    assert (tmp_path / "manifest.csv").read_bytes() == first["manifest.csv"]
    assert "No configured hourly files" in capsys.readouterr().err


def test_configured_codes_must_be_strings_and_unique(tmp_path, html):
    config = tmp_path / "settings.yaml"
    config.write_text(
        "project:\n  history_start_year: 1990\nepa:\n"
        f"  catalog_url: {URL}\n  hourly_groups:\n    ozone: 44201\n"
    )
    with pytest.raises(ValueError, match="must be strings"):
        epa.load_settings(config)
    with pytest.raises(ValueError, match="unique"):
        epa.build_manifest(html, URL, {"ozone": "44201", "duplicate": "44201"})
