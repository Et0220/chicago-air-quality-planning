"""Offline AQS listing tests using small EPA-shaped ZIPs."""

import csv
import io
import zipfile

import pytest

from ingestion import aqs_listings as aqs


def _zip(path, stem, rows):
    columns = sorted(set().union(*(row.keys() for row in rows)))
    content = io.StringIO()
    writer = csv.DictWriter(content, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(stem + ".csv", content.getvalue())


def _site(state, county, site):
    row = dict.fromkeys(aqs.SITE_FIELDS, "")
    row.update(
        {
            "State Code": state,
            "County Code": county,
            "Site Number": site,
            "Latitude": "41.9",
            "Longitude": "-87.6",
            "Datum": "WGS84",
            "Local Site Name": "TEST SITE",
            "Site Established Date": "2010-01-02",
            "Extraction Date": "2026-06-26",
        }
    )
    return row


def _monitor(state, county, site, parameter, poc, method):
    row = dict.fromkeys(aqs.MONITOR_FIELDS, "")
    row.update(
        {
            "State Code": state,
            "County Code": county,
            "Site Number": site,
            "Parameter Code": parameter,
            "POC": poc,
            "Latitude": "41.9",
            "Longitude": "-87.6",
            "Datum": "WGS84",
            "First Year of Data": "2017",
            "Last Sample Date": "2026-04-30",
            "Last Method Code": method,
            "Last Method": "Example method",
            "Local Site Name": "TEST SITE",
            "Extraction Date": "2026-06-26",
        }
    )
    return row


def test_full_listing_keys_chicago_report_and_advisory(tmp_path):
    sites = tmp_path / "aqs_sites.zip"
    monitors = tmp_path / "aqs_monitors.zip"
    _zip(sites, "aqs_sites", [_site("17", "031", "1"), _site("CC", "004", "2")])
    _zip(
        monitors,
        "aqs_monitors",
        [
            _monitor("17", "031", "1", "88101", "1", "236"),
            _monitor("17", "031", "1", "88101", "3", "736"),
            _monitor("CC", "004", "2", "44201", "1", "001"),
        ],
    )
    counts, chicago = aqs.inspect(sites, monitors)
    assert counts == {"sites": 2, "monitors": 3, "chicago_pm25": 2}
    assert [row["monitor_key"] for row in chicago] == [
        "17-031-0001-88101-01",
        "17-031-0001-88101-03",
    ]
    assert [row["pm25_advisory_superseded"] for row in chicago] == [True, False]
    assert chicago[0]["first_year_of_data"] == 2017
    assert chicago[0]["last_sample_date"].isoformat() == "2026-04-30"
    output = tmp_path / "chicago.csv"
    aqs.write_report(chicago, output)
    with output.open(newline="") as handle:
        assert len(list(csv.DictReader(handle))) == 2


def test_rejects_orphan_monitor(tmp_path):
    sites = tmp_path / "aqs_sites.zip"
    monitors = tmp_path / "aqs_monitors.zip"
    _zip(sites, "aqs_sites", [_site("17", "031", "1")])
    _zip(monitors, "aqs_monitors", [_monitor("17", "043", "2", "88101", "1", "636")])
    with pytest.raises(ValueError, match="no site listing"):
        aqs.inspect(sites, monitors)


def test_batch_insert_uses_one_statement_for_multiple_rows():
    statement, params = aqs._batch_statement(aqs.SITE_UPSERT, [(1,) * 19, (2,) * 19])
    assert statement.count("ON CONFLICT") == 1
    assert statement.count("%s") == 38
    assert params == (1,) * 19 + (2,) * 19
