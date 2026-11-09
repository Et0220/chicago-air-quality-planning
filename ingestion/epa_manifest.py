"""Build the S03 EPA hourly catalog manifest without downloading ZIP files.

Run ``python -m ingestion.epa_manifest`` from the repository root. Use
``--catalog-file output/epa/catalog.html`` to reproduce a saved catalog offline.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
FILE_PATTERN = re.compile(r"hourly_([A-Za-z0-9]+)_(\d{4})\.zip")
NUMBER = r"(?:\d{1,3}(?:,\d{3})+|\d+)"
KB_TO_BYTES = 1024  # Explicit approximation; the catalog does not define its KB.
STATUSES = ("listed_nonempty", "listed_empty", "missing")
FIELDS = (
    "file_name",
    "pollutant",
    "year",
    "url",
    "listed_rows",
    "listed_bytes",
    "last_modified",
    "status",
)


@dataclass(frozen=True)
class ManifestEntry:
    file_name: str | None
    pollutant: str
    year: int
    url: str | None
    listed_rows: int | None
    listed_bytes: int | None
    last_modified: str | None
    status: str


class CatalogParser(HTMLParser):
    """Read metadata from the same table cell as each hourly ZIP link."""

    def __init__(self, catalog_url: str, groups: dict[str, str]) -> None:
        super().__init__(convert_charrefs=True)
        self.catalog_url = catalog_url
        self.pollutants = {code: name for name, code in groups.items()}
        self.entries: dict[tuple[str, int], ManifestEntry] = {}
        self.years: set[int] = set()
        self.parts: list[str] | None = None
        self.links: set[tuple[str, str, int, str]] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"td", "th", "tr"}:
            self.finish_cell()
        if tag == "td":
            self.parts = []
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        url = urljoin(self.catalog_url, href)
        file_name = urlsplit(url).path.rsplit("/", 1)[-1]
        match = FILE_PATTERN.fullmatch(file_name)
        if not match:
            return
        code, year_text = match.groups()
        year = int(year_text)
        self.years.add(year)
        if code not in self.pollutants:
            return
        if self.parts is None:
            raise ValueError(f"Hourly link outside a table cell: {file_name}")
        if urlsplit(url).scheme not in {"http", "https"}:
            raise ValueError(f"Invalid download URL for {file_name}")
        self.links.add((file_name, code, year, url))

    def handle_data(self, data: str) -> None:
        if self.parts is not None:
            self.parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th", "tr", "table"}:
            self.finish_cell()

    def finish_cell(self) -> None:
        if self.links:
            if len(self.links) != 1:
                raise ValueError("Multiple hourly files in one catalog cell")
            file_name, code, year, url = next(iter(self.links))
            text = " ".join(self.parts or [])
            rows = self.metadata(text, rf"(?:^|\s)({NUMBER})\s+Rows?\b", file_name)
            kb = self.metadata(text, rf"(?:^|\s)({NUMBER})\s+KB\b", file_name)
            as_of = self.metadata(text, r"\bAs\s+of\s+(\d{4}-\d{2}-\d{2})\b", file_name)
            date.fromisoformat(as_of)  # Reject impossible dates rather than guessing.
            count = int(rows.replace(",", ""))
            entry = ManifestEntry(
                file_name,
                self.pollutants[code],
                year,
                url,
                count,
                int(kb.replace(",", "")) * KB_TO_BYTES,
                as_of,
                "listed_nonempty" if count else "listed_empty",
            )
            key = (code, year)
            if key in self.entries and self.entries[key] != entry:
                raise ValueError(f"Conflicting duplicate catalog entry: {file_name}")
            self.entries[key] = entry
        self.parts = None
        self.links.clear()

    @staticmethod
    def metadata(text: str, pattern: str, file_name: str) -> str:
        matches = re.findall(pattern, text, flags=re.IGNORECASE)
        if len(matches) != 1:
            raise ValueError(f"Missing or ambiguous catalog metadata: {file_name}")
        return matches[0]


def build_manifest(
    html: str,
    catalog_url: str,
    groups: dict[str, str],
    start_year: int = 1990,
    end_year: int | None = None,
) -> list[ManifestEntry]:
    """Complete the year × group grid, preserving nulls for absent cells."""
    if not groups or len(set(groups.values())) != len(groups):
        raise ValueError("Hourly groups must be nonempty with unique file codes")
    parser = CatalogParser(catalog_url, groups)
    parser.feed(html)
    parser.close()
    parser.finish_cell()
    if not parser.entries:
        raise ValueError("No configured hourly files found; check the catalog format")
    latest = max(parser.years)
    end_year = latest if end_year is None else end_year
    if not 1900 <= start_year <= end_year <= latest:
        raise ValueError(f"Year range must be within 1900–{latest} and ascending")
    return [
        parser.entries.get(
            (code, year),
            ManifestEntry(None, pollutant, year, None, None, None, None, "missing"),
        )
        for year in range(start_year, end_year + 1)
        for pollutant, code in sorted(groups.items())
    ]


def summarize(entries: list[ManifestEntry], groups: dict[str, str]) -> dict:
    """Catalog counts, not measured download sizes or deduplicated row counts."""

    def totals(rows: list[ManifestEntry]) -> dict:
        counts = Counter(row.status for row in rows)
        return {
            "cells": len(rows),
            **{status: counts[status] for status in STATUSES},
            "listed_rows": sum(row.listed_rows or 0 for row in rows),
            "listed_bytes": sum(row.listed_bytes or 0 for row in rows),
        }

    by_pollutant = {}
    for pollutant, code in sorted(groups.items()):
        rows = [row for row in entries if row.pollutant == pollutant]
        nonempty_years = [row.year for row in rows if row.status == "listed_nonempty"]
        by_pollutant[pollutant] = {
            "file_group": code,
            **totals(rows),
            "first_nonempty_year": min(nonempty_years, default=None),
            "last_nonempty_year": max(nonempty_years, default=None),
            "empty_years": [row.year for row in rows if row.status == "listed_empty"],
            "missing_years": [row.year for row in rows if row.status == "missing"],
            "flags": ["no_hourly_rows_listed"] if not nonempty_years else [],
        }
        if code == "LEAD":
            by_pollutant[pollutant]["flags"].append("longer_duration_samples_excluded")
    return {
        "start_year": min(row.year for row in entries),
        "end_year": max(row.year for row in entries),
        "kb_to_bytes": KB_TO_BYTES,
        "listed_bytes_are_approximate": True,
        "catalog_as_of_dates": sorted(
            {row.last_modified for row in entries if row.last_modified}
        ),
        "totals": totals(entries),
        "by_pollutant": by_pollutant,
        "notes": [
            "Listed sizes estimate compressed ZIP bytes using catalog KB × 1024; "
            "the catalog does not define KB. Verify actual bytes during S06.",
            "Zero hourly rows do not imply no measurements: longer-duration "
            "samples are excluded from hourly files, including lead samples.",
            "Missing entries have unknown rows/bytes and are excluded from totals.",
            "Latest-year counts may be partial; these are national source rows "
            "before deduplication or Chicago filtering.",
        ],
    }


def load_settings(path: Path) -> tuple[str, int, dict[str, str]]:
    settings = yaml.safe_load(path.read_text(encoding="utf-8"))
    try:
        url = (
            os.environ.get("AIR_QUALITY_EPA_CATALOG_URL")
            or settings["epa"]["catalog_url"]
        )
        start = int(
            os.environ.get("AIR_QUALITY_PROJECT_HISTORY_START_YEAR")
            or settings["project"]["history_start_year"]
        )
        groups = settings["epa"]["hourly_groups"]
    except (KeyError, TypeError) as exc:
        raise ValueError(
            "Settings require project.history_start_year and epa fields"
        ) from exc
    if not isinstance(groups, dict) or not groups:
        raise ValueError("epa.hourly_groups must be a nonempty mapping")
    if any(
        not isinstance(name, str)
        or not re.fullmatch(r"[a-z][a-z0-9_]*", name)
        or not isinstance(code, str)
        or not re.fullmatch(r"[A-Za-z0-9]+", code)
        for name, code in groups.items()
    ):
        raise ValueError("Hourly group names and file codes must be strings")
    return url, start, groups


def atomic_write(path: Path, content: bytes) -> None:
    """Replace, never append, and leave the previous file intact on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", type=Path, default=ROOT / "config/settings.yaml")
    parser.add_argument("--catalog-url", help="Override the configured catalog URL")
    parser.add_argument(
        "--catalog-file", type=Path, help="Saved HTML; no network request"
    )
    parser.add_argument("--start-year", type=int)
    parser.add_argument(
        "--end-year", type=int, help="Default: latest hourly catalog year"
    )
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output/epa")
    args = parser.parse_args(argv)
    try:
        url, start, groups = load_settings(args.settings)
        url = args.catalog_url or url
        if not isinstance(url, str) or urlsplit(url).scheme not in {"http", "https"}:
            raise ValueError("Catalog URL must use HTTP or HTTPS")
        source = {"catalog_url": url}
        if args.catalog_file:
            content = args.catalog_file.read_bytes()
            source["input_mode"] = "saved_html"
        else:
            request = Request(
                url, headers={"User-Agent": "ChicagoAirQualityManifest/0.1"}
            )
            with urlopen(request, timeout=60) as response:
                content = response.read()
                url = response.geturl()
                source.update(
                    {
                        "input_mode": "http",
                        "resolved_url": url,
                        "retrieved_at_utc": datetime.now(UTC).isoformat(),
                        "http_last_modified": response.headers.get("Last-Modified"),
                        "http_etag": response.headers.get("ETag"),
                    }
                )
        source["catalog_sha256"] = hashlib.sha256(content).hexdigest()
        entries = build_manifest(
            content.decode("utf-8-sig"),
            url,
            groups,
            start if args.start_year is None else args.start_year,
            args.end_year,
        )
        summary = summarize(entries, groups)
        summary["catalog_url"] = url
        summary["catalog_sha256"] = source["catalog_sha256"]
        manifest = io.StringIO(newline="")
        writer = csv.DictWriter(manifest, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(asdict(entry) for entry in entries)
        artifacts = {
            "manifest.csv": manifest.getvalue().encode("utf-8"),
            "summary.json": (json.dumps(summary, indent=2) + "\n").encode("utf-8"),
            "catalog.html": content,
            "catalog_source.json": (json.dumps(source, indent=2) + "\n").encode(
                "utf-8"
            ),
        }
        for name, data in artifacts.items():
            atomic_write(args.output_dir / name, data)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        print(f"EPA manifest failed: {exc}", file=sys.stderr)
        return 1

    totals = summary["totals"]
    print(
        f"EPA hourly manifest: {summary['start_year']}–{summary['end_year']}, "
        f"{len(groups)} groups, {totals['cells']} cells"
    )
    print("; ".join(f"{status}: {totals[status]}" for status in STATUSES))
    print(f"Listed rows: {totals['listed_rows']:,}")
    print(
        f"Listed bytes (approximate, KB × {KB_TO_BYTES}): "
        f"{totals['listed_bytes']:,} ({totals['listed_bytes'] / 1024**3:.2f} GiB)"
    )
    for pollutant, details in summary["by_pollutant"].items():
        if details["listed_empty"] or details["missing"] or details["flags"]:
            print(
                f"{pollutant}: {details['listed_empty']} empty, "
                f"{details['missing']} missing; flags={details['flags']}"
            )
    print(f"Output: {args.output_dir / 'manifest.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
