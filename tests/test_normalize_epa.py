"""Exercise real local Spark: units, revision precedence, DST, and Parquet."""

import csv
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from spark.normalize_epa import deduplicate, normalize, read_sources  # noqa: E402
from spark.schemas import CURATED_SCHEMA, EPA_COLUMNS  # noqa: E402


def source(path, year=2026, generation="10"):
    return {
        "stage_uri": str(path),
        "stage_generation": generation,
        "year": year,
        "raw_uri": "gs://fixture/raw.zip",
        "raw_generation": "9",
        "pollutant": "fixture",
    }


def row(**changes):
    values = dict(
        zip(
            EPA_COLUMNS,
            [
                "17",
                "031",
                "0001",
                "88101",
                "1",
                "41.9",
                "-87.6",
                "WGS84",
                "PM2.5",
                "2026-11-01",
                "00:00",
                "2026-11-01",
                "06:00",
                "10",
                "Micrograms/cubic meter (LC)",
                "0.1",
                "0.2",
                "V",
                "FEM",
                "736",
                "Corrected",
                "Illinois",
                "Cook",
                "2026-11-02",
            ],
            strict=True,
        )
    )
    values.update(changes)
    return values


def frame(spark_session, tmp_path, rows, generation="10"):
    path = tmp_path / f"input-{generation}.csv"
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=EPA_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return read_sources(
        spark_session, [source(path, generation=generation)], "2026-11-09T20:00:00Z"
    )


def test_dst_keys_units_and_preservation(spark_session, tmp_path):
    raw = frame(
        spark_session,
        tmp_path,
        [
            row(),
            row(**{"Time GMT": "07:00", "Time Local": "01:00"}),
            row(
                **{
                    "Date Local": "2026-03-08",
                    "Date GMT": "2026-03-08",
                    "Time GMT": "07:00",
                    "Time Local": "01:00",
                }
            ),
            row(
                **{
                    "Date Local": "2026-03-08",
                    "Date GMT": "2026-03-08",
                    "Time GMT": "08:00",
                    "Time Local": "02:00",
                }
            ),
            row(
                **{
                    "Parameter Code": "42101",
                    "Sample Measurement": "-0.2",
                    "Units of Measure": "Parts per million",
                }
            ),
            row(**{"POC": "2", "Sample Measurement": "", "MDL": "", "Uncertainty": ""}),
            row(
                **{
                    "State Code": "08",
                    "County Code": "031",
                    "Units of Measure": "Micrograms/cubic meter (25 C)",
                }
            ),
            row(
                **{
                    "Date Local": "2026-12-31",
                    "Time Local": "23:00",
                    "Date GMT": "2027-01-01",
                    "Time GMT": "05:00",
                }
            ),
        ],
    )
    valid, bad = normalize(raw)
    assert bad.count() == 0
    got = valid.collect()
    pm = [
        r
        for r in got
        if r.parameter_code == "88101" and r.poc == "01" and r.state_code == "17"
    ]
    fall = sorted(
        [r for r in pm if r.chicago_local_date.isoformat() == "2026-11-01"],
        key=lambda r: r.observed_at_utc,
    )
    assert [(r.chicago_local_hour, r.chicago_utc_offset_minutes) for r in fall] == [
        (1, -300),
        (1, -360),
    ]
    spring = sorted(
        [
            r.chicago_local_hour
            for r in pm
            if r.chicago_local_date.isoformat() == "2026-03-08"
        ]
    )
    assert spring == [1, 3]
    assert fall[0].monitor_key == "17-031-0001-88101-01"
    assert (
        fall[0].method_code,
        fall[0].qualifier,
        fall[0].mdl,
        fall[0].uncertainty,
    ) == ("736", "V", 0.1, 0.2)
    co = next(r for r in got if r.parameter_code == "42101")
    assert (co.sample_measurement, co.measurement_standard, co.unit_standard) == (
        -0.2,
        -200,
        "ppb",
    )
    assert next(r for r in got if r.poc == "02").measurement_standard is None
    assert next(r for r in got if r.state_code == "08").chicago_local_hour is None
    spill = next(r for r in got if r.date_local_standard.isoformat() == "2026-12-31")
    assert spill.year == 2026
    assert valid.filter("year(observed_at_utc) = 2027").count() == 1


def test_advisory_revision_generation_and_stable_replay(spark_session, tmp_path):
    a = frame(
        spark_session,
        tmp_path,
        [
            row(
                **{
                    "Method Code": "236",
                    "Date of Last Change": "2026-11-08",
                    "Sample Measurement": "99",
                }
            ),
            row(**{"Date of Last Change": "2026-11-01", "Sample Measurement": "9"}),
            row(),
            row(),
            row(**{"POC": "2", "Method Code": "238"}),
        ],
        generation="9",
    )
    b = frame(
        spark_session, tmp_path, [row(**{"Sample Measurement": "12"})], generation="10"
    )
    valid, bad = normalize(a.unionByName(b))
    assert bad.count() == 0
    result = deduplicate(valid).orderBy("monitor_key").collect()
    assert len(result) == 2
    assert result[0].measurement_standard == 12
    assert not result[0].pm25_advisory_superseded
    assert result[1].pm25_advisory_superseded
    assert result == deduplicate(valid.repartition(3)).orderBy("monitor_key").collect()


def test_invalid_rows_are_accounted_for(spark_session, tmp_path):
    cases = [
        {"POC": "123"},
        {"Date GMT": ""},
        {"Time GMT": "24:00"},
        {"Time GMT": "12:99"},
        {"Units of Measure": "bogus"},
        {"Sample Measurement": "garbage"},
        {"Sample Measurement": "NaN"},
        {"MDL": "Infinity"},
        {"Date GMT": "2026-02-30"},
        {"Date of Last Change": "2026-99-99"},
        {"Date Local": "2025-11-01"},
        {"Parameter Code": "42101", "Units of Measure": "Micrograms/cubic meter (LC)"},
    ]
    valid, bad = normalize(frame(spark_session, tmp_path, [row(**c) for c in cases]))
    assert valid.count() == 0
    assert bad.count() == len(cases)
    assert all(r.error_reasons and r.raw_record for r in bad.collect())


def test_real_old_recent_fixtures_and_parquet(spark_session, tmp_path):
    frames = []
    for year, filename in [
        (1990, "epa_hourly_1990.csv"),
        (2026, "epa_hourly_2026.csv"),
    ]:
        frames.append(
            read_sources(
                spark_session,
                [source(Path("tests/fixtures") / filename, year)],
                "2026-11-09T20:00:00Z",
            )
        )
    valid, bad = normalize(frames[0].unionByName(frames[1]))
    assert bad.count() == 0
    result = deduplicate(valid)
    assert result.count() == 12
    expected = [(f.name, f.dataType) for f in CURATED_SCHEMA]
    assert [(f.name, f.dataType) for f in result.schema] == expected
    path = str(tmp_path / "curated")
    result.write.mode("overwrite").partitionBy("year", "pollutant").parquet(path)
    restored = spark_session.read.schema(CURATED_SCHEMA).parquet(path)
    assert restored.count() == 12
    assert restored.filter("year = 1990").count() == 6
    assert (
        restored.selectExpr("CAST(ingested_at_utc AS STRING)").first()[0]
        == "2026-11-09 20:00:00"
    )
    # Replacement has no duplicate keys or extra rows.
    result.write.mode("overwrite").partitionBy("year", "pollutant").parquet(path)
    assert spark_session.read.parquet(path).count() == 12


def test_batch_runner_reconciles_actual_parquet(spark_session, tmp_path):
    from spark.run_epa import execute

    csv_path = Path("tests/fixtures/epa_hourly_2026.csv").resolve()
    entry = {
        **source(csv_path),
        "stage_uri": str(csv_path),
        "listed_rows": 6,
        "staged_rows": 6,
        "csv_bytes": csv_path.stat().st_size,
    }
    result = execute(
        spark_session,
        {
            "sources": [entry],
            "ingested_at_utc": "2026-11-09T20:00:00Z",
            "input_fingerprint": "test",
        },
        str(tmp_path / "batch"),
    )
    assert result["python_version"].startswith("3.15.")
    assert result["sources"][0]["output_rows"] == 6
    assert result["sources"][0]["duplicate_rows"] == 0
    assert result["sources"][0]["rejected_rows"] == 0
    assert result["output_bytes"] > 0
    assert (tmp_path / "batch/metrics.json").exists()


def test_nonzero_minute_sampling_is_retained_and_hour_key_is_explicit(
    spark_session, tmp_path
):
    raw = frame(
        spark_session,
        tmp_path,
        [
            row(**{"Time GMT": "08:26", "Time Local": "02:26"}),
            row(**{"Time GMT": "09:26", "Time Local": "03:26"}),
            row(
                **{
                    "Time GMT": "08:00",
                    "Time Local": "02:00",
                    "Date of Last Change": "2026-11-01",
                }
            ),
        ],
    )
    valid, bad = normalize(raw)
    assert valid.count() == 3
    assert bad.count() == 0
    clean = deduplicate(valid)
    assert clean.count() == 2
    assert (
        clean.filter(
            "minute(observed_at_utc) = 26 AND minute(observed_hour_utc) = 0 "
            "AND NOT is_hour_aligned"
        ).count()
        == 2
    )


def test_run_evidence_excludes_runtime_authentication():
    from spark.run_epa import resource_properties

    assert resource_properties(
        [
            ("spark.executor.cores", "4"),
            ("spark.authenticate.secret", "synthetic-test-value"),
            ("spark.unrecognized.secret", "synthetic-test-value"),
        ]
    ) == {"spark.executor.cores": "4"}


def test_lead_group_keeps_observed_string_parameters(spark_session, tmp_path):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType

    raw = frame(
        spark_session,
        tmp_path,
        [row(**{"Parameter Code": code}) for code in ("14129", "12128")],
    ).withColumn("source_file_group", F.lit("lead"))
    valid, bad = normalize(raw)
    assert bad.count() == 0
    path = str(tmp_path / "lead")
    valid.write.partitionBy("year", "pollutant").parquet(path)
    # Partition columns live in directory names, not physical Parquet fields.
    files = [str(p) for p in Path(path).rglob("*.parquet")]
    assert "pollutant" not in spark_session.read.parquet(files[0]).columns
    restored = (
        spark_session.read.schema(CURATED_SCHEMA)
        .option("basePath", path)
        .parquet(*files)
    )
    assert isinstance(restored.schema["pollutant"].dataType, StringType)
    assert isinstance(restored.schema["year"].dataType, IntegerType)
    assert {
        (r.pollutant, r.parameter_code, r.source_file_group) for r in restored.collect()
    } == {("14129", "14129", "lead"), ("12128", "12128", "lead")}
