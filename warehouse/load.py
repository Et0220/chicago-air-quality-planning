"""S13: prepare, bootstrap, and load a single committed EPA archive year."""

import argparse
import json
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import sqlparse

from spark.cloud_epa import save
from warehouse.connection import connect, environment, settings
from warehouse.manifest import read_commit, reconcile, verify_snapshot, year_lock
from warehouse.model import DIMENSIONS, EPA_TYPES
from warehouse.references import prepare_references
from warehouse.sql import (
    ROOT,
    copy_statements,
    desired_versions_sql,
    literal,
    render,
    typed_epa_sql,
)

DDL = [
    ROOT / "snowflake/ddl/001_bootstrap.sql",
    ROOT / "snowflake/ddl/002_model.sql",
    ROOT / "snowflake/views/003_chicago.sql",
]


def zero(cur, query, message):
    if cur.execute(query).fetchone()[0] != 0:
        raise ValueError(message)


def counts(cur, table, year):
    return dict(
        cur.execute(
            f"SELECT pollutant, COUNT(*) FROM {table} "
            "WHERE year = %s GROUP BY pollutant",
            (year,),
        ).fetchall()
    )


def prepare(client, config, year):
    snapshot = read_commit(client, config["bucket"], config["prefix"], year)
    verify_snapshot(client, snapshot)
    folder = ROOT / "output/warehouse" / snapshot["commit"]["batch"]
    references = prepare_references(client, config["bucket"], ROOT / "tmp/warehouse")
    save(
        folder / "load_plan.json",
        {
            "status": "prepared_not_loaded",
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "python_requirement": "3.15",
            "year": year,
            "commit_uri": snapshot["commit_uri"],
            "commit_generation": snapshot["commit_generation"],
            "commit_sha256": snapshot["commit_sha256"],
            "batch": snapshot["commit"]["batch"],
            "files": snapshot["files"],
            "expected_rows": snapshot["expected"],
            "reference": references,
        },
    )
    save(folder / "s12_commit.json", snapshot["commit"])
    for path in DDL:
        (folder / path.name).write_text(render(path, config))
    (folder / "copy_exact_files.sql").write_text(
        ";\n".join(copy_statements("EPA_CURATED_STAGE", snapshot["files"])) + ";\n"
    )
    return snapshot, references, folder


def bootstrap(config):
    with (
        closing(connect(config, bootstrap=True)) as conn,
        closing(conn.cursor()) as cur,
    ):
        for path in DDL:
            for statement in sqlparse.split(render(path, config)):
                cur.execute(statement)
        username = cur.execute("SELECT CURRENT_USER()").fetchone()[0]
        cur.execute("GRANT ROLE AQ_LOADER TO USER IDENTIFIER(%s)", (username,))
        properties = cur.execute("DESC STORAGE INTEGRATION AQ_GCS_READ").fetchall()
        principal = next(
            row[2] for row in properties if row[0] == "STORAGE_GCP_SERVICE_ACCOUNT"
        )
        result = {"status": "schema_created", "storage_gcp_service_account": principal}
        save(ROOT / "output/warehouse/bootstrap.json", result)
        return result


def stage_epa(cur, snapshot, config):
    properties = cur.execute("DESC STAGE EPA_CURATED_STAGE").fetchall()
    url = next(row[3] for row in properties if row[1] == "URL")
    expected_url = f"gcs://{config['bucket']}/{config['prefix']}/"
    if url != expected_url:
        raise ValueError("External stage URL does not match the validated commit root")
    cur.execute(
        "CREATE OR REPLACE TEMP TABLE COMMITTED_FILES ("
        "uri VARCHAR, generation VARCHAR, bytes NUMBER, md5 VARCHAR, "
        "relative_path VARCHAR, year INTEGER, pollutant VARCHAR)"
    )
    cur.executemany(
        "INSERT INTO COMMITTED_FILES VALUES (%s,%s,%s,%s,%s,%s,%s)",
        [
            tuple(
                item[k]
                for k in (
                    "uri",
                    "generation",
                    "bytes",
                    "md5",
                    "relative_path",
                    "year",
                    "pollutant",
                )
            )
            for item in snapshot["files"]
        ],
    )
    cur.execute(
        "CREATE OR REPLACE TEMP TABLE EPA_RAW ("
        "payload VARIANT, filename VARCHAR, file_row_number NUMBER)"
    )
    copy_results = []
    for statement in copy_statements("EPA_CURATED_STAGE", snapshot["files"]):
        cur.execute(statement)
        columns = [c[0].lower() for c in cur.description]
        copy_results.extend(
            dict(zip(columns, row, strict=True)) for row in cur.fetchall()
        )
    if len(copy_results) != len(snapshot["files"]) or any(
        row.get("status", "").upper() != "LOADED" or row.get("errors_seen", 0)
        for row in copy_results
    ):
        raise ValueError("COPY did not load every committed file without errors")
    zero(
        cur,
        """SELECT COUNT(*) FROM EPA_RAW r WHERE NOT EXISTS (
        SELECT 1 FROM COMMITTED_FILES f WHERE ENDSWITH(r.filename, f.relative_path))""",
        "COPY returned an uncommitted file",
    )
    zero(
        cur,
        """SELECT COUNT(*) FROM COMMITTED_FILES f WHERE NOT EXISTS (
        SELECT 1 FROM EPA_RAW r WHERE ENDSWITH(r.filename, f.relative_path))""",
        "A committed file produced no rows",
    )
    cur.execute(typed_epa_sql())
    year = snapshot["commit"]["year"]
    reconcile(snapshot["expected"], counts(cur, "EPA_TYPED", year))
    zero(
        cur,
        """SELECT COUNT(*) FROM EPA_TYPED WHERE monitor_key IS NULL
        OR site_key IS NULL OR observed_at_utc IS NULL OR observed_hour_utc IS NULL
        OR parameter_code IS NULL OR pollutant <> parameter_code OR row_hash IS NULL
        OR is_hour_aligned IS NULL OR pm25_advisory_superseded IS NULL
        OR year <> YEAR(date_local_standard)
        OR observed_hour_utc <> DATE_TRUNC('HOUR', observed_at_utc)
        OR is_hour_aligned <> (observed_hour_utc = observed_at_utc)""",
        "Typed Parquet violated the curated contract",
    )
    zero(
        cur,
        """SELECT COUNT(*) FROM (
        SELECT monitor_key, observed_hour_utc FROM EPA_TYPED
        GROUP BY 1,2 HAVING COUNT(*) <> 1)""",
        "Duplicate monitor-hour key",
    )
    zero(
        cur,
        f"""SELECT COUNT(*) FROM fact_epa_hourly f JOIN EPA_TYPED s
        ON f.monitor_key = s.monitor_key AND f.observed_hour_utc = s.observed_hour_utc
        WHERE f.year <> {year}""",
        "Monitor-hour key overlaps another archive year",
    )
    return copy_results


def stage_dimensions(cur, references):
    cur.execute("CREATE OR REPLACE TEMP STAGE REFERENCE_UPLOAD")
    uri = "file://" + Path(references["path"]).as_posix()
    cur.execute(
        f"PUT {literal(uri)} @REFERENCE_UPLOAD AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    cur.execute("CREATE OR REPLACE TEMP TABLE REFERENCE_RAW (payload VARIANT)")
    cur.execute(
        "COPY INTO REFERENCE_RAW FROM @REFERENCE_UPLOAD "
        "FILES=('dimension_events.jsonl.gz') FILE_FORMAT=(TYPE=JSON) "
        "ON_ERROR=ABORT_STATEMENT FORCE=TRUE"
    )
    cur.execute("""CREATE OR REPLACE TEMP TABLE EVENT_INPUT AS SELECT
        payload:kind::VARCHAR AS kind, payload:natural_key::VARCHAR AS natural_key,
        payload:effective_at_utc::TIMESTAMP_NTZ(6) AS effective_at_utc,
        payload:row_hash::VARCHAR AS row_hash, payload:attributes AS attributes,
        payload:source_uri::VARCHAR AS source_uri,
        payload:source_generation::VARCHAR AS source_generation,
        payload:effective_basis::VARCHAR AS effective_basis FROM REFERENCE_RAW""")
    actual = dict(
        cur.execute("SELECT kind, COUNT(*) FROM EVENT_INPUT GROUP BY kind").fetchall()
    )
    if actual != references["counts"]:
        raise ValueError("Reference snapshot counts do not reconcile")
    zero(
        cur,
        """SELECT COUNT(*) FROM (
        SELECT kind, natural_key, effective_at_utc FROM EVENT_INPUT
        GROUP BY 1,2,3 HAVING COUNT(*) <> 1)""",
        "Duplicate dimension event",
    )
    zero(
        cur,
        """SELECT COUNT(*) FROM EVENT_INPUT i JOIN dimension_event e
        ON i.kind=e.kind AND i.natural_key=e.natural_key
        AND i.effective_at_utc=e.effective_at_utc WHERE i.row_hash <> e.row_hash""",
        "Conflicting dimension attributes at the same effective instant",
    )
    for kind in DIMENSIONS:
        cur.execute(desired_versions_sql(kind))


def dimension_join(alias="s"):
    return f"""
LEFT JOIN dim_site ds ON {alias}.site_key = ds.site_key
    AND {alias}.observed_hour_utc >= ds.valid_from_utc
    AND (ds.valid_to_utc IS NULL OR {alias}.observed_hour_utc < ds.valid_to_utc)
    AND (ds.established_date IS NULL
        OR TO_DATE({alias}.observed_hour_utc) >= ds.established_date)
    AND (ds.closed_date IS NULL OR TO_DATE({alias}.observed_hour_utc) <= ds.closed_date)
LEFT JOIN dim_monitor dm ON {alias}.monitor_key = dm.monitor_key
    AND {alias}.observed_hour_utc >= dm.valid_from_utc
    AND (dm.valid_to_utc IS NULL OR {alias}.observed_hour_utc < dm.valid_to_utc)
    AND (dm.status IS NULL OR dm.status <> 'closed')
    AND (dm.activity_start IS NULL
        OR TO_DATE({alias}.observed_hour_utc) >= dm.activity_start)
    AND (dm.activity_end IS NULL
        OR TO_DATE({alias}.observed_hour_utc) <= dm.activity_end)
"""


def promote(cur, snapshot, references):
    year, batch = snapshot["commit"]["year"], snapshot["commit"]["batch"]
    generation = snapshot["commit_generation"]
    cur.execute("CALL apply_dimension_events()")
    # New historical evidence may change the valid version for already loaded facts.
    cur.execute(f"""UPDATE fact_epa_hourly f
        SET site_sk = j.site_sk, monitor_sk = j.monitor_sk
        FROM (SELECT s.monitor_key, s.observed_hour_utc,
            COALESCE(ds.site_sk,'0') AS site_sk,
            COALESCE(dm.monitor_sk,'0') AS monitor_sk
            FROM fact_epa_hourly s {dimension_join()}) j
        WHERE f.monitor_key=j.monitor_key AND f.observed_hour_utc=j.observed_hour_utc
        AND (f.site_sk <> j.site_sk OR f.monitor_sk <> j.monitor_sk)""")
    cur.execute("""MERGE INTO dim_pollutant d USING (
        SELECT pollutant, MIN(parameter_name) AS parameter_name,
        MIN(unit_standard) AS unit_standard FROM EPA_TYPED GROUP BY pollutant
        ) s ON d.pollutant=s.pollutant
        WHEN NOT MATCHED THEN INSERT (pollutant,parameter_name,unit_standard)
        VALUES (s.pollutant,s.parameter_name,s.unit_standard)""")
    cur.execute("""MERGE INTO dim_hour d USING (
        SELECT DISTINCT observed_hour_utc,
            CONVERT_TIMEZONE('UTC','America/Chicago',observed_hour_utc) AS local_hour
        FROM EPA_TYPED) s ON d.observed_hour_utc=s.observed_hour_utc
        WHEN NOT MATCHED THEN INSERT (observed_hour_utc,utc_date,utc_hour,
            chicago_local_date,chicago_local_hour,chicago_utc_offset_minutes)
        VALUES (s.observed_hour_utc,TO_DATE(s.observed_hour_utc),
            HOUR(s.observed_hour_utc),
            TO_DATE(s.local_hour),HOUR(s.local_hour),
            DATEDIFF('MINUTE',s.observed_hour_utc,s.local_hour))""")
    cur.execute("""MERGE INTO dim_date d USING (
        SELECT DISTINCT TO_DATE(observed_hour_utc) AS day FROM EPA_TYPED
        UNION SELECT DISTINCT chicago_local_date FROM EPA_TYPED
        WHERE chicago_local_date IS NOT NULL) s ON d.date_key=s.day
        WHEN NOT MATCHED THEN INSERT (date_key,year,month,day,day_of_week_iso)
        VALUES (s.day,YEAR(s.day),MONTH(s.day),DAY(s.day),DAYOFWEEKISO(s.day))""")
    # Transactional replacement also removes rows absent from the new commit.
    cur.execute("DELETE FROM fact_epa_hourly WHERE year=%s", (year,))
    columns = ",".join(EPA_TYPES)
    selected = ",".join("s." + name for name in EPA_TYPES)
    cur.execute(
        f"""INSERT INTO fact_epa_hourly ({columns},site_sk,monitor_sk,
        s12_batch,commit_generation,parquet_uri,parquet_generation,loaded_at_utc)
        SELECT {selected},COALESCE(ds.site_sk,'0'),COALESCE(dm.monitor_sk,'0'),
        %s,%s,s.parquet_uri,s.parquet_generation,SYSDATE()
        FROM EPA_TYPED s {dimension_join()}""",
        (batch, generation),
    )
    cur.execute("DELETE FROM warehouse_load_file WHERE year=%s", (year,))
    cur.execute(
        """INSERT INTO warehouse_load_file
        SELECT year,%s,%s,uri,generation,bytes,md5 FROM COMMITTED_FILES""",
        (batch, generation),
    )
    cur.execute(
        """MERGE INTO warehouse_commit d USING (
        SELECT %s AS year,%s AS batch,%s AS generation,
            %s AS sha,%s AS ref,%s AS row_count
        ) s ON d.year=s.year
        WHEN MATCHED THEN UPDATE SET s12_batch=s.batch,commit_generation=s.generation,
            commit_sha256=s.sha,reference_sha256=s.ref,
            row_count=s.row_count,loaded_at_utc=SYSDATE()
        WHEN NOT MATCHED THEN INSERT (year,s12_batch,commit_generation,commit_sha256,
            reference_sha256,row_count,loaded_at_utc)
        VALUES(s.year,s.batch,s.generation,s.sha,s.ref,s.row_count,SYSDATE())""",
        (
            year,
            batch,
            generation,
            snapshot["commit_sha256"],
            references["sha256"],
            sum(snapshot["expected"].values()),
        ),
    )


def verify_facts(cur, snapshot):
    year = snapshot["commit"]["year"]
    reconcile(snapshot["expected"], counts(cur, "fact_epa_hourly", year))
    zero(
        cur,
        f"""SELECT COUNT(*) FROM (SELECT monitor_key,observed_hour_utc
        FROM fact_epa_hourly WHERE year={year} GROUP BY 1,2 HAVING COUNT(*) <> 1)""",
        "Warehouse fact grain is not unique",
    )
    zero(
        cur,
        f"""SELECT COUNT(*) FROM fact_epa_hourly WHERE year={year} AND (
        s12_batch <> {literal(snapshot["commit"]["batch"])} OR
        commit_generation <> {literal(snapshot["commit_generation"])})""",
        "Fact rows have mixed commit lineage",
    )
    actual_files = cur.execute(
        """SELECT uri,generation,bytes,md5 FROM warehouse_load_file
        WHERE year=%s ORDER BY uri""",
        (year,),
    ).fetchall()
    expected_files = sorted(
        tuple(f[k] for k in ("uri", "generation", "bytes", "md5"))
        for f in snapshot["files"]
    )
    if actual_files != expected_files:
        raise ValueError("Warehouse file ledger differs from the committed files")
    return dict(
        cur.execute(f"""SELECT 'unknown_site',COALESCE(COUNT_IF(site_sk='0'),0)
        FROM fact_epa_hourly WHERE year={year} UNION ALL
        SELECT 'unknown_monitor',COALESCE(COUNT_IF(monitor_sk='0'),0)
        FROM fact_epa_hourly WHERE year={year}""").fetchall()
    )


def load(client, config, year):
    with (
        year_lock(client, config["bucket"], config["prefix"], "s13-warehouse"),
        year_lock(client, config["bucket"], config["prefix"], year),
    ):
        snapshot, references, folder = prepare(client, config, year)
        with closing(connect(config)) as conn, closing(conn.cursor()) as cur:
            prior = cur.execute(
                """SELECT commit_sha256,reference_sha256
                FROM warehouse_commit WHERE year=%s""",
                (year,),
            ).fetchall()
            if len(prior) > 1:
                raise ValueError("Duplicate warehouse year ledger")
            if prior and prior[0] == (snapshot["commit_sha256"], references["sha256"]):
                unknown = verify_facts(cur, snapshot)
                verify_snapshot(client, snapshot)
                result = {
                    "status": "unchanged",
                    "year": year,
                    "unknown_dimensions": unknown,
                }
                save(folder / "replay.json", result)
                return result
            copied = stage_epa(cur, snapshot, config)
            stage_dimensions(cur, references)
            # All temp-table DDL is complete before the atomic fact/dimension promotion.
            cur.execute("BEGIN")
            try:
                promote(cur, snapshot, references)
                unknown = verify_facts(cur, snapshot)
                verify_snapshot(client, snapshot)
                cur.execute("COMMIT")
            except BaseException:
                cur.execute("ROLLBACK")
                raise
            cur.execute("ALTER SESSION SET USE_CACHED_RESULT=FALSE")
            started = perf_counter()
            monthly = cur.execute(
                "SELECT * FROM chicago_pm25_monthly ORDER BY 1,2,3"
            ).fetchall()
            elapsed = perf_counter() - started
            result = {
                "status": "loaded",
                "year": year,
                "batch": snapshot["commit"]["batch"],
                "commit_generation": snapshot["commit_generation"],
                "rows": sum(snapshot["expected"].values()),
                "files": len(snapshot["files"]),
                "by_pollutant": snapshot["expected"],
                "unknown_dimensions": unknown,
                "reference_sha256": references["sha256"],
                "monthly_query_seconds": elapsed,
                "monthly_query_under_10s": elapsed < 10,
                "monthly_query_id": cur.sfqid,
                "monthly_result": [list(map(str, row)) for row in monthly],
                "copy_results": copied,
                "checked_at_utc": datetime.now(UTC).isoformat(),
            }
            save(folder / "load_result.json", result)
            return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "bootstrap", "check", "load"])
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    args = parser.parse_args()
    environment(args.env_file)
    config = settings()
    if args.action == "bootstrap":
        result = bootstrap(config)
    elif args.action == "check":
        with closing(connect(config)) as conn, closing(conn.cursor()) as cur:
            result = {
                "connection": cur.execute(
                    "SELECT CURRENT_ACCOUNT(),CURRENT_USER(),"
                    "CURRENT_ROLE(),CURRENT_VERSION()"
                ).fetchone()
            }
    else:
        from google.cloud import storage

        client = storage.Client(project=config["project"])
        if args.action == "prepare":
            snapshot, _, folder = prepare(client, config, args.year)
            result = {
                "status": "prepared_not_loaded",
                "folder": str(folder),
                "files": len(snapshot["files"]),
                "rows": sum(snapshot["expected"].values()),
            }
        else:
            result = load(client, config, args.year)
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
