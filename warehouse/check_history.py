"""Exercise the real SCD2 procedure in a disposable Snowflake schema."""

import json
from contextlib import closing
from uuid import uuid4

import sqlparse

from spark.cloud_epa import save
from warehouse.connection import connect, environment, settings
from warehouse.model import DIMENSIONS
from warehouse.scd import event
from warehouse.sql import ROOT, desired_versions_sql, render


def check(config):
    temporary = {**config, "schema": "S13_TEST_" + uuid4().hex.upper()}
    with (
        closing(connect(config, bootstrap=True)) as conn,
        closing(conn.cursor()) as cur,
    ):
        cur.execute(f"USE WAREHOUSE {config['warehouse']}")
        cur.execute(f"USE DATABASE {config['database']}")
        cur.execute(f"CREATE SCHEMA {temporary['schema']}")
        try:
            sql = render(ROOT / "snowflake/ddl/002_model.sql", temporary)
            for statement in sqlparse.split(sql):
                cur.execute(statement)
            cur.execute("CREATE TEMP TABLE event_input LIKE dimension_event")
            attrs = {"latitude": 41, "method_code": "736", "status": "active"}
            events = []
            for day, update in [
                (1, {}),
                (2, {}),
                (3, {"latitude": 42}),
                (4, {"latitude": 42, "method_code": "738"}),
                (5, {"latitude": 42, "method_code": "738", "status": "closed"}),
            ]:
                events.append(
                    event(
                        "monitor",
                        "synthetic",
                        f"2026-01-{day:02}T00:00:00Z",
                        {**attrs, **update},
                        "synthetic",
                        "1",
                        "documented change",
                    )
                )
            for item in events:
                cur.execute(
                    """INSERT INTO event_input SELECT %s,%s,%s,%s,
                    PARSE_JSON(%s),%s,%s,%s""",
                    (
                        item["kind"],
                        item["natural_key"],
                        item["effective_at_utc"],
                        item["row_hash"],
                        json.dumps(item["attributes"]),
                        item["source_uri"],
                        item["source_generation"],
                        item["effective_basis"],
                    ),
                )
            for kind in DIMENSIONS:
                cur.execute(desired_versions_sql(kind))
            cur.execute("BEGIN")
            cur.execute("CALL apply_dimension_events()")
            cur.execute("ROLLBACK")
            assert (
                cur.execute(
                    "SELECT COUNT(*) FROM dim_monitor WHERE monitor_sk <> '0'"
                ).fetchone()[0]
                == 0
            )
            for _ in range(2):
                for kind in DIMENSIONS:
                    cur.execute(desired_versions_sql(kind))
                cur.execute("BEGIN")
                cur.execute("CALL apply_dimension_events()")
                cur.execute("COMMIT")
            rows = cur.execute("""SELECT latitude,method_code,status,is_current
                FROM dim_monitor WHERE monitor_key='synthetic'
                ORDER BY valid_from_utc""").fetchall()
            assert rows == [
                (41.0, "736", "active", False),
                (42.0, "736", "active", False),
                (42.0, "738", "active", False),
                (42.0, "738", "closed", True),
            ]
            historical = cur.execute("""SELECT latitude,method_code FROM dim_monitor
                WHERE monitor_key='synthetic' AND '2026-01-02 12:00:00'::TIMESTAMP_NTZ
                >= valid_from_utc AND (valid_to_utc IS NULL OR
                '2026-01-02 12:00:00'::TIMESTAMP_NTZ < valid_to_utc)""").fetchall()
            assert historical == [(41.0, "736")]
            assert (
                cur.execute("""SELECT COUNT(*) FROM dim_monitor
                WHERE monitor_key='synthetic'
                AND valid_from_utc <= '2025-12-31'""").fetchone()[0]
                == 0
            )
            dst = cur.execute("""WITH hours AS (
                SELECT column1::TIMESTAMP_NTZ AS utc FROM VALUES
                ('2026-11-01 06:00:00'),('2026-11-01 07:00:00'))
                SELECT HOUR(CONVERT_TIMEZONE('UTC','America/Chicago',utc)),
                DATEDIFF('MINUTE',utc,CONVERT_TIMEZONE('UTC','America/Chicago',utc))
                FROM hours ORDER BY utc""").fetchall()
            assert dst == [(1, -300), (1, -360)]
            result = {
                "status": "passed",
                "versions": 4,
                "replay": "unchanged",
                "point_in_time": "passed",
                "unknown_history": "passed",
                "rollback": "passed",
                "dst": "passed",
            }
            save(ROOT / "output/warehouse/history_check.json", result)
            return result
        finally:
            cur.execute("ROLLBACK")
            cur.execute(
                f"DROP SCHEMA {config['database']}.{temporary['schema']} CASCADE"
            )


if __name__ == "__main__":
    environment(ROOT / ".env")
    print(json.dumps(check(settings())))
