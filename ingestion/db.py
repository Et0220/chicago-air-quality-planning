"""PostgreSQL connection and repeatable S05 migration commands.

Set AIR_QUALITY_CLOUDSQL_DSN for local PostgreSQL, or set the Cloud SQL
connection name and either an IAM username or built-in user credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pg8000.dbapi
import sqlparse

MIGRATIONS = Path(__file__).resolve().parents[1] / "cloudsql" / "migrations"
PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


@contextmanager
def connect() -> Iterator[pg8000.dbapi.Connection]:
    """Yield a DB-API connection; close the connector and socket afterwards."""
    dsn = os.environ.get("AIR_QUALITY_CLOUDSQL_DSN")
    instance = os.environ.get("AIR_QUALITY_CLOUDSQL_CONNECTION_NAME")
    if dsn:
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"}:
            raise ValueError("AIR_QUALITY_CLOUDSQL_DSN must be a PostgreSQL URL")
        if not parsed.hostname or not parsed.username or not parsed.path.strip("/"):
            raise ValueError("AIR_QUALITY_CLOUDSQL_DSN needs host, user, and database")
        if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("Use the Cloud SQL connector for non-local PostgreSQL")
        conn = pg8000.dbapi.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=unquote(parsed.username),
            password=unquote(parsed.password or ""),
            database=unquote(parsed.path.lstrip("/")),
        )
        try:
            yield conn
        finally:
            conn.close()
    elif instance:
        user = os.environ.get("AIR_QUALITY_CLOUDSQL_IAM_USER")
        admin_user = os.environ.get("AIR_QUALITY_CLOUDSQL_USER")
        admin_password = os.environ.get("AIR_QUALITY_CLOUDSQL_PASSWORD")
        if bool(user) == bool(admin_user and admin_password):
            raise ValueError(
                "Set either AIR_QUALITY_CLOUDSQL_IAM_USER or both "
                "AIR_QUALITY_CLOUDSQL_USER and AIR_QUALITY_CLOUDSQL_PASSWORD"
            )
        try:
            from google.cloud.sql.connector import Connector
        except ImportError as exc:
            raise RuntimeError("Install the project cloudsql extra") from exc
        with Connector() as connector:
            auth = (
                {"user": user, "enable_iam_auth": True}
                if user
                else {"user": admin_user, "password": admin_password}
            )
            conn = connector.connect(
                instance,
                "pg8000",
                db=os.environ.get("AIR_QUALITY_CLOUDSQL_DB", "air_quality"),
                **auth,
            )
            try:
                yield conn
            finally:
                conn.close()
    else:
        raise ValueError(
            "Set AIR_QUALITY_CLOUDSQL_DSN or AIR_QUALITY_CLOUDSQL_CONNECTION_NAME"
        )


def migrate(conn: pg8000.dbapi.Connection) -> list[str]:
    """Apply numbered SQL files once, verifying their recorded SHA-256 hashes."""
    applied: list[str] = []
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(2050505)")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS schema_migration (
                version TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL,
                applied_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
            )"""
        )
        for path in sorted(MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql")):
            content = path.read_text(encoding="utf-8")
            digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
            cur.execute(
                "SELECT sha256 FROM schema_migration WHERE version = %s",
                (path.name,),
            )
            row = cur.fetchone()
            if row:
                if row[0] != digest:
                    raise ValueError(f"Applied migration changed: {path.name}")
                continue
            for statement in sqlparse.split(content):
                if statement.strip():
                    cur.execute(statement)
            cur.execute(
                "INSERT INTO schema_migration (version, sha256) VALUES (%s, %s)",
                (path.name, digest),
            )
            applied.append(path.name)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return applied


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def grant_workers(conn: pg8000.dbapi.Connection, project_id: str) -> None:
    """Grant the four S02 IAM SQL users only their collector table access."""
    if not PROJECT_RE.fullmatch(project_id):
        raise ValueError("Invalid GCP project ID")
    privileges = {
        "aq-epa": {
            "read_write": (
                "ingest_run",
                "ingest_file",
                "ingest_file_version",
                "aqs_site",
                "aqs_monitor",
            ),
            "read_only": (),
            "sequences": ("ingest_file_ingest_file_id_seq",),
        },
        "aq-purpleair": {
            "read_write": (
                "ingest_run",
                "pa_sensor",
                "pa_sensor_history",
                "pa_reading",
                "pa_poll_log",
            ),
            "read_only": (),
            "sequences": (),
        },
        "aq-airnow": {
            "read_write": ("ingest_run", "airnow_obs", "airnow_obs_revision"),
            "read_only": (),
            "sequences": ("airnow_obs_revision_revision_id_seq",),
        },
        "aq-sync": {
            "read_write": ("ingest_run",),
            "read_only": (
                "pa_sensor",
                "pa_sensor_history",
                "pa_reading",
                "airnow_obs",
                "airnow_obs_revision",
            ),
            "sequences": (),
        },
    }
    cur = conn.cursor()
    try:
        cur.execute("SELECT current_database()")
        database = _quote_identifier(cur.fetchone()[0])
        for short_name, access in privileges.items():
            role = f"{short_name}@{project_id}.iam"
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            if cur.fetchone() is None:
                raise ValueError(f"Cloud SQL IAM database role is missing: {role}")
            quoted_role = _quote_identifier(role)
            cur.execute(f"GRANT CONNECT ON DATABASE {database} TO {quoted_role}")
            cur.execute(f"GRANT USAGE ON SCHEMA public TO {quoted_role}")
            for table in access["read_write"]:
                cur.execute(
                    f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{table} "
                    f"TO {quoted_role}"
                )
            for table in access["read_only"]:
                cur.execute(f"GRANT SELECT ON TABLE public.{table} TO {quoted_role}")
            for sequence in access["sequences"]:
                cur.execute(
                    f"GRANT USAGE, SELECT ON SEQUENCE public.{sequence} "
                    f"TO {quoted_role}"
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="S05 Cloud SQL schema operations")
    parser.add_argument("command", choices=("migrate", "grant-workers", "check"))
    args = parser.parse_args()
    with connect() as conn:
        if args.command == "migrate":
            applied = migrate(conn)
            print("Applied: " + (", ".join(applied) or "none; schema is current"))
        elif args.command == "grant-workers":
            project_id = os.environ.get("AIR_QUALITY_GCP_PROJECT_ID", "")
            grant_workers(conn, project_id)
            print("Granted collector privileges")
        else:
            cur = conn.cursor()
            cur.execute("SELECT version FROM schema_migration ORDER BY version")
            print("Applied migrations: " + ", ".join(row[0] for row in cur.fetchall()))
            cur.close()


if __name__ == "__main__":
    main()
