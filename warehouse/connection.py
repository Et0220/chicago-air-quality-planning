"""Local secrets stay in the ignored environment; reports contain no credentials."""

import os
import shlex
import sys
from pathlib import Path

from warehouse.sql import identifier


def environment(path=Path(".env")):
    if sys.version_info[:2] != (3, 15):
        raise RuntimeError("This project requires Python 3.15")
    if path.exists():
        for line in path.read_text().splitlines():
            if not line.startswith("AIR_QUALITY_") or "=" not in line:
                continue
            key, text = line.split("=", 1)
            parts = shlex.split(text, comments=True)
            if len(parts) > 1:
                raise ValueError(f"Quote whitespace in {key}")
            if parts:
                os.environ.setdefault(key, parts[0])


def settings():
    def get(key, default):
        return os.environ.get("AIR_QUALITY_SNOWFLAKE_" + key) or default

    return {
        "database": identifier(get("DATABASE", "AIR_QUALITY")),
        "schema": identifier(get("SCHEMA", "ANALYTICS")),
        "warehouse": identifier(get("WAREHOUSE", "AQ_XS")),
        "bucket": os.environ.get("AIR_QUALITY_GCS_BUCKET")
        or "final-team-514205-air-quality",
        "project": os.environ.get("AIR_QUALITY_GCP_PROJECT_ID") or "final-team-514205",
        "prefix": "curated/epa_hourly",
    }


def connect(config, bootstrap=False):
    import snowflake.connector

    def get(key):
        return os.environ.get("AIR_QUALITY_SNOWFLAKE_" + key)

    if not get("ACCOUNT") or not get("USER"):
        raise RuntimeError(
            "Snowflake account/user are not configured. Follow docs/warehouse_model.md."
        )
    auth = get("AUTHENTICATOR") or "externalbrowser"
    kwargs = {
        "account": get("ACCOUNT"),
        "user": get("USER"),
        "authenticator": auth,
        "role": "ACCOUNTADMIN" if bootstrap else (get("ROLE") or "AQ_LOADER"),
        "login_timeout": 120,
        "network_timeout": 120,
        "session_parameters": {
            "TIMEZONE": "UTC",
            "QUERY_TAG": "air-quality:S13",
            "STATEMENT_TIMEOUT_IN_SECONDS": 600,
            "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS": 60,
            "LOCK_TIMEOUT": 30,
            "ABORT_DETACHED_QUERY": True,
        },
    }
    if get("PRIVATE_KEY_FILE"):
        kwargs.update(
            authenticator="SNOWFLAKE_JWT", private_key_file=get("PRIVATE_KEY_FILE")
        )
    elif get("TOKEN"):
        kwargs["token"] = get("TOKEN")
    elif get("PASSWORD") and auth != "externalbrowser":
        kwargs["password"] = get("PASSWORD")
    if not bootstrap:
        kwargs.update({k: config[k] for k in ("database", "schema", "warehouse")})
    return snowflake.connector.connect(**kwargs)
