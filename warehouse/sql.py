"""Render reviewable Snowflake SQL with validated identifiers and exact FILES."""

import re
from pathlib import Path

from warehouse.model import DIMENSIONS, EPA_TYPES

ROOT = Path(__file__).resolve().parents[1]


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value.upper()


def literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def copy_statements(stage, files):
    stage = identifier(stage)
    if not files:
        raise ValueError("COPY must specify a nonempty committed FILES list")
    for offset in range(0, len(files), 1000):
        names = ",\n".join(
            literal(f["relative_path"]) for f in files[offset : offset + 1000]
        )
        yield (
            "COPY INTO EPA_RAW (payload, filename, file_row_number)\n"
            "FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER "
            f"FROM @{stage})\n"
            f"FILES = ({names})\n"
            "FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)\n"
            "ON_ERROR = ABORT_STATEMENT FORCE = TRUE PURGE = FALSE"
        )


def typed_epa_sql():
    columns = []
    for name, sql_type in EPA_TYPES.items():
        expression = f'r.payload:"{name}"::{sql_type}'
        if name in {"year", "pollutant"}:
            expression = f"f.{name}::{sql_type}"
        columns.append(f"{expression} AS {name}")
    return (
        "CREATE OR REPLACE TEMP TABLE EPA_TYPED AS SELECT\n"
        + ",\n".join(columns)
        + ", f.uri AS parquet_uri, f.generation AS parquet_generation, "
        "r.file_row_number FROM EPA_RAW r JOIN COMMITTED_FILES f "
        "ON ENDSWITH(r.filename, f.relative_path)"
    )


def dimension_ddl():
    statements = []
    for kind, attributes in DIMENSIONS.items():
        columns = ",\n".join(f"{k} {v}" for k, v in attributes.items())
        statements.append(f"""
CREATE TABLE IF NOT EXISTS dim_{kind} (
    {kind}_sk VARCHAR NOT NULL PRIMARY KEY,
    {kind}_key VARCHAR NOT NULL,
    valid_from_utc TIMESTAMP_NTZ(6) NOT NULL,
    valid_to_utc TIMESTAMP_NTZ(6), is_current BOOLEAN NOT NULL,
    row_hash VARCHAR NOT NULL, attributes VARIANT,
    source_uri VARCHAR, source_generation VARCHAR, effective_basis VARCHAR,
    {columns}
);
MERGE INTO dim_{kind} d USING (SELECT '0' AS sk) s ON d.{kind}_sk = s.sk
WHEN NOT MATCHED THEN INSERT (
    {kind}_sk, {kind}_key, valid_from_utc, is_current, row_hash, effective_basis
) VALUES ('0', '_UNKNOWN', '0001-01-01'::TIMESTAMP_NTZ, TRUE, 'unknown', 'unknown');
""")
    return "\n".join(statements)


def desired_versions_sql(kind):
    if kind not in DIMENSIONS:
        raise ValueError("Unknown dimension")
    return f"""
CREATE OR REPLACE TEMP TABLE version_{kind} AS
WITH all_events AS (
    SELECT * FROM dimension_event WHERE kind = '{kind}'
    UNION ALL
    SELECT i.* FROM event_input i WHERE i.kind = '{kind}' AND NOT EXISTS (
        SELECT 1 FROM dimension_event e WHERE e.kind = i.kind
        AND e.natural_key = i.natural_key AND e.effective_at_utc = i.effective_at_utc
    )
), ordered AS (
    SELECT *, LAG(row_hash) OVER (
        PARTITION BY natural_key ORDER BY effective_at_utc
    ) AS previous_hash FROM all_events
), changes AS (
    SELECT * FROM ordered WHERE previous_hash IS NULL OR previous_hash <> row_hash
)
SELECT SHA2(kind || '|' || natural_key || '|' ||
    TO_VARCHAR(effective_at_utc, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') || '|' || row_hash, 256)
    AS surrogate_key,
    natural_key, effective_at_utc AS valid_from_utc,
    LEAD(effective_at_utc) OVER (PARTITION BY natural_key ORDER BY effective_at_utc)
        AS valid_to_utc,
    row_hash, attributes, source_uri, source_generation, effective_basis
FROM changes
"""


def scd_merge_body():
    statements = [
        """
MERGE INTO dimension_event d USING event_input s
ON d.kind = s.kind AND d.natural_key = s.natural_key
    AND d.effective_at_utc = s.effective_at_utc
WHEN NOT MATCHED THEN INSERT (
    kind, natural_key, effective_at_utc, row_hash, attributes,
    source_uri, source_generation, effective_basis
) VALUES (s.kind, s.natural_key, s.effective_at_utc, s.row_hash, s.attributes,
    s.source_uri, s.source_generation, s.effective_basis);
"""
    ]
    for kind, attributes in DIMENSIONS.items():
        names = ", ".join(attributes)
        values = ", ".join(f's.attributes:"{k}"::{v}' for k, v in attributes.items())
        statements.append(f"""
MERGE INTO dim_{kind} d USING version_{kind} s ON d.{kind}_sk = s.surrogate_key
WHEN MATCHED AND (d.valid_to_utc IS DISTINCT FROM s.valid_to_utc) THEN
    UPDATE SET valid_to_utc = s.valid_to_utc, is_current = (s.valid_to_utc IS NULL)
WHEN NOT MATCHED THEN INSERT (
    {kind}_sk, {kind}_key, valid_from_utc, valid_to_utc, is_current,
    row_hash, attributes, source_uri, source_generation, effective_basis, {names}
) VALUES (
    s.surrogate_key, s.natural_key, s.valid_from_utc, s.valid_to_utc,
    s.valid_to_utc IS NULL, s.row_hash, s.attributes, s.source_uri,
    s.source_generation, s.effective_basis, {values}
);
-- Late documented events can make a previously inferred change boundary redundant.
-- The source event remains in the ledger; facts are re-keyed in the same transaction.
DELETE FROM dim_{kind} WHERE {kind}_sk <> '0' AND {kind}_sk NOT IN (
    SELECT surrogate_key FROM version_{kind}
);
""")
    return "\n".join(statements)


def render(path, config):
    context = {k: identifier(config[k]) for k in ("database", "schema", "warehouse")}
    bucket = config["bucket"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]+", bucket):
        raise ValueError("Invalid GCS bucket")
    context.update(
        bucket=bucket,
        epa_columns=",\n".join(f"{k} {v}" for k, v in EPA_TYPES.items()),
        dimension_ddl=dimension_ddl(),
        scd_merge_body=scd_merge_body(),
    )
    source = path.read_text()
    return re.sub(r"\{\{(\w+)\}\}", lambda match: context[match[1]], source)
