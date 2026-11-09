"""S06 operations against the S05 ingestion-state tables."""

import json
from contextlib import closing, contextmanager


class State:
    def __init__(self, connection):
        self.conn = connection

    def execute(self, query, params=(), *, one=False):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone() if one else None
        self.conn.commit()
        return result

    @contextmanager
    def lock(self, entry):
        # Session locks survive the commits below; closing the connection releases
        # a lock even if a worker is killed. Never share this connection by thread.
        key = f"epa:{entry.pollutant}:{entry.year}"
        acquired = self.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (key,), one=True
        )[0]
        if not acquired:
            raise RuntimeError(f"Another worker owns {key}; rerun after it finishes")
        try:
            yield
        finally:
            self.conn.rollback()
            self.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))

    def register(self, entry, run_id):
        skipped = entry.status != "listed_nonempty"
        row = self.execute(
            """INSERT INTO ingest_file AS f
               (pollutant, year, file_name, source_url, listed_rows, listed_bytes,
                catalog_last_modified, catalog_status, last_run_id,
                download_state, stage_state, process_state)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (pollutant, year) DO UPDATE SET
                 file_name = EXCLUDED.file_name, source_url = EXCLUDED.source_url,
                 listed_rows = EXCLUDED.listed_rows,
                 listed_bytes = EXCLUDED.listed_bytes,
                 catalog_last_modified = EXCLUDED.catalog_last_modified,
                 catalog_status = EXCLUDED.catalog_status,
                 last_run_id = EXCLUDED.last_run_id, updated_at_utc = now(),
                 download_state = CASE
                   WHEN EXCLUDED.catalog_status <> 'listed_nonempty' THEN 'skipped'
                   WHEN ROW(f.file_name, f.source_url, f.listed_rows, f.listed_bytes,
                            f.catalog_last_modified, f.catalog_status)
                        IS DISTINCT FROM
                        ROW(EXCLUDED.file_name, EXCLUDED.source_url,
                            EXCLUDED.listed_rows, EXCLUDED.listed_bytes,
                            EXCLUDED.catalog_last_modified, EXCLUDED.catalog_status)
                     THEN 'pending' ELSE f.download_state END,
                 stage_state = CASE
                   WHEN EXCLUDED.catalog_status <> 'listed_nonempty' THEN 'skipped'
                   WHEN f.catalog_status <> 'listed_nonempty' THEN 'pending'
                   ELSE f.stage_state END,
                 process_state = CASE
                   WHEN EXCLUDED.catalog_status <> 'listed_nonempty' THEN 'skipped'
                   WHEN f.catalog_status <> 'listed_nonempty' THEN 'pending'
                   ELSE f.process_state END
               RETURNING ingest_file_id, download_state, raw_uri, raw_generation""",
            (
                entry.pollutant,
                entry.year,
                entry.file_name,
                entry.url,
                entry.listed_rows,
                entry.listed_bytes,
                entry.last_modified,
                entry.status,
                run_id,
                *(["skipped" if skipped else "pending"] * 3),
            ),
            one=True,
        )
        return row

    def downloaded(self, file_id, artifact, run_id):
        # Publish current state and immutable version history in one transaction.
        try:
            with closing(self.conn.cursor()) as cursor:
                cursor.execute(
                    """UPDATE ingest_file SET
                       stage_state = CASE WHEN raw_generation = %s AND raw_uri = %s
                         THEN stage_state ELSE 'pending' END,
                       process_state = CASE WHEN raw_generation = %s AND raw_uri = %s
                         THEN process_state ELSE 'pending' END,
                       staged_rows = CASE WHEN raw_generation = %s AND raw_uri = %s
                         THEN staged_rows ELSE NULL END,
                       processed_rows = CASE WHEN raw_generation = %s AND raw_uri = %s
                         THEN processed_rows ELSE NULL END,
                       download_state = 'downloaded', raw_uri = %s,
                       raw_generation = %s, raw_md5 = %s, raw_size_bytes = %s,
                       last_run_id = %s, updated_at_utc = now()
                       WHERE ingest_file_id = %s""",
                    (
                        *(artifact.generation, artifact.uri) * 4,
                        artifact.uri,
                        artifact.generation,
                        artifact.md5,
                        artifact.size,
                        run_id,
                        file_id,
                    ),
                )
                cursor.execute(
                    """INSERT INTO ingest_file_version
                       (ingest_file_id, raw_generation, raw_uri, raw_md5,
                        raw_size_bytes, downloaded_at_utc, run_id)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (ingest_file_id, raw_generation) DO NOTHING""",
                    (
                        file_id,
                        artifact.generation,
                        artifact.uri,
                        artifact.md5,
                        artifact.size,
                        artifact.created_at,
                        run_id,
                    ),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def failed(self, file_id, run_id):
        self.conn.rollback()
        self.execute(
            """UPDATE ingest_file SET download_state = 'failed', last_run_id = %s,
               updated_at_utc = now() WHERE ingest_file_id = %s""",
            (run_id, file_id),
        )

    def start_run(self, run_id, count, details):
        self.execute(
            """INSERT INTO ingest_run
               (run_id, job_name, status, started_at_utc, input_count, details)
               VALUES (%s,'epa_download','running',now(),%s,%s::jsonb)""",
            (run_id, count, json.dumps(details)),
        )

    def finish_run(self, run_id, counts, errors):
        self.execute(
            """UPDATE ingest_run SET status = %s, finished_at_utc = now(),
               output_count = %s, error_message = %s,
               details = details || %s::jsonb WHERE run_id = %s""",
            (
                "failed" if errors else "succeeded",
                counts.get("downloaded", 0),
                f"{len(errors)} file(s) failed" if errors else None,
                json.dumps({"outcomes": counts, "errors": errors}),
                run_id,
            ),
        )
