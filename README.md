# Air Quality Planning for Outdoor Events

This project helps a Chicago-area outdoor fitness operator compare venues and
recurring class times using historical PM2.5, then review recent conditions
before a class. The planned pipeline lands national EPA AirData in Google Cloud
Storage, processes it with PySpark, keeps operational state in Cloud SQL, and
serves Chicago study tables from Snowflake to Streamlit and a read-only MCP
server. PurpleAir and AirNow provide recent context. See
[`plan/PROJECT_SEGMENTS.md`](plan/PROJECT_SEGMENTS.md) for scope and ownership.

## S01: scaffold and contracts

The shared schemas, keys, time rules, source mappings, and GCS object layout are
in [`docs/data_contracts.md`](docs/data_contracts.md). Non-secret defaults live
in [`config/settings.yaml`](config/settings.yaml). Copy `.env.example` to `.env`
for local development and fill values only when a later segment needs them.
Do not commit `.env` or credentials.

Use Python 3.11. Set up the development tools and run the checks:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
make lint test
```

The source data is not checked into this repository. EPA AirData comes from its
public [download catalog](https://aqs.epa.gov/aqsweb/airdata/download_files.html);
live sources and access requirements are recorded in the contracts. S01 is a
draft handoff until Yujie, Breanna, Bravo, and Tongyan review the contracts.
Changes to keys, grains, or units should be recorded there before dependent
segments implement them.

## Repository map

| Path | Purpose |
| --- | --- |
| `infra/` | Reproducible Google Cloud resources |
| `ingestion/` | Manifests, downloaders, and live collectors |
| `spark/` | Explicit schemas and distributed transformations |
| `cloudsql/migrations/` | Operational database schema |
| `snowflake/ddl/`, `snowflake/views/` | Warehouse model and serving layer |
| `analysis/eda/` | Exploratory work and Chicago scheduling analysis |
| `app/`, `mcp/` | Streamlit and read-only Claude access |
| `tests/fixtures/` | Small deterministic examples |
| `docs/`, `config/` | Contracts, reports, and non-secret configuration |
