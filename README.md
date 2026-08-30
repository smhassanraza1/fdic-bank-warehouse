# FIG Data Platform

An end-to-end data platform for U.S. bank regulatory filings: batch and streaming ingestion of FDIC
BankFind data into a dbt-modelled BigQuery star schema, orchestrated by Airflow and served over a
read-only REST API.

Roughly 4,500 institutions across 12 quarters, plus branch-level deposit data (millions of rows) as
the volume driver for the Spark layer. Every component runs on a free tier — no billing account, no
cloud deploy. `docker compose up` and one DAG trigger rebuild the warehouse from scratch.

## Architecture

```
FDIC BankFind API ──┐
                    ├──> RAW (parquet on R2) ──PySpark──> CLEANED (typed, deduped, SCD2)
Kafka topic ────────┘                                              │
(filing.received)                                         BigQuery load job
                                                                   ▼
                                                       CURATED (dbt: dims, facts, marts)
                                                                   │
                                                                   ▼
                                                             FastAPI (REST)
```

Airflow orchestrates every arrow. GitHub Actions gates every commit.

Three layers, three different contracts:

| Layer | Contract | Guarantees |
|---|---|---|
| **Raw** | Immutable audit trail | Payload as received plus `_ingested_at`, `_source_url`, `_batch_id`. Never modified; idempotent via partition overwrite. |
| **Cleaned** | Conformed engineering layer | Explicit schemas, typed casts, dedupe on natural keys, SCD2 history. Invalid rows quarantined with a reason, never dropped. |
| **Curated** | Consumer contract | dbt dims, facts, and marts. Tests gate publication — bad data is never marked fresh. |

## Stack

Python 3.13 · PySpark · Apache Kafka (KRaft) · Cloudflare R2 · BigQuery · dbt Core · Airflow ·
FastAPI · Docker Compose · GitHub Actions

## Quickstart

**Requirements:** Docker + Docker Compose, [`uv`](https://docs.astral.sh/uv/), a BigQuery Sandbox
project, and a Cloudflare R2 bucket. Both cloud services are free tier and need no card beyond R2's
signup.

```bash
git clone <repo-url> && cd fdic-bank-warehouse

cp .env.example .env     # fill in FDIC / R2 / BigQuery / Airflow values
                         # and place your GCP service-account key at ./gcp-credentials.json

uv sync --all-groups     # installs into .venv
docker compose up -d     # Kafka + Airflow (:8080) + API (:8000)
```

Run the pipeline: open `localhost:8080` (`admin`/`admin`), unpause `fig_pipeline`, and trigger it.
The DAG runs `ingest_api → validate_raw → spark_clean → load_bigquery → dbt_run → dbt_test →
publish_freshness_metric`.

Query the result:

```bash
curl "localhost:8000/health"
curl "localhost:8000/institutions/3510/financials?from=2024-01-01&to=2024-12-31&limit=4"
curl "localhost:8000/rankings?metric=roa&state=TX&limit=10"
```

Interactive API docs at `localhost:8000/docs`.

<details>
<summary>Running components outside Docker</summary>

```bash
# dbt — run from the repo root so the relative creds path in .env resolves
uv run --group dbt dbt build --target dev --project-dir dbt --profiles-dir dbt

# API
uv run --group api uvicorn api.main:app --reload

# Kafka replay into the streaming path
docker compose up -d stream-consumer
uv run --group streaming python -m streaming.producer --repdte 20230331 --limit 500
```

`dbt/profiles.yml` is committed and contains no credentials — every value is `env_var()`-sourced.
Airflow points `DBT_PROFILES_DIR` at it directly.

Dependency groups (`uv sync --group <name>`) split by concern so each environment pulls only what it
needs: `dev`, `api`, `spark`, `dbt`, `streaming`. Airflow is not among them — it runs only in
Docker.

</details>

## Repo structure

```
ingestion/     FDIC API client, raw-layer writer, generated schemas, validation
streaming/     Kafka producer (replay) and consumer (micro-batching to raw)
transform/     PySpark cleaning: typing, dedupe, SCD2, quarantine
loaders/       BigQuery load jobs and the freshness metric
dbt/           Curated models (staging → marts) and data tests
dags/          fig_pipeline — the single Airflow DAG over the whole chain
api/           FastAPI serving layer: routes, response models, SQL builders
tests/         Unit tests for the pure logic in all of the above
```

## Data model

Sourced from the [FDIC BankFind Suite API](https://banks.data.fdic.gov/docs/) (public, no key):
`/institutions` (one row per bank), `/financials` (one row per bank per quarter), and `/sod`
(branch-level deposits). The API caps `limit` at 10k, so offset pagination is mandatory.

| Curated model | Type | Grain |
|---|---|---|
| `dim_institution` | dimension (SCD2) | cert × validity window |
| `dim_date` | dimension | quarter-end |
| `fct_bank_quarterly_financials` | fact | cert × quarter |
| `mart_latest_financials` | mart | one row per cert — its latest reported quarter |
| `mart_profitability_trends` | mart | ROA/ROE quarter-over-quarter by bank |
| `mart_deposit_concentration` | mart | deposit share / HHI by state |
| `mart_asset_growth` | mart | QoQ and YoY asset growth, ranked |

The fact joins the dimension on the SCD2 validity window rather than on `cert` alone, so a filing
resolves against the bank's attributes *as of that quarter* instead of today's. dbt enforces
`unique`/`not_null` on keys, `relationships` from fact to dimension, `accepted_values` on `bkclass`,
and a custom test that no `cert` ever has two overlapping validity windows.

## API

Read-only, over the curated layer.

| Endpoint | Reads | Notes |
|---|---|---|
| `GET /institutions` | `dim_institution` + `mart_latest_financials` | filter `state`, `bkclass`, `min_asset`, `max_asset` |
| `GET /institutions/{cert}` | `dim_institution` (`is_current`) | 404 if no current row |
| `GET /institutions/{cert}/financials` | `fct_bank_quarterly_financials` | quarterly series, ascending, `from`/`to` |
| `GET /rankings` | `mart_profitability_trends`, `mart_asset_growth` | `metric`, `state`, `period`; defaults to latest period |
| `GET /health` | `pipeline_freshness` | liveness + warehouse freshness |

No user input reaches SQL as text: every filter is a BigQuery query parameter, and the two things
that cannot be parameters — the ranking metric's column and its source mart — resolve through a
closed enum, so an unknown metric is a 422 before any query is built. Responses share one error
envelope, `{"error": {"code", "message", "detail"}}`, whether the failure came from validation or
from BigQuery.

## Engineering invariants

Six properties the pipeline holds, each visible in code and checked by tests:

1. **Idempotency** — any task, any period, re-runnable with an identical result.
2. **SCD2 history** — attributes are joinable as-of any past quarter.
3. **Restatement handling** — an amended filing collapses onto its original by `(cert, repdte)`.
4. **Schema evolution tolerance** — a new upstream column does not break the pipeline.
5. **Cost awareness** — bytes scanned are measured, not assumed.
6. **No silent data loss** — nothing is dropped without a recorded reason.

## Testing & CI

```bash
uv run ruff check .    # lint
uv run pytest          # 83 unit tests
```

Unit tests cover the pure logic — pagination, retry and backoff, SCD2 change detection, dedupe,
schema casting, SQL building — with no network and no live warehouse. Data-quality tests are dbt's,
and they gate publication rather than merely reporting.

`.github/workflows/ci.yml` runs lint and tests on every push. It needs no credentials, so a fork
runs it clean. `dbt build` runs locally and in Airflow, where the warehouse is reachable.
Idempotency was verified separately by triggering `fig_pipeline` twice for the same period against
the live warehouse: the second run left every curated table's row count unchanged.

## License

[MIT](LICENSE).
