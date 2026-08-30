"""BigQuery config and client as FastAPI dependencies.

Both are `Depends`-injected rather than module globals so tests can override them with a fake
client, and lazy so importing `api.main` doesn't require credentials or a network round-trip.
"""

from __future__ import annotations

from functools import lru_cache

from dotenv import load_dotenv
from google.cloud import bigquery

from loaders.config import BigQueryConfig


@lru_cache(maxsize=1)
def get_config() -> BigQueryConfig:
    load_dotenv()
    return BigQueryConfig.from_env()


@lru_cache(maxsize=1)
def get_client() -> bigquery.Client:
    config = get_config()
    return bigquery.Client(project=config.project, location=config.location)
