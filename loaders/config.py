"""Where the warehouse lives — shared by the loader that writes it and the API that reads it.

Its own module, not part of `bq_load`, so `api/` can name a table without importing pyarrow,
boto3 and the R2 client it will never use.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BigQueryConfig:
    project: str
    dataset: str
    location: str = "US"

    @classmethod
    def from_env(cls) -> BigQueryConfig:
        return cls(
            project=os.environ["BQ_PROJECT"],
            dataset=os.environ.get("BQ_DATASET", "fig_gold"),
            location=os.environ.get("BQ_LOCATION", "US"),
        )

    @property
    def dataset_ref(self) -> str:
        return f"{self.project}.{self.dataset}"

    def table_ref(self, table: str) -> str:
        return f"{self.dataset_ref}.{table}"
