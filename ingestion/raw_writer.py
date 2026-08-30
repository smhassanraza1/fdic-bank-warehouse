"""Writes ingested records to the raw layer as partition-overwrite parquet.

One file per `(dataset, partition)`, written to a deterministic key so a re-run overwrites
via a plain PUT rather than accumulating `part-*` files — that's what makes raw idempotent
and keeps R2 Class A (write) ops low.
"""

from __future__ import annotations

import io
import os
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


class ObjectStore(Protocol):
    """Storage the raw layer writes to and the cleaning/loading layers read from.

    `LocalObjectStore` for tests, `R2ObjectStore` for real runs.
    """

    def put_bytes(self, key: str, data: bytes) -> None: ...
    def get_bytes(self, key: str) -> bytes: ...
    def list_partitions(self, dataset: str, layer: str = "raw") -> list[str]: ...
    def list_partition_keys(
        self, dataset: str, partition: str, layer: str = "raw"
    ) -> list[str]: ...


def _partition_from_key(key: str, dataset: str, layer: str = "raw") -> str | None:
    """Extract the `dt=<partition>` value from a `{layer}/{dataset}/dt=.../data.parquet` key."""
    prefix = f"{layer}/{dataset}/dt="
    if not key.startswith(prefix):
        return None
    return key[len(prefix) :].split("/", 1)[0]


class LocalObjectStore:
    """Writes to a local directory, mirroring the `key` as a relative path. Used in tests."""

    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir

    def put_bytes(self, key: str, data: bytes) -> None:
        path = os.path.join(self._base_dir, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)

    def get_bytes(self, key: str) -> bytes:
        with open(os.path.join(self._base_dir, key), "rb") as f:
            return f.read()

    def list_partitions(self, dataset: str, layer: str = "raw") -> list[str]:
        root = os.path.join(self._base_dir, layer, dataset)
        if not os.path.isdir(root):
            return []
        partitions = [
            name[len("dt=") :] for name in os.listdir(root) if name.startswith("dt=")
        ]
        return sorted(partitions)

    def list_partition_keys(
        self, dataset: str, partition: str, layer: str = "raw"
    ) -> list[str]:
        prefix = f"{layer}/{dataset}/dt={partition}"
        root = os.path.join(self._base_dir, prefix)
        if not os.path.isdir(root):
            return []
        return sorted(f"{prefix}/{name}" for name in os.listdir(root) if name.endswith(".parquet"))


class R2ObjectStore:
    """Writes to a Cloudflare R2 bucket via boto3's S3-compatible client."""

    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "auto",
    ) -> None:
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
        )

    def put_bytes(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)

    def get_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        return response["Body"].read()

    def list_partitions(self, dataset: str, layer: str = "raw") -> list[str]:
        prefix = f"{layer}/{dataset}/"
        partitions: set[str] = set()
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                partition = _partition_from_key(obj["Key"], dataset, layer)
                if partition is not None:
                    partitions.add(partition)
        return sorted(partitions)

    def list_partition_keys(
        self, dataset: str, partition: str, layer: str = "raw"
    ) -> list[str]:
        prefix = f"{layer}/{dataset}/dt={partition}/"
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            keys.extend(
                obj["Key"] for obj in page.get("Contents", []) if obj["Key"].endswith(".parquet")
            )
        return sorted(keys)

    @classmethod
    def from_env(cls) -> R2ObjectStore:
        account_id = os.environ["R2_ACCOUNT_ID"]
        endpoint_url = os.environ.get(
            "R2_ENDPOINT_URL", f"https://{account_id}.r2.cloudflarestorage.com"
        )
        return cls(
            bucket=os.environ["R2_BUCKET"],
            endpoint_url=endpoint_url,
            access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            region=os.environ.get("R2_REGION", "auto"),
        )


@dataclass(frozen=True)
class RawWriteResult:
    key: str
    row_count: int


def unwrap_record(item: dict[str, Any]) -> dict[str, Any]:
    """FDIC API items are shaped `{"data": {...}, "score": ...}`; `data` is the actual record."""
    return item["data"]


def build_partition_key(dataset: str, partition: str, filename: str = "data.parquet") -> str:
    return f"raw/{dataset}/dt={partition}/{filename}"


def write_raw_partition(
    store: ObjectStore,
    dataset: str,
    partition: str,
    records: Iterable[dict[str, Any]],
    *,
    source_url: str,
    batch_id: str | None = None,
    ingested_at: datetime | None = None,
    filename: str = "data.parquet",
    extra_columns: dict[str, Any] | None = None,
) -> RawWriteResult:
    """Write one overwrite-in-place parquet file for `(dataset, partition)`.

    A caller passes its own `filename` to land a file alongside the default `data.parquet`
    rather than clobbering it; `extra_columns` stamps per-write provenance onto every row.

    Adds `_ingested_at`, `_source_url`, `_batch_id` to every row. Records may have heterogeneous
    keys (new upstream fields, sparse fields); all rows are filled out to the union of keys
    seen so none are silently dropped.

    A record that already carries `_ingested_at`/`_batch_id` keeps its own, so a replayed copy
    never outranks its original in the cleaned layer's dedupe. `_source_url` is always this
    write's: it records how *this* copy arrived.
    """
    batch_id = batch_id or uuid.uuid4().hex
    ingested_at = ingested_at or datetime.now(UTC)
    ingested_at_iso = ingested_at.isoformat()

    rows = [
        {
            **record,
            "_ingested_at": record.get("_ingested_at", ingested_at_iso),
            "_batch_id": record.get("_batch_id", batch_id),
            "_source_url": source_url,
            **(extra_columns or {}),
        }
        for record in records
    ]
    if not rows:
        return RawWriteResult(
            key=build_partition_key(dataset, partition, filename), row_count=0
        )

    # Two full-size copies of the partition sit in memory here; fine at current volume.
    all_keys = dict.fromkeys(key for row in rows for key in row)
    normalized_rows = [{key: row.get(key) for key in all_keys} for row in rows]

    table = pa.Table.from_pylist(normalized_rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)

    key = build_partition_key(dataset, partition, filename)
    store.put_bytes(key, buf.getvalue())
    return RawWriteResult(key=key, row_count=len(rows))
