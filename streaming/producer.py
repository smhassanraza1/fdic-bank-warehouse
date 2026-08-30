"""Replays raw financials filings onto the `filing.received` Kafka topic.

The FDIC publishes quarterly, so there is no live feed to tail: replaying already-ingested raw
partitions demonstrates the pattern, and keeps the payload's field names identical to what the
cleaning layer expects.

Key = `cert`, so a bank's filings stay ordered relative to each other — the guarantee that
matters when an amended filing follows its original. JSON, not Avro: the compose broker is bare
KRaft with no Schema Registry, which buys nothing at this scale.

Run with:

    uv run python -m streaming.producer --repdte 20230331 --limit 500
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
from collections.abc import Iterator
from typing import Any, Protocol

import pyarrow.parquet as pq
from dotenv import load_dotenv

from ingestion.raw_writer import ObjectStore, R2ObjectStore, build_partition_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_TOPIC = "filing.received"
REPDTE_PARTITION_FIELD = "_repdte_partition"


class Producer(Protocol):
    """The slice of `confluent_kafka.Producer` this module uses, so tests can fake it."""

    def produce(self, topic: str, *, key: bytes, value: bytes) -> None: ...
    def flush(self, timeout: float = ...) -> int: ...


def read_raw_filings(store: ObjectStore, repdte: str) -> list[dict[str, Any]]:
    data = store.get_bytes(build_partition_key("financials", repdte))
    return pq.read_table(io.BytesIO(data)).to_pylist()


def build_messages(
    filings: list[dict[str, Any]], repdte: str
) -> Iterator[tuple[bytes, bytes]]:
    """Yield `(key, value)` pairs: key = `cert` bytes, value = filing JSON + partition marker.

    A filing with no `CERT` is skipped: a null key round-robins, breaking per-bank ordering.
    """
    for filing in filings:
        cert = filing.get("CERT")
        if cert is None:
            logger.warning("skipping filing with no CERT in dt=%s", repdte)
            continue
        payload = {**filing, REPDTE_PARTITION_FIELD: repdte}
        yield str(cert).encode(), json.dumps(payload, default=str).encode()


def produce_partition(
    producer: Producer,
    store: ObjectStore,
    repdte: str,
    *,
    topic: str = DEFAULT_TOPIC,
    limit: int | None = None,
) -> int:
    """Replay one raw financials partition onto `topic`. Returns the message count produced."""
    filings = read_raw_filings(store, repdte)
    if limit is not None:
        filings = filings[:limit]

    produced = 0
    for key, value in build_messages(filings, repdte):
        producer.produce(topic, key=key, value=value)
        produced += 1

    producer.flush()
    logger.info("dt=%s: produced %d filings -> %s", repdte, produced, topic)
    return produced


def build_kafka_producer(bootstrap_servers: str) -> Producer:
    from confluent_kafka import Producer as KafkaProducer

    # Broker-side dedupe of producer retries; the end-to-end path is still at-least-once.
    return KafkaProducer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay raw filings onto Kafka.")
    parser.add_argument("--repdte", required=True, help="raw financials partition, e.g. 20230331")
    parser.add_argument("--limit", type=int, default=None, help="produce at most N filings")
    args = parser.parse_args()

    load_dotenv()
    producer = build_kafka_producer(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    store = R2ObjectStore.from_env()
    produce_partition(
        producer,
        store,
        args.repdte,
        topic=os.environ.get("KAFKA_TOPIC_FILING_RECEIVED", DEFAULT_TOPIC),
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
