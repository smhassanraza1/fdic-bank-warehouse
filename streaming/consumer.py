"""Consumes `filing.received` and micro-batches it into the raw layer.

Writes to `raw/financials_stream/`, a sibling of the batch `raw/financials/` dataset, which the
cleaning layer unions in and dedupes on `(cert, repdte)`. They stay separate because each raw
partition is read with an inferred schema, and streamed JSON round-trips to different parquet
types than the batch file — so each is cast independently, then unioned on the cast schema.

One parquet per flush, never one per record: R2 bills per write op, not per byte.

Delivery is at-least-once — offsets move only after the write lands, so a crash between the two
replays the batch. Exactly-once would need a transaction spanning Kafka and R2, and the replay
is already absorbed downstream: same offsets overwrite the same object, same filing dedupes.

Run with (producer first, then this):

    uv run python -m streaming.consumer --max-messages 500
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from dotenv import load_dotenv

from ingestion.raw_writer import ObjectStore, R2ObjectStore, write_raw_partition
from streaming.producer import DEFAULT_TOPIC, REPDTE_PARTITION_FIELD

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STREAM_DATASET = "financials_stream"
DEFAULT_GROUP_ID = "fig-raw-writer"


@dataclass(frozen=True)
class ConsumedMessage:
    """One decoded Kafka record. Decouples the batching logic from `confluent_kafka`."""

    key: str
    value: dict[str, Any]
    partition: int
    offset: int


class MessageSource(Protocol):
    """A pollable, manually-committed message stream. Faked in tests; Kafka in production."""

    def poll(self, timeout: float) -> ConsumedMessage | None: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


def build_flush_filename(kafka_partition: int, first_offset: int, last_offset: int) -> str:
    """Name a flush by the offset range it covers, so a replay overwrites instead of appending."""
    return f"stream-p{kafka_partition}-{first_offset:012d}-{last_offset:012d}.parquet"


def group_for_write(
    messages: list[ConsumedMessage],
) -> dict[tuple[str, int], list[ConsumedMessage]]:
    """Group a buffered micro-batch by `(repdte partition, kafka partition)`, one file each.

    Splitting on the Kafka partition too keeps each file's offset range contiguous, so the
    filename can name it.
    """
    groups: dict[tuple[str, int], list[ConsumedMessage]] = defaultdict(list)
    for message in messages:
        repdte = message.value.get(REPDTE_PARTITION_FIELD)
        if repdte is None:
            logger.warning(
                "message p%d@%d has no %s — skipping",
                message.partition,
                message.offset,
                REPDTE_PARTITION_FIELD,
            )
            continue
        groups[(str(repdte), message.partition)].append(message)
    return dict(groups)


def flush(
    store: ObjectStore,
    messages: list[ConsumedMessage],
    *,
    topic: str = DEFAULT_TOPIC,
    consumed_at: datetime | None = None,
) -> list[str]:
    """Write one raw parquet file per `(repdte, kafka partition)` group. Returns written keys."""
    consumed_at = consumed_at or datetime.now(UTC)
    written: list[str] = []

    for (repdte, kafka_partition), group in sorted(group_for_write(messages).items()):
        offsets = [message.offset for message in group]
        records = [
            {
                **message.value,
                "_kafka_topic": topic,
                "_kafka_partition": message.partition,
                "_kafka_offset": message.offset,
            }
            for message in group
        ]
        result = write_raw_partition(
            store,
            STREAM_DATASET,
            repdte,
            records,
            source_url=f"kafka://{topic}",
            filename=build_flush_filename(kafka_partition, min(offsets), max(offsets)),
            extra_columns={"_consumed_at": consumed_at.isoformat()},
        )
        written.append(result.key)
        logger.info(
            "%s dt=%s: wrote %d filings -> %s", STREAM_DATASET, repdte, result.row_count, result.key
        )
    return written


def run_consumer(
    source: MessageSource,
    store: ObjectStore,
    *,
    topic: str = DEFAULT_TOPIC,
    max_messages: int = 1_000,
    max_seconds: float = 10.0,
    max_empty_polls: int = 3,
    poll_timeout: float = 1.0,
    now: Any = time.monotonic,
) -> int:
    """Poll, buffer, flush, then commit — in that order. Returns total messages written.

    Flushes on whichever of `max_messages`/`max_seconds` comes first, so a slow trickle of
    filings still lands in raw. `max_empty_polls=0` polls forever, which is how the consumer
    runs as a service; a positive value stops after that many consecutive empty polls, which is
    what makes a one-off demo run terminate.

    Committing only after `flush` returns is the at-least-once contract.
    """
    buffer: list[ConsumedMessage] = []
    written_total = 0
    empty_polls = 0
    # Age of the buffer, not time since last flush -- else an idle gap causes a 1-row flush.
    buffer_started: float | None = None

    def flush_and_commit() -> None:
        nonlocal buffer, written_total, buffer_started
        if buffer:
            flush(store, buffer, topic=topic)
            written_total += len(buffer)
            source.commit()
            buffer = []
        buffer_started = None

    def buffer_expired() -> bool:
        return buffer_started is not None and now() - buffer_started >= max_seconds

    while True:
        message = source.poll(poll_timeout)
        if message is None:
            empty_polls += 1
            if buffer_expired():
                flush_and_commit()
            if max_empty_polls and empty_polls >= max_empty_polls:
                break
            continue

        empty_polls = 0
        if not buffer:
            buffer_started = now()
        buffer.append(message)
        if len(buffer) >= max_messages or buffer_expired():
            flush_and_commit()

    flush_and_commit()
    logger.info("consumer finished: %d filings written to %s", written_total, STREAM_DATASET)
    return written_total


class KafkaMessageSource:
    """Adapts `confluent_kafka.Consumer` to `MessageSource`.

    Auto-commit off, since offsets must move only once the raw write has landed. Earliest
    offset reset, so a fresh consumer group replays rather than silently consuming nothing.
    """

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str) -> None:
        from confluent_kafka import Consumer

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([topic])

    def poll(self, timeout: float) -> ConsumedMessage | None:
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(f"kafka consume error: {message.error()}")
        key = message.key()
        return ConsumedMessage(
            key=key.decode() if key else "",
            value=json.loads(message.value().decode()),
            partition=message.partition(),
            offset=message.offset(),
        )

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Micro-batch filing.received into raw.")
    parser.add_argument("--max-messages", type=int, default=1_000, help="flush every N messages")
    parser.add_argument("--max-seconds", type=float, default=10.0, help="flush every N seconds")
    parser.add_argument(
        "--max-empty-polls",
        type=int,
        default=3,
        help="stop after N idle polls; 0 polls forever (how the service runs)",
    )
    args = parser.parse_args()

    load_dotenv()
    topic = os.environ.get("KAFKA_TOPIC_FILING_RECEIVED", DEFAULT_TOPIC)
    source = KafkaMessageSource(
        os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        topic,
        os.environ.get("KAFKA_CONSUMER_GROUP", DEFAULT_GROUP_ID),
    )
    store = R2ObjectStore.from_env()
    try:
        run_consumer(
            source,
            store,
            topic=topic,
            max_messages=args.max_messages,
            max_seconds=args.max_seconds,
            max_empty_polls=args.max_empty_polls,
        )
    finally:
        source.close()


if __name__ == "__main__":
    main()
