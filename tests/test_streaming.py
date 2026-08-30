"""Tests for the Kafka path: message keying, micro-batch grouping, offset-range filenames, and
the flush-then-commit ordering that makes delivery at-least-once rather than at-most-once.

No broker, no network — the producer and the message source are both faked.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

from ingestion.raw_writer import LocalObjectStore, write_raw_partition
from streaming.consumer import (
    STREAM_DATASET,
    ConsumedMessage,
    build_flush_filename,
    flush,
    group_for_write,
    run_consumer,
)
from streaming.producer import build_messages, produce_partition, read_raw_filings


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.flushes = 0

    def produce(self, topic: str, *, key: bytes, value: bytes) -> None:
        self.messages.append((topic, key, value))

    def flush(self, timeout: float = 0.0) -> int:
        self.flushes += 1
        return 0


class FakeSource:
    """Replays a scripted poll sequence; `None` stands in for an empty poll."""

    def __init__(self, script: list[ConsumedMessage | None]) -> None:
        self._script = list(script)
        self.commits = 0

    def poll(self, timeout: float) -> ConsumedMessage | None:
        if not self._script:
            return None
        return self._script.pop(0)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        pass


def message(offset: int, cert: int, repdte: str = "20230331", partition: int = 0):
    return ConsumedMessage(
        key=str(cert),
        value={"CERT": cert, "REPDTE": repdte, "ASSET": 100.0, "_repdte_partition": repdte},
        partition=partition,
        offset=offset,
    )


def ticking_clock(step: float = 1.0):
    """A monotonic clock that advances `step` per reading, so tests never sleep."""
    t = 0.0

    def now() -> float:
        nonlocal t
        t += step
        return t

    return now


def _filing(cert: int, repdte: str, asset: float) -> dict:
    """A raw financials row carrying every column `project_financials` selects."""
    return {
        "CERT": cert,
        "REPDTE": repdte,
        "NAME": f"Bank {cert}",
        "STALP": "VA",
        "STNAME": "Virginia",
        "BKCLASS": "N",
        "ASSET": asset,
        "DEP": asset / 2,
        "EQ": asset / 10,
        "NETINC": asset / 100,
        "ROA": 1.0,
        "ROE": 10.0,
    }


def seeded_store(tmp_path, partition: str = "20230331"):
    store = LocalObjectStore(str(tmp_path))
    write_raw_partition(
        store,
        "financials",
        partition,
        [_filing(10042, partition, 1_000.0), _filing(10043, partition, 2_000.0)],
        source_url="https://example.test/financials",
        batch_id="batch-1",
        ingested_at=None,
    )
    return store


# --- producer -------------------------------------------------------------------------


def test_messages_are_keyed_by_cert_so_a_bank_keeps_its_ordering() -> None:
    keys = [key for key, _ in build_messages([{"CERT": 10042}, {"CERT": 7}], "20230331")]
    assert keys == [b"10042", b"7"]


def test_message_payload_carries_the_raw_partition_and_original_provenance() -> None:
    filing = {"CERT": 10042, "ASSET": 5.0, "_ingested_at": "2026-01-01T00:00:00+00:00"}
    _, value = next(build_messages([filing], "20230331"))

    payload = json.loads(value)
    assert payload["_repdte_partition"] == "20230331"
    assert payload["_ingested_at"] == "2026-01-01T00:00:00+00:00"


def test_filing_without_cert_is_skipped_not_produced_under_a_null_key() -> None:
    assert list(build_messages([{"CERT": None, "ASSET": 1.0}], "20230331")) == []


def test_produce_partition_replays_a_raw_partition_and_flushes(tmp_path) -> None:
    store = seeded_store(tmp_path)
    producer = FakeProducer()

    produced = produce_partition(producer, store, "20230331", topic="filing.received")

    assert produced == 2
    assert producer.flushes == 1
    assert {json.loads(value)["CERT"] for _, _, value in producer.messages} == {10042, 10043}


def test_read_raw_filings_round_trips_the_batch_written_partition(tmp_path) -> None:
    filings = read_raw_filings(seeded_store(tmp_path), "20230331")
    assert [f["CERT"] for f in filings] == [10042, 10043]
    assert all(f["_batch_id"] == "batch-1" for f in filings)


# --- consumer batching ----------------------------------------------------------------


def test_group_for_write_splits_by_repdte_and_kafka_partition() -> None:
    groups = group_for_write(
        [
            message(0, 1, "20230331", partition=0),
            message(1, 2, "20230630", partition=0),
            message(0, 3, "20230331", partition=1),
        ]
    )
    assert set(groups) == {("20230331", 0), ("20230630", 0), ("20230331", 1)}


def test_message_without_a_partition_marker_is_skipped() -> None:
    stray = ConsumedMessage(key="1", value={"CERT": 1}, partition=0, offset=0)
    assert group_for_write([stray]) == {}


def test_flush_filename_encodes_the_offset_range_it_covers() -> None:
    assert build_flush_filename(2, 5, 99) == "stream-p2-000000000005-000000000099.parquet"


def test_replaying_the_same_offsets_overwrites_rather_than_duplicating(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    batch = [message(0, 10042), message(1, 10043)]

    first = flush(store, batch)
    second = flush(store, batch)

    assert first == second
    keys = store.list_partition_keys(STREAM_DATASET, "20230331")
    assert len(keys) == 1
    assert pq.read_table(tmp_path / keys[0]).num_rows == 2


def test_flush_stamps_kafka_provenance_without_overwriting_ingestion_provenance(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    replayed = ConsumedMessage(
        key="10042",
        value={
            "CERT": 10042,
            "_repdte_partition": "20230331",
            "_ingested_at": "2026-01-01T00:00:00+00:00",
            "_batch_id": "batch-1",
        },
        partition=0,
        offset=7,
    )

    (key,) = flush(store, [replayed], topic="filing.received")
    row = pq.read_table(tmp_path / key).to_pylist()[0]

    # Preserved: a streamed copy must not look newer than its batch original to dedupe_latest.
    assert row["_ingested_at"] == "2026-01-01T00:00:00+00:00"
    assert row["_batch_id"] == "batch-1"
    # Not preserved: _source_url records how this copy arrived, and this one came from Kafka.
    assert row["_source_url"] == "kafka://filing.received"
    assert (row["_kafka_partition"], row["_kafka_offset"]) == (0, 7)
    assert row["_kafka_topic"] == "filing.received"


# --- consumer loop --------------------------------------------------------------------


def test_run_consumer_writes_then_commits_and_terminates_on_empty_polls(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    source = FakeSource([message(0, 10042), message(1, 10043), None, None, None])

    written = run_consumer(source, store, max_messages=10, max_seconds=1_000, max_empty_polls=3)

    assert written == 2
    assert source.commits == 1
    assert len(store.list_partition_keys(STREAM_DATASET, "20230331")) == 1


def test_run_consumer_flushes_every_max_messages(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    source = FakeSource([message(i, 10000 + i) for i in range(4)] + [None, None, None])

    written = run_consumer(source, store, max_messages=2, max_seconds=1_000, max_empty_polls=3)

    assert written == 4
    assert source.commits == 2
    # One file per flush, not per record: R2 write ops are the quota that costs money.
    assert store.list_partition_keys(STREAM_DATASET, "20230331") == [
        f"raw/{STREAM_DATASET}/dt=20230331/stream-p0-000000000000-000000000001.parquet",
        f"raw/{STREAM_DATASET}/dt=20230331/stream-p0-000000000002-000000000003.parquet",
    ]


def test_run_consumer_flushes_a_slow_trickle_on_the_time_bound(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    source = FakeSource([message(0, 10042), None, None, None])

    written = run_consumer(
        source, store, max_messages=1_000, max_seconds=3.0, max_empty_polls=3, now=ticking_clock()
    )

    assert written == 1
    assert source.commits == 1


def test_idle_gap_does_not_make_the_next_message_a_file_of_one(tmp_path) -> None:
    """The flush deadline ages the buffer, not the broker: idling must not force a tiny write."""
    store = LocalObjectStore(str(tmp_path))
    source = FakeSource([None] * 5 + [message(0, 10042), message(1, 10043)] + [None] * 10)

    written = run_consumer(
        source,
        store,
        max_messages=1_000,
        max_seconds=5.0,
        max_empty_polls=10,
        now=ticking_clock(),
    )

    assert written == 2
    (key,) = store.list_partition_keys(STREAM_DATASET, "20230331")
    assert pq.read_table(tmp_path / key).num_rows == 2


def test_max_empty_polls_zero_keeps_polling_through_idle_gaps(tmp_path) -> None:
    """How the service runs: an idle broker must not end the consumer."""

    class Unplugged(Exception):
        pass

    class EndlessSource(FakeSource):
        def poll(self, timeout: float):
            if not self._script:
                raise Unplugged  # stands in for the service being stopped
            return self._script.pop(0)

    store = LocalObjectStore(str(tmp_path))
    source = EndlessSource([None] * 10 + [message(0, 10042)])

    with pytest.raises(Unplugged):
        run_consumer(source, store, max_messages=1, max_empty_polls=0)

    assert source.commits == 1
    assert len(store.list_partition_keys(STREAM_DATASET, "20230331")) == 1


def test_no_commit_when_the_write_fails(tmp_path) -> None:
    """At-least-once: offsets must not move past a batch that never landed in raw."""

    class ExplodingStore(LocalObjectStore):
        def put_bytes(self, key: str, data: bytes) -> None:
            raise OSError("R2 unavailable")

    source = FakeSource([message(0, 10042), None, None, None])

    with pytest.raises(OSError):
        run_consumer(source, ExplodingStore(str(tmp_path)), max_empty_polls=3)

    assert source.commits == 0
