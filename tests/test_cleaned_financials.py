"""Tests for cleaned_financials: allowlist projection (schema-evolution tolerance), dedupe
determinism, and quarantine reasons."""

from __future__ import annotations

from transform.cleaned_financials import clean_financials

_RAW_SCHEMA = (
    "CERT long, REPDTE string, NAME string, STALP string, STNAME string, BKCLASS string, "
    "ASSET double, DEP double, EQ double, NETINC double, ROA double, ROE double, "
    "_ingested_at string, _batch_id string"
)


def _row(
    cert,
    repdte,
    asset=1000.0,
    ingested_at="2026-01-01T00:00:00+00:00",
    batch_id="b1",
    name="Bank A",
):
    return (
        cert,
        repdte,
        name,
        "IL",
        "Illinois",
        "N",
        asset,
        500.0,
        100.0,
        10.0,
        1.0,
        5.0,
        ingested_at,
        batch_id,
    )


def test_allowlist_projection_tolerates_an_unknown_extra_raw_column(spark) -> None:
    """A new upstream column shouldn't break cleaning -- it's simply not selected."""
    df = spark.createDataFrame(
        [(*_row(1, "20230331"), "some new upstream value")],
        _RAW_SCHEMA + ", SOME_NEW_FIELD string",
    )
    clean, quarantine = clean_financials(df)
    assert clean.count() == 1
    assert quarantine.count() == 0
    assert "some_new_field" not in clean.columns


def test_dedupe_keeps_latest_ingested_at(spark) -> None:
    df = spark.createDataFrame(
        [
            _row(1, "20230331", name="Old Name", ingested_at="2026-01-01T00:00:00+00:00"),
            _row(1, "20230331", name="New Name", ingested_at="2026-02-01T00:00:00+00:00"),
        ],
        _RAW_SCHEMA,
    )
    clean, _ = clean_financials(df)
    rows = clean.collect()
    assert len(rows) == 1
    assert rows[0].name == "New Name"


def test_dedupe_is_deterministic_on_tied_ingested_at(spark) -> None:
    """Same `_ingested_at` on both rows -- `_batch_id` breaks the tie so reruns are stable."""
    df = spark.createDataFrame(
        [
            _row(1, "20230331", name="Batch A", ingested_at="2026-01-01T00:00:00Z", batch_id="a"),
            _row(1, "20230331", name="Batch Z", ingested_at="2026-01-01T00:00:00Z", batch_id="z"),
        ],
        _RAW_SCHEMA,
    )
    first_run = clean_financials(df)[0].collect()
    second_run = clean_financials(df)[0].collect()
    assert first_run == second_run
    assert first_run[0].name == "Batch Z"


def test_quarantines_missing_key_and_negative_asset(spark) -> None:
    df = spark.createDataFrame(
        [
            _row(1, "20230331", asset=1000.0),
            _row(None, "20230630", asset=1000.0),
            _row(2, "20230930", asset=-50.0),
        ],
        _RAW_SCHEMA,
    )
    clean, quarantine = clean_financials(df)
    assert clean.count() == 1
    reasons = {row.cert: row._reject_reason for row in quarantine.collect()}
    assert reasons[None] == "missing_key"
    assert reasons[2] == "negative_value"
