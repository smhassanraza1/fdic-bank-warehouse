"""Tests for cleaned_branch_deposits: YEAR->REPDTE derivation, dedupe, quarantine."""

from __future__ import annotations

from transform.cleaned_branch_deposits import clean_branch_deposits

_RAW_SCHEMA = (
    "CERT long, UNINUMBR long, NAMEFULL string, CITYBR string, STALPBR string, "
    "STNAMEBR string, DEPSUMBR double, ASSET double, YEAR long, "
    "_ingested_at string, _batch_id string"
)


def _row(cert, uninumbr, year, depsumbr=100.0, ingested_at="2026-01-01T00:00:00Z", batch_id="b1"):
    return (
        cert,
        uninumbr,
        "Bank A Branch",
        "Chicago",
        "IL",
        "Illinois",
        depsumbr,
        1000.0,
        year,
        ingested_at,
        batch_id,
    )


def test_repdte_is_derived_as_june_30th(spark) -> None:
    df = spark.createDataFrame([_row(1, 100, 2023)], _RAW_SCHEMA)
    clean, _ = clean_branch_deposits(df)
    assert clean.collect()[0].repdte == "20230630"


def test_dedupe_on_cert_uninumbr_repdte_keeps_latest(spark) -> None:
    df = spark.createDataFrame(
        [
            _row(1, 100, 2023, depsumbr=100.0, ingested_at="2026-01-01T00:00:00Z"),
            _row(1, 100, 2023, depsumbr=200.0, ingested_at="2026-02-01T00:00:00Z"),
        ],
        _RAW_SCHEMA,
    )
    clean, _ = clean_branch_deposits(df)
    rows = clean.collect()
    assert len(rows) == 1
    assert rows[0].depsumbr == 200.0


def test_quarantines_missing_key_and_negative_deposits(spark) -> None:
    df = spark.createDataFrame(
        [
            _row(1, 100, 2023, depsumbr=100.0),
            _row(2, None, 2023, depsumbr=100.0),
            _row(3, 300, 2023, depsumbr=-10.0),
        ],
        _RAW_SCHEMA,
    )
    clean, quarantine = clean_branch_deposits(df)
    assert clean.count() == 1
    reasons = {row.cert: row._reject_reason for row in quarantine.collect()}
    assert reasons[2] == "missing_key"
    assert reasons[3] == "negative_value"
