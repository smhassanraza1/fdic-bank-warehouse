"""Tests for cleaned_scd2: the pure hash function, and the Spark-based SCD2 collapse
(unchanged attrs -> no new row, changed attrs -> close+open, low-water-mark initial load)."""

from __future__ import annotations

from datetime import date

from transform.cleaned_scd2 import (
    LOW_WATER_MARK,
    OPEN_VALID_TO,
    build_institution_history,
    institution_row_hash,
)


def test_hash_is_independent_of_dict_key_order() -> None:
    row_a = {"NAME": "Bank A", "CITY": "Springfield", "STALP": "IL", "BKCLASS": "N"}
    row_b = {"BKCLASS": "N", "STALP": "IL", "CITY": "Springfield", "NAME": "Bank A"}
    assert institution_row_hash(row_a) == institution_row_hash(row_b)


def test_hash_normalizes_missing_and_none_and_empty() -> None:
    missing = institution_row_hash({"NAME": "Bank A"})
    explicit_none = institution_row_hash({"NAME": "Bank A", "REGAGNT": None})
    empty_string = institution_row_hash({"NAME": "Bank A", "REGAGNT": ""})
    assert missing == explicit_none == empty_string


def test_hash_changes_when_a_tracked_attr_changes() -> None:
    base = {"NAME": "Bank A", "CITY": "Springfield"}
    changed = {"NAME": "Bank A", "CITY": "Chicago"}
    assert institution_row_hash(base) != institution_row_hash(changed)


def _row(
    cert,
    snapshot_date,
    name,
    city="Springfield",
    stalp="IL",
    bkclass="N",
    regagnt="FDIC",
    namehcr=None,
):
    return (cert, snapshot_date, name, city, stalp, bkclass, regagnt, namehcr)


_SCHEMA = (
    "CERT long, SNAPSHOT_DATE date, NAME string, CITY string, STALP string, "
    "BKCLASS string, REGAGNT string, NAMEHCR string"
)


def test_single_snapshot_gets_low_water_mark_and_is_current(spark) -> None:
    df = spark.createDataFrame(
        [_row(1, date(2026, 7, 31), "Bank A")],
        _SCHEMA,
    )
    result = build_institution_history(df).collect()

    assert len(result) == 1
    row = result[0]
    assert row.valid_from == LOW_WATER_MARK
    assert row.valid_to == OPEN_VALID_TO
    assert row.is_current is True


def test_unchanged_attrs_across_snapshots_collapse_to_one_row(spark) -> None:
    df = spark.createDataFrame(
        [
            _row(2, date(2025, 1, 1), "Bank B"),
            _row(2, date(2026, 7, 31), "Bank B"),
        ],
        _SCHEMA,
    )
    result = build_institution_history(df).collect()

    assert len(result) == 1
    assert result[0].valid_from == LOW_WATER_MARK
    assert result[0].valid_to == OPEN_VALID_TO
    assert result[0].is_current is True


def test_changed_attr_closes_old_row_and_opens_new_one(spark) -> None:
    df = spark.createDataFrame(
        [
            _row(3, date(2025, 1, 1), "Bank C"),
            _row(3, date(2026, 7, 31), "Bank C Renamed"),
        ],
        _SCHEMA,
    )
    rows = sorted(build_institution_history(df).collect(), key=lambda r: r.valid_from)

    assert len(rows) == 2
    first, second = rows
    assert first.name == "Bank C"
    assert first.valid_from == LOW_WATER_MARK
    assert first.valid_to == "2026-07-30"
    assert first.is_current is False

    assert second.name == "Bank C Renamed"
    assert second.valid_from == "2026-07-31"
    assert second.valid_to == OPEN_VALID_TO
    assert second.is_current is True


def test_low_water_mark_makes_as_of_join_match_pre_snapshot_quarters(spark) -> None:
    """Guards against a zero-row as-of join for quarters before the institutions snapshot."""
    from pyspark.sql import functions as F

    history_df = build_institution_history(
        spark.createDataFrame([_row(4, date(2026, 7, 31), "Bank D")], _SCHEMA)
    )
    financials_df = spark.createDataFrame([(4, "20230331")], "cert long, repdte string")

    joined = financials_df.join(
        history_df,
        (financials_df.cert == history_df.cert)
        & (F.to_date(financials_df.repdte, "yyyyMMdd").between(
            F.to_date(history_df.valid_from), F.to_date(history_df.valid_to)
        )),
    )
    assert joined.count() == 1
