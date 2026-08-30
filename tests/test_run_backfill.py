from __future__ import annotations

from ingestion.run_backfill import quarters_in_range, years_in_range


def test_quarters_in_range_filters_to_bounds() -> None:
    assert quarters_in_range("20230630", "20240331") == [
        "20230630",
        "20230930",
        "20231231",
        "20240331",
    ]


def test_quarters_in_range_full_span_returns_all() -> None:
    assert quarters_in_range("20230331", "20251231") == [
        "20230331",
        "20230630",
        "20230930",
        "20231231",
        "20240331",
        "20240630",
        "20240930",
        "20241231",
        "20250331",
        "20250630",
        "20250930",
        "20251231",
    ]


def test_years_in_range_filters_to_bounds() -> None:
    assert years_in_range("20230630", "20240331") == [2023, 2024]


def test_years_in_range_single_year() -> None:
    assert years_in_range("20240101", "20241231") == [2024]
