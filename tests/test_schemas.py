"""Tests for the codegenned FDIC field schemas."""

from __future__ import annotations

from ingestion.schemas import load_schema


def test_institutions_schema_has_known_fields() -> None:
    schema = load_schema("institutions")
    assert "CERT" in schema
    assert "NAME" in schema


def test_financials_schema_has_repdte() -> None:
    schema = load_schema("financials")
    assert "REPDTE" in schema


def test_sod_schema_has_branch_grain_fields() -> None:
    schema = load_schema("sod")
    assert "CERT" in schema
    assert "UNINUMBR" in schema
