"""Codegen: derive per-dataset field schemas from FDIC's published property YAMLs.

Run manually (`python -m ingestion.schemas.generate_schemas`) whenever the upstream YAMLs
change. Output is committed JSON, not regenerated at pipeline runtime, so ingestion never
takes a network dependency on api.fdic.gov's docs host.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import yaml

SCHEMAS_DIR = Path(__file__).parent

SOURCES = {
    "institutions": "https://api.fdic.gov/banks/docs/institution_properties.yaml",
    "financials": "https://api.fdic.gov/banks/docs/risview_properties.yaml",
    "sod": "https://api.fdic.gov/banks/docs/sod_properties.yaml",
}


def _extract_fields(yaml_text: str) -> dict[str, dict[str, Any]]:
    """Pull the flat FIELD -> {type, title, description} map out of a property YAML.

    Each YAML is a JSON-schema-shaped document: properties.data.properties.<FIELD>.
    Only the field-level `type` is kept — nested `x-source-mapping[].formula.type`
    values (e.g. "percentage", "simpleDivision") describe how FDIC derives the field
    upstream, not its wire type, so they're ignored here.
    """
    doc = yaml.safe_load(yaml_text)
    raw_fields = doc["properties"]["data"]["properties"]
    fields: dict[str, dict[str, Any]] = {}
    for name, spec in raw_fields.items():
        fields[name] = {
            "type": spec.get("type", "string"),
            "title": spec.get("title") or "",
            "description": (spec.get("description") or "").strip(),
        }
    return fields


def generate() -> None:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for dataset, url in SOURCES.items():
            resp = client.get(url)
            resp.raise_for_status()
            fields = _extract_fields(resp.text)
            out_path = SCHEMAS_DIR / f"{dataset}.json"
            out_path.write_text(json.dumps(fields, indent=2, sort_keys=True) + "\n")
            print(f"{dataset}: wrote {len(fields)} fields -> {out_path}")


if __name__ == "__main__":
    generate()
