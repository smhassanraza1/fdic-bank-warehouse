"""Field schemas for FDIC datasets, codegenned from FDIC's property YAMLs.

Load them with `load_schema(dataset)` rather than reading the JSON directly, so callers
don't hardcode paths.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Literal

Dataset = Literal["institutions", "financials", "sod"]

_SCHEMAS_DIR = Path(__file__).parent


class FieldSpec:
    __slots__ = ("name", "type", "title", "description")

    def __init__(self, name: str, type: str, title: str, description: str) -> None:  # noqa: A002
        self.name = name
        self.type = type
        self.title = title
        self.description = description


@cache
def load_schema(dataset: Dataset) -> dict[str, FieldSpec]:
    """Return the known field->spec map for a dataset. Raises FileNotFoundError if ungenerated."""
    path = _SCHEMAS_DIR / f"{dataset}.json"
    raw: dict[str, dict[str, str]] = json.loads(path.read_text())
    return {name: FieldSpec(name=name, **spec) for name, spec in raw.items()}


__all__ = ["Dataset", "FieldSpec", "load_schema"]
