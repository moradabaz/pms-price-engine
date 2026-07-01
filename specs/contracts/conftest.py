"""Shared loaders for contract tests.

Schemas in specs/events/*.json are the source of truth for every event shape
in this project. These tests validate example messages (fixtures/) against
those schemas using structural JSON Schema validation (required fields, enums,
const, additionalProperties). "format" keywords (uuid, date, date-time) are
intentionally NOT enforced — jsonschema's format assertions need optional
dependencies not declared in specs/pyproject.toml, so this suite checks shape,
not string formatting, and calls that out explicitly rather than silently
skipping it.
"""

import json
from pathlib import Path

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "events"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def load_schema(filename: str) -> dict:
    return json.loads((SCHEMAS_DIR / filename).read_text())


def load_fixture(event_type: str, filename: str) -> dict:
    return json.loads((FIXTURES_DIR / event_type / filename).read_text())
