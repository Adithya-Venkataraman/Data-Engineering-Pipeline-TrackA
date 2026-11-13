"""Lead-row preprocessing for Assignment A.

The public entry point is :func:`normalize_row`.  It returns a small result
object so ingestion code can handle valid rows and rejections uniformly.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping


_WHITESPACE_OR_UNDERSCORE = re.compile(r"[_\s]+")
_NON_DIGIT = re.compile(r"\D+")
_CURRENCY_PREFIX = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)
_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y")


@dataclass(frozen=True)
class NormalizedRow:
    """Result of normalizing one non-blank CSV row."""

    values: dict[str, Any] | None
    warnings: tuple[str, ...] = field(default_factory=tuple)
    rejection_reason: str | None = None
    batch_id: str | None = None
    row_number: int | None = None

    @property
    def accepted(self) -> bool:
        return self.rejection_reason is None


def load_schema(path: str | Path) -> dict[str, Any]:
    """Load one workflow JSON schema without changing its source definition."""

    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)


def normalize_header(value: str) -> str:
    """Normalize a CSV header according to the assignment rules."""

    return _WHITESPACE_OR_UNDERSCORE.sub(" ", value.strip().lower())


def normalize_phone(value: str) -> str | None:
    """Return canonical Indian mobile format, or ``None`` if invalid."""

    digits = _NON_DIGIT.sub("", value.strip())
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]

    if len(digits) != 10 or digits[0] not in "6789":
        return None
    return f"+91{digits}"


def normalize_date(value: str) -> str | None:
    """Parse supported day-first formats and return ISO ``YYYY-MM-DD``."""

    text = value.strip()
    for date_format in _DATE_FORMATS:
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            continue
    return None


def normalize_decimal(value: str) -> Decimal | None:
    """Parse supported currency formats, returning a non-negative 2dp value."""

    text = _CURRENCY_PREFIX.sub("", value.strip()).replace(",", "")
    try:
        amount = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_enum(value: str, allowed: Iterable[str]) -> str | None:
    """Match enum values case-insensitively and store the schema value."""

    lowered = value.strip().lower()
    for candidate in allowed:
        if lowered == candidate.lower():
            return candidate.lower()
    return None


def normalize_row(
    raw_row: Mapping[str, Any],
    schema: Mapping[str, Any],
    *,
    workflow_id: str | None = None,
    batch_id: str | None = None,
    row_number: int | None = None,
) -> NormalizedRow:
    """Normalize and validate one CSV row against a workflow schema.

    Unknown columns are ignored.  Fields are processed in schema order so the
    first validation failure is deterministic.  Optional invalid values become
    ``None`` and add one warning; they do not reject the row.
    """

    normalized_input: dict[str, str] = {}
    for raw_name, raw_value in raw_row.items():
        if raw_name is None:
            continue
        normalized_name = normalize_header(str(raw_name))
        if normalized_name and normalized_name not in normalized_input:
            normalized_input[normalized_name] = "" if raw_value is None else str(raw_value).strip()

    field_by_header: dict[str, Mapping[str, Any]] = {}
    fields = schema.get("fields", [])
    for field_definition in fields:
        name = str(field_definition["name"])
        field_by_header[normalize_header(name)] = field_definition
        for alias in field_definition.get("aliases", []):
            field_by_header[normalize_header(str(alias))] = field_definition

    values: dict[str, Any] = {}
    warnings: list[str] = []
    effective_workflow_id = workflow_id or str(schema.get("workflow_id", ""))

    for field_definition in fields:
        name = str(field_definition["name"])
        field_type = str(field_definition.get("type", "string"))
        source_value = ""
        for header, raw_value in normalized_input.items():
            if field_by_header.get(header, {}).get("name") == name:
                source_value = raw_value
                break

        if not source_value:
            if field_definition.get("required"):
                return NormalizedRow(
                    None, tuple(warnings), f"missing_required:{name}", batch_id, row_number
                )
            values[name] = None
            continue

        converted: Any = source_value
        valid = True
        if field_type == "string":
            converted = source_value
        elif field_type == "phone":
            converted = normalize_phone(source_value)
            valid = converted is not None
        elif field_type == "date":
            converted = normalize_date(source_value)
            valid = converted is not None
        elif field_type == "decimal":
            converted = normalize_decimal(source_value)
            valid = converted is not None
        elif field_type == "enum":
            converted = normalize_enum(source_value, field_definition.get("values", []))
            valid = converted is not None

        if not valid:
            if field_type == "phone":
                return NormalizedRow(None, tuple(warnings), "invalid_phone", batch_id, row_number)
            if field_definition.get("required"):
                return NormalizedRow(
                    None, tuple(warnings), f"invalid_{name}", batch_id, row_number
                )
            warnings.append(f"invalid_{name}")
            values[name] = None
            continue

        values[name] = converted

    for field_definition in fields:
        name = str(field_definition["name"])
        if values.get(name) is None and field_definition.get("default") is not None:
            values[name] = field_definition["default"]

    result: dict[str, Any] = {"workflow_id": effective_workflow_id, **values}
    if batch_id is not None:
        result["batch_id"] = batch_id
    if row_number is not None:
        result["row_number"] = row_number
    return NormalizedRow(result, tuple(warnings))


def iter_normalized_csv(
    path: str | Path,
    schema: Mapping[str, Any],
    *,
    batch_id: str | None = None,
) -> Iterator[NormalizedRow]:
    """Yield results for non-blank rows in one UTF-8 CSV file."""

    with Path(path).open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        row_number = 0
        for row in reader:
            if not any(str(value or "").strip() for value in row.values()):
                continue
            row_number += 1
            yield normalize_row(row, schema, batch_id=batch_id, row_number=row_number)
