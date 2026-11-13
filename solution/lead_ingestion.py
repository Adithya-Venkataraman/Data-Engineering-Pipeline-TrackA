"""Deterministic lead-file ingestion and duplicate merging."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .preprocessing import NormalizedRow, iter_normalized_csv, load_schema


@dataclass
class LeadIngestionResult:
    """In-memory result ready for insertion into PostgreSQL."""

    leads: list[dict[str, Any]]
    rejections: list[dict[str, Any]]
    metrics: dict[str, Any]


@dataclass
class _MergedLead:
    workflow_id: str
    batch_id: str
    values: dict[str, Any] = field(default_factory=dict)


def _hmac_value(secret: bytes, value: str) -> str:
    return hmac.new(secret, value.encode("utf-8"), hashlib.sha256).hexdigest()


def _mask_phone(phone: str) -> str:
    return f"+91******{phone[-4:]}"


def _protect_lead(lead: _MergedLead, secret: bytes) -> dict[str, Any]:
    """Remove plaintext PII and return the database-facing lead record."""

    values = dict(lead.values)
    phone = values.pop("phone")
    customer_name = values.pop("customer_name")
    return {
        "workflow_id": lead.workflow_id,
        "batch_id": lead.batch_id,
        "phone_hash": _hmac_value(secret, phone),
        "phone_masked": _mask_phone(phone),
        "customer_name_hash": _hmac_value(secret, customer_name),
        "fields": values,
    }


def ingest_leads(
    data_root: str | Path,
    schema_root: str | Path,
    *,
    pii_secret: str | bytes,
) -> LeadIngestionResult:
    """Read all lead drops, normalize rows, merge duplicates, and protect PII.

    Files are processed in drop, workflow, filename, and row order.  The
    first valid row for ``(workflow_id, canonical_phone)`` fixes ``batch_id``;
    later valid rows overwrite only fields whose normalized value is non-null.
    """

    data_root = Path(data_root)
    schema_root = Path(schema_root)
    secret = pii_secret.encode("utf-8") if isinstance(pii_secret, str) else pii_secret
    if not secret:
        raise ValueError("pii_secret must not be empty")

    schemas: dict[str, Mapping[str, Any]] = {}
    for schema_path in sorted(schema_root.glob("*.json")):
        schema = load_schema(schema_path)
        schemas[str(schema["workflow_id"])] = schema

    merged: dict[tuple[str, str], _MergedLead] = {}
    rejections: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {
        "lead_rows_total": 0,
        "lead_rows_rejected": 0,
        "rejections_by_reason": {},
        "duplicate_lead_rows_merged": 0,
        "field_warnings": 0,
    }

    for drop_path in sorted(data_root.glob("drop_*")):
        leads_root = drop_path / "leads"
        for workflow_path in sorted(path for path in leads_root.iterdir() if path.is_dir()):
            workflow_id = workflow_path.name
            if workflow_id not in schemas:
                continue
            schema = schemas[workflow_id]
            for csv_path in sorted(workflow_path.glob("*.csv")):
                batch_id = csv_path.stem
                for result in iter_normalized_csv(csv_path, schema, batch_id=batch_id):
                    metrics["lead_rows_total"] += 1
                    metrics["field_warnings"] += len(result.warnings)
                    if not result.accepted:
                        reason = result.rejection_reason
                        metrics["lead_rows_rejected"] += 1
                        by_reason = metrics["rejections_by_reason"]
                        by_reason[reason] = by_reason.get(reason, 0) + 1
                        rejections.append(
                            {
                                "workflow_id": workflow_id,
                                "batch_id": batch_id,
                                "row_number": result.row_number,
                                "reason": reason,
                            }
                        )
                        continue

                    values = result.values
                    phone = values["phone"]
                    identity = (workflow_id, phone)
                    if identity not in merged:
                        merged[identity] = _MergedLead(
                            workflow_id=workflow_id,
                            batch_id=batch_id,
                            values=dict(values),
                        )
                        continue

                    existing = merged[identity]
                    for field_name, value in values.items():
                        if field_name not in {"workflow_id", "batch_id", "row_number"} and value is not None:
                            existing.values[field_name] = value
                    metrics["duplicate_lead_rows_merged"] += 1

    protected_leads = [
        _protect_lead(lead, secret)
        for _, lead in sorted(merged.items(), key=lambda item: item[0])
    ]
    rejections.sort(key=lambda row: (row["workflow_id"], row["batch_id"], row["row_number"] or 0))
    return LeadIngestionResult(protected_leads, rejections, metrics)
