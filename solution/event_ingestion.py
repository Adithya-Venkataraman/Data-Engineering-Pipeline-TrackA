"""JSONL event ingestion for Assignment A."""

from __future__ import annotations

import csv
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

from .preprocessing import normalize_phone


IST = timezone(timedelta(hours=5, minutes=30))
DISPOSITION_EVENT_TYPES = {"disposition.set", "disposition.override"}


@dataclass
class EventIngestionResult:
    events: list[dict[str, Any]]
    quarantined: list[dict[str, Any]]
    metrics: dict[str, int]


def load_dispositions(path: str | Path) -> dict[str, dict[str, dict[str, bool]]]:
    """Load disposition metadata indexed by workflow and code."""

    result: dict[str, dict[str, dict[str, bool]]] = {}
    with Path(path).open("r", encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            workflow_id = row["workflow_id"].strip()
            code = row["code"].strip().lower()
            result.setdefault(workflow_id, {})[code] = {
                "is_terminal": row["is_terminal"].strip().lower() == "true",
                "is_success": row["is_success"].strip().lower() == "true",
            }
    return result


def normalize_event_timestamp(value: str) -> str | None:
    """Normalize UTC, offset, and naive-IST timestamps to ISO-8601 IST."""

    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)
    return parsed.astimezone(IST).isoformat(timespec="seconds")


def _hmac_phone(secret: bytes, phone: str) -> str:
    return hmac.new(secret, phone.encode("utf-8"), hashlib.sha256).hexdigest()


def ingest_events(
    data_root: str | Path,
    *,
    lead_phone_hashes: Iterable[tuple[str, str]],
    dispositions: Mapping[str, Mapping[str, Mapping[str, bool]]],
    pii_secret: str | bytes,
) -> EventIngestionResult:
    """Read, deduplicate, validate, and link all event files.

    Direct events link through ``(workflow_id, phone_hash)``.  Override events
    link through the call ID discovered on another event for that call.  Raw
    phone numbers are never returned in the event records.
    """

    root = Path(data_root)
    secret = pii_secret.encode("utf-8") if isinstance(pii_secret, str) else pii_secret
    if not secret:
        raise ValueError("pii_secret must not be empty")

    lead_keys = set(lead_phone_hashes)
    metrics = {
        "events_total": 0,
        "malformed_events": 0,
        "duplicate_events_dropped": 0,
        "orphan_events": 0,
        "unknown_disposition_events": 0,
    }
    seen_ids: set[str] = set()
    parsed_events: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []

    for drop_path in sorted(root.glob("drop_*")):
        events_root = drop_path / "events"
        for event_path in sorted(events_root.glob("*.jsonl")):
            with event_path.open("r", encoding="utf-8-sig") as file:
                for line_number, line in enumerate(file, start=1):
                    if not line.strip():
                        continue
                    metrics["events_total"] += 1
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError:
                        metrics["malformed_events"] += 1
                        continue

                    event_id = str(raw.get("event_id", "")).strip()
                    if not event_id:
                        metrics["malformed_events"] += 1
                        continue
                    if event_id in seen_ids:
                        metrics["duplicate_events_dropped"] += 1
                        continue
                    seen_ids.add(event_id)

                    workflow_id = str(raw.get("workflow_id", "")).strip()
                    event_type = str(raw.get("event_type", "")).strip()
                    call_id = str(raw.get("call_id", "")).strip()
                    occurred_at = normalize_event_timestamp(str(raw.get("occurred_at", "")))
                    if not workflow_id or not event_type or not call_id or occurred_at is None:
                        metrics["malformed_events"] += 1
                        continue

                    payload = raw.get("payload")
                    if not isinstance(payload, dict):
                        payload = {}

                    disposition_code: str | None = None
                    if event_type in DISPOSITION_EVENT_TYPES:
                        disposition_code = str(payload.get("disposition_code", "")).strip().lower()
                        if disposition_code not in dispositions.get(workflow_id, {}):
                            metrics["unknown_disposition_events"] += 1
                            continue

                    phone_hash: str | None = None
                    phone = str(raw.get("phone", "")).strip()
                    if phone:
                        canonical_phone = normalize_phone(phone)
                        if canonical_phone is not None:
                            phone_hash = _hmac_phone(secret, canonical_phone)

                    parsed_events.append(
                        {
                            "event_id": event_id,
                            "event_type": event_type,
                            "workflow_id": workflow_id,
                            "call_id": call_id,
                            "occurred_at": occurred_at,
                            "phone_hash": phone_hash,
                            "payload": payload,
                            "_line_number": line_number,
                            "_source_file": event_path.name,
                        }
                    )

    call_links: dict[str, tuple[str, str]] = {}
    for event in parsed_events:
        if event["phone_hash"] is not None:
            key = (event["workflow_id"], event["phone_hash"])
            if key in lead_keys:
                call_links[event["call_id"]] = key

    linked_events: list[dict[str, Any]] = []
    for event in parsed_events:
        key = None
        if event["phone_hash"] is not None:
            candidate = (event["workflow_id"], event["phone_hash"])
            if candidate in lead_keys:
                key = candidate
        if key is None:
            key = call_links.get(event["call_id"])

        clean_event = {k: v for k, v in event.items() if not k.startswith("_")}
        if key is None:
            metrics["orphan_events"] += 1
            quarantined.append(clean_event)
            continue
        clean_event["lead_key"] = key
        linked_events.append(clean_event)

    return EventIngestionResult(linked_events, quarantined, metrics)
