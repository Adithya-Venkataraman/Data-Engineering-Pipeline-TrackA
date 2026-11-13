"""Derived lead state and report generation for Assignment A."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .database import get_connection, load_metrics
from .event_ingestion import IST, load_dispositions
from .preprocessing import load_schema


REPORT_HEADERS = {
    "batch_summary.csv": [
        "workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
        "in_progress_telephony_failed", "in_progress_dialed", "completed",
    ],
    "daily_overview.csv": [
        "workflow_id", "date", "total_calls", "connected_calls",
        "connectivity_rate", "success_leads",
    ],
    "lead_report.csv": [
        "workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
        "total_calls", "connected_calls", "last_call_at", "current_disposition",
        "ever_success",
    ],
    "rejections.csv": ["workflow_id", "batch_id", "row_number", "reason"],
}


def _write_csv(path: Path, headers: list[str], rows: list[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _effective_disposition(
    events: list[Mapping[str, Any]],
) -> tuple[str | None, Mapping[str, bool] | None]:
    overrides = [event for event in events if event["event_type"] == "disposition.override"]
    sets = [event for event in events if event["event_type"] == "disposition.set"]
    candidates = overrides or sets
    if not candidates:
        return None, None
    selected = max(candidates, key=lambda event: (event["occurred_at"], event["event_id"]))
    code = selected["payload"].get("disposition_code")
    return code, selected.get("disposition_meta")


def _build_lead_facts(
    lead: Mapping[str, Any],
    events: list[Mapping[str, Any]],
) -> dict[str, Any]:
    calls: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        calls[event["call_id"]].append(event)

    call_facts: list[dict[str, Any]] = []
    for call_events in calls.values():
        initiated = [event for event in call_events if event["event_type"] == "call.initiated"]
        if not initiated:
            continue
        initiated_event = min(initiated, key=lambda event: (event["occurred_at"], event["event_id"]))
        disposition, disposition_meta = _effective_disposition(call_events)
        call_facts.append(
            {
                "initiated_at": initiated_event["occurred_at"],
                "date": initiated_event["occurred_at"].date().isoformat(),
                "connected": any(event["event_type"] == "call.connected" for event in call_events),
                "telephony_failed": any(
                    event["event_type"] == "call.telephony_failed" for event in call_events
                ),
                "disposition": disposition,
                "disposition_meta": disposition_meta,
            }
        )

    if not call_facts:
        bucket = "yet_to_reach_out"
    elif any(
        fact["disposition_meta"] and fact["disposition_meta"].get("is_terminal")
        for fact in call_facts
    ):
        bucket = "completed"
    elif all(fact["telephony_failed"] for fact in call_facts):
        bucket = "in_progress_telephony_failed"
    else:
        bucket = "in_progress_dialed"

    disposition_calls = [fact for fact in call_facts if fact["disposition"]]
    latest_disposition = max(
        disposition_calls, key=lambda fact: fact["initiated_at"]
    )["disposition"] if disposition_calls else ""
    ever_success = any(
        fact["disposition_meta"] and fact["disposition_meta"].get("is_success")
        for fact in call_facts
    )
    return {
        "workflow_id": lead["workflow_id"],
        "external_ref": lead["fields"].get("external_ref", ""),
        "batch_id": lead["batch_id"],
        "phone_masked": lead["phone_masked"],
        "bucket": bucket,
        "total_calls": len(call_facts),
        "connected_calls": sum(fact["connected"] for fact in call_facts),
        "last_call_at": (
            max((fact["initiated_at"] for fact in call_facts)).isoformat(timespec="seconds")
            if call_facts
            else ""
        ),
        "current_disposition": latest_disposition,
        "ever_success": ever_success,
        "calls": call_facts,
    }


def generate_reports(
    output_dir: str | Path,
    *,
    schema_root: str | Path,
    dispositions_path: str | Path,
) -> None:
    """Generate all five reports from the current PostgreSQL state."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    schemas = {}
    for path in sorted(Path(schema_root).glob("*.json")):
        schema = load_schema(path)
        reference_field = next(
            field["name"] for field in schema["fields"] if field.get("reference")
        )
        schemas[schema["workflow_id"]] = reference_field
    dispositions = load_dispositions(dispositions_path)

    with get_connection() as connection:
        leads = connection.execute(
            "SELECT lead_id, workflow_id, batch_id, phone_masked, fields FROM leads"
        ).fetchall()
        events = connection.execute(
            """
            SELECT event_id, workflow_id, call_id, occurred_at, event_type,
                   payload, lead_id
            FROM interaction_events
            WHERE is_orphan = FALSE
            ORDER BY occurred_at, event_id
            """
        ).fetchall()
        rejections = connection.execute(
            """
            SELECT workflow_id, batch_id, row_number, reason
            FROM lead_rejections
            ORDER BY workflow_id, batch_id, row_number
            """
        ).fetchall()
        quality = connection.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM leads) AS lead_rows_total,
              (SELECT COUNT(*) FROM lead_rejections) AS lead_rows_rejected,
              (SELECT COUNT(*) FROM interaction_events) AS stored_events,
              (SELECT COUNT(*) FROM interaction_events WHERE is_orphan) AS orphan_events
            """
        ).fetchone()
    stored_metrics = load_metrics()

    events_by_lead: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        event = dict(event)
        event["occurred_at"] = event["occurred_at"].astimezone(IST)
        code = event["payload"].get("disposition_code")
        event["disposition_meta"] = (
            dispositions.get(event["workflow_id"], {}).get(code) if code else None
        )
        events_by_lead[event["lead_id"]].append(event)

    facts = []
    for lead in leads:
        lead = dict(lead)
        fields = dict(lead["fields"])
        lead["fields"] = fields
        lead["fields"]["external_ref"] = fields.get(
            schemas[lead["workflow_id"]], ""
        )
        facts.append(_build_lead_facts(lead, events_by_lead[lead["lead_id"]]))

    facts.sort(key=lambda row: (row["workflow_id"], row["external_ref"]))

    batch_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for fact in facts:
        batch_groups[(fact["workflow_id"], fact["batch_id"])].append(fact)
    batch_rows = []
    for (workflow_id, batch_id), group in sorted(batch_groups.items()):
        counts = defaultdict(int)
        for fact in group:
            counts[fact["bucket"]] += 1
        batch_rows.append(
            {
                "workflow_id": workflow_id,
                "batch_id": batch_id,
                "total_leads": len(group),
                "yet_to_reach_out": counts["yet_to_reach_out"],
                "in_progress_telephony_failed": counts["in_progress_telephony_failed"],
                "in_progress_dialed": counts["in_progress_dialed"],
                "completed": counts["completed"],
            }
        )

    daily_calls: dict[tuple[str, str], list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for fact in facts:
        for call in fact["calls"]:
            daily_calls[(fact["workflow_id"], call["date"])].append((fact, call))
    daily_rows = []
    for (workflow_id, day), calls in sorted(daily_calls.items()):
        total = len(calls)
        connected = sum(call["connected"] for _, call in calls)
        success_leads = {
            id(fact) for fact, call in calls
            if call["disposition_meta"] and call["disposition_meta"].get("is_success")
        }
        daily_rows.append(
            {
                "workflow_id": workflow_id,
                "date": day,
                "total_calls": total,
                "connected_calls": connected,
                "connectivity_rate": f"{connected / total:.4f}",
                "success_leads": len(success_leads),
            }
        )

    _write_csv(output_dir / "batch_summary.csv", REPORT_HEADERS["batch_summary.csv"], batch_rows)
    _write_csv(output_dir / "daily_overview.csv", REPORT_HEADERS["daily_overview.csv"], daily_rows)
    _write_csv(
        output_dir / "lead_report.csv",
        REPORT_HEADERS["lead_report.csv"],
        [
            {
                key: (
                    str(fact[key]).lower()
                    if isinstance(fact[key], bool)
                    else fact[key]
                )
                for key in REPORT_HEADERS["lead_report.csv"]
            }
            for fact in facts
        ],
    )
    _write_csv(output_dir / "rejections.csv", REPORT_HEADERS["rejections.csv"], [dict(row) for row in rejections])
    (output_dir / "data_quality.json").write_text(
        json.dumps(
            {
                "lead_rows_total": stored_metrics.get("lead_rows_total", quality["lead_rows_total"]),
                "lead_rows_rejected": stored_metrics.get("lead_rows_rejected", quality["lead_rows_rejected"]),
                "rejections_by_reason": {
                    reason: sum(1 for row in rejections if row["reason"] == reason)
                    for reason in sorted({row["reason"] for row in rejections})
                },
                "duplicate_lead_rows_merged": stored_metrics.get("duplicate_lead_rows_merged", 0),
                "field_warnings": stored_metrics.get("field_warnings", 0),
                "events_total": stored_metrics.get("events_total", quality["stored_events"]),
                "malformed_events": stored_metrics.get("malformed_events", 0),
                "duplicate_events_dropped": stored_metrics.get("duplicate_events_dropped", 0),
                "orphan_events": stored_metrics.get("orphan_events", quality["orphan_events"]),
                "unknown_disposition_events": stored_metrics.get("unknown_disposition_events", 0),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
