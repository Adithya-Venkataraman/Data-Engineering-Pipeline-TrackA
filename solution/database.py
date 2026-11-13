"""PostgreSQL schema and persistence helpers for Assignment A."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS leads (
    lead_id BIGSERIAL PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    phone_hash TEXT NOT NULL,
    phone_masked TEXT NOT NULL,
    customer_name_hash TEXT NOT NULL,
    fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workflow_id, phone_hash)
);

CREATE TABLE IF NOT EXISTS lead_rejections (
    rejection_id BIGSERIAL PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL,
    UNIQUE (workflow_id, batch_id, row_number)
);

CREATE TABLE IF NOT EXISTS interaction_events (
    event_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    phone_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    lead_id BIGINT REFERENCES leads(lead_id),
    is_orphan BOOLEAN NOT NULL DEFAULT TRUE,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS interaction_events_call_idx
    ON interaction_events (call_id);
CREATE INDEX IF NOT EXISTS interaction_events_lead_idx
    ON interaction_events (lead_id);

CREATE TABLE IF NOT EXISTS processed_files (
    file_path TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_name TEXT PRIMARY KEY,
    metric_value BIGINT NOT NULL
);
"""


def connection_string() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql://au:au_local_only@localhost:55432/au_assignment",
    )


@contextmanager
def get_connection() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(connection_string(), row_factory=dict_row) as connection:
        yield connection


def initialize_database() -> None:
    """Create all required tables and indexes safely on every run."""

    with get_connection() as connection:
        connection.execute(SCHEMA_SQL)


def upsert_leads(leads: list[dict[str, Any]]) -> int:
    """Insert protected leads; merge JSON fields on a duplicate identity."""

    query = """
    INSERT INTO leads
        (workflow_id, batch_id, phone_hash, phone_masked, customer_name_hash, fields)
    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (workflow_id, phone_hash) DO UPDATE SET
        fields = leads.fields || EXCLUDED.fields,
        phone_masked = EXCLUDED.phone_masked,
        customer_name_hash = EXCLUDED.customer_name_hash,
        updated_at = now()
    """
    with get_connection() as connection:
        for lead in leads:
            connection.execute(
                query,
                (
                    lead["workflow_id"],
                    lead["batch_id"],
                    lead["phone_hash"],
                    lead["phone_masked"],
                    lead["customer_name_hash"],
                    json.dumps(lead.get("fields", {}), default=str),
                ),
            )
    return len(leads)


def insert_rejections(rejections: list[dict[str, Any]]) -> int:
    query = """
    INSERT INTO lead_rejections (workflow_id, batch_id, row_number, reason)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (workflow_id, batch_id, row_number) DO NOTHING
    """
    with get_connection() as connection:
        for rejection in rejections:
            connection.execute(
                query,
                (
                    rejection["workflow_id"],
                    rejection["batch_id"],
                    rejection["row_number"],
                    rejection["reason"],
                ),
            )
    return len(rejections)


def insert_events(events: list[dict[str, Any]]) -> int:
    """Insert events idempotently and link direct/override events."""

    query = """
    INSERT INTO interaction_events
        (event_id, workflow_id, call_id, occurred_at, event_type,
         phone_hash, payload, lead_id, is_orphan)
    VALUES (
        %s, %s, %s, %s::timestamptz, %s, %s, %s::jsonb,
        COALESCE(
            (SELECT lead_id FROM leads WHERE workflow_id = %s AND phone_hash = %s),
            (SELECT lead_id FROM interaction_events prior
             WHERE prior.call_id = %s AND prior.lead_id IS NOT NULL
             LIMIT 1)
        ),
        (
            COALESCE(
                (SELECT lead_id FROM leads WHERE workflow_id = %s AND phone_hash = %s),
                (SELECT lead_id FROM interaction_events prior
                 WHERE prior.call_id = %s AND prior.lead_id IS NOT NULL
                 LIMIT 1)
            ) IS NULL
        )
    )
    ON CONFLICT (event_id) DO NOTHING
    """
    with get_connection() as connection:
        for event in events:
            lead_key = event.get("lead_key")
            lead_workflow, lead_phone_hash = lead_key if lead_key else (None, None)
            connection.execute(
                query,
                (
                    event["event_id"],
                    event["workflow_id"],
                    event["call_id"],
                    event["occurred_at"],
                    event["event_type"],
                    event.get("phone_hash"),
                    json.dumps(event.get("payload", {}), default=str),
                    lead_workflow,
                    lead_phone_hash,
                    event["call_id"],
                    lead_workflow,
                    lead_phone_hash,
                    event["call_id"],
                ),
            )
    return len(events)


def relink_orphans() -> int:
    """Link previously orphaned direct-phone events after leads arrive."""

    query = """
    UPDATE interaction_events AS events
    SET lead_id = leads.lead_id, is_orphan = FALSE
    FROM leads
    WHERE events.is_orphan = TRUE
      AND events.phone_hash IS NOT NULL
      AND events.workflow_id = leads.workflow_id
      AND events.phone_hash = leads.phone_hash
    """
    with get_connection() as connection:
        cursor = connection.execute(query)
        return cursor.rowcount


def save_metrics(metrics: dict[str, int]) -> None:
    """Persist the latest cumulative ingestion counters for reporting."""

    query = """
    INSERT INTO pipeline_metrics (metric_name, metric_value)
    VALUES (%s, %s)
    ON CONFLICT (metric_name) DO UPDATE SET metric_value = EXCLUDED.metric_value
    """
    with get_connection() as connection:
        for name, value in metrics.items():
            if isinstance(value, int):
                connection.execute(query, (name, value))


def load_metrics() -> dict[str, int]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT metric_name, metric_value FROM pipeline_metrics"
        ).fetchall()
    return {row["metric_name"]: row["metric_value"] for row in rows}
