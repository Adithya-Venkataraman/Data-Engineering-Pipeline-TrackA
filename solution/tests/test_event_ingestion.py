from pathlib import Path

from solution.event_ingestion import ingest_events, normalize_event_timestamp


def test_timestamps_are_normalized_to_ist():
    assert normalize_event_timestamp("2026-10-05T13:20:21Z") == "2026-10-05T18:50:21+05:30"
    assert normalize_event_timestamp("2026-10-05 10:00:00") == "2026-10-05T10:00:00+05:30"


def test_events_are_deduplicated_and_overrides_link_by_call(tmp_path: Path):
    events_dir = tmp_path / "drop_1" / "events"
    events_dir.mkdir(parents=True)
    lines = [
        '{"event_id":"e1","event_type":"call.initiated","workflow_id":"w","call_id":"c1","occurred_at":"2026-10-05T04:30:00Z","phone":"9876543210","payload":{}}',
        '{"event_id":"e1","event_type":"call.initiated","workflow_id":"w","call_id":"c1","occurred_at":"2026-10-05T04:30:00Z","phone":"9876543210","payload":{}}',
        '{"event_id":"e2","event_type":"disposition.override","workflow_id":"w","call_id":"c1","occurred_at":"2026-10-05T05:30:00Z","payload":{"disposition_code":"won"}}',
        '{"event_id":"e3","event_type":"call.initiated","workflow_id":"w","call_id":"c2","occurred_at":"2026-10-05T05:30:00Z","phone":"9999999999","payload":{}}',
    ]
    (events_dir / "events_0001.jsonl").write_text("\n".join(lines), encoding="utf-8")

    # This hash is deliberately produced using the same secret as ingestion.
    from solution.event_ingestion import _hmac_phone

    phone_hash = _hmac_phone(b"secret", "+919876543210")
    result = ingest_events(
        tmp_path,
        lead_phone_hashes={("w", phone_hash)},
        dispositions={"w": {"won": {"is_terminal": True, "is_success": True}}},
        pii_secret="secret",
    )

    assert result.metrics["events_total"] == 4
    assert result.metrics["duplicate_events_dropped"] == 1
    assert result.metrics["orphan_events"] == 1
    assert len(result.events) == 2
    assert all("phone" not in event for event in result.events)
