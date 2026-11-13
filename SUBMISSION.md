# Submission

## Candidate

- **Name:** Adithya Venkataraman
- **Assignment chosen:** Track-A Interaction Ledger & Reporting Pipeline (Data)
- **Time spent:** Approximately 5 hours

## How to run

From a clean clone, use Python 3.11+ and Docker Desktop:

```powershell
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
docker compose up -d
```

The Compose file exposes this project PostgreSQL container on host port
`55432` because host port `5432` was already occupied by another local
PostgreSQL service.

The current implementation can be exercised with:

```powershell
python -m py_compile solution\\*.py
python -c "from solution.database import initialize_database; initialize_database()"
```

The current report generation call is:

```powershell
python -c "from solution.reports import generate_reports; generate_reports('reports', schema_root='assignment-a-data/schemas', dispositions_path='assignment-a-data/schemas/dispositions.csv')"
```

Generate test data with:

```powershell
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
```

Generated data should not be committed.

## Design

```text
Lead CSVs ──> normalize/validate ──> merge duplicates ──> PostgreSQL leads
                                                           │
Event JSONL ─> parse/dedupe/link ─> PostgreSQL event ledger ─┘
                                                           │
                                                           ▼
                                           derived calls and lead buckets
                                                           │
                                                           ▼
                                                   five report files
```

Lead identity is `(workflow_id, canonical_phone)`. Workflow JSON files remain
the schema source of truth. Aliases are mapped; phones, dates, decimals, and
enums are normalized; invalid rows are rejected; optional invalid values become
null with a warning.

The PostgreSQL schema contains `leads`, `lead_rejections`,
`interaction_events`, `processed_files`, and `pipeline_metrics`. Leads store
protected phone/name hashes and a masked phone. Events are append-only and are
deduplicated by `event_id`. Reports are derived from the database rather than
maintained as mutable status tables.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| PostgreSQL through Docker Compose | Reproducible relational storage and explicit constraints | SQLite |
| Original JSON schemas remain authoritative | Avoids duplicated and conflicting configuration | Runtime consolidated field CSV |
| HMAC phone lookup plus masked output | Supports matching without storing plaintext phone numbers | Plaintext phone storage, rejected by the PII requirement |
| First valid duplicate fixes `batch_id` | Required by the assignment merge rules | Moving a lead to the latest batch |
| Latest non-null duplicate field wins | Preserves corrections without erasing values with nulls | Rejecting duplicate rows |
| Status is derived from dispositions | Keeps the event ledger append-only and auditable | Manually stored lead statuses |
| Host port 55432 | Port 5432 was already occupied locally | Stopping the existing PostgreSQL service |

## Failure modes considered

Handled:

- Blank rows, renamed/reordered headers, aliases, invalid phones, dates,
  decimals, enums, and missing required fields.
- Duplicate leads across batches.
- Malformed JSONL lines and duplicate event IDs.
- Unknown dispositions and orphan events.
- UTC, offset, and naive-IST timestamps.
- Supervisor overrides without a phone, linked through `call_id`.
- PII protection at rest.
- Repeated inserts using database uniqueness constraints.
- Out-of-order event arrival when all data is processed together.

Known limitations:

- A finished CLI for `ingest` and `report` still needs to be added.
- True drop-by-drop incremental processing is not fully wired through
  `processed_files`; current ingestion re-reads inputs and relies on unique
  database constraints for safe duplicates.
- Automatic orphan relinking after a later lead arrives needs to be integrated
  into the ingest command.
- The full pytest suite has not completed because the current environment
  fails inside pytest collection with an internal `NodeId` error.

## Testing

Verified:

- Phone, date, decimal, enum, default, rejection, and warning behavior.
- Generated-data lead ingestion: 2,057 rows, 108 rejected, 57 duplicates
  merged, and 122 field warnings.
- Generated-data event ingestion: 8,745 lines, 8 malformed, 237 duplicates,
  19 unknown dispositions, and 461 orphans.
- PostgreSQL schema initialization and insertion of protected leads,
  rejections, linked events, and orphan events.
- Generation of all five report files and persisted quality counters.

Not yet verified:

- A clean-clone run using the final CLI.
- Full pytest execution after repairing the local pytest environment.
- A second complete ingest producing byte-for-byte identical reports.

## Results

The small generated dataset produced the five required report files. The
pipeline produced 1,892 clean merged leads, 108 rejected rows, 8,020 linked
events, and 461 quarantined orphan events. Reports include masked phones,
derived lead buckets, effective dispositions, call connectivity, and quality
metrics.

## If I had another day

1. Add the CLI and make clean-clone setup/run/test commands exact.
2. Repair the pytest environment and run the complete automated suite.
3. Integrate `processed_files` and orphan relinking into incremental ingestion.
4. Add a deterministic end-to-end idempotency test for drop 1, drop 2, and
   repeated runs.
5. Run the large performance dataset and record timings.
6. Move the final code to a private repository and grant required reviewer
   access before submission.
