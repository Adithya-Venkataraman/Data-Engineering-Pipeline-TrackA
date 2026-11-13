# AI Usage Disclosure

## Tools used

- ChatGPT/Codex.
- PowerShell, Docker Compose, PostgreSQL, Python, and pytest were used to run and verify the work.

## What I used AI for

- Clarifying Assignment A and reviewing the architecture.
- Implementing and reviewing preprocessing, lead ingestion, event ingestion, PostgreSQL persistence, and reporting.
- Drafting focused tests and explaining Python, PostgreSQL, Docker, and Git concepts.
- Debugging the Docker/PostgreSQL port and authentication setup.

## What I wrote or decided myself

- I chose Assignment A and PostgreSQL with Docker Compose.
- I kept the workflow JSON schemas as the source of truth rather than introducing a runtime consolidated field CSV.
- I chose separate lead and event tables, an append-only interaction ledger, derived lead buckets, and database-generated reports.
- I reviewed the generated data, ran the pipeline, inspected outputs, and decided which limitations to document.

## Where the AI was wrong or unhelpful

- The initial guidance assumed a public fork could be changed to private. GitHub did not allow that, so a separate private repository is required for final submission.
- The first report implementation inferred data-quality counters from stored rows. That was incorrect for malformed and duplicate events, so a `pipeline_metrics` table was added.
- The first database connection used port 5432, but another local PostgreSQL service already owned that port. The Docker mapping was changed to host port 55432.

## How I verified AI-generated code

- Read the generated code and reviewed the design decisions.
- Ran Python compilation checks and direct normalization/ingestion checks.
- Verified lead, rejection, event, and orphan counts on generated data.
- Initialized PostgreSQL and inserted the generated records.
- Generated all five reports and checked headers, formats, timestamps, and quality counters.
- The installed pytest environment currently fails during collection with an internal `NodeId` error; this must be fixed and the full suite rerun before final submission.
