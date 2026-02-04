# Elasticsearch Persistence SOP

**Version:** 1.0
**Date:** 2026-02-04

## Overview
All raw data collected during OSINT investigations must be persisted to the Elasticsearch cluster for long-term retention, cross-reference, and analysis. This directive defines the standard operating procedure for data ingestion.

## Configuration
The ingestion pipeline relies on the following `.env` variables:
- `ES_HOST`: URL of the Elasticsearch instance (e.g., `http://localhost:9200`)
- `ES_USER`: Username (optional, leave empty if disabled)
- `ES_PASS`: Password (optional, leave empty if disabled)
- `ES_INDEX_PREFIX`: Prefix for indices (e.g., `doe_baa_`)

## Index Naming Convention
Indices are created dynamically based on the report generation time to ensure easy lifecycle management.

**Format:** `{PREFIX}{YYYYMMDD}_{HHMMSS}`
**Example:** `doe_baa_20260204_180200`

## Data Schema
All documents ingested must conform to the following schema structure. The ingestion script (`ingest_elastic.py`) is responsible for normalizing raw API responses into this format.

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | date | Time of ingestion (ISO 8601) |
| `source_file` | keyword | Path to the source JSON file |
| `source_type` | keyword | `news`, `social`, `geo`, `execution` |
| `data_type` | keyword | `article`, `post`, `segment`, `log` |
| `title` | text | Title or summary of the item |
| `body` | text | Main content/description |
| `url` | keyword | Original URL of the item |
| `raw_source` | object | The complete original JSON object |
| `report_id` | keyword | Identifier of the parent report directory |

## Ingestion Routine
1.  **Collection:** Raw data is saved to `reports/<timestamp>_<name>/raw_data/*.json`.
2.  **Execution:** The `execution/ingest_elastic.py` script is run (manually or via cron).
3.  **Processing:**
    - Script scans `reports/` recursively.
    - Identifies `raw_data` directories.
    - Parses JSON files.
    - Generates a document ID (SHA256 hash of URL + Content) to prevent duplicates.
    - Indexes valid documents.
4.  **Verification:** Success/Failure counts are logged to `logs/ingest_YYYYMMDD.log`.

## Maintenance
- **Retention:** Default retention is undefined (permanent).
- **Mapping:** Dynamic mapping is currently used. Explicit mapping should be applied if type conflicts occur.
