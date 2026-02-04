# SOP Update: Raw Source Data Persistence

**Date:** 2026-02-04  
**Updated By:** Antigravity  
**Change Type:** Mandatory Requirement Addition

---

## Summary

Updated both **OSINT SOP** and **Analyst SOP** to require saving all web search results as structured JSON files in the `raw_data/` directory.

## Changes Made

### 1. OSINT SOP (`directives/osint_sop.md`)

**Section:** Report Management → Report Contents

**Added:**
- Updated `raw_data/` description to include "raw web search results as JSON"
- New subsection: **Raw Source Data Requirements**
  - Mandatory JSON file creation for ALL web searches
  - Naming convention: `web_search_<topic>_YYYYMMDD_HHMMSS.json`
  - Required JSON schema fields:
    - `search_query` - Exact query string
    - `search_timestamp` - ISO 8601 timestamp
    - `search_method` - Tool/API used
    - `results_summary` - Brief findings summary
    - `key_findings` - Structured extracted data
    - `sources` - Array of source URLs
  - Purpose: Elasticsearch persistence, reproducibility, audit trail
  - Compliance: Required for all OSINT investigations

### 2. Analyst SOP (`directives/analyst_sop.md`)

**Section:** Analytical Workflow → Phase 1: Intake & Cataloging

**Added:**
- New `raw_data/` directory at top level of workspace structure
- Example files:
  - `web_search_<topic>_YYYYMMDD_HHMMSS.json`
  - `api_response_<source>_YYYYMMDD_HHMMSS.json`
- Added standard files to workspace structure:
  - `sources.txt` - Complete source list
  - `summary.md` - Executive summary
  - `timeline.md` - Chronological reconstruction
- **Mandatory note:** `raw_data/` directory must contain JSON files for all web searches

---

## Rationale

### Problem
Previously, web search results were only documented in narrative reports (`summary.md`, `sources.txt`). Raw search data was not preserved in machine-readable format, making it:
- Impossible to ingest into Elasticsearch
- Difficult to reproduce investigations
- Hard to audit search methodology
- Unable to perform meta-analysis across investigations

### Solution
By requiring JSON files for all web searches:
1. **Data Persistence:** Can be ingested into Elasticsearch via `ingest_elastic.py`
2. **Reproducibility:** Exact queries and timestamps documented
3. **Audit Trail:** Complete record of what was searched and when
4. **Meta-Analysis:** Can analyze search patterns across investigations
5. **Quality Control:** Structured data enables automated validation

---

## Implementation Example

**Berlin Cable Attack Investigation (20260204_153000_berlin_attack_socmint):**

Created 5 JSON files in `raw_data/`:
1. `web_search_events_20260204_151731.json` (5.2 KB)
2. `web_search_social_media_20260204_151900.json` (3.6 KB)
3. `web_search_timeline_20260204_152000.json` (3.3 KB)
4. `web_search_teltow_canal_20260204_151830.json` (2.4 KB)
5. `web_search_local_venues_20260204_152100.json` (3.0 KB)

Each file contains:
- Search query used
- Timestamp (ISO 8601)
- Search method (Google Vertex AI Web Search)
- Results summary
- Structured findings (events, photos, timeline data)
- Source URLs

**Result:** All search data is now:
- ✅ Machine-readable
- ✅ Ready for Elasticsearch ingestion
- ✅ Reproducible
- ✅ Auditable
- ✅ Compliant with persistence SOP

---

## Compliance

**Effective Immediately:** All future OSINT investigations MUST include:
1. `raw_data/` directory in report structure
2. JSON files for every web search performed
3. Proper naming convention and schema adherence

**Retroactive:** Existing reports do not need to be updated, but this is now standard practice going forward.

---

## Related Documents

- `directives/osint_sop.md` - Updated report structure requirements
- `directives/analyst_sop.md` - Updated workspace structure
- `directives/persistence_sop.md` - Data ingestion requirements
- `execution/ingest_elastic.py` - Elasticsearch ingestion script

---

**Status:** ✅ IMPLEMENTED  
**Version:** 1.0  
**Next Review:** 2026-03-04
