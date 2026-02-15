# SOP: MASTER INVESTIGATIVE DOSSIER SYNTHESIS (SOP-ID: 2026-MID-V1)

## TABLE OF CONTENTS
1. [0. MISSION OBJECTIVE](#0-mission-objective)
2. [1. SCOPE & ELIGIBILITY](#1-scope--eligibility)
3. [2. CORE STRUCTURAL REQUIREMENTS](#2-core-structural-requirements)
4. [3. SOURCE MATERIAL HIERARCHY](#3-source-material-hierarchy)
5. [4. SYNTHESIS METHODOLOGY (RECURSIVE LOOP)](#4-synthesis-methodology-recursive-loop)
6. [5. ANALYTICAL FORMULAS (QUANTITATIVE RIGOR)](#5-analytical-formulas-quantitative-rigor)
7. [6. FORMATTING & MARKUP STANDARDS](#6-formatting--markup-standards)

---

## 0. MISSION OBJECTIVE
To provide a standardized, high-fidelity methodology for synthesizing large-scale, multi-layered OSINT investigations into a single, cohesive "Master Dossier." This SOP ensures that disparate intelligence "dots" are connected across strategic, operational, and tactical layers.

## 1. SCOPE & ELIGIBILITY
A "Master Investigative Dossier" is required when an investigation spans:
- More than 10 individual analytic reports.
- Multiple jurisdictions (e.g., DE, RU, UK, CY).
- Combined Technical, Financial, and Political layers.

---

## 2. CORE STRUCTURAL REQUIREMENTS (THE 25-35 PAGE STANDARD)
A Master Dossier must achieve a cumulative depth of **25-35 pages equivalent** and include the following mandatory sections:

### 2.0 Table of Contents & Pagination
- **Requirement:** Every Master Dossier **MUST** begin with a comprehensive **Table of Contents (ToC)** mapping all 25+ sections to their respective page numbers.
- **Pagination:** Final PDF deliverables must have visible **page numbers** (e.g., "Page X of Y") in the footer to facilitate legal referencing and physical archival.

### 2.1 Strategic Intent (Executive Synthesis)
- **Goal:** Identify the primary objective of the adversary operation.
- **Requirement:** Must connect the "Command" (State/Board) to the "Frontline" (Kinetic Action).

### 2.2 Fused Operational Timeline (Chronological Mapping)
- **Methodology:** Reconcile dates/times from all sub-reports into a single, unified chain of events.
- **Visual:** A tabular or Mermaid-based sequence of events (2025-2026).

### 2.3 Lead Actor Repository (Dossier Mode)
**Standard for Person of Interest (POI) entries:**
1. **Original Script:** Always provide original names in native script (e.g., **Cyrillic** for Russian actors, **Chinese** for PRC actors) alongside transliterations.
2. **Visual Evidence:** Every primary suspect **MUST** have a face photo (OSINT screenshot) or verified avatar.
3. **Role Status:** Categorize POIs as *Strategic, Operational Fixing, Logistical Hub,* or *Frontline Proxy*.

### 2.4 Entity & Hub Network (GEOINT)
- **Requirement:** Identify all "Logistical Hubs" (HQ addresses, staging areas).
- **Visual Standards:** 
  - Include high-resolution **Google Maps/Earth** satellite imagery for tactical site overviews.
  - Utilize **OpenStreetMap (OSM)** for infrastructure POI analysis (e.g., power line tracing, substation names).
  - Annotate maps with **Points of Interest (POIs)**, access/egress routes, and surveillance blind spots.
- **Co-location Analysis:** Highlight hubs where multiple suspect entities share physical space (e.g., Großbeerenstraße).

## 3. SOURCE MATERIAL HIERARCHY
To maintain analytical speed without sacrificing depth, the following hierarchy MUST be observed:
- **3.1 Primary Synthesis Layer (Markdown):** Initial analysis should be derived from the `summary.md` and `full_report.md` files of the 40+ sub-investigations. These contain the refined human/AI analysis.
- **3.2 Supplementary Layer (JSON/Raw):** The `raw_data/*.json` and `processed/*.json` files are used strictly for **Verification** and **Data Drilling** (e.g., verifying a specific timestamp or payment ID).
- **3.3 Evidence Layer (Media):** Screenshots and binaries are used to provide final ocular proof.

---

## 4. SYNTHESIS METHODOLOGY (HOW TO CONNECT THE DOTS)
"Connecting the dots" is a recursive process of inductive reasoning.

### 4.1 The "Recursive Loop" Strategy
1. **Audit (Level 1):** Read all Markdown summaries to identify recurring names, companies, and locations.
2. **Matrix (Level 2):** Create a temporary "Dot Matrix" cross-referencing these entities against report IDs.
3. **Deep-Dive (Level 3):** When a "dot" appears in >3 reports, drill into the associated JSON files to find technical commonalities (e.g., same ASN, same registrar, same bank).
4. **Narrative Construction:** Transform the matrix into the "Fused Timeline."

### 4.2 Cognitive Patterns to Seek
- **Temporal Synchronization:** Look for actions happening within the same 48-hour window across different geographical zones.
- **Entity Reuse:** Identify personnel who appear in both logistics and security shells within the same week.
- **Narrative Mirroring:** Detect when the "Disinformation" layer (Amezit) begins spoofing a narrative *before* the kinetic action occurs.

---

## 5. ANALYTICAL FORMULAS (QUANTITATIVE RIGOR)
Analysts MUST utilize formal mathematical modeling to determine culpability and probability.

### 5.1 Culpability Probability ($P_C$)
$$P_C = \frac{\sum (E_i \cdot w_i)}{N \cdot w_{max}}$$

### 5.2 Linguistic Anomaly Index ($LAI$)
- Detect "Back-Transliteration" ghosts (Cyrillic phonemic leaks).
- Compare against known native-speaker transliteration errors.

### 5.3 Hybrid Attack Detection Coefficient ($HADC$)
$$HADC = \frac{T_s + L_m}{G_p + F_a}$$

---

## 6. FORMATTING & MARKUP STANDARDS
All dossiers must be authored in **GitHub Flavored Markdown (GFM)** with the following specific "Markup" conventions:

### 6.1 Table of Contents Markup
- **Auto-Generated List:** Use a flat list with internal anchors `[Section Name](#section-anchor)`.
- **Page Referencing:** Use dot-leaders (`................`) for the visual alignment of page numbers (e.g., `[Title](#link) .... Page X`).

### 6.2 Hierarchy & Pagination
- **Header 1 (#):** Reserved for the Dossier Title only.
- **Header 2 (##):** Reserved for the 25+ Mandatory Sections.
- **Pagination:** Footer must contain `Page X of Y` (Generated via `md_to_pdf_v4.py`).

### 6.3 Media & Source Markup
- **Image Embedding:** Mandatory absolute paths for screenshots: `![Caption](file:///path/to/img.png)`.
- **Source Hyperlinking:** 
  - **Mandatory URLs:** EVERY picture, citation, hypothesis, and strong claim **MUST** be backed by a direct URL or clickable archive link.
  - **Citations:** Every claim must terminate with an inline reference: `(Ref: Report ID | [URL](link))`.
- **Native Script:** Cyrillic/Chinese names must be bolded next to transliteration: `Anatoliy Paliy (**Анатолий Палий**)`.
- **Identity Markers (Bolding):** Key identity markers in suspect dossiers **MUST** be bolded to ensure high-fidelity PDF rendering and quick scannability:
  - **Status:** PRIMARY FINANCIER (e.g., `**Status:** PRIMARY FINANCIER`)
  - **Role:** STRATEGIC HUB (e.g., `**Role:** STRATEGIC HUB`)
  - **Entity:** GEM CAPITAL (e.g., `**Entity:** GEM CAPITAL`)

---

## 7. FILE MANAGEMENT & DELIVERY
To ensure professional version control, all final "Master Dossiers" must be handled as follows:

- **Target Directory:** `/deliverables/` (Root-level folder for final outputs).
- **Naming Convention:** `YYYYMMDD_HHMMSS_<report_title_slug>.md` (and `.pdf`).
  - Example: `20260215_143005_berlin_attack_main_suspects.pdf`
- **Signal:** The timestamp signal at the beginning of the filename is the authoritative version marker.

---

*Last Updated: 15 February 2026*  
*Authorized by: Strategic OSINT Unit (Antigravity)*  
*Compliance Status: MANDATORY FOR ALL MASTER SYNTHESIS OPERATIONS.*
