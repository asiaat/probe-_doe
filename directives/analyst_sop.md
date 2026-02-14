# OSINT Analytics SOP

This directive guides the analytical process for OSINT investigations. Analytics transform raw collected data into actionable intelligence through systematic organization, pattern recognition, and evidence-based reasoning.

## Mission

Convert disparate OSINT data (JSON, MD, TXT, PDF, images) into structured intelligence products that answer specific research questions with high confidence and clear sourcing.

## Analytical Workflow

### Phase 1: Intake & Cataloging (15% of time)

**Upon receiving a new dataset:**

1. **Create workspace structure:**
```
   reports/YYYYMMDD_HHMMSS_<target>/
   ├── raw_data/            # Raw web search results (JSON), API responses
   │   ├── web_search_<topic>_YYYYMMDD_HHMMSS.json
   │   └── api_response_<source>_YYYYMMDD_HHMMSS.json
   ├── 00_raw/              # Untouched original files
   │   ├── json/
   │   ├── documents/
   │   ├── images/
   │   ├── text/
   │   └── web_archives/
   ├── 01_processed/        # Cleaned, normalized data
   │   ├── entities.json    # People, orgs, locations
   │   ├── timeline.json    # Chronological events
   │   ├── network.json     # Relationships graph
   │   └── keywords.json    # Term frequency data
   ├── 02_analysis/         # Working analysis files
   │   ├── hypotheses.md    # Current theories
   │   ├── contradictions.md # Conflicting data
   │   ├── gaps.md          # Missing information
   │   └── notes/           # Analyst notes by date
   ├── 03_visualizations/   # Charts, graphs, maps
   ├── 04_deliverables/     # Final reports
   ├── metadata.json        # Collection metadata
   ├── sources.txt          # Complete source list
   ├── summary.md           # Executive summary
   └── timeline.md          # Chronological reconstruction
```

**Note:** The `raw_data/` directory is **mandatory** and must contain JSON files for all web searches performed during the investigation. See OSINT SOP for detailed JSON schema requirements.

**Note:** Third-party documents are stored at the **project level** in `/thirdparty/` (not per-report) and organized by trust level (trusted, unverified, disinformation, leaked). See `/thirdparty/README.md` for handling guidelines.


2. **Generate manifest:**
   - Create `00_raw/manifest.txt` listing all files with:
     - Filename
     - File type
     - Size
     - SHA-256 hash
     - Source URL (if applicable)
     - Collection timestamp
   
3. **Initial triage:**
   - Quick scan: What types of data do we have?
   - Quality assessment: Complete, partial, corrupted?
   - Relevance scoring: High/Medium/Low priority
   - Note gaps immediately in `02_analysis/gaps.md`

### Phase 2: Data Normalization (20% of time)

**Goal: Make data machine-readable and consistently structured.**

**Text Processing:**
- Extract text from PDFs using `pdfplumber` or `PyPDF2`
- OCR images with `tesseract` (preserve original + OCR output)
- Normalize encodings to UTF-8
- Remove boilerplate (headers, footers, navigation)
- Save as `01_processed/<filename>_clean.txt`

**Structured Data:**
- Validate JSON schemas
- Flatten nested structures if needed
- Standardize date formats (ISO 8601: `YYYY-MM-DDTHH:MM:SSZ`)
- Normalize names (consistent capitalization, remove titles)
- Geocode locations to coordinates where possible
- Save as `01_processed/<source>_normalized.json`

**Image Processing:**
- Extract EXIF metadata → `01_processed/image_metadata.json`
- Perform reverse image search (document results)
- OCR any text in images
- Facial recognition tags (if applicable and legal)
- Create thumbnail index for quick reference

**Entity Extraction:**
Create `01_processed/entities.json`:
```json
{
  "people": [
    {
      "name": "John Doe",
      "aliases": ["J. Doe", "Johnny"],
      "mentions": 15,
      "first_seen": "2024-01-15",
      "sources": ["doc1.pdf", "web_archive_5.html"],
      "attributes": {
        "title": "CEO",
        "organization": "Example Corp",
        "email": "john@example.com"
      }
    }
  ],
  "organizations": [...],
  "locations": [...],
  "phones": [...],
  "emails": [...],
  "urls": [...]
}
```


### Phase 2.5: Third-Party Document Handling (10% of time)

**Goal: Properly categorize, evaluate, and handle external documents by trust level.**

**CRITICAL:** All third-party documents must be organized in the **project-level `/thirdparty/` directory** (not per-report) by trust level. Refer to `/thirdparty/README.md` for detailed criteria.

**Trust Level Categories:**

1. **`/thirdparty/trusted/`** - Official government documents, verified law enforcement reports, authenticated court records, peer-reviewed publications
   - **Handling:** Can be cited directly with standard source attribution
   - **Verification:** Standard fact-checking required
   - **Examples:** BKA press releases, Generalbundesanwalt statements, academic papers

2. **`/thirdparty/unverified/`** - Credible sources but not independently verified, media reports, industry publications
   - **Handling:** Must be cross-referenced with other sources. Use "according to [source]" language
   - **Verification:** Require 2+ independent sources for critical claims
   - **Examples:** News articles (Tagesspiegel, Spiegel), technical manuals, Wikipedia articles

3. **`/thirdparty/disinformation/`** - Known or suspected disinformation, propaganda materials, manipulated documents
   - **⚠️ CRITICAL WARNINGS:**
     - **NEVER cite as factual evidence**
     - **ALWAYS label as disinformation in reports**
     - **Use ONLY for attribution analysis** (who is pushing what narrative)
   - **Acceptable Use:** Mapping information warfare campaigns, identifying coordinated narratives
   - **Examples:** Rybar/WarGonzo content, fake statements, coordinated inauthentic behavior

4. **`/thirdparty/leaked/`** - Leaked documents, whistleblower materials, data breaches
   - **Handling:** Verify authenticity through multiple sources. Document chain of custody
   - **Legal/Ethical:** Consider implications before use
   - **Verification:** Flag as "leaked/unverified" until authenticated by experts
   - **Examples:** Vulkan Files, internal communications, classified documents

5. **`/thirdparty/ai_generated/`** - Reports, summaries, or analyses generated by AI systems
   - **⚠️ CRITICAL WARNINGS:**
     - **AI can hallucinate facts** - Verify all claims independently
     - **AI has knowledge cutoffs** - May not reflect recent events
     - **Never cite as authoritative** - Use "AI analysis suggests..." language
   - **Handling:** MUST be verified against primary sources. Use ONLY as research starting points
   - **Examples:** ChatGPT/Claude analysis, Perplexity reports, AI-assisted OSINT tools

**Document Processing Workflow:**

1. **Intake:**
   - Receive third-party document
   - Assess initial trust level
   - Place in appropriate subdirectory

2. **Metadata Creation:**
   - Create `.meta.json` file with:
     ```json
     {
       "filename": "document_name.pdf",
       "source": "Organization/Individual",
       "acquisition_date": "YYYY-MM-DD",
       "trust_level": "trusted|unverified|disinformation|leaked|ai_generated",
       "verification_status": "verified|pending|unverifiable",
       "related_investigations": ["20260213_142123_berlin_grid_attack_eg_volt"],
       "analyst_notes": "Brief assessment",
       "cross_references": ["related_doc1.pdf"]
     }
     ```

3. **Verification:**
   - **Trusted:** Standard fact-checking
   - **Unverified:** Cross-reference with 2+ independent sources
   - **Disinformation:** Do NOT verify claims; analyze narrative only
   - **Leaked:** Authenticate with domain experts, verify chain of custody

4. **Citation Standards:**
   - **Trusted:** "According to BKA press release..."
   - **Unverified:** "Media reports suggest..." or "According to unverified sources..."
   - **Disinformation:** "Russian disinformation channels claimed..." (never cite as fact)
   - **Leaked:** "Leaked documents suggest..." (with authenticity caveat)
   - **AI Generated:** "AI analysis suggests..." (always verify with primary sources)

**Red Flags for Disinformation:**

- Coordinated timing across multiple sources
- Emotionally charged language designed to provoke
- Lack of verifiable details or sources
- Contradicts multiple trusted sources
- Amplified by known disinformation networks (Rybar, WarGonzo, RT)
- Uses "false flag" or "crisis actor" narratives

**When in Doubt:**
- Default to `unverified/` rather than `trusted/`
- Document your reasoning in `.meta.json`
- Flag for senior analyst review
- Do NOT use in ACH matrix until verified

### Phase 3: Pattern Recognition (30% of time)


**Temporal Analysis:**

1. **Build timeline** (`01_processed/timeline.json`):
   - Extract all dates from documents
   - Plot events chronologically
   - Identify:
     - Clusters (lots of activity in short period)
     - Gaps (suspicious silence)
     - Sequences (A always precedes B)
     - Contradictions (different dates for same event)

2. **Time-series analysis:**
   - Frequency: How often does entity X appear over time?
   - Correlation: Do events co-occur?
   - Anomalies: Spikes or drops that need explanation

**Network Analysis:**

1. **Map relationships** (`01_processed/network.json`):
```json
   {
     "nodes": [
       {"id": "person_1", "type": "person", "label": "John Doe"},
       {"id": "org_1", "type": "organization", "label": "Example Corp"}
     ],
     "edges": [
       {
         "source": "person_1",
         "target": "org_1",
         "type": "employed_by",
         "weight": 10,
         "first_seen": "2024-01-15",
         "sources": ["linkedin_profile.json"]
       }
     ]
   }
```

2. **Identify key nodes:**
   - High degree (connected to many others)
   - Betweenness centrality (bridge between groups)
   - Community detection (clusters/factions)

3. **Visualize:**
   - Use Gephi, Graphviz, or Neo4j for complex networks
   - Export to `03_visualizations/network_graph.png`

**Keyword & Text Analysis:**

1. **Term frequency:**
   - TF-IDF analysis to find distinctive terms
   - N-gram extraction (common phrases)
   - Save to `01_processed/keywords.json`

2. **Semantic clustering:**
   - Group documents by topic (LDA, word2vec)
   - Identify themes and sub-topics

3. **Sentiment analysis:**
   - Track positive/negative/neutral mentions
   - Changes in sentiment over time

**Geospatial Analysis:**

1. **Map all locations:**
   - Extract from text, EXIF, IP addresses
   - Plot on map (Google Earth, QGIS, Folium)
   
2. **Identify patterns:**
   - Movement patterns (person traveling)
   - Clustering (areas of interest)
   - Proximity (co-location of entities)

3. **Export:**
   - KML/KMZ for Google Earth
   - GeoJSON for web maps
   - Static maps to `03_visualizations/`

### Phase 4: Hypothesis Development (20% of time)

**Structured Analytic Techniques:**

1. **Analysis of Competing Hypotheses (ACH):**
   
   Create `02_analysis/hypotheses.md`:
```markdown
   # Hypothesis 1: [Clear statement]
   
   ## Evidence Supporting:
   - [Evidence 1] (Source: doc1.pdf, page 5) [Confidence: High]
   - [Evidence 2] (Source: screenshot_3.png) [Confidence: Medium]
   
   ## Evidence Contradicting:
   - [Evidence 3] (Source: web_archive.html) [Confidence: High]
   
   ## Overall Assessment: [Likely/Possible/Unlikely]
   ## Confidence Level: [High/Medium/Low]
   ## Key Assumptions:
   - [Assumption 1]
   
   ## What would prove this hypothesis?
   ## What would disprove this hypothesis?
```

2. **Devil's Advocate:**
   - Force yourself to argue against your leading hypothesis
   - Document in `02_analysis/contradictions.md`
   - Prevents confirmation bias

3. **Key Assumptions Check:**
   - List all assumptions explicitly
   - Rate confidence in each assumption
   - If key assumption is weak, hypothesis is weak

**Confidence Levels:**

Use standardized estimative language:
- **Almost Certain:** 95%+ confidence
- **Highly Likely:** 80-95%
- **Likely:** 60-80%
- **Even Chance:** 40-60%
- **Unlikely:** 20-40%
- **Highly Unlikely:** 5-20%
- **Remote:** <5%

Always tie confidence to:
- Source credibility
- Information consistency
- Number of independent sources
- Logical coherence

### Phase 5: Gap Analysis (10% of time)

**Systematic gap identification:**

1. **Create gap matrix** in `02_analysis/gaps.md`:
```markdown
   # Information Gaps
   
   ## Critical Gaps (blocks conclusion):
   - [ ] Date of transaction (need: exact date, have: month only)
   - [ ] Identity confirmation (need: photo ID, have: name only)
   
   ## Important Gaps (weakens confidence):
   - [ ] Secondary source for claim X
   - [ ] Geographic location on date Y
   
   ## Nice-to-Have (strengthens case):
   - [ ] Additional background on person Z
   
   ## Collection Recommendations:
   1. Search corporate registry for transaction date
   2. Request photo from public social media
```

2. **Prioritize collection:**
   - What gaps, if filled, would most increase confidence?
   - What gaps are feasibly fillable?
   - What gaps are dead ends?

3. **Document "known unknowns":**
   - Be explicit about what you don't know
   - Include in final report limitations section

### Phase 6: Quality Control (5% of time)

**Before finalizing analysis:**

1. **Source verification checklist:**
   - [ ] All claims have sources cited
   - [ ] Sources are primary where possible
   - [ ] Contradictory sources noted
   - [ ] Source credibility assessed
   - [ ] Chain of custody documented

2. **Logic review:**
   - [ ] Conclusions follow from evidence
   - [ ] No logical fallacies
   - [ ] Alternative explanations considered
   - [ ] Confidence levels justified

3. **Peer review (if available):**
   - Have colleague challenge your reasoning
   - Fresh eyes catch blind spots

4. **Red team:**
   - How would adversary attack this analysis?
   - What's the weakest link?

## Data Organization Best Practices

### File Naming Conventions

**Consistent structure:**
```
YYYYMMDD_<source>_<type>_<description>.<ext>

Examples:
20260203_linkedin_profile_johndoe.json
20260203_twitter_archive_companyxyz.zip
20260203_screenshot_facebook_evidence01.png
```

**Version control for analysis:**
```
hypotheses_v1_20260203.md
hypotheses_v2_20260204.md (after new evidence)
```

### Metadata Management

**Every file should have accompanying metadata:**

Create `metadata.json` in each subfolder:
```json
{
  "file": "document.pdf",
  "collected_by": "analyst_name",
  "collection_date": "2026-02-03T14:30:00Z",
  "source_url": "https://example.com/doc.pdf",
  "hash_sha256": "abc123...",
  "relevance": "high",
  "processed": true,
  "notes": "Contains financial records for Q3 2024"
}
```

### Tagging System

**Use consistent tags for cross-referencing:**

In markdown files:
```markdown
#person/johndoe #organization/examplecorp #2024 #financial
```

In JSON:
```json
{
  "tags": ["person_johndoe", "org_examplecorp", "2024", "financial"],
  "priority": "high"
}
```

**Tag categories:**
- Entity types: `#person/`, `#org/`, `#location/`
- Time periods: `#2024`, `#Q3_2024`
- Topics: `#financial`, `#legal`, `#technical`
- Priority: `#critical`, `#important`, `#reference`
- Status: `#verified`, `#unverified`, `#contradicted`

## Statistical Analysis

### Descriptive Statistics

**Always calculate when you have numerical data:**

1. **Central tendency:**
   - Mean, median, mode
   - Understand typical values

2. **Dispersion:**
   - Range, standard deviation
   - Understand variability

3. **Distribution:**
   - Histogram/frequency distribution
   - Identify outliers

**Example:** If analyzing social media posting times:
- Mean: 14:32 (average post time)
- Median: 15:00 (middle value)
- Mode: 16:00 (most common)
- Std dev: 2.3 hours
- Outlier: 03:00 (unusual, worth investigating)

### Correlation vs Causation

**Critical distinction:**

- **Correlation:** Two things happen together
- **Causation:** One thing causes another

**Document in analysis:**
```markdown
## Observed Correlation
- Variable A (social media posts) increases when Variable B (stock price) drops
- Correlation coefficient: -0.75 (strong negative correlation)

## Causal Assessment
- Does A cause B? [Unlikely - timing doesn't support]
- Does B cause A? [Possible - person posts when stressed about stock]
- Does C cause both? [Likely - external market events affect both]
- Confidence: [Medium - need more data]
```

### Link Analysis Metrics

**When analyzing networks:**

1. **Degree centrality:** Number of direct connections
   - High degree = well-connected hub
   
2. **Betweenness centrality:** Bridges between groups
   - High betweenness = gatekeeper/broker
   
3. **Closeness centrality:** Average distance to all others
   - High closeness = can reach others quickly
   
4. **Clustering coefficient:** How connected are neighbors?
   - High clustering = tight-knit group

**Save metrics to** `01_processed/network_metrics.json`

## Keyword Analysis Techniques

### Frequency Analysis

**Term frequency across corpus:**

1. **Single terms:**
```python
   # Top 20 terms by frequency
   "meeting": 156
   "contract": 89
   "payment": 67
```

2. **TF-IDF (Term Frequency-Inverse Document Frequency):**
   - Finds terms distinctive to specific documents
   - Filters out common words
   - Highlights unique vocabulary

3. **N-grams (phrases):**
   - Bigrams: "annual report", "board meeting"
   - Trigrams: "chief executive officer", "pursuant to section"
   - Context > individual words

### Co-occurrence Analysis

**Which terms appear together?**
```json
{
  "person_johndoe": {
    "co_occurs_with": {
      "org_examplecorp": 23,
      "location_newyork": 15,
      "term_ceo": 12
    }
  }
}
```

**Interpretation:**
- Strong co-occurrence suggests relationship
- Changes over time indicate relationship evolution

### Sentiment Tracking

**Track sentiment mentions of entities:**
```markdown
| Entity | Positive | Neutral | Negative | Trend |
|--------|----------|---------|----------|-------|
| John Doe | 45 | 30 | 25 | ↓ (more negative lately) |
| Example Corp | 60 | 35 | 5 | ↑ (improving) |
```

**Tools:**
- VADER (social media sentiment)
- TextBlob (general purpose)
- Manual coding (most accurate for critical analysis)

## Connecting the Dots

### Link Discovery Process

**Systematic approach to finding connections:**

1. **Entity co-occurrence:**
   - Who/what appears in same documents?
   - Frequency of co-occurrence
   - Context of co-occurrence

2. **Temporal proximity:**
   - Events happening close in time
   - Possible causal relationships
   - Coordination indicators

3. **Geospatial proximity:**
   - Same location, same time = potential contact
   - Repeated co-location = relationship
   - Movement patterns that intersect

4. **Transitive relationships:**
   - A knows B, B knows C, therefore A might know C
   - Document indirect links
   - Confidence degrades with distance

### Pattern Templates

**Common patterns to look for:**

**Pattern 1: Preparation → Action → Concealment**
```
Timeline:
- Jan 15: Entity acquires capability (e.g., registers domain)
- Feb 03: Entity takes action (e.g., launches campaign)
- Feb 04: Entity covers tracks (e.g., deletes social media)
```

**Pattern 2: Hub & Spoke**
```
Network:
- Central entity connects to many others
- Peripheral entities don't connect to each other
- Indicates command structure or information broker
```

**Pattern 3: Anomaly Detection**
```
Baseline: Person posts 5 times/day, 9AM-5PM, mostly work topics
Anomaly: Person posts 20 times/day, 2AM-4AM, personal/emotional
→ Investigate what changed
```

### Cross-Reference Matrix

**Systematically check all combinations:**

Create spreadsheet in `02_analysis/cross_reference.xlsx`:

|          | Person A | Person B | Org X | Location Y |
|----------|----------|----------|-------|------------|
| Person A | -        | 5 docs   | 12 docs| 3 times   |
| Person B | 5 docs   | -        | 0     | 1 time    |
| Org X    | 12 docs  | 0        | -     | HQ there  |
| Location Y| 3 times | 1 time   | HQ    | -         |

**Cells show:** How many documents/times entities appear together

**Analysis:**
- Person A strongly linked to Org X (12 documents)
- Person B weakly linked to overall network
- Location Y relevant primarily through Org X

## Analytical Biases to Avoid

### Common Cognitive Traps

1. **Confirmation Bias:**
   - Seeking evidence that supports your theory
   - **Counter:** Actively search for disconfirming evidence
   - **Counter:** Use ACH to force consideration of alternatives

2. **Anchoring:**
   - Over-relying on first piece of information
   - **Counter:** Deliberately ignore early info and re-analyze

3. **Availability Heuristic:**
   - Recent/dramatic events seem more likely
   - **Counter:** Use base rates and statistics

4. **Groupthink:**
   - Team converges on single explanation
   - **Counter:** Assign devil's advocate role

5. **Mirror Imaging:**
   - Assuming others think like you
   - **Counter:** Study target's cultural context

### Bias Mitigation Checklist

Before finalizing analysis, ask:
- [ ] Have I sought disconfirming evidence?
- [ ] Have I considered alternative explanations?
- [ ] Am I making unwarranted assumptions?
- [ ] Is my conclusion proportional to the evidence?
- [ ] Have I let personal views color analysis?
- [ ] Would I reach same conclusion if politically reversed?

## Visualization Best Practices

### Timeline Visualization

**Tools:**
- Horizontal bar charts (Gantt-style)
- Vertical timeline with annotations
- Interactive: TimelineJS

**Include:**
- Confidence indicators (solid vs dashed lines)
- Source annotations
- Gaps in knowledge (clearly marked)

**Export to:** `03_visualizations/timeline.png` or `.html`

### Network Graphs

**Best practices:**
- Color code by entity type
- Size nodes by importance/centrality
- Thickness of edges = strength of relationship
- Label only key nodes (avoid clutter)
- Include legend

**Tools:**
- Gephi (powerful, steep learning curve)
- Cytoscape (biological networks, works for social)
- NetworkX + matplotlib (Python)
- Neo4j (graph database with visualization)

### Geospatial Maps

**Layers to include:**
- Base map (satellite, street, terrain)
- Entity locations (different icons per type)
- Movement tracks (arrows, time-sequenced)
- Heat maps (concentration of activity)

**Tools:**
- Google Earth Pro (free, powerful)
- QGIS (open source GIS)
- Folium (Python, web maps)

### Statistical Charts

**When to use what:**
- **Bar chart:** Comparing categories
- **Line chart:** Trends over time
- **Scatter plot:** Relationship between two variables
- **Heat map:** Intensity across 2D space/time
- **Histogram:** Distribution of values

**Always include:**
- Clear labels
- Axis units
- Data sources
- Date created

## Documentation Standards

### Analyst Notes

**Daily log in** `02_analysis/notes/YYYYMMDD.md`:
```markdown
# Analysis Log - 2026-02-03

## Time: 09:00-12:00
**Focused on:** Entity extraction from PDF corpus
**Findings:** 
- Identified 47 unique persons
- 12 organizations
- 8 locations mentioned >3 times

**Questions raised:**
- Why is Location X mentioned so frequently in Jan but not Feb?
- Person Y has two different titles in different documents - which is current?

**Next steps:**
- Cross-reference person mentions with LinkedIn
- Map location mentions over time

## Time: 13:00-17:00
**Focused on:** Timeline construction
**Findings:**
- Clear sequence: Event A → Event B → Event C
- Gap between Event B and C (5 months silence)

**Hypothesis developing:**
- Gap may indicate operational pause or document destruction

**Need to verify:**
- Check if other sources cover the gap period
```

### Source Citation Format

**In all analysis documents:**
```markdown
## Finding: Person X is CEO of Company Y

**Source 1:** LinkedIn profile (archived 2026-02-03)
- URL: https://linkedin.com/in/personx
- Retrieved: 2026-02-03 14:30 UTC
- File: `00_raw/web_archives/linkedin_personx.html`
- Confidence: High (official profile, verified badge)

**Source 2:** Company press release (2024-01-15)
- URL: https://example.com/press/new-ceo
- Retrieved: 2026-02-03 15:00 UTC
- File: `00_raw/documents/press_release_20240115.pdf`
- Confidence: High (primary source)

**Assessment:** Highly confident - two independent primary sources confirm
```

### Contradiction Tracking

**In** `02_analysis/contradictions.md`:
```markdown
# Contradictions & Discrepancies

## Contradiction 1: Date of Incorporation

**Source A says:** Company incorporated Jan 15, 2024
- Source: State registry screenshot
- File: `00_raw/images/registry_screenshot.png`
- Confidence: High

**Source B says:** Company incorporated Jan 20, 2024
- Source: Company website "About" page
- File: `00_raw/web_archives/about_page.html`
- Confidence: Medium (secondary source)

**Resolution attempt:**
- Checked state registry again (paid search) - confirmed Jan 15
- Website may have typo or referring to different event

**Conclusion:** Jan 15 is correct (primary source, higher confidence)
**Action:** Note website discrepancy in report as caveat
```

## Deliverable Formats

### Executive Summary (1-2 pages)

**Structure:**
```markdown
# Executive Summary

## Research Question
[Original question/task]

## Key Findings
1. [Most important finding] (Confidence: High)
2. [Second most important] (Confidence: Medium)
3. [Third...] (Confidence: High)

## Implications
[What do these findings mean?]

## Recommendations
[What should be done with this intelligence?]

## Limitations
[What don't we know? What could affect confidence?]
```

### Full Analytical Report

**Structure in** `04_deliverables/full_report.md`:
```markdown
# Full OSINT Analytical Report

## Metadata
- **Report ID:** OSINT-20260203-001
- **Classification:** [As appropriate]
- **Date:** 2026-02-03
- **Analyst:** [Name]
- **Review:** [Name, if applicable]

## Table of Contents
[Auto-generated]

## 1. Executive Summary
[As above]

## 2. Research Question & Scope
[What were we trying to answer? What was in/out of scope?]

## 3. Methodology
[How did we collect data? What sources? What tools?]

## 4. Data Summary
[What types of data analyzed? Volume? Quality?]

## 5. Findings
### 5.1 Finding 1
[Detailed explanation with evidence]

### 5.2 Finding 2
[...]

## 6. Analysis
### 6.1 Timeline Analysis
[With visualizations]

### 6.2 Network Analysis
[With graphs]

### 6.3 Pattern Recognition
[Identified patterns]

## 7. Alternative Hypotheses
[ACH results - what we considered and why we ruled in/out]

## 8. Confidence Assessment
[Overall confidence, broken down by finding]

## 9. Gaps & Limitations
[What we don't know, what could change our assessment]

## 10. Recommendations
[Collection, action, further analysis]

## 11. Appendices
- Appendix A: Full source list
- Appendix B: Detailed timeline
- Appendix C: Network graph (full resolution)
- Appendix D: Statistical analysis details
- Appendix E: Glossary
```

### Intelligence Brief (1 page max)

**For quick dissemination:**
```markdown
# Intelligence Brief: [Topic]

**Date:** 2026-02-03
**Classification:** [As appropriate]

## Bottom Line Up Front (BLUF)
[One paragraph: most critical info]

## What We Know
- [Bullet 1] (Confidence: High)
- [Bullet 2] (Confidence: Medium)

## What We Don't Know
- [Bullet 1]
- [Bullet 2]

## So What?
[Why does this matter? What's the impact?]

## What's Next?
[Recommended actions or further collection]

## Sources
[Brief list - full sourcing in separate doc if needed]
```

### Visual Intelligence Product

**Single-page infographic:**
- Central finding in large text
- Key statistics highlighted
- Timeline visualization
- Network graph snippet
- Map (if relevant)
- Clear sourcing footer

**Export as:** `04_deliverables/visual_brief.png` (high resolution)

## Quality Assurance Checklist

**Before delivering any product:**

### Content Quality
- [ ] All claims sourced
- [ ] Confidence levels assigned
- [ ] Alternative explanations considered
- [ ] Limitations acknowledged
- [ ] No speculation without labeling
- [ ] Contradictions noted and resolved (or flagged)

### Technical Quality
- [ ] All files organized per SOP structure
- [ ] File naming conventions followed
- [ ] Metadata complete
- [ ] Hashes computed for critical files
- [ ] Visualizations exported in multiple formats
- [ ] Code/scripts documented

### Analytical Rigor
- [ ] Logical reasoning sound
- [ ] No cognitive biases unchecked
- [ ] Statistics used appropriately
- [ ] Patterns validated (not cherry-picked)
- [ ] Timeline complete and sourced
- [ ] Network analysis includes metrics

### Professional Standards
- [ ] Grammar and spelling checked
- [ ] Formatting consistent
- [ ] Classification markings correct (if applicable)
- [ ] No PII exposed inappropriately
- [ ] Legal/ethical boundaries respected
- [ ] Peer review completed (if available)

## Tools & Scripts

**Recommended toolkit** (add to `execution/analytics_tools/`):

- `entity_extractor.py` - NLP entity extraction
- `timeline_builder.py` - Auto-generate timeline JSON
- `network_analyzer.py` - Compute centrality metrics
- `keyword_analyzer.py` - TF-IDF and n-gram extraction
- `image_processor.py` - EXIF extraction and OCR
- `report_generator.py` - Template-based report creation

**Each script should:**
- Log to `logs/exec_YYYYMMDD.log`
- Accept config file for parameters
- Output to standardized JSON schemas
- Include error handling
- Be documented with docstrings

## Continuous Improvement

### After Action Review

**Post-project, document in** `02_analysis/lessons_learned.md`:
```markdown
# Lessons Learned - Project [Name]

## What Went Well
- [Success 1]
- [Success 2]

## What Could Improve
- [Challenge 1] - **Fix:** [How to prevent next time]
- [Challenge 2] - **Fix:** [...]

## New Techniques Discovered
- [Technique X] worked well for [use case]
  - Document in appropriate SOP section

## Tools Evaluation
- **Tool A:** [Effective/Ineffective because...]
- **Tool B:** [Keep/Deprecate]

## Time Analysis
- Planned: X hours
- Actual: Y hours
- Variance: [Reasons]
- Better estimate next time: [Adjustment]

## SOP Updates Needed
- [ ] Add section on [topic] to Analytics SOP
- [ ] Update OSINT SOP with [technique]
- [ ] Create new tool for [recurring task]
```

### Metric Tracking

**Track analytical performance over time:**

Create `analytics_metrics.json`:
```json
{
  "projects_completed": 23,
  "avg_turnaround_hours": 18,
  "accuracy_rate": 0.94,
  "sources_per_finding_avg": 2.8,
  "hypothesis_validation_rate": 0.76,
  "tools_most_used": ["entity_extractor", "timeline_builder"],
  "common_gaps": ["geolocation", "identity_confirmation"],
  "improvement_areas": ["faster entity extraction", "better visualization"]
}
```

**Review quarterly** - identify trends, optimize workflow

---

## Summary

Analytics transforms raw OSINT into intelligence through:
1. **Systematic organization** - Structured folders, consistent naming
2. **Data normalization** - Machine-readable formats
3. **Pattern recognition** - Temporal, network, textual analysis
4. **Hypothesis testing** - ACH, confidence levels, alternative explanations
5. **Gap identification** - Known unknowns, collection priorities
6. **Bias mitigation** - Devil's advocate, peer review
7. **Clear documentation** - Sourcing, reasoning, limitations
8. **Quality deliverables** - Executive summaries, full reports, visual products

**Remember:**
- Analysis is iterative - findings lead to new questions
- Confidence degrades with each inference - be explicit
- Sources matter - primary > secondary > tertiary
- Visualize to communicate - words + graphics > words alone
- Document everything - your future self will thank you

---

*Last Updated: 2026-02-03*
*Version: 1.0*