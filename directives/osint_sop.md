# OSINT SOP

This file tracks technical challenges, anti-scraping measures, and OSINT tradecraft findings.

## Report Management

**Directory Structure:**
- All OSINT reports must be saved into separate folders under `reports/`
- Folder naming convention: `reports/YYYYMMDD_HHMMSS_<target_identifier>/`
  - Example: `reports/20260203_143022_company_xyz/`
  - Example: `reports/20260203_150815_person_johndoe/`

**Report Contents:**
Each report folder should contain:
- `summary.md` - Executive summary and key findings
- `raw_data/` - Unprocessed scraped data, screenshots, archives, **raw web search results as JSON**
- `processed/` - Cleaned datasets, analysis outputs
- `sources.txt` - Complete list of URLs and sources consulted
- `timeline.md` - Chronological event reconstruction (if applicable)
- `metadata.json` - Collection metadata (timestamps, tools used, operators)

**Raw Source Data Requirements:**
- **ALL web searches must be saved as JSON files** in `raw_data/` directory
- Naming convention: `web_search_<topic>_YYYYMMDD_HHMMSS.json`
  - Example: `web_search_events_20260204_151731.json`
  - Example: `web_search_social_media_20260204_151900.json`
- Each JSON file must contain:
  - `search_query` - Exact query string used
  - `search_timestamp` - ISO 8601 timestamp (YYYY-MM-DDTHH:MM:SS+TZ)
  - `search_method` - Tool/API used (e.g., "Google Vertex AI Web Search")
  - `results_summary` - Brief summary of findings
  - `key_findings` - Structured data extracted from results
  - `sources` - Array of source URLs
  - Additional structured data relevant to the search
- **Purpose:** Enables data persistence to Elasticsearch, reproducibility, and audit trail
- **Compliance:** Required for all OSINT investigations per persistence SOP


## Encoding

**Character Set Handling:**
- Always check for Cyrillic and other non-UTF-8 encodings (windows-1251, ISO-8859-5, KOI8-R)
- Test with `chardet` library before processing
- Common encodings by region:
  - Russian/Ukrainian: windows-1251, KOI8-R
  - Western European: ISO-8859-1, windows-1252
  - Asian: Shift-JIS, GB2312, Big5
- Save all raw content in original encoding + UTF-8 conversion

## Anti-Scraping Countermeasures

**User-Agent Rotation:**
- Maintain pool of realistic desktop/mobile user agents
- Rotate per request or per session based on target sensitivity
- Match user-agent to accept headers and TLS fingerprint

**Rate Limiting:**
- Implement exponential backoff (1s, 2s, 4s, 8s...)
- Randomize delays between requests (jitter)
- Respect robots.txt where appropriate (or note violations)
- Monitor for 429/503 responses

**Session Management:**
- Rotate IP addresses for sensitive targets (proxy pools, VPN, Tor)
- Clear cookies between collection runs
- Use separate browser profiles for different personas

**WAF Detection & Bypass:**
- Common WAFs: Cloudflare, Akamai, Imperva, AWS WAF
- Indicators: challenge pages, 403 with specific headers, JavaScript challenges
- Bypass techniques:
  - Render JavaScript with Playwright/Selenium
  - Solve challenges with undetected-chromedriver
  - Use residential proxies for high-value targets
  - API endpoints often have weaker protection than web UI

**Tracking Findings:**
- Document each new WAF behavior in this section
- Note successful bypass techniques with timestamps
- Mark techniques that stop working (defenders adapt)

## Google Dorking

**Advanced Search Operators:**
- `site:` - Limit to specific domain or TLD
  - `site:gov` or `site:.gov` for government sites
  - `site:example.com -www` exclude www subdomain
- `inurl:` - Search within URL
  - `inurl:admin` or `inurl:login`
- `intitle:` / `allintitle:` - Search page titles
  - `intitle:"index of"` for open directories
- `intext:` / `allintext:` - Search body text only
- `filetype:` - Specific file types
  - `filetype:pdf`, `filetype:xlsx`, `filetype:sql`
- `cache:` - View Google's cached version
- `related:` - Similar websites
- `link:` - Pages linking to URL (deprecated but sometimes works)
- `-` - Exclude terms
- `""` - Exact phrase match
- `*` - Wildcard
- `OR` / `|` - Boolean OR
- `..` - Number range (e.g., `2020..2023`)

**Effective Combinations:**
- Exposed documents: `site:target.com filetype:pdf confidential`
- Open directories: `intitle:"index of" site:target.com`
- Login pages: `site:target.com inurl:admin OR inurl:login`
- Leaked credentials: `site:pastebin.com "target.com" password`
- Subdomains: `site:*.target.com -www`
- Email patterns: `site:target.com "@target.com" filetype:txt`
- Tech stack: `site:target.com "powered by" OR "built with"`
- Error messages: `site:target.com intext:"sql syntax" OR intext:"warning"`

**Other Search Engines:**
- **Bing**: Better for office documents, use `ip:` operator
- **Yandex**: Superior for Russian/Cyrillic content, facial recognition
- **DuckDuckGo**: Aggregates results, `!bangs` for specific sites
- **Shodan**: IoT/server search, use filters like `org:"Company"` or `product:"Apache"`
- **Censys**: Certificate transparency, TLS/SSL intelligence
- **Archive.org**: Wayback Machine for historical data

## Source Verification

**Primary vs Secondary Sources:**
- Prioritize: Official registries, government databases, corporate filings
- Corroborate: Cross-reference minimum 3 independent sources
- Document: Chain of custody for all critical findings

**Red Flags:**
- Single-source claims
- Circular reporting (A cites B, B cites A)
- Anonymous sources without verification
- Inconsistent dates/details across sources
- Recent domain registration for "news" sites

## Metadata Analysis

**File Metadata:**
- Extract EXIF from images (GPS, camera, timestamps)
- Office doc metadata (author, organization, edit history)
- PDF metadata (creator software, modification dates)
- Use `exiftool` for comprehensive extraction

**Web Metadata:**
- WHOIS records (historical via SecurityTrails, DomainTools)
- DNS records (subdomains, mail servers, historical changes)
- SSL certificates (Subject Alternative Names for subdomain discovery)
- IP geolocation and ASN information

## Social Media Intelligence (SOCMINT)

**Platform-Specific Techniques:**
- **LinkedIn**: Employee enumeration, org charts, technology stack
- **Twitter/X**: Geolocation via tweets, network mapping, sentiment analysis
- **Facebook**: Graph search (if still available), event OSINT
- **Instagram**: Location tags, image reverse search
- **GitHub**: Code leaks, employee accounts, technology discovery
- **Reddit**: Sentiment, community intelligence, historical posts (Pushshift)

**Tools:**
- Sherlock: Username enumeration across platforms
- Social-Analyzer: Profile discovery and analysis
- Twint: Twitter scraping (check if still functional)
- InstaLoader: Instagram archival

## Operational Security (OPSEC)

**Attribution Avoidance:**
- Use VPN/Tor for sensitive collections
- Separate browser profiles with consistent fingerprints
- Never use personal accounts for reconnaissance
- Disable WebRTC, canvas fingerprinting
- Consider time zone correlation (search during target's business hours)

**Legal Considerations:**
- Document legal basis for collection (public data, consent, etc.)
- Respect GDPR, CCPA, local privacy laws
- No unauthorized access (CFAA violations)
- Avoid social engineering without authorization
- Maintain audit trail for compliance

## Archival & Chain of Custody

**Immediate Capture:**
- Screenshot critical findings (full page + URL visible)
- Archive pages (archive.is, archive.org)
- Save raw HTML source
- Hash files for integrity (SHA-256)

**Timestamping:**
- RFC 3161 timestamps for legal contexts
- NTP-synced system clocks
- Screenshot with visible timestamp

## Fitness App Intelligence (Strava, etc.)

**Strava API Capabilities:**
- `GET /segments/explore` - Search segments by geographic bounding box [south_lat, west_lng, north_lat, east_lng]
- `GET /segments/{id}` - Segment details including start/end coordinates, athlete count, effort count
- Leaderboard endpoint (`/segments/{id}/leaderboard`) returns 403 without premium/partner API access

**OSINT Value:**
- Segments near POIs can identify athletes who frequent specific locations
- "Local Legend" feature shows most active recent users on each segment
- Polyline data (Google encoded format) enables precise route mapping
- Segment effort counts indicate foot/bike traffic patterns

**Limitations (as of 2026-02):**
- Leaderboard/effort history requires Strava Metro or partner API tier
- Date-filtered queries need enhanced access
- Rate limit: 200 requests/15 min, 2000/day

**Best Practices:**
- Define tight bounding boxes around POI (0.01° ≈ 1km)
- Search both `riding` and `running` activity types
- Cross-reference segment names for location hints (e.g., "Kraftwerksblick" = power plant view)
- Archive polyline data for overlay mapping

**Tool:** `execution/strava_geo.py`

---

## Sports Tracker

**API Base:** `https://api.sports-tracker.com/apiserver/v1/`

**Authentication:**
- Header: `STTAuthorization` with session token
- Login endpoint: `POST /login` with `l=email&p=password`
- Token persists until password change

**Endpoints (authenticated only):**
- `GET /workouts` - User's workouts list
- `GET /workouts/{key}` - Workout details with GPS
- `GET /workouts/{key}/exportGpx` - GPX export

**Limitations (as of 2026-02):**
- No public API - all endpoints require auth
- No geographic search for other users' workouts
- No heatmap/explore API exposed
- Rate limits undocumented

**Tool:** `execution/sports_tracker_geo.py`

---

## OpenStreetMap GPS Traces

**API:** `https://api.openstreetmap.org/api/0.6/trackpoints`

**Parameters:**
- `bbox=west,south,east,north` (lon,lat,lon,lat format)
- `page=N` for pagination

**Data Available:**
- Anonymous trackpoints (lat/lon only, no timestamps)
- Public named traces via web interface
- GPX downloads for specific traces

**OSINT Value:**
- Shows paths people actually take (vs theoretical routes)
- Useful for understanding terrain and access routes
- Limited for temporal analysis due to anonymization

**Tool:** `execution/osm_gps_traces.py`

---

## OpenStreetMap POI/Infrastructure Analysis

**API:** Overpass API
- Primary: `https://overpass-api.de/api/interpreter`
- Alternate: `https://overpass.kumi.systems/api/interpreter`

**Query Capabilities:**
- Power infrastructure (cables, substations, plants, transformers)
- Water features (canals, rivers, bridges)
- Access routes (highways, paths, footways)
- Buildings and cover (industrial, vegetation, parking)
- Surveillance and security (cameras, barriers, lighting)

**OSINT Value:**
- Infrastructure vulnerability assessment
- Access/egress route mapping
- Security posture analysis (cameras, lighting gaps)
- Barrier and fence identification
- Named feature discovery with coordinates

**Bounding Box Format:** `south,west,north,east` (lat,lon,lat,lon)

**Rate Limits:** Overpass may timeout on large queries. Use smaller bounding boxes or simpler queries.

**Tool:** `execution/osm_poi_analysis.py`

---

## Continuous Learning

**Update This SOP When:**
- New WAF behavior discovered
- Successful bypass technique validated
- Search operator combination proves effective
- Tool becomes deprecated or non-functional
- Legal/regulatory landscape changes

**Version Control:**
- Document changes with date and reason
- Preserve historical techniques even if deprecated
- Tag major revisions

---

*Last Updated: 2026-02-03*
*Version: 2.3 - Added OSM POI/Infrastructure Analysis section*