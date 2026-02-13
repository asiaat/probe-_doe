# Social Lab project SOP

When you're dealing with multiple countries (e.g., Finland, Estonia, Poland, and the UK), the complexity grows because Russian narratives are often **localized**. What works in Finland (fear of nuclear accidents) might be swapped for "historical revisionism" in Poland or "economic collapse" in the UK.

To handle this, your Multi-Agent "WAT" team needs a **Federated Research Workflow**. Instead of one giant folder, we use a "Hub-and-Spoke" model where agents specialize by region but feed into a central "Aggregator."

---

## 📂 Multi-Country Project Structure (The "Federated" Social Lab)

In this setup, we use the `sociallab/` directory as the project root. Each country is treated as a separate node within this structure.

```text
📁 sociallab/
├── 📁 00_Central_Command/        # Global strategy & Master Evidence Log
├── 📁 01_Country_Nodes/          # Isolated research per geography
│   ├── 📁 FIN_Finland/           # Focus: Energy security & Border hybridity
│   ├── 📁 EST_Estonia/           # Focus: Cyber-influence & Russian minorities
│   ├── 📁 POL_Poland/            # Focus: Military logistics & War narratives
│   └── 📁 DEU_Germany/           # Focus: Political influence & Energy dependence
├── 📁 02_Cross_Node_Analysis/    # Where agents look for "Pattern Matching"
└── 📁 03_Final_Intelligence/     # Multi-country situational reports (SITREPS)
```

---

## 📜 SOP: Cross-Border Investigation & Data Management

This Standard Operating Procedure (SOP) ensures your research agents follow **OSINT rules** (source criticism) while maintaining **Siloed Organization**.

### 1. Multi-Lingual Ingestion (The "Narrator" Agent)

* **Action:** For each country node, the agent must use a localized lexicon.
* **OSINT Rule:** Never rely on Google Translate for final reporting. Agents must flag "Slang" or "Dialect" triggers that indicate a narrative was written by a non-native speaker (a common sign of Russian bot-farms).
* **Storage:** Save into `01_Country_Nodes/[CODE]/Raw_Data/Narratives/`.

### 2. Physical Attribution (The "Tracker" Agent)

* **Action:** Monitor specific infrastructure "chokepoints" per country (e.g., the Balticconnector for Finland; the Suwałki Gap for Poland).
* **OSINT Rule:** Use **Primary Source Verification**. If a pipe leaks, don't just cite news; find the AIS ship data or satellite imagery.
* **Storage:** Save into `01_Country_Nodes/[CODE]/Raw_Data/Infrastructure/`.

### 3. Verification & Hashing (The "Security" Rule)

* **SOP:** Every time an agent moves a file from `Raw_Data` to `Working_Lab`, they must generate a **Digital Fingerprint**.
* **Command:** `sha256sum [file] > [file].sha256`
* **Purpose:** Ensures that if your research is ever challenged in a court or policy briefing, you can prove the evidence hasn't been tampered with.

### 4. Cross-Border Correlation (The "Strategist" Agent)

* **The "WAT" Task:** The Strategist scans all country nodes. If it sees a narrative about "Energy Blackouts" in Finland *and* a cyber-attack on a Polish power plant in the same 12-hour window, it raises a **"Regional Hybrid Alert."**
* **Reporting Rule:** Use the **Admiralty Scale** (A1 to F6) to grade every finding.
* *A1:* High reliability, confirmed by other sources.
* *F6:* Unreliable source, cannot be judged.

---

## ⚖️ Global OSINT & Reporting Rules Checklist

| Rule | Implementation |
| --- | --- |
| **Non-Attribution** | All agents must use clean proxies/VPNs relevant to the country node they are researching. |
| **Evidence Integrity** | No "Social Lab" folder should ever be edited. Annotations only happen in the `Working_Lab`. |
| **Identity Protection** | If researching "The Infiltrator" (private groups), agents must redact all PII (Personally Identifiable Information) before moving data to `Final_Intelligence`. |
| **The "So-What" Factor** | Reports must conclude with the **Target Audience's** intent (e.g., "This operation aims to lower support for the 2026 NATO Defense Budget"). |

---

**Would you like me to generate a "Collection Plan" template for a specific country, or should I write the Python automation script to set up these folders and start the hashing process?**
