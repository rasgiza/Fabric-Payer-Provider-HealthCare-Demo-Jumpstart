# Healthcare Payer/Provider Analytics on Microsoft Fabric

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric-blue.svg)](https://learn.microsoft.com/fabric/)
[![Jumpstart: payer-provider-healthcare](https://img.shields.io/badge/Jumpstart-payer--provider--healthcare-7B68EE.svg)](https://github.com/microsoft/fabric-jumpstart)

> **A Microsoft Fabric Jumpstart** &mdash; one-click deployment of a complete **Healthcare Payer/Provider Analytics** solution: Medallion lakehouse → Direct Lake semantic model → Data Agent → Real-Time Intelligence → Knowledge Graph. No Python install, no `.env` files, no manual setup.

```python
# Install via the Fabric Jumpstart catalog (run inside a setup notebook,
# e.g. jumpstart-setup -- not in Healthcare_Launcher)
# First time in a new workspace/session: install the package
%pip install -q fabric-jumpstart

import fabric_jumpstart
fabric_jumpstart.install("payer-provider-healthcare", update_existing=True)
```

> **The story in one line:** *Same Fabric data foundation, two delivery surfaces &mdash; **push** to Microsoft Teams via Data Activator the moment a fraud, readmit-risk, or capacity event fires, and **pull** via the Foundry Orchestrator Agent + Power BI / RTI Dashboard when leaders want to investigate. One governance model, real-time and on-demand.*

> [!NOTE]
> **Release tracks:** this repository is now managed with two tracks for catalog reliability.
> - **Stable track (current catalog ID):** `payer-provider-healthcare` (recommended for broad deployment)
> - **Preview track:** `payer-provider-healthcare-ontology-preview` (ontology + graph model validation path)
>
> The preview track is intentionally kept out of catalog listing until GraphModel lifecycle behavior is consistently stable across tenants.

> [!IMPORTANT]
> - All data in this demo is **100% synthetic**. No real patient information (PHI) is used.
> - Data was generated using a synthetic data generator with realistic distributions but entirely fictional names and records.
> - This is an **educational demo** showcasing Fabric capabilities. Production healthcare solutions require additional security, compliance (HIPAA/HITECH), and governance controls.

---

## 🎬 Recommended Demo Questions

> **For an easy, story-driven demo that showcases both the Fabric Data Agent and the Foundry IQ Knowledge Agent, ask these two questions back-to-back — same patient, same story.**

1. **In the Fabric Data Agent (`HealthcareHLSAgent`):**
   *"Show me medication adherence for Betty Brown age 83 by drug class."*
   → Returns per-class PDC, gap days, and adherence category from the gold lakehouse.

2. **In the Foundry IQ Knowledge Agent (`HLSAgent`):**
   *"Betty Brown was just discharged after a CHF admission with high readmission risk and is non-adherent on multiple chronic medications. What TCM and MTM interventions should her care team take in the next 7 days? Cite the guidelines."*
   → Returns a cited care plan grounded in the clinical knowledge docs at `lh_gold_curated/Files/healthcare_knowledge/`.

**Why this works:** Q1 shows Fabric's structured-data power; Q2 shows Foundry's reasoning + grounded citations. Together they tell the platform story — *data → decision* — in under 60 seconds.

---

## Table of Contents

1. [Why This Demo? — The Payer & Provider Pain Points](#why-this-demo--the-payer--provider-pain-points)
2. [Quick Start](#quick-start)
3. [What Gets Deployed](#what-gets-deployed)
   - [Data Volumes (Default)](#data-volumes-default)
4. [Architecture](#architecture)
5. [Deployment Flow — What "Run All" Does](#deployment-flow--what-run-all-does)
6. [After Deployment](#after-deployment)
   - [Explore the Data](#explore-the-data)
   - [Power BI Dashboard](#power-bi-dashboard)
7. [AI & Agents](#ai--agents)
   - [The Data Agent — HealthcareHLSAgent](#the-data-agent--healthcarehlsagent)
   - [Sample Questions](#sample-questions)
   - [Prep Data for AI (model-owner, one-time)](#prep-data-for-ai-model-owner-one-time)
   - [Ontology Agent (Graph) — Manual UI Setup](#ontology-agent-graph--manual-ui-setup)
   - [Azure AI Foundry Orchestrator (optional)](#azure-ai-foundry-orchestrator-optional)
8. [Real-Time Intelligence (optional)](#real-time-intelligence-optional)
   - [Claims Fraud Detection](#use-case-1-claims-fraud-detection)
   - [Care Gap Closure](#use-case-2-care-gap-closure-at-point-of-care)
   - [High-Cost Member Trajectory](#use-case-3-high-cost-member-trajectory)
   - [RTI Data Tables](#rti-data-tables)
   - [How Streaming Works](#how-streaming-works)
9. [Data Activator / Reflex Alerts](#data-activator--reflex-alerts-manual--15-min)
10. [Run Incremental Loads](#run-incremental-loads)
11. [Configuration Options](#configuration-options)
12. [Prerequisites](#prerequisites)
13. [Repository Structure](#repository-structure)
14. [Troubleshooting](#troubleshooting)
15. [Credits](#credits)

---

## Why This Demo? — The Payer & Provider Pain Points

Healthcare payers and providers face compounding operational challenges that erode revenue, increase regulatory risk, and compromise patient outcomes. This demo addresses **six critical pain points** that cost the U.S. healthcare system billions annually:

### 1. Claim Denials Are Draining Revenue

> **Industry average denial rate: 10-15%** — costing a mid-size health system **$4.2M+ per year** in rework, appeals, and lost revenue.

Payers deny claims for preventable reasons: missing documentation (23%), invalid codes (18%), eligibility issues (14%), and prior authorization gaps. Most organizations lack real-time visibility into *which* claims are at risk *before* submission. This demo builds a **denial risk scoring model** that flags high-risk claims proactively, surfaces root causes by payer, and tracks appeal success rates — turning reactive denial management into a predictive workflow.

### 2. Readmissions Drive CMS Penalties

> **CMS Hospital Readmission Reduction Program (HRRP)** penalizes hospitals **up to 3% of total Medicare reimbursement** — for a $450M system, that's **$13.5M at stake**.

30-day readmissions for CHF, COPD, pneumonia, AMI, and TKA/THA are tracked and penalized. Yet most providers lack integrated risk scoring that combines clinical data with social determinants. This demo computes **readmission risk scores** using encounter history, diagnosis complexity, and SDOH factors (food deserts, housing instability, transportation barriers), enabling targeted discharge planning before patients leave the facility.

### 3. Medication Non-Adherence Sinks Star Ratings

> **CMS Star Ratings** triple-weight medication adherence measures (diabetes, RAS antagonists, statins) — making PDC scores the **single largest driver** of plan quality ratings and bonus payments.

Plans with 4+ stars receive significant CMS bonus payments, but adherence gaps are invisible without pharmacy claims integration. This demo calculates **Proportion of Days Covered (PDC)** per patient per drug class, identifies non-adherent members with chronic conditions, and maps adherence gaps to HEDIS measures — giving care managers actionable intervention lists.

### 4. Social Determinants Are Invisible in Clinical Workflows

> **80% of health outcomes** are driven by factors outside the clinic — yet SDOH data rarely appears alongside clinical data.

Zip-code-level poverty rates, food desert flags, transportation scores, housing instability rates, and social vulnerability indices exist in public datasets but aren't integrated into analytics platforms. This demo joins **SDOH data at the zip-code level** to every patient, encounter, and claim — enabling population health stratification, SDOH-informed readmission prevention, and health equity reporting.

### 5. Provider-Payer Contract Complexity Creates Revenue Leakage

> Health systems manage **12+ payer contracts** with different reimbursement rates, PA requirements, timely filing deadlines, and denial behaviors.

Without contract-level analytics, systems can't identify which payers underpay, which deny most frequently, or where network adequacy gaps exist. This demo models **payer-specific analytics** across 12 simulated payers with realistic contract rates, denial patterns, and formulary coverage — revealing collection rate variance and contract negotiation priorities.

### 6. Analytics Teams Can't Stand Up Environments Fast Enough

> Traditional healthcare analytics projects take **weeks to provision** — installing Python, configuring credentials, deploying infrastructure, debugging authentication.

This demo eliminates the entire setup burden. **One notebook, one click, fifteen minutes.** SQL-only analysts, clinical informaticists, and business users can explore a fully functional environment without touching a command line.

### What This Demo Proves

By combining all six dimensions — **claims + readmissions + adherence + SDOH + provider network + quality measures** — in a single Fabric workspace, this demo shows how Microsoft Fabric's unified platform (OneLake, Spark, Direct Lake, Copilot AI) can deliver:

- **Real-time denial risk dashboards** with root cause analysis and appeal tracking
- **Predictive readmission scoring** with SDOH-informed discharge planning
- **HEDIS-aligned medication adherence** monitoring with care gap closure
- **Natural language analytics** via Fabric Data Agent and Azure AI Foundry
- **Ontology-driven knowledge graphs** connecting patients → encounters → claims → providers → payers

All from a single workspace deployed in minutes.

### How this maps to the Microsoft "Healthcare Provider Use Cases" framing

This demo aligns directly with the [Microsoft Healthcare Provider Use Cases](https://microsoft.sharepoint.com/teams/USHealthcareCloudandAIPlatforms/SitePages/Healthcare-Provider-Use-Cases.aspx) playbook. Position it against these solution areas and pain points:

| MS framing | This demo's coverage |
|---|---|
| **Data foundation** (connect data across systems) | ✅ Medallion (Bronze→Silver→Gold) on Fabric/OneLake unifying EHR, claims, ADT, SDOH, pharmacy; ontology + KB sit on top |
| **AI-powered experiences** (summarization, knowledge access, automation) | ✅ Foundry Orchestrator agent with KB grounding + citations; sub-agents for clinical / financial / ops questions |
| **Productivity & collaboration** (care teams, ops, back office) | ✅ Activator → **Teams cards** push the alert into the workflow the user already lives in |
| **Application platform** (targeted solutions, prototypes) | 🟡 Partial — `Healthcare_Launcher.Notebook` is itself a one-click reusable solution accelerator; complement with custom apps as needed |
| **Operations & analytics** (throughput, RCM, service line, exec decision support) | ✅ Sweet spot — Power BI exec dashboard, RTI Dashboard, fraud/SIU, contract analytics |
| **Care team productivity** (workflow automation, knowledge access, secure collab) | ✅ Real-time Teams alerts + grounded agent answers with citations |
| **Front door & access** (intake, contact center, scheduling) | ❌ Not in scope — complementary to Microsoft Cloud for Healthcare patient experience accelerators |
| **Clinical documentation** (scribe, ambient) | ❌ Not in scope — complementary to Dragon / DAX Copilot |

**The business pain answered:** provider operations are blind to events when they happen and slow to ask *why* when they don't. This demo unifies the data once on Fabric, then delivers the same intelligence two ways — pushed into Teams the instant something fires, and pulled into a grounded AI cockpit when leaders want to investigate.

---

## Quick Start

1. **Create an empty Fabric workspace.** **F4** is enough to install and run the
   Jumpstart end-to-end. If you plan to run a **live demo with everything firing at
   once** (streaming ingestion + RTI scoring notebooks + Data Agent + Foundry agent +
   Direct Lake reports), use **F16 or higher** &mdash; concurrent interactive load can
   throttle smaller capacities.
2. **Create a setup notebook** (for example `jumpstart-setup`) in that workspace and run:
   ```python
   %pip install -q fabric-jumpstart

   import fabric_jumpstart
   fabric_jumpstart.install("payer-provider-healthcare", update_existing=True)
   ```
   This deploys all Lakehouses, Notebooks, Pipelines, Semantic Model, Power BI Report, RTI items, and the HLS Data Agent into the workspace.

   > Why `update_existing=True`? It guarantees the deployed notebook definitions replace any stale or shell items from prior attempts.

   If you see `ValueError: Unknown jumpstart 'payer-provider-healthcare'`, the catalog entry may not be registered in your current library build yet. Use this preregistration fallback:
   ```python
   import fabric_jumpstart as jumpstart

   jumpstart._install_from_github(
       logical_id="payer-provider-healthcare",
       repo_url="https://github.com/rasgiza/Fabric-Payer-Provider-HealthCare-Demo-Jumpstart.git",
         repo_ref="main",  # use a branch/tag ref the installer can clone
       workspace_path="payer-provider-healthcare/",
       entry_point="Healthcare_Launcher.Notebook",
       items_in_scope=["Notebook", "Lakehouse", "DataPipeline", "SemanticModel", "Report", "DataAgent", "Eventhouse", "KQLDatabase"],
         update_existing=True,
   )
   ```
3. **Open `Healthcare_Launcher`** (the entry-point notebook deployed by step 2) and click **Run All** &mdash; wait ~15-20 minutes.

### Manual install (without the catalog)

If you don't yet have access to the Jumpstart catalog (`fabric_jumpstart` package), you can deploy this repo directly:

1. **Create an empty Fabric workspace** (**F4** minimum; **F16+** for a full live demo with streaming + agents running in parallel).
2. **Import** `payer-provider-healthcare/00_Start_Here/Healthcare_Launcher.Notebook/notebook-content.py` as a notebook
   *(Workspace &rarr; Import &rarr; Notebook &rarr; upload as `.py` source)*.
3. **Run All** &mdash; wait ~15-20 minutes.

> **That's it &mdash; no configuration needed.** The launcher pulls from the public repo `rasgiza/Fabric-Payer-Provider-HealthCare-Demo-Jumpstart` by default. Edit the CONFIG cell before running to customise &mdash; for example set `DEPLOY_STREAMING = False` to skip the Real-Time Intelligence stack (batch-only demo).
>
> The launcher runs the batch path (data → ETL → semantic model → ontology) and, when `DEPLOY_STREAMING = True` (the default), the full RTI stack (Eventhouse + KQL + Eventstream + scoring + RTI Dashboard).

The launcher generates sample data, runs the ETL pipeline, creates and refreshes the semantic model, deploys the RTI streaming topology and runs the scoring notebooks, then deploys the ontology + graph model — fully automated. RTI can be turned off via `DEPLOY_STREAMING = False`.

## What Gets Deployed

| Layer | Items | Description |
|-------|-------|-------------|
| **Lakehouses (4)** | `lh_bronze_raw`, `lh_silver_stage`, `lh_silver_ods`, `lh_gold_curated` | Medallion architecture storage |
| **Notebooks (7)** | 5 ETL + `NB_Generate_Sample_Data` + `NB_Generate_Incremental_Data` | Spark-based data processing |
| **Pipelines (2)** | `PL_Healthcare_Full_Load`, `PL_Healthcare_Master` | Orchestration with full/incremental modes |
| **Semantic Model** | `HealthcareDemoHLS` | Star schema for Power BI (facts + dimensions), Direct Lake |
| **Data Agent** | `HealthcareHLSAgent` | Copilot AI agent — sources: `HealthcareDemoHLS` semantic model + `lh_gold_curated` gold lakehouse |
| **Ontology** | `Healthcare_Demo_Ontology_HLS` | GraphQL entity model (12 entities, 18 relationships) — auto-deployed by Cell 7 |
| **Graph Agent** | `Healthcare Ontology Agent` | Copilot AI agent — ontology graph traversal (entity lookups, care pathways) — created manually in the UI |
| **Power BI Report** | `HealthcareAnalyticsDashboard` | 6 pages, 60+ visuals — deployed as a native item bound to the semantic model |
| **Eventhouse** ⚡ | `Healthcare_RTI_Eventhouse` | Git-tracked RTI compute engine (`DEPLOY_STREAMING` only) |
| **KQL Database** ⚡ | `Healthcare_RTI_DB` | 7 KQL tables (1 landing + 3 typed input + 3 scored output) + streaming/update policies (`DEPLOY_STREAMING` only) |
| **Eventstream** ⚡ | `Healthcare_RTI_Eventstream` | Custom Endpoint → Eventhouse + Lakehouse + Activator (`DEPLOY_STREAMING` only) |
| **RTI Notebooks (5)** ⚡ | Setup, Event Simulator, 3 Scoring (Fraud, Care Gap, HighCost) | RTI for fraud, care gaps, high-cost trajectory (`DEPLOY_STREAMING` only) |
| **RTI Dashboard** ⚡ | `Healthcare RTI Dashboard` | Real-time KQL dashboard (`DEPLOY_STREAMING` only) |

> ⚡ = Only deployed when `DEPLOY_STREAMING = True`

### Data Volumes (Default)

| Entity | Rows |
|--------|------|
| Patients | 10,000 |
| Providers | 500 |
| Encounters | 100,000 |
| Claims | 100,000 |
| Prescriptions | ~250,000 |
| Diagnoses | ~200,000 |
| SDOH Zip Codes | ~560 |

## Architecture

Dual-path design: **Batch ETL** (authoritative, historical) + **Real-Time Intelligence** (operational, sub-minute). Batch feeds streaming — Gold dimension tables are the enrichment layer for real-time scoring.

### Solution Architecture

![Provider Healthcare Solution with Microsoft Fabric & AI](diagrams/healthcare-architecture.png?v=2)

### 🔬 Interactive 3D Ontology Knowledge Graph

**[▶ Launch Interactive 3D Graph](https://rasgiza.github.io/Fabric-Payer-Provider-HealthCare-Demo/docs/ontology_graph_3d.html)** — Explore the full ontology in a cinematic Three.js visualization with bloom lighting, animated data-flow particles, and hover tooltips showing every property and relationship.

| Entities | Relationships | Domains |
|----------|---------------|---------|
| Patient · Provider · Encounter · Diagnosis · PatientDiagnosis · Medication · Prescription · MedicationAdherence · Claim · Payer · CommunityHealth | livesIn · treatedBy · involves · covers · billsFor · submittedBy · prescribedBy · serves · dispenses · originatesFrom · occursIn · references · affects · adherenceFor · adherenceMedication · ClaimHasPayer · PrescriptionHasPayer | Clinical · Financial · Pharmacy · Diagnostic · SDOH |

> *Drag to rotate · Scroll to zoom · Hover nodes for property details*
>
> To run locally: download [`docs/ontology_graph_3d.html`](docs/ontology_graph_3d.html) and open in any browser.

### 🎬 Interactive 3D Patient Story Demos

**[▶ Launch Nancy White Story](https://rasgiza.github.io/Fabric-Payer-Provider-HealthCare-Demo/demo_3d_story/nancy_white_story.html)** — Age 63, Medicare, CHF. 9 drug classes, 8/9 non-adherent, pharmacy desert. How streaming intelligence catches a $42,000 readmission risk in 48 hours instead of 28 days.

**[▶ Launch Sarah Johnson Story](https://rasgiza.github.io/Fabric-Payer-Provider-HealthCare-Demo/demo_3d_story/sarah_johnson_story.html)** — Age 41, Commercial. 13 providers, opioid + benzo FDA black-box combination, 3 psychiatrists, no PCP. How the ontology surfaces an overdose risk that no dashboard can see.

> Navigate with arrow keys, spacebar, click, or number keys. Press **A** for auto-play.

### Detailed Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     HEALTHCARE ANALYTICS ARCHITECTURE                        │
│                     Batch ETL + Real-Time Intelligence                       │
└──────────────────────────────────────────────────────────────────────────────┘

    BATCH PATH (cold)                        STREAMING PATH (hot)
    Historical, authoritative,               Operational, sub-minute,
    runs daily / on-demand                   runs continuously
    ─────────────────────────                ────────────────────────

    Source Systems / CSV Gen                 Live Events
    NB_Generate_Sample_Data                  ADT feeds, claims clearinghouse,
    NB_Generate_Incremental_Data             pharmacy PBM, EHR HL7
           │                                          │
           ▼                                          ▼
    ┌──────────────┐                         ┌─────────────────────┐
    │ lh_bronze_raw│                         │ Eventstream         │
    │ (CSV files)  │                         │ (Custom Endpoint)   │
    └──────┬───────┘                         └─────────┬───────────┘
           │                                           │
    01_Bronze_Ingest                              streaming ingestion
           │                                           │
           ▼                                           ▼
    ┌──────────────┐                         ┌─────────────────────┐
    │ lh_silver    │                         │ Healthcare_RTI_DB   │
    │ stage → ODS  │                         │ (KQL Database)      │
    │ (cleansed,   │                         │                     │
    │  enriched)   │                         │ rti_all_events      │
    └──────┬───────┘                         │   │ update policies  │
           │                                 │   ▼                  │
    03_Gold_Star_Schema                      │ claims_events        │
           │                                 │ adt_events           │
           ▼                                 │ rx_events            │
    ┌──────────────────────┐                 └─────────┬───────────┘
    │   lh_gold_curated    │◄──reads dims──────────────┤
    │                      │   to enrich events         │
    │ DIMENSIONS (SCD2):   │                            │
    │  dim_patient         │──────────────┐             │
    │  dim_provider        │              │             │
    │  dim_facility        │   reference  │     scoring │
    │  dim_payer           │     data     │             │
    │  dim_diagnosis       │              ▼             ▼
    │  dim_medication      │    ┌──────────────────────────────┐
    │  dim_sdoh            │    │     SCORING NOTEBOOKS         │
    │  care_gaps           │    │   (read KQL via Kusto SDK)    │
    │  hedis_measures      │    │                              │
    │                      │    │  NB_RTI_Fraud_Detection      │
    │ FACTS:               │    │    reads: claims_events      │
    │  fact_encounter      │    │    writes: fraud_scores      │
    │  fact_claim          │    │                              │
    │  fact_prescription   │    │  NB_RTI_Care_Gap_Alerts      │
    │  fact_diagnosis      │    │    reads: adt_events         │
    │                      │    │    joins: care_gaps,         │
    │ AGGREGATES:          │    │           hedis_measures     │
    │  agg_readmission     │    │    writes: care_gap_alerts   │
    │  agg_med_adherence   │    │                              │
    │                      │    │  NB_RTI_HighCost_Trajectory  │
    └──────────┬───────────┘    │    reads: claims + adt events│
               │                │    writes: highcost_alerts   │
               │                └──────────────┬───────────────┘
               │                               │
               ▼                               ▼
    ┌────────────────────┐      ┌──────────────────────────────┐
    │ BATCH CONSUMPTION  │      │ REAL-TIME CONSUMPTION        │
    │                    │      │                              │
    │ Semantic Model     │      │ KQL Real-Time Dashboard      │
    │ (Direct Lake)      │      │  • Fraud risk heatmap        │
    │                    │      │  • Care gap closure live     │
    │ Data Agent         │      │  • High-cost trend ticker    │
    │ (Copilot AI)       │      │                              │
    │                    │      │ Activator (Reflex)           │
    │ Ontology + Graph   │      │  • Teams / Email / PA        │
    │ (Knowledge Graph)  │      │                              │
    │ Power BI Reports   │      │ (Delta copies rti_* land in  │
    │                    │      │  lh_gold_curated for history)│
    └────────────────────┘      └──────────────────────────────┘
```

## Deployment Flow — What "Run All" Does

The launcher notebook (`Healthcare_Launcher.Notebook`) is the post-deployment orchestrator. The Fabric Jumpstart installer creates the workspace items from the repo ahead of time (Lakehouses, Notebooks, Pipelines, Semantic Model, Report, RTI items, Data Agent) and copies the knowledge docs to `lh_gold_curated/Files/healthcare_knowledge/`. This notebook then runs the runtime steps that bring the workspace to life.

| Cell | What It Does |
|------|--------------|
| **Markdown** | Welcome / overview header |
| **CONFIG** | Set `GENERATE_DATA`, `RUN_PIPELINE`, `DEPLOY_STREAMING` |
| **Constants** | Internal source refs (`GITHUB_OWNER` / `REPO` / `BRANCH`) — do not edit |
| **CELL 2** | Run `NB_Generate_Sample_Data` — ~10K patients, 100K encounters, HEDIS measures |
| **CELL 3** | Trigger `PL_Healthcare_Master` with `load_mode=full` — Bronze → Silver → Gold ETL (~8-15 min) |
| **CELL 4** | Create & refresh `HealthcareDemoHLS` semantic model (Direct Lake, TMDL; patches lakehouse IDs). The `HealthcareAnalyticsDashboard` report binds to it by name. |
| **CELL 5** ⚡ | Deploy RTI streaming topology (Eventhouse + KQL DB + Eventstream Custom Endpoint → KQL / Lakehouse / Activator) and the RTI Real-Time Dashboard |
| **CELL 6** ⚡ | Run RTI pipeline: auto-fetch connection string → Setup Eventhouse → Event Simulator → verify → scoring notebooks (Fraud, Care Gap, HighCost) |
| **CELL 7** | Deploy ontology (`Healthcare_Demo_Ontology_HLS`) + auto-provision the Graph Model |

> ⚡ = Only runs when `DEPLOY_STREAMING = True`
>
> **Why does the ontology/graph step run last?** Its graph-build is the longest-running operation; placing it after RTI ensures a long graph build (or a session timeout during it) never blocks the Real-Time Intelligence items.

## After Deployment

### Explore the Data
- Open **lh_gold_curated** → Tables → you'll see star schema tables (`fact_encounter`, `dim_patient`, etc.)
- Open **HealthcareDemoHLS** semantic model → create Power BI reports

### Power BI Dashboard

The **HealthcareAnalyticsDashboard** Power BI report is deployed from the `payer-provider-healthcare/06_AI_and_Graph/HealthcareAnalyticsDashboard.Report/` definition. It includes:

| Page | Focus | Key Visuals |
|------|-------|-------------|
| Executive Summary | KPIs, denial rates, encounter volume | Card KPIs, trend lines, donut charts |
| Claim Denials | Root cause, payer breakdown, financial impact | Waterfall, stacked bar, matrix |
| Readmission Risk | 30-day readmission by facility & diagnosis | Heatmap, scatter, decomposition tree |
| Medication Adherence | PDC rates, non-adherent populations | Gauge, grouped bar, line chart |
| Social Determinants | SDOH risk by zip code, demographics | Map, bar, correlation scatter |
| Provider Performance | Provider metrics, outlier detection | Table, bullet chart, ranking |

The report binds to the `HealthcareDemoHLS` semantic model via Direct Lake (live connection). It starts working as soon as the semantic model refresh completes (Cell 4).

For customization guidance (26 DAX measures, formatting tips, Direct Lake best practices) — see **[POWERBI_DASHBOARD_GUIDE.md](POWERBI_DASHBOARD_GUIDE.md)**.

---

## AI & Agents

This solution ships **one programmatically-deployed Data Agent** plus an optional **Graph (ontology) agent** and an optional **Azure AI Foundry orchestrator**.

### The Data Agent — HealthcareHLSAgent

`HealthcareHLSAgent` is deployed automatically by the launcher — no manual setup required. Its two data sources are:

- the **`HealthcareDemoHLS` semantic model** (DAX over the Direct Lake star schema), and
- the **`lh_gold_curated` gold lakehouse** (SQL over the curated gold tables).

It answers aggregation, rate, and trend questions ("What is the denial rate by payer?", "Top 10 providers by total billed amount", "Which patients have the highest readmission risk?"). Its datasource IDs are auto-remapped to the deployed items, so it works as soon as the semantic model refresh completes.

To test it, open the agent in your workspace and try:
- *"What is the overall denial rate by payer?"*
- *"Show me the top 10 providers by total billed amount"*
- *"Which patients have the highest readmission risk?"*

For the complete agent configuration — AI instructions, concept-to-table routing, SQL rules, few-shot examples, knowledge base, and customization guide — see **[DATA_AGENT_GUIDE.md](DATA_AGENT_GUIDE.md)**.

### Sample Questions

See **[SAMPLE_QUESTIONS.md](SAMPLE_QUESTIONS.md)** for 90+ copy-paste questions organized by domain and agent — including a top **[Executive Pain-Point Questions](SAMPLE_QUESTIONS.md#executive-pain-point-questions-boardroom--c-suite)** section (CFO, CMO, CMIO, COO, VP Pop Health, CIO) framed in real-world boardroom language.

#### Recommended demo warm-up sequence (Graph Ontology Agent)

The Graph Data Agent runs on top of an OpenAI assistant thread + tool-call harness. The first 1–2 calls after the agent is published spin up that thread and load the GQL examples from `aiInstructions` into the LLM's working context. Cold-starting straight into a complex aggregation question can surface as `submit_tool_outputs failed` (BadRequest) or "An error occurred". To get reliable demos, **always warm up with two simple list queries first**:

1. `List 5 providers` — confirms the graph is reachable and primes provider entity.
2. `Show me 5 patients` — primes patient entity and adherence relationship.
3. `Which patients have the most non-adherent drug classes?` — the headline aggregation question (uses `MATCH ... FILTER ... LET ... GROUP BY ... ORDER BY ... LIMIT`).
4. Pick a patient from step 3 and drill in: `Show me adherence details for <First> <Last>, age <N>`.

This gives you both reliability (the assistant thread is warm) and a stronger narrative arc (broad → specific → recommendation).

### Prep Data for AI (model-owner, one-time)

`HealthcareHLSAgent` and its `HealthcareDemoHLS` semantic model follow Microsoft's published guidance: **[Semantic model best practices for data agent](https://learn.microsoft.com/en-us/fabric/data-science/semantic-model-best-practices#prep-for-ai-make-semantic-model-ai-ready)** and **[Prepare your data for AI](https://learn.microsoft.com/en-us/power-bi/create-reports/copilot-prepare-data-ai)**.

- **Keep agent instructions cross-source and high-level.** The DAX generation tool ignores data-agent-level instructions when querying a semantic model, so model-specific guidance belongs in **Prep for AI** (AI instructions, AI data schema, verified answers) on the model itself. The agent prompt is kept thin: response formatting, cross-source routing, and tone only. Start lean and add context iteratively.
- **Make the semantic model AI-ready.** Tables, columns, and measures carry descriptions; model-level Prep-for-AI instructions and verified answers are documented for the model owner to apply in the Power BI UI (they can't be set through the agent API).

The copy-paste-ready instructions, verified answers, and the API-vs-UI deployment map are in **[HLS_AGENT_PREP_FOR_AI.md](HLS_AGENT_PREP_FOR_AI.md)**.

**How the model owner applies Prep for AI (one-time, ~15 min).** Git sync deploys the agent prompt and data-source config automatically, but the model-level **Prep for AI** content can't be set through the API — apply it once by hand. The steps below are the *click-path*; the actual text and DAX to copy live in the matching sections of **[HLS_AGENT_PREP_FOR_AI.md](HLS_AGENT_PREP_FOR_AI.md)**.

1. **AI instructions** — In the Fabric/Power BI **service**, open the `HealthcareDemoHLS`
   semantic model → **Prep data for AI** → **Add AI instructions**. Copy the block from
   **Section 3** of **[HLS_AGENT_PREP_FOR_AI.md](HLS_AGENT_PREP_FOR_AI.md)** and paste it in.
2. *(Optional)* **Simplify the data schema** — same pane → deselect fields Copilot doesn't need
   and add synonyms for terms users actually say.
3. **Verified answers** — these are created in **Power BI Desktop**, not the service. Build a
   visual that shows the answer (use the DAX in **Section 4** of
   **[HLS_AGENT_PREP_FOR_AI.md](HLS_AGENT_PREP_FOR_AI.md)** as the spec), then
   **right-click the visual → "Set up verified answer"** and add the phrasings users will ask.
   After you publish/save, the entries appear back in the service under **Prep data for AI →
   Verified answers**.
4. **Re-test and iterate** — start lean; add an instruction or verified answer whenever you
   spot a wrong answer.

### Ontology Agent (Graph) — Manual UI Setup

The **Healthcare Ontology Agent** (graph agent) traverses the `Healthcare_Demo_Ontology_HLS` graph model for entity lookups and relationships ("Tell me about patient PAT0000001", "Who treated this patient?", "Trace claim CLM0009999 from patient to payer"). It must be created manually in the Fabric UI — it cannot be fully deployed via API.

**Step 1: Create the Agent**

1. In your workspace → **+ New item** → **Data agent**

   ![Data agent card](docs/images/agent-new-item.png)

2. Name it `HealthcareHLSOntology Agent` → click **Create**

   ![Create data agent dialog](docs/images/agent-create-dialog.png)

**Step 2: Add the Data Source**

1. Click **Add Data** → **Data source**

   ![Add Data dropdown](docs/images/agent-add-data-source.png)

2. Browse to your workspace → select the ontology **`Healthcare_Demo_Ontology_HLS`** (Graph model)

   ![Select ontology graph](docs/images/agent-select-ontology.png)

3. The agent will connect to the graph model (12 entities, 18 relationships)

**Step 3: Configure AI Instructions**

1. Click **Agent instructions** (top toolbar)

   ![Agent instructions panel](docs/images/agent-instructions.png)

2. Copy the AI instructions text from **[DATA_AGENT_INSTRUCTIONS.md → Section 2a](DATA_AGENT_INSTRUCTIONS.md#2-healthcare-ontology-agent-graph-agent)** — AI Instructions
3. Paste into the instructions text box → click **Apply**

> These instructions tell the agent about the ontology entities, relationships, and graph traversal patterns for healthcare queries.

**Step 4: Test the Agent**

1. In the agent chat panel, try a sample question:
   - *"Which providers treat patients covered by Aetna?"*
   - *"Show the care pathway for patient P-1001"*
   - *"What prescriptions are linked to claims denied for medical necessity?"*
2. See **[SAMPLE_QUESTIONS.md → Graph Agent](SAMPLE_QUESTIONS.md#graph-agent-healthcare-ontology-agent)** section for more questions

### Azure AI Foundry Orchestrator (optional)

To set up the **Foundry Orchestrator Agent** that combines the Fabric Data Agent with a Knowledge Base (21 clinical documents indexed via Azure AI Search) and web search for hybrid clinical decision support — see **[FOUNDRY_IQ_SETUP_GUIDE.md](FOUNDRY_IQ_SETUP_GUIDE.md)**.

For troubleshooting hybrid query failures (compound questions, instruction truncation, fewshot phrasing issues) — see **[FOUNDRY_ORCHESTRATOR_TROUBLESHOOTING.md](FOUNDRY_ORCHESTRATOR_TROUBLESHOOTING.md)**.

---

## Real-Time Intelligence (optional)

When `DEPLOY_STREAMING=True` (the default), the launcher deploys a full RTI stack: **Eventhouse + KQL Database + Eventstream + Setup + 3 scoring notebooks + RTI Dashboard** that address high-value payer/provider pain points where batch analytics fall short.

**Architecture pattern: hot path / cold path separation.**

| Path | Store | Reader | Latency | Use case |
|------|-------|--------|---------|----------|
| **Hot** | KQL Eventhouse (`Healthcare_RTI_DB`) | KQL Real-Time Dashboard, Kusto SDK in scoring notebooks | sub-second | Live fraud / care-gap / cost alerts |
| **Cold** | Lakehouse (`lh_gold_curated`) | Power BI Direct Lake on `HealthcareDemoHLS` | minutes | Historical analytics, star schema, agents |

The two stores are queried independently — KQL via the native Kusto query endpoint, Lakehouse via Direct Lake. **OneLake Availability on the KQL DB is intentionally NOT enabled** by default because it would add mirroring latency that negates the real-time advantage. It is available as an **optional** step if you want unified lakehouse/Spark access — see **[RTI_STREAMING_GUIDE.md](RTI_STREAMING_GUIDE.md)**.

> **Full RTI walkthrough** — end-to-end flow, KQL table schemas + update policies, setup steps, and troubleshooting: **[RTI_STREAMING_GUIDE.md](RTI_STREAMING_GUIDE.md)** (and the folder README at [`payer-provider-healthcare/05_Real_Time_Intelligence/README.md`](payer-provider-healthcare/05_Real_Time_Intelligence/README.md)).

### Use Case 1: Claims Fraud Detection

> **$68B lost to healthcare fraud annually** (NHCAA). Most SIU teams investigate claims weeks after submission — by then, the money is gone.

**NB_RTI_Fraud_Detection** scores every claim in real-time using rule-based + statistical signals:
- **Velocity burst** — Provider submits many claims within a short window
- **Amount outlier** — Claim exceeds 3σ of provider's historical mean
- **Geographic anomaly** — Patient location far from provider facility
- **Upcoding** — Consistent use of the highest E&M code

Risk tiers: **CRITICAL** → **HIGH** → **MEDIUM** → **LOW**

**Output:** `fraud_scores` (KQL) + `rti_fraud_scores` (Delta in `lh_gold_curated`), with lat/long for map visuals showing fraud hotspots.

### Use Case 2: Care Gap Closure at Point of Care

> **Payers spend $2-4 per member per month** on outreach for HEDIS gaps. The highest-value moment is when the patient is *already in front of a provider* — but the care team doesn't know about open gaps.

**NB_RTI_Care_Gap_Alerts** fires when an ADT (Admit/Discharge/Transfer) event arrives:
1. Joins the encounter with the patient's **open HEDIS measures** (CDC, COL, BCS, SPC, CBP, SPD, OMW, PPC)
2. Checks for open care gaps and ranks by priority (CRITICAL if a high-value gap is long overdue)
3. Generates **human-readable alerts** for the care team at the bedside

**Output:** `care_gap_alerts` (KQL) + `rti_care_gap_alerts` (Delta), with facility lat/long for map visuals showing which facilities have the most gap-closure opportunities.

### Use Case 3: High-Cost Member Trajectory

> **5% of members drive 50% of total healthcare costs.** Early identification of members *trending toward* high-cost status enables care management intervention before catastrophic events.

**NB_RTI_HighCost_Trajectory** computes rolling windows over claims and encounters:
- **30-day and 90-day rolling spend** — flags members exceeding thresholds
- **ED superutilizer detection** — ≥3 emergency visits in 30 days
- **Readmission tracking** — multiple admits within 30 days
- **Cost trend** — ACCELERATING / RISING / STABLE / DECLINING

Risk tiers: **CRITICAL** → **HIGH** → **MEDIUM** → **LOW**

**Output:** `highcost_alerts` (KQL) + `rti_highcost_alerts` (Delta), with lat/long for map visuals showing cost hotspots.

### RTI Data Tables

All RTI tables live in the `Healthcare_RTI_DB` KQL database. Simulator events land in `rti_all_events`, then server-side **update policies** fan them out to the typed tables by the `_table` field.

| KQL table | Role | Description |
|-----------|------|-------------|
| `rti_all_events` | Landing | All simulator events (string-typed timestamp + `_table` routing field) |
| `claims_events` | Typed input | Claim submissions with fraud patterns |
| `adt_events` | Typed input | ADT events (admit/discharge/transfer) |
| `rx_events` | Typed input | Prescription fill events |
| `fraud_scores` | Scored output | Scored claims with risk tiers and fraud flags |
| `care_gap_alerts` | Scored output | Point-of-care gap closure alerts |
| `highcost_alerts` | Scored output | Members on escalating cost trajectory |

> Scoring notebooks also persist a **Delta copy** of each output to `lh_gold_curated` (`rti_fraud_scores`, `rti_care_gap_alerts`, `rti_highcost_alerts`) so real-time and historical views stay reconcilable.

### How Streaming Works

**Eventstream is the front door for all streaming data.** Cell 5 wires the full Eventstream topology via API; Cell 6 drives the run.

```
NB_RTI_Event_Simulator  (reads lh_gold_curated dims/facts)
    │  EventHub protocol
    ▼
Healthcare_RTI_Eventstream (Custom Endpoint)
    │
    ├──► Eventhouse / KQL DB  → rti_all_events ──(update policies)──► claims_events / adt_events / rx_events
    ├──► Lakehouse (lh_bronze_raw)   (optional raw event archive)
    └──► Activator / Reflex          (fraud / care-gap / high-cost alerts)
                                            │
            scoring notebooks read typed tables via Kusto SDK
                                            ▼
                       fraud_scores / care_gap_alerts / highcost_alerts → KQL Real-Time Dashboard
```

**Running it (fully automatic):**

1. **Cell 5** wires the Eventstream topology and deploys the RTI Real-Time Dashboard.
2. **Cell 6** auto-fetches the Custom Endpoint connection string via the Fabric Eventstream REST API (`GET /eventstreams/{id}/sources/{sourceId}/connection`) — no portal copy/paste — then runs Setup → Simulator → verification → all 3 scoring notebooks.
3. Re-run any scoring notebook directly anytime to reprocess the live KQL data.

> **No portal step is required for the core scenario.** Enabling OneLake Availability on `Healthcare_RTI_DB` is **optional** (only if you want to read the KQL tables as Delta from Spark) — see **[RTI_STREAMING_GUIDE.md](RTI_STREAMING_GUIDE.md)**.
>
> **Fallback:** if the connection-string auto-fetch fails (e.g. restricted token), open **Healthcare_RTI_Eventstream** → **HealthcareCustomEndpoint** in the portal, copy the **Connection String**, and paste it into `ES_CONNECTION_STRING` in Cell 6.

---

## Data Activator / Reflex Alerts (Manual — ~15 min)

Data Activator (Reflex) is the **production-grade alerting layer** for this solution. It monitors RTI KQL tables in real time and fires **proactive alerts** via Email, Teams, or Power Automate when scoring thresholds are breached — no code required.

> **Why Activator?** In real-world healthcare operations, compliance and audit teams need **deterministic, rule-based alerts** that fire consistently and can be traced back to exact thresholds. Activator provides this with built-in deduplication, configurable cadence, and direct integration with Teams/Email/Power Automate — making it the operational backbone for:
> - **Fraud SIU teams** receiving immediate referrals when anomaly scores spike
> - **Care coordinators** getting notified of overdue HEDIS gaps when patients are admitted
> - **Case managers** flagged on high-cost member trajectories before costs escalate
> - **IT/ops teams** alerted to pipeline staleness or data quality issues

### Step 1: Create a Reflex Item

1. In your Fabric workspace → **+ New item** → **Reflex**
2. Name it `Healthcare_RTI_Alerts`

### Step 2: Connect to the KQL Database

1. In the Reflex item → **Get data** → **KQL Database**
2. Select `Healthcare_RTI_DB` (in the `Healthcare_RTI_Eventhouse`)
3. You'll add triggers for each of the 3 scoring tables below

### Step 3: Configure Alert Rules

**Rule 1 — Fraud Detection (SIU Referral)**

| Setting | Value |
|---------|-------|
| **Table** | `fraud_scores` |
| **Monitor** | `fraud_score` |
| **Condition** | `fraud_score >= 50` |
| **Action 1** | **Email** → notify SIU team |
| **Action 2** | **Teams** → post to `#fraud-investigations` channel (optional) |
| **Card fields** | claim_id, patient_id, provider_id, fraud_score, fraud_flags, risk_tier |
| **Email Subject** | `🚨 CRITICAL Fraud Alert — SIU Referral: Patient {{patient_id}}` |
| **Email Body** | `Claim {{claim_id}}, Score {{fraud_score}}, Flags: {{fraud_flags}}` |

**Rule 2 — Care Gap Outreach (Overdue HEDIS Gaps)**

| Setting | Value |
|---------|-------|
| **Table** | `care_gap_alerts` |
| **Monitor** | `gap_days_overdue` |
| **Condition** | `gap_days_overdue > 90` |
| **Action 1** | **Email** → notify care coordinator |
| **Action 2** | **Teams** → post to `#care-coordination` channel (optional) |
| **Card fields** | patient_id, measure_name, gap_days_overdue, alert_priority, alert_text |
| **Email Subject** | `⚠️ Care Gap Alert — {{measure_name}}: Patient {{patient_id}}` |
| **Email Body** | `{{gap_days_overdue}} days overdue, Priority: {{alert_priority}}` |

**Rule 3 — High-Cost Member (Care Management Referral)**

| Setting | Value |
|---------|-------|
| **Table** | `highcost_alerts` |
| **Monitor** | `rolling_spend_30d` |
| **Condition** | `rolling_spend_30d > 50000` |
| **Action 1** | **Email** → notify case manager |
| **Action 2** | **Power Automate** → trigger care management workflow (optional) |
| **Card fields** | patient_id, rolling_spend_30d, ed_visits_30d, cost_trend, risk_tier |
| **Email Subject** | `💰 High-Cost Alert — Patient {{patient_id}}: ${{rolling_spend_30d}} in 30d` |
| **Email Body** | `ED Visits: {{ed_visits_30d}}, Trend: {{cost_trend}}, Tier: {{risk_tier}}` |

### Step 4: Verify Alerts Fire

1. Run **NB_RTI_Event_Simulator** in batch mode to generate test events
2. Run the 3 scoring notebooks (Fraud, Care Gap, HighCost)
3. Check your Teams channel / email for alert cards within ~60 seconds

> **Power Automate integration**: For complex routing (create ServiceNow tickets, update EHR systems, page on-call staff), select **Power Automate** as the action and build a flow that reads the alert payload. The Reflex trigger passes all card fields as dynamic content to the flow.

### Real-World Pain Points Solved

| Pain Point | How Activator Solves It |
|------------|------------------------|
| **Fraud goes undetected for days** | Fraud scores ≥ 50 trigger immediate SIU email — MTTD drops from days to minutes |
| **Care gaps missed during admissions** | Patients with overdue HEDIS measures are flagged on admission — care coordinators act while the patient is still in-house |
| **High-cost members escalate silently** | Spending trajectories > $50K/30d trigger proactive case management before costs spiral |
| **Alert fatigue from noisy dashboards** | Activator fires only when thresholds breach — no polling, no dashboards to watch |
| **Compliance audit trail gaps** | Every Activator trigger is logged with timestamp, threshold, and action taken — ready for audit |

## Run Incremental Loads

After the initial full load, you can simulate daily operational data arriving. The pipeline supports a `load_mode` parameter that switches between full rebuild and incremental processing.

### How It Works

The **PL_Healthcare_Master** pipeline accepts a `load_mode` parameter (default `"full"`). When set to `"incremental"`, the pipeline:

1. **Generates new data** — runs `NB_Generate_Incremental_Data` to create today's records
2. **Bronze: APPEND** — new CSVs are appended to existing Bronze tables (not overwritten), then archived to `Files/processed/` to prevent duplicate reads
3. **Silver: Full rebuild** — Silver notebooks always read all Bronze data, clean, deduplicate, and overwrite Silver tables (idempotent)
4. **Gold: MERGE** — Gold uses Delta Lake merge operations:
   - **SCD Type 2 dimensions** (`dim_patient`, `dim_provider`): detects attribute changes (city, state, zip, specialty, department), expires old versions (`is_current=0`), and inserts new versions with new surrogate keys
   - **Type 1 dimensions** (`dim_payer`, `dim_facility`, `dim_diagnosis`, `dim_medication`): overwritten (reference data, no history needed)
   - **Fact tables** (`fact_encounter`, `fact_claim`, `fact_prescription`): Delta MERGE on business key — updates existing rows, inserts new ones

### Data Volumes Per Incremental Run

| Entity | New Rows | Notes |
|--------|----------|-------|
| Encounters | ~50 | All dated today |
| Claims | ~50 | One per encounter |
| Diagnoses | ~100–150 | 1 principal + 0–2 secondary per encounter |
| Prescriptions | ~75–100 | 1–3 per encounter based on diagnosis |
| Patients | ~2 new + 2–3 updates | Updates simulate address/insurance changes |

### Steps

**Option A — Run from the Pipeline UI:**

1. Open **PL_Healthcare_Master** in your Fabric workspace
2. Click **Run** → set parameter `load_mode` = `incremental`
3. Wait ~10–12 minutes for the pipeline to complete

**Option B — Run the notebooks manually:**

1. Open **NB_Generate_Incremental_Data** → Run All
   *(writes timestamped CSVs to `Files/incremental/YYYY-MM-DD/`)*
2. Open **PL_Healthcare_Full_Load** → Run with parameter `load_mode=incremental`
   *(or run Bronze → Silver → Gold notebooks individually)*

### Scheduling

To automate daily incremental loads, add a **Schedule trigger** to `PL_Healthcare_Master`:

1. Open the pipeline → **Schedule** (top toolbar)
2. Set recurrence (e.g., daily at 6:00 AM)
3. Add parameter: `load_mode` = `incremental`

### Verifying Incremental Data

After an incremental run, check that new data flowed through:

```sql
-- Gold layer: count should increase by ~50 per run
SELECT COUNT(*) FROM lh_gold_curated.fact_encounter

-- SCD2: check for expired patient versions
SELECT patient_id, city, is_current, effective_end_date
FROM lh_gold_curated.dim_patient
WHERE is_current = 0
ORDER BY effective_end_date DESC
LIMIT 10
```

Repeat daily to build up a realistic data history showing trends over time in the Power BI dashboard.

## Configuration Options

Edit the **CONFIGURATION** cell at the top of `Healthcare_Launcher.Notebook`:

| Variable | Default | Description |
|----------|---------|-------------|
| `GENERATE_DATA` | `True` | Run `NB_Generate_Sample_Data` to populate `lh_bronze_raw` |
| `RUN_PIPELINE` | `True` | Run the full-load pipeline (`PL_Healthcare_Master`) |
| `DEPLOY_STREAMING` | `True` | Deploy + run Real-Time Intelligence (Eventhouse + KQL + Eventstream + scoring notebooks + RTI Dashboard). Set `False` for a batch-only demo. |
| `ES_CONNECTION_STRING` | `""` | *(Cell 6)* Eventstream Custom Endpoint connection string. Auto-fetched via the Eventstream REST API; leave empty unless you want to override it. |

> The source refs `GITHUB_OWNER` / `GITHUB_REPO` / `GITHUB_BRANCH` live in a separate **INTERNAL CONSTANTS — do not edit** cell and only matter if you deployed from a fork.
>
> **Restricted networks:** The launcher downloads from GitHub at runtime. If your environment blocks `github.com` or `raw.githubusercontent.com`, fork this repo to an allowed internal location and update those constants accordingly.

## Prerequisites

- **Microsoft Fabric** workspace capacity:
  - **F4** &mdash; minimum to install and run the Jumpstart end-to-end.
  - **F16+** &mdash; recommended for live demos that run streaming ingestion, RTI
    scoring notebooks, the Data Agent, and the Foundry agent **simultaneously**.
    Concurrent interactive load on F2/F4 can throttle mid-demo.
- User must be workspace **Admin** or **Member**
- Workspace should be **empty** (the launcher checks for this)
- Internet access to download from GitHub

## Repository Structure

```
fabric-payer-provider-healthcare-demo-jumpstart/
├── README.md
├── LICENSE
├── CONTRIBUTING.md
├── SECURITY.md
├── deployment.yaml                          # Optional: CI/CD config
│
├── payer-provider-healthcare/               # Jumpstart workspace_path (deployed by catalog)
│   ├── parameter.yml                        # fabric-cicd find/replace rules (stays at root)
│   ├── 00_Start_Here/
│   │   └── Healthcare_Launcher.Notebook/    # ENTRY POINT — orchestrator notebook
│   ├── 01_Data_Generation/
│   │   ├── NB_Generate_Sample_Data.Notebook/
│   │   └── NB_Generate_Incremental_Data.Notebook/
│   ├── 02_Medallion_Pipeline/
│   │   ├── 01_Bronze_Ingest_CSV.Notebook/
│   │   ├── 02_Silver_Stage_Clean.Notebook/
│   │   ├── 03_Silver_ODS_Enrich.Notebook/
│   │   ├── 06a_Create_Gold_Lakehouse_Tables.Notebook/
│   │   └── 06b_Gold_Transform_Load_v2.Notebook/
│   ├── 03_Lakehouses/
│   │   ├── lh_bronze_raw.Lakehouse/
│   │   ├── lh_silver_stage.Lakehouse/
│   │   ├── lh_silver_ods.Lakehouse/
│   │   └── lh_gold_curated.Lakehouse/
│   ├── 04_Orchestration/
│   │   ├── PL_Healthcare_Full_Load.DataPipeline/
│   │   ├── PL_Healthcare_Master.DataPipeline/
│   │   └── PL_Healthcare_RTI.DataPipeline/
│   ├── 05_Real_Time_Intelligence/
│   │   ├── README.md                        # RTI flow + use cases (links to RTI_STREAMING_GUIDE.md)
│   │   ├── Healthcare_RTI_Eventhouse.Eventhouse/
│   │   ├── Healthcare_RTI_DB.KQLDatabase/    # 7 tables (landing + typed + scored) + streaming/update policies
│   │   ├── NB_RTI_Setup_Eventhouse.Notebook/
│   │   ├── NB_RTI_Event_Simulator.Notebook/
│   │   ├── NB_RTI_Fraud_Detection.Notebook/
│   │   ├── NB_RTI_Care_Gap_Alerts.Notebook/
│   │   └── NB_RTI_HighCost_Trajectory.Notebook/
│   └── 06_AI_and_Graph/
│       ├── HealthcareDemoHLS.SemanticModel/
│       ├── HealthcareAnalyticsDashboard.Report/
│       ├── HealthcareHLSAgent.DataAgent/
│       ├── NB_Deploy_Graph_Model.Notebook/
│       └── NB_Refresh_Graph_Model.Notebook/
│
├── data_agents/                             # Reference agent configs (manual UI step)
│   └── HealthcareOntologyAgent.DataAgent/   # Graph-based agent — created via UI
│
├── ontology/                                # Ontology manifest (12 entities, 18 relationships)
│   └── Healthcare_Demo_Ontology_HLS/        # Deployed by NB_Deploy_Graph_Model
│
├── healthcare_knowledge/                    # AI agent knowledge base (uploaded to lh_gold_curated)
│   ├── clinical_guidelines/
│   ├── compliance/
│   ├── denial_management/
│   ├── formulary/
│   ├── provider_network/
│   └── quality_measures/
│
├── foundry_agent/
│   └── orchestrator_instructions.md         # Version-controlled orchestrator prompt
│
├── DATA_AGENT_GUIDE.md                      # Agent instructions, routing, few-shots, KB
├── DATA_AGENT_INSTRUCTIONS.md               # Step-by-step agent setup
├── POWERBI_DASHBOARD_GUIDE.md               # Power BI report pages, measures, Direct Lake tips
├── FOUNDRY_IQ_SETUP_GUIDE.md                # Azure AI Foundry orchestrator agent setup
├── FOUNDRY_ORCHESTRATOR_TROUBLESHOOTING.md  # Hybrid query debugging guide
├── HLS_AGENT_PREP_FOR_AI.md                 # AI-prep checklist for the HLS agent
├── RTI_DASHBOARD_GUIDE.md                   # RTI dashboard pages and KQL queries
├── RTI_STREAMING_GUIDE.md                   # RTI streaming scenario walkthrough
├── SAMPLE_QUESTIONS.md                      # 90+ copy-paste questions for all agents
└── EXECUTIVE_DEMO_RUNBOOK.md                # 5-minute exec walkthrough script
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Workspace is not empty" | The Jumpstart installer expects an empty workspace before deploying items |
| Pipeline fails | Open PL_Healthcare_Master → check activity run details. Common cause: lakehouse tables not yet created |
| Semantic model shows no data | Run the pipeline first (Cell 3) — it populates Gold lakehouse tables that the model reads |
| Data Agent returns generic answers | Open the agent → verify AI instructions are pasted from [DATA_AGENT_INSTRUCTIONS.md § 1a](DATA_AGENT_INSTRUCTIONS.md#1a--ai-instructions-stage_configjson) and data source is connected |
| Graph Agent shows no results | Re-run **Cell 7** of `Healthcare_Launcher` (deploys ontology + graph) |
| RTI tables stay empty | **Cell 6** auto-fetches the Eventstream Custom Endpoint connection string. If ingestion never starts, check the `[WARN]` output in Cell 6 — if auto-fetch failed, paste the connection string into `ES_CONNECTION_STRING` manually and re-run. Verify the update policies exist (see [RTI_STREAMING_GUIDE.md](RTI_STREAMING_GUIDE.md)) |

## Credits

Built with:
- [fabric-cicd](https://pypi.org/project/fabric-cicd/) for artifact deployment
- Synthetic data generated with [Faker](https://faker.readthedocs.io/)
