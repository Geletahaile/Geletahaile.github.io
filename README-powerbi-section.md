<!--
============================================================
APPEND THIS SECTION TO YOUR EXISTING README.md
Add it after "Results at a Glance" or wherever fits best.
============================================================
-->

---

## Power BI Analytics Layer

The same warehouse that powers the ETL pipeline is the foundation for a four-page Power BI dashboard built on a true star schema. The analytics layer connects directly to `sqldb-geletaedw-dev` via the native Azure SQL connector, demonstrating that **the ETL platform produces data that's actually usable** — not just loaded.

![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-F2C811?style=flat-square&logo=powerbi&logoColor=black) ![DAX](https://img.shields.io/badge/DAX-Measures-blue?style=flat-square) ![Star Schema](https://img.shields.io/badge/Star%20Schema-Modeled-green?style=flat-square)

### Architecture extension

The single fact table from Phase 1 was extended into a proper analytical model:

```
sqldb-geletaedw-dev (Azure SQL)
├── dw.Claim                  [fact — 27,742 rows]
├── dw.DimDate                [date dimension, marked as date table]
├── dw.DimProvider            [800 providers — name, specialty, network status]
├── dw.DimMember              [5,000 members — age band, state, plan type]
├── dw.DimClaimStatus         [status categories — Paid / Denied / Pending]
├── dw.vw_ClaimAnalytics      [pre-joined view for Power BI consumption]
└── meta.PipelineLog          [pipeline run history for operations page]
              ↓
       Power BI Desktop (Import mode)
              ↓
       4-page report (.pbix)
```

### Dashboard pages

| Page | Purpose | Key visuals |
|---|---|---|
| **1. Executive Overview** | High-level KPIs and trends for stakeholders | Paid / Billed / Claim Count / Denial Rate cards, monthly trend line, claim status donut, top providers bar |
| **2. Provider Analytics** | Provider performance and network analysis | Top 20 providers by paid, Billed-vs-Paid by network status (revealing write-off impact), provider detail table with conditional formatting on denial rate |
| **3. Member Analytics** | Demographic and geographic distribution | Claims by age band, filled map across 5-state service area, top 20 members detail table |
| **4. Data Quality & Operations** | Pipeline health monitoring (the differentiator) | Records processed, pipeline success rate, failure count, average run duration; pipeline runs over time by status; records processed per pipeline; recent runs table with traffic-light status coloring |

### Power BI techniques demonstrated

- **Star schema modeling** in the data model view — single fact, surrounded by conformed dimensions, no snowflaking
- **`DimDate` marked as date table** to unlock time intelligence (YTD, MTD, prior-year comparisons)
- **Custom DAX measure library** organized in a dedicated `_Measures` table with display folders — covers core metrics, time intelligence, ratios, and operational health
- **Sync slicers** — year selection on Page 1 carries across all four pages for consistent filter context
- **Conditional formatting with rules** — Denial Rate % uses a red-to-green gradient on Page 2; pipeline Status column uses traffic-light coloring (green / red / yellow) on Page 4
- **Top N filtering** with measure-based ordering (Top 20 providers by total paid, Top 25 pipeline runs by start time, etc.)
- **Custom format strings** on currency measures to display in millions with `M` suffix
- **Tooltips** carrying secondary metrics (claim count, average paid per provider) — keeps the canvas uncluttered while exposing depth on hover

### Sample DAX measures

```dax
Denial Rate % = 
DIVIDE(
    CALCULATE(COUNTROWS('dw Claim'), 'dw DimClaimStatus'[StatusCategory] = "Denied"),
    COUNTROWS('dw Claim')
)

Paid YTD = 
TOTALYTD([Total Paid], 'dw DimDate'[DateValue])

YoY Growth % = 
VAR CurrentPaid = [Total Paid]
VAR PriorYearPaid = CALCULATE([Total Paid], SAMEPERIODLASTYEAR('dw DimDate'[DateValue]))
RETURN DIVIDE(CurrentPaid - PriorYearPaid, PriorYearPaid)

Pipeline Success Rate % = 
DIVIDE(
    CALCULATE(COUNTROWS('meta PipelineLog'), 'meta PipelineLog'[Status] = "Success"),
    COUNTROWS('meta PipelineLog')
)
```

### Operational health story (Page 4 — the differentiator)

Most Power BI portfolios stop at "what's happening with the business." This dashboard adds **"is the pipeline healthy?"** — a question every real production data platform owner asks first thing every morning.

| Operational metric | Source | What it reveals |
|---|---|---|
| Total Records Processed | `meta.PipelineLog` | ~2.7M rows processed across 12 months |
| Pipeline Success Rate | derived measure | 93.98% across 133 historical runs |
| Pipeline Failure Count | derived measure | 7 failed runs with surfaced error context |
| Average Run Duration | DAX `AVERAGEX` over `DATEDIFF` | ~6.85 minutes per run |
| Recent Pipeline Runs | top-25 table, color-coded | Failed runs jump out via red background |

### Screenshots

| | |
|---|---|
| ![Executive Overview](./powerbi/screenshots/01-executive.png) | ![Provider Analytics](./powerbi/screenshots/02-provider.png) |
| **Page 1 — Executive Overview** | **Page 2 — Provider Analytics** |
| ![Member Analytics](./powerbi/screenshots/03-member.png) | ![Data Quality & Ops](./powerbi/screenshots/04-quality.png) |
| **Page 3 — Member Analytics** | **Page 4 — Data Quality & Operations** |

### Repository structure (additions)

```
/powerbi
├── claims-analytics.pbix          # the Power BI Desktop file
├── dax-measures.md                # full DAX measure library
├── data-model.png                 # star schema diagram
└── /screenshots
    ├── 01-executive.png
    ├── 02-provider.png
    ├── 03-member.png
    └── 04-quality.png
```

### Live dashboard

🔗 **[View the published report](#)** *(replace `#` with your Power BI Service link after publishing)*

---

<!--
============================================================
END APPEND SECTION
============================================================
-->
