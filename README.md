# 🏗️ CRE Intelligence Hub

**Cross-Source Real Estate Analytics Pipeline & Dashboard**

Turn raw, messy data into structured intelligence and actionable insights.

---

## 🎯 What It Does

A lightweight data engineering pipeline that ingests multiple data sources, normalises them into a unified dataset, and generates insights through cross-source analysis — visualised on an interactive dashboard.

## 📊 Data Sources (5 Mediums)

| Source | Medium | Records | Description |
|--------|--------|---------|-------------|
| CRE Lending | **Excel** (`openpyxl`) | 64 | European commercial real estate lending deals |
| Cities | **CSV** (`pandas`) | 128 | US city locations for normalisation |
| Homes / Zillow | **CSV** (`pandas`) | 70 | Residential property data |
| Property Week | **RSS** (`feedparser`) | 12 | UK property news articles |
| JLL / Altus Group | **Web Scrape** (`BeautifulSoup`) | 23 | Market insight articles |
| FMP API | **REST API** (`requests`) | 10 | REIT company profiles & stock data |

**Total: 307 unified records across 8 sources and 5 ingestion mediums**

## 🧠 Pipeline Architecture

```
Raw Data (Excel, CSV, RSS, HTML, API)
        │
        ▼
   ┌─────────┐
   │ INGEST  │  Read 8 sources via 5 different mediums
   └────┬────┘
        ▼
   ┌──────────┐
   │NORMALISE │  Unified JSON schema (id, source, entities, sectors...)
   └────┬─────┘
        ▼
   ┌──────────┐
   │ CLASSIFY │  Keyword NLP: sector tagging, entity extraction, summarisation
   └────┬─────┘
        ▼
   ┌────────┐
   │  LINK  │  Cross-source linking by shared entities/locations/sectors
   └────┬───┘
        ▼
   ┌──────────┐
   │ INSIGHTS │  6 cross-source insights from statistical analysis
   └────┬─────┘
        ▼
   Dashboard (HTML/CSS/JS + Chart.js)
```

## 🚀 Quick Start

### Run the Pipeline
```bash
python3 pipeline/run_pipeline.py
```

### View the Dashboard
```bash
npm run dev
# → http://localhost:3000
```

### Deploy to Vercel
```bash
npx -y vercel --prod
```

## ⚙️ NLP Strategy (No LLM)

- **Sector classification**: Dictionary-based keyword matching (30+ CRE terms → sector labels)
- **Entity extraction**: Regex for financial values (£/€/$), capitalisation heuristics for organisations, curated EU city list + CSV enrichment for locations
- **Cross-source linking**: Entity overlap scoring (locations + organisations + sectors) between records from different sources
- **Summarisation**: Sentence-boundary extraction (first N sentences under character limit)

## ⚖️ Trade-offs

| Decision | Pro | Con |
|----------|-----|-----|
| Fallback data when scraping fails | Pipeline always produces complete output | Some data may not be live |
| Keyword NLP vs ML models | Fast, transparent, no dependencies | Lower recall for edge cases |
| Static JSON dashboard | Zero-config deployment, instant load | No real-time updates |
| Single-file pipeline | Easy to understand and modify | Less modular for large teams |

## 📁 Project Structure

```
cre-intelligence/
├── pipeline/
│   └── run_pipeline.py      # ETL pipeline (723 lines)
├── dashboard/
│   ├── index.html            # Dashboard UI
│   ├── style.css             # Premium dark theme
│   ├── app.js                # Interactive charts & query
│   └── data.json             # Pipeline output (307 records)
├── data/
│   ├── raw/                  # Raw source files
│   └── processed/
│       └── unified_dataset.json
├── vercel.json               # Deployment config
├── package.json
└── README.md
```

## 📈 Key Insights Generated

1. **Lending Market Concentration** — Top lenders control disproportionate deal flow
2. **Media-Lending Sector Divergence** — Trending sectors in news underrepresented in lending
3. **US Residential Pricing Intelligence** — New-build premium mirrors EU "flight to quality"
4. **Public vs Private Capital Imbalance** — REIT dry powder implications for cap rates
5. **Geographic Hotspot Analysis** — Most-mentioned locations across all sources
6. **Property Type Intelligence** — Cross-source convergence on asset classes

---

Built as a data engineering demonstration — lightweight NLP, no LLM dependency.
