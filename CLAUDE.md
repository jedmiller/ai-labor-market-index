# CLAUDE.md — AI Labor Market Index

This file provides context for AI assistants working in this repository.

## Project Overview

The **AI Labor Market Index** tracks the impact of artificial intelligence on employment. It aggregates data from multiple sources to produce monthly metrics on AI-driven job displacement, augmentation, and creation across industries and occupations in the US economy.

The primary data source is the **Anthropic Economic Index** (via HuggingFace), supplemented by BLS employment data, academic research (arXiv), news events, and job posting data.

## Repository Structure

```
ai-labor-market-index/
├── scripts/
│   ├── run_updated_ai_impact.py      # Main workflow orchestrator
│   ├── collection/                   # Data collection scripts
│   │   ├── collect_anthropic_index.py
│   │   ├── collect_bls.py
│   │   ├── collect_bls_occupation_employment.py
│   │   ├── collect_ai_jobs.py
│   │   ├── collect_jobs.py
│   │   ├── collect_news.py
│   │   └── collect_arxiv.py
│   ├── processing/                   # Data transformation scripts
│   │   ├── process_anthropic_index.py    # Legacy format processor
│   │   ├── process_anthropic_index_v2.py # New format (August 2025+)
│   │   ├── process_anthropic_occupation_data.py
│   │   ├── process_employment.py
│   │   ├── process_jobs.py
│   │   ├── process_news.py
│   │   ├── process_research.py
│   │   └── data_alignment.py
│   ├── analysis/                     # Impact calculation and projections
│   │   ├── calculate_ai_impact.py    # Primary impact calculator
│   │   ├── calculate_index.py        # Traditional index (for comparison)
│   │   ├── project_impact.py         # 5-year S-curve projections
│   │   ├── confidence_intervals.py   # Monte Carlo uncertainty (1000 sims)
│   │   ├── occupation_industry_mapper.py
│   │   ├── history_manager.py
│   │   └── generate_visualization_export.py
│   ├── validation/
│   │   ├── validate_occupation_mapping.py
│   │   └── validate_apis.py
│   └── utils/
│       ├── soc_code_mapper.py
│       └── fix_anthropic_data_timestamps.py
├── data/
│   ├── raw/                          # Source data (committed to repo)
│   │   ├── anthropic_index/          # 40+ JSON files from Anthropic
│   │   ├── bls/                      # BLS employment statistics
│   │   ├── jobs/                     # Job posting data
│   │   ├── arxiv/                    # Academic paper metadata
│   │   └── news/                     # News article data
│   ├── processed/                    # Calculated outputs (committed)
│   │   └── projections/              # 5-year projection scenarios
│   ├── mappings/                     # Reference mappings (static)
│   │   ├── company_to_industries.json
│   │   ├── company_to_occupations.json
│   │   ├── occupation_to_industry.json
│   │   └── occupation_to_tasks.json
│   └── simulation/                   # Simulated data for testing
├── tests/
│   ├── test_occupation_mapping.py    # Main test suite (5 test classes)
│   └── test_projection_realism.py
├── docs/
│   ├── PORTFOLIO_INTEGRATION.md      # React component integration guide
│   └── AUGUST_2025_UPDATE_COMPARISON.md
├── .github/workflows/
│   ├── update_ai_labor_index.yml     # Weekly index update (Sundays 2 AM UTC)
│   ├── generate_ai_impact_projections.yml  # Monthly projections (7th, 6 AM UTC)
│   └── verify_history.yml
├── requirements.txt
├── README.md
└── updated-methodology-guide.md
```

## Development Workflow

### Environment Setup

```bash
pip install -r requirements.txt
```

Required Python version: **3.10+** (per CI configuration).

Dependencies:
- `requests==2.28.2` — HTTP client for API calls
- `beautifulsoup4==4.12.2` — Web scraping
- `pandas==2.0.0` — Data manipulation
- `numpy==1.24.3` — Numerical computation
- `nltk==3.8.1` — NLP (requires punkt and stopwords corpora)
- `huggingface_hub==0.19.4` — Fetching Anthropic Economic Index data
- `lxml==4.9.3` — XML/HTML parser (required by BeautifulSoup)

After install, download NLTK data:
```bash
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"
```

### Running the Full Pipeline

The main orchestrator handles the complete workflow:

```bash
python scripts/run_updated_ai_impact.py --year 2025 --month 8
```

Optional flags:
- `--simulate` — use simulated data (for testing without real API calls)
- `--no-projections` — skip generating 5-year projections
- `--no-confidence` — skip Monte Carlo confidence intervals
- `--projection-years N` — set projection horizon (default: 5)

### Running Individual Steps

Steps can be run independently, always requiring `--year` and `--month` arguments:

**Collection:**
```bash
python scripts/collection/collect_anthropic_index.py --year=2025 --month=8
python scripts/collection/collect_bls.py --year=2025 --month=8
python scripts/collection/collect_news.py --year=2025 --month=8
python scripts/collection/collect_arxiv.py --year=2025 --month=8
python scripts/collection/collect_ai_jobs.py --year=2025 --month=8
```

**Processing:**
```bash
python scripts/processing/process_anthropic_index.py --year=2025 --month=8
python scripts/processing/process_employment.py --year=2025 --month=8
python scripts/processing/process_news.py --year=2025 --month=8
python scripts/processing/process_research.py --year=2025 --month=8
```

**Analysis:**
```bash
python scripts/analysis/calculate_ai_impact.py --year=2025 --month=8 --input-dir data/processed --output-dir data/processed
python scripts/analysis/calculate_index.py --year=2025 --month=8 --input-dir data/processed --output-dir data/processed
```

### Running Tests

```bash
python -m pytest tests/
# or
python -m unittest discover tests/
```

Tests use `unittest` with mock data and do not require real API keys.

## Key Conventions

### File Naming

All data files use a consistent date-stamped naming pattern:

- Raw data: `data/raw/<source>/<source>_<YYYY>_<MM>_<type>.json`
  - Example: `data/raw/anthropic_index/anthropic_index_2025_08_occupations.json`
- Processed data: `data/processed/<type>_<YYYYMM>.json`
  - Example: `data/processed/ai_labor_impact_202508.json`
- Projections: `data/processed/projections/<type>_<YYYYMM>.json`

A `_latest` suffix variant is also maintained for the most recent output of each type.

### Anthropic Data Format Versioning

There are two processor versions for Anthropic Economic Index data:

- **Legacy format** (pre-August 2025): use `process_anthropic_index.py`
- **New format** (August 2025+): use `process_anthropic_index_v2.py`

The orchestrator automatically detects which to use by checking for the presence of `anthropic_index_<YYYY>_<MM>_occupations.json`.

### Impact Methodology

The primary methodology uses a component-based formula:

```
Net Employment Impact = Employment × [1 - Displacement Effect + Creation Effect × Market_Maturity + Demand Effect]
```

Four components:
1. **Displacement Effect** — direct job substitution by AI
2. **Creation Effect** — new jobs created due to AI (scaled by market maturity)
3. **Market Maturity** — adoption stage (early / growth / mature)
4. **Demand Effect** — productivity-driven demand expansion

Three projection scenarios: **Conservative**, **Moderate**, **Aggressive**.

See `updated-methodology-guide.md` for full details.

### Data Flow

```
External APIs / HuggingFace
        ↓
scripts/collection/  →  data/raw/
        ↓
scripts/processing/  →  data/processed/
        ↓
scripts/analysis/    →  data/processed/ + data/processed/projections/
```

All intermediate and final outputs are JSON files committed to the repository.

### Occupation Classification

- Occupations are mapped using SOC (Standard Occupational Classification) codes.
- `scripts/utils/soc_code_mapper.py` handles SOC code standardization.
- `scripts/analysis/occupation_industry_mapper.py` maps occupations to NAICS industries.
- `data/mappings/occupation_to_industry.json` is the primary reference mapping.
- As of August 2025 update, 82.9% of occupations are classified (up from 6.1%).

## Automation / CI

### GitHub Actions Workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| `update_ai_labor_index.yml` | Sundays 2 AM UTC + manual | Full weekly index update |
| `generate_ai_impact_projections.yml` | 7th of month 6 AM UTC | Monthly projection generation |
| `verify_history.yml` | After index calculation | Validate and update historical data |

### Required GitHub Secrets

| Secret | Used By |
|---|---|
| `BLS_API_KEY` | `collect_bls.py` |
| `NEWS_API_KEY` | `collect_news.py` |
| `PAT_TOKEN` | Git push after workflow runs |

The weekly workflow targets the **previous month's** data (not current month).

### Workflow Behavior

- Data collection failures for non-BLS sources are treated as warnings; the pipeline continues with default values.
- BLS data failures are fatal and halt the pipeline.
- The workflow automatically detects the most recent available Anthropic data if the target month isn't yet published.
- Results are auto-committed to the `master` branch by GitHub Actions Bot.

## Output Data Structure

Key output files produced by the analysis step:

```json
// data/processed/ai_labor_impact_<YYYYMM>.json
{
  "date": "2025-08",
  "components": {
    "displacement_effect": 0.12,
    "creation_effect": 0.08,
    "market_maturity": 0.45,
    "demand_effect": 0.03
  },
  "total_impact": -0.06,
  "employment_stats": { ... },
  "job_trends": {
    "top_augmented_roles": [{"occupation": "...", "score": 0.9}],
    "top_automated_roles": [{"occupation": "...", "score": 0.85}]
  }
}

// data/processed/projections/impact_projections_<YYYYMM>.json
{
  "Conservative": {"2026": -0.04, "2027": -0.06, ...},
  "Moderate":     {"2026": -0.07, "2027": -0.11, ...},
  "Aggressive":   {"2026": -0.10, "2027": -0.17, ...}
}
```

## Important Notes for AI Assistants

- **Do not modify files in `data/raw/` or `data/processed/`** — these are auto-generated outputs managed by CI workflows and committed as data artifacts.
- **Do not modify `data/mappings/`** without understanding the downstream impact on occupation classification coverage.
- When adding new collection or processing scripts, follow the existing pattern: accept `--year` and `--month` as required CLI arguments, write outputs to the appropriate `data/raw/` or `data/processed/` subdirectory.
- When modifying the impact calculation methodology, update `updated-methodology-guide.md` to reflect the change.
- The traditional `calculate_index.py` is maintained for historical comparison only — the `calculate_ai_impact.py` is the primary calculation.
- Tests in `tests/` use mock data; they should pass without any API keys or network access.
- Logs from workflow runs are written to `ai_impact_workflow.log` in the project root.
