# AI Labor Market Impact - Updated Methodology Guide

## Overview

This guide explains the updated methodology for calculating the AI Labor Market Impact and how to use it alongside the existing AI Labor Market Index system. The updated methodology uses a component-based calculation approach that more accurately models the displacement and creation effects of AI on employment.

## Core Formula

The updated methodology uses the following formula:

```
Net Employment Impact = Employment × [1 - Displacement Effect + Creation Effect × Market_Maturity + Demand Effect]
```

Where:

- **Displacement Effect** = Pure Automation Impact + Capacity Augmentation Impact
- **Creation Effect** = (Direct AI Jobs/Total Employment) + (AI Infrastructure Jobs/Total Employment)
- **Market Maturity** = Years Since AI Adoption / Expected Maturity Period
- **Demand Effect** = Productivity Gain × Labor Share × Elasticity Factor

This formula provides a more nuanced view of AI's impact on employment by considering both job displacement and creation factors, as well as the maturity of AI adoption in the market.

## Key Components

### 1. Displacement Effect

Measures the job displacement caused by AI through automation and augmentation:

- **Pure Automation Impact** = Automation% × Displacement Factor × Implementation Rate
- **Capacity Augmentation Impact** = Augmentation% × Efficiency Factor × Adoption Rate × Capacity Utilization Factor

The displacement effect considers both direct automation (jobs replaced) and capacity augmentation (making existing workers more efficient, potentially requiring fewer workers).

### 2. Creation Effect

Measures new jobs created by AI:

- Direct AI Jobs (roles that directly work with AI)
- Infrastructure AI Jobs (roles that support AI systems)

This component is calculated using data from O*NET emerging occupations and job posting analysis.

### 3. Market Maturity

A multiplier that represents the maturity of AI adoption in the market:

- Early stage (0-2 years): 0.2
- Growth stage (2-5 years): 0.5
- Mature stage (5+ years): 0.8

This factor recognizes that the job creation effects of AI increase as the market matures.

### 4. Demand Effect

Accounts for increased demand due to productivity improvements:

- Productivity Gain × Labor Share × Elasticity Factor

This component captures how productivity improvements from AI can lead to increased demand and potentially more jobs.

## Using the Updated Methodology

### Running the Calculation

The updated methodology is implemented in the `run_updated_ai_impact.py` script, which orchestrates the entire workflow:

```bash
python scripts/run_updated_ai_impact.py --year 2025 --month 5
```

Options:
- `--year`: Target year (required)
- `--month`: Target month (required)
- `--simulate`: Use simulated data when actual data is unavailable
- `--no-projections`: Skip generating future projections
- `--no-confidence`: Skip generating confidence intervals
- `--projection-years`: Number of years to project (default: 5)

### Output Files

The script produces several output files in the `data/processed` directory:

- `ai_labor_impact_YYYYMM.json`: Main impact calculation results
- `ai_labor_impact_latest.json`: Copy of the most recent calculation
- `methodology_comparison_YYYYMM.json`: Comparison between traditional and updated methodologies

Projection files in `data/processed/projections`:
- `ai_impact_projections_YYYYMM.json`: Summary projections
- `ai_impact_projections_full_YYYYMM.json`: Detailed projections
- `ai_impact_confidence_YYYYMM.json`: Confidence intervals

### Integration with Existing System

The updated methodology runs alongside the existing AI Labor Market Index calculation, using the same data collection pipeline. Both calculations are performed, allowing for comparison between the traditional index and the updated impact calculation.

## Component Scripts

The updated methodology is implemented across several new scripts:

### Analysis Scripts
- `scripts/analysis/calculate_ai_impact.py`: Main calculation script for the updated methodology
- `scripts/analysis/project_impact.py`: Generates future projections using S-curve adoption models
- `scripts/analysis/confidence_intervals.py`: Implements Monte Carlo simulation for confidence intervals

### Collection Scripts
- `scripts/collection/collect_ai_jobs.py`: New script to collect AI-specific job posting data

### Workflow Script
- `scripts/run_updated_ai_impact.py`: Orchestrates the entire calculation workflow

## Differences from Traditional Index

The updated methodology differs from the traditional AI Labor Market Index in several key ways:

1. **Component-Based Approach**: Uses distinct components (displacement, creation, demand) instead of weighted signals.

2. **Direct Employment Impact**: Calculates direct employment changes rather than a normalized index.

3. **Industry-Specific Parameters**: Uses industry-specific parameters for more accurate sector-level impacts.

4. **Forward-Looking**: Includes projection capabilities with confidence intervals.

5. **Validation Features**: Incorporates Monte Carlo simulation for uncertainty quantification.

The traditional index is still calculated for comparison and historical continuity.

## Data Requirements

The updated methodology uses the same data sources as the traditional index, with additional requirements:

- BLS employment data (required)
- Anthropic Economic Index data (for automation/augmentation rates)
- AI job postings data (for Creation Effect)
- News and research data (supplementary)

When actual data is unavailable, the system can use simulation mode to generate synthetic data for testing and development purposes.

## Scenario Definitions

The projection system includes three scenarios:

1. **Conservative**:
   - Adoption Growth: 15%
   - Efficiency Improvement: 5%
   - Implementation Acceleration: 10%

2. **Moderate**:
   - Adoption Growth: 30%
   - Efficiency Improvement: 10%
   - Implementation Acceleration: 20%

3. **Aggressive**:
   - Adoption Growth: 50%
   - Efficiency Improvement: 15%
   - Implementation Acceleration: 35%

These scenarios provide a range of possible futures for AI's impact on employment.