# AI Labor Market Index

## Overview

The AI Labor Market Index tracks the impact of artificial intelligence on the job market. It aggregates data from multiple sources to provide insights into how AI is affecting employment, job creation, and workforce transformation.

## Data Sources

The index incorporates data from the following sources:

1. **Anthropic Economic Index** (Primary job trends source)
   - Provides detailed occupation-level analysis of AI's impact
   - Distinguishes between automation (job displacement) and augmentation (job enhancement)
   - Includes task-level data on AI usage across occupations

2. **Bureau of Labor Statistics**
   - Provides industry-level employment statistics
   - Tracks employment changes across major economic sectors

3. **Research Publications**
   - Tracks academic and industry research on AI and employment
   - Analyzes sentiment and trends in AI research

4. **News Events**
   - Monitors news about AI-related hiring, layoffs, and workforce changes
   - Provides real-time signals about major AI workforce events

## Recent Updates

### April 2025: Anthropic Economic Index Integration
- The index now uses the Anthropic Economic Index as the primary source for job trends data
- This provides more reliable occupation-level analysis of AI's impact
- Added automation vs. augmentation framework to better understand AI's nuanced effects on jobs
- Legacy job collection scripts are maintained for backwards compatibility

## Usage

To generate the latest index:

```
python scripts/analysis/calculate_index.py
```

To collect the latest data:

```
python scripts/collection/collect_anthropic_index.py
python scripts/collection/collect_bls.py
python scripts/collection/collect_news.py
```

To process the collected data:

```
python scripts/processing/process_anthropic_index.py
python scripts/processing/process_employment.py
python scripts/processing/process_news.py
python scripts/processing/process_research.py
```