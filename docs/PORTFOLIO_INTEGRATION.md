# Portfolio Website Integration Guide

## Quick Start

### Step 1: Deploy the Data
From the ai-labor-market-index project, run:
```bash
./scripts/deploy_to_portfolio.sh
```

This will copy the latest projection data to your portfolio project.

### Step 2: Update Your React Component

In your portfolio project, update your projections component to fetch the new data:

```jsx
// Example: components/AILaborProjections.jsx

import { useEffect, useState } from 'react';

export default function AILaborProjections() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Fetch the latest projection data
    fetch('/data/ai-labor-projections-latest.json')
      .then(res => res.json())
      .then(data => {
        setData(data);
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load projection data:', err);
        setLoading(false);
      });
  }, []);

  if (loading) return <div>Loading projections...</div>;
  if (!data) return <div>No data available</div>;

  return (
    <div>
      {/* Current State */}
      <section>
        <h2>Current Impact (August 2025)</h2>
        <div className="metrics-grid">
          <div className="metric">
            <span className="label">Overall Impact</span>
            <span className="value">{data.current_state.overall_impact_percentage}%</span>
          </div>
          <div className="metric">
            <span className="label">Jobs Affected</span>
            <span className="value">{(data.current_state.total_jobs_affected / 1000000).toFixed(1)}M</span>
          </div>
          <div className="metric">
            <span className="label">US Automation Rate</span>
            <span className="value">{data.current_state.automation_rate_us}%</span>
          </div>
          <div className="metric">
            <span className="label">US Augmentation Rate</span>
            <span className="value">{data.current_state.augmentation_rate_us}%</span>
          </div>
        </div>
      </section>

      {/* Projections Chart */}
      <section>
        <h2>5-Year Projections</h2>
        <ProjectionsChart
          scenarios={data.projections.scenarios}
          timeline={data.projections.timeline}
        />
      </section>

      {/* Industry Breakdown */}
      <section>
        <h2>Industry Impact</h2>
        <IndustryTable industries={data.industry_breakdown} />
      </section>

      {/* Data Quality Indicator */}
      <section className="data-quality">
        <p>Data Quality: {data.metadata.data_sources.anthropic.classified_percentage}% classified</p>
        <p>Last Updated: {data.metadata.generated}</p>
      </section>
    </div>
  );
}
```

### Step 3: Key Data Points Available

The new data structure provides:

#### Current State Metrics:
- `overall_impact_percentage`: -7.6%
- `total_jobs_affected`: -12,108,981
- `automation_rate_us`: 49.07%
- `augmentation_rate_us`: 50.93%
- `transformation_rate`: 13.6%

#### Projections (2026-2030):
```javascript
data.projections.scenarios = {
  conservative: { 2026: -9.41, 2027: -7.61, ... },
  moderate: { 2026: -8.48, 2027: -7.30, ... },
  aggressive: { 2026: -7.90, 2027: -7.17, ... }
}
```

#### Industry Breakdown:
```javascript
data.industry_breakdown = {
  "Information": {
    current_impact: -0.076,
    transformation_rate: 17.0,
    employment: 3041000,
    projected_2030: -2.75
  },
  // ... other industries
}
```

#### US SOC Distribution:
```javascript
data.us_soc_distribution = [
  { category: "Computer and Mathematical", percentage: 26.11 },
  { category: "Educational Instruction", percentage: 10.65 },
  // ... more categories
]
```

#### Confidence Intervals:
```javascript
data.confidence_intervals = {
  2026: { low: -10.2, high: -7.5 },
  2027: { low: -8.5, high: -6.8 },
  // ... through 2030
}
```

## Visualization Recommendations

### 1. Enhanced Geographic Comparison
Show US vs Global automation/augmentation rates:
```jsx
<ComparisonChart
  us={{ automation: 49.07, augmentation: 50.93 }}
  global={{ automation: 56.71, augmentation: 43.29 }}
/>
```

### 2. SOC Distribution Donut Chart
Display the US occupation distribution with the 82.9% classified data:
```jsx
<DonutChart
  data={data.us_soc_distribution}
  unclassified={17.1}
/>
```

### 3. Transformation Rate by Industry
Show which industries are experiencing the most change:
```jsx
<BarChart
  data={Object.entries(data.industry_breakdown).map(([name, info]) => ({
    industry: name,
    rate: info.transformation_rate
  }))}
/>
```

### 4. Confidence Bands on Projections
Add uncertainty visualization:
```jsx
<LineChart
  central={data.projections.scenarios.moderate}
  confidence={data.confidence_intervals}
/>
```

## Automatic Updates

To automate updates when you refresh the AI Labor Market Index:

1. Add to your GitHub Action workflow (`.github/workflows/update_ai_labor_index.yml`):
```yaml
- name: Deploy to Portfolio
  run: |
    ./scripts/deploy_to_portfolio.sh
    cd ../jedidiah-miller-portfolio
    git push
```

2. Or create a cron job:
```bash
# Add to crontab
0 3 * * MON cd ~/Documents/ai-labor-market-index && ./scripts/deploy_to_portfolio.sh
```

## Data Structure Reference

Full data structure available in the JSON includes:
- `metadata`: Data sources and quality indicators
- `current_state`: Current impact metrics
- `projections`: 5-year scenarios
- `industry_breakdown`: 11 industry details
- `us_soc_distribution`: Top 10 occupation categories
- `geographic_insights`: US vs global comparison
- `workforce_events`: Recent hiring/layoff summary
- `research_insights`: Academic sentiment
- `confidence_intervals`: Uncertainty bounds
- `methodology_notes`: Data quality and limitations
- `recommendations`: Policy, business, and worker guidance

## Testing Locally

In your portfolio project:
```bash
# Install dependencies if needed
npm install

# Run development server
npm run dev

# Visit http://localhost:3000/analysis/ai-labor-impact-projections
```

## Deployment

If using Vercel/Netlify with auto-deploy:
```bash
git add .
git commit -m "Update AI projections with August 2025 data"
git push
```

The site will automatically rebuild with the new data.