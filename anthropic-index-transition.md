# Anthropic Economic Index Integration Plan

## Project Overview

This document outlines the plan to replace the current Job Trends component of the AI Labor Market Impact Index with data from the Anthropic Economic Index. This transition will improve the quality and representativeness of our labor market analysis by incorporating empirical data from millions of AI interactions across diverse economic tasks.

## Rationale for Change

The current implementation faces several limitations:

- Relies on the Remotive API which only captures remote jobs
- Uses static/hardcoded role definitions
- May not accurately reflect broader labor market AI adoption patterns

The Anthropic Economic Index offers significant advantages:

- Based on millions of real Claude conversations across diverse economic tasks
- Maps to the U.S. Department of Labor's O*NET Database
- Distinguishes between automation (43%) and augmentation (57%) patterns
- Provides comprehensive skill and task-level analysis

## Available Datasets

The Anthropic Economic Index provides several datasets available at [Hugging Face](https://huggingface.co/datasets/Anthropic/EconomicIndex/tree/main):

- `occupation_categories.json` - High-level occupation categories and usage
- `occupation_usage.json` - Detailed occupation-level AI usage
- `task_usage.json` - Task-level AI usage statistics
- `skill_presence.json` - Skills demonstrated in AI conversations
- `occupation_automation.json` - Automation vs. augmentation breakdown

## Implementation Plan

### Phase 1: Data Collection & Processing

1. Create a new `AnthropicIndexCollector` class that:
   - Fetches data from Hugging Face repository
   - Saves datasets to our data structure
   - Replaces the current remote jobs API calls

2. Update `scripts/processing/process_jobs.py` to:
   - Process the Anthropic data into our index format
   - Extract growing roles based on occupation usage data
   - Identify roles at risk using automation statistics
   - Calculate meaningful growth rates and trends

### Phase 2: Index Calculation Integration

1. Modify `scripts/analysis/calculate_index.py` to:
   - Update the `calculate_job_trends_score` method
   - Incorporate automation/augmentation ratios
   - Balance growing roles vs. roles at risk
   - Maintain the same -100 to +100 score range

2. Ensure backward compatibility:
   - Support both data formats during transition
   - Provide fallback mechanisms if Anthropic data isn't available

### Phase 3: Dashboard Enhancement

1. Add new visualizations to `AILaborMarketDashboard.jsx`:
   - Task-level usage breakdown
   - Automation vs. augmentation pie chart
   - Skills representation data
   - Occupation-level analysis

2. Update existing visualizations:
   - Emerging roles chart (based on augmentation data)
   - Roles at risk chart (based on automation data)
   - Industry impact visualization

### Phase 4: Testing & Deployment

1. Comprehensive testing:
   - Verify data collection reliability
   - Validate index calculation consistency
   - Ensure visualization accuracy

2. Documentation updates:
   - Update methodology documentation
   - Add new data source attributions
   - Explain new metrics and visualizations

## Technical Components

### New Data Collection Component

```python
# scripts/collection/collect_anthropic_index.py
import requests
import json
import os
import logging
from datetime import datetime

logger = logging.getLogger("anthropic-index-collector")

class AnthropicIndexCollector:
    def __init__(self, output_dir="./data/raw/jobs"):
        self.output_dir = output_dir
        self.base_url = "https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main"
        os.makedirs(output_dir, exist_ok=True)
        
    def fetch_dataset(self, filename):
        """Fetch a specific dataset file from the Anthropic Economic Index"""
        url = f"{self.base_url}/{filename}"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch {filename}: {response.status_code}")
            return None
            
    def collect_data(self):
        """Collect all relevant datasets from Anthropic Economic Index"""
        datasets = {
            "occupation_categories": "occupation_categories.json",
            "occupation_usage": "occupation_usage.json", 
            "task_usage": "task_usage.json",
            "skill_presence": "skill_presence.json",
            "occupation_automation": "occupation_automation.json"
        }
        
        collected_data = {}
        timestamp = datetime.now().isoformat()
        
        for key, filename in datasets.items():
            data = self.fetch_dataset(filename)
            if data:
                collected_data[key] = data
                
        return collected_data
```

### Key Dashboard Enhancements

```jsx
// New component for automation vs. augmentation visualization
const AutomationAugmentationChart = ({ data }) => {
  // Transform data for visualization
  const chartData = [
    { name: 'Automation', value: 43 },  // From the Anthropic paper
    { name: 'Augmentation', value: 57 }  // From the Anthropic paper
  ];
  
  return (
    <div className="border border-gray-700 rounded-lg p-4 mb-6">
      <h4 className="text-lg font-light tracking-wider mb-4">Automation vs. Augmentation</h4>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              innerRadius={60}
              outerRadius={80}
              fill="#8884d8"
              paddingAngle={5}
              dataKey="value"
            >
              {chartData.map((entry, index) => (
                <Cell 
                  key={`cell-${index}`} 
                  fill={index === 0 ? '#ef4444' : '#4ade80'} 
                />
              ))}
            </Pie>
            <Tooltip 
              contentStyle={{ backgroundColor: '#222', borderColor: '#555' }}
              formatter={(value) => [`${value}%`, '']}
            />
            <Legend />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};
```

## Using Claude Code for Development

The integration plan is well-suited for implementation with Claude Code:

1. Use Claude Code to:
   - Generate the new collection and processing scripts
   - Update the index calculation logic
   - Enhance the React dashboard components

2. Benefits of using Claude Code:
   - AI assistance in understanding the Anthropic datasets
   - Help with data transformation logic
   - Guidance on visualization techniques

3. Implementation approach:
   - Start with data collection scripts
   - Move to processing components
   - Finally update visualization elements

## Success Metrics

The integration will be considered successful when:

1. The index calculation incorporates Anthropic Economic Index data
2. The dashboard presents new insights from the Anthropic data
3. The system maintains backward compatibility
4. Documentation clearly explains the new methodology

## References

- [Anthropic Economic Index Dataset](https://huggingface.co/datasets/Anthropic/EconomicIndex)
- [Anthropic Paper: Which Economic Tasks are Performed with AI?](https://www.anthropic.com/news/the-anthropic-economic-index)
- [O*NET Database](https://www.onetonline.org/)

## Next Steps

1. Set up development environment with Claude Code
2. Implement the `AnthropicIndexCollector` class
3. Update the jobs processing component
4. Begin integration with the index calculation

This transition will significantly enhance the AI Labor Market Impact Index with empirical data about AI's impact across occupations and tasks, providing a more comprehensive and evidence-based tool for understanding AI's evolving role in the economy.
