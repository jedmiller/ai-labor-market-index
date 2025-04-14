# Data Alignment Strategy for AI Labor Market Index

## Overview

This document outlines a strategy for aligning data structures across the different data sources in the AI Labor Market Index, with a focus on integrating the Anthropic Economic Index with BLS data and news event data. The goal is to create a consistent framework for understanding how AI affects occupations, tasks, and industries.

## Current Data Structures

### Anthropic Economic Index
- **Focus**: Occupation-level and task-level analysis of AI impact
- **Key metrics**: Automation rates, augmentation rates, task usage
- **Strengths**: Detailed breakdown of which tasks AI automates vs. augments
- **Format**: Structured datasets with occupation-task relationships

### Bureau of Labor Statistics (BLS)
- **Focus**: Industry-level employment changes
- **Key metrics**: Employment counts, year-over-year changes
- **Strengths**: Reliable, official employment data
- **Format**: Time series data for broad industry categories

### News Events
- **Focus**: Company-specific hiring and layoff events
- **Key metrics**: Event type (hiring/layoff), company, count
- **Strengths**: Captures recent, newsworthy workforce changes
- **Format**: Extracted events from news articles

## Alignment Opportunities

### 1. Unified Occupation Taxonomy

**Challenge**: The three data sources use different classification systems:
- Anthropic: Specific occupation titles ("Software Developers", "Data Scientists")
- BLS: Industry categories ("Information", "Professional Services")
- News: Company names without occupation specificity

**Solution**: Create mapping tables that connect:
- Anthropic occupations to BLS industries
- Anthropic occupations to typical companies that employ them
- BLS industries to occupational categories
- News-mentioned companies to affected occupations

### 2. Task-Based Analysis Framework

**Challenge**: Only Anthropic data provides task-level information, while the other sources operate at higher levels.

**Solution**: Use Anthropic's task data to enrich the other data sources:
- Map BLS industry changes to likely affected tasks
- Analyze news events to identify which tasks might be affected
- Calculate automation vs. augmentation potential for industries and companies based on their typical task composition

### 3. Automation/Augmentation Impact Model

**Challenge**: Only Anthropic explicitly distinguishes between automation (job displacement) and augmentation (job enhancement).

**Solution**: Extend the automation/augmentation framework to other data sources:
- Analyze news events to classify as primarily automation or augmentation-driven
- Interpret BLS employment changes through the lens of automation vs. augmentation
- Create a unified impact score that accounts for both job creation and job enhancement

## Implementation Strategy

### 1. Create Mapping Files

Create structured JSON mapping files that connect the different taxonomies:

```json
{
  "occupation_to_industry": {
    "Software Developers": "Information",
    "Data Scientists": "Professional and Business Services",
    "Financial Analysts": "Financial Activities"
  },
  "occupation_categories": {
    "Technology": ["Software Developers", "Data Scientists", "IT Specialists"],
    "Finance": ["Financial Analysts", "Accountants"]
  },
  "company_to_occupations": {
    "Google": ["Software Developers", "Data Scientists", "Product Managers"],
    "JP Morgan": ["Financial Analysts", "Data Scientists"]
  }
}
```

### 2. Develop Data Enrichment Classes

Create utility classes that enrich each data source with insights from the others:

#### BLS Data Enrichment
- Add occupation-level breakdown to industry data
- Add automation/augmentation potential for each industry
- Map employment changes to likely task impacts

#### News Event Enrichment
- Classify events by automation vs. augmentation impact
- Map company events to affected occupations and tasks
- Estimate long-term impact beyond immediate hiring/layoff numbers

#### Anthropic Data Contextualization
- Add employment context to occupation data
- Connect task automation data to real-world news events
- Create industry-level aggregations of occupation-level insights

### 3. Implement Unified Impact Calculation

Modify the index calculation to incorporate the aligned data structure:

```python
def calculate_unified_impact(self, anthropic_data, bls_data, news_data):
    """Calculate unified impact across data sources using consistent taxonomy."""
    impact_by_occupation = {}
    
    # Process Anthropic occupation data
    for occupation, data in anthropic_data.get("occupation_automation", {}).items():
        impact_by_occupation[occupation] = {
            "automation_rate": data.get("automation_rate", 0),
            "augmentation_rate": data.get("augmentation_rate", 0),
            "net_impact": data.get("augmentation_rate", 0) - data.get("automation_rate", 0)
        }
    
    # Map BLS industry data to occupations
    for industry, data in bls_data.get("industries", {}).items():
        # Get occupations in this industry
        industry_occupations = [occ for occ, ind in self.occupation_to_industry.items() if ind == industry]
        
        for occupation in industry_occupations:
            if occupation in impact_by_occupation:
                # Add employment change as context
                impact_by_occupation[occupation]["employment_change"] = data.get("change_percentage", 0)
                
                # Adjust net impact based on employment data
                impact_by_occupation[occupation]["net_impact"] *= (1 + (data.get("change_percentage", 0) / 100))
    
    # Integrate news events by occupation
    for event in news_data.get("events", []):
        # Map company to related occupations
        company = event.get("company", "")
        related_occupations = self.company_to_occupations.get(company, [])
        
        for occupation in related_occupations:
            if occupation in impact_by_occupation:
                # Adjust net impact based on event type
                event_factor = 0.1  # Small effect for a single event
                if event.get("event_type") == "hiring" and event.get("ai_impact_type") == "augmentation":
                    impact_by_occupation[occupation]["net_impact"] += event_factor
                elif event.get("event_type") == "layoff" and event.get("ai_impact_type") == "automation":
                    impact_by_occupation[occupation]["net_impact"] -= event_factor
    
    return impact_by_occupation
```

### 4. Enhance Visualization Components

Update the dashboard to visualize the aligned data structure:

- **Occupation Impact View**: Shows automation vs. augmentation by occupation
- **Task Impact View**: Shows which tasks are being automated vs. augmented
- **Industry Trend View**: Shows employment changes with automation/augmentation context
- **Event Timeline**: Shows news events mapped to occupation and task impacts

## Technical Components to Implement

1. **Mapping File Generator**: Script to create and update cross-mapping files
2. **Data Enrichment Pipeline**: Process to enhance each data source with insights from others
3. **Unified Impact Model**: Updated index calculation using the aligned taxonomy
4. **Enhanced Visualization**: Dashboard updates to show the richer, aligned data structure

## Benefits

1. **Comprehensive Understanding**: Connect dots between industries, occupations, tasks, and events
2. **Forward-Looking Insights**: Distinguish between automation (job displacement) and augmentation (job enhancement)
3. **Actionable Intelligence**: Identify specific tasks being automated vs. augmented
4. **Nuanced Index**: More accurate reflection of AI's complex impact on the labor market

## Next Steps

1. Create initial mapping files between taxonomies
2. Implement data enrichment classes
3. Update index calculation to use unified model
4. Enhance visualization components
5. Create documentation for the aligned data model

## References

- Standard Occupational Classification (SOC) System
- O*NET Database of Occupational Information
- Anthropic Economic Index Methodology
- BLS Industry Classification System