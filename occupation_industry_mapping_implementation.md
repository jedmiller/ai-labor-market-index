# Occupation-to-Industry Mapping Implementation Guide

## Problem Statement

The current AI Labor Market Impact methodology is **under-utilizing** the granular occupation-level data from Anthropic's Economic Index. Instead of properly mapping occupation-specific automation/augmentation rates to industry employment patterns, the code simply applies average rates uniformly across all industries.

### Current Implementation Issues

```python
# CURRENT (INCORRECT) APPROACH
avg_automation = anthropic_data["statistics"].get("average_automation_rate", 0)
avg_augmentation = anthropic_data["statistics"].get("average_augmentation_rate", 0)

# Apply same rates to all industries - loses all granularity!
for industry in industry_data:
    industry_automation[industry] = avg_automation
    industry_augmentation[industry] = avg_augmentation
```

This approach treats all industries identically, ignoring that:
- Technology industries have different occupational compositions than Healthcare
- Some occupations have much higher automation potential than others
- Industry-specific impacts should reflect their actual workforce composition

## Correct Methodology: Occupation-to-Industry Aggregation

### Data Flow Architecture

```
Anthropic Economic Index → O*NET SOC Codes → BLS Industry×Occupation Matrix → Industry-Specific Rates
     (automation %)           (occupation           (employment by            (weighted averages)
                               definitions)          industry×occupation)
```

### Mathematical Foundation

For each industry `i`, the automation rate should be:

```
Industry_Automation_Rate_i = Σ(Occupation_Automation_Rate_j × Employment_Share_ij)
```

Where:
- `j` = each occupation in industry `i`
- `Employment_Share_ij` = (Employment in occupation `j` within industry `i`) / (Total employment in industry `i`)

## Implementation Plan

### Phase 1: Data Source Integration

#### 1.1 Anthropic Economic Index Processing
**File:** `scripts/collection/process_anthropic_data.py`

```python
def process_anthropic_occupation_data(anthropic_data):
    """
    Extract occupation-level automation/augmentation rates with SOC code mapping.
    
    Expected Anthropic data structure:
    {
        "occupations": [
            {
                "title": "Software Developers",
                "soc_code": "15-1252",
                "automation_rate": 0.25,
                "augmentation_rate": 0.75,
                "tasks": {...}
            }
        ]
    }
    """
    occupation_impacts = {}
    
    for occ in anthropic_data.get("occupations", []):
        soc_code = standardize_soc_code(occ.get("soc_code"))
        if soc_code:
            occupation_impacts[soc_code] = {
                "automation_rate": occ.get("automation_rate", 0),
                "augmentation_rate": occ.get("augmentation_rate", 0),
                "title": occ.get("title", ""),
                "confidence": occ.get("confidence", 0.5)
            }
    
    return occupation_impacts
```

#### 1.2 O*NET SOC Code Standardization
**File:** `scripts/collection/soc_code_mapper.py`

```python
def standardize_soc_code(soc_code):
    """
    Standardize SOC codes to 6-digit format (XX-XXXX).
    Handle various input formats: 15-1252, 151252, 15-1252.00
    """
    if not soc_code:
        return None
    
    # Remove dots, spaces, and ensure proper format
    cleaned = re.sub(r'[.\s]', '', str(soc_code))
    
    if len(cleaned) == 6 and cleaned.isdigit():
        return f"{cleaned[:2]}-{cleaned[2:]}"
    elif len(cleaned) == 7 and cleaned[:2].isdigit() and cleaned[2] == '-':
        return cleaned[:7]  # Already in XX-XXXX format
    
    logger.warning(f"Could not standardize SOC code: {soc_code}")
    return None

def get_onet_soc_mappings():
    """
    Load O*NET SOC code definitions and create mapping table.
    Source: https://www.onetcenter.org/taxonomy.html
    """
    # This should load from O*NET data files
    # For now, implement as lookup table of major SOC groups
    return {
        "11-0000": "Management Occupations",
        "13-0000": "Business and Financial Operations",
        "15-0000": "Computer and Mathematical",
        "17-0000": "Architecture and Engineering",
        # ... complete mapping
    }
```

#### 1.3 BLS Industry×Occupation Employment Data
**File:** `scripts/collection/collect_bls_occupation_employment.py`

```python
def fetch_bls_occupation_by_industry():
    """
    Fetch BLS Occupational Employment and Wage Statistics (OEWS) data.
    This provides employment by detailed occupation within each industry.
    
    API Endpoint: https://api.bls.gov/publicAPI/v2/timeseries/data/
    Series IDs: OEUS[AREA][INDUSTRY][OCCUPATION]
    """
    
    # Major industry codes (NAICS-based)
    industries = {
        "11": "Agriculture, Forestry, Fishing and Hunting",
        "21": "Mining, Quarrying, and Oil and Gas Extraction", 
        "22": "Utilities",
        "23": "Construction",
        "31-33": "Manufacturing",
        "42": "Wholesale Trade",
        "44-45": "Retail Trade",
        "48-49": "Transportation and Warehousing",
        "51": "Information",
        "52": "Finance and Insurance",
        "53": "Real Estate and Rental and Leasing",
        "54": "Professional, Scientific, and Technical Services",
        "55": "Management of Companies and Enterprises",
        "56": "Administrative and Support and Waste Management",
        "61": "Educational Services",
        "62": "Health Care and Social Assistance",
        "71": "Arts, Entertainment, and Recreation",
        "72": "Accommodation and Food Services",
        "81": "Other Services",
        "92": "Public Administration"
    }
    
    occupation_employment = {}
    
    for industry_code, industry_name in industries.items():
        # Fetch all occupations within this industry
        industry_data = fetch_industry_occupation_data(industry_code)
        occupation_employment[industry_name] = industry_data
    
    return occupation_employment

def fetch_industry_occupation_data(industry_code):
    """Fetch detailed occupation employment for a specific industry."""
    # Implementation would use BLS API to get OEWS data
    # Series format: OEUS[AREA][INDUSTRY][OCCUPATION]
    # Example: OEUS000000051000000 (National, Information sector, All occupations)
    pass
```

### Phase 2: Core Mapping Implementation

#### 2.1 Occupation-Industry Mapper
**File:** `scripts/analysis/occupation_industry_mapper.py`

```python
class OccupationIndustryMapper:
    """
    Maps occupation-level AI impacts to industry-level aggregated impacts.
    """
    
    def __init__(self):
        self.occupation_impacts = {}  # From Anthropic data
        self.occupation_employment = {}  # From BLS OEWS
        self.soc_mappings = {}  # From O*NET
        
    def load_data_sources(self, anthropic_file, bls_file, onet_file=None):
        """Load all required data sources."""
        # Load Anthropic occupation impacts
        with open(anthropic_file, 'r') as f:
            anthropic_data = json.load(f)
            self.occupation_impacts = process_anthropic_occupation_data(anthropic_data)
        
        # Load BLS employment by industry×occupation
        with open(bls_file, 'r') as f:
            self.occupation_employment = json.load(f)
        
        # Load O*NET SOC mappings if available
        if onet_file:
            with open(onet_file, 'r') as f:
                self.soc_mappings = json.load(f)
    
    def calculate_industry_automation_rates(self):
        """
        Calculate weighted automation rates for each industry based on 
        occupational composition and Anthropic impact data.
        """
        industry_rates = {}
        
        for industry, occupations in self.occupation_employment.items():
            total_employment = sum(occ.get('employment', 0) for occ in occupations.values())
            
            if total_employment == 0:
                logger.warning(f"No employment data for industry: {industry}")
                continue
            
            weighted_automation = 0
            weighted_augmentation = 0
            coverage = 0  # Track how much of industry employment we have impact data for
            
            for soc_code, occ_data in occupations.items():
                employment = occ_data.get('employment', 0)
                employment_share = employment / total_employment
                
                # Get AI impact data for this occupation
                impact_data = self.occupation_impacts.get(soc_code)
                
                if impact_data:
                    weighted_automation += impact_data['automation_rate'] * employment_share
                    weighted_augmentation += impact_data['augmentation_rate'] * employment_share
                    coverage += employment_share
                else:
                    # Handle missing data - use fallback estimation
                    fallback_rates = self.estimate_missing_occupation_rates(soc_code, occ_data)
                    weighted_automation += fallback_rates['automation'] * employment_share
                    weighted_augmentation += fallback_rates['augmentation'] * employment_share
            
            industry_rates[industry] = {
                'automation_rate': weighted_automation,
                'augmentation_rate': weighted_augmentation,
                'data_coverage': coverage,
                'total_employment': total_employment,
                'confidence': self.calculate_confidence_score(coverage, industry)
            }
        
        return industry_rates
    
    def estimate_missing_occupation_rates(self, soc_code, occ_data):
        """
        Estimate automation/augmentation rates for occupations not in Anthropic data.
        Use SOC code patterns and occupation characteristics.
        """
        # Extract major SOC group (first 2 digits)
        major_group = soc_code[:2] if soc_code else "99"
        
        # Default rates by major occupational group
        soc_group_defaults = {
            "11": {"automation": 0.15, "augmentation": 0.60},  # Management
            "13": {"automation": 0.35, "augmentation": 0.70},  # Business/Financial
            "15": {"automation": 0.25, "augmentation": 0.80},  # Computer/Math
            "17": {"automation": 0.30, "augmentation": 0.65},  # Architecture/Engineering
            "19": {"automation": 0.20, "augmentation": 0.75},  # Life/Physical Sciences
            "21": {"automation": 0.40, "augmentation": 0.50},  # Community/Social Services
            "23": {"automation": 0.25, "augmentation": 0.60},  # Legal
            "25": {"automation": 0.20, "augmentation": 0.70},  # Education
            "27": {"automation": 0.15, "augmentation": 0.65},  # Arts/Media
            "29": {"automation": 0.30, "augmentation": 0.70},  # Healthcare Practitioners
            "31": {"automation": 0.45, "augmentation": 0.40},  # Healthcare Support
            "33": {"automation": 0.50, "augmentation": 0.35},  # Protective Service
            "35": {"automation": 0.60, "augmentation": 0.25},  # Food Preparation
            "37": {"automation": 0.55, "augmentation": 0.30},  # Building Maintenance
            "39": {"automation": 0.50, "augmentation": 0.40},  # Personal Care
            "41": {"automation": 0.65, "augmentation": 0.30},  # Sales
            "43": {"automation": 0.70, "augmentation": 0.25},  # Office/Administrative
            "45": {"automation": 0.60, "augmentation": 0.20},  # Farming/Fishing
            "47": {"automation": 0.45, "augmentation": 0.35},  # Construction
            "49": {"automation": 0.50, "augmentation": 0.40},  # Installation/Maintenance
            "51": {"automation": 0.75, "augmentation": 0.20},  # Production
            "53": {"automation": 0.80, "augmentation": 0.15},  # Transportation
        }
        
        return soc_group_defaults.get(major_group, {"automation": 0.45, "augmentation": 0.35})
    
    def calculate_confidence_score(self, data_coverage, industry):
        """Calculate confidence score for industry-level estimates."""
        base_confidence = min(data_coverage, 1.0)  # Coverage-based component
        
        # Industry-specific adjustments
        industry_confidence_factors = {
            "Information": 1.2,  # High confidence - well-studied
            "Professional and Business Services": 1.1,
            "Financial Activities": 1.1,
            "Manufacturing": 1.0,
            "Healthcare": 0.9,  # Medium confidence - complex dynamics
            "Education": 0.8,
            "Government": 0.7,  # Lower confidence - limited data
            "Agriculture": 0.6
        }
        
        industry_factor = industry_confidence_factors.get(industry, 1.0)
        return min(base_confidence * industry_factor, 1.0)
```

#### 2.2 Updated Impact Calculator Integration
**File:** `scripts/analysis/calculate_ai_impact.py` (modifications)

```python
# Replace the existing calculate_displacement_effect method

def calculate_displacement_effect(self, industry_data, anthropic_data):
    """
    Calculate displacement effect using proper occupation-to-industry mapping.
    """
    # Initialize the mapper
    mapper = OccupationIndustryMapper()
    
    # Load data sources
    anthropic_file = os.path.join(self.input_dir, "anthropic_economic_index_latest.json")
    bls_occupation_file = os.path.join(self.input_dir, "bls_occupation_employment.json")
    
    try:
        mapper.load_data_sources(anthropic_file, bls_occupation_file)
        industry_rates = mapper.calculate_industry_automation_rates()
    except FileNotFoundError as e:
        logger.warning(f"Occupation mapping data not available: {e}")
        logger.warning("Falling back to simplified calculation")
        return self.calculate_displacement_effect_fallback(industry_data, anthropic_data)
    
    # Calculate displacement effects using mapped rates
    displacement_effects = {}
    
    for industry, data in industry_data.items():
        # Get industry-specific rates from mapping
        rates = industry_rates.get(industry, {})
        auto_rate = rates.get('automation_rate', 0.30)  # Default if not found
        aug_rate = rates.get('augmentation_rate', 0.70)
        confidence = rates.get('confidence', 0.5)
        
        # Calculate displacement components
        pure_automation = self.calculate_pure_automation_impact(auto_rate * 100, industry)
        capacity_augmentation = self.calculate_capacity_augmentation_impact(aug_rate * 100, industry)
        
        # Total displacement effect
        displacement_effect = pure_automation + capacity_augmentation
        displacement_effect = np.clip(displacement_effect, 0, 0.8)
        
        displacement_effects[industry] = {
            "effect": displacement_effect,
            "confidence": confidence,
            "data_coverage": rates.get('data_coverage', 0.5),
            "components": {
                "pure_automation": pure_automation,
                "capacity_augmentation": capacity_augmentation,
                "automation_rate": auto_rate,
                "augmentation_rate": aug_rate
            }
        }
    
    # Calculate weighted average
    total_employment = sum(data.get("current", 0) for data in industry_data.values())
    if total_employment > 0:
        weighted_displacement = sum(
            displacement_effects[industry]["effect"] * industry_data[industry].get("current", 0)
            for industry in displacement_effects
        ) / total_employment
    else:
        weighted_displacement = np.mean([data["effect"] for data in displacement_effects.values()])
    
    return weighted_displacement, displacement_effects
```

### Phase 3: Data Collection Scripts

#### 3.1 BLS OEWS Data Collector
**File:** `scripts/collection/collect_bls_occupation_employment.py`

```python
import requests
import json
import time
from datetime import datetime

class BLSOccupationEmploymentCollector:
    """
    Collects Occupational Employment and Wage Statistics (OEWS) from BLS.
    Provides employment by detailed occupation within each industry.
    """
    
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        
        # Industry codes mapping (NAICS to friendly names)
        self.industry_mapping = {
            "000000": "Total Nonfarm",
            "510000": "Information", 
            "520000": "Finance and Insurance",
            "540000": "Professional, Scientific, and Technical Services",
            "620000": "Health Care and Social Assistance",
            "310000": "Manufacturing",
            "440000": "Retail Trade",
            "720000": "Accommodation and Food Services",
            "920000": "Public Administration"
        }
        
        # Major occupation groups (SOC codes)
        self.occupation_groups = [
            "000000",  # All occupations
            "110000",  # Management
            "130000",  # Business and Financial Operations
            "150000",  # Computer and Mathematical
            "170000",  # Architecture and Engineering
            "190000",  # Life, Physical, and Social Science
            "210000",  # Community and Social Service
            "230000",  # Legal
            "250000",  # Educational Instruction and Library
            "270000",  # Arts, Design, Entertainment, Sports, and Media
            "290000",  # Healthcare Practitioners and Technical
            "310000",  # Healthcare Support
            "330000",  # Protective Service
            "350000",  # Food Preparation and Serving Related
            "370000",  # Building and Grounds Cleaning and Maintenance
            "390000",  # Personal Care and Service
            "410000",  # Sales and Related
            "430000",  # Office and Administrative Support
            "450000",  # Farming, Fishing, and Forestry
            "470000",  # Construction and Extraction
            "490000",  # Installation, Maintenance, and Repair
            "510000",  # Production
            "530000",  # Transportation and Material Moving
        ]
    
    def collect_employment_matrix(self, year=None):
        """
        Collect employment matrix: industries × occupations.
        Returns nested dictionary with employment counts.
        """
        if not year:
            year = datetime.now().year - 1  # Use previous year (most recent complete data)
        
        employment_matrix = {}
        
        for industry_code, industry_name in self.industry_mapping.items():
            logger.info(f"Collecting occupation data for {industry_name}")
            
            industry_occupations = {}
            
            for occ_group in self.occupation_groups:
                # Build BLS series ID: OEUS + Area + Industry + Occupation + DataType
                # OEUS000000 + 540000 + 150000 + 01 (National, Prof Services, Computer, Employment)
                series_id = f"OEUS000000{industry_code}{occ_group}01"
                
                try:
                    data = self.fetch_bls_series(series_id, year)
                    if data:
                        employment = data.get('value', 0)
                        if employment > 0:  # Only include occupations with employment
                            industry_occupations[occ_group] = {
                                'employment': employment,
                                'soc_code': self.format_soc_code(occ_group),
                                'title': self.get_occupation_title(occ_group),
                                'series_id': series_id
                            }
                except Exception as e:
                    logger.warning(f"Error fetching {series_id}: {e}")
                
                # Rate limiting
                time.sleep(0.1)
            
            employment_matrix[industry_name] = industry_occupations
        
        return employment_matrix
    
    def fetch_bls_series(self, series_id, year):
        """Fetch specific BLS time series data."""
        headers = {'Content-type': 'application/json'}
        
        data = {
            'seriesid': [series_id],
            'startyear': str(year),
            'endyear': str(year),
            'registrationkey': self.api_key
        }
        
        response = requests.post(self.base_url, data=json.dumps(data), headers=headers)
        
        if response.status_code == 200:
            json_data = response.json()
            
            if json_data['status'] == 'REQUEST_SUCCEEDED':
                series_data = json_data['Results']['series'][0]['data']
                if series_data:
                    # Return most recent data point
                    return {
                        'value': float(series_data[0]['value'].replace(',', '')),
                        'year': series_data[0]['year'],
                        'period': series_data[0]['period']
                    }
        
        return None
    
    def format_soc_code(self, occ_code):
        """Convert 6-digit occupation code to standard SOC format."""
        if len(occ_code) == 6:
            return f"{occ_code[:2]}-{occ_code[2:]}"
        return occ_code
    
    def get_occupation_title(self, occ_code):
        """Get occupation title from SOC code."""
        # This would ideally load from O*NET or BLS occupation definitions
        occupation_titles = {
            "000000": "All Occupations",
            "110000": "Management Occupations", 
            "130000": "Business and Financial Operations Occupations",
            "150000": "Computer and Mathematical Occupations",
            "170000": "Architecture and Engineering Occupations",
            # ... complete mapping
        }
        return occupation_titles.get(occ_code, f"Occupation {occ_code}")

def main():
    """Main execution function."""
    collector = BLSOccupationEmploymentCollector()
    
    # Collect employment matrix
    employment_data = collector.collect_employment_matrix()
    
    # Save to file
    output_file = f"data/processed/bls_occupation_employment_{datetime.now().strftime('%Y%m')}.json"
    
    with open(output_file, 'w') as f:
        json.dump(employment_data, f, indent=2)
    
    logger.info(f"Saved occupation employment data to {output_file}")

if __name__ == "__main__":
    main()
```

### Phase 4: Validation and Testing

#### 4.1 Validation Framework
**File:** `scripts/validation/validate_occupation_mapping.py`

```python
def validate_mapping_quality(industry_rates, employment_matrix):
    """
    Validate the quality of occupation-to-industry mapping.
    """
    validation_results = {}
    
    for industry, rates in industry_rates.items():
        coverage = rates.get('data_coverage', 0)
        confidence = rates.get('confidence', 0)
        total_employment = rates.get('total_employment', 0)
        
        # Validation checks
        checks = {
            'sufficient_coverage': coverage >= 0.7,  # At least 70% coverage
            'reasonable_automation': 0.1 <= rates.get('automation_rate', 0) <= 0.9,
            'reasonable_augmentation': 0.1 <= rates.get('augmentation_rate', 0) <= 0.9,
            'adequate_employment': total_employment >= 1000,  # Minimum employment threshold
            'rates_sum_reasonable': 0.5 <= (rates.get('automation_rate', 0) + rates.get('augmentation_rate', 0)) <= 1.5
        }
        
        validation_results[industry] = {
            'checks': checks,
            'overall_quality': sum(checks.values()) / len(checks),
            'coverage': coverage,
            'confidence': confidence
        }
    
    return validation_results

def compare_with_baseline(new_rates, baseline_rates):
    """
    Compare new occupation-mapped rates with baseline uniform rates.
    """
    comparison = {}
    
    for industry in new_rates:
        if industry in baseline_rates:
            new_auto = new_rates[industry].get('automation_rate', 0)
            baseline_auto = baseline_rates[industry].get('automation_rate', 0)
            
            comparison[industry] = {
                'automation_change': new_auto - baseline_auto,
                'augmentation_change': (new_rates[industry].get('augmentation_rate', 0) - 
                                      baseline_rates[industry].get('augmentation_rate', 0)),
                'improvement_score': new_rates[industry].get('confidence', 0)
            }
    
    return comparison
```

#### 4.2 Integration Testing
**File:** `tests/test_occupation_mapping.py`

```python
import unittest
from scripts.analysis.occupation_industry_mapper import OccupationIndustryMapper

class TestOccupationMapping(unittest.TestCase):
    
    def setUp(self):
        self.mapper = OccupationIndustryMapper()
        
        # Mock data for testing
        self.mock_anthropic_data = {
            "15-1252": {"automation_rate": 0.25, "augmentation_rate": 0.75},
            "43-3031": {"automation_rate": 0.80, "augmentation_rate": 0.20}
        }
        
        self.mock_employment_data = {
            "Information": {
                "15-1252": {"employment": 100000, "title": "Software Developers"},
                "43-3031": {"employment": 20000, "title": "Bookkeeping Clerks"}
            }
        }
    
    def test_industry_rate_calculation(self):
        """Test that industry rates are correctly weighted by employment."""
        self.mapper.occupation_impacts = self.mock_anthropic_data
        self.mapper.occupation_employment = self.mock_employment_data
        
        rates = self.mapper.calculate_industry_automation_rates()
        
        # Expected: (0.25 * 100k + 0.80 * 20k) / 120k = 0.375
        expected_automation = (0.25 * 100000 + 0.80 * 20000) / 120000
        
        self.assertAlmostEqual(
            rates["Information"]["automation_rate"], 
            expected_automation, 
            places=3
        )
    
    def test_fallback_estimation(self):
        """Test fallback estimation for missing occupations."""
        # Test SOC group-based estimation
        fallback = self.mapper.estimate_missing_occupation_rates("15-9999", {})
        
        # Should use defaults for Computer/Math occupations (15-xxxx)
        self.assertEqual(fallback['automation'], 0.25)
        self.assertEqual(fallback['augmentation'], 0.80)
    
    def test_confidence_calculation(self):
        """Test confidence score calculation."""
        confidence = self.mapper.calculate_confidence_score(0.8, "Information")
        
        # Information industry should have high confidence multiplier
        self.assertGreater(confidence, 0.8)

if __name__ == '__main__':
    unittest.main()
```

### Phase 5: Migration and Deployment

#### 5.1 Migration Script
**File:** `scripts/migration/migrate_to_occupation_mapping.py`

```python
def migrate_methodology():
    """
    Migrate from uniform rates to occupation-based mapping.
    """
    logger.info("Starting migration to occupation-based methodology")
    
    # 1. Backup existing calculations
    backup_existing_results()
    
    # 2. Collect new data sources
    collect_bls_occupation_data()
    
    # 3. Process Anthropic data with SOC mapping
    process_anthropic_with_soc_codes()
    
    # 4. Run new calculation methodology
    run_updated_calculations()
    
    # 5. Validate results
    validate_migration_results()
    
    logger.info("Migration completed successfully")

def backup_existing_results():
    """Backup current results before migration."""
    import shutil
    from datetime import datetime
    
    backup_dir = f"data/backup/pre_occupation_mapping_{datetime.now().strftime('%Y%m%d')}"
    os.makedirs(backup_dir, exist_ok=True)
    
    # Backup key result files
    files_to_backup = [
        "ai_labor_impact_latest.json",
        "methodology_comparison_*.json"
    ]
    
    for pattern in files_to_backup:
        for file in glob.glob(f"data/processed/{pattern}"):
            shutil.copy2(file, backup_dir)
    
    logger.info(f"Backed up existing results to {backup_dir}")
```

#### 5.2 Updated Workflow Integration
**File:** `scripts/run_updated_ai_impact.py` (modifications)

```python
# Add occupation mapping step to the workflow

def run_calculation_workflow(year, month, simulate=False):
    """Updated workflow with occupation mapping."""
    
    # Existing steps...
    collect_employment_data(year, month)
    collect_anthropic_data()
    
    # NEW: Collect occupation employment data
    if not simulate:
        logger.info("Collecting BLS occupation employment data...")
        collect_bls_occupation_employment()
    
    # NEW: Process occupation mapping
    logger.info("Processing occupation-to-industry mapping...")
    process_occupation_mapping()
    
    # Continue with existing calculation...
    calculate_ai_impact(year, month)
```

## Expected Outcomes

### Quantitative Improvements
- **Industry differentiation**: Technology sector should show ~25% automation vs Healthcare at ~15%
- **Data coverage**: 70%+ of employment covered by Anthropic occupation data
- **Confidence scores**: Industry-specific confidence metrics (0.6-0.9 range)

### Qualitative Benefits
- **Defensible methodology**: Clear occupation-level foundation
- **Granular insights**: Industry impacts reflect actual workforce composition  
- **Policy relevance**: Can trace impacts to specific occupational groups
- **Research credibility**: Aligns with academic approaches in labor economics

## Implementation Timeline

1. **Week 1**: Implement data collection scripts (BLS OEWS, Anthropic processing)
2. **Week 2**: Build occupation-industry mapper with validation
3. **Week 3**: Integrate with existing calculation pipeline  
4. **Week 4**: Testing, validation, and migration

This implementation will transform your methodology from a simplified average-based approach to a sophisticated, occupation-grounded analysis that properly leverages the granular insights available in Anthropic's Economic Index.