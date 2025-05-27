#!/usr/bin/env python3
# scripts/collection/collect_bls_occupation_employment.py
"""
BLS Occupational Employment and Wage Statistics (OEWS) Data Collector
Collects employment by detailed occupation within each industry for occupation-industry mapping.
"""
import requests
import json
import time
import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.soc_code_mapper import SOCCodeMapper

logger = logging.getLogger(__name__)

class BLSOccupationEmploymentCollector:
    """
    Collects Occupational Employment and Wage Statistics (OEWS) from BLS.
    Provides employment by detailed occupation within each industry.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('BLS_API_KEY')
        self.base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        self.soc_mapper = SOCCodeMapper()
        
        # Rate limiting settings
        self.request_delay = 0.5  # Seconds between requests
        self.max_requests_per_minute = 25 if self.api_key else 10
        
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
            "920000": "Public Administration",
            "230000": "Construction",
            "710000": "Arts, Entertainment, and Recreation",
            "810000": "Other Services"
        }
        
        # Priority occupation groups (most relevant for AI impact analysis)
        self.priority_occupation_groups = [
            "110000",  # Management
            "130000",  # Business and Financial Operations
            "150000",  # Computer and Mathematical
            "170000",  # Architecture and Engineering
            "250000",  # Educational Instruction and Library
            "290000",  # Healthcare Practitioners and Technical
            "410000",  # Sales and Related
            "430000",  # Office and Administrative Support
        ]
        
        # All major occupation groups
        self.all_occupation_groups = [
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

    def collect_employment_matrix(self, year: Optional[int] = None, priority_only: bool = False) -> Dict[str, Any]:
        """
        Collect employment matrix: industries × occupations.
        
        Args:
            year: Year to collect data for (defaults to most recent)
            priority_only: If True, only collect priority occupation groups
            
        Returns:
            Nested dictionary with employment counts and metadata
        """
        if not year:
            year = datetime.now().year - 1  # Use previous year (most recent complete data)
        
        logger.info(f"Collecting BLS OEWS data for year {year}")
        
        occupation_groups = self.priority_occupation_groups if priority_only else self.all_occupation_groups
        
        employment_matrix = {}
        collection_stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "industries_collected": 0,
            "occupations_per_industry": {},
            "collection_errors": []
        }
        
        for industry_code, industry_name in self.industry_mapping.items():
            logger.info(f"Collecting occupation data for {industry_name}")
            
            industry_occupations = {}
            industry_request_count = 0
            
            for occ_group in occupation_groups:
                try:
                    # Build BLS series ID: OEUS + Area + Industry + Occupation + DataType
                    # OEUS000000 + INDUSTRY + OCCUPATION + 01 (Employment)
                    series_id = f"OEUS000000{industry_code}{occ_group}01"
                    
                    data = self.fetch_bls_series(series_id, year)
                    collection_stats["total_requests"] += 1
                    industry_request_count += 1
                    
                    if data and data.get('value', 0) > 0:  # Only include occupations with employment
                        soc_code = self.soc_mapper.standardize_soc_code(occ_group)
                        
                        industry_occupations[soc_code or occ_group] = {
                            'employment': data['value'],
                            'soc_code': soc_code,
                            'title': self.get_occupation_title(occ_group),
                            'series_id': series_id,
                            'year': data['year'],
                            'period': data['period']
                        }
                        
                        collection_stats["successful_requests"] += 1
                    else:
                        collection_stats["failed_requests"] += 1
                
                except Exception as e:
                    logger.warning(f"Error fetching {series_id}: {e}")
                    collection_stats["failed_requests"] += 1
                    collection_stats["collection_errors"].append(f"{series_id}: {str(e)}")
                
                # Rate limiting
                time.sleep(self.request_delay)
                
                # Check if we're approaching rate limits
                if industry_request_count % 10 == 0:
                    logger.debug(f"Processed {industry_request_count} occupation requests for {industry_name}")
            
            if industry_occupations:
                employment_matrix[industry_name] = industry_occupations
                collection_stats["industries_collected"] += 1
                collection_stats["occupations_per_industry"][industry_name] = len(industry_occupations)
                logger.info(f"Collected {len(industry_occupations)} occupations for {industry_name}")
            else:
                logger.warning(f"No occupation data collected for {industry_name}")
        
        # Create final result with metadata
        result = {
            "employment_matrix": employment_matrix,
            "metadata": {
                "collection_date": datetime.now().isoformat(),
                "data_year": year,
                "collection_stats": collection_stats,
                "data_source": "BLS OEWS",
                "priority_only": priority_only,
                "api_key_used": self.api_key is not None
            }
        }
        
        self._log_collection_summary(collection_stats)
        
        return result

    def fetch_bls_series(self, series_id: str, year: int) -> Optional[Dict[str, Any]]:
        """
        Fetch specific BLS time series data.
        
        Args:
            series_id: BLS series identifier
            year: Year to fetch data for
            
        Returns:
            Dictionary with employment data or None if failed
        """
        headers = {'Content-type': 'application/json'}
        
        data_payload = {
            'seriesid': [series_id],
            'startyear': str(year),
            'endyear': str(year)
        }
        
        # Add API key if available
        if self.api_key:
            data_payload['registrationkey'] = self.api_key
        
        try:
            response = requests.post(
                self.base_url, 
                data=json.dumps(data_payload), 
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                json_data = response.json()
                
                if json_data.get('status') == 'REQUEST_SUCCEEDED':
                    series_data = json_data.get('Results', {}).get('series', [])
                    
                    if series_data and series_data[0].get('data'):
                        # Return most recent data point
                        latest_data = series_data[0]['data'][0]
                        
                        # Clean and convert employment value
                        employment_str = latest_data.get('value', '0')
                        if employment_str and employment_str != '-':
                            try:
                                employment = int(float(employment_str.replace(',', '')))
                                return {
                                    'value': employment,
                                    'year': latest_data.get('year'),
                                    'period': latest_data.get('period')
                                }
                            except (ValueError, TypeError):
                                logger.warning(f"Could not parse employment value: {employment_str}")
                                return None
                else:
                    logger.warning(f"BLS API error for {series_id}: {json_data.get('message', 'Unknown error')}")
            else:
                logger.warning(f"HTTP error {response.status_code} for series {series_id}")
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error for {series_id}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error for {series_id}: {e}")
        
        return None

    def get_occupation_title(self, occ_code: str) -> str:
        """
        Get occupation title from SOC code.
        In a full implementation, this would load from O*NET or BLS occupation definitions.
        """
        occupation_titles = {
            "000000": "All Occupations",
            "110000": "Management Occupations", 
            "130000": "Business and Financial Operations Occupations",
            "150000": "Computer and Mathematical Occupations",
            "170000": "Architecture and Engineering Occupations",
            "190000": "Life, Physical, and Social Science Occupations",
            "210000": "Community and Social Service Occupations",
            "230000": "Legal Occupations",
            "250000": "Educational Instruction and Library Occupations",
            "270000": "Arts, Design, Entertainment, Sports, and Media Occupations",
            "290000": "Healthcare Practitioners and Technical Occupations",
            "310000": "Healthcare Support Occupations",
            "330000": "Protective Service Occupations",
            "350000": "Food Preparation and Serving Related Occupations",
            "370000": "Building and Grounds Cleaning and Maintenance Occupations",
            "390000": "Personal Care and Service Occupations",
            "410000": "Sales and Related Occupations",
            "430000": "Office and Administrative Support Occupations",
            "450000": "Farming, Fishing, and Forestry Occupations",
            "470000": "Construction and Extraction Occupations",
            "490000": "Installation, Maintenance, and Repair Occupations",
            "510000": "Production Occupations",
            "530000": "Transportation and Material Moving Occupations"
        }
        
        return occupation_titles.get(occ_code, f"Occupation {occ_code}")

    def _log_collection_summary(self, stats: Dict[str, Any]):
        """Log summary of data collection results."""
        logger.info("=== BLS OEWS COLLECTION SUMMARY ===")
        logger.info(f"Total API requests: {stats['total_requests']}")
        logger.info(f"Successful requests: {stats['successful_requests']}")
        logger.info(f"Failed requests: {stats['failed_requests']}")
        logger.info(f"Industries collected: {stats['industries_collected']}")
        
        if stats['successful_requests'] > 0:
            success_rate = stats['successful_requests'] / stats['total_requests']
            logger.info(f"Success rate: {success_rate:.1%}")
        
        # Log industries with good data coverage
        good_coverage_industries = [
            industry for industry, count in stats['occupations_per_industry'].items()
            if count >= 5
        ]
        
        if good_coverage_industries:
            logger.info(f"Industries with good coverage (5+ occupations): {len(good_coverage_industries)}")
            for industry in good_coverage_industries[:5]:  # Show top 5
                count = stats['occupations_per_industry'][industry]
                logger.info(f"  {industry}: {count} occupations")
        
        # Log any critical errors
        if stats['collection_errors']:
            logger.warning(f"Collection errors encountered: {len(stats['collection_errors'])}")
            for error in stats['collection_errors'][:3]:  # Show first 3 errors
                logger.warning(f"  {error}")

    def save_employment_data(self, employment_data: Dict[str, Any], output_file: str):
        """Save employment data to file."""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(employment_data, f, indent=2)
        
        logger.info(f"Saved BLS occupation employment data to {output_file}")

    def validate_employment_data(self, employment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate collected employment data for quality and completeness."""
        validation = {
            "validation_passed": True,
            "warnings": [],
            "errors": [],
            "quality_metrics": {}
        }
        
        employment_matrix = employment_data.get("employment_matrix", {})
        
        if not employment_matrix:
            validation["errors"].append("No employment data collected")
            validation["validation_passed"] = False
            return validation
        
        # Check industry coverage
        expected_industries = len(self.industry_mapping)
        actual_industries = len(employment_matrix)
        coverage_ratio = actual_industries / expected_industries
        
        validation["quality_metrics"]["industry_coverage"] = coverage_ratio
        
        if coverage_ratio < 0.5:
            validation["warnings"].append(f"Low industry coverage: {coverage_ratio:.1%}")
        
        # Check occupation coverage per industry
        occupation_counts = []
        for industry, occupations in employment_matrix.items():
            count = len(occupations)
            occupation_counts.append(count)
            
            if count < 3:
                validation["warnings"].append(f"Low occupation coverage for {industry}: {count} occupations")
        
        if occupation_counts:
            validation["quality_metrics"]["avg_occupations_per_industry"] = sum(occupation_counts) / len(occupation_counts)
            validation["quality_metrics"]["min_occupations_per_industry"] = min(occupation_counts)
            validation["quality_metrics"]["max_occupations_per_industry"] = max(occupation_counts)
        
        # Check employment values
        total_employment = 0
        zero_employment_count = 0
        
        for industry, occupations in employment_matrix.items():
            for soc_code, occ_data in occupations.items():
                employment = occ_data.get('employment', 0)
                total_employment += employment
                
                if employment <= 0:
                    zero_employment_count += 1
        
        validation["quality_metrics"]["total_employment"] = total_employment
        validation["quality_metrics"]["zero_employment_entries"] = zero_employment_count
        
        if total_employment < 100000:  # Sanity check
            validation["warnings"].append(f"Suspiciously low total employment: {total_employment:,}")
        
        return validation


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect BLS occupation employment data')
    parser.add_argument('--output-dir', default='./data/processed', help='Output directory')
    parser.add_argument('--year', type=int, help='Year to collect data for (default: previous year)')
    parser.add_argument('--priority-only', action='store_true', help='Collect only priority occupation groups')
    parser.add_argument('--api-key', help='BLS API key (or set BLS_API_KEY environment variable)')
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = BLSOccupationEmploymentCollector(api_key=args.api_key)
    
    # Collect employment matrix
    logger.info("Starting BLS OEWS data collection...")
    employment_data = collector.collect_employment_matrix(
        year=args.year,
        priority_only=args.priority_only
    )
    
    # Validate data
    validation = collector.validate_employment_data(employment_data)
    
    if validation["warnings"]:
        for warning in validation["warnings"]:
            logger.warning(warning)
    
    if validation["errors"]:
        for error in validation["errors"]:
            logger.error(error)
    
    quality_metrics = validation.get("quality_metrics", {})
    if quality_metrics:
        logger.info(f"Data quality metrics:")
        for metric, value in quality_metrics.items():
            logger.info(f"  {metric}: {value}")
    
    # Save data
    year_str = args.year or datetime.now().year - 1
    suffix = "_priority" if args.priority_only else ""
    output_file = os.path.join(args.output_dir, f"bls_occupation_employment_{year_str}{suffix}.json")
    
    collector.save_employment_data(employment_data, output_file)
    
    # Also save as latest
    latest_file = os.path.join(args.output_dir, f"bls_occupation_employment_latest{suffix}.json")
    collector.save_employment_data(employment_data, latest_file)
    
    logger.info("BLS OEWS data collection completed successfully")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())