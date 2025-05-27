#!/usr/bin/env python3
# scripts/analysis/occupation_industry_mapper.py
"""
Occupation-Industry Mapper
Maps occupation-level AI impacts to industry-level aggregated impacts using
employment-weighted averages across detailed occupational data.
"""
import json
import logging
import os
import sys
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.soc_code_mapper import SOCCodeMapper
from processing.process_anthropic_occupation_data import AnthropicOccupationProcessor

logger = logging.getLogger(__name__)

class OccupationIndustryMapper:
    """
    Maps occupation-level AI impacts to industry-level aggregated impacts
    using detailed employment data from BLS and impact data from Anthropic.
    """
    
    def __init__(self, input_dir: str = "./data/processed"):
        self.input_dir = input_dir
        self.soc_mapper = SOCCodeMapper()
        self.anthropic_processor = AnthropicOccupationProcessor()
        
        # Data containers
        self.occupation_impacts = {}  # From Anthropic data
        self.occupation_employment = {}  # From BLS OEWS
        self.industry_rates = {}  # Calculated results
        
        # Processing metadata
        self.mapping_metadata = {
            "calculation_method": "occupation_weighted",
            "data_sources": {},
            "coverage_stats": {},
            "confidence_scores": {}
        }

    def load_data_sources(self, 
                         anthropic_file: Optional[str] = None,
                         bls_file: Optional[str] = None,
                         auto_discover: bool = True) -> bool:
        """
        Load all required data sources for occupation-industry mapping.
        
        Args:
            anthropic_file: Path to Anthropic occupation data
            bls_file: Path to BLS occupation employment data
            auto_discover: Whether to auto-discover files if not specified
            
        Returns:
            True if successfully loaded, False otherwise
        """
        logger.info("Loading data sources for occupation-industry mapping...")
        
        success = True
        
        # Load Anthropic occupation impacts
        if anthropic_file or auto_discover:
            success &= self._load_anthropic_data(anthropic_file)
        
        # Load BLS occupation employment data
        if bls_file or auto_discover:
            success &= self._load_bls_employment_data(bls_file)
        
        # Log data loading summary
        self._log_data_loading_summary()
        
        return success

    def _load_anthropic_data(self, anthropic_file: Optional[str] = None) -> bool:
        """Load Anthropic occupation impact data."""
        # Auto-discover Anthropic file if not specified
        if not anthropic_file:
            possible_files = [
                "anthropic_occupation_impacts.json",
                "anthropic_processed_occupations.json", 
                "job_trends_latest.json",  # May contain Anthropic data
                "anthropic_economic_index_latest.json"
            ]
            
            for filename in possible_files:
                filepath = os.path.join(self.input_dir, filename)
                if os.path.exists(filepath):
                    anthropic_file = filepath
                    break
        
        if not anthropic_file or not os.path.exists(anthropic_file):
            logger.warning("No Anthropic occupation data file found - will use SOC defaults")
            self.mapping_metadata["data_sources"]["anthropic"] = "not_available"
            return False
        
        try:
            with open(anthropic_file, 'r') as f:
                anthropic_data = json.load(f)
            
            # Check if this is raw or processed Anthropic data
            if "occupation_impacts" in anthropic_data:
                # Already processed
                self.occupation_impacts = anthropic_data["occupation_impacts"]
                logger.info(f"Loaded {len(self.occupation_impacts)} processed occupations from {anthropic_file}")
            else:
                # Raw data - needs processing
                logger.info("Processing raw Anthropic data...")
                processed = self.anthropic_processor.process_anthropic_data(anthropic_data)
                self.occupation_impacts = processed.get("occupation_impacts", {})
                logger.info(f"Processed {len(self.occupation_impacts)} occupations from raw data")
            
            self.mapping_metadata["data_sources"]["anthropic"] = {
                "file": anthropic_file,
                "occupations_count": len(self.occupation_impacts),
                "loaded_at": datetime.now().isoformat()
            }
            
            return len(self.occupation_impacts) > 0
            
        except Exception as e:
            logger.error(f"Error loading Anthropic data from {anthropic_file}: {e}")
            self.mapping_metadata["data_sources"]["anthropic"] = f"error: {str(e)}"
            return False

    def _load_bls_employment_data(self, bls_file: Optional[str] = None) -> bool:
        """Load BLS occupation employment data."""
        # Auto-discover BLS file if not specified
        if not bls_file:
            possible_files = [
                "bls_occupation_employment.json",
                "bls_occupation_employment_latest.json",
                "occupation_employment_matrix.json"
            ]
            
            for filename in possible_files:
                filepath = os.path.join(self.input_dir, filename)
                if os.path.exists(filepath):
                    bls_file = filepath
                    break
        
        if not bls_file or not os.path.exists(bls_file):
            logger.warning("No BLS occupation employment data found - will use estimated employment shares")
            self.mapping_metadata["data_sources"]["bls_employment"] = "not_available"
            return self._create_estimated_employment_data()
        
        try:
            with open(bls_file, 'r') as f:
                self.occupation_employment = json.load(f)
            
            logger.info(f"Loaded BLS occupation employment data from {bls_file}")
            logger.info(f"Industries covered: {list(self.occupation_employment.keys())}")
            
            self.mapping_metadata["data_sources"]["bls_employment"] = {
                "file": bls_file,
                "industries_count": len(self.occupation_employment),
                "loaded_at": datetime.now().isoformat()
            }
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading BLS employment data from {bls_file}: {e}")
            self.mapping_metadata["data_sources"]["bls_employment"] = f"error: {str(e)}"
            return self._create_estimated_employment_data()

    def _create_estimated_employment_data(self) -> bool:
        """Create estimated employment data based on typical industry compositions."""
        logger.info("Creating estimated employment shares based on typical industry compositions...")
        
        # Typical occupation distributions by industry (simplified)
        # In a real implementation, this would be based on historical BLS data
        estimated_employment = {
            "Information": {
                "15-1252": {"employment": 180000, "title": "Software Developers"},  # High concentration
                "15-1151": {"employment": 45000, "title": "Computer User Support Specialists"},
                "11-3021": {"employment": 25000, "title": "Computer and Information Systems Managers"},
                "43-3031": {"employment": 30000, "title": "Bookkeeping, Accounting, and Auditing Clerks"},
                "41-3099": {"employment": 20000, "title": "Sales Representatives, Services"}
            },
            "Professional and Business Services": {
                "13-2051": {"employment": 95000, "title": "Financial Analysts"},
                "13-1161": {"employment": 75000, "title": "Market Research Analysts"},
                "11-1021": {"employment": 180000, "title": "General and Operations Managers"},
                "43-3031": {"employment": 120000, "title": "Bookkeeping, Accounting, and Auditing Clerks"},
                "43-4051": {"employment": 200000, "title": "Customer Service Representatives"}
            },
            "Financial Activities": {
                "13-2011": {"employment": 140000, "title": "Accountants and Auditors"},
                "13-2051": {"employment": 110000, "title": "Financial Analysts"},
                "43-3031": {"employment": 180000, "title": "Bookkeeping, Accounting, and Auditing Clerks"},
                "41-3031": {"employment": 95000, "title": "Securities, Commodities, and Financial Services Sales Agents"},
                "43-4131": {"employment": 75000, "title": "Loan Interviewers and Clerks"}
            },
            "Education and Health Services": {
                "29-1141": {"employment": 320000, "title": "Registered Nurses"},
                "25-2021": {"employment": 380000, "title": "Elementary School Teachers"},
                "25-2031": {"employment": 180000, "title": "Secondary School Teachers"},
                "31-1014": {"employment": 140000, "title": "Nursing Assistants"},
                "43-3031": {"employment": 85000, "title": "Bookkeeping, Accounting, and Auditing Clerks"}
            },
            "Manufacturing": {
                "51-2092": {"employment": 180000, "title": "Team Assemblers"},
                "17-2112": {"employment": 65000, "title": "Industrial Engineers"},
                "11-1021": {"employment": 85000, "title": "General and Operations Managers"},
                "51-4121": {"employment": 95000, "title": "Welders, Cutters, Solderers, and Brazers"},
                "43-3031": {"employment": 75000, "title": "Bookkeeping, Accounting, and Auditing Clerks"}
            },
            "Trade, Transportation, and Utilities": {
                "41-2031": {"employment": 280000, "title": "Retail Salespersons"},
                "53-3032": {"employment": 190000, "title": "Heavy and Tractor-Trailer Truck Drivers"},
                "43-4051": {"employment": 160000, "title": "Customer Service Representatives"},
                "41-2011": {"employment": 120000, "title": "Cashiers"},
                "43-5061": {"employment": 95000, "title": "Production, Planning, and Expediting Clerks"}
            }
        }
        
        self.occupation_employment = estimated_employment
        
        self.mapping_metadata["data_sources"]["bls_employment"] = {
            "type": "estimated",
            "industries_count": len(estimated_employment),
            "created_at": datetime.now().isoformat(),
            "note": "Using estimated employment shares - recommend collecting actual BLS OEWS data"
        }
        
        return True

    def _log_data_loading_summary(self):
        """Log summary of data loading results."""
        logger.info("=== DATA LOADING SUMMARY ===")
        
        anthropic_source = self.mapping_metadata["data_sources"].get("anthropic", "not loaded")
        if isinstance(anthropic_source, dict):
            logger.info(f"Anthropic data: {anthropic_source['occupations_count']} occupations loaded")
        else:
            logger.info(f"Anthropic data: {anthropic_source}")
        
        bls_source = self.mapping_metadata["data_sources"].get("bls_employment", "not loaded")
        if isinstance(bls_source, dict):
            logger.info(f"BLS employment data: {bls_source['industries_count']} industries loaded")
            if bls_source.get("type") == "estimated":
                logger.warning("Using estimated employment data - collect actual BLS data for better accuracy")
        else:
            logger.info(f"BLS employment data: {bls_source}")

    def calculate_industry_automation_rates(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate weighted automation rates for each industry based on 
        occupational composition and Anthropic impact data.
        
        Returns:
            Dictionary with industry-level automation/augmentation rates
        """
        logger.info("Calculating industry automation rates using occupation-weighted methodology...")
        
        self.industry_rates = {}
        
        for industry, occupations in self.occupation_employment.items():
            industry_result = self._calculate_single_industry_rates(industry, occupations)
            self.industry_rates[industry] = industry_result
        
        # Calculate coverage statistics
        self._calculate_coverage_statistics()
        
        # Log results summary
        self._log_calculation_summary()
        
        return self.industry_rates

    def _calculate_single_industry_rates(self, industry: str, occupations: Dict) -> Dict[str, Any]:
        """Calculate automation rates for a single industry."""
        total_employment = sum(occ.get('employment', 0) for occ in occupations.values())
        
        if total_employment == 0:
            logger.warning(f"No employment data for industry: {industry}")
            return self._create_fallback_industry_rates(industry)
        
        weighted_automation = 0
        weighted_augmentation = 0
        coverage_employment = 0  # Employment covered by Anthropic data
        confidence_sum = 0
        confidence_count = 0
        
        # Process each occupation in the industry
        for soc_code, occ_data in occupations.items():
            employment = occ_data.get('employment', 0)
            employment_share = employment / total_employment
            
            # Get AI impact data for this occupation
            impact_data = self._get_occupation_impact_data(soc_code, occ_data)
            
            # Apply to weighted calculation
            weighted_automation += impact_data['automation_rate'] * employment_share
            weighted_augmentation += impact_data['augmentation_rate'] * employment_share
            
            # Track coverage and confidence
            if impact_data['source'] == 'anthropic_direct':
                coverage_employment += employment
                confidence_sum += impact_data['confidence']
                confidence_count += 1
            elif impact_data['source'] == 'soc_default':
                # Partial credit for SOC-based estimates
                coverage_employment += employment * 0.3
                confidence_sum += impact_data['confidence'] * 0.5
                confidence_count += 0.5
        
        # Calculate metrics
        data_coverage = coverage_employment / total_employment if total_employment > 0 else 0
        avg_confidence = confidence_sum / confidence_count if confidence_count > 0 else 0.5
        overall_confidence = self._calculate_industry_confidence(data_coverage, avg_confidence, industry)
        
        return {
            'automation_rate': weighted_automation,
            'augmentation_rate': weighted_augmentation,
            'data_coverage': data_coverage,
            'total_employment': total_employment,
            'confidence': overall_confidence,
            'avg_data_confidence': avg_confidence,
            'occupations_analyzed': len(occupations),
            'calculation_method': 'occupation_weighted'
        }

    def _get_occupation_impact_data(self, soc_code: str, occ_data: Dict) -> Dict[str, Any]:
        """Get AI impact data for a specific occupation."""
        # Standardize SOC code
        standardized_soc = self.soc_mapper.standardize_soc_code(soc_code)
        
        # Try direct match in Anthropic data
        if standardized_soc and standardized_soc in self.occupation_impacts:
            anthro_data = self.occupation_impacts[standardized_soc]
            return {
                'automation_rate': anthro_data['automation_rate'],
                'augmentation_rate': anthro_data['augmentation_rate'],
                'confidence': anthro_data.get('confidence', 0.7),
                'source': 'anthropic_direct',
                'title': anthro_data.get('title', occ_data.get('title', 'Unknown'))
            }
        
        # Fallback to SOC group defaults
        defaults = self.soc_mapper.get_ai_susceptibility_defaults(soc_code)
        return {
            'automation_rate': defaults['automation'],
            'augmentation_rate': defaults['augmentation'],
            'confidence': 0.4,  # Lower confidence for estimated data
            'source': 'soc_default',
            'title': occ_data.get('title', 'Unknown')
        }

    def _create_fallback_industry_rates(self, industry: str) -> Dict[str, Any]:
        """Create fallback rates when no employment data is available."""
        # Use industry-specific defaults similar to current methodology
        industry_defaults = {
            "Information": {"automation": 0.25, "augmentation": 0.75},
            "Professional and Business Services": {"automation": 0.35, "augmentation": 0.65},
            "Financial Activities": {"automation": 0.40, "augmentation": 0.60},
            "Education and Health Services": {"automation": 0.20, "augmentation": 0.60},
            "Manufacturing": {"automation": 0.50, "augmentation": 0.40},
            "Trade, Transportation, and Utilities": {"automation": 0.60, "augmentation": 0.30}
        }
        
        defaults = industry_defaults.get(industry, {"automation": 0.40, "augmentation": 0.50})
        
        return {
            'automation_rate': defaults['automation'],
            'augmentation_rate': defaults['augmentation'],
            'data_coverage': 0.0,
            'total_employment': 0,
            'confidence': 0.3,  # Low confidence for fallback data
            'avg_data_confidence': 0.3,
            'occupations_analyzed': 0,
            'calculation_method': 'industry_fallback'
        }

    def _calculate_industry_confidence(self, data_coverage: float, avg_confidence: float, industry: str) -> float:
        """Calculate overall confidence score for industry estimates."""
        base_confidence = min(data_coverage * avg_confidence, 1.0)
        
        # Industry-specific confidence adjustments based on research depth
        industry_confidence_factors = {
            "Information": 1.2,  # Well-studied industry
            "Professional and Business Services": 1.1,
            "Financial Activities": 1.1,
            "Manufacturing": 1.0,
            "Education and Health Services": 0.9,  # Complex dynamics
            "Trade, Transportation, and Utilities": 0.8,
            "Construction": 0.7,
            "Government": 0.6,  # Limited private sector data
            "Agriculture": 0.5
        }
        
        industry_factor = industry_confidence_factors.get(industry, 1.0)
        final_confidence = min(base_confidence * industry_factor, 1.0)
        
        return max(final_confidence, 0.1)  # Minimum confidence floor

    def _calculate_coverage_statistics(self):
        """Calculate overall coverage statistics."""
        if not self.industry_rates:
            return
        
        total_industries = len(self.industry_rates)
        high_coverage_industries = sum(1 for rates in self.industry_rates.values() if rates['data_coverage'] >= 0.7)
        medium_coverage_industries = sum(1 for rates in self.industry_rates.values() if 0.3 <= rates['data_coverage'] < 0.7)
        low_coverage_industries = sum(1 for rates in self.industry_rates.values() if rates['data_coverage'] < 0.3)
        
        avg_coverage = sum(rates['data_coverage'] for rates in self.industry_rates.values()) / total_industries
        avg_confidence = sum(rates['confidence'] for rates in self.industry_rates.values()) / total_industries
        
        self.mapping_metadata["coverage_stats"] = {
            "total_industries": total_industries,
            "high_coverage_industries": high_coverage_industries,
            "medium_coverage_industries": medium_coverage_industries,
            "low_coverage_industries": low_coverage_industries,
            "average_coverage": avg_coverage,
            "average_confidence": avg_confidence,
            "coverage_distribution": {
                "high (>=70%)": high_coverage_industries,
                "medium (30-70%)": medium_coverage_industries,
                "low (<30%)": low_coverage_industries
            }
        }

    def _log_calculation_summary(self):
        """Log summary of calculation results."""
        if not self.industry_rates:
            return
            
        logger.info("=== OCCUPATION-INDUSTRY MAPPING RESULTS ===")
        
        # Overall statistics
        stats = self.mapping_metadata.get("coverage_stats", {})
        logger.info(f"Industries processed: {stats.get('total_industries', 0)}")
        logger.info(f"Average data coverage: {stats.get('average_coverage', 0):.1%}")
        logger.info(f"Average confidence: {stats.get('average_confidence', 0):.2f}")
        
        # Coverage distribution
        coverage_dist = stats.get("coverage_distribution", {})
        for level, count in coverage_dist.items():
            logger.info(f"  {level} coverage: {count} industries")
        
        # Top and bottom industries by automation rate
        sorted_by_automation = sorted(
            self.industry_rates.items(), 
            key=lambda x: x[1]['automation_rate'], 
            reverse=True
        )
        
        logger.info("Top 3 industries by automation rate:")
        for industry, rates in sorted_by_automation[:3]:
            logger.info(f"  {industry}: {rates['automation_rate']:.1%} automation, "
                       f"{rates['augmentation_rate']:.1%} augmentation (confidence: {rates['confidence']:.2f})")
        
        logger.info("Bottom 3 industries by automation rate:")
        for industry, rates in sorted_by_automation[-3:]:
            logger.info(f"  {industry}: {rates['automation_rate']:.1%} automation, "
                       f"{rates['augmentation_rate']:.1%} augmentation (confidence: {rates['confidence']:.2f})")

    def get_mapping_results(self) -> Dict[str, Any]:
        """
        Get complete mapping results with metadata.
        
        Returns:
            Dictionary containing rates, metadata, and quality metrics
        """
        return {
            "industry_rates": self.industry_rates,
            "metadata": self.mapping_metadata,
            "calculation_summary": {
                "total_industries": len(self.industry_rates),
                "methodology": "occupation_weighted_aggregation",
                "data_sources_used": list(self.mapping_metadata["data_sources"].keys()),
                "calculated_at": datetime.now().isoformat()
            }
        }

    def save_results(self, output_file: str):
        """Save mapping results to file."""
        results = self.get_mapping_results()
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Saved occupation-industry mapping results to {output_file}")


def main():
    """Main execution function for testing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate industry automation rates using occupation mapping')
    parser.add_argument('--input-dir', default='./data/processed', help='Input directory')
    parser.add_argument('--anthropic-file', help='Path to Anthropic occupation data')
    parser.add_argument('--bls-file', help='Path to BLS occupation employment data')
    parser.add_argument('--output-file', help='Path to save results')
    
    args = parser.parse_args()
    
    # Initialize mapper
    mapper = OccupationIndustryMapper(input_dir=args.input_dir)
    
    # Load data sources
    success = mapper.load_data_sources(
        anthropic_file=args.anthropic_file,
        bls_file=args.bls_file,
        auto_discover=True
    )
    
    if not success:
        logger.warning("Some data sources could not be loaded - proceeding with available data")
    
    # Calculate industry rates
    industry_rates = mapper.calculate_industry_automation_rates()
    
    # Save results if output file specified
    if args.output_file:
        mapper.save_results(args.output_file)
    else:
        # Print summary
        print("\nIndustry Automation Rates Summary:")
        for industry, rates in industry_rates.items():
            print(f"{industry:.<40} Auto: {rates['automation_rate']:.1%}, "
                  f"Aug: {rates['augmentation_rate']:.1%}, "
                  f"Conf: {rates['confidence']:.2f}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())