#!/usr/bin/env python3
# scripts/processing/process_anthropic_occupation_data.py
"""
Anthropic Economic Index Occupation Data Processor
Extracts occupation-level automation/augmentation rates with SOC code mapping
"""
import json
import logging
import os
import sys
import re
from datetime import datetime
from typing import Dict, List, Optional, Any

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.soc_code_mapper import SOCCodeMapper

logger = logging.getLogger(__name__)

class AnthropicOccupationProcessor:
    """
    Processes Anthropic Economic Index data to extract occupation-level impacts
    with proper SOC code mapping and validation.
    """
    
    def __init__(self):
        self.soc_mapper = SOCCodeMapper()
        self.processed_occupations = {}
        self.processing_stats = {
            "total_occupations": 0,
            "successfully_mapped": 0,
            "missing_soc_codes": 0,
            "invalid_soc_codes": 0,
            "estimated_missing": 0
        }

    def process_anthropic_data(self, anthropic_data: Dict) -> Dict[str, Any]:
        """
        Process raw Anthropic Economic Index data to extract occupation-level impacts.
        
        Args:
            anthropic_data: Raw Anthropic Economic Index data
            
        Returns:
            Processed occupation impacts with standardized SOC codes
        """
        logger.info("Processing Anthropic Economic Index occupation data...")
        
        # Reset processing stats
        self.processing_stats = {k: 0 for k in self.processing_stats}
        self.processed_occupations = {}
        
        # Extract occupation data from various possible structures
        occupations_data = self._extract_occupations_data(anthropic_data)
        
        if not occupations_data:
            logger.warning("No occupation data found in Anthropic dataset")
            return self._create_empty_result()
        
        # Process each occupation
        for occupation in occupations_data:
            self._process_single_occupation(occupation)
        
        # Generate summary statistics
        summary_stats = self._calculate_summary_statistics()
        
        # Create final result
        result = {
            "processed_at": datetime.now().isoformat(),
            "source": "Anthropic Economic Index",
            "occupation_impacts": self.processed_occupations,
            "summary_statistics": summary_stats,
            "processing_stats": self.processing_stats,
            "data_coverage": self._calculate_data_coverage()
        }
        
        self._log_processing_summary()
        
        return result

    def _extract_occupations_data(self, anthropic_data: Dict) -> List[Dict]:
        """
        Extract occupation data from various possible Anthropic data structures.
        """
        # Try different possible keys where occupation data might be stored
        possible_keys = [
            "occupations",
            "occupation_data", 
            "detailed_occupations",
            "soc_occupations",
            "occupation_breakdown"
        ]
        
        for key in possible_keys:
            if key in anthropic_data and isinstance(anthropic_data[key], list):
                logger.info(f"Found occupation data under key: {key}")
                return anthropic_data[key]
        
        # Try nested structures
        if "data" in anthropic_data:
            for key in possible_keys:
                if key in anthropic_data["data"]:
                    logger.info(f"Found occupation data under data.{key}")
                    return anthropic_data["data"][key]
        
        # Try extracting from combined structures
        if "combined" in anthropic_data:
            return self._extract_from_combined_data(anthropic_data["combined"])
        
        logger.warning("Could not find occupation data in standard locations")
        return []

    def _extract_from_combined_data(self, combined_data: Dict) -> List[Dict]:
        """
        Extract occupation data from combined Anthropic data structure.
        """
        occupations = []
        
        # Look for occupation usage data
        if "occupation_usage" in combined_data:
            usage_data = combined_data["occupation_usage"]
            if isinstance(usage_data, list):
                occupations.extend(usage_data)
            elif isinstance(usage_data, dict) and "occupations" in usage_data:
                occupations.extend(usage_data["occupations"])
        
        # Look for occupation automation data
        if "occupation_automation" in combined_data:
            auto_data = combined_data["occupation_automation"]
            if isinstance(auto_data, list):
                # Merge with existing data or add new entries
                occupations.extend(auto_data)
            elif isinstance(auto_data, dict) and "occupations" in auto_data:
                occupations.extend(auto_data["occupations"])
        
        logger.info(f"Extracted {len(occupations)} occupations from combined data")
        return occupations

    def _process_single_occupation(self, occupation: Dict):
        """
        Process a single occupation entry and add to processed results.
        """
        self.processing_stats["total_occupations"] += 1
        
        # Extract basic information
        title = occupation.get("title", occupation.get("occupation", "Unknown"))
        raw_soc_code = occupation.get("soc_code", occupation.get("soc", ""))
        
        # Extract impact rates with various possible key names
        automation_rate = self._extract_rate(occupation, [
            "automation_rate", "automation", "auto_rate", 
            "displacement_rate", "automation_potential"
        ])
        
        augmentation_rate = self._extract_rate(occupation, [
            "augmentation_rate", "augmentation", "aug_rate",
            "enhancement_rate", "augmentation_potential"
        ])
        
        # Handle missing SOC codes
        if not raw_soc_code:
            self.processing_stats["missing_soc_codes"] += 1
            # Try to infer SOC code from title
            inferred_soc = self._infer_soc_from_title(title)
            if inferred_soc:
                raw_soc_code = inferred_soc
                logger.debug(f"Inferred SOC code {inferred_soc} for '{title}'")
            else:
                logger.warning(f"No SOC code available for occupation: {title}")
                return
        
        # Standardize SOC code
        standardized_soc = self.soc_mapper.standardize_soc_code(raw_soc_code)
        
        if not standardized_soc:
            self.processing_stats["invalid_soc_codes"] += 1
            logger.warning(f"Invalid SOC code '{raw_soc_code}' for occupation: {title}")
            return
        
        # Handle missing rates using SOC-based defaults
        if automation_rate is None or augmentation_rate is None:
            defaults = self.soc_mapper.get_ai_susceptibility_defaults(standardized_soc)
            if automation_rate is None:
                automation_rate = defaults["automation"]
                self.processing_stats["estimated_missing"] += 1
            if augmentation_rate is None:
                augmentation_rate = defaults["augmentation"]
                self.processing_stats["estimated_missing"] += 1
            
            logger.debug(f"Used SOC defaults for {title}: auto={automation_rate:.2f}, aug={augmentation_rate:.2f}")
        
        # Extract additional metadata
        confidence = occupation.get("confidence", occupation.get("data_quality", 0.5))
        task_count = len(occupation.get("tasks", []))
        
        # Store processed occupation
        self.processed_occupations[standardized_soc] = {
            "title": title,
            "automation_rate": float(automation_rate),
            "augmentation_rate": float(augmentation_rate),
            "confidence": float(confidence),
            "task_count": task_count,
            "raw_soc_code": raw_soc_code,
            "major_group": self.soc_mapper.get_major_group(standardized_soc),
            "processing_source": "anthropic_direct" if occupation.get("soc_code") else "soc_inferred"
        }
        
        self.processing_stats["successfully_mapped"] += 1

    def _extract_rate(self, occupation: Dict, possible_keys: List[str]) -> Optional[float]:
        """
        Extract rate value trying multiple possible key names.
        """
        for key in possible_keys:
            if key in occupation:
                value = occupation[key]
                if isinstance(value, (int, float)):
                    # Convert percentage to decimal if needed
                    if value > 1:
                        return value / 100.0
                    return float(value)
                elif isinstance(value, str):
                    try:
                        parsed = float(value.replace("%", ""))
                        return parsed / 100.0 if parsed > 1 else parsed
                    except ValueError:
                        continue
        
        return None

    def _infer_soc_from_title(self, title: str) -> Optional[str]:
        """
        Attempt to infer SOC code from occupation title using common patterns.
        This is a simplified heuristic - a full implementation would use
        O*NET occupation title database.
        """
        if not title:
            return None
        
        title_lower = title.lower()
        
        # Common occupation title patterns and their SOC codes
        soc_patterns = {
            # Computer and Mathematical (15-xxxx)
            r'software\s+(developer|engineer|programmer)': '15-1252',
            r'data\s+scientist': '15-2051',
            r'computer\s+programmer': '15-1251',
            r'web\s+developer': '15-1254',
            r'database\s+administrator': '15-1141',
            r'information\s+security': '15-1122',
            
            # Business and Financial (13-xxxx)
            r'financial\s+analyst': '13-2051',
            r'market\s+research\s+analyst': '13-1161',
            r'accountant': '13-2011',
            r'budget\s+analyst': '13-2031',
            
            # Management (11-xxxx)
            r'general\s+manager': '11-1021',
            r'operations\s+manager': '11-1021',
            r'chief\s+executive': '11-1011',
            
            # Office and Administrative Support (43-xxxx)
            r'customer\s+service': '43-4051',
            r'secretary': '43-6014',
            r'bookkeeping\s+clerk': '43-3031',
            
            # Healthcare (29-xxxx)
            r'registered\s+nurse': '29-1141',
            r'physician': '29-1062',
            r'pharmacist': '29-1051',
        }
        
        for pattern, soc_code in soc_patterns.items():
            if re.search(pattern, title_lower):
                logger.debug(f"Pattern matched '{title}' to SOC {soc_code}")
                return soc_code
        
        return None

    def _calculate_summary_statistics(self) -> Dict[str, float]:
        """
        Calculate summary statistics across all processed occupations.
        """
        if not self.processed_occupations:
            return {}
        
        automation_rates = [occ["automation_rate"] for occ in self.processed_occupations.values()]
        augmentation_rates = [occ["augmentation_rate"] for occ in self.processed_occupations.values()]
        confidences = [occ["confidence"] for occ in self.processed_occupations.values()]
        
        stats = {
            "average_automation_rate": sum(automation_rates) / len(automation_rates),
            "average_augmentation_rate": sum(augmentation_rates) / len(augmentation_rates),
            "average_confidence": sum(confidences) / len(confidences),
            "automation_augmentation_ratio": (sum(automation_rates) / sum(augmentation_rates)) if sum(augmentation_rates) > 0 else 0,
            "min_automation": min(automation_rates),
            "max_automation": max(automation_rates),
            "min_augmentation": min(augmentation_rates),
            "max_augmentation": max(augmentation_rates),
            "total_occupations": len(self.processed_occupations)
        }
        
        return stats

    def _calculate_data_coverage(self) -> Dict[str, Any]:
        """
        Calculate data coverage metrics by SOC major group.
        """
        coverage = {
            "by_major_group": {},
            "overall_coverage": 0
        }
        
        # Count occupations by major group
        major_group_counts = {}
        for soc_code, occ_data in self.processed_occupations.items():
            major_group = soc_code[:2]
            if major_group not in major_group_counts:
                major_group_counts[major_group] = {
                    "count": 0,
                    "avg_automation": 0,
                    "avg_augmentation": 0,
                    "group_name": occ_data["major_group"]
                }
            major_group_counts[major_group]["count"] += 1
            major_group_counts[major_group]["avg_automation"] += occ_data["automation_rate"]
            major_group_counts[major_group]["avg_augmentation"] += occ_data["augmentation_rate"]
        
        # Calculate averages
        for group, data in major_group_counts.items():
            if data["count"] > 0:
                data["avg_automation"] /= data["count"]
                data["avg_augmentation"] /= data["count"]
        
        coverage["by_major_group"] = major_group_counts
        coverage["overall_coverage"] = len(self.processed_occupations) / max(1, self.processing_stats["total_occupations"])
        
        return coverage

    def _create_empty_result(self) -> Dict[str, Any]:
        """
        Create empty result structure when no data is available.
        """
        return {
            "processed_at": datetime.now().isoformat(),
            "source": "Anthropic Economic Index",
            "occupation_impacts": {},
            "summary_statistics": {},
            "processing_stats": self.processing_stats,
            "data_coverage": {"by_major_group": {}, "overall_coverage": 0},
            "error": "No occupation data found in Anthropic dataset"
        }

    def _log_processing_summary(self):
        """
        Log a summary of the processing results.
        """
        stats = self.processing_stats
        logger.info(f"=== ANTHROPIC OCCUPATION PROCESSING SUMMARY ===")
        logger.info(f"Total occupations processed: {stats['total_occupations']}")
        logger.info(f"Successfully mapped: {stats['successfully_mapped']}")
        logger.info(f"Missing SOC codes: {stats['missing_soc_codes']}")
        logger.info(f"Invalid SOC codes: {stats['invalid_soc_codes']}")
        logger.info(f"Estimated missing rates: {stats['estimated_missing']}")
        
        if stats['total_occupations'] > 0:
            success_rate = stats['successfully_mapped'] / stats['total_occupations']
            logger.info(f"Success rate: {success_rate:.1%}")
        
        if self.processed_occupations:
            summary = self._calculate_summary_statistics()
            logger.info(f"Average automation rate: {summary['average_automation_rate']:.2%}")
            logger.info(f"Average augmentation rate: {summary['average_augmentation_rate']:.2%}")

    def save_processed_data(self, output_file: str, processed_data: Dict):
        """
        Save processed occupation data to file.
        """
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(processed_data, f, indent=2)
        
        logger.info(f"Saved processed Anthropic occupation data to {output_file}")

    def validate_processed_data(self, processed_data: Dict) -> Dict[str, Any]:
        """
        Validate the processed occupation data for quality and completeness.
        """
        validation = {
            "validation_passed": True,
            "warnings": [],
            "errors": [],
            "quality_score": 0.0
        }
        
        # Check basic structure
        if "occupation_impacts" not in processed_data:
            validation["errors"].append("Missing occupation_impacts in processed data")
            validation["validation_passed"] = False
            return validation
        
        occupation_impacts = processed_data["occupation_impacts"]
        
        # Validate SOC codes
        soc_validation = self.soc_mapper.validate_occupation_data(occupation_impacts)
        if not soc_validation["validation_passed"]:
            validation["warnings"].extend(soc_validation.get("warnings", []))
            validation["errors"].extend(soc_validation.get("invalid_soc_codes", []))
        
        # Check coverage across major groups
        major_groups_covered = set(soc[:2] for soc in occupation_impacts.keys())
        total_major_groups = len(self.soc_mapper.soc_major_groups)
        coverage_ratio = len(major_groups_covered) / total_major_groups
        
        if coverage_ratio < 0.3:
            validation["warnings"].append(f"Low SOC major group coverage: {coverage_ratio:.1%}")
        
        # Calculate quality score
        quality_factors = {
            "soc_validity": soc_validation["valid_soc_codes"] / max(1, len(occupation_impacts)),
            "major_group_coverage": coverage_ratio,
            "data_completeness": 1.0 - (len(soc_validation.get("missing_fields", [])) / max(1, len(occupation_impacts) * 2))
        }
        
        validation["quality_score"] = sum(quality_factors.values()) / len(quality_factors)
        validation["quality_factors"] = quality_factors
        
        return validation


def main():
    """Main execution function for testing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Anthropic Economic Index occupation data')
    parser.add_argument('input_file', help='Path to Anthropic data JSON file')
    parser.add_argument('--output-file', help='Path to save processed data')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = AnthropicOccupationProcessor()
    
    # Load input data
    try:
        with open(args.input_file, 'r') as f:
            anthropic_data = json.load(f)
    except Exception as e:
        logger.error(f"Error loading input file: {e}")
        return 1
    
    # Process data
    processed_data = processor.process_anthropic_data(anthropic_data)
    
    # Validate results
    validation = processor.validate_processed_data(processed_data)
    logger.info(f"Validation quality score: {validation['quality_score']:.2f}")
    
    if validation["warnings"]:
        for warning in validation["warnings"]:
            logger.warning(warning)
    
    if validation["errors"]:
        for error in validation["errors"]:
            logger.error(error)
    
    # Save if output file specified
    if args.output_file:
        processor.save_processed_data(args.output_file, processed_data)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())