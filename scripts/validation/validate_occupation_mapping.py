#!/usr/bin/env python3
# scripts/validation/validate_occupation_mapping.py
"""
Validation Framework for Occupation-Industry Mapping
Validates mapping quality, data coverage, and calculation consistency.
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
from analysis.occupation_industry_mapper import OccupationIndustryMapper

logger = logging.getLogger(__name__)

class OccupationMappingValidator:
    """
    Validates occupation-industry mapping results for quality and consistency.
    """
    
    def __init__(self):
        self.soc_mapper = SOCCodeMapper()
        self.validation_results = {}
        
    def validate_mapping_quality(self, 
                                industry_rates: Dict[str, Dict[str, Any]], 
                                employment_matrix: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate the quality of occupation-to-industry mapping.
        
        Args:
            industry_rates: Calculated industry automation/augmentation rates
            employment_matrix: Original employment data (optional)
            
        Returns:
            Comprehensive validation report
        """
        validation_results = {
            "validation_passed": True,
            "overall_quality_score": 0.0,
            "warnings": [],
            "errors": [],
            "quality_checks": {},
            "industry_analysis": {},
            "recommendations": []
        }
        
        logger.info("Validating occupation-industry mapping quality...")
        
        # Run individual validation checks
        coverage_check = self._validate_coverage(industry_rates)
        consistency_check = self._validate_consistency(industry_rates)
        reasonableness_check = self._validate_reasonableness(industry_rates)
        confidence_check = self._validate_confidence_scores(industry_rates)
        
        # Aggregate results
        validation_results["quality_checks"] = {
            "coverage": coverage_check,
            "consistency": consistency_check,
            "reasonableness": reasonableness_check,
            "confidence": confidence_check
        }
        
        # Calculate overall quality score
        quality_scores = [
            coverage_check.get("quality_score", 0.0),
            consistency_check.get("quality_score", 0.0),
            reasonableness_check.get("quality_score", 0.0),
            confidence_check.get("quality_score", 0.0)
        ]
        
        validation_results["overall_quality_score"] = sum(quality_scores) / len(quality_scores)
        
        # Collect warnings and errors
        for check in validation_results["quality_checks"].values():
            validation_results["warnings"].extend(check.get("warnings", []))
            validation_results["errors"].extend(check.get("errors", []))
        
        # Determine if validation passed
        validation_results["validation_passed"] = (
            len(validation_results["errors"]) == 0 and
            validation_results["overall_quality_score"] >= 0.6
        )
        
        # Generate recommendations
        validation_results["recommendations"] = self._generate_recommendations(validation_results)
        
        # Log validation summary
        self._log_validation_summary(validation_results)
        
        return validation_results

    def _validate_coverage(self, industry_rates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Validate data coverage across industries and occupations."""
        coverage_check = {
            "quality_score": 0.0,
            "warnings": [],
            "errors": [],
            "metrics": {}
        }
        
        if not industry_rates:
            coverage_check["errors"].append("No industry rates provided")
            return coverage_check
        
        # Check industry coverage
        total_industries = len(industry_rates)
        high_coverage_count = sum(1 for rates in industry_rates.values() if rates.get('data_coverage', 0) >= 0.7)
        medium_coverage_count = sum(1 for rates in industry_rates.values() if 0.3 <= rates.get('data_coverage', 0) < 0.7)
        low_coverage_count = sum(1 for rates in industry_rates.values() if rates.get('data_coverage', 0) < 0.3)
        
        coverage_check["metrics"] = {
            "total_industries": total_industries,
            "high_coverage_industries": high_coverage_count,
            "medium_coverage_industries": medium_coverage_count,
            "low_coverage_industries": low_coverage_count,
            "average_coverage": sum(rates.get('data_coverage', 0) for rates in industry_rates.values()) / total_industries
        }
        
        # Quality scoring
        high_coverage_ratio = high_coverage_count / total_industries
        avg_coverage = coverage_check["metrics"]["average_coverage"]
        
        # Coverage quality score (0-1)
        coverage_quality = (high_coverage_ratio * 0.6) + (avg_coverage * 0.4)
        coverage_check["quality_score"] = min(coverage_quality, 1.0)
        
        # Generate warnings
        if high_coverage_ratio < 0.3:
            coverage_check["warnings"].append(f"Low proportion of high-coverage industries: {high_coverage_ratio:.1%}")
        
        if avg_coverage < 0.5:
            coverage_check["warnings"].append(f"Low average data coverage: {avg_coverage:.1%}")
        
        if low_coverage_count > total_industries * 0.5:
            coverage_check["warnings"].append(f"Many industries have low coverage: {low_coverage_count}/{total_industries}")
        
        return coverage_check

    def _validate_consistency(self, industry_rates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Validate consistency of rates across similar industries."""
        consistency_check = {
            "quality_score": 0.0,
            "warnings": [],
            "errors": [],
            "metrics": {}
        }
        
        # Define industry groups for consistency checking
        industry_groups = {
            "technology_related": ["Information", "Professional and Business Services"],
            "service_oriented": ["Education and Health Services", "Leisure and Hospitality"],
            "traditional_industries": ["Manufacturing", "Construction", "Mining and Logging"]
        }
        
        group_consistency_scores = []
        
        for group_name, industries in industry_groups.items():
            available_industries = [ind for ind in industries if ind in industry_rates]
            
            if len(available_industries) < 2:
                continue  # Need at least 2 industries to check consistency
            
            # Check automation rate consistency within group
            auto_rates = [industry_rates[ind]['automation_rate'] for ind in available_industries]
            aug_rates = [industry_rates[ind]['augmentation_rate'] for ind in available_industries]
            
            auto_variance = np.var(auto_rates)
            aug_variance = np.var(aug_rates)
            
            # Lower variance indicates higher consistency
            auto_consistency = max(0, 1 - (auto_variance / 0.01))  # Scale variance
            aug_consistency = max(0, 1 - (aug_variance / 0.01))
            
            group_consistency = (auto_consistency + aug_consistency) / 2
            group_consistency_scores.append(group_consistency)
            
            consistency_check["metrics"][f"{group_name}_consistency"] = {
                "automation_variance": auto_variance,
                "augmentation_variance": aug_variance,
                "consistency_score": group_consistency,
                "industries_analyzed": available_industries
            }
            
            # Generate warnings for high variance
            if auto_variance > 0.02:  # 2% variance threshold
                consistency_check["warnings"].append(
                    f"High automation rate variance in {group_name}: {auto_variance:.3f}"
                )
            
            if aug_variance > 0.02:
                consistency_check["warnings"].append(
                    f"High augmentation rate variance in {group_name}: {aug_variance:.3f}"
                )
        
        # Overall consistency score
        if group_consistency_scores:
            consistency_check["quality_score"] = sum(group_consistency_scores) / len(group_consistency_scores)
        else:
            consistency_check["quality_score"] = 0.5  # Neutral if no groups to check
            consistency_check["warnings"].append("Insufficient data for consistency checking")
        
        return consistency_check

    def _validate_reasonableness(self, industry_rates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Validate that calculated rates are within reasonable bounds."""
        reasonableness_check = {
            "quality_score": 0.0,
            "warnings": [],
            "errors": [],
            "metrics": {}
        }
        
        reasonable_count = 0
        total_industries = len(industry_rates)
        
        # Expected ranges for different industries (based on research)
        expected_ranges = {
            "Information": {"automation": (0.15, 0.40), "augmentation": (0.60, 0.85)},
            "Professional and Business Services": {"automation": (0.25, 0.50), "augmentation": (0.50, 0.75)},
            "Financial Activities": {"automation": (0.30, 0.55), "augmentation": (0.45, 0.70)},
            "Education and Health Services": {"automation": (0.10, 0.35), "augmentation": (0.50, 0.80)},
            "Manufacturing": {"automation": (0.40, 0.70), "augmentation": (0.25, 0.50)},
            "Trade, Transportation, and Utilities": {"automation": (0.45, 0.75), "augmentation": (0.20, 0.45)}
        }
        
        for industry, rates in industry_rates.items():
            auto_rate = rates.get('automation_rate', 0)
            aug_rate = rates.get('augmentation_rate', 0)
            
            # Basic bounds check
            auto_reasonable = 0.05 <= auto_rate <= 0.85
            aug_reasonable = 0.10 <= aug_rate <= 0.90
            sum_reasonable = 0.30 <= (auto_rate + aug_rate) <= 1.20
            
            if auto_reasonable and aug_reasonable and sum_reasonable:
                reasonable_count += 1
            else:
                if not auto_reasonable:
                    reasonableness_check["warnings"].append(
                        f"{industry}: automation rate {auto_rate:.2%} outside reasonable bounds"
                    )
                if not aug_reasonable:
                    reasonableness_check["warnings"].append(
                        f"{industry}: augmentation rate {aug_rate:.2%} outside reasonable bounds"
                    )
                if not sum_reasonable:
                    reasonableness_check["warnings"].append(
                        f"{industry}: combined rates {auto_rate + aug_rate:.2%} outside reasonable bounds"
                    )
            
            # Industry-specific bounds check
            if industry in expected_ranges:
                expected = expected_ranges[industry]
                auto_in_range = expected["automation"][0] <= auto_rate <= expected["automation"][1]
                aug_in_range = expected["augmentation"][0] <= aug_rate <= expected["augmentation"][1]
                
                if not auto_in_range or not aug_in_range:
                    reasonableness_check["warnings"].append(
                        f"{industry}: rates outside expected range - "
                        f"auto: {auto_rate:.2%} (expected: {expected['automation'][0]:.1%}-{expected['automation'][1]:.1%}), "
                        f"aug: {aug_rate:.2%} (expected: {expected['augmentation'][0]:.1%}-{expected['augmentation'][1]:.1%})"
                    )
        
        reasonableness_check["quality_score"] = reasonable_count / total_industries if total_industries > 0 else 0
        reasonableness_check["metrics"]["reasonable_industries"] = reasonable_count
        reasonableness_check["metrics"]["total_industries"] = total_industries
        
        return reasonableness_check

    def _validate_confidence_scores(self, industry_rates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Validate confidence scores and their relationship to data quality."""
        confidence_check = {
            "quality_score": 0.0,
            "warnings": [],
            "errors": [],
            "metrics": {}
        }
        
        confidences = [rates.get('confidence', 0) for rates in industry_rates.values()]
        coverages = [rates.get('data_coverage', 0) for rates in industry_rates.values()]
        
        if not confidences:
            confidence_check["errors"].append("No confidence scores available")
            return confidence_check
        
        avg_confidence = sum(confidences) / len(confidences)
        avg_coverage = sum(coverages) / len(coverages)
        
        # Check correlation between confidence and coverage
        correlation = np.corrcoef(confidences, coverages)[0, 1] if len(confidences) > 1 else 0
        
        confidence_check["metrics"] = {
            "average_confidence": avg_confidence,
            "min_confidence": min(confidences),
            "max_confidence": max(confidences),
            "confidence_coverage_correlation": correlation
        }
        
        # Quality scoring based on confidence levels
        high_confidence_count = sum(1 for c in confidences if c >= 0.7)
        medium_confidence_count = sum(1 for c in confidences if 0.4 <= c < 0.7)
        low_confidence_count = sum(1 for c in confidences if c < 0.4)
        
        total_industries = len(confidences)
        high_confidence_ratio = high_confidence_count / total_industries
        
        confidence_check["quality_score"] = (avg_confidence * 0.6) + (high_confidence_ratio * 0.4)
        
        # Generate warnings
        if avg_confidence < 0.5:
            confidence_check["warnings"].append(f"Low average confidence: {avg_confidence:.2f}")
        
        if low_confidence_count > total_industries * 0.3:
            confidence_check["warnings"].append(f"Many industries have low confidence: {low_confidence_count}/{total_industries}")
        
        if abs(correlation) < 0.3:
            confidence_check["warnings"].append(f"Weak correlation between confidence and data coverage: {correlation:.2f}")
        
        return confidence_check

    def _generate_recommendations(self, validation_results: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on validation results."""
        recommendations = []
        
        overall_quality = validation_results["overall_quality_score"]
        quality_checks = validation_results["quality_checks"]
        
        # Coverage recommendations
        coverage_score = quality_checks.get("coverage", {}).get("quality_score", 0)
        if coverage_score < 0.6:
            recommendations.append("Collect additional BLS OEWS data to improve occupation coverage")
            recommendations.append("Focus on industries with low data coverage for targeted data collection")
        
        # Consistency recommendations
        consistency_score = quality_checks.get("consistency", {}).get("quality_score", 0)
        if consistency_score < 0.7:
            recommendations.append("Review calculation methodology for consistency across similar industries")
            recommendations.append("Consider industry-specific adjustments to reduce variance within industry groups")
        
        # Reasonableness recommendations
        reasonableness_score = quality_checks.get("reasonableness", {}).get("quality_score", 0)
        if reasonableness_score < 0.8:
            recommendations.append("Review and validate outlier automation/augmentation rates")
            recommendations.append("Cross-reference rates with academic research and industry studies")
        
        # Confidence recommendations
        confidence_score = quality_checks.get("confidence", {}).get("quality_score", 0)
        if confidence_score < 0.6:
            recommendations.append("Improve Anthropic occupation data coverage to increase confidence")
            recommendations.append("Validate SOC code mappings to ensure accurate occupation matching")
        
        # Overall recommendations
        if overall_quality < 0.7:
            recommendations.append("Consider using fallback methodology until data quality improves")
            recommendations.append("Implement gradual transition to occupation-based methodology")
        
        return recommendations

    def _log_validation_summary(self, validation_results: Dict[str, Any]):
        """Log summary of validation results."""
        logger.info("=== OCCUPATION MAPPING VALIDATION SUMMARY ===")
        logger.info(f"Overall quality score: {validation_results['overall_quality_score']:.2f}")
        logger.info(f"Validation passed: {validation_results['validation_passed']}")
        
        # Log individual quality scores
        for check_name, check_data in validation_results["quality_checks"].items():
            score = check_data.get("quality_score", 0)
            logger.info(f"{check_name.capitalize()} quality: {score:.2f}")
        
        # Log warnings
        if validation_results["warnings"]:
            logger.warning(f"Validation warnings ({len(validation_results['warnings'])}):")
            for warning in validation_results["warnings"][:5]:  # Show first 5
                logger.warning(f"  - {warning}")
        
        # Log errors
        if validation_results["errors"]:
            logger.error(f"Validation errors ({len(validation_results['errors'])}):")
            for error in validation_results["errors"]:
                logger.error(f"  - {error}")
        
        # Log recommendations
        if validation_results["recommendations"]:
            logger.info(f"Recommendations ({len(validation_results['recommendations'])}):")
            for rec in validation_results["recommendations"][:3]:  # Show first 3
                logger.info(f"  - {rec}")

    def compare_with_baseline(self, 
                             new_rates: Dict[str, Dict[str, Any]], 
                             baseline_rates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compare new occupation-mapped rates with baseline uniform rates.
        
        Args:
            new_rates: New rates from occupation mapping
            baseline_rates: Baseline rates from simplified calculation
            
        Returns:
            Comparison analysis
        """
        comparison = {
            "industries_compared": 0,
            "significant_changes": [],
            "improvement_metrics": {},
            "change_summary": {}
        }
        
        auto_changes = []
        aug_changes = []
        confidence_improvements = []
        
        for industry in new_rates:
            if industry in baseline_rates:
                comparison["industries_compared"] += 1
                
                new_auto = new_rates[industry].get('automation_rate', 0)
                new_aug = new_rates[industry].get('augmentation_rate', 0)
                new_confidence = new_rates[industry].get('confidence', 0)
                
                baseline_auto = baseline_rates[industry].get('automation_rate', 0)
                baseline_aug = baseline_rates[industry].get('augmentation_rate', 0)
                baseline_confidence = baseline_rates[industry].get('confidence', 0.5)
                
                auto_change = new_auto - baseline_auto
                aug_change = new_aug - baseline_aug
                confidence_change = new_confidence - baseline_confidence
                
                auto_changes.append(auto_change)
                aug_changes.append(aug_change)
                confidence_improvements.append(confidence_change)
                
                # Flag significant changes
                if abs(auto_change) > 0.1 or abs(aug_change) > 0.1:
                    comparison["significant_changes"].append({
                        "industry": industry,
                        "automation_change": auto_change,
                        "augmentation_change": aug_change,
                        "confidence_improvement": confidence_change
                    })
        
        # Calculate summary statistics
        if auto_changes:
            comparison["change_summary"] = {
                "avg_automation_change": sum(auto_changes) / len(auto_changes),
                "avg_augmentation_change": sum(aug_changes) / len(aug_changes),
                "avg_confidence_improvement": sum(confidence_improvements) / len(confidence_improvements),
                "max_automation_change": max(auto_changes),
                "min_automation_change": min(auto_changes),
                "industries_with_higher_automation": sum(1 for c in auto_changes if c > 0.05),
                "industries_with_lower_automation": sum(1 for c in auto_changes if c < -0.05)
            }
        
        # Calculate improvement score
        avg_confidence_improvement = comparison["change_summary"].get("avg_confidence_improvement", 0)
        change_magnitude = np.mean([abs(c) for c in auto_changes + aug_changes]) if auto_changes else 0
        
        improvement_score = (avg_confidence_improvement * 0.6) + (min(change_magnitude, 0.2) * 0.4)
        comparison["improvement_metrics"]["overall_improvement_score"] = improvement_score
        
        return comparison


def main():
    """Main execution function for testing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate occupation-industry mapping results')
    parser.add_argument('industry_rates_file', help='Path to industry rates JSON file')
    parser.add_argument('--baseline-file', help='Path to baseline rates for comparison')
    parser.add_argument('--output-file', help='Path to save validation results')
    
    args = parser.parse_args()
    
    # Initialize validator
    validator = OccupationMappingValidator()
    
    # Load industry rates
    try:
        with open(args.industry_rates_file, 'r') as f:
            rates_data = json.load(f)
            industry_rates = rates_data.get("industry_rates", rates_data)
    except Exception as e:
        logger.error(f"Error loading industry rates: {e}")
        return 1
    
    # Run validation
    validation_results = validator.validate_mapping_quality(industry_rates)
    
    # Compare with baseline if provided
    if args.baseline_file:
        try:
            with open(args.baseline_file, 'r') as f:
                baseline_data = json.load(f)
                baseline_rates = baseline_data.get("industry_rates", baseline_data)
            
            comparison = validator.compare_with_baseline(industry_rates, baseline_rates)
            validation_results["baseline_comparison"] = comparison
            
            logger.info(f"Compared {comparison['industries_compared']} industries with baseline")
            
        except Exception as e:
            logger.warning(f"Error loading baseline file: {e}")
    
    # Save results if requested
    if args.output_file:
        with open(args.output_file, 'w') as f:
            json.dump(validation_results, f, indent=2)
        logger.info(f"Saved validation results to {args.output_file}")
    
    # Return exit code based on validation result
    return 0 if validation_results["validation_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())