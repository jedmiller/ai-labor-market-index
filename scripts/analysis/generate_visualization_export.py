#!/usr/bin/env python3
"""
Generate comprehensive visualization export for AI Labor Market Index
"""
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
import os
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('visualization-exporter')

class VisualizationExporter:
    def __init__(self, year, month, base_dir=None):
        self.year = year
        self.month = month
        self.period = f"{year:04d}{month:02d}"

        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = project_root

        self.processed_dir = self.base_dir / 'data' / 'processed'
        self.projections_dir = self.base_dir / 'data' / 'projections'

    def load_json(self, filepath):
        """Load JSON file if it exists"""
        if filepath.exists():
            with open(filepath, 'r') as f:
                return json.load(f)
        return None

    def generate_export(self):
        """Generate comprehensive visualization export"""
        logger.info(f"Generating visualization export for {self.year}-{self.month:02d}")

        # Load all data files
        impact_data = self.load_json(self.processed_dir / f'ai_labor_impact_{self.period}.json')
        job_trends = self.load_json(self.processed_dir / f'job_trends_{self.period}.json')
        projections = self.load_json(self.projections_dir / f'ai_impact_projections_{self.period}.json')
        employment = self.load_json(self.processed_dir / f'employment_stats_{self.period}.json')

        if not impact_data:
            logger.error("No AI impact data found")
            return None

        # Build comprehensive export
        export = {
            "metadata": {
                "generated": datetime.now().isoformat() + "Z",
                "source_period": f"{self.year}-{self.month:02d}",
                "data_version": "v2",
                "anthropic_release": job_trends.get("source", "unknown") if job_trends else "unknown"
            },
            "current_state": self._build_current_state(impact_data, job_trends, employment),
            "projections": self._build_projections(projections),
            "industry_breakdown": self._build_industry_breakdown(impact_data, job_trends),
            "geographic_insights": self._build_geographic_insights(job_trends),
            "occupation_insights": self._build_occupation_insights(job_trends),
            "methodology_notes": self._build_methodology_notes(impact_data, job_trends)
        }

        # Save export
        output_path = self.processed_dir / f'visualization_export_{self.period}.json'
        with open(output_path, 'w') as f:
            json.dump(export, f, indent=2)

        logger.info(f"Saved visualization export to {output_path}")

        # Also save as _complete version for compatibility
        complete_path = self.processed_dir / f'visualization_export_{self.period}_complete.json'
        with open(complete_path, 'w') as f:
            json.dump(export, f, indent=2)

        logger.info(f"Saved complete export to {complete_path}")

        return export

    def _build_current_state(self, impact_data, job_trends, employment):
        """Build current state section"""
        state = {
            "overall_impact": impact_data.get("total_impact", 0),
            "overall_impact_percentage": impact_data.get("total_impact", 0) * 100,
            "total_jobs_affected": impact_data.get("jobs_affected", 0),
            "jobs_displaced": impact_data.get("jobs_displaced", 0),
            "jobs_created": impact_data.get("jobs_created", 0),
            "jobs_from_demand": impact_data.get("jobs_demand_effect", 0),
            "total_employment": impact_data.get("total_employment", 0)
        }

        # Add automation/augmentation rates
        if job_trends and "statistics" in job_trends:
            stats = job_trends["statistics"]
            state.update({
                "automation_rate_us": stats.get("us_automation_rate", 50),
                "augmentation_rate_us": stats.get("us_augmentation_rate", 50),
                "automation_rate_global": stats.get("global_automation_rate", 50),
                "augmentation_rate_global": stats.get("global_augmentation_rate", 50)
            })

        # Add data coverage
        if job_trends:
            state["data_coverage"] = {
                "countries_analyzed": len(job_trends.get("geographic_coverage", {}).get("countries", [])),
                "soc_categories": len(job_trends.get("soc_distribution", [])),
                "classified_percentage": 100 - job_trends.get("statistics", {}).get("us_unclassified_pct", 0),
                "unclassified_percentage": job_trends.get("statistics", {}).get("us_unclassified_pct", 0)
            }

        return state

    def _build_projections(self, projections):
        """Build projections section"""
        if not projections:
            return {
                "scenarios": {
                    "conservative": {},
                    "moderate": {},
                    "aggressive": {}
                },
                "timeline": []
            }

        return {
            "scenarios": projections.get("scenarios", {}),
            "timeline": projections.get("timeline", [])
        }

    def _build_industry_breakdown(self, impact_data, job_trends):
        """Build industry breakdown section"""
        breakdown = {}

        if "by_industry" in impact_data:
            for industry, data in impact_data["by_industry"].items():
                if industry == "Total Nonfarm":
                    continue

                # Get industry-specific rates from job_trends
                auto_rate = 50
                aug_rate = 50
                if job_trends and "industry_impacts" in job_trends:
                    industry_impact = job_trends["industry_impacts"].get(industry, {})
                    auto_rate = industry_impact.get("automation_rate", 50)
                    aug_rate = industry_impact.get("augmentation_rate", 50)

                breakdown[industry] = {
                    "current_impact": data.get("impact", 0),
                    "automation_rate": auto_rate,
                    "augmentation_rate": aug_rate,
                    "employment": data.get("jobs_affected", 0) / data.get("impact", -0.01) if data.get("impact", 0) != 0 else 0,
                    "projected_2030": data.get("impact", 0) * 0.35  # Rough projection
                }

        return breakdown

    def _build_geographic_insights(self, job_trends):
        """Build geographic insights section"""
        insights = {
            "top_automated_countries": [],
            "top_augmented_countries": [],
            "us_position": {}
        }

        if job_trends and "geographic_coverage" in job_trends:
            geo_data = job_trends["geographic_coverage"]

            # Get top automated countries
            countries = geo_data.get("countries", {})
            sorted_auto = sorted(countries.items(), key=lambda x: x[1].get("automation_rate", 0), reverse=True)[:5]
            insights["top_automated_countries"] = [
                {"country": c[0], "automation_rate": c[1].get("automation_rate", 0)}
                for c in sorted_auto
            ]

            # Get top augmented countries
            sorted_aug = sorted(countries.items(), key=lambda x: x[1].get("augmentation_rate", 0), reverse=True)[:5]
            insights["top_augmented_countries"] = [
                {"country": c[0], "augmentation_rate": c[1].get("augmentation_rate", 0)}
                for c in sorted_aug
            ]

            # US position
            us_data = countries.get("United States", {})
            if us_data:
                us_rank_auto = sum(1 for c in countries.values() if c.get("automation_rate", 0) > us_data.get("automation_rate", 0)) + 1
                us_rank_aug = sum(1 for c in countries.values() if c.get("augmentation_rate", 0) > us_data.get("augmentation_rate", 0)) + 1

                insights["us_position"] = {
                    "automation_rank": us_rank_auto,
                    "augmentation_rank": us_rank_aug,
                    "automation_rate": us_data.get("automation_rate", 0),
                    "augmentation_rate": us_data.get("augmentation_rate", 0)
                }

        return insights

    def _build_occupation_insights(self, job_trends):
        """Build occupation insights section"""
        insights = {
            "top_augmented_roles": [],
            "top_automated_roles": [],
            "soc_distribution": []
        }

        if job_trends:
            # Add top roles if available
            insights["top_augmented_roles"] = job_trends.get("top_augmented_roles", [])[:5]
            insights["top_automated_roles"] = job_trends.get("top_automated_roles", [])[:5]

            # Add SOC distribution
            if "soc_distribution" in job_trends:
                soc_data = job_trends["soc_distribution"]
                # Handle both dict and list formats
                if isinstance(soc_data, dict):
                    # Sort by percentage and take top 5
                    sorted_soc = sorted(soc_data.items(), key=lambda x: x[1], reverse=True)[:5]
                    insights["soc_distribution"] = [
                        {"category": cat, "percentage": pct}
                        for cat, pct in sorted_soc
                    ]
                elif isinstance(soc_data, list):
                    # Already a list, assume it's sorted
                    insights["soc_distribution"] = soc_data[:5]

        return insights

    def _build_methodology_notes(self, impact_data, job_trends):
        """Build methodology notes section"""
        notes = {
            "changes_from_previous": [
                "Updated to latest Anthropic Economic Index",
                "Industry-specific automation/augmentation rates",
                "SOC-based industry impact calculations",
                "US-specific data extraction from facet files",
                "Improved displacement and creation effect models"
            ],
            "data_quality_indicators": {
                "has_anthropic_data": job_trends is not None,
                "has_recent_employment": impact_data.get("total_employment", 0) > 0,
                "has_industry_breakdown": "by_industry" in impact_data,
                "uses_occupation_mapping": False,
                "confidence_level": "moderate" if job_trends else "low"
            }
        }

        return notes


def main():
    parser = argparse.ArgumentParser(description='Generate visualization export for AI Labor Market Index')
    parser.add_argument('--year', type=int, required=True, help='Year to process')
    parser.add_argument('--month', type=int, required=True, help='Month to process')
    parser.add_argument('--base-dir', type=str, help='Base directory')

    args = parser.parse_args()

    exporter = VisualizationExporter(args.year, args.month, args.base_dir)
    export = exporter.generate_export()

    if export:
        logger.info("Visualization export generation complete")
        return 0
    else:
        logger.error("Failed to generate visualization export")
        return 1


if __name__ == "__main__":
    sys.exit(main())