# scripts/processing/process_anthropic_index_v2.py
"""
Updated processor for Anthropic Economic Index data (August 2025+ format)
Handles the new multi-file structure with geographic and SOC category data
"""

import json
import logging
import os
import sys
import argparse
from datetime import datetime
import glob
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("anthropic_index_processing_v2.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("anthropic-index-processor-v2")

class AnthropicIndexProcessorV2:
    def __init__(self, input_dir="./data/raw/anthropic_index", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # SOC category to industry mapping (for BLS alignment)
        self.soc_to_industry = {
            "Computer and Mathematical": "Information",
            "Business and Financial Operations": "Financial Activities",
            "Architecture and Engineering": "Professional and Business Services",
            "Life, Physical, and Social Science": "Professional and Business Services",
            "Community and Social Service": "Education and Health Services",
            "Legal": "Professional and Business Services",
            "Educational Instruction and Library": "Education and Health Services",
            "Arts, Design, Entertainment, Sports, and Media": "Information",
            "Healthcare Practitioners and Technical": "Education and Health Services",
            "Healthcare Support": "Education and Health Services",
            "Protective Service": "Government",
            "Food Preparation and Serving Related": "Leisure and Hospitality",
            "Building and Grounds Cleaning and Maintenance": "Other Services",
            "Personal Care and Service": "Other Services",
            "Sales and Related": "Trade, Transportation, and Utilities",
            "Office and Administrative Support": "Professional and Business Services",
            "Farming, Fishing, and Forestry": "Other Services",
            "Construction and Extraction": "Construction",
            "Installation, Maintenance, and Repair": "Trade, Transportation, and Utilities",
            "Production": "Manufacturing",
            "Transportation and Material Moving": "Trade, Transportation, and Utilities",
            "Management": "Professional and Business Services"
        }

    def find_data_files(self, year, month):
        """Find all relevant data files for the specified period"""
        month_str = f"{month:02d}" if month else ""
        year_str = f"{year}" if year else ""

        files = {
            'occupations': None,
            'automation_augmentation': None,
            'by_facet': None,
            'hierarchy_1p_api': None,
            'hierarchy_claude_ai': None,
            'raw_csv': None
        }

        # Pattern for new format files
        base_pattern = f"anthropic_index_{year}_{month_str}"

        # Try to find each type of file
        patterns = {
            'occupations': f"{base_pattern}_occupations.json",
            'automation_augmentation': f"{base_pattern}_automation_augmentation.json",
            'by_facet': f"{base_pattern}_by_facet.json",
            'hierarchy_1p_api': f"{base_pattern}_hierarchy_1p_api.json",
            'hierarchy_claude_ai': f"{base_pattern}_hierarchy_claude_ai.json",
            'raw_csv': f"{base_pattern}_raw.csv"
        }

        for file_type, pattern in patterns.items():
            file_path = os.path.join(self.input_dir, pattern)
            if os.path.exists(file_path):
                files[file_type] = file_path
                logger.info(f"Found {file_type} file: {file_path}")
            else:
                logger.warning(f"Missing {file_type} file: {pattern}")

        # Check if we have minimum required files
        if not files['occupations'] and not files['automation_augmentation']:
            # Try to fall back to old format
            logger.warning("New format files not found, checking for old format...")
            old_pattern = f"anthropic_index_{year}_{month_str}_combined.json"
            old_file = os.path.join(self.input_dir, old_pattern)
            if os.path.exists(old_file):
                return self.process_old_format(old_file, year, month)

        return files

    def process_old_format(self, file_path, year, month):
        """Fall back to processing old combined format"""
        logger.info(f"Processing old format file: {file_path}")
        # Delegate to original processor
        from process_anthropic_index import AnthropicIndexProcessor
        processor = AnthropicIndexProcessor(self.input_dir, self.output_dir)
        return processor.process_anthropic_data(year, month)

    def calculate_industry_impacts(self, soc_data, geo_data, use_us_only=True):
        """Calculate industry-level automation/augmentation from SOC and geographic data

        Args:
            soc_data: SOC occupation percentages (can be global or US-specific)
            geo_data: Country-level automation/augmentation rates
            use_us_only: If True, only use US data for calculations
        """
        industry_impacts = {}

        # Use US data if available, otherwise global average
        us_data = geo_data.get("United States", {
            "automation_pct": 50.0,
            "augmentation_pct": 50.0
        })

        # Calculate weighted averages by industry
        for soc_category, data in soc_data.items():
            if soc_category == "not_classified":
                continue

            industry = self.soc_to_industry.get(soc_category)
            if not industry:
                logger.warning(f"No industry mapping for SOC category: {soc_category}")
                continue

            # Get SOC percentage - this should be US-specific if we extracted it correctly
            soc_pct = data.get("soc_pct", 0)

            if industry not in industry_impacts:
                industry_impacts[industry] = {
                    "total_weight": 0,
                    "weighted_automation": 0,
                    "weighted_augmentation": 0,
                    "soc_categories": []
                }

            # Weight the US automation/augmentation by SOC percentage
            industry_impacts[industry]["total_weight"] += soc_pct
            industry_impacts[industry]["weighted_automation"] += soc_pct * us_data["automation_pct"]
            industry_impacts[industry]["weighted_augmentation"] += soc_pct * us_data["augmentation_pct"]
            industry_impacts[industry]["soc_categories"].append(soc_category)

        # Calculate final percentages
        for industry in industry_impacts:
            if industry_impacts[industry]["total_weight"] > 0:
                weight = industry_impacts[industry]["total_weight"]
                industry_impacts[industry]["automation_rate"] = (
                    industry_impacts[industry]["weighted_automation"] / weight
                )
                industry_impacts[industry]["augmentation_rate"] = (
                    industry_impacts[industry]["weighted_augmentation"] / weight
                )
            else:
                # Use US baseline if no SOC data
                industry_impacts[industry]["automation_rate"] = us_data["automation_pct"]
                industry_impacts[industry]["augmentation_rate"] = us_data["augmentation_pct"]

        return industry_impacts

    def extract_us_soc_data_from_facet(self, facet_file):
        """Extract US-specific SOC occupation distribution from facet file"""
        try:
            with open(facet_file, 'r') as f:
                facet_data = json.load(f)

            # Navigate to SOC occupation facet
            soc_facet = facet_data.get("facets", {}).get("soc_occupation", {})
            soc_data_list = soc_facet.get("data", [])

            # Filter for US data only
            us_soc_data = {}
            for entry in soc_data_list:
                if entry.get("geo_id") == "USA" or entry.get("geo_name") == "United States":
                    cluster_name = entry.get("cluster_name")
                    value = entry.get("value", 0)
                    if cluster_name:
                        us_soc_data[cluster_name] = {"soc_pct": value}

            if us_soc_data:
                logger.info(f"Extracted US-specific SOC data: {len(us_soc_data)} categories")
                logger.info(f"US unclassified percentage: {us_soc_data.get('not_classified', {}).get('soc_pct', 0):.1f}%")
                return us_soc_data
            else:
                logger.warning("No US-specific SOC data found in facet file")
                return None

        except Exception as e:
            logger.error(f"Error extracting US SOC data from facet file: {e}")
            return None

    def extract_top_tasks_from_facet(self, facet_file, limit=15):
        """Extract top tasks from the by_facet file"""
        try:
            with open(facet_file, 'r') as f:
                facet_data = json.load(f)

            # Navigate the facet structure to find tasks
            tasks = {}
            if "data" in facet_data:
                for item in facet_data.get("data", []):
                    if isinstance(item, dict):
                        task_name = item.get("task", item.get("name", ""))
                        count = item.get("count", item.get("frequency", 1))
                        if task_name:
                            tasks[task_name] = tasks.get(task_name, 0) + count

            # Sort and return top tasks
            top_tasks = sorted(tasks.items(), key=lambda x: x[1], reverse=True)[:limit]
            return top_tasks
        except Exception as e:
            logger.error(f"Error extracting tasks from facet file: {e}")
            return []

    def process_anthropic_data(self, year=None, month=None):
        """Process new format Anthropic Economic Index data"""

        # Find relevant files
        files = self.find_data_files(year, month)

        # Check if we fell back to old format
        if isinstance(files, dict) and not any(files.values()):
            logger.error("No Anthropic data files found")
            return None
        elif not isinstance(files, dict):
            # Old format was processed
            return files

        try:
            # Try to load US-specific SOC data from facet file first
            occupation_data = {}
            us_specific_data = False

            if files['by_facet']:
                us_soc_data = self.extract_us_soc_data_from_facet(files['by_facet'])
                if us_soc_data:
                    occupation_data = us_soc_data
                    us_specific_data = True
                    logger.info(f"Using US-specific SOC data: {len(occupation_data)} categories")

            # Fall back to global occupation data if no US-specific data
            if not occupation_data and files['occupations']:
                with open(files['occupations'], 'r') as f:
                    occ_json = json.load(f)
                    occupation_data = occ_json.get("occupations", {})
                    logger.info(f"Using global occupation data: {len(occupation_data)} categories")

            # Load geographic automation/augmentation data
            geo_data = {}
            if files['automation_augmentation']:
                with open(files['automation_augmentation'], 'r') as f:
                    geo_json = json.load(f)
                    geo_data = geo_json.get("data", {})
                    logger.info(f"Loaded data for {len(geo_data)} countries")

            # Calculate US and global statistics
            us_stats = geo_data.get("United States", {
                "automation_pct": 50.0,
                "augmentation_pct": 50.0
            })

            # Calculate global average
            if geo_data:
                global_automation = sum(d.get("automation_pct", 0) for d in geo_data.values()) / len(geo_data)
                global_augmentation = sum(d.get("augmentation_pct", 0) for d in geo_data.values()) / len(geo_data)
            else:
                global_automation = 50.0
                global_augmentation = 50.0

            # Calculate industry-level impacts
            industry_impacts = self.calculate_industry_impacts(occupation_data, geo_data)

            # Extract top tasks if facet file available
            top_tasks = []
            if files['by_facet']:
                top_tasks = self.extract_top_tasks_from_facet(files['by_facet'])

            # Create synthetic occupation-level data for backward compatibility
            # Map SOC categories to example occupations
            soc_to_occupations = {
                "Computer and Mathematical": ["Software Developers", "Data Scientists", "AI Engineers"],
                "Business and Financial Operations": ["Financial Analysts", "Business Analysts", "Management Consultants"],
                "Healthcare Practitioners and Technical": ["Physicians", "Nurses", "Medical Technicians"],
                "Educational Instruction and Library": ["Teachers", "Professors", "Librarians"],
                "Office and Administrative Support": ["Administrative Assistants", "Data Entry Clerks", "Receptionists"],
                "Sales and Related": ["Sales Representatives", "Retail Salespersons", "Account Managers"],
                "Arts, Design, Entertainment, Sports, and Media": ["Graphic Designers", "Content Writers", "Video Editors"],
                "Legal": ["Lawyers", "Paralegals", "Legal Assistants"],
                "Management": ["General Managers", "Operations Managers", "Project Managers"],
                "Production": ["Assembly Workers", "Quality Control Inspectors", "Machine Operators"]
            }

            # Generate occupation examples with rates
            top_augmented = []
            top_automated = []

            for soc_category, occupations in soc_to_occupations.items():
                if soc_category in occupation_data:
                    # Use US rates as baseline
                    base_automation = us_stats["automation_pct"]
                    base_augmentation = us_stats["augmentation_pct"]

                    # Adjust based on SOC category characteristics
                    if "Computer" in soc_category:
                        # Tech roles tend to be more augmented
                        adj_automation = base_automation * 0.5
                        adj_augmentation = base_augmentation * 1.5
                    elif "Office" in soc_category or "Administrative" in soc_category:
                        # Admin roles tend to be more automated
                        adj_automation = base_automation * 1.3
                        adj_augmentation = base_augmentation * 0.7
                    elif "Healthcare" in soc_category:
                        # Healthcare tends to be augmented, not automated
                        adj_automation = base_automation * 0.3
                        adj_augmentation = base_augmentation * 1.7
                    else:
                        adj_automation = base_automation
                        adj_augmentation = base_augmentation

                    # Normalize to 100%
                    total = adj_automation + adj_augmentation
                    adj_automation = (adj_automation / total) * 100
                    adj_augmentation = (adj_augmentation / total) * 100

                    for occ in occupations[:1]:  # Take first occupation as representative
                        top_augmented.append({"title": occ, "augmentation_rate": adj_augmentation})
                        top_automated.append({"title": occ, "automation_rate": adj_automation})

            # Sort by rates
            top_augmented = sorted(top_augmented, key=lambda x: x["augmentation_rate"], reverse=True)[:10]
            top_automated = sorted(top_automated, key=lambda x: x["automation_rate"], reverse=True)[:10]

            # Calculate statistics
            total_occupations = len([k for k in occupation_data.keys() if k != "not_classified"])
            not_classified_pct = occupation_data.get("not_classified", {}).get("soc_pct", 0)
            classified_pct = 100 - not_classified_pct

            # Create trends object
            trends = {
                "date_analyzed": datetime.now().isoformat(),
                "source": "Anthropic Economic Index (v2 format)",
                "source_period": f"{year}-{month:02d}" if year and month else datetime.now().strftime('%Y-%m'),
                "data_format": "geographic_and_soc_categories",
                "data_scope": "US-specific" if us_specific_data else "global",
                "statistics": {
                    "total_soc_categories_analyzed": total_occupations,
                    "classified_percentage": classified_pct,
                    "us_automation_rate": us_stats.get("automation_pct", 50.0),
                    "us_augmentation_rate": us_stats.get("augmentation_pct", 50.0),
                    "global_average_automation": global_automation,
                    "global_average_augmentation": global_augmentation,
                    "countries_analyzed": len(geo_data)
                },
                "industry_impacts": industry_impacts,
                "geographic_coverage": {
                    "total_countries": len(geo_data),
                    "top_automated_countries": sorted(
                        [(c, d["automation_pct"]) for c, d in geo_data.items()],
                        key=lambda x: x[1],
                        reverse=True
                    )[:5],
                    "top_augmented_countries": sorted(
                        [(c, d["augmentation_pct"]) for c, d in geo_data.items()],
                        key=lambda x: x[1],
                        reverse=True
                    )[:5]
                },
                "soc_distribution": [
                    {"category": cat, "percentage": data.get("soc_pct", 0)}
                    for cat, data in occupation_data.items()
                    if cat != "not_classified"
                ],
                "top_augmented_roles": top_augmented,
                "top_automated_roles": top_automated,
                "top_tasks": [
                    {"task": task, "count": count}
                    for task, count in top_tasks
                ],
                "notes": [
                    "Data uses new August 2025 format with geographic and SOC category breakdowns",
                    f"{not_classified_pct:.1f}% of data is not classified to specific SOC categories" if not_classified_pct > 0 else "All data classified to SOC categories",
                    "Using US-specific SOC distribution from facet data" if us_specific_data else "Using global SOC distribution",
                    "Occupation-level rates are estimated from SOC category and US geographic data"
                ]
            }

            # Save output
            date_str = f"{year}{month:02d}" if year and month else datetime.now().strftime('%Y%m%d')
            output_file = os.path.join(self.output_dir, f"job_trends_{date_str}.json")

            with open(output_file, 'w') as f:
                json.dump(trends, f, indent=2)

            logger.info(f"Saved processed Anthropic data to {output_file}")

            return trends

        except Exception as e:
            logger.error(f"Error processing Anthropic data: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

def main():
    parser = argparse.ArgumentParser(description='Process Anthropic Economic Index data (v2 format)')
    parser.add_argument('--year', type=int, help='Year to process (e.g., 2025)')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    parser.add_argument('--input-dir', type=str, default='./data/raw/anthropic_index',
                       help='Input directory containing Anthropic data files')
    parser.add_argument('--output-dir', type=str, default='./data/processed',
                       help='Output directory for processed data')

    args = parser.parse_args()

    processor = AnthropicIndexProcessorV2(args.input_dir, args.output_dir)

    # Process the data
    trends = processor.process_anthropic_data(args.year, args.month)

    if trends:
        logger.info("Processing completed successfully")
        logger.info(f"US Automation Rate: {trends['statistics'].get('us_automation_rate', 'N/A')}%")
        logger.info(f"US Augmentation Rate: {trends['statistics'].get('us_augmentation_rate', 'N/A')}%")
    else:
        logger.error("Processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main()