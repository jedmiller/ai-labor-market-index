#!/usr/bin/env python3
# scripts/run_updated_ai_impact.py
"""
AI Labor Market Impact - Main Workflow Script

This script coordinates the full workflow for calculating the updated AI Labor Market Impact:
1. Collects required data from various sources
2. Processes the data into standardized formats
3. Calculates the impact using the updated methodology
4. Generates projections and confidence intervals
5. Outputs the results to the specified directories

The new methodology uses a component-based approach:
Net Employment Impact = Employment × [1 - Displacement Effect + Creation Effect × Market_Maturity + Demand Effect]
"""
import os
import sys
import logging
import argparse
import subprocess
import json
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ai_impact_workflow.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("ai-impact-workflow")

class AIImpactWorkflow:
    """
    Coordinates the workflow for calculating the AI Labor Market Impact using the updated methodology.
    """
    def __init__(self, 
                 base_dir=".",
                 raw_dir="./data/raw",
                 processed_dir="./data/processed",
                 projections_dir="./data/processed/projections",
                 scripts_dir="./scripts",
                 year=None,
                 month=None,
                 use_simulation=False,
                 generate_projections=True,
                 generate_confidence=True,
                 projection_years=5):
        self.base_dir = base_dir
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.projections_dir = projections_dir
        self.scripts_dir = scripts_dir
        self.year = year
        self.month = month
        self.use_simulation = use_simulation
        self.generate_projections = generate_projections
        self.generate_confidence = generate_confidence
        self.projection_years = projection_years
        
        # Create directories if they don't exist
        os.makedirs(os.path.join(raw_dir, "jobs"), exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        os.makedirs(projections_dir, exist_ok=True)
        
        # Format date string for filenames
        if year and month:
            self.date_str = f"{year}{month:02d}"
        else:
            self.date_str = datetime.now().strftime('%Y%m%d')

    def run_script(self, script_path, args, description):
        """
        Run a Python script with the specified arguments.
        """
        # Construct the full command
        cmd = [sys.executable, script_path] + args
        
        logger.info(f"Running {description}: {' '.join(cmd)}")
        
        try:
            # Run the script and capture output
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            
            # Log the output
            if result.stdout:
                for line in result.stdout.splitlines():
                    if line.strip():
                        logger.info(f"  {line}")
            
            logger.info(f"Successfully completed {description}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running {description}: {e}")
            if e.stdout:
                for line in e.stdout.splitlines():
                    if line.strip():
                        logger.info(f"  {line}")
            if e.stderr:
                for line in e.stderr.splitlines():
                    if line.strip():
                        logger.error(f"  {line}")
            return False

    def collect_data(self):
        """
        Collect all required data for the impact calculation.
        """
        success = True
        
        # 1. Collect BLS employment data
        bls_script = os.path.join(self.scripts_dir, "collection", "collect_bls.py")
        bls_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--output", os.path.join(self.raw_dir, "bls")
        ]
        if not self.run_script(bls_script, bls_args, "BLS data collection"):
            logger.error("Failed to collect BLS data, this is required for calculation")
            success = False
        
        # 2. Collect Anthropic Index data
        anthropic_script = os.path.join(self.scripts_dir, "collection", "collect_anthropic_index.py")
        anthropic_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--output", os.path.join(self.raw_dir, "anthropic_index")
        ]
        if self.use_simulation:
            anthropic_args.append("--simulation=yes")
        
        if not self.run_script(anthropic_script, anthropic_args, "Anthropic Index collection"):
            logger.warning("Failed to collect Anthropic Index data, calculation will use default values")
        
        # 3. Collect AI job postings data (new)
        jobs_script = os.path.join(self.scripts_dir, "collection", "collect_ai_jobs.py")
        jobs_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--output", os.path.join(self.raw_dir, "jobs")
        ]
        if self.use_simulation:
            jobs_args.append("--simulate")
        
        if not self.run_script(jobs_script, jobs_args, "AI jobs collection"):
            logger.warning("Failed to collect AI jobs data, calculation will use default values")
        
        # 4. Collect news/events data
        news_script = os.path.join(self.scripts_dir, "collection", "collect_news.py")
        news_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--output", os.path.join(self.raw_dir, "news")
        ]
        
        if not self.run_script(news_script, news_args, "News collection"):
            logger.warning("Failed to collect news data, calculation will use default values")
        
        # 5. Collect research trends data
        research_script = os.path.join(self.scripts_dir, "collection", "collect_arxiv.py")
        research_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--output", os.path.join(self.raw_dir, "arxiv")
        ]
        
        if not self.run_script(research_script, research_args, "Research data collection"):
            logger.warning("Failed to collect research data, calculation will use default values")
        
        return success

    def process_data(self):
        """
        Process the collected data into standardized formats.
        """
        success = True
        
        # 1. Process employment data
        employment_script = os.path.join(self.scripts_dir, "processing", "process_employment.py")
        employment_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--input", os.path.join(self.raw_dir, "bls"),
            "--output", self.processed_dir
        ]
        
        if not self.run_script(employment_script, employment_args, "Employment data processing"):
            logger.error("Failed to process employment data, this is required for calculation")
            success = False
        
        # 2. Process Anthropic Index data
        # Check if we have new format data (August 2025+)
        new_format_file = os.path.join(self.raw_dir, "anthropic_index",
                                      f"anthropic_index_{self.year}_{self.month:02d}_occupations.json")

        if os.path.exists(new_format_file):
            # Use v2 processor for new format
            anthropic_script = os.path.join(self.scripts_dir, "processing", "process_anthropic_index_v2.py")
            logger.info("Using v2 processor for new Anthropic data format")
        else:
            # Use original processor for old format
            anthropic_script = os.path.join(self.scripts_dir, "processing", "process_anthropic_index.py")
            logger.info("Using original processor for legacy Anthropic data format")

        anthropic_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--input", os.path.join(self.raw_dir, "anthropic_index"),
            "--output", self.processed_dir
        ]

        if not self.run_script(anthropic_script, anthropic_args, "Anthropic Index processing"):
            logger.warning("Failed to process Anthropic Index data, calculation will use default values")
        
        # 3. Process news/events data
        news_script = os.path.join(self.scripts_dir, "processing", "process_news.py")
        news_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--input", os.path.join(self.raw_dir, "news"),
            "--output", self.processed_dir
        ]
        
        if not self.run_script(news_script, news_args, "News processing"):
            logger.warning("Failed to process news data, calculation will use default values")
        
        # 4. Process research data
        research_script = os.path.join(self.scripts_dir, "processing", "process_research.py")
        research_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--input", os.path.join(self.raw_dir, "arxiv"),
            "--output", self.processed_dir
        ]
        
        if not self.run_script(research_script, research_args, "Research data processing"):
            logger.warning("Failed to process research data, calculation will use default values")
        
        return success

    def calculate_impact(self):
        """
        Calculate the AI Labor Market Impact using the updated methodology.
        """
        # Run the impact calculation script
        impact_script = os.path.join(self.scripts_dir, "analysis", "calculate_ai_impact.py")
        impact_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--input-dir", self.processed_dir,
            "--output-dir", self.processed_dir
        ]
        
        if not self.run_script(impact_script, impact_args, "AI Impact calculation"):
            logger.error("Failed to calculate AI Impact")
            return False
        
        # Also run the traditional index calculation for comparison
        index_script = os.path.join(self.scripts_dir, "analysis", "calculate_index.py")
        index_args = [
            "--year", str(self.year),
            "--month", str(self.month),
            "--input-dir", self.processed_dir,
            "--output-dir", self.processed_dir
        ]
        
        if not self.run_script(index_script, index_args, "Traditional index calculation"):
            logger.warning("Failed to calculate traditional index (for comparison)")
        
        return True

    def generate_projections_and_confidence(self):
        """
        Generate projections and confidence intervals for future impacts.
        """
        success = True
        
        # Generate projections if requested
        if self.generate_projections:
            projection_script = os.path.join(self.scripts_dir, "analysis", "project_impact.py")
            projection_args = [
                "--input-dir", self.processed_dir,
                "--output-dir", self.projections_dir,
                "--impact-file", f"ai_labor_impact_{self.date_str}.json",
                "--years", str(self.projection_years)
            ]
            
            if not self.run_script(projection_script, projection_args, "Impact projections"):
                logger.error("Failed to generate impact projections")
                success = False
        
        # Generate confidence intervals if requested
        if self.generate_confidence:
            confidence_script = os.path.join(self.scripts_dir, "analysis", "confidence_intervals.py")
            confidence_args = [
                "--input-dir", self.processed_dir,
                "--output-dir", self.projections_dir,
                "--impact-file", f"ai_labor_impact_{self.date_str}.json",
                "--years", str(self.projection_years),
                "--simulations", "1000"  # 1000 Monte Carlo simulations
            ]
            
            if not self.run_script(confidence_script, confidence_args, "Confidence intervals"):
                logger.error("Failed to generate confidence intervals")
                success = False
        
        return success

    def compare_methodologies(self):
        """
        Compare the results of the traditional and updated methodologies.
        """
        # Load the results from both calculations
        traditional_file = os.path.join(self.processed_dir, f"ai_labor_index_{self.date_str}.json")
        updated_file = os.path.join(self.processed_dir, f"ai_labor_impact_{self.date_str}.json")
        
        try:
            with open(traditional_file, 'r') as f:
                traditional = json.load(f)
                
            with open(updated_file, 'r') as f:
                updated = json.load(f)
                
            # Extract key metrics for comparison
            traditional_value = traditional.get("index_value", 0)
            updated_value = updated.get("total_impact", 0) * 100  # Convert to comparable scale
            
            # Log comparison
            logger.info("Methodology Comparison:")
            logger.info(f"  Traditional Index Value: {traditional_value:.2f}")
            logger.info(f"  Updated Impact Value: {updated_value:.2f}%")
            logger.info(f"  Difference: {abs(traditional_value - updated_value):.2f}")
            
            # Save comparison to file
            comparison = {
                "date": f"{self.year}-{self.month:02d}",
                "traditional_index": traditional_value,
                "updated_impact": updated_value,
                "traditional_interpretation": traditional.get("interpretation", ""),
                "updated_components": updated.get("components", {}),
                "methodology_notes": {
                    "traditional": "Weighted combination of news events, research trends, employment stats, and job trends",
                    "updated": "Component-based calculation with displacement, creation, market maturity, and demand effects"
                }
            }
            
            comparison_file = os.path.join(self.processed_dir, f"methodology_comparison_{self.date_str}.json")
            with open(comparison_file, 'w') as f:
                json.dump(comparison, f, indent=2)
                
            logger.info(f"Saved methodology comparison to {comparison_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error comparing methodologies: {str(e)}")
            return False

    def run_workflow(self):
        """
        Run the full workflow from data collection to impact calculation and projections.
        """
        logger.info(f"Starting AI Labor Market Impact workflow for {self.year}-{self.month:02d}")
        
        # Step 1: Collect data
        logger.info("Step 1: Collecting data")
        if not self.collect_data():
            logger.error("Data collection failed, cannot continue")
            return False
        
        # Step 2: Process data
        logger.info("Step 2: Processing data")
        if not self.process_data():
            logger.error("Data processing failed, cannot continue")
            return False
        
        # Step 3: Calculate impact
        logger.info("Step 3: Calculating AI Labor Market Impact")
        if not self.calculate_impact():
            logger.error("Impact calculation failed, cannot continue")
            return False
        
        # Step 4: Generate projections and confidence intervals (if requested)
        if self.generate_projections or self.generate_confidence:
            logger.info("Step 4: Generating projections and confidence intervals")
            if not self.generate_projections_and_confidence():
                logger.warning("Projections or confidence intervals failed")
        
        # Step 5: Compare methodologies
        logger.info("Step 5: Comparing methodologies")
        self.compare_methodologies()
        
        logger.info(f"AI Labor Market Impact workflow completed for {self.year}-{self.month:02d}")
        return True


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Run the AI Labor Market Impact workflow')
    parser.add_argument('--year', type=int, required=True, help='Year to calculate impact for')
    parser.add_argument('--month', type=int, required=True, help='Month to calculate impact for')
    parser.add_argument('--base-dir', default='.', help='Base directory for the project')
    parser.add_argument('--raw-dir', help='Directory for raw data (default: ./data/raw)')
    parser.add_argument('--processed-dir', help='Directory for processed data (default: ./data/processed)')
    parser.add_argument('--projections-dir', help='Directory for projections (default: ./data/processed/projections)')
    parser.add_argument('--scripts-dir', help='Directory containing scripts (default: ./scripts)')
    parser.add_argument('--simulate', action='store_true', help='Use simulation mode for data sources')
    parser.add_argument('--no-projections', action='store_true', help='Skip generating projections')
    parser.add_argument('--no-confidence', action='store_true', help='Skip generating confidence intervals')
    parser.add_argument('--projection-years', type=int, default=5, help='Number of years to project (default: 5)')
    
    args = parser.parse_args()
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error(f"Invalid month: {args.month}. Month must be between 1 and 12.")
        return 1
    
    # Set up default paths if not provided
    base_dir = args.base_dir
    raw_dir = args.raw_dir or os.path.join(base_dir, "data", "raw")
    processed_dir = args.processed_dir or os.path.join(base_dir, "data", "processed")
    projections_dir = args.projections_dir or os.path.join(processed_dir, "projections")
    scripts_dir = args.scripts_dir or os.path.join(base_dir, "scripts")
    
    # Initialize workflow
    workflow = AIImpactWorkflow(
        base_dir=base_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        projections_dir=projections_dir,
        scripts_dir=scripts_dir,
        year=args.year,
        month=args.month,
        use_simulation=args.simulate,
        generate_projections=not args.no_projections,
        generate_confidence=not args.no_confidence,
        projection_years=args.projection_years
    )
    
    # Run workflow
    if workflow.run_workflow():
        logger.info("Workflow completed successfully")
        return 0
    else:
        logger.error("Workflow failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())