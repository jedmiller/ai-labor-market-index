#!/usr/bin/env python3
# test_historical_month.py
# Script to test the historical collection for a single month

import argparse
import logging
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("historical_test.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("historical-test")

def run_script(script_path, year, month):
    """Run a script with year and month parameters."""
    logger.info(f"Running {script_path} for {year}-{month:02d}")
    
    try:
        # Run the script as a subprocess
        result = subprocess.run(
            [sys.executable, script_path, f"--year={year}", f"--month={month}"],
            check=True,
            capture_output=True,
            text=True
        )
        
        # Log the output
        logger.info(f"{script_path} output: {result.stdout}")
        if result.stderr:
            logger.warning(f"{script_path} errors: {result.stderr}")
            
        return result.returncode == 0
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_path}: {e.stderr}")
        return False

def check_output_files(year, month, base_dir="./data/processed"):
    """Check if the output files were created with the correct naming."""
    date_str = f"{year}{month:02d}"
    
    expected_files = [
        f"research_trends_{date_str}.json",
        f"employment_stats_{date_str}.json",
        f"job_trends_{date_str}.json",
        f"workforce_events_{date_str}.json",
        f"ai_labor_index_{date_str}.json"
    ]
    
    found_files = []
    missing_files = []
    
    for file in expected_files:
        path = os.path.join(base_dir, file)
        if os.path.exists(path):
            found_files.append(file)
        else:
            missing_files.append(file)
    
    return found_files, missing_files

def main():
    parser = argparse.ArgumentParser(description='Test historical data collection for a single month')
    parser.add_argument('--year', type=int, required=True, help='Year to test')
    parser.add_argument('--month', type=int, required=True, help='Month to test')
    
    args = parser.parse_args()
    
    year = args.year
    month = args.month
    
    logger.info(f"Testing historical data collection for {year}-{month:02d}")
    
    # Step 1: Collection scripts
    collectors = [
        "scripts/collection/collect_arxiv.py",
        "scripts/collection/collect_bls.py",
        "scripts/collection/collect_jobs.py",
        "scripts/collection/collect_news.py"
    ]
    
    for collector in collectors:
        success = run_script(collector, year, month)
        if not success:
            logger.error(f"Failed to run {collector}")
    
    # Step 2: Processing scripts
    processors = [
        "scripts/processing/process_research.py",
        "scripts/processing/process_employment.py",
        "scripts/processing/process_jobs.py",
        "scripts/processing/process_news.py"
    ]
    
    for processor in processors:
        success = run_script(processor, year, month)
        if not success:
            logger.error(f"Failed to run {processor}")
    
    # Step 3: Calculate index
    success = run_script("scripts/analysis/calculate_index.py", year, month)
    if not success:
        logger.error("Failed to calculate index")
    
    # Step 4: Check output files
    found_files, missing_files = check_output_files(year, month)
    
    if missing_files:
        logger.warning(f"Missing expected output files: {', '.join(missing_files)}")
    
    if found_files:
        logger.info(f"Found expected output files: {', '.join(found_files)}")
    
    # Step 5: Check if index history was updated
    history_file = "./data/processed/index_history.json"
    if os.path.exists(history_file):
        logger.info(f"Index history file exists: {history_file}")
    else:
        logger.error(f"Index history file not found: {history_file}")
    
    logger.info("Test complete")

if __name__ == "__main__":
    main()