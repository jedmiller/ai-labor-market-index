#!/usr/bin/env python3
# scripts/sync_github_data.py
# Syncs the latest data files from GitHub before running the index calculation

import os
import sys
import requests
import json
import logging
import subprocess
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("github_sync.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("github-sync")

# GitHub Repository Info - Update with your actual repository information
GITHUB_USER = "jedmiller"
GITHUB_REPO = "ai-labor-market-index"
GITHUB_BRANCH = "main"

# File patterns to sync - now with year/month placeholders
DATA_FILES = [
    "data/processed/job_trends_{year}_{month:02d}_*.json",
    "data/processed/employment_stats_{year}_{month:02d}_*.json",
    "data/processed/research_trends_{year}_{month:02d}_*.json",
    "data/processed/workforce_events_{year}_{month:02d}_*.json",
    "data/processed/ai_jobs_{year}_{month:02d}_*.json",
    "data/processed/news_{year}_{month:02d}_*.json",
    "data/processed/arxiv_{year}_{month:02d}_*.json",
    "data/processed/index_history.json"
]

def get_github_directory_contents(path):
    """Get contents of a directory in the GitHub repository."""
    url = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/{path}?ref={GITHUB_BRANCH}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching GitHub directory contents: {e}")
        return []

def download_file(github_path, local_path):
    """Download a file from GitHub."""
    url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{github_path}"
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        response = requests.get(url)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Downloaded {github_path} to {local_path}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {github_path}: {e}")
        return False

def sync_data_files(year=None, month=None):
    """
    Sync data files from GitHub repository for a specific month.
    
    Args:
        year (int): Target year (defaults to current)
        month (int): Target month (defaults to current)
    """
    # Set year and month to current if not specified
    if year is None or month is None:
        today = datetime.now()
        year = year or today.year
        month = month or today.month
    
    logger.info(f"Starting GitHub data sync for {year}-{month:02d}")
    
    # Get contents of the processed data directory
    contents = get_github_directory_contents("data/processed")
    
    if not contents:
        logger.error("Failed to get repository contents or directory is empty")
        return False
    
    # Format file patterns with year and month
    formatted_patterns = []
    for pattern in DATA_FILES:
        if "{year}" in pattern and "{month:02d}" in pattern:
            formatted_patterns.append(pattern.format(year=year, month=month))
        else:
            formatted_patterns.append(pattern)
    
    # Download each file that matches our patterns
    downloaded_files = 0
    
    for item in contents:
        if item.get("type") != "file":
            continue
        
        filename = item.get("name")
        
        # Check if file matches any of our patterns
        for pattern in formatted_patterns:
            if "*" in pattern:
                prefix = pattern.split("*")[0]
                suffix = pattern.split("*")[1] if len(pattern.split("*")) > 1 else ""
                
                if filename.startswith(prefix) and filename.endswith(suffix):
                    github_path = item.get("path")
                    local_path = github_path  # Use the same path locally
                    
                    if download_file(github_path, local_path):
                        downloaded_files += 1
                    break
            elif filename == pattern.split("/")[-1]:
                github_path = item.get("path")
                local_path = github_path  # Use the same path locally
                
                if download_file(github_path, local_path):
                    downloaded_files += 1
                break
    
    logger.info(f"Downloaded {downloaded_files} files from GitHub for {year}-{month:02d}")
    return downloaded_files > 0

def collect_monthly_data(year, month):
    """Collect data for the specified month using individual collection scripts."""
    logger.info(f"Starting data collection for {year}-{month:02d}")
    
    # API key (assuming it's stored in env var)
    news_api_key = os.environ.get("NEWS_API_KEY")
    
    try:
        # Collect news articles
        logger.info("Running news collection...")
        news_cmd = ["python3", "scripts/collection/collect_news.py", 
                   f"--year={year}", f"--month={month}"]
        if news_api_key:
            news_cmd.append(f"--api-key={news_api_key}")
            
        news_result = subprocess.run(
            news_cmd,
            check=True, capture_output=True, text=True
        )
        logger.info(f"News collection output: {news_result.stdout.strip()}")
        
        # Collect research papers
        logger.info("Running ArXiv collection...")
        arxiv_cmd = ["python3", "scripts/collection/collect_arxiv.py", 
                    f"--year={year}", f"--month={month}"]
        arxiv_result = subprocess.run(
            arxiv_cmd,
            check=True, capture_output=True, text=True
        )
        logger.info(f"ArXiv collection output: {arxiv_result.stdout.strip()}")
        
        # Collect job postings
        logger.info("Running jobs collection...")
        jobs_cmd = ["python3", "scripts/collection/collect_jobs.py", 
                   f"--year={year}", f"--month={month}"]
        jobs_result = subprocess.run(
            jobs_cmd,
            check=True, capture_output=True, text=True
        )
        logger.info(f"Jobs collection output: {jobs_result.stdout.strip()}")
        
        logger.info(f"All data collection tasks completed for {year}-{month:02d}")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Data collection failed with exit code {e.returncode}")
        logger.error(f"Stderr: {e.stderr}")
        return False

def run_index_calculation(year=None, month=None):
    """Run the index calculation script for the specified month."""
    cmd = ["python3", "scripts/analysis/calculate_index.py"]
    
    # Add year and month parameters if provided
    if year is not None and month is not None:
        cmd.extend([f"--year={year}", f"--month={month}"])
    
    logger.info(f"Running index calculation script: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info("Index calculation output:")
        logger.info(result.stdout)
        
        if result.stderr:
            logger.warning("Errors/warnings during calculation:")
            logger.warning(result.stderr)
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Index calculation failed with exit code {e.returncode}")
        logger.error(f"Stderr: {e.stderr}")
        return False

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Sync data and calculate AI Labor Market Index')
    parser.add_argument('--year', type=int, help='Target year')
    parser.add_argument('--month', type=int, help='Target month (1-12)')
    parser.add_argument('--collect', action='store_true', help='Collect new data (otherwise just sync existing data)')
    parser.add_argument('--skip-sync', action='store_true', help='Skip syncing data from GitHub')
    parser.add_argument('--skip-calculation', action='store_true', help='Skip index calculation')
    args = parser.parse_args()
    
    # Default to current month if not specified
    if args.year is None or args.month is None:
        today = datetime.now()
        args.year = args.year or today.year
        args.month = args.month or today.month
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error(f"Invalid month: {args.month}. Must be between 1 and 12.")
        sys.exit(1)
    
    # Collect data for the month if requested
    if args.collect:
        if not collect_monthly_data(args.year, args.month):
            logger.error("Data collection failed")
            sys.exit(1)
    
    # Sync data files from GitHub (unless skipped)
    if not args.skip_sync:
        if not sync_data_files(args.year, args.month):
            logger.error("Failed to sync data files from GitHub")
            sys.exit(1)
    
    # Run the index calculation (unless skipped)
    if not args.skip_calculation:
        if not run_index_calculation(args.year, args.month):
            logger.error("Index calculation failed")
            sys.exit(1)