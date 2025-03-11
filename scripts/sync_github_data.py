#!/usr/bin/env python3
# scripts/sync_github_data.py
# Syncs the latest data files from GitHub before running the index calculation

import os
import sys
import requests
import json
import logging
import subprocess
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

# File patterns to sync
DATA_FILES = [
    "data/processed/job_trends_*.json",
    "data/processed/employment_stats_*.json",
    "data/processed/research_trends_*.json",
    "data/processed/workforce_events_*.json",
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

def sync_data_files():
    """Sync data files from GitHub repository."""
    logger.info("Starting GitHub data sync")
    
    # Get contents of the processed data directory
    contents = get_github_directory_contents("data/processed")
    
    if not contents:
        logger.error("Failed to get repository contents or directory is empty")
        return False
    
    # Download each file that matches our patterns
    downloaded_files = 0
    
    for item in contents:
        if item.get("type") != "file":
            continue
        
        filename = item.get("name")
        
        # Check if file matches any of our patterns
        if any(filename.startswith(pattern.split("*")[0]) and filename.endswith(pattern.split("*")[1]) 
               for pattern in DATA_FILES if "*" in pattern):
            
            github_path = item.get("path")
            local_path = github_path  # Use the same path locally
            
            if download_file(github_path, local_path):
                downloaded_files += 1
        
        # Check for exact matches like index_history.json
        elif any(filename == pattern.split("/")[-1] for pattern in DATA_FILES if "*" not in pattern):
            github_path = item.get("path")
            local_path = github_path  # Use the same path locally
            
            if download_file(github_path, local_path):
                downloaded_files += 1
    
    logger.info(f"Downloaded {downloaded_files} files from GitHub")
    return downloaded_files > 0

def run_index_calculation():
    """Run the index calculation script."""
    logger.info("Running index calculation script")
    
    try:
        result = subprocess.run(
            ["python", "scripts/analysis/calculate_index.py"],
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
    # Sync data files from GitHub
    if sync_data_files():
        # Run the index calculation
        run_index_calculation()
    else:
        logger.error("Failed to sync data files from GitHub. Aborting index calculation.")
        sys.exit(1)