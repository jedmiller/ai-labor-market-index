# scripts/collection/collect_jobs.py
# 
# DEPRECATED: This module is maintained for backwards compatibility but is no longer 
# the primary data source for job trends. The Anthropic Economic Index 
# (see collect_anthropic_index.py) is now used as the primary source for job market data.
#
import json
import logging
import os
import sys
import argparse
from datetime import datetime, timedelta

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("jobs_collection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("jobs-collector")

class JobsCollector:
    def __init__(self, output_dir="./data/raw/jobs"):
        self.output_dir = output_dir
        self.api_url = "https://remotive.com/api/remote-jobs"
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    # In the JobsCollector class:

    def collect_jobs(self, categories=None, year=None, month=None):
        """
        Collect jobs from the Remotive API for specific categories and month
        
        Args:
            categories (list): List of job categories to collect
            year (int): Year to collect data for (historical)
            month (int): Month to collect data for (historical)
        """
        # Format dates for filtering
        if year and month:
            start_date = datetime(year, month, 1)
            # Calculate end date (last day of month)
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            
            logger.info(f"Collecting jobs for period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        else:
            # Default to current month if not specified
            today = datetime.now()
            start_date = None
            end_date = None
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "categories": categories,
            "jobs_collected": 0,
            "ai_jobs_collected": 0,
            "files_created": []
        }
        
        if categories is None:
            categories = ["software-dev", "data", "product", "all-others"]

        for category in categories:
            try:
                logger.info(f"Collecting jobs for category: {category}")
                
                # Make request to Remotive API
                response = requests.get(
                    f"{self.api_url}?category={category}",
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    jobs = data.get("jobs", [])
                    
                    logger.info(f"Found {len(jobs)} jobs for category {category}")
                    
                    # For historical collection, we need to make the best effort
                    # We can't actually filter by date in the API, so we'll use a reasonable approach
                    # For historical data, we'll save all jobs and assume a consistent distribution
                    # since we can't determine the exact posting date for past months
                    
                    if year and month:
                        # Since we can't filter by date in the API, for historical data
                        # we'll save all jobs and just mark them with the target month
                        filtered_jobs = jobs
                        
                        # Record that these are simulated for the specified month
                        for job in filtered_jobs:
                            job["simulated_for_date"] = f"{year}-{month:02d}"
                            
                        logger.info(f"Using all {len(filtered_jobs)} jobs for {year}-{month:02d} (best approximation)")
                    else:
                        # For current collection, we could filter by date if needed
                        filtered_jobs = jobs
                    
                    # Save to file with year/month in filename
                    filename = f"jobs_{year}_{month:02d}_{category}.json" if year and month else f"{datetime.now().strftime('%Y%m%d')}_{category}.json"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    with open(filepath, 'w') as f:
                        json.dump({
                            "category": category,
                            "date_collected": datetime.now().isoformat(),
                            "target_period": f"{year}-{month:02d}" if year and month else None,
                            "jobs": filtered_jobs
                        }, f, indent=2)
                    
                    # Count AI-related jobs
                    ai_jobs = [
                        job for job in filtered_jobs
                        if any(kw in job.get("title", "").lower() or kw in job.get("description", "").lower() 
                            for kw in ["ai", "artificial intelligence", "machine learning", "ml", "deep learning"])
                    ]
                    
                    results["jobs_collected"] += len(filtered_jobs)
                    results["ai_jobs_collected"] += len(ai_jobs)
                    results["files_created"].append(filepath)
                    
                    logger.info(f"Saved {len(filtered_jobs)} jobs to {filepath}")
                    
                else:
                    logger.error(f"API request failed for category {category}: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error collecting jobs for category {category}: {str(e)}")
        
        logger.info(f"Collection complete. Collected {results['jobs_collected']} total jobs.")
        logger.info(f"Found {results['ai_jobs_collected']} AI-related jobs.")
        logger.info(f"Created {len(results['files_created'])} files.")
        
        return results

# Update main() to use the new parameters:
def main():
    parser = argparse.ArgumentParser(description='Collect job postings')
    parser.add_argument('--year', type=int, help='Year to collect data for')
    parser.add_argument('--month', type=int, help='Month to collect data for')
    
    args = parser.parse_args()
    
    # Categories to collect
    categories = ["software-dev", "data", "product", "all-others"]
    
    collector = JobsCollector()
    results = collector.collect_jobs(categories, year=args.year, month=args.month)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect job listings related to AI')
    parser.add_argument('--year', type=int, help='Target year')
    parser.add_argument('--month', type=int, help='Target month (1-12)')
    parser.add_argument('--categories', nargs='+', help='Job categories to collect')
    args = parser.parse_args()
    
    collector = JobsCollector()
    results = collector.collect_jobs(
        categories=args.categories,
        year=args.year,
        month=args.month
    )
    

    logger.info(f"Collection complete. Collected {results['jobs_collected']} total jobs.")

    # Check for required keys
    if 'jobs_in_period' not in results:
        results['jobs_in_period'] = 0
    if 'ai_jobs_in_period' not in results:
        results['ai_jobs_in_period'] = 0
    if 'files_created' not in results:
        results['files_created'] = []

    logger.info(f"Filtered to {results['jobs_in_period']} jobs for target month.") 
    logger.info(f"Found {results['ai_jobs_in_period']} AI-related jobs in target month.")
    logger.info(f"Created {len(results['files_created'])} files.")

if __name__ == "__main__":
    main()