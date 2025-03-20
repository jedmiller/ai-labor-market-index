# scripts/collection/collect_jobs.py
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
    
    def collect_jobs(self, categories=None, year=None, month=None):
        """
        Collect job listings from the Remote Jobs API for a specific month.
        
        Args:
            categories (list): List of job categories to collect
            year (int): Target year (defaults to current year)
            month (int): Target month (defaults to current month)
            
        Returns:
            dict: Collection results
        """
        # Set year and month to current if not specified
        if year is None or month is None:
            today = datetime.now()
            year = year or today.year
            month = month or today.month
        
        # Calculate start and end dates for the month
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        logger.info(f"Collecting jobs for period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "target_period": f"{year}-{month:02d}",
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            "jobs_collected": 0,
            "jobs_in_period": 0,
            "ai_jobs_found": 0,
            "ai_jobs_in_period": 0,
            "files_created": []
        }
        
        # Default categories if none provided
        if not categories:
            categories = ["software-dev", "data", "product", "all-others"]
        
        for category in categories:
            logger.info(f"Collecting jobs for category: {category}")
            
            params = {"category": category} if category != "all-others" else {}
            
            try:
                response = requests.get(
                    self.api_url,
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    all_jobs = data.get("jobs", [])
                    
                    logger.info(f"Found {len(all_jobs)} jobs for category {category}")
                    
                    # Filter jobs by publication date
                    jobs_in_period = []
                    for job in all_jobs:
                        # Extract publication date - format depends on the API
                        pub_date_str = job.get("publication_date") or job.get("date") or job.get("created_at")
                        
                        if pub_date_str:
                            try:
                                # Try different date formats
                                try:
                                    pub_date = datetime.strptime(pub_date_str, "%Y-%m-%dT%H:%M:%S")
                                except ValueError:
                                    try:
                                        pub_date = datetime.strptime(pub_date_str, "%Y-%m-%d")
                                    except ValueError:
                                        pub_date = datetime.strptime(pub_date_str[:10], "%Y-%m-%d")
                                
                                # Include only jobs from the target month
                                if start_date <= pub_date <= end_date:
                                    jobs_in_period.append(job)
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Could not parse date: {pub_date_str} - {str(e)}")
                                # If we can't parse the date, include the job to be safe
                                jobs_in_period.append(job)
                        else:
                            # If no date field is found, include the job to be safe
                            jobs_in_period.append(job)
                    
                    logger.info(f"Filtered {len(jobs_in_period)} jobs from {len(all_jobs)} for {year}-{month:02d}")
                    
                    # Save filtered jobs
                    filename = f"jobs_{year}_{month:02d}_{category}.json"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    with open(filepath, 'w') as f:
                        json.dump({
                            "category": category,
                            "target_period": f"{year}-{month:02d}",
                            "date_collected": datetime.now().isoformat(),
                            "count": len(jobs_in_period),
                            "total_collected": len(all_jobs),
                            "jobs": jobs_in_period
                        }, f, indent=2)
                    
                    # Identify AI-related jobs
                    ai_terms = ["ai", "artificial intelligence", "machine learning", "ml", 
                               "deep learning", "nlp", "computer vision", "language model"]
                    
                    ai_jobs = []
                    for job in jobs_in_period:
                        title = job.get("title", "").lower()
                        description = job.get("description", "").lower()
                        company = job.get("company_name", "").lower()
                        
                        # Check if any AI term is in title, description, or company name
                        if any(term in title or term in description or term in company 
                               for term in ai_terms):
                            ai_jobs.append(job)
                    
                    # Save AI-related jobs
                    if ai_jobs:
                        ai_filename = f"ai_jobs_{year}_{month:02d}_{category}.json"
                        ai_filepath = os.path.join(self.output_dir, ai_filename)
                        
                        with open(ai_filepath, 'w') as f:
                            json.dump({
                                "category": category,
                                "target_period": f"{year}-{month:02d}",
                                "date_collected": datetime.now().isoformat(),
                                "count": len(ai_jobs),
                                "jobs": ai_jobs
                            }, f, indent=2)
                        
                        results["ai_jobs_found"] += len(ai_jobs)
                        results["ai_jobs_in_period"] += len(ai_jobs)
                        results["files_created"].append(ai_filepath)
                        logger.info(f"Found {len(ai_jobs)} AI-related jobs in {category} for {year}-{month:02d}")
                    
                    results["jobs_collected"] += len(all_jobs)
                    results["jobs_in_period"] += len(jobs_in_period)
                    results["files_created"].append(filepath)
                    logger.info(f"Saved {len(jobs_in_period)} jobs to {filepath}")
                
                else:
                    logger.error(f"API returned status code {response.status_code}")
                    logger.error(f"Response: {response.text}")
            
            except Exception as e:
                logger.error(f"Error collecting jobs for category {category}: {str(e)}")
        
        return results


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
    logger.info(f"Filtered to {results['jobs_in_period']} jobs for target month.")
    logger.info(f"Found {results['ai_jobs_in_period']} AI-related jobs in target month.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()