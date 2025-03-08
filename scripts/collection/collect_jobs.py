# scripts/collection/collect_jobs.py
import json
import logging
import os
import sys
from datetime import datetime

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
    
    def collect_jobs(self, categories=None):
        """
        Collect job listings from the Remote Jobs API.
        
        Args:
            categories (list): List of job categories to collect
            
        Returns:
            dict: Collection results
        """
        results = {
            "timestamp": datetime.now().isoformat(),
            "jobs_collected": 0,
            "ai_jobs_found": 0,
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
                    jobs = data.get("jobs", [])
                    
                    logger.info(f"Found {len(jobs)} jobs for category {category}")
                    
                    # Save all jobs
                    date_str = datetime.now().strftime("%Y%m%d")
                    filename = f"{date_str}_{category}_jobs.json"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    with open(filepath, 'w') as f:
                        json.dump(data, f, indent=2)
                    
                    # Identify AI-related jobs
                    ai_terms = ["ai", "artificial intelligence", "machine learning", "ml", 
                               "deep learning", "nlp", "computer vision", "language model"]
                    
                    ai_jobs = []
                    for job in jobs:
                        title = job.get("title", "").lower()
                        description = job.get("description", "").lower()
                        company = job.get("company_name", "").lower()
                        
                        # Check if any AI term is in title, description, or company name
                        if any(term in title or term in description or term in company 
                               for term in ai_terms):
                            ai_jobs.append(job)
                    
                    # Save AI-related jobs
                    if ai_jobs:
                        ai_filename = f"{date_str}_{category}_ai_jobs.json"
                        ai_filepath = os.path.join(self.output_dir, ai_filename)
                        
                        with open(ai_filepath, 'w') as f:
                            json.dump({
                                "count": len(ai_jobs),
                                "jobs": ai_jobs
                            }, f, indent=2)
                        
                        results["ai_jobs_found"] += len(ai_jobs)
                        results["files_created"].append(ai_filepath)
                        logger.info(f"Found {len(ai_jobs)} AI-related jobs in {category}")
                    
                    results["jobs_collected"] += len(jobs)
                    results["files_created"].append(filepath)
                    logger.info(f"Saved {len(jobs)} jobs to {filepath}")
                
                else:
                    logger.error(f"API returned status code {response.status_code}")
                    logger.error(f"Response: {response.text}")
            
            except Exception as e:
                logger.error(f"Error collecting jobs for category {category}: {str(e)}")
        
        return results


def main():
    collector = JobsCollector()
    results = collector.collect_jobs()
    
    logger.info(f"Collection complete. Collected {results['jobs_collected']} total jobs.")
    logger.info(f"Found {results['ai_jobs_found']} AI-related jobs.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()