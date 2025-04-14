# scripts/processing/process_jobs.py
# 
# DEPRECATED: This module is maintained for backwards compatibility but is no longer 
# the primary processor for job trends. The Anthropic Economic Index processor 
# (see process_anthropic_index.py) is now used to generate job trends data.
#
import json
import logging
import os
import sys
import argparse
from datetime import datetime
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("jobs_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("jobs-processor")

class JobsProcessor:
    def __init__(self, input_dir="./data/raw/jobs", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def process_job_data(self, year=None, month=None):
        """Process job postings data and identify trends."""
        # Find all job files
        job_files = glob.glob(os.path.join(self.input_dir, "jobs_*_*.json"))
        
        if not job_files:
            logger.warning(f"No job files found in {self.input_dir}")
            return None
        
        logger.info(f"Found {len(job_files)} AI job files")
        
        # Get most recent file for each category
        categories = {}
        for file_path in job_files:
            filename = os.path.basename(file_path)
            parts = filename.split('_')
            if len(parts) >= 3:
                date = parts[0]
                category = parts[1]
                
                if category not in categories or date > categories[category]["date"]:
                    categories[category] = {
                        "date": date,
                        "path": file_path
                    }
        
        # Load and process AI jobs
        ai_jobs = []
        job_titles = {}
        
        for category, info in categories.items():
            try:
                with open(info["path"], 'r') as f:
                    data = json.load(f)
                    jobs = data.get("jobs", [])
                    count = data.get("count", len(jobs))
                    
                    ai_jobs.append({
                        "category": category,
                        "date": info["date"],
                        "count": count
                    })
                    
                    # Count job titles
                    for job in jobs:
                        title = job.get("title", "").lower()
                        if title:
                            # Normalize common titles
                            if "data scientist" in title:
                                norm_title = "Data Scientist"
                            elif "machine learning" in title and "engineer" in title:
                                norm_title = "Machine Learning Engineer"
                            elif "ai engineer" in title or "artificial intelligence engineer" in title:
                                norm_title = "AI Engineer"
                            elif "data engineer" in title:
                                norm_title = "Data Engineer"
                            elif "prompt engineer" in title:
                                norm_title = "Prompt Engineer"
                            elif "nlp" in title:
                                norm_title = "NLP Specialist"
                            else:
                                # Skip unusual titles
                                continue
                            
                            if norm_title in job_titles:
                                job_titles[norm_title] += 1
                            else:
                                job_titles[norm_title] = 1
                    
                    logger.info(f"Processed {count} AI jobs from {category} category")
            
            except Exception as e:
                logger.error(f"Error loading file {info['path']}: {str(e)}")
        
        # Check for previous month's data (simulated for demo)
        # In a real implementation, would compare with historical data
        previous_month_counts = {
            "software-dev": 185,
            "data": 110,
            "product": 75,
            "all-others": 42
        }
        
        # Calculate growth rates for job titles (simulated)
        growth_rates = {
            "Prompt Engineer": 96,
            "AI Engineer": 68,
            "Machine Learning Engineer": 54,
            "NLP Specialist": 48,
            "Data Scientist": 32,
            "Data Engineer": 26
        }
        
        # Create trends object
        current_total = sum(j["count"] for j in ai_jobs)
        previous_total = sum(previous_month_counts.values())
        
        trends = {
            "date_analyzed": datetime.now().isoformat(),
            "ai_related_postings": [
                {"date": "previous_month", "count": previous_total},
                {"date": "current_month", "count": current_total}
            ],
            "growth_rate": ((current_total - previous_total) / previous_total * 100) if previous_total > 0 else 0,
            "top_job_titles": sorted(job_titles.items(), key=lambda x: x[1], reverse=True)[:10],
            "top_growing_titles": [
                {"title": title, "count": job_titles.get(title, 0), "growth_rate": rate}
                for title, rate in sorted(growth_rates.items(), key=lambda x: x[1], reverse=True)
            ]
        }
        
        # Determine output filename based on year and month parameters
        if year and month:
            # Format as YYYYMM for historical data
            date_str = f"{year}{month:02d}"
        else:
            # Keep existing format for current data
            date_str = datetime.now().strftime('%Y%m%d')
            
        output_file = os.path.join(
            self.output_dir,
            f"job_trends_{date_str}.json"
        )
        
        with open(output_file, 'w') as f:
            json.dump(trends, f, indent=2)
        
        logger.info(f"Saved job trends to {output_file}")
        
        return trends


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process job posting data')
    parser.add_argument('--year', type=int, help='Year to process (YYYY)')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    args = parser.parse_args()
    
    processor = JobsProcessor()
    trends = processor.process_job_data(args.year, args.month)
    
    if trends:
        logger.info(f"Processing complete. Analyzed {trends['ai_related_postings'][-1]['count']} AI-related jobs.")
        logger.info(f"Overall growth rate: {trends['growth_rate']:.2f}%")
    else:
        logger.error("Processing failed or no data was found.")


if __name__ == "__main__":
    main()