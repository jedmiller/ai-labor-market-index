# scripts/processing/process_anthropic_index.py
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
        logging.FileHandler("anthropic_index_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("anthropic-index-processor")

class AnthropicIndexProcessor:
    def __init__(self, input_dir="./data/raw/anthropic_index", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def find_latest_file(self, pattern):
        """Find the most recent file matching the given pattern"""
        files = glob.glob(os.path.join(self.input_dir, pattern))
        if not files:
            return None
        
        # Sort by modification time (newest first)
        return sorted(files, key=os.path.getmtime, reverse=True)[0]
    
    def process_anthropic_data(self, year=None, month=None):
        """Process Anthropic Economic Index data and identify job trends."""
        # Define pattern for combined files
        combined_pattern = "anthropic_index_*_combined.json"
        
        # Find most recent combined file
        latest_file = self.find_latest_file(combined_pattern)
        
        if not latest_file:
            logger.warning(f"No combined Anthropic Index files found in {self.input_dir}")
            return None
        
        logger.info(f"Processing Anthropic Index data from {latest_file}")
        
        try:
            # Load the combined data
            with open(latest_file, 'r') as f:
                combined_data = json.load(f)
                datasets = combined_data.get("datasets", {})
            
            # Extract and analyze occupation data
            occupation_usage = datasets.get("occupation_usage", {})
            occupation_automation = datasets.get("occupation_automation", {})
            occupation_categories = datasets.get("occupation_categories", {})
            task_usage = datasets.get("task_usage", {})
            skill_presence = datasets.get("skill_presence", {})
            
            # Identify top growing occupations (roles with high augmentation scores)
            augmentation_scores = {}
            automation_risk = {}
            
            for occupation, data in occupation_automation.items():
                if "automation_rate" in data and "augmentation_rate" in data:
                    augmentation_scores[occupation] = data["augmentation_rate"]
                    automation_risk[occupation] = data["automation_rate"]
            
            # Sort occupations by augmentation and automation rates
            top_augmented = sorted(
                augmentation_scores.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
            
            top_automated = sorted(
                automation_risk.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
            
            # Calculate overall statistics
            all_occupations = list(occupation_automation.keys())
            total_occupations = len(all_occupations)
            
            avg_automation = sum(data["automation_rate"] for occ, data in occupation_automation.items() 
                              if "automation_rate" in data) / total_occupations if total_occupations > 0 else 0
                              
            avg_augmentation = sum(data["augmentation_rate"] for occ, data in occupation_automation.items() 
                                if "augmentation_rate" in data) / total_occupations if total_occupations > 0 else 0
            
            # Get high-level occupation categories breakdown
            category_counts = {}
            for category, data in occupation_categories.items():
                if "count" in data:
                    category_counts[category] = data["count"]
            
            # Process task level data
            top_tasks = sorted(
                [(task, data.get("count", 0)) for task, data in task_usage.items()],
                key=lambda x: x[1],
                reverse=True
            )[:15]
            
            # Process skill data
            top_skills = sorted(
                [(skill, data.get("count", 0)) for skill, data in skill_presence.items()],
                key=lambda x: x[1],
                reverse=True
            )[:15]
            
            # Check if data is simulated
            is_simulated = combined_data.get("is_simulated_data", False)
            
            # Create trends object
            trends = {
                "date_analyzed": datetime.now().isoformat(),
                "source": "Anthropic Economic Index",
                "is_simulated_data": is_simulated,
                "statistics": {
                    "total_occupations_analyzed": total_occupations,
                    "average_automation_rate": avg_automation,
                    "average_augmentation_rate": avg_augmentation,
                    "automation_augmentation_ratio": avg_automation / avg_augmentation if avg_augmentation > 0 else 0
                },
                "categories": {
                    "data": [{"category": cat, "count": count} for cat, count in category_counts.items()]
                },
                "top_augmented_roles": [
                    {"title": title, "augmentation_rate": rate} 
                    for title, rate in top_augmented
                ],
                "top_automated_roles": [
                    {"title": title, "automation_rate": rate} 
                    for title, rate in top_automated
                ],
                "top_tasks": [
                    {"task": task, "count": count}
                    for task, count in top_tasks
                ],
                "top_skills": [
                    {"skill": skill, "count": count}
                    for skill, count in top_skills
                ]
            }
            
            # Determine output filename based on year and month parameters
            if year and month:
                # Format as YYYYMM for historical data
                date_str = f"{year}{month:02d}"
            else:
                # Use the date from the filename or current date
                filename_parts = os.path.basename(latest_file).split("_")
                if len(filename_parts) >= 2 and filename_parts[1].isdigit() and len(filename_parts[1]) == 8:
                    date_str = filename_parts[1]
                else:
                    date_str = datetime.now().strftime('%Y%m%d')
                
            output_file = os.path.join(
                self.output_dir,
                f"job_trends_{date_str}.json"
            )
            
            with open(output_file, 'w') as f:
                json.dump(trends, f, indent=2)
            
            logger.info(f"Saved Anthropic Index job trends to {output_file}")
            
            return trends
        
        except Exception as e:
            logger.error(f"Error processing Anthropic Index data: {str(e)}")
            return None

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process Anthropic Economic Index data')
    parser.add_argument('--year', type=int, help='Year to process (YYYY)')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    parser.add_argument('--input', type=str, help='Input directory', default="./data/raw/anthropic_index")
    parser.add_argument('--output', type=str, help='Output directory', default="./data/processed")
    args = parser.parse_args()
    
    processor = AnthropicIndexProcessor(input_dir=args.input, output_dir=args.output)
    trends = processor.process_anthropic_data(args.year, args.month)
    
    if trends:
        logger.info(f"Processing complete. Analyzed {trends['statistics']['total_occupations_analyzed']} occupations.")
        logger.info(f"Average automation rate: {trends['statistics']['average_automation_rate']:.2f}%")
        logger.info(f"Average augmentation rate: {trends['statistics']['average_augmentation_rate']:.2f}%")
    else:
        logger.error("Processing failed or no data was found.")

if __name__ == "__main__":
    main()