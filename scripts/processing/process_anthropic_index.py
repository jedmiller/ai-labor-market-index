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
        # Format month with leading zero if needed
        month_str = f"{month:02d}" if month else ""
        year_str = f"{year}" if year else ""
        
        # Define patterns for combined files
        if year and month:
            # Look for specific year/month file first
            target_pattern = f"anthropic_index_{year}_{month_str}_combined.json"
            combined_pattern = f"anthropic_index_{year}_{month_str}_combined.json"
        else:
            # Fall back to any combined file if no specific date requested
            target_pattern = "anthropic_index_*_combined.json"
            combined_pattern = "anthropic_index_*_combined.json"
        
        # Try to find the target file first
        target_file = glob.glob(os.path.join(self.input_dir, target_pattern))
        
        if target_file:
            latest_file = target_file[0]
            logger.info(f"Found target file for {year_str}-{month_str}: {latest_file}")
        else:
            # Target file not found, try to find the most recent alternative
            logger.warning(f"No Anthropic Index file found for {year_str}-{month_str}")
            
            # Find most recent combined file as fallback
            latest_file = self.find_latest_file(combined_pattern)
            
            if not latest_file:
                # If no combined file found, try to find any combined file
                any_combined_file = glob.glob(os.path.join(self.input_dir, "anthropic_index_*_combined.json"))
                if any_combined_file:
                    latest_file = sorted(any_combined_file, key=os.path.getmtime, reverse=True)[0]
                    logger.warning(f"Using alternative file: {latest_file}")
                else:
                    logger.warning(f"No combined Anthropic Index files found in {self.input_dir}")
                    return None
        
        logger.info(f"Processing Anthropic Index data from {latest_file}")
        
        # Extract actual date from the filename for reporting
        actual_date = "unknown"
        try:
            filename_parts = os.path.basename(latest_file).split("_")
            if len(filename_parts) >= 3:
                actual_year = filename_parts[1]
                actual_month = filename_parts[2]
                actual_date = f"{actual_year}-{actual_month}"
        except:
            logger.warning("Could not extract date from filename")
        
        # Check if we're using fallback data
        using_fallback = False
        if year and month:
            target_date = f"{year}-{month_str}"
            if actual_date != target_date and actual_date != "unknown":
                using_fallback = True
                logger.warning(f"Using fallback data from {actual_date} instead of requested {target_date}")
        
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
                "source_period": combined_data.get("target_period", actual_date),
                "requested_period": f"{year}-{month_str}" if year and month else None,
                "is_simulated_data": is_simulated,
                "using_fallback_data": using_fallback,
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
            
            # Add a note if using fallback data
            if using_fallback:
                if "notes" not in trends:
                    trends["notes"] = []
                trends["notes"].append(
                    f"Note: Using fallback data from {actual_date} instead of requested {year}-{month_str}"
                )
            
            # Determine output filename based on the requested date, not the actual data date
            # This ensures the index calculation uses the file it expects
            if year and month:
                # Format as YYYYMM for historical data (using requested date)
                date_str = f"{year}{month:02d}"
            else:
                # Use the date from the filename or current date
                filename_parts = os.path.basename(latest_file).split("_")
                if len(filename_parts) >= 2 and filename_parts[1].isdigit():
                    date_str = filename_parts[1]
                    if len(date_str) == 4 and len(filename_parts) >= 3:  # Format is year_month_...
                        date_str += filename_parts[2] if len(filename_parts[2]) == 2 else f"{int(filename_parts[2]):02d}"
                else:
                    date_str = datetime.now().strftime('%Y%m%d')
                
            # Create output files
            output_file = os.path.join(
                self.output_dir,
                f"job_trends_{date_str}.json"
            )
            
            # Also create an additional file that indicates the actual data source period
            source_date = ""
            if combined_data.get("target_period"):
                source_date = combined_data.get("target_period").replace("-", "")
            elif actual_date != "unknown":
                source_date = actual_date.replace("-", "")
            
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
    parser.add_argument('--year', type=int, required=True, help='Year to process (YYYY)')
    parser.add_argument('--month', type=int, required=True, help='Month to process (1-12)')
    parser.add_argument('--input', type=str, help='Input directory', default="./data/raw/anthropic_index")
    parser.add_argument('--output', type=str, help='Output directory', default="./data/processed")
    args = parser.parse_args()
    
    logger.info(f"Processing Anthropic Index data for {args.year}-{args.month:02d}")
    
    # Validate the requested date is not in the future
    current_date = datetime.now()
    if args.year > current_date.year or (args.year == current_date.year and args.month > current_date.month):
        logger.warning(f"Requested date {args.year}-{args.month:02d} is in the future. Using most recent available data.")
    
    processor = AnthropicIndexProcessor(input_dir=args.input, output_dir=args.output)
    trends = processor.process_anthropic_data(args.year, args.month)
    
    if trends:
        logger.info(f"Processing complete. Analyzed {trends['statistics']['total_occupations_analyzed']} occupations.")
        logger.info(f"Average automation rate: {trends['statistics']['average_automation_rate']:.2f}%")
        logger.info(f"Average augmentation rate: {trends['statistics']['average_augmentation_rate']:.2f}%")
        
        # Log data source information
        source_period = trends.get("source_period", "unknown")
        requested_period = trends.get("requested_period", "unknown")
        using_fallback = trends.get("using_fallback_data", False)
        
        if using_fallback:
            logger.warning(f"Used fallback data from {source_period} instead of requested {requested_period}")
        else:
            logger.info(f"Used data from requested period: {requested_period}")
            
        is_simulated = trends.get("is_simulated_data", False)
        if is_simulated:
            logger.warning("Using simulated data - this is not actual Anthropic data!")
    else:
        logger.error("Processing failed or no data was found.")

if __name__ == "__main__":
    main()