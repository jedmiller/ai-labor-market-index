# scripts/processing/process_employment.py
import json
import logging
import os
import sys
from datetime import datetime
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("employment_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("employment-processor")

class EmploymentProcessor:
    def __init__(self, input_dir="./data/raw/bls", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Industry mappings from BLS series IDs to industry names
        self.industry_mappings = {
            "CEU0000000001": "Total Nonfarm",
            "CEU1000000001": "Mining and Logging",
            "CEU2000000001": "Construction",
            "CEU3000000001": "Manufacturing",
            "CEU4000000001": "Trade, Transportation, and Utilities",
            "CEU5000000001": "Information",
            "CEU5500000001": "Financial Activities",
            "CEU6000000001": "Professional and Business Services",
            "CEU6500000001": "Education and Health Services",
            "CEU7000000001": "Leisure and Hospitality",
            "CEU8000000001": "Other Services",
            "CEU9000000001": "Government"
        }
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def process_employment_data(self):
        """Process BLS employment data and calculate industry trends."""
        # Find all BLS data files
        bls_files = glob.glob(os.path.join(self.input_dir, "*_bls_employment_*.json"))
        
        if not bls_files:
            logger.warning(f"No BLS data files found in {self.input_dir}")
            return None
        
        logger.info(f"Found {len(bls_files)} BLS data files")
        
        # Load all series data
        all_series_data = {}
        
        for file_path in bls_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                    if data["status"] != "REQUEST_SUCCEEDED":
                        logger.warning(f"BLS request in {file_path} did not succeed: {data['status']}")
                        continue
                    
                    # Process each series
                    series_list = data.get("Results", {}).get("series", [])
                    for series in series_list:
                        series_id = series.get("seriesID")
                        if series_id and "data" in series:
                            all_series_data[series_id] = series["data"]
                            logger.info(f"Loaded data for series {series_id} from {file_path}")
            
            except Exception as e:
                logger.error(f"Error loading file {file_path}: {str(e)}")
        
        if not all_series_data:
            logger.warning("No valid series data found in any file")
            return None
        
        # Calculate employment changes by industry
        industries = {}
        
        for series_id, data in all_series_data.items():
            # Skip series we don't have a mapping for
            if series_id not in self.industry_mappings:
                continue
                
            industry_name = self.industry_mappings[series_id]
            
            # Sort data by year and period (most recent first)
            sorted_data = sorted(
                data, 
                key=lambda x: (int(x.get("year", 0)), int(x.get("period", "M00").replace("M", ""))),
                reverse=True
            )
            
            # Calculate year-over-year change
            if len(sorted_data) >= 13:  # Need at least 13 months for YoY comparison
                current = sorted_data[0]
                year_ago = sorted_data[12]  # 12 months ago
                
                try:
                    current_value = float(current.get("value", 0))
                    year_ago_value = float(year_ago.get("value", 0))
                    
                    if year_ago_value > 0:
                        change_percentage = ((current_value - year_ago_value) / year_ago_value) * 100
                    else:
                        change_percentage = 0
                    
                    industries[industry_name] = {
                        "current_employment": current_value,
                        "year_ago_employment": year_ago_value,
                        "change": current_value - year_ago_value,
                        "change_percentage": change_percentage,
                        "current_period": f"{current.get('year')}-{current.get('period')}",
                        "year_ago_period": f"{year_ago.get('year')}-{year_ago.get('period')}"
                    }
                    
                except (ValueError, TypeError) as e:
                    logger.error(f"Error calculating change for {industry_name}: {str(e)}")
        
        # Create employment stats object
        stats = {
            "date_analyzed": datetime.now().isoformat(),
            "industries": industries
        }
        
        # Save stats to a file
        output_file = os.path.join(
            self.output_dir,
            f"employment_stats_{datetime.now().strftime('%Y%m%d')}.json"
        )
        
        with open(output_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Saved employment stats to {output_file}")
        
        return stats


def main():
    processor = EmploymentProcessor()
    stats = processor.process_employment_data()
    
    if stats:
        logger.info(f"Processing complete. Analyzed employment data for {len(stats['industries'])} industries.")
    else:
        logger.error("Processing failed or no data was found.")


if __name__ == "__main__":
    main()