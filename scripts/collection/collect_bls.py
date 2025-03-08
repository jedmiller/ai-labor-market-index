# scripts/collection/collect_bls.py
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
        logging.FileHandler("bls_collection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("bls-collector")

class BLSCollector:
    def __init__(self, output_dir="./data/raw/bls", api_key=None):
        self.output_dir = output_dir
        self.api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        self.api_key = api_key
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def collect_employment_data(self, series_ids, start_year, end_year):
        """
        Collect employment data from BLS API.
        
        Args:
            series_ids (list): List of BLS series IDs to collect
            start_year (str): Start year for data collection
            end_year (str): End year for data collection
            
        Returns:
            dict: Collection results
        """
        results = {
            "timestamp": datetime.now().isoformat(),
            "series_collected": 0,
            "files_created": []
        }
        
        # BLS API only allows 50 series per request
        for i in range(0, len(series_ids), 50):
            batch = series_ids[i:i+50]
            logger.info(f"Collecting batch of {len(batch)} series from BLS")
            
            headers = {'Content-Type': 'application/json'}
            payload = {
                "seriesid": batch,
                "startyear": start_year,
                "endyear": end_year,
                "registrationKey": self.api_key
            }
            
            # Remove API key if not provided
            if not self.api_key:
                logger.warning("No API key provided. Using limited access.")
                payload.pop("registrationKey", None)
            
            try:
                response = requests.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data["status"] == "REQUEST_SUCCEEDED":
                        # Save raw response
                        date_str = datetime.now().strftime("%Y%m%d")
                        filename = f"{date_str}_bls_employment_batch_{i}.json"
                        filepath = os.path.join(self.output_dir, filename)
                        
                        with open(filepath, 'w') as f:
                            json.dump(data, f, indent=2)
                        
                        results["series_collected"] += len(batch)
                        results["files_created"].append(filepath)
                        logger.info(f"Saved batch {i} data to {filepath}")
                    else:
                        logger.error(f"BLS API request failed: {data['status']}")
                        logger.error(f"Message: {data.get('message', 'No message')}")
                else:
                    logger.error(f"BLS API returned status code {response.status_code}")
                    logger.error(f"Response: {response.text}")
            
            except Exception as e:
                logger.error(f"Error collecting BLS data: {str(e)}")
        
        return results


def main():
    # Get API key from environment variable
    api_key = os.environ.get("BLS_API_KEY")
    
    # List of series IDs to collect
    # Using key industry employment series
    series_ids = [
        "CEU0000000001",  # All employees, total nonfarm
        "CEU1000000001",  # Mining and logging
        "CEU2000000001",  # Construction
        "CEU3000000001",  # Manufacturing
        "CEU4000000001",  # Trade, transportation, and utilities
        "CEU5000000001",  # Information
        "CEU5500000001",  # Financial activities
        "CEU6000000001",  # Professional and business services
        "CEU6500000001",  # Education and health services
        "CEU7000000001",  # Leisure and hospitality
        "CEU8000000001",  # Other services
        "CEU9000000001",  # Government
    ]
    
    # Use current year for end year
    current_year = str(datetime.now().year)
    start_year = str(int(current_year) - 3)  # Get 3 years of data
    
    collector = BLSCollector(api_key=api_key)
    results = collector.collect_employment_data(series_ids, start_year, current_year)
    
    logger.info(f"Collection complete. Collected {results['series_collected']} series.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()