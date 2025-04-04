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
        logging.FileHandler("news_collection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("news-collector")

class NewsCollector:
    def __init__(self, output_dir="./data/raw/news"):
        self.output_dir = output_dir
        self.api_url = "https://newsapi.org/v2/everything"
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def collect_news(self, query="AI layoffs hiring", api_key=None, year=None, month=None):
        """
        Collect news articles related to AI workforce impact for a specific month.
        
        Args:
            query (str): Search query for news articles
            api_key (str): News API key
            year (int): Target year (defaults to current year)
            month (int): Target month (defaults to current month)
            
        Returns:
            dict: Collection results
        """
        # Set default year and month if not provided
        if year is None or month is None:
            today = datetime.now()
            year = year or today.year
            month = month or today.month
        
        # Calculate start and end dates for the month
        start_date = datetime(year, month, 1).strftime('%Y-%m-%d')
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        end_date = end_date.strftime('%Y-%m-%d')
        
        logger.info(f"Collecting news for period: {start_date} to {end_date}")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "target_period": f"{year}-{month:02d}",
            "start_date": start_date,
            "end_date": end_date,
            "articles_collected": 0,
            "files_created": []
        }
        
        # Log API key status
        logger.info(f"Starting news collection with API key: {'PRESENT' if api_key else 'MISSING'}")
        
        if not api_key:
            logger.warning("No NEWS_API_KEY found in environment variables")
            return results
        
        # Set up request parameters with date filtering
        params = {
            'q': query,
            'apiKey': api_key,
            'pageSize': 100,  # Increased to get more articles for the month
            'language': 'en',
            'sortBy': 'publishedAt',
            'from': start_date,
            'to': end_date
        }
        
        try:
            # Log request details
            logger.info(f"Making request to News API: {self.api_url}")
            
            # Make the API call
            response = requests.get(self.api_url, params=params, timeout=30)
            
            # Log response status
            logger.info(f"News API response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])
                
                logger.info(f"Successfully received {len(articles)} articles")
                
                if articles:
                    # Save to file with year and month in filename
                    filename = f"news_{year}_{month:02d}_{datetime.now().strftime('%Y%m%d')}.json"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    with open(filepath, 'w') as f:
                        json.dump({
                            "query": query,
                            "target_period": f"{year}-{month:02d}",
                            "date_collected": datetime.now().isoformat(),
                            "articles": articles
                        }, f, indent=2)
                    
                    results["articles_collected"] = len(articles)
                    results["files_created"].append(filepath)
                    logger.info(f"Saved {len(articles)} articles to {filepath}")
                else:
                    logger.warning("No articles found in the API response")
            else:
                logger.error(f"News API error: {response.text}")
                # Try to parse error message
                try:
                    error_data = response.json()
                    logger.error(f"Error code: {error_data.get('code')}, Message: {error_data.get('message')}")
                except:
                    logger.error(f"Could not parse error response: {response.text[:200]}")
        
        except Exception as e:
            logger.error(f"Exception during News API request: {str(e)}")
        
        return results


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect news articles related to AI workforce impact')
    parser.add_argument('--api-key', help='News API key')
    parser.add_argument('--year', type=int, help='Target year')
    parser.add_argument('--month', type=int, help='Target month (1-12)')
    args = parser.parse_args()
    
    # Get API key from arguments or environment variable
    api_key = args.api_key or os.environ.get("NEWS_API_KEY")
    
    collector = NewsCollector()
    results = collector.collect_news(api_key=api_key, year=args.year, month=args.month)
    
    logger.info(f"Collection complete. Collected {results['articles_collected']} articles.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()