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
        
        articles_collected = False
        max_retries = 3
        retry_count = 0
        adjusted_start_date = start_date
        
        while not articles_collected and retry_count < max_retries:
            # Set up request parameters with date filtering
            params = {
                'q': query,
                'apiKey': api_key,
                'pageSize': 100,  # Increased to get more articles for the month
                'language': 'en',
                'sortBy': 'publishedAt',
                'from': adjusted_start_date,
                'to': end_date
            }
            
            try:
                # Log request details
                logger.info(f"Making request to News API: {self.api_url}")
                logger.info(f"Date range: {adjusted_start_date} to {end_date}")
                
                # Make the API call
                response = requests.get(self.api_url, params=params, timeout=30)
                
                # Log response status
                logger.info(f"News API response status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get("articles", [])
                    
                    logger.info(f"Successfully received {len(articles)} articles")
                    articles_collected = True
                    
                    if articles:
                        # Include adjusted date range in the filename and metadata
                        filename = f"news_{year}_{month:02d}_{datetime.now().strftime('%Y%m%d')}.json"
                        filepath = os.path.join(self.output_dir, filename)
                        
                        with open(filepath, 'w') as f:
                            json.dump({
                                "query": query,
                                "target_period": f"{year}-{month:02d}",
                                "actual_date_range": {
                                    "from": adjusted_start_date,
                                    "to": end_date
                                },
                                "date_collected": datetime.now().isoformat(),
                                "articles": articles
                            }, f, indent=2)
                        
                        results["articles_collected"] = len(articles)
                        results["files_created"].append(filepath)
                        # Update results with actual date range
                        results["actual_start_date"] = adjusted_start_date
                        results["actual_end_date"] = end_date
                        
                        logger.info(f"Saved {len(articles)} articles to {filepath}")
                    else:
                        logger.warning("No articles found in the API response")
                        
                elif response.status_code == 426:  # API limitation error
                    # Try to parse error message for date limit information
                    try:
                        error_data = response.json()
                        error_message = error_data.get('message', '')
                        logger.error(f"Error code: {error_data.get('code')}, Message: {error_message}")
                        
                        # Try to extract the earliest allowed date from the error message
                        # Example message: "You are trying to request results too far in the past. Your plan permits you to request articles as far back as 2025-04-02"
                        import re
                        date_match = re.search(r'as far back as (\d{4}-\d{2}-\d{2})', error_message)
                        
                        if date_match:
                            earliest_date = date_match.group(1)
                            logger.info(f"API limitation: earliest allowed date is {earliest_date}")
                            
                            # Adjust start date to one day after the earliest allowed date
                            # to work around the API behavior
                            from datetime import datetime, timedelta
                            earliest_date_obj = datetime.strptime(earliest_date, '%Y-%m-%d')
                            adjusted_date_obj = earliest_date_obj + timedelta(days=1)
                            adjusted_start_date = adjusted_date_obj.strftime('%Y-%m-%d')
                            logger.info(f"Adjusting date range to: {adjusted_start_date} to {end_date} (one day after API limitation)")
                            retry_count += 1
                            continue  # Try again with the adjusted date
                        else:
                            logger.warning(f"Could not extract date limitation from API error: {error_message}")
                            break  # Exit the retry loop if we can't parse the date
                    except Exception as e:
                        logger.error(f"Error parsing API response: {e}")
                        break  # Exit the retry loop
                else:
                    logger.error(f"News API error: {response.text}")
                    # Try to parse error message
                    try:
                        error_data = response.json()
                        logger.error(f"Error code: {error_data.get('code')}, Message: {error_data.get('message')}")
                    except:
                        logger.error(f"Could not parse error response: {response.text[:200]}")
                    break  # Exit the retry loop
            
            except Exception as e:
                logger.error(f"Exception during News API request: {str(e)}")
                break  # Exit the retry loop
            
            retry_count += 1
        
        if not articles_collected:
            logger.warning(f"Failed to collect articles after {retry_count} attempts")
            
            # Final fallback: try with the most recent week only
            logger.info("Attempting final fallback: collecting news for the most recent week only")
            try:
                # Calculate a date range for just the last week of the month
                from datetime import datetime, timedelta
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
                start_date_obj = end_date_obj - timedelta(days=7)
                recent_start_date = start_date_obj.strftime('%Y-%m-%d')
                
                logger.info(f"Final attempt with date range: {recent_start_date} to {end_date}")
                
                # Set up new parameters with the short date range
                params = {
                    'q': query,
                    'apiKey': api_key,
                    'pageSize': 100,
                    'language': 'en',
                    'sortBy': 'publishedAt',
                    'from': recent_start_date,
                    'to': end_date
                }
                
                response = requests.get(self.api_url, params=params, timeout=30)
                logger.info(f"News API response status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get("articles", [])
                    
                    logger.info(f"Successfully received {len(articles)} articles in fallback mode")
                    
                    if articles:
                        # Include adjusted date range in the filename and metadata
                        filename = f"news_{year}_{month:02d}_{datetime.now().strftime('%Y%m%d')}_fallback.json"
                        filepath = os.path.join(self.output_dir, filename)
                        
                        with open(filepath, 'w') as f:
                            json.dump({
                                "query": query,
                                "target_period": f"{year}-{month:02d}",
                                "actual_date_range": {
                                    "from": recent_start_date,
                                    "to": end_date
                                },
                                "date_collected": datetime.now().isoformat(),
                                "fallback_mode": True,
                                "articles": articles
                            }, f, indent=2)
                        
                        results["articles_collected"] = len(articles)
                        results["files_created"].append(filepath)
                        results["actual_start_date"] = recent_start_date
                        results["actual_end_date"] = end_date
                        results["fallback_mode"] = True
                        
                        logger.info(f"Saved {len(articles)} articles to {filepath} in fallback mode")
                        articles_collected = True
            except Exception as e:
                logger.error(f"Error in fallback mode: {str(e)}")
            
        # Update results with adjusted date range for reporting
        if adjusted_start_date != start_date or "fallback_mode" in results:
            if "fallback_mode" in results:
                results["note"] = f"Limited date range due to API restrictions: {results['actual_start_date']} to {results['actual_end_date']} (fallback mode)"
            else:
                results["note"] = f"Date range adjusted due to API limitations: {adjusted_start_date} to {end_date}"
        
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
    
    # Report any date adjustments made due to API limitations
    if "note" in results:
        logger.info(results["note"])
    
    if "actual_start_date" in results and "actual_end_date" in results:
        original_range = f"{results.get('start_date', 'unknown')} to {results.get('end_date', 'unknown')}"
        actual_range = f"{results['actual_start_date']} to {results['actual_end_date']}"
        
        if original_range != actual_range:
            logger.info(f"Original date range ({original_range}) was adjusted to ({actual_range})")
            
    # Return success if we collected any articles, regardless of date adjustment
    return 0 if results['articles_collected'] > 0 else 1


if __name__ == "__main__":
    main()