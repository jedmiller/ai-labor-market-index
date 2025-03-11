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
    
    def collect_news(self, query="AI layoffs hiring", api_key=None):
        """
        Collect news articles related to AI workforce impact.
        
        Args:
            query (str): Search query for news articles
            api_key (str): News API key
            
        Returns:
            dict: Collection results
        """
        results = {
            "timestamp": datetime.now().isoformat(),
            "articles_collected": 0,
            "files_created": []
        }
        
        # Log API key status
        logger.info(f"Starting news collection with API key: {'PRESENT' if api_key else 'MISSING'}")
        
        if not api_key:
            logger.warning("No NEWS_API_KEY found in environment variables")
            return results
        
        # Set up request parameters
        params = {
            'q': query,
            'apiKey': api_key,
            'pageSize': 20,
            'language': 'en',
            'sortBy': 'publishedAt'
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
                    # Save to file
                    date_str = datetime.now().strftime("%Y%m%d")
                    filename = f"{date_str}_news_articles.json"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    with open(filepath, 'w') as f:
                        json.dump({
                            "query": query,
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
    # Get API key from environment variable
    api_key = os.environ.get("NEWS_API_KEY")
    
    collector = NewsCollector()
    results = collector.collect_news(api_key=api_key)
    
    logger.info(f"Collection complete. Collected {results['articles_collected']} articles.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()