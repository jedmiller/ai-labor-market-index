# scripts/collection/collect_arxiv.py
import json
import logging
import os
import sys
import argparse
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("arxiv_collection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("arxiv-collector")

class ArxivCollector:
    def __init__(self, output_dir="./data/raw/arxiv"):
        self.output_dir = output_dir
        self.api_url = "http://export.arxiv.org/api/query"
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    # In the ArxivCollector class:

    def collect_papers(self, search_terms, max_results=100, year=None, month=None):
        """
        Collect papers from ArXiv based on search terms for a specific month.
        
        Args:
            search_terms (list): List of search term dictionaries
            max_results (int): Maximum results per search term
            year (int): Year to collect data for
            month (int): Month to collect data for
        """
        # Format dates for the query
        if year and month:
            start_date = f"{year}-{month:02d}-01"
            # Calculate end date (last day of month)
            if month == 12:
                next_month_year = year + 1
                next_month = 1
            else:
                next_month_year = year
                next_month = month + 1
            
            end_date = f"{next_month_year}-{next_month:02d}-01"
            
            logger.info(f"Collecting papers for period: {start_date} to {end_date}")
        else:
            # Default to current month if not specified
            today = datetime.now()
            start_date = None
            end_date = None
        
        # Format filename with year/month
        date_str = f"{year}{month:02d}" if year and month else datetime.now().strftime('%Y%m%d')
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "search_terms": search_terms,
            "papers_collected": 0,
            "files_created": []
        }
        
        for term_set in search_terms:
            primary = term_set["primary"]
            secondary = term_set["secondary"]
            
            # Format the search query for ArXiv API
            query = f"all:{primary} AND ({' OR '.join(secondary)})"
            logger.info(f"Collecting papers for query: {query}")
            
            params = {
                "search_query": query,
                "start": 0,
                "max_results": max_results,
                "sortBy": "submittedDate",
                "sortOrder": "descending"
            }
            
            # Add date range if specified
            if start_date and end_date:
                # ArXiv uses the format 'submittedDate:[YYYYMMDD* TO YYYYMMDD*]'
                # Convert dates to ArXiv format
                arxiv_start = start_date.replace('-', '')
                arxiv_end = end_date.replace('-', '')
                params["search_query"] += f" AND submittedDate:[{arxiv_start}0000 TO {arxiv_end}0000]"
            
            try:
                response = requests.get(f"{self.api_url}?{urlencode(params)}", timeout=30)
                
                if response.status_code == 200:
                    # Parse XML response
                    soup = BeautifulSoup(response.text, "xml")
                    entries = soup.find_all("entry")
                    
                    logger.info(f"Found {len(entries)} papers for query: {query}")
                    
                    # Process all entries without filtering - we're already filtering in the API query
                    papers = []
                    
                    for entry in entries:
                        published_date = entry.find("published").text
                        # Extract just the date part: 2023-01-15T12:30:45Z -> 2023-01-15
                        pub_date = published_date.split("T")[0]
                        
                        paper = {
                            "id": entry.find("id").text,
                            "title": entry.find("title").text.strip(),
                            "published": published_date,
                            "updated": entry.find("updated").text,
                            "summary": entry.find("summary").text.strip(),
                            "authors": [author.find("name").text for author in entry.find_all("author")],
                            "categories": [category["term"] for category in entry.find_all("category")],
                            "links": [link["href"] for link in entry.find_all("link")],
                            "primary_search_term": primary,
                            "secondary_search_terms": secondary
                        }
                        papers.append(paper)
                    
                    # Save the papers to a JSON file
                    term_str = primary.replace(" ", "_")
                    filename = f"arxiv_{year}_{month:02d}_{term_str}.json" if year and month else f"{datetime.now().strftime('%Y%m%d')}_{term_str}.json"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    with open(filepath, 'w') as f:
                        json.dump({
                            "query": query,
                            "date_collected": datetime.now().isoformat(),
                            "target_period": f"{year}-{month:02d}" if year and month else None,
                            "papers": papers
                        }, f, indent=2)
                    
                    results["papers_collected"] += len(papers)
                    results["files_created"].append(filepath)
                    logger.info(f"Saved {len(papers)} papers to {filepath}")
                
                else:
                    logger.error(f"ArXiv API request failed with status code {response.status_code}")
                    logger.error(f"Response: {response.text}")
            
            except Exception as e:
                logger.error(f"Error collecting papers for query {query}: {str(e)}")
        
        return results

# Update main() to use the new parameters:
def main():
    parser = argparse.ArgumentParser(description='Collect papers from ArXiv API')
    parser.add_argument('--year', type=int, help='Year to collect data for')
    parser.add_argument('--month', type=int, help='Month to collect data for')
    
    args = parser.parse_args()
    
    # Define search terms related to AI and labor markets
    search_terms = [
        {
            "primary": "artificial intelligence",
            "secondary": ["labor market", "employment", "jobs", "workforce", "automation"]
        },
        {
            "primary": "machine learning",
            "secondary": ["labor market", "employment", "jobs", "workforce", "automation"]
        },
        {
            "primary": "large language models",
            "secondary": ["labor market", "employment", "jobs", "workforce", "automation"]
        }
    ]
    
    collector = ArxivCollector()
    results = collector.collect_papers(search_terms, max_results=50, year=args.year, month=args.month)
    
    logger.info(f"Collection complete. Collected {results['papers_collected']} papers.")
    logger.info(f"Created {len(results['files_created'])} files.")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect research papers from ArXiv related to AI labor market impact')
    parser.add_argument('--year', type=int, help='Target year')
    parser.add_argument('--month', type=int, help='Target month (1-12)')
    parser.add_argument('--max-results', type=int, default=50, help='Maximum results per query')
    args = parser.parse_args()
    
    # Define search terms related to AI and labor markets
    search_terms = [
        {
            "primary": "artificial intelligence",
            "secondary": ["labor market", "employment", "jobs", "workforce", "automation"]
        },
        {
            "primary": "machine learning",
            "secondary": ["labor market", "employment", "jobs", "workforce", "automation"]
        },
        {
            "primary": "large language models",
            "secondary": ["labor market", "employment", "jobs", "workforce", "automation"]
        }
    ]
    
    collector = ArxivCollector()
    results = collector.collect_papers(
        search_terms, 
        max_results=args.max_results,
        year=args.year,
        month=args.month
    )
    
    logger.info(f"Collection complete. Found {results['papers_collected']} papers total.")
    
    if 'filtered_papers' not in results:
        results['filtered_papers'] = results.get('papers_collected', 0)
    
    logger.info(f"Filtered to {results['filtered_papers']} papers for target month.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()