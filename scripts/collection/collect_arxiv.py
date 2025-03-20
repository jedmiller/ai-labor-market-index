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
    
    def collect_papers(self, search_terms, max_results=100, year=None, month=None):
        """
        Collect papers from ArXiv based on search terms for a specific month.
        
        Args:
            search_terms (list): List of search term dictionaries with 'primary' and 'secondary' keys
            max_results (int): Maximum number of results to retrieve per search term
            year (int): Target year (defaults to current year)
            month (int): Target month (defaults to current month)
            
        Returns:
            dict: Collection results with counts and file paths
        """
        # Set year and month to current if not specified
        if year is None or month is None:
            today = datetime.now()
            year = year or today.year
            month = month or today.month
        
        # Calculate start and end dates for the month
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        logger.info(f"Collecting papers for period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "target_period": f"{year}-{month:02d}",
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            "search_terms": search_terms,
            "papers_collected": 0,
            "filtered_papers": 0,
            "files_created": []
        }
        
        for term_set in search_terms:
            primary = term_set["primary"]
            secondary = term_set["secondary"]
            
            # Format the search query for ArXiv API
            query = f"all:{primary} AND ({' OR '.join(secondary)})"
            logger.info(f"Collecting papers for query: {query}")
            
            # Request more results than needed since we'll filter by date
            adjusted_max = max_results * 3  # Request more to account for filtering
            params = {
                "search_query": query,
                "start": 0,
                "max_results": adjusted_max,
                "sortBy": "submittedDate",
                "sortOrder": "descending"
            }
            
            try:
                response = requests.get(f"{self.api_url}?{urlencode(params)}", timeout=30)
                
                if response.status_code == 200:
                    # Parse XML response
                    soup = BeautifulSoup(response.text, "xml")
                    entries = soup.find_all("entry")
                    
                    logger.info(f"Found {len(entries)} papers for query: {query}")
                    
                    if entries:
                        # Process the entries
                        all_papers = []
                        filtered_papers = []
                        
                        for entry in entries:
                            paper = {
                                "id": entry.find("id").text,
                                "title": entry.find("title").text.strip(),
                                "published": entry.find("published").text,
                                "updated": entry.find("updated").text,
                                "summary": entry.find("summary").text.strip(),
                                "authors": [author.find("name").text for author in entry.find_all("author")],
                                "categories": [category["term"] for category in entry.find_all("category")],
                                "links": [link["href"] for link in entry.find_all("link")],
                                "primary_search_term": primary,
                                "secondary_search_terms": secondary
                            }
                            all_papers.append(paper)
                            
                            # Filter papers from the target month
                            try:
                                pub_date = datetime.strptime(paper["published"], "%Y-%m-%dT%H:%M:%SZ")
                                if start_date <= pub_date <= end_date:
                                    filtered_papers.append(paper)
                            except ValueError as e:
                                logger.warning(f"Could not parse date: {paper['published']} - {str(e)}")
                        
                        logger.info(f"Filtered {len(filtered_papers)} papers from {len(all_papers)} total for {year}-{month:02d}")
                        
                        # Save the filtered papers to a JSON file
                        term_str = primary.replace(" ", "_")
                        filename = f"arxiv_{year}_{month:02d}_{term_str}.json"
                        filepath = os.path.join(self.output_dir, filename)
                        
                        with open(filepath, 'w') as f:
                            json.dump({
                                "query": query,
                                "target_period": f"{year}-{month:02d}",
                                "date_collected": datetime.now().isoformat(),
                                "papers": filtered_papers
                            }, f, indent=2)
                        
                        results["papers_collected"] += len(all_papers)
                        results["filtered_papers"] += len(filtered_papers)
                        results["files_created"].append(filepath)
                        logger.info(f"Saved {len(filtered_papers)} filtered papers to {filepath}")
                    
                else:
                    logger.error(f"ArXiv API request failed with status code {response.status_code}")
                    logger.error(f"Response: {response.text}")
            
            except Exception as e:
                logger.error(f"Error collecting papers for query {query}: {str(e)}")
        
        return results


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
    logger.info(f"Filtered to {results['filtered_papers']} papers for target month.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()