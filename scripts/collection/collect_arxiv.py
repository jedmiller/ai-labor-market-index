# scripts/collection/collect_arxiv.py
import json
import logging
import os
import sys
from datetime import datetime
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
    
    def collect_papers(self, search_terms, max_results=100):
        """
        Collect papers from ArXiv based on search terms.
        
        Args:
            search_terms (list): List of search term dictionaries with 'primary' and 'secondary' keys
            max_results (int): Maximum number of results to retrieve per search term
            
        Returns:
            dict: Collection results with counts and file paths
        """
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
            
            try:
                response = requests.get(f"{self.api_url}?{urlencode(params)}", timeout=30)
                
                if response.status_code == 200:
                    # Parse XML response
                    soup = BeautifulSoup(response.text, "xml")
                    entries = soup.find_all("entry")
                    
                    logger.info(f"Found {len(entries)} papers for query: {query}")
                    
                    if entries:
                        # Process the entries
                        papers = []
                        
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
                            papers.append(paper)
                        
                        # Save the papers to a JSON file
                        date_str = datetime.now().strftime("%Y%m%d")
                        term_str = primary.replace(" ", "_")
                        filename = f"{date_str}_{term_str}.json"
                        filepath = os.path.join(self.output_dir, filename)
                        
                        with open(filepath, 'w') as f:
                            json.dump({
                                "query": query,
                                "date_collected": datetime.now().isoformat(),
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


def main():
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
    results = collector.collect_papers(search_terms, max_results=50)
    
    logger.info(f"Collection complete. Collected {results['papers_collected']} papers.")
    logger.info(f"Created {len(results['files_created'])} files.")


if __name__ == "__main__":
    main()