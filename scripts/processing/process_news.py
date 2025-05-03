# scripts/processing/process_news.py
import json
import logging
import os
import sys
import re
import argparse
from datetime import datetime
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("news-processor")

class NewsProcessor:
    def __init__(self, input_dir="./data/raw/news", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Common tech companies for extraction
        self.common_companies = [
            "Google", "Microsoft", "Apple", "Amazon", "Meta", "Facebook",
            "Twitter", "X Corp", "IBM", "OpenAI", "Anthropic", "Tesla",
            "Nvidia", "Intel", "AMD", "Salesforce", "Oracle", "SAP",
            "Adobe", "Netflix", "Spotify", "Uber", "Lyft", "Airbnb"
        ]
    
    def extract_company(self, text):
        """Extract company name from text."""
        for company in self.common_companies:
            if company.lower() in text.lower():
                return company
        
        # Try to extract company followed by common suffixes
        company_pattern = r'([A-Z][a-zA-Z]+)\s+(Inc\.?|Corp\.?|Corporation|Company|Technologies|Tech)'
        match = re.search(company_pattern, text)
        if match:
            return match.group(1)
        
        return "Unknown"
    
    def extract_count(self, text):
        """Extract count of people affected from text."""
        # Look for patterns like "1,000 employees" or "500 workers"
        patterns = [
            r'(\d+,\d+|\d+)\s+(employees|workers|jobs|positions|staff)',
            r'(lay off|layoff|cut|hire|hiring)\s+(\d+,\d+|\d+)',
            r'(thousands|hundreds)\s+of\s+(employees|workers|jobs|positions)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                if 'thousands' in match.group():
                    return 1000
                elif 'hundreds' in match.group():
                    return 500
                
                # Extract number and remove commas
                # First check if group 1 contains digits
                if match.group(1) and any(c.isdigit() for c in match.group(1)):
                    num_str = match.group(1)
                # Then check if group 2 contains digits
                elif len(match.groups()) >= 2 and match.group(2) and any(c.isdigit() for c in match.group(2)):
                    num_str = match.group(2)
                else:
                    # No numeric group found
                    return 100
                
                try:
                    # Try to convert to integer, handling commas
                    return int(num_str.replace(',', ''))
                except ValueError:
                    # If conversion fails, return default
                    return 100
        
        # Default count if not found
        return 100

    def determine_event_type(self, text):
        """Determine if article is about hiring or layoffs."""
        hiring_terms = ['hire', 'hiring', 'recruit', 'add', 'create', 'expansion', 'grow']
        layoff_terms = ['layoff', 'lay off', 'cut', 'reduce', 'downsize', 'restructure', 'fire']
        
        text_lower = text.lower()
        
        # Check if hiring terms are present
        if any(term in text_lower for term in hiring_terms):
            return "hiring"
        
        # Check if layoff terms are present
        if any(term in text_lower for term in layoff_terms):
            return "layoff"
        
        # Default to unknown
        return "unknown"
    
    def determine_ai_relation(self, text):
        """Determine if the article is related to AI."""
        ai_terms = ['ai', 'artificial intelligence', 'machine learning', 'ml', 
                    'deep learning', 'llm', 'large language model', 'chatbot',
                    'automation', 'robot']
        
        text_lower = text.lower()
        
        # Check if explicit AI terms are present
        if any(term in text_lower for term in ai_terms):
            return "Direct"
        
        # Tech terms that suggest AI relation
        tech_terms = ['technology', 'tech', 'digital', 'transformation']
        if any(term in text_lower for term in tech_terms):
            return "Indirect"
        
        return "None"
    
    def process_news_data(self, year=None, month=None):
        """Process news data and identify events."""
        # Find news data files for the specified period
        if year and month:
            # Look for files matching the pattern for the specified year and month
            pattern = f"news_{year}_{month:02d}_*.json"
            news_files = glob.glob(os.path.join(self.input_dir, pattern))
            logger.info(f"Looking for files matching {pattern}")
        else:
            # If no year/month specified, use all available files
            news_files = glob.glob(os.path.join(self.input_dir, "*.json"))
        
        if not news_files:
            logger.warning(f"No news data files found in {self.input_dir}")
            
            # Fallback to sample events
            events = [
                {
                    "date": datetime.now().isoformat(),
                    "source": "Tech News Daily",
                    "title": "Google Announces AI Transformation Initiative",
                    "company": "Google",
                    "event_type": "hiring",
                    "count": 2500,
                    "ai_relation": "Direct",
                    "url": "https://example.com/news/google-ai"
                },
                {
                    "date": datetime.now().isoformat(),
                    "source": "Business Weekly",
                    "title": "Retail Giant Implements Large-Scale Automation",
                    "company": "Amazon",
                    "event_type": "layoff",
                    "count": 1500,
                    "ai_relation": "Direct",
                    "url": "https://example.com/news/retail-automation"
                }
            ]
            
            # Note about using sample data
            metadata = {
                "data_source": "sample",
                "reason": "No news data files found for the requested period"
            }
        else:
            logger.info(f"Found {len(news_files)} news files")
            events = []
            actual_date_ranges = []
            
            # Process each news file
            for file_path in news_files:
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        
                        # Check for adjusted date range in the data
                        if "actual_date_range" in data:
                            date_range = data["actual_date_range"]
                            logger.info(f"File {os.path.basename(file_path)} has adjusted date range: {date_range['from']} to {date_range['to']}")
                            actual_date_ranges.append(date_range)
                        
                        articles = data.get("articles", [])
                        logger.info(f"Processing {len(articles)} articles from {os.path.basename(file_path)}")
                        
                        for article in articles:
                            title = article.get("title", "")
                            description = article.get("description", "")
                            content = article.get("content", "")
                            
                            # Combine text for analysis
                            full_text = f"{title} {description} {content}"
                            
                            # Determine event type
                            event_type = self.determine_event_type(full_text)
                            
                            # Skip if not a relevant event
                            if event_type == "unknown":
                                continue
                            
                            # Extract event details
                            event = {
                                "date": article.get("publishedAt", datetime.now().isoformat()),
                                "source": article.get("source", {}).get("name", "Unknown"),
                                "title": title,
                                "company": self.extract_company(full_text),
                                "event_type": event_type,
                                "count": self.extract_count(full_text),
                                "ai_relation": self.determine_ai_relation(full_text),
                                "url": article.get("url", "")
                            }
                            
                            events.append(event)
                            
                except Exception as e:
                    logger.error(f"Error processing news file {file_path}: {str(e)}")
            
            # If no events were found in real articles, use sample data
            if not events:
                logger.warning("No events found in articles, using sample data")
                events = [
                    {
                        "date": datetime.now().isoformat(),
                        "source": "Tech News Daily",
                        "title": "Google Announces AI Transformation Initiative",
                        "company": "Google",
                        "event_type": "hiring",
                        "count": 2500,
                        "ai_relation": "Direct",
                        "url": "https://example.com/news/google-ai"
                    },
                    {
                        "date": datetime.now().isoformat(),
                        "source": "Business Weekly",
                        "title": "Retail Giant Implements Large-Scale Automation",
                        "company": "Amazon",
                        "event_type": "layoff",
                        "count": 1500,
                        "ai_relation": "Direct",
                        "url": "https://example.com/news/retail-automation"
                    }
                ]
                
                metadata = {
                    "data_source": "sample",
                    "reason": "No events extracted from available data"
                }
            else:
                # Actual data metadata
                metadata = {
                    "data_source": "News API",
                    "files_processed": [os.path.basename(f) for f in news_files],
                    "events_found": len(events)
                }
                
                # Include date range adjustments in metadata
                if actual_date_ranges:
                    metadata["actual_date_ranges"] = actual_date_ranges
        
        # Create output object
        output = {
            "date_processed": datetime.now().isoformat(),
            "target_period": f"{year}-{month:02d}" if year and month else "current",
            "metadata": metadata,
            "events": events
        }
        
        # Determine output filename based on year and month parameters
        if year and month:
            # Format as YYYYMM for historical data
            date_str = f"{year}{month:02d}"
        else:
            # Keep existing format for current data
            date_str = datetime.now().strftime('%Y%m%d')
            
        output_file = os.path.join(
            self.output_dir,
            f"workforce_events_{date_str}.json"
        )
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Saved {len(events)} workforce events to {output_file}")
        
        return output


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process news data')
    parser.add_argument('--year', type=int, help='Year to process (YYYY)')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    args = parser.parse_args()
    
    processor = NewsProcessor()
    result = processor.process_news_data(args.year, args.month)
    
    if result:
        logger.info(f"Processing complete. Processed {len(result['events'])} news events.")
    else:
        logger.error("Processing failed or no data was found.")


if __name__ == "__main__":
    main()