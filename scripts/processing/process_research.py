import json
import logging
import os
import sys
import glob
import argparse
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("research_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("research-processor")

class ResearchProcessor:
    def __init__(self, input_dir="./data/raw/arxiv", output_dir="./data/processed", year=None, month=None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.year = year
        self.month = month
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def find_input_files(self):
        """Find input files based on year/month if specified."""
        if self.year and self.month:
            # Look for files specific to the target year/month
            pattern = f"arxiv_{self.year}_{self.month:02d}_*.json"
            files = list(Path(self.input_dir).glob(pattern))
            
            # If we have specific files for this month, use them
            if files:
                logger.info(f"Found {len(files)} ArXiv data files for {self.year}-{self.month:02d}")
                return files
            
            # Otherwise fallback to general search but will filter contents by date
            logger.info(f"No specific files found for {self.year}-{self.month:02d}, searching all files")
        
        # Find all json files
        return glob.glob(os.path.join(self.input_dir, "*.json"))
    
    def process_research(self):
        """Process ArXiv research papers and extract trends."""
        # Find all ArXiv data files
        arxiv_files = self.find_input_files()
        
        if not arxiv_files:
            logger.warning(f"No ArXiv data files found in {self.input_dir}")
            return None
        
        logger.info(f"Found {len(arxiv_files)} ArXiv data files")
        
        # Load and process all papers
        all_papers = []
        
        for file_path in arxiv_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    papers = data.get("papers", [])
                    
                    # If we have a target year/month and this file isn't specifically for that month,
                    # filter papers by publication date
                    if self.year and self.month and not str(file_path).find(f"{self.year}_{self.month:02d}") >= 0:
                        filtered_papers = []
                        target_month_str = f"{self.year}-{self.month:02d}"
                        for paper in papers:
                            published = paper.get('published', '')
                            if published.startswith(target_month_str):
                                filtered_papers.append(paper)
                        
                        logger.info(f"Loaded {len(filtered_papers)} papers for {self.year}-{self.month:02d} from {file_path}")
                        all_papers.extend(filtered_papers)
                    else:
                        logger.info(f"Loaded {len(papers)} papers from {file_path}")
                        all_papers.extend(papers)
            except Exception as e:
                logger.error(f"Error loading file {file_path}: {str(e)}")
        
        # Basic trend analysis
        total_papers = len(all_papers)
        
        if total_papers == 0:
            logger.warning("No papers found in any file")
            return None
        
        # Calculate paper counts by category
        category_counts = {}
        for paper in all_papers:
            categories = paper.get("categories", [])
            for category in categories:
                if category in category_counts:
                    category_counts[category] += 1
                else:
                    category_counts[category] = 1
        
        # Get top categories
        top_categories = sorted(
            category_counts.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:10]
        
        # Calculate basic sentiment
        # In a real implementation, would use NLP to analyze abstracts
        # For now, using a simplified approach with keyword counting
        positive_keywords = [
            "create", "growth", "opportunity", "innovation", 
            "benefit", "improve", "enhance", "augment"
        ]
        
        negative_keywords = [
            "displace", "automation", "replace", "loss", 
            "eliminate", "risk", "threat", "challenge"
        ]
        
        positive_count = 0
        negative_count = 0
        
        for paper in all_papers:
            summary = paper.get("summary", "").lower()
            title = paper.get("title", "").lower()
            
            for keyword in positive_keywords:
                if keyword in summary or keyword in title:
                    positive_count += 1
                    break
            
            for keyword in negative_keywords:
                if keyword in summary or keyword in title:
                    negative_count += 1
                    break
        
        # Calculate positive sentiment percentage
        if positive_count + negative_count > 0:
            positive_sentiment = (positive_count / (positive_count + negative_count)) * 100
        else:
            positive_sentiment = 50  # Neutral by default
        
        # Create trends object
        trends = {
            "paper_count": total_papers,
            "date_analyzed": datetime.now().isoformat(),
            "top_categories": top_categories,
            "positive_sentiment": positive_sentiment,
            "sentiment_details": {
                "positive_mentions": positive_count,
                "negative_mentions": negative_count
            }
        }
        
        # Save trends to a file - use year/month in filename if provided
        if self.year and self.month:
            date_str = f"{self.year}{self.month:02d}"
        else:
            date_str = datetime.now().strftime('%Y%m%d')
            
        output_file = os.path.join(
            self.output_dir,
            f"research_trends_{date_str}.json"
        )
        
        with open(output_file, 'w') as f:
            json.dump(trends, f, indent=2)
        
        logger.info(f"Saved research trends to {output_file}")
        logger.info(f"Processing complete. Analyzed {trends['paper_count']} papers.")
        logger.info(f"Positive sentiment: {trends['positive_sentiment']:.2f}%")
        
        return trends


def main():
    parser = argparse.ArgumentParser(description='Process ArXiv papers data')
    parser.add_argument('--year', type=int, help='Year to process data for')
    parser.add_argument('--month', type=int, help='Month to process data for')
    parser.add_argument('--input-dir', default='./data/raw/arxiv', help='Input directory')
    parser.add_argument('--output-dir', default='./data/processed', help='Output directory')
    
    args = parser.parse_args()
    
    processor = ResearchProcessor(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        year=args.year,
        month=args.month
    )
    processor.process_research()


if __name__ == "__main__":
    main()