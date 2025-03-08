# scripts/processing/process_research.py
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
        logging.FileHandler("research_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("research-processor")

class ResearchProcessor:
    def __init__(self, input_dir="./data/raw/arxiv", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def process_research(self):
        """Process ArXiv research papers and extract trends."""
        # Find all ArXiv data files
        arxiv_files = glob.glob(os.path.join(self.input_dir, "*.json"))
        
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
                    all_papers.extend(papers)
                    logger.info(f"Loaded {len(papers)} papers from {file_path}")
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
        
        # Save trends to a file
        output_file = os.path.join(
            self.output_dir,
            f"research_trends_{datetime.now().strftime('%Y%m%d')}.json"
        )
        
        with open(output_file, 'w') as f:
            json.dump(trends, f, indent=2)
        
        logger.info(f"Saved research trends to {output_file}")
        
        return trends


def main():
    processor = ResearchProcessor()
    trends = processor.process_research()
    
    if trends:
        logger.info(f"Processing complete. Analyzed {trends['paper_count']} papers.")
        logger.info(f"Positive sentiment: {trends['positive_sentiment']:.2f}%")
    else:
        logger.error("Processing failed or no data was found.")


if __name__ == "__main__":
    main()