# scripts/analysis/calculate_index.py
import json
import logging
import os
import sys
from datetime import datetime

import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("index_calculation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("index-calculator")

class IndexCalculator:
    def __init__(self, 
                 input_dir="./data/processed", 
                 output_dir="./data/processed",
                 news_file="workforce_events_{date}.json",
                 research_file="research_trends_{date}.json",
                 employment_file="employment_stats_{date}.json",
                 job_file="job_trends_{date}.json"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Get latest files if date placeholders are used
        today = datetime.now().strftime('%Y%m%d')
        self.news_file = news_file.format(date=today)
        self.research_file = research_file.format(date=today)
        self.employment_file = employment_file.format(date=today)
        self.job_file = job_file.format(date=today)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Component weights for the index
        self.weights = {
            "news_events": 0.35,
            "research_trends": 0.10,
            "employment_stats": 0.30,
            "job_trends": 0.25
        }
    
    def load_data(self, filepath):
        """Load data from a JSON file."""
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"File not found: {filepath}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {filepath}")
            return None
    
    def calculate_news_events_score(self, news_data):
        """Calculate score from news events component."""
        if not news_data or "events" not in news_data:
            logger.warning("No news events data available")
            return 0, {"error": "No data available"}
        
        events = news_data["events"]
        
        if not events:
            return 0, {"count": 0}
        
        # Define event type scores
        event_scores = {
            "layoff": -1.0,
            "hiring": 1.0,
            "restructuring": -0.5,
            "reskilling": 0.5
        }
        
        # Define AI relation multipliers
        ai_relation_multipliers = {
            "Direct": 1.0,
            "Indirect": 0.6,
            "None": 0.2
        }
        
        # Calculate total weighted impact
        total_impact = 0
        event_counts = {event_type: 0 for event_type in event_scores}
        ai_relation_counts = {relation: 0 for relation in ai_relation_multipliers}
        company_impacts = {}
        
        for event in events:
            event_type = event.get("event_type", "unknown")
            if event_type not in event_scores:
                continue
                
            event_counts[event_type] += 1
            
            ai_relation = event.get("ai_relation", "None")
            ai_relation_counts[ai_relation] += 1
            
            # Get count (default to 100 if not specified)
            count = event.get("count") or 100
            
            # Calculate impact
            impact = event_scores[event_type] * ai_relation_multipliers[ai_relation] * (count / 1000)
            total_impact += impact
            
            # Track company impacts
            company = event.get("company", "Unknown")
            if company not in company_impacts:
                company_impacts[company] = 0
            company_impacts[company] += impact
        
        # Normalize to -100 to +100 scale
        total_count = sum(event_counts.values())
        if total_count > 0:
            avg_impact = total_impact / total_count
            normalized_score = np.clip(avg_impact * 20, -100, 100)  # Scale and clip
        else:
            normalized_score = 0
        
        # Top impacted companies (positive and negative)
        top_positive = sorted(
            [(k, v) for k, v in company_impacts.items() if v > 0],
            key=lambda x: x[1], reverse=True
        )[:5]
        
        top_negative = sorted(
            [(k, v) for k, v in company_impacts.items() if v < 0],
            key=lambda x: x[1]
        )[:5]
        
        return normalized_score, {
            "count": total_count,
            "event_counts": event_counts,
            "ai_relation_counts": ai_relation_counts,
            "top_positive_companies": top_positive,
            "top_negative_companies": top_negative
        }
    
    def calculate_job_trends_score(self, job_data):
        """Calculate score from job postings component."""
        if not job_data:
            logger.warning("No job trends data available")
            return 0, {"error": "No data available"}
        
        ai_postings = job_data.get("ai_related_postings", [])
        
        if not ai_postings or len(ai_postings) < 2:
            return 0, {"count": 0}
        
        # Calculate growth rate of AI job postings
        current = ai_postings[-1].get("count", 0)
        previous = ai_postings[-2].get("count", 0)
        
        if previous > 0:
            growth_rate = ((current - previous) / previous) * 100
        else:
            growth_rate = 0
        
        # Calculate average growth rate of emerging roles
        emerging_roles = job_data.get("top_growing_titles", [])
        if emerging_roles:
            avg_role_growth = sum(role.get("growth_rate", 0) for role in emerging_roles) / len(emerging_roles)
        else:
            avg_role_growth = 0
        
        # Combine metrics with weighting
        combined_score = (growth_rate * 0.6) + (avg_role_growth * 0.4)
        
        # Normalize to -100 to +100 scale
        normalized_score = np.clip(combined_score * 2, -100, 100)  # Scale and clip
        
        return normalized_score, {
            "ai_job_growth_rate": growth_rate,
            "avg_emerging_role_growth": avg_role_growth,
            "current_posting_count": current,
            "previous_posting_count": previous
        }
    
    def calculate_research_trends_score(self, research_data):
        """Calculate score from academic research component."""
        if not research_data:
            logger.warning("No research trends data available")
            return 0, {"error": "No data available"}
        
        # Simplified implementation - in a real system, would analyze research sentiment
        paper_count = research_data.get("paper_count", 0)
        positive_sentiment = research_data.get("positive_sentiment", 50)  # Default to neutral
        
        # Calculate normalized score based on sentiment
        # Assume research sentiment is on a 0-100 scale, convert to -100 to +100
        normalized_score = (positive_sentiment - 50) * 2
        
        return normalized_score, {
            "paper_count": paper_count,
            "positive_sentiment": positive_sentiment
        }
    
    def calculate_employment_stats_score(self, employment_data):
        """Calculate score from employment statistics component."""
        if not employment_data or "industries" not in employment_data:
            logger.warning("No employment statistics data available")
            return 0, {"error": "No data available"}
        
        industries = employment_data.get("industries", {})
        
        if not industries:
            return 0, {"count": 0}
        
        # Industry weights (importance to AI impact assessment)
        industry_weights = {
            "information": 2.0,
            "professional_services": 1.5,
            "finance": 1.2,
            "healthcare": 1.0,
            "manufacturing": 1.0,
            "retail": 0.8,
            "transportation": 0.7,
            "construction": 0.5,
            "hospitality": 0.5
        }
        
        # Calculate weighted employment change across industries
        total_weight = 0
        weighted_change = 0
        
        industry_impacts = {}
        
        for industry, data in industries.items():
            # Get standardized industry name
            std_industry = industry.lower().replace(" ", "_")
            weight = industry_weights.get(std_industry, 1.0)
            
            # Get employment change percentage
            change_pct = data.get("change_percentage", 0)
            
            weighted_change += change_pct * weight
            total_weight += weight
            
            industry_impacts[industry] = change_pct
        
        if total_weight > 0:
            avg_weighted_change = weighted_change / total_weight
        else:
            avg_weighted_change = 0
        
        # Normalize to -100 to +100 scale
        normalized_score = np.clip(avg_weighted_change * 25, -100, 100)  # Scale and clip
        
        # Sort industries by impact
        growing_industries = sorted(
            [(k, v) for k, v in industry_impacts.items() if v > 0],
            key=lambda x: x[1], reverse=True
        )
        
        declining_industries = sorted(
            [(k, v) for k, v in industry_impacts.items() if v < 0],
            key=lambda x: x[1]
        )
        
        return normalized_score, {
            "avg_weighted_change": avg_weighted_change,
            "growing_industries": growing_industries,
            "declining_industries": declining_industries
        }
    
    def calculate_index(self):
        """Calculate the overall AI Labor Market Impact Index."""
        # Load data files
        news_data = self.load_data(os.path.join(self.input_dir, self.news_file))
        research_data = self.load_data(os.path.join(self.input_dir, self.research_file))
        employment_data = self.load_data(os.path.join(self.input_dir, self.employment_file))
        job_data = self.load_data(os.path.join(self.input_dir, self.job_file))
        
        # Calculate component scores
        news_score, news_details = self.calculate_news_events_score(news_data)
        research_score, research_details = self.calculate_research_trends_score(research_data)
        employment_score, employment_details = self.calculate_employment_stats_score(employment_data)
        job_score, job_details = self.calculate_job_trends_score(job_data)
        
        logger.info(f"Component scores - News: {news_score:.2f}, Research: {research_score:.2f}, "
                   f"Employment: {employment_score:.2f}, Jobs: {job_score:.2f}")
        
        # Calculate weighted index
        components = {
            "news_events": {
                "score": news_score,
                "weight": self.weights["news_events"],
                "weighted_score": news_score * self.weights["news_events"],
                "details": news_details
            },
            "research_trends": {
                "score": research_score,
                "weight": self.weights["research_trends"],
                "weighted_score": research_score * self.weights["research_trends"],
                "details": research_details
            },
            "employment_stats": {
                "score": employment_score,
                "weight": self.weights["employment_stats"],
                "weighted_score": employment_score * self.weights["employment_stats"],
                "details": employment_details
            },
            "job_trends": {
                "score": job_score,
                "weight": self.weights["job_trends"],
                "weighted_score": job_score * self.weights["job_trends"],
                "details": job_details
            }
        }
        
        # Calculate final index value
        final_index = sum(component["weighted_score"] for component in components.values())
        
        # Determine index interpretation
        if final_index >= 50:
            interpretation = "Strong job creation from AI"
        elif final_index >= 20:
            interpretation = "Moderate job creation from AI"
        elif final_index >= 0:
            interpretation = "Slight job creation from AI"
        elif final_index >= -20:
            interpretation = "Slight job displacement from AI"
        elif final_index >= -50:
            interpretation = "Moderate job displacement from AI"
        else:
            interpretation = "Severe job displacement from AI"
        
        # Create the index object
        index = {
            "timestamp": datetime.now().isoformat(),
            "index_value": final_index,
            "interpretation": interpretation,
            "components": components,
            "meta": {
                "component_weights": self.weights,
                "data_sources": {
                    "news_events": self.news_file,
                    "research_trends": self.research_file,
                    "employment_stats": self.employment_file,
                    "job_trends": self.job_file
                }
            }
        }
        
        return index
    
    def save_index(self, index):
        """Save the calculated index to a file."""
        output_file = os.path.join(
            self.output_dir,
            f"ai_labor_index_{datetime.now().strftime('%Y%m%d')}.json"
        )
        
        with open(output_file, 'w') as f:
            json.dump(index, f, indent=2)
        
        logger.info(f"Saved AI Labor Market Impact Index to {output_file}")
        
        # Also update the latest index file
        latest_file = os.path.join(self.output_dir, "ai_labor_index_latest.json")
        with open(latest_file, 'w') as f:
            json.dump(index, f, indent=2)
        
        logger.info(f"Updated latest index file at {latest_file}")
        
        return output_file
    
    def update_index_history(self, index):
        """Update the index history file with the new index value."""
        history_file = os.path.join(self.output_dir, "index_history.json")
        
        try:
            if os.path.exists(history_file):
                with open(history_file, 'r') as f:
                    history = json.load(f)
            else:
                history = {
                    "history": []
                }
            
            # Add new entry
            history["history"].append({
                "timestamp": index["timestamp"],
                "index_value": index["index_value"],
                "interpretation": index["interpretation"]
            })
            
            # Sort by timestamp
            history["history"] = sorted(
                history["history"],
                key=lambda x: x["timestamp"]
            )
            
            # Save updated history
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2)
            
            logger.info(f"Updated index history at {history_file}")
            
        except Exception as e:
            logger.error(f"Error updating index history: {str(e)}")


def main():
    calculator = IndexCalculator()
    
    # Calculate the index
    index = calculator.calculate_index()
    
    # Save the index
    calculator.save_index(index)
    
    # Update index history
    calculator.update_index_history(index)
    
    logger.info(f"Index calculation complete. Index value: {index['index_value']:.2f}")
    logger.info(f"Interpretation: {index['interpretation']}")


if __name__ == "__main__":
    main()