import json
import logging
import os
import sys
import argparse
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
                 year=None,
                 month=None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.year = year
        self.month = month
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filenames based on year/month if provided, otherwise use current date
        if year and month:
            date_str = f"{year}{month:02d}"
        else:
            date_str = datetime.now().strftime('%Y%m%d')
            
        self.news_file = f"workforce_events_{date_str}.json"
        self.research_file = f"research_trends_{date_str}.json"
        self.employment_file = f"employment_stats_{date_str}.json"
        self.job_file = f"job_trends_{date_str}.json"
        
        # Log the files we'll be using
        logger.info(f"Using data files:")
        logger.info(f"  News: {self.news_file}")
        logger.info(f"  Research: {self.research_file}")
        logger.info(f"  Employment: {self.employment_file}")
        logger.info(f"  Jobs: {self.job_file}")
        
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
        
        # Check if this is Anthropic Economic Index data
        if job_data.get("source") == "Anthropic Economic Index":
            return self.calculate_anthropic_index_score(job_data)
        
        # Original logic for Remotive API data
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
    
    def calculate_anthropic_index_score(self, job_data):
        """Calculate score from Anthropic Economic Index data."""
        if not job_data or "statistics" not in job_data:
            logger.warning("No Anthropic Index statistics available")
            return 0, {"error": "No Anthropic data available"}
            
        # Check if data is simulated
        is_simulated = job_data.get("is_simulated_data", False)
        
        # Extract key metrics from the Anthropic data
        statistics = job_data.get("statistics", {})
        avg_augmentation = statistics.get("average_augmentation_rate", 0)
        avg_automation = statistics.get("average_automation_rate", 0)
        ratio = statistics.get("automation_augmentation_ratio", 1)
        
        # Extract top roles data
        top_augmented = job_data.get("top_augmented_roles", [])
        top_automated = job_data.get("top_automated_roles", [])
        
        # Calculate augmentation impact (positive factor)
        augmentation_score = avg_augmentation * 1.5  # Scale to appropriate range
        
        # Calculate automation impact (negative factor)
        automation_score = -avg_automation  # Negative impact
        
        # Calculate role diversity impact
        role_diversity = len(top_augmented) + len(top_automated)
        diversity_factor = min(role_diversity / 10, 1.0)  # Cap at 1.0
        
        # Combined score calculation
        # Higher augmentation and lower automation ratio = more positive score
        combined_score = (augmentation_score * 0.6) + (automation_score * 0.4)
        
        # Adjust for healthy automation/augmentation ratio
        if ratio < 1:  # More augmentation than automation
            ratio_bonus = (1 - ratio) * 20  # Bonus for more augmentation
        else:
            ratio_bonus = -(ratio - 1) * 10  # Penalty for more automation
            
        combined_score += ratio_bonus
        
        # Apply diversity factor
        combined_score *= (0.8 + (0.2 * diversity_factor))
        
        # Normalize to -100 to +100 scale
        normalized_score = np.clip(combined_score * 1.5, -100, 100)  # Scale and clip
        
        logger.info(f"Anthropic Index Score: Aug={avg_augmentation:.2f}, Auto={avg_automation:.2f}, Ratio={ratio:.2f}")
        logger.info(f"Combined score: {combined_score:.2f}, Normalized: {normalized_score:.2f}")
        
        return normalized_score, {
            "augmentation_rate": avg_augmentation,
            "automation_rate": avg_automation,
            "automation_augmentation_ratio": ratio,
            "top_augmented_roles_count": len(top_augmented),
            "top_automated_roles_count": len(top_automated),
            "source": "Anthropic Economic Index",
            "is_simulated_data": is_simulated
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
    
    def get_industry_weight(self, industry_name):
        """Get industry weight with robust matching."""
        # Industry weights matching actual BLS categories
        industry_weights = {
            "Information": 2.0,
            "Professional and Business Services": 1.5,
            "Financial Activities": 1.2,
            "Education and Health Services": 1.0,
            "Manufacturing": 1.0,
            "Trade, Transportation, and Utilities": 0.8,
            "Construction": 0.5,
            "Leisure and Hospitality": 0.5,
            "Mining and Logging": 0.4,
            "Other Services": 0.6,
            "Government": 0.7,
            "Total Nonfarm": 1.0  # Reference category, not typically used in calculation
        }
        
        # Direct match attempt
        if industry_name in industry_weights:
            return industry_weights[industry_name]
        
        # Try case-insensitive match
        for key, value in industry_weights.items():
            if key.lower() == industry_name.lower():
                logger.info(f"Industry '{industry_name}' matched to '{key}' via case-insensitive comparison")
                return value
        
        # Try standardized format (lowercase, no spaces/commas)
        std_name = industry_name.lower().replace(" ", "").replace(",", "")
        for key, value in industry_weights.items():
            if key.lower().replace(" ", "").replace(",", "") == std_name:
                logger.info(f"Industry '{industry_name}' matched to '{key}' via standardized format")
                return value
        
        # No match found
        logger.warning(f"No weight defined for industry: '{industry_name}', using default weight of 1.0")
        return 1.0
        
    def log_industry_weights_usage(self, industries):
        """Log which industry weights are being used in calculation"""
        logger.info("Industry weights being applied:")
        for industry in industries:
            weight = self.get_industry_weight(industry)
            logger.info(f"  {industry}: {weight}")
    
    def calculate_employment_stats_score(self, employment_data):
        """Calculate score from employment statistics component."""
        if not employment_data or "industries" not in employment_data:
            logger.warning("No employment statistics data available")
            return 0, {"error": "No data available"}
        
        industries = employment_data.get("industries", {})
        
        if not industries:
            return 0, {"count": 0}
        
        # Log which weights are being applied
        self.log_industry_weights_usage(industries)
        
        # Calculate weighted employment change across industries
        total_weight = 0
        weighted_change = 0
        
        industry_impacts = {}
        
        for industry, data in industries.items():
            # Get weight using robust matching
            weight = self.get_industry_weight(industry)
            
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
        
        # Check for any data source period mismatches or fallback data
        notes = []
        data_sources = {
            "news_events": self.news_file,
            "research_trends": self.research_file,
            "employment_stats": self.employment_file,
            "job_trends": self.job_file
        }
        
        actual_periods = {}
        target_period = f"{self.year}-{self.month:02d}" if self.year and self.month else None
        
        # Check for Anthropic data source mismatches
        if job_data and "source" in job_data and job_data["source"] == "Anthropic Economic Index":
            anthropic_source_period = job_data.get("source_period")
            using_fallback = job_data.get("using_fallback_data", False)
            
            if anthropic_source_period and target_period and anthropic_source_period != target_period:
                notes.append(f"Note: Anthropic Economic Index data is from {anthropic_source_period} instead of the requested period {target_period}")
                actual_periods["anthropic_index"] = anthropic_source_period
                
            if using_fallback:
                notes.append(f"Note: Using fallback Anthropic data due to unavailability of data for {target_period}")
                
            if job_data.get("is_simulated_data", False):
                notes.append("Warning: Using simulated Anthropic data instead of actual data")
        
        # Create the index object
        index = {
            "timestamp": datetime.now().isoformat(),
            "index_value": final_index,
            "interpretation": interpretation,
            "components": components,
            "news_events": news_data.get("events", []) if news_data else [],
            "notes": notes,
            "methodology": {
                "component_weights": self.weights,
                "industry_weights": {
                    "Information": 2.0,
                    "Professional and Business Services": 1.5,
                    "Financial Activities": 1.2,
                    "Education and Health Services": 1.0,
                    "Manufacturing": 1.0,
                    "Trade, Transportation, and Utilities": 0.8,
                    "Construction": 0.5,
                    "Leisure and Hospitality": 0.5,
                    "Mining and Logging": 0.4,
                    "Other Services": 0.6,
                    "Government": 0.7,
                    "Total Nonfarm": 1.0
                },
                "data_sources": data_sources,
                "actual_data_periods": actual_periods
            }
        }
        
        return index
    
    def save_index(self, index):
        """Save the calculated index to a file."""
        # Use year/month in filename if provided
        if self.year and self.month:
            date_str = f"{self.year}{self.month:02d}"
        else:
            date_str = datetime.now().strftime('%Y%m%d')
            
        output_file = os.path.join(
            self.output_dir,
            f"ai_labor_index_{date_str}.json"
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
            # Load existing history, ensuring we preserve it
            if os.path.exists(history_file):
                with open(history_file, 'r') as f:
                    history = json.load(f)
                    logger.info(f"Loaded existing history with {len(history.get('history', []))} entries")
            else:
                history = {
                    "generated_at": datetime.now().isoformat(),
                    "history": []
                }
                logger.info("No existing history found, creating new history")
            
            # Format date for this entry
            if self.year and self.month:
                date_str = f"{self.year}-{self.month:02d}"
            else:
                date_str = index["timestamp"].split("T")[0][:7]  # YYYY-MM
            
            # Create new entry
            new_entry = {
                "date": date_str,
                "value": index["index_value"],
                "interpretation": index["interpretation"],
                "timestamp": index["timestamp"]
            }
            
            # Update or add entry for current month only
            found = False
            for i, entry in enumerate(history.get("history", [])):
                if entry.get("date") == date_str:
                    history["history"][i] = new_entry
                    found = True
                    break
            
            if not found:
                if "history" not in history:
                    history["history"] = []
                history["history"].append(new_entry)
            
            # Sort by date
            history["history"] = sorted(
                history["history"],
                key=lambda x: x.get("date", "")
            )
            
            # Update generation timestamp and save
            history["generated_at"] = datetime.now().isoformat()
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2)
            
            # Critical fix: Add history to index AFTER sorting
            index["history"] = history["history"]
            
        except Exception as e:
            logger.error(f"Error updating index history: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='Calculate AI Labor Market Impact Index')
    parser.add_argument('--input-dir', default='./data/processed', help='Input directory containing processed data')
    parser.add_argument('--output-dir', default='./data/processed', help='Output directory for index files')
    parser.add_argument('--year', type=int, required=True, help='Year to calculate index for')
    parser.add_argument('--month', type=int, required=True, help='Month to calculate index for')
    
    args = parser.parse_args()
    
    logger.info(f"Calculating AI Labor Market Index for {args.year}-{args.month:02d}")
    
    calculator = IndexCalculator(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        year=args.year,
        month=args.month
    )
    
    # Calculate the index
    index = calculator.calculate_index()
    
    # Save the index
    calculator.save_index(index)
    
    # Update index history
    calculator.update_index_history(index)
    
    logger.info(f"Index calculation complete. Index value: {index['index_value']:.2f}")
    logger.info(f"Interpretation: {index['interpretation']}")
    
    # Log any notes about data source mismatches
    if index.get("notes"):
        logger.warning("Important notes about data sources:")
        for note in index.get("notes", []):
            logger.warning(f"  - {note}")
    
    # Log the actual data periods used
    actual_periods = index.get("methodology", {}).get("actual_data_periods", {})
    if actual_periods:
        logger.info("Actual data periods used:")
        for source, period in actual_periods.items():
            logger.info(f"  - {source}: {period}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())