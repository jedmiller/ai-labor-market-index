#!/usr/bin/env python3
# scripts/analysis/calculate_ai_impact.py
"""
AI Labor Market Impact Calculator - Implements the updated methodology
"""
import json
import logging
import os
import sys
import argparse
from datetime import datetime
import numpy as np
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ai_impact_calculation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("ai-impact-calculator")

class AIImpactCalculator:
    """
    Calculates AI impact on employment using the updated methodology:
    Net Employment Impact = Employment × [1 - Displacement Effect + Creation Effect × Market_Maturity + Demand Effect]
    """
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
            self.date_str = f"{year}{month:02d}"
        else:
            self.date_str = datetime.now().strftime('%Y%m%d')
        
        # Define input files needed for calculations
        self.employment_file = f"employment_stats_{self.date_str}.json"
        self.job_trends_file = f"job_trends_{self.date_str}.json"
        self.workforce_events_file = f"workforce_events_{self.date_str}.json"
        self.research_trends_file = f"research_trends_{self.date_str}.json"
        
        # Define industry-specific parameters for the calculation
        self.INDUSTRY_PARAMS = {
            "Information": {
                "weight": 2.0,
                "adoption_ceiling": 0.95,
                "adoption_speed": 1.5,
                "displacement_factor": 0.6
            },
            "Professional and Business Services": {
                "weight": 1.5,
                "adoption_ceiling": 0.85,
                "adoption_speed": 1.2,
                "displacement_factor": 0.5
            },
            "Financial Activities": {
                "weight": 1.2,
                "adoption_ceiling": 0.85,
                "adoption_speed": 1.3,
                "displacement_factor": 0.6
            },
            "Education and Health Services": {
                "weight": 1.0,
                "adoption_ceiling": 0.75,
                "adoption_speed": 0.7,
                "displacement_factor": 0.3
            },
            "Manufacturing": {
                "weight": 1.0,
                "adoption_ceiling": 0.80,
                "adoption_speed": 1.0,
                "displacement_factor": 0.8
            },
            "Trade, Transportation, and Utilities": {
                "weight": 0.8,
                "adoption_ceiling": 0.70,
                "adoption_speed": 1.2,
                "displacement_factor": 0.7
            },
            "Leisure and Hospitality": {
                "weight": 0.5,
                "adoption_ceiling": 0.65,
                "adoption_speed": 0.8,
                "displacement_factor": 0.7
            },
            "Construction": {
                "weight": 0.5,
                "adoption_ceiling": 0.60,
                "adoption_speed": 0.7,
                "displacement_factor": 0.6
            },
            "Mining and Logging": {
                "weight": 0.4,
                "adoption_ceiling": 0.55,
                "adoption_speed": 0.6,
                "displacement_factor": 0.5
            },
            "Other Services": {
                "weight": 0.6,
                "adoption_ceiling": 0.65,
                "adoption_speed": 0.8,
                "displacement_factor": 0.5
            },
            "Government": {
                "weight": 0.7,
                "adoption_ceiling": 0.60,
                "adoption_speed": 0.5,
                "displacement_factor": 0.4
            },
            "Total Nonfarm": {
                "weight": 1.0,
                "adoption_ceiling": 0.75,
                "adoption_speed": 1.0,
                "displacement_factor": 0.6
            }
        }
        
        # Default values for calculation parameters when data is missing
        self.DEFAULT_PARAMS = {
            "implementation_rate": 0.12,  # 12% annual implementation rate
            "efficiency_factor": 0.5,     # 50% efficiency for augmentation
            "adoption_rate": 0.3,         # 30% adoption rate from survey data
            "capacity_utilization": 0.7,  # 70% capacity utilization
            "market_maturity": 0.4,       # Market maturity (moderate stage)
            "productivity_gain": 0.15,    # 15% productivity gain
            "labor_share": 0.6,           # 60% labor share of revenue
            "elasticity_factor": 0.1      # Slight positive elasticity
        }

    def load_data(self, filepath):
        """Load data from a JSON file."""
        try:
            with open(os.path.join(self.input_dir, filepath), 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"File not found: {filepath}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {filepath}")
            return None

    def get_industry_param(self, industry, param_name):
        """Get industry-specific parameter with robust matching."""
        # First, try exact match
        if industry in self.INDUSTRY_PARAMS:
            return self.INDUSTRY_PARAMS[industry].get(param_name, self.DEFAULT_PARAMS.get(param_name))
        
        # Try case-insensitive match
        for key in self.INDUSTRY_PARAMS:
            if key.lower() == industry.lower():
                logger.info(f"Industry '{industry}' matched to '{key}' via case-insensitive comparison")
                return self.INDUSTRY_PARAMS[key].get(param_name, self.DEFAULT_PARAMS.get(param_name))
        
        # Try standardized format (lowercase, no spaces/commas)
        std_name = industry.lower().replace(" ", "").replace(",", "")
        for key in self.INDUSTRY_PARAMS:
            if key.lower().replace(" ", "").replace(",", "") == std_name:
                logger.info(f"Industry '{industry}' matched to '{key}' via standardized format")
                return self.INDUSTRY_PARAMS[key].get(param_name, self.DEFAULT_PARAMS.get(param_name))
        
        # No match found, use default
        logger.warning(f"No specific parameters for industry: '{industry}', using default value")
        return self.DEFAULT_PARAMS.get(param_name)

    def calculate_pure_automation_impact(self, automation_pct, industry):
        """
        Calculate the Pure Automation Impact component.
        Pure Automation Impact = Automation% × Displacement Factor × Implementation Rate
        """
        displacement_factor = self.get_industry_param(industry, "displacement_factor")
        implementation_rate = self.DEFAULT_PARAMS["implementation_rate"]
        
        # Convert percentage to decimal
        automation_pct = automation_pct / 100 if automation_pct > 1 else automation_pct
        
        return automation_pct * displacement_factor * implementation_rate

    def calculate_capacity_augmentation_impact(self, augmentation_pct, industry):
        """
        Calculate the Capacity Augmentation Impact component.
        Capacity Augmentation Impact = Augmentation% × Efficiency Factor × Adoption Rate × Capacity Utilization Factor
        """
        efficiency_factor = self.DEFAULT_PARAMS["efficiency_factor"]
        adoption_rate = self.DEFAULT_PARAMS["adoption_rate"]
        capacity_utilization = self.DEFAULT_PARAMS["capacity_utilization"]
        
        # Convert percentage to decimal
        augmentation_pct = augmentation_pct / 100 if augmentation_pct > 1 else augmentation_pct
        
        return augmentation_pct * efficiency_factor * adoption_rate * capacity_utilization

    def calculate_displacement_effect(self, industry_data, anthropic_data):
        """
        Calculate the overall Displacement Effect.
        Displacement Effect = Pure Automation Impact + Capacity Augmentation Impact
        """
        # Extract automation and augmentation percentages from Anthropic data
        industry_automation = {}
        industry_augmentation = {}
        
        # Get data from Anthropic Economic Index if available
        if anthropic_data and "statistics" in anthropic_data:
            avg_automation = anthropic_data["statistics"].get("average_automation_rate", 0)
            avg_augmentation = anthropic_data["statistics"].get("average_augmentation_rate", 0)
            
            # Use as default values for all industries
            for industry in industry_data:
                industry_automation[industry] = avg_automation
                industry_augmentation[industry] = avg_augmentation
        
        # Calculate displacement effect for each industry
        displacement_effects = {}
        for industry, data in industry_data.items():
            auto_pct = industry_automation.get(industry, 30)  # Default to 30% if no data
            aug_pct = industry_augmentation.get(industry, 70)  # Default to 70% if no data
            
            # Calculate components
            pure_automation = self.calculate_pure_automation_impact(auto_pct, industry)
            capacity_augmentation = self.calculate_capacity_augmentation_impact(aug_pct, industry)
            
            # Capacity augmentation has a negative impact on displacement (it offsets it)
            displacement_effect = pure_automation - capacity_augmentation
            
            # Ensure displacement effect is within reasonable bounds
            displacement_effect = np.clip(displacement_effect, 0, 0.8)
            
            displacement_effects[industry] = {
                "effect": displacement_effect,
                "components": {
                    "pure_automation": pure_automation,
                    "capacity_augmentation": -capacity_augmentation
                }
            }
        
        # Calculate overall average displacement effect
        if displacement_effects:
            avg_displacement = np.mean([data["effect"] for data in displacement_effects.values()])
        else:
            avg_displacement = 0
            
        return avg_displacement, displacement_effects

    def calculate_creation_effect(self, industry_data, job_data):
        """
        Calculate the Creation Effect component.
        Creation Effect = (Direct AI Jobs/Total Employment) + (AI Infrastructure Jobs/Total Employment)
        """
        # Extract AI job data from job trends
        direct_ai_jobs = {}
        infrastructure_ai_jobs = {}
        
        # Default ratios if job data is not available
        default_direct_ratio = 0.02  # 2% of jobs are direct AI roles
        default_infra_ratio = 0.01   # 1% of jobs are AI infrastructure roles
        
        # Get industry employment figures
        industry_employment = {}
        total_employment = 0
        
        for industry, data in industry_data.items():
            employment = data.get("current", 0)
            industry_employment[industry] = employment
            total_employment += employment
        
        # If job data is available, extract AI job counts
        ai_job_ratio = 0
        if job_data and "source" in job_data and job_data["source"] == "Anthropic Economic Index":
            # Use augmentation data as a proxy for AI job creation potential
            if "top_augmented_roles" in job_data:
                top_roles = job_data["top_augmented_roles"]
                # Estimate job creation based on augmentation rates
                for role in top_roles:
                    aug_rate = role.get("augmentation_rate", 0) / 100 if role.get("augmentation_rate", 0) > 1 else role.get("augmentation_rate", 0)
                    # Assume 10% of augmentation translates to new jobs
                    ai_job_ratio += aug_rate * 0.1 / len(top_roles)
        else:
            # Use default values
            ai_job_ratio = default_direct_ratio + default_infra_ratio
        
        # Calculate creation effect for each industry (apply ratio to all industries)
        creation_effects = {}
        for industry, employment in industry_employment.items():
            # Apply industry-specific multipliers to the baseline ratio
            weight = self.get_industry_param(industry, "weight")
            industry_ratio = ai_job_ratio * weight
            
            # Calculate creation effect
            creation_effect = industry_ratio
            
            # Ensure within reasonable bounds
            creation_effect = np.clip(creation_effect, 0, 0.5)
            
            creation_effects[industry] = {
                "effect": creation_effect,
                "components": {
                    "direct_ai_jobs_ratio": industry_ratio * 0.7,  # 70% direct jobs
                    "infrastructure_jobs_ratio": industry_ratio * 0.3  # 30% infrastructure jobs
                }
            }
        
        # Calculate overall average creation effect
        if creation_effects:
            avg_creation = np.mean([data["effect"] for data in creation_effects.values()])
        else:
            avg_creation = 0
            
        return avg_creation, creation_effects

    def calculate_market_maturity(self):
        """
        Calculate the Market Maturity factor.
        Market Maturity = Years Since AI Adoption / Expected Maturity Period
        
        Ranges:
        - Early (0-2 years): 0.2
        - Growth (2-5 years): 0.5
        - Mature (5+ years): 0.8
        """
        # For now, use a fixed value based on the current market stage
        # This can be refined with more data about industry-specific adoption rates
        current_year = datetime.now().year
        
        # Calculate years since significant AI adoption (2020 as the base year)
        years_since_adoption = current_year - 2020
        
        if years_since_adoption <= 2:
            maturity = 0.2  # Early stage
        elif years_since_adoption <= 5:
            maturity = 0.5  # Growth stage
        else:
            maturity = 0.8  # Mature stage
            
        return maturity

    def calculate_demand_effect(self, industry_data):
        """
        Calculate the Demand Effect component.
        Demand Effect = Productivity Gain × Labor Share × Elasticity Factor
        """
        # Get default values
        productivity_gain = self.DEFAULT_PARAMS["productivity_gain"]
        labor_share = self.DEFAULT_PARAMS["labor_share"]
        elasticity_factor = self.DEFAULT_PARAMS["elasticity_factor"]
        
        # Calculate demand effect for each industry
        demand_effects = {}
        for industry, data in industry_data.items():
            # Apply industry-specific weights
            weight = self.get_industry_param(industry, "weight")
            industry_productivity = productivity_gain * weight
            
            # Calculate demand effect
            demand_effect = industry_productivity * labor_share * elasticity_factor
            
            # Ensure within reasonable bounds
            demand_effect = np.clip(demand_effect, -0.1, 0.2)
            
            demand_effects[industry] = {
                "effect": demand_effect,
                "components": {
                    "productivity_gain": industry_productivity,
                    "labor_share": labor_share,
                    "elasticity_factor": elasticity_factor
                }
            }
        
        # Calculate overall average demand effect
        if demand_effects:
            avg_demand = np.mean([data["effect"] for data in demand_effects.values()])
        else:
            avg_demand = 0
            
        return avg_demand, demand_effects

    def calculate_net_impact(self, employment_data, job_data, events_data=None, research_data=None):
        """
        Calculate the Net Employment Impact using the formula:
        Net Employment Impact = Employment × [1 - Displacement Effect + Creation Effect × Market_Maturity + Demand Effect]
        """
        industries = employment_data.get("industries", {})
        
        # Calculate market maturity (a global factor)
        market_maturity = self.calculate_market_maturity()
        logger.info(f"Market Maturity: {market_maturity:.2f}")
        
        # Calculate components
        displacement_avg, displacement_by_industry = self.calculate_displacement_effect(industries, job_data)
        creation_avg, creation_by_industry = self.calculate_creation_effect(industries, job_data)
        demand_avg, demand_by_industry = self.calculate_demand_effect(industries)
        
        logger.info(f"Component averages - Displacement: {displacement_avg:.4f}, Creation: {creation_avg:.4f}, Demand: {demand_avg:.4f}")
        
        # Calculate net impact for each industry
        net_impact_by_industry = {}
        total_jobs_affected = 0
        
        for industry, data in industries.items():
            current_employment = data.get("current", 0)
            
            # Get effects for this industry
            displacement = displacement_by_industry.get(industry, {"effect": displacement_avg})["effect"]
            creation = creation_by_industry.get(industry, {"effect": creation_avg})["effect"]
            demand = demand_by_industry.get(industry, {"effect": demand_avg})["effect"]
            
            # Calculate net impact percentage
            net_impact_pct = 1 - displacement + (creation * market_maturity) + demand
            
            # Calculate jobs affected
            jobs_affected = current_employment * (net_impact_pct - 1)
            
            # Store results
            net_impact_by_industry[industry] = {
                "impact": net_impact_pct - 1,  # Convert to percentage change (-0.06 = 6% job loss)
                "jobs_affected": int(jobs_affected),
                "components": {
                    "displacement_effect": -displacement,
                    "creation_effect": creation * market_maturity,
                    "demand_effect": demand
                }
            }
            
            # Add to total (excluding "Total Nonfarm" to avoid double counting)
            if industry != "Total Nonfarm":
                total_jobs_affected += int(jobs_affected)
        
        # Calculate overall impact percentage (weighted by employment)
        total_employment = sum([data.get("current", 0) for industry, data in industries.items() if industry != "Total Nonfarm"])
        weighted_impact = sum([data["impact"] * industries[industry].get("current", 0) for industry, data in net_impact_by_industry.items() if industry != "Total Nonfarm"])
        
        if total_employment > 0:
            overall_impact = weighted_impact / total_employment
        else:
            overall_impact = 0
        
        # Prepare result object
        result = {
            "date": f"{self.year}-{self.month:02d}" if self.year and self.month else datetime.now().strftime("%Y-%m"),
            "total_impact": overall_impact,
            "jobs_affected": total_jobs_affected,
            "by_industry": net_impact_by_industry,
            "components": {
                "displacement_effect": -displacement_avg,
                "creation_effect": creation_avg,
                "market_maturity": market_maturity,
                "demand_effect": demand_avg
            }
        }
        
        return result

    def save_results(self, results):
        """Save the calculation results to a file."""
        # Define output filename
        output_file = os.path.join(
            self.output_dir,
            f"ai_labor_impact_{self.date_str}.json"
        )
        
        # Save to file
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Saved AI Labor Market Impact results to {output_file}")
        
        # Create a copy as the latest impact file
        latest_file = os.path.join(self.output_dir, "ai_labor_impact_latest.json")
        with open(latest_file, 'w') as f:
            json.dump(results, f, indent=2)
            
        logger.info(f"Updated latest impact file at {latest_file}")
        
        return output_file

    def calculate_impact(self):
        """Main method to calculate the AI Labor Market Impact index."""
        # Load required data
        employment_data = self.load_data(self.employment_file)
        job_data = self.load_data(self.job_trends_file)
        events_data = self.load_data(self.workforce_events_file)
        research_data = self.load_data(self.research_trends_file)
        
        # Check if we have the required data
        if not employment_data:
            logger.error(f"Missing required employment data file: {self.employment_file}")
            return None
            
        if not job_data:
            logger.warning(f"Missing job trends data file: {self.job_trends_file}")
            logger.warning("Proceeding with default values for automation/augmentation")
        
        # Calculate the impact
        impact_results = self.calculate_net_impact(employment_data, job_data, events_data, research_data)
        
        # Save results
        self.save_results(impact_results)
        
        # Log summary
        logger.info(f"AI Labor Market Impact calculation complete.")
        logger.info(f"Overall impact: {impact_results['total_impact']:.4f} ({impact_results['total_impact']*100:.2f}%)")
        logger.info(f"Total jobs affected: {impact_results['jobs_affected']}")
        
        # Return results
        return impact_results


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Calculate AI Labor Market Impact')
    parser.add_argument('--input-dir', default='./data/processed', help='Input directory containing processed data')
    parser.add_argument('--output-dir', default='./data/processed', help='Output directory for impact files')
    parser.add_argument('--year', type=int, required=True, help='Year to calculate impact for')
    parser.add_argument('--month', type=int, required=True, help='Month to calculate impact for')
    
    args = parser.parse_args()
    
    logger.info(f"Calculating AI Labor Market Impact for {args.year}-{args.month:02d}")
    
    calculator = AIImpactCalculator(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        year=args.year,
        month=args.month
    )
    
    # Calculate impact
    impact = calculator.calculate_impact()
    
    if impact:
        logger.info("Impact calculation successful")
        return 0
    else:
        logger.error("Impact calculation failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())