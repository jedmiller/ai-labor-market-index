#!/usr/bin/env python3
# scripts/analysis/project_impact.py
"""
AI Labor Market Impact Projection System - Forecasts future impacts using S-curve adoption models
"""
import json
import logging
import os
import sys
import argparse
from datetime import datetime
import numpy as np
import math
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ai_impact_projection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("ai-impact-projector")

class AIImpactProjector:
    """
    Projects future AI Labor Market Impact based on current impact data and adoption models.
    """
    def __init__(self, 
                 input_dir="./data/processed", 
                 output_dir="./data/processed/projections",
                 current_impact_file=None,
                 projection_years=5):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.current_impact_file = current_impact_file
        self.projection_years = projection_years
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Define scenario parameters
        self.SCENARIOS = {
            "conservative": {
                "adoption_growth": 0.15,
                "efficiency_improvement": 0.05,
                "implementation_acceleration": 0.10
            },
            "moderate": {
                "adoption_growth": 0.30,
                "efficiency_improvement": 0.10,
                "implementation_acceleration": 0.20
            },
            "aggressive": {
                "adoption_growth": 0.50,
                "efficiency_improvement": 0.15,
                "implementation_acceleration": 0.35
            }
        }

    def load_current_impact(self):
        """Load the current AI impact data."""
        # If no specific file is provided, use the latest
        if not self.current_impact_file:
            self.current_impact_file = "ai_labor_impact_latest.json"
        
        try:
            with open(os.path.join(self.input_dir, self.current_impact_file), 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Current impact file not found: {self.current_impact_file}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {self.current_impact_file}")
            return None

    def calculate_adoption_projection(self, current_adoption, years_ahead, sector, scenario="moderate"):
        """
        Calculate projected adoption using S-curve model.
        S-curve: adoption = L / (1 + e^(-k(t-t0)))
        L = adoption ceiling, k = speed, t0 = inflection point
        """
        # Get industry-specific parameters
        adoption_ceiling = self.get_sector_param(sector, "adoption_ceiling", default=0.8)
        adoption_speed = self.get_sector_param(sector, "adoption_speed", default=1.0)
        
        # Adjust parameters based on scenario
        scenario_params = self.SCENARIOS.get(scenario, self.SCENARIOS["moderate"])
        adoption_ceiling *= (1 + scenario_params["adoption_growth"])
        adoption_speed *= (1 + scenario_params["implementation_acceleration"])
        
        # Clip ceiling to maximum 0.95 (95% adoption)
        adoption_ceiling = min(0.95, adoption_ceiling)
        
        # Approximate current position on S-curve
        if current_adoption <= 0.01:
            current_adoption = 0.01  # Avoid negative logs
        
        # Solve for t0 (inflection point) using current adoption
        # If adoption = L / (1 + e^(-k(t-t0))), then t0 = t + (1/k)*ln((L/adoption) - 1)
        current_t = 0  # Current time is reference point
        t0 = current_t + (1/adoption_speed) * math.log((adoption_ceiling/current_adoption) - 1)
        
        # Calculate future adoption at each year
        future_adoptions = []
        for year in range(1, years_ahead + 1):
            future_t = current_t + year
            future_adoption = adoption_ceiling / (1 + math.exp(-adoption_speed * (future_t - t0)))
            future_adoptions.append(future_adoption)
        
        return future_adoptions

    def get_sector_param(self, sector, param_name, default=None):
        """
        Get sector-specific parameter values. 
        In a full implementation, this would load from a more comprehensive data store.
        """
        # Default parameter values for major sectors
        sector_params = {
            "Information": {
                "adoption_ceiling": 0.95,
                "adoption_speed": 1.5,
            },
            "Professional and Business Services": {
                "adoption_ceiling": 0.85,
                "adoption_speed": 1.2,
            },
            "Financial Activities": {
                "adoption_ceiling": 0.85,
                "adoption_speed": 1.3,
            },
            "Healthcare": {
                "adoption_ceiling": 0.75,
                "adoption_speed": 0.7,
            },
            "Education": {
                "adoption_ceiling": 0.70,
                "adoption_speed": 0.6,
            },
            "Manufacturing": {
                "adoption_ceiling": 0.80,
                "adoption_speed": 1.0,
            },
            "Retail": {
                "adoption_ceiling": 0.70,
                "adoption_speed": 1.2,
            },
            "Government": {
                "adoption_ceiling": 0.60,
                "adoption_speed": 0.5,
            }
        }
        
        # Try exact match
        if sector in sector_params and param_name in sector_params[sector]:
            return sector_params[sector][param_name]
        
        # Try normalized name matching
        sector_lower = sector.lower()
        for key, params in sector_params.items():
            if key.lower() in sector_lower or sector_lower in key.lower():
                if param_name in params:
                    return params[param_name]
        
        # Return default if no match
        return default

    def project_component_evolution(self, current_impact, scenario="moderate"):
        """
        Project how each component (displacement, creation, demand effects) will 
        evolve over the projection period.
        """
        # Extract current components
        components = current_impact.get("components", {})
        displacement_effect = abs(components.get("displacement_effect", 0.05))
        creation_effect = components.get("creation_effect", 0.02)
        market_maturity = components.get("market_maturity", 0.4)
        demand_effect = components.get("demand_effect", 0.01)
        
        # Get scenario parameters
        scenario_params = self.SCENARIOS.get(scenario, self.SCENARIOS["moderate"])
        
        # Project component evolution
        component_projections = {}
        
        # Calculate projected market maturity (increases over time)
        maturity_projections = []
        for year in range(1, self.projection_years + 1):
            # Market maturity increases and asymptotes to 1.0
            projected_maturity = min(1.0, market_maturity + (0.1 * year))
            maturity_projections.append(projected_maturity)
        
        # Project displacement effect (increases with adoption and implementation acceleration)
        displacement_projections = []
        for year in range(1, self.projection_years + 1):
            # Displacement increases with implementation acceleration but may slow due to diminishing returns
            growth_factor = 1 + (scenario_params["implementation_acceleration"] * 
                               (1 - 0.1 * year))  # Decreasing effect over time
            
            projected_displacement = displacement_effect * growth_factor
            displacement_projections.append(min(0.8, projected_displacement))  # Cap at 80%
        
        # Project creation effect (increases with adoption growth)
        creation_projections = []
        for year in range(1, self.projection_years + 1):
            # Creation effect increases with adoption growth
            growth_factor = 1 + (scenario_params["adoption_growth"] * year)
            projected_creation = creation_effect * growth_factor
            creation_projections.append(min(0.5, projected_creation))  # Cap at 50%
        
        # Project demand effect (increases with efficiency improvement)
        demand_projections = []
        for year in range(1, self.projection_years + 1):
            # Demand effect increases with efficiency improvement
            growth_factor = 1 + (scenario_params["efficiency_improvement"] * year)
            projected_demand = demand_effect * growth_factor
            demand_projections.append(min(0.3, projected_demand))  # Cap at 30%
        
        # Store all projections
        component_projections["market_maturity"] = maturity_projections
        component_projections["displacement_effect"] = displacement_projections
        component_projections["creation_effect"] = creation_projections
        component_projections["demand_effect"] = demand_projections
        
        return component_projections

    def calculate_projected_impact(self, current_impact, component_projections, scenario="moderate"):
        """
        Calculate the projected total impact based on component projections.
        """
        impact_projections = {}
        
        # Get current year to use as base for projection years
        current_date = current_impact.get("date", datetime.now().strftime("%Y-%m"))
        try:
            current_year = int(current_date.split("-")[0])
        except (ValueError, IndexError):
            current_year = datetime.now().year
        
        # Calculate impact for each projected year
        for year_idx in range(self.projection_years):
            projected_year = current_year + year_idx + 1
            
            # Get component values for this year
            market_maturity = component_projections["market_maturity"][year_idx]
            displacement = component_projections["displacement_effect"][year_idx]
            creation = component_projections["creation_effect"][year_idx]
            demand = component_projections["demand_effect"][year_idx]
            
            # Calculate impact using the formula:
            # Net Impact = 1 - Displacement + (Creation × Market_Maturity) + Demand
            net_impact = 1 - displacement + (creation * market_maturity) + demand
            
            # Convert to percentage change format (-0.05 = 5% job loss)
            impact_pct = net_impact - 1
            
            # Store projection
            impact_projections[str(projected_year)] = impact_pct
        
        return impact_projections

    def project_industry_impacts(self, current_impact, scenario="moderate"):
        """
        Project impacts by industry over the projection period.
        """
        industry_projections = {}
        
        # Get current year
        current_date = current_impact.get("date", datetime.now().strftime("%Y-%m"))
        try:
            current_year = int(current_date.split("-")[0])
        except (ValueError, IndexError):
            current_year = datetime.now().year
        
        # Get industry-specific current impacts
        industry_impacts = current_impact.get("by_industry", {})
        
        # Project each industry separately
        for industry, data in industry_impacts.items():
            current_impact_pct = data.get("impact", 0)
            
            # Project impact for each year
            yearly_projections = {}
            
            for year_idx in range(self.projection_years):
                projected_year = current_year + year_idx + 1
                
                # Calculate adoption projection for this industry
                # Convert current impact to an adoption proxy (transform from impact to adoption level)
                current_adoption = max(0.01, 0.3 + current_impact_pct)  # Base adoption level plus impact
                
                adoption_projections = self.calculate_adoption_projection(
                    current_adoption, 
                    year_idx + 1,  # Years ahead (1-indexed)
                    industry, 
                    scenario
                )
                
                # Get the projected adoption for this specific year
                projected_adoption = adoption_projections[-1]  # Last value is for this year
                
                # Calculate impact based on adoption level
                # In a more sophisticated model, this would use the full formula with all components
                base_impact = current_impact_pct
                impact_scaling = (projected_adoption / current_adoption) ** 0.7  # Diminishing returns
                
                # Apply scenario adjustments
                scenario_params = self.SCENARIOS.get(scenario, self.SCENARIOS["moderate"])
                scenario_adjustment = 1.0
                if base_impact < 0:  # Negative impact (job loss)
                    # Higher adoption means more displacement
                    scenario_adjustment = 1 + scenario_params["implementation_acceleration"]
                else:  # Positive impact (job creation)
                    # Higher adoption means more creation
                    scenario_adjustment = 1 + scenario_params["adoption_growth"]
                
                projected_impact = base_impact * impact_scaling * scenario_adjustment
                
                # Add uncertainty range (±20% for simplicity)
                impact_range = [projected_impact * 0.8, projected_impact * 1.2]
                
                # Store projection
                yearly_projections[str(projected_year)] = {
                    "central": projected_impact,
                    "range": impact_range
                }
            
            # Store industry projections
            industry_projections[industry] = yearly_projections
        
        return industry_projections

    def generate_projections(self, scenarios=None):
        """
        Generate projections for the specified scenarios.
        """
        # Load current impact data
        current_impact = self.load_current_impact()
        if not current_impact:
            logger.error("Cannot generate projections without current impact data")
            return None
        
        # Use specified scenarios or all scenarios
        if not scenarios:
            scenarios = list(self.SCENARIOS.keys())
        
        # Get current date information for filenames
        current_date = current_impact.get("date", datetime.now().strftime("%Y-%m"))
        date_parts = current_date.split("-")
        if len(date_parts) >= 2:
            year_str = date_parts[0]
            month_str = date_parts[1]
            date_str = f"{year_str}{month_str}"
        else:
            date_str = datetime.now().strftime("%Y%m")
        
        # Generate projections for each scenario
        all_projections = {
            "generated_at": datetime.now().isoformat(),
            "base_impact_date": current_date,
            "projection_years": self.projection_years,
            "scenarios": {}
        }
        
        for scenario in scenarios:
            logger.info(f"Generating projections for {scenario} scenario")
            
            # Project component evolution
            component_projections = self.project_component_evolution(current_impact, scenario)
            
            # Calculate projected impact based on components
            impact_projections = self.calculate_projected_impact(current_impact, component_projections, scenario)
            
            # Project industry-specific impacts
            industry_projections = self.project_industry_impacts(current_impact, scenario)
            
            # Store scenario results
            all_projections["scenarios"][scenario] = {
                "central_projections": impact_projections,
                "component_evolution": component_projections,
                "industry_projections": industry_projections
            }
        
        # Create a summarized projection object
        summary_projections = {
            "base_date": current_date,
            "projections": {},
            "by_industry": {}
        }
        
        # Extract years and calculate central/pessimistic/optimistic projections
        current_year = int(current_date.split("-")[0])
        for year_idx in range(self.projection_years):
            projected_year = current_year + year_idx + 1
            year_key = str(projected_year)
            
            # Get projections for each scenario
            conservative = all_projections["scenarios"]["conservative"]["central_projections"].get(year_key, 0)
            moderate = all_projections["scenarios"]["moderate"]["central_projections"].get(year_key, 0)
            aggressive = all_projections["scenarios"]["aggressive"]["central_projections"].get(year_key, 0)
            
            # Assign scenario values to pessimistic/central/optimistic
            # Conservative is usually more pessimistic (more job loss or less job creation)
            # Aggressive is usually more optimistic (less job loss or more job creation)
            if conservative < aggressive:
                pessimistic = conservative
                optimistic = aggressive
            else:
                pessimistic = aggressive
                optimistic = conservative
            
            summary_projections["projections"][year_key] = {
                "central": moderate,
                "pessimistic": pessimistic,
                "optimistic": optimistic
            }
        
        # Add industry projections
        industries = current_impact.get("by_industry", {}).keys()
        for industry in industries:
            summary_projections["by_industry"][industry] = {}
            
            for year_idx in range(self.projection_years):
                projected_year = current_year + year_idx + 1
                year_key = str(projected_year)
                
                # Get industry projections for the moderate scenario
                moderate_projection = all_projections["scenarios"]["moderate"]["industry_projections"].get(
                    industry, {}).get(year_key, {"central": 0, "range": [0, 0]})
                
                summary_projections["by_industry"][industry][year_key] = {
                    "central": moderate_projection["central"],
                    "range": moderate_projection["range"]
                }
        
        # Save the full and summarized projections
        full_output_file = os.path.join(
            self.output_dir,
            f"ai_impact_projections_full_{date_str}.json"
        )
        
        summary_output_file = os.path.join(
            self.output_dir,
            f"ai_impact_projections_{date_str}.json"
        )
        
        with open(full_output_file, 'w') as f:
            json.dump(all_projections, f, indent=2)
        
        with open(summary_output_file, 'w') as f:
            json.dump(summary_projections, f, indent=2)
        
        # Also save as latest projection files
        latest_full = os.path.join(self.output_dir, "ai_impact_projections_full_latest.json")
        latest_summary = os.path.join(self.output_dir, "ai_impact_projections_latest.json")
        
        with open(latest_full, 'w') as f:
            json.dump(all_projections, f, indent=2)
        
        with open(latest_summary, 'w') as f:
            json.dump(summary_projections, f, indent=2)
        
        logger.info(f"Saved full projections to {full_output_file}")
        logger.info(f"Saved summary projections to {summary_output_file}")
        
        # Return the summary projections
        return summary_projections


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Project future AI Labor Market Impact')
    parser.add_argument('--input-dir', default='./data/processed', help='Input directory containing processed data')
    parser.add_argument('--output-dir', default='./data/processed/projections', help='Output directory for projection files')
    parser.add_argument('--impact-file', help='Specific impact file to use (default: latest)')
    parser.add_argument('--years', type=int, default=5, help='Number of years to project (default: 5)')
    parser.add_argument('--scenarios', nargs='+', choices=['conservative', 'moderate', 'aggressive'], 
                        default=['conservative', 'moderate', 'aggressive'], 
                        help='Scenarios to generate projections for')
    
    args = parser.parse_args()
    
    logger.info(f"Generating AI Labor Market Impact projections for {args.years} years")
    logger.info(f"Using scenarios: {', '.join(args.scenarios)}")
    
    projector = AIImpactProjector(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        current_impact_file=args.impact_file,
        projection_years=args.years
    )
    
    # Generate projections
    projections = projector.generate_projections(args.scenarios)
    
    if projections:
        logger.info("Projection generation successful")
        return 0
    else:
        logger.error("Projection generation failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())