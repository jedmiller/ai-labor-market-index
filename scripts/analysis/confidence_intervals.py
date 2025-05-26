#!/usr/bin/env python3
# scripts/analysis/confidence_intervals.py
"""
AI Labor Market Impact Confidence Intervals - Monte Carlo simulation for uncertainty estimation
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
        logging.FileHandler("ai_impact_confidence.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("ai-impact-confidence")

class ConfidenceIntervalCalculator:
    """
    Calculates confidence intervals for AI Labor Market Impact projections using Monte Carlo simulation.
    """
    def __init__(self, 
                 input_dir="./data/processed", 
                 output_dir="./data/processed/projections",
                 current_impact_file=None,
                 num_simulations=1000):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.current_impact_file = current_impact_file
        self.num_simulations = num_simulations
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Define parameter variation ranges for Monte Carlo simulation
        self.PARAMETER_VARIATIONS = {
            "adoption": 0.10,         # ±10% adoption rate variation
            "efficiency": 0.05,       # ±5% efficiency variation
            "implementation": 0.15,   # ±15% implementation rate variation
            "displacement": 0.10,     # ±10% displacement factor variation
            "creation": 0.20,         # ±20% creation effect variation
            "demand": 0.15            # ±15% demand effect variation
        }
        
        # Define industry parameters (baseline values)
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

    def load_current_impact(self):
        """Load the current AI impact data."""
        # If no specific file is provided, use the latest
        if not self.current_impact_file:
            self.current_impact_file = "ai_labor_impact_latest.json"
        
        return self.load_data(self.current_impact_file)

    def generate_parameter_variation(self, base_value, variation_key):
        """
        Generate a varied parameter value based on the variation range for Monte Carlo simulation.
        """
        variation_pct = self.PARAMETER_VARIATIONS.get(variation_key, 0.1)
        lower_bound = base_value * (1 - variation_pct)
        upper_bound = base_value * (1 + variation_pct)
        return np.random.uniform(lower_bound, upper_bound)

    def simulate_impact_calculation(self, current_components, industry_data, simulation_id):
        """
        Simulate a single run of the impact calculation with varied parameters.
        """
        # Extract base component values
        base_displacement = abs(current_components.get("displacement_effect", 0.05))
        base_creation = current_components.get("creation_effect", 0.02)
        base_maturity = current_components.get("market_maturity", 0.4)
        base_demand = current_components.get("demand_effect", 0.01)
        
        # Generate varied parameters for this simulation
        displacement = self.generate_parameter_variation(base_displacement, "displacement")
        creation = self.generate_parameter_variation(base_creation, "creation")
        maturity = min(1.0, self.generate_parameter_variation(base_maturity, "adoption"))
        demand = self.generate_parameter_variation(base_demand, "demand")
        
        # Calculate net impact for each industry with varied parameters
        industry_impacts = {}
        total_employment = 0
        weighted_impact = 0
        
        for industry, data in industry_data.items():
            # Get current employment
            employment = data.get("current", 0)
            total_employment += employment if industry != "Total Nonfarm" else 0
            
            # Generate industry-specific parameter variations
            industry_displacement = displacement * self.generate_parameter_variation(1.0, "displacement")
            industry_creation = creation * self.generate_parameter_variation(1.0, "creation")
            industry_demand = demand * self.generate_parameter_variation(1.0, "demand")
            
            # Calculate net impact percentage using the formula
            net_impact_pct = 1 - industry_displacement + (industry_creation * maturity) + industry_demand
            
            # Calculate jobs affected
            impact = net_impact_pct - 1  # Convert to percentage change format
            jobs_affected = employment * impact
            
            # Store results
            industry_impacts[industry] = {
                "impact": impact,
                "jobs_affected": int(jobs_affected)
            }
            
            # Add to weighted impact (excluding Total Nonfarm)
            if industry != "Total Nonfarm":
                weighted_impact += impact * employment
        
        # Calculate overall impact percentage (weighted by employment)
        if total_employment > 0:
            overall_impact = weighted_impact / total_employment
        else:
            overall_impact = 0
        
        # Return the overall impact and industry impacts for this simulation
        return {
            "simulation_id": simulation_id,
            "overall_impact": overall_impact,
            "by_industry": industry_impacts,
            "parameters": {
                "displacement_effect": displacement,
                "creation_effect": creation,
                "market_maturity": maturity,
                "demand_effect": demand
            }
        }

    def run_monte_carlo_simulation(self, current_year=None, projection_years=5):
        """
        Run Monte Carlo simulation for the specified number of years.
        """
        # Load current impact data
        current_impact = self.load_current_impact()
        if not current_impact:
            logger.error("Cannot run simulation without current impact data")
            return None
        
        # Extract components and industry data
        components = current_impact.get("components", {})
        industry_data = current_impact.get("by_industry", {})
        
        # Extract current date information
        current_date = current_impact.get("date", datetime.now().strftime("%Y-%m"))
        if not current_year:
            try:
                current_year = int(current_date.split("-")[0])
            except (ValueError, IndexError):
                current_year = datetime.now().year
        
        # Run simulations for the current (base) calculation
        logger.info(f"Running {self.num_simulations} Monte Carlo simulations for baseline")
        baseline_results = []
        
        for sim_id in range(self.num_simulations):
            if sim_id % 100 == 0:
                logger.info(f"Running baseline simulation {sim_id}/{self.num_simulations}")
            
            result = self.simulate_impact_calculation(components, industry_data, sim_id)
            baseline_results.append(result)
        
        # Calculate projection years simulations
        projection_simulations = {}
        
        for year_idx in range(1, projection_years + 1):
            projected_year = current_year + year_idx
            year_key = str(projected_year)
            
            logger.info(f"Running {self.num_simulations} Monte Carlo simulations for year {year_key}")
            
            year_results = []
            
            # For projections, we need to modify the base parameters based on year
            # This is a simplified approach - in a real system, you would use the
            # projection model to evolve the parameters more accurately
            year_components = {
                "displacement_effect": components.get("displacement_effect", 0.05) * (1 + 0.1 * year_idx),
                "creation_effect": components.get("creation_effect", 0.02) * (1 + 0.15 * year_idx),
                "market_maturity": min(1.0, components.get("market_maturity", 0.4) + (0.1 * year_idx)),
                "demand_effect": components.get("demand_effect", 0.01) * (1 + 0.1 * year_idx)
            }
            
            # Simulate each year's projections
            for sim_id in range(self.num_simulations):
                if sim_id % 100 == 0:
                    logger.info(f"Running year {year_key} simulation {sim_id}/{self.num_simulations}")
                
                result = self.simulate_impact_calculation(year_components, industry_data, sim_id)
                year_results.append(result)
            
            projection_simulations[year_key] = year_results
        
        # Calculate confidence intervals and percentiles
        baseline_confidence = self.calculate_confidence_intervals(baseline_results)
        projection_confidence = {}
        
        for year_key, year_results in projection_simulations.items():
            projection_confidence[year_key] = self.calculate_confidence_intervals(year_results)
        
        # Add confidence by timeframe calculations
        confidence_by_timeframe = self.calculate_confidence_by_timeframe(projection_years)
        
        # Format and return final results
        simulation_results = {
            "generated_at": datetime.now().isoformat(),
            "base_date": current_date,
            "simulations": self.num_simulations,
            "baseline": baseline_confidence,
            "projections": projection_confidence,
            "confidence_by_timeframe": confidence_by_timeframe
        }
        
        return simulation_results

    def calculate_confidence_by_timeframe(self, projection_years):
        """Calculate confidence that decreases non-linearly based on actual uncertainty factors"""
        confidence_by_year = {}
        
        for year_idx in range(1, projection_years + 1):
            year = datetime.now().year + year_idx
            
            # Confidence decreases based on multiple factors
            base_confidence = 0.85
            
            # Time decay (non-linear)
            time_decay = np.exp(-0.15 * year_idx)  # Exponential decay
            
            # Market volatility (increases with AI advancement uncertainty)  
            volatility_factor = 1.0 - (0.05 * year_idx * year_idx / 25)  # Quadratic increase in uncertainty
            
            # Data availability factor (decreases as we project further)
            data_factor = 1.0 - (0.03 * year_idx)
            
            year_confidence = base_confidence * time_decay * volatility_factor * data_factor
            confidence_by_year[str(year)] = max(0.3, min(0.95, year_confidence))  # Bound between 30-95%
        
        return confidence_by_year

    def calculate_confidence_intervals(self, simulation_results):
        """
        Calculate confidence intervals and percentiles from simulation results.
        """
        # Extract overall impacts from all simulations
        overall_impacts = [sim["overall_impact"] for sim in simulation_results]
        
        # Calculate percentiles (p10, p25, p50, p75, p90)
        p10 = np.percentile(overall_impacts, 10)
        p25 = np.percentile(overall_impacts, 25)
        p50 = np.percentile(overall_impacts, 50)  # Median
        p75 = np.percentile(overall_impacts, 75)
        p90 = np.percentile(overall_impacts, 90)
        
        # Calculate mean and standard deviation
        mean_impact = np.mean(overall_impacts)
        std_dev = np.std(overall_impacts)
        
        # Extract industry impacts
        industries = simulation_results[0]["by_industry"].keys()
        industry_confidence = {}
        
        for industry in industries:
            industry_impacts = [sim["by_industry"][industry]["impact"] for sim in simulation_results]
            
            # Calculate industry-specific percentiles
            industry_confidence[industry] = {
                "p10": np.percentile(industry_impacts, 10),
                "p25": np.percentile(industry_impacts, 25),
                "p50": np.percentile(industry_impacts, 50),  # Median
                "p75": np.percentile(industry_impacts, 75),
                "p90": np.percentile(industry_impacts, 90),
                "mean": np.mean(industry_impacts),
                "std_dev": np.std(industry_impacts)
            }
        
        # Return the confidence intervals
        return {
            "overall": {
                "p10": p10,
                "p25": p25,
                "p50": p50,
                "p75": p75,
                "p90": p90,
                "mean": mean_impact,
                "std_dev": std_dev
            },
            "by_industry": industry_confidence
        }

    def save_simulation_results(self, results):
        """Save the simulation results to a file."""
        # Get date string for filename
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Define output filename
        output_file = os.path.join(
            self.output_dir,
            f"ai_impact_confidence_{date_str}.json"
        )
        
        # Save to file
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Saved confidence interval results to {output_file}")
        
        # Create a copy as the latest confidence file
        latest_file = os.path.join(self.output_dir, "ai_impact_confidence_latest.json")
        with open(latest_file, 'w') as f:
            json.dump(results, f, indent=2)
            
        logger.info(f"Updated latest confidence file at {latest_file}")
        
        return output_file

    def run_confidence_analysis(self, projection_years=5):
        """Main method to run the confidence interval analysis."""
        # Run Monte Carlo simulations
        simulation_results = self.run_monte_carlo_simulation(projection_years=projection_years)
        if not simulation_results:
            logger.error("Monte Carlo simulation failed")
            return None
        
        # Save results
        self.save_simulation_results(simulation_results)
        
        # Log summary
        baseline = simulation_results["baseline"]["overall"]
        logger.info(f"Confidence analysis complete.")
        logger.info(f"Baseline impact - Mean: {baseline['mean']:.4f}, Median (p50): {baseline['p50']:.4f}")
        logger.info(f"Confidence interval (p10-p90): [{baseline['p10']:.4f}, {baseline['p90']:.4f}]")
        
        # Return results
        return simulation_results


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Calculate confidence intervals for AI Labor Market Impact')
    parser.add_argument('--input-dir', default='./data/processed', help='Input directory containing processed data')
    parser.add_argument('--output-dir', default='./data/processed/projections', help='Output directory for confidence interval files')
    parser.add_argument('--impact-file', help='Specific impact file to use (default: latest)')
    parser.add_argument('--simulations', type=int, default=1000, help='Number of Monte Carlo simulations to run (default: 1000)')
    parser.add_argument('--years', type=int, default=5, help='Number of projection years (default: 5)')
    
    args = parser.parse_args()
    
    logger.info(f"Calculating confidence intervals using {args.simulations} Monte Carlo simulations")
    
    calculator = ConfidenceIntervalCalculator(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        current_impact_file=args.impact_file,
        num_simulations=args.simulations
    )
    
    # Run confidence analysis
    results = calculator.run_confidence_analysis(projection_years=args.years)
    
    if results:
        logger.info("Confidence interval calculation successful")
        return 0
    else:
        logger.error("Confidence interval calculation failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())