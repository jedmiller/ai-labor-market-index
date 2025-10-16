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
        
        # Additional data files for enhanced calculations
        self.ai_jobs_combined_file = f"ai_jobs_combined_{self.date_str}.json"
        self.news_events_file = f"news_{self.date_str[:6]}_{self.date_str}.json"  # News format: news_YYYY_MM_YYYYMMDD.json
        self.bls_productivity_file = f"{self.date_str}_bls_employment_batch_0.json"
        
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

    def load_data(self, filepath, search_subdirs=False):
        """Load data from a JSON file, optionally searching subdirectories."""
        file_paths = [os.path.join(self.input_dir, filepath)]
        
        if search_subdirs:
            # Add common subdirectory paths
            base_dir = os.path.dirname(self.input_dir)
            file_paths.extend([
                os.path.join(base_dir, "raw", "bls", filepath),
                os.path.join(base_dir, "raw", "jobs", filepath),
                os.path.join(base_dir, "raw", "news", filepath),
                os.path.join(base_dir, "raw", "anthropic_index", filepath)
            ])
        
        for full_path in file_paths:
            try:
                if os.path.exists(full_path):
                    with open(full_path, 'r') as f:
                        logger.info(f"Successfully loaded: {full_path}")
                        return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Error decoding JSON from {full_path}")
                continue
            except Exception as e:
                logger.warning(f"Error loading {full_path}: {e}")
                continue
        
        logger.warning(f"File not found in any location: {filepath}")
        return None

    def load_news_events_data(self):
        """Load news events data for momentum indicators."""
        news_data = self.load_data(self.news_events_file, search_subdirs=True)
        
        if news_data:
            # Extract momentum indicators from news
            momentum_score = 0
            positive_events = 0
            total_events = len(news_data.get("articles", []))
            
            for article in news_data.get("articles", []):
                # Simple sentiment scoring based on keywords
                title = article.get("title", "").lower()
                description = article.get("description", "").lower()
                content = f"{title} {description}"
                
                # Positive indicators
                if any(word in content for word in ["breakthrough", "advancement", "innovation", "growth", "opportunity"]):
                    momentum_score += 1
                    positive_events += 1
                # Negative indicators
                elif any(word in content for word in ["concern", "risk", "threat", "loss", "decline"]):
                    momentum_score -= 1
            
            return {
                "momentum_score": momentum_score,
                "positive_events": positive_events,
                "total_events": total_events,
                "momentum_factor": max(0.8, min(1.2, 1.0 + (momentum_score / max(1, total_events))))
            }
        
        return {"momentum_factor": 1.0, "momentum_score": 0, "total_events": 0}

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

    def calculate_capacity_utilization_factor(self, industry, employment_data=None):
        """
        Calculate dynamic capacity utilization factor based on industry characteristics and data.
        """
        # Industry-specific defaults based on growth characteristics
        industry_defaults = {
            "Information": 0.4,  # High-growth tech industries
            "Professional and Business Services": 0.5,
            "Financial Activities": 0.6,
            "Education and Health Services": 0.7,  # Stable service industries
            "Manufacturing": 0.8,  # Traditional industries
            "Trade, Transportation, and Utilities": 0.7,
            "Leisure and Hospitality": 0.8,  # Declining/stable industries
            "Construction": 0.8,
            "Mining and Logging": 0.9,
            "Other Services": 0.7,
            "Government": 0.8,
            "Total Nonfarm": 0.7
        }
        
        # Try to calculate from employment growth data if available
        if employment_data and industry in employment_data:
            try:
                data = employment_data[industry]
                current = data.get("current", 0)
                if current == 0:
                    current = data.get("current_employment", 0)
                if current == 0:
                    current = data.get("employment", 0)
                
                previous = data.get("previous", current)
                if previous == 0:
                    previous = data.get("year_ago_employment", current)
                
                if previous > 0:
                    growth_rate = (current - previous) / previous
                    
                    # Convert growth rate to capacity utilization
                    # Higher growth = lower capacity utilization (more room for AI)
                    if growth_rate > 0.05:  # >5% growth
                        utilization = 0.3
                    elif growth_rate > 0.02:  # 2-5% growth
                        utilization = 0.5
                    elif growth_rate > -0.02:  # Stable
                        utilization = 0.7
                    else:  # Declining
                        utilization = 0.9
                    
                    logger.info(f"Calculated capacity utilization for {industry}: {utilization:.2f} (growth: {growth_rate:.3f})")
                    return utilization
            except Exception as e:
                logger.warning(f"Error calculating capacity utilization for {industry}: {e}")
        
        # Use industry default
        default_utilization = industry_defaults.get(industry, self.DEFAULT_PARAMS["capacity_utilization"])
        logger.info(f"Using default capacity utilization for {industry}: {default_utilization}")
        return default_utilization

    def calculate_capacity_augmentation_impact(self, augmentation_pct, industry, employment_data=None):
        """
        Calculate the Capacity Augmentation Impact component.
        Capacity Augmentation Impact = Augmentation% × Efficiency Factor × Adoption Rate × Capacity Utilization Factor
        """
        efficiency_factor = self.DEFAULT_PARAMS["efficiency_factor"]
        adoption_rate = self.DEFAULT_PARAMS["adoption_rate"]
        capacity_utilization = self.calculate_capacity_utilization_factor(industry, employment_data)
        
        # Convert percentage to decimal
        augmentation_pct = augmentation_pct / 100 if augmentation_pct > 1 else augmentation_pct
        
        return augmentation_pct * efficiency_factor * adoption_rate * capacity_utilization

    def calculate_displacement_effect(self, industry_data, anthropic_data):
        """
        Calculate displacement effect using occupation-to-industry mapping when available,
        with fallback to simplified calculation for backward compatibility.
        """
        # Try occupation-based mapping first
        try:
            return self.calculate_displacement_effect_with_occupation_mapping(industry_data, anthropic_data)
        except Exception as e:
            logger.warning(f"Occupation mapping failed: {e}")
            logger.info("Falling back to simplified displacement calculation")
            return self.calculate_displacement_effect_fallback(industry_data, anthropic_data)

    def calculate_displacement_effect_with_occupation_mapping(self, industry_data, anthropic_data):
        """
        Calculate displacement effect using proper occupation-to-industry mapping.
        """
        try:
            from .occupation_industry_mapper import OccupationIndustryMapper
        except ImportError:
            # Fallback if the mapper isn't available
            logger.warning("OccupationIndustryMapper not available, using manual mapping")
            return self.calculate_displacement_with_manual_mapping(industry_data, anthropic_data)
        
        # Initialize the mapper
        mapper = OccupationIndustryMapper(input_dir=self.input_dir)
        
        # Try to load occupation mapping data
        mapping_success = mapper.load_data_sources(auto_discover=True)
        
        if not mapping_success:
            raise ValueError("Could not load occupation mapping data sources")
        
        # Calculate industry-specific rates using occupation mapping
        industry_rates = mapper.calculate_industry_automation_rates()
        
        if not industry_rates:
            raise ValueError("No industry rates calculated from occupation mapping")
        
        logger.info("Using occupation-based displacement calculation")
        
        # Calculate displacement effects using mapped rates
        displacement_effects = {}
        
        for industry, data in industry_data.items():
            # Get industry-specific rates from mapping
            rates = industry_rates.get(industry, {})
            auto_rate = rates.get('automation_rate', 0.30)  # Default if not found
            aug_rate = rates.get('augmentation_rate', 0.70)
            confidence = rates.get('confidence', 0.5)
            data_coverage = rates.get('data_coverage', 0.0)
            
            # Convert rates to percentages for existing calculation methods
            auto_pct = auto_rate * 100
            aug_pct = aug_rate * 100
            
            # Calculate displacement components using existing methods
            pure_automation = self.calculate_pure_automation_impact(auto_pct, industry)
            capacity_augmentation = self.calculate_capacity_augmentation_impact(aug_pct, industry, industry_data)
            
            # Total displacement effect
            displacement_effect = pure_automation + capacity_augmentation
            displacement_effect = np.clip(displacement_effect, 0, 0.8)
            
            displacement_effects[industry] = {
                "effect": displacement_effect,
                "confidence": confidence,
                "data_coverage": data_coverage,
                "calculation_method": "occupation_mapped",
                "components": {
                    "pure_automation": pure_automation,
                    "capacity_augmentation": capacity_augmentation,
                    "automation_rate": auto_rate,
                    "augmentation_rate": aug_rate
                }
            }
        
        # Calculate weighted average displacement
        def get_employment(data):
            employment = data.get("current", 0)
            if employment == 0:
                employment = data.get("current_employment", 0)
            if employment == 0:
                employment = data.get("employment", 0)
            return employment
        
        total_employment = sum(get_employment(data) for data in industry_data.values())
        if total_employment > 0:
            weighted_displacement = sum(
                displacement_effects[industry]["effect"] * get_employment(industry_data[industry])
                for industry in displacement_effects
                if industry in industry_data
            ) / total_employment
        else:
            weighted_displacement = np.mean([data["effect"] for data in displacement_effects.values()])
        
        # Add methodology metadata
        for industry_effect in displacement_effects.values():
            industry_effect["methodology"] = "occupation_weighted_aggregation"
        
        return weighted_displacement, displacement_effects
    
    def calculate_displacement_with_manual_mapping(self, industry_data, anthropic_data):
        """
        Calculate displacement effect using manual occupation-to-industry mapping.
        This is a simplified version when the full OccupationIndustryMapper isn't available.
        """
        logger.info("Using manual occupation-to-industry mapping")
        
        # Load occupation mapping manually
        mapping_file = os.path.join(os.path.dirname(self.input_dir), "mappings", "occupation_to_industry.json")
        if not os.path.exists(mapping_file):
            logger.warning(f"Mapping file not found: {mapping_file}")
            raise ValueError("No occupation mapping available")
        
        with open(mapping_file, 'r') as f:
            occupation_mapping = json.load(f)
        
        # Extract occupation automation data from Anthropic data
        if not anthropic_data or "datasets" not in anthropic_data:
            raise ValueError("No Anthropic occupation data available")
        
        occupation_automation = anthropic_data["datasets"].get("occupation_automation", {})
        if not occupation_automation:
            raise ValueError("No occupation automation data in Anthropic dataset")
        
        # Calculate industry-level rates by aggregating occupation data
        industry_rates = {}
        
        for industry in industry_data.keys():
            # Find occupations mapped to this industry
            industry_occupations = []
            for occupation, mapping_data in occupation_mapping.items():
                if mapping_data.get("primary_industry") == industry:
                    industry_occupations.append(occupation)
                elif industry in mapping_data.get("secondary_industries", []):
                    industry_occupations.append(occupation)
            
            if industry_occupations:
                # Calculate weighted average automation/augmentation rates
                automation_rates = []
                augmentation_rates = []
                
                for occupation in industry_occupations:
                    if occupation in occupation_automation:
                        auto_rate = occupation_automation[occupation].get("automation_rate", 0) / 100
                        aug_rate = occupation_automation[occupation].get("augmentation_rate", 0) / 100
                        automation_rates.append(auto_rate)
                        augmentation_rates.append(aug_rate)
                
                if automation_rates and augmentation_rates:
                    avg_automation = np.mean(automation_rates)
                    avg_augmentation = np.mean(augmentation_rates)
                    coverage = len(automation_rates) / max(1, len(industry_occupations))
                    
                    industry_rates[industry] = {
                        'automation_rate': avg_automation,
                        'augmentation_rate': avg_augmentation,
                        'confidence': min(1.0, coverage * 0.8 + 0.2),  # Scale confidence
                        'data_coverage': coverage,
                        'occupations_mapped': len(automation_rates)
                    }
                    
                    logger.info(f"{industry}: {len(automation_rates)} occupations, auto={avg_automation:.3f}, aug={avg_augmentation:.3f}")
                else:
                    logger.warning(f"No automation data for {industry} occupations")
            else:
                logger.warning(f"No occupations mapped to {industry}")
        
        if not industry_rates:
            raise ValueError("No industry rates calculated from occupation mapping")
        
        # Calculate displacement effects using mapped rates (similar to occupation_mapping version)
        displacement_effects = {}
        
        for industry, data in industry_data.items():
            if industry in industry_rates:
                rates = industry_rates[industry]
                auto_rate = rates['automation_rate']
                aug_rate = rates['augmentation_rate']
                confidence = rates['confidence']
                data_coverage = rates['data_coverage']
                
                # Convert to percentages for existing calculation methods
                auto_pct = auto_rate * 100
                aug_pct = aug_rate * 100
                
                # Calculate displacement components
                pure_automation = self.calculate_pure_automation_impact(auto_pct, industry)
                capacity_augmentation = self.calculate_capacity_augmentation_impact(aug_pct, industry, industry_data)
                
                displacement_effect = pure_automation + capacity_augmentation
                displacement_effect = np.clip(displacement_effect, 0, 0.8)
                
                displacement_effects[industry] = {
                    "effect": displacement_effect,
                    "confidence": confidence,
                    "data_coverage": data_coverage,
                    "calculation_method": "manual_occupation_mapped",
                    "components": {
                        "pure_automation": pure_automation,
                        "capacity_augmentation": capacity_augmentation,
                        "automation_rate": auto_rate,
                        "augmentation_rate": aug_rate
                    }
                }
            else:
                # Use fallback for unmapped industries
                displacement_effects[industry] = {
                    "effect": 0.1,  # Conservative default
                    "confidence": 0.3,
                    "data_coverage": 0.0,
                    "calculation_method": "default_fallback",
                    "components": {
                        "pure_automation": 0.05,
                        "capacity_augmentation": 0.05,
                        "automation_rate": 0.3,
                        "augmentation_rate": 0.7
                    }
                }
        
        # Calculate weighted average displacement  
        def get_employment(data):
            employment = data.get("current", 0)
            if employment == 0:
                employment = data.get("current_employment", 0)
            if employment == 0:
                employment = data.get("employment", 0)
            return employment
        
        total_employment = sum(get_employment(industry_data[industry]) for industry in displacement_effects.keys() if industry in industry_data and industry != "Total Nonfarm")
        
        if total_employment > 0:
            weighted_displacement = sum(
                displacement_effects[industry]["effect"] * get_employment(industry_data[industry])
                for industry in displacement_effects.keys()
                if industry in industry_data and industry != "Total Nonfarm"
            ) / total_employment
            logger.info(f"Manual mapping weighted displacement: {weighted_displacement:.4f}")
        else:
            weighted_displacement = np.mean([data["effect"] for data in displacement_effects.values()])
            logger.warning("No employment data for manual mapping, using simple average")
        
        return weighted_displacement, displacement_effects

    def calculate_displacement_effect_fallback(self, industry_data, anthropic_data):
        """
        Fallback displacement calculation using simplified approach (original methodology).
        """
        logger.info("Using simplified displacement calculation (fallback)")
        
        # Extract automation and augmentation percentages from Anthropic data
        industry_automation = {}
        industry_augmentation = {}
        
        # Get data from Anthropic Economic Index if available
        avg_automation = 0
        avg_augmentation = 0

        if anthropic_data:
            logger.info(f"Anthropic data structure: {list(anthropic_data.keys())}")

            # Check for new format with industry_impacts (August 2025+)
            if "industry_impacts" in anthropic_data:
                logger.info("Found industry-specific impacts in new format")
                for industry, impact_data in anthropic_data["industry_impacts"].items():
                    industry_automation[industry] = impact_data.get("automation_rate", 50.0)
                    industry_augmentation[industry] = impact_data.get("augmentation_rate", 50.0)
                    logger.info(f"  {industry}: auto={industry_automation[industry]:.1f}%, aug={industry_augmentation[industry]:.1f}%")
                # We have industry-specific data, so no need for averages
                avg_automation = -1  # Flag to indicate we have industry-specific data
                avg_augmentation = -1
            # Try to extract automation/augmentation data from various possible structures
            elif "statistics" in anthropic_data:
                avg_automation = anthropic_data["statistics"].get("average_automation_rate", 0)
                avg_augmentation = anthropic_data["statistics"].get("average_augmentation_rate", 0)
                logger.info(f"Found statistics: automation={avg_automation}, augmentation={avg_augmentation}")
            elif "datasets" in anthropic_data and "occupation_automation" in anthropic_data["datasets"]:
                # Calculate averages from occupation data
                occupation_data = anthropic_data["datasets"]["occupation_automation"]
                automation_rates = [data.get("automation_rate", 0) for data in occupation_data.values()]
                augmentation_rates = [data.get("augmentation_rate", 0) for data in occupation_data.values()]

                if automation_rates and augmentation_rates:
                    avg_automation = sum(automation_rates) / len(automation_rates)
                    avg_augmentation = sum(augmentation_rates) / len(augmentation_rates)
                    logger.info(f"Calculated from occupation data: automation={avg_automation:.1f}%, augmentation={avg_augmentation:.1f}%")
                else:
                    avg_automation = 0
                    avg_augmentation = 0
            else:
                avg_automation = 0
                avg_augmentation = 0
                logger.warning("No valid automation/augmentation data found in Anthropic data")

            # Use as default values for all industries if we found data (but not if we have industry-specific)
            if avg_automation > 0 or avg_augmentation > 0:
                for industry in industry_data:
                    industry_automation[industry] = avg_automation
                    industry_augmentation[industry] = avg_augmentation
                logger.info(f"Applied Anthropic data to {len(industry_data)} industries")
            else:
                logger.warning("Anthropic data available but no valid rates found")
        
        # Calculate displacement effect for each industry
        displacement_effects = {}
        for industry, data in industry_data.items():
            auto_pct = industry_automation.get(industry, 30)  # Default to 30% if no data
            aug_pct = industry_augmentation.get(industry, 70)  # Default to 70% if no data
            
            # Calculate components
            pure_automation = self.calculate_pure_automation_impact(auto_pct, industry)
            capacity_augmentation = self.calculate_capacity_augmentation_impact(aug_pct, industry, industry_data)
            
            # Both automation and augmentation contribute to displacement
            displacement_effect = pure_automation + capacity_augmentation
            
            # Ensure displacement effect is within reasonable bounds
            displacement_effect = np.clip(displacement_effect, 0, 0.8)
            
            displacement_effects[industry] = {
                "effect": displacement_effect,
                "confidence": 0.5,  # Medium confidence for simplified approach
                "data_coverage": 0.0,  # No detailed occupation data used
                "calculation_method": "simplified_uniform",
                "components": {
                    "pure_automation": pure_automation,
                    "capacity_augmentation": capacity_augmentation,
                    "automation_rate": auto_pct / 100,
                    "augmentation_rate": aug_pct / 100
                }
            }
        
        # Calculate employment-weighted average displacement effect instead of simple average
        if displacement_effects:
            def get_employment(data):
                employment = data.get("current", 0)
                if employment == 0:
                    employment = data.get("current_employment", 0)
                if employment == 0:
                    employment = data.get("employment", 0)
                return employment
            
            total_employment = sum(get_employment(industry_data[industry]) for industry in displacement_effects.keys() if industry in industry_data and industry != "Total Nonfarm")
            
            if total_employment > 0:
                weighted_displacement = sum(
                    displacement_effects[industry]["effect"] * get_employment(industry_data[industry])
                    for industry in displacement_effects.keys()
                    if industry in industry_data and industry != "Total Nonfarm"
                ) / total_employment
                logger.info(f"Used employment-weighted displacement average: {weighted_displacement:.4f} (total employment: {total_employment:,})")
            else:
                weighted_displacement = np.mean([data["effect"] for data in displacement_effects.values()])
                logger.warning("No employment data available, using simple average for displacement")
            
            avg_displacement = weighted_displacement
        else:
            avg_displacement = 0
            
        return avg_displacement, displacement_effects

    def load_ai_jobs_data(self):
        """
        Load AI jobs data from the collection files and process into usable format.
        """
        ai_jobs_data = {
            "total_ai_postings": 0,
            "industry_distribution": {},
            "job_titles": [],
            "growth_rate": 0
        }
        
        # Try to load processed job trends data first
        job_trends_file = os.path.join(self.input_dir, self.job_trends_file)
        if os.path.exists(job_trends_file):
            try:
                with open(job_trends_file, 'r') as f:
                    trends_data = json.load(f)
                    
                if "ai_related_postings" in trends_data:
                    # Extract current month AI postings count
                    for posting in trends_data["ai_related_postings"]:
                        if posting["date"] == "current_month":
                            ai_jobs_data["total_ai_postings"] = posting["count"]
                            break
                    
                    ai_jobs_data["growth_rate"] = trends_data.get("growth_rate", 0)
                    ai_jobs_data["job_titles"] = trends_data.get("top_job_titles", [])
                    
                    logger.info(f"Loaded AI jobs data: {ai_jobs_data['total_ai_postings']} postings")
                    return ai_jobs_data
            except Exception as e:
                logger.warning(f"Error loading job trends data: {e}")
        
        # Fallback: try to load raw AI jobs data and combine
        try:
            raw_dir = self.input_dir.replace("/processed", "/raw/jobs")
            ai_job_files = [
                f"ai_jobs_{self.date_str[:6]}_software-dev.json",
                f"ai_jobs_{self.date_str[:6]}_data.json", 
                f"ai_jobs_{self.date_str[:6]}_product.json",
                f"ai_jobs_{self.date_str[:6]}_all-others.json"
            ]
            
            total_count = 0
            for filename in ai_job_files:
                filepath = os.path.join(raw_dir, filename)
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                        total_count += data.get("count", 0)
            
            if total_count > 0:
                ai_jobs_data["total_ai_postings"] = total_count
                logger.info(f"Loaded raw AI jobs data: {total_count} postings")
                return ai_jobs_data
                
        except Exception as e:
            logger.warning(f"Error loading raw AI jobs data: {e}")
        
        logger.warning("No AI jobs data found, using defaults")
        return ai_jobs_data

    def calculate_creation_effect(self, industry_data, job_data):
        """
        Calculate the Creation Effect component using actual AI jobs data.
        Creation Effect = (Direct AI Jobs/Total Employment) + (AI Infrastructure Jobs/Total Employment)
        """
        # Load actual AI jobs data
        ai_jobs_data = self.load_ai_jobs_data()
        
        # Get industry employment figures
        industry_employment = {}
        total_employment = 0
        
        def get_employment(data):
            employment = data.get("current", 0)
            if employment == 0:
                employment = data.get("current_employment", 0)
            if employment == 0:
                employment = data.get("employment", 0)
            return employment
        
        for industry, data in industry_data.items():
            employment = get_employment(data)
            industry_employment[industry] = employment
            if industry != "Total Nonfarm":  # Avoid double counting
                total_employment += employment
        
        # Calculate base AI job ratio from actual data
        total_ai_postings = ai_jobs_data["total_ai_postings"]
        if total_employment > 0 and total_ai_postings > 0:
            # Estimate actual AI jobs as a fraction of total employment
            # Assume job postings represent ~1/12 of annual hires, and AI jobs fill at 2x rate
            base_ai_ratio = (total_ai_postings * 12 * 2) / total_employment
            logger.info(f"Calculated base AI job ratio: {base_ai_ratio:.4f} from {total_ai_postings} postings")
        else:
            # Fallback to defaults
            base_ai_ratio = 0.03  # 3% default ratio
            logger.warning(f"Using default AI job ratio: {base_ai_ratio:.4f}")
        
        # Calculate creation effect for each industry
        creation_effects = {}
        for industry, employment in industry_employment.items():
            # Apply industry-specific multipliers to the baseline ratio
            weight = self.get_industry_param(industry, "weight")
            industry_ratio = base_ai_ratio * weight
            
            # Split into direct AI jobs and infrastructure jobs
            direct_ratio = industry_ratio * 0.7  # 70% are direct AI roles
            infra_ratio = industry_ratio * 0.3   # 30% are AI infrastructure roles
            
            # Total creation effect
            creation_effect = direct_ratio + infra_ratio
            
            # Ensure within reasonable bounds
            creation_effect = np.clip(creation_effect, 0, 0.5)
            
            creation_effects[industry] = {
                "effect": creation_effect,
                "components": {
                    "direct_ai_jobs_ratio": direct_ratio,
                    "infrastructure_jobs_ratio": infra_ratio,
                    "total_ai_postings_used": total_ai_postings if industry != "Total Nonfarm" else 0
                }
            }
        
        # Calculate employment-weighted average creation effect
        if creation_effects:
            # Weight by employment size (excluding Total Nonfarm)
            weighted_sum = sum([data["effect"] * industry_employment[industry] 
                              for industry, data in creation_effects.items() 
                              if industry != "Total Nonfarm" and industry in industry_employment])
            total_emp_weight = sum([industry_employment[industry] 
                                  for industry in creation_effects.keys() 
                                  if industry != "Total Nonfarm" and industry in industry_employment])
            
            if total_emp_weight > 0:
                avg_creation = weighted_sum / total_emp_weight
                logger.info(f"Used employment-weighted creation average: {avg_creation:.4f} (total employment: {total_emp_weight:,})")
            else:
                avg_creation = np.mean([data["effect"] for data in creation_effects.values()])
                logger.warning("No employment data available for creation effect, using simple average")
        else:
            avg_creation = 0
            logger.warning("No creation effects calculated")
            
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

    def load_productivity_data(self):
        """
        Load BLS productivity data if available.
        """
        productivity_data = {}
        
        # Try to load from BLS data files
        try:
            bls_dir = self.input_dir.replace("/processed", "/raw/bls")
            
            # Look for BLS productivity files
            for filename in os.listdir(bls_dir):
                if "employment" in filename and filename.endswith(".json"):
                    filepath = os.path.join(bls_dir, filename)
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                        
                    # Extract productivity indicators from employment data
                    if "industries" in data:
                        for industry, industry_data in data["industries"].items():
                            current = industry_data.get("current", 0)
                            previous = industry_data.get("previous", current)
                            
                            if previous > 0:
                                employment_growth = (current - previous) / previous
                                productivity_data[industry] = {
                                    "employment_growth": employment_growth,
                                    "productivity_estimated": max(0.01, 0.15 - employment_growth)  # Inverse relationship
                                }
                    break
                    
        except Exception as e:
            logger.warning(f"Could not load productivity data: {e}")
        
        return productivity_data

    def get_industry_elasticity(self, industry):
        """
        Get industry-specific employment-productivity elasticity.
        """
        elasticities = {
            "Manufacturing": -0.3,  # Productivity gains reduce employment
            "Information": 0.1,     # Slight positive elasticity
            "Professional and Business Services": 0.2,  # Services benefit from productivity
            "Financial Activities": 0.1,
            "Education and Health Services": 0.2,  # Service sectors
            "Trade, Transportation, and Utilities": -0.1,
            "Leisure and Hospitality": 0.15,
            "Construction": -0.2,
            "Mining and Logging": -0.4,  # Most negative impact
            "Other Services": 0.1,
            "Government": 0.05,  # Conservative elasticity
            "Total Nonfarm": 0.1
        }
        
        return elasticities.get(industry, self.DEFAULT_PARAMS["elasticity_factor"])

    def calculate_demand_effect(self, industry_data):
        """
        Calculate the Demand Effect component using real productivity data.
        Demand Effect = Productivity Gain × Labor Share × Elasticity Factor
        """
        # Load actual productivity data
        productivity_data = self.load_productivity_data()
        
        # Calculate demand effect for each industry
        demand_effects = {}
        for industry, data in industry_data.items():
            # Get industry-specific parameters
            weight = self.get_industry_param(industry, "weight")
            elasticity = self.get_industry_elasticity(industry)
            
            # Use actual productivity data if available
            if industry in productivity_data:
                productivity_gain = productivity_data[industry]["productivity_estimated"]
                data_source = "BLS_estimated"
                logger.info(f"Using estimated productivity for {industry}: {productivity_gain:.3f}")
            else:
                productivity_gain = self.DEFAULT_PARAMS["productivity_gain"] * weight
                data_source = "default"
            
            # Calculate labor share (use industry-specific if available)
            labor_share = self.DEFAULT_PARAMS["labor_share"]
            
            # Calculate demand effect
            demand_effect = productivity_gain * labor_share * elasticity
            
            # Ensure within reasonable bounds
            demand_effect = np.clip(demand_effect, -0.2, 0.3)
            
            demand_effects[industry] = {
                "effect": demand_effect,
                "components": {
                    "productivity_gain": productivity_gain,
                    "labor_share": labor_share,
                    "elasticity_factor": elasticity,
                    "data_source": data_source
                }
            }
        
        # Calculate employment-weighted average demand effect 
        if demand_effects:
            def get_employment(data):
                employment = data.get("current", 0)
                if employment == 0:
                    employment = data.get("current_employment", 0)
                if employment == 0:
                    employment = data.get("employment", 0)
                return employment
            
            total_employment = sum([get_employment(data) for industry, data in industry_data.items() if industry != "Total Nonfarm"])
            if total_employment > 0:
                weighted_sum = sum([demand_effects[industry]["effect"] * get_employment(industry_data[industry]) 
                                  for industry in demand_effects.keys() 
                                  if industry != "Total Nonfarm" and industry in industry_data])
                avg_demand = weighted_sum / total_employment
                logger.info(f"Used employment-weighted demand average: {avg_demand:.4f} (total employment: {total_employment:,})")
            else:
                avg_demand = np.mean([data["effect"] for data in demand_effects.values()])
                logger.warning("No employment data available for demand effect, using simple average")
        else:
            avg_demand = 0
            logger.warning("No demand effects calculated")
            
        return avg_demand, demand_effects

    def calculate_net_impact(self, employment_data, job_data, events_data=None, research_data=None):
        """
        Calculate the Net Employment Impact using the formula:
        Net Employment Impact = Employment × [1 - Displacement Effect + Creation Effect × Market_Maturity + Demand Effect]
        """
        industries = employment_data.get("industries", {})
        
        # Load additional data sources
        news_events = self.load_news_events_data()
        momentum_factor = news_events["momentum_factor"]
        logger.info(f"News momentum factor: {momentum_factor:.3f} (from {news_events['total_events']} events)")
        
        # Calculate market maturity (a global factor)
        market_maturity = self.calculate_market_maturity()
        # Adjust market maturity by momentum factor
        adjusted_market_maturity = market_maturity * momentum_factor
        logger.info(f"Market Maturity: {market_maturity:.2f}, Adjusted: {adjusted_market_maturity:.2f}")
        
        # Calculate components
        displacement_avg, displacement_by_industry = self.calculate_displacement_effect(industries, job_data)
        creation_avg, creation_by_industry = self.calculate_creation_effect(industries, job_data)
        demand_avg, demand_by_industry = self.calculate_demand_effect(industries)
        
        # Log detailed component analysis
        logger.info(f"=== COMPONENT ANALYSIS ===")
        logger.info(f"Displacement Effect (avg): {displacement_avg:.4f}")
        logger.info(f"Creation Effect (avg): {creation_avg:.4f}")
        logger.info(f"Demand Effect (avg): {demand_avg:.4f}")
        logger.info(f"Market Maturity: {market_maturity:.4f} -> Adjusted: {adjusted_market_maturity:.4f}")
        
        # Log top 3 industries by impact for each component
        logger.info(f"\n=== TOP DISPLACEMENT EFFECTS ===")
        top_displacement = sorted(displacement_by_industry.items(), key=lambda x: x[1]['effect'], reverse=True)[:3]
        for industry, data in top_displacement:
            logger.info(f"{industry}: {data['effect']:.4f} (automation: {data['components']['pure_automation']:.4f}, augmentation: {data['components']['capacity_augmentation']:.4f})")
        
        logger.info(f"\n=== TOP CREATION EFFECTS ===")
        top_creation = sorted(creation_by_industry.items(), key=lambda x: x[1]['effect'], reverse=True)[:3]
        for industry, data in top_creation:
            logger.info(f"{industry}: {data['effect']:.4f} (direct: {data['components']['direct_ai_jobs_ratio']:.4f}, infra: {data['components']['infrastructure_jobs_ratio']:.4f})")
        
        logger.info(f"\n=== TOP DEMAND EFFECTS ===")
        top_demand = sorted(demand_by_industry.items(), key=lambda x: x[1]['effect'], reverse=True)[:3]
        for industry, data in top_demand:
            source = data['components'].get('data_source', 'unknown')
            logger.info(f"{industry}: {data['effect']:.4f} (productivity: {data['components']['productivity_gain']:.4f}, source: {source})")
        
        # Calculate net impact for each industry
        net_impact_by_industry = {}
        total_jobs_affected = 0
        total_jobs_displaced = 0
        total_jobs_created = 0
        total_jobs_demand_effect = 0
        
        for industry, data in industries.items():
            # Fix employment data access - check multiple possible field names
            current_employment = data.get("current", 0)
            if current_employment == 0:
                current_employment = data.get("current_employment", 0)
            if current_employment == 0:
                current_employment = data.get("employment", 0)
            
            # Convert employment from thousands to actual worker count
            # BLS data is typically reported in thousands
            current_employment_actual = current_employment * 1000
            
            # Get effects for this industry
            displacement = displacement_by_industry.get(industry, {"effect": displacement_avg})["effect"]
            creation = creation_by_industry.get(industry, {"effect": creation_avg})["effect"]
            demand = demand_by_industry.get(industry, {"effect": demand_avg})["effect"]
            
            # Calculate net impact percentage
            net_impact_pct = 1 - displacement + (creation * adjusted_market_maturity) + demand
            
            # Calculate jobs affected using actual worker count
            jobs_affected = current_employment_actual * (net_impact_pct - 1)
            
            # Calculate individual component job impacts for transparency
            jobs_displaced = current_employment_actual * displacement
            jobs_created = current_employment_actual * creation * adjusted_market_maturity
            jobs_demand_effect = current_employment_actual * demand
            
            # Store results
            net_impact_by_industry[industry] = {
                "impact": net_impact_pct - 1,  # Convert to percentage change (-0.06 = 6% job loss)
                "jobs_affected": int(jobs_affected),
                "jobs_displaced": int(jobs_displaced),
                "jobs_created": int(jobs_created),
                "jobs_demand_effect": int(jobs_demand_effect),
                "components": {
                    "displacement_effect": -displacement,
                    "creation_effect": creation * adjusted_market_maturity,
                    "demand_effect": demand
                }
            }
            
            # Add to total (excluding "Total Nonfarm" to avoid double counting)
            if industry != "Total Nonfarm":
                total_jobs_affected += int(jobs_affected)
                total_jobs_displaced += int(jobs_displaced)
                total_jobs_created += int(jobs_created)
                total_jobs_demand_effect += int(jobs_demand_effect)
        
        # Calculate overall impact percentage (weighted by employment)
        def get_employment(data):
            employment = data.get("current", 0)
            if employment == 0:
                employment = data.get("current_employment", 0)
            if employment == 0:
                employment = data.get("employment", 0)
            return employment
        
        # Convert to actual worker counts (from thousands)
        total_employment_thousands = sum([get_employment(data) for industry, data in industries.items() if industry != "Total Nonfarm"])
        total_employment = total_employment_thousands * 1000  # Convert to actual workers
        weighted_impact = sum([data["impact"] * get_employment(industries[industry]) for industry, data in net_impact_by_industry.items() if industry != "Total Nonfarm"])
        
        # Validate that total employment matches BLS total nonfarm if available
        total_nonfarm_data = industries.get("Total Nonfarm", {})
        total_nonfarm_employment_thousands = get_employment(total_nonfarm_data)
        total_nonfarm_employment = total_nonfarm_employment_thousands * 1000  # Convert to actual workers
        
        if total_nonfarm_employment > 0:
            employment_ratio = total_employment / total_nonfarm_employment
            if abs(employment_ratio - 1.0) > 0.1:  # More than 10% difference
                logger.warning(f"Employment sum mismatch: industries total={total_employment:,}, BLS total nonfarm={total_nonfarm_employment:,}, ratio={employment_ratio:.3f}")
        
        if total_employment_thousands > 0:
            overall_impact = weighted_impact / total_employment_thousands
        else:
            overall_impact = 0
        
        # Calculate transformation rate (total labor market churn)
        transformation_effects = {}
        total_transformation = 0
        
        for industry, data in industries.items():
            current_employment = get_employment(data)
            
            # Get effects for this industry
            displacement = displacement_by_industry.get(industry, {"effect": displacement_avg})["effect"]
            creation = creation_by_industry.get(industry, {"effect": creation_avg})["effect"] 
            demand = demand_by_industry.get(industry, {"effect": demand_avg})["effect"]
            
            # Transformation rate = total change (displacement + creation + demand)
            # This represents jobs experiencing significant change
            industry_transformation = abs(displacement) + abs(creation) + abs(demand)
            transformation_effects[industry] = industry_transformation
            
            # Weight by employment (excluding Total Nonfarm)
            if industry != "Total Nonfarm":
                total_transformation += industry_transformation * current_employment

        # Calculate overall transformation rate
        total_employment_calc = sum([get_employment(data) for industry, data in industries.items() if industry != "Total Nonfarm"])
        overall_transformation_rate = total_transformation / total_employment_calc if total_employment_calc > 0 else 0

        # Prepare result object
        result = {
            "date": f"{self.year}-{self.month:02d}" if self.year and self.month else datetime.now().strftime("%Y-%m"),
            "total_impact": overall_impact,
            "jobs_affected": total_jobs_affected,
            "jobs_displaced": total_jobs_displaced,
            "jobs_created": total_jobs_created,
            "jobs_demand_effect": total_jobs_demand_effect,
            "total_employment": total_employment,  # Now in actual worker count
            "total_employment_thousands": total_employment_thousands,  # Also provide thousands for reference
            "transformation_rate": overall_transformation_rate,
            "transformation_by_industry": transformation_effects,
            "by_industry": net_impact_by_industry,
            "components": {
                "displacement_effect": -displacement_avg,
                "creation_effect": creation_avg,
                "market_maturity": market_maturity,
                "adjusted_market_maturity": adjusted_market_maturity,
                "demand_effect": demand_avg,
                "momentum_factor": momentum_factor,
                "news_events_count": news_events['total_events']
            },
            "data_quality": {
                "calculation_method": self._determine_calculation_method(displacement_by_industry),
                "data_completeness": self.assess_data_completeness(employment_data, job_data),
                "last_updated": datetime.now().isoformat(),
                "confidence_factors": {
                    "has_anthropic_data": job_data is not None,
                    "has_recent_employment": employment_data is not None,
                    "has_industry_breakdown": len(industries) > 5,
                    "uses_occupation_mapping": self._uses_occupation_mapping(displacement_by_industry)
                },
                "methodology_details": self._get_methodology_details(displacement_by_industry)
            },
            "validation": {
                "employment_coverage": total_employment / total_nonfarm_employment if total_nonfarm_employment > 0 else 0,
                "bls_total_nonfarm": total_nonfarm_employment,  # Now in actual worker count
                "bls_total_nonfarm_thousands": total_nonfarm_employment_thousands  # Also provide thousands
            }
        }
        
        # Validate results before returning
        validation_results = self.validate_calculation_results(result, industries)
        result["validation"].update(validation_results)
        
        return result

    def validate_calculation_results(self, results, industry_data):
        """
        Validate calculation results for consistency and reasonableness.
        """
        validation = {
            "validation_passed": True,
            "warnings": [],
            "errors": []
        }
        
        try:
            # Check 1: Overall impact is within reasonable bounds
            total_impact = results["total_impact"]
            if abs(total_impact) > 0.5:  # More than 50% change seems unrealistic
                validation["warnings"].append(f"Very large total impact: {total_impact:.4f} ({total_impact*100:.1f}%)")
            
            # Check 2: Sum of industry employments vs BLS total
            total_employment = results["total_employment"]
            bls_total = results["validation"]["bls_total_nonfarm"]
            if bls_total > 0:
                coverage = total_employment / bls_total
                if coverage < 0.8:
                    validation["warnings"].append(f"Low employment coverage: {coverage:.2f} (missing {(1-coverage)*100:.1f}% of workforce)")
                elif coverage > 1.1:
                    validation["errors"].append(f"Employment sum exceeds BLS total: {coverage:.2f}")
            
            # Check 3: Industry-level impacts are reasonable
            extreme_impacts = []
            for industry, data in results["by_industry"].items():
                if industry != "Total Nonfarm":
                    impact = data["impact"]
                    if abs(impact) > 0.3:  # More than 30% change at industry level
                        extreme_impacts.append(f"{industry}: {impact:.3f}")
            
            if extreme_impacts:
                validation["warnings"].append(f"Extreme industry impacts: {', '.join(extreme_impacts)}")
            
            # Check 4: Component values are within expected ranges
            components = results["components"]
            
            if abs(components["displacement_effect"]) > 0.8:
                validation["warnings"].append(f"Very high displacement effect: {components['displacement_effect']:.3f}")
            
            if components["creation_effect"] > 0.5:
                validation["warnings"].append(f"Very high creation effect: {components['creation_effect']:.3f}")
            
            if abs(components["demand_effect"]) > 0.3:
                validation["warnings"].append(f"Unusual demand effect: {components['demand_effect']:.3f}")
            
            # Check 5: No division by zero or NaN values
            def check_for_nan(obj, path=""):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        check_for_nan(value, f"{path}.{key}" if path else key)
                elif isinstance(obj, (int, float)):
                    if np.isnan(obj) or np.isinf(obj):
                        validation["errors"].append(f"Invalid numeric value at {path}: {obj}")
            
            check_for_nan(results)
            
            # Check 6: Jobs affected is reasonable relative to total employment
            jobs_affected = abs(results["jobs_affected"])
            if total_employment > 0:
                jobs_ratio = jobs_affected / total_employment
                if jobs_ratio > 0.5:
                    validation["warnings"].append(f"Very high jobs affected ratio: {jobs_ratio:.3f}")
            
            # Set overall validation status
            if validation["errors"]:
                validation["validation_passed"] = False
            
            # Log validation results
            logger.info(f"\n=== VALIDATION RESULTS ===")
            logger.info(f"Validation passed: {validation['validation_passed']}")
            
            if validation["warnings"]:
                logger.warning(f"Validation warnings ({len(validation['warnings'])}):")
                for warning in validation["warnings"]:
                    logger.warning(f"  - {warning}")
            
            if validation["errors"]:
                logger.error(f"Validation errors ({len(validation['errors'])}):")
                for error in validation["errors"]:
                    logger.error(f"  - {error}")
            
            if not validation["warnings"] and not validation["errors"]:
                logger.info("All validation checks passed successfully.")
        
        except Exception as e:
            validation["errors"].append(f"Validation process failed: {str(e)}")
            validation["validation_passed"] = False
            logger.error(f"Validation error: {e}")
        
        return validation

    def assess_data_completeness(self, employment_data, job_data):
        """Assess the completeness of input data"""
        score = 0.0
        if employment_data: score += 0.4
        if job_data: score += 0.3
        if employment_data and len(employment_data.get("industries", {})) > 8: score += 0.3
        return score

    def _determine_calculation_method(self, displacement_by_industry):
        """Determine which calculation method was used based on displacement data"""
        if not displacement_by_industry:
            return "component_based"
        
        # Check if any industry used occupation mapping
        for industry_data in displacement_by_industry.values():
            if industry_data.get("calculation_method") == "occupation_mapped":
                return "occupation_weighted_component_based"
        
        return "component_based"

    def _uses_occupation_mapping(self, displacement_by_industry):
        """Check if occupation mapping was used for any industries"""
        if not displacement_by_industry:
            return False
        
        return any(
            industry_data.get("calculation_method") == "occupation_mapped"
            for industry_data in displacement_by_industry.values()
        )

    def _get_methodology_details(self, displacement_by_industry):
        """Get detailed methodology information"""
        details = {
            "displacement_calculation": "fallback",
            "occupation_mapping_coverage": 0.0,
            "average_data_coverage": 0.0,
            "average_confidence": 0.0
        }
        
        if not displacement_by_industry:
            return details
        
        # Analyze methodology used
        occupation_mapped_count = 0
        total_coverage = 0.0
        total_confidence = 0.0
        
        for industry_data in displacement_by_industry.values():
            if industry_data.get("calculation_method") == "occupation_mapped":
                occupation_mapped_count += 1
                total_coverage += industry_data.get("data_coverage", 0.0)
                total_confidence += industry_data.get("confidence", 0.0)
        
        total_industries = len(displacement_by_industry)
        
        if occupation_mapped_count > 0:
            details["displacement_calculation"] = "occupation_mapped"
            details["occupation_mapping_coverage"] = occupation_mapped_count / total_industries
            details["average_data_coverage"] = total_coverage / occupation_mapped_count
            details["average_confidence"] = total_confidence / occupation_mapped_count
        
        return details

    def save_results(self, results):
        """Save the calculation results to a file."""
        # Add comprehensive metadata
        results["metadata"] = {
            "calculation_type": "component_based",
            "components_included": ["displacement", "creation", "maturity", "demand"],
            "transformation_rate_included": True,
            "confidence_intervals_available": True,
            "last_calculation": datetime.now().isoformat()
        }
        
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

    def load_anthropic_data(self):
        """Load Anthropic Economic Index data with fallback search."""
        # Try multiple possible filenames for Anthropic data
        anthropic_filenames = [
            f"anthropic_index_{self.date_str}_combined.json",
            f"anthropic_index_{self.year}_{self.month:02d}_combined.json",
            "anthropic_index_20250413_combined.json",  # Latest known file
            "anthropic_index_2025_04_combined.json"
        ]
        
        for filename in anthropic_filenames:
            anthropic_data = self.load_data(filename, search_subdirs=True)
            if anthropic_data:
                logger.info(f"Loaded Anthropic data from: {filename}")
                return anthropic_data
        
        logger.warning("No Anthropic Economic Index data found, will use defaults")
        return None

    def validate_data_sources(self, employment_data, anthropic_data, job_data):
        """Validate data sources and log data quality metrics."""
        validation_results = {
            "employment_data_valid": False,
            "anthropic_data_valid": False,
            "job_data_valid": False,
            "total_employment": 0,
            "industry_count": 0,
            "automation_data_coverage": 0.0
        }
        
        # Validate employment data
        if employment_data and "industries" in employment_data:
            industries = employment_data["industries"]
            validation_results["industry_count"] = len(industries)
            
            def get_employment(data):
                employment = data.get("current", 0)
                if employment == 0:
                    employment = data.get("current_employment", 0)
                if employment == 0:
                    employment = data.get("employment", 0)
                return employment
            
            total_emp = sum(get_employment(data) for industry, data in industries.items() if industry != "Total Nonfarm")
            validation_results["total_employment"] = total_emp
            
            if total_emp > 0 and len(industries) > 5:
                validation_results["employment_data_valid"] = True
                logger.info(f"✓ Employment data valid: {len(industries)} industries, {total_emp:,} total employment")
            else:
                logger.error(f"✗ Employment data invalid: {len(industries)} industries, {total_emp:,} total employment")
        
        # Validate Anthropic data
        if anthropic_data:
            if "datasets" in anthropic_data and "occupation_automation" in anthropic_data["datasets"]:
                occupation_data = anthropic_data["datasets"]["occupation_automation"]
                if len(occupation_data) > 0:
                    validation_results["anthropic_data_valid"] = True
                    validation_results["automation_data_coverage"] = len(occupation_data)
                    logger.info(f"✓ Anthropic data valid: {len(occupation_data)} occupations")
                else:
                    logger.warning(f"✗ Anthropic data empty: {len(occupation_data)} occupations")
            else:
                logger.warning("✗ Anthropic data missing expected structure")
        else:
            logger.warning("✗ No Anthropic data loaded")
        
        # Validate job data
        if job_data:
            validation_results["job_data_valid"] = True
            logger.info("✓ Job trends data available")
        else:
            logger.warning("✗ No job trends data available")
        
        return validation_results

    def calculate_impact(self):
        """Main method to calculate the AI Labor Market Impact index."""
        # Load required data
        employment_data = self.load_data(self.employment_file)
        job_data = self.load_data(self.job_trends_file)
        events_data = self.load_data(self.workforce_events_file)
        research_data = self.load_data(self.research_trends_file)
        
        # Use job_trends data which contains the Anthropic data in the new format
        # If job_data exists and has industry_impacts, use it as the primary source
        if job_data and "industry_impacts" in job_data:
            logger.info("Using job_trends data with industry-specific impacts")
            anthropic_data = job_data  # job_trends contains the processed Anthropic data
        else:
            # Fall back to loading old format Anthropic data if needed
            logger.info("No industry impacts in job_trends, loading Anthropic data separately")
            anthropic_data = self.load_anthropic_data()

        # Validate all data sources
        validation_results = self.validate_data_sources(employment_data, anthropic_data, job_data)
        
        # Check if we have the required data
        if not employment_data:
            logger.error(f"Missing required employment data file: {self.employment_file}")
            return None
        
        if not validation_results["employment_data_valid"]:
            logger.error("Employment data validation failed - cannot proceed")
            return None
            
        if not job_data:
            logger.warning(f"Missing job trends data file: {self.job_trends_file}")
            
        if not anthropic_data:
            logger.warning("No Anthropic data found - will use default automation/augmentation rates")
        elif not validation_results["anthropic_data_valid"]:
            logger.warning("Anthropic data validation failed - will use default rates")
        
        # Calculate the impact using Anthropic data
        impact_results = self.calculate_net_impact(employment_data, anthropic_data, events_data, research_data)
        
        # Save results
        self.save_results(impact_results)
        
        # Log detailed summary
        logger.info(f"\n=== CALCULATION SUMMARY ===")
        logger.info(f"AI Labor Market Impact calculation complete.")
        logger.info(f"Overall impact: {impact_results['total_impact']:.4f} ({impact_results['total_impact']*100:.2f}%)")
        logger.info(f"Total jobs affected (NET): {impact_results['jobs_affected']:,}")
        logger.info(f"Jobs displaced: {impact_results['jobs_displaced']:,}")
        logger.info(f"Jobs created: {impact_results['jobs_created']:,}")
        logger.info(f"Jobs from demand effect: {impact_results['jobs_demand_effect']:,}")
        logger.info(f"Total employment analyzed: {impact_results['total_employment']:,}")
        
        validation = impact_results.get('validation', {})
        logger.info(f"Employment coverage: {validation.get('employment_coverage', 0):.3f}")
        logger.info(f"BLS Total Nonfarm: {validation.get('bls_total_nonfarm', 0):,}")
        
        # Log detailed data source usage
        components = impact_results['components']
        logger.info(f"\n=== DATA SOURCES USED ===")
        logger.info(f"News events: {components.get('news_events_count', 0)} articles (momentum: {components.get('momentum_factor', 1.0):.3f})")
        logger.info(f"Market maturity: {components.get('market_maturity', 0):.3f} -> {components.get('adjusted_market_maturity', 0):.3f}")
        
        # Count data source usage by type
        data_sources = {
            'anthropic_automation': 0,
            'default_automation': 0, 
            'real_productivity': 0,
            'default_productivity': 0,
            'ai_jobs_data': 0,
            'default_ai_jobs': 0
        }
        
        # Track which data sources were actually used
        methodology = impact_results.get('data_quality', {}).get('methodology_details', {})
        if methodology.get('displacement_calculation') == 'occupation_mapped':
            logger.info(f"✓ Using occupation-mapped displacement calculation")
            logger.info(f"  - Occupation mapping coverage: {methodology.get('occupation_mapping_coverage', 0):.1%}")
            logger.info(f"  - Average data coverage: {methodology.get('average_data_coverage', 0):.1%}")
            logger.info(f"  - Average confidence: {methodology.get('average_confidence', 0):.1%}")
        else:
            logger.info(f"⚠ Using fallback displacement calculation")
        
        # Log employment data coverage
        validation = impact_results.get('validation', {})
        logger.info(f"Employment coverage: {validation.get('employment_coverage', 0):.1%}")
        logger.info(f"BLS Total Nonfarm: {validation.get('bls_total_nonfarm', 0):,}")
        
        # Log confidence factors
        confidence = impact_results.get('data_quality', {}).get('confidence_factors', {})
        logger.info(f"\n=== DATA QUALITY CONFIDENCE ===")
        logger.info(f"Has Anthropic data: {confidence.get('has_anthropic_data', False)}")
        logger.info(f"Has recent employment: {confidence.get('has_recent_employment', False)}")
        logger.info(f"Has industry breakdown: {confidence.get('has_industry_breakdown', False)}")
        logger.info(f"Uses occupation mapping: {confidence.get('uses_occupation_mapping', False)}")
        
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