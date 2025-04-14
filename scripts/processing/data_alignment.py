#!/usr/bin/env python3
# scripts/processing/data_alignment.py
#
# Data alignment utilities for AI Labor Market Index
# Creates and manages mappings between different taxonomies

import os
import json
import logging
import sys
from datetime import datetime
import requests
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_alignment.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("data-alignment")

class DataAlignmentManager:
    """
    Manages data alignment between different taxonomies including:
    - Anthropic Economic Index (occupation-level data)
    - Bureau of Labor Statistics (industry-level data)
    - News events (company-level data)
    """
    
    def __init__(self, mapping_dir="./data/mappings", data_dir="./data/processed", 
                 raw_dir="./data/raw", use_onet=False, onet_api_key=None):
        """
        Initialize the data alignment manager
        
        Args:
            mapping_dir (str): Directory to store mapping files
            data_dir (str): Directory containing processed data
            raw_dir (str): Directory containing raw data
            use_onet (bool): Whether to use O*NET API for enhanced mappings
            onet_api_key (str): API key for O*NET Web Services (if use_onet is True)
        """
        self.mapping_dir = mapping_dir
        self.data_dir = data_dir
        self.raw_dir = raw_dir
        self.use_onet = use_onet
        self.onet_api_key = onet_api_key
        
        # Create mapping directory if it doesn't exist
        os.makedirs(mapping_dir, exist_ok=True)
        
        # Initialize mapping files
        self.mapping_files = {
            "occupation_to_industry": os.path.join(mapping_dir, "occupation_to_industry.json"),
            "occupation_categories": os.path.join(mapping_dir, "occupation_categories.json"),
            "company_to_occupations": os.path.join(mapping_dir, "company_to_occupations.json"),
            "industry_to_tasks": os.path.join(mapping_dir, "industry_to_tasks.json"),
            "occupation_to_tasks": os.path.join(mapping_dir, "occupation_to_tasks.json"),
            "company_to_industries": os.path.join(mapping_dir, "company_to_industries.json")
        }
        
        # Load existing mappings if available
        self.mappings = self._load_mappings()
    
    def _load_mappings(self):
        """Load all existing mapping files"""
        mappings = {}
        
        for key, filepath in self.mapping_files.items():
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r') as f:
                        mappings[key] = json.load(f)
                        logger.info(f"Loaded existing mapping: {key} with {len(mappings[key])} entries")
                except json.JSONDecodeError:
                    logger.error(f"Error loading mapping file: {filepath}")
                    mappings[key] = {}
            else:
                mappings[key] = {}
                
        return mappings
    
    def _save_mapping(self, mapping_name):
        """Save a specific mapping to file"""
        filepath = self.mapping_files[mapping_name]
        
        with open(filepath, 'w') as f:
            json.dump(self.mappings[mapping_name], f, indent=2)
            
        logger.info(f"Saved mapping {mapping_name} to {filepath}")
    
    def _find_latest_file(self, directory, pattern):
        """Find the most recent file matching the given pattern in directory"""
        import glob
        files = glob.glob(os.path.join(directory, pattern))
        if not files:
            return None
        
        # Sort by modification time (newest first)
        return sorted(files, key=os.path.getmtime, reverse=True)[0]
    
    def _load_anthropic_data(self):
        """Load the latest Anthropic Economic Index data"""
        # Find the latest combined file
        combined_file = self._find_latest_file(
            os.path.join(self.raw_dir, "anthropic_index"), 
            "anthropic_index_*_combined.json"
        )
        
        if not combined_file:
            logger.error("No Anthropic Index data found")
            return None
        
        try:
            with open(combined_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading Anthropic data: {str(e)}")
            return None
    
    def _load_bls_data(self):
        """Load the latest BLS employment data"""
        # Find the latest employment stats file
        employment_file = self._find_latest_file(
            self.data_dir,
            "employment_stats_*.json"
        )
        
        if not employment_file:
            logger.error("No BLS employment data found")
            return None
        
        try:
            with open(employment_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading BLS data: {str(e)}")
            return None
    
    def _load_news_data(self):
        """Load the latest news events data"""
        # Find the latest workforce events file
        events_file = self._find_latest_file(
            self.data_dir,
            "workforce_events_*.json"
        )
        
        if not events_file:
            logger.error("No news events data found")
            return None
        
        try:
            with open(events_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading news events data: {str(e)}")
            return None
    
    def build_occupation_to_industry_mapping(self):
        """
        Create mapping between occupations and industries
        
        This uses a combination of:
        1. SOC major groups from Anthropic data
        2. BLS industry categories
        3. O*NET data if available
        """
        logger.info("Building occupation to industry mapping")
        
        # Load the latest anthropic data
        anthropic_data = self._load_anthropic_data()
        if not anthropic_data or "datasets" not in anthropic_data:
            logger.error("Cannot build mapping without Anthropic data")
            return False
        
        # Extract occupation categories and automation data
        datasets = anthropic_data["datasets"]
        occupation_categories = datasets.get("occupation_categories", {})
        occupation_automation = datasets.get("occupation_automation", {})
        
        # Map SOC major groups to specific occupations
        soc_to_occupations = defaultdict(list)
        
        # Define a mapping between occupation categories and BLS industries
        category_to_industry = {
            "Management": "Professional and Business Services",
            "Business and Financial Operations": "Financial Activities",
            "Computer and Mathematical": "Information",
            "Architecture and Engineering": "Professional and Business Services",
            "Life, Physical, and Social Science": "Professional and Business Services",
            "Community and Social Service": "Education and Health Services",
            "Legal": "Professional and Business Services",
            "Educational Instruction and Library": "Education and Health Services",
            "Arts, Design, Entertainment, Sports, and Media": "Information",
            "Healthcare Practitioners and Technical": "Education and Health Services",
            "Healthcare Support": "Education and Health Services",
            "Protective Service": "Government",
            "Food Preparation and Serving Related": "Leisure and Hospitality",
            "Building and Grounds Cleaning and Maintenance": "Other Services",
            "Personal Care and Service": "Other Services",
            "Sales and Related": "Trade, Transportation, and Utilities",
            "Office and Administrative Support": "Professional and Business Services",
            "Farming, Fishing, and Forestry": "Mining and Logging",
            "Construction and Extraction": "Construction",
            "Installation, Maintenance, and Repair": "Trade, Transportation, and Utilities",
            "Production": "Manufacturing",
            "Transportation and Material Moving": "Trade, Transportation, and Utilities",
            "Military Specific": "Government"
        }
        
        # Map each occupation to a specific SOC group
        occupation_to_category = {
            "Software Developers": "Computer and Mathematical",
            "Data Scientists": "Computer and Mathematical",
            "Financial Analysts": "Business and Financial Operations",
            "Content Writers": "Arts, Design, Entertainment, Sports, and Media",
            "Customer Service Representatives": "Office and Administrative Support",
            "Graphic Designers": "Arts, Design, Entertainment, Sports, and Media",
            "Marketing Specialists": "Sales and Related",
            "Human Resources Specialists": "Business and Financial Operations",
            "Accountants": "Business and Financial Operations",
            "Legal Assistants": "Legal",
            "Teachers": "Educational Instruction and Library",
            "Nurses": "Healthcare Practitioners and Technical",
            "Product Managers": "Management",
            "Sales Representatives": "Sales and Related",
            "Research Scientists": "Life, Physical, and Social Science",
            "Physicians": "Healthcare Practitioners and Technical",
            "Administrative Assistants": "Office and Administrative Support",
            "Social Media Managers": "Arts, Design, Entertainment, Sports, and Media",
            "Mechanical Engineers": "Architecture and Engineering",
            "Project Managers": "Management"
        }
        
        # Combine the mappings to create occupation to industry
        occupation_to_industry = {}
        
        for occupation in occupation_automation.keys():
            if occupation in occupation_to_category:
                category = occupation_to_category[occupation]
                if category in category_to_industry:
                    industry = category_to_industry[category]
                    occupation_to_industry[occupation] = {
                        "primary_industry": industry,
                        "soc_category": category,
                        "secondary_industries": []
                    }
                    
                    # Add some secondary industries for certain occupations
                    if occupation == "Software Developers":
                        occupation_to_industry[occupation]["secondary_industries"] = [
                            "Financial Activities", "Professional and Business Services"
                        ]
                    elif occupation == "Data Scientists":
                        occupation_to_industry[occupation]["secondary_industries"] = [
                            "Professional and Business Services", "Financial Activities"
                        ]
                    elif occupation == "Marketing Specialists":
                        occupation_to_industry[occupation]["secondary_industries"] = [
                            "Professional and Business Services", "Information"
                        ]
        
        # Save the mapping
        self.mappings["occupation_to_industry"] = occupation_to_industry
        self._save_mapping("occupation_to_industry")
        
        logger.info(f"Built occupation to industry mapping with {len(occupation_to_industry)} entries")
        return True
    
    def build_company_to_occupations_mapping(self):
        """
        Create mapping between companies and occupations
        
        This uses:
        1. News event data for company names
        2. Industry information to infer likely occupations
        """
        logger.info("Building company to occupations mapping")
        
        # Load the latest news data
        news_data = self._load_news_data()
        if not news_data or "events" not in news_data:
            logger.error("Cannot build mapping without news data")
            return False
        
        # Get unique companies from news events
        events = news_data["events"]
        companies = set()
        
        for event in events:
            company = event.get("company")
            if company and company != "Unknown":
                companies.add(company)
        
        # Create tech-focused company mapping
        tech_companies = {
            "Google": ["Software Developers", "Data Scientists", "Product Managers"],
            "Meta": ["Software Developers", "Data Scientists", "Product Managers", "Content Writers", "Marketing Specialists"],
            "Microsoft": ["Software Developers", "Product Managers", "Data Scientists", "Project Managers"],
            "Amazon": ["Software Developers", "Data Scientists", "Product Managers", "Marketing Specialists", "Sales Representatives"],
            "Apple": ["Software Developers", "Product Managers", "Graphic Designers", "Marketing Specialists"],
            "Tesla": ["Mechanical Engineers", "Software Developers", "Product Managers", "Project Managers"],
            "Netflix": ["Software Developers", "Data Scientists", "Content Writers", "Marketing Specialists"],
            "IBM": ["Software Developers", "Data Scientists", "Project Managers", "Sales Representatives"],
            "Intel": ["Software Developers", "Mechanical Engineers", "Product Managers", "Project Managers"],
            "Salesforce": ["Software Developers", "Sales Representatives", "Marketing Specialists", "Product Managers"],
            "Oracle": ["Software Developers", "Sales Representatives", "Project Managers", "Data Scientists"],
            "Adobe": ["Software Developers", "Graphic Designers", "Product Managers", "Marketing Specialists"],
            "Twitter": ["Software Developers", "Data Scientists", "Content Writers", "Product Managers"],
            "LinkedIn": ["Software Developers", "Data Scientists", "Product Managers", "Sales Representatives"],
            "Cisco": ["Software Developers", "Project Managers", "Sales Representatives", "Product Managers"],
            "HP": ["Software Developers", "Mechanical Engineers", "Product Managers", "Sales Representatives"]
        }
        
        # Create financial company mapping
        financial_companies = {
            "JPMorgan Chase": ["Financial Analysts", "Data Scientists", "Software Developers", "Sales Representatives"],
            "Bank of America": ["Financial Analysts", "Accountants", "Customer Service Representatives", "Software Developers"],
            "Wells Fargo": ["Financial Analysts", "Accountants", "Customer Service Representatives", "Software Developers"],
            "Citigroup": ["Financial Analysts", "Accountants", "Software Developers", "Data Scientists"],
            "Goldman Sachs": ["Financial Analysts", "Data Scientists", "Software Developers", "Product Managers"],
            "Morgan Stanley": ["Financial Analysts", "Data Scientists", "Software Developers", "Sales Representatives"]
        }
        
        # Create healthcare company mapping
        healthcare_companies = {
            "UnitedHealth Group": ["Nurses", "Physicians", "Data Scientists", "Software Developers", "Administrative Assistants"],
            "CVS Health": ["Pharmacists", "Customer Service Representatives", "Administrative Assistants", "Sales Representatives"],
            "Johnson & Johnson": ["Research Scientists", "Physicians", "Product Managers", "Marketing Specialists"],
            "Pfizer": ["Research Scientists", "Data Scientists", "Product Managers", "Marketing Specialists"],
            "Merck": ["Research Scientists", "Data Scientists", "Product Managers", "Marketing Specialists"],
            "Novartis": ["Research Scientists", "Data Scientists", "Product Managers", "Marketing Specialists"]
        }
        
        # Create education company mapping
        education_companies = {
            "University": ["Teachers", "Research Scientists", "Administrative Assistants", "Project Managers"],
            "College": ["Teachers", "Administrative Assistants", "Marketing Specialists", "Project Managers"],
            "School": ["Teachers", "Administrative Assistants", "Social Media Managers", "Project Managers"]
        }
        
        # Create retail company mapping
        retail_companies = {
            "Walmart": ["Sales Representatives", "Customer Service Representatives", "Marketing Specialists", "Product Managers"],
            "Target": ["Sales Representatives", "Customer Service Representatives", "Marketing Specialists", "Product Managers"],
            "Costco": ["Sales Representatives", "Customer Service Representatives", "Administrative Assistants", "Product Managers"],
            "Home Depot": ["Sales Representatives", "Customer Service Representatives", "Marketing Specialists", "Project Managers"],
            "Lowe's": ["Sales Representatives", "Customer Service Representatives", "Marketing Specialists", "Project Managers"]
        }
        
        # Combine all company mappings
        company_to_occupations = {}
        
        for company_map in [tech_companies, financial_companies, healthcare_companies, education_companies, retail_companies]:
            company_to_occupations.update(company_map)
        
        # Add any companies from news data that aren't already mapped
        for company in companies:
            if company not in company_to_occupations:
                # Generic mapping for unknown companies
                company_to_occupations[company] = ["Software Developers", "Product Managers", "Marketing Specialists", "Project Managers"]
        
        # Add additional detail for each occupation
        for company, occupations in company_to_occupations.items():
            enriched_occupations = {}
            for occupation in occupations:
                # Guess the AI relationship based on occupation and company type
                is_tech = company in tech_companies
                is_financial = company in financial_companies
                
                if occupation in ["Software Developers", "Data Scientists", "Product Managers"]:
                    ai_impact = "high" if is_tech else "medium"
                elif occupation in ["Content Writers", "Graphic Designers", "Marketing Specialists"]:
                    ai_impact = "medium"
                elif occupation in ["Financial Analysts", "Accountants"]:
                    ai_impact = "high" if is_financial else "medium"
                else:
                    ai_impact = "low"
                
                enriched_occupations[occupation] = {
                    "relevance": "primary" if occupation == occupations[0] else "secondary",
                    "ai_impact": ai_impact
                }
            
            company_to_occupations[company] = enriched_occupations
        
        # Save the mapping
        self.mappings["company_to_occupations"] = company_to_occupations
        self._save_mapping("company_to_occupations")
        
        logger.info(f"Built company to occupations mapping with {len(company_to_occupations)} entries")
        return True
    
    def build_occupation_to_tasks_mapping(self):
        """
        Create mapping between occupations and tasks
        
        This uses:
        1. Anthropic task usage data
        2. O*NET task data if available
        """
        logger.info("Building occupation to tasks mapping")
        
        # Load the latest anthropic data
        anthropic_data = self._load_anthropic_data()
        if not anthropic_data or "datasets" not in anthropic_data:
            logger.error("Cannot build mapping without Anthropic data")
            return False
        
        # Extract task usage and occupation usage data
        datasets = anthropic_data["datasets"]
        task_usage = datasets.get("task_usage", {})
        occupation_usage = datasets.get("occupation_usage", {})
        
        # Create mapping between occupations and tasks
        occupation_to_tasks = {}
        
        # Start with tasks from occupation_usage
        for occupation, data in occupation_usage.items():
            tasks = data.get("tasks", [])
            task_details = {}
            
            for task in tasks:
                if task in task_usage:
                    task_data = task_usage[task]
                    task_details[task] = {
                        "count": task_data.get("count", 0),
                        "automation_potential": task_data.get("automation_potential", 50),
                        "augmentation_potential": task_data.get("augmentation_potential", 50)
                    }
            
            occupation_to_tasks[occupation] = task_details
        
        # Add additional task mappings based on occupation types
        occupation_task_examples = {
            "Software Developers": [
                "design software solutions to meet specific requirements",
                "write clean, maintainable code according to specifications",
                "debug and troubleshoot software issues",
                "collaborate with cross-functional teams on product development",
                "implement software testing procedures"
            ],
            "Data Scientists": [
                "analyze large datasets to identify patterns and trends",
                "develop machine learning models for predictive analytics",
                "create data visualizations to communicate insights",
                "clean and preprocess data for analysis",
                "collaborate with stakeholders to define data requirements"
            ],
            "Financial Analysts": [
                "analyze financial data to inform business decisions",
                "create financial models and forecasts",
                "evaluate investment opportunities",
                "prepare reports on financial performance",
                "monitor industry trends and market conditions"
            ],
            "Content Writers": [
                "create engaging written content for various platforms",
                "research topics to ensure accurate information",
                "edit and proofread content for grammar and style",
                "optimize content for search engines",
                "adapt tone and style for different audiences"
            ]
        }
        
        # Add example tasks for occupations that lack task data
        for occupation, tasks in occupation_task_examples.items():
            if occupation not in occupation_to_tasks or len(occupation_to_tasks[occupation]) < 3:
                task_details = {}
                
                for task in tasks:
                    # Make up reasonable values
                    if occupation in ["Software Developers", "Data Scientists"]:
                        auto_potential = 30
                        aug_potential = 70
                    elif occupation in ["Financial Analysts"]:
                        auto_potential = 50
                        aug_potential = 50
                    else:
                        auto_potential = 40
                        aug_potential = 60
                    
                    task_details[task] = {
                        "count": 1000,  # Arbitrary count
                        "automation_potential": auto_potential,
                        "augmentation_potential": aug_potential,
                        "is_example": True  # Flag to indicate this is an example task
                    }
                
                if occupation in occupation_to_tasks:
                    occupation_to_tasks[occupation].update(task_details)
                else:
                    occupation_to_tasks[occupation] = task_details
        
        # Save the mapping
        self.mappings["occupation_to_tasks"] = occupation_to_tasks
        self._save_mapping("occupation_to_tasks")
        
        logger.info(f"Built occupation to tasks mapping with {len(occupation_to_tasks)} entries")
        return True
    
    def build_company_to_industries_mapping(self):
        """
        Create mapping between companies and industries
        
        This uses:
        1. News event data for company names
        2. Company to occupation mapping
        3. Occupation to industry mapping
        """
        logger.info("Building company to industries mapping")
        
        # Load the latest news data
        news_data = self._load_news_data()
        if not news_data or "events" not in news_data:
            logger.error("Cannot build mapping without news data")
            return False
        
        # Get company to occupations mapping
        if not self.mappings.get("company_to_occupations"):
            logger.error("Cannot build mapping without company to occupations mapping")
            return False
        
        # Get occupation to industry mapping
        if not self.mappings.get("occupation_to_industry"):
            logger.error("Cannot build mapping without occupation to industry mapping")
            return False
        
        company_to_occupations = self.mappings["company_to_occupations"]
        occupation_to_industry = self.mappings["occupation_to_industry"]
        
        # Create mapping between companies and industries
        company_to_industries = {}
        
        for company, occupations in company_to_occupations.items():
            industry_counts = defaultdict(int)
            
            # Count industries associated with each occupation
            for occupation in occupations:
                if occupation in occupation_to_industry:
                    primary_industry = occupation_to_industry[occupation].get("primary_industry")
                    if primary_industry:
                        industry_counts[primary_industry] += 2  # Give more weight to primary industry
                    
                    secondary_industries = occupation_to_industry[occupation].get("secondary_industries", [])
                    for industry in secondary_industries:
                        industry_counts[industry] += 1
            
            if industry_counts:
                # Sort industries by count (descending)
                sorted_industries = sorted(industry_counts.items(), key=lambda x: x[1], reverse=True)
                
                primary_industry = sorted_industries[0][0]
                secondary_industries = [ind for ind, _ in sorted_industries[1:]]
                
                company_to_industries[company] = {
                    "primary_industry": primary_industry,
                    "secondary_industries": secondary_industries
                }
            else:
                # Default to Information for unknown companies
                company_to_industries[company] = {
                    "primary_industry": "Information",
                    "secondary_industries": ["Professional and Business Services"]
                }
        
        # Save the mapping
        self.mappings["company_to_industries"] = company_to_industries
        self._save_mapping("company_to_industries")
        
        logger.info(f"Built company to industries mapping with {len(company_to_industries)} entries")
        return True
    
    def build_all_mappings(self):
        """Build all mapping files"""
        logger.info("Building all data alignment mappings")
        
        # Build mappings in the correct order
        self.build_occupation_to_industry_mapping()
        self.build_company_to_occupations_mapping()
        self.build_occupation_to_tasks_mapping()
        self.build_company_to_industries_mapping()
        
        logger.info("Completed building all mappings")
        return True
    
    def enrich_bls_data(self, bls_data):
        """
        Enrich BLS data with occupation and task information
        
        Args:
            bls_data (dict): Original BLS employment data
            
        Returns:
            dict: Enriched BLS data with occupation breakdowns
        """
        logger.info("Enriching BLS employment data")
        
        if not bls_data or "industries" not in bls_data:
            logger.error("Invalid BLS data for enrichment")
            return bls_data
        
        # Get required mappings
        occupation_to_industry = self.mappings.get("occupation_to_industry", {})
        occupation_to_tasks = self.mappings.get("occupation_to_tasks", {})
        
        if not occupation_to_industry or not occupation_to_tasks:
            logger.error("Required mappings missing for BLS enrichment")
            return bls_data
        
        # Create a copy of the original data
        enriched_data = dict(bls_data)
        
        # Map occupations to industries
        industry_to_occupations = defaultdict(list)
        for occupation, data in occupation_to_industry.items():
            industry = data.get("primary_industry")
            if industry:
                industry_to_occupations[industry].append(occupation)
        
        # Enrich each industry with occupation data
        for industry, industry_data in enriched_data["industries"].items():
            occupations = industry_to_occupations.get(industry, [])
            
            # Get automation/augmentation rates for each occupation
            occupation_rates = []
            for occupation in occupations:
                # Get occupation details from Anthropic data
                anthropic_data = self._load_anthropic_data()
                if anthropic_data and "datasets" in anthropic_data:
                    occupation_automation = anthropic_data["datasets"].get("occupation_automation", {})
                    if occupation in occupation_automation:
                        occupation_rates.append({
                            "occupation": occupation,
                            "automation_rate": occupation_automation[occupation].get("automation_rate", 0),
                            "augmentation_rate": occupation_automation[occupation].get("augmentation_rate", 0)
                        })
            
            # Calculate industry-level automation/augmentation rates
            if occupation_rates:
                avg_automation = sum(rate["automation_rate"] for rate in occupation_rates) / len(occupation_rates)
                avg_augmentation = sum(rate["augmentation_rate"] for rate in occupation_rates) / len(occupation_rates)
                
                # Add enriched data to the industry
                industry_data["ai_impact"] = {
                    "occupations": occupation_rates,
                    "average_automation_rate": avg_automation,
                    "average_augmentation_rate": avg_augmentation,
                    "automation_augmentation_ratio": avg_automation / avg_augmentation if avg_augmentation > 0 else 0
                }
            
            # Add employment change classification
            change_percentage = industry_data.get("change_percentage", 0)
            
            if change_percentage > 3:
                growth_category = "rapid_growth"
            elif change_percentage > 1:
                growth_category = "moderate_growth"
            elif change_percentage > 0:
                growth_category = "slow_growth"
            elif change_percentage > -1:
                growth_category = "slight_decline"
            else:
                growth_category = "significant_decline"
            
            industry_data["growth_category"] = growth_category
        
        return enriched_data
    
    def enrich_news_data(self, news_data):
        """
        Enrich news event data with occupation and industry information
        
        Args:
            news_data (dict): Original news event data
            
        Returns:
            dict: Enriched news data with occupation and industry information
        """
        logger.info("Enriching news event data")
        
        if not news_data or "events" not in news_data:
            logger.error("Invalid news data for enrichment")
            return news_data
        
        # Get required mappings
        company_to_occupations = self.mappings.get("company_to_occupations", {})
        company_to_industries = self.mappings.get("company_to_industries", {})
        
        if not company_to_occupations or not company_to_industries:
            logger.error("Required mappings missing for news enrichment")
            return news_data
        
        # Create a copy of the original data
        enriched_data = dict(news_data)
        
        # Enrich each event with occupation and industry information
        for event in enriched_data["events"]:
            company = event.get("company")
            
            if company and company != "Unknown" and company in company_to_occupations:
                # Add occupation information
                event["related_occupations"] = list(company_to_occupations[company].keys())
                
                # Add industry information if available
                if company in company_to_industries:
                    event["related_industries"] = [
                        company_to_industries[company].get("primary_industry", "Information")
                    ] + company_to_industries[company].get("secondary_industries", [])
                
                # Categorize event by AI impact
                event_type = event.get("event_type", "")
                ai_relation = event.get("ai_relation", "None")
                
                # Determine if the event is likely automation or augmentation
                is_automation = False
                is_augmentation = False
                
                if event_type == "layoff" and ai_relation in ["Direct", "Indirect"]:
                    is_automation = True
                elif event_type == "hiring" and ai_relation in ["Direct", "Indirect"]:
                    is_augmentation = True
                
                if is_automation:
                    event["ai_impact_type"] = "automation"
                elif is_augmentation:
                    event["ai_impact_type"] = "augmentation"
                else:
                    event["ai_impact_type"] = "neutral"
        
        return enriched_data
    
    def create_unified_impact(self, anthropic_data, bls_data, news_data):
        """
        Calculate unified impact across data sources using consistent taxonomy
        
        Args:
            anthropic_data (dict): Anthropic Economic Index data
            bls_data (dict): BLS employment data
            news_data (dict): News event data
            
        Returns:
            dict: Unified impact metrics by occupation
        """
        logger.info("Creating unified impact model")
        
        # Get required mappings
        occupation_to_industry = self.mappings.get("occupation_to_industry", {})
        company_to_occupations = self.mappings.get("company_to_occupations", {})
        
        if not occupation_to_industry or not company_to_occupations:
            logger.error("Required mappings missing for unified impact calculation")
            return {}
        
        impact_by_occupation = {}
        
        # First, process Anthropic occupation data
        if anthropic_data and "datasets" in anthropic_data:
            occupation_automation = anthropic_data["datasets"].get("occupation_automation", {})
            
            for occupation, data in occupation_automation.items():
                impact_by_occupation[occupation] = {
                    "automation_rate": data.get("automation_rate", 0),
                    "augmentation_rate": data.get("augmentation_rate", 0),
                    "net_impact": data.get("augmentation_rate", 0) - data.get("automation_rate", 0)
                }
        
        # Next, map BLS industry data to occupations
        if bls_data and "industries" in bls_data:
            for industry, data in bls_data["industries"].items():
                # Get occupations in this industry
                industry_occupations = [
                    occ for occ, ind_data in occupation_to_industry.items() 
                    if ind_data.get("primary_industry") == industry
                ]
                
                for occupation in industry_occupations:
                    if occupation in impact_by_occupation:
                        # Add employment change as context
                        impact_by_occupation[occupation]["employment_change"] = data.get("change_percentage", 0)
                        
                        # Adjust net impact based on employment data
                        impact_by_occupation[occupation]["net_impact"] *= (1 + (data.get("change_percentage", 0) / 100))
        
        # Finally, integrate news events by occupation
        if news_data and "events" in news_data:
            for event in news_data["events"]:
                # Map company to related occupations
                company = event.get("company", "")
                if company in company_to_occupations:
                    related_occupations = list(company_to_occupations[company].keys())
                    
                    for occupation in related_occupations:
                        if occupation in impact_by_occupation:
                            # Adjust net impact based on event type
                            event_factor = 0.1  # Small effect for a single event
                            
                            event_type = event.get("event_type", "")
                            ai_relation = event.get("ai_relation", "None")
                            
                            if event_type == "hiring" and ai_relation in ["Direct", "Indirect"]:
                                impact_by_occupation[occupation]["net_impact"] += event_factor
                            elif event_type == "layoff" and ai_relation in ["Direct", "Indirect"]:
                                impact_by_occupation[occupation]["net_impact"] -= event_factor
        
        return impact_by_occupation

def main():
    """Main function to build data alignment mappings"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Build data alignment mappings')
    parser.add_argument('--mapping-dir', default='./data/mappings', help='Directory to store mapping files')
    parser.add_argument('--build-all', action='store_true', help='Build all mapping files')
    parser.add_argument('--enrich-bls', action='store_true', help='Enrich BLS data with occupation information')
    parser.add_argument('--enrich-news', action='store_true', help='Enrich news event data with occupation information')
    parser.add_argument('--unified-impact', action='store_true', help='Calculate unified impact across data sources')
    
    args = parser.parse_args()
    
    # Initialize data alignment manager
    manager = DataAlignmentManager(mapping_dir=args.mapping_dir)
    
    if args.build_all:
        # Build all mapping files
        manager.build_all_mappings()
    
    if args.enrich_bls:
        # Load the latest BLS data
        bls_data = manager._load_bls_data()
        
        if bls_data:
            # Enrich BLS data
            enriched_data = manager.enrich_bls_data(bls_data)
            
            # Write enriched data to file
            output_file = os.path.join(manager.data_dir, "enriched_employment_stats.json")
            with open(output_file, 'w') as f:
                json.dump(enriched_data, f, indent=2)
            
            logger.info(f"Wrote enriched BLS data to {output_file}")
    
    if args.enrich_news:
        # Load the latest news data
        news_data = manager._load_news_data()
        
        if news_data:
            # Enrich news data
            enriched_data = manager.enrich_news_data(news_data)
            
            # Write enriched data to file
            output_file = os.path.join(manager.data_dir, "enriched_workforce_events.json")
            with open(output_file, 'w') as f:
                json.dump(enriched_data, f, indent=2)
            
            logger.info(f"Wrote enriched news data to {output_file}")
    
    if args.unified_impact:
        # Load the latest data
        anthropic_data = manager._load_anthropic_data()
        bls_data = manager._load_bls_data()
        news_data = manager._load_news_data()
        
        if anthropic_data and bls_data and news_data:
            # Calculate unified impact
            impact_data = manager.create_unified_impact(anthropic_data, bls_data, news_data)
            
            # Write unified impact data to file
            output_file = os.path.join(manager.data_dir, "unified_impact_model.json")
            with open(output_file, 'w') as f:
                json.dump({
                    "date_calculated": datetime.now().isoformat(),
                    "impact_by_occupation": impact_data
                }, f, indent=2)
            
            logger.info(f"Wrote unified impact model to {output_file}")
    
    logger.info("Data alignment processing complete")

if __name__ == "__main__":
    main()