#!/usr/bin/env python3
# scripts/collection/collect_ai_jobs.py
"""
AI Job Postings Collector - Gathers data about AI-specific job postings for the Creation Effect calculation
"""
import json
import logging
import os
import sys
import argparse
from datetime import datetime
import requests
import re
import random
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ai_jobs_collection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("ai-jobs-collector")

class AIJobsCollector:
    """
    Collects data about AI-specific job postings from various sources.
    This supports the Creation Effect calculation in the AI Labor Market Impact model.
    """
    def __init__(self, output_dir="./data/raw/jobs", use_simulation=False):
        self.output_dir = output_dir
        self.use_simulation = use_simulation
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # List of AI-related skills to detect AI job postings
        self.AI_SKILLS = [
            "artificial intelligence", "machine learning", "deep learning", "neural networks",
            "natural language processing", "computer vision", "tensorflow", "pytorch",
            "generative ai", "large language models", "llm", "gpt", "transformers",
            "reinforcement learning", "data science", "ai ethics", "prompt engineering",
            "ml ops", "ai engineering", "computer vision", "speech recognition",
            "recommendation systems", "openai", "langchain", "ai applications"
        ]
        
        # Industry categories for job mapping
        self.INDUSTRY_CATEGORIES = {
            "Information": ["software", "it", "telecommunications", "media", "data", "information"],
            "Professional and Business Services": ["consulting", "legal", "marketing", "advertising", "professional", "business"],
            "Financial Activities": ["finance", "banking", "investment", "insurance", "real estate"],
            "Education and Health Services": ["education", "healthcare", "medical", "health", "hospital", "teaching"],
            "Manufacturing": ["manufacturing", "production", "industrial", "engineering", "automotive"],
            "Trade, Transportation, and Utilities": ["retail", "wholesale", "transportation", "logistics", "utilities"],
            "Leisure and Hospitality": ["hotel", "restaurant", "tourism", "entertainment", "food service"],
            "Construction": ["construction", "building", "architecture", "contractor"],
            "Mining and Logging": ["mining", "energy", "oil", "gas", "forestry", "logging"],
            "Other Services": ["repair", "maintenance", "personal services", "nonprofit"],
            "Government": ["government", "public sector", "federal", "state", "local government"]
        }
        
        # O*NET occupations and SOC codes for AI-related jobs
        self.AI_OCCUPATIONS = {
            "15-2051.00": "Data Scientists",
            "15-1252.00": "Software Developers",
            "15-1211.00": "Computer Systems Analysts",
            "15-1232.00": "Computer User Support Specialists",
            "15-1255.00": "Web and Digital Interface Designers",
            "15-2041.00": "Statisticians",
            "15-1299.00": "Computer Occupations, All Other",
            "15-1221.00": "Computer and Information Research Scientists",
            "15-2031.00": "Operations Research Analysts",
            "11-3021.00": "Computer and Information Systems Managers",
            "13-1161.00": "Market Research Analysts and Marketing Specialists",
            "25-1021.00": "Computer Science Teachers, Postsecondary",
            "17-2199.00": "Engineers, All Other",
            "15-2099.00": "Mathematical Science Occupations, All Other",
            "19-3022.00": "Survey Researchers"
        }

    def collect_onet_emerging_occupations(self):
        """
        Collect emerging occupation data from O*NET or simulate if not available.
        """
        logger.info("Collecting O*NET emerging occupations data")
        
        # In a real implementation, this would fetch data from O*NET API
        # For simulation, we'll create synthetic data based on known AI occupations
        
        if not self.use_simulation:
            try:
                # This would be the actual API call to O*NET
                # Replace with actual API endpoint and authentication
                onet_url = "https://services.onetcenter.org/ws/online/occupations"
                headers = {"Accept": "application/json"}
                
                # Check if API access is available by making a simple request
                test_response = requests.get(f"{onet_url}/15-2051.00", headers=headers, timeout=10)
                
                if test_response.status_code == 200:
                    logger.info("O*NET API is accessible")
                    
                    # In a real implementation, fetch emerging occupations
                    # For now, use simulation since we don't have actual API credentials
                    self.use_simulation = True
                else:
                    logger.warning(f"O*NET API returned status {test_response.status_code}")
                    self.use_simulation = True
                    
            except Exception as e:
                logger.error(f"Error accessing O*NET API: {str(e)}")
                self.use_simulation = True
        
        # Generate simulated data if needed
        if self.use_simulation:
            logger.info("Using simulated O*NET data")
            
            # Create structure for emerging occupations
            emerging_occupations = []
            
            # Add known AI occupations with growth estimates
            for soc_code, title in self.AI_OCCUPATIONS.items():
                # Generate simulated growth rate (range: 10-50%)
                growth_rate = random.uniform(0.1, 0.5)
                
                # Determine AI relevance (higher for data/ML roles)
                ai_relevance = 0.9 if any(term in title.lower() for term in ["data", "research", "computer", "developer"]) else 0.7
                
                # Generate employment estimate
                employment = int(random.uniform(5000, 100000))
                
                # Generate random key skills related to AI
                num_skills = random.randint(3, 6)
                skills = random.sample(self.AI_SKILLS, num_skills)
                
                # Map to industry sectors
                industries = []
                for industry, keywords in self.INDUSTRY_CATEGORIES.items():
                    if any(keyword in title.lower() for keyword in keywords):
                        industries.append(industry)
                
                # If no industry match, assign to Information by default
                if not industries:
                    industries = ["Information"]
                
                # Add to emerging occupations
                emerging_occupations.append({
                    "soc_code": soc_code,
                    "title": title,
                    "growth_rate": growth_rate,
                    "employment": employment,
                    "ai_relevance": ai_relevance,
                    "skills": skills,
                    "industries": industries
                })
            
            # Add a few emerging occupations not in the standard list
            emerging_titles = [
                "AI Ethics Specialist", 
                "ML Operations Engineer", 
                "Prompt Engineer",
                "AI Integration Specialist", 
                "Conversational AI Designer",
                "AI Compliance Officer",
                "Neural Network Architect",
                "Data Trust Officer",
                "Augmented Intelligence Specialist",
                "Human-AI Collaboration Coach"
            ]
            
            for title in emerging_titles:
                # Generate a synthetic SOC code
                soc_code = f"15-{random.randint(1000, 9999)}.00"
                
                # Generate simulated growth rate (higher for emerging roles: 30-80%)
                growth_rate = random.uniform(0.3, 0.8)
                
                # All emerging roles have high AI relevance
                ai_relevance = 0.95
                
                # Generate employment estimate (lower for emerging roles)
                employment = int(random.uniform(1000, 20000))
                
                # Generate random key skills related to AI
                num_skills = random.randint(4, 7)
                skills = random.sample(self.AI_SKILLS, num_skills)
                
                # Map to industry sectors
                industries = []
                for industry, keywords in self.INDUSTRY_CATEGORIES.items():
                    if any(keyword in title.lower() for keyword in keywords):
                        industries.append(industry)
                
                # If no industry match, assign to Information by default
                if not industries:
                    industries = ["Information"]
                
                # Add to emerging occupations
                emerging_occupations.append({
                    "soc_code": soc_code,
                    "title": title,
                    "growth_rate": growth_rate,
                    "employment": employment,
                    "ai_relevance": ai_relevance,
                    "skills": skills,
                    "industries": industries
                })
            
            # Return the simulated data
            return {
                "emerging_occupations": emerging_occupations,
                "is_simulated": True,
                "timestamp": datetime.now().isoformat()
            }
        
        # This would be the actual return from the API
        return None

    def collect_linkedin_job_postings(self):
        """
        Collect AI-related job posting data from LinkedIn or simulate if not available.
        """
        logger.info("Collecting LinkedIn AI job postings data")
        
        # In a real implementation, this would use the LinkedIn API
        # For simulation, we'll create synthetic data
        
        if not self.use_simulation:
            try:
                # This would be the actual API call to LinkedIn
                # LinkedIn doesn't offer a public API for job data, so would need
                # to use their Marketing Developer Platform or similar
                
                # Check if API access is available (using a dummy endpoint for illustration)
                test_response = requests.get("https://api.linkedin.com/v2/jobs", timeout=10)
                
                # Always use simulation since LinkedIn doesn't offer a public job search API
                self.use_simulation = True
                    
            except Exception as e:
                logger.error(f"Error accessing LinkedIn API: {str(e)}")
                self.use_simulation = True
        
        # Generate simulated data
        if self.use_simulation:
            logger.info("Using simulated LinkedIn job data")
            
            # Create structure for job postings
            job_postings = []
            
            # Define job categories
            job_categories = [
                "Software Development",
                "Data Science",
                "Machine Learning",
                "Artificial Intelligence",
                "Product Management",
                "Research",
                "Engineering"
            ]
            
            # Define companies (mix of tech and non-tech)
            companies = [
                {"name": "TechCorp", "industry": "Information"},
                {"name": "FinanceAI", "industry": "Financial Activities"},
                {"name": "HealthTech", "industry": "Education and Health Services"},
                {"name": "DataAnalytics", "industry": "Professional and Business Services"},
                {"name": "AIResearch", "industry": "Information"},
                {"name": "ManufacturingTech", "industry": "Manufacturing"},
                {"name": "RetailInnovation", "industry": "Trade, Transportation, and Utilities"},
                {"name": "GovTech", "industry": "Government"},
                {"name": "EnergyAI", "industry": "Mining and Logging"},
                {"name": "SmartConstruction", "industry": "Construction"}
            ]
            
            # Create AI job titles
            ai_job_titles = [
                "Machine Learning Engineer",
                "AI Research Scientist",
                "Data Scientist",
                "NLP Engineer",
                "Computer Vision Specialist",
                "AI Product Manager",
                "AI Ethics Researcher",
                "AI Solutions Architect",
                "ML Operations Engineer",
                "Conversational AI Designer",
                "AI Integration Specialist",
                "Deep Learning Engineer",
                "Reinforcement Learning Researcher",
                "AI Systems Engineer",
                "Applied AI Scientist",
                "AI Strategy Consultant",
                "AI Implementation Specialist",
                "AI Infrastructure Engineer",
                "AI Quality Assurance Engineer",
                "LLM Specialist"
            ]
            
            # Generate job posting data
            for i in range(200):  # Generate 200 simulated job postings
                # Select random company and category
                company = random.choice(companies)
                category = random.choice(job_categories)
                
                # Select an AI-related job title
                title = random.choice(ai_job_titles)
                
                # Generate required skills (3-6 AI skills)
                num_skills = random.randint(3, 6)
                required_skills = random.sample(self.AI_SKILLS, num_skills)
                
                # Generate salary range
                base_salary = random.randint(80000, 180000)
                salary_range = {
                    "min": base_salary,
                    "max": int(base_salary * random.uniform(1.1, 1.5))
                }
                
                # Generate location
                locations = ["New York", "San Francisco", "Seattle", "Boston", "Austin", 
                             "Chicago", "Los Angeles", "Denver", "Atlanta", "Washington DC",
                             "Remote"]
                location = random.choice(locations)
                
                # Add to job postings
                job_postings.append({
                    "id": f"job-{i+1}",
                    "title": title,
                    "company": company["name"],
                    "industry": company["industry"],
                    "category": category,
                    "location": location,
                    "required_skills": required_skills,
                    "salary_range": salary_range,
                    "posted_date": (datetime.now().replace(
                        day=random.randint(1, 28),
                        hour=random.randint(0, 23),
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )).isoformat(),
                    "is_ai_related": True
                })
            
            # Generate industry distribution
            industry_distribution = {}
            for posting in job_postings:
                industry = posting["industry"]
                if industry not in industry_distribution:
                    industry_distribution[industry] = 0
                industry_distribution[industry] += 1
            
            # Convert to percentages
            total_postings = len(job_postings)
            for industry in industry_distribution:
                industry_distribution[industry] = round(industry_distribution[industry] / total_postings * 100, 2)
            
            # Return the simulated data
            return {
                "job_postings": job_postings,
                "industry_distribution": industry_distribution,
                "total_postings": total_postings,
                "ai_job_percentage": 100.0,  # All are AI jobs in this simulation
                "is_simulated": True,
                "timestamp": datetime.now().isoformat()
            }
        
        # This would be the actual return from the API
        return None

    def validate_ai_job_data(self, job_data):
        """
        Validate that job postings contain AI-specific skills.
        """
        if not job_data or "job_postings" not in job_data:
            return None
        
        validated_postings = []
        
        for posting in job_data["job_postings"]:
            # Check if job title contains AI keywords
            title_has_ai = any(skill.lower() in posting["title"].lower() for skill in self.AI_SKILLS)
            
            # Check if required skills include AI skills
            skills_have_ai = False
            if "required_skills" in posting:
                skills_have_ai = any(
                    any(ai_skill.lower() in skill.lower() for ai_skill in self.AI_SKILLS)
                    for skill in posting["required_skills"]
                )
            
            # Validate as AI job if either condition is met
            if title_has_ai or skills_have_ai:
                # Mark as validated AI job
                posting["is_validated_ai_job"] = True
                validated_postings.append(posting)
            else:
                # Not an AI job
                posting["is_validated_ai_job"] = False
        
        # Update job data with validation results
        job_data["validated_ai_postings"] = validated_postings
        job_data["validated_ai_count"] = len(validated_postings)
        
        if job_data["total_postings"] > 0:
            job_data["validated_ai_percentage"] = round(len(validated_postings) / job_data["total_postings"] * 100, 2)
        else:
            job_data["validated_ai_percentage"] = 0
        
        return job_data

    def collect_data(self, year=None, month=None):
        """
        Collect AI job data from all sources.
        """
        # Use provided date or current date for filename formatting
        if year and month:
            date_str = f"{year}{month:02d}"
        else:
            date_str = datetime.now().strftime('%Y%m%d')
        
        # Create output file paths
        onet_output_file = os.path.join(self.output_dir, f"ai_emerging_occupations_{date_str}.json")
        linkedin_output_file = os.path.join(self.output_dir, f"ai_job_postings_{date_str}.json")
        combined_output_file = os.path.join(self.output_dir, f"ai_jobs_combined_{date_str}.json")
        
        # Collect data from each source
        onet_data = self.collect_onet_emerging_occupations()
        linkedin_data = self.collect_linkedin_job_postings()
        
        # Validate LinkedIn data
        if linkedin_data:
            linkedin_data = self.validate_ai_job_data(linkedin_data)
        
        # Save individual data files
        if onet_data:
            with open(onet_output_file, 'w') as f:
                json.dump(onet_data, f, indent=2)
            logger.info(f"Saved O*NET data to {onet_output_file}")
        
        if linkedin_data:
            with open(linkedin_output_file, 'w') as f:
                json.dump(linkedin_data, f, indent=2)
            logger.info(f"Saved LinkedIn data to {linkedin_output_file}")
        
        # Combine data for final output
        combined_data = {
            "date_collected": datetime.now().isoformat(),
            "target_period": f"{year}-{month:02d}" if year and month else None,
            "is_simulated": self.use_simulation,
            "sources": {
                "onet": onet_data is not None,
                "linkedin": linkedin_data is not None
            }
        }
        
        # Extract and combine emerging occupations
        if onet_data and "emerging_occupations" in onet_data:
            combined_data["emerging_occupations"] = onet_data["emerging_occupations"]
        
        # Extract and combine job postings
        if linkedin_data and "validated_ai_postings" in linkedin_data:
            combined_data["job_postings"] = linkedin_data["validated_ai_postings"]
            combined_data["industry_distribution"] = linkedin_data.get("industry_distribution", {})
            combined_data["total_ai_postings"] = linkedin_data.get("validated_ai_count", 0)
        
        # Calculate overall metrics
        combined_data["ai_job_metrics"] = self.calculate_job_metrics(combined_data)
        
        # Save combined data
        with open(combined_output_file, 'w') as f:
            json.dump(combined_data, f, indent=2)
        logger.info(f"Saved combined AI jobs data to {combined_output_file}")
        
        return {
            "files_created": [onet_output_file, linkedin_output_file, combined_output_file],
            "is_simulated": self.use_simulation,
            "combined_file": combined_output_file
        }

    def calculate_job_metrics(self, combined_data):
        """
        Calculate overall metrics from the combined job data.
        """
        metrics = {
            "total_emerging_occupations": 0,
            "total_job_postings": 0,
            "average_growth_rate": 0,
            "top_industries": [],
            "top_skills": [],
            "top_job_titles": []
        }
        
        # Process emerging occupations
        if "emerging_occupations" in combined_data:
            occupations = combined_data["emerging_occupations"]
            metrics["total_emerging_occupations"] = len(occupations)
            
            # Calculate average growth rate
            if occupations:
                growth_rates = [occ.get("growth_rate", 0) for occ in occupations]
                metrics["average_growth_rate"] = sum(growth_rates) / len(growth_rates)
            
            # Count industries
            industry_counts = {}
            for occ in occupations:
                for industry in occ.get("industries", []):
                    if industry not in industry_counts:
                        industry_counts[industry] = 0
                    industry_counts[industry] += 1
            
            # Get top industries
            top_industries = sorted(industry_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            metrics["top_industries"] = [{"industry": ind, "count": count} for ind, count in top_industries]
            
            # Count skills
            skill_counts = {}
            for occ in occupations:
                for skill in occ.get("skills", []):
                    if skill not in skill_counts:
                        skill_counts[skill] = 0
                    skill_counts[skill] += 1
            
            # Get top skills
            top_skills = sorted(skill_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            metrics["top_skills"] = [{"skill": skill, "count": count} for skill, count in top_skills]
        
        # Process job postings
        if "job_postings" in combined_data:
            postings = combined_data["job_postings"]
            metrics["total_job_postings"] = len(postings)
            
            # Count job titles
            title_counts = {}
            for posting in postings:
                title = posting.get("title", "Unknown")
                if title not in title_counts:
                    title_counts[title] = 0
                title_counts[title] += 1
            
            # Get top job titles
            top_titles = sorted(title_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            metrics["top_job_titles"] = [{"title": title, "count": count} for title, count in top_titles]
        
        return metrics


def main():
    """Main function to run the AI jobs data collection"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect AI job postings data')
    parser.add_argument('--year', type=int, required=True, help='Target year')
    parser.add_argument('--month', type=int, required=True, help='Target month (1-12)')
    parser.add_argument('--output', type=str, help='Output directory', default="./data/raw/jobs")
    parser.add_argument('--simulate', action='store_true', help='Use simulated data instead of API calls')
    
    args = parser.parse_args()
    
    # Validate inputs
    if args.month < 1 or args.month > 12:
        logger.error(f"Invalid month: {args.month}. Month must be between 1 and 12.")
        return 1
    
    # Validate that we're not trying to collect future data
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    
    if (args.year > current_year) or (args.year == current_year and args.month > current_month):
        logger.error(f"Error: Cannot collect future data for {args.year}-{args.month:02d}")
        logger.error(f"Current date is {current_year}-{current_month:02d}")
        logger.error("Exiting without collecting data")
        return 1
    
    # Initialize collector
    collector = AIJobsCollector(
        output_dir=args.output,
        use_simulation=args.simulate
    )
    
    # Run collection
    logger.info(f"Collecting AI jobs data for {args.year}-{args.month:02d}")
    results = collector.collect_data(year=args.year, month=args.month)
    
    logger.info(f"Collection complete. Created {len(results['files_created'])} files.")
    logger.info(f"Simulation mode: {results['is_simulated']}")
    
    # Return success
    return 0


if __name__ == "__main__":
    sys.exit(main())