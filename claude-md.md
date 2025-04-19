AI Labor Market Dashboard - Fixing the Anthropic Economic Index Integration
This document outlines the changes needed to properly integrate the Anthropic Economic Index data into your AI Labor Market Dashboard.
1. GitHub Actions Workflow Update
File: .github/workflows/update_ai_labor_index.yml

Update your GitHub workflow to include the Anthropic Economic Index data collection step:

```yaml
# .github/workflows/update_ai_labor_index.yml
name: Update AI Labor Market Index

on:
  schedule:
    # Run weekly on Sunday at 2 AM UTC
    - cron: '0 2 * * 0'
  workflow_dispatch:  # Allow manual triggering

jobs:
  update_index:
    runs-on: ubuntu-latest
    
    steps:
      # Existing steps remain unchanged
      - name: Checkout repository
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests beautifulsoup4 pandas numpy nltk huggingface_hub

      - name: Download NLTK data
        run: |
          python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"
      
      - name: Create directories
        run: |
          mkdir -p data/raw/arxiv
          mkdir -p data/raw/bls
          mkdir -p data/raw/jobs
          mkdir -p data/raw/news
          mkdir -p data/raw/anthropic_index
          mkdir -p data/processed
      
      - name: Get previous month
        id: date
        run: |
          echo "year=$(date -d 'last month' +'%Y')" >> $GITHUB_OUTPUT
          echo "month=$(date -d 'last month' +'%-m')" >> $GITHUB_OUTPUT
      
      # Add new step to collect Anthropic Economic Index data
      - name: Collect Anthropic Economic Index data
        run: python scripts/collection/collect_anthropic_index.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
      
      # Continue with existing steps
      - name: Collect ArXiv data
        run: python scripts/collection/collect_arxiv.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
      
      # ... other collection steps ...
      
      - name: Process data
        run: |
          python scripts/processing/process_anthropic_index.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_research.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_employment.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_jobs.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_news.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
      
      # ... remaining steps ...

2. Create Anthropic Index Processing Script
File: scripts/processing/process_anthropic_index.py

# scripts/processing/process_anthropic_index.py
import json
import logging
import os
import sys
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("anthropic_index_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("anthropic-index-processor")

class AnthropicIndexProcessor:
    def __init__(self, input_dir="./data/raw/anthropic_index", output_dir="./data/processed"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
    def process_data(self, year=None, month=None):
        """Process Anthropic Economic Index data and save to output directory"""
        # Generate date string for filenames
        if year and month:
            date_str = f"{year}{month:02d}"
        else:
            date_str = datetime.now().strftime('%Y%m%d')
            
        # Find the latest anthropic index combined file
        combined_files = [f for f in os.listdir(self.input_dir) if f.startswith("anthropic_index_") and f.endswith("_combined.json")]
        
        if not combined_files:
            logger.warning("No Anthropic Economic Index combined files found")
            return None
            
        # Sort by date (assuming the format anthropic_index_YYYYMMDD_combined.json)
        combined_files.sort(reverse=True)
        latest_file = combined_files[0]
        
        logger.info(f"Processing Anthropic Economic Index data from {latest_file}")
        
        try:
            # Load the combined file
            with open(os.path.join(self.input_dir, latest_file), 'r') as f:
                anthropic_data = json.load(f)
                
            # Extract and process the data
            processed_data = {
                "date_analyzed": datetime.now().isoformat(),
                "source": "Anthropic Economic Index",
                "is_simulated_data": anthropic_data.get("is_simulated_data", False)
            }
            
            # Extract statistics
            statistics = {}
            
            # Calculate automation and augmentation rates
            if "datasets" in anthropic_data and "occupation_automation" in anthropic_data["datasets"]:
                occupations = anthropic_data["datasets"]["occupation_automation"]
                automation_rates = []
                augmentation_rates = []
                
                for occupation, data in occupations.items():
                    auto_rate = data.get("automation_rate")
                    aug_rate = data.get("augmentation_rate")
                    
                    if auto_rate is not None:
                        automation_rates.append(auto_rate)
                    if aug_rate is not None:
                        augmentation_rates.append(aug_rate)
                
                # Calculate averages
                if automation_rates:
                    avg_automation = sum(automation_rates) / len(automation_rates)
                    statistics["average_automation_rate"] = avg_automation
                
                if augmentation_rates:
                    avg_augmentation = sum(augmentation_rates) / len(augmentation_rates)
                    statistics["average_augmentation_rate"] = avg_augmentation
                
                if automation_rates and augmentation_rates:
                    ratio = avg_automation / avg_augmentation if avg_augmentation > 0 else 1.0
                    statistics["automation_augmentation_ratio"] = ratio
                
                statistics["total_occupations_analyzed"] = len(occupations)
            
            processed_data["statistics"] = statistics
            
            # Extract categories from occupation_categories
            if "datasets" in anthropic_data and "occupation_categories" in anthropic_data["datasets"]:
                categories = anthropic_data["datasets"]["occupation_categories"]
                category_data = []
                
                for category_name, category_info in categories.items():
                    category_data.append({
                        "category": category_name,
                        "count": category_info.get("count", 0)
                    })
                
                processed_data["categories"] = {"data": category_data}
            
            # Extract top augmented roles
            if "datasets" in anthropic_data and "occupation_automation" in anthropic_data["datasets"]:
                occupations = anthropic_data["datasets"]["occupation_automation"]
                
                # Sort by augmentation rate (descending)
                top_augmented = []
                for occupation, data in occupations.items():
                    aug_rate = data.get("augmentation_rate")
                    if aug_rate is not None:
                        top_augmented.append({"title": occupation, "augmentation_rate": aug_rate})
                
                top_augmented.sort(key=lambda x: x["augmentation_rate"], reverse=True)
                processed_data["top_augmented_roles"] = top_augmented[:10]  # Keep top 10
                
                # Sort by automation rate (descending)
                top_automated = []
                for occupation, data in occupations.items():
                    auto_rate = data.get("automation_rate")
                    if auto_rate is not None:
                        top_automated.append({"title": occupation, "automation_rate": auto_rate})
                
                top_automated.sort(key=lambda x: x["automation_rate"], reverse=True)
                processed_data["top_automated_roles"] = top_automated[:10]  # Keep top 10
            
            # Extract top tasks
            if "datasets" in anthropic_data and "task_usage" in anthropic_data["datasets"]:
                tasks = anthropic_data["datasets"]["task_usage"]
                
                top_tasks = []
                for task, data in tasks.items():
                    count = data.get("count", 0)
                    top_tasks.append({"task": task, "count": count})
                
                top_tasks.sort(key=lambda x: x["count"], reverse=True)
                processed_data["top_tasks"] = top_tasks[:15]  # Keep top 15
            
            # Extract top skills
            if "datasets" in anthropic_data and "skill_presence" in anthropic_data["datasets"]:
                skills = anthropic_data["datasets"]["skill_presence"]
                
                top_skills = []
                for skill, data in skills.items():
                    count = data.get("count", 0)
                    top_skills.append({"skill": skill, "count": count})
                
                top_skills.sort(key=lambda x: x["count"], reverse=True)
                processed_data["top_skills"] = top_skills[:15]  # Keep top 15
            
            # Save the processed data
            output_file = f"job_trends_{date_str}.json"
            output_path = os.path.join(self.output_dir, output_file)
            
            # Save as job_trends file
            with open(output_path, 'w') as f:
                json.dump(processed_data, f, indent=2)
                
            logger.info(f"Saved processed Anthropic Economic Index data to {output_path}")
            
            # Also save with specific date in filename for historical reference
            specific_date = datetime.now().strftime('%Y%m%d')
            specific_file = f"job_trends_{specific_date}.json"
            specific_path = os.path.join(self.output_dir, specific_file)
            
            with open(specific_path, 'w') as f:
                json.dump(processed_data, f, indent=2)
                
            logger.info(f"Saved processed Anthropic Economic Index data to {specific_path}")
            
            return output_path
                
        except Exception as e:
            logger.error(f"Error processing Anthropic Economic Index data: {str(e)}")
            return None
            

def main():
    parser = argparse.ArgumentParser(description='Process Anthropic Economic Index data')
    parser.add_argument('--input-dir', default='./data/raw/anthropic_index', help='Input directory containing raw data')
    parser.add_argument('--output-dir', default='./data/processed', help='Output directory for processed data')
    parser.add_argument('--year', type=int, help='Year to process data for (optional)')
    parser.add_argument('--month', type=int, help='Month to process data for (optional)')
    
    args = parser.parse_args()
    
    processor = AnthropicIndexProcessor(
        input_dir=args.input_dir,
        output_dir=args.output_dir
    )
    
    result = processor.process_data(year=args.year, month=args.month)
    
    if result:
        logger.info("Processing complete.")
        sys.exit(0)
    else:
        logger.error("Processing failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()


3. Update Index Calculation to Better Handle Anthropic Data
File: scripts/analysis/calculate_index.py
Update the section that handles Anthropic Economic Index data to ensure it's correctly integrated into the main index:

# Update in the calculate_job_trends_score function

def calculate_job_trends_score(self, job_data):
    """Calculate score from job postings component."""
    if not job_data:
        logger.warning("No job trends data available")
        return 0, {"error": "No data available"}
    
    # Check if this is Anthropic Economic Index data
    if job_data.get("source") == "Anthropic Economic Index":
        score, details = self.calculate_anthropic_index_score(job_data)
        
        # Important: Preserve the original Anthropic data structure in the details
        details["top_augmented_roles"] = job_data.get("top_augmented_roles", [])
        details["top_automated_roles"] = job_data.get("top_automated_roles", [])
        details["source"] = "Anthropic Economic Index"
        
        return score, details
    
    # Original logic for Remotive API data
    # (rest of the function remains unchanged)

4. Make Dashboard Component More Robust
Make a small update to the AILaborMarketDashboard component to ensure it correctly detects Anthropic data:

// In the useEffect that processes data
useEffect(() => {
  if (!indexData) return;
  
  // Process job roles data
  console.log("Processing job roles data...");
  
  // Add better debug logging
  console.log("Index data job_trends details:", indexData.components?.job_trends?.details);
  
  // Check if the job_trends details contains the Anthropic source information
  const isAnthropicSource = indexData.components?.job_trends?.details?.source === "Anthropic Economic Index";
  const hasAnthropicData = indexData.components?.job_trends?.details?.top_augmented_roles && 
                          indexData.components?.job_trends?.details?.top_automated_roles;
  
  console.log("Is Anthropic source:", isAnthropicSource);
  console.log("Has Anthropic data structure:", hasAnthropicData);
  
  // Process roles data for split bar visualization (Anthropic Economic Index data)
  let processedRoles = [];
  let usingDerivedProcessedRoles = false;
  
  if (isAnthropicSource || hasAnthropicData) {
    const jobData = indexData.components?.job_trends?.details;
    processedRoles = processRolesDataForSplitBars(jobData);
    usingDerivedProcessedRoles = false;
    console.log("Using Anthropic Economic Index data for roles visualization:", processedRoles);
  }
  
  // Rest of the effect function remains unchanged
}, [indexData]);

5. Manual Testing Steps
After implementing the above changes, follow these steps to test:

Manually trigger the GitHub Actions workflow:

Go to your repository on GitHub
Navigate to "Actions" tab
Find the "Update AI Labor Market Index" workflow
Click "Run workflow" and select the branch to run on


Check the job logs:

Monitor the execution of each step
Verify that the Anthropic Economic Index data collection step runs successfully
Check for any errors in the processing steps


Examine the generated files:

After the workflow completes, check the data/processed/ directory
Verify that a job_trends_YYYYMMDD.json file has been created with Anthropic Economic Index data
Check that the main ai_labor_index_latest.json file contains the Anthropic data in the job_trends component


Check your dashboard:

Deploy the updated site (or run it locally)
Open the AI Labor Market Dashboard
Navigate to the "Job Trends" tab
Verify that the "Automation vs. Augmentation Analysis" shows actual data and not the "derived data" message

6. Enhancing the GitHub Actions Workflow with Dependencies
Make sure the workflow has all needed dependencies:

- name: Install dependencies
  run: |
    python -m pip install --upgrade pip
    pip install requests beautifulsoup4 pandas numpy nltk
    pip install huggingface_hub  # Add this for Anthropic Index

This comprehensive set of changes should address the main issue and ensure your dashboard displays actual data from the Anthropic Economic Index instead of derived data.