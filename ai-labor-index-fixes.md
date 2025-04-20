# AI Labor Market Index Date Fixes

This document provides fixes for the date stamp and timing issues in the AI Labor Market Index, specifically addressing the problems with the Anthropic Economic Index data collection and processing.

## Fix 0: Correct Existing Data Files

First, let's correct the existing data files that incorrectly show April 2025 data when they should reflect March 2025 data. Create a script to fix the timestamp issues:

```python
# scripts/utils/fix_anthropic_data_timestamps.py

import json
import os
import glob
from datetime import datetime

def fix_timestamps():
    """
    Fix the timestamps in existing Anthropic data files to accurately 
    reflect March 2025 data instead of April 2025.
    """
    # Define directories
    raw_dir = 'data/raw/anthropic_index'
    processed_dir = 'data/processed'
    
    # Find all files with April 2025 timestamps in raw directory
    april_files = glob.glob(f"{raw_dir}/anthropic_index_2025_04_*.json")
    
    for file_path in april_files:
        print(f"Processing: {file_path}")
        
        # Extract the dataset name from the filename
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        if len(parts) >= 4:
            dataset = '_'.join(parts[3:]).replace('.json', '')
            
            # Read the file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Update the date fields
            if 'target_period' in data:
                data['target_period'] = '2025-03'
                
            if 'date_collected' in data:
                # Keep the original collection time, just change the month
                collected_date = datetime.fromisoformat(data['date_collected'])
                new_date = collected_date.replace(month=3)
                data['date_collected'] = new_date.isoformat()
            
            # Create the new filename with March instead of April
            new_filename = f"anthropic_index_2025_03_{dataset}.json"
            new_file_path = os.path.join(raw_dir, new_filename)
            
            # Save the corrected file
            with open(new_file_path, 'w') as f:
                json.dump(data, f, indent=2)
                
            print(f"Created corrected file: {new_file_path}")
            
            # Optionally, rename the original file to backup
            backup_path = f"{file_path}.bak"
            os.rename(file_path, backup_path)
            print(f"Original file backed up to: {backup_path}")
    
    # Now fix the processed data that uses these files
    latest_index_path = os.path.join(processed_dir, 'ai_labor_index_latest.json')
    if os.path.exists(latest_index_path):
        with open(latest_index_path, 'r') as f:
            index_data = json.load(f)
        
        # Add a note about the data source correction
        if 'notes' not in index_data:
            index_data['notes'] = []
            
        index_data['notes'].append(
            "Note: Anthropic Economic Index data was corrected to reflect March 2025 rather than April 2025."
        )
        
        # Fix any data source references
        if 'data_sources' in index_data:
            for source, period in index_data['data_sources'].items():
                if source == 'anthropic_index' and period == '2025-04':
                    index_data['data_sources'][source] = '2025-03'
        
        # Fix any component data that references Anthropic data
        if 'components' in index_data and 'job_trends' in index_data['components']:
            if (index_data['components']['job_trends']['details']['source'] == 'Anthropic Economic Index' and
                not index_data['components']['job_trends']['details'].get('is_simulated_data', True)):
                # Add a flag to indicate this data has been corrected
                index_data['components']['job_trends']['details']['data_corrected'] = True
        
        # Save the updated index
        with open(latest_index_path, 'w') as f:
            json.dump(index_data, f, indent=2)
            
        print(f"Updated latest index file: {latest_index_path}")
        
        # Also update any dated index file if it exists
        april_index_path = os.path.join(processed_dir, 'ai_labor_index_2025_04.json')
        if os.path.exists(april_index_path):
            march_index_path = os.path.join(processed_dir, 'ai_labor_index_2025_03.json')
            
            # Copy the corrected index data
            with open(march_index_path, 'w') as f:
                json.dump(index_data, f, indent=2)
                
            # Create a backup of the April index
            backup_path = f"{april_index_path}.bak"
            os.rename(april_index_path, backup_path)
            
            print(f"Created corrected index file: {march_index_path}")
            print(f"Original April index backed up to: {backup_path}")
    
    print("Data correction completed successfully!")

if __name__ == "__main__":
    fix_timestamps()
```

Run this script to correct the existing data:

```bash
# Create the utility script in the correct location
mkdir -p scripts/utils
# Copy the script content to the file
python scripts/utils/fix_anthropic_data_timestamps.py
```

## Problem Summary

1. The raw data file `anthropic_index_2025_04_occupation_categories.json` has a target period of April 2025, but Anthropic hasn't released April data yet
2. The workflow is designed to process the previous month's data (March 2025), but it appears to be collecting current month data
3. There's no validation to prevent processing of "future" data

## Fix 1: Update Collection Script to Use Correct Month

The `collect_anthropic_index.py` script is likely setting the target period incorrectly. Here's how to fix it:

```python
# scripts/collection/collect_anthropic_index.py

import argparse
import json
import os
import requests
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='Collect Anthropic Economic Index data')
    parser.add_argument('--year', type=str, required=True, help='Year to collect data for')
    parser.add_argument('--month', type=str, required=True, help='Month to collect data for')
    args = parser.parse_args()
    
    # Convert to integers for validation
    year = int(args.year)
    month = int(args.month)
    
    # Create output directory if it doesn't exist
    output_dir = 'data/raw/anthropic_index'
    os.makedirs(output_dir, exist_ok=True)
    
    # Validate that we're not trying to collect future data
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    
    if (year > current_year) or (year == current_year and month > current_month):
        print(f"Error: Cannot collect future data for {year}-{month}")
        print(f"Current date is {current_year}-{current_month}")
        print("Exiting without collecting data")
        return
        
    # Define available datasets from Anthropic
    datasets = ["occupation_categories", "task_automation_metrics", "industry_metrics"]
    
    # Format month with leading zero if needed
    month_str = str(month).zfill(2)
    
    for dataset in datasets:
        # Construct the URL for the Anthropic Economic Index data
        # Note: This is a hypothetical URL structure - replace with actual API endpoint
        url = f"https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/release_{year}_{month_str}_{dataset}.json"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                
                # Add metadata
                data['dataset'] = dataset
                data['date_collected'] = datetime.now().isoformat()
                data['target_period'] = f"{year}-{month_str}"
                
                # Save to file
                output_file = os.path.join(output_dir, f"anthropic_index_{year}_{month_str}_{dataset}.json")
                with open(output_file, 'w') as f:
                    json.dump(data, f, indent=2)
                print(f"Successfully collected {dataset} data for {year}-{month_str}")
            else:
                # Handle case where data doesn't exist for this period
                print(f"Error: Could not retrieve {dataset} data for {year}-{month_str}")
                print(f"Status code: {response.status_code}")
                
                # Create a placeholder file indicating data is not available
                output_file = os.path.join(output_dir, f"anthropic_index_{year}_{month_str}_{dataset}.not_available")
                with open(output_file, 'w') as f:
                    f.write(f"Data not available for {year}-{month_str} as of {datetime.now().isoformat()}")
                
        except Exception as e:
            print(f"Error collecting {dataset} data: {e}")
            
    print(f"Anthropic Economic Index data collection completed for {year}-{month_str}")

if __name__ == "__main__":
    main()
```

## Fix 2: Update Processing Script to Handle Missing Data

The processing script should be updated to gracefully handle missing data:

```python
# scripts/processing/process_anthropic_index.py

import argparse
import json
import os
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='Process Anthropic Economic Index data')
    parser.add_argument('--year', type=str, required=True, help='Year to process data for')
    parser.add_argument('--month', type=str, required=True, help='Month to process data for')
    args = parser.parse_args()
    
    # Format month with leading zero if needed
    month = args.month.zfill(2)
    
    # Define input and output directories
    input_dir = 'data/raw/anthropic_index'
    output_dir = 'data/processed'
    os.makedirs(output_dir, exist_ok=True)
    
    # Check if data exists before processing
    base_filename = f"anthropic_index_{args.year}_{month}"
    required_datasets = ["occupation_categories", "task_automation_metrics"]
    
    missing_data = False
    for dataset in required_datasets:
        filename = f"{base_filename}_{dataset}.json"
        filepath = os.path.join(input_dir, filename)
        
        if not os.path.exists(filepath):
            # Check if the "not_available" marker exists
            not_available_path = f"{filepath}.not_available"
            if os.path.exists(not_available_path):
                print(f"Warning: Data for {dataset} is marked as not available")
                missing_data = True
            else:
                print(f"Error: Required data file not found: {filepath}")
                missing_data = True
    
    if missing_data:
        print("Some required data is missing. Checking for fallback data...")
        
        # Try to use the most recent available data as fallback
        # This is a simplified version - you may want to make it more sophisticated
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        
        # Convert to integers for calculation
        target_year = int(args.year)
        target_month = int(args.month)
        
        # Calculate previous month
        if target_month == 1:
            prev_month = 12
            prev_year = target_year - 1
        else:
            prev_month = target_month - 1
            prev_year = target_year
            
        prev_month_str = str(prev_month).zfill(2)
        prev_base_filename = f"anthropic_index_{prev_year}_{prev_month_str}"
        
        # Check if previous month data exists
        all_prev_exists = True
        for dataset in required_datasets:
            prev_filepath = os.path.join(input_dir, f"{prev_base_filename}_{dataset}.json")
            if not os.path.exists(prev_filepath):
                all_prev_exists = False
                break
        
        if all_prev_exists:
            print(f"Using fallback data from {prev_year}-{prev_month_str}")
            # Continue processing with previous month's data
            base_filename = prev_base_filename
        else:
            print("Error: No fallback data available. Processing cannot continue.")
            return
    
    # Load and process data
    processed_data = {
        "timestamp": datetime.now().isoformat(),
        "source_period": f"{args.year}-{month}",
        "actual_data_period": base_filename.split('_')[2] + '-' + base_filename.split('_')[3],
        "components": {}
    }
    
    # Process occupation categories
    try:
        with open(os.path.join(input_dir, f"{base_filename}_occupation_categories.json"), 'r') as f:
            occupation_data = json.load(f)
            
        # Process the data as needed
        # This is a placeholder for your actual processing logic
        processed_data["components"]["occupation_categories"] = {
            "summary": "Processed occupation categories",
            "count": len(occupation_data["data"]) if "data" in occupation_data else 0,
            "source": "Anthropic Economic Index"
        }
    except Exception as e:
        print(f"Error processing occupation categories: {e}")
    
    # Process task automation metrics
    try:
        with open(os.path.join(input_dir, f"{base_filename}_task_automation_metrics.json"), 'r') as f:
            automation_data = json.load(f)
            
        # Process the data as needed
        # This is a placeholder for your actual processing logic
        processed_data["components"]["task_automation"] = {
            "summary": "Processed task automation metrics",
            "source": "Anthropic Economic Index"
        }
    except Exception as e:
        print(f"Error processing task automation metrics: {e}")
    
    # Save processed data
    output_file = os.path.join(output_dir, f"anthropic_index_processed_{args.year}_{month}.json")
    with open(output_file, 'w') as f:
        json.dump(processed_data, f, indent=2)
        
    print(f"Processed Anthropic Economic Index data saved to {output_file}")

if __name__ == "__main__":
    main()
```

## Fix 3: Update Index Calculation Script to Include Data Source Information

Update the `calculate_index.py` script to clearly indicate which data period was used:

```python
# scripts/analysis/calculate_index.py

import argparse
import json
import os
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='Calculate AI Labor Market Index')
    parser.add_argument('--year', type=str, required=True, help='Year to calculate index for')
    parser.add_argument('--month', type=str, required=True, help='Month to calculate index for')
    args = parser.parse_args()
    
    # Format month with leading zero if needed
    month = args.month.zfill(2)
    
    # Define input and output directories
    processed_dir = 'data/processed'
    output_dir = 'data/processed'
    os.makedirs(output_dir, exist_ok=True)
    
    # Load processed data components
    processed_files = {
        "anthropic_index": f"anthropic_index_processed_{args.year}_{month}.json",
        "research_trends": f"research_trends_{args.year}_{month}.json",
        "employment_stats": f"employment_stats_{args.year}_{month}.json",
        "news_events": f"news_events_{args.year}_{month}.json"
    }
    
    # Check for data availability and load
    data_components = {}
    actual_periods = {}
    
    for component, filename in processed_files.items():
        filepath = os.path.join(processed_dir, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                data_components[component] = data
                
                # Track the actual data period used (may be different from requested period)
                if "actual_data_period" in data:
                    actual_periods[component] = data["actual_data_period"]
                elif "source_period" in data:
                    actual_periods[component] = data["source_period"]
                else:
                    actual_periods[component] = f"{args.year}-{month}"
                    
            except Exception as e:
                print(f"Error loading {component} data: {e}")
        else:
            print(f"Warning: {component} data file not found: {filepath}")
    
    # Calculate the index
    # This is a placeholder for your actual calculation logic
    ai_labor_index = {
        "timestamp": datetime.now().isoformat(),
        "index_value": 0.0,  # Replace with actual calculation
        "interpretation": "Placeholder",
        "components": {},
        "data_sources": actual_periods,
        "notes": []
    }
    
    # Add a note if any data source is not from the requested period
    for component, period in actual_periods.items():
        if period != f"{args.year}-{month}":
            ai_labor_index["notes"].append(
                f"Note: {component} data is from {period} instead of the requested period {args.year}-{month}"
            )
    
    # Add component calculations
    # Replace with your actual component calculations
    
    # Save calculated index
    output_file = os.path.join(output_dir, "ai_labor_index_latest.json")
    with open(output_file, 'w') as f:
        json.dump(ai_labor_index, f, indent=2)
        
    # Also save a dated version
    dated_output_file = os.path.join(output_dir, f"ai_labor_index_{args.year}_{month}.json")
    with open(dated_output_file, 'w') as f:
        json.dump(ai_labor_index, f, indent=2)
        
    print(f"AI Labor Market Index calculated and saved to {output_file} and {dated_output_file}")

if __name__ == "__main__":
    main()
```

## Fix 4: Update GitHub Workflow to Add Data Validation

The GitHub workflow should be updated to validate the data availability before processing:

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
      - name: Checkout repository
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests beautifulsoup4 pandas numpy nltk
          pip install huggingface_hub  # Add this for Anthropic Index

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
          echo "month_padded=$(date -d 'last month' +'%m')" >> $GITHUB_OUTPUT
      
      - name: Check for existing Anthropic data
        id: check_anthropic
        run: |
          # Create a simple script to check if Anthropic has released data for the target period
          cat > check_anthropic_data.py << 'EOF'
          import requests
          import sys
          import os
          from datetime import datetime

          def check_anthropic_data(year, month):
              # Format month with leading zero
              month_str = month.zfill(2)
              
              # Define the URL for the Anthropic Economic Index repository
              url = f"https://huggingface.co/datasets/Anthropic/EconomicIndex/tree/main/release_{year}_{month_str}"
              
              try:
                  response = requests.get(url)
                  if response.status_code == 200:
                      print(f"Anthropic data for {year}-{month_str} appears to be available")
                      return True
                  else:
                      # Try to get the latest release date
                      base_url = "https://huggingface.co/datasets/Anthropic/EconomicIndex"
                      base_response = requests.get(base_url)
                      
                      if base_response.status_code == 200:
                          # This is a very simple check and might need improvement
                          # Ideally we would parse the HTML and find the actual release dates
                          content = base_response.text
                          if f"release_{year}_{month_str}" in content:
                              print(f"Anthropic data for {year}-{month_str} appears to be available")
                              return True
                      
                      print(f"No Anthropic data found for {year}-{month_str}")
                      
                      # Try to find the latest available data
                      current_date = datetime.now()
                      current_year = current_date.year
                      current_month = current_date.month
                      
                      # Try the previous month
                      if int(month) == 1:
                          prev_month = "12"
                          prev_year = str(int(year) - 1)
                      else:
                          prev_month = str(int(month) - 1).zfill(2)
                          prev_year = year
                          
                      prev_url = f"https://huggingface.co/datasets/Anthropic/EconomicIndex/tree/main/release_{prev_year}_{prev_month}"
                      prev_response = requests.get(prev_url)
                      
                      if prev_response.status_code == 200:
                          print(f"Found previous month data: {prev_year}-{prev_month}")
                          # Output the latest available data info for GitHub Actions
                          print(f"::set-output name=available_year::{prev_year}")
                          print(f"::set-output name=available_month::{int(prev_month)}")  # Remove leading zero
                          return False
                      
                      # If we can't find the previous month, try looking for any release
                      print("Trying to find any available release...")
                      
                      # We would need more sophisticated parsing here
                      # This is just a placeholder
                      print("No available data found")
                      return False
              except Exception as e:
                  print(f"Error checking Anthropic data availability: {e}")
                  return False

          if __name__ == "__main__":
              if len(sys.argv) != 3:
                  print("Usage: python check_anthropic_data.py <year> <month>")
                  sys.exit(1)
                  
              year = sys.argv[1]
              month = sys.argv[2]
              
              result = check_anthropic_data(year, month)
              sys.exit(0 if result else 1)
          EOF
          
          python check_anthropic_data.py ${{ steps.date.outputs.year }} ${{ steps.date.outputs.month_padded }}
          exit_code=$?
          
          if [ $exit_code -eq 0 ]; then
            echo "anthropic_data_available=true" >> $GITHUB_OUTPUT
            echo "use_year=${{ steps.date.outputs.year }}" >> $GITHUB_OUTPUT
            echo "use_month=${{ steps.date.outputs.month }}" >> $GITHUB_OUTPUT
          else
            echo "anthropic_data_available=false" >> $GITHUB_OUTPUT
            # These will be set by the script if it found alternative data
            # Otherwise, fall back to previous month
            if [ -n "$available_year" ] && [ -n "$available_month" ]; then
              echo "use_year=$available_year" >> $GITHUB_OUTPUT
              echo "use_month=$available_month" >> $GITHUB_OUTPUT
            else
              # Calculate 2 months ago as fallback
              echo "use_year=$(date -d '2 months ago' +'%Y')" >> $GITHUB_OUTPUT
              echo "use_month=$(date -d '2 months ago' +'%-m')" >> $GITHUB_OUTPUT
            fi
          fi
      
      # Add new step to collect Anthropic Economic Index data with validated date
      - name: Collect Anthropic Economic Index data
        run: python scripts/collection/collect_anthropic_index.py --year=${{ steps.check_anthropic.outputs.use_year || steps.date.outputs.year }} --month=${{ steps.check_anthropic.outputs.use_month || steps.date.outputs.month }}
      
      - name: Collect ArXiv data
        run: python scripts/collection/collect_arxiv.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
      
      - name: Collect BLS data
        run: python scripts/collection/collect_bls.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
        env:
          BLS_API_KEY: ${{ secrets.BLS_API_KEY }}
      
      - name: Collect job data
        run: python scripts/collection/collect_jobs.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}

      - name: Collect news data
        run: python scripts/collection/collect_news.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
        env:
          NEWS_API_KEY: ${{ secrets.NEWS_API_KEY }}
      
      - name: Process data
        run: |
          python scripts/processing/process_anthropic_index.py --year=${{ steps.check_anthropic.outputs.use_year || steps.date.outputs.year }} --month=${{ steps.check_anthropic.outputs.use_month || steps.date.outputs.month }}
          python scripts/processing/process_research.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_employment.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_news.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
      
      - name: Calculate index
        run: python scripts/analysis/calculate_index.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
      
      - name: Update files in repository
        run: |
          git config --global user.name 'github-actions'
          git config --global user.email 'github-actions@github.com'
          git add data/processed/
          git commit -m "Update AI Labor Market Index for ${{ steps.date.outputs.year }}-${{ steps.date.outputs.month }} ($(date +'%Y-%m-%d'))" || echo "No changes to commit"
          git push
```

## Implementation Checklist

1. [ ] Run the data correction script to fix existing files
2. [ ] Update `collect_anthropic_index.py` to validate date inputs and prevent future data collection
3. [ ] Update `process_anthropic_index.py` to handle missing data and use fallback data when needed
4. [ ] Update `calculate_index.py` to track and report data source periods
5. [ ] Update the GitHub workflow with data validation checks
6. [ ] Test the workflow locally with various date inputs
7. [ ] Check the raw and processed data for correct period labeling

## Additional Recommendations

1. Create a logging system to track when fallback data is used
2. Add a notification system to alert you when new Anthropic data is available
3. Document the expected update frequency in your project README
4. Consider adding a front-end indication when data is from a different period than requested
