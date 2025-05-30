# AI Labor Market Index Workflow Fixes

## Overview
This document contains all the fixes needed to resolve issues with the AI Labor Market Index data collection workflow. The main problems identified are:
1. Missing XML parser dependency causing ArXiv collection failures
2. Deprecated GitHub Actions output commands
3. Raw data files not being committed to the repository
4. Lack of error handling and debugging information

## Fix 1: Update GitHub Actions Workflow File

Replace your `.github/workflows/update_ai_labor_index.yml` with the following fixed version:

```yaml
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
          pip install lxml  # XML parser for BeautifulSoup - CRITICAL FIX
      
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
          echo "Current date: $(date)"
          echo "year=$(date -d 'last month' +'%Y')" >> $GITHUB_OUTPUT
          echo "month=$(date -d 'last month' +'%-m')" >> $GITHUB_OUTPUT
          echo "month_padded=$(date -d 'last month' +'%m')" >> $GITHUB_OUTPUT
          echo "Calculated year: $(date -d 'last month' +'%Y')"
          echo "Calculated month: $(date -d 'last month' +'%-m')"
      
      # Add check for existing Anthropic data
      - name: Check for Anthropic data availability
        id: check_anthropic
        run: |
          # Create a simple script to check if Anthropic has released data for the target period
          cat > check_anthropic_data.py << 'EOF'
          import requests
          import sys
          import os
          import glob
          from datetime import datetime
          
          def check_anthropic_data(year, month):
              # Format month with leading zero
              month_str = str(month).zfill(2)
              year_str = str(year)
              
              # First check if we already have the data locally
              raw_dir = "./data/raw/anthropic_index"
              pattern = f"anthropic_index_{year_str}_{month_str}_combined.json"
              matching_files = glob.glob(os.path.join(raw_dir, pattern))
              
              if matching_files:
                  print(f"Found local data for {year_str}-{month_str}")
                  return True
                  
              # If no local files, check Hugging Face
              try:
                  # Define the URL for the Anthropic Economic Index repository
                  url = f"https://huggingface.co/datasets/Anthropic/EconomicIndex/tree/main/release_{year_str}_{month_str}"
                  
                  response = requests.get(url, timeout=10)
                  if response.status_code == 200:
                      print(f"Anthropic data for {year_str}-{month_str} appears to be available")
                      return True
                  else:
                      # Try to find previous month data
                      if int(month) == 1:
                          prev_month = "12"
                          prev_year = str(int(year) - 1)
                      else:
                          prev_month = str(int(month) - 1).zfill(2)
                          prev_year = year_str
                      
                      # Check if we have local data for previous month
                      prev_pattern = f"anthropic_index_{prev_year}_{prev_month}_combined.json"
                      prev_files = glob.glob(os.path.join(raw_dir, prev_pattern))
                      
                      if prev_files:
                          print(f"Found local data for previous month {prev_year}-{prev_month}")
                          # Write to environment file for GitHub Actions
                          with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
                              f.write(f"available_year={prev_year}\n")
                              f.write(f"available_month={int(prev_month)}\n")
                          return False
                      
                      # Check if previous month data is available on Hugging Face
                      prev_url = f"https://huggingface.co/datasets/Anthropic/EconomicIndex/tree/main/release_{prev_year}_{prev_month}"
                      prev_response = requests.get(prev_url, timeout=10)
                      
                      if prev_response.status_code == 200:
                          print(f"Found previous month data on Hugging Face: {prev_year}-{prev_month}")
                          # Write to environment file for GitHub Actions
                          with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
                              f.write(f"available_year={prev_year}\n")
                              f.write(f"available_month={int(prev_month)}\n")
                          return False
                      
                      # If all else fails, try looking for the most recent data we have locally
                      existing_files = glob.glob(os.path.join(raw_dir, "anthropic_index_*_combined.json"))
                      if existing_files:
                          newest_file = max(existing_files, key=os.path.getmtime)
                          filename = os.path.basename(newest_file)
                          parts = filename.split('_')
                          if len(parts) >= 3:
                              latest_year = parts[1]
                              latest_month = parts[2]
                              print(f"Found most recent local data: {latest_year}-{latest_month}")
                              # Write to environment file for GitHub Actions
                              with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
                                  f.write(f"available_year={latest_year}\n")
                                  f.write(f"available_month={int(latest_month)}\n")
                              return False
                      
                      print(f"No Anthropic data found for {year_str}-{month_str}")
                      return False
              except Exception as e:
                  print(f"Error checking Anthropic data availability: {e}")
                  # If we encounter an error, just return True to continue with requested date
                  return True
          
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
            # Check if available_year and available_month were set by the Python script
            if [ -f "$GITHUB_OUTPUT" ] && grep -q "available_year" "$GITHUB_OUTPUT"; then
              echo "Using available year and month from Python script"
            else
              # If not set by Python script, use the requested date
              echo "use_year=${{ steps.date.outputs.year }}" >> $GITHUB_OUTPUT
              echo "use_month=${{ steps.date.outputs.month }}" >> $GITHUB_OUTPUT
            fi
          fi
      
      # Add new step to collect Anthropic Economic Index data with validated date
      - name: Collect Anthropic Economic Index data
        run: |
          echo "Collecting Anthropic data..."
          python scripts/collection/collect_anthropic_index.py --year=${{ steps.check_anthropic.outputs.use_year || steps.date.outputs.year }} --month=${{ steps.check_anthropic.outputs.use_month || steps.date.outputs.month }}
          echo "Checking Anthropic output files..."
          ls -la data/raw/anthropic_index/
      
      - name: Collect ArXiv data
        run: |
          echo "Starting ArXiv collection..."
          python scripts/collection/collect_arxiv.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }} || {
            echo "ArXiv collection failed with exit code $?"
            exit 1
          }
          echo "ArXiv collection complete."
          echo "Checking for output files..."
          ls -la data/raw/arxiv/
      
      - name: Collect BLS data
        run: |
          echo "Collecting BLS data..."
          python scripts/collection/collect_bls.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          echo "Checking BLS output files..."
          ls -la data/raw/bls/
        env:
          BLS_API_KEY: ${{ secrets.BLS_API_KEY }}
      
      - name: Collect job data
        run: |
          echo "Collecting job data..."
          python scripts/collection/collect_jobs.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          echo "Checking job output files..."
          ls -la data/raw/jobs/

      - name: Collect news data
        run: |
          echo "Collecting news data..."
          python scripts/collection/collect_news.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          echo "Checking news output files..."
          ls -la data/raw/news/
        env:
          NEWS_API_KEY: ${{ secrets.NEWS_API_KEY }}
      
      - name: Process data
        run: |
          # Process Anthropic data using validated date
          python scripts/processing/process_anthropic_index.py --year=${{ steps.check_anthropic.outputs.use_year || steps.date.outputs.year }} --month=${{ steps.check_anthropic.outputs.use_month || steps.date.outputs.month }}
          
          # Process other data sources with the requested date
          python scripts/processing/process_research.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_employment.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          python scripts/processing/process_news.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
      
      - name: Calculate index
        run: |
          echo "Calculating index..."
          python scripts/analysis/calculate_index.py --year=${{ steps.date.outputs.year }} --month=${{ steps.date.outputs.month }}
          echo "Checking processed output files..."
          ls -la data/processed/
      
      - name: Verify data collection
        run: |
          # Check if files were created
          if [ -z "$(ls -A data/raw/arxiv/)" ]; then
            echo "Warning: No ArXiv data files created"
          fi
          
          if [ -z "$(ls -A data/processed/)" ]; then
            echo "Error: No processed data files created"
            exit 1
          fi
      
      - name: Update files in repository
        run: |
          git config --global user.name 'github-actions'
          git config --global user.email 'github-actions@github.com'
          git add data/raw/
          git add data/processed/
          echo "Files to be committed:"
          git status
          git commit -m "Update AI Labor Market Index for ${{ steps.date.outputs.year }}-${{ steps.date.outputs.month }} ($(date +'%Y-%m-%d'))" || echo "No changes to commit"
          git push
```

## Fix 2: Create Requirements File (Optional)

Create a `requirements.txt` file in your repository root to document all dependencies:

```txt
requests
beautifulsoup4
pandas
numpy
nltk
huggingface_hub
lxml
```

## Implementation Steps

1. **Copy this markdown file** to your project directory
2. **Open Claude Code** and navigate to your project
3. **Execute the following commands**:
   ```bash
   # Navigate to your project root
   cd /path/to/your/ai-labor-market-index
   
   # Open the workflow file
   code .github/workflows/update_ai_labor_index.yml
   ```
4. **Replace the entire workflow file** with the fixed version from this document
5. **Save and commit the changes**:
   ```bash
   git add .github/workflows/update_ai_labor_index.yml
   git commit -m "Fix workflow: Add XML parser and improve error handling"
   git push
   ```
6. **Run the workflow manually** from GitHub Actions to test the fixes

## Verification Steps

After implementing these fixes:

1. Go to your GitHub repository
2. Navigate to the Actions tab
3. Select "Update AI Labor Market Index"
4. Click "Run workflow"
5. Monitor the workflow execution
6. Check the logs for:
   - Successful ArXiv data collection (should show papers found)
   - Files being created in data/raw/ directories
   - Successful git commit and push

## Expected Results

After these fixes, you should see:
- ArXiv data being collected successfully (no XML parser errors)
- Raw data files being committed to the repository
- Better debugging output in the workflow logs
- Processed data files being updated
- Your website dashboard displaying the latest data

## Troubleshooting

If issues persist:
1. Check the detailed workflow logs for specific error messages
2. Verify that all required secrets (BLS_API_KEY, NEWS_API_KEY) are set in your repository
3. Ensure the Python scripts have proper error handling
4. Test individual collection scripts locally to isolate issues

## Additional Notes

- The workflow runs on UTC time, so schedule accordingly
- Manual runs are useful for testing and debugging
- Consider adding notifications for workflow failures
- Monitor API rate limits for external services