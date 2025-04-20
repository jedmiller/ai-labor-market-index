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
            job_trends = index_data['components']['job_trends']
            if 'details' in job_trends and 'source' in job_trends['details']:
                if (job_trends['details']['source'] == 'Anthropic Economic Index' and
                    not job_trends['details'].get('is_simulated_data', True)):
                    # Add a flag to indicate this data has been corrected
                    job_trends['details']['data_corrected'] = True
        
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