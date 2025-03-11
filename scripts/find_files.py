#!/usr/bin/env python3
# find_files.py
# Script to find where data files actually exist on the local machine

import os
import sys
import glob

def find_files(base_dirs, patterns):
    """Search for files matching patterns in base directories."""
    found_files = {}
    
    for pattern in patterns:
        found_files[pattern] = []
        
        for base_dir in base_dirs:
            # Try with and without 'data/processed' prefix
            search_paths = [
                os.path.join(base_dir, pattern),
                os.path.join(base_dir, "data", "processed", pattern),
                os.path.join(base_dir, "*", pattern),
                pattern  # Try absolute path or current directory
            ]
            
            for search_path in search_paths:
                matches = glob.glob(search_path)
                if matches:
                    found_files[pattern].extend(matches)
    
    return found_files

def main():
    # Base directories to search
    base_dirs = [
        ".",  # Current directory
        "..",  # Parent directory
        os.path.expanduser("~"),  # Home directory
        os.path.join(os.path.expanduser("~"), "ai-labor-market-index"),  # Project in home
        "/path/to/ai-labor-market-index"  # Example absolute path
    ]
    
    # Patterns to search for
    patterns = [
        "job_trends_*.json",
        "employment_stats_*.json",
        "research_trends_*.json",
        "workforce_events_*.json",
        "index_history.json",
        "ai_labor_index_*.json"
    ]
    
    # Find files
    found_files = find_files(base_dirs, patterns)
    
    # Print results
    print("Found files:")
    for pattern, files in found_files.items():
        print(f"\n{pattern}:")
        if files:
            for file in files:
                print(f"  - {file}")
        else:
            print("  No files found")
    
    # Print current working directory
    print(f"\nCurrent working directory: {os.getcwd()}")
    
    # List files in current directory
    print("\nFiles in current directory:")
    for item in os.listdir("."):
        if os.path.isdir(item):
            print(f"  DIR: {item}")
        else:
            print(f"  FILE: {item}")
    
    # If data/processed exists, list its contents
    data_processed_dir = os.path.join(".", "data", "processed")
    if os.path.exists(data_processed_dir):
        print(f"\nFiles in {data_processed_dir}:")
        try:
            for item in os.listdir(data_processed_dir):
                if os.path.isdir(os.path.join(data_processed_dir, item)):
                    print(f"  DIR: {item}")
                else:
                    print(f"  FILE: {item}")
        except Exception as e:
            print(f"  Error reading directory: {e}")

if __name__ == "__main__":
    main()