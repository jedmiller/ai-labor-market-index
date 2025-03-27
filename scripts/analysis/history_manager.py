#!/usr/bin/env python
import json
import os
import argparse
import logging
from datetime import datetime
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("history-manager")

def backup_history(data_dir="./data/processed", backup_dir="./data/backups"):
    """Create a backup of history files."""
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for filename in ["index_history.json", "ai_labor_index_latest.json"]:
        source = os.path.join(data_dir, filename)
        if os.path.exists(source):
            backup = os.path.join(backup_dir, f"{filename.split('.')[0]}_{timestamp}.json")
            shutil.copy2(source, backup)
            logger.info(f"Backed up {filename} to {backup}")
    
    return timestamp

def merge_history(data_dir="./data/processed"):
    """Merge history from all sources to create a complete timeline."""
    sources = [
        os.path.join(data_dir, "index_history.json"),
        os.path.join(data_dir, "ai_labor_index_latest.json")
    ]
    
    # Add monthly files
    sources.extend([
        os.path.join(data_dir, f) for f in os.listdir(data_dir)
        if f.startswith("ai_labor_index_") and f != "ai_labor_index_latest.json"
    ])
    
    all_entries = []
    for source in sources:
        if not os.path.exists(source):
            continue
            
        try:
            with open(source, 'r') as f:
                data = json.load(f)
            
            # Extract history entries
            if "history" in data and isinstance(data["history"], list):
                logger.info(f"Found {len(data['history'])} entries in {source}")
                all_entries.extend(data["history"])
            elif "index_value" in data:
                # Create entry from index file
                date_str = data["timestamp"].split("T")[0][:7]
                entry = {
                    "date": date_str,
                    "value": data["index_value"],
                    "interpretation": data["interpretation"],
                    "timestamp": data["timestamp"]
                }
                all_entries.append(entry)
        except Exception as e:
            logger.error(f"Error processing {source}: {e}")
    
    # Keep only latest entry for each date
    date_map = {}
    for entry in all_entries:
        date = entry.get("date")
        timestamp = entry.get("timestamp")
        if date and timestamp:
            if date not in date_map or timestamp > date_map[date]["timestamp"]:
                date_map[date] = entry
    
    merged_history = list(date_map.values())
    merged_history.sort(key=lambda x: x.get("date", ""))
    
    return {
        "generated_at": datetime.now().isoformat(),
        "history": merged_history
    }

def update_files(history_obj, data_dir="./data/processed"):
    """Update all files with merged history."""
    # Update index_history.json
    with open(os.path.join(data_dir, "index_history.json"), 'w') as f:
        json.dump(history_obj, f, indent=2)
    
    # Update ai_labor_index_latest.json
    latest_file = os.path.join(data_dir, "ai_labor_index_latest.json")
    if os.path.exists(latest_file):
        with open(latest_file, 'r') as f:
            latest_data = json.load(f)
        
        latest_data["history"] = history_obj["history"]
        
        with open(latest_file, 'w') as f:
            json.dump(latest_data, f, indent=2)
    
    logger.info(f"Updated files with {len(history_obj['history'])} history entries")

def main():
    parser = argparse.ArgumentParser(description='Manage AI Labor Market Index history')
    parser.add_argument('--data-dir', default='./data/processed', help='Data directory')
    parser.add_argument('--backup-dir', default='./data/backups', help='Backup directory')
    parser.add_argument('command', choices=['backup', 'merge', 'update'], help='Command to execute')
    
    args = parser.parse_args()
    
    if args.command == 'backup':
        backup_history(args.data_dir, args.backup_dir)
    elif args.command == 'merge':
        history_obj = merge_history(args.data_dir)
        print(json.dumps(history_obj, indent=2))
    elif args.command == 'update':
        backup_history(args.data_dir, args.backup_dir)
        history_obj = merge_history(args.data_dir)
        update_files(history_obj, args.data_dir)

if __name__ == "__main__":
    main()