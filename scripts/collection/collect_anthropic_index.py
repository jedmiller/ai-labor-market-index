# scripts/collection/collect_anthropic_index.py
import requests
import json
import os
import logging
import sys
from datetime import datetime
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("anthropic_index_collection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("anthropic-index-collector")

# Try importing huggingface_hub
try:
    from huggingface_hub import hf_hub_download
    HUGGINGFACE_AVAILABLE = True
except ImportError:
    HUGGINGFACE_AVAILABLE = False
    logger.warning("huggingface_hub package not installed. Install with: pip install huggingface_hub")

class AnthropicIndexCollector:
    def __init__(self, output_dir="./data/raw/anthropic_index", use_simulation=None):
        self.output_dir = output_dir
        self.repo_id = "Anthropic/EconomicIndex"
        self.release_dir = "release_2025_09_15"  # Updated to latest release

        # Determine whether to use simulation data
        if use_simulation is not None:
            self.use_simulation = use_simulation
        else:
            # Default to real data if huggingface_hub is available
            self.use_simulation = not HUGGINGFACE_AVAILABLE

        logger.info(f"Using simulation mode: {self.use_simulation}")
        logger.info(f"Release directory: {self.release_dir}")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def fetch_enriched_data(self, filename="data/output/aei_enriched_claude_ai_2025-08-04_to_2025-08-11.csv"):
        """
        Fetch the main enriched dataset from the Anthropic Economic Index

        Returns:
            pandas.DataFrame: The enriched dataset
        """
        if self.use_simulation:
            logger.error("Simulation mode not supported for new data format")
            return None

        if not HUGGINGFACE_AVAILABLE:
            logger.error("huggingface_hub package is required. Install with: pip install huggingface_hub")
            return None

        try:
            # Construct path in the latest release directory
            file_path = os.path.join(self.release_dir, filename)
            logger.info(f"Downloading {file_path} from {self.repo_id}")

            # Download file from Hugging Face
            local_path = hf_hub_download(
                repo_id=self.repo_id,
                filename=file_path,
                repo_type="dataset"
            )

            # Read the CSV file
            df = pd.read_csv(local_path)
            logger.info(f"Successfully loaded data with {len(df)} rows and {len(df.columns)} columns")
            logger.info(f"Date range: {df['date_start'].min()} to {df['date_end'].max()}")
            logger.info(f"Facets available: {df['facet'].unique().tolist()}")

            return df

        except Exception as e:
            logger.error(f"Error fetching enriched data: {str(e)}")
            return None

    def fetch_request_hierarchy(self, platform="claude_ai"):
        """
        Fetch request hierarchy tree data

        Args:
            platform: Either "claude_ai" or "1p_api"

        Returns:
            dict: The request hierarchy tree
        """
        if self.use_simulation:
            logger.error("Simulation mode not supported for new data format")
            return None

        if not HUGGINGFACE_AVAILABLE:
            logger.error("huggingface_hub package is required")
            return None

        try:
            filename = f"data/output/request_hierarchy_tree_{platform}.json"
            file_path = os.path.join(self.release_dir, filename)
            logger.info(f"Downloading {file_path} from {self.repo_id}")

            local_path = hf_hub_download(
                repo_id=self.repo_id,
                filename=file_path,
                repo_type="dataset"
            )

            with open(local_path, 'r') as f:
                data = json.load(f)

            logger.info(f"Successfully loaded request hierarchy for {platform}")
            return data

        except Exception as e:
            logger.error(f"Error fetching request hierarchy: {str(e)}")
            return None

    def process_by_facet(self, df):
        """
        Process the enriched data by facet type

        Args:
            df: pandas DataFrame with the enriched data

        Returns:
            dict: Processed data organized by facet
        """
        processed = {}

        facets = df['facet'].unique()
        logger.info(f"Processing {len(facets)} facets")

        for facet in facets:
            facet_data = df[df['facet'] == facet].copy()

            # Convert to records format for JSON serialization
            processed[facet] = {
                'row_count': len(facet_data),
                'date_range': {
                    'start': facet_data['date_start'].min(),
                    'end': facet_data['date_end'].max()
                },
                'variables': facet_data['variable'].unique().tolist(),
                'data': facet_data.to_dict('records')
            }

            logger.info(f"Processed facet '{facet}': {len(facet_data)} rows")

        return processed

    def process_occupation_data(self, df):
        """
        Extract and process occupation-specific data for labor market analysis

        Args:
            df: pandas DataFrame with the enriched data

        Returns:
            dict: Occupation data ready for labor market projections
        """
        # Filter for SOC occupation data
        soc_data = df[df['facet'] == 'soc_occupation'].copy()

        if len(soc_data) == 0:
            logger.warning("No SOC occupation data found")
            return {}

        # Pivot the data to get occupation-level metrics
        occupation_metrics = {}

        for _, row in soc_data.iterrows():
            cluster = row.get('cluster_name', 'Unknown')
            variable = row['variable']
            value = row['value']

            if cluster not in occupation_metrics:
                occupation_metrics[cluster] = {}

            occupation_metrics[cluster][variable] = value

        logger.info(f"Processed {len(occupation_metrics)} occupations")
        return occupation_metrics

    def process_automation_augmentation_data(self, df):
        """
        Extract automation vs augmentation collaboration patterns

        Args:
            df: pandas DataFrame with the enriched data

        Returns:
            dict: Automation/augmentation data
        """
        collab_auto_aug = df[df['facet'] == 'collaboration_automation_augmentation'].copy()

        if len(collab_auto_aug) == 0:
            logger.warning("No collaboration_automation_augmentation data found")
            return {}

        # Process by geography
        automation_data = {}

        for _, row in collab_auto_aug.iterrows():
            geo = row['geo_name']
            variable = row['variable']
            value = row['value']

            if geo not in automation_data:
                automation_data[geo] = {}

            automation_data[geo][variable] = value

        logger.info(f"Processed automation/augmentation data for {len(automation_data)} geographies")
        return automation_data

    def collect_data(self, year=None, month=None):
        """
        Collect and process all datasets from Anthropic Economic Index

        Args:
            year (int): Optional year for file naming
            month (int): Optional month for file naming
        """
        timestamp = datetime.now().isoformat()

        # Use provided date or current date for filename formatting
        if year and month:
            date_prefix = f"{year}_{month:02d}"
        else:
            current_date = datetime.now()
            date_prefix = current_date.strftime("%Y_%m")

        results = {
            "timestamp": timestamp,
            "release": self.release_dir,
            "files_created": [],
            "datasets_collected": 0
        }

        # Fetch main enriched dataset
        logger.info("Fetching main enriched dataset...")
        df = self.fetch_enriched_data()

        if df is None:
            logger.error("Failed to fetch enriched data")
            return results

        # Save raw data
        raw_filename = f"anthropic_index_{date_prefix}_raw.csv"
        raw_path = os.path.join(self.output_dir, raw_filename)
        df.to_csv(raw_path, index=False)
        results["files_created"].append(raw_path)
        results["datasets_collected"] += 1
        logger.info(f"Saved raw data to {raw_path}")

        # Process by facet
        logger.info("Processing data by facet...")
        facet_data = self.process_by_facet(df)
        facet_filename = f"anthropic_index_{date_prefix}_by_facet.json"
        facet_path = os.path.join(self.output_dir, facet_filename)

        with open(facet_path, 'w') as f:
            json.dump({
                "timestamp": timestamp,
                "release": self.release_dir,
                "target_period": f"{year}-{month:02d}" if year and month else None,
                "facets": facet_data
            }, f, indent=2)

        results["files_created"].append(facet_path)
        results["datasets_collected"] += 1
        logger.info(f"Saved facet data to {facet_path}")

        # Process occupation data
        logger.info("Processing occupation data...")
        occupation_data = self.process_occupation_data(df)
        if occupation_data:
            occ_filename = f"anthropic_index_{date_prefix}_occupations.json"
            occ_path = os.path.join(self.output_dir, occ_filename)

            with open(occ_path, 'w') as f:
                json.dump({
                    "timestamp": timestamp,
                    "release": self.release_dir,
                    "target_period": f"{year}-{month:02d}" if year and month else None,
                    "occupations": occupation_data
                }, f, indent=2)

            results["files_created"].append(occ_path)
            results["datasets_collected"] += 1
            logger.info(f"Saved occupation data to {occ_path}")

        # Process automation/augmentation data
        logger.info("Processing automation/augmentation data...")
        auto_aug_data = self.process_automation_augmentation_data(df)
        if auto_aug_data:
            aa_filename = f"anthropic_index_{date_prefix}_automation_augmentation.json"
            aa_path = os.path.join(self.output_dir, aa_filename)

            with open(aa_path, 'w') as f:
                json.dump({
                    "timestamp": timestamp,
                    "release": self.release_dir,
                    "target_period": f"{year}-{month:02d}" if year and month else None,
                    "data": auto_aug_data
                }, f, indent=2)

            results["files_created"].append(aa_path)
            results["datasets_collected"] += 1
            logger.info(f"Saved automation/augmentation data to {aa_path}")

        # Fetch request hierarchies
        logger.info("Fetching request hierarchy trees...")
        for platform in ["claude_ai", "1p_api"]:
            hierarchy = self.fetch_request_hierarchy(platform)
            if hierarchy:
                hier_filename = f"anthropic_index_{date_prefix}_hierarchy_{platform}.json"
                hier_path = os.path.join(self.output_dir, hier_filename)

                with open(hier_path, 'w') as f:
                    json.dump({
                        "timestamp": timestamp,
                        "release": self.release_dir,
                        "platform": platform,
                        "hierarchy": hierarchy
                    }, f, indent=2)

                results["files_created"].append(hier_path)
                results["datasets_collected"] += 1
                logger.info(f"Saved hierarchy data for {platform} to {hier_path}")

        logger.info(f"Collection complete. Collected {results['datasets_collected']} datasets.")
        return results

def main():
    """Main function to run the data collection"""
    import argparse
    parser = argparse.ArgumentParser(description='Collect data from Anthropic Economic Index')
    parser.add_argument('--year', type=int, help='Target year (defaults to current)')
    parser.add_argument('--month', type=int, help='Target month (1-12, defaults to current)')
    parser.add_argument('--output', type=str, help='Output directory', default="./data/raw/anthropic_index")
    parser.add_argument('--simulation', action='store_true', help='Use simulation mode (not supported for new format)')
    args = parser.parse_args()

    # Use current date if not specified
    current_date = datetime.now()
    year = args.year if args.year else current_date.year
    month = args.month if args.month else current_date.month

    # Validate date is not in the future
    if (year > current_date.year) or (year == current_date.year and month > current_date.month):
        logger.error(f"Error: Cannot collect future data for {year}-{month:02d}")
        logger.error(f"Current date is {current_date.year}-{current_date.month:02d}")
        return 1

    # Initialize collector
    collector = AnthropicIndexCollector(
        output_dir=args.output,
        use_simulation=args.simulation
    )

    # Run collection
    results = collector.collect_data(year=year, month=month)

    logger.info(f"Collection complete. Collected {results['datasets_collected']} datasets.")
    logger.info(f"Created {len(results['files_created'])} files.")

    # Return exit code based on collection success
    if results['datasets_collected'] > 0:
        return 0
    else:
        logger.error("No datasets were collected!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
