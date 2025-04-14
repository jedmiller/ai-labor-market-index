# AI Labor Market Impact Index

## Project Overview
This project creates a data-driven tool that tracks, visualizes, and analyzes how artificial intelligence is affecting employment across different industries. The index quantifies both job creation and displacement due to AI, providing valuable insights into the changing labor market.

## Strategic Purpose
This project demonstrates expertise in AI, data strategy, and social impact measurement, aligning with goals of securing a technology leadership role in social impact organizations.

## Architecture
The project uses a zero-cost architecture leveraging free tiers of cloud services:

- **Data Collection**: GitHub Actions workflows
- **Data Processing**: Python scripts for extraction and analysis
- **Data Storage**: GitHub repository + SQLite database
- **Visualization**: React components integrated with website
- **Deployment**: GitHub Pages or existing website infrastructure

## Key Components

1. **News Article Analysis**: Tracking AI-related layoffs, hiring, and workforce changes
2. **Academic Research Trends**: Monitoring research on AI labor impacts
3. **Labor Market Statistics**: Analyzing official employment data by industry
4. **Job Posting Analytics**: Identifying emerging roles and skill requirements
5. **Combined Impact Score**: A numerical representation of AI's overall labor market impact

## Project Structure
```
ai-labor-market-index/
├── .github/workflows/      # GitHub Actions workflows for automation
├── data/                   # Raw and processed data
├── scripts/                # Python scripts for data processing
├── components/             # React components for visualization
├── database/               # SQLite database
├── tests/                  # Test suite
└── docs/                   # Documentation
```

## Important Files

- **daily_collection.yml**: Scheduled GitHub Action for data collection
- **calculate_index.py**: Main script for computing the index value
- **AILaborMarketIndex.jsx**: Primary visualization component
- **validate_apis.py**: Script to test data source connections

## Development Conventions

- Python scripts follow PEP 8 style guidelines
- React components use functional style with hooks
- Data is stored as JSON for easy consumption by frontend
- All code includes detailed comments and documentation

## Data Sources

- ArXiv API: Academic research on AI and labor markets
- BLS API: Official employment statistics
- Remote Jobs API: Job posting data (planning to replace with Claude's AI Labor Market Index data from Hugging Face)
- News API: News about AI workforce impacts

## Current Development Status

The project's implementation is complete with the following components:
- Core architecture designed and implemented
- Data collection scripts fully implemented
- Data validation and error handling implemented
- GitHub Actions workflows configured and running
- Frontend visualization components implemented in the AILaborMarketDashboard.jsx React component
- Main index calculation logic implemented
- Automated data refresh pipeline operational

## Historical Data Implementation

The project includes comprehensive historical data management capabilities:

1. **Historical Data Generation**: Implemented through generate_historical_index.py which can retroactively calculate index values for any month
2. **History Validation**: Using history_manager.py to ensure data consistency
3. **Single Month Testing**: Via test_historical_month.py to validate historical processing for specific time periods
4. **Backup and Merge Functions**: Automated history file maintenance with backup capabilities
5. **Consistent Trending**: Ensures historical data follows realistic trend patterns with appropriate variance

## Future Enhancement Plans

1. Replace Remote Jobs API with Claude's AI Labor Market Index data from Hugging Face
2. Implement automated textual summaries of key trends when monthly files are generated
3. Add predictive analytics for future workforce trends
4. Create additional visualization components for industry-specific insights
5. Enhance documentation for public use and contributions

## Testing Approach

- Unit tests for individual scripts
- API validation tests to ensure data sources remain available
- Integration tests for the end-to-end pipeline
- Component tests for the React visualization

## Common Commands

- Run data collection: `python scripts/collection/collect_*.py`
- Process data: `python scripts/processing/process_*.py`
- Calculate index: `python scripts/analysis/calculate_index.py`
- Validate APIs: `python scripts/validation/validate_apis.py`
- Test components: `cd web && npm test`

## Code Style Guidelines

- Clear function and variable names that describe their purpose
- Comprehensive error handling and logging
- Detailed comments explaining logic and decisions
- Separation of concerns between data collection, processing, and presentation

## Additional Notes

- The project aims to run entirely on free-tier services
- Data privacy and security are maintained by not collecting personal data
- The index should be easily explainable to non-technical audiences
- All visualizations should be mobile-responsive
