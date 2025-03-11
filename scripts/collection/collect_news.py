import os

# Get API key from environment
api_key = os.environ.get('NEWS_API_KEY')
if not api_key:
    logger.warning("No NEWS_API_KEY found in environment variables")
    # Optional: exit or use dummy data

# Use the API key in your requests
params = {
    'q': 'AI layoffs hiring',
    'apiKey': api_key,
    'pageSize': 20,
    'language': 'en'
}

# Use params in your API requests