#!/bin/bash

# Deploy visualization data to portfolio website
# This script copies the latest projection data to the portfolio project

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PORTFOLIO_PROJECT="${HOME}/Documents/jedidiah-miller-portfolio"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}AI Labor Market Index - Portfolio Deployment${NC}"
echo "================================================"

# Check if portfolio project exists
if [ ! -d "$PORTFOLIO_PROJECT" ]; then
    echo -e "${RED}Error: Portfolio project not found at $PORTFOLIO_PROJECT${NC}"
    exit 1
fi

# Find the latest visualization export
LATEST_EXPORT=$(ls -t ${PROJECT_ROOT}/data/processed/visualization_export_*.json 2>/dev/null | head -1)

if [ -z "$LATEST_EXPORT" ]; then
    echo -e "${RED}Error: No visualization export files found${NC}"
    exit 1
fi

echo -e "${YELLOW}Found latest export: $(basename $LATEST_EXPORT)${NC}"

# Create data directory in portfolio if it doesn't exist
mkdir -p "${PORTFOLIO_PROJECT}/public/data"

# Copy the file with a consistent name for the website to reference
cp "$LATEST_EXPORT" "${PORTFOLIO_PROJECT}/public/data/ai-labor-projections-latest.json"

# Also keep a timestamped version
TIMESTAMP=$(date +%Y%m%d)
cp "$LATEST_EXPORT" "${PORTFOLIO_PROJECT}/public/data/ai-labor-projections-${TIMESTAMP}.json"

echo -e "${GREEN}✓ Copied to portfolio project:${NC}"
echo "  - public/data/ai-labor-projections-latest.json"
echo "  - public/data/ai-labor-projections-${TIMESTAMP}.json"

# Check if we should commit the changes
echo ""
echo -e "${YELLOW}Do you want to commit these changes to the portfolio project? (y/n)${NC}"
read -r response

if [[ "$response" =~ ^[Yy]$ ]]; then
    cd "$PORTFOLIO_PROJECT"
    git add public/data/ai-labor-projections-*.json
    git commit -m "Update AI Labor Market projections data - ${TIMESTAMP}

- Updated with August 2025 Anthropic data
- US-specific SOC classification (82.9% coverage)
- Aligned all data sources to August 2025
- Enhanced projections through 2030"

    echo -e "${GREEN}✓ Changes committed to portfolio project${NC}"
    echo -e "${YELLOW}Remember to push to deploy: git push${NC}"
else
    echo -e "${YELLOW}Changes copied but not committed${NC}"
fi

echo ""
echo -e "${GREEN}Deployment complete!${NC}"
echo ""
echo "Next steps in your portfolio project:"
echo "1. Update your React component to fetch from: /data/ai-labor-projections-latest.json"
echo "2. Test locally: npm run dev"
echo "3. Deploy: git push (if using Vercel/Netlify auto-deploy)"