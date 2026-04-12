#!/bin/bash

# Supraw Scraper Runner
# Usage: ./run_scraper.sh

cd "$(dirname "$0")"

# Check if .env exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and fill in your Supabase credentials"
    exit 1
fi

# Load environment variables
source .env

# Verify credentials are set
if [ -z "$SUPABASE_URL" ] || [ -z "$SUPABASE_KEY" ]; then
    echo "Error: SUPABASE_URL or SUPABASE_KEY not set in .env"
    exit 1
fi

echo "Starting Supraw scraper..."
echo "Supabase URL: $SUPABASE_URL"

python3 main.py
