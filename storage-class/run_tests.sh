#!/bin/bash

# S3 Storage Class Test Runner
# This script sets up the environment and runs the S3 storage class tests

set -e

echo "=== S3 Storage Class Test Runner ==="

# Check if virtual environment exists, create if not
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

# Check required environment variables
echo "Checking environment variables..."
required_vars=("DATABEND_DSN" "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "S3_BUCKET")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "Error: Missing required environment variables:"
    printf '  %s\n' "${missing_vars[@]}"
    echo ""
    echo "Please set the following environment variables:"
    echo "  export DATABEND_DSN='databend://user:pass@localhost:8000/default'"
    echo "  export AWS_ACCESS_KEY_ID='your_access_key'"
    echo "  export AWS_SECRET_ACCESS_KEY='your_secret_key'"
    echo "  export S3_BUCKET='your_test_bucket'"
    echo "  export AWS_REGION='us-east-1'  # optional"
    echo "  export S3_ROOT_PREFIX='data2/'  # optional"
    exit 1
fi

echo "Environment variables OK"

# Set default values for optional variables
export AWS_REGION="${AWS_REGION:-us-east-1}"
export S3_ROOT_PREFIX="${S3_ROOT_PREFIX:-data2/}"

echo "Configuration:"
echo "  DATABEND_DSN: $DATABEND_DSN"
echo "  S3_BUCKET: $S3_BUCKET"
echo "  AWS_REGION: $AWS_REGION"
echo "  S3_ROOT_PREFIX: $S3_ROOT_PREFIX"
echo ""

# Run tests
echo "Running S3 Storage Class tests..."
pytest test_s3_storage_class.py -v --tb=short "$@"

echo ""
echo "Tests completed!"

# Deactivate virtual environment
deactivate