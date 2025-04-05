#!/bin/bash

# Set the base directory to the current folder
BASE_DIR="$(dirname "$(realpath "$0")")"

# Export environment variables
export HOST="https://na3.kinaxis.net/"
export COMPANY_ID="ETND02_DEV01"
export FILE_XFER_WAIT_TIME_MINUTES=10
export DATA_UPDATE_WAIT_TIME_MINUTES=5
export MAX_FILE_TRANSFER_TIME_MINUTES=60
export SAFETY_TIMEOUT_MINUTES=120

# Activate the Poetry virtual environment
source $(poetry env info --path)/bin/activate

# Run the Python script
python "$BASE_DIR/main.py" --data-source "$1"

# Capture the exit code
EXIT_CODE=$?

# Print the exit code
echo "Exit code: $EXIT_CODE"

# Exit with the same code as the Python script
exit $EXIT_CODE