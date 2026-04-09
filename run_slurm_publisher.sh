#!/bin/bash
#
# Wrapper script for running SLURM log publisher via scrontab
# This script is designed to be called periodically by NERSC's scrontab mechanism
#

# Configuration
SCRIPT_DIR="<pathname to the directory where scripts like publish-slurm-logs.py can be found>"
PUBLISHER_SCRIPT="${SCRIPT_DIR}/publish_slurm_logs.py"
CONFIG_FILE="${SCRIPT_DIR}/publish_slurm_logs_config.json"
LOCK_FILE="${SCRIPT_DIR}/.publisher.lock"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Check if another instance is running
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        log "Another instance is running (PID: $PID), exiting"
        exit 0
    else
        log "Stale lock file found, removing"
        rm -f "$LOCK_FILE"
    fi
fi

# Create lock file
echo $$ > "$LOCK_FILE"

# Ensure lock is removed on exit
trap "rm -f '$LOCK_FILE'" EXIT INT TERM

# Log start
log "Starting SLURM log publisher"
log "Script: $PUBLISHER_SCRIPT"
log "Config: $CONFIG_FILE"

# Verify files exist
if [ ! -f "$PUBLISHER_SCRIPT" ]; then
    log "ERROR: Publisher script not found: $PUBLISHER_SCRIPT"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    log "ERROR: Config file not found: $CONFIG_FILE"
    exit 1
fi

# Run the publisher
python3.11 "$PUBLISHER_SCRIPT" --config "$CONFIG_FILE"
EXIT_CODE=$?

# Log completion
if [ $EXIT_CODE -eq 0 ]; then
    log "Completed successfully"
else
    log "Completed with errors (exit code: $EXIT_CODE)"
fi

exit $EXIT_CODE
