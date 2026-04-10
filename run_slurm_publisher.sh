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

# Function to find Python 3.11 or above
# Returns the path to the first Python executable that meets requirements
find_python() {
    local MIN_MAJOR=3
    local MIN_MINOR=11
    
    # Try common Python executable names in order of preference
    # Start with versioned names, then fall back to generic names
    local candidates=(
        "/usr/bin/python3.13"
        "/usr/bin/python3.12"
        "/usr/bin/python3.11"
        "python3.13"
        "python3.12"
        "python3.11"
        "python3"
        "python"
    )
    
    for pyexec in "${candidates[@]}"; do
        # Check if executable exists and is executable
        if command -v "$pyexec" >/dev/null 2>&1; then
            # Get the actual path (resolves if it's in PATH)
            local pypath=$(command -v "$pyexec")
            
            # Check version
            local version_output=$("$pypath" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null)
            
            if [ $? -eq 0 ]; then
                local major=$(echo "$version_output" | cut -d. -f1)
                local minor=$(echo "$version_output" | cut -d. -f2)
                
                # Check if version meets requirements (>= 3.11)
                if [ "$major" -gt "$MIN_MAJOR" ] || ([ "$major" -eq "$MIN_MAJOR" ] && [ "$minor" -ge "$MIN_MINOR" ]); then
                    echo "$pypath"
                    return 0
                fi
            fi
        fi
    done
    
    # If nothing found, return error
    return 1
}

# Find Python 3.11+ automatically
PYTHON=$(find_python)
if [ $? -ne 0 ]; then
    log "ERROR: Could not find Python 3.11 or above"
    log "Please ensure Python 3.11+ is installed and in PATH or at /usr/bin/python3.11"
    exit 1
fi
log "Using Python: $PYTHON ($($PYTHON --version 2>&1))"

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
"$PYTHON" "$PUBLISHER_SCRIPT" --config "$CONFIG_FILE"
EXIT_CODE=$?

# Log completion
if [ $EXIT_CODE -eq 0 ]; then
    log "Completed successfully"
else
    log "Completed with errors (exit code: $EXIT_CODE)"
fi

exit $EXIT_CODE
