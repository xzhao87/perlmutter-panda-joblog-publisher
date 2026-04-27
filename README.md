# SLURM Log Publisher System

Automated system for publishing SLURM job logs to NERSC Science Gateway in a PanDA-centric directory structure.

## Quick Start

For first-time users, follow these steps:

```bash
# 1. Verify configuration
cat publish_slurm_logs_config.json (follow comments to make necessary changes, see later sections for more detailed config info) 

# 2. Test with dry-run
python3 publish_slurm_logs.py --dry-run

# 3. Create CFS directories (if not exists), e.g : 
mkdir -p /global/cfs/cdirs/<nersc project name>/www/panda/jobs
chmod 755 /global/cfs/cdirs/<nersc project name>/www/panda/jobs

# 4. Run manually to test
python3 publish_slurm_logs.py

# 5. Set up scrontab for automation
scrontab -e
(to run it every 10 minutes : */10 * * * * /path/to/run_slurm_publisher.sh)

# 6. Verify logs are published
ls -la /global/cfs/cdirs/<nersc project name>/www/panda/jobs/

# 7. Access via web
# https://portal.nersc.gov/cfs/<nersc project name>/panda/jobs/<panda queue name>/<pandaid>/
```

**See "Installation" section below for detailed setup instructions.**

## Overview

This system automatically:
1. Scans for finished SLURM jobs in harvester workdirs
2. Splits combined SLURM output into per-task/per-PandaID files (handles multiple PandaIDs per task)
3. Copies pilotlog.txt from each task directory to all PandaID directories
4. Copies additional files from failed tasks (payload.stdout, payload.stderr, etc.)
5. Publishes logs to CFS in PandaID-organized structure
6. Processes jobs in parallel with configurable concurrency limits
7. Cleans up old logs after retention period
8. Tracks processed jobs using dual mechanisms (marker files + state file) to avoid re-processing

## Published Files

For each successfully completed task, the publisher copies:
- **SLURM split output**: Infrastructure/wrapper/pilot logs from SLURM stdout/stderr
- **pilotlog.txt**: Complete pilot logs including payload stdout/stderr and errors
- **Header file**: Untagged lines from SLURM output (wrapper-level info)

For failed tasks (containing `PanDA_Pilot-*` directory), additional files are copied:
- See `additional_files_for_failed_tasks` in config for patterns

## Components

### Configuration File
- **File**: `publish_slurm_logs_config.json`
- **Purpose**: Central configuration for paths, timing, and behavior
- **Key settings**:
  - `paths.workdir_root`: Where harvester places worker directories
  - `paths.cfs_destination`: CFS directory for published logs
  - `timing.retention_days`: How long to keep published logs
  - `processing.split_script`: Path to split_slurm_output.py
  - `processing.max_concurrent_jobs`: Maximum parallel job processing (default: 10)
  - `processing.delete_original_splits`: Clean up split files after publishing
  - `additional_files_for_failed_tasks`: Patterns for copying extra files from failed tasks

### Main Script
- **File**: `publish_slurm_logs.py`
- **Purpose**: Core logic for scanning, splitting, and publishing
- **Features**:
  - Checks if SLURM jobs are finished (not in queue, old enough)
  - Splits SLURM output using split_slurm_output.py (handles multiple PandaIDs per task)
  - Organizes files by PandaID
  - Copies pilotlog.txt to all PandaID directories
  - Copies additional files from failed tasks (configurable patterns)
  - Sets world-readable permissions (0o644) for web access
  - **Parallel processing**: Uses multiprocessing to handle multiple jobs concurrently
  - **Lock files**: Prevents concurrent processing of same job (`.slurm-<jobid>.lock`)
  - **Dual tracking**: Uses both marker files (`.publish-done`) and state file for reliability
  - Automatic cleanup of old directories

### Wrapper Script (for scrontab)
- **File**: `run_slurm_publisher.sh`
- **Purpose**: Wrapper for running via NERSC scrontab
- **Features**:
  - Lock file to prevent concurrent runs
  - Logging with timestamps
  - Error handling

## Directory Structure

### Input (Harvester Workdir)
```
/pscratch/sd/x/xin/panda/workdir/panda/
├── <panda queue name>/
│   ├── 11270/                    # Worker directory
│   │   ├── slurm-50685843.out    # Combined SLURM output
│   │   ├── 0/                    # Task 0 directory
│   │   ├── 1/                    # Task 1 directory
│   │   └── ...
│   └── 11271/
└── Other_Queue/
```

### Output (CFS/Web)
```
/global/cfs/cdirs/<nersc project name>/www/panda/jobs/
├── <panda queue name>/                   # Queue name
│   ├── 260789/                              # PandaID directory
│   │   ├── slurm-50685843-task67-panda260789.out  # SLURM split output
│   │   ├── slurm-50685843-header.out              # Header file
│   │   ├── pilotlog.txt                           # Full pilot log
│   │   └── PanDA_Pilot-260789/                    # For failed tasks only
│   │       ├── payload.stdout
│   │       ├── payload.stderr
│   │       └── workDir/
│   │           └── dataprod_rel*.csv
│   ├── 260790/                              # Another PandaID
│   │   ├── slurm-50685843-task68-panda260790.out
│   │   ├── slurm-50685843-header.out
│   │   └── pilotlog.txt
│   └── 7111501056/                          # PandaID from multi-job pilot
│       ├── slurm-51874506-task152-panda7111501056.out
│       ├── slurm-51874506-header.out
│       └── pilotlog.txt                     # Same pilotlog for all PandaIDs from task 152
└── Other_Queue/
```

**Note on multi-PandaID pilots**: When a single pilot (task) processes multiple PandaIDs:
- Separate output files are created for each PandaID (e.g., `task152-panda7111501056.out`, `task152-panda7111438591.out`)
- Each file contains the SAME content (full task output)
- pilotlog.txt is copied to EACH PandaID directory (contains continuous log for all jobs)
- This ensures users searching by PandaID have complete context

## Installation

### 1. Configure paths in config file
Edit `publish_slurm_logs_config.json` and verify:
- `paths.workdir_root` points to your harvester workdir
- `paths.cfs_destination` points to your CFS www directory
- `paths.split_script` points to split_slurm_output.py

### 2. Create CFS directories
```bash
# Create www directory structure
mkdir -p /global/cfs/cdirs/<nersc project name>/www/panda/jobs

# Set permissions for web access
chmod 755 /global/cfs/cdirs/<nersc project name>/www
chmod 755 /global/cfs/cdirs/<nersc project name>/www/panda
chmod 755 /global/cfs/cdirs/<nersc project name>/www/panda/jobs

# Queue subdirectories will be created automatically by the publisher
```

### 3. Test with dry-run
```bash
python3 publish_slurm_logs.py --dry-run
```

### 4. Set up scrontab
Edit your scrontab:
```bash
scrontab -e
```

For example:
```bash
#SCRON -C cron
#SCRON -q workflow
#SCRON -A <nersc project name>
#SCRON -t 00:30:00
#SCRON --time-min=00:05:00
#SCRON --job-name=slurm-log-publisher
#SCRON -o <path to directory where you install and run the publisher scripts>/scron-output-%j.out
#SCRON --open-mode=append
*/10 * * * * <path to directory where you install and run the publisher scripts>/run_slurm_publisher.sh
```

This runs every 10 minutes. Adjust `*/10` to change frequency.

### 5. Verify scrontab
```bash
scrontab -l
```

## Usage

### Manual Run
```bash
# Run normally
python3 publish_slurm_logs.py

# Dry run (no changes)
python3 publish_slurm_logs.py --dry-run

# Use custom config
python3 publish_slurm_logs.py --config /path/to/config.json
```

### Via Wrapper Script
```bash
# Run via wrapper (same as scrontab)
./run_slurm_publisher.sh
```

### Check Logs
```bash
# View main log
tail -f <path to directory where you install and run the publisher scripts>/publish_slurm_logs.log

# View scrontab output
tail -f <path to directory where you install and run the publisher scripts>/scron-output-*.out
```

### Check State
```bash
# View processed jobs state
cat <path to directory where you install and run the publisher scripts>/.slurm_publish_state.json | jq
```

## Integration with PanDA 

In order to publish these slurm job logs on panda monitor, the following changes are needed:

### 1. in SLURM job template used by a particular panda queue -- define the GTAG value pointing to the corresponding web directory, e.g. 
```
export GTAG="https://portal.nersc.gov/cfs/<nersc project name>/panda/jobs/<panda queue name>"
...
srun --export=HARVESTER_ID,HARVESTER_WORKER_ID,GTAG ...
```

### 2. make sure in the pilot wrapper script, export it to container:
```
echo "export GTAG="$GTAG >> myEnv.sh
```

### 3. use pilot version 3.13.0.23 or later.
