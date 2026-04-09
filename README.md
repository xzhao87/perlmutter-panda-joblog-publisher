# perlmutter-panda-joblog-publisher

This repo contains scripts for publishing SLURM job logs to CFS (NERSC Science Gateway) in a PanDA-centric directory structure.

## Overview

This system automatically:
1. Scans for finished SLURM jobs in harvester workdirs
2. Splits combined SLURM output into per-task/per-PandaID files
3. Copies pilotlog.txt from each task directory
4. Publishes logs to CFS in date/PandaID organized structure
5. Cleans up old logs after retention period
6. Tracks processed jobs to avoid re-processing

## Published Files

For each successfully completed task, the publisher copies:
- **SLURM split output**: Infrastructure/wrapper/pilot logs from SLURM stdout/stderr
- **pilotlog.txt**: Complete pilot logs including payload stdout/stderr and errors

These complement each other:
- SLURM output: Best for debugging wrapper scripts, environment setup, SLURM issues
- pilotlog.txt: Best for debugging payload failures, application errors 

## Components

### Configuration File
- **File**: `publish_slurm_logs_config.json`
- **Purpose**: Central configuration for paths, timing, and behavior
- **Key settings**:
  - `paths.workdir_root`: Where harvester places worker directories
  - `paths.cfs_destination`: CFS directory for published logs
  - `timing.scan_frequency_minutes`: How often to scan (for documentation)
  - `timing.retention_days`: How long to keep published logs
  - `processing.split_script`: Path to split_slurm_output.py

### Main Script
- **File**: `publish_slurm_logs.py`
- **Purpose**: Core logic for scanning, splitting, and publishing
- **Features**:
  - Checks if SLURM jobs are finished (not in queue, old enough)
  - Splits SLURM output using split_slurm_output.py
  - Organizes files by job execution date (from SLURM file mtime) and PandaID
  - Sets world-readable permissions (0o644) for web access
  - Maintains state to avoid re-processing
  - Automatic cleanup of old directories

### Wrapper Script (for scrontab)
- **File**: `run_slurm_publisher.sh`
- **Purpose**: Wrapper for running via NERSC scrontab
  - Make sure the SPLIT_DIR is correctly set inside it. 

## Installation

### 1. Configure paths in config file
Edit `publish_slurm_logs_config.json` and verify:
- `paths.workdir_root` points to your harvester workdir
- `paths.cfs_destination` points to your CFS www directory
- `paths.split_script` points to split_slurm_output.py

### 2. Create web visible directories on NERSC CFS 

Refer to the NERSC doc at :
https://docs.nersc.gov/services/science-gateways/#how-to-publish-your-data-on-ngf-to-the-web

Note you only need to create the top directory, queue subdirectories (and all its subdirs) will be created automatically by the publisher.

### 3. Test with dry-run
```bash
python3 publish_slurm_logs.py --dry-run
```


