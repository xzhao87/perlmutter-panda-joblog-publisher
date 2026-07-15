# SLURM Log Publisher System

Automated system for publishing SLURM job logs to web-accessible storage in a PanDA-centric directory structure.

## Quick Start

For first-time users, follow these steps:

```bash
# 1. Verify configuration
cat publish_slurm_logs_config.json
# Follow comments to make necessary changes
# See Configuration section below for details

# 2. Test with dry-run
python3 publish_slurm_logs.py --dry-run

# 3. Create web-accessible directories (if not exists)
mkdir -p /path/to/web/directory/panda/jobs
chmod 755 /path/to/web/directory/panda/jobs

# 4. Run manually to test
python3 publish_slurm_logs.py

# 5. Set up cron/scrontab for automation
# Run every 10 minutes:
# */10 * * * * /path/to/run_slurm_publisher.sh

# 6. Verify logs are published
ls -la /path/to/web/directory/panda/jobs/

# 7. Access via web browser
# https://your-web-portal/panda/jobs/<queue_name>/<pandaid>/
```

**See "Installation" section below for detailed setup instructions.**

## Overview

This system automatically:
1. Scans for finished SLURM jobs in harvester work directories
2. **Processes SLURM output** (supports two modes - see below)
3. Extracts PandaIDs and organizes files by PandaID
4. Copies pilotlog.txt from each task directory
5. Copies additional files from failed tasks (payload.stdout, payload.stderr, etc.)
6. Publishes logs to web-accessible storage
7. Processes jobs in parallel with configurable concurrency
8. Cleans up old logs after retention period
9. Tracks processed jobs to avoid re-processing

## SLURM Logging Modes

The publisher supports **two modes** for handling SLURM task output, controlled by the `single_slurm_out_file` configuration option:

### Legacy Mode (single_slurm_out_file: true)

**How it works:**
- All SLURM tasks write to **one big output file**: `slurm-<jobid>.out`
- Publisher splits this file into per-task, per-PandaID files using `split_slurm_output.py`
- Handles interleaved output from multiple concurrent tasks

**When to use:**
- Default SLURM configuration (no special `--output` flags)
- Existing deployments with standard SLURM templates
- When you need backward compatibility

**SLURM template:**
```bash
srun --export=HARVESTER_ID,HARVESTER_WORKER_ID,GTAG \
     --label -n $HARVESTER_NTASKS \
     /bin/bash ./wrapper.sh ...
# Output goes to default slurm-<jobid>.out
```

**Published files per PandaID:**
- `slurm-<jobid>-task<taskid>-panda<pandaid>.out` - Task output for this PandaID
- `slurm-<jobid>-header.out` - Untagged SLURM output (wrapper info)
- `pilotlog.txt` - Complete pilot log

### New Mode (single_slurm_out_file: false)

**How it works:**
- Each SLURM task writes to **separate files**: `slurm<jobid>-task<taskid>.out` and `.err`
- Publisher processes pre-split files directly (no splitting needed)
- Cleaner separation of stdout and stderr

**Benefits:**
- **Memory efficient** - No need to load large files for splitting
- **Faster processing** - Direct file copy instead of split-then-copy
- **Better debugging** - Separate stderr files, no interleaved output
- **Task isolation** - Each task's output in its own files

**When to use:**
- New deployments where you control the SLURM template
- Large-scale operations with many tasks per job
- When you need separate stderr for debugging

**SLURM template:**
```bash
srun --export=HARVESTER_ID,HARVESTER_WORKER_ID,GTAG \
     --output=slurm%j-task%t.out \
     --error=slurm%j-task%t.err \
     -n $HARVESTER_NTASKS \
     /bin/bash ./wrapper.sh ...
# Each task creates separate .out and .err files
```

**Published files per PandaID:**
- `slurm<jobid>-task<taskid>.out` - Task stdout
- `slurm<jobid>-task<taskid>.err` - Task stderr (separate file)
- `slurm-<jobid>.out` - Overall SLURM job info
- `pilotlog-task<taskid>.txt` - Pilot log

**Note:** In both modes, when a single pilot processes multiple PandaIDs, files are copied to each PandaID directory so users have complete context.

## Published Files

For each successfully completed task:
- **SLURM output**: Infrastructure/wrapper/pilot logs
  - Format depends on mode (see above)
  - Duplicated to each PandaID directory when pilot processes multiple jobs
- **pilotlog.txt** or **pilotlog-task<taskid>.txt**: Complete pilot logs
  - Contains payload stdout/stderr and errors
  - Copied to EACH PandaID directory

For failed tasks (containing `PanDA_Pilot-*` directory):
- Additional files copied based on `additional_files_for_failed_tasks` config patterns
- Examples: `payload.stdout`, `payload.stderr`, `workDir/*.csv`

## Components

### Configuration File
**File**: `publish_slurm_logs_config.json`

**Key settings:**

```json
{
  "paths": {
    "workdir_root": "/path/to/harvester/workdir",
    "cfs_destination": "/path/to/web/directory/jobs"
  },
  "timing": {
    "retention_days": 5
  },
  "processing": {
    "single_slurm_out_file": true,
    "split_script": "/path/to/split_slurm_output.py",
    "max_concurrent_jobs": 10,
    "delete_original_splits": true
  },
  "additional_files_for_failed_tasks": [
    "payload.stdout", "payload.stderr", "workDir/*.csv"
  ]
}
```

**Configuration options:**
- `single_slurm_out_file`: Choose logging mode (true=legacy, false=new pre-split)
- `split_script`: Path to split script (only used when `single_slurm_out_file` is true)
- `delete_original_splits`: Clean up split files (only applies in legacy mode)
- `max_concurrent_jobs`: Parallel processing limit (adjust based on I/O load)

### Main Script
**File**: `publish_slurm_logs.py`

**Features:**
- Dual processing modes (legacy split vs. new pre-split)
- Checks if SLURM jobs are finished (not in queue, old enough)
- Extracts PandaIDs and organizes files by PandaID
- Copies pilotlog and additional files
- Sets world-readable permissions for web access
- Parallel processing with multiprocessing
- Lock files prevent concurrent processing of same job
- Dual tracking (marker files + state file) for reliability
- Automatic cleanup of old directories

### Split Script (Legacy Mode Only)
**File**: `split_slurm_output.py`

**Purpose:** Splits combined SLURM output into per-task/per-PandaID files

**Used when:** `single_slurm_out_file: true`

**Not used when:** `single_slurm_out_file: false` (files are already split by SLURM)

### Wrapper Script
**File**: `run_slurm_publisher.sh`

**Purpose:** Wrapper for cron/scrontab automation

**Features:**
- Lock file prevents concurrent runs
- Logging with timestamps
- Error handling

## Directory Structure Examples

### Legacy Mode (single_slurm_out_file: true)

**Input (Harvester workdir):**
```
workdir/
└── <queue_name>/
    └── 11270/                      # Worker directory
        ├── slurm-50685843.out      # One big file with all task output
        ├── 50685843/               # Job directory
        │   ├── 0/                  # Task 0 directory
        │   │   └── pilotlog.txt
        │   ├── 1/                  # Task 1 directory
        │   │   └── pilotlog.txt
        │   └── ...
        └── .publish-done-50685843  # Marker file (created after processing)
```

**Output (Web directory):**
```
jobs/
└── <queue_name>/
    ├── 260789/                                           # PandaID directory
    │   ├── slurm-50685843-task67-panda260789.out        # Split task output
    │   ├── slurm-50685843-header.out                    # Header file
    │   └── pilotlog.txt                                 # Pilot log
    └── 260790/                                           # Another PandaID
        ├── slurm-50685843-task68-panda260790.out
        ├── slurm-50685843-header.out
        └── pilotlog.txt
```

### New Mode (single_slurm_out_file: false)

**Input (Harvester workdir):**
```
workdir/
└── <queue_name>/
    └── 11270/                        # Worker directory
        ├── slurm-55525475.out        # Overall SLURM info only
        ├── slurm55525475-task0.out   # Task 0 stdout
        ├── slurm55525475-task0.err   # Task 0 stderr
        ├── slurm55525475-task1.out   # Task 1 stdout
        ├── slurm55525475-task1.err   # Task 1 stderr
        ├── ...
        ├── 55525475/                 # Job directory
        │   ├── 0/                    # Task 0 directory
        │   │   └── pilotlog.txt
        │   ├── 1/                    # Task 1 directory
        │   │   └── pilotlog.txt
        │   └── ...
        └── .publish-done-55525475    # Marker file
```

**Output (Web directory):**
```
jobs/
└── <queue_name>/
    ├── 1010560/                          # PandaID directory
    │   ├── slurm-55525475.out            # Overall SLURM info
    │   ├── slurm55525475-task10.out      # Task stdout
    │   ├── slurm55525475-task10.err      # Task stderr (separate!)
    │   └── pilotlog-task10.txt           # Pilot log
    └── 1010561/                          # Another PandaID
        ├── slurm-55525475.out
        ├── slurm55525475-task11.out
        ├── slurm55525475-task11.err
        └── pilotlog-task11.txt
```

**Note:** In new mode, stderr is in a separate `.err` file for easier debugging.

## Installation

### 1. Choose Your SLURM Logging Mode

**For legacy mode (default):**
- Use standard SLURM template (no special output flags)
- Set `single_slurm_out_file: true` in config
- Requires `split_script` path in config

**For new mode (recommended for new deployments):**
- Update SLURM template to include:
  ```bash
  --output=slurm%j-task%t.out --error=slurm%j-task%t.err
  ```
- Set `single_slurm_out_file: false` in config
- No split script needed

### 2. Configure Paths

Edit `publish_slurm_logs_config.json`:
```json
{
  "paths": {
    "workdir_root": "/path/to/harvester/workdir",
    "cfs_destination": "/path/to/web/directory/jobs",
    "split_script": "/path/to/split_slurm_output.py",
    "state_file": "/path/to/.slurm_publish_state.json",
    "log_file": "/path/to/publish_slurm_logs.log"
  },
  "processing": {
    "single_slurm_out_file": true
  }
}
```

### 3. Create Web Directories
```bash
# Create directory structure
mkdir -p /path/to/web/directory/panda/jobs

# Set permissions for web access
chmod 755 /path/to/web/directory
chmod 755 /path/to/web/directory/panda
chmod 755 /path/to/web/directory/panda/jobs

# Queue subdirectories created automatically
```

### 4. Test with Dry-Run
```bash
python3 publish_slurm_logs.py --dry-run
# Review log output to verify behavior
```

### 5. Set Up Automation

**Using cron:**
```bash
# Run every 10 minutes
*/10 * * * * /path/to/run_slurm_publisher.sh
```

**Using scrontab (NERSC):**
```bash
scrontab -e

# Add:
#SCRON -C cron
#SCRON -q workflow
#SCRON -A <project>
#SCRON -t 00:30:00
#SCRON --time-min=00:05:00
#SCRON --job-name=slurm-log-publisher
#SCRON -o /path/to/scron-output-%j.out
#SCRON --open-mode=append
*/10 * * * * /path/to/run_slurm_publisher.sh
```

### 6. Verify
```bash
# Check logs are published
ls -la /path/to/web/directory/panda/jobs/

# Verify cron/scrontab is running
scrontab -l  # or: crontab -l
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
# Run via wrapper (same as cron/scrontab)
./run_slurm_publisher.sh
```

### Check Logs
```bash
# View main log
tail -f /path/to/publish_slurm_logs.log

# View cron/scrontab output
tail -f /path/to/scron-output-*.out
```

### Check State
```bash
# View processed jobs state
cat /path/to/.slurm_publish_state.json | jq
```

## Integration with PanDA

To publish SLURM logs on PanDA monitor:

### 1. Set GTAG in SLURM Template

Define GTAG pointing to your web directory:

**Legacy mode:**
```bash
export GTAG="https://your-web-portal/panda/jobs/<queue_name>"
...
srun --export=HARVESTER_ID,HARVESTER_WORKER_ID,GTAG \
     --label -n $HARVESTER_NTASKS \
     /bin/bash ./wrapper.sh ...
```

**New mode:**
```bash
export GTAG="https://your-web-portal/panda/jobs/<queue_name>"
...
srun --export=HARVESTER_ID,HARVESTER_WORKER_ID,GTAG \
     --output=slurm%j-task%t.out \
     --error=slurm%j-task%t.err \
     -n $HARVESTER_NTASKS \
     /bin/bash ./wrapper.sh ...
```

### 2. Export GTAG to Container

In pilot wrapper script:
```bash
echo "export GTAG=$GTAG" >> myEnv.sh
```

### 3. Use Compatible Pilot Version

Requires pilot version 3.13.0.23 or later.
