#!/usr/bin/env python3.11
"""
Automated SLURM Log Publisher for PanDA

This script scans for finished SLURM jobs and publishes their pre-split task
output to CFS for web access via NERSC Science Gateway.

File structure in CFS:
  $CFS/www/panda/workers/<queue_name>/<panda_id>/slurm-<jobid>-task<taskid>-panda<pandaid>.out
  $CFS/www/panda/workers/<queue_name>/<panda_id>/slurm-<jobid>-header.out
  $CFS/www/panda/workers/<queue_name>/<panda_id>/pilotlog.txt

Usage:
  python3 publish_slurm_logs.py [--config CONFIG_FILE] [--dry-run]

Key Features:
  - Parallel processing of multiple SLURM jobs with configurable concurrency
  - Workspace-local SQLite state; /pscratch is always read-only
  - One squeue query per publisher cycle
  - Copy-once and CFS hard-link deduplication for shared content
  - Existing JSON state and marker files are migration inputs only
  - Handles pilots that process multiple PandaIDs correctly
  - Copies failed task files (payload.stdout, etc.) based on configurable patterns
  - Extracts PandaID from PanDA_Pilot-<pandaid> to avoid duplicating failed files
  - Sets proper file permissions (0o644) for web accessibility
  - Retention cleanup is handled by cleanup_slurm_logs.py
  - Copies header files and pilotlog.txt to each PandaID directory
"""

import os
import sys
sys.dont_write_bytecode = True
import json
import re
import shutil
import subprocess
import argparse
import multiprocessing
import getpass
import uuid
from pathlib import Path
from datetime import datetime
import logging

from slurm_publish_state import PublisherState, WorkspaceLock


def _process_job_wrapper(config, queue_name, worker_dir, item, job_id, dry_run):
    """
    Wrapper function for multiprocessing.
    
    Creates a new SlurmLogPublisher instance and processes a single job.
    This is needed because multiprocessing requires picklable functions.
    """
    # Create a new publisher instance for this process
    # Note: We can't pass the publisher instance directly due to pickling issues with loggers
    class MinimalPublisher:
        def __init__(self, cfg):
            self.config = cfg
            self.dry_run = dry_run
            # Setup minimal logging for this process
            log_file = cfg['paths']['log_file']
            handlers = [logging.StreamHandler(sys.stdout)]
            if not dry_run:
                handlers.insert(0, logging.FileHandler(log_file))
            logging.basicConfig(
                level=logging.INFO,
                format=f'%(asctime)s [%(levelname)s] [PID:{os.getpid()}] %(message)s',
                handlers=handlers,
                force=True,
            )
            self.logger = logging.getLogger(f"{__name__}.worker.{job_id}")
        
        # Copy necessary methods from SlurmLogPublisher
        _split_slurm_output = SlurmLogPublisher._split_slurm_output
        _publish_files = SlurmLogPublisher._publish_files
        _process_single_job = SlurmLogPublisher._process_single_job
        _extract_panda_id = SlurmLogPublisher._extract_panda_id
        _extract_task_id = SlurmLogPublisher._extract_task_id
        _extract_job_id = SlurmLogPublisher._extract_job_id
        _task_has_multiple_panda_ids = SlurmLogPublisher._task_has_multiple_panda_ids
        _is_failed_task = SlurmLogPublisher._is_failed_task
        _copy_additional_files_for_failed_task = SlurmLogPublisher._copy_additional_files_for_failed_task
        _find_presplit_task_files = SlurmLogPublisher._find_presplit_task_files
        _extract_panda_ids_from_file = SlurmLogPublisher._extract_panda_ids_from_file
        _publish_presplit_files = SlurmLogPublisher._publish_presplit_files
        _copy_once_and_link = SlurmLogPublisher._copy_once_and_link
        _destination_matches_source = SlurmLogPublisher._destination_matches_source
    
    publisher = MinimalPublisher(config)
    return publisher._process_single_job(queue_name, worker_dir, item, job_id, dry_run)


class SlurmLogPublisher:
    """Manages automated publishing of SLURM logs to CFS"""
    
    def __init__(self, config_path, dry_run=False):
        """Initialize with configuration file"""
        self.config = self._load_config(config_path)
        self.dry_run = dry_run
        self._setup_logging(dry_run=dry_run)
        paths = self.config['paths']
        legacy_state = paths.get('state_file')
        self.state_db = PublisherState(
            paths['state_db'], legacy_json_path=legacy_state, dry_run=dry_run
        )
        self.process_lock = WorkspaceLock(paths['lock_file'], enabled=not dry_run)
        
    def _load_config(self, config_path):
        """Load JSON configuration"""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _setup_logging(self, dry_run=False):
        """Configure logging"""
        log_file = self.config['paths']['log_file']
        handlers = [logging.StreamHandler(sys.stdout)]
        if not dry_run:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            handlers.insert(0, logging.FileHandler(log_file))
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=handlers,
            force=True,
        )
        self.logger = logging.getLogger(__name__)
    
    def _is_job_finished(self, slurm_job_dir, active_job_ids, squeue_ok=True):
        """
        Check if a SLURM job is finished.
        Returns: (is_finished, job_id). ``is_finished`` is None when the
        worker directory disappeared and its stale DB row should be removed.
        
        A job is considered finished if:
        1. slurm-<jobid>.out file exists
        2. Job is not in squeue (RUNNING/PENDING)
        3. Job directory is older than min_job_age_seconds
        """
        if not squeue_ok:
            return False, None

        # One name-only directory pass avoids Path.glob() metadata calls.
        main_file = None
        job_id = None
        try:
            with os.scandir(slurm_job_dir) as entries:
                for entry in entries:
                    match = re.fullmatch(r'slurm-(\d+)\.out', entry.name)
                    if match:
                        main_file = entry.path
                        job_id = match.group(1)
                        break
                    if job_id is None:
                        task_match = re.fullmatch(r'slurm(\d+)-task\d+\.out', entry.name)
                        if task_match:
                            job_id = task_match.group(1)
        except FileNotFoundError:
            self.logger.info(f"Worker directory no longer exists: {slurm_job_dir}")
            return None, None
        except OSError as exc:
            self.logger.warning(f"Could not inspect worker directory {slurm_job_dir}: {exc}")
            return False, None

        # The pre-split layout still has the main output. Do not publish until
        # it appears, since it is also one of the published artifacts.
        if not main_file or not job_id:
            return False, None
        
        # Check if job is old enough
        min_age = self.config['filters']['min_job_age_seconds']
        try:
            file_age = datetime.now().timestamp() - os.stat(main_file).st_mtime
        except FileNotFoundError:
            self.logger.info(f"Worker directory disappeared while inspecting: {slurm_job_dir}")
            return None, job_id
        except OSError:
            return False, job_id
        if file_age < min_age:
            self.logger.debug(f"Job {job_id} too recent ({file_age:.0f}s < {min_age}s)")
            return False, job_id
        
        if job_id in active_job_ids:
            self.logger.debug(f"Job {job_id} still in queue")
            return False, job_id
        
        return True, job_id

    def _get_active_slurm_jobs(self):
        """Return (success, active_job_ids) using exactly one squeue call."""
        user = getpass.getuser()
        try:
            result = subprocess.run(
                ['squeue', '-h', '-u', user, '-o', '%A'],
                capture_output=True,
                text=True,
                timeout=20,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            self.logger.error(f"Unable to query squeue once for this cycle: {exc}")
            return False, set()
        if result.returncode != 0:
            self.logger.error(
                f"squeue failed with exit code {result.returncode}: {result.stderr.strip()}"
            )
            return False, set()
        active = {line.strip() for line in result.stdout.splitlines() if line.strip()}
        self.logger.info(f"Single squeue query found {len(active)} active jobs")
        return True, active
    
    def _split_slurm_output(self, slurm_file):
        """
        Split SLURM output file into per-task files.
        Returns list of created files.
        """
        split_script = self.config['processing']['split_script']
        
        if not os.path.exists(split_script):
            self.logger.error(f"Split script not found: {split_script}")
            return []
        
        try:
            self.logger.info(f"Splitting {slurm_file}...")
            result = subprocess.run(
                ['python3', split_script, str(slurm_file)],
                capture_output=True,
                text=True,
                timeout=600
            )
            
            if result.returncode != 0:
                self.logger.error(f"Split failed: {result.stderr}")
                return []
            
            # Find created files
            slurm_dir = os.path.dirname(slurm_file)
            job_id = re.search(r'slurm-(\d+)\.out', os.path.basename(slurm_file)).group(1)
            
            task_files = list(Path(slurm_dir).glob(f'slurm-{job_id}-task*.out'))
            header_files = list(Path(slurm_dir).glob(f'slurm-{job_id}-header.out'))
            
            all_files = task_files + header_files
            self.logger.info(f"Created {len(all_files)} split files")
            return all_files
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Split timeout for {slurm_file}")
            return []
        except Exception as e:
            self.logger.error(f"Split error: {e}")
            return []
    
    def _find_presplit_task_files(self, worker_dir, job_id):
        """
        Find pre-split task files when SLURM writes separate output per task.
        
        When single_slurm_out_file is false, SLURM writes each task to:
          - slurm<jobid>-task<taskid>.out (stdout)
          - slurm<jobid>-task<taskid>.err (stderr)
        
        Returns: list of (task_out_file, task_err_file) tuples
        """
        task_files = []
        
        # Find all task .out files
        pattern = f'slurm{job_id}-task*.out'
        out_files = list(Path(worker_dir).glob(pattern))
        
        for out_file in out_files:
            # Extract task ID from filename
            match = re.search(r'slurm\d+-task(\d+)\.out$', out_file.name)
            if not match:
                continue
            
            task_id = match.group(1)
            
            # Find corresponding .err file
            err_file = out_file.with_suffix('.err')
            if not err_file.exists():
                self.logger.warning(f"Missing .err file for task {task_id}: {err_file}")
                # Continue anyway, stderr might be empty
            
            task_files.append((str(out_file), str(err_file) if err_file.exists() else None))
        
        self.logger.info(f"Found {len(task_files)} pre-split task files for job {job_id}")
        return task_files
    
    def _extract_panda_ids_from_file(self, file_path):
        """
        Extract all PandaIDs from a task output file.
        
        A single pilot/task can process multiple PandaID jobs during its lifetime.
        This function finds all unique PandaIDs mentioned in the file.
        
        Returns: list of PandaID strings (in order of first appearance)
        """
        panda_ids = []
        seen = set()
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    # Look for PandaID in various formats
                    # Common patterns: 'PandaID': 1010560, "PandaID": 1010560, PandaID=1010560
                    matches = re.findall(r'["\']?PandaID["\']?\s*[:=]\s*(\d+)', line)
                    for panda_id in matches:
                        if panda_id not in seen:
                            panda_ids.append(panda_id)
                            seen.add(panda_id)
        except Exception as e:
            self.logger.warning(f"Error reading {file_path}: {e}")
        
        return panda_ids
    
    def _extract_panda_id(self, filename):
        """
        Extract PandaID from filename.
        Format: slurm-<jobid>-task<taskid>-panda<pandaid>.out
        Returns: panda_id or None
        """
        match = re.search(r'-panda(\d+)\.out$', filename)
        return match.group(1) if match else None
    
    def _extract_task_id(self, filename):
        """
        Extract task ID from filename.
        Format: slurm-<jobid>-task<taskid>-panda<pandaid>.out
        Returns: task_id or None
        """
        match = re.search(r'-task(\d+)-panda\d+\.out$', filename)
        return match.group(1) if match else None
    
    def _task_has_multiple_panda_ids(self, split_files, task_id):
        """
        Check if a task processed multiple PandaID jobs.
        
        When a pilot processes multiple PandaIDs, there will be multiple
        files with the same task ID but different PandaIDs.
        
        Returns: True if task has multiple PandaIDs, False otherwise
        """
        if not task_id:
            return False
        
        panda_ids_for_task = set()
        for split_file in split_files:
            filename = os.path.basename(split_file)
            t_id = self._extract_task_id(filename)
            if t_id == task_id:
                panda_id = self._extract_panda_id(filename)
                if panda_id:
                    panda_ids_for_task.add(panda_id)
        
        return len(panda_ids_for_task) > 1
    
    def _extract_job_id(self, filename):
        """
        Extract SLURM job ID from filename.
        Format: slurm-<jobid>-task<taskid>-panda<pandaid>.out
        Returns: job_id or None
        """
        match = re.search(r'^slurm-(\d+)-', filename)
        return match.group(1) if match else None
    
    def _is_failed_task(self, task_dir):
        """
        Check if a task is a failed/incomplete task.
        A task is considered failed if it contains a PanDA_Pilot-* subdirectory.
        
        Returns: (is_failed, pilot_dir_path, pilot_panda_id)
            - is_failed: True if PanDA_Pilot-* directory exists
            - pilot_dir_path: Full path to PanDA_Pilot-* directory (or None)
            - pilot_panda_id: PandaID extracted from directory name (or None)
        """
        if not os.path.isdir(task_dir):
            return False, None, None
        
        # Look for PanDA_Pilot-* directories
        try:
            for item in os.listdir(task_dir):
                item_path = os.path.join(task_dir, item)
                if os.path.isdir(item_path) and item.startswith('PanDA_Pilot-'):
                    # Extract PandaID from directory name: PanDA_Pilot-<pandaid>
                    # e.g., "PanDA_Pilot-7111501056" -> "7111501056"
                    pilot_panda_id = item.replace('PanDA_Pilot-', '')
                    return True, item_path, pilot_panda_id
        except OSError as e:
            self.logger.warning(f"Could not check for failed task in {task_dir}: {e}")
        
        return False, None, None
    
    def _copy_additional_files_for_failed_task(self, task_dir, panda_id, queue_dir, dry_run=False):
        """
        Copy additional files from failed tasks to CFS.
        
        For failed tasks (containing PanDA_Pilot-* subdirectory), copy
        additional files as specified in config's additional_files_for_failed_tasks.
        
        Only copy to the CORRECT PandaID directory! When a pilot processes
        multiple PandaIDs, the PanDA_Pilot-<pandaid> directory name tells us which
        PandaID the failed files belong to. Only copy if panda_id matches the
        pilot directory's PandaID.
        
        Args:
            task_dir: Path to task directory (e.g., worker_dir/job_id/task_id)
            panda_id: PandaID from the split file being processed
            queue_dir: CFS queue directory
            dry_run: Whether to do dry-run
        
        Returns: number of files copied
        """
        is_failed, pilot_dir, pilot_panda_id = self._is_failed_task(task_dir)
        if not is_failed:
            return 0
        
        # Check if this panda_id matches the pilot directory's PandaID
        # Only copy failed task files to the CORRECT PandaID directory
        if pilot_panda_id and pilot_panda_id != panda_id:
            self.logger.debug(
                f"Skipping failed task files for PandaID {panda_id}: "
                f"pilot directory indicates files belong to PandaID {pilot_panda_id}"
            )
            return 0
        
        # Get config for additional files
        additional_files_config = self.config.get('additional_files_for_failed_tasks', [])
        if not additional_files_config:
            return 0
        
        self.logger.info(
            f"Task {task_dir} is a failed task (pilot PandaID={pilot_panda_id}), "
            f"copying additional files to PandaID {panda_id}..."
        )
        
        panda_dir = os.path.join(queue_dir, panda_id)
        copied_count = 0
        
        # Process each configured file pattern
        for file_spec in additional_files_config:
            src_dir_pattern = file_spec.get('src_dir', '')
            files_pattern = file_spec.get('files', '')
            
            if not src_dir_pattern or not files_pattern:
                continue
            
            # Find matching source directories using glob
            src_dir_glob = os.path.join(task_dir, src_dir_pattern)
            matching_dirs = list(Path(task_dir).glob(src_dir_pattern))
            
            for src_dir in matching_dirs:
                if not src_dir.is_dir():
                    continue
                
                # For each file pattern (comma-separated)
                file_patterns = [p.strip() for p in files_pattern.split(',')]
                
                for file_pattern in file_patterns:
                    # Find matching files and directories
                    matching_items = list(src_dir.glob(file_pattern))
                    
                    for src_item in matching_items:
                        if src_item.is_file():
                            # Handle individual files
                            # Calculate relative path from task_dir to maintain structure
                            try:
                                rel_path = src_item.relative_to(task_dir)
                                dest_file = os.path.join(panda_dir, rel_path)
                                dest_dir = os.path.dirname(dest_file)
                                
                                if dry_run:
                                    self.logger.info(f"[DRY-RUN] Would copy failed task file: {src_item} -> {dest_file}")
                                    copied_count += 1
                                else:
                                    try:
                                        os.makedirs(dest_dir, exist_ok=True)
                                        shutil.copy2(src_item, dest_file)
                                        os.chmod(dest_file, 0o644)
                                        self.logger.debug(f"Copied failed task file: {rel_path}")
                                        copied_count += 1
                                    except Exception as e:
                                        self.logger.warning(f"Failed to copy {src_item}: {e}")
                            except ValueError as e:
                                self.logger.warning(f"Could not compute relative path for {src_item}: {e}")
                        
                        elif src_item.is_dir():
                            # Recursively copy directory and all its contents
                            try:
                                rel_path = src_item.relative_to(task_dir)
                                dest_dir_path = os.path.join(panda_dir, rel_path)
                                
                                if dry_run:
                                    # Count files in directory for accurate reporting
                                    file_count = sum(1 for _ in src_item.rglob('*') if _.is_file())
                                    self.logger.info(f"[DRY-RUN] Would recursively copy directory: {src_item} -> {dest_dir_path} ({file_count} files)")
                                    copied_count += file_count
                                else:
                                    # Walk directory tree and copy each file
                                    for root, dirs, files in os.walk(src_item):
                                        for file in files:
                                            src_file_path = os.path.join(root, file)
                                            rel_file_path = Path(src_file_path).relative_to(task_dir)
                                            dest_file_path = os.path.join(panda_dir, rel_file_path)
                                            dest_file_dir = os.path.dirname(dest_file_path)
                                            
                                            try:
                                                os.makedirs(dest_file_dir, exist_ok=True)
                                                shutil.copy2(src_file_path, dest_file_path)
                                                os.chmod(dest_file_path, 0o644)
                                                self.logger.debug(f"Copied failed task file: {rel_file_path}")
                                                copied_count += 1
                                            except Exception as e:
                                                self.logger.warning(f"Failed to copy {src_file_path}: {e}")
                            except ValueError as e:
                                self.logger.warning(f"Could not compute relative path for {src_item}: {e}")
        
        if copied_count > 0:
            self.logger.info(f"Copied {copied_count} additional files from failed task {task_dir}")
        
        return copied_count

    def _destination_matches_source(self, src, dest):
        """Cheap idempotency check for files previously copied with copy2()."""
        try:
            src_stat = os.stat(src, follow_symlinks=True)
            dest_stat = os.stat(dest, follow_symlinks=True)
            return (
                src_stat.st_size == dest_stat.st_size
                and src_stat.st_mtime_ns == dest_stat.st_mtime_ns
            )
        except OSError:
            return False

    def _copy_once_and_link(self, src, destinations, dry_run=False, label="file"):
        """Copy a source to CFS once, then hard-link duplicate destinations.

        All destinations must be on the CFS filesystem. If hard-link creation
        is unavailable, the fallback copies the canonical CFS file and never
        reopens the /pscratch source.
        """
        unique_destinations = list(dict.fromkeys(str(path) for path in destinations))
        if not unique_destinations:
            return 0

        canonical = unique_destinations[0]
        if dry_run:
            self.logger.info(f"[DRY-RUN] Would copy canonical {label}: {src} -> {canonical}")
            for dest in unique_destinations[1:]:
                self.logger.info(
                    f"[DRY-RUN] Would hard-link {label}: {canonical} -> {dest} "
                    f"(CFS-copy fallback)"
                )
            return len(unique_destinations)

        for dest in unique_destinations:
            os.makedirs(os.path.dirname(dest), exist_ok=True)

        if not self._destination_matches_source(src, canonical):
            temp_path = f"{canonical}.tmp.{os.getpid()}.{uuid.uuid4().hex}"
            try:
                shutil.copy2(src, temp_path)
                os.chmod(temp_path, 0o644)
                os.replace(temp_path, canonical)
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        for dest in unique_destinations[1:]:
            if self._destination_matches_source(src, dest):
                continue
            temp_path = f"{dest}.tmp.{os.getpid()}.{uuid.uuid4().hex}"
            try:
                try:
                    os.link(canonical, temp_path)
                    self.logger.debug(f"Hard-linked CFS {label}: {canonical} -> {dest}")
                except OSError as link_error:
                    shutil.copy2(canonical, temp_path)
                    self.logger.warning(
                        f"CFS hard link failed for {dest}: {link_error}; used CFS copy"
                    )
                os.chmod(temp_path, 0o644)
                os.replace(temp_path, dest)
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        return len(unique_destinations)
    
    def _publish_files(self, split_files, queue_name, worker_dir, job_id, dry_run=False):
        """
        Publish split files and pilotlog.txt to CFS in organized structure.
        
        Structure:
          <queue_name>/<panda_id>/slurm-<jobid>-task<taskid>-panda<pandaid>.out
          <queue_name>/<panda_id>/slurm-<jobid>-header.out
          <queue_name>/<panda_id>/pilotlog.txt
        
        Only publishes files from tasks that have PandaIDs.
        Copies header file to each PandaID directory.
        If NO tasks have PandaIDs, skip the entire job.
        If SOME tasks have PandaIDs, publish only those + header to each PandaID dir.
        """
        cfs_root = self.config['paths']['cfs_destination']
        queue_dir = os.path.join(cfs_root, queue_name)
        
        # Separate task files from header files
        task_files = []
        header_files = []
        
        for split_file in split_files:
            filename = os.path.basename(split_file)
            if '-header.out' in filename:
                header_files.append(split_file)
            else:
                task_files.append(split_file)
        
        # Check which task files have PandaIDs
        files_with_panda_id = []
        files_without_panda_id = []
        
        for task_file in task_files:
            filename = os.path.basename(task_file)
            panda_id = self._extract_panda_id(filename)
            if panda_id:
                files_with_panda_id.append(task_file)
            else:
                files_without_panda_id.append(task_file)
        
        # If NO tasks have PandaIDs, skip publishing entirely
        if not files_with_panda_id:
            self.logger.warning(
                f"No PandaIDs found in any task files "
                f"({len(files_without_panda_id)} tasks without PandaID). "
                f"Skipping publication for this job."
            )
            return 0
        
        # Log partial task case
        if files_without_panda_id:
            self.logger.info(
                f"Partial PandaID coverage: {len(files_with_panda_id)} tasks with PandaID, "
                f"{len(files_without_panda_id)} tasks without PandaID. "
                f"Publishing only tasks with PandaIDs."
            )
        
        # Collect unique PandaIDs for header file distribution
        unique_panda_ids = set()
        for split_file in files_with_panda_id:
            filename = os.path.basename(split_file)
            panda_id = self._extract_panda_id(filename)
            if panda_id:
                unique_panda_ids.add(panda_id)
        
        # Publish task files to their respective PandaID directories
        published_count = 0
        
        for split_file in files_with_panda_id:
            filename = os.path.basename(split_file)
            panda_id = self._extract_panda_id(filename)
            if not panda_id:
                self.logger.warning(f"No PandaID found in {filename}, skipping")
                continue
            
            panda_dir = os.path.join(queue_dir, panda_id)
            dest_path = os.path.join(panda_dir, filename)
            
            # Publish file
            if dry_run:
                self.logger.info(f"[DRY-RUN] Would copy: {split_file} -> {dest_path}")
                published_count += 1
            else:
                try:
                    os.makedirs(panda_dir, exist_ok=True)
                    shutil.copy2(split_file, dest_path)
                    # Set world-readable permissions (rw-r--r--)
                    os.chmod(dest_path, 0o644)
                    self.logger.debug(f"Published: {filename}")
                    published_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to publish {filename}: {e}")
        
        # Copy header file(s) to each PandaID directory
        header_count = 0
        if header_files:
            for header_file in header_files:
                header_filename = os.path.basename(header_file)
                for panda_id in unique_panda_ids:
                    panda_dir = os.path.join(queue_dir, panda_id)
                    header_dest = os.path.join(panda_dir, header_filename)
                    
                    if dry_run:
                        self.logger.info(f"[DRY-RUN] Would copy header: {header_file} -> {header_dest}")
                        header_count += 1
                    else:
                        try:
                            os.makedirs(panda_dir, exist_ok=True)
                            shutil.copy2(header_file, header_dest)
                            os.chmod(header_dest, 0o644)
                            self.logger.debug(f"Published header to PandaID {panda_id}: {header_filename}")
                            header_count += 1
                        except Exception as e:
                            self.logger.error(f"Failed to publish header to {panda_id}: {e}")
        
        # Also copy pilotlog.txt files for each task with PandaID
        # Since pilotlog.txt is continuous across all PandaIDs in a pilot,
        # copy the SAME pilotlog.txt to EACH PandaID directory (like slurm output duplication)
        pilotlog_count = 0
        
        # Group files by task_id to avoid redundant pilotlog.txt copies
        tasks_processed = set()
        
        for split_file in files_with_panda_id:
            filename = os.path.basename(split_file)
            panda_id = self._extract_panda_id(filename)
            task_id = self._extract_task_id(filename)
            
            if not panda_id or not task_id:
                continue
            
            # Construct path to pilotlog.txt: worker_dir/job_id/task_id/pilotlog.txt
            pilotlog_source = os.path.join(worker_dir, job_id, task_id, 'pilotlog.txt')
            
            if os.path.exists(pilotlog_source):
                panda_dir = os.path.join(queue_dir, panda_id)
                pilotlog_dest = os.path.join(panda_dir, 'pilotlog.txt')
                
                if dry_run:
                    self.logger.info(f"[DRY-RUN] Would copy pilotlog: {pilotlog_source} -> {pilotlog_dest}")
                    pilotlog_count += 1
                else:
                    try:
                        os.makedirs(panda_dir, exist_ok=True)
                        shutil.copy2(pilotlog_source, pilotlog_dest)
                        # Set world-readable permissions (rw-r--r--)
                        os.chmod(pilotlog_dest, 0o644)
                        self.logger.debug(f"Published pilotlog for task {task_id} -> PandaID {panda_id}")
                        pilotlog_count += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to copy pilotlog for task {task_id}: {e}")
            else:
                self.logger.debug(f"pilotlog.txt not found for task {task_id}: {pilotlog_source}")
        
        # Copy additional files from failed tasks
        failed_task_files_count = 0
        for split_file in files_with_panda_id:
            filename = os.path.basename(split_file)
            panda_id = self._extract_panda_id(filename)
            task_id = self._extract_task_id(filename)
            
            if not panda_id or not task_id:
                continue
            
            # Check if this task is a failed task and copy additional files
            task_dir = os.path.join(worker_dir, job_id, task_id)
            if os.path.isdir(task_dir):
                copied = self._copy_additional_files_for_failed_task(
                    task_dir, panda_id, queue_dir, dry_run=dry_run
                )
                failed_task_files_count += copied
        
        self.logger.info(
            f"Published {published_count} task files + {header_count} header copies + "
            f"{pilotlog_count} pilotlogs + {failed_task_files_count} failed task files to {queue_dir} "
            f"({len(unique_panda_ids)} unique PandaIDs)"
        )
        return published_count + header_count + pilotlog_count + failed_task_files_count
    
    def _publish_presplit_files(self, task_files, queue_name, worker_dir, job_id, dry_run=False):
        """
        Publish pre-split task files to CFS.
        
        When single_slurm_out_file is false, SLURM writes each task to separate files:
          - slurm<jobid>-task<taskid>.out (stdout)
          - slurm<jobid>-task<taskid>.err (stderr)
        
        For each task file with PandaIDs:
        1. Extract ALL PandaIDs from the .out file (a task can process multiple jobs)
        2. Copy the .out file to each PandaID directory
        3. Copy the corresponding .err file to each PandaID directory
        4. Copy the overall slurm-<jobid>.out file to each PandaID directory
        5. Copy pilotlog.txt to each PandaID directory
        6. Copy additional files for failed tasks (if configured)
        
        Args:
            task_files: list of (task_out_file, task_err_file) tuples
            queue_name: PanDA queue name
            worker_dir: harvester worker directory
            job_id: SLURM job ID
            dry_run: if True, only log what would be done
        
        Returns: (number of files published, sorted list of PandaIDs)
        """
        cfs_root = self.config['paths']['cfs_destination']
        queue_dir = os.path.join(cfs_root, queue_name)
        
        # Overall slurm job output file (contains generic SLURM info)
        slurm_main_out = os.path.join(worker_dir, f'slurm-{job_id}.out')
        
        # Track each source task once. This prevents duplicate /pscratch reads
        # when one pilot handled multiple PandaIDs.
        task_records = []
        tasks_without_panda = []
        
        # Process each task file to extract PandaIDs
        for task_out_file, task_err_file in task_files:
            # Extract task ID from filename
            match = re.search(r'slurm\d+-task(\d+)\.out$', os.path.basename(task_out_file))
            if not match:
                continue
            task_id = match.group(1)
            
            # Extract PandaIDs from task output
            panda_ids = self._extract_panda_ids_from_file(task_out_file)
            
            if not panda_ids:
                tasks_without_panda.append(task_id)
                continue
            
            task_records.append((task_id, task_out_file, task_err_file, panda_ids))
        
        # If NO tasks have PandaIDs, skip publishing
        if not task_records:
            self.logger.warning(
                f"No PandaIDs found in any task files "
                f"({len(tasks_without_panda)} tasks without PandaID). "
                f"Skipping publication for this job."
            )
            return 0, []
        
        # Log partial task case
        if tasks_without_panda:
            self.logger.info(
                f"Partial PandaID coverage: {len(task_records)} tasks with PandaIDs, "
                f"{len(tasks_without_panda)} tasks without PandaID. "
                f"Publishing only tasks with PandaIDs."
            )
        
        unique_panda_ids = sorted({
            panda_id
            for _, _, _, panda_ids in task_records
            for panda_id in panda_ids
        })

        # Publish each distinct source once, then link duplicate CFS targets.
        published_count = 0

        for task_id, task_out_file, task_err_file, panda_ids in task_records:
            out_destinations = [
                os.path.join(queue_dir, panda_id, f'slurm{job_id}-task{task_id}.out')
                for panda_id in panda_ids
            ]
            published_count += self._copy_once_and_link(
                task_out_file, out_destinations, dry_run=dry_run,
                label=f"task {task_id} stdout",
            )

            if task_err_file and os.path.exists(task_err_file):
                err_destinations = [
                    os.path.join(queue_dir, panda_id, f'slurm{job_id}-task{task_id}.err')
                    for panda_id in panda_ids
                ]
                published_count += self._copy_once_and_link(
                    task_err_file, err_destinations, dry_run=dry_run,
                    label=f"task {task_id} stderr",
                )

            pilotlog_source = os.path.join(worker_dir, job_id, task_id, 'pilotlog.txt')
            if os.path.exists(pilotlog_source):
                pilotlog_destinations = [
                    os.path.join(queue_dir, panda_id, f'pilotlog-task{task_id}.txt')
                    for panda_id in panda_ids
                ]
                published_count += self._copy_once_and_link(
                    pilotlog_source, pilotlog_destinations, dry_run=dry_run,
                    label=f"task {task_id} pilotlog",
                )

            task_dir = os.path.join(worker_dir, job_id, task_id)
            for panda_id in panda_ids:
                published_count += self._copy_additional_files_for_failed_task(
                    task_dir, panda_id, queue_dir, dry_run=dry_run
                )

        if os.path.exists(slurm_main_out):
            main_destinations = [
                os.path.join(queue_dir, panda_id, f'slurm-{job_id}.out')
                for panda_id in unique_panda_ids
            ]
            published_count += self._copy_once_and_link(
                slurm_main_out, main_destinations, dry_run=dry_run,
                label="main SLURM output",
            )
        
        self.logger.info(
            f"Published {published_count} files to {queue_dir} "
            f"({len(unique_panda_ids)} unique PandaIDs)"
        )
        return published_count, unique_panda_ids
    
    def _process_single_job(self, queue_name, worker_dir, item, job_id, dry_run=False):
        """
        Process a single SLURM job: split output and publish to CFS.
        This method is designed to be called by multiple processes in parallel.
        
        Returns: (success, published_count, panda_ids)
        """
        # The parent process handles SQLite state and the workspace lock.
        # Worker processes never write state or lock files under /pscratch.
        self.logger.info(f"Processing queue={queue_name} worker={item} job={job_id}")
        single_slurm_out_file = self.config['processing'].get('single_slurm_out_file', True)

        if single_slurm_out_file:
            # Legacy mode is preserved, but a legacy dry-run is refused because
            # the existing splitter writes output beside the source file.
            if dry_run:
                self.logger.error(
                    "Legacy-mode dry-run is disabled because the legacy splitter "
                    "is not guaranteed to be read-only"
                )
                return False, 0, []
            slurm_file = os.path.join(worker_dir, f'slurm-{job_id}.out')
            if not os.path.exists(slurm_file):
                self.logger.warning(f"SLURM output not found: {slurm_file}")
                return False, 0, []
            split_files = self._split_slurm_output(slurm_file)
            if not split_files:
                self.logger.warning(f"No split files created for job {job_id}")
                return False, 0, []
            pub_count = self._publish_files(
                split_files, queue_name, worker_dir, job_id, dry_run=dry_run
            )
            panda_ids = sorted({
                self._extract_panda_id(os.path.basename(path))
                for path in split_files
                if self._extract_panda_id(os.path.basename(path))
            })
            if self.config['processing']['delete_original_splits']:
                for path in split_files:
                    try:
                        os.remove(path)
                    except Exception as exc:
                        self.logger.warning(f"Could not remove {path}: {exc}")
        else:
            task_files = self._find_presplit_task_files(worker_dir, job_id)
            if not task_files:
                self.logger.warning(f"No task files found for job {job_id}")
                return False, 0, []
            pub_count, panda_ids = self._publish_presplit_files(
                task_files, queue_name, worker_dir, job_id, dry_run=dry_run
            )

        if pub_count == 0:
            self.logger.info(
                f"Job {job_id}: No files published (no PandaIDs found). "
                f"Marking as processed to avoid reprocessing."
            )
        return True, pub_count, panda_ids
    
    def _collect_queue_jobs(self, queue_name, active_job_ids, squeue_ok,
                            worker_filters=None):
        """Collect eligible jobs without running per-worker Slurm commands."""
        workdir_root = self.config['paths']['workdir_root']
        queue_dir = os.path.join(workdir_root, queue_name)
        if not os.path.exists(queue_dir):
            self.logger.warning(f"Queue directory not found: {queue_dir}")
            return []

        jobs_to_process = []
        discovery_high_water = self.state_db.highest_worker_id(queue_name)
        if worker_filters:
            items = sorted(set(str(item) for item in worker_filters), key=int, reverse=True)
        else:
            with os.scandir(queue_dir) as entries:
                available_worker_ids = {
                    entry.name for entry in entries if entry.name.isdigit()
                }

            # Reuse the queue directory's existing name-only scan to reconcile
            # stale failed/pending/processing rows. This adds no per-worker
            # /pscratch metadata operations.
            for missing_id in (
                set(self.state_db.unpublished_worker_ids(queue_name))
                - available_worker_ids
            ):
                if self.state_db.forget_missing_worker(queue_name, missing_id):
                    self.logger.info(
                        f"Stopped tracking missing worker queue={queue_name} "
                        f"worker={missing_id}"
                    )

            items = sorted(available_worker_ids, key=int, reverse=True)
            probe_limit = self.config.get('discovery', {}).get(
                'max_worker_probes_per_cycle', 500
            )
            items = items[:probe_limit]

        for item in items:
            worker_status = self.state_db.worker_status(queue_name, item)
            if worker_status == 'published':
                continue
            if (
                not worker_filters
                and discovery_high_water is not None
                and int(item) <= discovery_high_water
                and worker_status is None
            ):
                # Detailed rows may have been purged by retention. The compact
                # queue high-water mark prevents those workers being republished.
                continue
            worker_dir = os.path.join(queue_dir, item)
            if not os.path.isdir(worker_dir):
                if self.state_db.forget_missing_worker(queue_name, item):
                    self.logger.info(
                        f"Stopped tracking missing worker queue={queue_name} worker={item}"
                    )
                continue
            is_finished, job_id = self._is_job_finished(
                worker_dir, active_job_ids, squeue_ok=squeue_ok
            )
            if is_finished is None:
                if self.state_db.forget_missing_worker(queue_name, item):
                    self.logger.info(
                        f"Stopped tracking missing worker queue={queue_name} worker={item}"
                    )
                continue
            if job_id:
                if self.state_db.job_is_published(queue_name, job_id):
                    self.state_db.record_worker(
                        queue_name, item, job_id, status='published'
                    )
                    continue
                marker = os.path.join(worker_dir, f'.slurm-{job_id}.publish-done')
                if os.path.exists(marker):
                    self.state_db.mark_published(
                        queue_name, item, job_id, [],
                        self.config['paths']['cfs_destination'],
                    )
                    continue
            if not is_finished:
                self.state_db.record_worker(
                    queue_name, item, job_id, status='pending'
                )
                continue
            self.state_db.record_worker(queue_name, item, job_id, status='pending')
            jobs_to_process.append((queue_name, worker_dir, item, job_id))

        self.logger.info(
            f"Found {len(jobs_to_process)} new jobs to process for queue {queue_name}"
        )
        return jobs_to_process
    
    def run(self, dry_run=False, queue_filters=None, worker_filters=None,
            offline_dry_run=False):
        """Main execution loop"""
        self.logger.info("=" * 60)
        self.logger.info("Starting SLURM log publisher")
        if dry_run:
            self.logger.info("DRY RUN MODE - No changes will be made")
        
        if worker_filters and (not queue_filters or len(queue_filters) != 1):
            self.logger.error("--worker requires exactly one --queue")
            return 2

        if not self.process_lock.acquire():
            self.logger.warning("Another publisher or cleanup process holds the workspace lock")
            return 0

        try:
            workdir_root = self.config['paths']['workdir_root']
            if not os.path.exists(workdir_root):
                self.logger.error(f"Workdir root not found: {workdir_root}")
                return 1

            ignored = set(self.config['filters'].get('ignore_queues', []))
            if queue_filters:
                queues = [queue for queue in queue_filters if queue not in ignored]
            else:
                with os.scandir(workdir_root) as entries:
                    queues = sorted(
                        entry.name for entry in entries
                        if entry.name not in ignored and entry.is_dir(follow_symlinks=False)
                    )
            self.logger.info(f"Found {len(queues)} queues: {', '.join(queues)}")

            if offline_dry_run:
                if not dry_run:
                    self.logger.error("--offline-dry-run requires --dry-run")
                    return 2
                self.logger.warning(
                    "OFFLINE DRY RUN: skipping squeue and treating selected jobs as inactive"
                )
                squeue_ok, active_job_ids = True, set()
            else:
                squeue_ok, active_job_ids = self._get_active_slurm_jobs()
            jobs_to_process = []
            for queue_name in queues:
                jobs_to_process.extend(self._collect_queue_jobs(
                    queue_name,
                    active_job_ids,
                    squeue_ok,
                    worker_filters=worker_filters if len(queues) == 1 else None,
                ))

            if not jobs_to_process:
                self.logger.info("Finished. Processed 0 new jobs")
                return 0

            max_concurrent = int(
                self.config['processing'].get('max_concurrent_jobs', 3)
            )
            max_concurrent = max(1, min(max_concurrent, len(jobs_to_process)))
            self.logger.info(
                f"Processing {len(jobs_to_process)} jobs with one pool of "
                f"{max_concurrent} processes"
            )

            tasks = []
            with multiprocessing.Pool(processes=max_concurrent) as pool:
                for queue_name, worker_dir, worker_id, job_id in jobs_to_process:
                    self.state_db.mark_processing(queue_name, worker_id, job_id)
                    result = pool.apply_async(
                        _process_job_wrapper,
                        args=(self.config, queue_name, worker_dir, worker_id,
                              job_id, dry_run),
                    )
                    tasks.append((queue_name, worker_id, job_id, result))

                processed_count = 0
                for queue_name, worker_id, job_id, result in tasks:
                    try:
                        success, pub_count, panda_ids = result.get(timeout=3600)
                        if not success:
                            self.state_db.mark_failed(
                                queue_name, worker_id, job_id, "worker returned failure"
                            )
                            continue
                        self.state_db.mark_published(
                            queue_name, worker_id, job_id, panda_ids,
                            self.config['paths']['cfs_destination'],
                        )
                        processed_count += 1
                        self.logger.info(
                            f"Job {job_id} completed: {pub_count} files published, "
                            f"{len(panda_ids)} PandaIDs"
                        )
                    except multiprocessing.TimeoutError:
                        self.state_db.mark_failed(
                            queue_name, worker_id, job_id,
                            "processing timed out after 1 hour",
                        )
                        self.logger.error(f"Job {job_id} processing timed out after 1 hour")
                    except Exception as exc:
                        self.state_db.mark_failed(
                            queue_name, worker_id, job_id, str(exc)
                        )
                        self.logger.error(f"Job {job_id} processing failed: {exc}")

            self.logger.info(f"Finished. Processed {processed_count} new jobs")
            self.logger.info("=" * 60)
            return 0
        finally:
            self.process_lock.release()
            self.state_db.close()


def main():
    parser = argparse.ArgumentParser(
        description='Automated SLURM Log Publisher for PanDA'
    )
    parser.add_argument(
        '--config',
        default='<path to publish_slurm_logs_config.json file>',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--queue',
        action='append',
        dest='queues',
        help='Only inspect this queue (repeatable)'
    )
    parser.add_argument(
        '--worker',
        action='append',
        dest='workers',
        help='Only inspect this numeric worker ID (repeatable; requires one --queue)'
    )
    parser.add_argument(
        '--offline-dry-run',
        action='store_true',
        help='Dry-run only: bypass squeue when Slurm authentication is unavailable'
    )
    
    args = parser.parse_args()
    
    try:
        publisher = SlurmLogPublisher(args.config, dry_run=args.dry_run)
        return publisher.run(
            dry_run=args.dry_run,
            queue_filters=args.queues,
            worker_filters=args.workers,
            offline_dry_run=args.offline_dry_run,
        )
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
