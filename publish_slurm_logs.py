#!/usr/bin/env python3.11
"""
Automated SLURM Log Publisher for PanDA

This script scans for finished SLURM jobs, splits their output by task/PandaID,
and publishes them to CFS for web access via NERSC Science Gateway.

File structure in CFS:
  $CFS/www/panda/workers/<queue_name>/<panda_id>/slurm-<jobid>-task<taskid>-panda<pandaid>.out
  $CFS/www/panda/workers/<queue_name>/slurm-<jobid>-header.out

Usage:
  python3 publish_slurm_logs.py [--config CONFIG_FILE] [--dry-run]

Recent Fixes:
  2026-04-09: Removed date directory layer to avoid race condition between pilot and publisher
  2026-04-09: Updated cleanup to use PandaID directory mtime instead of date parsing
  2026-04-06: Fixed cleanup function to iterate through queue subdirectories
  2026-04-02: Fixed file permissions to 0o644 for web accessibility
  2026-04-02: Fixed job detection to handle random glob ordering
"""

import os
import sys
import json
import re
import shutil
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import logging


class SlurmLogPublisher:
    """Manages automated publishing of SLURM logs to CFS"""
    
    def __init__(self, config_path):
        """Initialize with configuration file"""
        self.config = self._load_config(config_path)
        self.state = self._load_state()
        self._setup_logging()
        
    def _load_config(self, config_path):
        """Load JSON configuration"""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _load_state(self):
        """Load state tracking (which jobs have been processed)"""
        state_file = self.config['paths']['state_file']
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                return json.load(f)
        return {
            'processed_jobs': {},  # {queue: {slurm_job_id: timestamp}}
            'last_run': None
        }
    
    def _save_state(self):
        """Save state tracking"""
        state_file = self.config['paths']['state_file']
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def _setup_logging(self):
        """Configure logging"""
        log_file = self.config['paths']['log_file']
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _is_job_finished(self, slurm_job_dir):
        """
        Check if a SLURM job is finished.
        Returns: (is_finished, job_id)
        
        A job is considered finished if:
        1. slurm-<jobid>.out file exists
        2. Job is not in squeue (RUNNING/PENDING)
        3. Job directory is older than min_job_age_seconds
        """
        # Find main slurm output file (slurm-<jobid>.out without task/panda suffixes)
        slurm_files = list(Path(slurm_job_dir).glob('slurm-*.out'))
        if not slurm_files:
            return False, None
        
        # Find the main output file by looping through all matches
        main_file = None
        job_id = None
        for f in slurm_files:
            match = re.search(r'slurm-(\d+)\.out$', f.name)
            if match:
                main_file = f
                job_id = match.group(1)
                break
        
        if not main_file or not job_id:
            return False, None
        
        if not main_file or not job_id:
            return False, None
        
        # Check if job is old enough
        min_age = self.config['filters']['min_job_age_seconds']
        file_age = datetime.now().timestamp() - main_file.stat().st_mtime
        if file_age < min_age:
            self.logger.debug(f"Job {job_id} too recent ({file_age:.0f}s < {min_age}s)")
            return False, job_id
        
        # Check if job is still running
        try:
            result = subprocess.run(
                ['squeue', '-j', job_id, '-h'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                self.logger.debug(f"Job {job_id} still in queue")
                return False, job_id
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
            self.logger.warning(f"Could not check job status for {job_id}: {e}")
            # Assume finished if squeue fails but file is old enough
            pass
        
        return True, job_id
    
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
    
    def _extract_job_id(self, filename):
        """
        Extract SLURM job ID from filename.
        Format: slurm-<jobid>-task<taskid>-panda<pandaid>.out
        Returns: job_id or None
        """
        match = re.search(r'^slurm-(\d+)-', filename)
        return match.group(1) if match else None
    
    def _publish_files(self, split_files, queue_name, worker_dir, job_id, dry_run=False):
        """
        Publish split files and pilotlog.txt to CFS in organized structure.
        
        Structure:
          <queue_name>/<panda_id>/slurm-<jobid>-task<taskid>-panda<pandaid>.out
          <queue_name>/<panda_id>/pilotlog.txt
          <queue_name>/slurm-<jobid>-header.out
        
        Task 19: Only publish files from tasks that have PandaIDs.
        If NO tasks have PandaIDs, skip the entire job.
        If SOME tasks have PandaIDs, publish only those + header.
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
        
        # Task 19: If NO tasks have PandaIDs, skip publishing entirely
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
        
        # Publish files with PandaIDs and header files
        published_count = 0
        pilotlog_count = 0
        files_to_publish = files_with_panda_id + header_files
        
        for split_file in files_to_publish:
            filename = os.path.basename(split_file)
            
            # Determine destination
            if '-header.out' in filename:
                # Header files go in queue directory
                dest_path = os.path.join(queue_dir, filename)
            else:
                # Task files go in <queue>/<panda_id>/
                panda_id = self._extract_panda_id(filename)
                if not panda_id:
                    # Should not happen since we filtered above, but safety check
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
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(split_file, dest_path)
                    # Set world-readable permissions (rw-r--r--)
                    os.chmod(dest_path, 0o644)
                    self.logger.debug(f"Published: {filename}")
                    published_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to publish {filename}: {e}")
        
        # Also copy pilotlog.txt files for each task with PandaID
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
        
        self.logger.info(
            f"Published {published_count}/{len(files_to_publish)} files + {pilotlog_count} pilotlog.txt to {queue_dir} "
            f"({len(files_with_panda_id)} task files, {len(header_files)} header files, {pilotlog_count} pilotlogs)"
        )
        return published_count + pilotlog_count
    
    def _cleanup_old_directories(self, dry_run=False):
        """Remove PandaID directories older than retention_days within each queue"""
        cfs_root = self.config['paths']['cfs_destination']
        retention_days = self.config['timing']['retention_days']
        cutoff_time = (datetime.now() - timedelta(days=retention_days)).timestamp()
        
        if not os.path.exists(cfs_root):
            return
        
        removed_count = 0
        
        # Iterate through queue directories
        for queue_name in os.listdir(cfs_root):
            queue_path = os.path.join(cfs_root, queue_name)
            if not os.path.isdir(queue_path):
                continue
            
            # Within each queue, look for PandaID directories
            for panda_dir_name in os.listdir(queue_path):
                panda_dir_path = os.path.join(queue_path, panda_dir_name)
                if not os.path.isdir(panda_dir_path):
                    continue
                
                # Skip header files and non-numeric directories
                if not panda_dir_name.isdigit():
                    continue
                
                # Check directory mtime (last modification time)
                try:
                    dir_mtime = os.path.getmtime(panda_dir_path)
                    if dir_mtime < cutoff_time:
                        if dry_run:
                            self.logger.info(f"[DRY-RUN] Would remove old directory: {panda_dir_path}")
                        else:
                            shutil.rmtree(panda_dir_path)
                            self.logger.info(f"Removed old directory: {queue_name}/{panda_dir_name}")
                            removed_count += 1
                except OSError as e:
                    self.logger.warning(f"Could not check/remove {panda_dir_path}: {e}")
                    continue
        
        if removed_count > 0:
            self.logger.info(f"Cleaned up {removed_count} old directories")
    
    def _process_queue(self, queue_name, dry_run=False):
        """Process all finished jobs for a queue"""
        workdir_root = self.config['paths']['workdir_root']
        queue_dir = os.path.join(workdir_root, queue_name)
        
        if not os.path.exists(queue_dir):
            self.logger.warning(f"Queue directory not found: {queue_dir}")
            return 0
        
        # Get state for this queue
        if queue_name not in self.state['processed_jobs']:
            self.state['processed_jobs'][queue_name] = {}
        processed = self.state['processed_jobs'][queue_name]
        
        processed_count = 0
        
        # Scan for worker directories (numeric subdirs)
        for item in os.listdir(queue_dir):
            worker_dir = os.path.join(queue_dir, item)
            if not os.path.isdir(worker_dir):
                continue
            
            # Check if job is finished
            is_finished, job_id = self._is_job_finished(worker_dir)
            if not is_finished:
                continue
            
            # Skip if already processed
            if job_id in processed:
                self.logger.debug(f"Skipping already processed job: {job_id}")
                continue
            
            self.logger.info(f"Processing queue={queue_name} worker={item} job={job_id}")
            
            # Find slurm output file
            slurm_file = os.path.join(worker_dir, f'slurm-{job_id}.out')
            if not os.path.exists(slurm_file):
                self.logger.warning(f"SLURM output not found: {slurm_file}")
                continue
            
            # Split output
            split_files = self._split_slurm_output(slurm_file)
            if not split_files:
                self.logger.warning(f"No split files created for job {job_id}")
                continue
            
            # Publish to CFS (Task 19: only publishes files with PandaIDs)
            # Also publishes pilotlog.txt for each task
            pub_count = self._publish_files(split_files, queue_name, worker_dir, job_id, dry_run=dry_run)
            
            if pub_count == 0:
                self.logger.info(
                    f"Job {job_id}: No files published (no PandaIDs found). "
                    f"Marking as processed to avoid reprocessing."
                )
            
            # Clean up split files if configured (regardless of publish count)
            if not dry_run and self.config['processing']['delete_original_splits']:
                for f in split_files:
                    try:
                        os.remove(f)
                    except Exception as e:
                        self.logger.warning(f"Could not remove {f}: {e}")
            
            # Mark as processed (even if no files were published)
            # This prevents reprocessing jobs that have no PandaIDs
            if not dry_run:
                processed[job_id] = datetime.now().isoformat()
                self._save_state()
            
            processed_count += 1
        
        return processed_count
    
    def run(self, dry_run=False):
        """Main execution loop"""
        self.logger.info("=" * 60)
        self.logger.info("Starting SLURM log publisher")
        if dry_run:
            self.logger.info("DRY RUN MODE - No changes will be made")
        
        workdir_root = self.config['paths']['workdir_root']
        
        # Find all queues
        if not os.path.exists(workdir_root):
            self.logger.error(f"Workdir root not found: {workdir_root}")
            return 1
        
        queues = [d for d in os.listdir(workdir_root) 
                  if os.path.isdir(os.path.join(workdir_root, d))
                  and d not in self.config['filters']['ignore_queues']]
        
        self.logger.info(f"Found {len(queues)} queues: {', '.join(queues)}")
        
        total_processed = 0
        for queue in queues:
            count = self._process_queue(queue, dry_run=dry_run)
            total_processed += count
        
        # Cleanup old directories
        self._cleanup_old_directories(dry_run=dry_run)
        
        # Update last run time
        if not dry_run:
            self.state['last_run'] = datetime.now().isoformat()
            self._save_state()
        
        self.logger.info(f"Finished. Processed {total_processed} new jobs")
        self.logger.info("=" * 60)
        
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Automated SLURM Log Publisher for PanDA'
    )
    parser.add_argument(
        '--config',
        default='/global/homes/x/xin/ws-panda/slurm-jobs/publish_slurm_logs_config.json',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    
    args = parser.parse_args()
    
    try:
        publisher = SlurmLogPublisher(args.config)
        return publisher.run(dry_run=args.dry_run)
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
