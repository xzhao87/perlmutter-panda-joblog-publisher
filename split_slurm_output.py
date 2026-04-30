#!/usr/bin/env python3
"""
Split SLURM Job Output Files by Task ID

This script splits a large SLURM job output file (slurm-<jobid>.out) into separate
files for each task. The SLURM output file contains interleaved output from multiple
tasks running in parallel, with each line prefixed by the task ID.

The script:
1. Reads the slurm-<jobid>.out file
2. Separates output lines by task ID (format: "taskid: log line")
3. Extracts ALL PandaIDs from each task's logs (a single pilot can process multiple jobs)
4. Creates individual output files:
   - If 1 PandaID: slurm-<jobid>-task<taskid>-panda<pandaid>.out
   - If multiple PandaIDs: Creates separate file for EACH PandaID with same content
   - If no PandaID: slurm-<jobid>-task<taskid>.out
5. Keeps the original slurm-<jobid>.out file intact

Usage:
    python3 split_slurm_output.py <path_to_slurm_output_file>
    python3 split_slurm_output.py /path/to/workdir/panda/NERSC_Perlmutter_epic/11270/slurm-50685843.out

Example:
    python3 split_slurm_output.py /global/homes/x/xin/ws-panda/epic-harvester-workdir/panda/NERSC_Perlmutter_epic/11270/slurm-50685843.out

The script will create files in the same directory as the input file:
    - slurm-50685843-task0-panda118014.out
    - slurm-50685843-task1-panda118015.out
    - etc.
    
For tasks that process multiple PandaIDs:
    - slurm-51874506-task152-panda7111257044.out (full task log)
    - slurm-51874506-task152-panda7111438591.out (duplicate of same full task log)
    - slurm-51874506-task152-panda7111501056.out (duplicate of same full task log)
"""

import sys
import os
import re
from collections import defaultdict
from pathlib import Path


def extract_panda_ids(lines):
    """
    Extract ALL PandaIDs from task logs.
    
    A single pilot (task) can process multiple PandaID jobs during its lifetime.
    This function finds all unique PandaIDs in the order they appear.
    
    Searches for patterns like:
    - 'PandaID': '118014'
    - "PandaID": "118014"
    - received job: 118014
    
    Args:
        lines: List of log lines for a specific task
    
    Returns:
        List of unique PandaIDs in order of appearance, or empty list if none found
    """
    # Pattern 1: 'PandaID': '118014' or "PandaID": "118014"
    panda_id_pattern1 = re.compile(r"['\"]PandaID['\"]:\s*['\"](\d+)['\"]")
    
    # Pattern 2: received job: 118014
    panda_id_pattern2 = re.compile(r"received\s+job:\s+(\d+)")
    
    # Pattern 3: PandaID=118014 (from URL-encoded strings)
    panda_id_pattern3 = re.compile(r"PandaID=(\d+)")
    
    found_ids = []
    seen = set()
    
    for line in lines:
        # Try all patterns
        match = panda_id_pattern1.search(line)
        if not match:
            match = panda_id_pattern2.search(line)
        if not match:
            match = panda_id_pattern3.search(line)
        
        if match:
            panda_id = match.group(1)
            # Add to list only if not seen before (preserve order)
            if panda_id not in seen:
                found_ids.append(panda_id)
                seen.add(panda_id)
    
    return found_ids


def split_slurm_output(slurm_file_path):
    """
    Split SLURM output file by task ID.
    
    Args:
        slurm_file_path: Path to the slurm-<jobid>.out file
    """
    # Validate input file
    if not os.path.exists(slurm_file_path):
        print(f"Error: File not found: {slurm_file_path}", file=sys.stderr)
        return 1
    
    # Extract SLURM job ID from filename
    filename = os.path.basename(slurm_file_path)
    slurm_job_id_match = re.match(r"slurm-(\d+)\.out", filename)
    if not slurm_job_id_match:
        print(f"Error: Invalid filename format. Expected slurm-<jobid>.out, got: {filename}", 
              file=sys.stderr)
        return 1
    
    slurm_job_id = slurm_job_id_match.group(1)
    output_dir = os.path.dirname(os.path.abspath(slurm_file_path))
    
    print(f"Processing SLURM job {slurm_job_id} from {slurm_file_path}")
    print(f"Output directory: {output_dir}")
    
    # Dictionary to store lines for each task
    # Format: {task_id: [line1, line2, ...]}
    task_lines = defaultdict(list)
    
    # Lines that don't have task prefix (header/footer lines)
    untagged_lines = []
    
    # Pattern to match lines with task prefix: "taskid: log line"
    # Note: SLURM output may have leading whitespace before task ID
    # Format can be " 67: log line" or "67: log line"
    task_line_pattern = re.compile(r"^\s*(\d+):\s*(.*)")
    
    # Read and parse the SLURM output file
    print("Reading and parsing SLURM output file...")
    line_count = 0
    with open(slurm_file_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line_count += 1
            if line_count % 10000 == 0:
                print(f"  Processed {line_count} lines...")
            
            match = task_line_pattern.match(line)
            if match:
                task_id = int(match.group(1))
                log_content = match.group(2)
                task_lines[task_id].append(log_content + '\n')
            else:
                # Lines without task prefix (usually header/footer)
                untagged_lines.append(line)
    
    print(f"Total lines processed: {line_count}")
    print(f"Found {len(task_lines)} tasks")
    print(f"Untagged lines: {len(untagged_lines)}")
    
    # Extract PandaID(s) for each task and write output files
    print("\nCreating individual task output files...")
    created_files = []
    
    for task_id in sorted(task_lines.keys()):
        lines = task_lines[task_id]
        panda_ids = extract_panda_ids(lines)
        
        # If a pilot processes multiple PandaIDs, create separate files for each
        if len(panda_ids) == 0:
            # No PandaID found - create single file without PandaID suffix
            output_filename = f"slurm-{slurm_job_id}-task{task_id}.out"
            output_path = os.path.join(output_dir, output_filename)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            created_files.append(output_filename)
            print(f"  Task {task_id:3d}: {output_filename:50s} - {len(lines):6d} lines (PandaID not found)")
        
        elif len(panda_ids) == 1:
            # Single PandaID - create single file with PandaID suffix
            panda_id = panda_ids[0]
            output_filename = f"slurm-{slurm_job_id}-task{task_id}-panda{panda_id}.out"
            output_path = os.path.join(output_dir, output_filename)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            created_files.append(output_filename)
            print(f"  Task {task_id:3d}: {output_filename:50s} - {len(lines):6d} lines (PandaID: {panda_id})")
        
        else:
            # Multiple PandaIDs - create duplicate files for each PandaID
            # This happens when one pilot processes multiple jobs
            for panda_id in panda_ids:
                output_filename = f"slurm-{slurm_job_id}-task{task_id}-panda{panda_id}.out"
                output_path = os.path.join(output_dir, output_filename)
                
                # Write the SAME content (entire task log) for each PandaID
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.writelines(lines)
                
                created_files.append(output_filename)
            
            panda_info = ", ".join(panda_ids)
            print(f"  Task {task_id:3d}: Created {len(panda_ids)} files for multiple PandaIDs: {panda_info}")
            print(f"             ({len(lines):6d} lines each)")
    
    
    # Optionally write untagged lines to a separate file
    if untagged_lines:
        header_filename = f"slurm-{slurm_job_id}-header.out"
        header_path = os.path.join(output_dir, header_filename)
        with open(header_path, 'w', encoding='utf-8') as f:
            f.writelines(untagged_lines)
        print(f"\n  Header/untagged lines: {header_filename} - {len(untagged_lines)} lines")
        created_files.append(header_filename)
    
    print(f"\nSuccessfully created {len(created_files)} output files")
    print(f"Original file {filename} remains intact")
    
    return 0


def main():
    """Main entry point for the script."""
    if len(sys.argv) != 2:
        print(__doc__)
        print("\nError: Missing required argument", file=sys.stderr)
        print("\nUsage: python3 split_slurm_output.py <path_to_slurm_output_file>", 
              file=sys.stderr)
        return 1
    
    slurm_file_path = sys.argv[1]
    return split_slurm_output(slurm_file_path)


if __name__ == "__main__":
    sys.exit(main())
