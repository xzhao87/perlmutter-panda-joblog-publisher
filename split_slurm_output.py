#!/usr/bin/env python3
"""
Split SLURM Job Output Files by Task ID

This script splits a large SLURM job output file (slurm-<jobid>.out) into
separate files for each task. The SLURM output file contains interleaved output
from multiple tasks running in parallel, with each line prefixed by the task ID.

The script:
1. Reads the slurm-<jobid>.out file
2. Separates output lines by task ID (format: "taskid: log line")
3. Extracts ALL PandaIDs from each task's logs (a single pilot can process multiple jobs)
4. Creates individual output files:
   - If 1 PandaID: slurm-<jobid>-task<taskid>-panda<pandaid>.out
   - If multiple PandaIDs: Creates separate file for EACH PandaID with same content
   - If no PandaID: slurm-<jobid>-task<taskid>.out
5. Keeps the original slurm-<jobid>.out file intact

Implementation note:
    This version streams task output into temporary per-task files instead of
    storing all task lines in memory. This avoids memory spikes for very large
    slurm-*.out files.

Usage:
    python3 split_slurm_output.py <slurm-output-file>

Example:
    python3 split_slurm_output.py /path/to/workdir/panda/NERSC_Perlmutter_epic/11270/slurm-50685843.out

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
import shutil
import tempfile
from pathlib import Path
from collections import OrderedDict


# PandaID patterns.
# These are compiled once and used while streaming the input file line by line.
PANDA_ID_PATTERN1 = re.compile(r"['\"]PandaID['\"]:\s*['\"](\d+)['\"]")
PANDA_ID_PATTERN2 = re.compile(r"received\s+job:\s+(\d+)")
PANDA_ID_PATTERN3 = re.compile(r"PandaID=(\d+)")


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

    Compatibility note:
        This function is kept for compatibility with the original version.
        The streaming implementation below does not call this function with a
        full in-memory list. Instead, it extracts PandaIDs line by line using
        extract_panda_ids_from_line().
    """
    found_ids = []
    seen = set()

    for line in lines:
        # Try all patterns
        match = PANDA_ID_PATTERN1.search(line)
        if not match:
            match = PANDA_ID_PATTERN2.search(line)
        if not match:
            match = PANDA_ID_PATTERN3.search(line)

        if match:
            panda_id = match.group(1)
            # Add to list only if not seen before (preserve order)
            if panda_id not in seen:
                found_ids.append(panda_id)
                seen.add(panda_id)

    return found_ids


def extract_panda_ids_from_line(line):
    """
    Extract PandaID(s) from a single log line.

    This is the streaming equivalent of extract_panda_ids(lines). It lets us
    record PandaIDs while reading the file, without storing all lines in memory.

    Args:
        line: One task log line

    Returns:
        List of PandaIDs found in this line. Usually empty or one.
    """
    found_ids = []

    # Try all patterns
    match = PANDA_ID_PATTERN1.search(line)
    if match:
        found_ids.append(match.group(1))

    match = PANDA_ID_PATTERN2.search(line)
    if match:
        found_ids.append(match.group(1))

    match = PANDA_ID_PATTERN3.search(line)
    if match:
        found_ids.append(match.group(1))

    return found_ids


class TaskInfo:
    """
    Metadata for one SLURM task.

    The original version stored:
        {task_id: [line1, line2, ...]}

    This streaming version stores only small metadata here. The actual lines are
    written immediately to temp_path on disk.
    """

    def __init__(self, task_id, temp_path):
        self.task_id = task_id
        self.temp_path = temp_path
        self.line_count = 0
        self.panda_ids = []
        self._seen_panda_ids = set()

    def add_panda_id(self, panda_id):
        """
        Add PandaID only if not seen before, preserving order of appearance.
        """
        if panda_id not in self._seen_panda_ids:
            self.panda_ids.append(panda_id)
            self._seen_panda_ids.add(panda_id)


class LRUFileWriterCache:
    """
    Small LRU cache for open temporary files.

    Why this is useful:
    - Opening and closing the temp file for every line is slow.
    - Keeping one file open per task can exceed the file descriptor limit if a
      job has many tasks.
    - This cache keeps only max_open_files handles open at once.
    """

    def __init__(self, max_open_files=128):
        self.max_open_files = max_open_files
        self._handles = OrderedDict()

    def write(self, path, text):
        """
        Write text to a temporary file, opening it lazily in append mode.
        """
        path = os.path.abspath(path)

        handle = self._handles.get(path)

        if handle is None:
            if len(self._handles) >= self.max_open_files:
                _, old_handle = self._handles.popitem(last=False)
                old_handle.close()

            handle = open(path, "a", encoding="utf-8")
            self._handles[path] = handle
        else:
            self._handles.move_to_end(path)

        handle.write(text)

    def close_all(self):
        """
        Close all open temporary file handles.
        """
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()


def copy_file_streaming(src, dst):
    """
    Copy a file without loading it into memory.

    This is used when one task has multiple PandaIDs. In that case the original
    behavior is to create duplicate files with the same full task log content.
    """
    with open(src, "rb") as fsrc:
        with open(dst, "wb") as fdst:
            shutil.copyfileobj(fsrc, fdst, length=1024 * 1024)


def move_or_copy_temp_to_final(temp_path, output_path):
    """
    Move a temporary task file to the final output path when possible.

    For the common case of zero or one PandaID, we can use os.replace(), which
    avoids an extra copy and reduces disk I/O.

    For multiple PandaIDs, the caller must copy the same temp file multiple
    times instead of moving it.
    """
    os.replace(temp_path, output_path)


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
        print(
            f"Error: Invalid filename format. Expected slurm-<jobid>.out, got: {filename}",
            file=sys.stderr,
        )
        return 1

    slurm_job_id = slurm_job_id_match.group(1)
    output_dir = os.path.dirname(os.path.abspath(slurm_file_path))

    print(f"Processing SLURM job {slurm_job_id} from {slurm_file_path}")
    print(f"Output directory: {output_dir}")

    try:
        input_size_mb = os.path.getsize(slurm_file_path) / 1024 / 1024
        print(f"Input file size: {input_size_mb:.1f} MB")
    except OSError:
        pass

    # Dictionary to store metadata for each task.
    #
    # Original version:
    #   task_lines = defaultdict(list)
    #   Format: {task_id: [line1, line2, ...]}
    #
    # Streaming version:
    #   task_infos = {task_id: TaskInfo(...)}
    #
    # The task log lines themselves are written to temporary files immediately.
    task_infos = {}

    # Lines that don't have task prefix (header/footer lines)
    #
    # Original version stored these lines in a list:
    #   untagged_lines = []
    #
    # Streaming version writes them to a temp file.
    untagged_line_count = 0

    # Pattern to match lines with task prefix: "taskid: log line"
    # Note: SLURM output may have leading whitespace before task ID
    # Format can be " 67: log line" or "67: log line"
    task_line_pattern = re.compile(r"^\s*(\d+):\s*(.*)")

    # Create temporary directory under output_dir.
    # Keeping it in the same directory/filesystem makes os.replace() cheap and
    # avoids relying on /tmp capacity.
    temp_dir = tempfile.mkdtemp(
        prefix=f".split-slurm-{slurm_job_id}-",
        dir=output_dir,
    )
    header_temp_path = os.path.join(temp_dir, "header-untagged.tmp")

    writer_cache = LRUFileWriterCache(max_open_files=128)

    try:
        # Read and parse the SLURM output file
        print("Reading and parsing SLURM output file...")
        print("Streaming task output to temporary files to avoid high memory usage...")

        line_count = 0

        with open(slurm_file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line_count += 1

                if line_count % 10000 == 0:
                    print(f"  Processed {line_count} lines...")

                match = task_line_pattern.match(line)

                if match:
                    task_id = int(match.group(1))
                    log_content = match.group(2)

                    if task_id not in task_infos:
                        task_temp_path = os.path.join(temp_dir, f"task-{task_id}.tmp")
                        task_infos[task_id] = TaskInfo(task_id, task_temp_path)

                    task_info = task_infos[task_id]

                    # Preserve original behavior:
                    # Original code wrote:
                    #   task_lines[task_id].append(log_content + '\n')
                    #
                    # So this version writes the task log content without the
                    # leading "taskid:" prefix.
                    output_line = log_content + "\n"
                    writer_cache.write(task_info.temp_path, output_line)
                    task_info.line_count += 1

                    # Extract PandaIDs while streaming. This avoids reading the
                    # temp file back just to discover PandaIDs.
                    for panda_id in extract_panda_ids_from_line(output_line):
                        task_info.add_panda_id(panda_id)

                else:
                    # Lines without task prefix (usually header/footer)
                    writer_cache.write(header_temp_path, line)
                    untagged_line_count += 1

        writer_cache.close_all()

        print(f"Total lines processed: {line_count}")
        print(f"Found {len(task_infos)} tasks")
        print(f"Untagged lines: {untagged_line_count}")

        # Extract PandaID(s) for each task and write output files
        print("\nCreating individual task output files...")

        created_files = []

        for task_id in sorted(task_infos.keys()):
            task_info = task_infos[task_id]
            panda_ids = task_info.panda_ids

            # If a pilot processes multiple PandaIDs, create separate files for each
            if len(panda_ids) == 0:
                # No PandaID found - create single file without PandaID suffix
                output_filename = f"slurm-{slurm_job_id}-task{task_id}.out"
                output_path = os.path.join(output_dir, output_filename)

                # Common case: one final output from one temp file.
                # Use rename/replace instead of copy to reduce disk I/O.
                move_or_copy_temp_to_final(task_info.temp_path, output_path)

                created_files.append(output_filename)

                print(
                    f"  Task {task_id:3d}: {output_filename:50s} "
                    f"- {task_info.line_count:6d} lines (PandaID not found)"
                )

            elif len(panda_ids) == 1:
                # Single PandaID - create single file with PandaID suffix
                panda_id = panda_ids[0]
                output_filename = (
                    f"slurm-{slurm_job_id}-task{task_id}-panda{panda_id}.out"
                )
                output_path = os.path.join(output_dir, output_filename)

                # Common case: one final output from one temp file.
                # Use rename/replace instead of copy to reduce disk I/O.
                move_or_copy_temp_to_final(task_info.temp_path, output_path)

                created_files.append(output_filename)

                print(
                    f"  Task {task_id:3d}: {output_filename:50s} "
                    f"- {task_info.line_count:6d} lines (PandaID: {panda_id})"
                )

            else:
                # Multiple PandaIDs - create duplicate files for each PandaID
                # This happens when one pilot processes multiple jobs
                for panda_id in panda_ids:
                    output_filename = (
                        f"slurm-{slurm_job_id}-task{task_id}-panda{panda_id}.out"
                    )
                    output_path = os.path.join(output_dir, output_filename)

                    # Write the SAME content (entire task log) for each PandaID.
                    # Because there are multiple final outputs, we must copy
                    # instead of rename.
                    copy_file_streaming(task_info.temp_path, output_path)

                    created_files.append(output_filename)

                panda_info = ", ".join(panda_ids)
                print(
                    f"  Task {task_id:3d}: Created {len(panda_ids)} files "
                    f"for multiple PandaIDs: {panda_info}"
                )
                print(f"              ({task_info.line_count:6d} lines each)")

        # Optionally write untagged lines to a separate file
        if untagged_line_count > 0:
            header_filename = f"slurm-{slurm_job_id}-header.out"
            header_path = os.path.join(output_dir, header_filename)

            # The header temp file has only one final output, so use rename.
            move_or_copy_temp_to_final(header_temp_path, header_path)

            print(
                f"\n  Header/untagged lines: {header_filename} "
                f"- {untagged_line_count} lines"
            )
            created_files.append(header_filename)

        print(f"\nSuccessfully created {len(created_files)} output files")
        print(f"Original file {filename} remains intact")

        return 0

    except Exception as exc:
        writer_cache.close_all()
        print(f"Error while processing {slurm_file_path}: {exc}", file=sys.stderr)
        print(f"Temporary files kept for debugging at: {temp_dir}", file=sys.stderr)
        return 1

    finally:
        # Clean up temporary directory.
        #
        # If processing failed, this removes partial temp files. If you prefer to
        # keep temp files for debugging on failure, comment out this block.
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception as cleanup_exc:
            print(
                f"Warning: failed to remove temporary directory {temp_dir}: {cleanup_exc}",
                file=sys.stderr,
            )


def main():
    """Main entry point for the script."""
    if len(sys.argv) != 2:
        print(__doc__)
        print("\nError: Missing required argument", file=sys.stderr)
        print("\nUsage: python3 split_slurm_output.py <slurm-output-file>", file=sys.stderr)
        return 1

    slurm_file_path = sys.argv[1]
    return split_slurm_output(slurm_file_path)


if __name__ == "__main__":
    sys.exit(main())

