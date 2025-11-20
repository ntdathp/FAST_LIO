#!/usr/bin/env python3
"""
Run LIO-SAM's run_mcdviral.launch once per NTU folder,
playing ALL *.bag in that folder concurrently (interleaved by timestamp).

- Published folders: <root>/ntu_day_* and <root>/ntu_night_*
- Unpublished:       <root>/unpublished_sequences/ntu_day_* and ntu_night_*
- For each folder D, call:
    roslaunch run_mcdviral.launch bag_file:=D/*.bag autorun:=true

Notes:
- The provided launch uses a bash launch-prefix, so the *.bag wildcard is expanded by Bash.
- rosbag play can accept multiple bag inputs and will interleave messages by time.

Example:
  ./run_ntu_parallel_per_folder.py \
    --root /home/dat/data/MCDVIRAL/NTU \
    --launch /home/dat/liosam_ws/src/LIO-SAM/launch/run_mcdviral.launch
"""

import argparse
import os
import shlex
import signal
import sys
import time
from pathlib import Path
from subprocess import Popen, CalledProcessError
from typing import List

DEFAULT_LAUNCH = "/home/dat/slict_ws/src/FAST_LIO/launch/run_mcdviral.launch"
DEFAULT_ROOT   = "/home/dat/data/MCDVIRAL/NTU"

def find_dirs(root: Path, only: str, include_published: bool, include_unpublished: bool) -> List[Path]:
    """Collect NTU subfolders according to filters."""
    dirs: List[Path] = []

    def want_day() -> bool:   return only in ("all", "day")
    def want_night() -> bool: return only in ("all", "night")

    # Published
    if include_published:
        if want_day():
            dirs += sorted(root.glob("ntu_day_*"))
        if want_night():
            dirs += sorted(root.glob("ntu_night_*"))

    # Unpublished
    if include_unpublished:
        up = root / "unpublished_sequences"
        if up.is_dir():
            if want_day():
                dirs += sorted(up.glob("ntu_day_*"))
            if want_night():
                dirs += sorted(up.glob("ntu_night_*"))

    # Keep directories only and de-duplicate
    out: List[Path] = []
    seen = set()
    for d in dirs:
        if not d.is_dir(): 
            continue
        rp = str(d.resolve())
        if rp not in seen:
            out.append(d)
            seen.add(rp)
    return out

def run_roslaunch_for_folder(launch_file: Path, folder: Path, autorun: bool, extra_args: List[str], sleep_after: float):
    """
    Run roslaunch once for this folder, passing bag_file:=<folder>/*.bag
    to play all bags together.
    """
    # IMPORTANT: do NOT quote the '*' here; we want it to reach roslaunch,
    # and then the launch-prefix bash will expand it for rosbag.
    bag_arg = f"bag_file:={str(folder)}/" + "*.bag"

    args = [
        "roslaunch",
        str(launch_file),
        bag_arg,
        f"autorun:={'true' if autorun else 'false'}",
    ]
    if extra_args:
        args.extend(extra_args)

    print("\n========== RUNNING FOLDER ==========")
    print(f"Folder : {folder}")
    print(f"Cmd    : {' '.join(shlex.quote(a) for a in args)}")
    print("====================================\n")

    proc = None
    try:
        proc = Popen(args)
        rc = proc.wait()
        if rc != 0:
            raise CalledProcessError(rc, args)
        print(f"[OK] roslaunch finished with code 0 → {folder.name}")
    except KeyboardInterrupt:
        print("\n[INFO] Ctrl+C detected, sending SIGINT to roslaunch…")
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=20)
            except Exception:
                proc.kill()
        raise
    except CalledProcessError as e:
        print(f"[ERROR] roslaunch failed (code={e.returncode}) for folder: {folder}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()

    if sleep_after > 0:
        print(f"[INFO] Sleeping {sleep_after:.1f}s to ensure PCD/log saving…")
        time.sleep(sleep_after)

def main():
    parser = argparse.ArgumentParser(
        description="Run LIO-SAM once per NTU folder, playing ALL *.bag in each folder concurrently."
    )
    parser.add_argument("--root", default=DEFAULT_ROOT,
                        help=f"Root of NTU datasets (default: {DEFAULT_ROOT})")
    parser.add_argument("--launch", default=DEFAULT_LAUNCH,
                        help=f"Path to LIO-SAM launch file (default: {DEFAULT_LAUNCH})")
    parser.add_argument("--only", default="all", choices=["all", "day", "night"],
                        help="Run only day or night folders (default: all)")
    parser.add_argument("--published-only", action="store_true",
                        help="Run only published folders")
    parser.add_argument("--unpublished-only", action="store_true",
                        help="Run only unpublished folders")
    parser.add_argument("--sleep-after", type=float, default=10.0,
                        help="Seconds to wait after each folder's run (default: 10s)")
    parser.add_argument("--no_autorun", action="store_true",
                        help="Disable autorun (not recommended; you'd need to Ctrl+C manually to trigger saving)")
    args, extra = parser.parse_known_args()

    root = Path(args.root)
    launch_file = Path(args.launch)

    if not root.is_dir():
        print(f"[FATAL] Root folder not found: {root}")
        sys.exit(2)
    if not launch_file.is_file():
        print(f"[FATAL] Launch file not found: {launch_file}")
        sys.exit(3)

    # Determine published/unpublished inclusion
    if args.published_only and args.unpublished_only:
        include_published = True
        include_unpublished = True
    elif args.published_only:
        include_published = True
        include_unpublished = False
    elif args.unpublished_only:
        include_published = False
        include_unpublished = True
    else:
        include_published = True
        include_unpublished = True

    # Collect NTU folders (day/night + published/unpublished)
    ntu_dirs = find_dirs(root, args.only, include_published, include_unpublished)
    if not ntu_dirs:
        print("[FATAL] No NTU folders matched the criteria.")
        sys.exit(4)

    print(f"Total NTU folders to run: {len(ntu_dirs)}")
    for i, d in enumerate(ntu_dirs, 1):
        print(f"  {i:02d}. {d}")

    autorun = (not args.no_autorun)

    # Run per-folder (one roslaunch run per folder, *.bag together)
    for idx, d in enumerate(ntu_dirs, 1):
        print(f"\n===== FOLDER {idx}/{len(ntu_dirs)}: {d} =====")
        run_roslaunch_for_folder(
            launch_file=launch_file,
            folder=d,
            autorun=autorun,
            extra_args=extra,
            sleep_after=args.sleep_after,
        )

    print(f"\n🎉 DONE. Processed folders: {len(ntu_dirs)}")

if __name__ == "__main__":
    main()
