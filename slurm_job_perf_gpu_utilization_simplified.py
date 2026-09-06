#!/usr/bin/env python3
"""
Slurm GPU utilization monitor.

Python 3.6 compatible.

Features:
  - GPU utilization report
  - Per-user GPU utilization summary
  - GPU-hour weighted utilization
  - Save last GPU activity date in CSV
  - Reset priority to 100 for users with no GPU jobs in 14 days
  - Optionally update Slurm priority based on GPU utilization

Important Slurm behavior:
  - Parent job records are NOT counted
  - .extern records are NOT counted
  - Actual job steps such as 88212.0 ARE counted
"""

from __future__ import print_function

import argparse
import csv
import os
import random
import subprocess
from collections import OrderedDict
from datetime import datetime, timedelta


# ======================================================================
# Configuration
# ======================================================================

SACCT = "/cm/shared/apps/slurm/current/bin/sacct"
SACCTMGR = "/cm/shared/apps/slurm/current/bin/sacctmgr"

# CSV contains only:
#
# user,last
#
# It is NOT the job report.
GPU_HISTORY_FILE = (
    "/lustre/nvwulf/home/fenzhang/"
    "gpuusers.csv"
)

INACTIVE_DAYS = 14

DEBUG = False

PARTITIONS = (
    "b40x4,b40x4-long,"
    "h200x4,h200x4-long,"
    "h200x8,h200x8-long,"
    "p-b40x4,p-b40x4-long,"
    "p-h200x4,p-h200x4-long,"
    "p-h200x8,p-h200x8-03-long"
)

SACCT_FORMAT = (
    "USER,JobID,Partition,State,Start,Elapsed,"
    "NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,"
    "TRESUsageInAve,AllocTRES"
)


# ======================================================================
# Time conversion
# ======================================================================

def time_to_hours(value):
    """Convert Slurm elapsed time to hours."""

    if not value:
        return 0.0

    value = value.split(".", 1)[0]

    days = 0

    if "-" in value:
        day_string, value = value.split("-", 1)
        days = int(day_string)

    parts = value.split(":")

    if len(parts) != 3:
        return 0.0

    hours = int(parts[0])
    minutes = int(parts[1])
    seconds = int(parts[2])

    return (
        days * 24.0
        + hours
        + minutes / 60.0
        + seconds / 3600.0
    )


# ======================================================================
# GPU parsing
# ======================================================================

def parse_gpu_info(tres_usage, alloc_tres):
    """
    Extract GPU utilization and GPU count.

    Example:

      TRESUsageInAve:
        cpu=00:52:45,...,gres/gpuutil=100,...

      AllocTRES:
        cpu=1,gres/gpu:h200=1,gres/gpu=1,...

    Returns:

        (gpu_utilization, gpu_count)
    """

    gpu_util = None
    gpu_count = None

    if tres_usage:
        for item in tres_usage.split(","):

            item = item.strip()

            if item.startswith("gres/gpuutil="):

                try:
                    gpu_util = float(
                        item.split("=", 1)[1]
                    )
                except ValueError:
                    pass

    if alloc_tres:
        for item in alloc_tres.split(","):

            item = item.strip()

            if item.startswith("gres/gpu="):

                try:
                    gpu_count = int(
                        float(item.split("=", 1)[1])
                    )
                except ValueError:
                    pass

    if gpu_util is None:
        return 0.0, 0

    if gpu_count is None or gpu_count <= 0:
        gpu_count = 1

    # Protect against bad data.
    gpu_util = max(
        0.0,
        min(100.0, gpu_util)
    )

    return gpu_util, gpu_count


# ======================================================================
# Run sacct
# ======================================================================

def run_sacct(args, starttime, endtime):
    """Run sacct."""

    command = [
        SACCT,
        "-n",
        "-P",
        "-r", PARTITIONS,
        "--format=" + SACCT_FORMAT,
        "-S", starttime,
        "-E", endtime,
    ]

    if args.jobs:

        command.extend([
            "-j", args.jobs,
            "-a"
        ])

    elif args.allusers:

        command.append("-a")

    else:

        command.extend([
            "-u", args.user
        ])

    if not args.allstates:

        command.extend([
            "-s", args.state
        ])

    if DEBUG:

        print()
        print("Running:")
        print(" ".join(command))
        print()

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )

    stdout, stderr = process.communicate()

    if process.returncode != 0:

        raise RuntimeError(
            "sacct failed: {}".format(
                stderr.strip()
            )
        )

    return stdout


# ======================================================================
# Parse sacct
# ======================================================================

def parse_sacct(output):
    """
    Parse sacct output.

    Parent records:

        88212

    are metadata only.

    Extern records:

        88212.extern

    are ignored.

    Actual steps:

        88212.0

    are used for GPU utilization.
    """

    jobs = OrderedDict()

    fields = SACCT_FORMAT.split(",")

    current_user = None
    current_job = None

    for line in output.splitlines():

        if not line.strip():
            continue

        values = line.split("|")

        if len(values) != len(fields):

            if DEBUG:
                print(
                    "Skipping malformed line:",
                    line
                )

            continue

        data = dict(
            zip(fields, values)
        )

        user = data["USER"].strip()
        jobid = data["JobID"].strip()

        # --------------------------------------------------------------
        # Parent job
        # --------------------------------------------------------------

        if user:

            current_user = user
            current_job = jobid

            if user not in jobs:
                jobs[user] = OrderedDict()

            if jobid not in jobs[user]:

                jobs[user][jobid] = {
                    "partition": data["Partition"],
                    "state": data["State"],
                    "start": data["Start"],
                    "elapsed": 0.0,
                    "gpu_util_hours": 0.0,
                    "gpu_hours": 0.0,
                    "gpu_count": 0,
                    "steps": 0,
                }

            # DO NOT count parent record.
            continue

        if current_user is None:
            continue

        # --------------------------------------------------------------
        # Ignore extern
        # --------------------------------------------------------------

        if ".extern" in jobid:
            continue

        # --------------------------------------------------------------
        # Only actual job steps
        # --------------------------------------------------------------

        if "." not in jobid:
            continue

        if "batch" in jobid:
            continue

        job = jobs[current_user][current_job]

        job["steps"] += 1

        elapsed = time_to_hours(
            data["Elapsed"]
        )

        gpu_util, gpu_count = parse_gpu_info(
            data["TRESUsageInAve"],
            data["AllocTRES"]
        )

        if gpu_count <= 0:
            continue

        gpu_hours = (
            elapsed * gpu_count
        )

        job["elapsed"] += elapsed

        job["gpu_hours"] += gpu_hours

        job["gpu_util_hours"] += (
            gpu_util * gpu_hours
        )

        job["gpu_count"] = max(
            job["gpu_count"],
            gpu_count
        )

    # --------------------------------------------------------------
    # Calculate final utilization
    # --------------------------------------------------------------

    for user in jobs:

        for jobid in jobs[user]:

            job = jobs[user][jobid]

            if job["gpu_hours"] > 0:

                job["utilization"] = (
                    job["gpu_util_hours"]
                    / job["gpu_hours"]
                )

            else:

                job["utilization"] = 0.0

    return jobs


# ======================================================================
# Select GPU jobs
# ======================================================================

def select_gpu_jobs(user_jobs):
    """Return jobs that actually used GPUs."""

    selected = []

    for jobid, job in user_jobs.items():

        if job["gpu_hours"] <= 0:
            continue

        # Preserve your old behavior of ignoring A100.
        if "a100" in job["partition"].lower():
            continue

        selected.append(
            (jobid, job)
        )

    return selected


# ======================================================================
# User summary
# ======================================================================

def summarize_user(selected_jobs):
    """
    GPU-hour weighted utilization.
    """

    total_gpu_hours = 0.0
    total_util_gpu_hours = 0.0

    for jobid, job in selected_jobs:

        total_gpu_hours += job["gpu_hours"]

        total_util_gpu_hours += (
            job["gpu_util_hours"]
        )

    if total_gpu_hours <= 0:

        utilization = 0.0

    else:

        utilization = (
            total_util_gpu_hours
            / total_gpu_hours
        )

    return {
        "jobs": len(selected_jobs),
        "gpu_hours": total_gpu_hours,
        "utilization": utilization,
    }


# ======================================================================
# Priority calculation
# ======================================================================

def priority_from_utilization(utilization):
    """
    Map GPU utilization to priority.

        100% -> 100
         95% -> 99
         90% -> 98
         80% -> 96
         50% -> 90
    """

    priority = (
        int(utilization * 0.20)
        + 80
    )

    return max(
        80,
        min(100, priority)
    )


# ======================================================================
# Print detailed jobs
# ======================================================================

def print_job_report(user, selected_jobs):

    print()
    print("User: {}".format(user))

    print(
        "{:<12} {:>10} {:>8} {:>12} {:>10}".format(
            "JobID",
            "Elapsed",
            "GPU",
            "GPU-hours",
            "GPU Util"
        )
    )

    print("-" * 58)

    for jobid, job in selected_jobs:

        print(
            "{:<12} {:>10.2f} {:>8d} {:>12.2f} {:>9.2f}%".format(
                jobid,
                job["elapsed"],
                job["gpu_count"],
                job["gpu_hours"],
                job["utilization"]
            )
        )


# ======================================================================
# Print summary
# ======================================================================

def print_summary(summary):

    print()
    print("=" * 72)
    print("GPU UTILIZATION SUMMARY")
    print("=" * 72)

    print(
        "{:<16} {:>8} {:>14} {:>14} {:>10}".format(
            "User",
            "Jobs",
            "GPU-hours",
            "GPU Util",
            "Priority"
        )
    )

    print("-" * 72)

    for user in sorted(summary):

        item = summary[user]

        priority = priority_from_utilization(
            item["utilization"]
        )

        print(
            "{:<16} {:>8d} {:>14.2f} {:>13.2f}% {:>10d}".format(
                user,
                item["jobs"],
                item["gpu_hours"],
                item["utilization"],
                priority
            )
        )

    print()


# ======================================================================
# CSV history
# ======================================================================

def load_gpu_history(filename):
    """
    Read:

        user,last

    Returns:

        {
            user: YYYY-MM-DD
        }
    """

    history = {}

    if not os.path.exists(filename):
        return history

    try:

        with open(
            filename,
            "r",
            newline=""
        ) as csvfile:

            reader = csv.DictReader(
                csvfile
            )

            for row in reader:

                user = row.get(
                    "user",
                    ""
                ).strip()

                last = row.get(
                    "last",
                    ""
                ).strip()

                if user:
                    history[user] = last

    except IOError as error:

        print(
            "WARNING: Cannot read {}: {}".format(
                filename,
                error
            )
        )

    return history


def save_gpu_history(filename, history):
    """
    Atomically write the GPU history CSV.
    """

    directory = os.path.dirname(filename)

    if directory and not os.path.exists(directory):

        os.makedirs(directory)

    temporary = filename + ".tmp"

    with open(
        temporary,
        "w",
        newline=""
    ) as csvfile:

        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "user",
                "last"
            ]
        )

        writer.writeheader()

        for user in sorted(history):

            writer.writerow({
                "user": user,
                "last": history[user]
            })

    # Atomic replacement on the same filesystem.
    os.rename(
        temporary,
        filename
    )


# ======================================================================
# Update history
# ======================================================================

def update_gpu_history(history, active_users):

    today = datetime.now().strftime(
        "%Y-%m-%d"
    )

    for user in active_users:

        history[user] = today


# ======================================================================
# Reset inactive users
# ======================================================================

def reset_inactive_users(
    history,
    active_users,
    dry_run=False
):
    """
    Reset priority to 100 for users that have not
    had a GPU job in the last 14 days.

    IMPORTANT:

    Users currently active in the 14-day sacct query
    are never reset.
    """

    cutoff = (
        datetime.now()
        - timedelta(days=INACTIVE_DAYS)
    ).date()

    for user in sorted(history):

        if user in active_users:
            continue

        last_string = history[user]

        if not last_string:
            continue

        try:

            last_date = datetime.strptime(
                last_string,
                "%Y-%m-%d"
            ).date()

        except ValueError:

            print(
                "WARNING: Invalid date for {}: {}".format(
                    user,
                    last_string
                )
            )

            continue

        if last_date < cutoff:

            print(
                "No GPU jobs for {} since {} -> reset priority to 100".format(
                    user,
                    last_string
                )
            )

            if not dry_run:

                update_priority(
                    user,
                    100
                )


# ======================================================================
# Slurm priority update
# ======================================================================

def update_priority(user, priority):

    command = [
        SACCTMGR,
        "modify",
        "user",
        user,
        "set",
        "-i",
        "priority={}".format(priority)
    ]

    if DEBUG:

        print(
            "Running:",
            " ".join(command)
        )

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )

    stdout, stderr = process.communicate()

    if process.returncode != 0:

        print(
            "ERROR updating {}: {}".format(
                user,
                stderr.strip()
            )
        )


# ======================================================================
# Arguments
# ======================================================================

def parse_args():

    now = datetime.today()

    default_start = (
        now - timedelta(days=7)
    ).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    default_end = now.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    default_user = (
        os.environ.get("USER")
        or os.environ.get("USERNAME")
    )

    parser = argparse.ArgumentParser(
        description=(
            "Slurm GPU utilization monitor"
        )
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "-u",
        "--user",
        default=default_user,
        help="Username or comma-separated usernames"
    )

    group.add_argument(
        "--allusers",
        action="store_true",
        help="Report all users"
    )

    group.add_argument(
        "-j",
        "--jobs",
        help="Specific job or jobs"
    )

    parser.add_argument(
        "-S",
        "--starttime",
        default=default_start,
        help="Start time for utilization report"
    )

    parser.add_argument(
        "-E",
        "--endtime",
        default=default_end,
        help="End time for utilization report"
    )

    parser.add_argument(
        "-n",
        "--njobs",
        type=int,
        default=-1,
        help="Maximum jobs per user"
    )

    parser.add_argument(
        "-s",
        "--state",
        default="CD",
        help="Slurm state"
    )

    parser.add_argument(
        "--allstates",
        action="store_true",
        help="Include all states"
    )

    parser.add_argument(
        "--details",
        action="store_true",
        help="Show individual GPU jobs"
    )

    parser.add_argument(
        "--update",
        action="store_true",
        help="Update Slurm priorities"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not modify Slurm priorities"
    )

    return parser.parse_args()


# ======================================================================
# Main
# ======================================================================

def main():

    args = parse_args()

    # --------------------------------------------------------------
    # 1. Normal utilization report
    # --------------------------------------------------------------

    try:

        output = run_sacct(
            args,
            args.starttime,
            args.endtime
        )

    except RuntimeError as error:

        print(
            "ERROR:",
            error
        )

        return 1

    jobs = parse_sacct(output)

    summary = OrderedDict()

    for user in sorted(jobs):

        selected = select_gpu_jobs(
            jobs[user]
        )

        if not selected:
            continue

        # Optional random selection for detailed report.
        if (
            args.njobs > 0
            and len(selected) > args.njobs
        ):

            selected_for_report = random.sample(
                selected,
                args.njobs
            )

        else:

            selected_for_report = selected

        if args.details:

            print_job_report(
                user,
                selected_for_report
            )

        summary[user] = summarize_user(
            selected
        )

    if summary:

        print_summary(
            summary
        )

    else:

        print(
            "No GPU jobs found in the requested report period."
        )

    # --------------------------------------------------------------
    # 2. Load GPU activity history
    # --------------------------------------------------------------

    history = load_gpu_history(
        GPU_HISTORY_FILE
    )

    # Users that actually had GPU jobs in the report.
    active_users = set(
        summary.keys()
    )

    # Update their last GPU activity.
    update_gpu_history(
        history,
        active_users
    )

    # --------------------------------------------------------------
    # 3. Save history
    # --------------------------------------------------------------

    save_gpu_history(
        GPU_HISTORY_FILE,
        history
    )

    # --------------------------------------------------------------
    # 4. Check inactivity over the LAST 14 DAYS
    # --------------------------------------------------------------

    if args.update or args.dry_run:

        inactive_start = (
            datetime.now()
            - timedelta(days=INACTIVE_DAYS)
        ).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        inactive_end = datetime.now().strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        try:

            inactive_output = run_sacct(
                args,
                inactive_start,
                inactive_end
            )

        except RuntimeError as error:

            print(
                "ERROR checking 14-day GPU activity:",
                error
            )

            return 1

        inactive_jobs = parse_sacct(
            inactive_output
        )

        inactive_active_users = set()

        for user in inactive_jobs:

            selected = select_gpu_jobs(
                inactive_jobs[user]
            )

            if selected:

                inactive_active_users.add(
                    user
                )

                # Update history from the actual
                # 14-day activity query as well.
                history[user] = datetime.now().strftime(
                    "%Y-%m-%d"
                )

        save_gpu_history(
            GPU_HISTORY_FILE,
            history
        )

        # ----------------------------------------------------------
        # 5. Set priorities for active GPU users
        # ----------------------------------------------------------

        if args.update:

            print()
            print(
                "Updating GPU-user priorities..."
            )

            for user in sorted(summary):

                if user == "rharrison":
                    continue

                utilization = summary[user][
                    "utilization"
                ]

                priority = priority_from_utilization(
                    utilization
                )

                print(
                    "{}: {:.2f}% GPU utilization -> priority {}".format(
                        user,
                        utilization,
                        priority
                    )
                )

                update_priority(
                    user,
                    priority
                )

        # ----------------------------------------------------------
        # 6. Reset inactive users
        # ----------------------------------------------------------

        print()
        print(
            "Checking users with no GPU jobs in the last {} days...".format(
                INACTIVE_DAYS
            )
        )

        reset_inactive_users(
            history,
            inactive_active_users,
            dry_run=(
                args.dry_run
                or not args.update
            )
        )

    return 0


if __name__ == "__main__":

    raise SystemExit(
        main()
    )
