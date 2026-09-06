#!/usr/bin/env python3
"""
Slurm GPU utilization report.

Python 3.6 compatible.

Reports:
  - GPU utilization for individual jobs
  - Weighted GPU utilization per user
  - GPU-hours used
  - Number of GPU jobs
  - Suggested priority factor

Important:
  - Parent sacct records are ignored for utilization.
  - .extern records are ignored.
  - Actual job/step records such as 88212.0 are used.
"""

from __future__ import print_function

import argparse
import random
import re
import subprocess
from collections import OrderedDict
from datetime import datetime, timedelta


SACCT = "/cm/shared/apps/slurm/current/bin/sacct"
SACCTMGR = "/cm/shared/apps/slurm/current/bin/sacctmgr"

DEBUG = False

PARTITIONS = (
    "b40x4,b40x4-long,"
    "h200x4,h200x4-long,"
    "h200x8,h200x8-long,"
    "p-b40x4,p-b40x4-long,"
    "p-h200x4,p-h200x4-long,"
    "p-h200x8,p-h200x8-03-long,p-h200x8-long"
)

SACCT_FORMAT = (
    "USER,JobID,Partition,State,Start,Elapsed,"
    "NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,"
    "TRESUsageInAve,AllocTRES"
)


# ----------------------------------------------------------------------
# Conversion functions
# ----------------------------------------------------------------------

def time_to_hours(value):
    """Convert Slurm time to hours."""
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


# ----------------------------------------------------------------------
# GPU parsing
# ----------------------------------------------------------------------

def parse_gpu_info(tres_usage, alloc_tres):
    """
    Return:

        gpu_utilization, gpu_count

    Example:

        TRESUsageInAve:
          cpu=00:52:45,...,gres/gpuutil=100,...

        AllocTRES:
          cpu=1,gres/gpu:h200=1,gres/gpu=1,...

    Returns:
        100.0, 1
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

    # Protect against bad Slurm data.
    gpu_util = max(0.0, min(100.0, gpu_util))

    return gpu_util, gpu_count


# ----------------------------------------------------------------------
# Run sacct
# ----------------------------------------------------------------------

def run_sacct(args):
    """Run sacct without using a shell."""

    command = [
        SACCT,
        "-n",
        "-P",
        "-r", PARTITIONS,
        "--format=" + SACCT_FORMAT,
        "-S", args.starttime,
        "-E", args.endtime,
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
            "sacct failed: {}".format(stderr.strip())
        )

    return stdout


# ----------------------------------------------------------------------
# Parse sacct
# ----------------------------------------------------------------------

def parse_sacct(output):
    """
    Return:

        {
            user: {
                jobid: {
                    ...
                }
            }
        }

    Only actual job steps are used for utilization.

    Example:

        88212          -> parent, ignored
        88212.extern   -> ignored
        88212.0        -> used
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

        data = dict(zip(fields, values))

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
                    "gpu_util": 0.0,
                    "gpu_count": 0,
                    "steps": 0,
                }

            # VERY IMPORTANT:
            # Do not use parent record for GPU utilization.
            continue

        if current_user is None:
            continue

        # --------------------------------------------------------------
        # Ignore extern
        # --------------------------------------------------------------
        if ".extern" in jobid:
            continue

        job = jobs[current_user][current_job]

        # --------------------------------------------------------------
        # Actual job step
        # --------------------------------------------------------------
        if "." not in jobid:
            continue

        if "batch" in jobid:
            continue

        job["steps"] += 1

        elapsed = time_to_hours(data["Elapsed"])

        gpu_util, gpu_count = parse_gpu_info(
            data["TRESUsageInAve"],
            data["AllocTRES"]
        )

        # Accumulate GPU-hours.
        job["elapsed"] += elapsed

        if gpu_count > 0:
            job["gpu_count"] = max(
                job["gpu_count"],
                gpu_count
            )

            # Weighted utilization numerator.
            job["gpu_util"] += (
                gpu_util
                * elapsed
                * gpu_count
            )

    return jobs


# ----------------------------------------------------------------------
# Finalize jobs
# ----------------------------------------------------------------------

def finalize_job(job):
    """
    Convert accumulated GPU utilization into a percentage.
    """

    gpu_hours = (
        job["elapsed"]
        * job["gpu_count"]
    )

    if gpu_hours <= 0:
        job["utilization"] = 0.0
    else:
        job["utilization"] = (
            job["gpu_util"]
            / gpu_hours
        )

    job["gpu_hours"] = gpu_hours

    return job


# ----------------------------------------------------------------------
# Select useful jobs
# ----------------------------------------------------------------------

def select_jobs(user_jobs):
    """
    Remove jobs that cannot contribute to GPU utilization.
    """

    result = []

    for jobid, job in user_jobs.items():

        finalize_job(job)

        # Ignore jobs without GPU information.
        if job["gpu_count"] <= 0:
            continue

        # Ignore zero-length jobs.
        if job["elapsed"] <= 0:
            continue

        # Existing special-case behavior.
        if "a100" in job["partition"].lower():
            continue

        result.append(
            (jobid, job)
        )

    return result


# ----------------------------------------------------------------------
# User summary
# ----------------------------------------------------------------------

def summarize_user(selected_jobs):
    """
    Calculate weighted GPU utilization.

    GPU utilization is weighted by GPU-hours, not by number of jobs.

        sum(utilization * GPU-hours)
        ----------------------------
              sum(GPU-hours)
    """

    total_gpu_hours = 0.0
    utilization_gpu_hours = 0.0

    for jobid, job in selected_jobs:

        gpu_hours = job["gpu_hours"]

        total_gpu_hours += gpu_hours

        utilization_gpu_hours += (
            job["utilization"]
            * gpu_hours
        )

    if total_gpu_hours <= 0:
        utilization = 0.0
    else:
        utilization = (
            utilization_gpu_hours
            / total_gpu_hours
        )

    return {
        "jobs": len(selected_jobs),
        "gpu_hours": total_gpu_hours,
        "utilization": utilization,
    }


# ----------------------------------------------------------------------
# Priority
# ----------------------------------------------------------------------

def priority_from_utilization(utilization):
    """
    Convert GPU utilization into the existing 80-100
    priority scale.

        100% -> 100
         90% -> 98
         80% -> 96
         50% -> 90
    """

    priority = int(
        utilization * 0.2
    ) + 80

    return max(
        80,
        min(100, priority)
    )


# ----------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------

def print_job_report(user, selected_jobs):

    print()
    print(
        "User: {}".format(user)
    )

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


def print_summary(summary):

    print()
    print("=" * 60)
    print("GPU UTILIZATION SUMMARY")
    print("=" * 60)

    print(
        "{:<15} {:>8} {:>14} {:>14} {:>10}".format(
            "User",
            "Jobs",
            "GPU-hours",
            "GPU Util",
            "Priority"
        )
    )

    print("-" * 60)

    for user in sorted(summary):

        item = summary[user]

        priority = priority_from_utilization(
            item["utilization"]
        )

        print(
            "{:<15} {:>8d} {:>14.2f} {:>13.2f}% {:>10d}".format(
                user,
                item["jobs"],
                item["gpu_hours"],
                item["utilization"],
                priority
            )
        )

    print()


# ----------------------------------------------------------------------
# Update Slurm priorities
# ----------------------------------------------------------------------

def update_priority(user, priority):
    """Set Slurm association priority."""

    command = [
        SACCTMGR,
        "modify",
        "user",
        user,
        "set",
        "-i",
        "priority={}".format(priority)
    ]

    print(
        "Updating {} -> priority {}".format(
            user,
            priority
        )
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


# ----------------------------------------------------------------------
# Arguments
# ----------------------------------------------------------------------

def parse_args():

    now = datetime.today()

    default_start = (
        now - timedelta(days=7)
    ).strftime("%Y-%m-%dT%H:%M:%S")

    default_end = now.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    default_user = (
        __import__("os").environ.get("USER")
        or __import__("os").environ.get("USERNAME")
    )

    parser = argparse.ArgumentParser(
        description="Report Slurm GPU utilization"
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
        help="Start time"
    )

    parser.add_argument(
        "-E",
        "--endtime",
        default=default_end,
        help="End time"
    )

    parser.add_argument(
        "-n",
        "--njobs",
        type=int,
        default=-1,
        help="Maximum number of jobs per user"
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
        "--update",
        action="store_true",
        help="Update Slurm priority"
    )

    parser.add_argument(
        "--details",
        action="store_true",
        help="Show individual jobs"
    )

    return parser.parse_args()


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():

    args = parse_args()

    try:
        output = run_sacct(args)

    except RuntimeError as error:
        print("ERROR:", error)
        return 1

    if not output.strip():
        print("No records found.")
        return 0

    jobs = parse_sacct(output)

    summary = OrderedDict()

    for user in sorted(jobs):

        selected = select_jobs(
            jobs[user]
        )

        if not selected:
            continue

        # Random sampling, if requested.
        if (
            args.njobs > 0
            and len(selected) > args.njobs
        ):
            selected = random.sample(
                selected,
                args.njobs
            )

        user_summary = summarize_user(
            selected
        )

        summary[user] = user_summary

        if args.details:
            print_job_report(
                user,
                selected
            )

    if not summary:
        print(
            "No GPU jobs found."
        )
        return 0

    print_summary(summary)

    # --------------------------------------------------------------
    # Optionally update Slurm priorities
    # --------------------------------------------------------------
    if args.update:

        print(
            "Updating Slurm association priorities..."
        )

        for user in sorted(summary):

            if user == "rharrison":
                continue

            priority = priority_from_utilization(
                summary[user]["utilization"]
            )

            update_priority(
                user,
                priority
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
