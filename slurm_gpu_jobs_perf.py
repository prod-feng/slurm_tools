#!/usr/bin/env python3

# Slurm GPU utilization monitor.
#
# Python 3.6 compatible.
#
# Logical Slurm jobs:
#
#   12345       normal job
#   12345_0     array task
#   12345+0     heterogeneous job component
#
# Job steps:
#
#   12345.batch
#   12345.0
#   12345.1
#   12345_0.batch
#   12345_0.0
#   12345+0.batch
#
# Job steps are NOT counted as separate jobs.
#
# The parent job record provides allocation information.
# Child records provide GPU utilization when available.
#
# Example:
#
#   83120
#   83120.extern
#   83120.0
#   83120.35
#   83120.36
#
# is one logical job.
#
# --gputype=h200
# --gputype=6000
# --gputype='h200*'
#
# --partitions=h200x8*
#
# Wildcards use shell-style fnmatch matching.

from __future__ import print_function

import argparse
import csv
import fnmatch
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

GPU_HISTORY_FILE = (
    "/home/feng/"
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

    if not value:
        return 0.0

    value = value.strip()

    if not value:
        return 0.0

    value = value.split(".", 1)[0]

    days = 0

    if "-" in value:

        day_string, value = value.split("-", 1)

        try:
            days = int(day_string)
        except ValueError:
            return 0.0

    parts = value.split(":")

    if len(parts) != 3:
        return 0.0

    try:

        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])

    except ValueError:

        return 0.0

    return (
        days * 24.0
        + hours
        + minutes / 60.0
        + seconds / 3600.0
    )


# ======================================================================
# Job ID helpers
# ======================================================================

def is_extern_record(jobid):

    return (
        jobid == "extern"
        or jobid.endswith(".extern")
    )


def is_batch_record(jobid):

    return (
        jobid == "batch"
        or jobid.endswith(".batch")
    )


def is_job_step(jobid):

    return "." in jobid


def is_array_job(jobid):

    return (
        "_" in jobid
        and "." not in jobid
    )


def is_het_job(jobid):

    return (
        "+" in jobid
        and "." not in jobid
    )


def get_job_type(jobid):

    if is_array_job(jobid):
        return "array"

    if is_het_job(jobid):
        return "het"

    if is_job_step(jobid):
        return "step"

    return "job"


def get_parent_jobid(jobid):

    # 83120.batch -> 83120
    # 83120.35   -> 83120
    # 83120_2.35 -> 83120_2
    # 83120+1.35 -> 83120+1

    if "." in jobid:

        return jobid.split(".", 1)[0]

    return jobid


# ======================================================================
# GPU parsing
# ======================================================================

def parse_gpu_info(tres_usage, alloc_tres):

    gpu_util = None
    gpu_count = None
    gpu_type = None

    # --------------------------------------------------------------
    # GPU utilization
    # --------------------------------------------------------------

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

    # --------------------------------------------------------------
    # GPU allocation
    #
    # Examples:
    #
    #   gres/gpu:h200=1
    #   gres/gpu:6000=1
    #   gres/gpu=1
    #
    # Prefer the typed GPU entry.
    # --------------------------------------------------------------

    if alloc_tres:

        for item in alloc_tres.split(","):

            item = item.strip()

            if not item.startswith("gres/gpu"):

                continue

            if "=" not in item:

                continue

            left, right = item.split("=", 1)

            try:

                count = int(float(right))

            except ValueError:

                continue

            if count <= 0:
                continue

            if left.startswith("gres/gpu:"):

                candidate_type = left.split(
                    ":", 1
                )[1].strip()

                if candidate_type:

                    gpu_type = candidate_type
                    gpu_count = count
                    break

            elif left == "gres/gpu":

                if gpu_count is None:

                    gpu_count = count

    if gpu_util is None:
        gpu_util = 0.0

    if gpu_count is None:
        gpu_count = 0

    gpu_util = max(
        0.0,
        min(100.0, gpu_util)
    )

    return (
        gpu_util,
        gpu_count,
        gpu_type
    )


# ======================================================================
# GPU type filter
# ======================================================================

def gpu_type_matches(gpu_type, patterns):

    if not patterns:
        return True

    if not gpu_type:
        return False

    gpu_type = gpu_type.lower()

    for pattern in patterns:

        pattern = pattern.strip().lower()

        if not pattern:
            continue

        if fnmatch.fnmatchcase(
            gpu_type,
            pattern
        ):

            return True

    return False


# ======================================================================
# Partition filter
# ======================================================================

def partition_matches(partition, patterns):

    if not patterns:
        return True

    if not partition:
        return False

    partition = partition.lower()

    for pattern in patterns:

        pattern = pattern.strip().lower()

        if not pattern:
            continue

        if fnmatch.fnmatchcase(
            partition,
            pattern
        ):

            return True

    return False


# ======================================================================
# Run sacct
# ======================================================================

def run_sacct(args, starttime, endtime, all_states=False):

    command = [
        SACCT,
        "-n",
        "-P",
        "--starttime={}".format(starttime),
        "--endtime={}".format(endtime),
        "--format={}".format(SACCT_FORMAT),
    ]

    # --------------------------------------------------------------
    # Job selection
    # --------------------------------------------------------------

    if args.jobs:

        # IMPORTANT:
        #
        # Do not add -r or partition restrictions when using -j.
        #
        # -j is the authoritative job selector.
        #
        command.extend([
            "--jobs={}".format(args.jobs),
            "-a"
        ])

    else:

        # ----------------------------------------------------------
        # Partition selection
        # ----------------------------------------------------------

        if args.partitions:

            # sacct supports comma-separated partition lists,
            # but wildcard filtering is easier and safer to do
            # after parsing.
            #
            # Therefore request the configured partitions and
            # apply --partitions locally.
            command.extend([
                "-r",
                PARTITIONS
            ])

        else:

            command.extend([
                "-r",
                PARTITIONS
            ])

        # ----------------------------------------------------------
        # User selection
        # ----------------------------------------------------------

        if args.allusers:

            command.append("-a")

        else:

            command.extend([
                "-u",
                args.user
            ])

    # --------------------------------------------------------------
    # State selection
    # --------------------------------------------------------------

    if all_states or args.allstates:

        # No -s.
        pass

    else:

        command.extend([
            "-s",
            args.state
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

def parse_sacct(output, args):

    jobs = OrderedDict()

    fields = SACCT_FORMAT.split(",")

    # --------------------------------------------------------------
    # IMPORTANT:
    #
    # Do NOT use current_user/current_job.
    #
    # sacct child records normally have blank USER.
    #
    # Instead, determine the parent directly from the JobID.
    #
    # Example:
    #
    #   83120
    #   83120.extern
    #   83120.0
    #   83120.35
    #
    # All child records are attached to 83120.
    # --------------------------------------------------------------

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

        if not jobid:
            continue

        # ----------------------------------------------------------
        # Determine logical parent.
        # ----------------------------------------------------------

        parent_jobid = get_parent_jobid(jobid)

        # ----------------------------------------------------------
        # Parent record.
        #
        # A non-empty USER normally identifies the logical job.
        # ----------------------------------------------------------

        if user:

            logical_user = user

        else:

            # ------------------------------------------------------
            # Child record.
            #
            # Find the user by looking for an already-created
            # logical job.
            # ------------------------------------------------------

            logical_user = None

            for candidate_user in jobs:

                if parent_jobid in jobs[candidate_user]:

                    logical_user = candidate_user
                    break

            if logical_user is None:

                # This can happen if sacct output is not ordered
                # parent-first.
                #
                #We cannot attach this child yet.
                #
                # Save it for the second-pass parser below.
                continue

        # ----------------------------------------------------------
        # Ignore extern records.
        # ----------------------------------------------------------

        if is_extern_record(jobid):
            continue

        # ----------------------------------------------------------
        # Only parent records create logical jobs.
        # ----------------------------------------------------------

        if user:

            if logical_user not in jobs:

                jobs[logical_user] = OrderedDict()

            if jobid not in jobs[logical_user]:

                jobs[logical_user][jobid] = {
                    "partition": data["Partition"].strip(),
                    "state": data["State"].strip(),
                    "start": data["Start"].strip(),
                    "elapsed": 0.0,
                    "gpu_util_hours": 0.0,
                    "gpu_hours": 0.0,
                    "gpu_count": 0,
                    "gpu_type": None,
                    "steps": 0,
                    "job_type": get_job_type(jobid),
                    "gpu_util_seen": False,
                }

            job = jobs[logical_user][jobid]

            elapsed = time_to_hours(
                data["Elapsed"]
            )

            gpu_util, gpu_count, gpu_type = (
                parse_gpu_info(
                    data["TRESUsageInAve"],
                    data["AllocTRES"]
                )
            )

            if gpu_count > 0:

                job["gpu_count"] = max(
                    job["gpu_count"],
                    gpu_count
                )

            if gpu_type:

                job["gpu_type"] = gpu_type

            has_gpu_util = (
                data["TRESUsageInAve"]
                and
                "gres/gpuutil=" in
                data["TRESUsageInAve"]
            )

            if (
                has_gpu_util
                and gpu_count > 0
                and elapsed > 0
            ):

                gpu_hours = (
                    elapsed * gpu_count
                )

                # --------------------------------------------------
                # Parent is a candidate.
                #
                # Do not replace a real utilization value with
                # a zero-utilization record having the same or
                # smaller GPU-hours.
                # --------------------------------------------------

                if gpu_hours > job["gpu_hours"]:

                    job["elapsed"] = elapsed
                    job["gpu_hours"] = gpu_hours
                    job["gpu_util_hours"] = (
                        gpu_util * gpu_hours
                    )
                    job["gpu_count"] = max(
                        job["gpu_count"],
                        gpu_count
                    )

                    if gpu_type:
                        job["gpu_type"] = gpu_type

                    job["gpu_util_seen"] = True

                elif (
                    gpu_hours == job["gpu_hours"]
                    and gpu_util > 0
                    and job["gpu_util_hours"] == 0
                ):

                    job["elapsed"] = elapsed
                    job["gpu_util_hours"] = (
                        gpu_util * gpu_hours
                    )

                    if gpu_type:
                        job["gpu_type"] = gpu_type

                    job["gpu_util_seen"] = True

            continue

        # ----------------------------------------------------------
        # Child record.
        # ----------------------------------------------------------

        if logical_user is None:
            continue

        if parent_jobid not in jobs[logical_user]:
            continue

        job = jobs[logical_user][parent_jobid]

        elapsed = time_to_hours(
            data["Elapsed"]
        )

        gpu_util, gpu_count, gpu_type = (
            parse_gpu_info(
                data["TRESUsageInAve"],
                data["AllocTRES"]
            )
        )

        # ----------------------------------------------------------
        # Child allocation can be blank or incomplete.
        #
        # Inherit GPU count/type from parent.
        # ----------------------------------------------------------

        if gpu_count <= 0:

            gpu_count = job["gpu_count"]

        if not gpu_type:

            gpu_type = job["gpu_type"]

        if gpu_count <= 0:
            continue

        # ----------------------------------------------------------
        # Only records containing gpuutil are useful.
        # ----------------------------------------------------------

        has_gpu_util = (
            data["TRESUsageInAve"]
            and
            "gres/gpuutil=" in
            data["TRESUsageInAve"]
        )

        if not has_gpu_util:
            continue

        if elapsed <= 0:
            continue

        # ----------------------------------------------------------
        # Apply GPU type filter at the logical job level.
        # ----------------------------------------------------------

        if not gpu_type_matches(
            gpu_type,
            args.gputypes
        ):

            continue

        gpu_hours = (
            elapsed * gpu_count
        )

        job["steps"] += 1

        # ----------------------------------------------------------
        # The key algorithm:
        #
        # Never add .batch + .0 + .35 + .36.
        #
        # They may overlap.
        #
        # Select the record with the largest GPU-hours.
        #
        # BUT:
        #
        # If GPU-hours are equal, prefer non-zero utilization.
        #
        # Also, if an existing record has zero utilization and the
        # new record has real utilization, replace it even when
        # floating point differences make the GPU-hours slightly
        # different.
        # ----------------------------------------------------------

        replace = False

        if gpu_hours > job["gpu_hours"]:

            replace = True

        elif (
            gpu_hours == job["gpu_hours"]
            and gpu_util > 0
            and job["gpu_util_hours"] == 0
        ):

            replace = True

        elif (
            gpu_util > 0
            and job["gpu_util_hours"] == 0
            and gpu_hours > 0
        ):

            # ------------------------------------------------------
            # This is particularly important for jobs such as 83120.
            #
            # The parent can have:
            #
            #   gres/gpu=1
            #
            # but no useful gpuutil.
            #
            # A later step can have:
            #
            #   gres/gpuutil=23
            #
            # We must not leave the parent zero-utilization value.
            # ------------------------------------------------------

            replace = True

        if replace:

            job["elapsed"] = elapsed

            job["gpu_hours"] = gpu_hours

            job["gpu_util_hours"] = (
                gpu_util * gpu_hours
            )

            job["gpu_count"] = max(
                job["gpu_count"],
                gpu_count
            )

            if gpu_type:

                job["gpu_type"] = gpu_type

            job["gpu_util_seen"] = True

    # ==================================================================
    # Second pass
    #
    # The first pass assumes parent records occur before child records.
    #
    # sacct normally does this, but we should not depend on it.
    #
    # Re-process all child records and attach them to the logical
    # parent now that all parent records are known.
    # ==================================================================

    for line in output.splitlines():

        if not line.strip():
            continue

        values = line.split("|")

        if len(values) != len(fields):
            continue

        data = dict(
            zip(fields, values)
        )

        jobid = data["JobID"].strip()
        user = data["USER"].strip()

        if not jobid:
            continue

        if user:
            continue

        if is_extern_record(jobid):
            continue

        parent_jobid = get_parent_jobid(jobid)

        logical_user = None

        for candidate_user in jobs:

            if parent_jobid in jobs[candidate_user]:

                logical_user = candidate_user
                break

        if logical_user is None:
            continue

        job = jobs[logical_user][parent_jobid]

        elapsed = time_to_hours(
            data["Elapsed"]
        )

        gpu_util, gpu_count, gpu_type = (
            parse_gpu_info(
                data["TRESUsageInAve"],
                data["AllocTRES"]
            )
        )

        if gpu_count <= 0:

            gpu_count = job["gpu_count"]

        if not gpu_type:

            gpu_type = job["gpu_type"]

        if gpu_count <= 0:
            continue

        has_gpu_util = (
            data["TRESUsageInAve"]
            and
            "gres/gpuutil=" in
            data["TRESUsageInAve"]
        )

        if not has_gpu_util:
            continue

        if elapsed <= 0:
            continue

        if not gpu_type_matches(
            gpu_type,
            args.gputypes
        ):

            continue

        gpu_hours = (
            elapsed * gpu_count
        )

        replace = False

        if gpu_hours > job["gpu_hours"]:

            replace = True

        elif (
            gpu_hours == job["gpu_hours"]
            and gpu_util > 0
            and job["gpu_util_hours"] == 0
        ):

            replace = True

        elif (
            gpu_util > 0
            and job["gpu_util_hours"] == 0
            and gpu_hours > 0
        ):

            replace = True

        if replace:

            job["elapsed"] = elapsed

            job["gpu_hours"] = gpu_hours

            job["gpu_util_hours"] = (
                gpu_util * gpu_hours
            )

            job["gpu_count"] = max(
                job["gpu_count"],
                gpu_count
            )

            if gpu_type:
                job["gpu_type"] = gpu_type

            job["gpu_util_seen"] = True

    # ==================================================================
    # Final logical-job filtering and utilization calculation.
    # ==================================================================

    for user in jobs:

        remove = []

        for jobid in jobs[user]:

            job = jobs[user][jobid]

            # ----------------------------------------------------------
            # Partition filter.
            # ----------------------------------------------------------

            if not partition_matches(
                job["partition"],
                args.partitions
            ):

                remove.append(jobid)
                continue

            # ----------------------------------------------------------
            # GPU type filter.
            # ----------------------------------------------------------

            if not gpu_type_matches(
                job["gpu_type"],
                args.gputypes
            ):

                remove.append(jobid)
                continue

            if job["gpu_hours"] > 0:

                job["utilization"] = (
                    job["gpu_util_hours"]
                    / job["gpu_hours"]
                )

            else:

                job["utilization"] = 0.0

        for jobid in remove:

            del jobs[user][jobid]

    # Remove users with no remaining jobs.

    empty_users = []

    for user in jobs:

        if not jobs[user]:

            empty_users.append(user)

    for user in empty_users:

        del jobs[user]

    return jobs


# ======================================================================
# Select GPU jobs
# ======================================================================

def select_gpu_jobs(user_jobs):

    selected = []

    for jobid, job in user_jobs.items():

        if job["gpu_hours"] <= 0:
            continue

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

    total_gpu_hours = 0.0
    total_util_gpu_hours = 0.0

    for jobid, job in selected_jobs:

        total_gpu_hours += (
            job["gpu_hours"]
        )

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
        "{:<18} {:<8} {:<18} {:>10} {:>8} {:>12} {:>10}".format(
            "JobID",
            "Type",
            "Partition",
            "Elapsed",
            "GPU",
            "GPU-hours",
            "GPU Util"
        )
    )

    print("-" * 100)

    for jobid, job in selected_jobs:

        print(
            "{:<18} {:<8} {:<18} {:>10.2f} {:>8d} {:>12.2f} {:>9.2f}%".format(
                jobid,
                job["job_type"],
                job["partition"],
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
    print("=" * 78)
    print("GPU UTILIZATION SUMMARY")
    print("=" * 78)

    print(
        "{:<16} {:>8} {:>14} {:>14} {:>10}".format(
            "User",
            "Jobs",
            "GPU-hours",
            "GPU Util",
            "Priority"
        )
    )

    print("-" * 78)

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

    os.rename(
        temporary,
        filename
    )


# ======================================================================
# Update history
# ======================================================================

def update_gpu_history(
    history,
    active_users
):

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

def update_priority(
    user,
    priority
):

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
        help="Specific job or comma-separated jobs"
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
        help="Maximum jobs per user in detailed report"
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
        "--detail",
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

    parser.add_argument(
        "--gputype",
        "--gputypes",
        dest="gputypes",
        action="append",
        default=[],
        help=(
            "GPU type filter. May be specified multiple times. "
            "Wildcards are supported."
        )
    )

    parser.add_argument(
        "--partitions",
        dest="partitions",
        action="append",
        default=[],
        help=(
            "Partition filter. May be specified multiple times. "
            "Wildcards are supported."
        )
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

    jobs = parse_sacct(
        output,
        args
    )

    summary = OrderedDict()

    for user in sorted(jobs):

        selected = select_gpu_jobs(
            jobs[user]
        )

        if not selected:
            continue

        # ----------------------------------------------------------
        # This only affects the detailed display.
        # Summary always uses ALL selected jobs.
        # ----------------------------------------------------------

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
    # 2. Load history
    # --------------------------------------------------------------

    history = load_gpu_history(
        GPU_HISTORY_FILE
    )

    active_users = set(
        summary.keys()
    )

    update_gpu_history(
        history,
        active_users
    )

    save_gpu_history(
        GPU_HISTORY_FILE,
        history
    )

    # --------------------------------------------------------------
    # 3. Inactivity / priority processing
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
                inactive_end,
                all_states=True
            )

        except RuntimeError as error:

            print(
                "ERROR checking 14-day GPU activity:",
                error
            )

            return 1

        inactive_jobs = parse_sacct(
            inactive_output,
            args
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

                history[user] = (
                    datetime.now().strftime(
                        "%Y-%m-%d"
                    )
                )

        save_gpu_history(
            GPU_HISTORY_FILE,
            history
        )

        # ----------------------------------------------------------
        # Set active-user priorities.
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
        # Reset inactive users.
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


# ======================================================================
# Entry point
# ======================================================================

if __name__ == "__main__":

    raise SystemExit(
        main()
    )
