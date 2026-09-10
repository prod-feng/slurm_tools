#!/usr/bin/env python3

from __future__ import print_function

import argparse
import csv
import fnmatch
import os
import random
import subprocess
from collections import OrderedDict
from datetime import datetime, timedelta

#
# ======================================================================
# Configuration
# ======================================================================
#
# For very large logical job, like with 100+ tasks, be careful, since user can have multiple parallel 
# processes/tasks to run at the same time and utilize GPUs, or a fraction of a GPU. Use the wall time of the longest task, like "12333.0", 
# as the real GPU elapsed wall time. 
# For gpuutil calculation, it is too complicated now, there are a lot of overlaped time slots for many tasks, so now only simply to count the averaege across all task(total number of tasks).
# Sum(each gpuutil)/Sum(Gpu_hours), which are also # of GPU weighted. This mostly will get 
# very underestimated ave GPUUTIL for the whole job. More to come...
#
#
#
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

SACCT_FIELDS = [
    "USER",
    "JobID",
    "Partition",
    "State",
    "Start",
    "Elapsed",
    "NNodes",
    "NCPUS",
    "NodeList",
    "CPUTime",
    "SystemCPU",
    "TotalCPU",
    "UserCPU",
    "TRESUsageInAve",
    "AllocTRES"
]

SACCT_FORMAT = ",".join(SACCT_FIELDS)


# ======================================================================
# Time conversion
# ======================================================================

def time_to_hours(value):
    # Convert Slurm elapsed time to hours.

    if not value:
        return 0.0

    value = value.strip()

    if not value:
        return 0.0

    value = value.split(".", 1)[0]

    days = 0

    if "-" in value:
        try:
            day_string, value = value.split("-", 1)
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
# TRES parsing
# ======================================================================

def parse_tres_items(value):
    # Parse a comma-separated Slurm TRES string.

    result = []

    if not value:
        return result

    for item in value.split(","):
        item = item.strip()

        if not item:
            continue

        if "=" in item:
            key, val = item.split("=", 1)
            result.append(
                (
                    key.strip(),
                    val.strip()
                )
            )

    return result


def parse_gpu_info(tres_usage, alloc_tres):
    # Return:
    #
    #   gpu_util
    #   gpu_count
    #   gpu_type
    #
    # Example:
    #
    #   gres/gpuutil=37
    #   gres/gpu:h200=1
    #   gres/gpu=1

    gpu_util = None
    gpu_count = 0
    gpu_type = None

    # --------------------------------------------------------------
    # GPU utilization
    # --------------------------------------------------------------

    for key, value in parse_tres_items(tres_usage):

        key_lower = key.lower()

        if key_lower == "gres/gpuutil":

            try:
                gpu_util = float(value)
            except ValueError:
                pass

    # --------------------------------------------------------------
    # GPU allocation and type
    # --------------------------------------------------------------

    for key, value in parse_tres_items(alloc_tres):

        key_lower = key.lower()

        if key_lower == "gres/gpu":

            try:
                gpu_count = int(float(value))
            except ValueError:
                pass

        elif key_lower.startswith("gres/gpu:"):

            type_name = key.split(":", 1)[1].strip()

            try:
                count = int(float(value))
            except ValueError:
                count = 0

            if count > gpu_count:
                gpu_count = count

            if type_name:
                gpu_type = type_name

    if gpu_util is None:
        gpu_util = 0.0

    gpu_util = max(
        0.0,
        min(100.0, gpu_util)
    )

    return gpu_util, gpu_count, gpu_type


# ======================================================================
# Job ID helpers
# ======================================================================

def is_extern_record(jobid):
    return (
        ".extern" in jobid.lower()
    )


def is_batch_record(jobid):
    return (
        ".batch" in jobid.lower()
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

    return "job"


def get_parent_jobid(jobid):
    # Convert:
    #
    #   83120.35       -> 83120
    #   83120.batch    -> 83120
    #   83120.extern   -> 83120
    #   83120_5.35     -> 83120_5
    #   83120+1.35     -> 83120+1
    #
    # Only the first dot is significant.

    if "." not in jobid:
        return jobid

    return jobid.split(".", 1)[0]


# ======================================================================
# GPU filters
# ======================================================================

def matches_gpu_type(gpu_type, patterns):
    # No filter means everything matches.

    if not patterns:
        return True

    if not gpu_type:
        return False

    value = gpu_type.lower()

    for pattern in patterns:

        pattern = pattern.strip().lower()

        if not pattern:
            continue

        if fnmatch.fnmatchcase(
            value,
            pattern
        ):
            return True

    return False


def matches_partition(partition, patterns):
    # No filter means everything matches.

    if not patterns:
        return True

    if not partition:
        return False

    value = partition.lower()

    for pattern in patterns:

        pattern = pattern.strip().lower()

        if not pattern:
            continue

        if fnmatch.fnmatchcase(
            value,
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
        "-r", PARTITIONS,
        "--format=" + SACCT_FORMAT,
        "-S", starttime,
        "-E", endtime
    ]

    # --------------------------------------------------------------
    # User selection
    # --------------------------------------------------------------

    if args.jobs:

        command.extend([
            "-j",
            args.jobs,
            "-a"
        ])

    elif args.allusers:

        command.append("-a")

    else:

        command.extend([
            "-u",
            args.user
        ])

    # --------------------------------------------------------------
    # State selection
    # --------------------------------------------------------------

    if not all_states and not args.allstates:

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
# Create raw sacct records
# ======================================================================

def parse_raw_sacct(output):

    records = []

    fields = SACCT_FIELDS

    for line in output.splitlines():

        if not line.strip():
            continue

        values = line.split("|")

        if len(values) != len(fields):

            if DEBUG:

                print(
                    "Skipping malformed line with {} fields:".format(
                        len(values)
                    )
                )

                print(line)

            continue

        data = dict(
            zip(
                fields,
                values
            )
        )

        for key in data:
            data[key] = data[key].strip()

        records.append(data)

    return records


# ======================================================================
# Parse sacct
# ======================================================================

def parse_sacct(output, args):

    # The important design here is:
    #
    # 1. First collect every sacct record.
    #
    # 2. Remember the USER associated with each top-level job.
    #
    # 3. Attach child records to their parent using JobID.
    #
    # 4. Do NOT depend on USER being present in child records.
    #
    # 5. Do NOT use the parent record itself as the utilization record.
    #
    # 6. Sum all GPU-using child steps.
    #
    # This avoids the old bug where only the largest child step was used.
    records = parse_raw_sacct(output)
    jobs = OrderedDict()

    # --------------------------------------------------------------
    # First pass:
    #
    # Build all logical parent jobs.
    # --------------------------------------------------------------

    for data in records:

        jobid = data["JobID"]

        if not jobid:
            continue

        # Child records are handled later.
        if is_job_step(jobid):
            continue

        user = data["USER"]

        if not user:
            continue

        if is_extern_record(jobid):
            continue

        partition = data["Partition"]

        if not matches_partition(
            partition,
            args.partition_patterns
        ):
            continue

        gpu_util, gpu_count, gpu_type = parse_gpu_info(
            data["TRESUsageInAve"],
            data["AllocTRES"]
        )

        jobs[jobid] = {
            "user": user,
            "partition": partition,
            "state": data["State"],
            "start": data["Start"],
            "elapsed": 0.0,
            "gpu_hours": 0.0,
            "gpu_util_hours": 0.0,
            "gpu_count": gpu_count,
            "gpu_type": gpu_type,
            "job_type": get_job_type(jobid),
            "steps": 0,
            "gpu_util_seen": False,
            "child_records": 0,
            "parent_gpu_util": gpu_util,
            "parent_gpu_hours": 0.0,
        }
    # --------------------------------------------------------------
    # Second pass:
    #
    # Attach every child record to its parent.
    # --------------------------------------------------------------

    for data in records:
        jobid = data["JobID"]

        if not jobid:
            continue

        if not is_job_step(jobid):
            continue

        if is_extern_record(jobid):
            continue
        #if ".0" in jobid.lower():
            # for Large logical jobs, the first one is not counted
        #    continue
        parent_id = get_parent_jobid(jobid)
        if parent_id not in jobs:
            continue

        job = jobs[parent_id]

        job["child_records"] += 1
        # ----------------------------------------------------------
        # Ignore .extern.
        # ----------------------------------------------------------

        if is_extern_record(jobid):
            continue

        # ----------------------------------------------------------
        # Parse the step itself.
        # ----------------------------------------------------------

        gpu_util, gpu_count, gpu_type = parse_gpu_info(
            data["TRESUsageInAve"],
            data["AllocTRES"]
        )
        #large logical job, ignore the .0 master task
        #if ".0" in jobid.lower() and gpu_util<=0.0:
            # for Large logical jobs, the first one is not counted
        #    continue
        # ----------------------------------------------------------
        # If the child doesn't contain GPU allocation,
        # inherit from the parent.
        # ----------------------------------------------------------

        effective_gpu_count = gpu_count

        if effective_gpu_count <= 0:

            effective_gpu_count = job["gpu_count"]

        if effective_gpu_count <= 0:
            continue

        effective_gpu_type = gpu_type

        if not effective_gpu_type:
            effective_gpu_type = job["gpu_type"]

        # ----------------------------------------------------------
        # GPU type filter.
        #
        # If the user requested:
        #
        #   --gputype=h200
        #
        # then a job whose GPU type is h200 is included.
        #
        # Wildcards such as:
        #
        #   --gputype=h200*
        #
        # also work.
        # ----------------------------------------------------------

        if not matches_gpu_type(
            effective_gpu_type,
            args.gpu_type_patterns
        ):
            continue

        # ----------------------------------------------------------
        # We only count records that actually contain GPU
        # utilization information.
        #
        # This is important because .extern and other steps may
        # have AllocTRES but no meaningful GPU utilization.
        # ----------------------------------------------------------

        tres_usage = data["TRESUsageInAve"]

        has_gpu_util = (
            "gres/gpuutil=" in tres_usage.lower()
        )

        if not has_gpu_util:
            continue

        elapsed = time_to_hours(
            data["Elapsed"]
        )

        if elapsed <= 0:
            continue

        # ----------------------------------------------------------
        # Every GPU-utilizing step contributes GPU-hours.
        #
        # IMPORTANT:
        #
        # We deliberately SUM these instead of taking only the
        # largest one.
        #
        # For example:
        #
        #   83120.35   2.09 hours
        #   83120.36   2.30 hours
        #   83120.37   2.51 hours
        #
        # all contribute to the user's GPU utilization.
        # ----------------------------------------------------------

        step_gpu_hours = (
            elapsed
            * effective_gpu_count
        )

        step_util_gpu_hours = (
            gpu_util
            * step_gpu_hours
        )
        #Use the actual gpu_hours?
        #if step_gpu_hours > job["gpu_hours"]:
        #    job["gpu_hours"] = step_gpu_hours
        job["gpu_hours"] += step_gpu_hours

        job["gpu_util_hours"] += (
            step_util_gpu_hours
        )
        #Use the actual gpu_hours?
        if step_gpu_hours > job["elapsed"]:
            job["elapsed"] = step_gpu_hours
        #job["elapsed"] += elapsed

        job["steps"] += 1

        job["gpu_util_seen"] = True

        job["gpu_count"] = max(
            job["gpu_count"],
            effective_gpu_count
        )

        if effective_gpu_type:
            job["gpu_type"] = effective_gpu_type

    # --------------------------------------------------------------
    # Third pass:
    #
    # Calculate utilization.
    # --------------------------------------------------------------

    result = OrderedDict()

    for jobid in sorted(jobs):

        job = jobs[jobid]

        # ----------------------------------------------------------
        # A parent record without GPU-utilizing child records is NOT
        # considered a GPU job.
        #
        # This specifically prevents:
        #
        #   83120
        #   83120.extern
        #   83120.0
        #
        # from becoming a fake 40-hour GPU job.
        # ----------------------------------------------------------

        if not job["gpu_util_seen"]:
            continue

        if job["gpu_hours"] <= 0:
            continue

        if "a100" in job["partition"].lower():
            continue

        if not matches_partition(
            job["partition"],
            args.partition_patterns
        ):
            continue

        if not matches_gpu_type(
            job["gpu_type"],
            args.gpu_type_patterns
        ):
            continue

        job["utilization"] = (
            job["gpu_util_hours"]
            / job["gpu_hours"]
        )

        result[jobid] = job

    # --------------------------------------------------------------
    # Convert into user -> jobs.
    # --------------------------------------------------------------

    users = OrderedDict()

    for jobid in result:

        job = result[jobid]

        user = job["user"]

        if user not in users:
            users[user] = OrderedDict()

        users[user][jobid] = job

    return users


# ======================================================================
# Select GPU jobs
# ======================================================================

def select_gpu_jobs(user_jobs):

    selected = []

    for jobid, job in user_jobs.items():

        if job["gpu_hours"] <= 0:
            continue

        if not job["gpu_util_seen"]:
            continue

        selected.append(
            (
                jobid,
                job
            )
        )

    return selected


# ======================================================================
# User summary
# ======================================================================

def summarize_user(selected_jobs):

    total_gpu_hours = 0.0

    total_util_gpu_hours = 0.0
    #
    total_elapsed = 0.0
    for jobid, job in selected_jobs:

        total_gpu_hours += (
            job["gpu_hours"]
        )

        total_util_gpu_hours += (
            job["gpu_util_hours"]
        )
        #
        total_elapsed += (
            job["elapsed"]
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
        "gpu_hours": total_elapsed,#total_gpu_hours,
        "utilization": utilization
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
    print(
        "User: {}".format(user)
    )

    print(
        "{:<18} {:<8} {:>10} {:>8} {:>12} {:>10} {:<10}".format(
            "JobID",
            "Type",
            "Elapsed",
            "GPU",
            "GPU-hours",
            "GPU Util",
            "GPU Type"
        )
    )

    print("-" * 90)

    for jobid, job in selected_jobs:

        print(
            "{:<18} {:<8} {:>10.2f} {:>8d} {:>12.2f} {:>9.2f}% {:<10}".format(
                jobid,
                job["job_type"],
                job["elapsed"],
                job["gpu_count"],
                job["gpu_hours"],
                job["utilization"],
                job["gpu_type"] or "-"
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
        dest="details",
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
        action="append",
        dest="gpu_type_patterns",
        default=[],
        help=(
            "Only count GPU types matching the pattern. "
            "Can be specified multiple times. "
            "Examples: --gputype=h200 "
            "--gputype=h200*"
        )
    )

    parser.add_argument(
        "--partitions",
        action="append",
        dest="partition_patterns",
        default=[],
        help=(
            "Only count partitions matching the pattern. "
            "Can be specified multiple times. "
            "Example: --partitions=h200x8*"
        )
    )

    return parser.parse_args()


# ======================================================================
# Main
# ======================================================================

def main():

    args = parse_args()

    # --------------------------------------------------------------
    # Normal utilization report
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
        # njobs only affects detailed display.
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

        # ----------------------------------------------------------
        # IMPORTANT:
        #
        # Summary always uses ALL selected jobs.
        # ----------------------------------------------------------

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
    # GPU history
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
    # Priority/inactivity processing
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
        # Update active user priorities.
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

