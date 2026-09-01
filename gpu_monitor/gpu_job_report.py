#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Completed Slurm GPU job performance report.

Python 3.6 compatible.

Directory layout:

    gpu_job_report.py
    gpu_utilization_monitor.py
    modules/
        __init__.py
        config.py
        slurm.py
        reporting.py
        utils.py

Examples:

    ./gpu_job_report.py
    ./gpu_job_report.py --allusers -S 2026-08-20
    ./gpu_job_report.py --print
    ./gpu_job_report.py --csv
    ./gpu_job_report.py --emailusers
    ./gpu_job_report.py --update-priority
    ./gpu_job_report.py --emailusers --update-priority
"""

from __future__ import print_function

import argparse
import csv
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta

from modules.config import Config
from modules.reporting import (
    aggregate_jobs,
    build_user_summaries,
    calculate_priority,
    calculate_user_gpu_efficiency,
    format_user_csv_report,
    format_user_text_report,
    load_gpu_users,
    save_gpu_users,
    send_performance_email,
)
from modules.slurm import SlurmClient
from modules.utils import (
    human_size,
    size_to_gb,
    time_to_hours,
)


LOG = logging.getLogger("gpu_monitor.report")


# ============================================================================
# Local compatibility helpers
#
# These are deliberately kept here instead of importing FileLock and
# get_user_email from modules.utils.  This prevents ImportError if the
# current modules/utils.py does not contain those functions.
# ============================================================================

class FileLock(object):
    """
    Very small lock-file implementation.

    Python 3.6 compatible.

    The lock is created atomically with O_CREAT | O_EXCL.
    """

    def __init__(self, filename):
        self.filename = filename
        self.fd = None

    def acquire(self):
        lock_dir = os.path.dirname(self.filename)

        if lock_dir and not os.path.isdir(lock_dir):
            try:
                os.makedirs(lock_dir)
            except OSError:
                if not os.path.isdir(lock_dir):
                    raise

        try:
            self.fd = os.open(
                self.filename,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY
            )

            message = (
                "pid={}\n"
                "host={}\n"
                "time={}\n"
            ).format(
                os.getpid(),
                os.uname().nodename
                if hasattr(os, "uname")
                else "unknown",
                datetime.now().isoformat()
            )

            os.write(
                self.fd,
                message.encode("utf-8")
            )

        except OSError as exc:

            if exc.errno == 17:
                raise RuntimeError(
                    "Another gpu_job_report.py process "
                    "appears to be running "
                    "(lock file: {})".format(
                        self.filename
                    )
                )

            raise

    def release(self):

        if self.fd is not None:

            try:
                os.close(self.fd)
            except OSError:
                pass

            self.fd = None

        try:
            os.unlink(self.filename)
        except OSError:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


def get_user_email(user):
    """
    Obtain the user's email address from getent passwd.

    The passwd GECOS field is expected to contain something like:

        Full Name,email@example.com

    If no address can be found, return None.
    """

    if not user:
        return None

    try:

        output = subprocess.check_output(
            [
                "getent",
                "passwd",
                user
            ],
            stderr=subprocess.STDOUT
        )

        output = output.decode(
            "utf-8",
            "ignore"
        ).strip()

    except Exception:
        return None

    if not output:
        return None

    fields = output.split(":")

    if len(fields) < 5:
        return None

    gecos = fields[4]

    for item in gecos.split(","):

        item = item.strip()

        if "@" in item and " " not in item:

            return item

    return None


# ============================================================================
# Logging
# ============================================================================

def configure_logging(args):

    if args.debug:

        level = logging.DEBUG

    elif args.verbose:

        level = logging.INFO

    else:

        level = logging.WARNING

    logging.basicConfig(
        level=level,
        format=(
            "%(asctime)s "
            "%(levelname)-5s "
            "%(name)s: "
            "%(message)s"
        )
    )


# ============================================================================
# Defaults
# ============================================================================

def default_start():

    return (
        datetime.today()
        -
        timedelta(
            days=Config.DEFAULT_REPORT_DAYS
        )
    ).isoformat(
        sep="T",
        timespec="seconds"
    )


def default_end():

    return datetime.today().isoformat(
        sep="T",
        timespec="seconds"
    )


# ============================================================================
# CLI
# ============================================================================

def parse_args():

    current_user = os.environ.get(
        "USER",
        os.environ.get(
            "USERNAME",
            ""
        )
    )

    default_user = getattr(
        Config,
        "DEFAULT_USER",
        current_user
    )

    parser = argparse.ArgumentParser(
        description=(
            "Calculate CPU, memory, disk and GPU "
            "usage for completed Slurm jobs."
        )
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "-u",
        "--user",
        default=default_user,
        help="Comma-separated users"
    )

    group.add_argument(
        "--allusers",
        action="store_true",
        help="Display all users' jobs"
    )

    group.add_argument(
        "-j",
        "--jobs",
        help=(
            "Specific job IDs or comma-separated jobs"
        )
    )

    parser.add_argument(
        "-S",
        "--starttime",
        default=default_start(),
        help=(
            "Select jobs after "
            "YYYY-MM-DD[THH:MM[:SS]]"
        )
    )

    parser.add_argument(
        "-E",
        "--endtime",
        default=default_end(),
        help=(
            "Select jobs before this time"
        )
    )

    parser.add_argument(
        "-n",
        "--njobs",
        default=getattr(
            Config,
            "DEFAULT_NJOBS",
            100
        ),
        type=int,
        help=(
            "Maximum number of randomly "
            "selected jobs per user"
        )
    )

    parser.add_argument(
        "-s",
        "--state",
        default=getattr(
            Config,
            "DEFAULT_STATE",
            "CD"
        ),
        help="Select jobs in this state"
    )

    parser.add_argument(
        "--allstates",
        action="store_true",
        help="Display jobs in all states"
    )

    parser.add_argument(
        "--emailusers",
        action="store_true",
        help="Send report email to users"
    )

    parser.add_argument(
        "--csv",
        action="store_true",
        help="Print CSV output"
    )

    parser.add_argument(
        "--print",
        dest="print_report",
        action="store_true",
        help="Print job details"
    )

    parser.add_argument(
        "--update-priority",
        action="store_true",
        help=(
            "Update Slurm user priority "
            "according to GPU efficiency"
        )
    )

    parser.add_argument(
        "--reset-inactive",
        action="store_true",
        help=(
            "Reset priority to 100 for users "
            "with no GPU jobs in the report"
        )
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable informational logging"
    )

    parser.add_argument(
        "--gpu-users-file",
        default=getattr(
            Config,
            "GPU_USERS_FILE",
            "/lustre/nvwulf/admin/scripts/"
            "gpu_utilization_monitor/gpujobs.csv"
        ),
        help="GPU user tracking CSV"
    )

    parser.add_argument(
        "--lock-file",
        default=getattr(
            Config,
            "LOCK_FILE",
            "/tmp/gpu_job_report.lock"
        ) + ".report",
        help="Process lock file"
    )

    return parser.parse_args()


# ============================================================================
# Slurm retrieval
# ============================================================================

def get_jobs(args, slurm):

    users = None

    if (
        args.user
        and not args.allusers
        and not args.jobs
    ):

        users = [
            user.strip()
            for user in args.user.split(",")
            if user.strip()
        ]

    jobs = None

    if args.jobs:

        jobs = [
            job.strip()
            for job in args.jobs.split(",")
            if job.strip()
        ]

    states = None

    if not args.allstates and args.state:

        states = [
            args.state
        ]

    LOG.debug(
        "Slurm query: users=%s all_users=%s jobs=%s "
        "start=%s end=%s states=%s",
        users,
        args.allusers,
        jobs,
        args.starttime,
        args.endtime,
        states
    )

    return slurm.get_completed_jobs(
        users=users,
        all_users=args.allusers,
        jobs=jobs,
        start=args.starttime,
        end=args.endtime,
        states=states
    )


# ============================================================================
# Reporting
# ============================================================================

def print_reports(
        summaries,
        csv_output=False):

    if csv_output:

        for user in sorted(summaries.keys()):

            print(
                format_user_csv_report(
                    summaries[user]
                )
            )

        return

    for user in sorted(summaries.keys()):

        summary = summaries[user]

        print("")
        print(
            "User: {}".format(
                user
            )
        )

        print(
            format_user_text_report(
                summary
            )
        )

        print("")


def email_reports(summaries):

    fallback_email = getattr(
        Config,
        "FALLBACK_EMAIL",
        ""
    )

    getent_command = getattr(
        Config,
        "GETENT",
        "getent"
    )

    getent_timeout = getattr(
        Config,
        "GETENT_TIMEOUT",
        10
    )

    for user in sorted(summaries.keys()):

        email_address = None

        try:

            email_address = get_user_email(
                user
            )

        except Exception:

            LOG.exception(
                "Unable to determine email for %s",
                user
            )

        if not email_address:

            email_address = fallback_email

            LOG.warning(
                "No email found for %s; "
                "using fallback %s",
                user,
                email_address
            )

        try:

            send_performance_email(
                summaries[user],
                email_address
            )

            print(
                "Sent report to {} ({})".format(
                    user,
                    email_address
                )
            )

        except Exception:

            LOG.exception(
                "Unable to send report to %s",
                user
            )


# ============================================================================
# GPU priority management
# ============================================================================

def update_priorities(
        summaries,
        slurm,
        update=False):

    gpu_users = {}

    today = datetime.now().strftime(
        "%Y-%m-%d"
    )

    excluded_users = getattr(
        Config,
        "PRIORITY_EXCLUDED_USERS",
        []
    )

    for user in sorted(summaries.keys()):

        if user in excluded_users:

            LOG.info(
                "Skipping priority update "
                "for excluded user %s",
                user
            )

            continue

        summary = summaries[user]

        efficiency = (
            calculate_user_gpu_efficiency(
                summary
            )
        )

        priority = calculate_priority(
            efficiency
        )

        gpu_users[user] = today

        command_text = (
            "sacctmgr modify user {} "
            "set -i priority={}"
            .format(
                user,
                priority
            )
        )

        print(command_text)

        if update:

            try:

                slurm.set_user_priority(
                    user,
                    priority
                )

            except Exception:

                LOG.exception(
                    "Unable to update priority "
                    "for %s",
                    user
                )

    return gpu_users


def reset_inactive_users(
        current_gpu_users,
        old_gpu_users,
        slurm,
        update=False):

    inactive = []

    for user in old_gpu_users:

        if user in current_gpu_users:

            continue

        inactive.append(
            user
        )

    default_priority = getattr(
        Config,
        "DEFAULT_PRIORITY",
        100
    )

    excluded_users = getattr(
        Config,
        "PRIORITY_EXCLUDED_USERS",
        []
    )

    for user in sorted(inactive):

        if user in excluded_users:

            continue

        command_text = (
            "sacctmgr modify user {} "
            "set -i priority={}"
            .format(
                user,
                default_priority
            )
        )

        print(
            "Inactive user: {}".format(
                user
            )
        )

        print(
            command_text
        )

        if update:

            try:

                slurm.set_user_priority(
                    user,
                    default_priority
                )

            except Exception:

                LOG.exception(
                    "Unable to reset priority "
                    "for %s",
                    user
                )


# ============================================================================
# GPU-user CSV handling
# ============================================================================

def update_gpu_user_file(
        filename,
        current_gpu_users):

    old_gpu_users = {}

    try:

        old_gpu_users = load_gpu_users(
            filename
        )

    except Exception:

        LOG.exception(
            "Unable to load GPU user file %s",
            filename
        )

    all_users = dict(
        old_gpu_users
    )

    all_users.update(
        current_gpu_users
    )

    save_gpu_users(
        filename,
        all_users
    )

    return old_gpu_users


# ============================================================================
# Main
# ============================================================================

def main():

    args = parse_args()

    configure_logging(
        args
    )

    if args.njobs < 1:

        LOG.warning(
            "Invalid --njobs value %d; "
            "using 10",
            args.njobs
        )

        args.njobs = 10

    if args.update_priority:

        LOG.warning(
            "Slurm priority modification "
            "is ENABLED"
        )

    # ------------------------------------------------------------------
    # Make sure required directories exist if Config supports it.
    # ------------------------------------------------------------------

    if hasattr(
        Config,
        "ensure_directories"
    ):

        Config.ensure_directories()

    # ------------------------------------------------------------------
    # Lock.
    # ------------------------------------------------------------------

    lock = FileLock(
        args.lock_file
    )

    try:

        lock.acquire()

    except RuntimeError as exc:

        LOG.error(
            "%s",
            exc
        )

        return 2

    try:

        # --------------------------------------------------------------
        # Slurm client.
        # --------------------------------------------------------------

        slurm = SlurmClient(
            Config
        )

        # --------------------------------------------------------------
        # Get accounting data.
        #
        # IMPORTANT:
        # gpu_job_report.py does NOT construct sacct_cmd.
        #
        # modules/slurm.py owns all sacct parsing/query details.
        # --------------------------------------------------------------

        records = get_jobs(
            args,
            slurm
        )

        LOG.info(
            "Retrieved %d Slurm records",
            len(records)
        )

        if not records:

            print(
                "No records found. Please try again."
            )

            return 0

        # --------------------------------------------------------------
        # Aggregate primary jobs and job steps.
        # --------------------------------------------------------------

        jobs = aggregate_jobs(
            records
        )

        LOG.info(
            "Aggregated into %d primary jobs",
            len(jobs)
        )

        if not jobs:

            print(
                "No usable jobs found."
            )

            return 0

        # --------------------------------------------------------------
        # Build per-user summaries.
        # --------------------------------------------------------------

        summaries = build_user_summaries(
            jobs,
            max_jobs=args.njobs,
            randomize=not args.csv
        )

        if not summaries:

            print(
                "No eligible GPU jobs found."
            )

            return 0

        # --------------------------------------------------------------
        # Output.
        # --------------------------------------------------------------

        if (
            args.print_report
            or args.csv
        ):

            print_reports(
                summaries,
                csv_output=args.csv
            )

        # --------------------------------------------------------------
        # Email.
        # --------------------------------------------------------------

        if args.emailusers:

            email_reports(
                summaries
            )

        # --------------------------------------------------------------
        # GPU priority tracking.
        #
        # Do not touch Slurm priority unless explicitly requested.
        # --------------------------------------------------------------

        if (
            args.update_priority
            or args.reset_inactive
        ):

            old_gpu_users = load_gpu_users(
                args.gpu_users_file
            )

            current_gpu_users = (
                update_priorities(
                    summaries,
                    slurm,
                    update=args.update_priority
                )
            )

            if args.reset_inactive:

                reset_inactive_users(
                    current_gpu_users,
                    old_gpu_users,
                    slurm,
                    update=args.update_priority
                )

            # ----------------------------------------------------------
            # Preserve users already in the tracking file and add
            # current users.
            # ----------------------------------------------------------

            all_users = dict(
                old_gpu_users
            )

            all_users.update(
                current_gpu_users
            )

            save_gpu_users(
                args.gpu_users_file,
                all_users
            )

        return 0

    except KeyboardInterrupt:

        LOG.warning(
            "Interrupted"
        )

        return 130

    except Exception:

        LOG.exception(
            "Job report failed"
        )

        return 1

    finally:

        lock.release()


if __name__ == "__main__":

    sys.exit(
        main()
    )

