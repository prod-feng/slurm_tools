#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GPU Job Utilization Monitor.

Python 3.6 compatible.
"""

from __future__ import print_function

import argparse
import logging
import subprocess
import sys
import time

from modules.config import Config

from modules.gpu import (
    GPUCollector,
    calculate_metrics,
    classify_utilization,
)

from modules.slurm import SlurmClient

from modules.utils import (
    FileLock,
    StateStore,
    get_user_email,
)


LOG = logging.getLogger(
    "gpu_monitor"
)


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
# State
# ============================================================================

def update_job_state(
        state,
        job,
        classification,
        metrics):

    job_id = str(
        job["job_id"]
    )

    now = int(
        time.time()
    )

    record = state.get(
        job_id,
        {}
    )

    record["user"] = job["user"]
    record["job_name"] = job["job_name"]
    record["node"] = job["nodes"]
    record["gpu_indexes"] = job["gpu_indexes"]
    record["last_seen"] = now
    record["last_classification"] = classification
    record["last_average_utilization"] = (
        metrics["average_utilization"]
    )

    if classification == "NORMAL":

        record["bad_samples"] = 0

    else:

        record["bad_samples"] = (
            record.get(
                "bad_samples",
                0
            )
            + 1
        )

    record.setdefault(
        "last_email",
        0
    )

    record.setdefault(
        "emails_sent",
        0
    )

    state[job_id] = record

    return record


def notification_allowed(record):

    required = getattr(
        Config,
        "REQUIRED_BAD_SAMPLES",
        3
    )

    interval = getattr(
        Config,
        "EMAIL_INTERVAL_SECONDS",
        3600
    )

    if (
        record.get(
            "bad_samples",
            0
        )
        < required
    ):

        return False

    last_email = record.get(
        "last_email",
        0
    )

    if (
        time.time() - last_email
        < interval
    ):

        return False

    return True


# ============================================================================
# Email
# ============================================================================

def send_gpu_notification(
        job,
        metrics,
        email_address,
        dry_run=False):

    subject = (
        "GPU utilization warning "
        "for Slurm job {}".format(
            job["job_id"]
        )
    )

    body = """
GPU utilization warning

User: {user}
Job ID: {job_id}
Job name: {job_name}
Node: {node}
GPU indexes: {gpu_indexes}

Average GPU utilization: {avg:.1f}%
Minimum GPU utilization: {minimum:.1f}%
Maximum GPU utilization: {maximum:.1f}%
Idle GPUs: {idle}

Please review the GPU utilization of this job.
""".format(
        user=job["user"],
        job_id=job["job_id"],
        job_name=job["job_name"],
        node=job["nodes"],
        gpu_indexes=",".join(
            str(x)
            for x in job["gpu_indexes"]
        ),
        avg=metrics[
            "average_utilization"
        ],
        minimum=metrics[
            "minimum_utilization"
        ],
        maximum=metrics[
            "maximum_utilization"
        ],
        idle=metrics[
            "idle_gpu_count"
        ],
    )

    if dry_run:

        print("")
        print("=" * 70)
        print(
            "DRY RUN EMAIL -> {}".format(
                email_address
            )
        )
        print("=" * 70)
        print(
            body
        )
        print("=" * 70)

        return True

    command = [
        "mail",
        "-s",
        subject,
        email_address,
    ]

    try:

        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        stdout, stderr = process.communicate(
            body
        )

    except Exception as exc:

        LOG.error(
            "Unable to send email to %s: %s",
            email_address,
            exc
        )

        return False

    if process.returncode != 0:

        LOG.error(
            "mail failed for %s: %s",
            email_address,
            stderr.strip()
        )

        return False

    LOG.info(
        "GPU notification sent to %s",
        email_address
    )

    return True


# ============================================================================
# Table
# ============================================================================

def print_job_table(rows):

    if not rows:

        print(
            "No GPU jobs with available telemetry."
        )

        return

    header = (
        "{:<12} "
        "{:<10} "
        "{:<20} "
        "{:<18} "
        "{:<12} "
        "{:>8} "
        "{:>8} "
        "{:>8} "
        "{:>8} "
        "{:<16} "
        "{:>5}"
    ).format(
        "USER",
        "JOBID",
        "JOBNAME",
        "NODE",
        "GPU",
        "AVG%",
        "MIN%",
        "MAX%",
        "IDLE",
        "STATE",
        "BAD"
    )

    print(header)
    print("-" * len(header))

    for row in rows:

        print(
            "{:<12} "
            "{:<10} "
            "{:<20} "
            "{:<18} "
            "{:<12} "
            "{:>8.1f} "
            "{:>8.1f} "
            "{:>8.1f} "
            "{:>8} "
            "{:<16} "
            "{:>5}".format(
                str(
                    row["user"]
                )[:12],

                str(
                    row["job_id"]
                )[:10],

                str(
                    row["job_name"]
                )[:20],

                str(
                    row["node"]
                )[:18],

                str(
                    row["gpus"]
                )[:12],

                row["avg"],
                row["min"],
                row["max"],
                row["idle"],

                str(
                    row["state"]
                )[:16],

                row["bad"]
            )
        )


# ============================================================================
# Monitor
# ============================================================================

def monitor(
        slurm,
        gpu_collector,
        state_store,
        dry_run=False):

    start_time = time.time()

    jobs = slurm.get_running_jobs()

    LOG.info(
        "Slurm returned %d running GPU jobs",
        len(jobs)
    )

    if not jobs:

        state_store.save({})

        print(
            "No running GPU jobs."
        )

        return

    # --------------------------------------------------------------
    # IMPORTANT:
    #
    # get_running_jobs() already provides:
    #
    #   nodes
    #   gpu_indexes
    #   gpu_count
    #
    # Therefore we do not discard the GPU information by merging
    # incomplete job details.
    # --------------------------------------------------------------

    eligible_jobs = []

    minimum_runtime = getattr(
        Config,
        "MIN_RUNTIME_SECONDS",
        0
    )

    for job in jobs:

        runtime = int(
            job.get(
                "runtime",
                0
            )
        )

        if runtime < minimum_runtime:

            LOG.debug(
                "Skipping job %s: runtime=%d < minimum=%d",
                job["job_id"],
                runtime,
                minimum_runtime
            )

            continue

        if not job.get(
                "gpu_indexes"):

            LOG.warning(
                "Skipping job %s: GPU allocation could not be parsed. "
                "gres=%s gpu_count=%s",
                job["job_id"],
                job.get(
                    "tres_per_node",
                    ""
                ),
                job.get(
                    "gpu_count",
                    0
                )
            )

            continue

        eligible_jobs.append(
            job
        )

    LOG.info(
        "%d GPU jobs eligible for monitoring",
        len(eligible_jobs)
    )

    if not eligible_jobs:

        state_store.save({})

        print(
            "No eligible GPU jobs."
        )

        return

    # --------------------------------------------------------------
    # Group by node.
    # --------------------------------------------------------------

    jobs_by_node = {}

    monitorable_jobs = []

    for job in eligible_jobs:

        node = str(
            job.get(
                "nodes",
                ""
            )
        ).strip()

        if not node:

            continue

        # Current nvidia-smi collection operates on one node.
        if (
            "," in node
            or "[" in node
            or "]" in node
        ):

            LOG.warning(
                "Skipping multi-node job %s: %s",
                job["job_id"],
                node
            )

            continue

        jobs_by_node.setdefault(
            node,
            []
        ).append(job)

        monitorable_jobs.append(
            job
        )

    if not monitorable_jobs:

        state_store.save({})

        print(
            "No eligible single-node GPU jobs."
        )

        return

    # --------------------------------------------------------------
    # Collect GPU telemetry.
    # --------------------------------------------------------------

    node_gpu_data = (
        gpu_collector.collect_nodes(
            jobs_by_node.keys()
        )
    )

    state = state_store.load()

    current_job_ids = set()

    table_rows = []

    normal_jobs = 0
    bad_jobs = 0

    for job in monitorable_jobs:

        job_id = str(
            job["job_id"]
        )

        current_job_ids.add(
            job_id
        )

        LOG.debug(
            "Analyzing job %s on %s GPUs=%s",
            job_id,
            job["nodes"],
            job["gpu_indexes"]
        )

        metrics = calculate_metrics(
            job,
            node_gpu_data
        )

        if metrics is None:

            LOG.warning(
                "No telemetry available for job %s",
                job_id
            )

            continue

        classification = (
            classify_utilization(
                metrics
            )
        )

        record = update_job_state(
            state,
            job,
            classification,
            metrics
        )

        if classification == "NORMAL":

            normal_jobs += 1

        else:

            bad_jobs += 1

        table_rows.append({
            "job_id": job["job_id"],
            "user": job["user"],
            "job_name": job["job_name"],
            "node": job["nodes"],
            "gpus": ",".join(
                str(x)
                for x in job["gpu_indexes"]
            ),
            "avg": metrics[
                "average_utilization"
            ],
            "min": metrics[
                "minimum_utilization"
            ],
            "max": metrics[
                "maximum_utilization"
            ],
            "idle": metrics[
                "idle_gpu_count"
            ],
            "state": classification,
            "bad": record[
                "bad_samples"
            ],
        })

        if classification == "NORMAL":

            continue

        excluded_users = getattr(
            Config,
            "EXCLUDED_USERS",
            []
        )

        if job["user"] in excluded_users:

            LOG.debug(
                "User %s is excluded",
                job["user"]
            )

            continue

        if not notification_allowed(
                record):

            continue

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

        email_address = get_user_email(
            job["user"],
            getent_command=getent_command,
            timeout=getent_timeout
        )

        if not email_address:

            LOG.warning(
                "No email address found for %s",
                job["user"]
            )

            continue

        if send_gpu_notification(
                job,
                metrics,
                email_address,
                dry_run=dry_run):

            if not dry_run:

                record["last_email"] = int(
                    time.time()
                )

                record["emails_sent"] = (
                    record.get(
                        "emails_sent",
                        0
                    )
                    + 1
                )

    # --------------------------------------------------------------
    # Remove stale jobs.
    # --------------------------------------------------------------

    for job_id in list(
            state.keys()):

        if job_id not in current_job_ids:

            del state[job_id]

    # --------------------------------------------------------------
    # Display report.
    # --------------------------------------------------------------

    print_job_table(
        table_rows
    )

    LOG.info(
        "GPU monitor summary: "
        "jobs=%d normal=%d underutilized=%d nodes=%d",
        len(table_rows),
        normal_jobs,
        bad_jobs,
        len(node_gpu_data)
    )

    state_store.save(
        state
    )

    LOG.info(
        "Monitoring completed in %.2f seconds",
        time.time() - start_time
    )


# ============================================================================
# CLI
# ============================================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Monitor GPU utilization of Slurm jobs"
        )
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show informational messages"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show DEBUG messages"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Do not send email; display email content"
        )
    )

    parser.add_argument(
        "--state-file",
        default=getattr(
            Config,
            "STATE_FILE",
            "/tmp/gpu_monitor_state.json"
        ),
        help="Persistent state file"
    )

    parser.add_argument(
        "--lock-file",
        default=getattr(
            Config,
            "LOCK_FILE",
            "/tmp/gpu_monitor.lock"
        ),
        help="Process lock file"
    )

    return parser.parse_args()


# ============================================================================
# Main
# ============================================================================

def main():

    args = parse_args()

    configure_logging(
        args
    )

    if hasattr(
            Config,
            "ensure_directories"):

        Config.ensure_directories()

    LOG.info(
        "Starting GPU job utilization monitor"
    )

    if args.dry_run:

        LOG.warning(
            "DRY RUN MODE: no emails will be sent"
        )

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

        slurm = SlurmClient(
            Config
        )

        gpu_collector = GPUCollector(
            Config
        )

        state_store = StateStore(
            args.state_file
        )

        monitor(
            slurm=slurm,
            gpu_collector=gpu_collector,
            state_store=state_store,
            dry_run=args.dry_run
        )

        return 0

    except KeyboardInterrupt:

        LOG.warning(
            "Interrupted"
        )

        return 130

    except Exception:

        LOG.exception(
            "GPU monitor failed"
        )

        return 1

    finally:

        lock.release()


if __name__ == "__main__":

    sys.exit(
        main()
    )

