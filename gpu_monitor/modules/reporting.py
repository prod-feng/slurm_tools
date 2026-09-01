#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reporting and job aggregation for gpu_job_report.py.

Python 3.6 compatible.

This module works with the normalized records returned by
modules.slurm.SlurmClient.

A Slurm allocation and its steps are combined:

    81591
    81591.batch
    81591.extern

into one primary job:

    81591

GPU allocation comes from the primary allocation's AllocTRES.

GPU utilization, CPU time, memory and disk usage normally come
from the .batch step.
"""

from __future__ import print_function

import csv
import logging
import os
import random
import re
import subprocess

from io import StringIO

try:
    from html import escape as html_escape
except ImportError:
    import cgi

    def html_escape(value):

        return cgi.escape(
            str(value),
            quote=True
        )

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

from .config import Config


LOG = logging.getLogger(
    "gpu_monitor.reporting"
)


# ============================================================================
# Email template configuration
# ============================================================================

EMAIL_TEMPLATE_FILE = os.path.join(
    os.path.dirname(
        os.path.abspath(
            __file__
        )
    ),
    "email_templates.conf"
)


def _load_email_templates():

    """
    Load email subjects and bodies from:

        modules/email_templates.conf

    The file is intentionally loaded each time an email is generated.
    This means the administrator can modify the template without
    restarting a long-running Python process.
    """

    if not os.path.isfile(
            EMAIL_TEMPLATE_FILE):

        raise RuntimeError(
            "Email template file does not exist: {}".format(
                EMAIL_TEMPLATE_FILE
            )
        )

    parser = ConfigParser(
        interpolation=None
    )

    try:

        with open(
                EMAIL_TEMPLATE_FILE,
                "r"
        ) as config_file:

            parser.read_file(
                config_file
            )

    except Exception as exc:

        raise RuntimeError(
            "Unable to read email template file {}: {}".format(
                EMAIL_TEMPLATE_FILE,
                exc
            )
        )

    required_sections = (
        "performance_email",
        "gpu_notification",
    )

    templates = {}

    for section in required_sections:

        if not parser.has_section(
                section):

            raise RuntimeError(
                "Missing [{}] section in {}".format(
                    section,
                    EMAIL_TEMPLATE_FILE
                )
            )

        if not parser.has_option(
                section,
                "subject"
        ):

            raise RuntimeError(
                "Missing 'subject' in [{}] section of {}".format(
                    section,
                    EMAIL_TEMPLATE_FILE
                )
            )

        if not parser.has_option(
                section,
                "body"
        ):

            raise RuntimeError(
                "Missing 'body' in [{}] section of {}".format(
                    section,
                    EMAIL_TEMPLATE_FILE
                )
            )

        templates[section] = {
            "subject": parser.get(
                section,
                "subject"
            ).strip(),

            "body": parser.get(
                section,
                "body"
            ),
        }

    return templates


def _get_email_template(
        section,
        values):

    """
    Return:

        subject, body

    after replacing template variables.

    Example template:

        Hello {user}

    becomes:

        Hello feng
    """

    templates = _load_email_templates()

    if section not in templates:

        raise RuntimeError(
            "Email template [{}] not found".format(
                section
            )
        )

    template = templates[
        section
    ]

    try:

        subject = template[
            "subject"
        ].format(
            **values
        )

        body = template[
            "body"
        ].format(
            **values
        )

    except KeyError as exc:

        raise RuntimeError(
            "Missing template variable {} "
            "in [{}] email template".format(
                exc,
                section
            )
        )

    except ValueError as exc:

        raise RuntimeError(
            "Invalid format expression in [{}] "
            "email template: {}".format(
                section,
                exc
            )
        )

    return subject, body


# ============================================================================
# Basic conversion helpers
# ============================================================================

def _number(value, default=0.0):

    if value is None:

        return default

    text = str(
        value
    ).strip()

    if not text:

        return default

    match = re.match(
        r"^([-+]?[0-9]*\.?[0-9]+)",
        text
    )

    if not match:

        return default

    try:

        return float(
            match.group(1)
        )

    except ValueError:

        return default


def time_to_hours(value):

    if value is None:

        return 0.0

    text = str(
        value
    ).strip()

    if not text:

        return 0.0

    text = text.split(
        ".",
        1
    )[0]

    parts = re.split(
        r"[:-]",
        text
    )

    try:

        numbers = [
            int(x)
            for x in parts
        ]

    except ValueError:

        return 0.0

    if len(numbers) == 3:

        hours, minutes, seconds = numbers

        return (
            hours
            +
            minutes / 60.0
            +
            seconds / 3600.0
        )

    if len(numbers) == 4:

        days, hours, minutes, seconds = numbers

        return (
            days * 24.0
            +
            hours
            +
            minutes / 60.0
            +
            seconds / 3600.0
        )

    return 0.0


def size_to_gb(value):

    if value is None:

        return 0.0

    text = str(
        value
    ).strip()

    if not text:

        return 0.0

    number = _number(
        text
    )

    upper = text.upper()

    if upper.endswith("T"):

        return number * 1000.0

    if upper.endswith("G"):

        return number

    if upper.endswith("M"):

        return number / 1000.0

    if upper.endswith("K"):

        return number / 1000000.0

    return number / 1000000.0


def human_size(
        value,
        decimal_places=1):

    if value is None:

        return "0.0B"

    text = str(
        value
    ).strip()

    if not text:

        return "0.0B"

    number = _number(
        text
    )

    upper = text.upper()

    if upper.endswith("P"):

        number *= 1024.0 ** 5

    elif upper.endswith("T"):

        number *= 1024.0 ** 4

    elif upper.endswith("G"):

        number *= 1024.0 ** 3

    elif upper.endswith("M"):

        number *= 1024.0 ** 2

    elif upper.endswith("K"):

        number *= 1024.0

    units = [
        "B",
        "K",
        "M",
        "G",
        "T",
        "P",
    ]

    index = 0

    while (
        number >= 1024.0
        and index < len(units) - 1
    ):

        number /= 1024.0

        index += 1

    return (
        "{:.{}f}{}".format(
            number,
            decimal_places,
            units[index]
        )
    )


# ============================================================================
# TRES helpers
# ============================================================================

def parse_tres(value):

    result = {}

    if value is None:

        return result

    text = str(
        value
    ).strip()

    if not text:

        return result

    for item in text.split(","):

        item = item.strip()

        if not item:

            continue

        if "=" not in item:

            continue

        key, val = item.split(
            "=",
            1
        )

        key = key.strip()

        val = val.strip()

        if key:

            result[key] = val

    return result


def gpu_count_from_tres(
        alloc_tres):

    tres = parse_tres(
        alloc_tres
    )

    if "gres/gpu" in tres:

        count = int(
            _number(
                tres["gres/gpu"]
            )
        )

        if count > 0:

            return count

    total = 0

    for key, value in tres.items():

        if key.startswith(
                "gres/gpu:"
        ):

            total += int(
                _number(
                    value
                )
            )

    return total


def gpu_util_from_tres(
        tres_usage):

    tres = parse_tres(
        tres_usage
    )

    return _number(
        tres.get(
            "gres/gpuutil",
            0
        )
    )


# ============================================================================
# Record helpers
# ============================================================================

def _record_value(
        record,
        key,
        default=None):

    if not isinstance(
            record,
            dict
    ):

        return default

    return record.get(
        key,
        default
    )


def _primary_id(record):

    value = _record_value(
        record,
        "primary_job_id"
    )

    if value:

        return str(
            value
        )

    value = _record_value(
        record,
        "job_id",
        ""
    )

    value = str(
        value
    )

    if "." in value:

        return value.split(
            ".",
            1
        )[0]

    return value


def _is_step(record):

    value = _record_value(
        record,
        "is_primary"
    )

    if value is False:

        return True

    job_id = str(
        _record_value(
            record,
            "job_id",
            ""
        )
    )

    return "." in job_id


# ============================================================================
# Step aggregation
# ============================================================================

def _apply_batch_step(
        job,
        step):

    tres_usage = _record_value(
        step,
        "tres_usage",
        ""
    )

    tres = parse_tres(
        tres_usage
    )

    gpu_util = _number(
        tres.get(
            "gres/gpuutil",
            0
        )
    )

    if gpu_util >= 0:

        job["gpu_utilization"] = (
            gpu_util
        )

    job["gpumem"] = tres.get(
        "gres/gpumem",
        job.get(
            "gpumem",
            ""
        )
    )

    cpu_time = tres.get(
        "cpu",
        ""
    )

    if cpu_time:

        job["cpu_time"] = cpu_time

    memory = tres.get(
        "mem",
        ""
    )

    if memory:

        job["memory"] = memory

        job["max_rss"] = size_to_gb(
            memory
        )

    disk = tres.get(
        "fs/disk",
        ""
    )

    if disk:

        job["disk"] = disk

        job["disk_gb"] = size_to_gb(
            disk
        )

    if cpu_time:

        job["total_cpu"] = cpu_time

        job["user_cpu"] = cpu_time

    return job


def _apply_extern_step(
        job,
        step):

    return job


# ============================================================================
# Aggregate jobs
# ============================================================================

def aggregate_jobs(records):

    if not records:

        return {}

    grouped = {}

    malformed = 0

    for record in records:

        if not isinstance(
                record,
                dict
        ):

            malformed += 1

            continue

        job_id = str(
            _record_value(
                record,
                "job_id",
                ""
            )
        ).strip()

        if not job_id:

            malformed += 1

            continue

        primary_id = _primary_id(
            record
        )

        if not primary_id:

            malformed += 1

            continue

        if primary_id not in grouped:

            grouped[primary_id] = {
                "primary": None,
                "steps": [],
            }

        if _is_step(record):

            grouped[
                primary_id
            ][
                "steps"
            ].append(
                record
            )

        else:

            grouped[
                primary_id
            ][
                "primary"
            ] = record

    if malformed:

        LOG.warning(
            "Skipped %d invalid normalized job records",
            malformed
        )

    jobs = {}

    for primary_id in sorted(
            grouped.keys(),
            key=str):

        group = grouped[
            primary_id
        ]

        primary = group[
            "primary"
        ]

        if primary is None:

            LOG.debug(
                "Ignoring step-only job %s",
                primary_id
            )

            continue

        job = {
            "user": _record_value(
                primary,
                "user",
                ""
            ),

            "job_id": primary_id,

            "job_name": _record_value(
                primary,
                "job_name",
                ""
            ),

            "partition": _record_value(
                primary,
                "partition",
                ""
            ),

            "state": _record_value(
                primary,
                "state",
                ""
            ),

            "start": _record_value(
                primary,
                "start",
                ""
            ),

            "elapsed": _record_value(
                primary,
                "elapsed",
                "00:00:00"
            ),

            "max_rss": _number(
                _record_value(
                    primary,
                    "max_rss",
                    0
                )
            ),

            "memory": _record_value(
                primary,
                "memory",
                ""
            ),

            "disk": _record_value(
                primary,
                "disk",
                ""
            ),

            "disk_gb": _number(
                _record_value(
                    primary,
                    "disk_gb",
                    0
                )
            ),

            "gpumem": _record_value(
                primary,
                "gpumem",
                ""
            ),

            "alloc_tres": _record_value(
                primary,
                "alloc_tres",
                ""
            ),

            "tres_usage": _record_value(
                primary,
                "tres_usage",
                ""
            ),

            "gpu_count": 0,

            "gpu_utilization": 0.0,

            "cpu_time": _record_value(
                primary,
                "cpu_time",
                "00:00:00"
            ),

            "system_cpu": _record_value(
                primary,
                "system_cpu",
                "00:00:00"
            ),

            "total_cpu": _record_value(
                primary,
                "total_cpu",
                "00:00:00"
            ),

            "user_cpu": _record_value(
                primary,
                "user_cpu",
                "00:00:00"
            ),

            "nnodes": _record_value(
                primary,
                "nnodes",
                1
            ),

            "ncpus": _record_value(
                primary,
                "ncpus",
                0
            ),

            "nodelist": _record_value(
                primary,
                "nodelist",
                ""
            ),

            "req_mem": _record_value(
                primary,
                "req_mem",
                ""
            ),
        }

        job["gpu_count"] = (
            gpu_count_from_tres(
                job["alloc_tres"]
            )
        )

        if job["gpu_count"] <= 0:

            job["gpu_count"] = int(
                _number(
                    _record_value(
                        primary,
                        "gpu_count",
                        0
                    )
                )
            )

        primary_gpu_util = _number(
            _record_value(
                primary,
                "gpu_utilization",
                0
            )
        )

        if primary_gpu_util > 0:

            job[
                "gpu_utilization"
            ] = primary_gpu_util

        batch_steps = []

        other_steps = []

        for step in group[
                "steps"]:

            job_step_id = str(
                _record_value(
                    step,
                    "job_id",
                    ""
                )
            )

            if job_step_id.endswith(
                    ".batch"
            ):

                batch_steps.append(
                    step
                )

            else:

                other_steps.append(
                    step
                )

        for step in batch_steps:

            _apply_batch_step(
                job,
                step
            )

        if not batch_steps:

            for step in other_steps:

                step_gpu = _number(
                    _record_value(
                        step,
                        "gpu_utilization",
                        0
                    )
                )

                step_cpu = _record_value(
                    step,
                    "cpu_time",
                    ""
                )

                step_memory = _record_value(
                    step,
                    "memory",
                    ""
                )

                if (
                    step_gpu > 0
                    or step_cpu
                    or step_memory
                ):

                    _apply_batch_step(
                        job,
                        step
                    )

        try:

            job["nnodes"] = int(
                _number(
                    job["nnodes"],
                    1
                )
            )

        except Exception:

            job["nnodes"] = 1

        try:

            job["ncpus"] = int(
                _number(
                    job["ncpus"],
                    0
                )
            )

        except Exception:

            job["ncpus"] = 0

        job["elapsed_hours"] = (
            time_to_hours(
                job["elapsed"]
            )
        )

        job["cpu_hours"] = (
            time_to_hours(
                job["cpu_time"]
            )
        )

        job["system_cpu_hours"] = (
            time_to_hours(
                job["system_cpu"]
            )
        )

        job["total_cpu_hours"] = (
            time_to_hours(
                job["total_cpu"]
            )
        )

        job["user_cpu_hours"] = (
            time_to_hours(
                job["user_cpu"]
            )
        )

        if (
            job["user_cpu_hours"] <= 0
            and job["total_cpu_hours"] > 0
        ):

            job["user_cpu_hours"] = (
                job["total_cpu_hours"]
            )

            job["user_cpu"] = (
                job["total_cpu"]
            )

        if (
            job["cpu_hours"] <= 0
            and job["total_cpu_hours"] > 0
        ):

            job["cpu_hours"] = (
                job["total_cpu_hours"]
            )

            job["cpu_time"] = (
                job["total_cpu"]
            )

        if job["ncpus"] <= 0:

            alloc_tres = parse_tres(
                job["alloc_tres"]
            )

            job["ncpus"] = int(
                _number(
                    alloc_tres.get(
                        "cpu",
                        0
                    )
                )
            )

        if job["max_rss"] <= 0:

            if job["memory"]:

                job["max_rss"] = (
                    size_to_gb(
                        job["memory"]
                    )
                )

        if job["disk_gb"] <= 0:

            if job["disk"]:

                job["disk_gb"] = (
                    size_to_gb(
                        job["disk"]
                    )
                )

        jobs[primary_id] = job

    LOG.info(
        "Aggregated %d primary jobs from %d records",
        len(jobs),
        len(records)
    )

    return jobs


# ============================================================================
# GPU job filtering
# ============================================================================

def _is_gpu_job(job):

    try:

        gpu_count = int(
            _number(
                job.get(
                    "gpu_count",
                    0
                )
            )
        )

    except Exception:

        gpu_count = 0

    if gpu_count <= 0:

        gpu_count = (
            gpu_count_from_tres(
                job.get(
                    "alloc_tres",
                    ""
                )
            )
        )

    return gpu_count > 0


# ============================================================================
# Job selection
# ============================================================================

def select_jobs_for_user(
        jobs,
        max_jobs=100,
        randomize=True):

    if not jobs:

        return []

    gpu_jobs = [
        job
        for job in jobs
        if _is_gpu_job(job)
    ]

    eligible = []

    for job in gpu_jobs:

        elapsed = time_to_hours(
            job.get(
                "elapsed",
                "00:00:00"
            )
        )

        if elapsed < 0.5:

            continue

        job["elapsed_hours"] = elapsed

        eligible.append(
            job
        )

    if not eligible:

        return []

    if max_jobs is None or max_jobs <= 0:

        max_jobs = 10

    if (
        randomize
        and len(eligible) > max_jobs
    ):

        return random.sample(
            eligible,
            max_jobs
        )

    return eligible


# ============================================================================
# User summaries
# ============================================================================

def build_user_summaries(
        jobs,
        max_jobs=100,
        randomize=True):

    if not jobs:

        return {}

    by_user = {}

    if isinstance(
            jobs,
            dict
    ):

        iterable = jobs.values()

    else:

        iterable = jobs

    for job in iterable:

        if not isinstance(
                job,
                dict
        ):

            continue

        user = str(
            job.get(
                "user",
                ""
            )
        ).strip()

        if not user:

            continue

        if user not in by_user:

            by_user[user] = []

        by_user[user].append(
            job
        )

    summaries = {}

    for user in sorted(
            by_user.keys()):

        selected = select_jobs_for_user(
            by_user[user],
            max_jobs=max_jobs,
            randomize=randomize
        )

        if not selected:

            continue

        total_elapsed_gpu_hours = 0.0

        total_gpu_util_hours = 0.0

        for job in selected:

            elapsed = time_to_hours(
                job.get(
                    "elapsed",
                    "00:00:00"
                )
            )

            gpu_count = int(
                _number(
                    job.get(
                        "gpu_count",
                        0
                    )
                )
            )

            gpu_util = _number(
                job.get(
                    "gpu_utilization",
                    0
                )
            )

            total_elapsed_gpu_hours += (
                elapsed * gpu_count
            )

            total_gpu_util_hours += (
                elapsed
                * gpu_count
                * gpu_util
            )

        summaries[user] = {
            "user": user,

            "jobs": selected,

            "total_jobs": len(
                by_user[user]
            ),

            "reported_jobs": len(
                selected
            ),

            "gpu_hours": (
                total_elapsed_gpu_hours
            ),

            "gpu_util_hours": (
                total_gpu_util_hours
            ),
        }

    LOG.info(
        "Built summaries for %d users",
        len(summaries)
    )

    return summaries


# ============================================================================
# GPU efficiency
# ============================================================================

def calculate_user_gpu_efficiency(
        summary):

    if not summary:

        return 0.0

    gpu_hours = _number(
        summary.get(
            "gpu_hours",
            0
        )
    )

    util_hours = _number(
        summary.get(
            "gpu_util_hours",
            0
        )
    )

    if gpu_hours <= 0:

        return 0.0

    efficiency = (
        util_hours
        /
        gpu_hours
    )

    if efficiency < 0:

        efficiency = 0.0

    if efficiency > 100:

        efficiency = 100.0

    return efficiency


def calculate_priority(
        efficiency):

    efficiency = _number(
        efficiency
    )

    if efficiency < 0:

        efficiency = 0

    if efficiency > 100:

        efficiency = 100

    priority = (
        int(
            efficiency * 0.2
        )
        + 80
    )

    if priority < 80:

        priority = 80

    if priority > 100:

        priority = 100

    return int(
        priority
    )


# ============================================================================
# Text formatting
# ============================================================================

def _job_line(job):

    user = str(
        job.get(
            "user",
            ""
        )
    )

    job_id = str(
        job.get(
            "job_id",
            ""
        )
    )

    job_name = str(
        job.get(
            "job_name",
            ""
        )
    )

    start = str(
        job.get(
            "start",
            ""
        )
    )

    if "T" in start:

        start = start.split(
            "T",
            1
        )[0]

    elapsed = job.get(
        "elapsed_hours",
        time_to_hours(
            job.get(
                "elapsed",
                "00:00:00"
            )
        )
    )

    mem_used = _number(
        job.get(
            "max_rss",
            0
        )
    )

    req_mem = job.get(
        "req_mem",
        ""
    )

    nodes = int(
        _number(
            job.get(
                "nnodes",
                1
            ),
            1
        )
    )

    cpus = int(
        _number(
            job.get(
                "ncpus",
                0
            )
        )
    )

    cpu_hours = time_to_hours(
        job.get(
            "cpu_time",
            "00:00:00"
        )
    )

    if cpu_hours <= 0:

        cpu_hours = time_to_hours(
            job.get(
                "total_cpu",
                "00:00:00"
            )
        )

    user_cpu_hours = time_to_hours(
        job.get(
            "user_cpu",
            "00:00:00"
        )
    )

    if user_cpu_hours <= 0:

        user_cpu_hours = cpu_hours

    cpu_hours_report = round(
        cpu_hours + 1.0,
        1
    )

    if cpu_hours_report <= 0:

        cpu_hours_report = 1.0

    cpu_usage = (
        user_cpu_hours
        /
        cpu_hours_report
    )

    system_cpu_hours = time_to_hours(
        job.get(
            "system_cpu",
            "00:00:00"
        )
    )

    disk_gb = _number(
        job.get(
            "disk_gb",
            0
        )
    )

    gpu_util = _number(
        job.get(
            "gpu_utilization",
            0
        )
    )

    gpu_count = int(
        _number(
            job.get(
                "gpu_count",
                0
            )
        )
    )

    return (
        "{:<12} "
        "{:<10} "
        "{:<20} "
        "{:<12} "
        "{:>9.2f}h "
        "{:>10.2f}G "
        "{:<10} "
        "{:>6} "
        "{:>7} "
        "{:>11.1f}h "
        "{:>9.3f} "
        "{:>9.2f}h "
        "{:>9.2f}h "
        "{:>10.2f}G "
        "{:<18} "
        "{:<12} "
        "{:<16} "
        "{:>8.1f}% "
        "{:>3}"
    ).format(
        user[:12],
        job_id[:10],
        job_name[:20],
        start[:12],
        elapsed,
        mem_used,
        str(req_mem)[:10],
        nodes,
        cpus,
        cpu_hours_report,
        cpu_usage,
        system_cpu_hours,
        user_cpu_hours,
        disk_gb,
        str(
            job.get(
                "partition",
                ""
            )
        )[:18],
        str(
            job.get(
                "state",
                ""
            )
        )[:12],
        str(
            job.get(
                "nodelist",
                ""
            )
        )[:16],
        gpu_util,
        gpu_count
    )


def format_user_text_report(
        summary):

    if not summary:

        return ""

    lines = []

    header = (
        "{:<12} "
        "{:<10} "
        "{:<20} "
        "{:<12} "
        "{:>10} "
        "{:>11} "
        "{:<10} "
        "{:>6} "
        "{:>7} "
        "{:>12} "
        "{:>10} "
        "{:>10} "
        "{:>10} "
        "{:>11} "
        "{:<18} "
        "{:<12} "
        "{:<16} "
        "{:>9} "
        "{:>3}"
    ).format(
        "USER",
        "JobID",
        "Jobname",
        "Start",
        "Elapsed",
        "MemUsed",
        "MemAsked",
        "Nodes",
        "CPUs",
        "CPUhours",
        "CPUUsage",
        "CPUSYST",
        "CPUUSER",
        "Disk",
        "Partition",
        "State",
        "NodeList",
        "GPUUTIL",
        "GPU"
    )

    lines.append(
        header
    )

    lines.append(
        "-" * len(header)
    )

    for job in summary.get(
            "jobs",
            []):

        lines.append(
            _job_line(
                job
            )
        )

    efficiency = (
        calculate_user_gpu_efficiency(
            summary
        )
    )

    priority = calculate_priority(
        efficiency
    )

    lines.append("")

    lines.append(
        "Reported jobs: {}".format(
            summary.get(
                "reported_jobs",
                0
            )
        )
    )

    lines.append(
        "Total jobs: {}".format(
            summary.get(
                "total_jobs",
                0
            )
        )
    )

    lines.append(
        "GPU efficiency: {:.2f}%".format(
            efficiency
        )
    )

    lines.append(
        "Calculated priority: {}".format(
            priority
        )
    )

    return "\n".join(
        lines
    )


# ============================================================================
# CSV formatting
# ============================================================================

def format_user_csv_report(
        summary):

    if not summary:

        return ""

    output = StringIO()

    writer = csv.writer(
        output
    )

    writer.writerow([
        "USER",
        "JobID",
        "Jobname",
        "Start",
        "ElapsedHours",
        "MemUsedGB",
        "MemAsked",
        "Nodes",
        "CPUs",
        "CPUhours",
        "CPUUsage",
        "CPUSYST",
        "CPUUSER",
        "DiskGB",
        "Partition",
        "State",
        "NodeList",
        "GPUUTIL",
        "GPUCount",
    ])

    user = summary.get(
        "user",
        ""
    )

    for job in summary.get(
            "jobs",
            []):

        elapsed = time_to_hours(
            job.get(
                "elapsed",
                "00:00:00"
            )
        )

        cpu_hours = time_to_hours(
            job.get(
                "cpu_time",
                "00:00:00"
            )
        )

        if cpu_hours <= 0:

            cpu_hours = time_to_hours(
                job.get(
                    "total_cpu",
                    "00:00:00"
                )
            )

        report_cpu_hours = (
            cpu_hours + 1.0
        )

        if report_cpu_hours <= 0:

            report_cpu_hours = 1.0

        user_cpu = time_to_hours(
            job.get(
                "user_cpu",
                "00:00:00"
            )
        )

        if user_cpu <= 0:

            user_cpu = cpu_hours

        cpu_usage = (
            user_cpu
            /
            report_cpu_hours
        )

        start = str(
            job.get(
                "start",
                ""
            )
        )

        if "T" in start:

            start = start.split(
                "T",
                1
            )[0]

        writer.writerow([
            user,

            job.get(
                "job_id",
                ""
            ),

            job.get(
                "job_name",
                ""
            ),

            start,

            round(
                elapsed,
                6
            ),

            round(
                _number(
                    job.get(
                        "max_rss",
                        0
                    )
                ),
                2
            ),

            job.get(
                "req_mem",
                ""
            ),

            job.get(
                "nnodes",
                1
            ),

            job.get(
                "ncpus",
                0
            ),

            round(
                report_cpu_hours,
                6
            ),

            round(
                cpu_usage,
                6
            ),

            round(
                time_to_hours(
                    job.get(
                        "system_cpu",
                        "00:00:00"
                    )
                ),
                6
            ),

            round(
                user_cpu,
                6
            ),

            round(
                _number(
                    job.get(
                        "disk_gb",
                        0
                    )
                ),
                6
            ),

            job.get(
                "partition",
                ""
            ),

            job.get(
                "state",
                ""
            ),

            str(
                job.get(
                    "nodelist",
                    ""
                )
            ).replace(
                ",",
                "|"
            ),

            round(
                _number(
                    job.get(
                        "gpu_utilization",
                        0
                    )
                ),
                2
            ),

            job.get(
                "gpu_count",
                0
            ),
        ])

    return output.getvalue().rstrip()


# ============================================================================
# Performance email
# ============================================================================

def send_performance_email(
        summary,
        email_address,
        sender=None,
        fallback_bcc=None):

    if not summary:

        return False

    if not email_address:

        return False

    if sender is None:

        sender = getattr(
            Config,
            "MAIL_FROM",
            ""
        )

    if fallback_bcc is None:

        fallback_bcc = getattr(
            Config,
            "MAIL_BCC",
            ""
        )

    user = summary.get(
        "user",
        ""
    )

    reported_jobs = summary.get(
        "reported_jobs",
        0
    )

    total_jobs = summary.get(
        "total_jobs",
        0
    )

    efficiency = (
        calculate_user_gpu_efficiency(
            summary
        )
    )

    report = format_user_text_report(
        summary
    )

    if efficiency < 60:

        color = "red"

    else:

        color = "black"

    values = {
        "user": html_escape(
            user
        ),

        "reported_jobs": reported_jobs,

        "total_jobs": total_jobs,

        "report": html_escape(
            report
        ),

        "color": color,

        "efficiency": efficiency,

        "support_url": html_escape(
            getattr(
                Config,
                "SUPPORT_URL",
                ""
            )
        ),
    }

    subject, body = _get_email_template(
        "performance_email",
        values
    )

    command = [
        getattr(
            Config,
            "MAIL",
            "mail"
        ),

        "-s",
        subject,

        "-r",
        sender,
    ]

    if fallback_bcc:

        command.extend([
            "-b",
            fallback_bcc
        ])

    command.extend([
        "-S",
        "Content-Type=text/html; charset=UTF-8",

        "-S",
        "Content-Transfer-Encoding=quoted-printable",

        email_address,
    ])

    LOG.info(
        "Sending performance email to %s",
        email_address
    )

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

    if process.returncode != 0:

        raise RuntimeError(
            "mail command failed: {}".format(
                stderr.strip()
            )
        )

    return True


# ============================================================================
# GPU-user CSV tracking
# ============================================================================

def load_gpu_users(filename):

    users = {}

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

                user = str(
                    row.get(
                        "user",
                        ""
                    )
                ).strip()

                if not user:

                    continue

                users[user] = row.get(
                    "last",
                    ""
                )

    except IOError:

        LOG.info(
            "GPU user tracking file does not exist: %s",
            filename
        )

    return users


def save_gpu_users(
        filename,
        users):

    import os
    import tempfile

    directory = os.path.dirname(
        filename
    )

    if (
        directory
        and not os.path.isdir(
            directory
        )
    ):

        os.makedirs(
            directory
        )

    temporary = (
        filename
        +
        ".tmp"
    )

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

        for user in sorted(
                users.keys()):

            writer.writerow({
                "user": user,
                "last": users[user],
            })

    os.rename(
        temporary,
        filename
    )


# ============================================================================
# GPU monitor table
# ============================================================================

def print_job_table(rows):

    if not rows:

        print(
            "No GPU jobs to display."
        )

        return

    header = (
        "{:<12} "
        "{:<10} "
        "{:<22} "
        "{:<18} "
        "{:<18} "
        "{:>8} "
        "{:>8} "
        "{:>8} "
        "{:>8} "
        "{:<16} "
        "{:>4}"
    ).format(
        "USER",
        "JOBID",
        "JOBNAME",
        "NODE",
        "GPUS",
        "AVG%",
        "MIN%",
        "MAX%",
        "IDLE",
        "STATE",
        "BAD"
    )

    print(header)

    print(
        "-" * len(header)
    )

    for row in rows:

        print(
            "{:<12} "
            "{:<10} "
            "{:<22} "
            "{:<18} "
            "{:<18} "
            "{:>8.1f} "
            "{:>8.1f} "
            "{:>8.1f} "
            "{:>8} "
            "{:<16} "
            "{:>4}".format(

                str(
                    row.get(
                        "user",
                        ""
                    )
                )[:12],

                str(
                    row.get(
                        "job_id",
                        ""
                    )
                )[:10],

                str(
                    row.get(
                        "job_name",
                        ""
                    )
                )[:22],

                str(
                    row.get(
                        "node",
                        ""
                    )
                )[:18],

                str(
                    row.get(
                        "gpus",
                        ""
                    )
                )[:18],

                float(
                    row.get(
                        "avg",
                        0
                    )
                ),

                float(
                    row.get(
                        "min",
                        0
                    )
                ),

                float(
                    row.get(
                        "max",
                        0
                    )
                ),

                int(
                    row.get(
                        "idle",
                        0
                    )
                ),

                str(
                    row.get(
                        "state",
                        ""
                    )
                )[:16],

                int(
                    row.get(
                        "bad",
                        0
                    )
                )
            )
        )


# ============================================================================
# GPU monitor notification
# ============================================================================

def send_gpu_notification(
        job,
        metrics,
        email_address,
        sender=None,
        fallback_bcc=None,
        dry_run=False):

    if not job:

        return False

    if not email_address:

        return False

    if sender is None:

        sender = getattr(
            Config,
            "MAIL_FROM",
            ""
        )

    if fallback_bcc is None:

        fallback_bcc = getattr(
            Config,
            "MAIL_BCC",
            ""
        )

    user = str(
        job.get(
            "user",
            ""
        )
    )

    job_id = str(
        job.get(
            "job_id",
            ""
        )
    )

    job_name = str(
        job.get(
            "job_name",
            ""
        )
    )

    node = str(
        job.get(
            "nodes",
            ""
        )
    )

    gpu_indexes = job.get(
        "gpu_indexes",
        []
    )

    gpu_text = ", ".join(
        str(index)
        for index in gpu_indexes
    )

    average = float(
        metrics.get(
            "average_utilization",
            0
        )
    )

    minimum = float(
        metrics.get(
            "minimum_utilization",
            0
        )
    )

    maximum = float(
        metrics.get(
            "maximum_utilization",
            0
        )
    )

    idle_count = int(
        metrics.get(
            "idle_gpu_count",
            0
        )
    )

    gpu_count = int(
        metrics.get(
            "gpu_count",
            len(gpu_indexes)
        )
    )

    values = {
        "user": user,

        "job_id": job_id,

        "job_name": job_name,

        "node": node,

        "gpu_text": gpu_text,

        "gpu_count": gpu_count,

        "average": average,

        "minimum": minimum,

        "maximum": maximum,

        "idle_count": idle_count,
    }

    subject, body = _get_email_template(
        "gpu_notification",
        values
    )

    if dry_run:

        print("")
        print("=" * 72)

        print(
            "DRY RUN - GPU notification"
        )

        print("=" * 72)

        print(
            "To: {}".format(
                email_address
            )
        )

        print(
            "Subject: {}".format(
                subject
            )
        )

        print("")

        print(body)

        print("=" * 72)

        print("")

        return True

    command = [
        getattr(
            Config,
            "MAIL",
            "mail"
        ),

        "-s",
        subject,

        "-r",
        sender,
    ]

    if fallback_bcc:

        command.extend([
            "-b",
            fallback_bcc
        ])

    command.append(
        email_address
    )

    LOG.info(
        "Sending GPU notification to %s for job %s",
        email_address,
        job_id
    )

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

    if process.returncode != 0:

        LOG.error(
            "GPU notification mail failed for %s: %s",
            email_address,
            stderr.strip()
        )

        return False

    return True

