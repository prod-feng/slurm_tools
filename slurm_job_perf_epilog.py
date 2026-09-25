#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import csv
import io
import json
import os
import re
import subprocess
import sys
import time


# ================================================================
# sacct fields
# ================================================================

SACCT_FIELDS = [
    "JobID",
    "JobIDRaw",
    "JobName",
    "User",
    "Partition",
    "State",
    "Elapsed",
    "Timelimit",
    "Start",
    "End",
    "AllocTRES",
    "TRESUsageInAve",
    "TRESUsageInTot",
    "TRESUsageInMax",
]


# ================================================================
# Data structures
# ================================================================

class SacctRecord(object):

    def __init__(self, row):
        self.job_id = row.get("JobID", "")
        self.job_id_raw = row.get("JobIDRaw", "")
        self.job_name = row.get("JobName", "")
        self.user = row.get("User", "")
        self.partition = row.get("Partition", "")
        self.state = row.get("State", "")
        self.elapsed = row.get("Elapsed", "")
        self.timelimit = row.get("Timelimit", "")
        self.start = row.get("Start", "")
        self.end = row.get("End", "")
        self.alloc_tres = row.get("AllocTRES", "")
        self.tres_usage_in_ave = row.get(
            "TRESUsageInAve", ""
        )
        self.tres_usage_in_tot = row.get(
            "TRESUsageInTot", ""
        )
        self.tres_usage_in_max = row.get(
            "TRESUsageInMax", ""
        )


class PartitionInfo(object):

    def __init__(self):
        self.name = ""
        self.default_time = ""
        self.max_time = ""

        self.default_seconds = None
        self.max_seconds = None


class JobResult(object):

    def __init__(self):

        self.job_id = ""
        self.job_id_raw = ""
        self.user = ""
        self.partition = ""
        self.state = ""
        self.elapsed = ""

        self.requested_time = ""
        self.requested_seconds = None

        self.cpus = None

        self.memory_bytes = None
        self.memory_used_bytes = None

        self.gpu_count = None
        self.gpu_types = ""
        self.gpu_efficiency_percent = None

        self.cpu_used_seconds = None
        self.cpu_efficiency_percent = None

        self.memory_efficiency_percent = None

        self.elapsed_seconds = None

        self.partition_default_time = ""
        self.partition_max_time = ""

        self.elapsed_requested_percent = None
        self.elapsed_default_percent = None
        self.elapsed_max_percent = None

        self.step_count = 0
        self.application_step_count = 0

        self.job_step = ""

        self.alloc_tres = ""
        self.tres_usage_in_ave = ""


# ================================================================
# Utilities
# ================================================================

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def parse_elapsed(value):
    """
    Convert Slurm duration into seconds.

    Examples:

        00:07:48
        01:02:03
        1-02:03:04
        30
    """

    if not value:
        return None

    value = value.strip()

    if value.upper() in (
        "UNLIMITED",
        "INFINITE",
        "NONE",
        "N/A",
        "PARTITION",
    ):
        return None

    try:
        days = 0

        if "-" in value:
            day, value = value.split("-", 1)
            days = int(day)

        parts = value.split(":")

        if len(parts) == 3:

            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])

        elif len(parts) == 2:

            hours = 0
            minutes = int(parts[0])
            seconds = float(parts[1])

        elif len(parts) == 1:

            hours = 0
            minutes = 0
            seconds = float(parts[0])

        else:
            return None

        return (
            days * 86400
            + hours * 3600
            + minutes * 60
            + seconds
        )

    except (ValueError, TypeError):
        return None


def parse_time_limit(value):
    """
    Parse Slurm Timelimit.

    Timelimit can be:

        30
        01:00:00
        1-00:00:00
        UNLIMITED
        Partition
    """

    if not value:
        return None

    value = value.strip()

    if value.upper() in (
        "UNLIMITED",
        "INFINITE",
        "NONE",
        "N/A",
        "PARTITION",
    ):
        return None

    # Slurm time limits may be represented as
    # an integer number of minutes.
    if re.match(r"^[0-9]+$", value):

        try:
            return int(value) * 60
        except ValueError:
            return None

    return parse_elapsed(value)


def format_duration(seconds):

    if seconds is None:
        return "-"

    try:
        seconds = int(round(float(seconds)))
    except (ValueError, TypeError):
        return "-"

    days = seconds // 86400
    seconds %= 86400

    hours = seconds // 3600
    seconds %= 3600

    minutes = seconds // 60
    seconds %= 60

    if days:

        return "%d-%02d:%02d:%02d" % (
            days,
            hours,
            minutes,
            seconds,
        )

    return "%02d:%02d:%02d" % (
        hours,
        minutes,
        seconds,
    )


def parse_memory(value):
    """
    Convert Slurm memory value to bytes.

    Examples:

        32G
        412564K
        1.5G
        1024M
    """

    if not value:
        return None

    value = value.strip()

    match = re.match(
        r"^\s*([0-9]+(?:\.[0-9]+)?)"
        r"\s*([KMGTPE]?)"
        r"(?:i?B)?\s*$",
        value,
        re.IGNORECASE,
    )

    if not match:
        return None

    number = float(match.group(1))
    prefix = match.group(2).upper()

    multiplier = {
        "": 1,
        "K": 1024 ** 1,
        "M": 1024 ** 2,
        "G": 1024 ** 3,
        "T": 1024 ** 4,
        "P": 1024 ** 5,
        "E": 1024 ** 6,
    }.get(prefix)

    if multiplier is None:
        return None

    return number * multiplier


def format_bytes(value):

    if value is None:
        return "-"

    value = float(value)

    units = [
        "B",
        "KiB",
        "MiB",
        "GiB",
        "TiB",
        "PiB",
    ]

    for unit in units:

        if abs(value) < 1024:

            return "%.2f%s" % (
                value,
                unit,
            )

        value /= 1024.0

    return "%.2fPiB" % value


def parse_tres(value):

    result = {}

    if not value:
        return result

    for item in value.split(","):

        if "=" not in item:
            continue

        key, val = item.split("=", 1)

        result[key.strip()] = val.strip()

    return result


def parse_gpu_info(tres):

    gpu_count = None
    gpu_types = []

    for key, value in tres.items():

        if key.startswith("gres/gpu:"):

            try:

                count = float(value)

                if gpu_count is None:
                    gpu_count = 0

                gpu_count += count

            except ValueError:
                pass

            gpu_type = key.split(":", 1)[1]

            gpu_types.append(
                "%s=%s" % (
                    gpu_type,
                    value,
                )
            )

        elif key == "gres/gpu":

            try:
                gpu_count = float(value)
            except ValueError:
                pass

    return gpu_count, ",".join(gpu_types)


def format_number(value):

    if value is None:
        return "-"

    if float(value).is_integer():
        return str(int(value))

    return "%.2f" % float(value)


def format_percent(value):

    if value is None:
        return "-"

    return "%.2f%%" % float(value)


# ================================================================
# sacct
# ================================================================

def run_sacct(
    job_id=None,
    start=None,
    end=None,
    user=None,
    all_users=False,
    allocations_only=False,
):

    cmd = [
        "sacct",
        "-Pn",
        "--delimiter=|",
        "--format=" + ",".join(SACCT_FIELDS),
    ]

    if job_id:

        cmd.extend([
            "-j",
            job_id,
        ])

    if start:

        cmd.extend([
            "-S",
            start,
        ])

    if end:

        cmd.extend([
            "-E",
            end,
        ])

    if all_users:
        cmd.append("-a")

    if user:

        for u in user:

            cmd.extend([
                "-u",
                u,
            ])

    if allocations_only:
        cmd.append("-X")

    try:

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )

        stdout, stderr = proc.communicate()

    except OSError as exc:

        raise RuntimeError(
            "Cannot execute sacct: %s"
            % exc
        )

    if proc.returncode != 0:

        raise RuntimeError(
            "sacct failed (%d): %s"
            % (
                proc.returncode,
                stderr.strip(),
            )
        )

    records = []

    for line in stdout.splitlines():

        if not line.strip():
            continue

        fields = line.split("|")

        if len(fields) < len(SACCT_FIELDS):

            fields.extend(
                [""] * (
                    len(SACCT_FIELDS)
                    - len(fields)
                )
            )

        row = dict(
            zip(
                SACCT_FIELDS,
                fields,
            )
        )

        records.append(
            SacctRecord(row)
        )

    return records


# ================================================================
# Job / step identification
# ================================================================

def is_step(record):

    return "." in record.job_id


def is_extern(record):

    return record.job_id.endswith(".extern")


def is_batch(record):

    return record.job_id.endswith(".batch")


def find_job_record(records, job_id):

    for record in records:

        if record.job_id == job_id:
            return record

    for record in records:

        if record.job_id_raw == job_id:
            return record

    return None


def find_steps(records, job_id):

    prefix = job_id + "."

    return [
        record
        for record in records
        if record.job_id.startswith(prefix)
    ]


# ================================================================
# Usage selection
# ================================================================

def usage_has_cpu_or_memory(record):

    usage = parse_tres(
        record.tres_usage_in_ave
    )

    return bool(
        usage.get("cpu")
        or usage.get("mem")
    )


def select_usage_record(
    job,
    steps,
):
    """
    Select the usage step.

    Priority:

        1. .batch if it has CPU/memory usage
        2. single application step

    .extern is never selected.

    If there are multiple application steps,
    aggregation is performed instead.
    """

    for step in steps:

        if (
            is_batch(step)
            and usage_has_cpu_or_memory(step)
        ):

            return step

    application_steps = [
        step
        for step in steps
        if not is_extern(step)
        and not is_batch(step)
    ]

    if len(application_steps) == 1:

        return application_steps[0]

    return None


def parse_gpu_percent(value):

    if not value:
        return None

    value = value.strip()

    match = re.match(
        r"^\s*([0-9]+(?:\.[0-9]+)?)"
        r"\s*%?\s*$",
        value,
    )

    if not match:
        return None

    try:

        result = float(
            match.group(1)
        )

    except ValueError:

        return None

    if result < 0 or result > 100:
        return None

    return result


def aggregate_step_usage(
    application_steps
):
    """
    Aggregate usage from multiple application steps.

    CPU:
        sum of CPU time.

    Memory:
        maximum reported memory usage.

    GPU:
        average of explicitly reported GPU
        utilization values, if available.
    """

    cpu_seconds = 0.0
    have_cpu = False

    max_memory = None

    gpu_util_sum = 0.0
    gpu_util_count = 0

    for step in application_steps:

        usage = parse_tres(
            step.tres_usage_in_ave
        )

        # ----------------------------------------------------------
        # CPU
        # ----------------------------------------------------------

        if "cpu" in usage:

            cpu = parse_elapsed(
                usage["cpu"]
            )

            if cpu is not None:

                cpu_seconds += cpu
                have_cpu = True

        # ----------------------------------------------------------
        # Memory
        # ----------------------------------------------------------

        if "mem" in usage:

            mem = parse_memory(
                usage["mem"]
            )

            if mem is not None:

                if (
                    max_memory is None
                    or mem > max_memory
                ):

                    max_memory = mem

        # ----------------------------------------------------------
        # GPU utilization
        # ----------------------------------------------------------

        for key in (
            "gres/gpuutil",
            "gpuutil",
        ):

            if key in usage:

                gpu_value = parse_gpu_percent(
                    usage[key]
                )

                if gpu_value is not None:

                    gpu_util_sum += gpu_value
                    gpu_util_count += 1

    gpu_efficiency = None

    if gpu_util_count:

        gpu_efficiency = (
            gpu_util_sum
            / float(gpu_util_count)
        )

    return (
        cpu_seconds if have_cpu else None,
        max_memory,
        gpu_efficiency,
    )


# ================================================================
# Partition information
# ================================================================

PARTITION_CACHE = {}


def get_partition_info(partition):

    if not partition:
        return None

    if partition in PARTITION_CACHE:

        return PARTITION_CACHE[
            partition
        ]

    info = PartitionInfo()

    info.name = partition

    cmd = [
        "scontrol",
        "show",
        "partition",
        partition,
    ]

    try:

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )

        stdout, stderr = proc.communicate()

    except OSError as exc:

        eprint(
            "[WARN] Cannot execute scontrol: %s"
            % exc
        )

        PARTITION_CACHE[
            partition
        ] = info

        return info

    if proc.returncode != 0:

        eprint(
            "[WARN] Cannot get partition %s: %s"
            % (
                partition,
                stderr.strip(),
            )
        )

        PARTITION_CACHE[
            partition
        ] = info

        return info

    text = stdout.replace(
        "\n",
        " "
    )

    match = re.search(
        r"\bDefaultTime=([^\s]+)",
        text,
    )

    if match:

        info.default_time = (
            match.group(1)
        )

        info.default_seconds = (
            parse_time_limit(
                info.default_time
            )
        )

    match = re.search(
        r"\bMaxTime=([^\s]+)",
        text,
    )

    if match:

        info.max_time = (
            match.group(1)
        )

        info.max_seconds = (
            parse_time_limit(
                info.max_time
            )
        )

    PARTITION_CACHE[
        partition
    ] = info

    return info


# ================================================================
# Calculate result
# ================================================================

def calculate_job_result(
    job,
    records,
):

    result = JobResult()

    result.job_id = job.job_id
    result.job_id_raw = job.job_id_raw
    result.user = job.user
    result.partition = job.partition
    result.state = job.state
    result.elapsed = job.elapsed
    result.alloc_tres = job.alloc_tres

    # --------------------------------------------------------------
    # Requested wall time
    # --------------------------------------------------------------

    result.requested_time = job.timelimit

    result.requested_seconds = parse_time_limit(
        job.timelimit
    )

    # --------------------------------------------------------------
    # Elapsed
    # --------------------------------------------------------------

    result.elapsed_seconds = parse_elapsed(
        job.elapsed
    )

    allocation = parse_tres(
        job.alloc_tres
    )

    # --------------------------------------------------------------
    # CPU allocation
    # --------------------------------------------------------------

    if "cpu" in allocation:

        try:

            result.cpus = float(
                allocation["cpu"]
            )

        except ValueError:

            pass

    # --------------------------------------------------------------
    # Memory allocation
    # --------------------------------------------------------------

    result.memory_bytes = parse_memory(
        allocation.get(
            "mem",
            ""
        )
    )

    # --------------------------------------------------------------
    # GPU allocation
    # --------------------------------------------------------------

    (
        result.gpu_count,
        result.gpu_types,
    ) = parse_gpu_info(
        allocation
    )

    # --------------------------------------------------------------
    # Partition
    # --------------------------------------------------------------

    partition_info = get_partition_info(
        job.partition
    )

    if partition_info:

        result.partition_default_time = (
            partition_info.default_time
        )

        result.partition_max_time = (
            partition_info.max_time
        )

        if (
            result.elapsed_seconds is not None
            and partition_info.default_seconds
            and partition_info.default_seconds > 0
        ):

            result.elapsed_default_percent = (
                result.elapsed_seconds
                / float(
                    partition_info.default_seconds
                )
                * 100.0
            )

        if (
            result.elapsed_seconds is not None
            and partition_info.max_seconds
            and partition_info.max_seconds > 0
        ):

            result.elapsed_max_percent = (
                result.elapsed_seconds
                / float(
                    partition_info.max_seconds
                )
                * 100.0
            )

    # --------------------------------------------------------------
    # Elapsed / requested time
    # --------------------------------------------------------------

    if (
        result.elapsed_seconds is not None
        and result.requested_seconds is not None
        and result.requested_seconds > 0
    ):

        result.elapsed_requested_percent = (
            result.elapsed_seconds
            / float(
                result.requested_seconds
            )
            * 100.0
        )

    # --------------------------------------------------------------
    # Steps
    # --------------------------------------------------------------

    steps = find_steps(
        records,
        job.job_id
    )

    result.step_count = len(
        steps
    )

    application_steps = [
        step
        for step in steps
        if not is_extern(step)
        and not is_batch(step)
    ]

    result.application_step_count = (
        len(application_steps)
    )

    # --------------------------------------------------------------
    # Select usage step
    # --------------------------------------------------------------

    usage_record = select_usage_record(
        job,
        steps
    )

    if usage_record is not None:

        result.job_step = (
            usage_record.job_id
        )

        result.tres_usage_in_ave = (
            usage_record.tres_usage_in_ave
        )

        usage = parse_tres(
            usage_record.tres_usage_in_ave
        )

        # CPU
        if "cpu" in usage:

            result.cpu_used_seconds = (
                parse_elapsed(
                    usage["cpu"]
                )
            )

        # Memory
        if "mem" in usage:

            result.memory_used_bytes = (
                parse_memory(
                    usage["mem"]
                )
            )

        # GPU utilization
        for key in (
            "gres/gpuutil",
            "gpuutil",
        ):

            if key in usage:

                result.gpu_efficiency_percent = (
                    parse_gpu_percent(
                        usage[key]
                    )
                )

                if (
                    result.gpu_efficiency_percent
                    is not None
                ):
                    break

    # --------------------------------------------------------------
    # Multiple application steps
    # --------------------------------------------------------------

    elif application_steps:

        (
            result.cpu_used_seconds,
            result.memory_used_bytes,
            result.gpu_efficiency_percent,
        ) = aggregate_step_usage(
            application_steps
        )

        result.job_step = (
            "multiple-steps"
        )

    # --------------------------------------------------------------
    # CPU efficiency
    # --------------------------------------------------------------

    if (
        result.cpu_used_seconds is not None
        and result.cpus is not None
        and result.elapsed_seconds is not None
        and result.cpus > 0
        and result.elapsed_seconds > 0
    ):

        result.cpu_efficiency_percent = (
            result.cpu_used_seconds
            / (
                result.cpus
                * result.elapsed_seconds
            )
            * 100.0
        )

    # --------------------------------------------------------------
    # Memory efficiency
    # --------------------------------------------------------------

    if (
        result.memory_used_bytes is not None
        and result.memory_bytes is not None
        and result.memory_bytes > 0
    ):

        result.memory_efficiency_percent = (
            result.memory_used_bytes
            / result.memory_bytes
            * 100.0
        )

    # --------------------------------------------------------------
    # GPU efficiency
    # --------------------------------------------------------------

    if (
        result.gpu_count is None
        or result.gpu_count <= 0
    ):

        result.gpu_efficiency_percent = None

    return result


# ================================================================
# Retry
# ================================================================

def get_job_with_retry(
    job_id,
    retries,
    retry_delay,
):

    records = []

    for attempt in range(
        retries + 1
    ):

        try:

            records = run_sacct(
                job_id=job_id
            )

        except RuntimeError as exc:

            eprint(
                "[WARN] %s"
                % exc
            )

            records = []

        job = find_job_record(
            records,
            job_id
        )

        if job:

            return (
                job,
                records,
            )

        if attempt < retries:

            time.sleep(
                retry_delay
            )

    return (
        None,
        records,
    )


# ================================================================
# Output
# ================================================================

def result_dict(result):

    return {
        "JobID": result.job_id,
        "User": result.user,
        "Partition": result.partition,
        "State": result.state,
        "Elapsed": result.elapsed,

        "Requested": (
            result.requested_time
            if result.requested_seconds is not None
            else "-"
        ),

        "DefaultTime": (
            result.partition_default_time
        ),

        "MaxTime": (
            result.partition_max_time
        ),

        "Elapsed/Requested": (
            format_percent(
                result.elapsed_requested_percent
            )
        ),

        "Elapsed/Default": (
            format_percent(
                result.elapsed_default_percent
            )
        ),

        "Elapsed/Max": (
            format_percent(
                result.elapsed_max_percent
            )
        ),

        "CPUs": result.cpus,

        "CPUUsedSeconds": (
            result.cpu_used_seconds
        ),

        "CPUEff": (
            format_percent(
                result.cpu_efficiency_percent
            )
        ),

        "MemAllocated": format_bytes(
            result.memory_bytes
        ),

        "MemUsed": format_bytes(
            result.memory_used_bytes
        ),

        "MemEff": format_percent(
            result.memory_efficiency_percent
        ),

        "GPUs": result.gpu_count,

        "GPUTypes": result.gpu_types,

        "GPUEff": format_percent(
            result.gpu_efficiency_percent
        ),

        "# of Steps": result.step_count,

        "JobStep": result.job_step,
    }


def print_table(results):

    if not results:

        print("No jobs found.")
        return

    headers = [
        "JobID",
        "User",
        "Partition",
        "State",
        "Elapsed",
        "Requested",
        "Default",
        "MaxTime",
        "Elap/Req",
        "Elap/Def",
        "Elap/Max",
        "CPU",
        "CPU Used",
        "CPU Eff",
        "Mem Alloc",
        "Mem Used",
        "Mem Eff",
        "GPUs",
        "GPU Eff",
        "# of Steps",
        "Job Step",
    ]

    rows = []

    for result in results:

        rows.append([
            result.job_id,
            result.user,
            result.partition,
            result.state,
            result.elapsed,

            (
                result.requested_time
                if result.requested_seconds
                is not None
                else "-"
            ),

            (
                result.partition_default_time
                or "-"
            ),

            (
                result.partition_max_time
                or "-"
            ),

            format_percent(
                result.elapsed_requested_percent
            ),

            format_percent(
                result.elapsed_default_percent
            ),

            format_percent(
                result.elapsed_max_percent
            ),

            format_number(
                result.cpus
            ),

            (
                "%.0fs"
                % result.cpu_used_seconds
                if result.cpu_used_seconds
                is not None
                else "-"
            ),

            format_percent(
                result.cpu_efficiency_percent
            ),

            format_bytes(
                result.memory_bytes
            ),

            format_bytes(
                result.memory_used_bytes
            ),

            format_percent(
                result.memory_efficiency_percent
            ),

            format_number(
                result.gpu_count
            ),

            format_percent(
                result.gpu_efficiency_percent
            ),

            str(
                result.step_count
            ),

            result.job_step or "-",
        ])

    widths = []

    for i, header in enumerate(headers):

        width = len(header)

        for row in rows:

            width = max(
                width,
                len(str(row[i]))
            )

        widths.append(width)

    print(
        "  ".join(
            headers[i].ljust(
                widths[i]
            )
            for i in range(
                len(headers)
            )
        )
    )

    print(
        "  ".join(
            "-" * widths[i]
            for i in range(
                len(headers)
            )
        )
    )

    for row in rows:

        print(
            "  ".join(
                str(row[i]).ljust(
                    widths[i]
                )
                for i in range(
                    len(headers)
                )
            )
        )


def print_steps(
    records,
    job_id,
):

    steps = find_steps(
        records,
        job_id
    )

    if not steps:
        return

    print()
    print(
        "Steps for %s:"
        % job_id
    )

    headers = [
        "Step",
        "State",
        "Elapsed",
        "AllocTRES",
        "CPU Ave",
        "Mem Ave",
    ]

    rows = []

    for record in steps:

        usage = parse_tres(
            record.tres_usage_in_ave
        )

        rows.append([
            record.job_id,
            record.state,
            record.elapsed,
            record.alloc_tres,
            usage.get("cpu", ""),
            usage.get("mem", ""),
        ])

    widths = []

    for i, header in enumerate(headers):

        width = len(header)

        for row in rows:

            width = max(
                width,
                len(str(row[i]))
            )

        widths.append(width)

    print(
        "  ".join(
            headers[i].ljust(
                widths[i]
            )
            for i in range(
                len(headers)
            )
        )
    )

    print(
        "  ".join(
            "-" * widths[i]
            for i in range(
                len(headers)
            )
        )
    )

    for row in rows:

        print(
            "  ".join(
                str(row[i]).ljust(
                    widths[i]
                )
                for i in range(
                    len(headers)
                )
            )
        )


def output_results(
    results,
    output_format,
    output_file,
):

    if output_format == "table":

        if output_file:

            with open(
                output_file,
                "w"
            ) as output:

                old_stdout = sys.stdout

                try:

                    sys.stdout = output
                    print_table(results)

                finally:

                    sys.stdout = old_stdout

        else:

            print_table(results)

        return

    data = [
        result_dict(result)
        for result in results
    ]

    if output_format == "json":

        text = json.dumps(
            data,
            indent=2
        )

    elif output_format == "csv":

        output = io.StringIO()

        if data:

            fields = list(
                data[0].keys()
            )

            writer = csv.DictWriter(
                output,
                fieldnames=fields
            )

            writer.writeheader()
            writer.writerows(data)

        text = output.getvalue()

    else:

        raise ValueError(
            "Unknown format: %s"
            % output_format
        )

    if output_file:

        with open(
            output_file,
            "w"
        ) as output:

            output.write(text)

    else:

        print(
            text,
            end=""
        )


# ================================================================
# Job list
# ================================================================

def parse_job_list(values):

    result = []

    for value in values:

        for job_id in value.split(","):

            job_id = job_id.strip()

            if (
                job_id
                and job_id not in result
            ):

                result.append(job_id)

    return result


def process_jobs(
    job_ids,
    args,
):

    results = []

    for job_id in job_ids:

        eprint(
            "[INFO] Processing %s"
            % job_id
        )

        job, records = get_job_with_retry(
            job_id,
            args.retry,
            args.retry_delay,
        )

        if not job:

            eprint(
                "[WARN] No sacct record found "
                "for %s"
                % job_id
            )

            continue

        result = calculate_job_result(
            job,
            records
        )

        results.append(result)

        if args.steps:

            print_steps(
                records,
                job.job_id
            )

    output_results(
        results,
        args.format,
        args.output
    )


# ================================================================
# Historical mode
# ================================================================

def historical_mode(args):

    eprint(
        "[INFO] Running historical query"
    )

    try:

        records = run_sacct(
            start=args.start,
            end=args.end,
            user=args.user,
            all_users=args.allusers,
            allocations_only=True,
        )

    except RuntimeError as exc:

        eprint(
            "[ERROR] %s"
            % exc
        )

        sys.exit(1)

    job_ids = []

    for record in records:

        if is_step(record):
            continue

        job_ids.append(
            record.job_id
        )

    job_ids = sorted(
        set(job_ids)
    )

    eprint(
        "[INFO] Found %d jobs"
        % len(job_ids)
    )

    process_jobs(
        job_ids,
        args
    )


# ================================================================
# slurmctld log monitoring
# ================================================================

JOB_COMPLETE_RE = re.compile(
    r"_job_complete:\s+"
    r"JobId=([^\s(]+)"
    r"(?:\((\d+)\))?"
    r"\s+done"
)


def follow_file(filename):

    while True:

        try:

            with open(
                filename,
                "r"
            ) as log:

                log.seek(
                    0,
                    os.SEEK_END
                )

                inode = os.fstat(
                    log.fileno()
                ).st_ino

                while True:

                    line = log.readline()

                    if line:

                        yield line
                        continue

                    time.sleep(
                        0.25
                    )

                    try:

                        new_inode = os.stat(
                            filename
                        ).st_ino

                        if new_inode != inode:
                            break

                    except OSError:

                        break

        except IOError:

            eprint(
                "[WARN] Cannot open %s; waiting..."
                % filename
            )

            time.sleep(2)


def live_mode(args):

    eprint(
        "[INFO] Monitoring %s"
        % args.log
    )

    seen = set()

    for line in follow_file(
        args.log
    ):

        match = JOB_COMPLETE_RE.search(
            line
        )

        if not match:
            continue

        job_id = match.group(1)
        numeric_id = match.group(2)

        if job_id in seen:
            continue

        seen.add(job_id)

        if numeric_id:

            eprint(
                "[INFO] Job completed: %s "
                "(numeric=%s)"
                % (
                    job_id,
                    numeric_id,
                )
            )

        else:

            eprint(
                "[INFO] Job completed: %s"
                % job_id
            )

        job, records = get_job_with_retry(
            job_id,
            args.retry,
            args.retry_delay,
        )

        if not job:

            eprint(
                "[WARN] No accounting record "
                "found for %s"
                % job_id
            )

            continue

        result = calculate_job_result(
            job,
            records
        )

        output_results(
            [result],
            args.format,
            args.output
        )

        if args.steps:

            print_steps(
                records,
                job.job_id
            )

        if len(seen) > 100000:

            seen.clear()


# ================================================================
# CLI
# ================================================================

def build_parser():

    parser = argparse.ArgumentParser(
        description=(
            "Slurm job CPU/memory/GPU "
            "efficiency monitor."
        )
    )

    parser.add_argument(
        "-j",
        "--jobs",
        action="append",
        default=[],
        metavar="JOB[,JOB...]",
        help=(
            "Process specific job IDs. "
            "Can be specified multiple times."
        ),
    )

    parser.add_argument(
        "-S",
        "--start",
        metavar="TIME",
        help=(
            "Historical query start time."
        ),
    )

    parser.add_argument(
        "-E",
        "--end",
        metavar="TIME",
        help=(
            "Historical query end time."
        ),
    )

    parser.add_argument(
        "-a",
        "--allusers",
        action="store_true",
        help=(
            "Query jobs from all users."
        ),
    )

    parser.add_argument(
        "-u",
        "--user",
        action="append",
        help=(
            "Restrict historical query to "
            "user. Can be specified multiple times."
        ),
    )

    parser.add_argument(
        "-l",
        "--log",
        default="/var/log/slurmctld.log",
        help=(
            "slurmctld log file. "
            "Default: /var/log/slurmctld.log"
        ),
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Output file.",
    )

    parser.add_argument(
        "--format",
        choices=[
            "table",
            "csv",
            "json",
        ],
        default="table",
        help="Output format.",
    )

    parser.add_argument(
        "--steps",
        action="store_true",
        help=(
            "Display all job steps."
        ),
    )

    parser.add_argument(
        "--retry",
        type=int,
        default=5,
        help=(
            "Number of sacct retries. "
            "Default: 5."
        ),
    )

    parser.add_argument(
        "--retry-delay",
        type=float,
        default=1.0,
        help=(
            "Seconds between sacct retries. "
            "Default: 1."
        ),
    )

    return parser


def main():

    parser = build_parser()

    args = parser.parse_args()

    # --------------------------------------------------------------
    # Explicit jobs
    # --------------------------------------------------------------

    if args.jobs:

        job_ids = parse_job_list(
            args.jobs
        )

        if not job_ids:

            parser.error(
                "No job IDs supplied."
            )

        process_jobs(
            job_ids,
            args
        )

        return

    # --------------------------------------------------------------
    # Historical query
    # --------------------------------------------------------------

    if args.start or args.end:

        if not args.start:

            parser.error(
                "-E/--end requires -S/--start"
            )

        historical_mode(args)

        return

    # --------------------------------------------------------------
    # Live monitoring
    # --------------------------------------------------------------

    live_mode(args)


if __name__ == "__main__":
    main()

