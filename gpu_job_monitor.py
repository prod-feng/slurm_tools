#!/usr/bin/env python3
"""
GPU Job Utilization Monitor
===========================

Python 3.6 compatible.

Default behavior:
    Print only the GPU job utilization table.
    Warnings and errors are still printed.

Verbose:
    -v / --verbose
    Print INFO logging in addition to the job table.

Debug:
    --debug
    Print DEBUG + INFO logging in addition to the job table.

Examples:

    ./gpu_job_monitor.py

    ./gpu_job_monitor.py --verbose

    ./gpu_job_monitor.py -v --dry-run

    ./gpu_job_monitor.py --debug
"""

from __future__ import print_function

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time

try:
    import fcntl
except ImportError:
    fcntl = None


# ============================================================================
# Configuration
# ============================================================================

SLURM_BIN = "/cm/shared/apps/slurm/current/bin"

SQUEUE = os.path.join(
    SLURM_BIN,
    "squeue"
)

SCONTROL = os.path.join(
    SLURM_BIN,
    "scontrol"
)

PDSH = "pdsh"
NVIDIA_SMI = "nvidia-smi"
MAIL = "mail"


#WORK DIR. Change it according to your environement.
WORKDIR="/tmp"

# Persistent state.
STATE_FILE = (
    f"{WORKDIR}/jobstats.json"
)

# Lock prevents two cron instances from running simultaneously.
LOCK_FILE = (
    f"{WORKDIR}/jobstats.lock"
)


# ============================================================================
# GPU utilization policy
# ============================================================================

# Ignore jobs that have been running for less than 30 minutes.
MIN_RUNTIME_SECONDS = 30 * 60

# A GPU below this utilization is considered idle.
IDLE_GPU_THRESHOLD = 1

# Average GPU utilization below this is considered under-utilized.
LOW_UTILIZATION_THRESHOLD = 20

# Number of consecutive bad samples before notification.
#
# Cron interval = 10 minutes.
# 6 samples ~= 1 hour.
REQUIRED_BAD_SAMPLES = 6


# ============================================================================
# Email policy
# ============================================================================

# Maximum number of automatic emails per day.
MAX_EMAILS_PER_DAY = 2

# Minimum time between automatic emails.
EMAIL_INTERVAL_SECONDS = (
    24 * 60 * 60 /
    float(MAX_EMAILS_PER_DAY)
)

MAIL_FROM = "hpc_support@example.com"
MAIL_CC = "hpc_admins@example.com"
MAIL_BCC = "john@example.com"

SUPPORT_URL = (
    "https://support.example.com/"
)

# Users excluded from automatic notifications.
EXCLUDED_USERS = set([
    "feng",
])


# ============================================================================
# Command timeouts
# ============================================================================

SLURM_TIMEOUT = 120
GPU_TIMEOUT = 60
MAIL_TIMEOUT = 30


# ============================================================================
# Logging
# ============================================================================

LOG = logging.getLogger(
    "gpu_job_monitor"
)


def configure_logging(args):
    """
    Configure logging.

    Default:
        WARNING and ERROR only.

    --verbose:
        INFO and above.

    --debug:
        DEBUG and above.
    """

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
# Command execution
# ============================================================================

class CommandError(Exception):
    """Raised when an external command fails."""
    pass


def run_command(
        command,
        timeout=60,
        stdin_data=None):
    """
    Execute an external command safely.

    No shell=True is used.
    """

    LOG.debug(
        "Executing command: %s",
        " ".join(command)
    )

    try:

        process = subprocess.Popen(
            command,
            stdin=(
                subprocess.PIPE
                if stdin_data is not None
                else None
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        try:

            stdout, stderr = process.communicate(
                stdin_data.encode("utf-8")
                if stdin_data is not None
                else None
            )

        except Exception:

            process.kill()
            process.wait()
            raise

    except OSError as exc:

        raise CommandError(
            "Unable to execute '{}': {}".format(
                command[0],
                exc
            )
        )

    stdout = stdout.decode(
        "utf-8",
        "replace"
    )

    stderr = stderr.decode(
        "utf-8",
        "replace"
    )

    if process.returncode != 0:

        raise CommandError(
            "Command failed rc={} command='{}' stderr='{}'".format(
                process.returncode,
                " ".join(command),
                stderr.strip()
            )
        )

    return stdout


# ============================================================================
# Parsing helpers
# ============================================================================

def parse_runtime(value):
    """
    Parse Slurm runtime.

    Examples:

        00:30
        01:02:03
        2-01:02:03
    """

    if not value:
        return 0

    days = 0

    if "-" in value:

        day_string, value = value.split(
            "-",
            1
        )

        try:
            days = int(day_string)

        except ValueError:
            return 0

    parts = value.split(":")

    try:

        if len(parts) == 2:

            minutes = int(parts[0])
            seconds = int(parts[1])

            return (
                days * 86400 +
                minutes * 60 +
                seconds
            )

        if len(parts) == 3:

            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])

            return (
                days * 86400 +
                hours * 3600 +
                minutes * 60 +
                seconds
            )

    except ValueError:

        return 0

    return 0


def parse_key_value_line(line):
    """
    Parse a Slurm one-line key=value record.

    Example:

        JobId=84225 JobName=bash UserId=feng(11)
        JobState=RUNNING Nodes=h200x8-04
    """

    result = {}

    matches = list(
        re.finditer(
            r"(\w+)=",
            line
        )
    )

    for index, match in enumerate(matches):

        key = match.group(1)
        start = match.end()

        if index + 1 < len(matches):

            end = matches[
                index + 1
            ].start()

        else:

            end = len(line)

        result[key] = (
            line[start:end]
            .strip()
        )

    return result


def parse_gpu_indexes(value):
    """
    Convert compressed GPU indexes into a list.

    Examples:

        0
            -> [0]

        0,1,2
            -> [0, 1, 2]

        0-3
            -> [0, 1, 2, 3]

        0-2,5,7-8
            -> [0, 1, 2, 5, 7, 8]
    """

    result = []

    if not value:
        return result

    for part in value.split(","):

        part = part.strip()

        if not part:
            continue

        if "-" in part:

            pieces = part.split(
                "-",
                1
            )

            if len(pieces) != 2:

                raise ValueError(
                    "Invalid GPU range '{}'".format(
                        part
                    )
                )

            start = int(
                pieces[0]
            )

            end = int(
                pieces[1]
            )

            if start > end:
                start, end = end, start

            result.extend(
                range(
                    start,
                    end + 1
                )
            )

        else:

            result.append(
                int(part)
            )

    return sorted(
        set(result)
    )


def extract_gpu_indexes(gres):
    """
    Extract physical GPU indexes from Slurm GRES.

    Examples:

        gpu:h200:1(IDX:0)
            -> [0]

        gpu:h200:2(IDX:1-2)
            -> [1, 2]

        gpu:h200:4(IDX:0-3)
            -> [0, 1, 2, 3]

    If GRES is:

        gres:gpu:1

    there is no IDX information, so [] is returned.

    We deliberately do NOT guess which physical GPU was assigned.
    """

    if not gres:
        return []

    match = re.search(
        r"IDX:([^)]+)",
        gres
    )

    if not match:
        return []

    try:

        return parse_gpu_indexes(
            match.group(1)
        )

    except ValueError:

        LOG.warning(
            "Invalid GPU IDX in GRES: %s",
            gres
        )

        return []


def format_gpu_indexes(indexes):
    """
    Convert GPU index list into compact notation.

    Examples:

        [0]
            -> 0

        [0, 1, 2]
            -> 0-2

        [0, 1, 3, 4, 5]
            -> 0-1,3-5
    """

    if not indexes:
        return ""

    indexes = sorted(
        set(indexes)
    )

    ranges = []

    start = indexes[0]
    previous = indexes[0]

    for value in indexes[1:]:

        if value == previous + 1:

            previous = value
            continue

        if start == previous:

            ranges.append(
                str(start)
            )

        else:

            ranges.append(
                "{}-{}".format(
                    start,
                    previous
                )
            )

        start = value
        previous = value

    if start == previous:

        ranges.append(
            str(start)
        )

    else:

        ranges.append(
            "{}-{}".format(
                start,
                previous
            )
        )

    return ",".join(
        ranges
    )


# ============================================================================
# Slurm client
# ============================================================================

class SlurmClient(object):

    def __init__(self):

        self.squeue = SQUEUE
        self.scontrol = SCONTROL

    def get_running_jobs(self):
        """
        Get currently running jobs.

        Using squeue's formatted output is much safer than parsing
        shell pipelines involving awk/grep/paste.
        """

        command = [
            self.squeue,
            "-a",
            "-h",
            "-t",
            "RUNNING",
            "-o",
            "%i|%u|%N|%M|%j"
        ]

        output = run_command(
            command,
            timeout=SLURM_TIMEOUT
        )

        jobs = []

        for line in output.splitlines():

            if not line.strip():
                continue

            fields = line.split("|")

            if len(fields) != 5:

                LOG.warning(
                    "Malformed squeue line: %s",
                    line
                )

                continue

            jobs.append({
                "job_id": fields[0],
                "user": fields[1],
                "nodes": fields[2],
                "runtime": parse_runtime(
                    fields[3]
                ),
                "job_name": fields[4],
                "gres": "",
                "gpu_indexes": [],
            })

        return jobs

    def get_job_details(self):
        """
        Get detailed information for all jobs.

        -o produces one job per line.
        """

        command = [
            self.scontrol,
            "show",
            "job",
            "-d",
            "-o"
        ]

        output = run_command(
            command,
            timeout=SLURM_TIMEOUT
        )

        details = {}

        for line in output.splitlines():

            if not line.strip():
                continue

            fields = parse_key_value_line(
                line
            )

            job_id = fields.get(
                "JobId"
            )

            if not job_id:
                continue

            details[
                str(job_id)
            ] = fields

        return details

    def merge_details(
            self,
            jobs,
            details):

        result = []

        for job in jobs:

            job_id = str(
                job["job_id"]
            )

            detail = details.get(
                job_id
            )

            if detail is None:

                LOG.warning(
                    "Job %s disappeared before scontrol query",
                    job_id
                )

                continue

            user = detail.get(
                "UserId",
                job["user"]
            )

            user = user.split(
                "(",
                1
            )[0]

            nodes = detail.get(
                "Nodes",
                job["nodes"]
            )

            gres = (
                detail.get("GRES")
                or detail.get("Gres")
                or detail.get("JOB_GRES")
                or ""
            )

            gpu_indexes = extract_gpu_indexes(
                gres
            )

            # If this is a GPU job but Slurm did not give us
            # physical indexes, don't guess.
            if (
                "gpu" in gres.lower()
                and not gpu_indexes
            ):

                LOG.warning(
                    "Unable to find GPU IDX in GRES: %s",
                    gres
                )

                continue

            job["user"] = user
            job["nodes"] = nodes
            job["gres"] = gres
            job["gpu_indexes"] = gpu_indexes

            result.append(
                job
            )

        return result


# ============================================================================
# GPU collector
# ============================================================================

class GPUCollector(object):

    QUERY_FIELDS = (
        "index,"
        "name,"
        "pci.bus_id,"
        "temperature.gpu,"
        "memory.used,"
        "memory.total,"
        "utilization.gpu,"
        "utilization.memory"
    )

    def collect_node(self, node):
        """
        Collect all GPUs from a node.

        This is intentionally done once per node rather than once per job.
        """

        command = [
            PDSH,
            "-w",
            node,
            NVIDIA_SMI,
            "--query-gpu={}".format(
                self.QUERY_FIELDS
            ),
            "--format=csv,noheader,nounits"
        ]

        try:

            output = run_command(
                command,
                timeout=GPU_TIMEOUT
            )

        except CommandError as exc:

            LOG.error(
                "GPU collection failed on %s: %s",
                node,
                exc
            )

            return {}

        gpu_data = {}

        for line in output.splitlines():

            if not line.strip():
                continue

            # pdsh normally gives:
            #
            # h200x8-04: 0, NVIDIA H200 NVL, ...
            #
            if ":" not in line:

                LOG.warning(
                    "Unexpected GPU output from %s: %s",
                    node,
                    line
                )

                continue

            _, payload = line.split(
                ":",
                1
            )

            fields = [
                item.strip()
                for item in payload.split(",")
            ]

            if len(fields) != 8:

                LOG.warning(
                    "Malformed nvidia-smi output on %s: %s",
                    node,
                    line
                )

                continue

            try:

                index = int(
                    fields[0]
                )

                gpu_data[index] = {
                    "index": index,
                    "name": fields[1],
                    "bus_id": fields[2],
                    "temperature": int(
                        fields[3]
                    ),
                    "memory_used": int(
                        fields[4]
                    ),
                    "memory_total": int(
                        fields[5]
                    ),
                    "utilization": int(
                        fields[6]
                    ),
                    "memory_utilization": int(
                        fields[7]
                    ),
                }

            except (
                ValueError,
                TypeError
            ):

                LOG.warning(
                    "Unable to parse GPU data on %s: %s",
                    node,
                    line
                )

        LOG.info(
            "Collected %d GPUs from %s",
            len(gpu_data),
            node
        )

        return gpu_data


# ============================================================================
# GPU metrics
# ============================================================================

def calculate_metrics(
        job,
        node_gpu_data):

    node = job["nodes"]

    gpu_indexes = job[
        "gpu_indexes"
    ]

    if not gpu_indexes:
        return None

    node_data = node_gpu_data.get(
        node
    )

    if not node_data:

        LOG.warning(
            "No GPU telemetry available for node %s",
            node
        )

        return None

    selected = []

    for gpu_index in gpu_indexes:

        gpu = node_data.get(
            gpu_index
        )

        if gpu is None:

            LOG.warning(
                "GPU %d for job %s not found on node %s",
                gpu_index,
                job["job_id"],
                node
            )

            continue

        selected.append(
            gpu
        )

    if not selected:
        return None

    utilization = [
        gpu["utilization"]
        for gpu in selected
    ]

    memory_utilization = [
        gpu["memory_utilization"]
        for gpu in selected
    ]

    idle_gpu_count = sum(
        1
        for value in utilization
        if value < IDLE_GPU_THRESHOLD
    )

    return {
        "node": node,
        "gpu_count": len(selected),
        "gpu_indexes": gpu_indexes,
        "gpus": selected,

        "average_utilization":
            float(sum(utilization))
            / len(utilization),

        "minimum_utilization":
            min(utilization),

        "maximum_utilization":
            max(utilization),

        "average_memory_utilization":
            float(sum(memory_utilization))
            / len(memory_utilization),

        "idle_gpu_count":
            idle_gpu_count,
    }


def classify_utilization(metrics):

    if (
        metrics["average_utilization"]
        < LOW_UTILIZATION_THRESHOLD
    ):

        return "LOW_UTILIZATION"

    if metrics["idle_gpu_count"] > 0:

        return "IMBALANCED"

    return "NORMAL"


# ============================================================================
# Job table
# ============================================================================

def format_job_table(rows):
    """
    Create a clean fixed-width table.

    This table is intentionally NOT sent through LOG.info().
    It is printed directly so logger timestamps don't destroy alignment.
    """

    if not rows:
        return ""

    lines = []

    header = (
        "{:<10} {:<12} {:<16} {:<10} "
        "{:>7} {:>7} {:>7} {:>5} {:<18} {:>4}"
    ).format(
        "JOBID",
        "USER",
        "NODE",
        "GPUS",
        "AVG",
        "MIN",
        "MAX",
        "IDLE",
        "STATE",
        "BAD"
    )

    separator = (
        "{:<10} {:<12} {:<16} {:<10} "
        "{:>7} {:>7} {:>7} {:>5} {:<18} {:>4}"
    ).format(
        "-" * 10,
        "-" * 12,
        "-" * 16,
        "-" * 10,
        "-" * 7,
        "-" * 7,
        "-" * 7,
        "-" * 5,
        "-" * 18,
        "-" * 4
    )

    lines.append(header)
    lines.append(separator)

    for row in rows:

        lines.append(
            "{:<10} {:<12} {:<16} {:<10} "
            "{:>6.1f}% {:>6d}% {:>6d}% {:>5d} "
            "{:<18} {:>4d}".format(
                str(
                    row["job_id"]
                )[:10],

                str(
                    row["user"]
                )[:12],

                str(
                    row["node"]
                )[:16],

                str(
                    row["gpus"]
                )[:10],

                float(
                    row["avg"]
                ),

                int(
                    row["min"]
                ),

                int(
                    row["max"]
                ),

                int(
                    row["idle"]
                ),

                str(
                    row["state"]
                )[:18],

                int(
                    row["bad"]
                )
            )
        )

    return "\n".join(
        lines
    )


def print_job_table(rows):
    """
    Print the primary user-facing report.

    This always happens, regardless of --verbose.
    """

    if not rows:
        print("No GPU jobs to report.")
        return

    table = format_job_table(
        rows
    )

    print("")
    print(table)
    print("")


# ============================================================================
# Persistent state
# ============================================================================

class StateStore(object):

    def __init__(self, filename):

        self.filename = filename

    def load(self):

        if not os.path.exists(
            self.filename
        ):

            return {}

        try:

            with open(
                self.filename,
                "r"
            ) as handle:

                data = json.load(
                    handle
                )

            if not isinstance(
                data,
                dict
            ):

                raise ValueError(
                    "State file must contain a JSON object"
                )

            return data

        except Exception:

            LOG.exception(
                "Unable to load state file %s",
                self.filename
            )

            return {}

    def save(self, data):

        directory = os.path.dirname(
            self.filename
        )

        if (
            directory
            and not os.path.exists(directory)
        ):

            os.makedirs(
                directory
            )

        fd, temporary_file = tempfile.mkstemp(
            prefix=".gpu-monitor.",
            dir=directory
        )

        try:

            with os.fdopen(
                fd,
                "w"
            ) as handle:

                json.dump(
                    data,
                    handle,
                    indent=2,
                    sort_keys=True
                )

                handle.write(
                    "\n"
                )

                handle.flush()

                os.fsync(
                    handle.fileno()
                )

            os.rename(
                temporary_file,
                self.filename
            )

        except Exception:

            try:

                os.unlink(
                    temporary_file
                )

            except OSError:
                pass

            raise


# ============================================================================
# Process locking
# ============================================================================

class FileLock(object):

    def __init__(self, filename):

        self.filename = filename
        self.handle = None

    def acquire(self):

        if fcntl is None:

            LOG.warning(
                "fcntl is unavailable; process locking disabled"
            )

            return

        directory = os.path.dirname(
            self.filename
        )

        if (
            directory
            and not os.path.exists(directory)
        ):

            os.makedirs(
                directory
            )

        self.handle = open(
            self.filename,
            "w"
        )

        try:

            fcntl.flock(
                self.handle.fileno(),
                fcntl.LOCK_EX |
                fcntl.LOCK_NB
            )

        except IOError:

            self.handle.close()
            self.handle = None

            raise RuntimeError(
                "Another GPU monitor instance is already running"
            )

    def release(self):

        if self.handle is None:
            return

        try:

            fcntl.flock(
                self.handle.fileno(),
                fcntl.LOCK_UN
            )

        except IOError:

            pass

        self.handle.close()
        self.handle = None


# ============================================================================
# User email
# ============================================================================

def get_user_email(user):

    try:

        output = run_command([
            "getent",
            "passwd",
            user
        ])

    except CommandError:

        LOG.warning(
            "Unable to query passwd information for %s",
            user
        )

        return None

    fields = output.strip().split(
        ":"
    )

    if len(fields) < 5:
        return None

    gecos = fields[4]

    match = re.search(
        r"[\w.+-]+@[\w.-]+\.[A-Za-z]+",
        gecos
    )

    if match:

        return match.group(
            0
        )

    return None


# ============================================================================
# Email formatting
# ============================================================================

def format_gpu_information(metrics):

    lines = []

    lines.append(
        "{:<15} {:<24} {:<6} {:<6} "
        "{:<14} {:<7} {:<8}".format(
            "Node",
            "GPU",
            "GPUID",
            "T(C)",
            "Gmem",
            "GPU%",
            "Gmem%"
        )
    )

    lines.append(
        "-" * 95
    )

    for gpu in metrics["gpus"]:

        lines.append(
            "{:<15} {:<24} {:<6} {:<6} "
            "{:<14} {:<7} {:<8}".format(
                metrics["node"],
                gpu["name"][:24],
                gpu["index"],
                gpu["temperature"],
                "{} MiB".format(
                    gpu["memory_used"]
                ),
                "{}%".format(
                    gpu["utilization"]
                ),
                "{}%".format(
                    gpu["memory_utilization"]
                )
            )
        )

    return "\n".join(
        lines
    )


def send_email(
        job,
        metrics,
        email_address,
        dry_run=False):

    classification = classify_utilization(
        metrics
    )

    gpu_info = format_gpu_information(
        metrics
    )

    subject = (
        "GPU job {} {} on NvWulf Cluster"
        .format(
            job["job_id"],
            classification.lower()
        )
    )

    body = """\
<html>
<body style="font-family: sans-serif, Arial, Helvetica;">

<p>Dear {user},</p>

<p>
Your computing job <strong>{job_id}</strong> appears to be
under-utilizing its allocated GPU resources.
</p>

<pre>
Job ID:                    {job_id}
Job name:                  {job_name}
User:                      {user}
Node:                      {node}
Allocated GPUs:            {gpu_indexes}
Average GPU utilization:   {average:.1f}%
Minimum GPU utilization:   {minimum}%
Maximum GPU utilization:   {maximum}%
Idle GPUs:                 {idle}
Average GPU memory usage:  {memory:.1f}%
Status:                    {classification}
</pre>

<p>GPU information:</p>

<pre>{gpu_info}</pre>

<p>
This condition has been observed over multiple monitoring samples
during approximately the last hour.
</p>

<p>
Please review your job configuration and application to determine
whether the allocated GPU resources are being used as intended.
It may be appropriate to check whether your application is CPU/I/O
bound, whether it has allocated more GPUs than required, or whether
all allocated GPUs are being used correctly.
</p>

<p>
To ensure fair access to GPU resources, please do not leave GPUs
idle once your job is no longer actively using them.
</p>

<p>
If you need assistance optimizing GPU resource usage, please submit
a ticket at:
<a href="{support_url}">{support_url}</a>
</p>

<p>
Thanks!<br>
HPC Support
</p>

</body>
</html>
""".format(
        user=job["user"],
        job_id=job["job_id"],
        job_name=job["job_name"],
        node=metrics["node"],
        gpu_indexes=", ".join(
            str(x)
            for x in metrics["gpu_indexes"]
        ),
        average=metrics[
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
        memory=metrics[
            "average_memory_utilization"
        ],
        classification=classification,
        gpu_info=gpu_info,
        support_url=SUPPORT_URL
    )

    if dry_run:

        LOG.warning(
            "DRY RUN: would send email for job %s to %s",
            job["job_id"],
            email_address
        )

        print("")
        print("=" * 80)
        print("DRY RUN EMAIL")
        print("To: {}".format(
            email_address
        ))
        print("Subject: {}".format(
            subject
        ))
        print("=" * 80)
        print(body)
        print("=" * 80)

        return True

    command = [
        MAIL,
        "-M",
        "text/html",
        "-s",
        subject,
        "-r",
        MAIL_FROM,
        "-b",
        MAIL_BCC,
        "-c",
        MAIL_CC,
        email_address
    ]

    try:

        run_command(
            command,
            timeout=MAIL_TIMEOUT,
            stdin_data=body
        )

        LOG.info(
            "Email sent for job %s to %s",
            job["job_id"],
            email_address
        )

        return True

    except CommandError:

        LOG.exception(
            "Unable to send email for job %s",
            job["job_id"]
        )

        return False


# ============================================================================
# Persistent job state
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

    record["user"] = job[
        "user"
    ]

    record["job_name"] = job[
        "job_name"
    ]

    record["node"] = job[
        "nodes"
    ]

    record["gpu_indexes"] = job[
        "gpu_indexes"
    ]

    record["last_seen"] = now

    record["last_classification"] = (
        classification
    )

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
            ) + 1
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

    if (
        record.get(
            "bad_samples",
            0
        )
        < REQUIRED_BAD_SAMPLES
    ):

        return False

    last_email = record.get(
        "last_email",
        0
    )

    if (
        time.time() - last_email
        < EMAIL_INTERVAL_SECONDS
    ):

        return False

    return True


# ============================================================================
# Monitor
# ============================================================================

def monitor(
        slurm,
        gpu_collector,
        state_store,
        dry_run=False):

    start_time = time.time()

    # ------------------------------------------------------------------------
    # Get running jobs.
    # ------------------------------------------------------------------------

    jobs = slurm.get_running_jobs()

    LOG.info(
        "squeue returned %d running jobs",
        len(jobs)
    )

    if not jobs:

        state_store.save({})

        print("No running GPU jobs.")
        return

    # ------------------------------------------------------------------------
    # Get detailed Slurm information.
    # ------------------------------------------------------------------------

    details = slurm.get_job_details()

    jobs = slurm.merge_details(
        jobs,
        details
    )

    # ------------------------------------------------------------------------
    # Select eligible GPU jobs.
    # ------------------------------------------------------------------------

    eligible_jobs = []

    for job in jobs:

        if (
            job["runtime"]
            < MIN_RUNTIME_SECONDS
        ):

            LOG.debug(
                "Skipping job %s: runtime=%d seconds",
                job["job_id"],
                job["runtime"]
            )

            continue

        if not job["gpu_indexes"]:
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

        print("No eligible GPU jobs.")
        return

    # ------------------------------------------------------------------------
    # Group jobs by node.
    # ------------------------------------------------------------------------

    jobs_by_node = {}

    for job in eligible_jobs:

        node = job["nodes"]

        # Current implementation handles one node per job.
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
        ).append(
            job
        )

    # ------------------------------------------------------------------------
    # Collect GPU telemetry once per node.
    # ------------------------------------------------------------------------

    node_gpu_data = {}

    for node in sorted(
        jobs_by_node.keys()
    ):

        node_gpu_data[node] = (
            gpu_collector.collect_node(
                node
            )
        )

    # ------------------------------------------------------------------------
    # Load state.
    # ------------------------------------------------------------------------

    state = state_store.load()

    current_job_ids = set()

    table_rows = []

    normal_jobs = 0
    bad_jobs = 0

    # ------------------------------------------------------------------------
    # Evaluate jobs.
    # ------------------------------------------------------------------------

    for job in eligible_jobs:

        job_id = str(
            job["job_id"]
        )

        current_job_ids.add(
            job_id
        )

        metrics = calculate_metrics(
            job,
            node_gpu_data
        )

        if metrics is None:
            continue

        classification = classify_utilization(
            metrics
        )

        record = update_job_state(
            state,
            job,
            classification,
            metrics
        )

        # --------------------------------------------------------------------
        # Counters.
        # --------------------------------------------------------------------

        if classification == "NORMAL":

            normal_jobs += 1

        else:

            bad_jobs += 1

        # --------------------------------------------------------------------
        # Table data.
        # --------------------------------------------------------------------

        table_rows.append({
            "job_id": job["job_id"],

            "user": job["user"],

            "node": job["nodes"],

            "gpus": format_gpu_indexes(
                job["gpu_indexes"]
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

        # --------------------------------------------------------------------
        # Email notification.
        # --------------------------------------------------------------------

        if classification == "NORMAL":
            continue

        if job["user"] in EXCLUDED_USERS:

            LOG.debug(
                "User %s is excluded from notifications",
                job["user"]
            )

            continue

        if not notification_allowed(
            record
        ):

            continue

        email_address = get_user_email(
            job["user"]
        )

        if not email_address:

            LOG.warning(
                "No email address found for user %s",
                job["user"]
            )

            continue

        if send_email(
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
                    ) + 1
                )

    # ------------------------------------------------------------------------
    # Remove state for jobs that are no longer running.
    # ------------------------------------------------------------------------

    for job_id in list(
        state.keys()
    ):

        if job_id not in current_job_ids:

            LOG.debug(
                "Removing stale state for job %s",
                job_id
            )

            del state[
                job_id
            ]

    # ------------------------------------------------------------------------
    # PRIMARY REPORT
    #
    # Always print the table.
    #
    # DO NOT use LOG.info() here.
    # ------------------------------------------------------------------------

    print_job_table(
        table_rows
    )

    # ------------------------------------------------------------------------
    # Operational information.
    #
    # These only appear with --verbose or --debug.
    # ------------------------------------------------------------------------

    LOG.info(
        "GPU monitor summary: "
        "jobs=%d normal=%d underutilized=%d nodes=%d",
        len(table_rows),
        normal_jobs,
        bad_jobs,
        len(node_gpu_data)
    )

    # ------------------------------------------------------------------------
    # Save persistent state.
    # ------------------------------------------------------------------------

    state_store.save(
        state
    )

    elapsed = (
        time.time()
        - start_time
    )

    LOG.info(
        "Monitoring completed in %.2f seconds",
        elapsed
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
        help=(
            "Show informational monitor messages"
        )
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help=(
            "Show DEBUG and INFO messages"
        )
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Do not send email; display email content instead"
        )
    )

    parser.add_argument(
        "--state-file",
        default=STATE_FILE,
        help=(
            "Persistent state file "
            "(default: {})".format(
                STATE_FILE
            )
        )
    )

    parser.add_argument(
        "--lock-file",
        default=LOCK_FILE,
        help=(
            "Process lock file "
            "(default: {})".format(
                LOCK_FILE
            )
        )
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

        slurm = SlurmClient()

        gpu_collector = GPUCollector()

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


# ============================================================================
# Entry point
# ============================================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )

