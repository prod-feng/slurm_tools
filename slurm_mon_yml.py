#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Slurm cluster statistics collector.

Compatible with:
    Python 3.6+
    Python 3.13+

Requires:
    PyYAML


============================================================
DATA SOURCES
============================================================

Physical node information:

    scontrol show node -o

Job information:

    squeue -a -h


============================================================
COMPUTE NODE FILTER
============================================================

scontrol show node lists all Slurm nodes, which can include
login nodes, service nodes, etc.

Only nodes with a valid Partitions= field are considered
compute nodes.

Example:

    Partitions=cpu_extra,cpu_long,cpu_short

is a compute node.

Nodes with:

    Partitions=(null)

    Partitions=N/A

    Partitions=None

    Partitions=-

or an empty Partitions field are ignored.


============================================================
GPU LOGIC
============================================================

A compute node is a GPU node only when its Gres= field
contains a GPU GRES.

Examples:

    Gres=gpu:a100:8
    Gres=gpu:h200:8
    Gres=gpu:rtx6000:4
    Gres=gpu:a100:4,gpu:h200:4


GPU totals come from:

    Gres=


GPU allocation comes primarily from:

    GresUsed=


Example:

    Gres=gpu:a100:44
    GresUsed=gpu:42

becomes:

    a100:
        total: 44
        allocated: 42
        idle: 2


If:

    Gres=gpu:h200:8
    GresUsed=gpu:4

then:

    h200:
        total: 8
        allocated: 4
        idle: 4


If GresUsed explicitly contains a GPU type:

    GresUsed=gpu:a100:4

then that type is used directly.


============================================================
MULTIPLE GPU TYPES
============================================================

If a node has:

    Gres=gpu:a100:4,gpu:h200:4

and:

    GresUsed=gpu:3

the GPU type cannot safely be determined from the generic
allocation alone.

The script therefore reports:

    generic: 3

rather than guessing.


============================================================
OUTPUT
============================================================

timestamp: "2026-09-18T19:00:00Z"

nodes:
  total: 100
  idle: 40
  allocated: 50
  down: 10
  available: 90

cpu:
  total: 6400
  allocated: 3000
  idle: 3400

gpu:
  total: 44
  allocated: 42
  idle: 2
  types:
    a100:
      total: 16
      allocated: 14
      idle: 2
    h200:
      total: 16
      allocated: 16
      idle: 0
    rtx6000:
      total: 12
      allocated: 12
      idle: 0

jobs:
  total: 100
  running: 40
  pending: 60
"""

from __future__ import print_function

import argparse
import datetime
import re
import subprocess
import sys
from collections import OrderedDict

import yaml


# ============================================================
# YAML support
# ============================================================

def represent_ordered_dict(dumper, data):
    return dumper.represent_dict(data.items())


yaml.add_representer(
    OrderedDict,
    represent_ordered_dict,
    Dumper=yaml.SafeDumper
)


# ============================================================
# Run command
# ============================================================

def run_command(command):
    """
    Run a command and return stdout.

    Raises RuntimeError on failure.
    """

    try:

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        stdout, stderr = process.communicate()

    except OSError as exc:

        raise RuntimeError(
            "Unable to execute {}: {}".format(
                command[0],
                exc
            )
        )

    if process.returncode != 0:

        raise RuntimeError(
            "Command failed: {}\n{}".format(
                " ".join(command),
                stderr.strip()
            )
        )

    return stdout.strip()


# ============================================================
# Basic helpers
# ============================================================

def parse_number(value):
    """
    Extract the first integer.

    Examples:

        123
        123G
        123/456
    """

    if not value:
        return 0

    match = re.match(
        r"^\s*(\d+)",
        value
    )

    if match:
        return int(
            match.group(1)
        )

    return 0


def count_lines(output):
    """
    Count non-empty lines.
    """

    if not output:
        return 0

    return sum(
        1
        for line in output.splitlines()
        if line.strip()
    )


def normalize_gpu_type(gpu_type):
    """
    Normalize GPU type names.

    Examples:

        A100       -> a100
        H200       -> h200
        RTX6000    -> rtx6000
    """

    if not gpu_type:
        return "generic"

    return gpu_type.strip().lower()


# ============================================================
# Parse scontrol key=value records
# ============================================================

def parse_key_value_record(record):
    """
    Parse one line from:

        scontrol show node -o

    Example:

        NodeName=gpu001 NodeAddr=gpu001 State=IDLE
        CPUTot=128 CPUAlloc=0 Gres=gpu:a100:8
        GresUsed=(null)
        AllocTRES=cpu=0 Partitions=gpu

    Returns a dictionary.
    """

    result = {}

    if not record:
        return result

    for item in record.split():

        if "=" not in item:
            continue

        key, value = item.split(
            "=",
            1
        )

        result[key] = value

    return result


# ============================================================
# Compute node filter
# ============================================================

def is_compute_node(node):
    """
    Determine whether an scontrol node record represents a
    compute node.

    The primary indicator is the Partitions= field.

    Example:

        Partitions=cpu_extra,cpu_long,cpu_short

    is considered a compute node.

    Login/service nodes with no compute partitions are ignored.

    Invalid values such as:

        (null)
        N/A
        None
        none
        -

    are not considered compute nodes.
    """

    partitions = node.get(
        "Partitions",
        ""
    )

    if not partitions:
        return False

    partitions = partitions.strip()

    if partitions in (
        "",
        "(null)",
        "N/A",
        "None",
        "none",
        "-"
    ):
        return False

    return True


# ============================================================
# Parse GPU GRES
# ============================================================

def parse_gpu_gres(gres):
    """
    Parse GPU information from a Slurm Gres or GresUsed field.

    Supported examples:

        gpu:8

        gpu:a100:8

        gpu:h200:8

        gpu:rtx6000:4

        gpu:a100:8(S:0-7)

        gpu:a100:8(IDX:0-7)

        gpu:a100:4,gpu:h200:4

    Returns:

        {
            "generic": 8
        }

    or:

        {
            "a100": 8
        }

    or:

        {
            "a100": 4,
            "h200": 4
        }
    """

    result = OrderedDict()

    if not gres:
        return result

    gres = gres.strip()

    if gres in (
        "",
        "(null)",
        "N/A",
        "None",
        "none",
        "-"
    ):
        return result

    for item in gres.split(","):

        item = item.strip()

        if not item:
            continue

        # Remove socket/index information.

        item = item.split(
            "(",
            1
        )[0]

        fields = item.split(":")

        # ----------------------------------------------------
        # gpu:8
        # ----------------------------------------------------

        if len(fields) == 2:

            if fields[0].lower() != "gpu":
                continue

            try:

                count = int(
                    fields[1]
                )

            except ValueError:

                continue

            if count > 0:

                result["generic"] = (
                    result.get(
                        "generic",
                        0
                    )
                    + count
                )

            continue

        # ----------------------------------------------------
        # gpu:a100:8
        # gpu:h200:8
        # gpu:rtx6000:4
        # ----------------------------------------------------

        if len(fields) >= 3:

            if fields[0].lower() != "gpu":
                continue

            gpu_type = normalize_gpu_type(
                fields[1]
            )

            try:

                count = int(
                    fields[2]
                )

            except ValueError:

                continue

            if count <= 0:
                continue

            result[gpu_type] = (
                result.get(
                    gpu_type,
                    0
                )
                + count
            )

    return result


# ============================================================
# Determine whether node is a GPU node
# ============================================================

def is_gpu_node(gres):
    """
    A node is a GPU node when its Gres field contains a GPU.
    """

    return bool(
        parse_gpu_gres(
            gres
        )
    )


# ============================================================
# Parse GPU information from TRES
# ============================================================

def parse_gpu_tres(value):
    """
    Parse GPU information from AllocTRES/TRES.

    Supported examples:

        gres/gpu=4

        gres/gpu:a100=4

        gres/gpu:h200=4

        gres/gpu:rtx6000=4

        gpu=4

        gpu:a100=4
    """

    result = OrderedDict()

    if not value:
        return result

    value = value.strip()

    if not value:
        return result

    # --------------------------------------------------------
    # Typed GPU.
    # --------------------------------------------------------

    typed_pattern = re.compile(
        r"(?:gres/)?gpu:([^:=,]+)=(\d+)"
    )

    for match in typed_pattern.finditer(
        value
    ):

        gpu_type = normalize_gpu_type(
            match.group(1)
        )

        count = int(
            match.group(2)
        )

        if count <= 0:
            continue

        result[gpu_type] = (
            result.get(
                gpu_type,
                0
            )
            + count
        )

    if result:
        return result

    # --------------------------------------------------------
    # Generic GPU.
    # --------------------------------------------------------

    generic_match = re.search(
        r"(?:gres/)?gpu=(\d+)",
        value
    )

    if generic_match:

        count = int(
            generic_match.group(1)
        )

        if count > 0:

            result["generic"] = count

    return result


# ============================================================
# Resolve GPU allocation using the same node's Gres
# ============================================================

def resolve_gpu_allocation(
    total_gpu_info,
    allocated_gpu_info
):
    """
    Resolve generic GPU allocation using the GPU type(s)
    configured on the SAME physical node.

    Example:

        Gres=gpu:a100:44
        GresUsed=gpu:42

    becomes:

        {"a100": 42}


    Example:

        Gres=gpu:h200:8
        GresUsed=gpu:4

    becomes:

        {"h200": 4}


    Example:

        Gres=gpu:a100:4,gpu:h200:4
        GresUsed=gpu:3

    becomes:

        {"generic": 3}

    because the exact GPU type cannot be determined.
    """

    if not allocated_gpu_info:
        return {}

    result = OrderedDict()

    # GPU types configured on this physical node.

    configured_types = [
        gpu_type
        for gpu_type in total_gpu_info
        if gpu_type != "generic"
    ]

    for gpu_type, count in (
        allocated_gpu_info.items()
    ):

        if count <= 0:
            continue

        # ----------------------------------------------------
        # Explicitly typed allocation.
        # ----------------------------------------------------

        if gpu_type != "generic":

            result[gpu_type] = (
                result.get(
                    gpu_type,
                    0
                )
                + count
            )

            continue

        # ----------------------------------------------------
        # Generic allocation.
        #
        # Exactly one configured GPU type means we can resolve
        # it safely.
        # ----------------------------------------------------

        if len(configured_types) == 1:

            real_gpu_type = (
                configured_types[0]
            )

            result[real_gpu_type] = (
                result.get(
                    real_gpu_type,
                    0
                )
                + count
            )

        else:

            # Multiple GPU types on this node.
            # Do not guess.
            result["generic"] = (
                result.get(
                    "generic",
                    0
                )
                + count
            )

    return result


# ============================================================
# Get all Slurm nodes
# ============================================================

def get_node_info():
    """
    Get all nodes from:

        scontrol show node -o

    Then keep only nodes with a valid Partitions= field.
    """

    output = run_command([
        "scontrol",
        "show",
        "node",
        "-o"
    ])

    nodes = []

    if not output:
        return nodes

    for line in output.splitlines():

        line = line.strip()

        if not line:
            continue

        record = parse_key_value_record(
            line
        )

        if not record:
            continue

        node_name = record.get(
            "NodeName"
        )

        if not node_name:
            continue

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Ignore login/service/etc. nodes.
        # ----------------------------------------------------

        if not is_compute_node(
            record
        ):
            continue

        nodes.append(
            record
        )

    return nodes


# ============================================================
# Node and CPU statistics
# ============================================================

def get_node_stats(nodes):
    """
    Calculate node and CPU statistics.

    Only compute nodes are passed to this function.
    """

    stats = OrderedDict()

    stats["nodes"] = OrderedDict()

    stats["nodes"]["total"] = 0
    stats["nodes"]["idle"] = 0
    stats["nodes"]["allocated"] = 0
    stats["nodes"]["down"] = 0
    stats["nodes"]["available"] = 0

    stats["cpu"] = OrderedDict()

    stats["cpu"]["total"] = 0
    stats["cpu"]["allocated"] = 0
    stats["cpu"]["idle"] = 0

    # ========================================================
    # Process each compute node exactly once.
    # ========================================================

    for node in nodes:

        stats["nodes"]["total"] += 1

        state = node.get(
            "State",
            ""
        ).lower()

        state_tokens = set(
            token.strip().lower()
            for token in state.split("+")
        )

        is_down = (
            "down" in state_tokens
            or
            "drain" in state_tokens
            or
            "fail" in state_tokens
            or
            "unknown" in state_tokens
        )

        # ----------------------------------------------------
        # Node state.
        # ----------------------------------------------------

        if "mixed" in state_tokens:

            stats["nodes"]["allocated"] += 1

        elif "allocated" in state_tokens:

            stats["nodes"]["allocated"] += 1

        elif "alloc" in state_tokens:

            stats["nodes"]["allocated"] += 1

        elif "idle" in state_tokens and not is_down:

            stats["nodes"]["idle"] += 1

        elif is_down:

            stats["nodes"]["down"] += 1

        # ----------------------------------------------------
        # Available.
        # ----------------------------------------------------

        if not is_down:

            stats["nodes"]["available"] += 1

        # ----------------------------------------------------
        # CPU totals.
        # ----------------------------------------------------

        cpu_total = parse_number(
            node.get(
                "CPUTot",
                "0"
            )
        )

        cpu_allocated = parse_number(
            node.get(
                "CPUAlloc",
                "0"
            )
        )

        stats["cpu"]["total"] += (
            cpu_total
        )

        stats["cpu"]["allocated"] += (
            cpu_allocated
        )

    # --------------------------------------------------------
    # CPU idle.
    # --------------------------------------------------------

    stats["cpu"]["idle"] = max(
        0,
        stats["cpu"]["total"]
        -
        stats["cpu"]["allocated"]
    )

    return stats


# ============================================================
# GPU statistics
# ============================================================

def get_gpu_stats(nodes):
    """
    Calculate GPU statistics from compute nodes.

    GPU totals:
        Gres=

    GPU allocation:
        GresUsed=

    Fallback:
        AllocTRES=

    Generic GPU allocations are resolved using the GPU type
    configured in Gres on the same physical node.
    """

    gpu = OrderedDict()

    gpu["total"] = 0
    gpu["allocated"] = 0
    gpu["idle"] = 0

    gpu["types"] = OrderedDict()

    # ========================================================
    # Process each compute node.
    # ========================================================

    for node in nodes:

        node_name = node.get(
            "NodeName",
            "unknown"
        )

        gres = node.get(
            "Gres",
            ""
        )

        # ----------------------------------------------------
        # Only GPU nodes.
        # ----------------------------------------------------

        if not is_gpu_node(
            gres
        ):
            continue

        # ----------------------------------------------------
        # Total GPU configuration.
        # ----------------------------------------------------

        total_gpu_info = parse_gpu_gres(
            gres
        )

        # ----------------------------------------------------
        # GPU allocation.
        #
        # Preferred source:
        #
        #     GresUsed=
        # ----------------------------------------------------

        allocated_gpu_info = parse_gpu_gres(
            node.get(
                "GresUsed",
                ""
            )
        )

        # ----------------------------------------------------
        # Fallback to AllocTRES.
        # ----------------------------------------------------

        if not allocated_gpu_info:

            allocated_gpu_info = parse_gpu_tres(
                node.get(
                    "AllocTRES",
                    ""
                )
            )

        # ----------------------------------------------------
        # Resolve generic allocation based on this node's
        # configured GPU type.
        # ----------------------------------------------------

        allocated_gpu_info = (
            resolve_gpu_allocation(
                total_gpu_info,
                allocated_gpu_info
            )
        )

        # ====================================================
        # Add total GPU count.
        # ====================================================

        for gpu_type, count in (
            total_gpu_info.items()
        ):

            gpu_type = normalize_gpu_type(
                gpu_type
            )

            if gpu_type not in gpu["types"]:

                gpu["types"][gpu_type] = (
                    OrderedDict()
                )

                gpu["types"][gpu_type]["total"] = 0
                gpu["types"][gpu_type]["allocated"] = 0
                gpu["types"][gpu_type]["idle"] = 0

            gpu["types"][gpu_type]["total"] += (
                count
            )

            gpu["total"] += count

        # ====================================================
        # Add allocated GPU count.
        # ====================================================

        for gpu_type, count in (
            allocated_gpu_info.items()
        ):

            gpu_type = normalize_gpu_type(
                gpu_type
            )

            if gpu_type not in gpu["types"]:

                gpu["types"][gpu_type] = (
                    OrderedDict()
                )

                gpu["types"][gpu_type]["total"] = 0
                gpu["types"][gpu_type]["allocated"] = 0
                gpu["types"][gpu_type]["idle"] = 0

            gpu["types"][gpu_type]["allocated"] += (
                count
            )

            gpu["allocated"] += count

        # ----------------------------------------------------
        # Sanity check.
        # ----------------------------------------------------

        total_node_gpus = sum(
            total_gpu_info.values()
        )

        allocated_node_gpus = sum(
            allocated_gpu_info.values()
        )

        if allocated_node_gpus > total_node_gpus:

            sys.stderr.write(
                "WARNING: node {} reports {} allocated "
                "GPUs but only {} total GPUs\n".format(
                    node_name,
                    allocated_node_gpus,
                    total_node_gpus
                )
            )

    # ========================================================
    # Calculate idle GPUs per type.
    # ========================================================

    for gpu_type in gpu["types"]:

        data = gpu["types"][gpu_type]

        data["idle"] = max(
            0,
            data["total"]
            -
            data["allocated"]
        )

    # ========================================================
    # Calculate overall idle GPUs.
    # ========================================================

    gpu["idle"] = max(
        0,
        gpu["total"]
        -
        gpu["allocated"]
    )

    return gpu


# ============================================================
# Job statistics
# ============================================================

def get_job_stats():
    """
    Get job counts.

    -a includes all partitions.
    """

    # --------------------------------------------------------
    # ALL jobs
    # --------------------------------------------------------

    total_output = run_command([
        "squeue",
        "-a",
        "-h",
        "-o",
        "%i"
    ])

    # --------------------------------------------------------
    # RUNNING jobs
    # --------------------------------------------------------

    running_output = run_command([
        "squeue",
        "-a",
        "-h",
        "-t",
        "RUNNING",
        "-o",
        "%i"
    ])

    # --------------------------------------------------------
    # PENDING jobs
    # --------------------------------------------------------

    pending_output = run_command([
        "squeue",
        "-a",
        "-h",
        "-t",
        "PENDING",
        "-o",
        "%i"
    ])

    jobs = OrderedDict()

    jobs["total"] = count_lines(
        total_output
    )

    jobs["running"] = count_lines(
        running_output
    )

    jobs["pending"] = count_lines(
        pending_output
    )

    return jobs


# ============================================================
# Collect statistics
# ============================================================

def collect_stats():

    # --------------------------------------------------------
    # Get all physical nodes, then filter to compute nodes.
    # --------------------------------------------------------

    nodes = get_node_info()

    # --------------------------------------------------------
    # Node / CPU statistics.
    # --------------------------------------------------------

    node_stats = get_node_stats(
        nodes
    )

    # --------------------------------------------------------
    # GPU statistics.
    # --------------------------------------------------------

    gpu_stats = get_gpu_stats(
        nodes
    )

    # --------------------------------------------------------
    # Job statistics.
    # --------------------------------------------------------

    job_stats = get_job_stats()

    # --------------------------------------------------------
    # Final result.
    # --------------------------------------------------------

    stats = OrderedDict()

    stats["timestamp"] = (
        datetime.datetime.now(
            datetime.timezone.utc
        ).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )

    stats["nodes"] = (
        node_stats["nodes"]
    )

    stats["cpu"] = (
        node_stats["cpu"]
    )

    stats["gpu"] = gpu_stats

    stats["jobs"] = job_stats

    return stats


# ============================================================
# Write YAML
# ============================================================

def write_yaml(
    data,
    filename
):
    """
    Write statistics to YAML.
    """

    with open(
        filename,
        "w"
    ) as output:

        yaml.safe_dump(
            data,
            output,
            default_flow_style=False
        )


# ============================================================
# Main
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Collect Slurm cluster statistics "
            "for compute nodes and write them to YAML."
        )
    )

    parser.add_argument(
        "-o",
        "--output",
        default="slurm_stats.yaml",
        help=(
            "Output YAML file "
            "(default: slurm_stats.yaml)"
        )
    )

    args = parser.parse_args()

    try:

        stats = collect_stats()

        write_yaml(
            stats,
            args.output
        )

        print(
            "Slurm statistics written to {}".format(
                args.output
            )
        )

    except RuntimeError as exc:

        print(
            "ERROR: {}".format(exc),
            file=sys.stderr
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
