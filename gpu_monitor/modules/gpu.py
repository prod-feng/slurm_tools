# -*- coding: utf-8 -*-

"""
Fast GPU collection and utilization analysis.

Python 3.6 compatible.

The collector intentionally performs a single pdsh invocation for all
requested nodes.  This avoids the very expensive pattern of starting
one pdsh/SSH process per node.

Expected pdsh output:

    node1: 0, NVIDIA H200, 00000000:01:00.0, 35, 1000, 81920, 75, 20
    node1: 1, NVIDIA H200, 00000000:02:00.0, 36, 1200, 81920, 80, 25
    node2: 0, NVIDIA H200, ...

"""

from __future__ import print_function

import logging

from .config import Config
from .utils import run_command


LOG = logging.getLogger("gpu_monitor.gpu")


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

    def __init__(self, config=None):

        self.config = config or Config

        self.pdsh = getattr(
            self.config,
            "PDSH",
            "pdsh"
        )

        self.nvidia_smi = getattr(
            self.config,
            "NVIDIA_SMI",
            "nvidia-smi"
        )

        self.timeout = getattr(
            self.config,
            "GPU_TIMEOUT",
            30
        )

        # pdsh has a few optional settings that can substantially reduce
        # startup/connection overhead on clusters with many nodes.
        self.pdsh_args = getattr(
            self.config,
            "PDSH_ARGS",
            []
        )

        if self.pdsh_args is None:
            self.pdsh_args = []

    # ==================================================================
    # Command
    # ==================================================================

    def _build_command(self, nodes):

        node_list = ",".join(nodes)

        command = [
            self.pdsh,
            "-w",
            node_list
        ]

        # Optional site-specific pdsh arguments.
        #
        # For example Config.PDSH_ARGS could contain:
        #
        #     ["-f", "32"]
        #
        # to limit pdsh fanout.
        command.extend(
            self.pdsh_args
        )

        command.extend([
            self.nvidia_smi,
            "--query-gpu={}".format(
                self.QUERY_FIELDS
            ),
            "--format=csv,noheader,nounits"
        ])

        return command

    # ==================================================================
    # Number parsing
    # ==================================================================

    @staticmethod
    def _int(value, default=0):

        try:

            return int(
                str(value).strip()
            )

        except (
            ValueError,
            TypeError
        ):

            return default

    # ==================================================================
    # Parse one GPU payload
    # ==================================================================

    @classmethod
    def _parse_payload(
            cls,
            node,
            payload,
            gpu_data):

        fields = [
            item.strip()
            for item in payload.split(",")
        ]

        if len(fields) != 8:

            LOG.debug(
                "Malformed GPU record on %s: %s",
                node,
                payload
            )

            return

        index = cls._int(
            fields[0],
            -1
        )

        if index < 0:

            LOG.debug(
                "Invalid GPU index on %s: %s",
                node,
                payload
            )

            return

        gpu_data[index] = {
            "index": index,
            "name": fields[1],
            "bus_id": fields[2],
            "temperature": cls._int(
                fields[3]
            ),
            "memory_used": cls._int(
                fields[4]
            ),
            "memory_total": cls._int(
                fields[5]
            ),
            "utilization": cls._int(
                fields[6]
            ),
            "memory_utilization": cls._int(
                fields[7]
            ),
        }

    # ==================================================================
    # Parse complete pdsh output
    # ==================================================================

    def _parse_output(
            self,
            nodes,
            output):

        result = {}

        # Exact node lookup is much faster than repeatedly scanning
        # the complete node list.
        known_nodes = {}

        for node in nodes:

            known_nodes[node] = node

            short_node = node.split(
                ".",
                1
            )[0]

            known_nodes.setdefault(
                short_node,
                node
            )

        for line in output.splitlines():

            line = line.strip()

            if not line:
                continue

            # pdsh output normally looks like:
            #
            # node: payload
            #
            if ":" not in line:

                LOG.debug(
                    "Ignoring unexpected pdsh output: %s",
                    line
                )

                continue

            node, payload = line.split(
                ":",
                1
            )

            node = node.strip()
            payload = payload.strip()

            target_node = known_nodes.get(
                node
            )

            if target_node is None:

                target_node = known_nodes.get(
                    node.split(
                        ".",
                        1
                    )[0]
                )

            if target_node is None:

                LOG.debug(
                    "Ignoring telemetry from unknown node %s",
                    node
                )

                continue

            if target_node not in result:

                result[target_node] = {}

            self._parse_payload(
                target_node,
                payload,
                result[target_node]
            )

        # Ensure every requested node has an entry.
        for node in nodes:

            result.setdefault(
                node,
                {}
            )

        return result

    # ==================================================================
    # Collect one node
    # ==================================================================

    def collect_node(self, node):

        """
        Compatibility method.

        Prefer collect_nodes() whenever possible because it performs
        one pdsh call for multiple nodes.
        """

        result = self.collect_nodes(
            [node]
        )

        return result.get(
            node,
            {}
        )

    # ==================================================================
    # Collect multiple nodes
    # ==================================================================

    def collect_nodes(self, nodes):

        """
        Collect GPU telemetry from all requested nodes.

        This is deliberately a single pdsh invocation.

        Example:

            pdsh -w node1,node2,node3 nvidia-smi ...

        instead of:

            pdsh -w node1 nvidia-smi ...
            pdsh -w node2 nvidia-smi ...
            pdsh -w node3 nvidia-smi ...
        """

        # Materialize once because callers may pass dict_keys,
        # generators, etc.
        nodes = sorted(
            set(
                str(node).strip()
                for node in nodes
                if node and str(node).strip()
            )
        )

        if not nodes:

            return {}

        command = self._build_command(
            nodes
        )

        LOG.info(
            "Collecting GPU telemetry from %d nodes",
            len(nodes)
        )

        LOG.debug(
            "GPU command: %s",
            " ".join(
                str(x)
                for x in command
            )
        )

        try:

            output = run_command(
                command,
                timeout=self.timeout
            )

        except Exception as exc:

            LOG.error(
                "GPU collection failed: %s",
                exc
            )

            # IMPORTANT:
            #
            # Do NOT fall back to one command per node here.
            #
            # That fallback can turn a temporary pdsh problem into a
            # very slow monitor run.
            #
            # The next monitor invocation will try again.

            return dict(
                (
                    node,
                    {}
                )
                for node in nodes
            )

        result = self._parse_output(
            nodes,
            output
        )

        total_gpus = sum(
            len(data)
            for data in result.values()
        )

        LOG.info(
            "GPU telemetry complete: nodes=%d GPUs=%d",
            len(nodes),
            total_gpus
        )

        return result


# ======================================================================
# GPU metrics
# ======================================================================

def calculate_metrics(
        job,
        node_gpu_data,
        idle_threshold=None):

    if idle_threshold is None:

        idle_threshold = (
            Config.IDLE_GPU_THRESHOLD
        )

    node = job["nodes"]

    gpu_indexes = job["gpu_indexes"]

    if not gpu_indexes:

        return None

    node_data = node_gpu_data.get(
        node
    )

    if not node_data:

        LOG.debug(
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

            LOG.debug(
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

    return {
        "node": node,

        "gpu_count": len(
            selected
        ),

        "gpu_indexes": [
            gpu["index"]
            for gpu in selected
        ],

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
            sum(
                1
                for value in utilization
                if value < idle_threshold
            ),
    }


def classify_utilization(
        metrics,
        low_threshold=None):

    if low_threshold is None:

        low_threshold = (
            Config.LOW_UTILIZATION_THRESHOLD
        )

    if (
        metrics["average_utilization"]
        < low_threshold
    ):

        return "LOW_UTILIZATION"

    if metrics["idle_gpu_count"] > 0:

        return "IMBALANCED"

    return "NORMAL"

