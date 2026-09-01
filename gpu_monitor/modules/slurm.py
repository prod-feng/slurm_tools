#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Slurm interface for GPU monitor and GPU job reporting.

Python 3.6 compatible.

This module provides:

    get_running_jobs()
        Used by gpu_job_monitor.py

    get_completed_jobs()
        Used by gpu_job_report.py

Historical sacct records are intentionally retrieved WITHOUT
-X / --allocations so that records such as:

    85511
    85511.batch
    85511.extern
    85511.0
    85511.1

are available to the reporting/aggregation layer.
"""

from __future__ import print_function

import logging
import re

from .utils import run_command


LOG = logging.getLogger(
    "gpu_monitor.slurm"
)


class SlurmClient(object):

    def __init__(self, config=None):

        if config is None:

            from .config import Config

            config = Config

        self.config = config

        self.sacct = getattr(
            config,
            "SACCT",
            "sacct"
        )

        self.squeue = getattr(
            config,
            "SQUEUE",
            "squeue"
        )

        self.scontrol = getattr(
            config,
            "SCONTROL",
            "scontrol"
        )

        self.slurm_timeout = getattr(
            config,
            "SLURM_TIMEOUT",
            60
        )

    # ==================================================================
    # Generic helpers
    # ==================================================================

    @staticmethod
    def _parse_tres(value):

        result = {}

        if value is None:
            return result

        text = str(value).strip()

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

    @staticmethod
    def _number(value, default=0):

        if value is None:
            return default

        text = str(value).strip()

        if not text:
            return default

        match = re.match(
            r"^([-+]?[0-9]+(?:\.[0-9]+)?)",
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

    @staticmethod
    def _parse_runtime(value):

        """
        Convert Slurm elapsed time into seconds.

        Supported:

            HH:MM:SS
            MM:SS
            D-HH:MM:SS
        """

        if not value:
            return 0

        text = str(value).strip()

        if not text:
            return 0

        # Remove fractional seconds.
        text = text.split(
            ".",
            1
        )[0]

        days = 0

        if "-" in text:

            day_text, text = text.split(
                "-",
                1
            )

            try:

                days = int(
                    day_text
                )

            except ValueError:

                return 0

        parts = text.split(":")

        try:

            if len(parts) == 3:

                return (
                    days * 86400
                    + int(parts[0]) * 3600
                    + int(parts[1]) * 60
                    + int(parts[2])
                )

            if len(parts) == 2:

                return (
                    days * 86400
                    + int(parts[0]) * 60
                    + int(parts[1])
                )

        except ValueError:

            return 0

        return 0

    # ==================================================================
    # GPU allocation parsing
    # ==================================================================

    @classmethod
    def get_gpu_count(cls, alloc_tres):

        tres = cls._parse_tres(
            alloc_tres
        )

        # --------------------------------------------------------------
        # Generic GPU TRES.
        #
        # Example:
        #
        #   gres/gpu=4
        # --------------------------------------------------------------

        if "gres/gpu" in tres:

            count = int(
                cls._number(
                    tres["gres/gpu"]
                )
            )

            if count > 0:
                return count

        # --------------------------------------------------------------
        # Typed GPU TRES.
        #
        # Example:
        #
        #   gres/gpu:h200=4
        # --------------------------------------------------------------

        total = 0

        for key, value in tres.items():

            if key.startswith(
                    "gres/gpu:"
            ):

                total += int(
                    cls._number(
                        value
                    )
                )

        return total

    @classmethod
    def get_gpu_type(cls, alloc_tres):

        tres = cls._parse_tres(
            alloc_tres
        )

        for key in tres:

            if key.startswith(
                    "gres/gpu:"
            ):

                return key.split(
                    ":",
                    1
                )[1]

        return ""

    @classmethod
    def get_gpu_utilization(cls, tres_usage):

        tres = cls._parse_tres(
            tres_usage
        )

        return cls._number(
            tres.get(
                "gres/gpuutil",
                0
            )
        )

    @classmethod
    def _parse_gpu_indexes(cls, gres):

        """
        Parse GPU information from squeue GRES.

        Examples:

            gpu:2
            gpu:h200:2
            gpu:0,1
            gpu:h200:0,1
            gpu:h200(IDX:0,1)

        Returns GPU indexes when explicit indexes are available.

        For count-only allocations, indexes are assigned sequentially
        starting from zero.
        """

        if not gres:
            return []

        text = str(gres).strip()

        if not text:
            return []

        indexes = []

        # --------------------------------------------------------------
        # Explicit IDX syntax.
        #
        # Examples:
        #
        #   IDX:0,1
        #   IDX=0,1
        #   IDX:0-3
        # --------------------------------------------------------------

        match = re.search(
            r"IDX[:=]([0-9,\-]+)",
            text,
            re.IGNORECASE
        )

        if match:

            value = match.group(1)

            for part in value.split(","):

                part = part.strip()

                if not part:
                    continue

                if "-" in part:

                    left, right = part.split(
                        "-",
                        1
                    )

                    try:

                        left = int(left)
                        right = int(right)

                        for index in range(
                                left,
                                right + 1):

                            indexes.append(
                                index
                            )

                    except ValueError:

                        pass

                else:

                    try:

                        indexes.append(
                            int(part)
                        )

                    except ValueError:

                        pass

            if indexes:

                return sorted(
                    set(indexes)
                )

        # --------------------------------------------------------------
        # gpu:0,1
        # gpu:h200:0,1
        # --------------------------------------------------------------

        match = re.search(
            r"gpu(?::[^,()]+)?:"
            r"([0-9]+(?:,[0-9]+)*)",
            text,
            re.IGNORECASE
        )

        if match:

            for value in match.group(1).split(","):

                try:

                    indexes.append(
                        int(value)
                    )

                except ValueError:

                    pass

            if indexes:

                return sorted(
                    set(indexes)
                )

        # --------------------------------------------------------------
        # Count-only GPU allocation.
        #
        # gpu:2
        # gpu:h200:2
        # --------------------------------------------------------------

        match = re.search(
            r"gpu(?::[^:,()]+)?(?::([0-9]+))?",
            text,
            re.IGNORECASE
        )

        if match:

            count_text = match.group(1)

            if count_text:

                try:

                    count = int(
                        count_text
                    )

                except ValueError:

                    count = 0

                if count > 0:

                    return list(
                        range(count)
                    )

        # --------------------------------------------------------------
        # Last fallback.
        # --------------------------------------------------------------

        match = re.search(
            r"gpu[:=]([0-9]+)",
            text,
            re.IGNORECASE
        )

        if match:

            try:

                count = int(
                    match.group(1)
                )

            except ValueError:

                count = 0

            if count > 0:

                return list(
                    range(count)
                )

        return []

    # ==================================================================
    # Running jobs
    # ==================================================================

    def get_running_jobs(self):

        """
        Return currently running GPU jobs.

        This is used by gpu_job_monitor.py.

        Uses squeue because sacct is intended for accounting/history.
        """

        command = [
            self.squeue,
            "-h",
            "-t",
            "RUNNING",
            "-o",
            "%u|%A|%j|%T|%M|%D|%N|%b|%C|%m|%P"
        ]

        LOG.debug(
            "Executing squeue command: %s",
            " ".join(command)
        )

        try:

            output = run_command(
                command,
                timeout=self.slurm_timeout
            )

        except Exception as exc:

            LOG.error(
                "squeue failed: %s",
                exc
            )

            return []

        jobs = []

        for line in output.splitlines():

            line = line.rstrip("\n")

            if not line.strip():
                continue

            fields = line.split("|")

            if len(fields) < 11:

                LOG.warning(
                    "Malformed squeue record with %d fields: %s",
                    len(fields),
                    line
                )

                continue

            (
                user,
                job_id,
                job_name,
                state,
                elapsed,
                nnodes,
                nodelist,
                tres_per_node,
                ncpus,
                memory,
                partition
            ) = fields[:11]

            job_id = job_id.strip()

            if not job_id:
                continue

            try:

                nnodes_int = int(
                    self._number(
                        nnodes,
                        1
                    )
                )

            except Exception:

                nnodes_int = 1

            try:

                ncpus_int = int(
                    self._number(
                        ncpus,
                        0
                    )
                )

            except Exception:

                ncpus_int = 0

            gpu_indexes = (
                self._parse_gpu_indexes(
                    tres_per_node
                )
            )

            gpu_count = len(
                gpu_indexes
            )

            if gpu_count == 0:

                gpu_count = (
                    self._gpu_count_from_squeue(
                        tres_per_node
                    )
                )

                if gpu_count > 0:

                    gpu_indexes = list(
                        range(gpu_count)
                    )

            # ----------------------------------------------------------
            # scontrol fallback.
            # ----------------------------------------------------------

            if gpu_count == 0:

                details = self._get_job_scontrol(
                    job_id
                )

                if details:

                    extra_gres = details.get(
                        "gres",
                        ""
                    )

                    if extra_gres:

                        gpu_indexes = (
                            self._parse_gpu_indexes(
                                extra_gres
                            )
                        )

                        gpu_count = len(
                            gpu_indexes
                        )

                        if gpu_count == 0:

                            gpu_count = (
                                self._gpu_count_from_squeue(
                                    extra_gres
                                )
                            )

                            if gpu_count > 0:

                                gpu_indexes = list(
                                    range(gpu_count)
                                )

            if gpu_count == 0:

                LOG.debug(
                    "Skipping non-GPU running job "
                    "%s user=%s gres=%s",
                    job_id,
                    user,
                    tres_per_node
                )

                continue

            node = nodelist.strip()

            # ----------------------------------------------------------
            # Current GPU telemetry operates on one node.
            # ----------------------------------------------------------

            if "," in node:

                LOG.debug(
                    "Job %s has multiple nodes: %s",
                    job_id,
                    node
                )

            jobs.append({
                "user": user.strip(),

                "job_id": job_id,

                "job_name": job_name.strip(),

                "state": state.strip(),

                "elapsed": elapsed.strip(),

                "runtime": self._parse_runtime(
                    elapsed
                ),

                "nnodes": nnodes_int,

                "nodes": node,

                "nodelist": node,

                "ncpus": ncpus_int,

                "memory": memory.strip(),

                "req_mem": memory.strip(),

                "partition": partition.strip(),

                "tres_per_node": tres_per_node.strip(),

                "gpu_count": gpu_count,

                "gpu_indexes": gpu_indexes,
            })

        LOG.info(
            "Parsed %d running GPU jobs",
            len(jobs)
        )

        for job in jobs:

            LOG.debug(
                "Running GPU job %s user=%s node=%s "
                "gpu_count=%d gpu_indexes=%s runtime=%d",
                job["job_id"],
                job["user"],
                job["nodes"],
                job["gpu_count"],
                job["gpu_indexes"],
                job["runtime"]
            )

        return jobs

    @classmethod
    def _gpu_count_from_squeue(cls, value):

        if not value:
            return 0

        text = str(value).strip()

        # --------------------------------------------------------------
        # gpu:h200:2
        # --------------------------------------------------------------

        match = re.search(
            r"gpu:[^:,]+:([0-9]+)",
            text,
            re.IGNORECASE
        )

        if match:

            return int(
                match.group(1)
            )

        # --------------------------------------------------------------
        # gpu:2
        # --------------------------------------------------------------

        match = re.search(
            r"gpu:([0-9]+)",
            text,
            re.IGNORECASE
        )

        if match:

            return int(
                match.group(1)
            )

        # --------------------------------------------------------------
        # Plain gpu
        # --------------------------------------------------------------

        if re.search(
                r"(?:^|,)gpu(?:,|$)",
                text,
                re.IGNORECASE):

            return 1

        return 0

    # ==================================================================
    # scontrol fallback
    # ==================================================================

    def _get_job_scontrol(self, job_id):

        command = [
            self.scontrol,
            "show",
            "job",
            str(job_id)
        ]

        LOG.debug(
            "Executing scontrol command: %s",
            " ".join(command)
        )

        try:

            output = run_command(
                command,
                timeout=self.slurm_timeout
            )

        except Exception as exc:

            LOG.debug(
                "scontrol failed for job %s: %s",
                job_id,
                exc
            )

            return {}

        result = {}

        for token in output.replace(
                "\n",
                " "
        ).split():

            if "=" not in token:
                continue

            key, value = token.split(
                "=",
                1
            )

            result[key.lower()] = value

        return result

    # ==================================================================
    # Job details compatibility
    # ==================================================================

    def get_job_details(self):

        """
        Compatibility method for older monitor code.

        get_running_jobs() already returns the data currently required.
        """

        return {}

    def merge_job_details(
            self,
            jobs,
            details):

        if not jobs:

            return []

        return jobs

    # ==================================================================
    # Historical / completed jobs
    # ==================================================================

    def get_completed_jobs(
            self,
            users=None,
            all_users=False,
            jobs=None,
            start=None,
            end=None,
            states=None):

        """
        Retrieve historical Slurm accounting records.

        IMPORTANT:

        We intentionally DO NOT use:

            -X
            --allocations

        because the report needs to see both primary allocations
        and job steps.

        Example records:

            85511
            85511.batch
            85511.extern
            85511.0
            85511.1

        aggregate_jobs() later combines these records into the primary
        job.

        Parameters:

            users:
                Optional list of usernames.

            all_users:
                Request accounting records for all users.

            jobs:
                Optional list of JobIDs.

            start:
                Start time passed to sacct.

            end:
                End time passed to sacct.

            states:
                Optional Slurm state filter.
        """

        format_fields = [
            "User",
            "JobID",
            "JobName",
            "Partition",
            "State",
            "Start",
            "Elapsed",
            "NodeList",
            "AllocCPUS",
            "ReqMem",
            "MaxRSS",
            "TotalCPU",
            "SystemCPU",
            "UserCPU",
            "AllocTRES",
            "TRESUsageInAve",
            "NNodes"
        ]

        command = [
            self.sacct,

            # No header.
            "-n",

            # Pipe separated output.
            "-P",
        ]

        # --------------------------------------------------------------
        # Time range.
        # --------------------------------------------------------------

        if start:

            command.extend([
                "--starttime",
                str(start)
            ])

        else:

            command.extend([
                "--starttime",
                "today-1day"
            ])

        if end:

            command.extend([
                "--endtime",
                str(end)
            ])

        else:

            command.extend([
                "--endtime",
                "now"
            ])

        # --------------------------------------------------------------
        # User filtering.
        # --------------------------------------------------------------

        if users and not all_users:

            valid_users = [
                str(user).strip()
                for user in users
                if str(user).strip()
            ]

            if valid_users:

                command.extend([
                    "--user",
                    ",".join(
                        valid_users
                    )
                ])

        elif all_users:

            command.append(
                "--allusers"
            )

        # --------------------------------------------------------------
        # Job filtering.
        # --------------------------------------------------------------

        if jobs:

            valid_jobs = [
                str(job).strip()
                for job in jobs
                if str(job).strip()
            ]

            if valid_jobs:

                command.extend([
                    "--jobs",
                    ",".join(
                        valid_jobs
                    )
                ])

        # --------------------------------------------------------------
        # State filtering.
        # --------------------------------------------------------------

        if states:

            valid_states = [
                str(state).strip()
                for state in states
                if str(state).strip()
            ]

            if valid_states:

                command.extend([
                    "--state",
                    ",".join(
                        valid_states
                    )
                ])

        # --------------------------------------------------------------
        # Fields.
        # --------------------------------------------------------------

        command.extend([
            "--format",
            ",".join(
                format_fields
            )
        ])

        LOG.info(
            "Executing sacct historical query"
        )

        LOG.debug(
            "sacct command: %s",
            " ".join(command)
        )

        try:

            output = run_command(
                command,
                timeout=self.slurm_timeout
            )

        except Exception as exc:

            LOG.error(
                "sacct failed: %s",
                exc
            )

            return []

        records = []

        primary_count = 0
        step_count = 0

        # --------------------------------------------------------------
        # Parse output.
        # --------------------------------------------------------------

        for raw_line in output.splitlines():

            line = raw_line.rstrip("\r\n")

            if not line.strip():
                continue

            fields_value = line.split("|")

            # ----------------------------------------------------------
            # We requested 17 fields.
            #
            # Be tolerant of additional fields.
            # ----------------------------------------------------------

            if len(fields_value) < 17:

                LOG.warning(
                    "Malformed sacct record with %d fields: %s",
                    len(fields_value),
                    line
                )

                continue

            (
                user,
                job_id,
                job_name,
                partition,
                state,
                start_value,
                elapsed,
                nodelist,
                ncpus,
                req_mem,
                max_rss,
                total_cpu,
                system_cpu,
                user_cpu,
                alloc_tres,
                tres_usage,
                nnodes
            ) = fields_value[:17]

            user = user.strip()
            job_id = job_id.strip()

            # ----------------------------------------------------------
            # Ignore empty JobID records.
            # ----------------------------------------------------------

            if not job_id:
                continue

            record = {
                "user": user,

                "job_id": job_id,

                "job_name": job_name.strip(),

                "partition": partition.strip(),

                "state": state.strip(),

                "start": start_value.strip(),

                "elapsed": elapsed.strip(),

                "nodelist": nodelist.strip(),

                "ncpus": ncpus.strip(),

                "req_mem": req_mem.strip(),

                "max_rss": max_rss.strip(),

                "total_cpu": total_cpu.strip(),

                "system_cpu": system_cpu.strip(),

                "user_cpu": user_cpu.strip(),

                "alloc_tres": alloc_tres.strip(),

                "tres_usage": tres_usage.strip(),

                "nnodes": nnodes.strip(),
            }

            # ----------------------------------------------------------
            # Identify primary allocation vs step.
            #
            # 85511
            #     primary
            #
            # 85511.batch
            #     step
            #
            # 85511.extern
            #     step
            #
            # 85511.0
            #     step
            # ----------------------------------------------------------

            if "." in job_id:

                record["is_primary"] = False

                record["primary_job_id"] = (
                    job_id.split(
                        ".",
                        1
                    )[0]
                )

                step_count += 1

            else:

                record["is_primary"] = True

                record["primary_job_id"] = job_id

                primary_count += 1

            records.append(
                record
            )

        LOG.info(
            "Parsed %d sacct records",
            len(records)
        )

        LOG.info(
            "sacct records: primary=%d steps=%d",
            primary_count,
            step_count
        )

        # --------------------------------------------------------------
        # Diagnostic fallback.
        #
        # If our full query produces zero records, perform a much
        # simpler query. This tells us whether the accounting database
        # itself is returning data.
        # --------------------------------------------------------------

        if not records:

            LOG.warning(
                "Primary sacct query returned no records."
            )

            fallback_command = [
                self.sacct,
                "-n",
                "-P",
            ]

            if start:

                fallback_command.extend([
                    "--starttime",
                    str(start)
                ])

            else:

                fallback_command.extend([
                    "--starttime",
                    "today-1day"
                ])

            if end:

                fallback_command.extend([
                    "--endtime",
                    str(end)
                ])

            else:

                fallback_command.extend([
                    "--endtime",
                    "now"
                ])

            if all_users:

                fallback_command.append(
                    "--allusers"
                )

            elif users:

                valid_users = [
                    str(user).strip()
                    for user in users
                    if str(user).strip()
                ]

                if valid_users:

                    fallback_command.extend([
                        "--user",
                        ",".join(
                            valid_users
                        )
                    ])

            fallback_command.extend([
                "--format",
                "User,JobID,JobName,State,Start,Elapsed,AllocTRES"
            ])

            LOG.warning(
                "Running simplified sacct diagnostic query: %s",
                " ".join(
                    fallback_command
                )
            )

            try:

                fallback_output = run_command(
                    fallback_command,
                    timeout=self.slurm_timeout
                )

                fallback_lines = [
                    line
                    for line in fallback_output.splitlines()
                    if line.strip()
                ]

                LOG.warning(
                    "Simplified sacct query returned %d lines",
                    len(fallback_lines)
                )

                for line in fallback_lines[:20]:

                    LOG.warning(
                        "sacct diagnostic: %s",
                        line
                    )

            except Exception as exc:

                LOG.error(
                    "Simplified sacct diagnostic failed: %s",
                    exc
                )

        return records

