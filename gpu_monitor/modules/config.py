# -*- coding: utf-8 -*-

"""
Central configuration for the Slurm/GPU monitoring tools.

Python 3.6 compatible.
"""

from __future__ import print_function

import os


class Config(object):

    # ------------------------------------------------------------------
    # Cluster commands
    # ------------------------------------------------------------------

    SLURM_BIN = "/cm/shared/apps/slurm/current/bin"

    SQUEUE = os.path.join(
        SLURM_BIN,
        "squeue"
    )

    SCONTROL = os.path.join(
        SLURM_BIN,
        "scontrol"
    )

    SACCT = os.path.join(
        SLURM_BIN,
        "sacct"
    )

    SACCTMGR = os.path.join(
        SLURM_BIN,
        "sacctmgr"
    )

    PDSH = "pdsh"

    NVIDIA_SMI = "nvidia-smi"

    MAIL = "mail"

    GETENT = "getent"

    # ------------------------------------------------------------------
    # Working files
    # ------------------------------------------------------------------

    WORKDIR = "/tmp/feng/slurm_tools"

    STATE_FILE = os.path.join(
        WORKDIR,
        "jobstats.json"
    )

    LOCK_FILE = os.path.join(
        WORKDIR,
        "jobstats.lock"
    )

    # Script 2 priority tracking file.
    GPU_USERS_FILE = (
        "/tmp/"
        "gpu_utilization_monitor/gpujobs.csv"
    )

    # ------------------------------------------------------------------
    # GPU monitor policy
    # ------------------------------------------------------------------

    MIN_RUNTIME_SECONDS = 30 * 60

    IDLE_GPU_THRESHOLD = 1

    LOW_UTILIZATION_THRESHOLD = 20

    REQUIRED_BAD_SAMPLES = 6

    # ------------------------------------------------------------------
    # Email policy
    # ------------------------------------------------------------------

    MAX_EMAILS_PER_DAY = 2

    EMAIL_INTERVAL_SECONDS = (
        24 * 60 * 60 /
        float(MAX_EMAILS_PER_DAY)
    )

    MAIL_FROM = "hpc_support@xx.yy"

    MAIL_CC = ""

    MAIL_BCC = ""

    SUPPORT_URL = (
        "https://support.xx.yy/"
    )

    EXCLUDED_USERS = set([
        "feng",
    ])

    # ------------------------------------------------------------------
    # Timeouts
    # ------------------------------------------------------------------

    SLURM_TIMEOUT = 120

    GPU_TIMEOUT = 60

    MAIL_TIMEOUT = 30

    GETENT_TIMEOUT = 30

    # ------------------------------------------------------------------
    # Script 2
    # ------------------------------------------------------------------

    DEFAULT_REPORT_DAYS = 1

    DEFAULT_NJOBS = 100

    DEFAULT_STATE = "CD"

    REPORT_PARTITIONS = (
        "b40x4,"
        "b40x4-long,"
        "h200x4,"
        "h200x4-long,"
        "h200x8,"
        "h200x8-long,"
        "p-b40x4,"
        "p-b40x4-long,"
        "p-h200x4,"
        "p-h200x4-long,"
        "p-h200x8,"
        "p-h200x8-03-long,"
        "p-h200x8-long"
    )

    IGNORED_GPU_PARTITION_SUBSTRINGS = (
        "a100",
    )

    PRIORITY_EXCLUDED_USERS = set([
        "feng",
    ])

    DEFAULT_PRIORITY = 100

    MIN_REPORT_ELAPSED_HOURS = 0.5

    GPU_PRIORITY_BASE = 80

    GPU_PRIORITY_SCALE = 0.20

    # ------------------------------------------------------------------
    # Email fallback
    # ------------------------------------------------------------------

    FALLBACK_EMAIL = "feng.zhang@xx.yy"

    # ------------------------------------------------------------------
    # Cluster names
    # ------------------------------------------------------------------

    CLUSTER_NAME = "XXX Cluster"

    GPU_CLUSTER_NAME = "YYY cluster"

    # ------------------------------------------------------------------
    # Directories
    # ------------------------------------------------------------------

    @classmethod
    def ensure_directories(cls):

        if (
            cls.WORKDIR
            and not os.path.exists(
                cls.WORKDIR
            )
        ):

            os.makedirs(
                cls.WORKDIR
            )

