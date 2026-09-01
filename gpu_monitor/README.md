GPU Job Monitoring and Reporting Tools
======================================

Overview
--------

This project provides a set of Python tools for monitoring and reporting
GPU job utilization on a Slurm-based HPC cluster.

The tools are designed for clusters using Slurm, NVIDIA GPUs, pdsh, and
the standard Linux mail command.

The project provides two main functions:

1. Real-time GPU utilization monitoring for running jobs.
2. Periodic GPU job performance reporting for completed jobs.

The monitor can detect jobs with low or imbalanced GPU utilization and
send notification emails to users.

The reporting tool aggregates Slurm job allocations and job steps and
generates per-user performance reports.


Main Features
-------------

- Monitor GPU utilization of running Slurm jobs.
- Collect NVIDIA GPU statistics using nvidia-smi.
- Query multiple compute nodes in parallel using pdsh.
- Detect low GPU utilization.
- Detect imbalanced GPU utilization across allocated GPUs.
- Maintain persistent monitoring state.
- Avoid sending repeated notification emails too frequently.
- Generate GPU job performance reports.
- Aggregate Slurm primary jobs with .batch and .extern steps.
- Calculate GPU hours and GPU utilization efficiency.
- Calculate CPU usage statistics.
- Report memory and disk usage.
- Generate text and CSV reports.
- Send HTML performance reports by email.
- Configure email subjects and message bodies without modifying Python code.
- Support Python 3.6.


Project Structure
-----------------

gpu_job_monitor.py

    Main program for monitoring currently running GPU jobs.

gpu_job_report.py

    Main program for generating GPU job performance reports.

modules/

    config.py

        Central configuration for Slurm commands, cluster settings,
        monitoring thresholds, reporting settings, and email settings.

    gpu.py

        NVIDIA GPU telemetry collection and GPU utilization analysis.

    slurm.py

        Slurm command interface and job information normalization.

    reporting.py

        Job aggregation, GPU job filtering, statistics calculation,
        report formatting, and email generation.

    utils.py

        Common utilities such as command execution, state storage,
        locking, and user/email lookup.

    email_templates.conf

        External email template configuration.

        Email subjects and message bodies can be changed here without
        modifying the Python source code.

README.md

    Project documentation.


Requirements
------------

The tools require:

- Linux
- Python 3.6 or newer
- Slurm
- NVIDIA GPUs
- nvidia-smi
- pdsh
- mail

Cluster Permissions
-------------------

The monitoring host must have sufficient permissions to query Slurm
job information.

The host must also be able to execute nvidia-smi remotely on the
target compute nodes.

For example:

    pdsh -w h200x8-04 nvidia-smi

should return NVIDIA GPU information from the requested node.


Configuration
-------------

Most system-level configuration is stored in:

    modules/config.py

Running the GPU Monitor
-----------------------

Run:

    ./gpu_job_monitor.py

To display informational logging:

    ./gpu_job_monitor.py --verbose

To display debug logging:

    ./gpu_job_monitor.py --debug

To run without actually sending email:

    ./gpu_job_monitor.py --dry-run

A dry run is recommended when testing email templates or monitoring
configuration.

Example GPU Monitor Output
--------------------------

Example:

    USER         JOBID      JOBNAME              NODE               GPU
    -----------------------------------------------------------------------
    feng         85467      nccensus08315         h200x8-04          0
    AVG%     MIN%     MAX%     IDLE     STATE             BAD
    33.0      33.0     33.0        0     NORMAL              0

The actual output depends on the Slurm allocation and GPU telemetry
available at the time of execution.

GPU Job Selection
-----------------

Only GPU jobs are included in GPU performance reports.

A job is considered a GPU job when its GPU allocation is greater than
zero.

Jobs shorter than the configured minimum reporting time are ignored.

The default minimum reporting time is:

    0.5 hours

The value can be changed using:

    MIN_REPORT_ELAPSED_HOURS


GPU Efficiency
--------------

GPU efficiency is calculated from GPU hours and GPU utilization hours.

The general calculation is:

    GPU efficiency =
        GPU utilization hours / GPU hours * 100

For example, if a job uses:

    100 GPU hours

with an average utilization of:

    60%

the calculated GPU efficiency is:

    60%


Priority Calculation
--------------------

The project can calculate a GPU-based priority score.

The default formula is:

    priority = 80 + GPU efficiency * 0.20

Therefore:

    0% utilization      -> 80
    50% utilization     -> 90
    100% utilization    -> 100

The following settings control the calculation:

    GPU_PRIORITY_BASE = 80
    GPU_PRIORITY_SCALE = 0.20

Users listed in:

    PRIORITY_EXCLUDED_USERS

are not affected by priority changes.

Installation
------------

Clone the repository:

    git clone <repository-url>

Change into the project directory:

    cd <project-directory>

Make the main scripts executable:

    chmod +x gpu_job_monitor.py
    chmod +x gpu_job_report.py


