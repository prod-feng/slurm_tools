# Slurm Job Performance Report

A lightweight Python tool for generating CPU, memory, disk I/O, and resource-utilization reports for Slurm jobs.

The tool uses `sacct` to collect completed-job information, calculates useful resource-utilization metrics, generates human-readable or CSV reports, and can optionally send personalized HTML email reports to users.

The project is intentionally organized into a few larger functional modules rather than many small files.

## Features

- Query completed Slurm jobs using `sacct`
- Select jobs by:
  - User
  - Multiple users
  - All users
  - Job ID
  - Time range
  - Slurm state
- Randomly select a limited number of jobs for user reports
- Calculate:
  - CPU hours
  - CPU utilization
  - User CPU time
  - System CPU time
  - Maximum memory usage
  - Requested memory
  - Disk read
  - Disk write
- Ignore selected partitions, such as A100 GPU jobs
- Generate terminal-friendly reports
- Generate CSV reports
- Generate personalized HTML email reports
- Configure email sender, CC, BCC, and subject without modifying Python code
- Customize the complete email body through an external HTML template
- Avoid shell pipelines when executing Slurm and system commands
- Keep Slurm, reporting, utility, and email functionality separated for easier maintenance

## Project Structure

```text
slurm-job-report/
├── job_report.py
├── job_report.conf
├── job_report.html
│
└── src/
    ├── __init__.py
    ├── slurm.py
    ├── report.py
    ├── utils.py
    └── emailer.py
