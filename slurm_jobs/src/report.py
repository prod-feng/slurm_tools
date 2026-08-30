import csv
import io
import random
import subprocess

from .slurm import SlurmJob
from .utils import (
    human_size,
    size_to_gb,
    time_to_hours,
)


class Report:
    """Container for one user's job report."""

    def __init__(
        self,
        user,
        email_address,
        job_info,
        num_report,
        total_jobs,
        reportable_jobs,
    ):
        self.user = user
        self.email_address = email_address
        self.job_info = job_info
        self.num_report = num_report
        self.total_jobs = total_jobs
        self.reportable_jobs = reportable_jobs


def get_user_email(user):
    """
    Get the user's email address from the passwd/GECOS entry.

    Falls back to the HPC support address if no address
    can be found.
    """

    fallback = "feng.zhang@stonybrook.edu"

    try:
        result = subprocess.run(
            ["getent", "passwd", user],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False,
        )

        if result.returncode != 0:
            return fallback

        fields = result.stdout.strip().split(":")

        if len(fields) < 5:
            return fallback

        gecos = fields[4]

        for item in gecos.split(","):
            if "@" in item:
                return item.strip()

    except OSError:
        pass

    return fallback


def normalize_memory(value):
    """Convert MaxRSS/MaxVMSize to GB."""

    return round(size_to_gb(value), 2)


def calculate_cpu_usage(job):
    """
    Calculate CPU metrics.

    CPUUsage = UserCPU / CPUTime.

    We retain the original script's +1 hour behavior
    to avoid division by zero.
    """

    cpu_hours = time_to_hours(job.cpu_time)
    cpu_hours_for_rate = cpu_hours + 1.0

    user_cpu_hours = time_to_hours(job.user_cpu)
    system_cpu_hours = time_to_hours(job.system_cpu)
    total_cpu_hours = time_to_hours(job.total_cpu)

    cpu_usage = round(
        user_cpu_hours / cpu_hours_for_rate,
        3,
    )

    return (
        round(cpu_hours_for_rate, 1),
        round(user_cpu_hours, 6),
        round(system_cpu_hours, 6),
        round(total_cpu_hours, 6),
        cpu_usage,
    )


def is_ignored_job(job):
    """Return True for jobs intentionally excluded from reports."""

    return "a100" in job.partition.lower()


def format_header(csv_mode=False):
    """Create report header."""

    if csv_mode:
        fields = [
            "USER",
            "JobID",
            "Jobname",
            "Start",
            "TElapsed",
            "MemUsed",
            "MemAsked",
            "nNodes",
            "nCPUs",
            "CPUhours",
            "CPUUsage",
            "CPUSYST",
            "CPUUSER",
            "DiskWrite",
            "DiskRead",
            "Partition",
            "NodeList",
            "State",
        ]

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(fields)

        return output.getvalue().rstrip("\n")

    return (
        "{:>11.10}"
        "{:>10.10}"
        "{:>12.10}"
        "{:>12.10}"
        "{:>12.10}"
        "{:>9.7}"
        "{:>10.8}"
        "{:>8.6}"
        "{:>7.5}"
        "{:>12.11}"
        "{:>10.9}"
        "{:>9.7}"
        "{:>11.9}"
        "{:>11.9}"
        "{:>11.9}"
        "{:^20.18}"
        "{:^14.13}"
        "{:^8.6}"
    ).format(
        "USER",
        "JobID",
        "Jobname",
        "Start",
        "TElapsed",
        "MemUsed",
        "MemAsked",
        "nNodes",
        "nCPUs",
        "CPUhours",
        "CPUUsage",
        "CPUSYST",
        "CPUUSER",
        "DiskWrite",
        "DiskRead",
        "Partition",
        "NodeList",
        "State",
    )


def format_job(job, csv_mode=False):
    """Format one job for the report."""

    mem_used = normalize_memory(job.max_rss)
    mem_asked = size_to_gb(job.req_mem)

    disk_write = size_to_gb(job.max_disk_write)
    disk_read = size_to_gb(job.max_disk_read)

    (
        cpu_hours,
        user_cpu_hours,
        system_cpu_hours,
        total_cpu_hours,
        cpu_usage,
    ) = calculate_cpu_usage(job)

    start_date = job.start.split("T")[0]

    if csv_mode:
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow(
            [
                job.user,
                job.job_id,
                job.job_name,
                start_date,
                job.elapsed,
                mem_used,
                mem_asked,
                job.n_nodes,
                job.n_cpus,
                cpu_hours,
                cpu_usage,
                round(system_cpu_hours, 2),
                round(user_cpu_hours, 2),
                disk_write,
                disk_read,
                job.partition,
                job.node_list.replace(",", "|"),
                job.state,
            ]
        )

        return output.getvalue().rstrip("\n")

    return (
        "{:>10.10}"
        "{:>10.10}"
        "{:>12.10}"
        "{:>12.10}"
        "{:>12.10}"
        "{:>10.8}"
        "{:>9.8}"
        "{:>8.6}"
        "{:>7.5}"
        "{:>12.11}"
        "{:>10.8}"
        "{:>9.7}"
        "{:>11.9}"
        "{:>11.9}"
        "{:>11.9}"
        "{:^20.18}"
        "{:^14.13}"
        "{:^8.6}"
    ).format(
        job.user,
        job.job_id,
        job.job_name,
        start_date,
        job.elapsed,
        str(mem_used) + "G",
        human_size(job.req_mem),
        job.n_nodes,
        job.n_cpus,
        str(cpu_hours) + "h",
        cpu_usage,
        str(round(system_cpu_hours, 2)) + "h",
        str(round(user_cpu_hours, 2)) + "h",
        human_size("{}G".format(disk_write)),
        human_size("{}G".format(disk_read)),
        job.partition,
        "  " + job.node_list,
        "  " + job.state,
    )


def build_reports(
    jobs,
    max_jobs=10,
    csv=False,
    random_module=random,
):
    """
    Build a report for each user.

    Normal mode:
        Randomly select up to max_jobs.

    CSV mode:
        Include all reportable jobs.
    """

    jobs_by_user = {}

    for job in jobs:
        if job.user not in jobs_by_user:
            jobs_by_user[job.user] = []

        jobs_by_user[job.user].append(job)

    reports = {}

    for user, user_jobs in jobs_by_user.items():

        reportable_jobs = [
            job
            for job in user_jobs
            if not is_ignored_job(job)
        ]

        if csv:
            selected_jobs = reportable_jobs

        elif len(reportable_jobs) > max_jobs:
            selected_jobs = random_module.sample(
                reportable_jobs,
                max_jobs,
            )

        else:
            selected_jobs = reportable_jobs

        if csv:
            lines = [
                format_header(csv_mode=True)
            ]

            for job in selected_jobs:
                lines.append(
                    format_job(
                        job,
                        csv_mode=True,
                    )
                )

        else:
            lines = [
                format_header(csv_mode=False),
                "",
            ]

            for job in selected_jobs:
                lines.append(
                    format_job(job)
                )

        job_info = "\n".join(lines).rstrip()

        reports[user] = Report(
            user=user,
            email_address=get_user_email(user),
            job_info=job_info,
            num_report=len(selected_jobs),
            total_jobs=len(user_jobs),
            reportable_jobs=len(reportable_jobs),
        )

    return reports

