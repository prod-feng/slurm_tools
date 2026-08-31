import random

from .utils import (
    escape_csv,
    human_size,
    safe_int,
    size2GB,
    time2hours,
)


def parse_job_records(result):
    """
    Parse sacct pipe-delimited output into job records.

    Returns:

        {
            "username": {
                "jobid": record
            }
        }
    """
    jobs = {}

    if not result:
        return jobs

    for line in result.splitlines():
        if not line.strip():
            continue

        fields = line.rstrip("\n").split("|")

        if len(fields) < 19:
            continue

        (
            user,
            job_id,
            job_name,
            partition,
            state,
            start,
            elapsed,
            max_rss,
            max_vm_size,
            nnodes,
            ncpus,
            nodelist,
            cpu_time,
            system_cpu,
            total_cpu,
            user_cpu,
            req_mem,
            max_disk_write,
            max_disk_read,
        ) = fields[:19]

        if not user or not job_id:
            continue

        record = {
            "user": user,
            "job_id": job_id,
            "job_name": job_name,
            "partition": partition,
            "state": state,
            "start": start,
            "elapsed": elapsed,
            "max_rss": size2GB(max_rss),
            "max_vm_size": size2GB(max_vm_size),
            "nnodes": safe_int(nnodes),
            "ncpus": safe_int(ncpus),
            "nodelist": nodelist,
            "cpu_time": cpu_time,
            "system_cpu": system_cpu,
            "total_cpu": total_cpu,
            "user_cpu": user_cpu,
            "req_mem": req_mem,
            "max_disk_write": size2GB(max_disk_write),
            "max_disk_read": size2GB(max_disk_read),
        }

        jobs.setdefault(user, {})[job_id] = record

    return jobs


def calculate_job_metrics(job):
    """
    Calculate derived performance metrics for a job.
    """
    cpu_hours = time2hours(job["cpu_time"])
    system_cpu_hours = time2hours(job["system_cpu"])
    total_cpu_hours = time2hours(job["total_cpu"])
    user_cpu_hours = time2hours(job["user_cpu"])

    # Keep the original script's behavior of adding one hour to avoid
    # a zero denominator, but do it explicitly and safely.
    denominator = max(cpu_hours, 1.0)

    cpu_usage = user_cpu_hours / denominator

    return {
        "cpu_hours": round(cpu_hours, 2),
        "system_cpu_hours": round(system_cpu_hours, 2),
        "total_cpu_hours": round(total_cpu_hours, 2),
        "user_cpu_hours": round(user_cpu_hours, 2),
        "cpu_usage": round(cpu_usage, 3),
    }


def select_jobs_for_user(records, max_jobs):
    """
    Randomly select jobs if more than max_jobs are available.

    The selection is deterministic in structure but intentionally
    random in which jobs are included, matching the original tool.
    """
    values = list(records.values())

    if max_jobs <= 0:
        max_jobs = 10

    if len(values) <= max_jobs:
        return values

    return random.sample(values, max_jobs)


def job_header(csv=False):
    if csv:
        return ",".join([
            "USER",
            "JobID",
            "Jobname",
            "Start",
            "TElapsed",
            "MemUsedGB",
            "MemAsked",
            "nNodes",
            "nCPUs",
            "CPUhours",
            "CPUUsage",
            "CPUSYST",
            "CPUUSER",
            "DiskWriteGB",
            "DiskReadGB",
            "Partition",
            "NodeList",
            "State",
        ])

    return (
        "{:>10} {:>10} {:>12} {:>12} {:>12} "
        "{:>10} {:>10} {:>8} {:>7} {:>12} "
        "{:>10} {:>10} {:>10} {:>11} {:>11} "
        "{:^16} {:^14} {:^8}"
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


def format_job(job, csv=False):
    metrics = calculate_job_metrics(job)

    if csv:
        values = [
            job["user"],
            job["job_id"],
            job["job_name"],
            job["start"].split("T")[0],
            job["elapsed"],
            "{:.2f}".format(job["max_rss"]),
            "{:.2f}".format(size2GB(job["req_mem"])),
            job["nnodes"],
            job["ncpus"],
            "{:.2f}".format(metrics["cpu_hours"]),
            "{:.3f}".format(metrics["cpu_usage"]),
            "{:.2f}".format(metrics["system_cpu_hours"]),
            "{:.2f}".format(metrics["user_cpu_hours"]),
            "{:.2f}".format(job["max_disk_write"]),
            "{:.2f}".format(job["max_disk_read"]),
            job["partition"],
            job["nodelist"].replace(",", "|"),
            job["state"],
        ]

        return ",".join(
            escape_csv(value)
            for value in values
        )

    return (
        "{:>10} {:>10} {:>12.12} {:>12} {:>12} "
        "{:>10.2f} {:>10} {:>8} {:>7} {:>12.2f} "
        "{:>10.3f} {:>10.2f} {:>10.2f} {:>11} {:>11} "
        "{:^16.16} {:^14.14} {:^8}"
    ).format(
        job["user"],
        job["job_id"],
        job["job_name"],
        job["start"].split("T")[0],
        job["elapsed"],
        job["max_rss"],
        human_size(job["req_mem"]),
        job["nnodes"],
        job["ncpus"],
        metrics["cpu_hours"],
        metrics["cpu_usage"],
        metrics["system_cpu_hours"],
        metrics["user_cpu_hours"],
        human_size(
            "{}G".format(job["max_disk_write"])
        ),
        human_size(
            "{}G".format(job["max_disk_read"])
        ),
        job["partition"],
        job["nodelist"],
        job["state"],
    )


def build_job_report(records, max_jobs=10, csv=False):
    """
    Build a report for all users.

    Returns:

        {
            username: report_text
        }
    """
    reports = {}

    for user in sorted(records):
        selected = select_jobs_for_user(
            records[user],
            max_jobs,
        )

        if csv:
            lines = [job_header(csv=True)]
        else:
            lines = [job_header(csv=False)]

        for job in selected:
            lines.append(
                format_job(
                    job,
                    csv=csv,
                )
            )

        reports[user] = "\n".join(lines)

    return reports

