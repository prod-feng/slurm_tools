#!/usr/bin/env python3
"""Standalone Slurm CPU/GPU/RAM efficiency and allocation-share report.

Python 3.6 compatible and independent of any application or project package.
Can collect directly from sacct/sacctmgr or read portable pipe-delimited sacct
exports and account-parent JSON. Only the Python standard library and PyYAML
are required.
"""
from __future__ import print_function
import argparse
import datetime
import json
import math
import os
import re
import statistics
import subprocess
import tempfile
import sys

try:
    import yaml
except ImportError:
    yaml = None

# Keep this order aligned with LIVE_SACCT_FORMAT below.  ``sacct -P`` uses the
# same pipe-delimited representation as the existing export files.
FIELDS = [
    "jobid", "jobidraw", "cluster", "partition", "qos", "account", "user",
    "submit", "eligible", "start", "end", "elapsed", "exitcode", "state",
    "nnodes", "ntasks", "ncpus", "reqcpus", "reqmem", "reqtres", "alloctres",
    "timelimit", "nodelist", "tresusageinave", "tresusageinmax", "avecpu", "totalcpu", "maxrss",
    "maxdiskread", "maxdiskwrite", "jobname"
]
EXCLUDED_STATES = set(["PENDING", "RUNNING", "PREEMPTED", "REQUEUED", "REVOKED", "SUSPENDED", "RESIZING"])



# Use only fields supported by the target Slurm version. GPU telemetry is
# read from TRESUsageInAve and TRESUsageInMax.
LIVE_SACCT_FORMAT = [
    "JobID", "JobIDRaw", "Cluster", "Partition", "QOS", "Account", "User",
    "Submit", "Eligible", "Start", "End", "Elapsed", "ExitCode", "State",
    "AllocNodes", "NTasks", "AllocCPUS", "ReqCPUS", "ReqMem", "ReqTRES",
    "AllocTRES", "Timelimit", "NodeList", "TRESUsageInAve", "TRESUsageInMax",
    "AveCPU", "TotalCPU", "MaxRSS", "MaxDiskRead", "MaxDiskWrite", "JobName"
]


def run_command(command, description):
    """Run a command and return stdout, with a useful diagnostic on failure."""
    try:
        result = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  universal_newlines=True)
        stdout, stderr = result.communicate()
    except OSError as exc:
        raise RuntimeError("Could not run {} ({}): {}".format(description, command[0], exc))
    if result.returncode != 0:
        detail = (stderr or stdout or "no diagnostic output").strip()
        raise RuntimeError("{} failed (exit {}): {}".format(description, result.returncode, detail))
    return stdout


def collect_sacct(start, end, output_path, sacct_bin="sacct", sacct_cluster=None,
                  all_users=True):
    """Collect job allocations and steps directly from Slurm accounting.

    Do not pass ``-X``: job steps are needed for GPU and peak-RSS telemetry.
    """
    if not start or not end:
        raise ValueError("--start and --end are required with --from-sacct")
    command = [sacct_bin, "-P", "-n", "-S", start, "-E", end,
               "--format=" + ",".join(LIVE_SACCT_FORMAT)]
    if all_users:
        command.insert(1, "-a")
    if sacct_cluster:
        command.extend(["-M", sacct_cluster])
    output = run_command(command, "sacct collection")
    with open(output_path, "w") as handle:
        handle.write("|".join(FIELDS) + "\n")
        for line in output.splitlines():
            if not line.strip():
                continue
            parts = line.rstrip("\r\n").split("|")
            if len(parts) < len(LIVE_SACCT_FORMAT):
                parts += [""] * (len(LIVE_SACCT_FORMAT) - len(parts))
            source = dict(zip(LIVE_SACCT_FORMAT, parts[:len(LIVE_SACCT_FORMAT)]))
            row = [
                source.get("JobID", ""), source.get("JobIDRaw", ""), source.get("Cluster", ""),
                source.get("Partition", ""), source.get("QOS", ""), source.get("Account", ""),
                source.get("User", ""), source.get("Submit", ""), source.get("Eligible", ""),
                source.get("Start", ""), source.get("End", ""), source.get("Elapsed", ""),
                source.get("ExitCode", ""), source.get("State", ""), source.get("AllocNodes", ""),
                source.get("NTasks", ""), source.get("AllocCPUS", ""), source.get("ReqCPUS", ""),
                source.get("ReqMem", ""), source.get("ReqTRES", ""), source.get("AllocTRES", ""),
                source.get("Timelimit", ""), source.get("NodeList", ""),
                # GPU telemetry comes only from the supported TRES usage fields.
                source.get("TRESUsageInAve", ""), source.get("TRESUsageInMax", ""),
                source.get("AveCPU", ""), source.get("TotalCPU", ""), source.get("MaxRSS", ""),
                source.get("MaxDiskRead", ""), source.get("MaxDiskWrite", ""), source.get("JobName", "")
            ]
            handle.write("|".join(row) + "\n")
    return sum(1 for line in output.splitlines() if line.strip())


def collect_accounts(accounts_bin="sacctmgr"):
    """Collect the Slurm account hierarchy from association-tree output.

    Use the site's supported command and explicit ParentName column. Do not
    query ``show account`` first: on some Slurm versions it succeeds but omits
    the parent links needed to distinguish departments, PIs, and projects.
    User-association rows have a non-empty User column and are not account
    nodes; account-parent relationships come from the account rows.
    """
    command = [accounts_bin, "-n", "-p", "list", "associations", "tree",
               "format=Account,user,parentname"]
    output = run_command(command, "sacctmgr association-tree collection")
    by_name = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        # -p (parseable2) commonly leaves a trailing pipe, so preserve empty
        # columns. Expected columns: Account | User | ParentName |.
        parts = [part.strip() for part in line.rstrip("\r\n").split("|")]
        if not parts:
            continue
        name = parts[0]
        user = parts[1] if len(parts) > 1 else ""
        parent = parts[2] if len(parts) > 2 else ""
        if not name or name.lower() in ("account", "root"):
            continue
        # Association rows repeat the account name for each user; they do not
        # define hierarchy nodes. Only retain rows with an empty User field.
        if user:
            continue
        # Tree indentation is presentation only; parent_name is authoritative.
        name = re.sub(r"^(?:(?:\|--|\\--|\+--|`--|\|-|\\-)\s*)+", "", name).strip()
        parent = re.sub(r"^(?:(?:\|--|\\--|\+--|`--|\|-|\\-)\s*)+", "", parent).strip()
        if not name or name.lower() == "root":
            continue
        existing = by_name.get(name)
        if existing is None:
            by_name[name] = {"account_name": name, "parent_name": parent}
        elif not existing["parent_name"] and parent:
            by_name[name]["parent_name"] = parent

    if not by_name:
        raise RuntimeError(
            "The command 'sacctmgr -n -p list associations tree "
            "format=Account,user,parentname' returned no account rows."
        )
    # Keep top-level departments, but do not include Slurm's synthetic root.
    for item in by_name.values():
        if item["parent_name"].lower() == "root":
            item["parent_name"] = ""
    return {"accounts": list(by_name.values())}


def write_accounts_payload(payload, path):
    with open(path, "w") as handle:
        json.dump(payload, handle)


def number(value):
    try:
        if value is None or str(value).strip().lower() in ("", "unknown", "n/a", "none", "null", "-", "nan"):
            return None
        result = float(value)
        return result if not math.isnan(result) and result >= 0 else None
    except (TypeError, ValueError):
        return None


def duration_seconds(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value or value.lower() in ("unknown", "n/a", "unlimited", "infinite", "inf", "none"):
        return None
    try:
        days = 0
        if "-" in value:
            day_text, value = value.split("-", 1)
            days = int(day_text)
        parts = value.split(":")
        if len(parts) == 3:
            return days * 86400 + int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        if len(parts) == 2:
            return days * 86400 + int(parts[0]) * 60 + float(parts[1])
        if len(parts) == 1:
            return days * 86400 + float(parts[0])
    except (TypeError, ValueError):
        return None
    return None


def memory_bytes(value):
    """Parse Slurm memory values into bytes (K/M/G/T/P, optional /c or /n)."""
    if value is None:
        return None
    match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)\s*(?:/[cn])?\s*$", str(value), re.I)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2).upper()
    power = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}.get(unit, 0)
    return amount * (1024 ** power)


def parse_tres(value):
    result = {}
    if not value:
        return result
    # Slurm exports commonly use "gres/gpuutil=20,gres/gpumem=..." or "gpuutil=20".
    for part in re.split(r"[,;]", str(value)):
        if "=" not in part:
            continue
        key, raw = part.split("=", 1)
        val = number(raw)
        if val is not None:
            result[key.strip().lower()] = val
    return result


def gpu_util(value):
    parsed = parse_tres(value)
    for key in ("gres/gpuutil", "gpuutil", "gpu/util"):
        if key in parsed:
            return parsed[key]
    return None


def allocated_tres_value(text, key):
    match = re.search(r"(?:^|,)\s*{}=([^,]+)".format(re.escape(key)), str(text or ""), re.I)
    return match.group(1).strip() if match else None


def allocated_gpu_count(alloctres):
    text = str(alloctres or "")
    typed = re.findall(r"(?:^|,)\s*gres/gpu:[^=,]+=(\d+(?:\.\d+)?)", text, re.I)
    if typed:
        return sum(float(x) for x in typed)
    value = allocated_tres_value(text, "gres/gpu")
    return number(value) or 0.0


def elapsed_job(row):
    return duration_seconds(row.get("elapsed"))


def canonical_job_id(value):
    value = str(value or "").strip()
    # Array task IDs (e.g. 123_4) are kept distinct; only strip step suffix.
    return value.split(".", 1)[0]


def read_sacct(paths, cluster_name=None):
    """Load allocation rows and collect step telemetry by parent JobID.

    When the export contains a Cluster field, select only the requested Slurm
    cluster so reports from ``--clusters all`` do not mix cluster allocations.
    """
    allocations = {}
    steps = {}
    for path in paths:
        with open(path, "r") as handle:
            for line in handle:
                if not line.strip():
                    continue
                parts = line.rstrip("\r\n").split("|")
                if len(parts) < len(FIELDS):
                    parts += [""] * (len(FIELDS) - len(parts))
                row = dict(zip(FIELDS, parts[:len(FIELDS)]))
                jobid = row.get("jobid", "").strip()
                if not jobid or jobid.lower() == "jobid":
                    continue
                row["_base_id"] = canonical_job_id(jobid)
                row_cluster = str(row.get("cluster") or "").strip()
                if cluster_name and row_cluster and row_cluster.lower() != cluster_name.lower():
                    continue
                if "." in jobid:
                    steps.setdefault(row["_base_id"], []).append(row)
                else:
                    # Duplicate records may occur with --duplicates. Prefer the row
                    # with the most recent end time while keeping one allocation count.
                    prior = allocations.get(jobid)
                    if prior is None or (row.get("end") or "") >= (prior.get("end") or ""):
                        allocations[jobid] = row
    return list(allocations.values()), steps


def read_account_tree(path):
    """Read account-parent links from supported exporter accounts.json formats.

    Exporter versions may use ``account`` or ``account_name`` for the name.
    If a parent is absent, derive it from the account's hierarchical ``path``.
    """
    with open(path, "r") as handle:
        payload = json.load(handle)

    items = payload.get("accounts", []) if isinstance(payload, dict) else payload
    parents = {}
    paths = {}
    for item in items or []:
        if isinstance(item, dict):
            name = (item.get("account_name") or item.get("account") or
                    item.get("name") or item.get("Account"))
            parent = (item.get("parent_name") or item.get("parent") or
                      item.get("ParentName"))
            account_path = item.get("path") or item.get("Path")
        else:
            name = item[0] if len(item) > 0 else None
            parent = item[1] if len(item) > 1 else None
            account_path = item[4] if len(item) > 4 else None

        if not name:
            continue
        name = str(name).strip()
        parent = str(parent or "").strip()
        parents[name] = parent
        if account_path:
            paths[name] = str(account_path).strip()

    # Some exported account lists omit parent explicitly but retain a full path.
    for name, account_path in paths.items():
        if parents.get(name):
            continue
        parts = [part.strip() for part in re.split(r"[/\\>]+", account_path) if part.strip()]
        if parts and parts[-1] != name:
            parts.append(name)
        if len(parts) >= 2 and parts[-1] == name:
            parents[name] = parts[-2]

    return parents


def account_chain(account, parents):
    """Return only real hierarchy levels, excluding Slurm root/default nodes."""
    chain, seen, current = [], set(), str(account or "").strip()
    ignored = ("root", "default", "")
    while current and current not in seen:
        seen.add(current)
        if current.lower() not in ignored:
            chain.append(current)
        current = str(parents.get(current, "") or "").strip()
    chain.reverse()
    return chain


def stats(values):
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {"average_pct": None, "median_pct": None, "measured_jobs": 0}
    return {"average_pct": round(statistics.mean(clean), 2),
            "median_pct": round(statistics.median(clean), 2), "measured_jobs": len(clean)}


def summarize(jobs, cluster_totals):
    cpu_hours = sum(j.get("cpu_hours", 0.0) for j in jobs)
    gpu_hours = sum(j.get("gpu_hours", 0.0) for j in jobs)
    memory_gb_hours = sum(j.get("memory_gb_hours", 0.0) for j in jobs)
    def share(value, total):
        return round(value * 100.0 / total, 2) if total > 0 else None
    return {
        "jobs": len(jobs),
        "allocation_usage": {
            "cpu_hours": round(cpu_hours, 2),
            "cpu_hours_pct_cluster": share(cpu_hours, cluster_totals["cpu_hours"]),
            "gpu_hours": round(gpu_hours, 2),
            "gpu_hours_pct_cluster": share(gpu_hours, cluster_totals["gpu_hours"]),
            "memory_gb_hours": round(memory_gb_hours, 2),
            "memory_gb_hours_pct_cluster": share(memory_gb_hours, cluster_totals["memory_gb_hours"]),
        },
        "efficiency": {
            "cpu": stats([j.get("cpu_efficiency_pct") for j in jobs]),
            "ram": stats([j.get("ram_efficiency_pct") for j in jobs]),
            "gpu": stats([j.get("gpu_utilization_pct") for j in jobs]),
        }
    }


def build_report(paths, accounts_path, cluster_name, period_start=None, period_end=None):
    parents = read_account_tree(accounts_path)
    print("Loaded {} account-parent mappings from {}".format(len(parents), accounts_path), file=sys.stderr)
    allocation_rows, step_rows = read_sacct(paths, cluster_name)
    jobs = []
    default_jobs = []
    warned_incomplete_accounts = set()
    for row in allocation_rows:
        state = re.sub(r"\s+BY\s+.*$", "", str(row.get("state") or "UNKNOWN").upper()).rstrip("+").strip()
        if state in EXCLUDED_STATES:
            continue
        elapsed = elapsed_job(row)
        if elapsed is None or elapsed <= 0:
            continue
        account = str(row.get("account") or "(Unknown account)").strip()
        # These are Slurm/system placeholder accounts, not reportable departments.
        if account.lower() == "root":
            continue
        is_default_account = account.lower() == "default"
        user = str(row.get("user") or "(Unknown user)").strip()
        chain = [] if is_default_account else account_chain(account, parents)
        if not is_default_account and len(chain) < 2:
            # Do not discard jobs just because the hierarchy export is incomplete.
            # First try live/merged hierarchy data (assembled in main); if the
            # account still has no usable parents, keep it visible in an explicit
            # fallback bucket rather than silently losing its resource usage.
            if account.lower().startswith("pn_"):
                department, pi = "(Unmapped department)", "(Unknown PI)"
                fallback_project = account
            elif account.lower().startswith("pi_"):
                department, pi = "(Unmapped department)", account
                fallback_project = "(No project)"
            else:
                department, pi = account, "(Unknown PI)"
                fallback_project = "(No project)"
            if account not in warned_incomplete_accounts:
                print(
                    "WARNING: account {!r} still has incomplete hierarchy chain {!r}; "
                    "its jobs are retained under {!r} / {!r} instead of being skipped. "
                    "Check sacctmgr output or the --accounts JSON.".format(
                        account, chain, department, pi),
                    file=sys.stderr)
                warned_incomplete_accounts.add(account)
        else:
            department = chain[0] if chain else None
            pi = chain[1] if len(chain) > 1 else None
            fallback_project = None
        # Account chain is department -> PI -> PN/project. The default account
        # is summarized separately and never enters department/PI/project/user rows.
        alloctres = row.get("alloctres") or ""
        ncpus = number(row.get("ncpus")) or number(allocated_tres_value(alloctres, "cpu")) or 0.0
        cpu_seconds = duration_seconds(row.get("totalcpu"))
        cpu_pct = max(0.0, cpu_seconds / (ncpus * elapsed) * 100.0) if cpu_seconds is not None and ncpus > 0 else None
        allocated_mem = memory_bytes(allocated_tres_value(alloctres, "mem"))
        if not allocated_mem:
            reqmem = row.get("reqmem")
            reqmem_bytes = memory_bytes(reqmem)
            if reqmem_bytes:
                # ReqMem suffix /c means per CPU; /n means per node. No suffix
                # follows Slurm's configured default, so use it only as a fallback.
                if str(reqmem).lower().endswith("c"):
                    allocated_mem = reqmem_bytes * max(1.0, ncpus)
                elif str(reqmem).lower().endswith("n"):
                    allocated_mem = reqmem_bytes * max(1.0, number(row.get("nnodes")) or 1.0)
                else:
                    allocated_mem = reqmem_bytes
        peak_rss = memory_bytes(row.get("maxrss"))
        gpu_values = []
        for step in step_rows.get(row.get("jobid", ""), []):
            # This script supports Slurm installations that expose GPU
            # telemetry only through TRESUsageInAve/TRESUsageInMax.
            val = gpu_util(step.get("tresusageinave"))
            if val is None:
                val = gpu_util(step.get("tresusageinmax"))
            if val is not None:
                suffix = step.get("jobid", "").rsplit(".", 1)[-1].lower()
                gpu_values.append((suffix, val))
            step_rss = memory_bytes(step.get("maxrss"))
            if step_rss is not None:
                peak_rss = max(peak_rss or 0.0, step_rss)
        batch = [v for suffix, v in gpu_values if suffix == "batch"]
        non_extern = [v for suffix, v in gpu_values if suffix != "extern"]
        selected_gpu = batch or non_extern or [v for _, v in gpu_values]
        if not selected_gpu:
            for field in ("tresusageinave", "tresusageinmax"):
                val = gpu_util(row.get(field))
                if val is not None:
                    selected_gpu = [val]
                    break
        gpu_count = allocated_gpu_count(alloctres)
        cpu_hours = ncpus * elapsed / 3600.0
        gpu_hours = gpu_count * elapsed / 3600.0
        memory_gb_hours = (allocated_mem / (1024.0 ** 3)) * elapsed / 3600.0 if allocated_mem else 0.0
        ram_pct = max(0.0, peak_rss / allocated_mem * 100.0) if peak_rss is not None and allocated_mem else None
        # Slurm's gres/gpuutil telemetry can be accumulated across all GPUs
        # allocated to the job (e.g. 398 for four GPUs). Normalize to a
        # per-GPU percentage before aggregating jobs into user/project/PI rows.
        gpu_pct = (statistics.mean(selected_gpu) / gpu_count) if selected_gpu and gpu_count > 0 else None
        job_summary = {
            "job_id": row.get("jobid"), "user": user, "account": account,
            "department": department, "pi": pi,
            # If a job is charged directly to a PI account, keep it under an
            # explicit placeholder rather than repeating the PI name as project.
            "project": chain[2] if len(chain) > 2 else (fallback_project or "(No project)"),
            "state": state, "cpu_hours": cpu_hours, "gpu_hours": gpu_hours,
            "memory_gb_hours": memory_gb_hours,
            "cpu_efficiency_pct": round(cpu_pct, 2) if cpu_pct is not None else None,
            "ram_efficiency_pct": round(ram_pct, 2) if ram_pct is not None else None,
            "gpu_utilization_pct": round(gpu_pct, 2) if gpu_pct is not None else None,
        }
        if is_default_account:
            default_jobs.append(job_summary)
        else:
            jobs.append(job_summary)
    all_report_jobs = jobs + default_jobs
    totals = {
        "cpu_hours": sum(j["cpu_hours"] for j in all_report_jobs),
        "gpu_hours": sum(j["gpu_hours"] for j in all_report_jobs),
        "memory_gb_hours": sum(j["memory_gb_hours"] for j in all_report_jobs),
    }
    all_summary = summarize(all_report_jobs, totals)
    default_summary = summarize(default_jobs, totals)
    departments = {}
    for dept in sorted(set(j["department"] for j in jobs), key=lambda x: x.lower()):
        dept_jobs = [j for j in jobs if j["department"] == dept]
        dept_item = {"name": dept}
        dept_item.update(summarize(dept_jobs, totals))
        dept_item["pis"] = []
        for pi in sorted(set(j["pi"] for j in dept_jobs), key=lambda x: x.lower()):
            pi_jobs = [j for j in dept_jobs if j["pi"] == pi]
            pi_item = {"name": pi}
            pi_item.update(summarize(pi_jobs, totals))
            pi_item["projects"] = []
            for project in sorted(set(j["project"] for j in pi_jobs), key=lambda x: x.lower()):
                project_jobs = [j for j in pi_jobs if j["project"] == project]
                project_item = {"name": project}
                project_item.update(summarize(project_jobs, totals))
                project_item["users"] = []
                for user in sorted(set(j["user"] for j in project_jobs), key=lambda x: x.lower()):
                    user_jobs = [j for j in project_jobs if j["user"] == user]
                    user_item = {"name": user}
                    user_item.update(summarize(user_jobs, totals))
                    project_item["users"].append(user_item)
                pi_item["projects"].append(project_item)
            dept_item["pis"].append(pi_item)
        departments[dept] = dept_item
    return {
        "format_version": 1,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "cluster": cluster_name,
        "period": {"start": period_start, "end_exclusive": period_end},
        "definitions": {
            "allocation_usage": "CPU/GPU/memory allocation-hours from completed/final jobs; percentages are each group's share of all included jobs in this report, not hardware utilization.",
            "cpu_efficiency": "TotalCPU / (allocated CPUs * elapsed seconds) * 100.",
            "ram_efficiency": "peak MaxRSS / allocated memory from AllocTRES mem (ReqMem fallback) * 100.",
            "gpu_efficiency": "Per-GPU utilization percentage: Slurm gres/gpuutil telemetry from TRESUsageInAve/TRESUsageInMax divided by the number of GPUs allocated to the job; missing telemetry is excluded.",
            "excluded_states": sorted(EXCLUDED_STATES),
            "rollup_rule": "Each allocation row is counted once; step rows only provide telemetry and are not counted as jobs.",
        },
        "default_account": {
            "name": "default",
            "summary": default_summary,
        },
        "cluster_totals": {
            "jobs": len(all_report_jobs),
            "cpu_hours": round(totals["cpu_hours"], 2),
            "gpu_hours": round(totals["gpu_hours"], 2),
            "memory_gb_hours": round(totals["memory_gb_hours"], 2),
            "efficiency": all_summary["efficiency"],
        },
        "departments": list(departments.values()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sacct", nargs="+", help="One or more pipe-delimited sacct export files")
    source.add_argument("--from-sacct", action="store_true",
                        help="Collect job/accounting data live using sacct instead of reading export files")
    parser.add_argument("--accounts", default=None,
                        help="Optional JSON account hierarchy; live mode collects it directly via sacctmgr if omitted")
    parser.add_argument("--cluster", required=True, help="Cluster label for this report")
    parser.add_argument("--sacct-cluster", default=None,
                        help="Optional SlurmDBD cluster name passed to sacct -M (defaults to local/default cluster)")
    parser.add_argument("--sacct-bin", default="sacct", help="Path to sacct executable (default: sacct)")
    parser.add_argument("--sacctmgr-bin", default="sacctmgr", help="Path to sacctmgr executable (default: sacctmgr)")
    parser.add_argument("--start", default=None, help="Start date/time for the report and live collection")
    parser.add_argument("--end", default=None, help="Report end date/time; passed to sacct -E in live mode")
    parser.add_argument("--output", required=True, help="Output YAML path")
    parser.add_argument("--current-user-only", action="store_true",
                        help="In live mode, omit sacct -a and collect only jobs visible for the invoking user")
    args = parser.parse_args()
    if yaml is None:
        parser.error("PyYAML is required to write YAML. Install a Python-3.6-compatible PyYAML release (e.g. PyYAML 5.3.1).")

    temporary_paths = []
    try:
        accounts_path = args.accounts
        sacct_paths = args.sacct or []
        if args.from_sacct:
            fd, sacct_path = tempfile.mkstemp(prefix="slurm-efficiency-sacct-", suffix=".out")
            os.close(fd)
            temporary_paths.append(sacct_path)
            count = collect_sacct(args.start, args.end, sacct_path, args.sacct_bin,
                                  args.sacct_cluster, all_users=not args.current_user_only)
            sacct_paths = [sacct_path]
            print("Collected {} sacct rows for {} to {}".format(count, args.start, args.end), file=sys.stderr)
            # In live mode always query sacctmgr, even when an offline JSON was
            # supplied. Merge the live parent links over the JSON so stale or
            # incomplete exports cannot cause every project job to be dropped.
            live_payload = collect_accounts(args.sacctmgr_bin)
            live_parents = {}
            for item in live_payload.get("accounts", []):
                name = str(item.get("account_name") or "").strip()
                parent = str(item.get("parent_name") or "").strip()
                if name:
                    live_parents[name] = parent
            merged_parents = {}
            if accounts_path:
                merged_parents.update(read_account_tree(accounts_path))
            for name, parent in live_parents.items():
                if parent or name not in merged_parents:
                    merged_parents[name] = parent
            fd, merged_path = tempfile.mkstemp(prefix="slurm-efficiency-accounts-merged-", suffix=".json")
            os.close(fd)
            temporary_paths.append(merged_path)
            write_accounts_payload({"accounts": [
                {"account_name": name, "parent_name": parent}
                for name, parent in merged_parents.items()
            ]}, merged_path)
            accounts_path = merged_path
            print("Collected {} live hierarchy entries via sacctmgr; merged {} account-parent mappings".format(
                len(live_parents), len(merged_parents)), file=sys.stderr)
        elif not accounts_path:
            parser.error("--accounts is required when reading existing --sacct export files")

        for path in sacct_paths + [accounts_path]:
            if not os.path.isfile(path):
                parser.error("Input file not found: {}".format(path))
        report = build_report(sacct_paths, accounts_path, args.cluster, args.start, args.end)
        output_dir = os.path.dirname(os.path.abspath(args.output))
        if not os.path.isdir(output_dir):
            parser.error("Output directory does not exist: {}".format(output_dir))
        with open(args.output, "w") as handle:
            yaml.safe_dump(report, handle, default_flow_style=False, sort_keys=False, allow_unicode=True)
        print("Wrote {}: {} jobs, {} departments".format(args.output, report["cluster_totals"]["jobs"], len(report["departments"])))
    except (RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    finally:
        for path in temporary_paths:
            try:
                os.unlink(path)
            except OSError:
                pass


if __name__ == "__main__":
    main()

