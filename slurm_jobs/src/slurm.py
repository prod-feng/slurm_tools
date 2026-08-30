import subprocess


SACCT_FORMAT = (
    "User,"
    "JobID,"
    "Account,"
    "Partition,"
    "State,"
    "Start,"
    "Elapsed,"
    "MaxRSS,"
    "MaxVMSize,"
    "NNodes,"
    "NCPUS,"
    "NodeList,"
    "CPUTime,"
    "SystemCPU,"
    "TotalCPU,"
    "UserCPU,"
    "ReqMem,"
    "MaxDiskWrite,"
    "MaxDiskRead,"
    "JobName"
)


class SlurmJob:
    """Container for information about one Slurm job."""

    def __init__(
        self,
        user,
        job_id,
        account,
        partition,
        state,
        start,
        elapsed,
        max_rss,
        max_vm_size,
        n_nodes,
        n_cpus,
        node_list,
        cpu_time,
        system_cpu,
        total_cpu,
        user_cpu,
        req_mem,
        max_disk_write,
        max_disk_read,
        job_name,
    ):
        self.user = user
        self.job_id = job_id
        self.account = account
        self.partition = partition
        self.state = state
        self.start = start
        self.elapsed = elapsed
        self.max_rss = max_rss
        self.max_vm_size = max_vm_size
        self.n_nodes = n_nodes
        self.n_cpus = n_cpus
        self.node_list = node_list
        self.cpu_time = cpu_time
        self.system_cpu = system_cpu
        self.total_cpu = total_cpu
        self.user_cpu = user_cpu
        self.req_mem = req_mem
        self.max_disk_write = max_disk_write
        self.max_disk_read = max_disk_read
        self.job_name = job_name


def build_sacct_command(
    user,
    all_users,
    jobs,
    start_time,
    end_time,
    state,
    all_states,
):
    """Build a safe sacct command."""

    command = [
        "sacct",
        "-n",
        "-P",
        "--starttime={}".format(start_time),
        "--endtime={}".format(end_time),
        "--format={}".format(SACCT_FORMAT),
    ]

    if all_users:
        command.append("-a")

    elif jobs:
        command.extend(["-j", jobs])

    elif user:
        command.extend(["-u", user])

    if not all_states and state:
        command.extend(["-s", state])

    return command


def get_jobs(
    user,
    all_users,
    jobs,
    start_time,
    end_time,
    state,
    all_states,
):
    """Run sacct and return parsed Slurm jobs."""

    command = build_sacct_command(
        user=user,
        all_users=all_users,
        jobs=jobs,
        start_time=start_time,
        end_time=end_time,
        state=state,
        all_states=all_states,
    )

    result = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    return parse_sacct_output(result.stdout)


def parse_sacct_output(output):
    """Parse pipe-delimited sacct output."""

    jobs = []

    for line_number, line in enumerate(
        output.splitlines(),
        start=1,
    ):
        if not line.strip():
            continue

        fields = line.split("|", 19)

        if len(fields) != 20:
            raise ValueError(
                "Unexpected sacct output on line {}: "
                "expected 20 fields, got {}".format(
                    line_number,
                    len(fields),
                )
            )

        job = SlurmJob(
            user=fields[0],
            job_id=fields[1],
            account=fields[2],
            partition=fields[3],
            state=fields[4],
            start=fields[5],
            elapsed=fields[6],
            max_rss=fields[7],
            max_vm_size=fields[8],
            n_nodes=fields[9],
            n_cpus=fields[10],
            node_list=fields[11],
            cpu_time=fields[12],
            system_cpu=fields[13],
            total_cpu=fields[14],
            user_cpu=fields[15],
            req_mem=fields[16],
            max_disk_write=fields[17],
            max_disk_read=fields[18],
            job_name=fields[19],
        )

        if job.user:
            jobs.append(job)

    return jobs

