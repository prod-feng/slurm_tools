import os
import subprocess


class SlurmError(RuntimeError):
    """Raised when a Slurm command fails."""


class SlurmClient(object):
    """
    Common interface to Slurm commands.

    Keeping Slurm command execution here means jobs.py and nodes.py
    do not need to know how Slurm is installed or how subprocesses
    are executed.
    """

    def __init__(self, config):
        self.config = config

        slurm_bin_dir = config.get(
            "cluster",
            "slurm_bin_dir",
            fallback="",
        ).strip()

        if slurm_bin_dir:
            slurm_bin_dir = os.path.expandvars(
                os.path.expanduser(slurm_bin_dir)
            )

        self.slurm_bin_dir = slurm_bin_dir

    def command_path(self, command):
        """
        Return the full path to a Slurm command if a command directory
        is configured; otherwise return the command name.
        """
        if self.slurm_bin_dir:
            return os.path.join(
                self.slurm_bin_dir,
                command,
            )

        return command

    def run(self, command, args=None):
        """
        Run a Slurm command safely without a shell.
        """
        if args is None:
            args = []

        cmd = [self.command_path(command)]
        cmd.extend(str(arg) for arg in args)

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=False,
            )
        except OSError as exc:
            raise SlurmError(
                "Unable to execute '{}': {}".format(
                    " ".join(cmd),
                    exc,
                )
            )

        if result.returncode != 0:
            raise SlurmError(
                "Slurm command failed (exit {}): {}\n{}".format(
                    result.returncode,
                    " ".join(cmd),
                    result.stderr.strip(),
                )
            )

        return result.stdout

    def sacct_jobs(
        self,
        user=None,
        all_users=False,
        jobs=None,
        start=None,
        end=None,
        state=None,
    ):
        """
        Query Slurm accounting records through sacct.

        The Slurm -s option provides the initial filtering.
        A second filtering pass is performed locally to ensure
        that only records matching the requested state are returned.
        """

        fields = [
            "User",
            "JobID",
            "JobName",
            "Partition",
            "State",
            "Start",
            "Elapsed",
            "MaxRSS",
            "MaxVMSize",
            "NNodes",
            "NCPUS",
            "NodeList",
            "CPUTime",
            "SystemCPU",
            "TotalCPU",
            "UserCPU",
            "ReqMem",
            "MaxDiskWrite",
            "MaxDiskRead",
        ]

        args = [
            "-n",
            "-P",
            "--format={}".format(",".join(fields)),
        ]

        if user:
            args.extend(["-u", user])
        elif all_users:
            args.append("-a")

        if jobs:
            args.extend(["-j", jobs])

        if start:
            args.extend(["-S", start])

        if end:
            args.extend(["-E", end])

        if state:
            args.extend(["-s", state])

        result = self.run("sacct", args)

        if not state:
            return result

        requested_state = str(state).strip().upper()

        state_map = {
            "R": "RUNNING",
            "PD": "PENDING",
            "CF": "CONFIGURING",
            "CG": "COMPLETING",
            "CD": "COMPLETED",
            "F": "FAILED",
            "TO": "TIMEOUT",
            "CA": "CANCELLED",
            "NF": "NODE_FAIL",
            "PR": "PREEMPTED",
            "OOM": "OUT_OF_MEMORY",
            "S": "SUSPENDED",
            "ST": "STOPPED",
        }

        expected_state = state_map.get(
            requested_state,
            requested_state,
        )

        filtered_lines = []

        for line in result.splitlines():
            if not line.strip():
                continue

            fields_in_line = line.split("|")

            if len(fields_in_line) < 5:
                continue

            actual_state = fields_in_line[4].strip().upper()

            # Slurm may append '+' to a state.
            actual_base_state = actual_state.rstrip("+")

            if actual_base_state == expected_state:
                filtered_lines.append(line)

        return "\n".join(filtered_lines)

    def sinfo_nodes(self):
        """
        Return node information from sinfo.

        Delimiter is | to make parsing robust.
        """
        fields = [
            "%N",
            "%O",
            "%c",
            "%m",
            "%e",
            "%a",
            "%t",
            "%E",
            "%G",
        ]

        args = [
            "-a",
            "--Node",
            "-o",
            "|".join(fields),
        ]

        return self.run("sinfo", args)

    def squeue_node(self, node):
        """
        Return running jobs associated with a node.
        """
        fields = [
            "%i",
            "%P",
            "%j",
            "%u",
            "%M",
            "%l",
            "%C",
            "%D",
            "%R",
        ]

        args = [
            "-t",
            "R",
            "-h",
            "-w",
            node,
            "-o",
            "|".join(fields),
        ]

        return self.run("squeue", args)

    def scontrol_job(self, job_id):
        """
        Return detailed information for a job.
        """
        return self.run(
            "scontrol",
            ["show", "job", str(job_id)],
        )

    def job_batch_flag(self, job_id):
        """
        Return BatchFlag from scontrol.

        BatchFlag >= 1 normally indicates a batch job.
        """
        output = self.scontrol_job(job_id)

        for token in output.split():
            if token.startswith("BatchFlag="):
                try:
                    return int(token.split("=", 1)[1])
                except ValueError:
                    return 0

        return 0

    def job_mail_user(self, job_id):
        """
        Return MailUser from scontrol if configured.
        """
        output = self.scontrol_job(job_id)

        for token in output.split():
            if token.startswith("MailUser="):
                return token.split("=", 1)[1].strip()

        return ""

    def passwd_email(self, username):
        """
        Try to determine a user's email address from getent.

        This uses getent directly instead of a shell pipeline.
        """
        try:
            result = subprocess.run(
                [
                    "getent",
                    "passwd",
                    username,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=False,
            )
        except OSError:
            return ""

        if result.returncode != 0:
            return ""

        line = result.stdout.strip()

        if not line:
            return ""

        # This preserves the behavior of the original script:
        # inspect the GECOS field for an email-looking address.
        fields = line.split(":")

        if len(fields) < 5:
            return ""

        gecos = fields[4]

        for part in gecos.split(","):
            part = part.strip()

            if "@" in part and " " not in part:
                return part

        return ""

