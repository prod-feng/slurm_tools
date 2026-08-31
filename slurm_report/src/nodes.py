#!/usr/bin/env python3

import subprocess


class NodeInfo(object):
    def __init__(
        self,
        node,
        load,
        cpus,
        avail="",
        state="",
        reason="",
        utilization=0.0,
        job_id="",
        partition="",
        job_name="",
        user="",
        elapsed="",
    ):
        self.node = node
        self.load = load
        self.cpus = cpus
        self.avail = avail
        self.state = state
        self.reason = reason
        self.utilization = utilization
        self.job_id = job_id
        self.partition = partition
        self.job_name = job_name
        self.user = user
        self.elapsed = elapsed


class SlurmNodeReporter(object):

    def __init__(
        self,
        low_threshold=10.0,
        high_threshold=110.0,
        slurm_bin="/cm/shared/apps/slurm/current/bin",
    ):
        self.low_threshold = float(low_threshold)
        self.high_threshold = float(high_threshold)
        self.slurm_bin = slurm_bin

    def _run_command(self, command):
        """
        Execute a command and return stdout.

        Raises RuntimeError if the command cannot be executed
        or returns a non-zero exit code.
        """

        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )

            stdout, stderr = process.communicate()

        except OSError as exc:
            raise RuntimeError(
                "Failed to execute Slurm command: {}".format(exc)
            )

        if process.returncode != 0:
            raise RuntimeError(
                "Slurm command failed: {}\n{}".format(
                    " ".join(command),
                    stderr.strip(),
                )
            )

        return stdout

    def _get_node_data(self):
        """
        Get node utilization and state information with ONE
        sinfo call.

        Multiple sinfo records for the same physical node are
        collapsed into one record.

        Returns:
            list of NodeInfo objects
        """

        sinfo = "{}/sinfo".format(self.slurm_bin)

        command = [
            sinfo,
            "-a",
            "--Node",
            "-o",
            "%N|%O|%c|%a|%T|%E",
        ]

        result = self._run_command(command)

        nodes_by_name = {}

        for line in result.splitlines():
            line = line.strip()

            if not line:
                continue

            parts = line.split("|", 5)

            if len(parts) != 6:
                continue

            node = parts[0]
            load_text = parts[1]
            cpus_text = parts[2]
            avail = parts[3]
            state = parts[4]
            reason = parts[5]

            if not node:
                continue

            # Ignore GPU nodes such as A100 nodes.
            if "a100" in node.lower():
                continue

            try:
                load = float(load_text)
            except ValueError:
                continue

            try:
                cpus = int(cpus_text)
            except ValueError:
                continue

            if cpus <= 0:
                continue

            utilization = (load / float(cpus)) * 100.0

            node_info = NodeInfo(
                node=node,
                load=load,
                cpus=cpus,
                avail=avail,
                state=state,
                reason=reason,
                utilization=utilization,
            )

            # sinfo can return multiple records for the same
            # physical node. Keep one record per node.
            #
            # If duplicates have different loads, retain the
            # record with the highest load.
            if node not in nodes_by_name:
                nodes_by_name[node] = node_info
            else:
                if load > nodes_by_name[node].load:
                    nodes_by_name[node] = node_info

        return list(nodes_by_name.values())

    def _get_running_jobs(self):
        """
        Get all running jobs with ONE squeue call.

        Returns:
            dictionary mapping node name to job information.
        """

        squeue = "{}/squeue".format(self.slurm_bin)

        command = [
            squeue,
            "-t",
            "R",
            "-h",
            "-o",
            "%i|%P|%j|%u|%M|%D|%N",
        ]

        result = self._run_command(command)

        jobs_by_node = {}

        for line in result.splitlines():
            line = line.strip()

            if not line:
                continue

            parts = line.split("|", 6)

            if len(parts) != 7:
                continue

            job_id = parts[0]
            partition = parts[1]
            job_name = parts[2]
            user = parts[3]
            elapsed = parts[4]
            node_count = parts[5]
            node_list = parts[6]

            try:
                node_count = int(node_count)
            except ValueError:
                node_count = 1

            # Ignore Open OnDemand jobs.
            if "sys/dashboard" in job_name:
                continue

            if not node_list:
                continue

            # Expand compressed Slurm node lists.
            nodes = self._expand_nodelist(node_list)

            for node in nodes:
                jobs_by_node[node] = {
                    "job_id": job_id,
                    "partition": partition,
                    "job_name": job_name,
                    "user": user,
                    "elapsed": elapsed,
                    "node_count": node_count,
                }

        return jobs_by_node

    def _expand_nodelist(self, node_list):
        """
        Expand common Slurm node-list syntax.

        Examples:

            dg001
            dg[001-004]
            dg[001,003-005]

        This handles the common numeric Slurm node-list
        formats used by the cluster.
        """

        if not node_list:
            return []

        if "[" not in node_list:
            return [node_list]

        prefix = node_list.split("[", 1)[0]
        inside = node_list.split("[", 1)[1]

        if "]" not in inside:
            return [node_list]

        inside = inside.split("]", 1)[0]

        result = []

        for item in inside.split(","):

            if "-" in item:
                start, end = item.split("-", 1)

                try:
                    start_num = int(start)
                    end_num = int(end)
                except ValueError:
                    result.append(prefix + item)
                    continue

                width = max(
                    len(start),
                    len(end),
                )

                for number in range(
                    start_num,
                    end_num + 1,
                ):
                    result.append(
                        "{}{:0{}d}".format(
                            prefix,
                            number,
                            width,
                        )
                    )

            else:
                result.append(prefix + item)

        return result

    def get_nodes(self):
        """
        Retrieve node information and associate running jobs.

        Only TWO Slurm commands are executed:

            1. sinfo
            2. squeue
        """

        nodes = self._get_node_data()
        jobs_by_node = self._get_running_jobs()

        for node in nodes:
            job = jobs_by_node.get(node.node)

            if job is None:
                continue

            node.job_id = job["job_id"]
            node.partition = job["partition"]
            node.job_name = job["job_name"]
            node.user = job["user"]
            node.elapsed = job["elapsed"]

        return nodes

    def print_overview(self, nodes):
        """
        Print all nodes.
        """

        self._print_header()

        for node in sorted(
            nodes,
            key=lambda item: item.node,
        ):
            self._print_node(node)

    def print_alerts(self, nodes):
        """
        Print only nodes that are either:

            utilization < low_threshold

        or:

            utilization > high_threshold
        """

        alerts = []

        for node in nodes:

            if node.utilization < self.low_threshold:
                alerts.append(node)

            elif node.utilization > self.high_threshold:
                alerts.append(node)

        self._print_header()

        for node in sorted(
            alerts,
            key=lambda item: item.utilization,
        ):
            self._print_node(node)

    def _print_header(self):
        """
        Print the node report header.
        """

        header = (
            "{:<13} "
            "{:>6} "
            "{:>6} "
            "{:<7} "
            "{:<12} "
            "{:<18} "
            "{:<10} "
            "{:<16} "
            "{:<20} "
            "{:<9} "
            "{:<10}"
        ).format(
            "Node",
            "Util%",
            "CPUs",
            "AVAIL",
            "STATE",
            "REASON",
            "JobID",
            "Partition",
            "JobName",
            "User",
            "Elapsed",
        )

        print(header)
        print("-" * len(header))

    def _print_node(self, node):
        """
        Print one node.

        Utilization is displayed as a number without the
        percent sign, as requested.
        """

        print(
            "{:<13.13} "
            "{:>6.1f} "
            "{:>6} "
            "{:<7.7} "
            "{:<12.12} "
            "{:<18.18} "
            "{:<10.10} "
            "{:<16.16} "
            "{:<20.20} "
            "{:<9.9} "
            "{:<10.10}".format(
                node.node,
                node.utilization,
                node.cpus,
                node.avail,
                node.state,
                node.reason,
                node.job_id,
                node.partition,
                node.job_name,
                node.user,
                node.elapsed,
            )
        )


def run_node_report(args, config):
    """
    Run the node performance report.

    The configuration is supplied as a ConfigParser object.
    """

    low_threshold = 10.0
    high_threshold = 110.0

    if config.has_option(
        "nodes",
        "low_threshold",
    ):
        low_threshold = config.getfloat(
            "nodes",
            "low_threshold",
        )

    if config.has_option(
        "nodes",
        "high_threshold",
    ):
        high_threshold = config.getfloat(
            "nodes",
            "high_threshold",
        )

    mode = getattr(
        args,
        "mode",
        "overview",
    )

    reporter = SlurmNodeReporter(
        low_threshold=low_threshold,
        high_threshold=high_threshold,
    )

    nodes = reporter.get_nodes()

    if mode == "alert":
        reporter.print_alerts(nodes)

    else:
        reporter.print_overview(nodes)

    return 0

