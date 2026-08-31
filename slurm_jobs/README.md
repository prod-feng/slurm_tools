# Slurm Job & Node Performance Report

A lightweight Python 3 tool for monitoring **Slurm job and compute-node performance**.

The project is designed for HPC environments where administrators and users need a simple way to identify:

- Jobs that are under-utilizing allocated resources
- Compute nodes with low CPU utilization
- Compute nodes with unusually high CPU utilization
- Node availability and state
- Slurm reasons for nodes being unavailable
- Jobs currently running on individual nodes

The project is intentionally organized into a few larger functional modules rather than many small files.

## Features

### Job performance

The `jobs` command reports resource utilization for Slurm jobs.

It can be used to identify jobs that are:

- Under-utilizing CPUs
- Under-utilizing memory
- Generating high or unusual disk I/O
- Using resources inefficiently

Example:

```text
./slurm_report jobs
```

### Node performance

The `nodes` command provides an overview of compute-node performance.

Example:

```text
./slurm_report nodes
```

The node overview includes:

- Node name
- CPU utilization
- Number of CPUs
- Availability
- Node state
- Slurm reason
- Running job ID
- Partition
- Job name
- User
- Elapsed time

Example output:

```text
Node          Util%   CPUs AVAIL   STATE        REASON             JobID      Partition        JobName              User      Elapsed
------------------------------------------------------------------------------------------------------------------------------------
xm094           3.1     96 up      allocated    None               2137608    medium-96core    Regge1_lambdaPi_     feng      9:47:53
xm095          87.4     96 up      allocated    None               2137612    medium-96core    SomeJob              user1     4:12:10
xm096         123.8     96 up      allocated    None               2137615    medium-96core    AnotherJob           user2     6:31:22
```

## Node Alert Mode

The node command also supports an alert mode.

By default, nodes are considered to require attention when:

- CPU utilization is below 10%
- CPU utilization is above 110%

Run:

```text
./slurm_report nodes alert
```

This is useful for quickly finding:

- Under-performing nodes
- Potentially over-subscribed nodes
- Nodes where CPU load is unexpectedly high

The thresholds can be changed in the configuration file.

## Command Structure

The command-line interface is intentionally simple:

```text
./slurm_report jobs
./slurm_report nodes
```

The first argument selects the type of report.

### Job report

```text
./slurm_report jobs
```

### Node overview

```text
./slurm_report nodes
```

### Node alerts

```text
./slurm_report nodes alert
```

This structure makes it easy to add additional report types in the future, for example:

```text
./slurm_report jobs
./slurm_report nodes
./slurm_report partitions
./slurm_report users
```

## Project Structure

The project uses a small number of larger functional modules:

```text
slurm-job-report/
├── slurm_report
├── slurm_report.conf
├── job_report.html
└── src/
    ├── __init__.py
    ├── jobs.py
    ├── nodes.py
    ├── report.py
    ├── slurm.py
    ├── utils.py
    └── emailer.py
```

### `slurm_report`

The main executable command.

Examples:

```text
./slurm_report jobs
./slurm_report nodes
./slurm_report nodes alert
```

### `src/slurm.py`

Contains common Slurm-related functionality.

This module provides the interface used by the report modules to interact with Slurm.

### `src/jobs.py`

Contains job-performance reporting functionality.

It is responsible for collecting and processing job resource information.

### `src/nodes.py`

Contains node-performance reporting functionality.

It provides:

- Node overview
- Node alert detection
- CPU utilization calculation
- Node state information
- Node availability
- Node reason
- Running-job information

The node implementation is optimized to avoid running a Slurm command separately for every node.

### `src/report.py`

Contains report-generation functionality shared by the application.

### `src/utils.py`

Contains common utility functions such as configuration loading and other reusable helpers.

### `src/emailer.py`

Contains email functionality.

Email handling is kept separate from report-generation code so that email settings and delivery logic can be changed without modifying the reporting modules.

### `job_report.html`

HTML email template used for job reports.

The email body can be changed independently of the Python code.

### `slurm_report.conf`

Application configuration file.

It contains configurable settings such as:

- Node alert thresholds
- Email settings
- Sender address
- CC addresses
- BCC addresses
- Email subject
- Other application settings

## Configuration

The application uses a text-based configuration file:

```text
slurm_report.conf
```

A typical configuration may look like:

```ini
[nodes]
low_threshold = 10
high_threshold = 110

[email]
sender = admin@example.com
cc =
bcc =
subject = Slurm Job Performance Report
```

### Node thresholds

The node alert thresholds are configurable.

```ini
[nodes]
low_threshold = 10
high_threshold = 110
```

For example:

```text
./slurm_report nodes alert
```

will report nodes with:

```text
Utilization < 10%
```

or:

```text
Utilization > 110%
```

If the thresholds are changed to:

```ini
[nodes]
low_threshold = 20
high_threshold = 100
```

then alert mode will report nodes below 20% or above 100%.

## Email Configuration

Email configuration is intentionally kept outside of the Python source code.

This makes it possible to change email settings without modifying `emailer.py`.

Example:

```ini
[email]
sender = admin@example.com
cc =
bcc =
subject = Slurm Node Performance Report
```

### Sender

The sender address can be configured with:

```ini
sender = admin@example.com
```

### CC

Multiple CC addresses can be specified according to the configuration format used by the application.

If no CC addresses are configured, leave the value empty:

```ini
cc =
```

### BCC

BCC is also optional.

If no BCC addresses are required:

```ini
bcc =
```

### Subject

The email subject can be configured without changing Python code:

```ini
subject = Slurm Job Performance Report
```

This allows different deployments to use their own email subjects.

## HTML Email Template

The job report email uses an external HTML template:

```text
job_report.html
```

The template can contain normal HTML together with Python variable substitution.

This keeps the email presentation separate from the email-sending code.

For example, the Python code can supply values such as:

```text
user
job_id
node
utilization
partition
job_name
```

while the HTML template controls how those values are displayed.

This makes it easy to modify the appearance of the email without changing the reporting logic.

## Python Version

The project is designed to support **Python 3.6**.

It intentionally does not depend on the `dataclasses` module because `dataclasses` was introduced into the Python standard library in Python 3.7.

The implementation therefore uses regular Python classes and other Python 3.6-compatible features.

Check the Python version:

```text
python3 --version
```

Example:

```text
Python 3.6.x
```

## Slurm Requirements

The application requires access to the standard Slurm commands used for querying jobs and nodes.

The node-performance module uses:

```text
sinfo
squeue
```

The actual Slurm installation path can be configured through the application configuration or the implementation's Slurm path settings.

The default environment used by this project is:

```text
/cm/shared/apps/slurm/current/bin
```

If your cluster uses a different installation path, update the corresponding configuration or source setting.

## Node Performance Implementation

The node report is designed to be fast even on large clusters.

A naive implementation would run:

```text
squeue
```

once for every node.

For a cluster with hundreds or thousands of nodes, this can be very slow.

Instead, the current implementation retrieves the information in two main Slurm calls:

```text
1. sinfo
2. squeue
```

The Python code then joins the node and job information in memory.

Conceptually:

```text
sinfo ──────────────┐
                    ├──> Python ──> Node Report
squeue ─────────────┘
```

This avoids executing `squeue` separately for every node.

## Duplicate Nodes

Some Slurm configurations can cause `sinfo` to return multiple records for the same physical node.

The node reporter explicitly deduplicates nodes by node name.

For example, if Slurm returns:

```text
xm094
xm094
xm094
xm094
```

the report will contain only:

```text
xm094
```

If multiple records for a node have different load values, the implementation keeps the record with the highest load.

## Node Utilization

CPU utilization is calculated using the Slurm CPU load and CPU count:

```text
Utilization = CPU Load / CPU Count × 100
```

For example, if a node has:

```text
CPU Load = 3.1
CPUs     = 96
```

the utilization is approximately:

```text
3.1 / 96 × 100 = 3.23%
```

The report displays the numeric value without the `%` character:

```text
3.2
```

This makes the output easier to parse and process with other command-line tools.

## Availability, State and Reason

The node overview also displays Slurm node status information:

- `AVAIL` — whether the node is available
- `STATE` — current Slurm node state
- `REASON` — reason reported by Slurm when applicable

For example:

```text
Node          Util%   CPUs AVAIL   STATE        REASON
xm094           3.1     96 up      allocated    None
```

Long values are truncated in the terminal output to keep the report readable.

## Ignored Nodes

The current implementation ignores nodes whose name contains:

```text
a100
```

This behavior was inherited from the original node-performance script.

If GPU nodes should be included in the future, this filtering can be removed or moved into the configuration file.

## Typical Usage

### Check all node performance

```text
./slurm_report nodes
```

### Find under-performing or over-subscribed nodes

```text
./slurm_report nodes alert
```

### Generate a job report

```text
./slurm_report jobs
```

### Display command help

```text
./slurm_report --help
```

Depending on the command-line parser configuration, command-specific help may also be available with:

```text
./slurm_report jobs --help
./slurm_report nodes --help
```

## Example Workflow

A typical HPC administrator workflow might be:

```text
# Check overall node performance
./slurm_report nodes

# Check only nodes that need attention
./slurm_report nodes alert

# Check job resource utilization
./slurm_report jobs
```

The overview provides a complete picture, while alert mode provides a quick way to focus on potential problems.

## Design Goals

The project follows several simple design principles.

### Keep the number of modules small

Instead of splitting every function into a separate Python file, related functionality is grouped together.

The major functional areas are:

```text
Slurm interaction
Job reporting
Node reporting
Report generation
Utilities
Email
```

### Keep email separate

Email delivery is isolated in `emailer.py`.

This allows email settings and delivery behavior to evolve independently from report-generation logic.

### Keep presentation separate

HTML email formatting is stored in `job_report.html`.

The Python code provides the data, while the HTML template controls presentation.

### Keep configuration outside the source code

Operational settings such as email addresses and node thresholds belong in:

```text
slurm_report.conf
```

This avoids changing Python source code for normal administrative configuration changes.

### Optimize expensive Slurm operations

The node report avoids running one Slurm command per node.

This is especially important on large HPC clusters.

## Future Extensions

The command structure is intentionally designed to make additional report types easy to add.

Possible future commands include:

```text
./slurm_report jobs
./slurm_report nodes
./slurm_report users
./slurm_report partitions
./slurm_report queues
```

Possible future node features include:

- GPU utilization
- Memory utilization
- Network utilization
- Disk I/O
- Temperature
- More detailed node health information
- Configurable node exclusions
- Different alert thresholds by partition

Possible future job features include:

- GPU utilization
- Memory efficiency
- CPU efficiency
- Job-level I/O statistics
- Historical utilization
- Email alerts

## Troubleshooting

### `ModuleNotFoundError: No module named 'dataclasses'`

The project is designed for Python 3.6 and does not require the `dataclasses` package.

Make sure the current source tree does not contain:

```text
from dataclasses import dataclass
```

Check your Python version:

```text
python3 --version
```

### `ImportError: cannot import name 'run_node_report'`

Make sure `src/nodes.py` contains:

```text
def run_node_report(args, config):
```

Also make sure the main executable imports it from the correct module.

### Slurm command not found

Verify that the configured Slurm directory contains:

```text
sinfo
squeue
sacct
scontrol
```

For example:

```text
ls /cm/shared/apps/slurm/current/bin/
```

If Slurm is installed somewhere else, update the corresponding configuration.

### Node report is empty

Try the underlying Slurm command directly:

```text
/cm/shared/apps/slurm/current/bin/sinfo -a --Node
```

Then verify that the account running `slurm_report` has permission to query the cluster.

## License

Add the project's license information here.

For example:

```text
MIT License
```

or replace this section with the license appropriate for your organization.

## Author / Maintainer

Add project author or HPC support team information here.

## Summary

`slurm_report` provides a simple command-line interface for Slurm performance monitoring:

```text
./slurm_report jobs
./slurm_report nodes
./slurm_report nodes alert
```

The project emphasizes simplicity, reuse, and expandability while avoiding unnecessary fragmentation into many small modules.
```
