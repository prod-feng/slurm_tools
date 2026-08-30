# Slurm Job Performance Report

A lightweight Python3 tool for generating CPU, memory, disk I/O, and resource-utilization reports for Slurm jobs.

The tool uses `sacct` to collect completed-job information, calculates useful resource-utilization metrics, generates human-readable or CSV reports, and can optionally send personalized HTML email reports to users.

The project is intentionally organized into a few larger functional modules rather than many small files.

## Features

- Query Slurm jobs using `sacct`
- Select jobs by user, multiple users, all users, or job ID
- Select jobs by time range
- Select jobs by Slurm state
- Randomly select a limited number of jobs for reports
- Calculate CPU hours and CPU utilization
- Calculate user CPU and system CPU time
- Report maximum memory usage
- Report requested memory
- Report disk read and disk write
- Generate human-readable reports
- Generate CSV reports
- Generate personalized HTML email reports
- Configure email sender, CC, BCC, and subject through a configuration file
- Customize the complete email body through an external HTML template
- No third-party Python packages required
- Designed for HPC/Slurm environments
- Simple modular structure for easy maintenance and reuse

## Project Structure

```text
slurm-job-report/
├── slurm_job-report.py
├── job_report.conf
├── job_report.html
│
└── src/
    ├── __init__.py
    ├── slurm.py
    ├── report.py
    ├── utils.py
    └── emailer.py
```

## Requirements

- Python 3
- Slurm `sacct`
- System `mail` command for email functionality

No third-party Python packages are required.

Check Python:

```bash
python3 --version
```

Check Slurm:

```bash
which sacct
```

Check the mail command:

```bash
which mail
```

## Installation

Clone the repository:

```bash
git clone <repository-url>
cd slurm-job-report
```

Make the main program executable:

```bash
chmod +x slurm_job-report.py
```

Run the program:

```bash
./slurm_job-report.py
```

Or:

```bash
python3 slurm_job-report.py
```

## Usage

### Command Syntax

```text
./slurm_job-report.py [OPTIONS]
```

### Command-Line Options

| Option | Description |
|---|---|
| `-u`, `--user USER` | Use a comma-separated list of UIDs or usernames |
| `--allusers` | Display all users' jobs |
| `-j`, `--jobs JOBS` | Display specified job or job-step IDs |
| `-S`, `--starttime STARTTIME` | Select jobs after the specified time |
| `-E`, `--endtime ENDTIME` | Select jobs before the specified time |
| `-n`, `--njobs NJOBS` | Number of randomly selected jobs |
| `-s`, `--state STATE` | Select jobs in a specific Slurm state |
| `--allstates` | Display jobs in all states |
| `--csv` | Generate CSV output |
| `--emailusers` | Send email reports to users |

## Usage Examples

### Report Your Own Completed Jobs

```bash
./slurm_job-report.py
```

By default, the program uses the current user and selects completed jobs.

### Report Jobs for a Specific User

```bash
./slurm_job-report.py --user jsmith
```

Or:

```bash
./slurm_job-report.py -u jsmith
```

### Report Jobs for Multiple Users

```bash
./slurm_job-report.py --user jsmith,adoe,bsmith
```

### Report Jobs for All Users

```bash
./slurm_job-report.py --allusers
```

### Report a Specific Job

```bash
./slurm_job-report.py --jobs 123456
```

Or:

```bash
./slurm_job-report.py -j 123456
```

### Report Multiple Jobs

```bash
./slurm_job-report.py --jobs 123456,123457,123458
```

### Report a Specific Job Step

```bash
./slurm_job-report.py --jobs 123456.batch
```

## Time Range Examples

### Specify a Start Time

```bash
./slurm_job-report.py --starttime 2026-08-01T00:00:00
```

Short form:

```bash
./slurm_job-report.py -S 2026-08-01T00:00:00
```

### Specify an End Time

```bash
./slurm_job-report.py --endtime 2026-08-30T23:59:59
```

Short form:

```bash
./slurm_job-report.py -E 2026-08-30T23:59:59
```

### Specify Both Start and End Times

```bash
./slurm_job-report.py \
    --starttime 2026-08-01T00:00:00 \
    --endtime 2026-08-31T23:59:59
```

## Slurm State Examples

### Completed Jobs

The default state is `CD`:

```bash
./slurm_job-report.py --state CD
```

### Failed Jobs

```bash
./slurm_job-report.py --state F
```

### Cancelled Jobs

```bash
./slurm_job-report.py --state CA
```

### All States

```bash
./slurm_job-report.py --allstates
```

## Number of Jobs

### Report Up to 10 Random Jobs

The default is up to 10 randomly selected jobs per user:

```bash
./slurm_job-report.py
```

### Report Up to 20 Random Jobs

```bash
./slurm_job-report.py --njobs 20
```

### Report Only 5 Random Jobs

```bash
./slurm_job-report.py --user jsmith --njobs 5
```

## CSV Reports

### Generate a CSV Report

```bash
./slurm_job-report.py --csv
```

Save the output:

```bash
./slurm_job-report.py --user jsmith --csv > jobs.csv
```

### Generate CSV for All Users

```bash
./slurm_job-report.py --allusers --csv > all_jobs.csv
```

CSV mode reports all selected jobs rather than randomly selecting a limited number.

### Generate CSV for a Time Range

```bash
./slurm_job-report.py \
    --allusers \
    -S 2026-08-01T00:00:00 \
    -E 2026-08-31T23:59:59 \
    --csv > august_jobs.csv
```

## Email Reports

### Send a Report to Users

```bash
./slurm_job-report.py --emailusers
```

### Send Reports to All Users

```bash
./slurm_job-report.py --allusers --emailusers
```

Each user receives a personalized report.

### Send Reports for a Specific Time Range

```bash
./slurm_job-report.py \
    --allusers \
    -S 2026-08-29T00:00:00 \
    -E 2026-08-30T23:59:59 \
    --emailusers
```

### Send Reports with 20 Jobs per User

```bash
./slurm_job-report.py \
    --allusers \
    --njobs 20 \
    --emailusers
```

## Email Configuration

Email configuration is stored in:

```text
job_report.conf
```

Example:

```ini
[email]

sender = hpc_support@example.edu

cc =
bcc = admin@example.edu

subject = Summary report of your computing jobs performance
```

### Sender

```ini
sender = hpc_support@example.edu
```

### CC

Multiple addresses can be separated by commas:

```ini
cc = admin@example.edu, support@example.edu
```

If no CC is required:

```ini
cc =
```

### BCC

```ini
bcc = manager@example.edu, audit@example.edu
```

If no BCC is required:

```ini
bcc =
```

### Subject

The subject can be changed without modifying Python:

```ini
subject = HPC Job Performance Report
```

## Email Template

The complete email body is stored in:

```text
job_report.html
```

The template uses Python-style variable substitution.

Example:

```html
<html>
<body style="font-family: sans-serif, Arial, Helvetica;">

<p>Dear {user},</p>

<p>
Here is your Slurm job performance report.
</p>

{job_info}

<p>
You had {total_jobs} completed jobs.
</p>

</body>
</html>
```

The HTML template can be modified without changing the Python code.

### Template Variables

| Variable | Description |
|---|---|
| `{user}` | Slurm username |
| `{num_report}` | Number of jobs included in the report |
| `{total_jobs}` | Total number of jobs found |
| `{job_info}` | Formatted job-performance report |

## Report Metrics

### CPUhours

CPU hours are approximately:

```text
CPUhours = Elapsed Time × CPU Count
```

For example, a job running for 2 hours using 40 CPUs consumes:

```text
2 × 40 = 80 CPU hours
```

### CPUUsage

CPU utilization is estimated as:

```text
CPUUsage = UserCPU / CPUhours
```

A value closer to `1.0` generally indicates better utilization of allocated CPU resources.

A significantly lower value may indicate that the job requested more CPU resources than it actually used.

Possible causes include:

- Inefficient MPI configuration
- Too many allocated CPUs
- Insufficient parallelism
- I/O bottlenecks
- Threading configuration problems
- Serial portions of the application

### CPUSYST

CPU time spent in system/OS activity.

A relatively high value may warrant investigation into:

- Heavy I/O
- Excessive process activity
- System-level overhead
- Oversubscription
- Other resource-intensive operations

### CPUUSER

CPU time attributed to the user's computation.

Higher values generally indicate that more CPU time was spent doing application work.

### MemUsed

Maximum memory usage reported by Slurm.

Depending on the Slurm configuration and application behavior, memory usage may occasionally be reported multiple times for multithreaded jobs.

A rough estimate may sometimes be:

```text
Corrected MemUsed ≈ MemUsed / nCPUs
```

This is only an estimate.

### MemAsked

Memory requested by the job through Slurm.

Comparing `MemUsed` with `MemAsked` can help identify jobs that consistently request substantially more memory than they actually use.

### DiskRead and DiskWrite

Reported disk I/O associated with the job.

Large values may help identify jobs that could benefit from optimization of:

- File access patterns
- Temporary files
- Parallel I/O
- Data staging
- Storage layout

## GPU Jobs

The current implementation intentionally ignores jobs running in partitions containing:

```text
a100
```

This filtering is implemented in:

```text
src/report.py
```

Additional GPU partitions can be added to the filtering logic if needed.

## Customization

Most customization can be performed without modifying the Python source code.

### Change Email Wording

Edit:

```text
job_report.html
```

### Change Email Sender

Edit:

```text
job_report.conf
```

### Change CC/BCC

Edit:

```text
job_report.conf
```

### Change Email Subject

Edit:

```text
job_report.conf
```

### Change Slurm Query

Modify:

```text
src/slurm.py
```

### Change Report Calculations

Modify:

```text
src/report.py
```

### Change Unit Conversion

Modify:

```text
src/utils.py
```

### Change Email Delivery

Modify:

```text
src/emailer.py
```

## Troubleshooting

### `sacct: command not found`

Check:

```bash
which sacct
```

If your system uses environment modules, load the appropriate Slurm module.

For example:

```bash
module load slurm
```

Then:

```bash
which sacct
```

### No Jobs Found

Try a broader time range:

```bash
./slurm_job-report.py \
    -S 2026-08-01T00:00:00 \
    -E 2026-08-30T23:59:59
```

Also check whether state filtering is the reason:

```bash
./slurm_job-report.py --allstates
```

### Email Is Not Sent

Check that `mail` exists:

```bash
which mail
```

Also verify:

```text
job_report.conf
```

Check the sender, CC, BCC, and subject configuration.

## Design Philosophy

The project intentionally uses a small number of functional modules.

The goal is to avoid both extremes.

### Too Little Structure

```text
One huge Python script
```

This becomes difficult to maintain as functionality grows.

### Too Much Structure

```text
Many tiny modules with one function each
```

This can make a relatively simple application unnecessarily complicated.

### Current Structure

Functionality is grouped according to its natural responsibility:

```text
Slurm         → src/slurm.py
Reporting     → src/report.py
Utilities     → src/utils.py
Email         → src/emailer.py
Application   → slurm_job-report.py
Configuration → job_report.conf
Template      → job_report.html
```

This keeps the application modular while remaining easy to understand and reuse.

## Security Considerations

External commands should preferably be executed using argument lists rather than constructing shell command strings from user input.

Avoid:

```python
subprocess.getoutput("sacct -u " + user)
```

Prefer:

```python
subprocess.run(
    ["sacct", "-u", user],
    ...
)
```

This prevents shell interpretation of user-provided values.

The same principle should be applied to other external commands.

## Future Improvements

Potential future enhancements include:

- Support for additional Slurm metrics
- Better GPU utilization reporting
- Configurable ignored partitions
- Configurable fallback email address
- Configurable report selection strategy
- HTML tables instead of `<pre>` formatted reports
- Optional logging to a file
- Dry-run email mode
- SMTP support
- Automated scheduled reports
- Unit tests
- Configurable CPU-utilization thresholds
- Highlighting jobs with particularly low CPU utilization
- Summary statistics across all users
- Historical report generation
- JSON output
- Configurable report columns

## License
```text
MIT License
```
