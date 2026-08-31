# slurm_tools

This is a collection of the Slurm tools which are useful.

Some original standalone python scripts are in legacy/ folder.

## slurm_accounts 
Lists Slurm accounts, users, and association, providing advanced filtering function.

## slurp_report
View past or running jobs' performance, and node-based performance. CPU only.


## gpu_job_monitor.py

It is supposed to run in CRONTAB jobs, every 10 minuets. Sampling a jobs GPU utilization,if all 6 samples are all below, say 20%, then send email to this user.

```
usage: gpu_job_monitor.py  [-h] [-v] [--debug] [--dry-run] [--state-file STATE_FILE] [--lock-file LOCK_FILE]

Monitor GPU utilization of Slurm jobs

options:
  -h, --help            show this help message and exit
  -v, --verbose         Show informational monitor messages
  --debug               Show DEBUG and INFO messages
  --dry-run             Do not send email; display email content instead
  --state-file STATE_FILE
                        Persistent state file (default: /tmp/jobstats.json)
  --lock-file LOCK_FILE
                        Process lock file (default: /tmp/jobstats.lock)
```

The result is like:

```
>gpu_job_monitor.py --dry-run
JOBID      USER         NODE             GPUS           AVG     MIN     MAX  IDLE STATE               BAD
---------- ------------ ---------------- ---------- ------- ------- ------- ----- ------------------ ----
85248      Feng    b40x4-09         0             0.0%      0%      0%     1 LOW_UTILIZATION           5
85167      feng    b40x4-08         1             0.0%      0%      0%     1 LOW_UTILIZATION           6
85249      Zhang   b40x4-09         1             0.0%      0%      0%     1 LOW_UTILIZATION           5
84117      zhang   b40x4-02         0-3          94.8%     92%     97%     0 NORMAL                    0

```

You can ue the --dry-run mode to run it interactively, not triggering to track the jobs states.


