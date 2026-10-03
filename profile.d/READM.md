## slurm_efficiency_motd.sh

Copy to  /etc/profile.d/ folder. Print's last finished job's metrics to users after they SSH login.

```
╭─ Last Slurm job ─────────────────────────────────
│ Job:       48260
│ Partition: gpu_short
│ State:     COMPLETED
│
│ Time
│   Limit:        01:00:00
│   Runtime:      00:02:55
│   Usage:        4.9%
│
│ CPU
│   Allocated:    1 core(s)
│   Used:         00:00.142
│   Efficiency:   0.1%
│
│ Memory
│   Allocated:    100G
│   Peak:         3760.0 KB
│   Efficiency:   0.00%
│
│ GPU
│   Type:         RTX PRO 6000
│   Allocated:    1
│   Utilization:  0%
│   Memory used:  0
│
│ Disk I/O
│   Read:         3.01M
│   Write:        0.01M
╰──────────────────────────────────────────────────

``
