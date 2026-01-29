# slurm_tools

This is a collection of the Slurm tools which are useful.

For the email function, you may need to setup your way to get the users email addresses, and email server, etc.

The print out formats are different from the Linux terminal and Google email.

Python 3.6.

# 1
```
slurm_nodes_perf.py 
```
list all under utilized, or over scribed compute nodes and the jobs on them. Good for regular users. The output s like:

```
Load    CPUs  Node    Job
   0%    40  node010 427356     long                 z500we xxxyyy           1:15 1-00:00:00    1 node010               
   0%    40  node013 427357     short                z500we yyyxxx           1:15    4:00:00    1 node013               
   0%    40  node014 424573     long             jupyter-nb zzzxxx       12:43:59 1-00:00:00    1 node014
...
1448%    96  node053 427607     long                 z500we xxxyyy          15:25    4:00:00    1 node053               
1490%    96  node052 427605     long                 z500we xxxyyy          21:17 1-00:00:00    1 node052   
```


# 2
```
slurm_nodes_perf_email.py
```
list all under utilized compute nodes and the jobs on them, and email users. Good for Admin.

N.B. This script can not properly process compute node shared by multiple users at present.

# 3
```
./slurm_job_perf_email.py  --allusers  -S 2024-01-01 -E  now    --csv
```

Use sacct command to retrieve the jobs performance info of users. If too many jobs for a user, then randomly pick 10(or more) jobs, email users.

```
    Here is the summary report of randomly selected 10 out of 4638 total jobs on the Seawulf Cluster. Please have a review.   
              
  USER,    JobID,    Jobname,       Start,    TElapsed,  MemUsed,  MemAsked,  nNodes,  nCPUs,    CPUhours, CPUUsage,  CPUSYST,    CPUUSER,  DiskWrite,   DiskRead,     Partition      ,   NodeList   ,
  feng,   360256,        bash, 2024-04-29,   00:51:07,      0.0,    388.73,        1,    96,         82.8,       0.0,    0.05,      0.02,       0.0,      1.08,    hbm-short-96core,       node093
  feng,   360597,  50003_mars, 2024-04-30,   01:50:08,      1.6,    192.03,        1,    40,         74.4,     0.034,    0.13,      2.52,      2.71,      7.12,     extended-40core,       node011


CPUUsgae=UserCPU/(CPUhours==CPUTime elapsed)

```

# 4
```
slurm_job_perf_show.py
```

Same as "slurm_job_perf_email.py", just list all users job info.

# 5 
```
slurm_job_perf_gpu_utilization.py
```

Check users' GPU jobs utilization efficiency in the past week. Modify users' ASSOC priority factors accordingly, vary from 80 to 100(with GPU efficiency vary from 0% to 100%).

