# slurm_tools

This is a collection of the Slurm tools which are useful.

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
slurm_job_perf_email.py
```

Use sacct command to retrieve the jobs performance info of users. If too many jobs for a user, then randomly pick 10(or more) jobs, email users.

```
    Here is the summary report of randomly selected 10 out of 4638 total jobs on the Seawulf Cluster. Please have a review.   
              
    USER      JobID     Jobname           start               elapsed      MaxRss      MaxVMSize  nodes  ncpus    CPUhours  CPUUsgae CPURateU2S
  feng      365392  training_detren  2024-05-04T23:12:21      02:55:21    21.16GB       34.49GB     1     96        281.6   0.3101    0.9826 
  feng      369660  eva_detrend_day  2024-05-11T06:26:12      01:13:24     5.03GB       19.14GB     1     96        118.4   0.5187    0.9798 
  feng      370607  eva_detrend_day  2024-05-12T11:46:53      00:48:21     5.03GB        10.9GB     1     40         33.2   0.2189    0.8755


CPUUsgae=UserCPU/(CPUhours==CPUTime)
CPURateU2S=UserCPU/(SystemCPU+UserCPU)
```
