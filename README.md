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
