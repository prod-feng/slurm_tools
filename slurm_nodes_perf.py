#!/usr/bin/env python
import subprocess

# Find out <10% loaded nodes...
sinfo_cmd="/cm/shared/apps/slurm/current/bin/sinfo -a --Node -o '%.10N %8O %c %.10mMB %.10eMB %.5a %.6t %12E %G'|uniq|grep alloc|grep -v -e a100|awk '{if (($2/$3)<100.3||($2/$3)>0.) print $1,int($2/$3*100), $3, $7}'|sort -n -k1"

# With format of: node  load  CPUs Status(alloc) 
result = subprocess.getoutput(sinfo_cmd)

if len(result)<1:
    print("No records found. Please try again")
    quit()

print("   Load   CPUs  Node  Job       partition              jobname           user        Telapsed    Trequsted  nodes   nodenames")
# 
for line in result.split("\n"):
    #print("Found line",line)
    node,load,cpus,state = line.split()
    #print("Found node: ",node, " with Load: ",load, " CPUS: ",cpus)
    squeue_cmd="/cm/shared/apps/slurm/current/bin/squeue -t r -o '%10i %22P %16j %12u %.10M %10l %.4D %20R' -ahw " + node
    #print("squeue_cmd ",squeue_cmd)
    job = subprocess.getoutput(squeue_cmd).split("\n")[0]
    #print("Found job: ")
    print(" %5.f"% float(load)+"%    "+cpus+"  "+node+" "+job)

