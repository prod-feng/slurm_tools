#!/usr/bin/python
import subprocess

# Find out <10% loaded nodes...
sinfo_cmd="sinfo -a --Node -o '%.10N %8O %c %.10mMB %.10eMB %.5a %.6t %12E %G'|uniq|grep alloc|grep -v -e a100|awk '{if (($2/$3)<0.3||($2/$3)>1.1) print $1,int($2/$3*100), $3, $7}'|sort -n -k2"

# With format of: node  load  CPUs Status(alloc) 
result = subprocess.getoutput(sinfo_cmd)
print("Load  CPUs  Node  Job")
for line in result.split("\n"):
    #print("Found line",line)
    node,load,cpus,state = line.split()
    #print("Found node: ",node, " with Load: ",load, " CPUS: ",cpus)
    squeue_cmd="squeue -o '%10i %22P %16j %12u %.10M %10l %.4D %20R' -ahw " + node
    #print("squeue_cmd ",squeue_cmd)
    job = subprocess.getoutput(squeue_cmd)
    #print("Found job: ")
    print(" "+load+"%"+"    "+cpus+"  "+node+" "+job)

