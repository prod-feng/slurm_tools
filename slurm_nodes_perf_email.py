#!/usr/bin/python
import os
import subprocess

# Find out <10% loaded nodes...

sinfo_cmd="/cm/shared/apps/slurm/current/bin/sinfo -a --Node -o '%.10N %8O %c %.10mMB %.10eMB %.5a %.6t %12E %G'|uniq|grep -e alloc -e mix|grep -v -e a100|awk '{if (($2/$3) <0.1) print $1,int($2/$3*100), $3, $7}'|sort -n -k2"

# With format of: node  load  CPUs Status(alloc) 
result = subprocess.getoutput(sinfo_cmd)
print("Load  CPUs  Node  Job")
for line in result.split("\n"):
    node,load,cpus,state = line.split()
    #print("Found node: ",node, " with Load: ",load, " CPUS: ",cpus)
    #ignore shared queue now...fix later...
    squeue_cmd="/cm/shared/apps/slurm/current/bin/squeue -o '%10i %22P %16j %12u %.10M %10l %.10D %20R' -ahw " + node + "|grep -v -e shared"
    job = subprocess.getoutput(squeue_cmd)
    if not job:  #empty or shared jobs?
        continue
    myqueue=job.split() #%10i %22P %16j %12u %.10M %10l %.4D %20R#
    myjob=myqueue[0]
    user=myqueue[3]
    sconf_cmd="/cm/shared/apps/slurm/current/bin/scontrol show job "+myjob+"|grep MailUser|awk '{print $1}'|sed 's/MailUser=//g'"
    email_addr= subprocess.getoutput(sconf_cmd)
    if not email_addr:
        user_cmd="getent passwd|grep "+user+"|awk -F':' '{print $5}'|awk -F',' '{print $2}'|egrep -ho '[[:graph:]]+@[[:graph:]]+'"
        email_addr=subprocess.getoutput(user_cmd)
        if not email_addr:
            email_addr="myemail@example.com" # default email address
    print(" "+load+"%"+"    "+cpus+"  "+node+" "+job+" ",email_addr)
    nodeinfo=load+"%   "+node+"    "+job+" "
    sub="You job "+myjob+" on the Cluster needs attention"
    contents="""
    
    Dear """+user+""":

    It seems that your computing job """+myjob+""" is under utilizing the compute node it is running on. The utilization of the node is as follows:

  Load  NodeName   JobID         Partition                JobName         User       Time      Time_limit      Nodes Nodelist 
    """ + nodeinfo + """ 

    Please note that the load on this node is at only """+load+"""%.

    Please review your job settings to ensure your job is functioning as intended and if not, adjust the settings and resubmit. 
    
    If you have any questions, please contact us.

    Thanks!

    HPC Support
    """
#Please check your job settings to make sure your job is functioning well as expected, and it is actually using the resources that it intended to allocate.
#    print(sub)
#    print(contents)
    mail_cmd="mail -s '"+sub+"'  -r 'admin@example.com' -b 'admin2@example.com'  "+email_addr+ "  <<EOF" +contents +"""
EOF"""
    print(mail_cmd)
    mailit = subprocess.getoutput(mail_cmd)
