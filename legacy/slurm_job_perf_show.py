#!/usr/bin/env python
import os
import subprocess
import re
import random
import argparse
from datetime import datetime, timedelta

# Find completed job ...

def running_time(time):
    time=time.split('.')[0]  #trim the .xxx seconds
    mytime=re.split(":|-",time)
    #print("re,splut", mytime)
    return sum(x * int(t) for x, t in zip([1, 60, 3600,86400], reversed(mytime)))

def time2hours(time):
    if not time:
       time="00:00"
       #print("empty time object",time)# for CANCELD/TIMEOUT jobs    
    time=time.split('.')[0]  #trim the .xxx seconds
    mytime=re.split(":|-",time)
    #print("re,splut", mytime)
    return sum(x * int(t) for x, t in zip([0, 1./60., 1,24], reversed(mytime)))

def size2GB(size):
    #mysize = size
    #print("size =",size)
    if "T" in size:
        mysize=(float(size.split("T")[0])*1000)
    elif "G" in size:
        mysize=float(size.split("G")[0])
    elif "M" in size:
        mysize=float(size.split("M")[0])/1000.
    elif "K" in size:
        mysize=float(size.split("K")[0])/1000000.
    else:
        mysize = float(size)
    #print("mysize = ",mysize)
    mysize=round(mysize,2)
    return mysize #now in GB

def size2KB(size):
    if "T" in size:
        mysize=(float(size.split("T")[0])*10024*1024*1024.)
    elif "G" in size:
        mysize=float(size.split("G")[0])*1024*1024
    elif "M" in size:
        mysize=float(size.split("M")[0])*1024.
    elif "K" in size:
        mysize=float(size.split("K")[0])
    else:
        mysize = float(size)/1000. #Bytes
    mysize=round(mysize,2)
    return mysize #now in KB

def human_size(size, decimal_places=1):
    #print("size=",size)
    size= size.split("n")[0]  # for compatibility of slurm 17
    units = ['B', 'K', 'M', 'G', 'T', 'P']
    #if args.csv:
    #    units = ['B', 'K', 'M']
    idx=0
    for un in units:
        #print("OK",idx,units[idx])
        if un in size:
            size=float(size.split(un)[0])
            break
        idx+=1
    #print("idx =",idx,units[idx],"  un= ",units[idx+1])    
    if idx > 5: # for compatibility of slurm 17
        #print("Unit not found, use B")
        idx=0
        size=float(size)    
    for un in units[idx:]:
        if size < 1024.0 or un == 'P':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{un}"
#
start= (datetime.today() - timedelta(days=37)).isoformat(sep='T', timespec='seconds')
end=(datetime.today()).isoformat(sep='T', timespec='seconds')

uid = os.environ.get('USER', os.environ.get('USERNAME')) #os.getlogin() #os.getuid()

parser = argparse.ArgumentParser(description="Calculate CPU and Memory usage for each node running a Slurm job")
group = parser.add_mutually_exclusive_group()

group.add_argument("-u", "--user", help="Use this comma separated list of UIDs or user names to select jobs, default is the user running this command", required=False,default=uid)
group.add_argument("--allusers", help="Displays all users' jobs", action='store_true',required=False)
group.add_argument("-j", "--jobs", help="Displays information about the specified job[.step] or list of job[.step]s", required=False)

parser.add_argument("-S", "--starttime", help="Select  jobs in any state after the specified time. YYYY-MM-DD[THH:MM[:SS]], default is from last week", required=False,default=start)
parser.add_argument("-E", "--endtime", help="Select  jobs in any state before the specified time, default is today", required=False,default=end)
parser.add_argument("-n", "--njobs", help="List njobs  of randomly picked jobs, default is 10", required=False,default=10,type=int)

state="CD"
parser.add_argument("-s", "--state", help="Select jobs in specific state", required=False,default=state)
parser.add_argument("--allstates", help="Displays jobs in all states", action='store_true',required=False)

parser.add_argument("--csv", help="write jobs report to csv format", action='store_true',required=False)

args = parser.parse_args()

if args.user:
    opt=" -u "+args.user
if args.starttime:
    start=args.starttime
if args.endtime:
    end=args.endtime
if args.allusers:
    opt=" -a "
if args.jobs:
    opt=" -j "+args.jobs + " -a"
    args.allstates=1
if args.njobs:
    if int(args.njobs)<1:
        args.njobs=10
else:
    args.njobs=10 #incase "-n 0)

if not args.allstates:
    if args.state:
        opt+=" -s "+args.state

opt = opt + " -S "+start + " -E "+end
sacct_cmd="sacct -n -P  " + opt + "  --format=USER,JobID,partition,partition,state,start,elapsed,MaxRss,MaxVMSize%15,nnodes,ncpus,nodelist,CPUTime%15,SystemCPU%15,TotalCPU%15,UserCPU%15,ReqMem,MaxDiskWrite,MaxDiskRead%20,Jobname%20"

#print(sacct_cmd)

# Find completed job ...
# With format of: node  load  CPUs Status(alloc) 

result = subprocess.getoutput(sacct_cmd)

if len(result)<1 or len(result.split("|"))<19:
    print(result)
    print("No records found. Please try again")
    quit()

job_perf_dict={}
primary_job_id=0
primary_user=""
# 
Jobname2=[]
for line in result.split("\n"):
    # 
    #avoid job name contians "|".
    User,JobID,Account,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead,JobName = line.split("|",19)
    if User:
        #same_job_flag=1
        MaxRSS_sum=0
        MaxVMSize_sum=0
        MaxDiskWrite_sum=0
        MaxDiskRead_sum=0
        if(not MaxRSS):
            MaxRSS=0.
        if(not MaxVMSize):
            MaxVMSize=0.
        #print(">>>>Found User:",User,"JobID:",JobID)
        if not User in job_perf_dict:
            job_perf_dict[User]={JobID:[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead]}
        else:
            if not JobID in job_perf_dict[User]:
                job_perf_dict[User][JobID]=[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead]
        #job_perf_dict={User:{JobID:[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU]}}
        primary_job_id=JobID
        primary_user=User
   # 
    if not User or User:
        #same_job_flag=0
        if MaxRSS:
            if "G" in MaxRSS:
                MaxRSS=str(float(MaxRSS.split("G")[0])*1000000.)+"K"
            if "M" in MaxRSS:
                MaxRSS=str(float(MaxRSS.split("M")[0])*1000.)+"K"
            MaxRSS=float(MaxRSS.split("K")[0])/1000000. #convert to GB
            MaxRSS_sum+=MaxRSS
            MaxRSS_sum=round(MaxRSS_sum,1)
        if MaxVMSize:
            if "G" in MaxVMSize:
                MaxVMSize=str(float(MaxVMSize.split("G")[0])*1000000.)+"K"
            if "M" in MaxVMSize:
                MaxVMSize=str(float(MaxVMSize.split("M")[0])*1000.)+"K"
            MaxVMSize=float(MaxVMSize.split("K")[0])/1000000. #convert to Gb
            MaxVMSize_sum+=MaxVMSize
            MaxVMSize_sum=round(MaxVMSize_sum,2)
        if MaxDiskRead:
            MaxDiskRead_sum=size2GB(MaxDiskRead)

        if MaxDiskWrite:
            MaxDiskWrite_sum=size2GB(MaxDiskWrite)

        job_perf_dict[primary_user][primary_job_id][5]= MaxRSS_sum
        job_perf_dict[primary_user][primary_job_id][6]= MaxVMSize_sum
        job_perf_dict[primary_user][primary_job_id][15]=MaxDiskWrite_sum
        job_perf_dict[primary_user][primary_job_id][16]=MaxDiskRead_sum
    #print("primary_job_id=",primary_job_id,primary_user)    
    #print("hahah",job_perf_dict[primary_user][primary_job_id][5],job_perf_dict[primary_user][primary_job_id][6])
    #if not User and same_job_flag ==0:
        #print("Found job: ",User, " JobID ",JobID,primary_job_id, " CPUS: ",UserCPU, " : ",round(UserCPU_sec/TotalCPU_sec,3), "MaxRSS=",MaxRSS_sum,"GB","MaxVMSize=",MaxVMSize_sum,"GB")
        #print("done",job_perf_dict)
# The data block has the format of:
# key    key    0        1      2     3       4     5        6        7      8       9        10      11        12      13    14         15         16          17
# USER,JobID,Jobname,Partition,state,start,elapsed,MaxRss,MaxVMSize,nnodes,ncpus,nodelist,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead

max_num_report=args.njobs #radonmly pick some jobs if too many...
job_info_dict={}
num_report=1

print("Found ",len(job_perf_dict)," Users")
if args.csv:
    sep=","
else: 
    sep=""
job_head=""+"{:>11.10}".format('USER'+sep)+"{:>10.10}".format('JobID'+sep)+"{:>12.10}".format('Jobname'+sep)+ \
             "{:>12.10}".format('Start')+sep+"{:>12.10}".format('TElapsed')+sep+ \
             "{:>9.7}".format('MemUsed')+sep+ "{:>10.8}".format('MemAsked')+sep+\
             "{:>8.6}".format('nNodes')+sep+ "{:>7.5}".format('nCPUs')+sep+ \
             "{:>12.11}".format('CPUhours')+sep + "{:>10.9}".format('CPUUsage'+sep) +\
              "{:>9.7}".format("CPUSYST")+sep +"{:>11.9}".format("CPUUSER")+sep+"{:>11.9}".format("DiskWrite")+sep+"{:>11.9}".format("DiskRead")+sep+\
              "{:^20.18}".format("Partition")+sep+\
              "{:^11.10}".format('State')+sep+" "+\
              "{:^14.13}".format('NodeList')+sep+" "
if args.csv:
    print(job_head)
#
#
num_job_ignore={}
for user in job_perf_dict:
    #random pick jobs
    # 
    color_l=""
    color_r=""
    id_report=0
    if len(job_perf_dict[user]) > max_num_report:
        randompick=random.sample(range(1, len(job_perf_dict[user])+1), max_num_report)  #+1, [1,2], sample =[1]
    else:
        randompick=[]  #random.sample(range(1, len(job_perf_dict[user])+1), len(job_perf_dict[user]))
    #
    if args.csv:
        randompick=[]  #select all jobs 
    # 
    # 
    job_info=job_head + "\n "
    #  
    if args.csv:
        job_info=" "
    # initilize num job should be ignored
    num_job_ignore[user] = 0
    # Loop to check this user's jobs
    for job in job_perf_dict[user]:
        # 
        #if time2hours(job_perf_dict[user][job][4]) <0.5:  #skip short jobs < 1hour
        #    continue
        #
        #Ignore GPU josb now
        #
        if "a100" in str(job_perf_dict[user][job][1]):
            #print("Skip GPU jobs",str(job_perf_dict[user][job][1]),num_job_ignore[user],user)
            num_job_ignore[user]=num_job_ignore[user]+1
            continue
        #Chose randomly pick jobs only
        if len(randompick)>0:
            id_report=id_report+1 
            if not id_report in randompick:
                continue
        # 
        # 
        allcpuhours=round(time2hours(job_perf_dict[user][job][10])+1.,1) # add 1 extra hour here, avoid 0
        UserCPU_hours=round(time2hours(job_perf_dict[user][job][13]),6)
        avgrateall=round(UserCPU_hours/allcpuhours,3)
        SystemCPU_hours=round(time2hours(job_perf_dict[user][job][11]),6)
        TotalCPU_hours=round(time2hours(job_perf_dict[user][job][12]),6) # add 1 extra hour here, avoid 0
        #avgratejob=round(SystemCPU_hours/UserCPU_hours,4)
        # 
# key    key    0        1      2     3       4     5        6        7      8       9        10      11        12      13    14         15         16         17
# USER,JobID,Jobname,partition,state,start,elapsed,MaxRss,MaxVMSize,nnodes,ncpus,nodelist,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead,Partition
        if not args.csv:
            job_info2="{:>10.10}".format(user) +"{:>10.10}".format(job) +"{:>12.10}".format(job_perf_dict[user][job][0])+ \
                  "{:>12.10}".format(job_perf_dict[user][job][3].split("T")[0])+"{:>12.10}".format(job_perf_dict[user][job][4])+ \
                  "{:>10.8}".format(str(job_perf_dict[user][job][5])+"G ") + "{:>9.8}".format(str(human_size(job_perf_dict[user][job][14])))+\
                  "{:>8.6}".format(str(job_perf_dict[user][job][7]))+"{:>7.5}".format(str(job_perf_dict[user][job][8]))+ \
                  "{:>12.11}".format(str(allcpuhours)+"h")+\
                  color_l+"{:>10.8}".format(str(avgrateall)) +color_r+\
                  "{:>9.7}".format(str(round(SystemCPU_hours,2))+"h")+\
                  "{:>11.9}".format(str(round(UserCPU_hours,2))+"h")+\
                  "{:>11.9}".format(str(human_size(str(job_perf_dict[user][job][15])+"G")))+\
                  "{:>11.9}".format(str(human_size(str(job_perf_dict[user][job][16])+"G")))+\
                  "{:^20.18}".format(str(job_perf_dict[user][job][1]))+\
                  "{:^11.10}".format("  "+str(job_perf_dict[user][job][2]))+\
                  "{:^14.13}".format("  "+str(job_perf_dict[user][job][9]))
        else:
            job_info2="{:>11.10}".format(user)+"," +"{:>11.10}".format(job)+"," +"{:>12.10}".format(job_perf_dict[user][job][0])+","+ \
                  "{:>12.11}".format(job_perf_dict[user][job][3].split("T")[0])+","+"{:>12.10}".format(job_perf_dict[user][job][4])+","+ \
                  "{:>10.8}".format(str(job_perf_dict[user][job][5]))+"," + "{:>11.10}".format(str(size2GB(job_perf_dict[user][job][14])))+","+\
                  "{:>8.6}".format(str(job_perf_dict[user][job][7]))+","+"{:>7.5}".format(str(job_perf_dict[user][job][8]))+","+ \
                  "{:>12.11}".format(str(allcpuhours))+","+\
                  color_l+"{:>10.8}".format(str(avgrateall)) +color_r+","+\
                  "{:>9.7}".format(str(round(SystemCPU_hours,2)))+","+\
                  "{:>11.9}".format(str(round(UserCPU_hours,2)))+","+\
                  "{:>11.9}".format(str(size2GB(str(job_perf_dict[user][job][15]))))+","+\
                  "{:>11.9}".format(str(size2GB(str(job_perf_dict[user][job][16]))))+","+\
                  "{:>20.18}".format(str(job_perf_dict[user][job][1]))+","+\
                  "{:^11.10}".format("  "+str(job_perf_dict[user][job][2]))+","+\
                  "{:^14.13}".format("  "+str(job_perf_dict[user][job][9]).replace(",","|"))+","
        job_info=job_info  +job_info2+ """ \n """
    job_info_dict[user]=job_info.rstrip()
    # 
for user in job_info_dict:
    # 
    # 
    if len(job_perf_dict[user]) <= num_job_ignore[user]:
        #no report needed.
        #print("All jobs ignores, for ", user)
        continue
#    
    print("Found "+str(len(job_perf_dict[user]))," jobs of", user)
    print(job_info_dict[user])
