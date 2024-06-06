#!/usr/bin/python
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
    time=time.split('.')[0]  #trim the .xxx seconds
    mytime=re.split(":|-",time)
    #print("re,splut", mytime)
    return sum(x * int(t) for x, t in zip([0, 1./60., 1,24], reversed(mytime)))
#
start= (datetime.today() - timedelta(days=1)).isoformat(sep='T', timespec='seconds')
end=(datetime.today()).isoformat(sep='T', timespec='seconds')

uid = os.environ.get('USER', os.environ.get('USERNAME')) #os.getlogin() #os.getuid()

parser = argparse.ArgumentParser(description="Calculate CPU and Memory usage for each node running a Slurm job")
group = parser.add_mutually_exclusive_group()

group.add_argument("-u", "--user", help="Use this comma separated list of UIDs or user names to select jobs", required=False,default=uid)
group.add_argument("--allusers", help="Displays all users' jobs", action='store_true',required=False)

parser.add_argument("-S", "--starttime", help="Select  jobs in any state after the specified time. YYYY-MM-DD[THH:MM[:SS]]", required=False,default=start)
parser.add_argument("-E", "--endtime", help="Select  jobs in any state before the specified time", required=False,default=end)

parser.add_argument("-j", "--jobs", help="Displays information about the specified job[.step] or list of job[.step]s", required=False)
args = parser.parse_args()

if args.user:
    opt=" -u "+args.user
if args.starttime:
    start=args.starttime
if args.endtime:
    end=args.endtime
if args.allusers:
    opt=" -a "

opt = opt + " -S "+start + " -E "+end
sacct_cmd="sacct -n -P -s CD " + opt + "  --format=USER,JobID,Jobname%20,partition,state,start,elapsed,MaxRss,MaxVMSize%15,nnodes,ncpus,nodelist,CPUTime%14,SystemCPU%14,TotalCPU%14,UserCPU%14"

# print(sacct_cmd)
# Find completed job ...
# With format of: node  load  CPUs Status(alloc) 

result = subprocess.getoutput(sacct_cmd)

if len(result)<1:
    print("No records found. Please try again")
    quit()

# Now work on it
job_perf_dict={}
primary_job_id=0
primary_user=""
#same_job_flag=0 # record from sacct is from the same job

for line in result.split("\n"):
    #print("++++++",line)
    User,JobID,JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU = line.split("|")
    if User:
        #same_job_flag=1
        MaxRSS_sum=0
        MaxVMSize_sum=0
        #print(">>>>Found User:",User,"JobID:",JobID)
        if not User in job_perf_dict:
            job_perf_dict[User]={JobID:[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU]}
        else:
            if not JobID in job_perf_dict[User]:
                job_perf_dict[User][JobID]=[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU]
        #job_perf_dict={User:{JobID:[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU]}}
        primary_job_id=JobID
        primary_user=User

        #Calculate the CPU times as in seconds.
        ##print("Spliting: ",JobID,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU)
        #UserCPU_sec = running_time(UserCPU)
        #TotalCPU_sec = running_time(TotalCPU)
        #SystemCPU_sec = running_time(SystemCPU)
        #CPUTime_sec = running_time(CPUTime)
    #print("++++++",line)
    if not User:
        #same_job_flag=0
        if MaxRSS:
            if "G" in MaxRSS:
                MaxRSS=str(float(MaxRSS.split("G")[0])*1000000.)+"K"
            if "M" in MaxRSS:
                MaxRSS=str(float(MaxRSS.split("M")[0])*1000.)+"K"
            MaxRSS=float(MaxRSS.split("K")[0])/1000000.
            MaxRSS_sum+=MaxRSS
            MaxRSS_sum=round(MaxRSS_sum,2)
        if MaxVMSize:
            if "G" in MaxVMSize:
                MaxVMSize=str(float(MaxVMSize.split("G")[0])*1000000.)+"K"
            if "M" in MaxVMSize:
                MaxVMSize=str(float(MaxVMSize.split("M")[0])*1000.)+"K"
            MaxVMSize=float(MaxVMSize.split("K")[0])/1000000.
            MaxVMSize_sum+=MaxVMSize
            MaxVMSize_sum=round(MaxVMSize_sum,2)
            
        job_perf_dict[primary_user][primary_job_id][5]= MaxRSS_sum
        job_perf_dict[primary_user][primary_job_id][6]= MaxVMSize_sum
        #print("primary_job_id=",primary_job_id,primary_user)    
        #print("hahah",job_perf_dict[primary_user][primary_job_id][5],job_perf_dict[primary_user][primary_job_id][6])
        #if not User and same_job_flag ==0:
        #print("Found job: ",User, " JobID ",JobID,primary_job_id, " CPUS: ",UserCPU, " : ",round(UserCPU_sec/TotalCPU_sec,3), "MaxRSS=",MaxRSS_sum,"GB","MaxVMSize=",MaxVMSize_sum,"GB")
        #print("done",job_perf_dict)
# The data block has the format of:
# key    key    0        1      2     3       4     5        6        7      8       9        10      11        12      13
# USER,JobID,Jobname,partition,state,start,elapsed,MaxRss,MaxVMSize,nnodes,ncpus,nodelist,CPUTime,SystemCPU,TotalCPU,UserCPU

max_num_report=10 #radonmly pick some jobs if too many...
job_info_dict={}
num_report=1

# Start to construct email body for each user
print("Found ",len(job_perf_dict)," Users")
for user in job_perf_dict:
    #random pick jobs
    #print("random sample",len(job_perf_dict[user]), max_num_report)
    id_report=0
    if len(job_perf_dict[user]) > max_num_report:
        randompick=random.sample(range(1, len(job_perf_dict[user])+1), max_num_report)  #+1, [1,2], sample =[1]
    else:
        randompick=random.sample(range(1, len(job_perf_dict[user])+1), len(job_perf_dict[user]))
    #print("dandom ",randompick)
    #print("Found ",len(job_perf_dict[user])," of User: ",user)
    job_info="\n"+"{:>11.10}".format('USER')+"{:>10.10}".format('JobID')+"{:>20.15}".format('Jobname')+ \
             "{:>22.19}".format('Start')+"{:>15.12}".format('TElapsed')+ \
             "{:>17.15}".format('MaxRss')+"{:>17.15}".format('MaxVMSize')+ \
             "{:>10.4}".format('nodes')+ "{:>10.5}".format('ncpus')+ \
             "{:>13.11}".format('CPUhours')+  "{:>10.8}".format('CPUUsgae')+ "{:>12.10}".format('CPURateU2S')+"\n "
    #              slthomas  488502  NEB.WH1.0.91363  2024-06-05T05:53:10  00:00:08          0.0GB        0.39GB     2     80          1.2   0.0139    0.0161 
    #             jnicasio  359942         sicryres  2024-04-30T16:16:31    1-05:11:54    29.11GB     1655.22GB     6     576     16818.23  0.1658    1.0 
    for job in job_perf_dict[user]:
        #if len(job_perf_dict[user])>max_num_report:
        #    num=random.random() #randomly pick 10 jobs for summary
        #    if num>float(max_num_report)/float(len(job_perf_dict[user])) and num_report<max_num_report:
        #        continue
        id_report=id_report+1 
        if not id_report in randompick:
            continue
        #print("Found User/job:",user,job,job_perf_dict[user][job])
        #prepare email body
        #print("time2hours",round(time2hours(job_perf_dict[user][job][10]),2))
        allcpuhours=round(time2hours(job_perf_dict[user][job][10])+1.,1) # add 1 extra hour here, avoid 0
        UserCPU_hours=round(time2hours(job_perf_dict[user][job][13]),6)
        avgrateall=round(UserCPU_hours/allcpuhours,4)
        SystemCPU_hours=round(time2hours(job_perf_dict[user][job][11]),6)
        TotalCPU_hours=round(time2hours(job_perf_dict[user][job][12])+1,6) # add 1 extra hour here, avoid 0
        avgratejob=round(UserCPU_hours/TotalCPU_hours,4)
        #job_info1=user,job,job_perf_dict[user][job][0], job_perf_dict[user][job][2],job_perf_dict[user][job][3],job_perf_dict[user][job][4],str(job_perf_dict[user][job][5])+"GB ",str(job_perf_dict[user][job][6])+"GB",job_perf_dict[user][job][7],job_perf_dict[user][job][8],allcpuhours,avgrateall,avgratejob
  
        job_info2="{:>10.10}".format(user) +"{:>10.10}".format(job) +"{:>20.15}".format(job_perf_dict[user][job][0])+ \
                  "{:>22.19}".format(job_perf_dict[user][job][3])+"{:>15.12}".format(job_perf_dict[user][job][4])+ \
                  "{:>17.15}".format(str(job_perf_dict[user][job][5])+"GB ") +"{:>17.15}".format(str(job_perf_dict[user][job][6])+"GB")+\
                  "{:>10.4}".format(str(job_perf_dict[user][job][7]))+"{:>10.5}".format(str(job_perf_dict[user][job][8]))+ \
                  "{:>13.11}".format(str(allcpuhours))+"{:>10.8}".format(str(avgrateall))+"{:>12.10}".format(str(avgratejob))
        job_info=job_info  +job_info2+ """ \n """
    job_info_dict[user]=job_info
    #print("hahah",job_info2)
for user in job_info_dict:
    print("Found ",len(job_perf_dict[user])," jobs of User: ",user)
    print(job_info_dict[user])
    #continue
