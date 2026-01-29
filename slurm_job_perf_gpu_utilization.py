#!/usr/bin/python
import os
import subprocess
import re
import random
import argparse
from datetime import datetime, timedelta

# Find completed job ...


debug =0 
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
    for un in units[idx:]:
        if size < 1024.0 or un == 'P':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{un}"
#
start= (datetime.today() - timedelta(days=1)).isoformat(sep='T', timespec='seconds')
end=(datetime.today()).isoformat(sep='T', timespec='seconds')

uid = os.environ.get('USER', os.environ.get('USERNAME')) #os.getlogin() #os.getuid()

parser = argparse.ArgumentParser(description="Calculate CPU and Memory usage for each node running a Slurm job")
group = parser.add_mutually_exclusive_group()

group.add_argument("-u", "--user", help="Use this comma separated list of UIDs or user names to select jobs", required=False,default=uid)
group.add_argument("--allusers", help="Displays all users' jobs", action='store_true',required=False)
group.add_argument("-j", "--jobs", help="Displays information about the specified job[.step] or list of job[.step]s", required=False)

parser.add_argument("-S", "--starttime", help="Select  jobs in any state after the specified time. YYYY-MM-DD[THH:MM[:SS]]", required=False,default=start)
parser.add_argument("-E", "--endtime", help="Select  jobs in any state before the specified time", required=False,default=end)
parser.add_argument("-n", "--njobs", help="List njobs  of randomly picked jobs", required=False,default=100,type=int)

parser.add_argument("--emailusers", help="Send email to users with the jobs report", action='store_true',required=False)

state="CD"
parser.add_argument("-s", "--state", help="Select jobs in specific state", required=False,default=state)
parser.add_argument("--allstates", help="Displays jobs in all states", action='store_true',required=False)


parser.add_argument("--csv", help="write jobs report to csv format", action='store_true',required=False)

parser.add_argument("--print", help="print job details", action='store_true',required=False)

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
sacct_cmd="/cm/shared/apps/slurm/current/bin/sacct -n -P " + opt + "  --format=USER,JobID,partition,partition,state,start,elapsed,MaxRss,MaxVMSize%15,nnodes,ncpus,nodelist,CPUTime%15,SystemCPU%15,TotalCPU%15,UserCPU%15,ReqMem,MaxDiskWrite,MaxDiskRead%20,Jobname%20,TRESUsageInAve%130,AllocTRES%100"

#debug=10 
if (debug>0): 
    print(sacct_cmd)

# Find completed job ...
# With format of: node  load  CPUs Status(alloc) 

result = subprocess.getoutput(sacct_cmd)

if len(result)<1:
    print("No records found. Please try again")
    quit()

job_perf_dict={}
gpu_util_eff={}
primary_job_id=0
primary_user=""
#same_job_flag=0 # record from sacct is from the same job
Jobname2=[]
for line in result.split("\n"):
    #print("++++++",line, " num of ",line.count('|'))
    #avoid job name contians "|".
    #print("line =", line)
    User,JobID,Account,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead,JobName,TRESUsageInAve,AllocTRES = line.split("|",21)
    #print("JobName= ",JobName,TRESUsageInAve, AllocTRES)
    if User:
        #same_job_flag=1
        MaxRSS_sum=0
        MaxVMSize_sum=0
        MaxDiskWrite_sum=0
        MaxDiskRead_sum=0
        gpuutil_sum=0
        gpu_num=0
        if(not MaxRSS):
            MaxRSS=0.
        if(not MaxVMSize):
            MaxVMSize=0.
        #print(">>>>Found User:",User,"JobID:",JobID)
        if not User in job_perf_dict:
            job_perf_dict[User]={JobID:[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead,gpuutil_sum,gpu_num]}
        else:
            if not JobID in job_perf_dict[User]:
                job_perf_dict[User][JobID]=[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU,ReqMem,MaxDiskWrite,MaxDiskRead,gpuutil_sum,gpu_num]
        #job_perf_dict={User:{JobID:[JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU]}}
        primary_job_id=JobID
        primary_user=User

        #Calculate the CPU times as in seconds.
        ##print("Spliting: ",JobID,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU)
        #UserCPU_sec = running_time(UserCPU)
        #TotalCPU_sec = running_time(TotalCPU)
        #SystemCPU_sec = running_time(SystemCPU)
        #CPUTime_sec = running_time(CPUTime)
   # print("++++++",line)
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
        
        #find GPU info.

        if (TRESUsageInAve.strip()!="" and 'gpu' in TRESUsageInAve):
            #print("TRESUsageInAve= ", TRESUsageInAve)
            gpuutil=TRESUsageInAve.split(",")[4]
            if('gpu' in AllocTRES):
                if("billing" in AllocTRES):
                    gpunum=AllocTRES.split(",")[2]
                else:
                    gpunum=AllocTRES.split(",")[1]
                if(not 'gres/gpu' in gpunum):
                    print("Warning: Wrong gpunum!")
                if(not 'gres/gpuutil=' in gpuutil):
                    print("Warning: Wrong gpuutil!")
                gpu_num=gpunum.split("=")[1]
            else:
                gpu_num=1 # just avoid /zero
            g_util=int(gpuutil.split("=")[1])/int(gpu_num)
            gpuutil_sum=gpuutil_sum+g_util
            #print("GPU utiliation: ",gpuutil,gpunum,g_util, JobID,gpuutil_sum)
        job_perf_dict[primary_user][primary_job_id][17]=gpuutil_sum
        job_perf_dict[primary_user][primary_job_id][18]=gpu_num
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

# Start to construct email body for each user
if debug>0: 
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
              "{:^14.13}".format('NodeList')+sep+" "+\
              "{:^14.13}".format('GPUUTIL(%)')+sep+" "
if args.csv:
    print(job_head)
#
#
num_job_ignore={}
for user in job_perf_dict:
    elapsed_sum=0.0
    gpu_util_sum=0.0
    #random pick jobs
    #print("random sample",len(job_perf_dict[user]), max_num_report)
    if args.emailusers:
        color_l="<span style='color:black'> <b>"
        color_r="</b></span>"
    else:
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
    #print("dandom ",randompick)
    #print("Found ",len(job_perf_dict[user])," of User: ",user)
    job_info=job_head + "\n "
    # "\n"+"{:>11.10}".format('USER')+"{:>10.10}".format('JobID')+"{:>12.10}".format('Jobname')+ \
    #         "{:>12.10}".format('Start')+"{:>12.10}".format('TElapsed')+ \
    #         "{:>9.8}".format('MemUsed')+ "{:>10.8}".format('MemAsked')+\
    #         "{:>8.6}".format('nNodes')+ "{:>7.5}".format('nCPUs')+ \
    #         "{:>12.11}".format('CPUhours')+ color_l+ "{:>10.8}".format('CPUUsage') +color_r+\
    #          "{:>9.7}".format("CPUSYST") +"{:>11.9}".format("CPUUSER")+"{:>11.9}".format("DiskWrite")+"{:>11.9}".format("DiskRead")+\
    #          "{:^20.18}".format("Partition")+\
    #          "{:^14.13}".format('NodeList')+"\n "
    #              slthomas  488502  NEB.WH1.0.91363  2024-06-05T05:53:10  00:00:08          0.0GB        0.39GB     2     80          1.2   0.0139    0.0161 
    #             jnicasio  359942         sicryres  2024-04-30T16:16:31    1-05:11:54    29.11GB     1655.22GB     6     576     16818.23  0.1658    1.0 
    if args.csv:
        job_info=" "
    # initilize num job should be ignored
    num_job_ignore[user] = 0
    # Loop to check this user's jobs
    num_good_jobs=0
    for job in job_perf_dict[user]:
        #if len(job_perf_dict[user])>max_num_report:
        #    num=random.random() #randomly pick 10 jobs for summary
        #    if num>float(max_num_report)/float(len(job_perf_dict[user])) and num_report<max_num_report:
        #        continue
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
        #print("Found User/job:",user,job,job_perf_dict[user][job])
        #prepare email body
        #print("time2hours",round(time2hours(job_perf_dict[user][job][10]),2))
        allcpuhours=round(time2hours(job_perf_dict[user][job][10])+1.,1) # add 1 extra hour here, avoid 0
        UserCPU_hours=round(time2hours(job_perf_dict[user][job][13]),6)
        avgrateall=round(UserCPU_hours/allcpuhours,3)

        elapsed_t=round(time2hours(job_perf_dict[user][job][4]),6)
        if(elapsed_t < 0.5): # > half hour walltime?
            continue
        if(int(job_perf_dict[user][job][18])<=0): # NO GPU, only CPU jobs
            continue
        job_perf_dict[user][job][4]=elapsed_t

        #calculate the weighted GPU walltime and utilization
        gpu_num=int(job_perf_dict[user][job][18])
        elapsed_sum=elapsed_sum+elapsed_t*gpu_num

        gpu_util_sum=gpu_util_sum+int(job_perf_dict[user][job][17])*elapsed_t*gpu_num

        num_good_jobs=num_good_jobs+1 
        if args.emailusers:
            if avgrateall <0.6:
                color_l="<span style='color:red'>"
            else:
                color_l="<span style='color:black'>"
        SystemCPU_hours=round(time2hours(job_perf_dict[user][job][11]),6)
        TotalCPU_hours=round(time2hours(job_perf_dict[user][job][12]),6) # add 1 extra hour here, avoid 0
        #avgratejob=round(SystemCPU_hours/UserCPU_hours,4)
        #job_info1=user,job,job_perf_dict[user][job][0], job_perf_dict[user][job][2],job_perf_dict[user][job][3],job_perf_dict[user][job][4],str(job_perf_dict[user][job][5])+"GB ",str(job_perf_dict[user][job][6])+"GB",job_perf_dict[user][job][7],job_perf_dict[user][job][8],allcpuhours,avgrateall,avgratejob
 
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
                  "{:^14.13}".format("  "+str(job_perf_dict[user][job][9])) +\
                  "{:^14.13}".format("  "+str(job_perf_dict[user][job][17]))+\
                  "{:^4.3}".format("  "+str(job_perf_dict[user][job][18]))
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
    #
    ##
    #
    if(num_good_jobs>0):
        job_info_dict[user]=job_info.rstrip()
        if(elapsed_sum <=0):
            elapsed_sum=1.0  #avoid by dividing by Zero
    
        gpu_eff=int(gpu_util_sum/elapsed_sum*0.2) + 80 # force range from 80 to 100
        #if(gpu_eff<1):
        #    gpu_eff=1  # set the minumum
        gpu_util_eff[user]={"eff":gpu_eff}
        #if(gpu_util_sum/(100*elapsed_sum) <2 ):
        if (debug>0): 
            print("Found under utilization GPUs of:",user, " assoc_priority_factor=", gpu_eff, "/100")
    #print("hahah",len(job_info_dict[user]))
for user in job_info_dict:
    #if not args.csv:
    #print("Found ",len(job_perf_dict[user])," jobs of User: ",user, ", to ignore ",str(num_job_ignore[user])+ " jobs...")
    if len(job_perf_dict[user]) <= num_job_ignore[user]:
        #no report needed.
        #print("All jobs ignores, for ", user)
        continue
    #print(job_info_dict[user])
    #continue
#    if not email_addr:
    user_cmd="getent passwd|grep "+user+"|awk -F':' '{print $5}'|awk -F',' '{print $2}'|egrep -ho '[[:graph:]]+@[[:graph:]]+'"
    email_addr=subprocess.getoutput(user_cmd)
    if not email_addr:
         email_addr="feng.zhang@stonybrook.edu"
    #nodeinfo=load+"%   "+node+"    "+job+" "
    if len(job_perf_dict[user]) > max_num_report:
        num_report=max_num_report
    else:
        num_report=len(job_perf_dict[user])
    sub="Summary report of your computing jobs performance on SeaWulf Cluster\nContent-Type: text/html"
    contents="""
<body style="font-family: sans-serif, Arial, Helvetica;">    
<pre>

Dear """+user+""":

Here is the summary report of randomly selected """+ str(num_report)+""" out of """+str(len(job_perf_dict[user]))+""" total completed jobs on the Seawulf Cluster in the last two weeks. Please have a review.   

""" +job_info_dict[user]+ """

Among the metrcs list above, the "CPUUsgae" is very helpful for checking your jobs' performance and efficiency.
 
"CPUUsage" = CPUUser/CPUhours, is the rate of your job's real usage of CPU time, divided by the total CPUhours, including idle and working time. It is a good estimate of your jobs' efficiency on the clluster. It should be very close to 1. Otherwise, if it is less than 1 a lot(e.g., <0.9, etc), then it should be very probably that your jobs under-utilized the resource they requested. There could be many reasons. Please check your job settings, like MPI job configurations; ask fewer resources(nodes, CPU cores, etc) for your jobs; submit your jobs to shared queue instead of ask for a whole node; double check if you are over run the nodes with huge amount of processes by multuple threading; check if there's heavy disk IOs of your jobs. Also if possible, combine many small jobs in a much bigger job can greatly improve the efficiency, to avoid the overhead from the OS and Slurm.

"CPUhours": CPU time in hours used (Elapsed time * CPU count) by your job.

"CPUSYST": The CPU hours used by OS and related. If it is high, then you may need to consider checking if there's anything that over run the node. The smaller it is, the better.

"CPUSUER": The CPU hours used by your job's real computation. The bigger it is, the better.

"MemUsed": The maximum memory used by the job. Due to our current setting, it sometimes can be accounted repeatedly for the same memory usage, which in turn can show a number larger than the requested memory. This offten happens to multiple threading jobs. The rough estimate for the correted MemUsed in this case can be obtained by MemUsed/nCPUs. This issue will be fixed later when we have a chance to upgrade Slurm. 

If you have any questions, please submit a ticket through https://iacs.supportsystem.com/.

Thanks!

HPC Support
</pre>

<body>
    """
#Please check your job settings to make sure your job is functioning well as expected, and it is actually using the resources that it intended to allocate.
#    print(sub)
#    print("email addr: ",email_addr)
    #print(contents)
    mail_cmd="mail -s '"+sub+"'  -r xxx' -b 'xxx'  "+" -S 'Content-Type: text/html; charset=UTF-8' -S 'Content-Transfer-Encoding: quoted-printable' "+email_addr+ "  <<EOF" +contents +"""
EOF"""
    
    #print(mail_cmd)
    if args.emailusers:
        print("Sending email to user: ",user)
        print(mail_cmd)
        mailit = subprocess.getoutput(mail_cmd)
    elif(args.print):
        print(job_info_dict[user])
#
## deal with underutilizing jobs and their users now.
#
import csv
from typing import Dict, List
import shutil

rows=[]
csv_users=[]
csv_users_dict=[]
ignore_users=[]

filename='/myloaction/gpujobs.csv'

with open(filename, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    rows: List[Dict[str, str]] = list(reader)
    #print(rows)
    for row in rows:
        if(row['user']  in gpu_util_eff):
            csv_users.append(row['user'])
        else:
            ignore_users.append(row['user'])
            #print('Ignore this user:', row['user'], row['last'])

for user in gpu_util_eff:
    if user not in csv_users:
        csv_users.append(user)
        if debug>0:
            print("new user",user, end)

simple_end=datetime.now().strftime("%Y-%m-%d")
for user in csv_users:
    useritems={}
    #print("csv users:",user)
    useritems['user']=user
    useritems['last']=simple_end
    csv_users_dict.append(useritems)

###Write the new user list out, replace the original one.
with open(filename+'.tmp', 'w', newline="", encoding="utf-8") as newfile:
     writer = csv.DictWriter(newfile, fieldnames=['user','last'])
     writer.writeheader()
     writer.writerows(csv_users_dict)

shutil.move(filename+'.tmp', filename)

### Genarate appropriate command lines for update the ASSOC priority factors according to GPU utilization efficiency.
for user in gpu_util_eff:
    cmdline="sacctmgr modify user {} set -i priority={:2d}".format(user,gpu_util_eff[user]["eff"])
    print(cmdline)
    # execute the command now?
    #os.system(cmdline)
    #print("sacctmgr modify user",user, "set -i priority=%d"%gpu_util_eff[user]["eff"])
### Now process the users that do not have jobs in the past week.
for user in ignore_users:
    cmdline="sacctmgr modify user {} set -i priority=100".format(user) #reset to 100 after 1 week without any jobs
    print(cmdline)
    # execute the command now?
    #os.system(cmdline)
