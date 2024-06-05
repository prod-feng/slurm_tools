#!/usr/bin/python
import os
import subprocess
import re
import random
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

# Find completed job ...
job_perf_dict={}
primary_job_id=0
primary_user=""
same_job_flag=0 # record from sacct is from the same job

#change the time rang of the jobs you want to check
sacct_cmd="sacct -n -P -s CD --starttime 2024-05-01 --endtime 2024-05-22 --format=USER,JobID,Jobname%20,partition,state,start,elapsed,MaxRss,MaxVMSize%15,nnodes,ncpus,nodelist,CPUTime%14,SystemCPU%14,TotalCPU%14,UserCPU%14"

# With format of: node  load  CPUs Status(alloc) 
result = subprocess.getoutput(sacct_cmd)

for line in result.split("\n"):
    User,JobID,JobName,Partition,State,Start,Elapsed,MaxRSS,MaxVMSize,NNodes,NCPUS,NodeList,CPUTime,SystemCPU,TotalCPU,UserCPU = line.split("|")
    if User:
        same_job_flag=1
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
        same_job_flag=0
        if MaxRSS:
            if "M" in MaxRSS:
                MaxRSS=str(float(MaxRSS.split("M")[0])*1000.)+"K"
            MaxRSS=float(MaxRSS.split("K")[0])/1000000.
            MaxRSS_sum+=MaxRSS
            MaxRSS_sum=round(MaxRSS_sum,2)
        if MaxVMSize:
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

# Start to construct email body for each user
for user in job_perf_dict:
    #random pick jobs
    id_report=1
    if len(job_perf_dict[user]) > max_num_report:
        randompick=random.sample(range(1, len(job_perf_dict[user])+1), max_num_report)  #+1, [1,2], sample =[1]
    else:
        randompick=random.sample(range(1, len(job_perf_dict[user])+1), len(job_perf_dict[user]))
    #print("dandom ",randompick)

    #randompick=random.sample(range(1, len(job_perf_dict[user])), max_num_report)
    #print("dandom ",randompick)
    #print("Found ",len(job_perf_dict[user])," of User: ",user)
    job_info="\n    USER      JobID     Jobname           start               elapsed      MaxRss      MaxVMSize  nodes  ncpus    CPUhours  CPUUsgae CPURateU2S\n "

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

        job_info2=user+"  " +job +"  " +"{:15.15}".format(job_perf_dict[user][job][0])+"  "+job_perf_dict[user][job][3]+ "  "+"{:>12}".format(job_perf_dict[user][job][4])+ "  "+"{:>10}".format(str(job_perf_dict[user][job][5])+"GB ") + "  "+"{:>11}".format(str(job_perf_dict[user][job][6])+"GB")+"  "+"{:>4}".format(str(job_perf_dict[user][job][7]))+"  "+"{:>5}".format(str(job_perf_dict[user][job][8]))+"  "+"{:>11}".format(str(allcpuhours))+"  "+"{:>7}".format(str(avgrateall))+"  "+"{:>8}".format(str(avgratejob))
        job_info=""" """ +job_info +""" """ +job_info2+ """ \n """
    job_info_dict[user]=job_info
    #print("hahah",job_info2)
for user in job_info_dict:
    print("Found ",len(job_perf_dict[user])," of User: ",user)
    #print(job_info_dict[user])
    #continue
#    if not email_addr:
    user_cmd="getent passwd|grep "+user+"|awk -F':' '{print $5}'|awk -F',' '{print $2}'|egrep -ho '[[:graph:]]+@[[:graph:]]+'"
    email_addr=subprocess.getoutput(user_cmd)
    if not email_addr:
         email_addr="my@emai.server" #change to your email address
    #print(" "+load+"%"+"    "+cpus+"  "+node+" "+job+" ",email_addr)
    #nodeinfo=load+"%   "+node+"    "+job+" "
    if len(job_perf_dict[user]) > max_num_report:
        num_report=max_num_report
    else:
        num_report=len(job_perf_dict[user])
        
    sub="Summary report of your computing jobs performance on SeaWulf Cluster"
    contents="""
    
Dear """+user+""":

Here is the summary report of randomly selected """+ str(num_report)+""" out of """+str(len(job_perf_dict[user]))+""" total jobs on the Seawulf Cluster. Please have a review.   
    """ +job_info_dict[user]+ """ 

Among the metrcs list above, the "CPUUsgae" and "CPURateU2S" are very helpful for checking your jobs' performace and efficiency.

"CPUUsgae": which is the rate of your job's real usage of CPU time, divided by the total CPUhours, including idle and working time: UserCPU/CPUhours. If it is far less than 1, then it should be very probably that your jobs under-utilized the resource they requested. Please check your job setting to improve.

"CPURateU2S": which is the rate of our job's real usage of CPU time, diveided by the CPU working time: UserCPU/(SystemCPU+UserCPU). If it is far less than 1, then it should be very probably that your jobs over-utilized the resource they requested, e.g., your job started too many processes on the node; used way big sized data set and then RAM, etc.

"MaxRss": The peak usage of the memory.

"MaxVMSize": The peake usage of VM memory.

If you have any questions, please submit a ticket.

Thanks!

HPC Support
    """
#Please check your job settings to make sure your job is functioning well as expected, and it is actually using the resources that it intended to allocate.
#    print(sub)
#    print("email addr: ",email_addr)
    #print(contents)
    mail_cmd="mail -s '"+sub+" -S 'Content-Type: text/html; charset=UTF-8' "+email_addr+ "  <<EOF" +contents +"""
EOF"""
    print(mail_cmd)
    mailit = subprocess.getoutput(mail_cmd)

