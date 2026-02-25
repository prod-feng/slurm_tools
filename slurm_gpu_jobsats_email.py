#!/usr/bin/env python
import os
import subprocess
import json


debug=9
file_path = "/lustre/nvwulf/admin/scripts/jobstats.csv"
maxcount=6 - 1  # max -1.  E.g.,  6X10 minutes=1 hour. Cronjob interval:10 minues
emailmax=2    # max emails to users per day
emailcount=(24/emailmax)*6  # 10 minutes interval, 1 hour has 6 counts


# Find out <10% loaded nodes...
#sinfo_cmd="/cm/shared/apps/slurm/current/bin/sinfo -a --Node -o '%.10N %8O %c %.10mMB %.10eMB %.5a %.6t %12E %G'|uniq|grep alloc|grep -v -e a100|awk '{if (int($2)==8) print $1,int($2), $3, $7}'|sort -n -k2"

#sinfo_cmd="/cm/shared/apps/slurm/current/bin/sinfo -a --Node -o '%.10N %8O %c %.10mMB %.10eMB %.5a %.6t %12E %G'|uniq|grep -e alloc -e mix|grep -v -E 'nvda|gpu|a100|v100|p100'|awk '{if (($2/$3) <0.1) print $1,int($2/$3*100), $3, $7}'|sort -n -k2"


def convert_compressed_string_to_list(compressed_string):
    result_list = []
    # Split the string by commas to get individual components (e.g., "1-3", "5")
    parts = compressed_string.split(',')

    for part in parts:
        # Check if the part represents a range (contains a hyphen)
        if '-' in part:
            start_str, end_str = part.split('-')
            start = int(start_str)
            end = int(end_str)
            if start > end: #swap the start and end if needed.
                tmp=start
                start=end
                end=tmp
            # Extend the list with numbers in the range (inclusive)
            result_list.extend(range(start, end + 1))
        else:
            # If it's a single number, convert it to an integer and add to the list
            result_list.append(int(part))
    return result_list

def read_slurm_jobinfo_json():
  try:
    with open(file_path, 'r') as file:
        data_dict = json.load(file)
    print("JSON data successfully loaded into a dictionary:")
    #print(data_dict)
    print(data_dict['jobs'][0]['job_id'],data_dict['jobs'][0]['nodes'], data_dict['jobs'][0]['gres_detail'][0])
  except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
  except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from '{file_path}'. Check file format.")
  except Exception as e:
    print(f"An unexpected error occurred: {e}")


def email_users(Nodes,UserId,JobId,GRES,gusage_total,gpuinfo):
      #Get users info
      username_cmd="getent passwd|grep "+UserId+" |awk -F':' '{print $5}'|awk -F',' '{print $1}'"
      username=subprocess.getoutput(username_cmd)      
      sconf_cmd="/cm/shared/apps/slurm/current/bin/scontrol show job "+JobId+"|grep MailUser|awk '{print $1}'|sed 's/MailUser=//g'"
      email_addr= subprocess.getoutput(sconf_cmd)
      if not email_addr:
         user_cmd="getent passwd|grep "+UserId+"|awk -F':' '{print $5}'|awk -F',' '{print $2}'|egrep -ho '[[:graph:]]+@[[:graph:]]+'"
         email_addr=subprocess.getoutput(user_cmd)
         if not email_addr:
             email_addr="feng.zhang@stonybrook.edu"
      print(" "+str(gusage_total)+"%"+"    "+GRES+"  "+Nodes+" "+JobId+" ",UserId +" "+email_addr)
      #if (gusg >10 and gram>5) or int(tmax)> 56 or nalloc<=1:  # only 1 GPU?, not accurate...some jobs will run with GPU for a while, stop, maybe loading data, them run again..
      ngpus=len(gpuinfo.splitlines())
      #   continue   #>10% of GPU average usage, OK, next.
      nodeinfo=" "+str(gusage_total)+"%"+"    "+GRES+"    "+Nodes+"     "+JobId+"     "+UserId
      #print("nodeinfo: ",nodeinfo)
      #h200x8-02: NVIDIA H200 NVL, 00000000:01:00.0, 47, 124199 MiB, 40 %, 7 %
#load+"%   "+node+"   "+str(gusg)+"%  "+str(gram)+"%  "+tmax+"deg     "+job+" "
      sub="GPU job "+JobId+" on NvWulf Cluster"
      contents="""
<body style="font-family: sans-serif, Arial, Helvetica;">    
<pre>    
Dear """+username+""":

It seems that your computing job """+JobId+""" is under utilizing the """+str(ngpus)+""" GPU(s) on the server. The brief job information is here:
     GPU%   Gindex   Nodes   JobID    User      
    """ + nodeinfo + """

Six samples taken over the last hour show that average usage is < 20%. The GPU information is as the following:
Node         GPU                 GPUID      T(C)  Gmem      GPU%  Gmem%
""" + gpuinfo + """

Please note that the current GPUs load on this node is """+str(gusage_total)+"""%, with """+str(ngpus)+""" GPU(s). It also may be that your job leaves 1 or more GPU(s) idling, which needs your attention.

If this GPU underutilization is unexpected, please review your job settings to ensure your job is functioning as intended and if not, adjust the settings and resubmit. In addition, to ensure fair access for all users, please do not leave GPUs idle once your job is no longer actively using them.

Please note: the under-utilization of your past and running GPU jobs' will negatively affect your new jobs' priority in the future.

If you need assistance or have any questions about how to optimize the resource usage for your job, please submit a ticket <a href="https://xxx.com/">HERE</a>.

Thanks!

HPC Support
</pre>

<body>
"""
#Please check your job settings to make sure your job is functioning well as expected, and it is actually using the resources that it intended to allocate.
#    print(sub)
#    print(contents)
      # -M 'text/html', for s-nail 14.9
      mail_cmd="mail -M 'text/html' -s '"+sub+"'  -r xxx@xxx   -b xxx@xxx  -c xxx@xxx "+email_addr+ "  <<EOF" +contents +"""EOF"""
      #print(mail_cmd)
      mailit = subprocess.getoutput(mail_cmd)


def check_lastrun(runningjobs):
    #file_path, global var for the bad candidate job list
    jcontents=""
    badcandidates=[]
    try:
        with open(file_path, 'r') as file:
            jcontents=file.read()
            jcontents=jcontents.rstrip() #strip the last line added by python
            for badjob in jcontents.split("\n"):
                if badjob.split(",")[0] in runningjobs:
                    badcandidates.append(badjob)
                    if debug>10:
                        print("Found bad job still running",badjob)

        # Perform operations with the file
    except FileNotFoundError:
        print(f"The file '{file_path}' does not exist. ")
        with open(file_path, 'w') as file:
            file.write(jcontents)
    except Exception as e:
        print(f"An error occurred: {e}")
    return(badcandidates)


# Now Start to work
# get Running jobs only
runningjobs = subprocess.getoutput("/cm/shared/apps/slurm/current/bin/squeue -a -h -t r|awk {'print $1'}|tr '\n' ',' |sed 's/,$//'")
if debug > 10:
    print("Found Running jobs", runningjobs)

#Check jobstats from last run
running_bad_jobs=check_lastrun(runningjobs)
if debug > 10:
    print("Found all running bad jobs:")
    print(running_bad_jobs)

# Get jobs summary info fro Slurm
jobstats = subprocess.getoutput("/cm/shared/apps/slurm/current/bin/scontrol -a show job -d "+runningjobs+" |grep -e '     Nodes' -e JobId -e UserId -e RunTime|paste -d ' ' - - - -")
if debug > 10:
    print(jobstats)

#No we quickly get the sumary info from Slurm.
#
##JobId=3675 JobName=interactive    UserId=ksengupta(116591908) GroupId=ksengupta(116591908) MCS_label=N/A    RunTime=1-15:09:42 TimeLimit=2-00:00:00 TimeMin=N/A      Nodes=h200x4-04 CPU_IDs=0-3,8-18,32-33,38-50 Mem=65536 GRES=gpu:h200:2(IDX:0-1)
##JobId=3791 JobName=llm-description    UserId=zedong(113323874) GroupId=zedong(113323874) MCS_label=N/A      Nodes=h200x4-04 CPU_IDs=34-37 Mem=32768 GRES=gpu:h200:1(IDX:2)


#global job list
new_bad_jobs=[]
# Loop to check each job
for jobline in jobstats.split("\n"):
    if debug > 10:
       print("Found line",jobline)
    if "ArrayJobId" in jobline: # array jobs
        JobId,ArrayJobId,ArrayTaskId,JobName,UserId,GroupId,MCS_label,RunTime,TimeLimit,TimeMin,Nodes,CPU_IDs,Mem,GRES = jobline.split()
        JobId=ArrayJobId.split("=")[1]+"_"+ArrayTaskId.split("=")[1]
    else: #regular jobs
        JobId,JobName,UserId,GroupId,MCS_label,RunTime,TimeLimit,TimeMin,Nodes,CPU_IDs,Mem,GRES = jobline.split()
        JobId=JobId.split("=")[1]

    GRES=GRES.split("=")[1]
    RunTime=RunTime.split("=")[1]
    JobName=JobName.split("=")[1]
    CPU_IDs=CPU_IDs.split("=")[1]
    Mem=Mem.split("=")[1]
    UserId=UserId.split("=")[1]
    UserId=UserId.split("(")[0]
    GroupId=GroupId.split("=")[1]
    time=RunTime.split("-") # Now ignore days-, can take care of it later.
    Nodes=Nodes.split("=")[1]
    if debug > 10:
        print("Job: ",JobId,UserId,Nodes,GRES,RunTime)
            
    if GRES == "" or GRES is None:
        #print("Ignore CPU job")
        if debug > 10 :
            print(jobline)
            print("\n")
        continue
    #print(time, " len=",len(time),RunTime)
    #Now ignore days-, can take care of it later.
    runtime=sum(x * float(t) for x, t in zip([1, 60, 3600],reversed(time[len(time)-1].split(":"))))
    if(runtime<1800.0):  #30 minues
        print("This job has been running <=",runtime," seconds. Skips...")
        continue

    if "," in Nodes:
       print("Does not support multipple node yet: ",Nodes)
       continue
    
    GRES=GRES.split("IDX:")[1].split(")")[0]
    GRES=",".join(map(str,convert_compressed_string_to_list(GRES))) # convert the decompressed list to string.
    #This is slow, but is the easiest way to code
    #Check one job at  time, get the job's GPU utilization info.
    mycmd="pdsh -w "+Nodes+ " nvidia-smi   --query-gpu=gpu_name,gpu_bus_id,temperature.gpu,memory.used,utilization.gpu,utilization.memory --format=csv,noheader -i "+ GRES+ "  |sort  -k1 " 
    if debug > 10:
       print("mycmd= ", mycmd)
    gpuinfo=subprocess.getoutput(mycmd)
#
#   Something like this:
#   h200x8-02: NVIDIA H200 NVL, 00000000:11:00.0, 30, 1 MiB, 0 %, 0 %
#
    idle_gpus=0
    if debug > 10:
      print("gpuinfo ")
      print(gpuinfo)
      #print("\n")
    gusage_total=0 
    ngpus=len(gpuinfo.splitlines())
    #print(" # of GPUs: ",ngpus)
    for gpuline in gpuinfo.split("\n"):
        if debug > 10:
           print("gpuline: ",gpuline)
        gname,gid,gtemp,gmem,gpusage,gmemusage=gpuline.split(",")
        gpusage=gpusage.split("%")[0]
        if int(gpusage) < 1 :
            idle_gpus = idle_gpus + 1
        gusage_total = gusage_total + int(gpusage)
    gusage_total = gusage_total /ngpus
    if debug > 10:
        print("gusage_total: ",gusage_total)
    if gusage_total < 20 or idle_gpus > 0: #<20% usage?
        datastream={"Nodes":Nodes, "UserId":UserId,"JobId":JobId,"GRES":GRES,"gusage_total":gusage_total,"gpuinfo":gpuinfo}
        item=[str(JobId)+",1",datastream]
        new_bad_jobs.append(item)  # format: jobid, count #
        #email_users(Nodes,UserId,JobId,GRES,gusage_total,gpuinfo)

# new_bad_jobs is a complex list, contains detailed job info
if debug>10:
    print("new_bad_jobs= ",new_bad_jobs)
#
#Now compre the new bad_list to the old one. Keep the common ones.
#  running_bad_jobs  vs new_bad_jobs

updated_list = []
emailjobs =[]
            
for badjob in new_bad_jobs:
    count=1
    jobid=badjob[0].split(",")[0]  # Format "jobid,count", {DICT}
    result=[v for v in running_bad_jobs if jobid in v]
    if result:   # this job also shows in last run?
        #print("OK found: ", jobid)
        jobdetails= [ll for ll in running_bad_jobs if jobid in ll.split(",")[0]]
        #print("job details= ",jobdetails)
        lastjobid,count=jobdetails[0].split(",")
        if debug>10:
            print(lastjobid, " : ",count)
        if int(count) >=maxcount:
            updated_list.append(lastjobid+","+str(-int(emailcount)) )
            if debug > 10:
                print("Found under-performancing job, email user:",emailjobs)
            #OK let's email
            jobitem = badjob[1]  # Dic for job details
            if debug > 10:
                print(jobitem)
            email_users(jobitem["Nodes"],jobitem["UserId"],jobitem["JobId"],jobitem["GRES"],jobitem["gusage_total"],jobitem["gpuinfo"])
        else:
            updated_list.append(lastjobid+","+str(int(count)+1))
            if debug > 10:
                print("append update list:",updated_list)
    else: # brand new one?
        updated_list.append(badjob[0])
        if debug>10:
            print("append update list2:",updated_list)
    #jvars = badjob.split(",")
    #if jvars[0] in new_bad_jobs:
    #    if  jvars[1] >=2:  #if already found 2 times, plus this time, it is 3 times. Email the user.
    #        emailjobs.append[jvars[0]] # save jobid here
# pick up jobs that have emailed users. Keep the record, to avoid recounting from 1
for x in running_bad_jobs:
    tmp = x.split(",")[0]
    if  (not (any(tmp in sub for sub in updated_list))) and (int(x.split(",")[1]) < 0):
         updated_list.append(x)

with open(file_path, "w") as file:
    for item in updated_list:
        file.write(f"{item}\n")
