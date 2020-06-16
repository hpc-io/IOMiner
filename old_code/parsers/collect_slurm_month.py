from datetime import datetime
import subprocess
import os
import sys
import calendar
cluster_name = "cori"

"""
month = int(sys.argv[1])
day = int(sys.argv[2])

monthString = ""
if month == 3:
    monthString = "March2016"
elif month == 4:
    monthString = "April2016"
elif month == 5:
    monthString = "May2016"
else:
    monthString = "June2016"

host_name = os.getenv('HOSTNAME')
cluster_name = "local"
if host_name != None:
    if host_name.startswith("cori"):
        cluster_name = "cori"
    elif host_name.startswith("edison"):
        cluster_name = "edison"
    elif host_name.startswith("b"):
        cluster_name = "babbage"
    elif host_name.startswith("n"):
        cluster_name = "lawrencium"

start_date = datetime(2016,month,day,0,0,0)
end_date = datetime(2016,month,day,23,59,59)
"""

# out_dir = ('/global/homes/m/mbae/edison/ru/')
#out_dir = '/project/projectdirs/m1248/slurm/' + cluster_name +'/' + monthString + '/'

out_dir = '/project/projectdirs/m888/ssio/wyoo_data/slurm_copy/' + cluster_name +'/'
for i in range(6,7):
    start_date = datetime(2017,i,1,0,0,0)
    end_date = datetime(2017,i,calendar.monthrange(2017,i)[1],23,59,59)
    start_date = datetime(2017,i-1,20,0,0,0)
    end_date = datetime(2017,i,20,0,0,0)
    start_time = start_date.strftime("%m/%d/%y-%H:%M:%S")
    end_time = end_date.strftime("%m/%d/%y-%H:%M:%S")
    filename = out_dir+cluster_name+'slurm_%s_%s.log'%(start_date.strftime("%y_%m_%d-%H-%M-%S"),end_date.strftime("%y_%m_%d-%H-%M-%S"))
    # print filename
    
    cmd = ['sacct','--allusers',
            '--starttime=' + start_time,
            '--endtime=' + end_time,
            '--state=CD',
            '--format=JobID%20,User%15,jobname%50,Start%22,End%22,Elapsed%20,State%20,AllocNodes,Ntasks,\
            AllocCPUs,ReqCPUS,SystemCPU%15,UserCPU%15,TotalCPU%16,\
            AveCPU%15,MinCPU%15,MinCPUNode,MinCPUTask,\
            AveVMSize%15,MaxVMSize%15,MaxVMSizeNode,MaxVMSizeTask,\
            AveRSS%15,MaxRSS%15,MaxRSSNode,MaxRSSTask,\
            AvePages%20,MaxPages%20,MaxPagesNode,MaxPagesTask,\
            AllocGRES%20,ReqGres%20,AveCPUFreq, ReqCPUFreqMin, ReqCPUFreqMax, ReqCPUFreqGov,\
            ConsumedEnergy,Layout,Partition%10,ExitCode%10,NodeList%600']
            # AveDiskRead, MaxDiskRead, MaxDiskReadNode, MaxDiskReadTask,\
            # AveDiskWrite, MaxDiskWrite, MaxDiskWriteNode, MaxDiskWritetask,\
    
    
    with open(filename, "w") as outfile:
        #subprocess.call(cmd)
        subprocess.call(cmd, stdout=outfile)
    
    #print start_date.strftime("%m/%d/%y-%H:%M:%S")
