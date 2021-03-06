# -*- coding: utf-8 -*-
"""
Created on Sun Jun 20 11:45:50 2021

@author: tzw00
"""

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import copy
import argparse
import subprocess
import os
import re
import ntpath
import math
import glob
import numpy as np
import sys
import datetime
import time
import os
import re
from datetime import datetime,timedelta
import glob
import subprocess
import errno
import json
import pickle
import logging
import pickle
import string
import re
import multiprocessing
from multiprocessing import Manager

def output_exists(darshan_log, cpy_darshan_dir):
    filename = cpy_darshan_dir+darshan_log.rpartition('/')[2]
    return (os.path.exists(filename) and os.path.getsize(filename) > 0)

def save_cpy_output(darshan_log, cpy_darshan_dir):
    filename = cpy_darshan_dir+darshan_log.rpartition('/')[2]
    cmd = "cp %s %s"%(darshan_log, filename)
#    print("##########cmd is %s\n"%cmd)
    ret = subprocess.call(cmd, shell=True)
    return ret

def repl_slurm_dir(dst_slurm_dir,\
                   start_time,\
                       end_time):
    cmd = ['sacct','--allusers','--parsable',
        '--starttime=' + start_time,
        '--endtime=' + end_time,
        '--format=JobID%20,User%15,jobname%50,Start%22,End%22,Elapsed%20,State%20,AllocNodes,Ntasks,\
        AllocCPUs,ReqCPUS,SystemCPU%15,UserCPU%15,TotalCPU%16,\
        AveCPU%15,MinCPU%15,MinCPUNode,MinCPUTask,\
        AveVMSize%15,MaxVMSize%15,MaxVMSizeNode,MaxVMSizeTask,\
        AveRSS%15,MaxRSS%15,MaxRSSNode,MaxRSSTask,\
        AvePages%20,MaxPages%20,MaxPagesNode,MaxPagesTask,\
        AllocTRES%20,ReqTRES%20,AveCPUFreq, ReqCPUFreqMin, ReqCPUFreqMax, ReqCPUFreqGov,\
        ConsumedEnergy,Layout,Partition%10,ExitCode%10,NodeList%600']
        
    start_date = datetime.strptime(start_time, "%m/%d/%y-%H:%M:%S" )
    end_date = datetime.strptime(end_time, "%m/%d/%y-%H:%M:%S")
    fmt_start_date = start_date.strftime("%m/%d/%y-%H:%M:%S")
    fmt_end_date = end_date.strftime("%m/%d/%y-%H:%M:%S")
    
    fname = dst_slurm_dir + "/slurm_%d_%d.log"%(datetime.timestamp(start_date), datetime.timestamp(end_date))
    ret = 0
    with open(fname, "w") as outfile:
        ret = subprocess.call(cmd, stdout=outfile)
    
    if (ret != 0):
        print("fail to process slurm command\n")
        return -1

def repl_darshan_dir(src_darshan_dir, dst_darshan_dir,\
                      start_time,\
                      end_time):

    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    
    d = datetime(start_date.year,start_date.month,start_date.day)

    delta = timedelta(days=1)
    
    file_list = []
    zip_file_list = []
    
#    print("start date:%s, end_date:%s, src_dir:%s, dst_dir:%s\n"%(start_time, end_time, src_darshan_dir, dst_darshan_dir))
   
    while d <= end_date:
        darshan_files = src_darshan_dir + d.strftime("%Y/%m/%d/*.darshan").replace('/0', '/')
        darshan_zip_files = src_darshan_dir + d.strftime("%Y/%m/%d/*.darshan.gz").replace('/0', '/')
        cpy_darshan_dir = dst_darshan_dir + d.strftime("%Y/%m/%d/").replace('/0', '/')
        # print("dst_darshan_dir is %s\n"%dst_darshan_dir)
        if not os.path.exists(cpy_darshan_dir):
        #    print("%s not exists\n"%cpy_darshan_dir)
            try:
                os.makedirs(cpy_darshan_dir)
            except OSError as exc:  # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(cpy_darshan_dir):
                    pass
                else:
                    raise
        # print("darshan_files:%s\n"%darshan_files)
        file_list = glob.glob(darshan_files)
        zip_file_list = glob.glob(darshan_zip_files)
        d += delta

        results = []
        for my_file in file_list:
            # print("my file is %s\n"%my_file)
            if not output_exists(my_file, cpy_darshan_dir):
              #  save_cpy_output(my_file, cpy_darshan_dir)
              result = processes.apply_async(save_cpy_output, [my_file, cpy_darshan_dir])
              results.append((result, my_file))
        for elem in results:
            if elem[0].get() < 0:
                print("Fail to procss file %s\n"%(elem[1]))
                return -1
    return 0
        
def repl_lmt_dir(src_lmt_dir, dst_lmt_dir,\
                      start_time,\
                      end_time):
    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    
    d = datetime(start_date.year,start_date.month,start_date.day)

    delta = timedelta(days=1)
    
    file_list = []
   
    while d <= end_date:
        lmt_files = src_lmt_dir + d.strftime("%Y-%m-%d/*")
        cpy_lmt_dir = dst_lmt_dir + d.strftime("%Y-%m-%d/")
        if not os.path.exists(cpy_lmt_dir):
            try:
                os.makedirs(cpy_lmt_dir)
            except OSError as exc:  # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(cpy_lmt_dir):
                    pass
                else:
                    raise
                    
        file_list = glob.glob(lmt_files)
        d += delta

        results = []
        for my_file in file_list:
    #        print("my file is %s\n"%my_file)
            if not output_exists(my_file, cpy_lmt_dir):
            #    save_cpy_output(my_file, cpy_lmt_dir)
                result = processes.apply_async(save_cpy_output, [my_file, cpy_lmt_dir])
                results.append((result, my_file))
        for elem in results:
            if elem[0].get() < 0:
                print("Fail to procss file %s\n"%(elem[1]))
                return -1
    return 0      
                
def is_output_saved(darshan_log, parsed_darshan_dir, flag='total'):
#    print("processing file %s, pid:%d\n"%(darshan_log, os.getpid()))
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.'+flag
    return (os.path.exists(filename) and os.path.getsize(filename) > 0)


cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
cmd_parser.add_argument("src_dir", help = "top directory of Darshan")
cmd_parser.add_argument("dst_dir", help = "top directory of parsed Darshan")
cmd_parser.add_argument("--thread_count", default=1, type=int, help = "number of parser threads")
cmd_parser.add_argument("--repl_type", help = "choose between lmt or darshan")

args = cmd_parser.parse_args()

start_date = str(args.start_date)
end_date = str(args.end_date)

#print("type is %s\n"%args.repl_type)
if args.repl_type == "lmt":
    is_repl_lmt = True
    is_repl_slurm = False
elif args.repl_type == "slurm":
    is_repl_lmt = False
    is_repl_slurm = True
    
start_date += " 00:00:00"
end_date += " 23:59:59"
src_dir = args.src_dir
dst_dir = args.dst_dir

NTHREADS = args.thread_count
if src_dir[len(src_dir) - 2] != '/':
    src_dir = src_dir + '/'
if dst_dir[len(dst_dir) - 2] != '/':
    dst_dir = dst_dir + '/'

start_date_arr = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
int_start_date = int(datetime.timestamp(start_date_arr))
str_start_time = start_date_arr.strftime("%m/%d/%y-%H:%M:%S")

end_date_arr = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
int_end_date = int(datetime.timestamp(end_date_arr))
str_end_time = end_date_arr.strftime("%m/%d/%y-%H:%M:%S")

NTHREADS = multiprocessing.cpu_count()
processes = multiprocessing.Pool(NTHREADS)

if is_repl_lmt:
    repl_lmt_dir(src_dir, dst_dir,\
                     int_start_date,\
                         int_end_date)
elif is_repl_slurm:
    repl_slurm_dir(dst_dir,\
                      str_start_time, \
                         str_end_time)
else:
    repl_darshan_dir(src_dir, dst_dir,\
                 int_start_date,\
                     int_end_date)

processes.close()

processes.join()
