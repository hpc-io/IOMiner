# -*- coding: utf-8 -*-
"""
Created on Sat Jul 10 10:54:46 2021

@author: tzw00
"""
import argparse
import collections
import json
from collections import OrderedDict
import pickle
import datetime
from datetime import datetime,timedelta
import time
import pandas as pd
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
#from slurm_plot import *

def join_pd(job_tot_df, job_day_df_lst):
    for job_day_df in job_day_df_lst:
#        job_day_df.info()
        for (row_label, row_series) in job_day_df.iterrows():
            for column,value in row_series.items():
                job_tot_df.loc[row_label, column] = value

def convert_to_df(f, dst_dir, start_ts, end_ts):
    
    filename_w_date = "%s/slurm_df_%d_%d"%(dst_dir, start_ts, end_ts) 
    try:
        tot_fd = open(filename_w_date, 'wb')
    except IOError as e:
        print("Fail to open file %s:%s"%(filename_w_date, e))
        return -1
  
    text = f.readline()
    header_lines = text.split("|")
    useful_metas = ["JobID", "User", "JobName", "Start", "End", "Elapsed", "State", "AllocNodes", "NTasks", "AllocCPUS", "ReqCPUS"]

    slurm_df = pd.DataFrame()

    glb_cursor = 0
    while True:
        text = f.readline()
#        print("line is %s, glb_cursor is %d\n"%(text, glb_cursor))
        if not text:
            break
        if glb_cursor >= 0:
            cursor = 0
            tmp_dict = {}
            record_lines = text.split("|")
            job_name = record_lines[0].split('.')[0].split('_')[0]
#            print("job_name is %s\n"%job_name)
            if job_name in slurm_df.index:
                cur_cnt = int(slurm_df.loc[job_name, "count"])
                slurm_df.loc[job_name, "count"] = cur_cnt + 1
                job_name = job_name + "." + str(cur_cnt + 1)

#               print "job_name is %s, end:%s, cursor:%d, user:%s, app:%s\n"%(tmp_dict["JobID"], tmp_dict["End"], glb_cursor, tmp_dict["User"], tmp_dict["JobName"]) 
            else:
                slurm_df.loc[job_name, "count"] = 1
               
#            print("job name:%s\n"%job_name)
            for tmp_str in record_lines:
                if header_lines[cursor] not in useful_metas:
                    cursor += 1
                    continue
                if "JobID" == header_lines[cursor]:
                    if "COMPLETED" not in tmp_str:
                        cursor += 1
                        continue
        
#                    if "batch" in tmp_str or "extern" in tmp_str:
#                        cursor += 1
#                        continue
                
                slurm_df.loc[job_name, header_lines[cursor]] = tmp_str
                
                if header_lines[cursor] == "NTasks":
                    str_ntasks = slurm_df.loc[job_name, header_lines[cursor]]
                    if tmp_str != "":
                        idx = str_ntasks.find('K')
                        if idx != -1: 
                            ntasks = float(str_ntasks[0:idx]) * 1000
                            ntasks = int(ntasks)
                            slurm_df.loc[job_name, header_lines[cursor]] = ntasks
                if header_lines[cursor] == "Elapsed":
                    time_str = slurm_df.loc[job_name, header_lines[cursor]]
                    slurm_df.loc[job_name, header_lines[cursor]] = get_sec(time_str)
                if header_lines[cursor] == "AllocCPUS":
                    str_alloc_cpus = slurm_df.loc[job_name, header_lines[cursor]] 
                    idx = str_alloc_cpus.find('K')
                    if idx != -1:
                        alloc_cpus = float(str_alloc_cpus[0:idx]) * 1000
                        slurm_df.loc[job_name, header_lines[cursor]]  = int(alloc_cpus)
                if header_lines[cursor] == "AllocNodes":
                    str_alloc_nodes = slurm_df.loc[job_name, header_lines[cursor]] 
                    idx = str_alloc_nodes.find('K')
                    if idx != -1:
                        alloc_nodes = float(str_alloc_nodes[0:idx]) * 1000
                        slurm_df.loc[job_name, header_lines[cursor]]  = int(alloc_nodes)
#                print("%s:%s"%(header_lines[cursor], str(slurm_df.loc[job_name][header_lines[cursor]])))
                        
                cursor = cursor + 1
#                print "job_name is %s, end:%s, cursor:%d, user:%s, app:%s\n"%(tmp_dict["JobID"], tmp_dict["End"], glb_cursor, tmp_dict["User"], tmp_dict["JobName"]) 
        glb_cursor = glb_cursor + 1
#        print("\n")
    pickle.dump(slurm_df, tot_fd, -1)
    tot_fd.close()

def format_slurm_files(src_slurm_dir, dst_slurm_dir, start_ts, end_ts):
    
    fmt_slurm_files = src_slurm_dir + "slurm_*.log"
    print("fmt_slurm_files is %s\n"%fmt_slurm_files)
    file_list = glob.glob(fmt_slurm_files)
    qualified_files = []
    
    job_tot_df = pd.DataFrame()
    
    FNAME_PATTERN = 'slurm_([0-9]+)_([0-9]+).log'
    
    fname_pat = re.compile(FNAME_PATTERN)
    
    for cur_file in file_list:
        print("cur file is %s\n"%cur_file)
        fname_match = fname_pat.match(os.path.basename(cur_file))
        
        if not fname_match:
            print("Invalid darshan state name format:%s\n"%fmt_slurm_files)
            continue
        cur_strt_time = int(fname_match.group(1))
        cur_end_time = int (fname_match.group(2))
        
        if not (cur_end_time < start_ts or cur_strt_time > end_ts):
            qualified_files.append(cur_file)
    
    for cur_file in qualified_files:
        with open(cur_file) as fd:
            df = convert_to_df(fd, dst_slurm_dir, start_ts, end_ts )
            join_pd(job_tot_df, df)
#    
#
# 
#    tmp_str = "%sformat_slurm%s_%s.log"%(format_slurm_root, start_ts, end_ts)
#    save_fd = open(tmp_str, 'wb')
#    pickle.dump(job_tot_df, save_fd, -1)
#    save_fd.close()


def get_sec(time_str):
    arr = time_str.split('-')
    if len(arr) == 1:
        h, m, s = time_str.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)
    else:
        day_secs = 3600 * 24 * int(arr[0])
        h, m, s = arr[1].split(':')
        return  day_secs + int(h) * 3600 + int(m) * 60 + int(s)
    
    
cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
cmd_parser.add_argument("src_dir", help = "top directory of Darshan")
cmd_parser.add_argument("dst_dir", help = "top directory of parsed Darshan")


args = cmd_parser.parse_args()

start_date = str(args.start_date)
end_date = str(args.end_date)
    
start_date += " 00:00:00"
end_date += " 23:59:59"
src_dir = args.src_dir
dst_dir = args.dst_dir

if src_dir[len(src_dir) - 2] != '/':
    src_dir = src_dir + '/'
if dst_dir[len(dst_dir) - 2] != '/':
    dst_dir = dst_dir + '/'
start_date_arr = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
int_start_date = int(time.mktime(start_date_arr))

end_date_arr = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
int_end_date = int(time.mktime(end_date_arr))

format_slurm_files(src_dir, dst_dir, int_start_date, int_end_date)
            
