# -*- coding: utf-8 -*-
"""
Created on Sat Sep  5 15:59:31 2020

@author: tzw00
"""

import numpy as np
from bitmap import *
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
import matplotlib
import argparse
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd
import multiprocessing
from multiprocessing import Manager

DARSHAN_FILE_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
darshan_file_pattern = re.compile(DARSHAN_FILE_PATTERN)

VERSION_PATTERN = '(#\s+)(darshan log version):\s+(\d+(?:\.\d+)?)'
darshan_version_pattern = re.compile(VERSION_PATTERN)
file_pos = 0
darshan_df = pd.DataFrame()
def GetDarshanJobId(s):
    DARSHAN_FILE_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
    darshan_file_pattern = re.compile(DARSHAN_FILE_PATTERN)
    match = darshan_file_pattern.match(s)
    if match is not None:
        return match.group(2)
    else:
        return "0"

def FormatFileList(format_darshan_dir, file_list):
    print("processing file directory %s, pid:%d\n"%(format_darshan_dir, os.getpid()))
    sys.stdout.flush()
    tot_stat_file_name = format_darshan_dir + "tot_stat.pkl"
    tot_fd = open(tot_stat_file_name, 'wb')
        
    perfile_stat_file_name = format_darshan_dir + "perfile_stat.log"
    perfile_fd = open(perfile_stat_file_name, 'wb')
    darshan_df = pd.DataFrame()
    counter = 0
    for fname in file_list:
        job_key = FormStatDict(fname, darshan_df, tot_fd, perfile_fd)
        if job_key == -1:
            counter += 1
        if "darshan_version" in darshan_df.columns:
        #    print("######fname:%s, darshan_version:%s"%(fname, darshan_df.loc[job_key, "darshan_version"]))
            try:
                if not pd.isnull(darshan_df.loc[job_key, "darshan_version"]):
                    if float(darshan_df.loc[job_key, "darshan_version"]) < 3.1:
                        # print("fname:%sdarshan_version:%s"%(fname, darshan_df.loc[job_key, "darshan_version"]))
                        continue;
            except:
                print("file format error %s, darshan format:%s\n"%(fname, darshan_df.loc[job_key, "darshan_version"]))

    pickle.dump(darshan_df, tot_fd, -1)
    tot_fd.close()
    perfile_fd.close()
    return counter

    

def FormatParsedFiles(parsed_darshan_root, 
                      format_darshan_root, 
                      start_time, 
                      end_time):
    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    d = datetime(start_date.year,start_date.month,start_date.day)
    delta = timedelta(days=1)
    file_list = []
    zip_file_list = []
    results = []
    while d <= end_date:
        parsed_darshan_files = parsed_darshan_root + d.strftime("%Y/%-m/%-d/*.darshan.all")
        print("parsed file is %s\n"%parsed_darshan_files)
        sys.stdout.flush()
        format_darshan_dir = format_darshan_root + d.strftime("%Y/%-m/%-d/")
        if not os.path.exists(format_darshan_dir):
            try:
                os.makedirs(format_darshan_dir)
            except OSError as exc:  # Python >2.5
                log_str = "fail to make directory\n"
                logging.info(log_str)
                if exc.errno == errno.EEXIST and os.path.isdir(format_darshan_dir):
                    pass
                else:
                    raise
        file_list = glob.glob(parsed_darshan_files)
        d += delta

        result = processes.apply_async(FormatFileList, [format_darshan_dir, file_list])
        results.append((result, format_darshan_dir))
#        status = result.get()
#        print("%s result is %d\n"%(format_darshan_dir, status))
    return results
        

 

def GetDarshanUserAppname(s):
    DARSHAN_FILE_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
    darshan_file_pattern = re.compile(DARSHAN_FILE_PATTERN)
    match = darshan_file_pattern.match(s)
    if match is not None:
        return match.group(1)
    else:
        return None  
    
def GetDarshanVersion(s):
    VERSION_PATTERN = '(#\s+)(darshan log version):\s+(\d+(?:\.\d+)?)'
    darshan_version_pattern = re.compile(VERSION_PATTERN)
    version_match = darshan_version_pattern.match(s)
    if version_match is not None:
        counter_val = version_match.group(3)
    else:
        return None
        
def FormStatDict(darshan_fname, darshan_df, tot_fd, perfile_fd):
    print("darshan_fname:%s"%darshan_fname)
    sys.stdout.flush()
    suffix = darshan_fname.rsplit('/')[-1]
    
    DARSHAN_FILE_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
    darshan_file_pattern = re.compile(DARSHAN_FILE_PATTERN)
    match = darshan_file_pattern.match(suffix)
    if match is not None:
        job_id = match.group(2)
    else:
        return -1
    
    darshan_file_df = pd.DataFrame()
    darshan_df.loc[job_id, "file_name"] = darshan_fname


    app_user_name = match.group(1).split("_")
    darshan_df.loc[job_id, "user_name"] = app_user_name[0]
    darshan_df.loc[job_id, "app_name"] = app_user_name[1]

    
    version_time = 0
    with open(darshan_fname) as infile:
        for line in infile:
            VERSION_PATTERN = '(#\s+)(darshan log version):\s+(\d+(?:\.\d+)?)'
            darshan_version_pattern = re.compile(VERSION_PATTERN)
            version_match = darshan_version_pattern.match(line)
            if version_match is not None:
                counter_val = version_match.group(3)
                darshan_df.loc[job_id, "darshan_version"] = counter_val
#                print("darshan name here:%s, version:%s\n"%(darshan_fname, counter_val))
            
            HEADER_PATTERN = '(#\s+)(\S+):(\s+)(\d+)'
            header_pattern = re.compile(HEADER_PATTERN)
            header_match = header_pattern.match(line)
            if header_match is not None:
                counter_key = header_match.group(2)
                counter_val = header_match.group(4)
                darshan_df.loc[job_id, counter_key] = counter_val
    #            print "key:%s, val:%s\n"%(counter_key, counter_val)
            KEY_VALUE_PATTERN = '(\S+):(\s+)([+-]?\d+(?:\.\d+)?)'
            kv_pattern = re.compile(KEY_VALUE_PATTERN)
            kv_match = kv_pattern.match(line)
            if kv_match is not None:
                counter_key = kv_match.group(1)
#                counter_val = float(kv_match.group(3))
                counter_val = kv_match.group(3)
#                if cmp(counter_key.strip(), "total_POSIX_OPENS"):
#                    out_open_count = counter_val
#                if cmp(counter_key.strip(), "total_POSIX_READS"):
#                    out_read_count = counter_val
#                if cmp(counter_key.strip(), "total_POSIX_WRITES"):
#                    out_write_count = counter_val
#                if cmp(counter_key.strip(), "total_POSIX_WRITES"):
#                    out_write_count = counter_val
                darshan_df.loc[job_id, counter_key] = counter_val


     # unique: 0 0 0
     # shared: 1 6442450944 6442450944
    IO_MODE_PATTERN = '(#\s+)(\S+):\s+(\d+)\s+(\d+)\s+(\d+)'
  
    # unique files: slowest_rank_io_time: 0.000000
    # unique files: slowest_rank_meta_only_time: 0.000000
    # unique files: slowest rank: 0
    IND_SHARED_PATTERN = '(#\s+)(\S+\s+\S+):\s+(\S+):\s+(\d+\.\d+)'
    
    # This is log for each file
    PER_FILE_PATTERN = '([^#]+)(\s+)(\S+)(\s+)(\d+)(\s+(\S+)){12,}'
    
    # Performance pattern, bandwidth
    PERF_PATTERN = '(#\s+)(\S+):\s+(\d+\.\d+)'
    
    # This is log for each process on each file
    PER_PROC_FILE_PATTERN = '(\S+)\t([+-]?\d+(?:\.\d+)?)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t(\S+)\t(\S+)'
   
    proc_file_pattern = re.compile(PER_PROC_FILE_PATTERN)

  
    darshan_io_mode_pattern = re.compile(IO_MODE_PATTERN) 
    darshan_ind_shared_pattern = re.compile(IND_SHARED_PATTERN)
    darshan_proc_file_pattern = re.compile(PER_PROC_FILE_PATTERN)
    darshan_per_file_pattern = re.compile(PER_FILE_PATTERN)
    darshan_perf_pattern = re.compile(PERF_PATTERN)

    with open(darshan_fname) as infile:
        for line in infile:
            matched = 0
            if line.find("total_MPIIO") != -1:
                key_prefix="MPIIO_"
            if line.find("total_STDIO") != -1:
                key_prefix="STDIO_"
            if line.find("total_POSIX") != -1:
                key_prefix="POSIX_"
                
            proc_file_match = proc_file_pattern.match(line)
            if proc_file_match is not None:
                matched = 1
                file_name = proc_file_match.group(6).strip()
#                print("line is %s"%line)
                per_file_key = proc_file_match.group(4).strip()
                per_file_val = proc_file_match.group(5).strip()
#                if "version" in per_file_key:
#                    print("found, fname is %s"%darshan_fname)

                if file_name not in darshan_file_df.index:
#                    print("fname1:%s, key:%s, value:%s\n"%(file_name, per_file_key, per_file_val))
#                    print("fs_type is %s, name is %s"%(proc_file_match.group(8).strip(), file_name))
                    darshan_file_df.loc[file_name, "fs_type"] = proc_file_match.group(8).strip()
                    darshan_file_df.loc[file_name, "filename"] = file_name
             
#                print("fname2:%s, key:%s, value:%s\n"%(file_name, per_file_key, per_file_val))
                flag = 0
                if per_file_key not in darshan_file_df.columns or pd.isnull(darshan_file_df.loc[file_name, per_file_key]):
                    darshan_file_df.loc[file_name, per_file_key] = per_file_val
                    flag = 1
                # the same file (file_name)  can be accessed by different ranks. As Darshan generates the same set of its counters for each rank accessing this file, we select the earliest POSIX_F_READ_START_TIMESTAMP of all POSIX_F_READ_START_TIMESTAMP counters as this file's read start time. Same reasoning applies for other counters in the following code. 
                if "POSIX_F_READ_START_TIMESTAMP" in per_file_key:
                    flag = 1
                    
                    if float(per_file_val) < float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
                if "POSIX_F_READ_END_TIMESTAMP" in per_file_key:
                    flag = 1
                    if float(per_file_val) > float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
                if "POSIX_F_WRITE_START_TIMESTAMP" in per_file_key:
                    flag = 1
                    if float(per_file_val) < float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
                if "POSIX_F_WRITE_END_TIMESTAMP" in per_file_key:
                    flag = 1
                    if float(per_file_val) > float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
                if "POSIX_F_OPEN_START_TIMESTAMP" in per_file_key:
                    flag = 1
                    if float(per_file_val) < float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
                if "POSIX_F_OPEN_END_TIMESTAMP" in per_file_key:
                    flag = 1
                    if float(per_file_val) > float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val

                if "POSIX_F_CLOSE_START_TIMESTAMP" in per_file_key:
                    flag = 1
                    if float(per_file_val) < float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
                if "POSIX_F_CLOSE_END_TIMESTAMP" in per_file_key:
                    flag = 1
                    if float(per_file_val) > float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
                if "POSIX_F_READ_TIME" in per_file_key:
                    flag = 1
                    if float(per_file_val) > float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val
#                        print("filename:%s, key:%s, value:%s\n"%(file_name, per_file_key, per_file_val))
                if "POSIX_F_WRITE_TIME" in per_file_key:
                    flag = 1
                    if float(per_file_val) > float(darshan_file_df.loc[file_name, per_file_key]):
                        darshan_file_df.loc[file_name, per_file_key] = per_file_val

                if "LUSTRE_STRIPE_WIDTH" in per_file_key:
                    flag = 1
                if "LUSTRE_STRIPE_SIZE" in per_file_key:
                    flag = 1
                if "LUSTRE_OST_ID" in per_file_key:
                    flag = 1
                if flag == 0:
                    cur_val = float(darshan_file_df.loc[file_name, per_file_key])
                    cur_val = cur_val + float(per_file_val)
                    darshan_file_df.loc[file_name, per_file_key] = cur_val
#                print("key:%s, value:%s\n"%(per_file_key, darshan_file_df.loc[file_name, per_file_key]))

                        
                if "LUSTRE_OST_ID" in per_file_key:
                    ost_id = per_file_key.rsplit('_', 1)[1]
                    real_ost_id = int(per_file_val)
#                    print("real_ost_id is %d"%int(per_file_val))
                    # store all OSTs of a file in bitmap
                    if "OST_MAP" not in darshan_file_df.columns or pd.isnull(darshan_file_df.loc[file_name, "OST_MAP"]):
                        bitmap = Bitmap(279)
                        bitmap.set(int(real_ost_id))
                        darshan_file_df.loc[file_name, "OST_MAP"] = bitmap
                    else:
                        bitmap = darshan_file_df.loc[file_name, "OST_MAP"]
                        bitmap.set(int(real_ost_id))
                        darshan_file_df.loc[file_name, "OST_MAP"] = bitmap

#                print "fname:%s, key:%s, value:%s, fs_type:%s\n"%(file_name, per_file_key, per_file_dict[file_name][per_file_key], per_file_dict[file_name]["fs_type"])
            if matched == 1:
                continue
            io_mode_match = darshan_io_mode_pattern.match(line)

            #IO_MODE_PATTERN = '(#\s+)(\S+):\s+(\d+)\s+(\d+)\s+(\d+)'
            if io_mode_match is not None:
                matched = 1
                tmp_key_prefix = key_prefix + io_mode_match.group(2)
                tmp_file_count = io_mode_match.group(3)
                tmp_io_bytes = io_mode_match.group(4)
                # total: 4 3596 899
                # read_only: 0 0 0
                # write_only: 4 3596 899
                # read_write: 0 0 0
                # unique: 4 3596 899
                # shared: 0 0 0

                darshan_df.loc[job_id, tmp_key_prefix+"_file_count"] = tmp_file_count
                darshan_df.loc[job_id, tmp_key_prefix+"_io_bytes"] = tmp_io_bytes
             #   print "key,value is %s,%s\n"%(tmp_key, tmp_val)
            if matched == 1:
                continue
            ind_shared_match = darshan_ind_shared_pattern.match(line)
            if ind_shared_match is not None:
                matched = 1
                tmp_key = key_prefix + ind_shared_match.group(2)
                tmp_val = ind_shared_match.group(4) 

                if tmp_key.find("unique files") != -1:
                    middle_key = ind_shared_match.group(3)
                    if middle_key.find("slowest_rank_io_time") != -1:
                        darshan_df.loc[job_id, "unique_time_by_slowest"] = tmp_val
             #           print "key,value %s,%s\n"%(tmp_key, tmp_val)
                if tmp_key.find("shared files") != -1:
                    middle_key = ind_shared_match.group(3)
                    if middle_key.find("shared_time_by_slowest") != -1:
                        darshan_df.loc[job_id, tmp_key] = tmp_val
             #           print "key,value %s,%s\n"%(tmp_key, tmp_val)
            if matched == 1:
                continue


            perf_pattern_match = darshan_perf_pattern.match(line)
            if perf_pattern_match is not None:
                tmp_key = key_prefix + perf_pattern_match.group(2)
                tmp_val = perf_pattern_match.group(3)
                darshan_df.loc[job_id, tmp_key] = tmp_val
               # print "######perf:counter:%s, value:%s\n"%(tmp_key, perf_pattern_match.group(3))

            per_file_pattern_match = darshan_per_file_pattern.match(line)
#            print "here, file_pattern_match is :%lf\n"%file_pattern_match_time
            if per_file_pattern_match is not None:
                matched = 1
                file_name = line.split("\t")[1]
                nprocs = line.split("\t")[2]
                darshan_file_df.loc[file_name, "nprocs"] = nprocs
               # print "###path:%s, matched file is %s, nprocs is %s\n"%(out_dict["FileName"], file_name, nprocs)

    serialize_obj = pickle.dumps(darshan_file_df);
    
    global file_pos
    perfile_fd.seek(file_pos)
    perfile_fd.write(serialize_obj)
    darshan_df.loc[job_id, "EXTERNAL_FILE_OFFSET"] = str(file_pos);
    darshan_df.loc[job_id, "EXTERNAL_FILE_LENGTH"] = str(len(serialize_obj))
    file_pos += len(serialize_obj)
#    job_key = str(darshan_df.loc[job_id, "start_time"]) + "#" +  str(job_id)
#    print("job key is %s\n"%job_key)
#    print("darshan name here1:%s, version:%s\n"%(darshan_fname, darshan_df.loc[job_id, "darshan_version"]))
#    darshan_df.rename(index = {job_id:job_key}, inplace = True)
#    print("darshan name here2:%s, version:%s, start time:%s\n"%(darshan_fname, darshan_df.loc[job_key, "darshan_version"], darshan_df.loc[job_key, "start_time"]))
#    print("app_name is %s\n"%darshan_df.loc[job_key, "app_name"])

    # Test read
#    test_offset = int(darshan_df.loc[job_key, "EXTERNAL_FILE_OFFSET"])
#    test_len = int(darshan_df.loc[job_key, "EXTERNAL_FILE_LENGTH"])

#    perfile_fd.seek(test_offset)
#    serialized_obj = perfile_fd.read(test_len)
#    test_darshan_df = pickle.loads(serialized_obj)
#    print("##########here is the darshan info, length is %d\n"%test_len)
#    data_top = test_darshan_df.head()
#    print(data_top)
#    print(list(data_top.index.values))
#    idx_lst = test_darshan_df.index
#    print("######index is \n")
#    print(list(idx_lst))
#    print(list(test_darshan_df))

    return job_id

cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
cmd_parser.add_argument("src_dir", help = "top directory of parsed Darshan log")
cmd_parser.add_argument("dst_dir", help = "top directory of formatted Darshan")
cmd_parser.add_argument("--thread_count", default=1, type=int, help = "number of parser threads")
args = cmd_parser.parse_args()

start_date = str(args.start_date)
end_date = str(args.end_date)

start_date += " 00:00:00"
end_date += " 23:59:59"
src_darshan_dir = args.src_dir
dst_darshan_dir = args.dst_dir

NTHREADS = args.thread_count
if src_darshan_dir[len(src_darshan_dir) - 2] != '/':
    src_darshan_dir = src_darshan_dir + '/'
if dst_darshan_dir[len(dst_darshan_dir) - 2] != '/':
    dst_darshan_dir = dst_darshan_dir + '/'
start_date_arr = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
int_start_date = int(time.mktime(start_date_arr))

end_date_arr = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
int_end_date = int(time.mktime(end_date_arr))

# NTHREADS = multiprocessing.cpu_count()
processes = multiprocessing.Pool(NTHREADS)

results = FormatParsedFiles(src_darshan_dir, 
                            dst_darshan_dir, 
                            int_start_date, 
                            int_end_date)
for elem in results:
    print("error file of dir:%s is %d\n"%(elem[1], elem[0].get()))


processes.close()
processes.join()
