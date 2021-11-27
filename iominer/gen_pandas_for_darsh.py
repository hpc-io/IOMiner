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

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def ConstructDataFrame(format_darshan_root,
                       start_time, 
                       end_time):

    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    d = datetime(start_date.year, start_date.month, start_date.day)
    delta = timedelta(days=1)
    file_list = []
    zip_file_list = []
    results = []
    job_day_df_lst = []

    filename_w_date = "%s/darshan_state_%d_%d"%(format_darshan_root, start_time, end_time) 
    try:
        tot_fd = open(filename_w_date, 'wb')
    except IOError as e:
        print("Fail to open file %s:%s"%(filename_w_date, e))
        return -1

    job_tot_df = pd.DataFrame()
    while d <= end_date:
        format_darshan_dir = format_darshan_root + "/" + d.strftime("%Y/%-m/%-d/")
        if not os.path.exists(format_darshan_dir):
            d += delta
            continue

        tot_stat_file_name = format_darshan_dir + "perjob_stat3.log"
        perfile_stat_file_name = format_darshan_dir + "perfile_stat3.log"
        meta_stat_file_name = format_darshan_dir + "meta_stat3.log"

        if not os.path.exists(tot_stat_file_name):
            d += delta
            continue

        if not os.path.exists(perfile_stat_file_name):
            d += delta
            continue

        if not os.path.exists(meta_stat_file_name):
            d += delta
            continue

        result = processes.apply_async(DeserToDataFrame, [tot_stat_file_name, perfile_stat_file_name, meta_stat_file_name]) 
        results.append((result, format_darshan_dir))
        d += delta 
        counter = 0 
#    print("results is %d\n"%len(results))
    for elem in results:
        counter += elem[0].get()[0]
        job_day_df_lst.append(elem[0].get()[1])
        print("thread %d done with file %s"%(elem[0].get()[2], elem[0].get()[3]))
    job_tot_df = join_pd(job_day_df_lst)
    pickle.dump(job_tot_df, tot_fd, -1)
    return counter


def join_pd(job_day_df_lst):

    job_tot_df = pd.DataFrame()

    
    for job_day_df in job_day_df_lst:
        if job_tot_df.empty:
            job_tot_df = job_tot_df.append(job_day_df, ignore_index = True)
        else:
            job_tot_df = job_tot_df.append(job_day_df)
    return job_tot_df

def DeserToDataFrame(tot_stat_file_name, perfile_stat_file_name, meta_stat_file_name):
    try:
        perfile_fd = open(perfile_stat_file_name, 'r')
    except IOError as e:
        print("Fail to open file %s:%s"%(perfile_stat_file_name, e))
        return -1

    try:
        perjob_fd = open(tot_stat_file_name, 'rb')
    except IOError as e:
        print("Fail to open file %s:%s"%(tot_stat_file_name, e))
        return -1

    try:
        meta_fd = open(meta_stat_file_name, 'r')
    except IOError as e:
        print("Fail to open file %s:%s"%(meta_stat_file_name, e))
        return -1

    job_day_df = pd.DataFrame()
    job_day_dict = {}
    META_PATTERN = '([^:]+):([0-9]+):([0-9]+),([0-9]+):([0-9]+)'
    meta_pat = re.compile(META_PATTERN)
    counter = 0
    meta_tuples = []

    with open(meta_stat_file_name) as tmp_fp:
        line = tmp_fp.readline()
#        print("file is %s,line is %s\n"%(meta_stat_file_name, line))

        prev_cnt = 0
        counter = 0

        line_cnt = 0

        while line:
            match = meta_pat.match(line)
#            if line_cnt == 100:
#                break

            if match:
                jname = match.group(1)
#                print("jname is %s"%jname)
                if not jname:
                    counter += 1
                else:
                    try:
                        joffset = int(match.group(2))
                        jlen = int(match.group(3))
                        foffset = int(match.group(4))
                        flen = int(match.group(5))
                    except:
                        counter += 1
            else:
                print("Invalid python file format\n")
                quit()
            if prev_cnt == counter: #no error
                meta_tuples.append((jname, joffset, jlen, foffset, flen))
                prev_cnt = counter
            line = tmp_fp.readline()
            line_cnt += 1

    for (jname, joffset, jlen, foffset, flen) in meta_tuples:
        perjob_fd.seek(joffset)
        serialized_job_obj = perjob_fd.read(jlen)
        try:
            jdarshan_df = pickle.loads(serialized_job_obj)
        except:
            print("###failed file is %s\n"%jname)
            continue

        for row in range(len(jdarshan_df)):
            for column, value in jdarshan_df.iloc[row].items():
#                print("column:%s, value:%s, jname:%s\n"%(column, value, jname));
                if job_day_dict.get(jname, -1) == -1:
                    job_day_dict[jname] = {}
                job_day_dict[jname][column] = value
                job_day_dict[jname]["DETAIL_LOG_OFFSET"] = foffset
                job_day_dict[jname]["DETAIL_LOG_LEN"] = flen
                job_day_dict[jname]["DETAIL_LOG_FNAME"] = perfile_stat_file_name 
        print("finished processing job:%s, pid:%d\n"%(jname, os.getpid()))

    job_day_df = pd.DataFrame.from_dict(job_day_dict, orient = 'index')

    return (counter, job_day_df, os.getpid(), tot_stat_file_name)

cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
cmd_parser.add_argument("format_dir", help = "directory containing the formatted  dataframe for specified date")
cmd_parser.add_argument("--thread_count", default=1, type=int, help = "number of parser threads")
args = cmd_parser.parse_args()

start_date = str(args.start_date)
end_date = str(args.end_date)

start_date += " 00:00:00"
end_date += " 23:59:59"
src_darshan_dir = args.format_dir

NTHREADS = args.thread_count
if src_darshan_dir[len(src_darshan_dir) - 2] != '/':
    src_darshan_dir = src_darshan_dir + '/'
start_date_arr = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
int_start_date = int(time.mktime(start_date_arr))

end_date_arr = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
int_end_date = int(time.mktime(end_date_arr))

job_tot_df = pd.DataFrame()
meta_tot_df = pd.DataFrame()

processes = multiprocessing.Pool(NTHREADS)

tot_df = ConstructDataFrame(src_darshan_dir, 
                            int_start_date, 
                            int_end_date)


processes.close()
processes.join()
