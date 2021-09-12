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

def parse_darshan_dir(src_darshan_dir, dst_darshan_dir,\
                      start_time,\
                      end_time,\
                      ext = 'total',\
                      version=3):

    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    
    d = datetime(start_date.year,start_date.month,start_date.day)

    delta = timedelta(days=1)
    
    file_list = []
    zip_file_list = []
  
    suc_count = 0
    err_count = 0
    while d <= end_date:
        darshan_files = src_darshan_dir + d.strftime("%Y/%m/%d/*.darshan").replace('/0', '/')
        darshan_zip_files = src_darshan_dir + d.strftime("%Y/%m/%d/*.darshan.gz").replace('/0', '/')
        parsed_darshan_dir = dst_darshan_dir + d.strftime("%Y/%m/%d/").replace('/0', '/')
        if not os.path.exists(parsed_darshan_dir):
            try:
                os.makedirs(parsed_darshan_dir)
            except OSError as exc:  # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(parsed_darshan_dir):
                    pass
                else:
                    raise
                    
        file_list = glob.glob(darshan_files)
        zip_file_list = glob.glob(darshan_zip_files)
        d += delta
#        for my_file in file_list:
#            save_parser_output(my_file, parsed_darshan_dir, ext)

        results = []
        for my_file in file_list:
            result = processes.apply_async(save_parser_output, [my_file, parsed_darshan_dir, ext])
            results.append((result, my_file))
        for elem in results:
            if elem[0].get() < 0:
                err_count += 1
                print("Fail to procss file %s\n"%(elem[1]))
#                return -1
            else:
                suc_count += 1
        print("done, suc_count:%d, err_count:%d, total:%d\n"%(suc_count, err_count, suc_count+err_count))
    return 0
        
        
                
def is_output_saved(darshan_log, parsed_darshan_dir, flag='total'):
    print("processing file %s, pid:%d\n"%(darshan_log, os.getpid()))
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.'+flag
    return (os.path.exists(filename) and os.path.getsize(filename) > 0)

def save_parser_output(darshan_log, parsed_darshan_dir, flag='total', version = 3):
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.'+flag
    ret = 0
    if not is_output_saved(darshan_log, parsed_darshan_dir, flag):
        with open(filename, 'wb') as target:
            ret = subprocess.call(['darshan-parser','--'+flag,darshan_log],stdout=target)
    return ret

cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
cmd_parser.add_argument("src_dir", help = "top directory of Darshan")
cmd_parser.add_argument("dst_dir", help = "top directory of parsed Darshan")
cmd_parser.add_argument("--thread_count", default=1, type=int, help = "number of parser threads")
cmd_parser.add_argument("--all", help = "whether to parse darshan.all besides darshan.total", action = 'store_true')
args = cmd_parser.parse_args()

start_date = str(args.start_date)
end_date = str(args.end_date)

start_date += " 00:00:00"
end_date += " 23:59:59"
src_darshan_dir = args.src_dir
dst_darshan_dir = args.dst_dir
parse_darshan = args.all

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

parse_darshan_dir(src_darshan_dir, dst_darshan_dir,\
                      int_start_date,\
                      int_end_date,\
                      ext = 'total',\
                      version=3)

if parse_darshan:
    parse_darshan_dir(src_darshan_dir, dst_darshan_dir,\
                      int_start_date,\
                      int_end_date,\
                      ext = 'all',\
                      version=3)

processes.close()
processes.join()

