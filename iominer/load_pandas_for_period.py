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

# specify the output directory of the script gen_pandas_for_periods.py,
# as well as the period (start_time, end_time), this function loads
# all the persisted pandas dataframe between these periods into 
# memory, concatenate the into a new dataframe, and return it.
META_PATTERN = 'darshan_state_([0-9]+)_([0-9]+)'
def LoadDarshanForPeriod(format_darshan_root, start_time, end_time):
    job_tot_df = pd.DataFrame()
    meta_pat = re.compile(META_PATTERN)
    for root, dirs, files in os.walk(format_darshan_root):
        for file in files:
            print("file is %s\n"%file)
            match = meta_pat.match(file)
            if match:
                cur_strt_time = int(match.group(1))
                cur_end_time = int(match.group(2))
                cmp_start_time = int(start_time)
                cmp_end_time = int(end_time)
                
                if not (cmp_start_time > cur_end_time or cur_end_time < cmp_start_time):
                    try:
                        tot_fd = open(format_darshan_root+"/"+file, 'rb')
                    except IOError as e:
                        print("Fail to open file %s:%s"%(file, e))
                        return -1
                    
                    local_df = pickle.load(tot_fd)
                    join_pd(job_tot_df, local_df)
    return job_tot_df 





def join_pd(job_tot_df, job_day_df):
    for (row_label, row_series) in job_day_df.iterrows():
        print("row label is %s\n"%row_label)
        for column,value in row_series.items():
            job_tot_df.loc[row_label, column] = value
            print("column:%s, value:%s\n"%(column, value))

#cmd_parser = argparse.ArgumentParser()
#cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
#cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
#cmd_parser.add_argument("format_dir", help = "directory containing the formatted  dataframe for specified date")
#args = cmd_parser.parse_args()
#
#start_date = str(args.start_date)
#end_date = str(args.end_date)
#
#start_date += " 00:00:00"
#end_date += " 23:59:59"
#src_darshan_dir = args.format_dir
#
#if src_darshan_dir[len(src_darshan_dir) - 2] != '/':
#    src_darshan_dir = src_darshan_dir + '/'
#start_date_arr = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
#int_start_date = int(time.mktime(start_date_arr))
#
#end_date_arr = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
#int_end_date = int(time.mktime(end_date_arr))

#tot_df = LoadDarshanForPeriod(src_darshan_dir, 
#                            int_start_date, 
#                            int_end_date)
