# -*- coding: utf-8 -*-
"""
Created on Sat Jun 19 20:16:51 2021

@author: tzw00
"""
import pickle
import re
import argparse
import datetime
from datetime import datetime,timedelta
import time
import pandas as pd
import glob
import os

def join_pd(job_tot_df, job_day_df_lst):
    for job_day_df in job_day_df_lst:
#        job_day_df.info()
        for (row_label, row_series) in job_day_df.iterrows():
            for column,value in row_series.items():
                job_tot_df.loc[row_label, column] = value

def extract_fields_for_periods(
                src_darshan_dir, \
                    dst_darshan_dir,\
                        start_time,\
                            end_time,\
                                field_lst):
   
    fmt_darshan_files = src_darshan_dir + "darshan_state*"
    print("fmt_darshan_files is %s\n"%fmt_darshan_files)
    file_list = glob.glob(fmt_darshan_files)
    qualified_files = []
    
    job_tot_df = pd.DataFrame()
    
    FNAME_PATTERN = 'darshan_state_([0-9]+)_([0-9]+)'
    
    fname_pat = re.compile(FNAME_PATTERN)
    
    for cur_file in file_list:
        fname_match = fname_pat.match(os.path.basename(cur_file))
        
        if not fname_match:
            continue
        cur_strt_time = int(fname_match.group(1))
        cur_end_time = int (fname_match.group(2))
       
#        print("cur_strt_time:%d, cur_end_time is %d, start_time:%d, end_time:%d\n"%(cur_strt_time, cur_end_time, start_time, end_time))
        if not (cur_end_time < start_time or cur_strt_time > end_time):
            qualified_files.append(cur_file)
    
    list.sort(field_lst)
    for cur_file in qualified_files:
        print("cur_file is %s\n"%cur_file)
        with open(cur_file, 'rb') as infile:
            df = pickle.load(infile)
#            for col in df.columns:
#                print("%s\n"%col)
            if job_tot_df.empty:
                job_tot_df = job_tot_df.append(df, ignore_index = True)
            else:
                job_tot_df = job_tot_df.append(df)


    result_df = job_tot_df[field_lst]

    field_str = ""
    count = 0
    for field in field_lst:
        if count <= 5:
            field_str += "_" + field
        count += 1
    dump_fname = dst_darshan_dir  + "/darshan_%d_%d"%(start_time, end_time) + field_str 
    
    try:
        tot_fd = open(dump_fname, 'wb')
    except IOError as e:
        print("Fail to open file %s:%s"%(dump_fname, e))
        return -1
    pickle.dump(result_df, tot_fd, -1)

    
cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
cmd_parser.add_argument("src_dir", help = "src directory of darshan dataframe files")
cmd_parser.add_argument("dst_dir", help = "target directory of new dataframe files with extracted files ")
cmd_parser.add_argument("fields", help = "comma separated darshan counters to be extracted into new darshan files")

args = cmd_parser.parse_args()

start_date = str(args.start_date)
end_date = str(args.end_date)
    
start_date += " 00:00:00"
end_date += " 23:59:59"
src_dir = args.src_dir
dst_dir = args.dst_dir
fields = args.fields

if src_dir[len(src_dir) - 2] != '/':
    src_dir = src_dir + '/'
if dst_dir[len(dst_dir) - 2] != '/':
    dst_dir = dst_dir + '/'
start_date_arr = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
int_start_date = int(time.mktime(start_date_arr))

end_date_arr = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
int_end_date = int(time.mktime(end_date_arr))

field_lst = fields.split(",")
extract_fields_for_periods(src_dir, \
                           dst_dir,\
                           int_start_date,\
                           int_end_date,\
                           field_lst)
