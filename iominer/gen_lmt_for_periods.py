# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 21:54:30 2021

@author: tzw00
"""

import sys

import pandas as pd
import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams.update({'font.size': 14})
import time
import pandas
import pickle

import os
import multiprocessing
from multiprocessing import Manager
import sqlite3
import re
from datetime import datetime
import subprocess
import argparse



cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("start_date", help = "start date in the format of y-m-d")
cmd_parser.add_argument("end_date", help = "end date in the format of y-m-d")
cmd_parser.add_argument("tokio", help = "directory of pytokio")
cmd_parser.add_argument("save_lmt_path", help = "directory of output")
args = cmd_parser.parse_args()

start_date = str(args.start_date)
end_date = str(args.end_date)

start_date += " 00:00:00"
end_date += " 23:59:59"
tokio_dir = args.tokio
save_lmt_path = args.save_lmt_path

start_date_arr = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")

end_date_arr = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

sys.path.append(tokio_dir)
import tokio
import tokio.tools
import tokio.config



mdsCPU = tokio.tools.hdf5.get_dataframe_from_time_range(
                                       fsname="scratch1",
                                       dataset_name='MDSCPUGroup/MDSCPUDataSet',
                                       datetime_start=start_date_arr,
                                       datetime_end=end_date_arr)

ossCPU = tokio.tools.hdf5.get_dataframe_from_time_range(
                                      fsname="scratch1",
                                      dataset_name='OSSCPUGroup/OSSCPUDataSet',
                                      datetime_start=start_date_arr,
                                      datetime_end=end_date_arr)

ostRead = tokio.tools.hdf5.get_dataframe_from_time_range(
                                       fsname="scratch1",
                                       dataset_name='OSTReadGroup/OSTBulkReadDataSet',
                                       datetime_start=start_date_arr,
                                       datetime_end=end_date_arr)

ostWrite = tokio.tools.hdf5.get_dataframe_from_time_range(
                                fsname="scratch1",
                                dataset_name='OSTWriteGroup/OSTBulkWriteDataSet',
                                datetime_start=start_date_arr,
                                datetime_end=end_date_arr)

int_start_date = int(datetime.timestamp(start_date_arr))
int_end_date = int(datetime.timestamp(end_date_arr))


dict_lmt = {}
dict_lmt["mdsCPU"] = mdsCPU
dict_lmt["ossCPU"] = ossCPU
dict_lmt["ostRead"] = ostRead
dict_lmt["ostWrite"] = ostWrite


filename_w_date = "%s/lmt_state_%d_%d"%(save_lmt_path, int_start_date, int_end_date) 
try:
    tot_fd = open(filename_w_date, 'wb')
except IOError as e:
    print("Fail to open file %s:%s"%(filename_w_date, e))
pickle.dump(dict_lmt, tot_fd, -1)
