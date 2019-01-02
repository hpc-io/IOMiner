import datetime
import time
import sys
import os
import re
import timeit
from datetime import datetime,timedelta
import glob
import subprocess
import errno
import logging
import json
import pickle
import logging
import pickle
import string
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
sys.path.insert(0, '../data_prep/')
from construct_low_bw import *
from miner_stat import *

cluster_name = "cori"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner1217/miner_para.conf'))
darshan_root = miner_param[cluster_name]["darshan_root"]
parsed_darshan_root = miner_param[cluster_name]["parsed_darshan_root"]
cpy_darshan_root = miner_param[cluster_name]["cpy_darshan_root"]
format_darshan_root = miner_param[cluster_name]["format_darshan_root"]
per_file_log = miner_param[cluster_name]["per_file_log"]
bigtable_log = miner_param[cluster_name]["bigtable_log"]

per_file_handle = open(per_file_log, 'rb+'); 

timeStartArray = time.strptime(miner_param["start_ts"], "%Y-%m-%d %H:%M:%S")
job_start_ts = int(time.mktime(timeStartArray))

timeEndArray = time.strptime(miner_param["end_ts"], "%Y-%m-%d %H:%M:%S")
#job_start_ts = int(time.mktime(timeEndArray))
job_end_ts = int(time.mktime(timeEndArray))

start_ts = job_start_ts
end_ts = job_end_ts

def get_file_record_dict(record, per_file_handle):
    print "appname:%s\n"%record["AppName"]
    ost_cnt = 0
    offset = long(record["EXTERNAL_FILE_OFFSET"])
    length = long(record["EXTERNAL_FILE_LENGTH"])

    per_file_handle.seek(offset)
    serialized_obj = per_file_handle.read(length)

    return serialized_obj

def format_spark_store(stat_table, per_file_handle, output_dir, output_fname_prefix, per_file_size, ost_cnt, ost_offset):
    tmp_list = []
    ost_dict = {} 

    file_pos = 0
    job_size = 0

    file_idx = 0
    tot_file_name = output_dir+output_fname_prefix + "_total%d.log"%file_idx
    per_file_name = output_dir+output_fname_prefix + "_perfile%d.log"%file_idx
    ost_file_name = output_dir+output_fname_prefix + "_ost.log"
    print "tot_fname:%s, per_fname:%s, ost_fname:%s\n"%(tot_file_name, per_file_name, ost_file_name)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # set stripe
    stripe_dir_cmd = "lfs setstripe -c 1 -i %d %s"%(ost_offset, tot_file_name)
    os.system(stripe_dir_cmd)
    tot_fd = open(tot_file_name, 'wb')
    stripe_dir_cmd = "lfs setstripe -c 1 -i %d %s"%(ost_offset, per_file_name)
    os.system(stripe_dir_cmd)
    per_file_fd = open(per_file_name, 'wb')
    stripe_dir_cmd = "lfs setstripe -c 1 -i %d %s"%(ost_offset, ost_file_name)
    os.system(stripe_dir_cmd)
    ost_fd = open(ost_file_name, 'wb')
    for record in stat_table:
        serialize_obj = get_file_record_dict(record, per_file_handle)
#        cur_dict = pickle.loads(serialize_obj)
#
#        for k in cur_dict:
#            if cur_dict[k].get("POSIX_BYTES_READ", -1) != -1:
#                print "k is %s, data read:%ld\n"%(k, long(cur_dict[k].get("POSIX_BYTES_READ")))
                
        cpy_record = record.copy()
        #set external offset
        if file_pos + len(serialize_obj) >= per_file_size:
            pickle.dump(tmp_list, tot_fd, -1)
            tot_fd.close()
            per_file_fd.close()
            tmp_list = []

            if ost_dict.get(file_idx%ost_cnt, -1) == -1:
                ost_dict[file_idx%ost_cnt] = []
            ost_dict[file_idx%ost_cnt].append((tot_file_name, per_file_name))
            print "appending %d, file:%s\n"%(file_idx%ost_cnt, tot_file_name)
            file_idx += 1

            tot_file_name = output_dir + output_fname_prefix + "_total%d.log"%file_idx
            per_file_name = output_dir + output_fname_prefix + "_perfile%d.log"%file_idx
            # set stripe

            stripe_dir_cmd = "lfs setstripe -c 1 -i %d %s"%(ost_offset + file_idx%ost_cnt, tot_file_name)
            os.system(stripe_dir_cmd)
            tot_fd = open(tot_file_name, 'wb')


            stripe_dir_cmd = "lfs setstripe -c 1 -i %d %s"%(ost_offset + file_idx%ost_cnt, per_file_name)
            os.system(stripe_dir_cmd)
            per_file_fd = open(per_file_name, 'wb')
            file_pos = 0
            # dump to disk
            # dump stat to disk as well
            # note the current file index number
            # add to hash table
        per_file_fd.seek(file_pos)
        per_file_fd.write(serialize_obj)
        cpy_record["EXTERNAL_FILE_OFFSET"] = str(file_pos)
        cpy_record["EXTERNAL_FILE_LENGTH"] = str(len(serialize_obj))
        file_pos += len(serialize_obj)
        tmp_list.append(cpy_record)

    print "appending %d, file:%s\n"%(file_idx%ost_cnt, tot_file_name)
    if ost_dict.get(file_idx%ost_cnt, -1) == -1:
        ost_dict[file_idx%ost_cnt] = []
    ost_dict[file_idx%ost_cnt].append((tot_file_name, per_file_name))
    pickle.dump(ost_dict, ost_fd, -1)
    ost_fd.close()
    pickle.dump(tmp_list, tot_fd, -1)
    tot_fd.close()
    per_file_fd.close()

tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()

# split bigtable_log_formated_tot_stat_adv.pkl into multiple logs (16777216 is the size of each log) placed under the directory miner_param["spark"]["out_dir"], these logs are placed on 64 OSTs.
format_spark_store(stat_table, per_file_handle, miner_param["spark"]["out_dir"], miner_param["spark"]["out_prefix"], 16777216, 64, 0)

per_file_handle.close()
