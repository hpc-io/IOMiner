import datetime
import time
import os
import re
import timeit
from datetime import datetime,timedelta
#from pyspark import SparkContext
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
from construct_low_bw import *
from analyze_low_bw import *
from miner_stat import *
#from slurm_stat import *
from miner_plot import *
from analyze_bug import *

#sc = SparkContext.getOrCreate()
plot_dir="/global/cscratch1/sd/tengwang/latestminer/plots/"
cluster_name = "cori"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/latestminer/miner_para.conf'))
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


def extract_file_counters(file_list):
    tot_counter = 0
    cust_counter = 0
    tuple_list = []
    for record in file_list:
        tot_fd = open(record[0], "rb")
        stat_table = pickle.load(tot_fd)
        per_file_fd = open(record[1], "rb")
#		print "openning:%s\n"%record[1]
#		print "openning :%s\n"%record[1]
        for row in stat_table:
            offset = long(row["EXTERNAL_FILE_OFFSET"])
            length = long(row["EXTERNAL_FILE_LENGTH"])
            tot_proc_cnt = long(row["nprocs"])
            per_file_fd.seek(offset)
            serialize_obj = per_file_fd.read(length)
            tmp_dict = pickle.loads(serialize_obj)
#            print "###job"
            cust_flag = 0
            stripe_flag = 0
            local_file_cnt = 0
            local_proc_cnt = 0
			for k in tmp_dict:
				tmp_tmp_dict = tmp_dict[k]
				if tmp_tmp_dict.get("POSIX_BYTES_READ", -1) != -1 or tmp_tmp_dict.get("POSIX_BYTES_WRITTEN", -1) != -1:
					local_proc_cnt += long(tmp_tmp_dict["nprocs"])
					local_file_cnt += 1
			tuple_list.append((row["FileName"], k, tot_proc_cnt, local_proc_cnt, local_file_cnt))
#			print "app:%s, local_proc_cnt:%ld, local_file_cnt:%d, tot_proc_cnt:%d\n"%(row["FileName"], local_proc_cnt, local_file_cnt, tot_proc_cnt)
#	per_file_fd.close()
#	tot_fd.close()
	return tuple_list

def parse_spark_files(file_list):
    tot_counter = 0
    cust_counter = 0
#    print "file_list len is %d\n"%len(file_list) 
    for record in file_list:
#        print "file1 is %s \n"%(record[0])
        tot_fd = open(record[0], "rb")
        stat_table = pickle.load(tot_fd)
        per_file_fd = open(record[1], "rb")
        for row in stat_table:
            offset = long(row["EXTERNAL_FILE_OFFSET"])
            length = long(row["EXTERNAL_FILE_LENGTH"])
            per_file_fd.seek(offset)
            serialize_obj = per_file_fd.read(length)
            tmp_dict = pickle.loads(serialize_obj)

#            print "###job"
            cust_flag = 0
            stripe_flag = 0
            for k in tmp_dict:
#                print "k is %s\n"%k
                tmp_tmp_dict = tmp_dict[k]
                if tmp_tmp_dict.get("LUSTRE_STRIPE_SIZE", -1) != -1:
                    stripe_flag = 1
                    stripe_size = long(tmp_tmp_dict["LUSTRE_STRIPE_SIZE"])
                    stripe_cnt = long(tmp_tmp_dict["LUSTRE_STRIPE_WIDTH"])
                    if stripe_size != 1048576 or stripe_cnt != 1:
#                        print "job:%s, file %s:stripe size:%d, width:%d\n"%(record[0], k, stripe_size, stripe_cnt)
                        cust_flag = 1
            if cust_flag == 1:
                cust_counter += 1
            if stripe_flag == 1:
                tot_counter += 1

        per_file_fd.close()
        tot_fd.close()
        print "cust_counter:%d, tot_counter:%d\n"%(cust_counter, tot_counter)
    return (cust_counter, tot_counter)

def form_task_list(out_dir, output_fname_prefix, task_cnt):
    ost_file_name = out_dir+output_fname_prefix + "_ost.log"
    ost_fd = open(ost_file_name, 'rb')
    ost_dict = pickle.load(ost_fd)

    task_list = []
    print "ost_dict len is %d\n"%len(ost_dict)
    if task_cnt >= len(ost_dict):
        # split each ost list into task_cnt/len(ost_dict)
        for key,value in ost_dict.iteritems():
            fname_list = value
            cursor = 0
            tmp_list = []
            print "###key:%d\n"%key
            for record in fname_list:
                print "fname1:%s, fname2:%s\n"%(record[0], record[1])
                tmp_list.append(record)
                cursor += 1
                if cursor == task_cnt/len(ost_dict):
                    print "switched, list len is %d\n"%(len(tmp_list))
                    task_list.append(tmp_list)
                    tmp_list = []
                    cursor = 0
            if len(tmp_list) != 0:
                task_list.append(tmp_list)

    else:
        # group ost list into len(ost_dict)/task_cnt groups
        cursor = 0
        tmp_list = []
        for key,value in ost_dict.iteritems():
            fname_list = value
#            print "###key is %d\n"%key
#            for fname in fname_list:
#                print "fname is %s\n"%fname[0]
            tmp_list = tmp_list + fname_list
            cursor = cursor + 1
            if cursor == len(ost_dict)/task_cnt:
#                print "cursor is %d\n"%cursor
                task_list.append(tmp_list)
                tmp_list = []
                cursor = 0
        if len(tmp_list) != 0:
            task_list.append(tmp_list)
#        print "task_list length is %d\n"%len(task_list)
#        cursor = 0
#        for cur_list in task_list:
#            print "cursor:%d\n"%cursor
#            for record in cur_list:
#                print "fname is %s\n"%record[0]
#            cursor += 1
    ost_fd.close()
    return task_list

print "/global/cscratch1/sd/darshanlogs/2017/9/24/lfu_vasp_std_id6990941_9-24-21080-1482367210173183023_1.darshan".rpartition('/')[2]
#task_list = form_task_list(miner_param["spark"]["out_dir"], miner_param["spark"]["out_prefix"], 4)
#parse_spark_files(task_list[2])
task_list = form_task_list(miner_param["spark"]["out_dir"], miner_param["spark"]["out_prefix"], int(os.environ['task_count']))

#start = time.time()
#tot_cust_cnt = 0
#tot_cnt = 0
#for task in task_list:
#	(tmp_cust_cnt, tmp_tot_cnt) = parse_spark_files(task)
#	tot_cust_cnt += tmp_cust_cnt
#	tot_cnt += tmp_tot_cnt

#log_rdd = sc.parallelize(task_list, numSlices=int(os.environ['task_count'])).map(lambda x:(parse_spark_files(x)))
#log_rdd = sc.parallelize(task_list, numSlices=int(os.environ['task_count'])).map(lambda x:(parse_spark_files(x)))
#for record in log_rdd.collect():
#        print "cust_counter:%d, counter:%d\n"%(record[0],record[1])
#end = time.time()
#print "ratio is %lf\n"%(float(tot_cust_cnt)/tot_cnt)
#print "time is %lf\n"%(end-start)
plot_path = miner_param["dataset_path"]+"spark_file_info.pkl" 
tuple_list = []
for task in task_list:
	ret_tuple_list = extract_file_counters(task)
	tuple_list += ret_tuple_list

save_fd = open(plot_path, 'wb')
pickle.dump(tuple_list, save_fd, -1)
save_fd.close()

per_file_handle.close()
