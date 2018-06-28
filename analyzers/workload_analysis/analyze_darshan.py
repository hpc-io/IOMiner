import datetime
import time
import os
import re
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
from construct_low_bw import *
from analyze_low_bw import *
from miner_stat import *
#from slurm_stat import *
from miner_plot import *
from analyze_bug import *

plot_dir="/global/cscratch1/sd/tengwang/miner0612/plots/"
cluster_name = "cori"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner0612/miner_para.conf'))
darshan_root = miner_param[cluster_name]["darshan_root"]
parsed_darshan_root = miner_param[cluster_name]["parsed_darshan_root"]
cpy_darshan_root = miner_param[cluster_name]["cpy_darshan_root"]
cpy_darshan_root = "/global/cscratch1/sd/darshanlogs/" 
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

def is_output_copied(darshan_log, cpy_darshan_dir):
    filename = cpy_darshan_dir+darshan_log.rpartition('/')[2]
    return (os.path.exists(filename) and os.path.getsize(filename) > 0)

def save_cpy_output(darshan_log, cpy_darshan_dir):
    filename = cpy_darshan_dir+darshan_log.rpartition('/')[2]
    cmd = "cp %s %s"%(darshan_log, filename)
    subprocess.call(cmd, shell=True)
#    return (filename, 1)

def is_output_saved(darshan_log, parsed_darshan_dir, flag='total'):
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.'+flag
    log_str="######in output_saved, file name is %s\n"%filename
    logging.info(log_str)
    return (os.path.exists(filename) and os.path.getsize(filename) > 0)

def save_parser_output(darshan_log, parsed_darshan_dir, flag='total', version = 3):
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.'+flag
    if not is_output_saved(darshan_log, parsed_darshan_dir, flag):
        with file(filename, 'wb') as target:
            subprocess.call(['darshan-parser','--'+flag,darshan_log],stdout=target)

def construct_bigtable(format_darshan_root, start_time, end_time, formated_filename):
    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    d = datetime(start_date.year,start_date.month,start_date.day)
    delta = timedelta(days=1)

    stat_table = []

    while d <= end_date:
        formated_darshan_path = format_darshan_root + d.strftime("%Y/%-m/%-d/") + formated_filename
        print "formated_darshan_path:%s\n"%formated_darshan_path
        if os.path.exists(formated_darshan_path):
            save_fd = open(formated_darshan_path, 'rb')
            tmp_stat_table = pickle.load(save_fd)
            if tmp_stat_table:
                print "tmp_stat_table length:%d\n"%len(tmp_stat_table)
                for tmp_stat in tmp_stat_table:
#                    print "tmp_stat is %s\n"%tmp_stat["PATH"]
                    stat_table.append(tmp_stat)
            save_fd.close()
        d += delta
    print "stat_table %s length is %d\n"%(formated_filename, len(stat_table))
    tmp_str = bigtable_log+"_"+formated_filename+".log"
    save_fd = open(tmp_str, 'wb')
    pickle.dump(stat_table, save_fd, -1)
    save_fd.close()
    return stat_table

def get_parsed_list(cpy_darshan_root, parsed_darshan_root, start_time, end_time, ext = 'total', decompressed_darshan_dir = "", version=3):
    print "get parsing\n"
    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    d = datetime(start_date.year,start_date.month,start_date.day)
    delta = timedelta(days=1)
    file_list = []
    zip_file_list = []
    path_list = []
    parsed_rdd_list = []
    while d <= end_date:
        darshan_files = cpy_darshan_root + d.strftime("%Y/%-m/%-d/*.darshan")
        print "cpy darshan_files is %s\n"%darshan_files
        darshan_zip_files = cpy_darshan_root + d.strftime("%Y/%-m/%-d/*.darshan.gz")
        path_list.append(parsed_darshan_root + d.strftime("%Y/%-m/%-d/"))
        parsed_darshan_dir = parsed_darshan_root + d.strftime("%Y/%-m/%-d/")
        if not os.path.exists(parsed_darshan_dir):
            try:
                os.makedirs(parsed_darshan_dir)
            except OSError as exc:  # Python >2.5
                log_str = "fail to make directory\n"
                if exc.errno == errno.EEXIST and os.path.isdir(parsed_darshan_dir):
                    pass
                else:
                    raise
        file_list = glob.glob(darshan_files)
        zip_file_list = glob.glob(darshan_zip_files)
        d += delta

        for my_file in file_list:
            tmp_record = is_output_saved(my_file, parsed_darshan_dir, ext)
            if tmp_record != True:
                print "parsing %s\n"%my_file
                save_parser_output(my_file, parsed_darshan_dir, ext)
#        rdd = sc.parallelize(file_list).map(lambda x:(x,is_output_saved(x, parsed_darshan_dir, ext)))

#        rdd2 = rdd.filter(lambda x:not x[1]).map(lambda x:save_parser_output(x[0], parsed_darshan_dir, ext, 3)).collect()
#        parsed_rdd_list.append(rdd)


def format_parsed_files(parsed_darshan_root, format_darshan_root, start_time, end_time):
    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    d = datetime(start_date.year,start_date.month,start_date.day)
    delta = timedelta(days=1)
    file_list = []
    zip_file_list = []
    path_list = []
    parsed_rdd_list = []
    while d <= end_date:
        parsed_darshan_files = parsed_darshan_root + d.strftime("%Y/%-m/%-d/*.darshan.total")
        print "parsed file is %s\n"%parsed_darshan_files
        path_list.append(format_darshan_root + d.strftime("%Y/%-m/%-d/"))
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
        index = 0
        stat_table = []
        small_bw_stat_table = []
        burst_small_bw_stat_table = []
        tot_stat_table = []
        my_tot_stat_table = []

        min_procs = int(miner_param["format"]["min_procs"])
        max_rd_bw = float(miner_param["format"]["max_read_bw"]) 
        max_wr_bw = float(miner_param["format"]["max_write_bw"]) 
        min_rd_size = float(miner_param["format"]["min_read_size"]) 
        min_wr_size = float(miner_param["format"]["min_write_size"]) 

        sum_record1 = 0
        sum_record2 = 0
        sum_record3 = 0
        for record in file_list:
            print "record is %s\n"%record
            sum_record1 += 1
            tmp_record = replace_ext(record, '.all')
            path_tuple = getOnePath(tmp_record)
            if not path_tuple:
                print "####no path tuple record is %s\n"%record
                continue
            out_dict = getStatTable(record, tmp_record, path_tuple, per_file_handle)
            sum_record2 += 1
            if not out_dict:
                continue
            my_tot_stat_table.append(out_dict)
#            print "version:%s\n"%out_dict["darshan_version"]
#            print out_dict["darshan_version"] == "3.00"
            if out_dict.get("darshan_version", -1) == -1:
                continue
            if out_dict.get("total_POSIX_BYTES_READ", -1) == -1:
                continue;
            if out_dict.get("total_POSIX_BYTES_WRITTEN", -1) == -1:
                continue;
            if out_dict.get("total_POSIX_F_READ_TIME", -1) == -1:
                continue;
            if out_dict.get("total_POSIX_F_WRITE_TIME", -1) == -1:
                continue;
               
            correct_darshan = True
            if out_dict["darshan_version"] != "3.10":
                correct_darshan = False
            correct_process = True
            if int(out_dict["nprocs"]) < min_procs:
                correct_process = False
            read_size = float(out_dict["total_POSIX_BYTES_READ"])
            small_read = False
            small_write = False
            if read_size < min_rd_size:
                small_read = True

            write_size = float(out_dict["total_POSIX_BYTES_WRITTEN"])
            if write_size < min_wr_size:
                small_write = True

            correct_data_size = True
            if small_read and small_write:
                correct_data_size = False


            read_time = float(out_dict["total_POSIX_F_READ_TIME"])
            write_time = float(out_dict["total_POSIX_F_WRITE_TIME"])
            if read_time != 0:
                read_bw = read_size/read_time
            else:
                read_bw = -1

            if write_time != 0:
                write_bw = write_size/write_time
            else:
                write_bw = -1
#            print "version:%s, read_size:%lf, write_size:%lf\n"%(out_dict["darshan_version"], read_size, write_size)
#            print "read_bw:%lf, write_bw:%lf, time%lf\n"%(read_bw, write_bw, read_time)

            if correct_darshan and correct_process and correct_data_size:
                stat_table.append(out_dict)
            tot_stat_table.append(out_dict)
            sum_record3 += 1

            slow_read = False
            slow_write = False
            if read_bw > 0 and read_bw < max_rd_bw and correct_darshan and correct_process and correct_data_size:
                print "read_bw:%lf, max_bw:%lf\n"%(read_bw, max_rd_bw)
                slow_read = True
            if write_bw > 0 and write_bw < max_wr_bw:
                slow_write = True
            if slow_read and slow_write:
                small_bw_stat_table.append(out_dict)

            read_start = float(out_dict["total_POSIX_F_READ_START_TIMESTAMP"]) 
            read_end = float(out_dict["total_POSIX_F_READ_END_TIMESTAMP"]) 
            write_start = float(out_dict["total_POSIX_F_WRITE_START_TIMESTAMP"]) 
            write_end = float(out_dict["total_POSIX_F_WRITE_END_TIMESTAMP"]) 
            read_time = read_end - read_start
            write_time = write_end - write_start

            if read_time != 0:
                read_bw = read_size/read_time
            else:
                read_bw = -1

            if write_time != 0:
                write_bw = write_size/write_time
            else:
                write_bw = -1

            slow_read = False
            slow_write = False
            if read_bw > 0 and read_bw < max_rd_bw:
                print "read_bw:%lf, max_bw:%lf\n"%(read_bw, max_rd_bw)
                slow_read = True
            if write_bw > 0 and write_bw < max_wr_bw:
                slow_write = True
            if slow_read and slow_write and correct_darshan and correct_process and correct_data_size:
                burst_small_bw_stat_table.append(out_dict)
           
        print "sum_record1:%d, sum_record2:%d, sum_record3:%d\n"%(sum_record1, sum_record2, sum_record3)
        save_file_name = format_darshan_dir + "formated_stat.pkl"
        save_fd = open(save_file_name, 'wb')
        pickle.dump(stat_table, save_fd, -1)
        save_fd.close()

        save_file_name = format_darshan_dir + "formated_lowbw_stat.pkl"
        save_fd = open(save_file_name, 'wb')
        pickle.dump(small_bw_stat_table, save_fd, -1)
        save_fd.close()


        save_file_name = format_darshan_dir + "formated_my_tot_stat.pkl"
        save_fd = open(save_file_name, 'wb')
        pickle.dump(my_tot_stat_table, save_fd, -1)
        save_fd.close()

        save_file_name = format_darshan_dir + "formated_tot_stat.pkl"
        save_fd = open(save_file_name, 'wb')
        pickle.dump(tot_stat_table, save_fd, -1)
        save_fd.close()

        save_file_name = format_darshan_dir + "formated_burst_lowbw_stat.pkl"
        save_fd = open(save_file_name, 'wb')
        pickle.dump(burst_small_bw_stat_table, save_fd, -1)
        save_fd.close()

def cpy_darshan_files(darshan_root, cpy_darshan_root, start_time, end_time):
    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    d = datetime(start_date.year,start_date.month,start_date.day)
    delta = timedelta(days=1)
    file_list = []
    zip_file_list = []
    path_list = []
    parsed_rdd_list = []
    while d <= end_date:
        darshan_files = darshan_root + d.strftime("%Y/%-m/%-d/*.darshan")
        darshan_zip_files = darshan_root + d.strftime("%Y/%-m/%-d/*.darshan.gz")
        path_list.append(cpy_darshan_root + d.strftime("%Y/%-m/%-d/"))
        cpy_darshan_dir = cpy_darshan_root + d.strftime("%Y/%-m/%-d/")
        if not os.path.exists(cpy_darshan_dir):
            try:
                os.makedirs(cpy_darshan_dir)
            except OSError as exc:  # Python >2.5
                log_str = "fail to make directory\n"
                logging.info(log_str)
                if exc.errno == errno.EEXIST and os.path.isdir(parsed_darshan_dir):
                    pass
                else:
                    raise
        file_list = glob.glob(darshan_files)
        zip_file_list = glob.glob(darshan_zip_files)
        d += delta

#        for my_file in file_list:
#            print "test file:%s, cpy dir:%s\n"%(cpy_darshan_dir+my_file.rpartition('/')[2], cpy_darshan_dir)
#        rdd = sc.parallelize(file_list).map(lambda x:(x,is_output_copied(x, cpy_darshan_dir)))
        for my_file in file_list:
            tmp_record = is_output_copied(my_file, cpy_darshan_dir)
            if tmp_record != True:
                print "copying %s\n"%my_file
                save_cpy_output(my_file, cpy_darshan_dir)
#        rdd2 = rdd.filter(lambda x:not x[1]).map(lambda x:save_cpy_output(x[0], cpy_darshan_dir))

print "/global/cscratch1/sd/darshanlogs/2017/9/24/lfu_vasp_std_id6990941_9-24-21080-1482367210173183023_1.darshan".rpartition('/')[2]
#cpy_darshan_files(darshan_root, cpy_darshan_root, start_ts, \
#        end_ts)
#get_parsed_list(cpy_darshan_root, parsed_darshan_root, start_ts, end_ts, ext = 'all', decompressed_darshan_dir = "", version=3)
#get_parsed_list(cpy_darshan_root, parsed_darshan_root, start_ts, end_ts, ext = 'total', decompressed_darshan_dir = "", version=3)
format_parsed_files(parsed_darshan_root, format_darshan_root, start_ts, end_ts)

io_types = ["WRITE", "READ"]
fs = "all"

stat_table = construct_bigtable(format_darshan_root, start_ts, end_ts, "formated_tot_stat.pkl")

#start here
tmp_str = bigtable_log+"_"+"formated_tot_stat.pkl"+".log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()
print "number of records is %d\n"%len(stat_table)
#
new_stat_table = covert_app_name(stat_table)
tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'wb')
pickle.dump(new_stat_table, save_fd, -1)
save_fd.close()
#
#
tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()

#format_slurm_files(miner_param["slurm_job_dir"], miner_param["start_ts"], miner_param["end_ts"])
#slurm_table = retrieve_slurm_files(miner_param["slurm_job_dir"])
#slurm_util_percent = get_utilization_dist(slurm_table)

sum_job = 0
sum_file = 0
print "stat_table size is %d"%len(stat_table)
for record in stat_table:
    tmp_cnt = print_record_info(record, per_file_handle)
    if tmp_cnt != 0:
        sum_job = sum_job + 1;
        sum_file = sum_file + tmp_cnt
print "sum file is %ld, sum job is %d\n"%(sum_file, sum_job)
#
tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'wb')
pickle.dump(stat_table, save_fd, -1)
save_fd.close()

##start here
#tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
#save_fd = open(tmp_str, 'rb')
#stat_table = pickle.load(save_fd)
#save_fd.close()
#agg_tuple = tot_app_size_time(stat_table)
#print "read_size:%ld, write_size:%ld, time:%ld, len:%d\n"%(agg_tuple[0], agg_tuple[1], agg_tuple[2], len(stat_table))
#
#
#app_list = app_total_size_order(stat_table, "READ")
#plot_path = miner_param["dataset_path"]+"top_read_app.pkl" 
#save_fd = open(plot_path, 'wb')
#pickle.dump(app_list, save_fd, -1)
#save_fd.close()
#plotTopIOApp(app_list, "READ")
#
#cursor = 0
#print "######top readers based on aggregate read size:\n"
#for row in app_list:
#    print "app name:%s, aggregate read size:%ld, path:%s, nprocs:%ld, write size:%ld\n"%(row[0], row[1], row[2], row[3], row[4])
#    cursor = cursor + 1
#    if cursor == 10:
#        break
#
#print "######top writers based on aggregate write size:\n"
#cursor = 0
#app_list = app_total_size_order(stat_table, "WRITE")
#plot_path = miner_param["dataset_path"]+"top_write_app.pkl" 
#save_fd = open(plot_path, 'wb')
#pickle.dump(app_list, save_fd, -1)
#save_fd.close()
#plotTopIOApp(app_list, "WRITE")
#
#for row in app_list:
#    print "app name:%s, aggregate write size:%ld, path:%s, nprocs:%ld, read size:%ld\n"%(row[0], row[1], row[2], row[3], row[5])
#    cursor = cursor + 1
#    if cursor == 10:
#        break
#
#out_dict = {}
#abnormal_lst = {}
#io_type = "READ"
#(out_dict, abnormal_lst) = get_io_pat_hist(stat_table, 100)
#plot_path = miner_param["dataset_path"]+"seq_dist.pkl" 
#save_fd = open(plot_path, 'wb')
#pickle.dump(out_dict, save_fd, -1)
#save_fd.close()
#plotSeqDist(out_dict["seq"], "ALL", 10)
#
#
#ratio_list = []
#ratio_list = read_write_ratio(stat_table, 100)
#plot_path = miner_param["dataset_path"]+"read_write_ratio.pkl" 
#save_fd = open(plot_path, 'wb')
#pickle.dump(ratio_list, save_fd, -1)
#save_fd.close()
#plotReadWriteRatioDist(ratio_list, 10, 100)
#

#out_dict = {}
#out_dict = ioTypeBin(stat_table)
#plot_path = miner_param["dataset_path"]+"io_type.pkl" 
#save_fd = open(plot_path, 'wb')
#pickle.dump(out_dict, save_fd, -1)
#save_fd.close()
#plotIOTypeDist(out_dict)
#
#out_dict = {}
#out_dict = getProcFileRatio(stat_table, 5)
#plot_path = miner_param["dataset_path"]+"proc_file_ratio.pkl" 
#save_fd = open(plot_path, 'wb')
#pickle.dump(out_dict, save_fd, -1)
#save_fd.close()
#plotProcFileRatio(out_dict)

#save_fd = open("abnormal_read.log", 'wb')
#pickle.dump(abnormal_lst, save_fd, -1)
#save_fd.close()
    
#plotConsecDist(out_dict["consec"], io_type, 10)
#plotSmallDist(out_dict["small_io"], io_type, 10)


#
#print "after filtering there are %d jobs\n"%len(stat_table)
#
#io_type = "READ"
#out_dict = {}
#out_dict = get_fs_hist(stat_table, io_type, 1000, 1024*1048576)
#plotFSDist(out_dict)
#
#for key,value in out_dict.iteritems():
#	print "fs:key:%s, value:%d\n"%(key, value)
#
#io_type = "READ"
#out_lst = []
#out_lst = getDataSizeHist(stat_table, 100, io_type)
#plotDataSizeDist(out_lst, io_type, 10)
#
#io_type = "WRITE"
#out_lst = []
#out_lst = getDataSizeHist(stat_table, 100, io_type)
#plotDataSizeDist(out_lst, io_type, 10)
#
#
#out_lst = []
#out_lst = getProcCntHist(stat_table, 100)
#plotProcCntDist(out_lst, 10)
#io_type = "WRITE"
#out_list = []
#out_list = getBWHist(stat_table, 100, io_type)
##for i in out_list:
##    print "mybw:%lf\n"%i
#plotBWDist(out_list, io_type, 10)
#
#
#out_list = []
#io_type = "READ"
#out_list = getBWHist(stat_table, 100, io_type)
#plotBWDist(out_list, io_type, 10)
#
#
#
#out_dict = {}
#abnormal_lst = {}
#io_type = "WRITE"
#(out_dict, abnormal_lst) = get_fs_pattern_hist(stat_table, io_type, 100)
#save_fd = open("abnormal_write.log", 'wb')
#pickle.dump(abnormal_lst, save_fd, -1)
#save_fd.close()


#save_fd = open("abnormal_read.log", 'rb')
#abn_table = pickle.load(save_fd)
#save_fd.close()
#bug_dict = get_app_hist(abn_table)
#
#get_bug_hist("read", stat_table, bug_dict)
#
#save_fd = open("abnormal_write.log", 'rb')
#abn_table = pickle.load(save_fd)
#save_fd.close()
#bug_dict = get_app_hist(abn_table)
#get_bug_hist("write", stat_table, bug_dict)

#out_tuples = vectorize(stat_table, "READ", 4, 1073741824, 1000, long(10737418240), long(107374182400))
#tmp_str = bigtable_log+"_"+"formated_tot_stat_adv_third.pkl"+".log"
#save_fd = open(tmp_str, 'rb')
#stat_table = pickle.load(save_fd)
#save_fd.close()

#data_size = 1073741824
#proc_cnt = 1000
#stat_table = filterMeaningfulJob(stat_table, data_size, proc_cnt)
#
#job_name = "meaningful_%ld.log"%data_size
#meaningful_job_fd = open(job_name, 'wb')
#pickle.dump(stat_table, meaningful_job_fd, -1)
#meaningful_job_fd.close()
#
#data_size = 10737418240
#proc_cnt = 1000
#stat_table = filterMeaningfulJob(stat_table, data_size, proc_cnt)
#
#job_name = "meaningful_%ld.log"%data_size
#meaningful_job_fd = open(job_name, 'wb')
#pickle.dump(stat_table, meaningful_job_fd, -1)
#meaningful_job_fd.close()

data_size = 1073741824
proc_cnt = 1000
job_name = "meaningful_%ld.log"%data_size
meaningful_job_fd = open(job_name, 'rb')
stat_table = pickle.load(meaningful_job_fd)
meaningful_job_fd.close()
tot_sum = 0
slurm_sum = 0
for row in stat_table:
    if row.get("nnodes", -1) == -1:
        slurm_sum += 1
    tot_sum += 1
#    print "file:%s, node count:%s, proc count:%s\n"%(row["FileName"], row["nnodes"], row["nprocs"])
print "slurm_sum:%d, tot_sum:%d\n"%(slurm_sum, tot_sum)

#out_lst = largeFileIOHist(stat_table, 100, "READ", 1073741824, 1000, 0, 1073741824)
#plotLargeFileIODist(out_lst, "READ", 10)
#
#out_lst = largeFileIOHist(stat_table, 100, "WRITE", 1073741824, 1000, 0, 1073741824)
#plotLargeFileIODist(out_lst, "WRITE", 10)
#

is_per_factor = 0
if is_per_factor == 0:
    multiply = 10
else:
    multiply = 16
per_proc_size = 10485760
print "###bad read performance instance:\n"
out_lst = vectorize_sweepline(stat_table, "READ", 4, 10737418240, 1073741824000, 1000, 0, long(1073741824), per_proc_size, is_per_factor)
print "out_lst length is %d\n"%len(out_lst)

tmp_dict = {}
max_val = 0
max_app = ""
for i in range(0, len(out_lst)):
    if tmp_dict.get(out_lst[i][2][1], -1) == -1:
        tmp_dict[out_lst[i][2][1]] = 1
    else:
        tmp_dict[out_lst[i][2][1]] += 1
        if tmp_dict[out_lst[i][2][1]] > max_val:
            max_val = tmp_dict[out_lst[i][2][1]]
            max_app = out_lst[i][0]
            max_idx = out_lst[i][2][1]
print "read app is %s, count:%d, max_idx is %d\n"%(max_app, max_val, max_idx)



output_arr = extract_array(out_lst)

plot_path = miner_param["dataset_path"]+"sweep_read.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(output_arr, save_fd, -1)
#plotDistri(output_arr,  "READ", "10_1000GB", [0]*multiply, 0, is_per_factor)
##
#
#
##out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, int(stat_row["nnodes"]), col_enable*100, float(cur_bw), small_io_percent, float(data_size)/1048576/1024, nconsec_io_percent, ost_cnt, proc_ost_level)))
#
#
print "###bad write performance instance:\n"
out_lst = vectorize_sweepline(stat_table, "WRITE", 4, 10737418240, 1073741824000, 1000, 0, long(1073741824), per_proc_size, is_per_factor)
print "out_lst length is %d\n"%len(out_lst)

tmp_dict = {}
max_val = 0
max_app = ""
max_idx = -1
for i in range(0, len(out_lst)):
    if tmp_dict.get(out_lst[i][2][1], -1) == -1:
        tmp_dict[out_lst[i][2][1]] = 1
    else:
        tmp_dict[out_lst[i][2][1]] += 1
        if tmp_dict[out_lst[i][2][1]] > max_val:
            max_val = tmp_dict[out_lst[i][2][1]]
            max_app = out_lst[i][0]
            max_idx = out_lst[i][2][1]
print "write app is %s, count:%d, id :%d\n"%(max_app, max_val, max_idx)


output_arr = extract_array(out_lst)

plot_path = miner_param["dataset_path"]+"sweep_write.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(output_arr, save_fd, -1)
#plotDistri(output_arr, "WRITE", "10_1000GB", [0]*multiply, 0, is_per_factor)
#save_fd.close()


print "###good read performance instance:\n"
out_lst = vectorize_sweepline(stat_table, "READ", 4, 10737418240, 1073741824000, 1000, 21474836480, long(10737418240000), per_proc_size, is_per_factor)
print "out_lst length is %d\n"%len(out_lst)
output_arr = extract_array(out_lst)
#plotDistri(output_arr,  "READ", "good10_1000GB", [0]*multiply, 0, is_per_factor, 1)
tmp_dict = {}
max_val = 0
max_app = ""
for i in range(0, len(out_lst)):
    if tmp_dict.get(out_lst[i][2][1], -1) == -1:
        tmp_dict[out_lst[i][2][1]] = 1
    else:
        tmp_dict[out_lst[i][2][1]] += 1
        if tmp_dict[out_lst[i][2][1]] > max_val:
            max_val = tmp_dict[out_lst[i][2][1]]
            max_app = out_lst[i][0]
            max_idx = out_lst[i][2][1]
print "read app is %s, count:%d, max_idx is %d\n"%(max_app, max_val, max_idx)


#plot_path = miner_param["dataset_path"]+"sweep_read_good.pkl" 
#save_fd = open(plot_path, 'wb')
#pickle.dump(output_arr, save_fd, -1)
#
#
#
#
print "###good write performance instance:\n"
out_lst = vectorize_sweepline(stat_table, "WRITE", 4, 10737418240, 1073741824000, 1000, 21474836480, long(10737418240000), per_proc_size, is_per_factor)
print "out_lst length is %d\n"%len(out_lst)
output_arr = extract_array(out_lst)
#plotDistri(output_arr, "WRITE", "good10_1000GB", [0]*multiply, 0, is_per_factor, 1)
plot_path = miner_param["dataset_path"]+"sweep_write_good.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(output_arr, save_fd, -1)

tmp_dict = {}
max_val = 0
max_app = ""
for i in range(0, len(out_lst)):
    if tmp_dict.get(out_lst[i][2][1], -1) == -1:
        tmp_dict[out_lst[i][2][1]] = 1
    else:
        tmp_dict[out_lst[i][2][1]] += 1
        if tmp_dict[out_lst[i][2][1]] > max_val:
            max_val = tmp_dict[out_lst[i][2][1]]
            max_app = out_lst[i][0]
            max_idx = out_lst[i][2][1]
print "write app is %s, count:%d, max_idx is %d\n"%(max_app, max_val, max_idx)

#plotSeqDist(out_dict["seq"], io_type, 10)
#plotConsecDist(out_dict["consec"], io_type, 10)
#plotSmallDist(out_dict["small_io"], io_type, 10)
#
#out_dict = {}
#out_dict = get_app_hist(stat_table)
#plotAppDist(out_dict)
#
#percent_lst = []
#percent_lst = get_meta_hist(stat_table, 0, 0, 100)
#
#plotMetaDist(percent_lst, 10)
#percent_lst = []
#percent_lst = get_io_com_hist(stat_table, 0, 0, 100)
#
#plotIOCompDist(percent_lst, 10)


for io_type in io_types:
    plot_small_io_hist(stat_table, io_type, fs, plot_dir_lowbw)
    plot_nonconsec_io_hist(stat_table, io_type, fs, plot_dir_lowbw)
    plot_bw_size(stat_table, io_type, plot_dir_lowbw)
    plot_bw_proc(stat_table, io_type, plot_dir_lowbw)
    reformat_low_factor_table(stat_table, io_type)

per_file_handle.close()
