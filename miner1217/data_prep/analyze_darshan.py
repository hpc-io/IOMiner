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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
from construct_low_bw import *
sys.path.append('../slurm')
from miner_stat import *
from slurm_stat import *

# miner_para is the path of the json configuration file
# cori is the entry defined in "miner_para.conf"
# root_dir is the iominer's top directory
# darshan_root is the location of the darshan logs, defined in miner_para.conf as darshan_root
# parsed_darshan_root is the location to store the parsed Darshan files by iominer, defined in miner_para.conf as parsed_darshan_root (under cori)
# cpy_darshan_root is the location that stores the copied Darshan files from darshan_root, defined in miner_para.conf as cpy_darshan_root
# bigtable_log is the location of the constructed Darshan bigtable.
# per_file_log is the path of the file that stores all the per-file counters parsed by iominer. If this file does not exist, you can create a file by touch <path of per_file_log>
# miner_param["start_ts"], miner_para["end_ts"] define the start time and end time of the analyzed logs

cluster_name = "cori"
root_dir="/global/cscratch1/sd/tengwang/miner1217/"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner1217/miner_para.conf'))
darshan_root = miner_param[cluster_name]["darshan_root"]
parsed_darshan_root = miner_param[cluster_name]["parsed_darshan_root"]
cpy_darshan_root = miner_param[cluster_name]["cpy_darshan_root"]
#cpy_darshan_root = "/global/cscratch1/sd/darshanlogs/" 
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
#        print "formated_darshan_path:%s\n"%formated_darshan_path
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
#    print "stat_table %s length is %d\n"%(formated_filename, len(stat_table))
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
#        print "cpy darshan_files is %s\n"%darshan_files
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


# copy darshan files produced during start_ts and end_ts to the directory given by cpy_darshan_root

cpy_darshan_files(darshan_root, cpy_darshan_root, start_ts, \
        end_ts)

# parse the darshan files into text format, two types of logs are generated, one is .total and one is .all. Refer to Darshan page for understanding these two types (darshan-parser --total, and darshan-parser --all). 

get_parsed_list(cpy_darshan_root, parsed_darshan_root, start_ts, end_ts, ext = 'all', decompressed_darshan_dir = "", version=3)
get_parsed_list(cpy_darshan_root, parsed_darshan_root, start_ts, end_ts, ext = 'total', decompressed_darshan_dir = "", version=3)


# format_parsed_files and construct_bigtable construct a bigtable that format each job (darshan log)'s counters as one record in the table. E.g. stat_table[N]["total_POSIX_BYTES_WRITTEN"] gives the total bytes written by Nth job.


format_parsed_files(parsed_darshan_root, format_darshan_root, start_ts, end_ts)

stat_table = construct_bigtable(format_darshan_root, start_ts, end_ts, "formated_tot_stat.pkl")

# inline application name with each record, the application name is extracted from the filename of the log. An alternative way is to get the application name by "exe" counter in the Darshan log. 
new_stat_table = covert_app_name(stat_table)
tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'wb')
pickle.dump(new_stat_table, save_fd, -1)
save_fd.close()


tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()


# retrieve_slurm_files reads the SLURM table that contains one record for each job. The followed script combines the SLURM records with the records of bigtable_log_formated_tot_stat_adv.pkl.log. Note: SLURM table needs to be constructed before calling retrieve_slurm_files by the scripts slurm_stat.py under slurm/. If there is no such table constructed, please set slurm_table = {}. This script will automatically extract nnode count by sacct 

#slurm_table = retrieve_slurm_files(miner_param["slurm_job_dir"])
slurm_table = {}
darshan_counter = 0
darshan_slurm_counter = 0
one_job = 0
one_job_rest = 0
noinfo = 0
slurm_dict = {}
for record in stat_table:
        darshan_counter += 1
        if slurm_table.get(record["JobID"], -1) == -1:
            cmd = "sacct -j " + str(record["JobID"]) + " --format=ntasks,ncpus,nnodes"
            tempOut =  subprocess.check_output(cmd, shell=True)
            tempOut = tempOut.strip()
            tempOut = tempOut.split(" ")
            temp = []
            for i in tempOut:
                if i.isdigit():
                    temp.append(i)
            numTasks = int(temp[len(temp) - 3])
            numCPU = int(temp[len(temp) - 2])
            numNode = int(temp[len(temp) - 1])
	    record["nnodes"] = numNode
	    record["cpus"] = numCPU
            record["ntasks"] = numTasks
            slurm_dict[record["JobID"]] = {}
            slurm_dict[record["JobID"]]["nnodes"] = str(numNode)
            slurm_dict[record["JobID"]]["cpus"] = str(numCPU)
            slurm_dict[record["JobID"]]["ntasks"] = str(numTasks)
            if numTasks == 1:
                one_job += 1
                one_job_rest += 1
            print "unfound job id is %s, darshan_counter:%d, darshan_slurm_counter:%d, cpu count is %d, node count is %d\n"%(record["JobID"], darshan_counter, darshan_slurm_counter, numCPU, numNode)

	if (slurm_table.get(record["JobID"], -1) != -1):
                darshan_slurm_counter += 1
		value = slurm_table.get(record["JobID"])
		print "job id is %s in slurm\n"%record["JobID"]
		for tmp_dict in value:
			if "batch" not in tmp_dict["JobID"] and "extern" not in tmp_dict["JobID"]:
				record["nnodes"] = tmp_dict["AllocNodes"] 
				record["cpus"] = tmp_dict["AllocCPUS"]
                                record["ntasks"] = tmp_dict["NTasks"]
                                sub_value = tmp_dict["NTasks"]
                                if sub_value != "":
                                    idx = sub_value.find('K')
                                    if idx != -1:
                                        ntasks = float(sub_value[0:idx]) * 1000
                                        ntasks = int(ntasks)
                                    else:
                                        ntasks = int(sub_value)
                                    if ntasks == 1:
                                        one_job += 1
                                else:
                                    noinfo += 1

                                slurm_dict[record["JobID"]] = {}
                                slurm_dict[record["JobID"]]["nnodes"] = record["nnodes"]
                                slurm_dict[record["JobID"]]["cpus"] = record["cpus"]
                                slurm_dict[record["JobID"]]["ntasks"] = record["ntasks"]

tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'wb')
pickle.dump(stat_table, save_fd, -1)
save_fd.close()


per_file_handle.close()
