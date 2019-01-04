
import numpy as np
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
import sys
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
from batch_sweep_analyze_mul import *
import multiprocessing
from multiprocessing import Manager
import subprocess

#hardcoded: root_dir: the top iominer directory
#cur_dir: the directory under root_dir that stores the output of this script


root_dir="/global/cscratch1/sd/tengwang/miner1217/"
pool_size = 1
cur_dir = "contri_factors"
#app_name = "s3d.x"
#app_name = "hacc_tpm"
#app_name = "Nyx3d.intel.mic-knl.COMTR_PROF.MPI.OMP.ex"
#app_name = "cesm.exe"
#app_name = "cesm.exe"
app_name = sys.argv[1] # application name

def get_level(bw):
    if bw < 1:
        return 0
    if bw >= 1 and bw < 10:
        return 1
    if bw >= 10 and bw < 100:
        return 2
    if bw >= 100:
        return 3


def print_factor_dict(factor_dict, fs_dict):
    glb_r_factors = factor_dict["glb_r_ractors"]
    sub_r_factors = factor_dict["sub_r_factors"]
    glb_w_factors = factor_dict["glb_w_factors"]
    sub_w_factors = factor_dict["sub_w_factors"]
    glb_rw_factors = factor_dict["glb_rw_factors"]
    sub_rw_factors = factor_dict["sub_rw_factors"]
    glb_contr = factor_dict["g_contr"]
    glb_dict = factor_dict["glb_dict"]

    job_id = glb_dict["JobID"]
    start_time = glb_dict["start_time"]
    end_time = glb_dict["end_time"]

    mdsCPUAve = 0
    ossCPUAve = 0
    ostIOAve = 0

    print "job_id:%s, size of fs_dict:%d\n"%(job_id, len(fs_dict[job_id]))
    record_lst = fs_dict[job_id]
    for record in record_lst:
        if record[0] == start_time and record[1] == end_time:
            mdsCPUAve = record[2]
            ossCPUAve = record[3]
            ostIOAve = record[4]
            print "id:%s, mdsCPUAve:%s, ossCPUAve:%s, ostIOAve:%s\n"%(job_id, mdsCPUAve, ossCPUAve, ostIOAve)
            break


    record = (glb_dict["UserName"], glb_dict["FileName"], float(glb_dict["nprocs"]), glb_contr["read_size"] + glb_contr["write_size"], glb_contr["ost_cnt"], glb_contr["darshan_bw"], glb_contr["miner_bw"], glb_contr["seq_io_ratio"], sub_rw_factors["seq_io_ratio"], glb_contr["small_io_ratio"], sub_rw_factors["small_io_ratio"], glb_contr["mid_io_ratio"], sub_rw_factors["mid_io_ratio"], glb_contr["meta_ratio"], sub_rw_factors["meta_ratio"], glb_contr["max_rank_pct_wr"], sub_rw_factors["stripe_width"], sub_rw_factors["slowest_wr_pct"], sub_rw_factors["nprocs"], mdsCPUAve, ossCPUAve, ostIOAve)
    print ("user:%s, path:%s, nprocs:%lf, size:%lf, ost_cnt:%lf, darshan_bw:%lf, miner_bw:%lf, glb_seq_io_ratio:%lf, sweep_seq_io_ratio:%lf, glb_small_io_ratio:%lf, sweep_small_io_ratio:%lf, glb_mid_io_ratio:%lf, sweep_mid_io_ratio:%lf, glb_meta_ratio:%lf, sweep_meta_ratio:%lf, max_rank_pct_wr:%lf, stripe_width:%lf, slowest_wr_pct:%lf, sub_nprocs:%lf\n"%(record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18]))
    return record

fs_dict = {}
save_fd = open(root_dir+"fs_dict.log", 'rb')
fs_dict = pickle.load(save_fd)
save_fd.close()


result_dict = []
for i in range(0, pool_size):
    fname = root_dir+"%s/contri_factors_%s.%d.log"%(cur_dir, app_name, i)
    if pool_size == 1:
        fname = root_dir+"%s/contri_factors_%s.log"%(cur_dir, app_name)
    save_fd = open(fname, 'rb')
    factor_dict = pickle.load(save_fd)
    for record in factor_dict:
        result_dict.append(print_factor_dict(record, fs_dict))
    save_fd.close()

fname=root_dir+"%s_factors.log"%(app_name)
save_fd = open(fname, 'wb')
pickle.dump(result_dict, save_fd, -1)
save_fd.close()

fname = root_dir+"%s_factors.log"%(app_name)
save_fd = open(fname, 'rb')
result_tbl = pickle.load(save_fd)
save_fd.close()
result_tbl.sort()
sorted_tbl = sorted(result_tbl, key = lambda x:x[0])
for record in sorted_tbl:
    print ("user:%s, path:%s, nprocs:%lf, size:%lf, ost_cnt:%lf, darshan_bw:%lf, miner_bw:%lf, glb_seq_io_ratio:%lf, sweep_seq_io_ratio:%lf, glb_small_io_ratio:%lf, sweep_small_io_ratio:%lf, glb_mid_io_ratio:%lf, sweep_mid_io_ratio:%lf, glb_meta_ratio:%lf, sweep_meta_ratio:%lf, max_rank_pct_wr:%lf, stripe_width:%lf, slowest_wr_pct:%lf, sub_nprocs:%lf\n"%(record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18]))

user_profile = {}
user_dict = {}
result_tuples = []
user_id = 0
jobid = 0
for record in sorted_tbl:
    if user_dict.get(record[0], -1) == -1:
        user_dict[record[0]] = [user_id, 1]
        user_id += 1
    else:
        user_dict[record[0]][1] += 1
    jobid += 1
    cur_tuple = (int(user_dict.get(record[0])[0]), jobid, int(float(record[20])), int(float(record[21])), int((record[3]/1024/1048576)), int(record[2]), int(record[4]), int(record[8] * 100), int(record[10] * 100), int(float(record[19])), int(record[14] * 100),  int(record[15] * 100), get_level(int(float(record[6])/1024)))
    if user_profile.get(user_dict[record[0]][0], -1) == -1:
        user_profile[user_dict[record[0]][0]] = []
#    user_profile[user_dict[record[0]][0]].append((cur_tuple, float(record[6])/1024))
    user_profile[user_dict[record[0]][0]].append(cur_tuple)
#    print "bw:%lf, level:%d\n"%(float(record[6])/1024, get_level(int(float(record[6])/1024)))
    print ("uid:%d, user:%s, id:%d, size:%lf, nprocs:%lf, ost_cnt:%lf, seq_io_ratio:%lf, small_io_ratio:%lf, max_rank_pct_wr:%lf, bw:%lf"%(user_id ,record[0], cur_tuple[0], cur_tuple[2], cur_tuple[3], cur_tuple[4], cur_tuple[5], cur_tuple[6], cur_tuple[8], cur_tuple[9]))
    result_tuples.append(cur_tuple)

#fname = "%s_user_profile.log"%app_name
#save_fd = open(fname, 'wb')
#pickle.dump(user_profile, save_fd, -1)
#save_fd.close()


fname=root_dir+"%s_plot_factors.log"%(app_name)
save_fd = open(fname, 'wb')
pickle.dump(result_tuples, save_fd, -1)
save_fd.close()
#
#for key,value in user_profile.iteritems():
#    print ("######user id:%d, count:%d\n"%(key, len(value)))   
#    if key == 19:
#        fname=root_dir+"%s_%d_plot_factors.log"%(app_name, key)
#        save_fd = open(fname, 'wb')
#        pickle.dump(value, save_fd, -1)
#        save_fd.close()






