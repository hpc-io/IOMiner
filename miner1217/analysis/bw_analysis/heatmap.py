
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
sys.path.insert(0, '../../slurm')
sys.path.insert(0, '../../data_prep/')
sys.path.insert(0, '../../plot/')
from construct_low_bw import *
from miner_stat import *
#from slurm_stat import *
#from miner_plot import *

root_dir="/global/cscratch1/sd/tengwang/miner1217/"
cluster_name = "cori"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner1217/miner_para.conf'))
bigtable_log = miner_param[cluster_name]["bigtable_log"]

tmp_str = root_dir+"meaningful_job.log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
print "stat_table size is %d\n"%len(stat_table)
save_fd.close()

# calculate the total CPU cycles for each application by summing up the CPU cycles of its individual jobs, the output (app_cnt.log) is used as input by plots/app_cnt_rename.ipynb 
out_dict = get_app_cycles(stat_table)

tmp_str = root_dir+"app_cnt.log"
save_fd = open(tmp_str, 'wb')
pickle.dump(out_dict, save_fd, -1)
save_fd.close()

# filter the top 15 CPU cycle consumer applications' records, and save their bandwidth in app_bw_ext.log, this file is used as input by plot/app_box.ipynb 
sorted_stat = labeling_record_by_app(stat_table, out_dict)
tmp_str = root_dir+"filtered_meaningful_job.log"
save_fd = open(tmp_str, 'wb')
pickle.dump(sorted_stat, save_fd, -1)
save_fd.close()

str_bw = "POSIX_agg_perf_by_slowest"

bw_lst = []
for record in sorted_stat:
    bw_lst.append((record["AppID"], record["FileName"], record[str_bw], record["AppName"], int((float(record["total_POSIX_BYTES_READ"]) + float(record["total_POSIX_BYTES_WRITTEN"]))/1048576/1024), int(record["nprocs"])))
tmp_str = root_dir+"app_bw_ext.log"
save_fd = open(tmp_str, 'wb')
pickle.dump(bw_lst, save_fd, -1)
save_fd.close()

# plot the heatmap based on resultDict.log generated from get_bw_matrix.py, which stores the correlation matrix of different I/O contributing factors.
tmp_str = root_dir+"resultDict.log"
save_fd = open(tmp_str, 'rb')
feature_vec = pickle.load(save_fd)
save_fd.close()

bw_matrix = plotFullBWCorrMatrix(feature_vec)

