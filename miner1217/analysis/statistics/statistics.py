
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
from construct_low_bw import *
from miner_stat import *
#from slurm_stat import *
#from miner_plot import *

root_dir="/global/cscratch1/sd/tengwang/miner1217/"
cluster_name = "cori"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner1217/miner_para.conf'))
bigtable_log = miner_param[cluster_name]["bigtable_log"]

tmp_str = "/global/cscratch1/sd/tengwang/miner1217/meaningful_job.log"
#tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
print "tmp_str is %s"%tmp_str
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()

print "######top readers based on aggregate read size:\n"
app_list = app_total_size_order(stat_table, "READ")
plot_path = miner_param["dataset_path"]+"top_read_app.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(app_list, save_fd, -1)
save_fd.close()


print "######top writers based on aggregate write size:\n"
cursor = 0
app_list = app_total_size_order(stat_table, "WRITE")
plot_path = miner_param["dataset_path"]+"top_write_app.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(app_list, save_fd, -1)
save_fd.close()

# get the distribution of sequential I/O ratio
out_dict = {}
abnormal_lst = {}
io_type = "READ"
(out_dict, abnormal_lst) = get_io_pat_hist(stat_table, 100)
plot_path = miner_param["dataset_path"]+"seq_dist.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(out_dict, save_fd, -1)
save_fd.close()

# get distribution of read write ratio
ratio_list = []
ratio_list = read_write_ratio(stat_table, 100)
plot_path = miner_param["dataset_path"]+"read_write_ratio.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(ratio_list, save_fd, -1)
save_fd.close()

# get the percent of jobs using different I/O types (e.g. POSIX, MPI-IO)
out_dict = {}
out_dict = ioTypeBin(stat_table)
plot_path = miner_param["dataset_path"]+"io_type.pkl" 
save_fd = open(plot_path, 'wb')
pickle.dump(out_dict, save_fd, -1)
save_fd.close()

