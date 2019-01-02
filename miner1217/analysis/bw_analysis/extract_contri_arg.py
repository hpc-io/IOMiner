
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
from collections import OrderedDict
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
from batch_sweep_analyze_mul import *
import multiprocessing
from multiprocessing import Manager
import subprocess

# Extract all the performance contributing factors for each job of a given application, and store them under the directory contri_factors. These output are used by app_analyze.py for generating the data source of this application's parallel coordinate plot.  
root_dir="/global/cscratch1/sd/tengwang/miner1217/"
tmp_str = root_dir+"app_bw_ext.log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()


tmp_str = root_dir+"app_cnt.log"
save_fd = open(tmp_str, 'rb')
cycle_table = pickle.load(save_fd)
save_fd.close()

counter = 0
for key,value in cycle_table.items():
    print ("key:%s, value:%ld\n"%(key, value[0]))
    counter += 1
    if counter == 15:
        break
app_name = sys.argv[1]
#app_list = ["cesm.exe", "pw.x", "hacc_tpm", "gene_cori"]
app_path_dict = {}
tot_size = 0

# record the paths of all the logs of the top 15 CPU cycle consumer applications in app_path_dict. E.g., app_path_dict[app_name][0] for app_name
for record in stat_table:
    if app_path_dict.get(record[3], -1) == -1:
        app_path_dict[record[3]] = ([], cycle_table[record[3]][0])
        print ("app:%s, cycles:%ld\n"%(record[3], cycle_table[record[3]][0]))
    new_fname = record[1].rpartition("/")[0]+"/"+record[1].rpartition("/")[2].rpartition(".")[0]+".all"
    if record[3] == app_name:
        size = int(os.path.getsize(new_fname))
        if int(os.path.getsize(new_fname)) > 1073741824:
            continue
        print ("new_fname is %s, size is %s\n"%(new_fname, size))
        tot_size += size
        app_path_dict[record[3]][0].append(new_fname)
        print ("tot_size is %ld"%(tot_size))

sorted_dict = OrderedDict(sorted(app_path_dict.items(), key = lambda x:-x[1][1]))

pool_size = int(sys.argv[2])
p = multiprocessing.Pool(pool_size)
for key,value in sorted_dict.items():
    if key == app_name:
        print ("appname:%s\n"%key)
        shard_cnt = pool_size
        per_shard = len(value[0])/shard_cnt
        max_len = len(value[0])

        cursor = 0
        start = 0
        for i in range(0, shard_cnt):
            if i != 0:
                tmp_key = "%s.%d"%(key, i)
            else:
                tmp_key = key
            end = start + per_shard - 1
            if i == shard_cnt - 1:
                end = max_len - 1
            print "###start:%d, end:%d\n"%(start, end)
            cpy_values = list(value[0][start:end + 1])
            p.apply_async(save_mul_files, [cpy_values, tmp_key, "contri_factors"])
            if i < shard_cnt:
                start = end + 1
p.close()
p.join()

