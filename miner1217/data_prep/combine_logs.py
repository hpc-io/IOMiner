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
# hardcoded parameter:
# cluster_name, root_dir, bigtable_log: same as analyze_darshan.py, as well as the location of miner_para.conf

miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner1217/miner_para.conf'))
cluster_name = "cori"
bigtable_log = miner_param[cluster_name]["bigtable_log"]
root_dir="/global/cscratch1/sd/tengwang/teng_miner/"

# merge the LMT information for each job (lustre_info.log) into the Darshan bigtable, and store the new table.


tmp_str = root_dir+"lustre_info.log"
save_fd = open(tmp_str, 'rb')
lmt_dict = pickle.load(save_fd)
save_fd.close()

tmp_str = bigtable_log+"_"+"formated_tot_stat_adv.pkl"+".log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()
print "path is %s\n"%tmp_str

out_table = []
output_log = []
for record in stat_table:
	if record.get("tot_ost_cnt", -1) == -1 or int(record["tot_ost_cnt"]) == 0:
            continue
	if lmt_dict.get(record["JobID"], -1) == -1:
            continue
	else:
            tmp_table = lmt_dict.get(record["JobID"])
            for sec_record in tmp_table:
                if sec_record["start_time"] == record["start_time"] and sec_record["end_time"] == record["end_time"]:
                    if sec_record.get("ossCPUAve", -1) == -1:
                        continue
                    if sec_record.get("ossCPUMax", -1) == -1:
                        continue
                    if sec_record.get("ossCPUMin", -1) == -1:
                        continue
                    if sec_record.get("mdsCPUAve", -1) == -1:
                        continue
                    if sec_record.get("ostIOAve", -1) == -1:
			continue
		    if sec_record.get("ostIOMin", -1) == -1:
			continue
		    if sec_record.get("ostIOMax", -1) == -1:
			continue
		    record["ossCPUAve"] = sec_record["ossCPUAve"] 
		    record["ossCPUMax"] = sec_record["ossCPUMax"] 
		    record["ossCPUMin"] = sec_record["ossCPUMin"] 
		    record["ostIOAve"] = sec_record["ostIOAve"] 
		    record["ostIOMin"] = sec_record["ostIOMin"] 
		    record["ostIOMax"] = sec_record["ostIOMax"]
		    record["mdsCPUAve"] = sec_record["mdsCPUAve"]
                    if record.get("nnodes", -1) != -1:
			print "app name:%s, path:%s, sec app name:%s, sec path:%s\n"%(record["AppName"], record["FileName"], sec_record["AppName"], sec_record["Path"])
		    break;

tmp_str = bigtable_log + "_"+"formatted_tot_wlmt_complete.log"
save_fd = open(tmp_str, 'wb')
pickle.dump(stat_table, save_fd, -1)
save_fd.close()

# extract the job logs with a runtime > 300s, and process count > 1, and store these logs' information into a new table (meaningful_job.log), you can change the number to use a different filtering criteria.
file_name = bigtable_log+"_"+"formatted_tot_wlmt_complete.log"
max_data_size = 1073741824
min_time = 300
save_fd = open(file_name, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()

out_table = []
for record in stat_table:
    run_time = float(record["end_time"]) - float(record["start_time"])
    if record.get("ossCPUAve", -1) == -1:
        continue
    if run_time < min_time:
        continue
    nprocs = int(record["nprocs"])
    if nprocs == 1:
        continue
    read_size = long(record["total_POSIX_BYTES_READ"])
    write_size = long(record["total_POSIX_BYTES_WRITTEN"])
    if write_size + read_size < max_data_size:
        continue
    print "start time:%s, end time:%s\n"%(record["start_time"], record["end_time"])
    out_table.append(record)
    print "id:%s, path:%s, ostIOMax:%s,nnodes:%s, mdscpu:%s\n"%(record["JobID"], record["FileName"], record["ostIOMax"],record["nnodes"], record["mdsCPUAve"])


tmp_str = root_dir+"meaningful_job.log"
save_fd = open(tmp_str, 'wb')
pickle.dump(out_table, save_fd, -1)
save_fd.close()


tmp_str = root_dir+"meaningful_job.log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()

#tmp_str = root_dir+"meta_pct.log"
#save_fd = open(tmp_str, 'rb')
#mds_dict = pickle.load(save_fd)
#save_fd.close()


fs_dict = {}
for record in stat_table:
    print "job id is %s\n"%(record["JobID"])
    if fs_dict.get(record["JobID"], -1) == -1:
        fs_dict[record["JobID"]] = []
    fs_dict[record["JobID"]].append((record["start_time"], record["end_time"], record["mdsCPUAve"], record["ossCPUAve"], record["ostIOAve"]))
    print "jobid:%s, cpu:%s, start:%s, end:%s\n"%(record["JobID"], str(record["mdsCPUAve"]), record["start_time"], record["end_time"])


#tmp_str = root_dir+"meaningful_job_mds.log"
#save_fd = open(tmp_str, 'wb')
#pickle.dump(stat_table, save_fd, -1)
#save_fd.close()

tmp_str = root_dir+"fs_dict.log"
save_fd = open(tmp_str, 'wb')
pickle.dump(fs_dict, save_fd, -1)
save_fd.close()

