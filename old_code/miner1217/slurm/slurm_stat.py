import collections
import json
from collections import OrderedDict
import pickle
import datetime
from datetime import datetime,timedelta
import time
#from slurm_plot import *

miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner1217/miner_para.conf'))
 
def format_slurm_files(format_slurm_root, start_ts, end_ts):
    timeStartArray = time.strptime(start_ts, "%Y-%m-%d %H:%M:%S")
    job_start_ts = int(time.mktime(timeStartArray))
    timeEndArray = time.strptime(end_ts, "%Y-%m-%d %H:%M:%S")
    job_end_ts = int(time.mktime(timeEndArray)) 
    fmt_start_time = time.localtime(job_start_ts)
    fmt_end_time = time.localtime(job_end_ts)
    start_date = datetime(fmt_start_time[0], fmt_start_time[1], fmt_start_time[2], fmt_start_time[3], fmt_start_time[4], fmt_start_time[5])
    end_date = datetime(fmt_end_time[0], fmt_end_time[1], fmt_end_time[2], fmt_end_time[3], fmt_end_time[4], fmt_end_time[5])
    start_time = start_date.strftime("%m/%d/%y-%H:%M:%S")
    end_time = end_date.strftime("%m/%d/%y-%H:%M:%S")
    filename = format_slurm_root + 'slurm_%s_%s.log'%(start_date.strftime("%y_%m_%d-%H-%M-%S"),end_date.strftime("%y_%m_%d-%H-%M-%S"))

    f = open(filename, 'r')
    text = f.readline()
    headerLines = text.split("|")
    useful_metas = ["JobID", "User", "JobName", "Start", "End", "Elapsed", "State", "AllocNodes", "NTasks", "AllocCPUS", "ReqCPUS"]

    glb_cursor = 0
    slurm_table = {}
    while True:
        text = f.readline()
        if not text:
            break
        if glb_cursor >= 2:
            tmp_dict = {}
            record_lines = text.split("|")
            job_name = record_lines[0].split('.')[0].split('_')[0]
            if slurm_table.get(job_name, -1) != -1:
                tmp_lst = slurm_table[job_name]
                cursor = 0
                for tmp_str in record_lines:
                    if headerLines[cursor] not in useful_metas:
                        continue
                    tmp_dict[headerLines[cursor]] = tmp_str
                    cursor = cursor + 1
                ret_dict = collections.OrderedDict(tmp_dict)
                tmp_lst.append(ret_dict)
#                print "job_name is %s, end:%s, cursor:%d, user:%s, app:%s\n"%(tmp_dict["JobID"], tmp_dict["End"], glb_cursor, tmp_dict["User"], tmp_dict["JobName"]) 
            else:
                tmp_lst = []
                cursor = 0
                for tmp_str in record_lines:
                    if headerLines[cursor] not in useful_metas:
                        continue
                    tmp_dict[headerLines[cursor]] = tmp_str
                    cursor = cursor + 1
                ret_dict = collections.OrderedDict(tmp_dict)
                tmp_lst.append(ret_dict)
#                print "job_name is %s, end:%s, cursor:%d, user:%s, app:%s\n"%(tmp_dict["JobID"], tmp_dict["End"], glb_cursor, tmp_dict["User"], tmp_dict["JobName"]) 
                slurm_table[job_name] = tmp_lst
        glb_cursor = glb_cursor + 1
    tmp_str = "%sformat_slurm%s.log"%(format_slurm_root, start_ts)
    save_fd = open(tmp_str, 'wb')
    pickle.dump(slurm_table, save_fd, -1)
    save_fd.close()

def retrieve_slurm_files(format_slurm_root):
    tmp_str = "%sformat_slurm.log"%format_slurm_root
    save_fd = open(tmp_str, 'rb')
    slurm_table = pickle.load(save_fd)
    save_fd.close()
    return slurm_table

def get_sec(time_str):
    arr = time_str.split('-')
    if len(arr) == 1:
        h, m, s = time_str.split(':')
        return long(h) * 3600 + long(m) * 60 + long(s)
    else:
        day_secs = 3600 * 24 * long(arr[0])
        h, m, s = arr[1].split(':')
        return  day_secs + long(h) * 3600 + long(m) * 60 + long(s)

def get_slurm_hours(slurm_table):
    print "number of jobs is len%d\n"%(len(slurm_table))
    tot_time = 0
    for key,value in slurm_table.iteritems():
        for tmp_dict in value:
#            if "COMPLETED" not in tmp_dict["State"]:
#                continue
            if "batch" in tmp_dict["JobID"] or "extern" in tmp_dict["JobID"]:
                continue
            time_str = tmp_dict["Elapsed"]

            sub_value = tmp_dict["NTasks"]
            if sub_value != "":
                idx = sub_value.find('K')
		if idx != -1:
                    ntasks = float(sub_value[0:idx]) * 1000
                    ntasks = int(ntasks)
                    tot_time += ntasks * get_sec(time_str)
                else:
                    ntasks = int(sub_value)
                    tot_time += ntasks * get_sec(time_str)
    print "tot_time is %ld\n"%tot_time
             
def print_slurm_info(slurm_table):
    print "JobID\tUser\tAppName\tStart\tEnd\tNNodes\tState\n"
    for key, value in slurm_table.iteritems():
        for tmp_dict in value:
            sub_value = tmp_dict["JobID"]
            if "batch" in sub_value or "extern" in sub_value:
                continue

            job_id = sub_value
            sub_value = tmp_dict["AllocCPUS"]
	    idx = sub_value.find('K')
	    if idx != -1:
                alloc_cpus = float(sub_value[0:idx]) * 1000
                alloc_cpus = int(alloc_cpus)
	    else:
                alloc_cpus = int(sub_value)

            sub_value = tmp_dict["AllocNodes"]
            if sub_value != "":
                idx = sub_value.find('K')
                if idx != -1:
                    alloc_nodes = float(sub_value[0:idx]) * 1000
                    alloc_nodes = int(ntasks)
                else:
                    alloc_nodes = int(sub_value)

            sub_value = tmp_dict["NTasks"]
            if sub_value != "":
                idx = sub_value.find('K')
		if idx != -1:
                    ntasks = float(sub_value[0:idx]) * 1000
                    ntasks = int(ntasks)
                else:
                    ntasks = int(sub_value)

            start_ts = tmp_dict["Start"]
            end_ts = tmp_dict["End"]
            state = tmp_dict["State"]
            user_name = tmp_dict["User"]
            app_name = tmp_dict["JobName"]
            print "%s, %s, %s, %s, %s, %d, %s\n"%(job_id, user_name, app_name, start_ts, end_ts, alloc_nodes, state)
            


def get_utilization_dist(slurm_table):
    percent_knl_lst = []
    percent_has_lst = []
    tot_cnt = 0
    knl_cnt = 0
    has_cnt = 0
    avail_knl_cnt = 0
    avail_has_cnt = 0
    overscribed_knl_cnt = 0
    overscribed_has_cnt = 0
    one_proc = 0
    tot_proc = 0
    for key, value in slurm_table.iteritems():
        for tmp_dict in value:
            cursor = 0
            alloc_cpus = -1
            alloc_nodes = -1
            ntasks = -1

            tot_proc += 1
#            print "jobid:%s\n"%tmp_dict["JobID"]
#            if "COMPLETED" not in tmp_dict["State"]:
#                continue

            sub_value = tmp_dict["JobID"]
            if "batch" in sub_value or "extern" in sub_value:
                continue
            sub_value = tmp_dict["AllocCPUS"]
	    idx = sub_value.find('K')
	    if idx != -1:
                alloc_cpus = float(sub_value[0:idx]) * 1000
                alloc_cpus = int(alloc_cpus)
	    else:
                alloc_cpus = int(sub_value)

            sub_value = tmp_dict["AllocNodes"]
            if sub_value != "":
                idx = sub_value.find('K')
                if idx != -1:
                    alloc_nodes = float(sub_value[0:idx]) * 1000
                    alloc_nodes = int(ntasks)
                else:
                    alloc_nodes = int(sub_value)

            sub_value = tmp_dict["NTasks"]
            if sub_value != "":
                idx = sub_value.find('K')
		if idx != -1:
                    ntasks = float(sub_value[0:idx]) * 1000
                    ntasks = int(ntasks)
                else:
                    ntasks = int(sub_value)

            if ntasks == 1:
                one_proc += 1

            tot_cnt = tot_cnt + 1
            if alloc_cpus%272 == 0:
               knl_cnt = knl_cnt + 1
            if alloc_cpus%272 == 0 and ntasks != -1 and alloc_nodes != -1:
                avail_knl_cnt += 1
                per_node_procs = ntasks/alloc_nodes
                if per_node_procs > 272:
                    overscribed_knl_cnt += 1
                percent_knl_lst.append(per_node_procs)


            if alloc_cpus%272 != 0:
               has_cnt = has_cnt + 1
            if alloc_cpus%272 != 0 and ntasks != -1 and alloc_nodes != -1:
                avail_has_cnt += 1
                per_node_procs = ntasks/alloc_nodes
                if per_node_procs > 64:
                    overscribed_has_cnt += 1
                percent_has_lst.append(per_node_procs)
            else:
                print "problematic jobid:%s\n"%tmp_dict["JobID"]

    print "node utilization dist:tot_cnt:%d, knl_cnt:%d, avail_knl_cnt:%d, over_knl_cnt:%d, has_cnt:%d, avail_has_cnt:%d, over_has_cnt:%d, one proc:%d, tot_cnt:%d\n"%(tot_cnt, knl_cnt, avail_knl_cnt, overscribed_knl_cnt, has_cnt, avail_has_cnt, overscribed_has_cnt, one_proc, tot_cnt)
    sorted_has_lst = sorted(percent_has_lst)
    sorted_knl_lst = sorted(percent_knl_lst)
    return (sorted_has_lst, sorted_knl_lst)

#format_slurm_files(miner_param["slurm_job_dir"], "2017-10-01 00:00:00", "2017-10-31 23:59:59")
# construct the slurm log produced from collect_slurm_month.py into table format by format_slurm_files

#format_slurm_files(miner_param["slurm_job_dir"], "2017-11-01 00:00:00", "2017-11-30 23:59:59")

# combine the slurm tables produced during 10/2017 and 11/2017
#tmp_str = "format_slurm_10.log"
#save_fd = open(tmp_str, 'rb')
#slurm_table = pickle.load(save_fd)
#save_fd.close()
#
#tmp_str = "format_slurm_11.log"
#save_fd = open(tmp_str, 'rb')
#slurm_table1 = pickle.load(save_fd)
#save_fd.close()
#
#slurm_table.update(slurm_table1)
#save_fd = open("format_slurm.log", 'wb')
#pickle.dump(slurm_table, save_fd, -1)
#save_fd.close()


#print "retrieving slurm files\n"
#slurm_table = retrieve_slurm_files(miner_param["slurm_job_dir"])
#print_slurm_info(slurm_table)

# calculate the CPU coverage of Slurm logs
# get_slurm_hours(slurm_table)

#(sorted_has_lst, sorted_knl_lst) = get_utilization_dist(slurm_table)
