import collections
import h5py
import numpy as np
import json
from collections import OrderedDict
import pickle
import datetime
import pytz
import calendar
import time
import glob
import os

cluster_name = "cori"
fs_name_map = {
    'scratch1': 'edison_snx11025',
    'scratch2': 'edison_snx11035',
    'scratch3': 'edison_snx11036',
    'cscratch': 'cori_snx11168',
}
miner_param = json.load(open('/global/cscratch1/sd/tengwang/latestminer/miner_para.conf'))

def convert_to_ts(d):
    ptz = pytz.timezone("US/Pacific")
    return calendar.timegm(ptz.localize(d).utctimetuple())

def getIndex(date):
    index = 0
    d = datetime.datetime(date.year,date.month,date.day)
    if date > datetime.datetime(d.year,d.month,d.day):
        index = int((date - d).total_seconds()/5)
    return index

def get_lmt_file_list(file_path, start_date, end_date, lmt_scratch_file = 'cori_snx11168'):
    utc = pytz.utc
    ptz = pytz.timezone("US/Pacific")
    start_ts = convert_to_ts(start_date)
    end_ts = convert_to_ts(end_date)
    d = datetime.datetime(start_date.year,start_date.month,start_date.day)

    start_index = getIndex(start_date)
    end_index = getIndex(end_date)
    print start_index, end_index
    delta = datetime.timedelta(days=1)
    file_list = []
    while d <= end_date:
        file_list.append(file_path + d.strftime("%Y-%m-%d")+'/%s.h5lmt'%lmt_scratch_file)
        log_str0 = file_path + d.strftime("%Y-%m-%d")+'/%s.h5lmt'%lmt_scratch_file
        log_str1 = "path is "
        log_str = log_str0 + log_str1 + "\n"
        print log_str
#        logging.info(log_str)
#        print(log_str)
        d += delta
    return (start_index,end_index,file_list)

def lmt_list_wrapper(start_index, end_index, file_list, dataset_name):
    lmt_list = []
    lmt_list_meta = []
    cursor = 0
    cur_cnt = 0
    prev_cnt = 0
    next_cnt = 0
    for i in range(len(file_list)):
        filename = file_list[i]
        if not os.path.exists(filename):
            print "file:%s not exist\n"%filename
            cur_cnt = 0
            lmt_list_meta.append(cur_cnt)
            continue
        f=h5py.File(filename,'r')
        print dataset_name
        ossdata=f[str(dataset_name)]

        if i == 0 and len(file_list) == 1:
            lmt_list = ossdata[:,start_index:end_index]
        elif i == 0:
            lmt_list = ossdata[:,start_index:17280]
        elif i == len(file_list)-1:
            lmt_list = np.append(lmt_list,ossdata[:,0:end_index], axis=1)
        else:
            lmt_list = np.append(lmt_list,ossdata[:,0:17280], axis=1)
        prev_cnt = next_cnt
        next_cnt = len(lmt_list[0])
        cur_cnt = next_cnt - prev_cnt
        lmt_list_meta.append(cur_cnt)
#        print "filename:%s, cur_cnt is %d, list len:%d\n"%(filename, cur_cnt, \
#            len(lmt_list[0]))
    for i in range(0, len(lmt_list_meta)):
        print "filename:%s, lmt_list_meta is %d\n"%(filename, lmt_list_meta[i])

    return (lmt_list, lmt_list_meta)

def lmt_rw_list(start_index,end_index,file_list):
    (rl, rl_meta) = lmt_list_wrapper(start_index, end_index, file_list, "/OSTReadGroup/OSTBulkReadDataSet")
    (wl, wl_meta) = lmt_list_wrapper(start_index, end_index, file_list, "/OSTWriteGroup/OSTBulkWriteDataSet")
    return (rl, rl_meta, wl, wl_meta)


def lmt_parse_lst():
    lmt_server_path = miner_param[cluster_name]["lmt_server_path"]
    timeStartArray = time.strptime(miner_param["start_ts"], "%Y-%m-%d %H:%M:%S")
    job_start_ts = int(time.mktime(timeStartArray))
    timeEndArray = time.strptime(miner_param["end_ts"], "%Y-%m-%d %H:%M:%S")
    job_end_ts = int(time.mktime(timeEndArray))
    (start_index, end_index, file_list) = get_lmt_file_list(lmt_server_path, datetime.datetime.fromtimestamp(job_start_ts), datetime.datetime.fromtimestamp(job_end_ts), fs_name_map[miner_param[cluster_name]["scratch_name"]])
    print "start_index:%d, end_index:%d\n"%(start_index, end_index)
    (lmt_read, lmt_read_meta, lmt_write, lmt_write_meta) = lmt_rw_list(start_index, end_index, file_list)
    return (lmt_read, lmt_write)

(lmt_read, lmt_write) = lmt_parse_lst()
lmt_read = lmt_read * 5
lmt_write = lmt_write * 5
agg_write = np.sum(lmt_write, axis = 1)
agg_read = np.sum(lmt_read, axis = 1)
tot_write = np.sum(agg_write)
tot_read = np.sum(agg_read)
print "total_read_size:%ld, total_write_size:%ld\n"%(tot_read, tot_write)


