import pickle
import string
import re
import math
import collections
import operator
from operator import itemgetter
from collections import OrderedDict
from construct_low_bw import *
from bitmap import *

plot_dir = "/global/cscratch1/sd/tengwang/latestminer/plots/allpkl/"
fname = plot_dir + "mpi_prb0_1.pkl"
file_pos = 0

def parse_darshan_jobid(s):
    match = darshan_file_pattern.match(s)
    if match is not None:
        return match.group(2)
    else:
        return "0"

def parse_darshan_user_appname(s):
    match = darshan_file_pattern.match(s)
    if match is not None:
        return match.group(1)
    else:
        return "0"

def getNameFromPath(fname):
    suffix = fname.rsplit('/')[-1]
    jobID = parse_darshan_jobid(suffix)
    userAppName = parse_darshan_user_appname(suffix)
    appName = userAppName.split("_")[0]
    myAppName = appName+"_"
    myAppName = userAppName.split(myAppName)[1]
    return myAppName

def covert_app_name(stat_table):
    for stat_row in stat_table:
        file_name = stat_row["FileName"]
        extract_app = getNameFromPath(file_name)
        stat_row["AppName"] = extract_app
        print "path:%s, extract app is %s\n"%(file_name, extract_app)
    return stat_table




def tot_app_size_time(stat_table):
    tot_read_size = 0
    tot_write_size = 0
    tot_seconds = 0 
    for stat_row in stat_table:
        if stat_row.get("total_POSIX_BYTES_READ", -1) != -1:
            tot_read_size += long(stat_row["total_POSIX_BYTES_READ"])
        if stat_row.get("total_POSIX_BYTES_WRITTEN", -1) != -1:
            tot_write_size += long(stat_row["total_POSIX_BYTES_WRITTEN"])
        end_time = stat_row["end_time"]
        start_time = stat_row["start_time"]
        nprocs = stat_row["nprocs"]
        tot_seconds += long(nprocs) * (long(end_time) - long(start_time))
    return (tot_read_size, tot_write_size, tot_seconds)

        

def app_total_size_order(stat_table, io_type):
    app_cnt = 0
    app_dict = {}
    app_list = []

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
        other_io_size = "total_POSIX_BYTES_WRITTEN"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
        other_io_size = "total_POSIX_BYTES_READ"

    str_write_size = "total_POSIX_BYTES_WRITTEN"
    str_read_size = "total_POSIX_BYTES_READ"

    for stat_row in stat_table:
        if app_dict.get(stat_row["AppName"], -1) != -1:
            (data_size, fname, nprocs, write_size, read_size) = app_dict[stat_row["AppName"]]
            if io_type != "ALL":
                data_size = data_size + long(stat_row[str_io_size])
            else:
                data_size = data_size + long(stat_row[str_write_size]) + long(stat_row[str_read_size])
            write_size = write_size + long(stat_row[str_write_size])
            read_size = read_size + long(stat_row[str_read_size])
            nprocs = nprocs + long(stat_row["nprocs"])
            app_dict[stat_row["AppName"]] = (data_size, fname, nprocs, write_size, read_size)
        else:
            if io_type == "ALL":
                data_size = long(stat_row[str_write_size]) + long(stat_row[str_read_size])
            else:
                data_size = long(stat_row[str_io_size])
            app_dict[stat_row["AppName"]] = (data_size, stat_row["FileName"], long(stat_row["nprocs"]), long(stat_row[str_write_size]), long(stat_row[str_read_size]))

    for key,value in app_dict.iteritems():
        app_list.append((key, value[0], value[1], value[2], value[3], value[4]))

    sorted_app_list = sorted(app_list, key = lambda x:x[1], reverse=True)
#    sorted_app_list = sorted(app_dict.items(), key = operator.itemgetter(1)[0], reverse = True)
    return sorted_app_list

def app_ind_avgsize_order(stat_table, io_type):
    app_cnt = 0
    app_dict = {}
    app_list = []

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
        other_io_size = "total_POSIX_BYTES_WRITTEN"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
        other_io_size = "total_POSIX_BYTES_READ"

    str_write_size = "total_POSIX_BYTES_WRITTEN"
    str_read_size = "total_POSIX_BYTES_READ"

    for stat_row in stat_table:
        nprocs = long(stat_row["nprocs"])
        if app_dict.get(stat_row["AppName"], -1) != -1:
            (avgsize, fname, proc_cnt, write_size, read_size) = app_dict[stat_row["AppName"]]
            if io_type == "ALL":
                cur_data_size = long(stat_row[str_write_size]) + long(stat_row[str_read_size])
            else:
                cur_data_size = long(stat_row[str_io_size])
            if cur_data_size/nprocs > avgsize:
                app_dict[stat_row["AppName"]] = (cur_data_size/nprocs, stat_row["FileName"], stat_row["nprocs"], long(stat_row[str_write_size])/long(stat_row["nprocs"]), long(stat_row[str_read_size])/long(stat_row["nprocs"]))
        else:
            if io_type == "ALL":
                data_size = long(stat_row[str_write_size]) + long(stat_row[str_read_size])
            else:
                data_size = long(stat_row[str_io_size])
            app_dict[stat_row["AppName"]] = (data_size/nprocs, stat_row["FileName"], stat_row["nprocs"], long(stat_row[str_write_size])/long(stat_row["nprocs"]), long(stat_row[str_read_size])/long(stat_row["nprocs"]))

    for key,value in app_dict.iteritems():
        app_list.append((key, value[0], value[1], value[2], value[3], value[4]))

    sorted_app_list = sorted(app_list, key = lambda x:x[1], reverse=True)
#    sorted_app_list = sorted(app_dict.items(), key = operator.itemgetter(1)[0], reverse = True)
    return sorted_app_list

#def app_ind_avgsize_order(stat_table, io_type):
#    app_dict = {}
#
#    if io_type == "READ":
#        str_io_size = "total_POSIX_BYTES_READ"
#    if io_type == "WRITE":
#        str_io_size = "total_POSIX_BYTES_WRITTEN"
#
#    for stat_row in stat_table:
#        app_dict[stat_row["FileName"]] = long(stat_row[str_io_size])/long(stat_row["nprocs"])
#    sorted_app_list = sorted(app_dict.items(), key = operator.itemgetter(1), reverse = True)
##    for elem in sorted_app_list:
##        print "#####key:%s, value:%ld\n"%(elem[0], elem[1])
#    return sorted_app_list

def app_ind_size_order(stat_table, io_type):
    app_cnt = 0
    app_dict = {}
    app_list = []

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
        other_io_size = "total_POSIX_BYTES_WRITTEN"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
        other_io_size = "total_POSIX_BYTES_READ"

    str_write_size = "total_POSIX_BYTES_WRITTEN"
    str_read_size = "total_POSIX_BYTES_READ"

    for stat_row in stat_table:
        if app_dict.get(stat_row["AppName"], -1) != -1:
            (data_size, fname, nprocs, write_size, read_size) = app_dict[stat_row["AppName"]]
            if io_type == "ALL":
                cur_data_size = long(stat_row[str_write_size]) + long(stat_row[str_read_size])
            else:
                cur_data_size = long(stat_row[str_io_size])
            if cur_data_size > data_size:
                app_dict[stat_row["AppName"]] = (cur_data_size, stat_row["FileName"], stat_row["nprocs"], stat_row[str_write_size], stat_row[str_read_size])
        else:
            if io_type == "ALL":
                data_size = long(stat_row[str_write_size]) + long(stat_row[str_read_size])
            else:
                data_size = long(stat_row[str_io_size])
            app_dict[stat_row["AppName"]] = (data_size, stat_row["FileName"], stat_row["nprocs"], stat_row[str_write_size], stat_row[str_read_size])

    for key,value in app_dict.iteritems():
        app_list.append((key, value[0], value[1], value[2], value[3], value[4]))

    sorted_app_list = sorted(app_list, key = lambda x:x[1], reverse=True)
#    sorted_app_list = sorted(app_dict.items(), key = operator.itemgetter(1)[0], reverse = True)
    return sorted_app_list

def filterMeaningfulJob(stat_table, min_data_size, min_proc_cnt):
    small_stat_table = []
    tot_cnt = 0
    long_io_cnt = 0
    for stat_row in stat_table:
        tot_cnt += 1
        read_size = long(stat_row["total_POSIX_BYTES_READ"])
        write_size = long(stat_row["total_POSIX_BYTES_WRITTEN"])
        if read_size < min_data_size and write_size < min_data_size:
            continue
        proc_cnt = long(stat_row["nprocs"])
        if proc_cnt < min_proc_cnt:
            continue
        tot_read_time = long(stat_row["tot_read_time"])
        tot_write_time = long(stat_row["tot_write_time"])
        if tot_read_time > 15 or tot_write_time > 15:
            long_io_cnt += 1
        small_stat_table.append(stat_row)
    print "min_data_size:%ld,tot_cnt:%ld, long_io_cnt:%ld, qualified:%ld\n"%(min_data_size,\
            tot_cnt, long_io_cnt, len(small_stat_table))
    return small_stat_table


def largeFileIOHist(stat_table, divide, io_type,\
        min_data_size, min_proc_cnt, min_bw, max_bw):
    ratio_lst = []
    if io_type == "READ":
        str_max_io = "max_read_time"
        str_tot_io = "tot_read_time"
    if io_type == "WRITE":
        str_max_io = "max_write_time"
        str_tot_io = "tot_write_time"
    if io_type == "READ":
        tot_size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        tot_size_key = "total_POSIX_BYTES_WRITTEN"
    for stat_row in stat_table:
        if float(stat_row[str_tot_io]) < min_bw or float(stat_row[str_tot_io]) > max_bw:
            continue
        if float(stat_row[tot_size_key]) < min_data_size:
            continue
        if float(stat_row["nprocs"] < min_proc_cnt):
            continue
        tot_io = float(stat_row[str_tot_io])
        max_io = float(stat_row[str_max_io])
        if tot_io <= 0:
            continue
        ratio = float(stat_row[str_max_io])/float(stat_row[str_tot_io])
        print "ratio is %lf\n"%ratio
        if ratio >= 1:
            ratio = 1
        ratio_lst.append(ratio)
    ratio_lst.sort()

    min_divide = min(len(ratio_lst), divide) 
#    print "min_divide is %d\n"%min_divide
    per_percent = len(ratio_lst)/min_divide
    low = 0
    high = low + per_percent 
    out_lst = [float(0)] * min_divide
    for i in range(1,min_divide):
#        print "type:%s, bw:%lf\n"%(io_type, bw_lst[low])
        out_lst[i - 1] = ratio_lst[low] 
        low = high
        high = high + per_percent
    out_lst[min_divide - 1] = ratio_lst[low] 

    return out_lst

def extract_array(vector_arr):
    out_arr = []
    for row in vector_arr:
        tmp_arr = []
        for idx in range(0, len(row[2])):
            tmp_arr.append(row[2][idx])
        out_arr.append(tmp_arr)
    return out_arr

def vectorize_file_data(stat_table, io_type, levels, min_data_size, max_data_size, min_proc_cnt, min_bw, max_bw, min_per_proc_size, is_per_factor):
    out_tuples = []
    tmp_bitmap = Bitmap(279)

    if io_type == "READ":
        file_counter = "max_read_stat"
    if io_type == "WRITE":
        file_counter = "max_write_stat"


    str_tot_posix_io = "POSIX_%sS"%io_type
    str_tot_posix_size_0_100 = "POSIX_SIZE_%s_0_100"%io_type
    str_tot_posix_size_100_1K = "POSIX_SIZE_%s_100_1K"%io_type
    str_tot_posix_size_1K_10K = "POSIX_SIZE_%s_1K_10K"%io_type
    str_tot_posix_size_10K_100K = "POSIX_SIZE_%s_10K_100K"%io_type
    str_consec_io = "POSIX_CONSEC_%sS"%io_type
    str_seq_io = "POSIX_SEQ_%sS"%io_type
    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type

    app_dict = {}

    if io_type == "READ":
        str_io_size = "POSIX_BYTES_READ"
        str_col_vol = "MPIIO_COLL_READS"
    if io_type == "WRITE":
        str_io_size = "POSIX_BYTES_WRITTEN"
        str_col_vol = "MPIIO_COLL_WRITES"

    if io_type == "READ":
        tot_size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        tot_size_key = "total_POSIX_BYTES_WRITTEN"

    str_io_time = "total_POSIX_F_%s_TIME"%io_type

    if io_type == "READ":
        size_key = "POSIX_BYTES_READ"
    if io_type == "WRITE":
        size_key = "POSIX_BYTES_WRITTEN"

    col_vol = 0
    app_cnt = 0
    app_id = -1
    job_id = 0
    for stat_row in stat_table:
        file_meta = stat_row[file_counter]
        if app_dict.get(stat_row["AppName"], -1) != -1:
            app_id = app_dict[stat_row["AppName"]]
        else:
            app_cnt = app_cnt + 1
            app_dict[stat_row["AppName"]] = app_cnt
            app_id = app_cnt

        if file_meta.get(str_col_vol, -1) != -1:
            col_vol = long(file_meta[str_col_vol])
#            print "col_vol:%ld\n"%col_vol
            if col_vol > 0:
                col_enable = 1
            else:
                col_enable = 0
        else:
            col_vol = 0
            col_enable = 0

        if file_meta.get("OST_MAP", -1) != -1:
            tmp_bitmap = file_meta.get("OST_MAP")
            ost_cnt = bitmap_counter(tmp_bitmap)
#            print "app:%s,ost_cnt is %d\n"%(stat_row["FileName"], ost_cnt)
        else:
            continue

        if ost_cnt <= 0:
#            print "app:%s, does not have lustre info\n"%stat_row["FileName"]
            continue

#        print "app:%s, does contains lustre info\n"%stat_row["FileName"]
        data_size = float(file_meta[size_key])
        base = 1073741824
        if data_size > 0 and data_size < base:
            data_size_level = 0
        if data_size >= base and data_size < base * 2:
            data_size_level = 1
        if data_size >= base * 2 and data_size < base * 4:
            data_size_level = 2
        if data_size >= base * 4 and data_size < base * 8:
            data_size_level = 3
        if data_size >= base * 8 and data_size < base * 16:
            data_size_level = 4
        if data_size >= base * 16 and data_size < base * 100:
            data_size_level = 5
        if data_size >= base * 100:
            data_size_level = 6

        nprocs = int(stat_row["nprocs"])
        if nprocs < min_proc_cnt:
#            print "procs is %d\n"%nprocs
            continue

        data_size = float(stat_row[tot_size_key])
        if data_size < min_data_size:
#            print "data size is %ld\n"%data_size
            continue
        if data_size > max_data_size:
            continue

        print "app:%s, data_size:%ld\n"%(stat_row["FileName"], data_size)
        io_start = float(stat_row[str_io_start])
        io_end = float(stat_row[str_io_end])
        io_time = io_end - io_start
        cur_bw = data_size/io_time
        if cur_bw >= max_bw or cur_bw <= min_bw:
#            print "bw:%ld"%cur_bw
            continue

#        print "nprocs:%d, data_size:%ld, cur_bw:%ld\n"%(nprocs, data_size, cur_bw)
        nprocs = int(file_meta["nprocs"])
        proc_ost_ratio = float(nprocs)/ost_cnt 

        per_proc_size = float(data_size)/nprocs
        if per_proc_size < max_per_proc_size:
            continue
#        print "nprocs:%d, ost_cnt:%d\n"%(nprocs, ost_cnt)
        small_io_cnt1= float(file_meta[str_tot_posix_size_0_100])
        small_io_cnt2 = float(file_meta[str_tot_posix_size_100_1K])
        small_io_cnt3 = float(file_meta[str_tot_posix_size_1K_10K])
        small_io_cnt4 = float(file_meta[str_tot_posix_size_10K_100K])
        tot_io_cnt =  float(file_meta[str_tot_posix_io])
        if tot_io_cnt <= 0:
            continue
        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
        small_io_percent = small_io_percent * 100
        if small_io_percent > 100:
            continue

        consec_io = float(file_meta[str_consec_io])
        consec_io_percent = consec_io / tot_io_cnt * 100
        if consec_io_percent > 100:
            continue

        seq_io = float(file_meta[str_seq_io])
        seq_io_percent = seq_io / tot_io_cnt * 100
        if seq_io_percent > 100:
            continue
        small_io_level = int(small_io_percent/(100/levels))
#        nonseq_io_level = int((1 - seq_io_percent)/25)
        nconsec_io_percent = 100 - consec_io_percent
        nonconsec_io_level = int((100 - consec_io_percent - 0.1)/(100/levels))
        if ost_cnt == 1:
            ost_level = 0
        if ost_cnt > 1 and ost_cnt < 10:
            ost_level = 1
        if ost_cnt >= 10 and ost_cnt < 100:
            ost_level = 2
        if ost_cnt > 100 and ost_cnt < 1000:
            ost_level = 3

#        print "ratio is %lf\n"%proc_ost_ratio
        if proc_ost_ratio > 0 and proc_ost_ratio <= 1:
            proc_ost_level = 0
        if proc_ost_ratio > 1 and proc_ost_ratio < 10:
            proc_ost_level = 1
        if proc_ost_ratio > 10 and proc_ost_ratio < 100:
            proc_ost_level = 2
        if proc_ost_ratio >= 100:
            proc_ost_level = 3
        record = stat_row
#        print "appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(stat_row["AppName"], stat_row["FileName"], small_io_level, nonconsec_io_level, col_enable, ost_level, proc_ost_level, nprocs, ost_cnt)
#        print "appname:%s, filename:%s, max_file_name:%s, small:%d, nconsec:%d, col:%d, ost_cnt:%d, proc_cnt:%s, cur_data_size:%s, data size:%ld\n"%(stat_row["AppName"], stat_row["FileName"], stat_row["max_write_fname"], small_io_percent, nconsec_io_percent, col_enable, ost_cnt, file_meta["nprocs"], file_meta[size_key], data_size)

#        out_tuples.append((stat_row["AppName"], stat_row["FileName"], (app_id, small_io_percent, nconsec_io_percent, col_enable, ost_cnt, proc_ost_ratio)))
        if is_per_factor == 1:
            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, job_id, col_enable, job_id, float(cur_bw)/1048576, job_id, small_io_percent, job_id, float(data_size)/1048576/1024, job_id, nconsec_io_percent, job_id, ost_cnt, job_id, proc_ost_level)))
        else:
            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, col_enable, float(cur_bw)/1048576, small_io_percent, float(data_size)/1048576/1024, nconsec_io_percent, ost_cnt, proc_ost_level)))

        job_id += 1
    sort_out_tuples = sorted(out_tuples, key = lambda x:x[2])
    for record in sort_out_tuples:
        print "###appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_cnt:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(record[0], record[1], record[2][4], record[2][6], record[2][2], record[2][7], record[2][8], nprocs, ost_cnt)
#        print "###appname:%s, filename:%s\n"%(record[0], record[1])
#        print record[2]
    return out_tuples

def vectorize_sweepline(stat_table, io_type, levels, min_data_size, max_data_size, min_proc_cnt, min_bw, max_bw, min_per_proc_size, is_per_factor):
    out_tuples = []
    tmp_bitmap = Bitmap(279)
    str_tot_posix_io = "total_POSIX_%sS"%io_type
    str_tot_posix_size_0_100 = "total_POSIX_SIZE_%s_0_100"%io_type
    str_tot_posix_size_100_1K = "total_POSIX_SIZE_%s_100_1K"%io_type
    str_tot_posix_size_1K_10K = "total_POSIX_SIZE_%s_1K_10K"%io_type
    str_tot_posix_size_10K_100K = "total_POSIX_SIZE_%s_10K_100K"%io_type
    str_consec_io = "total_POSIX_CONSEC_%sS"%io_type
    str_seq_io = "total_POSIX_SEQ_%sS"%io_type
    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type

    app_dict = {}

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
        str_col_vol = "total_MPIIO_COLL_READS"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
        str_col_vol = "total_MPIIO_COLL_WRITES"

    str_io_time = "total_POSIX_F_%s_TIME"%io_type

    if io_type == "READ":
        size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        size_key = "total_POSIX_BYTES_WRITTEN"

    if io_type == "READ":
        str_tuple = "sum_read_tuple"
    if io_type == "WRITE":
        str_tuple = "sum_write_tuple"

    col_vol = 0
    app_cnt = 0
    app_id = -1
    job_id = 0
    for stat_row in stat_table:
        app_cnt_flg = 0
#        print "str_tuple is:%s, 0th:%lf\n"%(str_tuple, stat_row[str_tuple][0][0])
#        if "gtc" in stat_row["AppName"] and io_type == "WRITE":
#            continue
        if "abinit" in stat_row["AppName"] and io_type == "WRITE":
#            print "abinit\n"
            continue
        if "ccsm" in stat_row["AppName"] and io_type == "WRITE":
            print "ccsm\n"
            continue
        if stat_row[str_tuple] == -1:
            continue
        if stat_row[str_tuple][0][3] < 0:
            continue
        if app_dict.get(stat_row["AppName"], -1) != -1:
            app_id = app_dict[stat_row["AppName"]]
        else:
            app_cnt_flg = 1

        if stat_row.get(str_col_vol, -1) != -1:
            col_vol = long(stat_row[str_col_vol])
#            print "col_vol:%ld\n"%col_vol
            if col_vol > 0:
                col_enable = 1
            else:
                col_enable = 0
        else:
            col_vol = 0
            col_enable = 0

        if stat_row.get("ost_map", -1) != -1:
            tmp_bitmap = stat_row.get("ost_map")
            ost_cnt = bitmap_counter(tmp_bitmap)
#            print "ost_cnt is %d\n"%ost_cnt

        if ost_cnt <= 0:
#            print "app:%s, does not have lustre info\n"%stat_row["FileName"]
            continue

#        print "app:%s, does contains lustre info\n"%stat_row["FileName"]
        data_size = float(stat_row[size_key])
        nprocs = int(stat_row["nprocs"])

        per_proc_size = data_size/nprocs
        if per_proc_size < min_per_proc_size:
            continue
        if nprocs < min_proc_cnt:
#            print "procs is %d\n"%nprocs
            continue

        if data_size < min_data_size:
#            print "data size is %ld\n"%data_size
            continue

        if data_size > max_data_size:
            continue
        base = 1073741824
        if data_size >= base and data_size < base * 2:
            data_size_level = 0
        if data_size >= base * 2 and data_size < base * 4:
            data_size_level = 1
        if data_size >= base * 4 and data_size < base * 8:
            data_size_level = 2
        if data_size >= base * 8 and data_size < base * 16:
            data_size_level = 3
        if data_size >= base * 16 and data_size < base * 100:
            data_size_level = 4
        if data_size >= base * 100:
            data_size_level = 5

        io_start = float(stat_row[str_io_start])
        io_end = float(stat_row[str_io_end])
#        io_time = io_end - io_start
        io_time = float(stat_row[str_io_time])/nprocs
#        cur_bw = data_size/io_time
        cur_bw = data_size/stat_row[str_tuple][1]
        if cur_bw >= max_bw or cur_bw <= min_bw:
#            print "bw:%ld"%cur_bw
            continue

#        print "nprocs:%d, data_size:%ld, cur_bw:%ld\n"%(nprocs, data_size, cur_bw)
#        proc_ost_ratio = float(nprocs)/ost_cnt 
        proc_ost_ratio = stat_row[str_tuple][0][3]
#        print "nprocs:%d, ost_cnt:%d\n"%(nprocs, ost_cnt)
        per_proc_data = data_size/nprocs;
        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100])
        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K])
        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K])
        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K])
        tot_io_cnt =  float(stat_row[str_tot_posix_io])
        if tot_io_cnt <= 0:
            continue
        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
#        small_io_percent = small_io_percent * 100
        small_io_percent = stat_row[str_tuple][0][0] * 100 
        if small_io_percent > 100:
            continue

        if stat_row.get("nnodes", -1) == -1:
            continue
        consec_io = float(stat_row[str_consec_io])
        consec_io_percent = consec_io / tot_io_cnt * 100
        if consec_io_percent > 100:
            continue

        seq_io = float(stat_row[str_seq_io])
        seq_io_percent = seq_io / tot_io_cnt * 100
        if seq_io_percent > 100:
            continue
        small_io_level = int(small_io_percent/(100/levels))
        if app_cnt_flg == 1:
            app_cnt = app_cnt + 1
            app_dict[stat_row["AppName"]] = app_cnt
            app_id = app_cnt

#        nonseq_io_level = int((1 - seq_io_percent)/25)
#        nconsec_io_percent = 100 - consec_io_percent
        nconsec_io_percent = stat_row[str_tuple][0][1] * 100
        nonconsec_io_level = int((100 - consec_io_percent - 0.1)/(100/levels))
        if ost_cnt == 1:
            ost_level = 0
        if ost_cnt > 1 and ost_cnt < 10:
            ost_level = 1
        if ost_cnt >= 10 and ost_cnt < 100:
            ost_level = 2
        if ost_cnt > 100 and ost_cnt < 1000:
            ost_level = 3

#        print "ratio is %lf\n"%proc_ost_ratio
        if proc_ost_ratio > 0 and proc_ost_ratio <= 1:
            proc_ost_level = 0
        if proc_ost_ratio > 1 and proc_ost_ratio < 10:
            proc_ost_level = 1
        if proc_ost_ratio > 10 and proc_ost_ratio < 100:
            proc_ost_level = 2
        if proc_ost_ratio >= 100:
            proc_ost_level = 3
        record = stat_row
        col_enable = stat_row[str_tuple][0][2]
        job_id += 1
#        print "appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(stat_row["AppName"], stat_row["FileName"], small_io_level, nonconsec_io_level, col_enable, ost_level, proc_ost_level, nprocs, ost_cnt)
#        print "appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(stat_row["AppName"], stat_row["FileName"], small_io_percent, nconsec_io_percent, col_enable, ost_level, proc_ost_level, nprocs, ost_cnt)

#        out_tuples.append((stat_row["AppName"], stat_row["FileName"], (app_id, small_io_percent, nconsec_io_percent, col_enable, ost_cnt, proc_ost_ratio)))
        if is_per_factor == 1:
            cur_bw = float(cur_bw)/1048576
#            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, job_id, col_enable*100, job_id, float(cur_bw)/1048576, job_id, small_io_percent, job_id, float(data_size)/1048576/1024, job_id, nconsec_io_percent, job_id, ost_cnt, job_id, proc_ost_level, proc_ost_ratio)))
#            if cur_bw > 1048576*1024:
#                cur_bw = float(cur_bw)/1048576/1024
#            else:
#                cur_bw = float(cur_bw)/1048576
#            if cur_bw > 1048576*1024:
#                cur_bw = float(cur_bw)/1048576/1024
#            else:
#                cur_bw = float(cur_bw)/1048576
            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, job_id, col_enable*100, job_id, float(cur_bw), job_id, small_io_percent, job_id, float(data_size)/1048576/1024, job_id, nconsec_io_percent, job_id, ost_cnt, job_id, proc_ost_level, proc_ost_ratio)))
#            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, col_enable*100, float(cur_bw)/1048576, small_io_percent, float(data_size)/1048576/1024, nconsec_io_percent, ost_cnt, proc_ost_level, proc_ost_ratio)))
        else:
#            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, col_enable*100, float(cur_bw)/1048576, small_io_percent, float(data_size)/1048576/1024, nconsec_io_percent, ost_cnt, proc_ost_level, proc_ost_ratio)))
            cur_bw = cur_bw/1048576
#            if cur_bw > 1048576*1024:
#                cur_bw = float(cur_bw)/1048576/1024
#            else:
#                cur_bw = float(cur_bw)/1048576
#            print "###not per factor\n"            
            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, int(stat_row["nnodes"]), col_enable*100, small_io_percent, float(data_size)/1048576/1024, nconsec_io_percent, ost_cnt, proc_ost_level, cur_bw, proc_ost_ratio)))
#            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, col_enable*100, float(cur_bw)/1048576, small_io_percent, float(data_size)/1048576/1024, nconsec_io_percent, ost_cnt, proc_ost_level, proc_ost_ratio)))

    sort_out_tuples = sorted(out_tuples, key = lambda x:(x[2][1], x[2][4], x[2][6]))
    for record in sort_out_tuples:
        print "###appname:%s, filename:%s, jobid:%d, app_id:%d, small:%d, nconsec:%d, col:%d, ost_cnt:%d, proc_ost_level:%d, proc_ost_ratio:%lf\n"%(record[0], record[1], record[2][0], record[2][1], record[2][4], record[2][6], record[2][3], record[2][7], record[2][8], record[2][10])
#        print "###appname:%s, filename:%s, jobid:%d, app_id:%d, small:%d, nconsec:%d, col:%d, ost_cnt:%d, proc_ost_level:%d, proc_ost_ratio:%lf\n"%(record[0], record[1], record[2][0], record[2][1], record[2][4], record[2][6], record[2][2], record[2][7], record[2][8], record[2][9])
#    for record in sort_out_tuples:
#        print "###appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(record[0], record[1], record[2][0], record[2][1], record[2][2], record[2][3], record[2][4], record[2][5], nprocs, ost_cnt)
#        print "###appname:%s, filename:%s\n"%(record[0], record[1])
#        print record[2]
    return out_tuples


def vectorize_data(stat_table, io_type, levels, min_data_size, max_data_size, min_proc_cnt, min_bw, max_bw, min_per_proc_size, is_per_factor):
    out_tuples = []
    tmp_bitmap = Bitmap(279)
    str_tot_posix_io = "total_POSIX_%sS"%io_type
    str_tot_posix_size_0_100 = "total_POSIX_SIZE_%s_0_100"%io_type
    str_tot_posix_size_100_1K = "total_POSIX_SIZE_%s_100_1K"%io_type
    str_tot_posix_size_1K_10K = "total_POSIX_SIZE_%s_1K_10K"%io_type
    str_tot_posix_size_10K_100K = "total_POSIX_SIZE_%s_10K_100K"%io_type
    str_consec_io = "total_POSIX_CONSEC_%sS"%io_type
    str_seq_io = "total_POSIX_SEQ_%sS"%io_type
    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type

    app_dict = {}

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
        str_col_vol = "total_MPIIO_COLL_READS"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
        str_col_vol = "total_MPIIO_COLL_WRITES"

    str_io_time = "total_POSIX_F_%s_TIME"%io_type

    if io_type == "READ":
        size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        size_key = "total_POSIX_BYTES_WRITTEN"


    col_vol = 0
    app_cnt = 0
    app_id = -1
    job_id = -1
    for stat_row in stat_table:
        if app_dict.get(stat_row["AppName"], -1) != -1:
            app_id = app_dict[stat_row["AppName"]]
        else:
            app_cnt = app_cnt + 1
            app_dict[stat_row["AppName"]] = app_cnt
            app_id = app_cnt

        if stat_row.get(str_col_vol, -1) != -1:
            col_vol = long(stat_row[str_col_vol])
#            print "col_vol:%ld\n"%col_vol
            if col_vol > 0:
                col_enable = 1
            else:
                col_enable = 0
        else:
            col_vol = 0
            col_enable = 0

        if stat_row.get("ost_map", -1) != -1:
            tmp_bitmap = stat_row.get("ost_map")
            ost_cnt = bitmap_counter(tmp_bitmap)
#            print "ost_cnt is %d\n"%ost_cnt

        if ost_cnt <= 0:
#            print "app:%s, does not have lustre info\n"%stat_row["FileName"]
            continue

#        print "app:%s, does contains lustre info\n"%stat_row["FileName"]
        data_size = float(stat_row[size_key])
        nprocs = int(stat_row["nprocs"])

        per_proc_size = data_size/nprocs
        if per_proc_size < min_per_proc_size:
            continue
        if nprocs < min_proc_cnt:
#            print "procs is %d\n"%nprocs
            continue

        if data_size < min_data_size:
#            print "data size is %ld\n"%data_size
            continue

        if data_size > max_data_size:
            continue
        base = 1073741824
        if data_size >= base and data_size < base * 2:
            data_size_level = 0
        if data_size >= base * 2 and data_size < base * 4:
            data_size_level = 1
        if data_size >= base * 4 and data_size < base * 8:
            data_size_level = 2
        if data_size >= base * 8 and data_size < base * 16:
            data_size_level = 3
        if data_size >= base * 16 and data_size < base * 100:
            data_size_level = 4
        if data_size >= base * 100:
            data_size_level = 5

        io_start = float(stat_row[str_io_start])
        io_end = float(stat_row[str_io_end])
#        io_time = io_end - io_start
        io_time = float(stat_row[str_io_time])/nprocs
        cur_bw = data_size/io_time
        if cur_bw >= max_bw or cur_bw <= min_bw:
#            print "bw:%ld"%cur_bw
            continue

#        print "nprocs:%d, data_size:%ld, cur_bw:%ld\n"%(nprocs, data_size, cur_bw)
        proc_ost_ratio = float(nprocs)/ost_cnt 
#        print "nprocs:%d, ost_cnt:%d\n"%(nprocs, ost_cnt)
        per_proc_data = data_size/nprocs;
        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100])
        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K])
        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K])
        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K])
        tot_io_cnt =  float(stat_row[str_tot_posix_io])
        if tot_io_cnt <= 0:
            continue
        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
        small_io_percent = small_io_percent * 100
        if small_io_percent > 100:
            continue

        consec_io = float(stat_row[str_consec_io])
        consec_io_percent = consec_io / tot_io_cnt * 100
        if consec_io_percent > 100:
            continue

        seq_io = float(stat_row[str_seq_io])
        seq_io_percent = seq_io / tot_io_cnt * 100
        if seq_io_percent > 100:
            continue
        small_io_level = int(small_io_percent/(100/levels))
#        nonseq_io_level = int((1 - seq_io_percent)/25)
        nconsec_io_percent = 100 - consec_io_percent
        nonconsec_io_level = int((100 - consec_io_percent - 0.1)/(100/levels))
        if ost_cnt == 1:
            ost_level = 0
        if ost_cnt > 1 and ost_cnt < 10:
            ost_level = 1
        if ost_cnt >= 10 and ost_cnt < 100:
            ost_level = 2
        if ost_cnt > 100 and ost_cnt < 1000:
            ost_level = 3

#        print "ratio is %lf\n"%proc_ost_ratio
        if proc_ost_ratio > 0 and proc_ost_ratio <= 1:
            proc_ost_level = 0
        if proc_ost_ratio > 1 and proc_ost_ratio < 10:
            proc_ost_level = 1
        if proc_ost_ratio > 10 and proc_ost_ratio < 100:
            proc_ost_level = 2
        if proc_ost_ratio >= 100:
            proc_ost_level = 3
        record = stat_row
        job_id += 1
#        print "appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(stat_row["AppName"], stat_row["FileName"], small_io_level, nonconsec_io_level, col_enable, ost_level, proc_ost_level, nprocs, ost_cnt)
#        print "appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(stat_row["AppName"], stat_row["FileName"], small_io_percent, nconsec_io_percent, col_enable, ost_level, proc_ost_level, nprocs, ost_cnt)

#        out_tuples.append((stat_row["AppName"], stat_row["FileName"], (app_id, small_io_percent, nconsec_io_percent, col_enable, ost_cnt, proc_ost_ratio)))
        if is_per_factor == 1:
            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, job_id, col_enable, job_id, float(cur_bw)/1048576, job_id, small_io_percent, job_id, float(data_size)/1048576/1024, job_id, nconsec_io_percent, job_id, ost_cnt, job_id, proc_ost_level)))
        else:
            out_tuples.append((stat_row["AppName"], stat_row["FileName"], (job_id, app_id, col_enable, float(cur_bw)/1048576, small_io_percent, float(data_size)/1048576/1024, nconsec_io_percent, ost_cnt, proc_ost_level)))

    sort_out_tuples = sorted(out_tuples, key = lambda x:(x[1], x[2][4],x[2][6]))
    for record in sort_out_tuples:
        print "###appname:%s, filename:%s, jobid:%d, app_id:%d, small:%d, nconsec:%d, col:%d, ost_cnt:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(record[0], record[1], record[2][0], record[2][1], record[2][4], record[2][6], record[2][2], record[2][7], record[2][8], nprocs, ost_cnt)
#    for record in sort_out_tuples:
#        print "###appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(record[0], record[1], record[2][0], record[2][1], record[2][2], record[2][3], record[2][4], record[2][5], nprocs, ost_cnt)
#        print "###appname:%s, filename:%s\n"%(record[0], record[1])
#        print record[2]
    return out_tuples


#def vectorize_data(stat_table, io_type, levels, min_data_size, min_proc_cnt, min_bw, max_bw):
#    out_tuples = []
#    tmp_bitmap = Bitmap(279)
#    str_tot_posix_io = "total_POSIX_%sS"%io_type
#    str_tot_posix_size_0_100 = "total_POSIX_SIZE_%s_0_100"%io_type
#    str_tot_posix_size_100_1K = "total_POSIX_SIZE_%s_100_1K"%io_type
#    str_tot_posix_size_1K_10K = "total_POSIX_SIZE_%s_1K_10K"%io_type
#    str_tot_posix_size_10K_100K = "total_POSIX_SIZE_%s_10K_100K"%io_type
#    str_consec_io = "total_POSIX_CONSEC_%sS"%io_type
#    str_seq_io = "total_POSIX_SEQ_%sS"%io_type
#    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
#    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type
#
#    if io_type == "READ":
#        str_io_size = "total_POSIX_BYTES_READ"
#        str_col_vol = "total_MPIIO_COLL_READS"
#    if io_type == "WRITE":
#        str_io_size = "total_POSIX_BYTES_WRITTEN"
#        str_col_vol = "total_MPIIO_COLL_WRITES"
#
#    str_io_time = "total_POSIX_F_%s_TIME"%io_type
#
#    if io_type == "READ":
#        size_key = "total_POSIX_BYTES_READ"
#    if io_type == "WRITE":
#        size_key = "total_POSIX_BYTES_WRITTEN"
#
#
#    col_vol = 0
#    for stat_row in stat_table:
#        if stat_row.get(str_col_vol, -1) != -1:
#            col_vol = long(stat_row[str_col_vol])
##            print "col_vol:%ld\n"%col_vol
#            if col_vol > 0:
#                col_enable = 1
#            else:
#                col_enable = 0
#        else:
#            col_vol = 0
#            col_enable = 0
#
#        if stat_row.get("ost_map", -1) != -1:
#            tmp_bitmap = stat_row.get("ost_map")
#            ost_cnt = bitmap_counter(tmp_bitmap)
##            print "ost_cnt is %d\n"%ost_cnt
#
#        if ost_cnt <= 0:
##            print "app:%s, does not have lustre info\n"%stat_row["FileName"]
#            continue
#
##        print "app:%s, does contains lustre info\n"%stat_row["FileName"]
#        data_size = float(stat_row[size_key])
#        nprocs = int(stat_row["nprocs"])
#        if nprocs < min_proc_cnt:
##            print "procs is %d\n"%nprocs
#            continue
#
#        if data_size < min_data_size:
##            print "data size is %ld\n"%data_size
#            continue
#
#        io_start = float(stat_row[str_io_start])
#        io_end = float(stat_row[str_io_end])
#        io_time = io_end - io_start
#        cur_bw = data_size/io_time
#        if cur_bw >= max_bw or cur_bw <= min_bw:
##            print "bw:%ld"%cur_bw
#            continue
#
##        print "nprocs:%d, data_size:%ld, cur_bw:%ld\n"%(nprocs, data_size, cur_bw)
#        proc_ost_ratio = float(nprocs)/ost_cnt 
##        print "nprocs:%d, ost_cnt:%d\n"%(nprocs, ost_cnt)
#        per_proc_data = data_size/nprocs;
#        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100])
#        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K])
#        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K])
#        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K])
#        tot_io_cnt =  float(stat_row[str_tot_posix_io])
#        if tot_io_cnt <= 0:
#            continue
#        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
#        small_io_percent = small_io_percent * 100
#        if small_io_percent > 100:
#            continue
#
#        consec_io = float(stat_row[str_consec_io])
#        consec_io_percent = consec_io / tot_io_cnt * 100
#        if consec_io_percent > 100:
#            continue
#
#        seq_io = float(stat_row[str_seq_io])
#        seq_io_percent = seq_io / tot_io_cnt * 100
#        if seq_io_percent > 100:
#            continue
#        small_io_level = int(small_io_percent/(100/levels))
##        nonseq_io_level = int((1 - seq_io_percent)/25)
#        nonconsec_io_level = int((100 - consec_io_percent - 0.1)/(100/levels))
#        if ost_cnt == 1:
#            ost_level = 0
#        if ost_cnt > 1 and ost_cnt < 10:
#            ost_level = 1
#        if ost_cnt >= 10 and ost_cnt < 100:
#            ost_level = 2
#        if ost_cnt > 100 and ost_cnt < 1000:
#            ost_level = 3
#
##        print "ratio is %lf\n"%proc_ost_ratio
#        if proc_ost_ratio > 0 and proc_ost_ratio <= 1:
#            proc_ost_level = 0
#        if proc_ost_ratio > 1 and proc_ost_ratio < 10:
#            proc_ost_level = 1
#        if proc_ost_ratio > 10 and proc_ost_ratio < 100:
#            proc_ost_level = 2
#        if proc_ost_ratio >= 100:
#            proc_ost_level = 3
#        record = stat_row
##        print "appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(stat_row["AppName"], stat_row["FileName"], small_io_level, nonconsec_io_level, col_enable, ost_level, proc_ost_level, nprocs, ost_cnt)
#
#        out_tuples.append((stat_row["AppName"], stat_row["FileName"], (small_io_percent, nonconsec_io_percent, col_enable, ost_cnt, proc_ost_ratio)))
#    sort_out_tuples = sorted(out_tuples, key = lambda x:x[2])
##    for record in sort_out_tuples:
##        print "###appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(record[0], record[1], record[2][0], record[2][1], record[2][2], record[2][3], record[2][4], nprocs, ost_cnt)
#    return sort_out_tuples

def vectorize(stat_table, io_type, levels, min_data_size, min_proc_cnt, min_bw, max_bw):
    out_tuples = []
    tmp_bitmap = Bitmap(279)
    str_tot_posix_io = "total_POSIX_%sS"%io_type
    str_tot_posix_size_0_100 = "total_POSIX_SIZE_%s_0_100"%io_type
    str_tot_posix_size_100_1K = "total_POSIX_SIZE_%s_100_1K"%io_type
    str_tot_posix_size_1K_10K = "total_POSIX_SIZE_%s_1K_10K"%io_type
    str_tot_posix_size_10K_100K = "total_POSIX_SIZE_%s_10K_100K"%io_type
    str_consec_io = "total_POSIX_CONSEC_%sS"%io_type
    str_seq_io = "total_POSIX_SEQ_%sS"%io_type
    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
        str_col_vol = "total_MPIIO_COLL_READS"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
        str_col_vol = "total_MPIIO_COLL_WRITES"

    str_io_time = "total_POSIX_F_%s_TIME"%io_type

    if io_type == "READ":
        size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        size_key = "total_POSIX_BYTES_WRITTEN"


    col_vol = 0
    for stat_row in stat_table:
        if stat_row.get(str_col_vol, -1) != -1:
            col_vol = long(stat_row[str_col_vol])
#            print "col_vol:%ld\n"%col_vol
            if col_vol > 0:
                col_enable = 1
            else:
                col_enable = 0
        else:
            col_vol = 0
            col_enable = 0

        if stat_row.get("ost_map", -1) != -1:
            tmp_bitmap = stat_row.get("ost_map")
            ost_cnt = bitmap_counter(tmp_bitmap)
#            print "ost_cnt is %d\n"%ost_cnt

        if ost_cnt <= 0:
#            print "app:%s, does not have lustre info\n"%stat_row["FileName"]
            continue

#        print "app:%s, does contains lustre info\n"%stat_row["FileName"]
        data_size = float(stat_row[size_key])
        nprocs = int(stat_row["nprocs"])
        if nprocs < min_proc_cnt:
#            print "procs is %d\n"%nprocs
            continue

        if data_size < min_data_size:
#            print "data size is %ld\n"%data_size
            continue

        io_start = float(stat_row[str_io_start])
        io_end = float(stat_row[str_io_end])
#        io_time = io_end - io_start
        io_time = float(stat_row[str_io_time])/nprocs
        cur_bw = data_size/io_time
        if cur_bw >= max_bw or cur_bw <= min_bw:
#            print "bw:%ld"%cur_bw
            continue

#        print "nprocs:%d, data_size:%ld, cur_bw:%ld\n"%(nprocs, data_size, cur_bw)
        proc_ost_ratio = float(nprocs)/ost_cnt 
#        print "nprocs:%d, ost_cnt:%d\n"%(nprocs, ost_cnt)
        per_proc_data = data_size/nprocs;
        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100])
        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K])
        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K])
        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K])
        tot_io_cnt =  float(stat_row[str_tot_posix_io])
        if tot_io_cnt <= 0:
            continue
        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
        small_io_percent = small_io_percent * 100
        if small_io_percent >= 100:
            small_io_percent = 99.9

        consec_io = float(stat_row[str_consec_io])
        consec_io_percent = consec_io / tot_io_cnt * 100

        seq_io = float(stat_row[str_seq_io])
        seq_io_percent = seq_io / tot_io_cnt * 100
        if seq_io_percent >= 100:
            seq_io_percent = 99.9
        small_io_level = int(small_io_percent/(100/levels))
#        nonseq_io_level = int((1 - seq_io_percent)/25)
        nonconsec_io_level = int((100 - consec_io_percent - 0.1)/(100/levels))
        if ost_cnt == 1:
            ost_level = 0
        if ost_cnt > 1 and ost_cnt < 10:
            ost_level = 1
        if ost_cnt >= 10 and ost_cnt < 100:
            ost_level = 2
        if ost_cnt > 100 and ost_cnt < 1000:
            ost_level = 3

#        print "ratio is %lf\n"%proc_ost_ratio
        if proc_ost_ratio > 0 and proc_ost_ratio <= 1:
            proc_ost_level = 0
        if proc_ost_ratio > 1 and proc_ost_ratio < 10:
            proc_ost_level = 1
        if proc_ost_ratio > 10 and proc_ost_ratio < 100:
            proc_ost_level = 2
        if proc_ost_ratio >= 100:
            proc_ost_level = 3
        record = stat_row
#        print "appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(stat_row["AppName"], stat_row["FileName"], small_io_level, nonconsec_io_level, col_enable, ost_level, proc_ost_level, nprocs, ost_cnt)

        out_tuples.append((stat_row["AppName"], stat_row["FileName"], (small_io_level, nonconsec_io_level, col_enable, ost_level, proc_ost_level)))
    sort_out_tuples = sorted(out_tuples, key = lambda x:x[2])
#    for record in sort_out_tuples:
#        print "###appname:%s, filename:%s, small:%d, nconsec:%d, col:%d, ost_level:%d, proc_ost_level:%d, proc_cnt:%d, ost_cnt:%d\n"%(record[0], record[1], record[2][0], record[2][1], record[2][2], record[2][3], record[2][4], nprocs, ost_cnt)
    return sort_out_tuples

def convert_vector_to_dict(out_tuples):
    out_dict = {}
    for stat_tuple in out_tuples:
        if out_dict.get(stat_tuple[2], -1) != -1:
            out_dict[stat_tuple[2]] = out_dict[stat_tuple[2]] + 1
        else:
            out_dict[stat_tuple[2]] = 1
    return sorted(out_dict.iteritems(), key = lambda(k,v):(k,v))

def convert_dict_to_plot(out_lst, level):
    #small_io:0-3, nonconsec:4-7, col_enable: 8-9, ost_ratio: 10-13, ost_cnt: 14-17
    y_list = []
    x_list = []
    x_ticks = []
    x_values = []

    base = [1, 5, 9, 11, 15]
    cursor = 0
    tmp_sum = 0
    for record in out_lst:
#        tmp_sum = tmp_sum + record[1]
#        x_list.append(tmp_sum)
#        x_ticks.append(record[1])
        y_list.append((base[0] + record[0][0], base[1] + record[0][1], base[2] + record[0][2], base[3] + record[0][3], base[4] + record[0][4]))
        x_list.append(int(cursor))
        x_values.append(record[1])
        tmp_sum = tmp_sum + long(record[1])
        cursor = cursor + 1

    tot_sum = tmp_sum
    tmp_sum = 0

    percent_lst = [20, 40, 60, 80, 100]
    percent_cursor = 0
    x_show_ticks = []
    x_show_ticks_text = []
    for i in range(0, len(x_values)):
        tmp_sum = tmp_sum + x_values[i]
        percent = float(tmp_sum)/tot_sum * 100
        if percent > percent_lst[percent_cursor]:
            x_show_ticks.append(i)
            x_show_ticks_text.append(str(int(percent)))
#            print "show_ticks:%d, tmp_sum:%d, tot_sum:%d, text:%s, percent_cursor:%d\n"%(x_show_ticks[i], tmp_sum, tot_sum, x_show_ticks_text[i], percent_cursor)
            percent_cursor = percent_cursor + 1

#    x_show_ticks.append(i)
#    x_show_ticks_text.append(str(percent_lst[percent_cursor + 1]))
#    print "show_ticks:%d, tmp_sum:%d, tot_sum:%d, text:%s\n"%(x_show_ticks[i], tmp_sum, tot_sum, x_show_ticks_text[i])

    y_ticks = range(1, 19)
    y_ticks_texts = ["small[0,25%)", "small[25%,50%)", "small[50%,75%)", "small[75%,100%]", "nconsec[0,25%)", "nconsec[25%,50%)", "nconsec[50%,75%)", "nconsec[75%,100%]", "col", "ncol", "ost[1]", "ost[2,10)", "ost[10,100)", "ost[100,248]", "procs/ost(0-1)", "procs/ost[1,10)", "procs/ost[10,100)", "procs/ost(>=100)"]
#    for i in range(0, len(y_list)):
#        print "%dth y_list is small:%d,nconsec:%d,col:%d,nost:%d,ost_ratio:%d value:%d\n"%(i, y_list[i][0], y_list[i][1], y_list[i][2], y_list[i][3], y_list[i][4], out_lst[i][1])
#
    return (x_list, x_values, x_show_ticks, x_show_ticks_text, y_list, y_ticks, y_ticks_texts)

def get_io_com_hist(stat_table, proc_cnt, data_size, min_divide):
    str_read_start = "total_POSIX_F_READ_START_TIMESTAMP"
    str_read_end = "total_POSIX_F_READ_END_TIMESTAMP"
    str_write_start = "total_POSIX_F_WRITE_START_TIMESTAMP"
    str_write_end = "total_POSIX_F_WRITE_END_TIMESTAMP"
    str_read_size = "total_POSIX_BYTES_READ"
    str_write_size = "total_POSIX_BYTES_WRITTEN"
    str_nprocs = "nprocs"

    io_com_ratio_lst = []
    counter = 0
    for stat_row in stat_table:
        tot_size = long(stat_row[str_read_size]) + long(stat_row[str_write_size])
        nprocs = long(stat_row[str_nprocs])
        if data_size != 0 and tot_size < data_size:
            continue
        if nprocs != 0 and nprocs < proc_cnt:
            continue
        read_start = float(stat_row[str_read_start])
        read_end = float(stat_row[str_read_end])
        read_time = read_end - read_start
#        read_time = float(stat_row["total_POSIX_F_READ_TIME"]) 

        write_start = float(stat_row[str_write_start])
        write_end = float(stat_row[str_write_end])
        write_time = write_end - write_start
#        write_time = float(stat_row["total_POSIX_F_WRITE_TIME"]) 
        
        str_meta_time = "total_POSIX_F_META_TIME"
        meta_time = float(stat_row[str_meta_time])
        tot_io_time = long(read_time + write_time + meta_time)
        tot_time = long(stat_row["end_time"]) - long(stat_row["start_time"]) + 1
#        tot_time = long(stat_row["run time"])
        if tot_io_time >= tot_time:
            counter = counter + 1
            continue
        if tot_time == 0:
            continue
        io_ratio = float(tot_io_time)/tot_time*100
        if io_ratio < 0 or io_ratio > 100:
            continue
#        print "app:%s,io_ratio:%lf, tot_io_time:%ld, tot_time:%ld, write_time:%lf, meta_time:%lf, read_time:%lf, write_start:%s, write_end:%s\n"%(stat_row["FileName"],io_ratio, tot_io_time, tot_time, write_time, meta_time, read_time, stat_row[str_write_start], stat_row[str_write_end])
        if tot_io_time > tot_time:
#            print "larger, app:%s,io_ratio:%lf"%(stat_row["FileName"],io_ratio)
            io_ratio = 1
        io_com_ratio_lst.append(io_ratio)
    io_com_ratio_lst.sort() 

    per_percent = len(io_com_ratio_lst)/min_divide
    low = 0
    high = low + per_percent 

    percent_lst = [0] * min_divide
    for i in range(1, min_divide):
#        print "seq:io_type:%s, percent:%d\n"%(io_type, seq_percent_lst[low])
        percent_lst[i - 1] = io_com_ratio_lst[low] 
        print "io_comp: %dth percent, low is %d, ratio is %d\n"%(i - 1, low, io_com_ratio_lst[low])
#        print "low is %d, value:%d, per_percent:%d\n"%(low, meta_ratio_lst[low], per_percent)
        low = high
        high = high + per_percent
    percent_lst[min_divide - 1] = io_com_ratio_lst[len(io_com_ratio_lst) - 1] 
    print "io_comp: %dth percent, low is %d, ratio is %d\n"%(min_divide - 1, len(io_com_ratio_lst) - 1, io_com_ratio_lst[len(io_com_ratio_lst) - 1])
    print "tot_io_time larger than tot_time, there are %d steps\n"%counter

    return percent_lst


def get_meta_hist(stat_table, data_size, proc_cnt, min_divide):
    str_read_start = "total_POSIX_F_READ_START_TIMESTAMP"
    str_read_end = "total_POSIX_F_READ_END_TIMESTAMP"
    str_write_start = "total_POSIX_F_WRITE_START_TIMESTAMP"
    str_write_end = "total_POSIX_F_WRITE_END_TIMESTAMP"
    str_read_size = "total_POSIX_BYTES_READ"
    str_write_size = "total_POSIX_BYTES_WRITTEN"
    str_nprocs = "nprocs"
    
    meta_ratio_lst = []
    for stat_row in stat_table:
        tot_size = long(stat_row[str_read_size]) + long(stat_row[str_write_size])
        nprocs = long(stat_row[str_nprocs])
        if data_size != 0 and tot_size < data_size:
            continue
        if nprocs != 0 and nprocs < proc_cnt:
            continue
        read_start = float(stat_row[str_read_start])
        read_end = float(stat_row[str_read_end])
        read_time = read_end - read_start
        
        write_start = float(stat_row[str_write_start])
        write_end = float(stat_row[str_write_end])
        write_time = write_end - write_start
        
        str_meta_time = "total_POSIX_F_META_TIME"
        meta_time = float(stat_row[str_meta_time])

        if meta_time + read_time + write_time == 0:
            continue
        meta_ratio = int(meta_time/(meta_time + read_time + write_time) * 100)
        if meta_ratio > 100 or meta_ratio < 0:
            continue
#        print "meta_ratio:%lf, meta_time:%lf, read_time:%lf, write_time:%lf\n"%(meta_ratio, meta_time, read_time, write_time)
        meta_ratio_lst.append(meta_ratio)
    meta_ratio_lst.sort()
    
    per_percent = len(meta_ratio_lst)/min_divide
    low = 0
    high = low + per_percent 

    percent_lst = [0] * min_divide
    for i in range(1, min_divide):
#        print "seq:io_type:%s, percent:%d\n"%(io_type, seq_percent_lst[low])
        percent_lst[i - 1] = meta_ratio_lst[low]
        print "meta_ratio: %dth percent, low is %d, ratio is %d\n"%(i - 1, low, meta_ratio_lst[low])
#        print "low is %d, value:%d, per_percent:%d\n"%(low, meta_ratio_lst[low], per_percent)
        low = high
        high = high + per_percent
    percent_lst[min_divide - 1] = meta_ratio_lst[len(meta_ratio_lst) - 1] 
    print "meta_ratio: %dth percent, low is %d, ratio is %d\n"%(min_divide - 1, len(meta_ratio_lst) - 1, meta_ratio_lst[len(meta_ratio_lst) - 1])

#    print "%dth is %ld\n"%(min_divide - 1, read_size_lst[min_divide - 1])
    return percent_lst

def get_app_hist(stat_table):
    app_cnt = 0
    out_dict = {}
    for stat_row in stat_table:
#        if stat_row["AppName"] == "gamess.01.x" or stat_row["AppName"] == "vasp":
#            print "app:%s, path:%s, proc_cnt:%s, read:%s, write:%s\n"%(stat_row["AppName"], stat_row["FileName"], stat_row["nprocs"], stat_row["total_POSIX_BYTES_READ"], stat_row["total_POSIX_BYTES_WRITTEN"])
        if out_dict.get(stat_row["AppName"], -1) != -1:
            out_dict[stat_row["AppName"]] = out_dict[stat_row["AppName"]] + 1
        else:
            app_cnt = app_cnt + 1
            out_dict[stat_row["AppName"]] = 1 
#            print "appname:%s, file path:%s, log path:%s\n"%(stat_row["AppName"], stat_row["PATH"], \
#                    stat_row["FileName"])
    sorted_dict = OrderedDict(sorted(out_dict.items(), key = lambda x:x[1]))
    for key,value in sorted_dict.items():
        print "appname:%s, count:%d\n"%(key, value)
    return sorted_dict


def get_io_pat_hist(stat_table, divide):
    out_dict = {}
    out_abnormal_list = []
    str_tot_posix_read = "total_POSIX_READS"
    str_tot_posix_write = "total_POSIX_WRITES"
    str_tot_posix_size_0_100_read = "total_POSIX_SIZE_READ_0_100"
    str_tot_posix_size_0_100_write = "total_POSIX_SIZE_WRITE_0_100"
    str_tot_posix_size_100_1K_read = "total_POSIX_SIZE_READ_100_1K"
    str_tot_posix_size_100_1K_write = "total_POSIX_SIZE_WRITE_100_1K"
    str_tot_posix_size_1K_10K_read = "total_POSIX_SIZE_READ_1K_10K"
    str_tot_posix_size_1K_10K_write = "total_POSIX_SIZE_WRITE_1K_10K"
    str_tot_posix_size_10K_100K_read = "total_POSIX_SIZE_READ_10K_100K"
    str_tot_posix_size_10K_100K_write = "total_POSIX_SIZE_WRITE_10K_100K"
    str_consec_read = "total_POSIX_CONSEC_READS"
    str_consec_write = "total_POSIX_CONSEC_WRITES"
    str_seq_read = "total_POSIX_SEQ_READS"
    str_seq_write = "total_POSIX_SEQ_WRITES"
    str_read_start = "total_POSIX_F_READ_START_TIMESTAMP"
    str_write_start = "total_POSIX_F_WRITE_START_TIMESTAMP"
    str_read_end = "total_POSIX_F_READ_END_TIMESTAMP"
    str_write_end = "total_POSIX_F_WRITE_END_TIMESTAMP"

    str_read_size = "total_POSIX_BYTES_READ"
    str_write_size = "total_POSIX_BYTES_WRITTEN"

    str_read_time = "total_POSIX_F_READ_TIME"
    str_write_time = "total_POSIX_F_WRITE_TIME"
    
    read_size_key = "total_POSIX_BYTES_READ"
    write_size_key = "total_POSIX_BYTES_WRITTEN"

    small_percent_lst = []
    consec_percent_lst = []
    seq_percent_lst = []
    small_io_percent_lst = []
    for stat_row in stat_table:
        tmp_map = {}
        tot_io_cnt =  float(stat_row[str_tot_posix_read]) + float(stat_row[str_tot_posix_write])
        io_size = float(stat_row[read_size_key]) + float(stat_row[write_size_key])
#        if (io_size < min_io_size):
#            continue;
        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100_read]) + float(stat_row[str_tot_posix_size_0_100_write])
        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K_read]) + float(stat_row[str_tot_posix_size_100_1K_write])
        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K_read]) + float(stat_row[str_tot_posix_size_1K_10K_write])
        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K_read]) + float(stat_row[str_tot_posix_size_10K_100K_write])
        if tot_io_cnt <= 0:
            continue
        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
        small_io_percent = small_io_percent * 100
        consec_io = float(stat_row[str_consec_read]) + float(stat_row[str_consec_write])
        consec_io_percent = consec_io / tot_io_cnt * 100
#        print "app:%s, type:%s,consec_io_percent:%d\n"%(stat_row["FileName"], io_type, consec_io_percent)
        seq_io = float(stat_row[str_seq_read]) + float(stat_row[str_seq_write])
        seq_io_percent = seq_io / tot_io_cnt * 100
        if seq_io_percent > 100 or consec_io_percent > 100 or small_io_percent > 100:
            print "appname:%s, seq_io_percent:%lf, consec:%lf, small:%lf"%(stat_row["FileName"], seq_io_percent, consec_io_percent, small_io_percent)
            out_abnormal_list.append((stat_row["FileName"], stat_row["AppName"], seq_io_percent, consec_io_percent, small_io_percent, stat_row["start_time"], stat_row["end_time"]))
            continue
        seq_percent_lst.append(seq_io_percent)
        consec_percent_lst.append(consec_io_percent)
        small_io_percent_lst.append(small_io_percent)
   
    seq_percent_lst.sort()
    small_io_percent_lst.sort()
    consec_percent_lst.sort()

    min_divide = min(len(seq_percent_lst), divide) 
    out_seq_percent_lst = [0] * min_divide

    per_percent = len(seq_percent_lst)/min_divide
    low = 0
    high = low + per_percent 
    for i in range(1, min_divide):
#        print "seq:io_type:%s, percent:%d\n"%(io_type, seq_percent_lst[low])
        out_seq_percent_lst[i - 1] = seq_percent_lst[low] 
        low = high
        high = high + per_percent
    out_seq_percent_lst[min_divide - 1] = seq_percent_lst[len(seq_percent_lst) - 1] 
#    print "seq:io_type:%s, percent:%d\n"%(io_type, seq_percent_lst[low])

    min_divide = min(len(small_io_percent_lst), divide) 
    out_small_io_percent_lst = [0] * min_divide

    per_percent = len(small_io_percent_lst)/min_divide
    low = 0
    high = low + per_percent 
    for i in range(1, min_divide):
        out_small_io_percent_lst[i - 1] = small_io_percent_lst[low] 
        low = high
        high = high + per_percent
    out_small_io_percent_lst[min_divide - 1] = small_io_percent_lst[len(small_io_percent_lst) - 1] 

    min_divide = min(len(consec_percent_lst), divide) 
    out_consec_percent_lst = [0] * min_divide

    per_percent = len(consec_percent_lst)/min_divide
    low = 0
    high = low + per_percent 
    for i in range(1, min_divide):
        out_consec_percent_lst[i - 1] = consec_percent_lst[low] 
        low = high
        high = high + per_percent
    out_consec_percent_lst[min_divide - 1] = consec_percent_lst[len(consec_percent_lst) - 1] 
    out_dict["seq"] = out_seq_percent_lst
    out_dict["consec"] = out_consec_percent_lst
    out_dict["small_io"] = out_small_io_percent_lst

    cursor = 0
    for info in out_dict["seq"]:
#        print "inner %dth ratio is %lf\n"%(cursor, info)
        cursor = cursor + 1
    return (out_dict, out_abnormal_list)

def get_fs_pattern_hist(stat_table, io_type, divide):
    out_dict = {}
    out_abnormal_list = []
    str_tot_posix_io = "total_POSIX_%sS"%io_type
    str_tot_posix_size_0_100 = "total_POSIX_SIZE_%s_0_100"%io_type
    str_tot_posix_size_100_1K = "total_POSIX_SIZE_%s_100_1K"%io_type
    str_tot_posix_size_1K_10K = "total_POSIX_SIZE_%s_1K_10K"%io_type
    str_tot_posix_size_10K_100K = "total_POSIX_SIZE_%s_10K_100K"%io_type
    str_consec_io = "total_POSIX_CONSEC_%sS"%io_type
    str_seq_io = "total_POSIX_SEQ_%sS"%io_type
    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"

    str_io_time = "total_POSIX_F_%s_TIME"%io_type

    if io_type == "READ":
        size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        size_key = "total_POSIX_BYTES_WRITTEN"

    small_percent_lst = []
    consec_percent_lst = []
    seq_percent_lst = []
    small_io_percent_lst = []
    for stat_row in stat_table:
#        print "name:%s, path:%s, fs:%s\n"%(stat_row["FileName"], stat_row["PATH"], stat_row["FS"])
        tmp_map = {}
        tot_io_cnt =  float(stat_row[str_tot_posix_io])
        io_size = float(stat_row[size_key])
#        if (io_size < min_io_size):
#            continue;
        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100])
        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K])
        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K])
        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K])
        if tot_io_cnt <= 0:
            continue
        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
        small_io_percent = small_io_percent * 100
        consec_io = float(stat_row[str_consec_io])
        consec_io_percent = consec_io / tot_io_cnt * 100
#        print "app:%s, type:%s,consec_io_percent:%d\n"%(stat_row["FileName"], io_type, consec_io_percent)
        seq_io = float(stat_row[str_seq_io])
        seq_io_percent = seq_io / tot_io_cnt * 100
        if seq_io_percent > 100 or consec_io_percent > 100 or small_io_percent > 100:
            print "appname:%s, seq_io_percent:%lf, consec:%lf, small:%lf"%(stat_row["FileName"], seq_io_percent, consec_io_percent, small_io_percent)
            out_abnormal_list.append((stat_row["FileName"], stat_row["AppName"], seq_io_percent, consec_io_percent, small_io_percent, stat_row["start_time"], stat_row["end_time"]))
            continue
        seq_percent_lst.append(seq_io_percent)
        consec_percent_lst.append(consec_io_percent)
        small_io_percent_lst.append(small_io_percent)
   
    seq_percent_lst.sort()
    small_io_percent_lst.sort()
    consec_percent_lst.sort()

    min_divide = min(len(seq_percent_lst), divide) 
    out_seq_percent_lst = [0] * min_divide

    per_percent = len(seq_percent_lst)/min_divide
    low = 0
    high = low + per_percent 
    for i in range(1, min_divide):
#        print "seq:io_type:%s, percent:%d\n"%(io_type, seq_percent_lst[low])
        out_seq_percent_lst[i - 1] = seq_percent_lst[low] 
        low = high
        high = high + per_percent
    out_seq_percent_lst[min_divide - 1] = seq_percent_lst[len(seq_percent_lst) - 1] 
#    print "seq:io_type:%s, percent:%d\n"%(io_type, seq_percent_lst[low])

    min_divide = min(len(small_io_percent_lst), divide) 
    out_small_io_percent_lst = [0] * min_divide

    per_percent = len(small_io_percent_lst)/min_divide
    low = 0
    high = low + per_percent 
    for i in range(1, min_divide):
        out_small_io_percent_lst[i - 1] = small_io_percent_lst[low] 
        low = high
        high = high + per_percent
    out_small_io_percent_lst[min_divide - 1] = small_io_percent_lst[len(small_io_percent_lst) - 1] 

    min_divide = min(len(consec_percent_lst), divide) 
    out_consec_percent_lst = [0] * min_divide

    per_percent = len(consec_percent_lst)/min_divide
    low = 0
    high = low + per_percent 
    for i in range(1, min_divide):
        out_consec_percent_lst[i - 1] = consec_percent_lst[low] 
        low = high
        high = high + per_percent
    out_consec_percent_lst[min_divide - 1] = consec_percent_lst[len(consec_percent_lst) - 1] 
    out_dict["seq"] = out_seq_percent_lst
    out_dict["consec"] = out_consec_percent_lst
    out_dict["small_io"] = out_small_io_percent_lst

    cursor = 0
    for info in out_dict["seq"]:
#        print "inner %dth ratio is %lf\n"%(cursor, info)
        cursor = cursor + 1
    return (out_dict, out_abnormal_list)
#
def get_fs_hist(stat_table, io_type, proc_cnt, data_size):
    out_dict = {}
    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
    for stat_row in stat_table:
        nprocs = int(stat_row["nprocs"])
        if proc_cnt != 0:
            if nprocs < proc_cnt:
                continue
        cur_data_size = long(stat_row[str_io_size])
        if cur_data_size != 0:
            if cur_data_size < data_size:
                continue
        if "cray/dws/mounts" in stat_row["PATH"]:
            print "datawarp, app:%s, path:%s, fname:%s\n"%(stat_row["AppName"], stat_row["PATH"], stat_row["FileName"])
            stat_row["FS"] = "DataWarp"
        if out_dict.get(stat_row["FS"], -1) != -1:
            out_dict[stat_row["FS"]] = out_dict[stat_row["FS"]] + 1
        else:
            out_dict[stat_row["FS"]] = 1

    for key,value in out_dict.iteritems():
        print "fs:key:%s, value:%d, total:%d\n"%(key, value, len(out_dict))
    return out_dict
#
def getBWHist(stat_table, divide, io_type):
    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type
    str_io_time = "total_POSIX_F_%s_TIME"%io_type
    bw_lst = [];
    size_lst = [];
    proc_cnt_lst = [];

    if io_type == "READ":
        str_io_size = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        str_io_size = "total_POSIX_BYTES_WRITTEN"
    for stat_row in stat_table:
       #transform read start, read end
       io_start = float(stat_row[str_io_start])
       io_end = float(stat_row[str_io_end])
       io_time = io_end - io_start
#       io_time = float(stat_row[str_io_time])
       if io_time == 0:
           continue
       data_size = float(stat_row[str_io_size])
       if data_size == 0:
           continue
       proc_cnt = float(stat_row["nprocs"])
       if proc_cnt < 1000:
           continue
       bw_lst.append(data_size/io_time/1048576)
    bw_lst.sort()

    min_divide = min(len(bw_lst), divide) 
#    print "min_divide is %d\n"%min_divide
    per_percent = len(bw_lst)/min_divide
    low = 0
    high = low + per_percent 
    out_lst = [float(0)] * min_divide
    for i in range(1,min_divide):
#        print "type:%s, bw:%lf\n"%(io_type, bw_lst[low])
        out_lst[i - 1] = bw_lst[low] 
        low = high
        high = high + per_percent
    out_lst[min_divide - 1] = bw_lst[len(bw_lst) - 1] 

    return out_lst
#
def getProcCntHist(stat_table, divide):
    proc_cnt_lst = []

    for record in stat_table:
        proc_cnt_lst.append(int(record["nprocs"]))
    proc_cnt_lst.sort()
    
    min_divide = min(len(proc_cnt_lst), divide) 

    per_percent = len(proc_cnt_lst)/min_divide
    low = 0
    high = low + per_percent 
    out_lst = [long(0)] * min_divide
    for i in range(1, min_divide):
#        print "proc count:%d\n"%proc_cnt_lst[low]
        out_lst[i - 1] = proc_cnt_lst[low] 
        low = high
        high = high + per_percent
    out_lst[min_divide - 1] = proc_cnt_lst[len(proc_cnt_lst) - 1]
    return out_lst

def getDataSizeHist(stat_table, divide, direction):
    read_size_lst = []

    posix_key = "total_POSIX_BYTES_"
    stdio_key = "total_STDIO_BYTES_"
    if direction == "WRITE":
        posix_key = posix_key + "WRITTEN"
        stdio_key = stdio_key + "WRITTEN"
    if direction == "READ":
        posix_key = posix_key + "READ"
        stdio_key = stdio_key + "READ"
    
    tot_posix_read = 0
    tot_stdio_read = 0
    for record in stat_table:
        if record.get(posix_key, -1) != -1:
            tot_posix_read = long(record[posix_key])
        if record.get(stdio_key, -1) != -1:
            tot_stdio_read = long(record[stdio_key])
        tot_read = tot_posix_read + tot_stdio_read
        if tot_read == 0:
            continue
        read_size_lst.append(float(tot_read)/1048576)
#        print "app:%s, size is %ld\n"%(record["AppName"], tot_read/1048576)
    read_size_lst.sort()

    min_divide = min(len(read_size_lst), divide) 
    per_percent = len(read_size_lst)/min_divide
    low = 0
    high = low + per_percent
    out_lst = [long(0)] * min_divide
    for i in range(1, min_divide):
#        print "data size:%ld\n"%read_size_lst[low]
        out_lst[i - 1] = read_size_lst[low] 
#        print "%dth is %ld\n"%(i - 1, read_size_lst[low])
        low = high
        high = high + per_percent
    out_lst[min_divide - 1] = read_size_lst[len(read_size_lst) - 1] 
#    print "%dth is %ld\n"%(min_divide - 1, read_size_lst[min_divide - 1])
    return out_lst

def getProcFileRatio(statTable, divide):
    per_proc_files_list = []
    n_1_cnt = 0
    n_n_cnt = 0
    n_plus_n_cnt = 0
    for record in statTable:
        nprocs = int(record["nprocs"])
        if int(nprocs) == 1:
            continue
        stdio_cnt = int(record["stdio_cnt"])
        posix_cnt = int(record["posix_cnt"])
        if posix_cnt == 0:
            continue

        mpiio_cnt = int(record["mpiio_cnt"])
#        print "appname:%s,path:%s,proc_cnt:%d, file_cnt:%d\n"%(record["AppName"], record["FileName"], nprocs, posix_cnt)
        if posix_cnt == 1:
            n_1_cnt = n_1_cnt + 1
            continue
        else:
            if posix_cnt > nprocs:
                n_plus_n_cnt = n_plus_n_cnt + 1
                continue
            else:
                if posix_cnt == nprocs:
                    n_n_cnt = n_n_cnt + 1
                    continue
                else:
                    tot_file_cnt = posix_cnt
                    per_proc_files = float(tot_file_cnt)/nprocs
#                    print "total_file_cnt:%d, nprocs:%d, per_proc_files:%f\n"%(tot_file_cnt, nprocs, per_proc_files)
                    per_proc_files_list.append(per_proc_files)
    per_proc_files_list.sort()
    percent_lst =   procFileRatioStat(per_proc_files_list, divide)
#    out_dict = [("(N=>1)",n_1_cnt), ("(N=>0.2N-)",percent_lst[0]), ("(N=>0.4N-)",percent_lst[1]), ("(N=>0.6N-)",percent_lst[2]), ("(N=>0.8N-)",percent_lst[3]), ("(N=>0.8N+)",percent_lst[4]), ("(N=>N)",n_n_cnt), ("(N=>N+)",n_plus_n_cnt)]
    out_dict = [("N-1",n_1_cnt), ("N-M (0<M<=0.2N)",percent_lst[0]), ("N-M (0.2N<M<=0.4N)",percent_lst[1]), ("N-M (0.4N<M<=0.6N)",percent_lst[2]), ("N-M (0.6N<M<=0.8N)",percent_lst[3]), ("N-M (0.8N<M<N)",percent_lst[4]), ("N=>N",n_n_cnt), ("N=>M (M>N)",n_plus_n_cnt)]
#    cursor = 0
    ret_dict = collections.OrderedDict(out_dict)
    for tmp_key in ret_dict.keys():
        print "shared_ind:key:%s, value:%d\n"%(tmp_key, ret_dict[tmp_key])
#        if cursor > 2:
#            out_dict[tmp_key] = percent_lst[cursor - 3]
#        cursor = cursor + 1

    return ret_dict

def ioTypeBin(stat_table):
    out_dict = {}
    # first:posix, second:collective third: independent fourth:others
    for record in stat_table:
        proc_cnt = int(record["nprocs"])
        if proc_cnt == 1:
            if out_dict.get("one", -1) == -1:
                out_dict["one"] = (0, 0, 0, 0)
            tmp_tuple = out_dict["one"]
            tag = "one"
            
        if proc_cnt > 1 and proc_cnt < 1024:
            if out_dict.get("small", -1) == -1:
                out_dict["small"] = (0, 0, 0, 0)
            tmp_tuple = out_dict["small"]
            tag = "small"

        if proc_cnt >= 1024 and proc_cnt < 8192:
            if out_dict.get("medium", -1) == -1:
                out_dict["medium"] = (0, 0, 0, 0)
            tmp_tuple = out_dict["medium"]
            tag = "medium"

        if proc_cnt >= 8192:
            if out_dict.get("large", -1) == -1:
                out_dict["large"] = (0, 0, 0, 0)
            tmp_tuple = out_dict["large"]
            tag = "large"

        col_reads = False
        col_writes = False
        if record.get("total_MPIIO_COLL_READS", -1) != -1 and int(record["total_MPIIO_COLL_READS"]) > 0:
            col_reads = True
        if record.get("total_MPIIO_COLL_WRITES", -1) != -1 and int(record["total_MPIIO_COLL_WRITES"]) > 0:
            col_writes = True
     
        cur_pos_cnt = tmp_tuple[0]
        cur_col_cnt = tmp_tuple[1]
        cur_ind_cnt = tmp_tuple[2]
        cur_other_cnt = tmp_tuple[3]
        
        stdio_cnt = int(record["stdio_cnt"])
        posix_cnt = int(record["posix_cnt"])
        mpiio_cnt = int(record["mpiio_cnt"])
        if col_reads or col_writes:
            cur_col_cnt += 1
        else:
            if mpiio_cnt > 0:
                cur_ind_cnt += 1
            else:
                if posix_cnt > 0:
                    cur_pos_cnt += 1
                else:
                    cur_other_cnt += 1
        out_dict[tag] = (cur_pos_cnt, cur_col_cnt, cur_ind_cnt, cur_other_cnt)
        print "tags:%s, posix:%d, col:%d, ind_col:%d, other:%d\n"%(tag, out_dict[tag][0], out_dict[tag][1], out_dict[tag][2], out_dict[tag][3])
    return out_dict


def ioTypeRatio(stat_table):
    pure_posix = 0
    pure_mpiio_ind = 0
    pure_stdio = 0
    pure_mpiio_col = 0
    pure_mpiio = 0
    posix_stdio = 0
    mpiio_stdio = 0
    mpiio_col_stdio = 0
    for record in stat_table:
        proc_cnt = float(record["nprocs"])
#        if proc_cnt < 1000:
#            continue
        col_reads = False
        col_writes = False
        if record.get("total_MPIIO_COLL_READS", -1) != -1 and int(record["total_MPIIO_COLL_READS"]) > 0:
            col_reads = True
        if record.get("total_MPIIO_COLL_WRITES", -1) != -1 and int(record["total_MPIIO_COLL_WRITES"]) > 0:
            col_writes = True
        stdio_cnt = int(record["stdio_cnt"])
        posix_cnt = int(record["posix_cnt"])
        mpiio_cnt = int(record["mpiio_cnt"])
        if col_reads or col_writes:
            pure_mpiio_col = pure_mpiio_col + 1
        if mpiio_cnt > 0:
            pure_mpiio = pure_mpiio + 1
        if mpiio_cnt == 0 and posix_cnt > 0:
            pure_posix = pure_posix + 1
        if posix_cnt == 0 and stdio_cnt > 0:
            pure_stdio = pure_stdio + 1
        if mpiio_cnt > 0 and stdio_cnt > 0:
            mpiio_stdio = mpiio_stdio + 1
            if col_reads or col_writes:
                mpiio_col_stdio = mpiio_col_stdio + 1
        if mpiio_cnt == 0 and posix_cnt > 0 and stdio_cnt > 0:
            posix_stdio = posix_stdio + 1
#        print "app:%s, path:%s, pos:%d, mpiio:%d, stdio:%d\n"%(record["AppName"], record["FileName"], posix_cnt, mpiio_cnt, stdio_cnt)
#        print "app:%s, pos:%d, pos_std:%d, mpi:%d, mpi_col:%d, mpi_std:%d, mpicol_std:%d\n"%(record["AppName"], posix_cnt, posix_stdio, mpiio_cnt, pure_mpiio_col, mpiio_stdio, mpiio_col_stdio)
    out_dict = {}
    out_dict["pos"] = pure_posix
    out_dict["mpicol"] = pure_mpiio_col
    out_dict["mpiind"] = pure_mpiio - pure_mpiio_col
    out_dict["std"] = pure_stdio
    out_dict["pos_std"] = posix_stdio
    out_dict["mpicol_std"] = mpiio_col_stdio
    out_dict["mpiind_std"] = mpiio_stdio - mpiio_col_stdio

    for key,value in out_dict.iteritems():
        print "io_type, key:%s, value:%d\n"%(key, value)
    return out_dict

def read_write_ratio(stat_table, divide):
    tot_posix_read = 0
    tot_posix_write = 0
    sum_read = 0
    sum_write = 0
    tot_stdio_read = 0
    tot_stdio_write = 0
    write_ratio_lst = [0] * (divide)
    for record in stat_table:
        if record.get("total_POSIX_BYTES_READ", -1) != -1:
            tot_posix_read = long(record["total_POSIX_BYTES_READ"])
        if record.get("total_POSIX_BYTES_WRITTEN", -1) != -1:
            tot_posix_write = long(record["total_POSIX_BYTES_WRITTEN"])
        if record.get("total_STDIO_BYTES_READ", -1) != -1:
            tot_stdio_read = long(record["total_STDIO_BYTES_READ"])
        if record.get("total_STDIO_BYTES_WRITTEN", -1) != -1:
            tot_stdio_write = long(record["total_STDIO_BYTES_WRITTEN"])

        read_size = tot_posix_read + tot_stdio_read
        write_size = tot_posix_write + tot_stdio_write
        sum_read = sum_read + read_size
        sum_write = sum_write + write_size

        if write_size + read_size == 0:
            continue
        ratio = float(read_size)/(write_size + read_size)
        index = int(ratio * divide)
        if index == divide:
            index = index - 1
        write_ratio_lst[index] = write_ratio_lst[index] + 1
#        print "%dth, app:%s, posix_read:%ld, posix_write:%ld, stdio_read:%ld, stdio_write:%ld, read_size:%ld, write_size:%ld, sum_read:%ld, sum_write:%ld, ratio:%lf\n"%(index, record["AppName"], tot_posix_read, tot_posix_write, \
#                tot_stdio_read, tot_stdio_write, read_size, write_size, sum_read, sum_write, ratio)
    print "total write ratio is %f\n"%(float(sum_write)/(sum_write + sum_read))
    return write_ratio_lst
#
#
def procFileRatioStat(input_list, divide):
    input_cnt = len(input_list)
    out_dict = []
    per_divide = input_cnt/divide
    percent_lst = [0]*divide

#    print "length is %d, divide is %d, %f\n"%(len(input_list), divide, input_list[0])
    for ratio in input_list:
        index = int(ratio * divide)
#        print "index is %d\n"%index
        percent_lst[index] = percent_lst[index] + 1
    return percent_lst
#
#
#
#
