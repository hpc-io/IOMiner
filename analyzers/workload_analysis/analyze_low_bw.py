import pickle
import string
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
import json
# vertical line: bandwidth, file system type, horizontal line: percent of 
# small reads
plot_dir="/global/cscratch1/sd/tengwang/miner0612/plots/"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner0612/miner_para.conf'))
min_io_size = float(miner_param["format"]["min_write_size"])


def reformat_low_factor_table(stat_table, io_type):
    str_tot_posix_io = "total_POSIX_%sS"%io_type
    str_tot_posix_size_0_100 = "total_POSIX_SIZE_%s_0_100"%io_type
    str_tot_posix_size_100_1K = "total_POSIX_SIZE_%s_100_1K"%io_type
    str_tot_posix_size_1K_10K = "total_POSIX_SIZE_%s_1K_10K"%io_type
    str_tot_posix_size_10K_100K = "total_POSIX_SIZE_%s_10K_100K"%io_type
    str_consec_io = "total_POSIX_CONSEC_%sS"%io_type
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

    small_io_map = []
    result_table = []
    for stat_row in stat_table:
#        print "name:%s, path:%s, fs:%s\n"%(stat_row["FileName"], stat_row["PATH"], stat_row["FS"])
        tmp_map = {}
        tot_io_cnt =  float(stat_row[str_tot_posix_io])
        io_size = float(stat_row[size_key])
        if (io_size < min_io_size):
            continue;
        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100])
        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K])
        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K])
        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K])
        if tot_io_cnt < 0:
            continue
        small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
        small_io_percent = small_io_percent * 100
        consec_io = float(stat_row[str_consec_io])
        consec_io_percent = consec_io / tot_io_cnt * 100
        str_app_name = stat_row["AppName"]
        data_size = float(stat_row[str_io_size])
        if data_size < min_io_size:
            continue
        proc_cnt = float(stat_row["nprocs"]) 
        io_start = float(stat_row[str_io_start])
        io_end = float(stat_row[str_io_end])
        io_time = io_end - io_start
        bw = data_size/io_time
        tmp_map["bandwidth"] = bw
        tmp_map["AppName"] = str_app_name
        tmp_map["datasize"] = data_size
        tmp_map["small_io_percent"] = small_io_percent
        tmp_map["consec_io_percent"] = consec_io_percent
        tmp_map["proc_cnt"] = proc_cnt
        tmp_map["per_proc_size"] = float(data_size)/proc_cnt
        tmp_map["PATH"] = stat_row["PATH"]
        tmp_map["FileName"] = stat_row["FileName"]
        result_table.append(tmp_map.copy())
    return result_table 
#    for row in result_table:
#        print "bandwidth:%f, app_name:%s, data_size:%ld, small_io_percent:%d, consec_io_percent:%d, proc_cnt:%d\n"%(float(row["bandwidth"]), row["AppName"], int(row["datasize"]), int(row["small_io_percent"]), int(row["consec_io_percent"]), int(row["proc_cnt"]))
def plot_small_io_hist(stat_table,io_type, fs, plot_dir):
    plt.gcf().clear()
    x = []
    str_tot_posix_io = "total_POSIX_%sS"%io_type
    str_tot_posix_size_0_100 = "total_POSIX_SIZE_%s_0_100"%io_type
    str_tot_posix_size_100_1K = "total_POSIX_SIZE_%s_100_1K"%io_type
    str_tot_posix_size_1K_10K = "total_POSIX_SIZE_%s_1K_10K"%io_type
    str_tot_posix_size_10K_100K = "total_POSIX_SIZE_%s_10K_100K"%io_type

    if io_type == "READ":
        size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        size_key = "total_POSIX_BYTES_WRITTEN"

    small_io_map = []
    for stat_row in stat_table:
        tot_io_cnt =  float(stat_row[str_tot_posix_io])
        io_size = float(stat_row[size_key])
        if (io_size < min_io_size):
            continue;

        small_io_cnt1= float(stat_row[str_tot_posix_size_0_100])
        small_io_cnt2 = float(stat_row[str_tot_posix_size_100_1K])
        small_io_cnt3 = float(stat_row[str_tot_posix_size_1K_10K])
        small_io_cnt4 = float(stat_row[str_tot_posix_size_10K_100K])
        if tot_io_cnt > 0:
                small_io_percent = (small_io_cnt1 + small_io_cnt2 + small_io_cnt3 + small_io_cnt4)/tot_io_cnt;
                small_io_percent = small_io_percent * 100
#                print "percent:%lf, name:%s\n"%(small_io_percent, stat_row["PATH"])
                x.append(int(small_io_percent))
#                str_app_name = stat_row["AppName"]
#                small_io_map.append((str_app_name, small_io_percent))
        else:
            stat_row["small_io_percent"] = int(-1)

    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    weights = np.ones_like(x)/float(len(x))*100
    plt.hist(x, bins, range = (0, 100), histtype='bar', weights = weights)
    title = plot_dir + "small_hist_%s_%s"%(io_type, fs)
    plt.xlabel("percent of small IO")
    plt.ylabel("percent of jobs")
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
    return small_io_map

def get_table_by_range(stat_table, low, high, key):
    newlist = sorted(stat_table, key = lambda x: x[key])
    ret_list = []
    for i in range(low, high):
        ret_list.append(newlist[i])
    return ret_list

def cal_num_apps(stat_table):
    app_cnt = 0
    out_dict = {}
    for stat_row in stat_table:
        if out_dict.get(stat_row["AppName"], -1) != -1:
            out_dict[stat_row["AppName"]] = out_dict[stat_row["AppName"]] + 1
        else:
            app_cnt = app_cnt + 1
            out_dict[stat_row["AppName"]] = 1 
            print "appname:%s, file path:%s, log path:%s\n"%(stat_row["AppName"], stat_row["PATH"], \
                    stat_row["FileName"])
    return app_cnt


def plot_nonconsec_io_hist(stat_table, io_type, fs, plot_dir):
    plt.gcf().clear()
    x = []
    str_tot_posix_io = "total_POSIX_%sS"%io_type
    str_consec_io = "total_POSIX_CONSEC_%sS"%io_type

    if io_type == "READ":
        size_key = "total_POSIX_BYTES_READ"
    if io_type == "WRITE":
        size_key = "total_POSIX_BYTES_WRITTEN"

    nonconsec_io_map = {}
    for stat_row in stat_table:
        tot_io_cnt = float(stat_row[str_tot_posix_io])
        io_size = float(stat_row[size_key])
        if (io_size < min_io_size):
            print "io_size is %lf\n"%io_size
            continue;
        consec_io = float(stat_row[str_consec_io])
        if tot_io_cnt > 0:
            consec_io_percent = consec_io / tot_io_cnt * 100
            x.append(int(consec_io_percent))
        
    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    weights = np.ones_like(x)/float(len(x))*100
    plt.hist(x, bins, range = (0, 100), histtype='bar', weights = weights)
    title = plot_dir + "consec_hist_%s_%s"%(io_type, fs)
    plt.xlabel("percent of consecutive IO")
    plt.ylabel("percent of jobs")
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
    return nonconsec_io_map

#plot_small_io_hist(stat_table, io_type, "all")
#plot_nonconsec_io_hist(stat_table, io_type, "all")
def plot_fs_hist(stat_table, io_type):
    out_dict = {}
    for stat_row in stat_table:
        if out_dict.get(stat_row["FS"], -1) != -1:
            out_dict[stat_row["FS"]] = out_dict[stat_row["FS"]] + 1
        else:
            out_dict[stat_row["FS"]] = 0 
    return out_dict

def get_fs(stat_table, io_type, fs_type):
    out_lst = []
    for stat_row in stat_table:
        if stat_row["FS"] == fs_type:
            out_lst.append(stat_row)
    return out_lst
#out_dict = plot_fs_hist(stat_table, io_type)

#for k,v in out_dict.iteritems():
#    print "fs:%s, count:%d\n"%(k, v)

def plot_bw_proc(stat_table, io_type, plot_dir):
    plt.gcf().clear()
    str_io_start = "total_POSIX_F_%s_START_TIMESTAMP"%io_type
    str_io_end = "total_POSIX_F_%s_END_TIMESTAMP"%io_type
    bw_lst = [];
    size_lst = [];
    proc_cnt_lst = [];

    str_io_time = "total_POSIX_F_%s_TIME"%io_type
    bw_size_map = {}
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
           continue;
       data_size = float(stat_row[str_io_size])
       if data_size < min_io_size:
           continue
       proc_cnt = float(stat_row["nprocs"]) 
       print "io_type:%s, data_size:%ld, time:%lf,bw:%d, path:%s\n"%(io_type, data_size, io_time, int(data_size/io_time/1048576/1024), stat_row["PATH"])
       bw_lst.append(data_size/io_time/1048576/1024)
       proc_cnt_lst.append(int(proc_cnt))
       size_lst.append(int(data_size/1048576/1024))
       str_app_name = stat_row["AppName"]
#       bw_size_map.append((str_app_name, size_lst, proc_cnt_lst))

    colors = []
    index = 0
    for i in size_lst:
        if i >= 0 and i < 100:
            colors.append('r')
        else:
            if i >= 100 and i < 1000:
                colors.append('y')
            else:
                colors.append('g')

#    plt.scatter(size_lst, bw_lst)
#    plt.xlabel('size')
#    plt.ylabel('bandwidth')
#    plt.xscale('log')
#    plt.title('bandwidth distribution versus size')
#    plt.legend()
#    title=plot_dir+"bw_size"
#    print "saving to %s\n"%title
#    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
#    cmap = plt.cm.get_cmap('Spectral', 21) 
#		scat = plt.scatter(x, y, c=colors, alpha=0.35, s=3,cmap=cmap, vmax=max(colors), vmin=min(colors))
#
#    colorsList = [(255, 0, 0),(0, 255, 0),(0, 0, 255)]
#    CustomCmap = matplotlib.colors.ListedColormap(colorsList)
#    scat = plt.scatter(proc_cnt_lst, bw_lst, c = colors, cmap=CustomCmap, vmax=max(colors), vmin = min(colors), s = 3)
#    bounds = [min(size_lst), 10, 1000, max(size_lst)];
    scat = plt.scatter(proc_cnt_lst, bw_lst, c = colors, s = 3)
#    cb = plt.colorbar(scat)
#    cb.set_label('Size (GB)')
    plt.xlabel('process count')
    plt.ylabel('bandwidth (GB/s)')
#    plt.xscale('log')
    plt.title('bandwidth distribution versus process count (red:size < 100GB, yellow: (100GB <= data size < 1TB), green: data size > 1TB).')
    plt.legend()
    title=plot_dir+"bw_proc_cnt_%s"%io_type
#    print "saving to %s\n"%title
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
    return bw_size_map


def plot_bw_size(stat_table, io_type, plot_dir):
    plt.gcf().clear()
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
       if data_size < min_io_size:
           continue
       proc_cnt = float(stat_row["nprocs"]) 
       bw_lst.append(data_size/io_time/1048576/1024)
       proc_cnt_lst.append(int(proc_cnt))
       size_lst.append(int(data_size/1048576/1024))
    
    plt.scatter(size_lst, bw_lst)
    plt.xlabel('size (GB)')
    plt.ylabel('bandwidth (GB/s)')
#    plt.xscale('log')
#    plt.xlim('xmin=1')
    plt.title('bandwidth distribution versus size')
    plt.legend()
    title=plot_dir+"bw_size_%s"%io_type
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
