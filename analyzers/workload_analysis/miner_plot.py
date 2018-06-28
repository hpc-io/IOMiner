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
from distri_plot import *
# vertical line: bandwidth, file system type, horizontal line: percent of 
# small reads
plot_dir="/global/cscratch1/sd/tengwang/miner0612/plots/"
miner_param = json.load(open('/global/cscratch1/sd/tengwang/miner0612/miner_para.conf'))
min_io_size = float(miner_param["format"]["min_write_size"])

def plotLargeFileIODist(out_lst, io_type, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Percent of Largest File\' IO Time')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"large_%s"%io_type
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotDistri(data, io_type, data_size, is_log_scale, is_per_file, is_per_factor, is_large = 0):
#    print "dimension:%d\n"%len(data)
    title = "low_bw_analyze_%s_%s_perfile%d.jpg"%(io_type, data_size, is_per_file)
#    for i in range(0, len(data)):
#        print data[i]
#    parallel_coordinates(data, is_log_scale, is_per_file, is_per_factor).savefig(title, format = 'jpg', bbox_inches = 'tight')
    parallel_coordinates(data, is_log_scale, is_per_file, is_per_factor, is_large).savefig(title, bbox_inches = 'tight')

def plotVector(x_list, x_values, x_show_ticks, x_show_ticks_text, y_list, y_ticks, y_ticks_texts, io_type, suffix):
    plt.gcf().clear()
#    plt.xlabel('Count of Each Vector (Combination of Factor Values)')
    plt.xlabel('Percentile of Bins')
    plt.ylabel('I/O Factors')
    title = plot_dir + "factor_analysis_%s_%s"%(io_type,suffix)
#    for i in range(0, len(x_list)):
#        print "%dth x_list:%d, y_list:%d,%d,%d\n"%(i, x_list[i], y_list[i][0], y_list[i][1], y_list[i][2])

    plt.plot(x_list, y_list)
    plt.yticks(y_ticks, y_ticks_texts)
    plt.xticks(x_show_ticks, x_show_ticks_text)
    plt.legend()
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotIOCompDist(out_lst, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('IO Time Percent')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"iocomp_dist"
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
        print "%dth is %d\n"%(i - 1, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])
    print "%dth is %d\n"%(len(out_lst) - 1, out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotMetaDist(out_lst, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Metadata Ratio')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"metadata_dist"
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
        print "%dth is %d\n"%(i - 1, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])
    print "%dth is %d\n"%(len(out_lst) - 1, out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotProcFileRatio(out_dict):
    plt.xlabel('Process to File Ratio')
    plt.ylabel('Number of Jobs')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"proc_io_ratio"
    cursor = 0
    for tmp_key in out_dict.keys():
        x.append(cursor)
        xlabels.append(tmp_key)
        y.append(int(out_dict[tmp_key]))
        cursor = cursor + 1
    plt.bar(x, y, color = 'b')
    plt.xticks(x, xlabels, fontsize=6)
    plt.xticks(rotation=90)
    plt.legend()
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotIOTypeDist(out_dict):
    plt.gcf().clear()
    plt.xlabel('IO Type')
    plt.ylabel('Number of Jobs')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"io_type_dist"
    cursor = 0
    for tmp_key in out_dict.keys():
        x.append(cursor)
        xlabels.append(tmp_key)
        y.append(int(out_dict[tmp_key]))
        cursor = cursor + 1
    plt.bar(x, y, color = 'b')
    plt.xticks(x, xlabels, fontsize=8)
    plt.legend()
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotTopIOApp(app_list, io_type):
    plt.gcf().clear()
    plt.xlabel('Applications')
    plt.ylabel('Data Size (TB)')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"top_app_%s"%io_type
    cursor = 0
    for record in app_list:
        x.append(cursor)
        xlabels.append(record[0])
#        xlabels.append(tmp_key)
        y.append(record[1]/1024/1048576/1024)
        cursor = cursor + 1
        if cursor == 10:
            break;
    plt.bar(x, y, color = 'b')
#    plt.yscale('log')
    plt.xticks(x, xlabels, fontsize=8)
    plt.legend()
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotAppDist(out_dict):
    plt.gcf().clear()
    plt.xlabel('Applications')
    plt.ylabel('Application Count')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"app_dist"
    cursor = 0
    for tmp_key in out_dict.keys():
        x.append(cursor)
#        xlabels.append(tmp_key)
        y.append(int(out_dict[tmp_key]))
        cursor = cursor + 1
    plt.bar(x, y, color = 'b')
    plt.yscale('log')
#    plt.xticks(x, xlabels, fontsize=8)
    plt.legend()
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')


    
def plotFSDist(out_dict):
    plt.gcf().clear()
    plt.xlabel('File System Type')
    plt.ylabel('Number of Jobs')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"fs_dist"
    cursor = 0
    for tmp_key in out_dict.keys():
        x.append(cursor)
        xlabels.append(tmp_key)
        y.append(int(out_dict[tmp_key]))
        cursor = cursor + 1
    plt.bar(x, y, color = 'b')
    plt.xticks(x, xlabels, fontsize=8)
    plt.legend()
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotSeqDist(out_lst, io_type, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Percent of Sequential Request')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"seq_%s"%io_type
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
#plotConsecDist(out_dict["consec"], 10)
#plotSmallDist(out_dict["small_io"], 10)

def plotConsecDist(out_lst, io_type, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Percent of Consecutive Request')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"consec_%s"%io_type
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')


def plotSmallDist(out_lst, io_type, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Percent of Small Request')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"small_%s"%io_type
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')


def plotProcCntDist(out_lst, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Process Count')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"proc_cnt_dist"
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotBWDist(out_lst, io_type, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    str_ylabel = "%s Bandwidth (MB/s)"%io_type
    plt.ylabel(str_ylabel)
    x = []
    y = []
    xlabels = []
    title = plot_dir+"bw_dist_%s"%io_type
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')


def plotDataSizeDist(out_lst, io_type, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Data Size (MB)')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"data_size_dist_%s"%io_type
    cursor = 0

    delta = 100/len(out_lst)
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')

def plotReadWriteRatioDist(out_lst, max_ticks, divide):
    plt.gcf().clear()
    plt.xlabel('Write Ratio in Percent')
    plt.ylabel('Job Count')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"write_ratio_dist"
    cursor = 0

    delta = 100/len(out_lst) #divide should be 100
    tmp_sum = delta
    for i in range(1, len(out_lst)):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(tmp_sum)
        tmp_sum = tmp_sum + delta
        y.append(out_lst[i - 1])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
#    print "tick_delta is %d\n"%tick_delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')


