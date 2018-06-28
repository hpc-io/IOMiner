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

def plotUtilizationDist(in_lst, max_ticks, node_type):
    plt.gcf().clear()
    plt.xlabel('Process Count Per Node')
    plt.ylabel('Number of Jobs')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"slurm_util_%s"%node_type
    cursor = 0
   
    if node_type == "has":
        out_lst = [0] * 256
        for i in range(0, len(in_lst)):
            out_lst[in_lst[i] - 1] += 1
    else:
        out_lst = [0] * 272
        for i in range(0, len(in_lst)):
            out_lst[in_lst[i] - 1] += 1

    delta = 1
    for i in range(1, len(out_lst) + 1):
#        print "%dth is %d\n"%(tmp_sum, out_lst[i - 1])
        x.append(i)
        y.append(out_lst[i - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = len(x)/cur_ticks*delta
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
#plotConsecDist(out_dict["consec"], 10)
#plotSmallDist(out_dict["small_io"], 10)
def plotUtilizationPercent(out_lst, node_type, max_ticks):
    plt.gcf().clear()
    plt.xlabel('Percentile of Job Count')
    plt.ylabel('Processes Per Node')
    x = []
    y = []
    xlabels = []
    title = plot_dir+"slurm_proc_per_node%s"%node_type
    cursor = 0

    delta = len(out_lst)/100
    tmp_sum = delta
    cursor = 0
    for i in range(0, len(out_lst)):
        cursor += 1
        if cursor%tmp_sum == 0:
            x.append(cursor/tmp_sum)
            y.append(out_lst[i])
            if cursor/tmp_sum == 99:
                break
            print "%dth is %d\n"%(cursor/tmp_sum, out_lst[i])

    x.append(100)
    y.append(out_lst[len(out_lst) - 1])
    print "%dth is %d\n"%(100, out_lst[len(out_lst) - 1])

    cur_ticks = min(len(x), max_ticks)

    tick_delta = 10
    plt.bar(x, y, color = 'b')
#    plt.xticks(x, x, fontsize=8)
    plt.xticks(np.arange(0, max(x) + 1, tick_delta), fontsize=8)
    plt.legend()
#    plt.yscale('log')
#    plt.ylim((0, max(y)))
    plt.savefig(title+'.jpg', format = 'jpg', bbox_inches='tight')
