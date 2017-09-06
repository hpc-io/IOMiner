import matplotlib
matplotlib.use('Agg')
import datetime
import os
import re
import traceback
from pyspark.sql import Row
from os.path import expanduser
from operator import add
import sys
from pyspark import SparkContext
from analyzers.process_darshan_v3 import *
from analyzers.process_lmt import *
from analyzers.process_slurm import *

#%matplotlib inline
#%matplotlib auto
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
#import pandas as pd
from operator import add, itemgetter,div
from pyspark.sql import SQLContext
import glob
import h5py
import csv
from matplotlib.ticker import FormatStrFormatter
from scipy import stats
import pickle

plot_dir = '/global/homes/w/wyoo/plots/'


def convert(l):
    r = []
    for v in l:
        r.append(v/(2**30))
    return r

def OSTActivityPlot(k, lmt_read, lmt_write):
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    title = plot_dir + '#OST_' +str(k) + '_Activity'
    ax1.plot(lmt_read[k],'g-',label='read')
    ax1.set_ylabel('read')
    ax2.plot(lmt_write[k],'b-',label='write')
    ax2.set_ylabel('write')
    ax2.set_ylim(0, 2e8)
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    #ax = df.plot(ax=ax, x='date',secondary_y=['dW'], title=title)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_write_plot(job_id, k, lmt_write, max_write):
    fig, ax1 = plt.subplots()
    xloc = np.arange(lmt_write.shape[1])
    xloc = xloc*5
    ax1.plot(xloc,lmt_write[k],'b-',label='write')
    ax1.set_ylabel('write')
    ax1.set_ylim(0, max_write)
    title = plot_dir + 'OST_' +str(k) + '_Write_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_read_plot(job_id, k, lmt_read, max_read):
    fig, ax1 = plt.subplots()
    ax1.plot(lmt_read[k],'g-',label='read')
    ax1.set_ylabel('read')
    ax1.set_ylim(0, max_read)
    title = plot_dir + 'OST_' +str(k) + '_Read_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_RW_plot(job_id, k, lmt_write, max_write, lmt_read, max_read):
    lmt_sum = lmt_read+lmt_write
    lmt_write = lmt_write/2**30
    lmt_read = lmt_read/2**30
    xloc = np.arange(lmt_write.shape[1])
    xloc = xloc*5
    fig, ax = plt.subplots()
    ax.set_xlabel('Time (s)', fontsize=10)
    ax.set_ylabel('Written/Read (GB)', fontsize=10)
    ax.plot(xloc, lmt_write[k], 'b')
    ax.plot(xloc, lmt_read[k], 'r')
    ax.tick_params(axis='both', which='major', labelsize=10)
    ax.tick_params(axis='both', which='minor', labelsize=8)
    ax.legend(['Written', 'Read'], loc= 'upper left')
    ax.legend(['Written', 'Read'], loc= 'upper right')
    title = plot_dir + 'OST_' +str(k) + '_RW_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_cWdiff_plot(job_id, k, lmt_write, agg_cw):
    cw = np.cumsum(lmt_write[k])
    fig, ax1 = plt.subplots()
    ax1.plot(agg_cw-cw, 'b')
    title = plot_dir + 'OST_' +str(k) + '_cWdiff_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_cW_plot(job_id, k, lmt_write):
    cw = np.cumsum(lmt_write[k])
    xloc = np.arange(lmt_write.shape[1])
    xloc = xloc*5
    fig, ax = plt.subplots()
    ax.set_xlabel('Time (s)', fontsize=9)
    ax.set_ylabel('Cumulative Written (GB)', fontsize=9)
    ax.plot(xloc,cw, 'b')
    title = plot_dir + 'OST_' +str(k) + '_cW_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_cW_merged_plot(job_id, k, lmt_write):
    cw = np.cumsum(lmt_write[k])/2**30
    cw2 = np.cumsum(lmt_write[k+1])/2**30
    xloc = np.arange(lmt_write.shape[1])
    xloc = xloc*5
    fig, ax = plt.subplots()
    ax.set_xlabel('Time (s)', fontsize=10)
    ax.set_ylabel('Cumulative Written (GB)', fontsize=10)
    ax.plot(xloc,cw, 'r')
    ax.plot(xloc,cw2,'b')
    ax.tick_params(axis='both', which='major', labelsize=10)
    ax.tick_params(axis='both', which='minor', labelsize=8)
    ax.legend(['OST '+str(k), 'OST '+str(k+1)], loc= 'upper left')
    title = plot_dir + 'OST_' +str(k) + '_cW_merged_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_write_BW_boxplot(job_id, lmt_write):
    tick_spacing = 5
    lmt_write = lmt_write/5
    max_bandwidth = 700e9/248
    fig, ax1 = plt.subplots()
    ax1.boxplot(lmt_write/max_bandwidth*100)
    #for i in range(lmt_write.shape[1]):
    #    ax1.boxplot(lmt_write[:,i]/max_bandwidth*100)
    ax1.set_ylabel('Write Bandwidth (%) in log scale')
    ax1.set_xlabel('Time (s)')
    ticks = ax1.get_xticks()*5
    ax1.set_xticklabels(ticks.astype(int))
    #xaxis = ax1.get_xaxis()
    #xaxis.set_major_locator(MaxNLocator(integer=True))
    for label in ax1.get_xticklabels()[::2]:
        label.set_visible(False)
    ax1.tick_params(axis='x', labelsize=8)
    ax1.tick_params(axis='y', labelsize=10)
    ax1.set_yscale("log")
    ax1.set_ylim(2)
    title = plot_dir + 'OST_write_Bandwidth_box_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_write_boxplot(job_id, lmt_write):
    #tick_spacing = 5
    fig, ax1 = plt.subplots()
    lmt_write=lmt_write/(2**30)
    ax1.boxplot(lmt_write)
    #for i in range(lmt_write.shape[1]):
    #    ax1.boxplot(lmt_write[:,i]/max_bandwidth*100)
    ax1.set_ylabel('Data Written to File System (GB)')
    ax1.set_xlabel('Time (s)')
    #for label in ax1.get_xticklabels()[::2]:
    #    label.set_visible(False)
    tick_list = []
    i = 0 
    while i <= lmt_write.shape[1]:
        tick_list.append(i)
        i += 8 
    ax1.set_xticks(tick_list)
    ticks = ax1.get_xticks()*5
    ax1.set_xticklabels(ticks.astype(int))
    #xaxis = ax1.get_xaxis()
    #xaxis.set_major_locator(MaxNLocator(integer=True))
    
    ax1.tick_params(axis='x', labelsize=8)
    ax1.tick_params(axis='y', labelsize=10)

    #ax1.set_ylim(0,8e9)
    title = plot_dir + 'OST_write_box_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_read_BW_boxplot(job_id, lmt_read):
    tick_spacing = 5
    lmt_read = lmt_read/5
    max_bandwidth = 700e9/248
    fig, ax1 = plt.subplots()
    ax1.boxplot(lmt_read/max_bandwidth*100)
    #for i in range(lmt_read.shape[1]):
    #    ax1.boxplot(lmt_read[:,i]/max_bandwidth*100)
    ax1.set_ylabel('Read Bandwidth (%) in log scale')
    ax1.set_xlabel('Time (s)')
    ticks = ax1.get_xticks()*5
    ax1.set_xticklabels(ticks.astype(int))
    #xaxis = ax1.get_xaxis()
    #xaxis.set_major_locator(MaxNLocator(integer=True))
    for label in ax1.get_xticklabels()[::2]:
        label.set_visible(False)
    ax1.tick_params(axis='x', labelsize=8)
    ax1.tick_params(axis='y', labelsize=10)
    ax1.set_yscale("log")
    #ax1.set_ylim(2)
    title = plot_dir + 'OST_read_Bandwidth_box_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def OST_read_boxplot(job_id, lmt_read):
    tick_spacing = 5
    fig, ax1 = plt.subplots()
    lmt_read=lmt_read/(2**30)
    ax1.boxplot(lmt_read)
    #for i in range(lmt_read.shape[1]):
    #    ax1.boxplot(lmt_read[:,i]/max_bandwidth*100)
    ax1.set_ylabel('Data Read from File System (GB)')
    ax1.set_xlabel('Time (s)')
    ticks = ax1.get_xticks()*5
    ax1.set_xticklabels(ticks.astype(int))
    #xaxis = ax1.get_xaxis()
    #xaxis.set_major_locator(MaxNLocator(integer=True))
    for label in ax1.get_xticklabels()[::2]:
        label.set_visible(False)
    ax1.tick_params(axis='x', labelsize=8)
    ax1.tick_params(axis='y', labelsize=10)
    #ax1.set_ylim(0,8e9)
    title = plot_dir + 'OST_read_box_'+str(job_id)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')


def AggregatedOSTPlot(lmt_read, lmt_write):
    read_rdd = sc.parallelize(lmt_read)
    write_rdd = sc.parallelize(lmt_write)
    agg_read = read_rdd.reduce(add)
    agg_write = write_rdd.reduce(add)
    fig, ax = plt.subplots()
    title = plot_dir + 'Aggregated_OST_Activity'
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(agg_read,'g-',label='read')
    ax1.set_ylabel('read')
    ax2.plot(agg_write,'b-',label='write')
    ax2.set_ylabel('write')
    ax2.set_ylim(0, 40)
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    #ax = df.plot(ax=ax, x='date',secondary_y=['dW'], title=title)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')

def agg_OST_write_plot(jobid, lmt_write):
    lmt_write_convert=lmt_write/(2**30)
    write_rdd = sc.parallelize(lmt_write_convert)
    agg_write = write_rdd.reduce(add)
    title = plot_dir + 'Aggregated_write_OST_Activity_'+str(jobid)
    fig, ax1 = plt.subplots()
    ax1.plot(agg_write,'b-',label='Write')
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('Aggregated Data Written to File System (GB)')
    ticks = ax1.get_xticks()*5
    ax1.set_xticklabels(ticks.astype(int))
    ax1.tick_params(axis='x', labelsize=8)
    ax1.tick_params(axis='y', labelsize=10)
    #ax = df.plot(ax=ax, x='date',secondary_y=['dW'], title=title)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')
    with open(title+'_write.csv','wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in lmt_write:
            writer.writerow(row)

def agg_OST_read_plot(jobid, lmt_read):
    lmt_read_convert=lmt_read/(2**30)
    read_rdd = sc.parallelize(lmt_read_convert)
    agg_read = read_rdd.reduce(add)
    title = plot_dir + 'Aggregated_read_OST_Activity_'+str(jobid)
    fig, ax1 = plt.subplots()
    ax1.plot(agg_read,'b-',label='Read')
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('Aggregated Data Read from File System (GB)')
    ticks = ax1.get_xticks()*5
    ax1.set_xticklabels(ticks.astype(int))
    ax1.tick_params(axis='x', labelsize=8)
    ax1.tick_params(axis='y', labelsize=10)
    #ax = df.plot(ax=ax, x='date',secondary_y=['dW'], title=title)
    plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')
    with open(title+'.csv','wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in lmt_read:
            writer.writerow(row)
            
def bandwidth_histogram_helper(sc, blist, metric, isLog):
    labels = ['Posix Read', 'Posix Write', 'MPI Read', 'MPI Write']
    if metric == 'Bandwidth':
        index = 0
        unit = 'GB/s'
    elif metric == 'IO Time':
        index = 1
        unit = 's'
    elif metric == 'Size':
        index = 2
        unit = 'GB'
        
    for i,name in enumerate(['pr','pw','mr','mw']):
        fig, ax = plt.subplots()
        if index == 2:
            x = sc.parallelize(blist).map(lambda x:float(x[1][index][i])/2**30).filter(lambda x: x>0).collect()
        else:
            x = sc.parallelize(blist).map(lambda x:float(x[1][index][i])).filter(lambda x: x>0).collect()
        #print name, x
        ax.hist(x, histtype='bar', bins=100)
        ax.legend(prop={'size': 10})
        title = plot_dir + '%s_histo_'%metric + name
        if isLog:
            ax.set_ylabel('# of instances (log scale)')
            ax.set_yscale('log')
            title += ' log'
        else:
            ax.set_ylabel('# of instances')

        ax.set_xlabel(labels[i] + ' %s (%s)'%(metric, unit))

        plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')
        print title

def bandwidth_histogram(sc,blist):
    metric_list = ['Bandwidth', 'IO Time', 'Size']
    isLog_list = [True, False]
    for metric in metric_list:
        for isLog in isLog_list:
            bandwidth_histogram_helper(sc, blist, metric, isLog)
    

def bandwidth_scatter_helper(sc, blist, isLog, cut = 0):
    labels = ['Posix Read', 'Posix Write', 'MPI Read', 'MPI Write']
        
    for i,name in enumerate(['pr','pw','mr','mw']):
        #colors = sc.parallelize(blist).map(lambda x:float(x[1][0][i])).filter(lambda x: x>0).collect()
        #x = sc.parallelize(blist).map(lambda x:float(x[1][2][i])).filter(lambda x: x>0).collect()
        #y = sc.parallelize(blist).map(lambda x:float(x[1][1][i])/2**30).filter(lambda x: x>0).collect()
        filtered = sc.parallelize(blist).map(lambda x:(float(x[1][0][i]),float(x[1][1][i]),float(x[1][2][i])/2**30))\
        .filter(lambda x: x[0] > 0 and x[1] > cut and x[2] > cut).collect()
        x = map(lambda x:x[2],filtered)
        y = map(lambda x:x[1],filtered)
        colors = map(lambda x:x[0],filtered)
        
        #print x[0],y[0],colors[0],x[0]/y[0]
        
        fig = plt.figure(num=None, figsize=(10, 6), dpi=300, facecolor='w', edgecolor='k')
        ax = fig.add_subplot(111)
        plt.title('Observed IO Time and Size Colored by Bandwidth')
        
        title = plot_dir + 'size_time_'+ name
        if isLog:
            ax.set_xlabel('Size (GB) (log scale)')
            ax.set_xscale('log')
            ax.set_ylabel('IO Time (s) (log scale)')
            ax.set_yscale('log')
            title += '_log'
            if cut > 0: title += '_%d'%cut
        else:
            ax.set_xlabel('Size (GB)')
            ax.set_ylabel('IO Time (s)')
            if cut > 0: title += '_%d'%cut

        #print name, x
        cmap = plt.cm.get_cmap('Spectral', 21) 
        scat = plt.scatter(x, y, c=colors, alpha=0.35, s=3,cmap=cmap,
       vmax=max(colors), vmin=min(colors))
        cb = plt.colorbar(scat, spacing='proportional')
        cb.set_label('Bandwidth (GB/s)')
        
        plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')
        print title
        

def bandwidth_scatter(sc,blist, cut =0):
    isLog_list = [True, False]
    for isLog in isLog_list:
        bandwidth_scatter_helper(sc, blist, isLog, cut)
 
def lustre_bandwidth_scatter_helper(sc, blist, isLog, cut = 0):
    labels = ['Posix Read', 'Posix Write', 'MPI Read', 'MPI Write']
        
    for i,name in enumerate(['pr','pw','mr','mw']):
        #colors = sc.parallelize(blist).map(lambda x:float(x[1][0][i])).filter(lambda x: x>0).collect()
        #x = sc.parallelize(blist).map(lambda x:float(x[1][2][i])).filter(lambda x: x>0).collect()
        #y = sc.parallelize(blist).map(lambda x:float(x[1][1][i])/2**30).filter(lambda x: x>0).collect()
        filtered = sc.parallelize(blist).map(lambda x:(float(x[1][0][i]),float(x[1][1][i]),float(x[1][2][i])/2**30))\
        .filter(lambda x: x[0] > 0 and x[1] > cut and x[2] > cut).collect()
        x = map(lambda x:x[2],filtered)
        y = map(lambda x:x[1],filtered)
        colors = map(lambda x:x[0],filtered)
        
        #print x[0],y[0],colors[0],x[0]/y[0]
        
        fig = plt.figure(num=None, figsize=(10, 6), dpi=300, facecolor='w', edgecolor='k')
        ax = fig.add_subplot(111)
        plt.title('Observed IO Time and Size Colored by Bandwidth')
        
        title = plot_dir + 'size_time_'+ name
        if isLog:
            ax.set_xlabel('Size (GB) (log scale)')
            ax.set_xscale('log')
            ax.set_ylabel('IO Time (s) (log scale)')
            ax.set_yscale('log')
            title += '_log'
            if cut > 0: title += '_%d'%cut
        else:
            ax.set_xlabel('Size (GB)')
            ax.set_ylabel('IO Time (s)')
            if cut > 0: title += '_%d'%cut

        #print name, x
        cmap = plt.cm.get_cmap('Spectral', 21) 
        scat = plt.scatter(x, y, c=colors, alpha=0.35, s=3,cmap=cmap, vmax=max(colors), vmin=min(colors))
        cb = plt.colorbar(scat, spacing='proportional')
        cb.set_label('Bandwidth (GB/s)')
        
        plt.savefig(title+'.pdf', format='pdf', bbox_inches='tight')
        print title
        

def slowness(lmt_write):
    agg_cw = np.cumsum(np.sum(lmt_write, axis=0))/248
    diff_cw = np.cumsum(lmt_write,axis=0) - np.tile(agg_cw,(248,1))
    #avg_diff_cw = np.average(diff_cw,axis=1) 
    for i in range(lmt_write.shape[0]):    
        #np.average(agg_cw - np.cumsum(lmt_write[i]))
        print i, agg_cw - np.cumsum(lmt_write[i],axis=0)
        print i, diff_cw[i]

#sys.path.append('/Users/wyoo/perf')
home = expanduser("~")
cluster_name = "local"
darshan_root = ""
host_name = os.getenv('NERSC_HOST')
if host_name ==None or len(host_name) == 0:
    host_name = os.getenv('HOSTNAME')
if host_name != None:
   if host_name.startswith("cori"):
       cluster_name = "cori"
       darshan_root = "/global/cscratch1/sd/darshanlogs/"
       parsed_darshan_root = "/global/cscratch1/sd/wyoo/parsed_darshan/"
   elif host_name.startswith("edison"):
       cluster_name = "edison"
       #darshan_root = "/scratch1/scratchdirs/darshanlogs/"
       darshan_root = "/global/cscratch1/sd/darshanlogs/edison-temp/"
       parsed_darshan_root = "/global/cscratch1/sd/wyoo/parsed_edison_darshan/"
       decompressed_darshan_root = "/global/cscratch1/sd/wyoo/decompressed_edison_darshan/"
   elif host_name.startswith("b"):
       cluster_name = "babbage"
   elif host_name.startswith("n"):
       cluster_name = "lawrencium"
else:
    plot_dir = "/Users/wyoo/ssio/plots/"

job_id = "2993268"
job_start_ts = 1473890915 
job_end_ts = 1473891295 

#app job list
#job_id_list = [2989930, 2990097, 2991903, 2992507, 2992864, 2993268, 2994028, 2994289, 2994935, 2994997]
#job_id_list = [2135285,2135286,2135287,2135288,2135289,2185195,2185196,2185197,2185198,2185199,2186611,2186612,2186614,2186616,2186708]
#job_id_list = [2262036,2262037,2262038,2262039,2262040,2262041,2262042,2262043,2262045,2262047,2262048,2262049,2262050,2262051,2262052] 
#job_id_list = [3037744,3037745,3037752,3037753,3037755,3037756]
#job_id_list = [3104583, 3037745, 3103121, 3103122, 3103123, 3103124]
#vpic 
#job_id_list = [2993268, 2994028, 299499, 3104583, 3037745, 3103121, 3103122, 3103123, 3103124, 2989930, 2990097, 2991903, 2992507, 2992864] 
#vorpal
#job_id_list = [3068703, 3068840, 3090786, 3090787, 3090790, 3090791, 3085553, 3085554, 3175783, 3175784, 3175793, 3175794, 3175796, 3175798, 3241403, 3241404, 3241405, 3241406, 3241407]

time_list = []
lmt_w_sum_list = []
lmt_r_sum_list = []
pws_list = []
mws_list = []
prs_list = []
mrs_list = []
darshan_version = 3

#cori_snx11168, edison_snx11025 scratch1, edison_snx11035 scratch2, edison_snx11036 scratch3
#target_list = [(4282226, 'vpicio_uni', 'scratch3')]
#target_list = [(4255092, 'vpicio_uni', 'scratch3')]
#target_list = [(4311081, 'vpicio_uni', 'scratch3')]
#target_list = [('edison', 4222018, 'vpicio_uni', 'scratch1', 0),
#('edison', 4255092, 'vpicio_uni', 'scratch3', 0)]
#target_list = [('edison', 4255092, 'vpicio_uni', 'scratch3', 0)]
#target_list = [('edison', 4421017, 'vpicio_uni', 'scratch1', 0)]
#target_list = [('edison', 4497048, 'vpicio_uni', 'scratch3', 1)]
#target_list = [('edison', 4328048, 'ior', 'scratch1', 1)]
#target_list = [('edison', 4525644, 'ior', 'scratch1', 1)]
#target_list = [('edison', 4126010, 'hacc_io_write', 'scratch1', 1)]
#target_list = [('edison', 4247652, 'hacc_io_write', 'scratch1', 1)]
target_list = [
    ('cori', 2994028, 'vpicio_uni', 'cscratch', 0),
    #('cori', 2993268, 'vpicio_uni', 'cscratch', 0),
    ('cori', 3241407, 'vorpalio', 'cscratch', 0),
    ('cori', 3068703, 'vorpalio', 'cscratch', 0),
    ]


fs_name_map = {
    'scratch1': 'edison_snx11025',
    'scratch2': 'edison_snx11035',
    'scratch3': 'edison_snx11036',
    'cscratch': 'cori_snx11168',
}

ost_count_map = {
    'scratch1': 24,
    'scratch2': 24,
    'scratch3': 32,
    'cscratch': 248,
}

DARSHAN_FILE_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
darshan_file_pattern = re.compile(DARSHAN_FILE_PATTERN)

def parse_darshan_jobid(s):
    match = darshan_file_pattern.match(s)
    if match is not None:
        return int(match.group(2))
    else:
        return 0


#if len(sys.argv) > 1:
#    app_name = sys.argv[1]
#    raise Exception("") 

# timestamp for getting Darshan logs
start_ts = 1495263600
end_ts = 1497942000
filtered_list = []
bandwidth_list = []

# dates for getting slurm job logs
jobData = get_jobData(sc, datetime(2017,5,20,0,0,0),datetime(2017,6,20,0,0,0))
# filter only jobs using 1000 more cores on Cori Haswell (core number per node <= 64)
job_list = filter_large_jobs(sc, jobData)

# get all darshan logs (with parsing if necessary) overlapping with start_ts and end_ts
#overlap_list = load_overlap_darshan(sc, darshan_root, parsed_darshan_root, start_ts, end_ts)

# get all darshan logs (with parsing if necessary) matched (not considering overlapping) with start_ts and end_ts
# .total is default without ext
parsed_list = get_parsed_list(sc, darshan_root, parsed_darshan_root, start_ts, end_ts)
# get all darshan logs with 'base' ext
parsed_base_list = get_parsed_list(sc, darshan_root, parsed_darshan_root, start_ts, end_ts, 'base')

def replace_ext(fname,ext):
    return fname.rsplit('.', 1)[0]+ext

# get darshan log list using more than 1000 cores (joined on job_list)
def get_filtered_list(start_ts, end_ts):
    filtered_list_fn = plot_dir+"filtered_%d_%d.pk"%(start_ts,end_ts)
    
    if os.path.exists(filtered_list_fn):
        with open(filtered_list_fn, "rb") as fp:
            filtered_list = pickle.load(fp)
    else:
        filtered_list =  sc.parallelize(parsed_list).map(lambda x:((parsed_darshan_root+x[0].split(darshan_root)[1]+'.total'),parse_darshan_jobid(x[0].rsplit('/')[-1]))).filter(lambda x:x[1] in job_list).map(lambda x:x[0]).collect()
        with open(filtered_list_fn, "wb") as fp:
            pickle.dump(filtered_list, fp)
    return filtered_list

# get lustre bandwdith
def get_bandwidth_list(start_ts, end_ts, filtered_list):
    bandwidth_list_fn = plot_dir+"bandwidth_%d_%d.pk"%(start_ts,end_ts)
    if os.path.exists(bandwidth_list_fn):
        with open(bandwidth_list_fn, "rb") as fp:
            bandwidth_list = pickle.load(fp)
    else:
        bandwidth_list = get_bandwidth(sc,filtered_list)
        with open(bandwidth_list_fn, "wb") as fp:
            pickle.dump(bandwidth_list, fp)
    return bandwidth_list

# get lustre stripes 
def get_lustre_stripe_list(start_ts, end_ts, filtered_list):
    lustre_stripe_list_fn = plot_dir+"lustre_stripe_%d_%d.pk"%(start_ts,end_ts)
    if os.path.exists(lustre_stripe_list_fn):
        with open(lustre_stripe_list_fn, "rb") as fp:
            lustre_stripe_list = pickle.load(fp)
    else:
        lustre_stripe_list = get_lustre_stripe(sc,filtered_list)
        with open(lustre_stripe_list_fn, "wb") as fp:
            pickle.dump(lustre_stripe_list, fp)

    return lustre_stripe_list

def save_load_list(filename, input_list, func):
    if os.path.exists(filename):
        with open(filename, "rb") as fp:
            result_list = pickle.load(fp)
    else:
        result_list = func(sc,input_list)
        if len(result_list) > 0:
            with open(filename, "wb") as fp:
                pickle.dump(result_list, fp)
    return result_list

lustre_filtered_list = []

# get darshan log list (.total) using more than 1000 cores (joined on job_list)
filtered_list = get_filtered_list(start_ts, end_ts)
# get darshan log list (.base) using more than 1000 cores (joined on job_list)
filtered_base_list = map(lambda x:replace_ext(x,'.base'), filtered_list)

# get only 3.1 version darshan logs (having lustre info)
lustre_filtered_list = get_version(sc, filtered_base_list, 3.1)
lustre_stripe_list_fn = plot_dir+"lustre_stripe_%d_%d.pk"%(start_ts,end_ts)
# get lustre strips
lustre_stripe_list = get_lustre_stripe_list(start_ts, end_ts, lustre_filtered_list)
bandwidth_list_fn = plot_dir+"bandwidth_%d_%d.pk"%(start_ts,end_ts)
# get lustre bandwdith
bandwidth_list = get_bandwidth_list(start_ts, end_ts, filtered_list)
# draw bandwidth hitstogram
bandwidth_histogram(sc, bandwidth_list)
# draw bandwidth scatter plot 
bandwidth_scatter(sc, bandwidth_list)
# draw bandwidth scatter plot with cut (bandwidith > 1GB/s)
bandwidth_scatter(sc, bandwidth_list,1)

for (cluster_name, job_id, app_name, lmt_scratch_file, darshan_index) in target_list:
#for job_id in job_id_list:
    if cluster_name == "local":
        sc.setLogLevel("FATAL")
        dirname = "/Users/wyoo/spark-1.6.1/cori/"
        darshan_logname = dirname + "darshan/%s/wyoo_vpicio_uni_id%s.darshan.total"%(job_id,job_id)
        vpic_logname = dirname + "slurm/%s/slurm-%s.out"%(job_id,job_id)
        lmt_client_loglist = glob.glob(dirname+"LMT_client/%s/*.out"%job_id)
        lmt_server_path = dirname + "LMT_server/"
        slurm_path = dirname + "slurm"
    #elif cluster_name == "cori" or cluster_name == "edison":
    elif cluster_name == "cori":
        if sc == None:
            sc = SparkContext()
        sc.setLogLevel("FATAL")
        dirname = "/project/projectdirs/m888/ssio/wyoo_data/cori/"
        #darshan_logname = dirname + "darshan/%s/wyoo_vpicio_uni_id%s.darshan.total"%(job_id,job_id)
        app_logname = dirname + "slurm/%s/slurm-%s.out"%(job_id,job_id)
        lmt_client_loglist = glob.glob(dirname+"LMT_client/%s/*.out"%job_id)
        lmt_server_path = "/project/projectdirs/pma/www/daily/"
        slurm_path = dirname + "slurm"
    elif cluster_name == "edison":
        if sc == None:
            sc = SparkContext()
        sc.setLogLevel("FATAL")
        dirname = "/project/projectdirs/m888/ssio/wyoo_data/edison/"
        #darshan_logname = dirname + "darshan/%s/wyoo_vpicio_uni_id%s.darshan.total"%(job_id,job_id)
        app_logname = dirname + "slurm/%s/slurm-%s.out"%(job_id,job_id)
        lmt_client_loglist = glob.glob(dirname+"LMT_client/%s/*.out"%job_id)
        lmt_server_path = "/project/projectdirs/pma/www/daily/"
        slurm_path = dirname + "slurm"
        darshan_version = 2

    darshan_lognames = sorted(glob.glob(dirname+'darshan/%s/wyoo_%s_id%s*.darshan.total'%(job_id,app_name,job_id)))
    #darshan_lognames = sorted(glob.glob(dirname+'darshan/%s/glock_%s_id%s*.darshan.total'%(job_id,app_name,job_id)))
    if len(darshan_lognames) == 1:    
        darshan_logname = darshan_lognames[0]
    elif len(darshan_lognames) >= 2:
        darshan_logname = darshan_lognames[darshan_index]
    else:
        print dirname+'darshan/%s/*_%s_id%s*.darshan.total'%(job_id,app_name,job_id)
        break

    """
    (darshan_rdd, darshan_keys, darshan_jobid, darshan_start_ts, darshan_end_ts, darshan_nprocs) = load_darshan(sc, darshan_logname, darshan_version)
    (start_index,end_index,file_list) = \
        get_lmt_file_list(sc, lmt_server_path, datetime.datetime.fromtimestamp(darshan_start_ts), datetime.datetime.fromtimestamp(darshan_end_ts), fs_name_map[lmt_scratch_file])

    (lmt_read, lmt_write) = lmt_rw_list(sc,start_index,end_index,file_list)

    lmt_read = lmt_read*5
    lmt_write = lmt_write*5
    max_write = np.amax(lmt_write)
    max_read = np.amax(lmt_read)
    """


    #overlap_list = load_overlap_darshan(sc, darshan_root, parsed_darshan_root, 1497942000,1498028400)
    #overlap_list = load_overlap_darshan(sc, darshan_root, parsed_darshan_root, 1495238400,1498028400)
    #overlap_list = load_overlap_darshan(sc, darshan_root, parsed_darshan_root, 1495263600,1497942000)
    #print len(overlap_list)
    #parsed_list =  sc.parallelize(overlap_list).filter(lambda x:int(x[1]) in job_list).map(lambda x:x[0]).collect()
    #bandwidth_list = get_bandwidth(sc,parsed_list)
    #bandwidth_plot(sc, bandwidth_list)

    #slowness(lmt_write)
    #2216757316

    #agg_write = np.sum(lmt_write, axis=1)/248
    #agg_read = np.sum(lmt_read, axis=1)/248
    """
    write_cw = np.cumsum(lmt_write,axis=1)
    read_cw = np.cumsum(lmt_read,axis=1)
    #avg_diff_cw = np.average(diff_cw,axis=1)
    #print write_cw[0].shape
	"""
#bottleneck detection
	"""
    #app_size = 4398046515520/248
    #vpic
    #app_size = float(2199023259968)/ost_count_map[lmt_scratch_file]
    #ior posix
    app_size = float(2199023255552)/ost_count_map[lmt_scratch_file]
    lmt_cross = np.empty([lmt_write.shape[0],10])
    for r in range(0,10):
        cross_point = []
        for i in range(lmt_write.shape[0]):
            #np.average(agg_cw - np.cumsum(lmt_write[i]))
            #print i, agg_cw - np.cumsum(lmt_write[i],axis=0)
            #print i, diff_cw[i]
            
            write_point = float(app_size)*0.1*(r+1)
            #write_point = float(vorpal_size)*0.1*(r+1)

            for j in range(write_cw.shape[1]):
                if write_cw[i][j] >= write_point: 
                #if write_cw[i][j] >= 856196974*0.8: 
                    #print i, j
                    cross_point.append(j)
                    break
            lmt_cross[i][r] = j
        #print r, cross_point
    #print lmt_cross
    #for i,e in enumerate(cross_point):
        #if e >= 4: print i,e
        #if e >= 9: print i,e
    #print lmt_cross[177]
    #print lmt_cross[178]
    avg_cross = np.average(lmt_cross,axis=0)
    diff_cross = np.zeros((lmt_write.shape[0],10))
    diff_cross_count = np.zeros((lmt_write.shape[0],10))
    for r in range(0,10):
        for i in range(lmt_write.shape[0]):
            if lmt_cross[i][r] >= (avg_cross[r] + 2):
                #diff_cross[i][r] += lmt_cross[i][r] - avg_cross[r] - 2
                diff_cross_count[i][r] += 1
                diff_cross[i][r] += lmt_cross[i][r] - avg_cross[r] 
    diff_avg = np.average(diff_cross,axis=0)
    #for i in range(lmt_write.shape[0]):
    #    for r in range(0,10):
    #        if diff_cross[i][r] > diff_avg[r] + 2 and diff_avg[r] > 0:
    #            print i,r, diff_cross[i][r], diff_avg[r], avg_cross[r], lmt_cross[i][r]
    diff_count_sum = np.sum(diff_cross_count,axis=1)
    diff_sum = np.sum(diff_cross,axis=1)
    for i in range(lmt_write.shape[0]):
        #print i, diff_count_sum[i], diff_sum[i]
        if diff_count_sum[i] >= 7:
            print i, diff_count_sum[i], diff_sum[i]

    #interval =  stats.t.interval(0.9973, len(diff_sum)-1, loc=np.mean(diff_sum), scale=stats.sem(diff_sum))
    #for i in range(diff_sum.shape[0]):
    #    if diff_sum[i] > interval[1]:
    #        print i, diff_sum[i]

            
    
    #print lmt_cross[186]
    #print lmt_cross[187]
    #print write_cw[186]
	"""
    #bottleneck detection ends
    """
    for r in range(0,10):
        a = lmt_cross[:,r]
        interval =  stats.t.interval(0.9973, len(a)-1, loc=np.mean(a), scale=stats.sem(a))
        print interval
        for i in range(a.shape[0]):
            if a[i] > interval[1]:
                print r, i, int(a[i])
    lmt_sum = lmt_write + lmt_read 
    a = lmt_sum.flatten()
    interval =  stats.t.interval(0.9973, len(a)-1, loc=np.mean(a), scale=stats.sem(a))
    """
    #interval =  stats.t.interval(0.9545, len(a)-1, loc=np.mean(a), scale=stats.sem(a))
    #print interval
    #for i in range(lmt_sum.shape[0]):
    #    for j in range(lmt_sum.shape[1]):
    #        if lmt_sum[i][j] > interval[1]:
    #            print i,j,lmt_sum[i][j]
    """
    agg_sum = agg_write + agg_read
    a = agg_sum
    interval =  stats.t.interval(0.9973, len(a)-1, loc=np.mean(a), scale=stats.sem(a))
    for i in range(agg_sum.shape[0]):
        if agg_sum[i] > interval[1]:
            print i, agg_sum[i]
    """    
    """
    lmt_count = np.zeros(lmt_sum.shape[0])
    for j in range(lmt_sum.shape[1]):
        #print j, stats.zscore(lmt_sum[:,j])
        a = lmt_sum[:,j]

        for i,v in enumerate(a):
            if v > interval[1]:
                print j, i,v
                lmt_count[i] += 1
    for i,v in enumerate(lmt_count):
        print i,int(v)
    """
    #plot starts 
    """
    OST_write_boxplot(job_id,lmt_write)
    OST_write_BW_boxplot(job_id,lmt_write)
    agg_OST_write_plot(job_id, lmt_write)
    OST_read_BW_boxplot(job_id,lmt_read)
    OST_read_boxplot(job_id,lmt_read)
    agg_OST_read_plot(job_id, lmt_read)
    

    write_rdd = sc.parallelize(lmt_write)
    agg_write = write_rdd.reduce(add)
    agg_cw = np.cumsum(agg_write)/248
    #OST_cW_merged_plot(job_id, 186, lmt_write)
    #OST_RW_plot(job_id, 186, lmt_write, max_write, lmt_read, max_read)
    #OST_cW_merged_plot(job_id, 177, lmt_write)
    #OST_RW_plot(job_id, 177, lmt_write, max_write, lmt_read, max_read)

    for i in range(lmt_write.shape[0]):
        OST_RW_plot(job_id, i, lmt_write, max_write, lmt_read, max_read)
        OST_cW_plot(job_id, i, lmt_write)
        OST_write_plot(job_id, i, lmt_write, max_write)
    """
    #plot ends 
# Darshan overlap analysis 
	"""
    #max_write = np.amax(lmt_write)
    #for i in range(lmt_write.shape[0]):
    #    OST_write_plot(job_id, i, lmt_write, max_write)
    #slurm_rdd = get_slurm_rdd(sc,slurm_path,datetime.fromtimestamp(job_start_ts),datetime.fromtimestamp(job_end_ts))
    #(darshan_rdd, darshan_keys, darshan_jobid, darshan_start_ts, darshan_end_ts, darshan_nprocs) = load_darshan(sc, darshan_logname)
    #(lmt_read, lmt_write) = lmt_rw_list(sc,lmt_server_path,datetime.datetime.fromtimestamp(job_start_ts),datetime.datetime.fromtimestamp(job_end_ts))
    darshan_start_date = datetime.datetime.fromtimestamp(darshan_start_ts)
    darshan_end_date = datetime.datetime.fromtimestamp(darshan_end_ts)
    time_list.append(darshan_end_ts-darshan_start_ts)
    #(lmt_read, lmt_write) = lmt_rw_list(sc,lmt_server_path,darshan_start_date,darshan_end_date)
    #lmt_read = map(lambda x:x*5,lmt_read)
    #lmt_write = map(lambda x:x*5,lmt_write)
    lmt_w_sum_list.append(np.sum(lmt_write))
    lmt_r_sum_list.append(np.sum(lmt_read))
    #np.amax(lmt_write, axis=0)
    #overlap_list = load_overlap_darshan(sc, '/global/cscratch1/sd/darshanlogs/', '/global/cscratch1/sd/wyoo/parsed_darshan/', darshan_start_ts, darshan_end_ts)
    if darshan_version == 2:
        overlap_list = load_overlap_darshan(sc, darshan_root, parsed_darshan_root, darshan_start_ts, darshan_end_ts, decompressed_darshan_root, darshan_version)
    else:
        overlap_list = load_overlap_darshan(sc, darshan_root, parsed_darshan_root, darshan_start_ts, darshan_end_ts)
    #print overlap_list
    overlap_pws = get_overlap_value(sc, overlap_list,'pw')
    overlap_mws = get_overlap_value(sc, overlap_list,'mw')
    overlap_prs = get_overlap_value(sc, overlap_list,'pr')
    overlap_mrs = get_overlap_value(sc, overlap_list,'mr')
    
    #print overlap_pws
    #print overlap_mws
    #print overlap_prs
    #print overlap_mrs
    #print overlap_mws
    #print sc.parallelize(overlap_mws).map(lambda x:((x[2]-x[1])*x[0])).reduce(add)

    print job_id
    print sc.parallelize(overlap_pws).map(lambda x:((x[2]-x[1])*x[0])).collect()
    print sc.parallelize(overlap_mws).map(lambda x:((x[2]-x[1])*x[0])).collect()
    #print sc.parallelize(overlap_prs).map(lambda x:((x[2]-x[1])*x[0])).collect()
    #print sc.parallelize(overlap_mrs).map(lambda x:((x[2]-x[1])*x[0])).collect()

    pws_temp = sc.parallelize(overlap_pws).map(lambda x:((x[2]-x[1])*x[0]))
    if pws_temp.count() > 0:
        pws_list.append(pws_temp.reduce(add))
    mws_temp = sc.parallelize(overlap_mws).map(lambda x:((x[2]-x[1])*x[0]))
    if mws_temp.count() > 0:
        mws_list.append(mws_temp.reduce(add))

    prs_temp = sc.parallelize(overlap_prs).map(lambda x:((x[2]-x[1])*x[0]))
    if prs_temp.count() > 0:
        prs_list.append(prs_temp.reduce(add))
    mrs_temp = sc.parallelize(overlap_mrs).map(lambda x:((x[2]-x[1])*x[0]))
    if mrs_temp.count() > 0:
        mrs_list.append(mrs_temp.reduce(add))
	"""
# some prints
"""
print 'time: ',time_list
print 'lmt_w_sum:',convert(lmt_w_sum_list)
print 'lmt_r_sum:',convert(lmt_r_sum_list)
print 'pws: ',convert(pws_list)
print 'mws: ',convert(mws_list)
print 'prs: ',convert(prs_list)
print 'mrs: ',convert(mrs_list)
#lmt_client_log_list = dirname + "LMT_client/cori_snx11168.h5lmt"    
"""

