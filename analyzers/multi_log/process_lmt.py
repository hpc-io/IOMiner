import datetime
import pytz
import calendar
import h5py
from operator import add 
import numpy as np
import os

#excerpts from https://github.com/valiantljk/h5spark/blob/master/src/main/python/h5spark/read.py
#read one dataset/file each time. 
def readones(filename_dataname_tuple, start_index, end_index):
    filename, dataset, flag = filename_dataname_tuple
    try:
        f=h5py.File(filename,'r')
        d=f[dataset]
        a=list(d[:])
    except Exception, e:
        print "ioerror:%s"%e, filename
    else:
        f.close()
        if(flag == 0): return [s[0:-1] for s in a]
        elif(flag == -1): return [s[start_index:-1] for s in a] 
        elif(flag == 1): return [s[:end_index] for s in a]

def readH5Multi(sc, file_list_or_txt_file, start_index, end_index, partitions=None):
    '''Takes in a list of (file, dataset) tuples, one such tuple or the name of a file that contains
    a list of files and returns rdd with each row as a record'''

    #a list of tuples each with the pair (file_path, datasetname)
    if isinstance(file_list_or_txt_file,list):
        rdd = sc.parallelize(file_list_or_txt_file)
    
    #a string describing a file with list of hdf5 files
    #elif isinstance(file_list_or_txt_file,str):
    #    rdd = sc.textFile(file_list_or_txt_file, use_unicode=False).map(lambda line: tuple(str(line).replace(" ", "").split(',')))
    
    #elif isinstance(file_list_or_txt_file,tuple):
    #    rdd = sc.parallelize([file_list_or_txt_file])
    #if partitions:
    #    rdd = rdd.repartition(partitions)
    #flag 0: normal, flag -1: start, flag 1: end
    ret = rdd.map(lambda (f_path,dataset,flag): (os.path.abspath(os.path.expandvars(os.path.expanduser(f_path))), dataset, flag))\
        .flatMap(lambda x:readones(x,start_index,end_index))
    return ret

def convert_to_ts(d):
    ptz = pytz.timezone("US/Pacific")
    return calendar.timegm(ptz.localize(d).utctimetuple())

def getIndex(date):
    index = 0
    d = datetime.datetime(date.year,date.month,date.day)
    if date > datetime.datetime(d.year,d.month,d.day):
        index = int((date - d).total_seconds()/5)
    return index

def get_lmt_file_list(sc, file_path, start_date, end_date, lmt_scratch_file = 'cori_snx11168'):
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
        d += delta
    return (start_index,end_index,file_list)

#defuncted
def get_read_write_array(sc, file_path, start_date, end_date):
    (start_index,end_index,file_list) = get_lmt_file_list(sc, file_path, start_date, end_date)
    read_list = [(f,"/OSTReadGroup/OSTBulkReadDataSet",0) for f in file_list]
    read_list[0] = (read_list[0][0],read_list[0][1],-1)
    read_list[len(read_list)-1] = (read_list[len(read_list)-1][0],read_list[len(read_list)-1][1],1)
    #print read_list
    write_list = [(f,"/OSTWriteGroup/OSTBulkWriteDataSet",0) for f in file_list]
    write_list[0] = (write_list[0][0],write_list[0][1],-1)
    write_list[len(write_list)-1] = (write_list[len(write_list)-1][0],write_list[len(write_list)-1][1],1)
    #print write_list
    read_array = readH5Multi(sc,read_list, start_index, end_index)
    #print read_array.take(1)
    write_array = readH5Multi(sc,write_list, start_index, end_index)
    #print write_array.take(1)
    return (read_array, write_array) 

def lmt_list_wrapper(start_index, end_index, file_list, dataset_name):
    lmt_list = None 
    for i in range(len(file_list)):
        filename = file_list[i]
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
    return lmt_list 

def lmt_rw_list(sc, start_index,end_index,file_list):
    rl = lmt_list_wrapper(start_index, end_index, file_list, "/OSTReadGroup/OSTBulkReadDataSet")
    wl = lmt_list_wrapper(start_index, end_index, file_list, "/OSTWriteGroup/OSTBulkWriteDataSet")
    return (rl, wl)
"""
def lmt_rwio_list(sc, start_index,end_index,file_list):
    for filename in file_list:
        f=h5py.File(filename,'r')
        rdata=f["/OSTReadGroup/OSTIosizeReadDataSet"]
        rl = rdata[:,start_index:end_index]
        wdata=f["/OSTWriteGroup/OSTIosizeWriteDataSet"]
        wl = wdata[:,start_index:end_index]
        return (rl, wl)
"""

def lmt_mds_list(sc, start_index,end_index,file_list):
    return lmt_list_wrapper(start_index, end_index, file_list, "/MDSOpsGroup/MDSOpsDataSet")

def lmt_mds_cpu_list(sc, start_index,end_index,file_list):
    return lmt_list_wrapper(start_index, end_index, file_list,"/MDSCPUGroup/MDSCPUDataSet")


def lmt_oss_list(sc, start_index,end_index,file_list):
    return lmt_list_wrapper(start_index, end_index, file_list, "/OSSCPUGroup/OSSCPUDataSet")

def lmt_mds_ops(sc, start_index,end_index,file_list):
    for filename in file_list:
        f=h5py.File(filename,'r')
        opnames = f["/MDSOpsGroup/MDSOpsDataSet"].attrs['OpNames']
        return opnames
"""
job_start_ts = 1475391600 
job_end_ts = 1475391605
job_end_ts = 1475478010
#ob_end_ts = 1475564405
start_date = datetime.datetime.fromtimestamp(job_start_ts)
end_date = datetime.datetime.fromtimestamp(job_end_ts)
print start_date, end_date
(start_index,end_index,file_list) = get_lmt_file_list(sc, file_path, start_date, end_date)
ol = lmt_oss_list(sc, start_index, end_index, file_list)
"""
"""
#2993268
job_start_ts = 1473890915
job_end_ts = 1473891295
#2992864
#job_start_ts = 1473889066
#job_end_ts = 1473889168
file_path = "/project/projectdirs/pma/www/daily/"
start_date = datetime.datetime.fromtimestamp(job_start_ts)
end_date = datetime.datetime.fromtimestamp(job_end_ts)

(start_index,end_index,file_list) = get_lmt_file_list(sc, file_path, start_date, end_date)
ops = lmt_mds_ops(sc, start_index, end_index, file_list)

#ol = lmt_oss_list(sc, start_index, end_index, file_list)
#for i in range(ol.shape[1]):
#    print sum(ol[:,i])*5,
#print ''

ml = lmt_mds_list(sc, start_index, end_index, file_list)
for i in range(len(ml)):
    total = int(sum(ml[i])*5)
    if total > 0: 
        print ops[i], total
        print ops[i], map(int, list(ml[i]))

mcl = lmt_mds_cpu_list(sc, start_index, end_index, file_list)
total = int(sum(mcl))
if total > 0: 
    print total
    print 5*mcl

"""
"""
(rl,wl) = lmt_rw_list(sc, start_index, end_index, file_list)
print 'read:'
for i in range(rl.shape[1]):
    print sum(rl[:,i])*5,
print '\nwrite:'
for i in range(wl.shape[1]):
    print sum(wl[:,i])*5,
print ''
"""
