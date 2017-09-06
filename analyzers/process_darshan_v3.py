import datetime
import os
import re
import traceback
from pyspark.sql import Row
from os.path import expanduser
from operator import add, sub
import numpy as npy
import scipy as spy
import fileinput
import math
import matplotlib as mpl
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
import glob
import subprocess
import errno

home = expanduser("~")

COMMENT_PATTERN = '(#...)'
DARSHAN_LOG_PATTERN = '(\S+)\t([+-]?\d+(?:\.\d+)?)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t(\S+)\t(\S+)'
#DARSHAN_LOG_PATTERN = '([+-]?\d+(?:\.\d+)?)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t(\S+)\t(\S+)'
HEADER_PATTERN = '(#\s+)(\S+):(\s+)(\d+)'
KEY_VALUE_PATTERN = '(\S+):(\s+)([+-]?\d+(?:\.\d+)?)'
NPROCS_PATTERN = '(#\s+nprocs:\s+)(\d+)'
VERSION_PATTERN = '(#\s+)(darshan log version):\s+(\d+(?:\.\d+)?)'

#global darshan_jobid, darshan_nprocs, darshan_start_time, darshan_end_time
darshan_jobid = darshan_nprocs = darshan_start_time = darshan_end_time = 0

def displaymatch(match):
    if match is None:
        return None
    return '<Match: %r, groups=%r>' % (match.group(), match.groups())

comment = re.compile(COMMENT_PATTERN)
valid = re.compile(DARSHAN_LOG_PATTERN)
nprocs_detailed = re.compile(NPROCS_PATTERN)
header = re.compile(HEADER_PATTERN)
key_value = re.compile(KEY_VALUE_PATTERN)
version_pattern = re.compile(VERSION_PATTERN)

#<module>       <rank>  <record id>     <counter>       <value> <file name>     <mount pt>      <fs type>

def parse_darshan_log_line(logline):
    if len(logline) == 0 or len(logline) == 1:
        return (logline, 0)
    comment_match = comment.match(logline)
    if comment_match is not None:
        header_match = header.match(logline)
        if header_match is not None:
            return ((header_match.group(2),int(header_match.group(4))),-11)
        else:
            version_match = version_pattern.match(logline)
            if version_match is not None:
                return ((version_match.group(2),float(version_match.group(3))),-11)
        return (logline,-1)
    key_value_match = key_value.match(logline)
    if key_value_match is not None:
        return ((key_value_match.group(1), key_value_match.group(3)), 2) 
    
    match = valid.match(logline)
    
    if match is None:
        print len(logline)
        print "Invalid logline: %s" % logline
        print ":".join("{:02x}".format(ord(c)) for c in logline)
        return (logline,-2)
    
    return ((
            match.group(4), # counter
            (
            match.group(2), # rank
            match.group(5), # value
            match.group(3), # hashfile
            match.group(6), # suffix
            match.group(7), # mount
            match.group(8), # fs
            #match.group(1)  # module
            )
    ),1)

def parse_darshan_log_line_v2(logline):
    if len(logline) == 0 or len(logline) == 1:
        return (logline, 0)
    comment_match = comment.match(logline)
    if comment_match is not None:
        header_match = header.match(logline)
        if header_match is not None:
            return ((header_match.group(2),int(header_match.group(4))),-11)
        return (logline,-1)
    key_value_match = key_value.match(logline)
    if key_value_match is not None:
        return ((key_value_match.group(1), key_value_match.group(3)), 2) 
    
    match = valid.match(logline)
    
    if match is None:
        print len(logline)
        print "Invalid logline: %s" % logline
        return (logline,-2)
    
    return ((
            match.group(4), # counter
            (
            match.group(2), # rank
            match.group(5), # value
            match.group(3), # hashfile
            match.group(6), # suffix
            match.group(7), # mount
            match.group(8), # fs
            #match.group(1)  # module
            )
    ),1)

#parser to get nprocs from detailed file
def parse_darshan_detailed_nprocs(nprocsline):
    if len(nprocsline) == 0:
        return (nprocsline, 0)
    nprocs_match = nprocs_detailed.match(nprocsline)
    if nprocs_match is None:
        return (None, -1)
    return (nprocs_match.group(2),1)

def iterCounters(x):
    values = []
    
    kvMap = {}
    #for (k,v,v2,v3,v4,v5) in x:
    #    kvMap[k] = v
    #for v in x:
    #    kvMap[v[0]] = v[1] # counter = value
    #kvMap['rank'] = v[2]
    for v in x:
        kvMap[v[0]] = v[1] # rank = value
    kvMap['file'] = v[2]
    kvMap['suffix'] = v[3]
    kvMap['mount'] = v[4]
    kvMap['fs'] = v[5]
    return Row(**kvMap)

# Function to calculate slowest time
def calc_slowest_time(row):
    slowest_time = 0

    if (int(row.rank) == -1):
        slowest_time = float(row.CP_F_SLOWEST_RANK_TIME)
    else:
        if (float(row.CP_INDEP_OPENS) or float(row.CP_COLL_OPENS)):
            slowest_time = max(slowest_time, (float(row.CP_F_POSIX_META_TIME)\
                                                  + float(row.CP_F_POSIX_READ_TIME)\
                                                  + float(row.CP_F_POSIX_WRITE_TIME)))
        else:
            slowest_time = max(slowest_time, (float(row.CP_F_POSIX_META_TIME)\
                                                 + float(row.CP_F_POSIX_READ_TIME)\
                                                 + float(row.CP_F_POSIX_WRITE_TIME)))
    return round(float(slowest_time), 6)

# Function to calculate average time
def calc_avgtest_time(row):
    avg_time = 0

    if (float(row.CP_INDEP_OPENS) or float(row.CP_COLL_OPENS)):
        avg_time = max(avg_time, (float(row.CP_F_POSIX_META_TIME)\
                                              + float(row.CP_F_POSIX_READ_TIME)\
                                              + float(row.CP_F_POSIX_WRITE_TIME)))
    else:
        avg_time = max(avg_time, (float(row.CP_F_POSIX_META_TIME)\
                                             + float(row.CP_F_POSIX_READ_TIME)\
                                             + float(row.CP_F_POSIX_WRITE_TIME)))
    if (int(row.rank) == -1):
        avg_time = avg_time / nprocs
        
    return round(float(avg_time), 6)

# Function to calculate total bytes in perf file
def total_bytes_func(row):
    total_bytes = 0
    total_bytes += float(row.CP_BYTES_READ) + float(row.CP_BYTES_WRITTEN)
    return total_bytes
#total_bytes = new_rdd.map(lambda x:x[1]).map(lambda x: total_bytes_func(x)).reduce(lambda a,b:a+b)

# Function to calculate mpi_file in perf file
def get_mpi_file(row):
    mpi_file = float(row.CP_INDEP_OPENS) + float(row.CP_COLL_OPENS)
    return mpi_file
#mpi_file = new_rdd.map(lambda x:x[1]).map(lambda x: get_mpi_file(x)).reduce(lambda a,b:a+b)

# Function to calculate shared_time_by_open in perf file
def calc_shared_time_by_open(row):
    shared_time_by_open = 0
    if (int(row.rank) == -1):
        if (float(row.CP_F_CLOSE_TIMESTAMP) > float(row.CP_F_OPEN_TIMESTAMP)):
            shared_time_by_open += (float(row.CP_F_CLOSE_TIMESTAMP) - float(row.CP_F_OPEN_TIMESTAMP))
    return shared_time_by_open
#shared_time_by_open = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared_time_by_open(x)).reduce(lambda a,b:a+b)

# Function to calculate shared_time_by_open_lastio in perf file
def calc_shared_time_by_open_lastio(row):
    shared_time_by_open_lastio = 0
    if (int(row.rank) == -1):
        if (float(row.CP_F_READ_END_TIMESTAMP) > float(row.CP_F_WRITE_END_TIMESTAMP)):
            if (float(row.CP_F_READ_END_TIMESTAMP) > float(row.CP_F_OPEN_TIMESTAMP)):
                shared_time_by_open_lastio += (float(row.CP_F_READ_END_TIMESTAMP) - float(row.CP_F_OPEN_TIMESTAMP))
        else:
            if (float(row.CP_F_WRITE_END_TIMESTAMP) > float(row.CP_F_OPEN_TIMESTAMP)):
                shared_time_by_open_lastio += (float(row.CP_F_WRITE_END_TIMESTAMP) - float(row.CP_F_OPEN_TIMESTAMP))
    return shared_time_by_open_lastio
#shared_time_by_open_lastio = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared_time_by_open_lastio(x))\
#                                                        .reduce(lambda a, b:a+b)

# Function to calculate shared_time_by_cumul in perf file
def calc_shared_time_by_cumul(row):
    shared_time_by_cumul = 0
    if (int(row.rank) == -1):
        if (mpi_file):
            shared_time_by_cumul += (float(row.CP_F_MPI_META_TIME) + float(row.CP_F_MPI_READ_TIME)\
                                     + float(row.CP_F_MPI_WRITE_TIME))
        else:
            shared_time_by_cumul += (float(row.CP_F_POSIX_META_TIME) + float(row.CP_F_POSIX_READ_TIME)\
                                     + float(row.CP_F_POSIX_WRITE_TIME))
    shared_time_by_cumul = shared_time_by_cumul/nprocs
    return round(shared_time_by_cumul, 6)
#shared_time_by_cumul = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared_time_by_cumul(x)).reduce(lambda a, b: a+b)

# Function to calculate shared_meta_time in perf file
def calc_shared_meta_time(row):
    shared_meta_time = 0
    if (int(row.rank) == -1):
        if (mpi_file):
            shared_meta_time += float(row.CP_F_MPI_META_TIME)
        else:
            shared_meta_time += float(row.CP_F_POSIX_META_TIME)
    shared_meta_time = shared_meta_time/nprocs
    return round(shared_meta_time, 6)
#shared_meta_time = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared_meta_time(x)).reduce(lambda a, b: a+b)

# Function to calculate rank_cumul_io_time in perf file
def calc_rank_cumul_io_time_dict(row):
    if (mpi_file):
        return (int(row.rank), (float(row.CP_F_MPI_META_TIME), float(row.CP_F_MPI_READ_TIME), float(row.CP_F_MPI_WRITE_TIME)))
    else:
        return (int(row.rank), (float(row.CP_F_POSIX_META_TIME), float(CP_F_POSIX_READ_TIME), float(row.CP_F_POSIX_WRITE_TIME)))

#rank_cumul_io_time_dict = new_rdd.map(lambda x:x[1]).map(lambda x: calc_rank_cumul_io_time_dict(x)).reduceByKey(lambda x,y:tuple(map(add, x,y)))
#rank_cumul_io_time_dict.map(lambda x:x[1]).map(lambda x:sum(x)).collect()

# Function to calculate rank_cumul_md_time in perf file
def calc_rank_cumul_md_time_dict(row):
    if (mpi_file):
        return (int(row.rank), (float(row.CP_F_MPI_META_TIME), 0))
    else:
        return (int(row.rank), (float(row.CP_F_POSIX_META_TIME), 0))
#rank_cumul_md_time_dict = new_rdd.map(lambda x:x[1]).map(lambda x: calc_rank_cumul_md_time_dict(x)).reduceByKey(lambda x,y:tuple(map(add, x,y)))
#rank_cumul_md_time_dict.map(lambda x:x[1]).map(lambda x:sum(x)).collect()

# Function to calculate agg_perf_by_cumul in perf file
def calc_agg_perf_by_cumul(row):
    agg_perf_by_cumul = 0
    if (shared_time_by_cumul):
        agg_perf_by_cumul = (total_bytes / 1048576.0) / shared_time_by_cumul
    return agg_perf_by_cumul
#agg_perf_by_cumul = new_rdd.map(lambda x:x[1]).map(lambda x: calc_agg_perf_by_cumul(x)).reduce(lambda a,b:a)

# Function to calculate agg_perf_by_open in perf file
def calc_agg_perf_by_open(row):
    agg_perf_by_open = 0
    if (shared_time_by_open):
        agg_perf_by_open = (total_bytes / 1048576.0) / shared_time_by_open
    return agg_perf_by_open
#agg_perf_by_open = new_rdd.map(lambda x:x[1]).map(lambda x: calc_agg_perf_by_open(x)).reduce(lambda a,b:a)

# Function to calculate calc_agg_perf_by_open_lastio
def calc_agg_perf_by_open_lastio(row):
    agg_perf_by_open_lastio = 0
    if (shared_time_by_open_lastio):
        agg_perf_by_open_lastio = (total_bytes / 1048576.0) / shared_time_by_open_lastio
    return agg_perf_by_open_lastio
#agg_perf_by_open_lastio = new_rdd.map(lambda x:x[1]).map(lambda x: calc_agg_perf_by_open_lastio(x)).reduce(lambda a,b:a)

# Function to calculate calc_shared_time_by_slowest
def calc_shared_time_by_slowest(row):
    shared_time_by_slowest = 0
    if (int(row.rank) == -1):
        shared_time_by_slowest += float(row.CP_F_SLOWEST_RANK_TIME)
    return shared_time_by_slowest
#shared_time_by_slowest = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared_time_by_slowest(x)).reduce(lambda a,b:a+b)

# Function to calculate calc_agg_perf_by_slowest
def calc_agg_perf_by_slowest(row):
    agg_perf_by_slowest = 0
    if (shared_time_by_slowest):
        agg_perf_by_slowest = (total_bytes / 1048576.0) / shared_time_by_slowest
    return agg_perf_by_slowest
#agg_perf_by_slowest = new_rdd.map(lambda x:x[1]).map(lambda x: calc_agg_perf_by_slowest(x)).reduce(lambda a,b: a)

# Function to calculate total in .file
def calc_total_files(row):
    total = 0
    total += 1
    return total
#total = new_rdd.map(lambda x:x[1]).map(lambda x: calc_total_files(x)).reduce(lambda x,y:x+y)

# Function to calculate total_size in .file
def calc_total_size_files(row):
    total_size = 0
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    total_size += maxed
    return total_size
#total_size = new_rdd.map(lambda x:x[1]).map(lambda x: calc_total_size_files(x)).reduce(lambda x,y: x+y)

# Function to calculate total_max in .file
def calc_total_max_files(row):
    total_max = 0
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    total_max = max(total_max, maxed)
    return total_max
#total_max = new_rdd.map(lambda x:x[1]).map(lambda x: calc_total_max_files(x)).reduce(lambda a,b: max(a, b))

# Function to calculate read_only in .file
def calc_read_only(row):
    read_only = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)

    if ((r != 0) and (w == 0)):
        read_only += 1
    return read_only

#read_only = new_rdd.map(lambda x:x[1]).map(lambda x: calc_read_only(x)).reduce(lambda x,y: x+y)

# Function to calculate read_only_size in .file
def calc_read_only_size(row):
    read_only_size = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
    
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    
    if ((r != 0) and (w == 0)):
        read_only_size += maxed
    return read_only_size

#read_only_size = new_rdd.map(lambda x:x[1]).map(lambda x: calc_read_only_size(x)).reduce(lambda x,y: x+y)

# Function to calculate read_only_max in .file
def calc_read_only_max(row):
    read_only_max = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
    
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    
    if ((r != 0) and (w == 0)):
        read_only_max = max(read_only_max, maxed)
    return read_only_max
#read_only_max = new_rdd.map(lambda x:x[1]).map(lambda x: calc_read_only_max(x)).reduce(lambda x,y:max(x,y))

# Function to calculate write_only in .file
def calc_write_only(row):
    write_only = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
        
    if ((r == 0) and (w != 0)):
        write_only += 1
    return write_only
#write_only = new_rdd.map(lambda x:x[1]).map(lambda x: calc_write_only(x)).reduce(lambda x,y:x+y)

# Function to calculate write_only_size in .file
def calc_write_only_size(row):
    write_only_size = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
        
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    
    if ((r == 0) and (w != 0)):
        write_only_size += maxed
    return write_only_size
#write_only_size = new_rdd.map(lambda x:x[1]).map(lambda x: calc_write_only_size(x)).reduce(lambda x,y:x+y)

# Function to calculate write_only_max in .file
def calc_write_only_max(row):
    write_only_max = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
        
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    
    if ((r == 0) and (w != 0)):
        write_only_max = max(write_only_max, maxed)
    return write_only_max

#write_only_max = new_rdd.map(lambda x:x[1]).map(lambda x: calc_write_only_max(x)).reduce(lambda x,y: max(x, y))

# Function to calculate read_write in .file
def calc_read_write(row):
    read_write = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
    
    if ((r != 0) and (w != 0)):
        read_write += 1
    return read_write
#read_write = new_rdd.map(lambda x:x[1]).map(lambda x: calc_read_write(x)).reduce(lambda x,y: x+y)

# Function to calculate read_write_size in .file
def calc_read_write_size(row):
    read_write_size = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
    
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    
    if ((r != 0) and (w != 0)):
        read_write_size += maxed
    return read_write_size
#read_write_size = new_rdd.map(lambda x:x[1]).map(lambda x: calc_read_write(x)).reduce(lambda x,y: x+y)

# Function to calculate read_write_max in .file
def calc_read_write_max(row):
    read_write_max = 0
    r = float(row.CP_POSIX_READS) + float(row.CP_POSIX_FREADS) + float(row.CP_INDEP_READS) + float(row.CP_COLL_READS)\
        + float(row.CP_SPLIT_READS) + float(row.CP_NB_READS)
    w = float(row.CP_POSIX_WRITES) + float(row.CP_POSIX_FWRITES) + float(row.CP_INDEP_WRITES)\
        + float(row.CP_SPLIT_WRITES) + float(row.CP_NB_WRITES)
    
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    
    if ((r != 0) and (w != 0)):
        read_write_max = max(read_write_max, maxed)
    return read_write_max
#read_write_max = new_rdd.map(lambda x:x[1]).map(lambda x: calc_read_write_max(x)).reduce(lambda x,y: max(x,y))

# Needed to calculate shared and unique
FILETYPE_SHARED = (1 << 0)
FILETYPE_UNIQUE = (1 << 1)
FILETYPE_PARTSHARED = (1 << 2)

def cur_type(row):
    procs = 0
    procs += 1
    curtype = 0
    if (int(row.rank) == -1):
        procs = nprocs
        curtype |= FILETYPE_SHARED
    elif (procs > 1):
        curtype &= (~FILETYPE_UNIQUE)
        curtype |= FILETYPE_PARTSHARED
    else:
        curtype |= FILETYPE_UNIQUE
    return curtype

# Function to calculate number shared in .file
def calc_shared(row):
    shared = 0
    curtype = cur_type(row)
    if (curtype & (FILETYPE_SHARED | FILETYPE_PARTSHARED)):
        shared += 1;
    return shared
#shared = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared(x)).reduce(lambda x,y: x+y)

# Function to calculate shared_size in .file
def calc_shared_size(row):
    shared_size = 0
    curtype = cur_type(row)
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    if (curtype & (FILETYPE_SHARED | FILETYPE_PARTSHARED)):
        shared_size += maxed
    return shared_size
#shared_size = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared_size(x)).reduce(lambda x,y: x+y)

# Function to calculate shared_max in .file
def calc_shared_max(row):
    shared_max = 0
    curtype = cur_type(row)
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    if (curtype & (FILETYPE_SHARED | FILETYPE_PARTSHARED)):
        shared_max = max(shared_max, maxed)
    return shared_max
#shared_max = new_rdd.map(lambda x:x[1]).map(lambda x: calc_shared_max(x)).reduce(lambda x,y: max(x, y))

# Function to calculate unique in .file
def calc_unique(row):
    unique = 0
    curtype = cur_type(row)
    if (curtype & (FILETYPE_UNIQUE)):
        unique += 1;
    return unique
#unique = new_rdd.map(lambda x:x[1]).map(lambda x: calc_unique(x)).reduce(lambda x,y: x+y)

# Function to calculate unique_size in .file
def calc_unique_size(row):
    unique_size = 0
    curtype = cur_type(row)
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    if (curtype & (FILETYPE_UNIQUE)):
        unique_size += maxed;
    return unique_size
#unique_size = new_rdd.map(lambda x:x[1]).map(lambda x: calc_unique_size(x)).reduce(lambda x,y: x+y)

# Function to calculate unique_max in .file
def calc_unique_max(row):
    unique_max = 0
    curtype = cur_type(row)
    maxed = max(float(row.CP_SIZE_AT_OPEN), float(row.CP_MAX_BYTE_READ))
    maxed = max(maxed, float(row.CP_MAX_BYTE_WRITTEN))
    if (curtype & (FILETYPE_UNIQUE)):
        unique_max = max(unique_max, maxed);
    return unique_max
#unique_max = new_rdd.map(lambda x:x[1]).map(lambda x: calc_unique_max(x)).reduce(lambda x,y: max(x, y))

def parse_darshan(sc, logname, version = 3):
    rdd = sc.textFile(logname, use_unicode=False).map(parse_darshan_log_line, version)
    new_rdd = rdd.filter(lambda x:x[1] == 1).map(lambda x:x[0])\
            .aggregateByKey((),lambda x,y: x+(y,), lambda x,y: x + y)\
            .mapValues(lambda x:iterCounters(x))
    header_list = rdd.filter(lambda x:x[1] == -11).collect()
    darshan_jobid = darshan_nprocs = darshan_start_time = darshan_end_time = 0
    for ((k,v), flag) in header_list:
        if k == 'jobid': darshan_jobid = v
        elif k == 'start_time': darshan_start_time = v
        elif k == 'end_time': darshan_end_time = v
        elif k == 'nprocs': darshan_nprocs = v    
        elif k == 'darshan log version': darshan_version = v
    
    return (rdd, new_rdd, darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs, darshan_version)
    #print (darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs)

    ### plot functions for counter plots below
###
def calc_my_ave_time(row):
    avg_time = 0
    #print "row=", row[0:2048]
    for i in range(0,len(row)):
        avg_time += float(row[i])
    avg_time = avg_time / (len(row))      
    return round(float(avg_time), 6)

def calc_my_total_time(row):
    tot_time = 0
    for i in range(0,len(row)):
        tot_time += float(row[i])
    return round(float(tot_time), 6)


# collecting the row with the key.  e.g. CP_F_OPEN_TIMESTAMP

def collect_my_row(new_rdd, darshan_nprocs, key):
    """
    my_row = -1;
    for i in range(0, len(my_keys)):
        #if my_keys[i] == 'CP_F_OPEN_TIMESTAMP':
        if my_keys[i] == key:
            my_row = i
            #print "new row=", new_rdd.map(lambda x:x[1]).map(lambda x:x).collect()[i]
    
    tmp_y_values = new_rdd.map(lambda x:x[1]).collect()[my_row][0:2048]
    """
    tmp_y_values = new_rdd.filter(lambda x:x[0] == key).collect()[0][1][0:darshan_nprocs]
    return tmp_y_values
"""
def collect_my_row(key):
    return tuple(new_rdd.filter(lambda x:x[0] == 'POSIX_F_META_TIME').map(lambda x:x[1][0:2048]).collect()[0])
"""
def plot_my_values(key, xvalues, yvalues, timeflag, combineflag):
    if (timeflag): # time entry
        ptitle = key+': '+'total_time='+str(calc_my_total_time(yvalues)) \
            +'\nave_time/proc='+str(calc_my_ave_time(yvalues))
    else:  # size
        ptitle = key+': '+'\nmax='+str(max(yvalues))+'\nmin='+str(min(yvalues))
    plt.scatter(xvalues, yvalues, s=4, marker='s', color="blue", label=key)
    #plt.xlabel('RANK', fontsize=8)
    plt.xlim(-1, 2049)
    if (combineflag):
        plt.ylabel(key, fontsize=4)
        plt.title(ptitle, fontsize=4)
    else:
        plt.ylabel(key, fontsize=8)
        plt.title(ptitle, fontsize=8)

# Plots
mpl.rcParams['xtick.labelsize'] = 5
mpl.rcParams['ytick.labelsize'] = 5

myx = range(0,2048)
#print "DONE"

def avg_ts_size(ts_length, start, end, Size):
    start_index = (int(start)/5)
    end_index = int(end)/5
    ts_size_list = [0]*ts_length
    BW = float(Size)/(end-start)
    ts_size_list[start_index] = BW*float((start_index+1)*5-start)/5
    ts_size_list[end_index] = BW*float(end - (end_index)*5)/5
    for i in range(start_index+1,end_index-start_index):
        ts_size_list[i] = BW
    return ts_size_list

def getAvgValue(new_rdd, darshan_start_time, darshan_end_time, darshan_nprocs, flag):
    if(flag == 'POSIX_WRITE'):
        start_array = collect_my_row(new_rdd,darshan_nprocs,'POSIX_F_WRITE_START_TIMESTAMP')
        end_array = collect_my_row(new_rdd,darshan_nprocs,'POSIX_F_WRITE_END_TIMESTAMP')
        size_array = collect_my_row(new_rdd,darshan_nprocs,'POSIX_BYTES_WRITTEN')
    elif(flag == 'MPIIO_WRITE'):
        start_array = collect_my_row(new_rdd,darshan_nprocs,'MPIIO_F_WRITE_START_TIMESTAMP')
        end_array = collect_my_row(new_rdd,darshan_nprocs,'MPIIO_F_WRITE_END_TIMESTAMP')
        size_array = collect_my_row(new_rdd,darshan_nprocs,'MPIIO_BYTES_WRITTEN')
    elif(flag == 'POSIX_READ'):
        start_array = collect_my_row(new_rdd,darshan_nprocs,'POSIX_F_READ_START_TIMESTAMP')
        end_array = collect_my_row(new_rdd,darshan_nprocs,'POSIX_F_READ_END_TIMESTAMP')
        size_array = collect_my_row(new_rdd,darshan_nprocs,'POSIX_BYTES_READ')
    elif(flag == 'MPIIO_READ'):
        start_array = collect_my_row(new_rdd,darshan_nprocs,'MPIIO_F_READ_START_TIMESTAMP')
        end_array = collect_my_row(new_rdd,darshan_nprocs,'MPIIO_F_READ_END_TIMESTAMP')
        size_array = collect_my_row(new_rdd,darshan_nprocs,'MPIIO_BYTES_READ')

    size_list = []
    for i in range(0,len(start_array)):
        if size_array[i] == '0': continue
        size_list.append((float(start_array[i]),float(end_array[i]),float(size_array[i])))
    time_list = [0]*((darshan_end_time - darshan_start_time) / 5 + 1)
    for i in range(0,len(time_list)):
        time_list[i] = darshan_start_time + 5*i

    start_time = (int(darshan_start_time)/5)*5
    end_time = int(math.ceil(float(darshan_end_time)/5)*5)
    ts_length = (end_time-start_time)/5

    #avg_ts_size(4.357826, 79.097946, 8858370048.0,20)
    sum_list = [0]*ts_length
    test = 0
    for w in size_list:
        ts_size_list = avg_ts_size(ts_length, w[0],w[1],w[2])
        test += sum(ts_size_list)
        sum_list = map(add, ts_size_list, sum_list)
    print test
    return sum_list

def get_sum_list(start_array, end_array, size_array, darshan_start_time, darshan_end_time):
    size_list = []
    for i in range(0,len(start_array)):
        if size_array[i] == '0': continue
        size_list.append((float(start_array[i]),float(end_array[i]),float(size_array[i])))
    time_list = [0]*((darshan_end_time - darshan_start_time) / 5 + 1)
    for i in range(0,len(time_list)):
        time_list[i] = darshan_start_time + 5*i

    start_time = (int(darshan_start_time)/5)*5
    end_time = int(math.ceil(float(darshan_end_time)/5)*5)
    ts_length = (end_time-start_time)/5

    sum_list = [0]*ts_length
    test = 0
    for w in size_list:
        ts_size_list = avg_ts_size(ts_length, w[0],w[1],w[2])
        test += sum(ts_size_list)
        sum_list = map(add, ts_size_list, sum_list)
    print test
    return sum_list

def get_avg_value_helper(size_array,start_array,end_array,overlap_start_time,overlap_end_time,darshan_start_time,darshan_end_time):
    if len(size_array) == 0 or size_array[0] == '0' or int(end_array[0]) <= int(start_array[0]):
        return (0,0,0)  
    max_start = max(int(start_array[0])+darshan_start_time,overlap_start_time)
    min_end = min(int(end_array[0])+darshan_start_time, overlap_end_time)

    if len(start_array) == 1 and len(end_array) == 1 and len(size_array) == 1 and max_start < min_end:
        return (float(size_array[0])/((float(end_array[0])-float(start_array[0]))),max_start, min_end)
    else:
        return (0,len(start_array),len(end_array))

def get_avg_value_from_file(file_name, overlap_start_time, overlap_end_time, darshan_start_time, darshan_end_time, option): 
    pr_size_array = []
    pr_start_array = []
    pr_end_array = []
    pw_size_array = []
    pw_start_array = []
    pw_end_array = []
    mr_size_array = []
    mr_start_array = []
    mr_end_array = []
    mw_size_array = []
    mw_start_array = []
    mw_end_array = []

    with open(file_name) as infile:
        for line in infile:
            (content, flag) = parse_darshan_log_line(line)
            #if flag == -2:
            #    print file_name, ': ', count
            if flag == 2:
                (k,v) = content
                if(k == 'total_POSIX_BYTES_READ'):
                    pr_size_array.append(v)
                elif(k == 'total_POSIX_F_READ_START_TIMESTAMP'):
                    pr_start_array.append(v)
                elif(k == 'total_POSIX_F_READ_END_TIMESTAMP'):
                    pr_end_array.append(v)
    
                elif(k == 'total_POSIX_BYTES_WRITTEN'):
                    pw_size_array.append(v)
                elif(k == 'total_POSIX_F_WRITE_START_TIMESTAMP'):
                    pw_start_array.append(v)
                elif(k == 'total_POSIX_F_WRITE_END_TIMESTAMP'):
                    pw_end_array.append(v)

                elif(k == 'total_MPIIO_BYTES_READ'):
                    mr_size_array.append(v)
                elif(k == 'total_MPIIO_F_READ_START_TIMESTAMP'):
                    mr_start_array.append(v)
                elif(k == 'total_MPIIO_F_READ_END_TIMESTAMP'):
                    mr_end_array.append(v)

                elif(k == 'total_MPIIO_BYTES_WRITTEN'):
                    mw_size_array.append(v)
                elif(k == 'total_MPIIO_F_WRITE_START_TIMESTAMP'):
                    mw_start_array.append(v)
                elif(k == 'total_MPIIO_F_WRITE_END_TIMESTAMP'):
                    mw_end_array.append(v)
                    infile.close()
                    break
    
    if option == 'mw':
        return get_avg_value_helper(mw_size_array, mw_start_array, mw_end_array,overlap_start_time, overlap_end_time, darshan_start_time,darshan_end_time)
    if option == 'pw':
        return get_avg_value_helper(pw_size_array, pw_start_array, pw_end_array,overlap_start_time, overlap_end_time,darshan_start_time,darshan_end_time)

    if option == 'mr':
        return get_avg_value_helper(mr_size_array, mr_start_array, mr_end_array,overlap_start_time, overlap_end_time,darshan_start_time,darshan_end_time)
    if option == 'pr':
        return get_avg_value_helper(pr_size_array, pr_start_array, pr_end_array,overlap_start_time, overlap_end_time,darshan_start_time,darshan_end_time)

def cal_bandwidth(size, time):
    if time == 0: return -1
    else: return size/time/(2**30)

def get_bandwidth_from_file(file_name):
    pr_size=pr_start=pr_end=0.0
    pw_size=pw_start=pw_end=0.0
    mr_size=mr_start=mr_end=0.0
    mw_size=mw_start=mw_end=0.0
    with open(file_name) as infile:
        for line in infile:
            (content, flag) = parse_darshan_log_line(line)
            #if flag == -2:
                #print file_name, ': ', count
            if flag == 2:
                (k,v) = content
                #print (k,v)
                if(k == 'total_POSIX_BYTES_READ'):
                    pr_size = float(v)
                elif(k == 'total_POSIX_F_READ_START_TIMESTAMP'):
                    pr_start = float(v)
                elif(k == 'total_POSIX_F_READ_END_TIMESTAMP'):
                    pr_end = float(v)
                elif(k == 'total_POSIX_BYTES_WRITTEN'):
                    pw_size = float(v)
                elif(k == 'total_POSIX_F_WRITE_START_TIMESTAMP'):
                    pw_start = float(v)
                elif(k == 'total_POSIX_F_WRITE_END_TIMESTAMP'):
                    pw_end = float(v)
                elif(k == 'total_MPIIO_BYTES_READ'):
                    mr_size = float(v)
                elif(k == 'total_MPIIO_F_READ_START_TIMESTAMP'):
                    mr_start = float(v)
                elif(k == 'total_MPIIO_F_READ_END_TIMESTAMP'):
                    mr_end = float(v)
                elif(k == 'total_MPIIO_BYTES_WRITTEN'):
                    mw_size = float(v)
                elif(k == 'total_MPIIO_F_WRITE_START_TIMESTAMP'):
                    mw_start = float(v)
                elif(k == 'total_MPIIO_F_WRITE_END_TIMESTAMP'):
                    mw_end = float(v)
                    infile.close()
                    break
    pr_time = pr_end - pr_start
    pw_time = pw_end - pw_start
    mr_time = mr_end - mr_start
    mw_time = mw_end - mw_start
    prb = cal_bandwidth(pr_size,pr_time)
    pwb = cal_bandwidth(pw_size,pw_time)
    mrb = cal_bandwidth(mr_size,mr_time)
    mwb = cal_bandwidth(mw_size,mw_time)
    return ((prb,pwb,mrb,mwb),(pr_time,pw_time,mr_time,mw_time),(pr_size,pw_size,mr_size,mw_size))
            
def get_lustre_stripe_from_file(file_name):
    w = 0
    s = 0
    with open(file_name) as infile:
        for line in infile:
            (content, flag) = parse_darshan_log_line(line)
            if flag == -11:
                (k,v) = content
                if k == 'darshan log version':
                    version = v
                    if version < 3.1:
                        infile.close()
                        return (-1,-1)

            elif flag == 1:
                (k,v) = content
                if(k == 'LUSTRE_STRIPE_WIDTH'):
                    w = max(w,int(v[1]))
                elif(k == 'LUSTRE_STRIPE_SIZE'):
                    s = max(s,float(v[1])/(2**20))
        infile.close()
    return (w,s)

#print rdd.filter(lambda x:x[1] == 1).map(lambda x:x[0]).aggregateByKey((),lambda x,y:x+(y,), \
#    lambda x,y:x+y).mapValues(lambda x:iterCounters(x)).map(lambda x:(x[0], x[1])).take(5)

#print new_rdd.map(lambda x:x[1]).map(lambda x:x).collect()
#my_time2 = new_rdd.map(lambda x:x[1]).map(lambda x: calc_my_time_2(x))
#print my_time2.collect()

#rdd for test_darshan_log_line function
#test_rdd = sc.textFile(logname, use_unicode=False).map(test_darshan_log_line)

#dirname = "/Users/wyoo/spark-1.5.0/ssio/"
#logname = dirname + "sbyna_vpicio_uni_id1112618_2-10-33742-14227578900887249491_1.darshan.all"
#logname = dirname + 'wyoo_vpicio_uni_id2154259_5-11-56290-8971700712097655210_1.darshan.all'
#nprocsline = dirname + "sbyna_vpicio_uni_id1112618_2-10-33742-14227578900887249491_1.darshan.all"
#new rdd of (hash, Row(counter = value, ...)
#
#nprocs = float(sc.textFile(nprocsline, use_unicode=False).map(parse_darshan_detailed_nprocs)\
#                                                    .filter(lambda x: x[1] == 1)\
#                                                    .map(lambda (a, b): a)\
#                                                    .reduce(lambda x: x))

def load_darshan(sc, logname, version = 3):
    (rdd, new_rdd, darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs, darshan_version) = parse_darshan(sc, logname, version)
    #rdd.filter(lambda x:x[1] == -2).count()
    print (darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs)
    #new_rdd = rdd.filter(lambda x:x[1] == 1).map(lambda x:x[0])\
    #        .aggregateByKey((),lambda x,y: x+(y,), lambda x,y: x + y)\
    #        .mapValues(lambda x:iterCounters(x))
    nprocs = darshan_nprocs
    #print new_rdd.take(1)

    my_keys = rdd.filter(lambda x:x[1] == 1).map(lambda x:x[0]).aggregateByKey((),lambda x,y:x+(y,), \
        lambda x,y:x+y).mapValues(lambda x:iterCounters(x)).map(lambda x:(x[0])).collect()
    return (new_rdd, my_keys, darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs, darshan_version)

def is_output_saved(darshan_log, parsed_darshan_dir, flag='total'):
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.'+flag
    return (os.path.exists(filename) and os.path.getsize(filename) > 0)

def save_parser_output(darshan_log, parsed_darshan_dir, flag='total', version = 3):
    #output = subprocess.check_output(['darshan-parser','--all',darshan_log])
    #output, err = subprocess.Popen(['darshan-parser','--all',darshan_log], stdout=subprocess.PIPE).communicate() 
    #print output.splitlines()
    #output = StringIO.StringIO()
    #subprocess.Popen(['darshan-parser','--all',darshan_log],stdout=output)
    
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.'+flag
    if not is_output_saved(darshan_log, parsed_darshan_dir, flag):
        with file(filename, 'wb') as target:
            if version == 3:
                subprocess.call(['darshan-parser','--'+flag,darshan_log],stdout=target)
            #elif version == 3.1:
            #    subprocess.call(['/global/homes/w/wyoo/darshan/darshan-3.1.3/darshan-util/darshan-parser','--'+flag,darshan_log],stdout=target)
            else:
                pipes = subprocess.Popen(['darshan-parser','--'+flag,darshan_log],stdout=target, stderr=subprocess.PIPE)
                std_out, std_err = pipes.communicate()
                if pipes.returncode != 0:
                    #err_msg = "%s: %s. Code: %s" % (filename, std_err.strip(), pipes.returncode)
                    #print err_msg
                    alt_filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.total'
                    with file(alt_filename, 'wb') as alt_target:
                        subprocess.call(['/usr/common/software/darshan/3.1.4/bin/darshan-parser','--total',darshan_log],stdout=alt_target)

    return filename

def save_darshan_zip_files(darshan_log, decompressed_darshan_dir):
    filename = (decompressed_darshan_dir+darshan_log.rpartition('/')[2]).rstrip('.gz')
    with file(filename, 'wb') as target:
        subprocess.call(['gzip','-c','-d',darshan_log,filename], stdout=target)
    return filename


def getOverlap(start1, end1, start2, end2):
    max_start = max(start1, start2)
    min_end = min(end1, end2)
    if max_start >= min_end:
        return None
    else:
        return (max_start, min_end)

def parse_darshan_get_overlap(file_name, start_time, end_time, version = 3):
    with open(file_name) as infile:
        for line in infile:
            if version == 3:
                (content, flag) = parse_darshan_log_line(line)
            else:
                (content, flag) = parse_darshan_log_line_v2(line)
            if flag == -11:
                (k,v) = content
                if k == 'jobid': jobid = v                                                        
                elif k == 'start_time': start_time2 = v                                            
                elif k == 'end_time': 
                    end_time2 = v 
                    overlap = getOverlap(start_time, end_time, start_time2, end_time2)
                    infile.close() 
                    if overlap is None: 
                        return None
                    else: 
                        return (file_name, jobid, overlap[0], overlap[1], start_time, end_time, start_time2, end_time2)
            else:
                continue
        infile.close() 
    return None

# get all darshan logs (with parsing if necessary) overlapping with start_ts and end_ts
# .total is default without ext
def get_parsed_list(sc, darshan_root, parsed_darshan_root, start_time, end_time, ext = 'total', decompressed_darshan_dir = "", version=3):
    start_date = datetime.fromtimestamp(start_time)
    end_date = datetime.fromtimestamp(end_time)
    d = datetime(start_date.year,start_date.month,start_date.day)
    delta = timedelta(days=1)
    file_list = []
    zip_file_list = []
    path_list = []
    parsed_rdd_list = []
    while d <= end_date:
        darshan_files = darshan_root + d.strftime("%Y/%-m/%-d/*.darshan")
        darshan_zip_files = darshan_root + d.strftime("%Y/%-m/%-d/*.darshan.gz")
        path_list.append(parsed_darshan_root + d.strftime("%Y/%-m/%-d/"))
        parsed_darshan_dir = parsed_darshan_root + d.strftime("%Y/%-m/%-d/")
        if not os.path.exists(parsed_darshan_dir):
            try:
                os.makedirs(parsed_darshan_dir)
            except OSError as exc:  # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(parsed_darshan_dir):
                    pass
                else:
                    raise
        file_list = glob.glob(darshan_files)
        zip_file_list = glob.glob(darshan_zip_files)
        d += delta

        #darshan_rdd = sc.parallelize(file_list).map(lambda x:save_parser_output(x, parsed_darshan_dir, 'total'))
        if version == 2:
            rdd = sc.parallelize(file_list).map(lambda x:save_parser_output(x, parsed_darshan_dir, 'perf', version))
            rdd2 = sc.parallelize(zip_file_list).map(lambda x:save_darshan_zip_files(x, decompressed_darshan_dir))
            print rdd2.collect()
            darshan_rdd = sc.union([rdd,rdd2])
            parsed_rdd_list.append(darshan_rdd)
        else:
            rdd = sc.parallelize(file_list).map(lambda x:(x,is_output_saved(x, parsed_darshan_dir, ext)))
            
            rdd2 = rdd.filter(lambda x:not x[1]).map(lambda x:save_parser_output(x[0], parsed_darshan_dir, ext, 3)).collect()
            parsed_rdd_list.append(rdd)
    return sc.union(parsed_rdd_list).collect() 

#def find_diff(sc, darshan_root, parsed_darshan_root, d):
#    darshan_files = glob.glob(darshan_root + d.strftime("%Y/%-m/%-d/*.darshan"))
#    parsed_darshan_files = glob.glob(parsed_darshan_root + d.strftime("%Y/%-m/%-d/*.darshan.total"))
#    rdd = sc.parallelize(darshan_files)
#    rdd2 = sc.parallelize(parsed_darshan_files)
#    return rdd

# get all darshan logs (with parsing if necessary) matched (not considering overlapping) with start_ts and end_ts
def load_overlap_darshan(sc, darshan_root, parsed_darshan_root, start_time, end_time, getOverlap = False, decompressed_darshan_dir = "", version=3):
    
        #for f in parsed_list:
        #    if f.find('wyoo_vpicio_uni_id3103121_11-16-66045-') <> -1:
        #        print f, parse_darshan_get_overlap(f,start_time,end_time)
    overlap_list = parsed_list = get_parsed_list(sc, darshan_root, parsed_darshan_root, start_time, end_time) 
    if getOverlap:
        overlap_list = sc.parallelize(parsed_list).map(lambda x:parse_darshan_get_overlap(x,start_time,end_time)).filter(lambda x:x is not None).collect()
    return overlap_list

def get_job_id(sc, darshan_root, parsed_darshan_root, start_time, end_time):
    overlap_list = parsed_list = get_parsed_list(sc, darshan_root, parsed_darshan_root, start_time, end_time) 
    
    #darshan_rdd = sc.parallelize(file_list).map(lambda x:save_parser_output(x, parsed_darshan_dir, 'total'))
    rdd = sc.parallelize(file_list).map(lambda x:save_parser_output(x, parsed_darshan_dir, 'total', 3.1))
    parsed_list.append(rdd.collect())

        #for f in parsed_list:
        #    if f.find('wyoo_vpicio_uni_id3103121_11-16-66045-') <> -1:
        #        print f, parse_darshan_get_overlap(f,start_time,end_time)
    overlap_list = parsed_list


def get_bandwidth(sc, parsed_list):
    return sc.parallelize(parsed_list).map(lambda x:(x,get_bandwidth_from_file(x))).collect()

# get darshan version from a file
def get_version_from_file(file_name):
    with open(file_name) as infile:
        line = infile.readline()
        version_match = version_pattern.match(line)
        if version_match is not None:
            return float(version_match.group(3))
        else: return -1

# get darshan version from list
def get_version(sc, parsed_list, cond_version = 3):
    return sc.parallelize(parsed_list).map(lambda x:(x,get_version_from_file(x)))\
        .filter(lambda x:x[1] >= cond_version).collect()


def get_lustre_stripe(sc, parsed_list):
    return sc.parallelize(parsed_list).map(lambda x:(x,get_lustre_stripe_from_file(x[0])))\
        .filter(lambda x:x[1][0] >= 0).collect()

def get_overlap_value(sc,overlap_list, option):
    return sc.parallelize(overlap_list).map(lambda x:get_avg_value_from_file(x[0], x[2], x[3], x[6], x[7], option)).collect()
    """ 
    for parsed_file in parsed_list:
        rdd = sc.textFile(parsed_file).map(parse_darshan_log_line)
        header_list = rdd.filter(lambda x:x[1] == -11).collect() 
        for ((k,v), flag) in header_list:                                                             
            if k == 'jobid': rdd_jobid = v                                                        
            elif k == 'start_time': rdd_start_time = v                                            
            elif k == 'end_time': rdd_end_time = v                                                
        overlap = getOverlap(start_time, end_time, rdd_start_time, rdd_end_time)
        if overlap is None:
            continue
        else:
            overlap_list.append((parsed_file, rdd_jobid, overlap[0], overlap[1]))
    print overlap_list
    print len(overlap_list)
    """
    #print sc.textFile(','.join(parsed_list)).take(2)
    #rdd = sc.parallelize(parsed_list).map(parse_darshan_log_line)
    #print len(parsed_list)
    #print path_list
    #print sc.wholeTextFiles(','.join(path_list)).take(2)
    #rdd = sc.textFile(parsed_list).map(parse_darshan_log_line)
    #print rdd.take(2)
    #print parsed_list
    
    #rdd = darshan_rdd.map(lambda x:sc.parallelize(x.split('\n')).map(parse_darshan_log_line))
    #print rdd.take(1)

    #new_rdd = rdd.filter(lambda x:x[1] == 1).map(lambda x:x[0])\
    #        .aggregateByKey((),lambda x,y: x+(y,), lambda x,y: x + y)\
    #        .mapValues(lambda x:iterCounters(x))
    #header_list = rdd.filter(lambda x:x[1] == -11).collect()
    #darshan_jobid = darshan_nprocs = darshan_start_time = darshan_end_time = 0
    #for ((k,v), flag) in header_list:
    #    if k == 'jobid': darshan_jobid = v
    #    elif k == 'start_time': darshan_start_time = v
    #    elif k == 'end_time': darshan_end_time = v
    #    elif k == 'nprocs': darshan_nprocs = v

    #return (rdd, new_rdd, darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs)
    """
    for darshan_log in file_list:
        print darshan_log
        parsed = subprocess.check_output(['darshan-parser','--all',darshan_log])
        print parsed
        break

    (rdd, new_rdd, darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs) = parse_darshan(sc, logname)
    print (darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs)

    my_keys = rdd.filter(lambda x:x[1] == 1).map(lambda x:x[0]).aggregateByKey((),lambda x,y:x+(y,), \
        lambda x,y:x+y).mapValues(lambda x:iterCounters(x)).map(lambda x:(x[0])).collect()
    return (new_rdd, my_keys, darshan_jobid, darshan_start_time, darshan_end_time, darshan_nprocs)
    """
#overlap_list = load_overlap_darshan(sc, '/global/cscratch1/sd/darshanlogs/', '/global/cscratch1/sd/wyoo/parsed_darshan/', 1465422770, 1465422868)
#overlap_values = get_overlap_value(overlap_list,'pw')
#get_overlap_value(overlap_list,'mw')
#get_overlap_value(overlap_list,'mr')
#get_overlap_value(overlap_list,'pr')
#(darshan_rdd, darshan_keys, darshan_jobid, darshan_start_ts, darshan_end_ts, darshan_nprocs) = load_darshan(sc, '/project/projectdirs/m888/ssio/wyoo_data/cori/darshan/3392828/glock_hacc_io_write_id3392828_1-5-38435-11600638631776972893_3.darshan.total', 3.1)
#test_darshan = '/Users/wyoo/ssio/asim-amrex_id5452597.darshan.base'
#(darshan_rdd, darshan_keys, darshan_jobid, darshan_start_ts, darshan_end_ts, darshan_nprocs, version) = load_darshan(sc, test_darshan, 3.1)
#print version
#print get_lustre_stripe_from_file(test_darshan)
