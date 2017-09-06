import datetime
import os
import re
import traceback
from pyspark.sql import Row
from os.path import expanduser
from operator import add
home = expanduser("~")


COMMENT_PATTERN = '(#...)'
DARSHAN_LOG_PATTERN = '(\S+)\t([+-]?\d+(?:\.\d+)?)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t(\S+)\t(\S+)'
#DARSHAN_LOG_PATTERN = '(\d+)\t(\d+)\t(\S+)\t(\d+)\t(\S+)\t(\S+)\t(\S+)'
NPROCS_PATTERN = '(#\s+nprocs:\s+)(\d+)'

def displaymatch(match):
    if match is None:
        return None
    return '<Match: %r, groups=%r>' % (match.group(), match.groups())

comment = re.compile(COMMENT_PATTERN)
valid = re.compile(DARSHAN_LOG_PATTERN)
nprocs_detailed = re.compile(NPROCS_PATTERN)


def test_darshan_log_line(logline):
    if len(logline) == 0:
        return (logline, 0)
    comment_match = comment.match(logline)
    match = valid.match(logline)
    if comment_match is not None:
        return (logline,-1)
    
    if match is None:
        print len(logline)
        print "Invalid logline: %s" % logline
        return (logline,-2)
    
    return (Row(
        module = match.group(1),
        rank = match.group(2),
        filehash = match.group(3),
        counter = match.group(4),
        value = match.group(5),
        suffix = match.group(6),
        mount = match.group(7),
        fs = match.group(8)        
    ),1)

def parse_darshan_log_line(logline):
    if len(logline) == 0:
        return (logline, 0)
    comment_match = comment.match(logline)
    match = valid.match(logline)
    if comment_match is not None:
        return (logline,-1)
    
    if match is None:
        print len(logline)
        print "Invalid logline: %s" % logline
        return (logline,-2)
    
    return ((
            match.group(3), # hashfile
            (
            match.group(4), # counter
            match.group(5), # value
            match.group(2), # rank
            match.group(6), # suffix
            match.group(7), # mount
            match.group(8), # fs
            match.group(1)  # module
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
    for v in x:
        kvMap[v[0]] = v[1]
    kvMap['rank'] = v[2]
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