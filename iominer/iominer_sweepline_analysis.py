# -*- coding: utf-8 -*-
"""
Created on Sat Dec 21 22:32:34 2019

@author: tzw00
"""
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import argparse
import subprocess
import os
import re
import ntpath
import math

print_file_cnt = 5
max_ticks_to_show = 10
max_ost_id = -1

# This class corresponds to each 
# Darshan record, i.e, for each rank, 
# Darshan has one record for each 
# of its file. If there are n ranks, 
# each rank access m files, then there 
# are nm such records.
class RankFileState(object):
    def __init__(self, 
                 rank = -1, 
                 id_ = -1, 
                 filename = "", 
                 start = 0, 
                 end = 0, 
                 interval = 0, 
                 data_size = 0):
        self.start = start
        self.end = end
        self.id = id
        self.rank = rank
        self.filename = filename
        self.interval = interval
        self.data_size = data_size


# This class generates a timing information 
# for each RankFileState record, which is 
# used by the sweep-line analysis.
class TPoints(object):
    def __init__(self,
                 id = -1,
                 start_time = -1,
                 interval = -1,
                 point_type = -1):
        self.id = id
        self.time = start_time
        self.interval = interval
        self.type = point_type
        
class PltPoints(object):
    def __init__(self, 
                 rank = -1, 
                 id_ = -1, 
                 filename = "", 
                 start = 0, 
                 end = 0, 
                 interval = 0, 
                 data_size = 0):
        self.start = start
        self.end = end
        self.id = id
        self.rank = rank
        self.filename = filename
        self.interval = interval
        self.data_size = data_size  

# In case of a rank issuing IO on multiple files, 
# and the I/O period on these multiple files are
# overlapped, we adjust this rank's IO period on 
# these files as sequentially aligned with each 
# other.
def AdjustTiming(tmp_list, rank):
    prev_point = tmp_list[0]
    for i in range(1, len(tmp_list)):
        cur_point = tmp_list[i]
        if cur_point.start < prev_point.end:
            cur_point.start = prev_point.end
            cur_point.end = cur_point.start + cur_point.interval
        prev_point = cur_point    

# sort the records in ds_rank_file_rstat_lst by
# rank, and each rank's io start time.
def ReshuffleArr(ds_rank_file_rstat_lst):
    rank_dict = {}
    for record in ds_rank_file_rstat_lst:
        if rank_dict.get(record.rank, -1) == -1:
            rank_dict[record.rank] = []
        rank_dict[record.rank].append(record)
       # print("record start:%d, interval:%d\n"%(record.start, record.interval))
  
    for key,value in rank_dict.items():
        cur_list = value
        cur_list.sort(key=lambda x: (x.start, -(x.end - x.start)))

        AdjustTiming(cur_list, key)
    
    result_arr = []
    for key,value in rank_dict.items():
        cur_list = value
        for record in cur_list:
            result_arr.append(record)
    return result_arr
    


def ExtractProcInfo(file_name):
    DARSHAN_RANK_FILE_PATTERN = '(\S+)\t([+-]?\d+(?:\.\d+)?)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t(\S+)\t(\S+)'
    ds_rank_file_pattern = re.compile(DARSHAN_RANK_FILE_PATTERN)
    
    DARSHAN_NPROCS_PATTERN = '(#\s+nprocs:\s+)(\d+)'
    ds_nprocs_pattern = re.compile(DARSHAN_NPROCS_PATTERN)
    suffix = file_name.rsplit('/')[-1]
    
    DARSHAN_PERF_PATTERN = '(#\s+)(\S+):\s+(\d+\.\d+)'
    ds_perf_pattern = re.compile(DARSHAN_PERF_PATTERN)
    
    DARSHAN_FILE_PATTERN = '([^#]+)(\s+)(\S+)(\s+)(\d+)(\s+(\S+)){11,}'
    ds_file_pattern = re.compile(DARSHAN_FILE_PATTERN)
  
    DARSHAN_HEADER_PATTERN = '(#\s+)(\S+):(\s+)(\d+)'
    ds_header_pattern = re.compile(DARSHAN_HEADER_PATTERN)
    
    DARSHAN_KV_PATTERN = '(\S+):(\s+)([+-]?\d+(?:\.\d+)?)'
    ds_kv_pattern = re.compile(DARSHAN_KV_PATTERN)
    
    job_id = ParseDarshanJobID(suffix)
    user_name = ParseDarshanUserName(suffix)
    
    # The ds_glb prefix represents the IO statistics
    # pertaining to a job
    ds_glb_dict = {}
    ds_glb_dict["job_id"] = job_id
    ds_glb_dict["file_name"] = file_name
    ds_glb_dict["user_name"] = user_name
    
    # wstat/rstat/wrstat are the 
    # list of all ranks' read/write/rw 
    # statistics on each file. E.g., 
    # rank1's read/write/rw on file1 is 
    # in one entry, rank1's read/write/rw 
    # on file2 is in another entry.
    # Each item is an IOStat object for a rank
    ds_rank_file_wstat_lst = []
    ds_rank_file_rstat_lst = []
    ds_rank_file_wrstat_lst = []
    
    # The IO statistics for each rank
    ds_rank_stat_dict = {}
    
    # The IO statistics for each file
    ds_file_stat_dict = {}
    
    # The IO statistics for each OST
    ds_ost_dict = {}
    
    # The list of OSTs used in the job
    ds_used_ost = []
    
   
    # Each hash bucket contains a rank's io size (read size, write size, total size)
    ds_rank_io = {} 
    counter = 0
    
    global max_ost_id
        
    with open(file_name) as infile:
        for line in infile:          
            header_match = ds_header_pattern.match(line)
            if header_match is not None:
                counter_key = header_match.group(2)
                counter_val = header_match.group(4)
                if counter_key == "start_time" or counter_key == "end_time":
                    ds_glb_dict[counter_key] = counter_val
                    continue
                
            kv_match = ds_kv_pattern.match(line)
            if kv_match is not None:
                counter_key = kv_match.group(1)
                counter_val = kv_match.group(3)
                ds_glb_dict[counter_key] = counter_val
                continue
            
            nprocs_match = ds_nprocs_pattern.match(line)
            if nprocs_match is not None:
                nprocs = int(nprocs_match.group(2))
                ds_glb_dict["nprocs"] = str(nprocs)
                continue
            
            rank_file_pattern_match = ds_rank_file_pattern.match(line)
            if rank_file_pattern_match is not None:
                cur_rank = int(rank_file_pattern_match.group(2))
                file_name = rank_file_pattern_match.group(6).strip()
                io_type = rank_file_pattern_match.group(1)
                key_prefix = io_type
                if "POSIX" in io_type:
                    key_prefix = "POSIX_"
                if "STDIO" in io_type:
                    key_prefix = "STDIO_"
                if "MPIIO" in io_type:
                    key_prefix = "MPIIO_"
                
                if file_name == "<STDOUT>" or file_name == "<STDIN>" or file_name == "<STDERR>":
                    # we rule out the IO on STDOUT/STDIN
                    continue
                per_file_key = rank_file_pattern_match.group(4).strip()
                per_file_val = rank_file_pattern_match.group(5).strip()
                if per_file_key == key_prefix+"BYTES_READ":
                    read_size = int(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"BYTES_WRITTEN":
                    write_size = int(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_READ_START_TIMESTAMP":
                    cur_read_start = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_READ_END_TIMESTAMP":
                    cur_read_end = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_WRITE_START_TIMESTAMP":
                    cur_write_start = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_WRITE_END_TIMESTAMP":
                    cur_write_end = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_READ_TIME":
                    cur_read_time = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_WRITE_TIME":
                    cur_write_time = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_SLOWEST_RANK_TIME":
                   # if file_name == "/global/cscratch1/sd/gaoheng/calc/QE/Ge3Bi2Te6/wannier/tmp/GeBiTe.save/data-file-schema.xml":
                   #     print ("###file_name:%s, key:%s, value:%s, cur_rank:%d\n"%(file_name, per_file_key, per_file_val, cur_rank))
                    cur_io_time = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_OPEN_START_TIMESTAMP":
                    cur_io_start = float(per_file_val)
                    counter += 1
                if per_file_key == key_prefix+"F_META_TIME":
                    cur_meta_time = float(per_file_val)
                    counter += 1
                if counter == 11:
                    if ds_rank_io.get(cur_rank, -2) == -2:
                        # We use -2 here because cur_rank can be -1 in the shared io case
                        ds_rank_io[cur_rank] = [0, 0, 0]
                    if read_size != 0:
                        # We temporarily set id as -1
                        ds_rank_file_rstat_lst.append(RankFileState(cur_rank, 
                                                                    -1,
                                                                    file_name, 
                                                                    cur_read_start,
                                                                    cur_read_end, 
                                                                    cur_read_time, 
                                                                    read_size))
                        
                    if write_size != 0:
                        ds_rank_file_wstat_lst.append(RankFileState(cur_rank, 
                                                                    -1,
                                                                    file_name,
                                                                    cur_write_start,
                                                                    cur_write_end,
                                                                    cur_write_time,
                                                                    write_size))
                    if "MPIIO" not in io_type:
                        ds_rank_io[cur_rank][0] += read_size
                        ds_rank_io[cur_rank][1] += write_size
                        ds_rank_io[cur_rank][2] += (read_size + write_size)
                    
                    if (read_size != 0 or write_size != 0 or cur_meta_time != 0): 
                        # This counts each darshan record's total read/write/metadata time
                        if cur_rank == -1:
                            # SHARED IO that involves all the processes
                            if cur_io_time != 0:
                                ds_rank_file_wrstat_lst.append(RankFileState(cur_rank, 
                                                                             -1,
                                                                             file_name,
                                                                             cur_io_start,
                                                                             cur_io_start + cur_io_time,
                                                                             cur_io_time,
                                                                             write_size + read_size))
                        else:
                            # Individual IO
                            cur_io_time = cur_meta_time + cur_read_time + cur_write_time
                            ds_rank_file_wrstat_lst.append(RankFileState(cur_rank, 
                                                                         -1,
                                                                         file_name,
                                                                         cur_io_start,
                                                                         cur_io_start + cur_io_time,
                                                                         cur_io_time, 
                                                                         write_size + read_size))
                            
                     
                    counter = 0  
                if "LUSTRE" not in per_file_key:
                    # print("######cur_rank is %d######\n"%cur_rank)
                    if ds_rank_stat_dict.get(cur_rank, -2) == -2:
                        ds_rank_stat_dict[cur_rank] = {}
                  #      print("resetting 1, file:%s, rank:%d\n"%(file_name, cur_rank))
                    if ds_rank_stat_dict[cur_rank].get(file_name, -1) == -1:
                        ds_rank_stat_dict[cur_rank][file_name] = {}
                  #      print("resetting 2, file:%s, rank:%d\n"%(file_name, cur_rank))
                    ds_rank_stat_dict[cur_rank][file_name][per_file_key] = per_file_val
                  #  if "/global/cscratch1/sd/chenb/vasp/batteryrunner/allpaths_harvest2/Li4Cr3CoO8_mp-769750_3267_neb_path34-55_variant0/00/INCAR" in file_name:
                  #      print("here, rank:%d, file name:%s, key:%s, value:%s, count:%d\n"%(cur_rank, file_name, per_file_key, per_file_val, len(ds_rank_stat_dict[cur_rank][file_name].items())))
                    
                if "LUSTRE_OST_ID_" in per_file_key:
                    ost_id_in_fs = int(per_file_val)
 #                   print("##########file name is %s, id:%d, key:%s\n"%(file_name, ost_id_in_fs, per_file_key))
                    ost_id_in_file = per_file_key.rfind('_')
                    if ds_ost_dict.get(ost_id_in_fs, -1) == -1:
                        ds_ost_dict[ost_id_in_fs] = {}
                    if ds_ost_dict[ost_id_in_fs].get(file_name, -1) == -1:
                        ds_ost_dict[ost_id_in_fs][file_name] = per_file_key[ost_id_in_file + 1:]


                    if ost_id_in_fs > max_ost_id:
                        max_ost_id = ost_id_in_fs
                      
                    if ost_id_in_fs not in ds_used_ost:
                        ds_used_ost.append(ost_id_in_fs)
                    continue

                    
                if "LUSTRE_STRIPE_WIDTH" in per_file_key:
                    if ds_file_stat_dict.get(file_name, -1) == -1:
                        ds_file_stat_dict[file_name] = {}
                    ds_file_stat_dict[file_name][per_file_key] = str(per_file_val)
                    continue
                if "LUSTRE_STRIPE_SIZE" in per_file_key:
                    if ds_file_stat_dict.get(file_name, -1) == -1:
                        ds_file_stat_dict[file_name] = {}
                    ds_file_stat_dict[file_name][per_file_key] = str(per_file_val)
                    continue
            else:               
                perf_pattern_match = ds_perf_pattern.match(line)
                if perf_pattern_match is not None:
                    tmp_key = key_prefix + perf_pattern_match.group(2)
                    tmp_val = perf_pattern_match.group(3)
                    ds_glb_dict[tmp_key] = tmp_val
                    continue
            
                file_pattern_match = ds_file_pattern.match(line)
                if file_pattern_match is not None:
                    file_name = line.split("\t")[1]
                    fnprocs = line.split("\t")[2]
                    if ds_file_stat_dict.get(file_name, -1) == -1:
                        ds_file_stat_dict[file_name] = {}
                    ds_file_stat_dict[file_name]["nprocs"] = fnprocs
                    
    ds_glb_dict["ost_cnt"] = len(ds_used_ost)
    
   # for cur_rank, file_info in ds_rank_stat_dict.items():
   #     for file_name, counters in ds_rank_stat_dict[cur_rank].items():
   #         if "/global/cscratch1/sd/chenb/vasp/batteryrunner/allpaths_harvest2/Li4Cr3CoO8_mp-769750_3267_neb_path34-55_variant0/00/INCAR" in file_name:
   #              for key,value in ds_rank_stat_dict[cur_rank][file_name].items():
   #                  print("here, rank:%d, file name:%s, key:%s, value:%s, len:%d\n"%(cur_rank, file_name, key, value, len(ds_rank_stat_dict[cur_rank][file_name])))
    # The for loop below generate more statistics for each file, i.e.,
    # ds_file_stat_dict, by summarizing the statisics from ds_rank_stat_dict
    for cur_rank, file_info in ds_rank_stat_dict.items():
        for file_name, counters in ds_rank_stat_dict[cur_rank].items():
           # print("filename is %s"%file_name)
            if ds_file_stat_dict.get(file_name, -2) == -2:
                continue
            if ds_file_stat_dict[file_name].get("nprocs", -2) != -2:
                for key,value in ds_rank_stat_dict[cur_rank][file_name].items():
                    if ds_file_stat_dict[file_name].get(key, -100) == -100:
                        ds_file_stat_dict[file_name][key] = value
                        continue

                    if key == "POSIX_F_SLOWEST_RANK_TIME":
                        slowest_rank_time = float(ds_file_stat_dict[file_name]["POSIX_F_SLOWEST_RANK_TIME"])
                        cur_read_time = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_F_READ_TIME"])
                        cur_write_time = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_F_WRITE_TIME"])
                        cur_meta_time = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_F_META_TIME"])
                        cur_io_time = cur_read_time + cur_write_time + cur_meta_time
                        
                        write_size = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_BYTES_WRITTEN"])
                        read_size = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_BYTES_READ"])
                        
                        if cur_rank != -1 and slowest_rank_time < cur_io_time:
                            ds_file_stat_dict[file_name]["POSIX_F_SLOWEST_RANK_TIME"] = str(cur_io_time)
                            ds_file_stat_dict[file_name]["POSIX_SLOWEST_RANK_BYTES"] = str(write_size + read_size)
                            slowest_rank = cur_rank  
                            ds_file_stat_dict[file_name]["POSIX_SLOWEST_RANK"] = str(slowest_rank)
                        continue
                                    
                    if key == "POSIX_MAX_BYTE_WRITTEN":
                        max_byte_written = int(ds_file_stat_dict[file_name]["POSIX_MAX_BYTE_WRITTEN"])
                        if cur_rank != -1 and int(ds_rank_stat_dict[cur_rank][file_name]["POSIX_MAX_BYTE_WRITTEN"]) > max_byte_written:
                            ds_file_stat_dict[file_name]["POSIX_MAX_BYTE_WRITTEN"] = ds_rank_stat_dict[cur_rank][file_name]["POSIX_MAX_BYTE_WRITTEN"]
                       
                        continue
                     
                    if key == "POSIX_MAX_BYTE_READ":
                        max_byte_read = int(ds_file_stat_dict[file_name]["POSIX_MAX_BYTE_READ"])
                        if cur_rank != -1 and int(ds_rank_stat_dict[cur_rank][file_name]["POSIX_MAX_BYTE_READ"]) > max_byte_read:
                            ds_file_stat_dict[file_name]["POSIX_MAX_BYTE_READ"] = ds_rank_stat_dict[cur_rank][file_name]["POSIX_MAX_BYTE_READ"]
                        continue
                            
                    if cur_rank != -1:
                            ds_file_stat_dict[file_name][key] = str(float(ds_file_stat_dict[file_name][key])\
                                + float(ds_rank_stat_dict[cur_rank][file_name][key]))    
                      

    shared_io_lst = []
    individual_io_lst = []
    
    # The following adjust the io end based on io start and the real io time.
    # The original io end time in Darshan is POSIX_F_IO_END_TIMESTAMP.
    # One way to calculate the 
    # io time is to calculate the delta between POSIX_F_IO_END_TIMESTAMP
    # and POSIX_F_IO_START_TIMESTAMP, but there can be non-IO time between 
    # START_TIMESTAMP and END_TIMESTAP. In calculating the real IO time, 
    # we need to remove these intervals. For N-1 pattern, we use the 
    # SLOWEST_RANK_IO_TIME as the real io time, and we adjust the 
    # io end time as io start time + real io time. For N-M pattern, 
    # we use POSIX_F_IO_TIME as the real io time, and we adjust the
    # io end time as io start time + real io time.
    for i in range(0, len(ds_rank_file_rstat_lst)):
        # This adjusts the io time for read, i.e., records in ds_rank_file_rstat_lst
        if ds_rank_file_rstat_lst[i].rank == -1:
            # This file's IO pattern is N-1
        #    if ds_rank_stat_dict[ds_rank_file_rstat_lst[i].rank][ds_rank_file_rstat_lst[i].filename].get("POSIX_F_SLOWEST_RANK_TIME", -1) == -1:
        #        print("Darshan log is missing POSIX_F_SLOWEST_RANK_TIME for file:%s rank:%d\n"%(ds_rank_file_rstat_lst[i].rank, ds_rank_file_rstat_lst[i].filename))
            if (ds_rank_stat_dict[ds_rank_file_rstat_lst[i].rank][ds_rank_file_rstat_lst[i].filename].get("POSIX_F_SLOWEST_RANK_TIME", -1) != -1):
                slowest_rank_time = float(ds_rank_stat_dict[ds_rank_file_rstat_lst[i].rank][ds_rank_file_rstat_lst[i].filename]["POSIX_F_SLOWEST_RANK_TIME"])
            else:
                slowest_rank_time = 0  
            
            if (ds_rank_stat_dict[ds_rank_file_rstat_lst[i].rank][ds_rank_file_rstat_lst[i].filename].get("POSIX_BYTES_WRITTEN", -1) != -1):
                write_size = float(ds_rank_stat_dict[ds_rank_file_rstat_lst[i].rank][ds_rank_file_rstat_lst[i].filename]["POSIX_BYTES_WRITTEN"])
            else:
                write_size = 0
                    
            if (ds_rank_stat_dict[ds_rank_file_rstat_lst[i].rank][ds_rank_file_rstat_lst[i].filename].get("POSIX_BYTES_READ", -1) != -1):
                
                read_size = float(ds_rank_stat_dict[ds_rank_file_rstat_lst[i].rank][ds_rank_file_rstat_lst[i].filename]["POSIX_BYTES_READ"])
            else:
                read_size = 0


 #           print("######rank:%d, file:%s, interval:%lf, start:%lf, end:%lf, slowest_rank_time:%lf, delta:%lf, read_size:%lf, write_size:%lf"%(ds_rank_file_rstat_lst[i].rank,
 #                 ds_rank_file_rstat_lst[i].filename,
 #                 ds_rank_file_rstat_lst[i].interval,
 #                 ds_rank_file_rstat_lst[i].start,
 #                 ds_rank_file_rstat_lst[i].end,
 #                 slowest_rank_time,
 #                 ds_rank_file_rstat_lst[i].end - ds_rank_file_rstat_lst[i].start,
 #                 read_size,
 #                 write_size))
            if (slowest_rank_time < ds_rank_file_rstat_lst[i].end - ds_rank_file_rstat_lst[i].start) and (read_size + write_size != 0):
                # This means for N-1 pattern, processes are not doing I/O all the time between
                # read_start and read_end, there should be some idle time. To approximate the 
                # real read time, we pick the slowest rank I/O time, and proportionally calculate 
                # the read time based on read size and write size.
                interval = slowest_rank_time * (read_size/(read_size + write_size))
                ds_rank_file_rstat_lst[i].interval = interval
      #          print("---rank:%d, file:%s, interval:%lf, end:%lf\n"%(ds_rank_file_rstat_lst[i].rank,
      #                ds_rank_file_rstat_lst[i].filename,
      #                interval,
      #                ds_rank_file_rstat_lst[i].end))
                # we adjust the io end time for N-1 pattern
                ds_rank_file_rstat_lst[i].end = ds_rank_file_rstat_lst[i].start + interval
                
            continue
        # we adjust the io end time for N-M pattern
        ds_rank_file_rstat_lst[i].end = ds_rank_file_rstat_lst[i].start + ds_rank_file_rstat_lst[i].interval   

    ds_rank_file_rstat_lst = ReshuffleArr(ds_rank_file_rstat_lst) 


    for i in range(0, len(ds_rank_file_rstat_lst)):
        if ds_rank_file_rstat_lst[i].rank != -1:
            individual_io_lst.append(RankFileState(ds_rank_file_rstat_lst[i].rank, ds_rank_file_rstat_lst[i].id,\
                                                   ds_rank_file_rstat_lst[i].filename, ds_rank_file_rstat_lst[i].start, \
                                                   ds_rank_file_rstat_lst[i].end, ds_rank_file_rstat_lst[i].interval,\
                                                   ds_rank_file_rstat_lst[i].data_size))
        else:
            shared_io_lst.append(RankFileState(ds_rank_file_rstat_lst[i].rank, ds_rank_file_rstat_lst[i].id,\
                                               ds_rank_file_rstat_lst[i].filename, ds_rank_file_rstat_lst[i].start, \
                                               ds_rank_file_rstat_lst[i].end, ds_rank_file_rstat_lst[i].interval,\
                                               ds_rank_file_rstat_lst[i].data_size))
            
  #  for i in range(0, len(shared_io_lst)):
  #      print("######shared rank:%d, file:%s, interval:%lf, start:%lf, end:%lf, slowest_rank_time:%lf, delta:%lf, read_size:%lf, write_size:%lf"%(shared_io_lst[i].rank,
  #                shared_io_lst[i].filename,
  #                shared_io_lst[i].interval,
  #                shared_io_lst[i].start,
  #                shared_io_lst[i].end,
  #                slowest_rank_time,
  #                shared_io_lst[i].end - shared_io_lst[i].start,
  #                read_size,
  #                write_size))        
    for i in range(0, len(shared_io_lst)):
        # For N-1 pattern, we create one record for each rank, all with the same start and end
        for j in range(0, nprocs):
            individual_io_lst.append(RankFileState(j, -1, shared_io_lst[i].filename,\
                                     shared_io_lst[i].start, shared_io_lst[i].end,\
                                     shared_io_lst[i].interval, shared_io_lst[i].data_size))
    
    ds_rank_file_rstat_lst = individual_io_lst
    ds_rank_file_rstat_lst = ReshuffleArr(ds_rank_file_rstat_lst) 
    
    ds_rank_file_rstat_lst.sort(key=lambda x: (x.start, -(x.end - x.start)))
    counter = 0
    for i in range(0, len(ds_rank_file_rstat_lst)):
        ds_rank_file_rstat_lst[i].id = counter
        counter += 1
    
    
    shared_io_lst = []
    individual_io_lst = []
    for i in range(0, len(ds_rank_file_wstat_lst)):
        # This adjust the io time for write, i.e., records in ds_rank_file_wstat_lst
        if ds_rank_file_wstat_lst[i].rank == -1:
            if (ds_rank_stat_dict[ds_rank_file_wstat_lst[i].rank][ds_rank_file_wstat_lst[i].filename].get("POSIX_F_SLOWEST_RANK_TIME", -1) != -1):
                slowest_rank_time = float(ds_rank_stat_dict[ds_rank_file_wstat_lst[i].rank][ds_rank_file_wstat_lst[i].filename]["POSIX_F_SLOWEST_RANK_TIME"])
            else:
                slowest_rank_time = 0
              
            if (ds_rank_stat_dict[ds_rank_file_wstat_lst[i].rank][ds_rank_file_wstat_lst[i].filename].get("POSIX_BYTES_WRITTEN", -1) != -1):
                write_size = float(ds_rank_stat_dict[ds_rank_file_wstat_lst[i].rank][ds_rank_file_wstat_lst[i].filename]["POSIX_BYTES_WRITTEN"])
            else:
                write_size = 0
                    
            if (ds_rank_stat_dict[ds_rank_file_wstat_lst[i].rank][ds_rank_file_wstat_lst[i].filename].get("POSIX_BYTES_READ", -1) != -1):
                read_size = float(ds_rank_stat_dict[ds_rank_file_wstat_lst[i].rank][ds_rank_file_wstat_lst[i].filename]["POSIX_BYTES_READ"])
            else:
                read_size = 0

            if slowest_rank_time < ds_rank_file_wstat_lst[i].end - ds_rank_file_wstat_lst[i].start and (read_size + write_size != 0):
                interval = slowest_rank_time * (write_size/(read_size + write_size))
                ds_rank_file_wstat_lst[i].interval = interval
                ds_rank_file_wstat_lst[i].end = ds_rank_file_wstat_lst[i].start + interval
            continue
        ds_rank_file_wstat_lst[i].end = ds_rank_file_wstat_lst[i].start + ds_rank_file_wstat_lst[i].interval
    ds_rank_file_wstat_lst = ReshuffleArr(ds_rank_file_wstat_lst) 
    
    for i in range(0, len(ds_rank_file_wstat_lst)):
        if ds_rank_file_wstat_lst[i].rank != -1:
            individual_io_lst.append(RankFileState(ds_rank_file_wstat_lst[i].rank, ds_rank_file_wstat_lst[i].id,\
                                                   ds_rank_file_wstat_lst[i].filename, ds_rank_file_wstat_lst[i].start, \
                                                   ds_rank_file_wstat_lst[i].end, ds_rank_file_wstat_lst[i].interval,\
                                                   ds_rank_file_wstat_lst[i].data_size))
        else:
            shared_io_lst.append(RankFileState(ds_rank_file_wstat_lst[i].rank, ds_rank_file_wstat_lst[i].id,\
                                               ds_rank_file_wstat_lst[i].filename, ds_rank_file_wstat_lst[i].start, \
                                               ds_rank_file_wstat_lst[i].end, ds_rank_file_wstat_lst[i].interval,\
                                               ds_rank_file_wstat_lst[i].data_size))
    ds_rank_file_wstat_lst = individual_io_lst
    
    for i in range(0, len(shared_io_lst)):
        for j in range(0, nprocs):
            ds_rank_file_wstat_lst.append(RankFileState(j, -1, shared_io_lst[i].filename,\
                                     shared_io_lst[i].start, shared_io_lst[i].end,\
                                     shared_io_lst[i].interval, shared_io_lst[i].data_size))

    ds_rank_file_wstat_lst = ReshuffleArr(ds_rank_file_wstat_lst) 
    
    ds_rank_file_wstat_lst.sort(key=lambda x: (x.start, -(x.end - x.start)))
    counter = 0
    for i in range(0, len(ds_rank_file_wstat_lst)):
        ds_rank_file_wstat_lst[i].id = counter
        counter += 1
         
    shared_io_lst = []
    individual_io_lst = []
    for i in range(0, len(ds_rank_file_wrstat_lst)):
        # This adjust the io time for total io time, i.e., records in ds_rank_file_wrstat_lst
        if ds_rank_file_wrstat_lst[i].rank == -1:
            shared_io_lst.append(RankFileState(ds_rank_file_wrstat_lst[i].rank, ds_rank_file_wrstat_lst[i].id,\
                                               ds_rank_file_wrstat_lst[i].filename, ds_rank_file_wrstat_lst[i].start, \
                                               ds_rank_file_wrstat_lst[i].end, ds_rank_file_wrstat_lst[i].interval,\
                                               ds_rank_file_wrstat_lst[i].data_size)) 
            continue
        individual_io_lst.append(RankFileState(ds_rank_file_wrstat_lst[i].rank, ds_rank_file_wrstat_lst[i].id,\
                                               ds_rank_file_wrstat_lst[i].filename, ds_rank_file_wrstat_lst[i].start,\
                                               ds_rank_file_wrstat_lst[i].end, ds_rank_file_wrstat_lst[i].interval,\
                                               ds_rank_file_wrstat_lst[i].data_size))
    ds_rank_file_wrstat_lst = individual_io_lst

    for i in range(0, len(shared_io_lst)):
        for j in range(0, nprocs):
            ds_rank_file_wrstat_lst.append(RankFileState(j, -1, shared_io_lst[i].filename,\
                                                         shared_io_lst[i].start, shared_io_lst[i].end,\
                                                         shared_io_lst[i].interval))
    
    ds_rank_file_wrstat_lst = ReshuffleArr(ds_rank_file_wrstat_lst) 
    ds_rank_file_wrstat_lst.sort(key=lambda x: (x.start, -(x.end - x.start)))
  
    counter = 0
    for i in range(0, len(ds_rank_file_wrstat_lst)):
        ds_rank_file_wrstat_lst[i].id = counter
        counter += 1
        
    ret_dict = {}
    ret_dict["global_dict"] = ds_glb_dict
    ret_dict["rank_file_wstat_lst"] = ds_rank_file_wstat_lst
    ret_dict["rank_file_rstat_lst"] = ds_rank_file_rstat_lst
    ret_dict["rank_file_wrstat_lst"] = ds_rank_file_wrstat_lst
    ret_dict["ds_rank_stat_dict"] = ds_rank_stat_dict
    ret_dict["ds_file_stat_dict"] = ds_file_stat_dict
    ret_dict["ds_ost_dict"] = ds_ost_dict
    ret_dict["ds_used_ost_lst"] = ds_used_ost 
    ret_dict["ds_rank_io"] = ds_rank_io
        
    return ret_dict

def write_log(fmt_str):
    try:
        log_fd.write(fmt_str)
        return 0
    except:
        print("Fail to write log file\n")
        return 1

def GetLineSize(line_tuples):
    line_size = 0
    for tmp_tuple in line_tuples:
        line_size += tmp_tuple[1] - tmp_tuple[0]
    return line_size

# normalize the I/O time to a given percent of total time
def NormalizeLineTuples(tot_lpoints,
                        sweep_line_set,
                        line_tuples):
    min_start = sweep_line_set[0].start
    min_end = sweep_line_set[len(sweep_line_set) - 1].end
    interval = min_end - min_start
    
    line_size = float(GetLineSize(line_tuples))
    blank_size = interval - line_size
    if line_size >= interval - line_size:
        # io time >= 50% of the time span of covering set
        norm_line_size = float(interval) * 0.6
    else:
        # io time < 50% of the time span of covering set
        norm_line_size = float(interval) * 0.4
    norm_blank_size = interval - norm_line_size

    norm_line_tuples = []

    tmp_start = line_tuples[0][0]
    for idx in range(0, len(line_tuples)):
        tmp_end = tmp_start + float(line_tuples[idx][1] - line_tuples[idx][0]) * norm_line_size / line_size
        norm_line_tuples.append((tmp_start, tmp_end))
        if idx != len(line_tuples) - 1:
            if blank_size == 0:
                tmp_start = tmp_end
            else:
                tmp_start = tmp_end + (line_tuples[idx + 1][0]\
                         - line_tuples[idx][1]) * float(norm_blank_size)/blank_size
    return (norm_line_tuples, norm_line_size, line_size)

def GenNewTicksLabels(old_ticks, old_labels, max_ticks):
    new_ticks = []
    new_labels = []
    
    interval = int(len(old_ticks)/max_ticks)

    
    if interval == 0:
        interval = 1
    
    cursor = 0
    while True:
        new_ticks.append(old_ticks[cursor])
        new_labels.append(old_labels[cursor])
        cursor += interval
        if cursor >= len(old_ticks):
            break
    return (new_ticks, new_labels)
        
    

# calculate the lines on the covering set.
def GenSweepLine(ds_rank_file_stat_lst):
    counter = 0
    plt_points = []
    time_lines = []
    sweep_dict = {}
    if len(ds_rank_file_stat_lst) == 0:
        return (None, None, None, None, None, None)
    
          #          print("---rank:%d, file:%s, interval:%lf, end:%lf\n"%(ds_rank_file_rstat_lst[i].rank,
      #                ds_rank_file_rstat_lst[i].filename,
      #                interval,
      #                ds_rank_file_rstat_lst[i].end))

    #print("len:%d\n"%(len(ds_rank_file_stat_lst)))
    cursor = 0
    for i in range(0, len(ds_rank_file_stat_lst)):
   #     print("rank:%d, id:%d, filename:%s, start:%lf, end:%lf, data_size:%lf\n"%(ds_rank_file_stat_lst[i].rank, ds_rank_file_stat_lst[i].id, ds_rank_file_stat_lst[i].filename, ds_rank_file_stat_lst[i].start, ds_rank_file_stat_lst[i].end, ds_rank_file_stat_lst[i].data_size))
       
        plt_points.append(PltPoints(ds_rank_file_stat_lst[i].rank,
                                   ds_rank_file_stat_lst[i].id,
                                   ds_rank_file_stat_lst[i].filename,
                                   ds_rank_file_stat_lst[i].start,
                                   ds_rank_file_stat_lst[i].end,
                                   ds_rank_file_stat_lst[i].end - ds_rank_file_stat_lst[i].start,
                                   ds_rank_file_stat_lst[i].data_size))
        
 
     #   print("appending benchmark, start:%f, interval:%f, i:%d\n"%(ds_rank_file_stat_lst[i].start, ds_rank_file_stat_lst[i].end - ds_rank_file_stat_lst[i].start, i))
        
        time_lines.append(TPoints(ds_rank_file_stat_lst[i].id,
                                  ds_rank_file_stat_lst[i].start,
                                  ds_rank_file_stat_lst[i].end - ds_rank_file_stat_lst[i].start,
                                  0))
     #   print("appending, start:%f, interval:%f, i:%d\n"%(time_lines[cursor].time, time_lines[cursor].interval, i))
        cursor +=1 
        time_lines.append(TPoints(ds_rank_file_stat_lst[i].id,
                                  ds_rank_file_stat_lst[i].end,
                                  ds_rank_file_stat_lst[i].end - ds_rank_file_stat_lst[i].start,
                                  1))
     #   print("appending, start:%f, interval:%f, i:%d\n"%(time_lines[cursor].time, time_lines[cursor].interval, i))
        cursor += 1
  #  for i in  range(0, len(time_lines)):
  #      print("start:%lf, interval：%lf\n"%(time_lines[i].time, time_lines[i].interval))
    time_lines.sort(key=lambda x: (x.time, -x.type, -x.interval))
    

    
    max_time = -1
    counter = 0
    sweep_line_set = []
    
    line_tuples = []
    last_end = -1
  
    for line_point in time_lines:
        if line_point.type == 0:
            sweep_dict[line_point.id] = line_point
            counter += 1
            if counter == 1:
                line_start = plt_points[line_point.id].start
                last_id = line_point.id
                last_end = plt_points[last_id].end
                sweep_line_set.append(plt_points[last_id])
                
        if line_point.type == 1:
            counter -= 1
            if counter == 0:
                line_end = line_point.time
                line_tuples.append((line_start, line_end, plt_points[line_point.id].rank))
     
            del sweep_dict[line_point.id]
            max_time = -1
            if line_point.id == last_id:
                if len(sweep_dict) > 0:
                    for key, value in sweep_dict.items():
                        if plt_points[key].end <= last_end:
                            continue
                        if sweep_dict[key].interval > max_time:
                            max_id = key
                            max_time = sweep_dict[key].interval
                    if max_time > 0:
                        last_id = max_id
                        sweep_line_set.append(plt_points[last_id])
                        last_end = plt_points[last_id].end

  #  for cur_point in sweep_line_set:
  #      print ("sweepline file:%s, start:%lf, end:%lf, interval:%lf, size:%ld, rank:%d\n"%(cur_point.filename,\
  #                                                                     cur_point.start, cur_point.end,\
  #                                                                               cur_point.end - cur_point.start,\
  #                                                                                         cur_point.data_size, cur_point.rank))
                        
    plt_points.sort(key=lambda x: (x.start, -x.end))
    return (plt_points, sweep_line_set, line_tuples)   

def CalMaxIO(ds_rank_io):
    max_rank_read = 0
    max_rank_write = 0
    max_rank_io = 0
    max_r_rank = -1
    max_w_rank = -1
    max_io_rank = -1
    tot_read = 0
    tot_write = 0
    tot_io = 0
    for key,value in ds_rank_io.items():
        if key == -1:
            continue
        tot_read += value[0]
        tot_write += value[1]
        tot_io += value[2]
        if value[0] > max_rank_read:
            max_rank_read = value[0]
            max_r_rank = key
        if value[1] > max_rank_write:
            max_rank_write = value[1]
            max_w_rank = key
        if value[2] > max_rank_io:
            max_rank_io = value[2]
            max_io_rank = key
    
    return (max_rank_read,\
            max_rank_write,\
            max_rank_io,\
            tot_read,\
            tot_write,\
            tot_io,\
            max_r_rank,\
            max_w_rank,\
            max_io_rank)

# calculate I/O contributing factors for the whole job, as well as the individual files
# if prefix == "total_", the the contributing factors are calculated for the whole job
# if prefix != "total_", the contributing factors are calculated for the individual file accessed by a rank, or all ranks (-1)
def ExtractFactors(prefix, \
                   ds_rank_stat_dict, \
                   ds_glb_dict, \
                   ds_file_stat_dict, \
                   ds_rank_io, \
                   ds_tot_rtime, \
                   ds_tot_wtime, \
                   ds_tot_iotime, \
                   line_point = None):
    record = {}

    if prefix == "total_":
        record = ds_glb_dict
    else:
        if line_point != None:
            rank = line_point.rank
            file = line_point.filename
            if ds_rank_stat_dict.get(rank, -1) == -1 or \
               ds_rank_stat_dict[rank].get(file, -1) == -1 or \
               (ds_rank_stat_dict[rank][file].get("POSIX_OPENS", -1) == -1 and \
                ds_rank_stat_dict[rank][file].get("STDIO_OPENS", -1) == -1):
                   # In the case that point.rank accessing point.file is not found in 
                   # ds_rank_stat_dict, or it is found but point.rank's stat on point.file is 
                   # not exist in the Darshan log, or point.rank's stat on point.file is 
                   # invalid (verified by retrieving POSIX_SIZE_READ_0_100),
                   # there should be a record for point.file with rank number -1 in
                   # the Darshan log, meaning it is
                   # a shared file (Note we have unfolded those "-1" records for each rank in
                   # generating ds_rank_file_stat_lst, the list used to produce the
                   # sweep line, that's why the rank may not originally appear in ds_rank_stat_dict).
                   # In all other cases, there should be something wrong with the Darshan log.
                   if ds_rank_stat_dict.get(-1, -1) == -1 or \
                      ds_rank_stat_dict[-1].get(file, -1) == -1:
                          print("Invalid Darshan format for file %s\n"%line_point.filename)
                          return;
                   else:
                       record = ds_rank_stat_dict[-1][file] # shared file marked by -1 as rank
            else:
                record = ds_rank_stat_dict[rank][file]

    str_pos_read_time = "%sPOSIX_F_READ_TIME"%(prefix)
    str_pos_write_time = "%sPOSIX_F_WRITE_TIME"%(prefix)
    str_pos_meta_time = "%sPOSIX_F_META_TIME"%(prefix)
    str_nprocs = "nprocs"
    str_pos_opens = "%sPOSIX_OPENS"%(prefix)
    str_pos_stats = "%sPOSIX_STATS"%(prefix)
    str_pos_fsync = "%sPOSIX_FSYNCS"%(prefix)
    str_pos_seeks = "%sPOSIX_SEEKS"%(prefix)
    
    str_pos_reads = "%sPOSIX_READS"%(prefix)
    str_pos_writes = "%sPOSIX_WRITES"%(prefix)
    str_pos_read_size = "%sPOSIX_BYTES_READ"%(prefix)
    str_pos_write_size = "%sPOSIX_BYTES_WRITTEN"%(prefix)
    str_pos_bw = "POSIX_agg_perf_by_slowest"
    str_pos_seq_r = "%sPOSIX_CONSEC_READS"%(prefix)
    str_pos_seq_w = "%sPOSIX_CONSEC_WRITES"%(prefix)
    str_tot_posix_size_0_100_r = "%sPOSIX_SIZE_READ_0_100"%(prefix)
    str_tot_posix_size_0_100_w = "%sPOSIX_SIZE_WRITE_0_100"%(prefix)
    str_tot_posix_size_100_1k_r = "%sPOSIX_SIZE_READ_100_1K"%(prefix)
    str_tot_posix_size_100_1k_w = "%sPOSIX_SIZE_WRITE_100_1K"%(prefix)
    str_tot_posix_size_unaligned = "%sPOSIX_FILE_NOT_ALIGNED"%(prefix)
    
    str_std_read_time = "%sSTDIO_F_READ_TIME"%(prefix)
    str_std_write_time = "%sSTDIO_F_WRITE_TIME"%(prefix)
    str_std_meta_time = "%sSTDIO_F_META_TIME"%(prefix)
    str_std_opens = "%sSTDIO_OPENS"%(prefix)
    str_std_fsync = "%sSTDIO_FLUSHES"%(prefix)
    str_std_seeks = "%sSTDIO_SEEKS"%(prefix)
    
    str_std_reads = "%sSTDIO_READS"%(prefix)
    str_std_writes = "%sSTDIO_WRITES"%(prefix)
    str_std_read_size = "%sSTDIO_BYTES_READ"%(prefix)
    str_std_write_size = "%sSTDIO_BYTES_WRITTEN"%(prefix)
    str_std_bw = "STDIO_agg_perf_by_slowest"
    
    g_contr = {}

    if record.get(str_pos_opens, -1) != -1:
        tot_posix_size_0_100_r = float(record[str_tot_posix_size_0_100_r])
        tot_posix_size_100_1k_r = float(record[str_tot_posix_size_100_1k_r])
        tot_posix_size_0_100_w = float(record[str_tot_posix_size_0_100_w])
        tot_posix_size_100_1k_w = float(record[str_tot_posix_size_100_1k_w])
   
        pos_small_reads = float(tot_posix_size_0_100_r + tot_posix_size_100_1k_r)
        pos_small_writes = float(tot_posix_size_0_100_w + tot_posix_size_100_1k_w)
        pos_unaligned_io = float(record[str_tot_posix_size_unaligned])
        pos_open_count = int(record[str_pos_opens])
        pos_stat_count = int(record[str_pos_stats])
        pos_fsync_count = int(record[str_pos_fsync])
        pos_seek_count = int(record[str_pos_seeks])
        pos_seq_reads = float(record[str_pos_seq_r])
        pos_seq_writes = float(record[str_pos_seq_w]) 
        
        posix_writes = float(record[str_pos_writes])
        posix_reads = float(record[str_pos_reads])
        posix_bytes_read = float(record[str_pos_read_size])
        posix_bytes_written = float(record[str_pos_write_size])
        
        sum_r_time = float(record[str_pos_read_time])
        sum_w_time = float(record[str_pos_write_time])
        sum_meta_time = float(record[str_pos_meta_time])

        g_contr["pos_read_size"] = float(posix_bytes_read)
        g_contr["pos_write_size"] = float(posix_bytes_written)
        g_contr["pos_read_count"] = float(posix_reads)
        g_contr["pos_write_count"] = float(posix_writes)
        g_contr["pos_open_count"] = int(pos_open_count)
        g_contr["pos_read_time"] = float(record[str_pos_read_time])
        g_contr["pos_write_time"] = float(record[str_pos_write_time])
        g_contr["pos_meta_time"] = float(record[str_pos_meta_time])
        g_contr["pos_stat_count"] = pos_stat_count
        g_contr["pos_fsync_count"] = pos_fsync_count
        g_contr["pos_seek_count"] = pos_seek_count
    
        if posix_writes != 0:
            g_contr["pos_seq_w_ratio"] = float(pos_seq_writes/posix_writes) 
        else:
            g_contr["pos_seq_w_ratio"] = 0
    
        if posix_reads != 0:
            g_contr["pos_seq_r_ratio"] = float(pos_seq_reads/posix_reads)
        else:
            g_contr["pos_seq_r_ratio"] = 0
        
        if float(posix_writes + posix_reads) != 0:
            g_contr["pos_seq_io_ratio"] =\
                float(pos_seq_writes + pos_seq_reads)/float(posix_writes + posix_reads)
        else:
            g_contr["pos_seq_io_ratio"] = 0
        
        if float(posix_reads) != 0:
            g_contr["pos_small_r_ratio"] = float(pos_small_reads)/float(posix_reads)
        else:
            g_contr["pos_small_r_ratio"] = 0
        
        if float(posix_writes) != 0:
            g_contr["pos_small_w_ratio"] = float(pos_small_writes)/float(posix_writes)
        else:
            g_contr["pos_small_w_ratio"] = 0
        
        if float(posix_writes + posix_reads) != 0:
            g_contr["pos_small_io_ratio"] =\
                float(pos_small_writes + pos_small_reads)/float(posix_writes + posix_reads)
        else:
            g_contr["pos_small_io_ratio"] = 0
        
        if float(posix_writes + posix_reads) != 0:
            g_contr["pos_unaligned_ratio"] =\
                float(pos_unaligned_io)/float(posix_writes + posix_reads)    
        
        if float(sum_r_time + sum_w_time + sum_meta_time) != 0:
            g_contr["pos_meta_ratio"] =\
                float(sum_meta_time)/(sum_r_time + sum_w_time + sum_meta_time)
        else:
            g_contr["pos_meta_ratio"] = 0
            
    if record.get(str_std_opens, -1) != -1:  
        std_open_count = int(record[str_std_opens])
        std_fsync_count = int(record[str_std_fsync])
        std_seek_count = int(record[str_std_seeks])
        
        std_writes = float(record[str_std_writes])
        std_reads = float(record[str_std_reads])
        std_bytes_read = float(record[str_std_read_size])
        std_bytes_written = float(record[str_std_write_size])
        
        sum_r_time = float(record[str_std_read_time])
        sum_w_time = float(record[str_std_write_time])
        sum_meta_time = float(record[str_std_meta_time])

        g_contr["std_read_size"] = float(std_bytes_read)
        g_contr["std_write_size"] = float(std_bytes_written)
        g_contr["std_read_count"] = float(std_reads)
        g_contr["std_write_count"] = float(std_writes)
        g_contr["std_open_count"] = int(std_open_count)
        g_contr["std_read_time"] = float(record[str_std_read_time])
        g_contr["std_write_time"] = float(record[str_std_write_time])
        g_contr["std_meta_time"] = float(record[str_std_meta_time])
        g_contr["std_flush_count"] = std_fsync_count
        g_contr["std_seek_count"] = std_seek_count
           
        if float(sum_r_time + sum_w_time + sum_meta_time) != 0:
            g_contr["pos_meta_ratio"] =\
                float(sum_meta_time)/(sum_r_time + sum_w_time + sum_meta_time)
        else:
            g_contr["pos_meta_ratio"] = 0    
    if prefix == "total_":
        (max_read,\
                max_write,\
                max_io,\
                tot_read,\
                tot_write,\
                tot_io,\
                max_r_rank,\
                max_w_rank,\
                max_io_rank) = CalMaxIO(ds_rank_io)
        if tot_read != 0:
            g_contr["max_rank_pct_r"] = float(max_read)/tot_read
        else:
            g_contr["max_rank_pct_r"] = 0
            
        if tot_write != 0:
            g_contr["max_rank_pct_w"] = float(max_write)/tot_write
        else:
            g_contr["max_rank_pct_w"] = 0
        if tot_write + tot_read != 0:
            g_contr["max_rank_pct_wr"] = float(max_io)/(tot_write + tot_read)
        else:
            g_contr["max_rank_pct_wr"] = 0   
             
        if record.get(str_pos_opens, -1) != -1:
            g_contr["pos_darshan_bw"] = str(float(record[str_pos_bw]))+"MB/s"
        if record.get(str_std_opens, -1) != -1: 
            g_contr["std_darshan_bw"] = str(float(record[str_std_bw]))+"MB/s"
            
        g_contr["nprocs"] = float(ds_glb_dict[str_nprocs])
        g_contr["ost_cnt"] = float(ds_glb_dict["ost_cnt"])
        
        if ds_tot_rtime != 0:
            if  g_contr.get("std_read_size", -1) != -1:
                tmp_std_read_size = float(g_contr["std_read_size"])
            else:
                tmp_std_read_size = 0
            if g_contr.get("pos_read_size", -1) != -1:
                tmp_pos_read_size = float(g_contr["pos_read_size"])
            else:
                tmp_pos_read_size = 0
            g_contr["miner_r_bw"] = str((tmp_std_read_size + tmp_pos_read_size)/ds_tot_rtime/1048576)+"MB/s"
        else:
            g_contr["miner_r_bw"] = 0
        
        if ds_tot_wtime != 0:
            if  g_contr.get("std_write_size", -1) != -1:
                tmp_std_write_size = float(g_contr["std_write_size"])
            else:
                tmp_std_write_size = 0
            if g_contr.get("pos_write_size", -1) != -1:
                tmp_pos_write_size = float(g_contr["pos_write_size"])
            else:
                tmp_pos_write_size = 0
            g_contr["miner_w_bw"] = str((tmp_std_write_size + tmp_pos_write_size)/ds_tot_wtime/1048576)+"MB/s"
        else:
            g_contr["miner_w_bw"] = 0
        g_contr["username"] = ds_glb_dict["user_name"]
        
        fmt_str = "\t\tpure read time (s):%lf\n\t\tpure write time (s):%lf\n"%(ds_tot_rtime,\
                                                                               ds_tot_wtime)
        write_log(fmt_str)
    else:
      #  print("###line file is %s###\n"%line_point.filename)
        g_contr["nprocs"] = int(ds_file_stat_dict[line_point.filename]["nprocs"])
    
        if record.get(str_pos_opens, -1) != -1:
            total_pos_wr = g_contr["pos_read_size"] + g_contr["pos_write_size"]
            if g_contr["nprocs"] > 1:
                if record.get("POSIX_SLOWEST_RANK_BYTES", -1) == -1 or \
                       (g_contr["pos_read_size"] + g_contr["pos_write_size"] != 0 and \
                        float(record.get("POSIX_SLOWEST_RANK_BYTES")) == 0):
                           g_contr["pos_slowest_w_pct"] = 0
                           g_contr["pos_slowest_r_pct"] = 0
                           g_contr["pos_slowest_wr_pct"] = 0 
                else:
                    pos_slowest_rank_wr = float(record.get("POSIX_SLOWEST_RANK_BYTES"))               
                if total_pos_wr != 0:
                    g_contr["pos_slowest_wr_pct"] = pos_slowest_rank_wr/total_pos_wr
                else:
                    g_contr["pos_slowest_wr_pct"] = 0
                    
        if record.get(str_std_opens, -1) != -1:
            total_std_wr = g_contr["std_read_size"] + g_contr["std_write_size"] 
            if g_contr["nprocs"] > 1:
                if record.get("STDIO_SLOWEST_RANK_BYTES", -1) == -1 or \
                       (g_contr["std_read_size"] + g_contr["std_write_size"] != 0 and \
                        float(record.get("STDIO_SLOWEST_RANK_BYTES")) == 0):
                           g_contr["std_slowest_w_pct"] = 0
                           g_contr["std_slowest_r_pct"] = 0
                           g_contr["std_slowest_wr_pct"] = 0 
                else:
                    slowest_rank_wr = float(record.get("STDIO_SLOWEST_RANK_BYTES"))               
                if total_std_wr != 0:
                    g_contr["stdio_slowest_wr_pct"] = slowest_rank_wr/total_std_wr
                else:
                    g_contr["stdio_slowest_wr_pct"] = 0
  #      print ("filename:%s, getting stripe width\n"%(point.filename))
        if ds_file_stat_dict[line_point.filename].get("LUSTRE_STRIPE_WIDTH", -1) != -1:
            g_contr["stripe_width"] = int(ds_file_stat_dict[line_point.filename]["LUSTRE_STRIPE_WIDTH"])
        else:
            g_contr["stripe_width"] = 1
        
        if ds_file_stat_dict[line_point.filename].get("LUSTRE_STRIPE_SIZE", -1) != -1:
            g_contr["stripe_size"] = int(ds_file_stat_dict[line_point.filename]["LUSTRE_STRIPE_SIZE"])
        else:
            g_contr["stripe_size"] = 1048576
        
    return g_contr

# print the I/O contributing factors of a job based on the files
# on the covering set (sweep_line_set), 
def CalSweeplineFactors(ds_rank_stat_dict,\
                        sweep_line_set,\
                        ds_glb_dict,\
                        ds_file_stat_dict,\
                        ds_rank_io,\
                        r_time,\
                        w_time,\
                        wr_time):
    
    sweep_line_set.sort(key=lambda x: (-(x.end - x.start)))

    counter = 0
    tot_time = 0
    for point in sweep_line_set:
        tot_time += point.end - point.start
        counter += 1
        

    counter = 0
    for point in sweep_line_set:
        line_factors = ExtractFactors("",\
                                      ds_rank_stat_dict,\
                                      ds_glb_dict,\
                                      ds_file_stat_dict,\
                                      ds_rank_io, \
                                      r_time,\
                                      w_time,\
                                      wr_time,\
                                      point)
        fmt_str = "\tfilename:%s:\n"%(point.filename)
        write_log(fmt_str)
#        print("\tfilename:%s:\n"%(point.filename))
        for key,value in line_factors.items():
            fmt_str = "\t\tkey:%s, value:%s\n"%(str(key), str(value))
            write_log(fmt_str)
            # print("\t\tkey:%s, value:%s\n"%(str(key), str(value)))
        counter += 1
        if print_file_cnt != -1 and counter > print_file_cnt:
            break;

# extract IO contributing factors for three covering set:
# r_sweep_line_set: read covering set couting only the read activities
# w_sweep_line_set: write covering set couting only the write activities 
# rw_sweep_line_set: io covering set counting both the write and read activities
            
def ExtractContriFactors(ds_rank_stat_dict, ds_glb_dict,\
                           ds_file_stat_dict, wr_sweep_line_set,\
                           r_line_tuples, w_line_tuples,\
                           wr_line_tuples, ds_rank_io, \
                           line_point = None):

    ds_tot_iotime = GetLineSize(wr_line_tuples)
    ds_tot_rtime = GetLineSize(r_line_tuples)
    ds_tot_wtime = GetLineSize(w_line_tuples)

    fmt_str = "global statistics:\n"
    write_log(fmt_str)
    # print ("global statistics:")
    
    fmt_str = "\t\tpure IO time (s): %d\n"%ds_tot_iotime
    write_log(fmt_str)
    #print ("\t total IO time span: %d(s)\n"%ds_tot_iotime)
    g_contr = ExtractFactors("total_",\
                             ds_rank_stat_dict,\
                             ds_glb_dict,\
                             ds_file_stat_dict,\
                             ds_rank_io,\
                             ds_tot_rtime,\
                             ds_tot_wtime,\
                             ds_tot_iotime)
    if g_contr != None:
        for key,value in g_contr.items():
            fmt_str = "\t\tkey:%s, value:%s\n"%(str(key), str(value))
            write_log(fmt_str)
            #print("\t\tkey:%s, value:%s\n"%(str(key), str(value)))

    fmt_str = "top IO consumer file statistics:\n"
    write_log(fmt_str)
    #print ("top IO consumer file statistics:")
    if wr_sweep_line_set != None and len(wr_sweep_line_set) != 0:
        CalSweeplineFactors(ds_rank_stat_dict,\
                            wr_sweep_line_set,\
                            ds_glb_dict,\
                            ds_file_stat_dict,\
                            ds_rank_io,\
                            ds_tot_rtime,\
                            ds_tot_wtime,\
                            ds_tot_iotime)
        
# calculate the number of files, number of IO requests, and the IO size of each rank
def PlotRankDataDistr(ds_rank_stat_dict, subtitle):

    max_rank = -1
    for cur_rank, file_info in ds_rank_stat_dict.items():
        if cur_rank > max_rank:
            max_rank = cur_rank

    rank_stat = [dict() for x in range(0, max_rank + 1)]
    rank_stat.append(dict())

    rank_size_lst = []
    rank_label_lst = []
    
    for cur_rank, file_info in ds_rank_stat_dict.items():
        rank_stat[cur_rank]["file_count"] = len(ds_rank_stat_dict[cur_rank].items())
        
        rank_io_size = 0
        rank_io_cnt = 0
        for file_name, counters in ds_rank_stat_dict[cur_rank].items():
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_BYTES_WRITTEN", -1) == -1:
                pos_rank_write_size = 0
            else:
                pos_rank_write_size = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_BYTES_WRITTEN"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_BYTES_WRITTEN", -1) == -1:
                std_rank_write_size = 0
            else:
                std_rank_write_size = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_BYTES_WRITTEN"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_BYTES_READ", -1) == -1:
                pos_rank_read_size = 0
            else:
                pos_rank_read_size = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_BYTES_READ"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_BYTES_READ", -1) == -1:
                std_rank_read_size = 0
            else:
                std_rank_read_size = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_BYTES_READ"])
                
            tmp_io_size = int(pos_rank_write_size + pos_rank_read_size + std_rank_write_size + std_rank_read_size)
            rank_io_size += tmp_io_size
            
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_READS", -1) == -1:
                pos_rank_read_cnt = 0
            else:
                pos_rank_read_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_READS"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_READS", -1) == -1:
                std_rank_read_cnt = 0
            else:
                std_rank_read_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_READS"])
            
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_WRITES", -1) == -1:
                pos_rank_write_cnt = 0
            else:
                pos_rank_write_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_WRITES"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_WRITES", -1) == -1:
                std_rank_write_cnt = 0
            else:
                std_rank_write_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_WRITES"])
      
            tmp_io_cnt = int(pos_rank_read_cnt + pos_rank_write_cnt + std_rank_read_cnt + std_rank_write_cnt)
            rank_io_cnt += tmp_io_cnt
            # print("#####rank:%d, io_cnt:%ld, io_size:%ld, filename:%s\n"%(cur_rank,\
            #            rank_io_cnt, rank_io_size, file_name))
            rank_stat[cur_rank + 1]["req_count"] = rank_io_cnt
            rank_stat[cur_rank + 1]["rank_io_size"] = rank_io_size
         
    plt.gcf().clear()    
    # print("rank io size:%d\n"%len(rank_stat))
    rank_cursor = 0
    for record in rank_stat:
        if record.get("rank_io_size", -1) != -1:
            rank_size_lst.append(float(record["rank_io_size"])/1024)
            rank_label_lst.append(str(rank_cursor-1))
            rank_cursor += 1
            # print("size:%d\n"%int(record["rank_io_size"]))
            
   # print(rank_label_lst)   
    t = range(0, len(rank_size_lst))
    fig, ax1 = plt.subplots()
    
    ax1.set_xlabel('Rank')
    ax1.set_ylabel('Data Size (KB)')
    ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
    
 #   print("######size of range is %d, max_ticks:%d######\n"%(len(t), max_ticks_to_show))
    
    (new_ticks, new_labels) = GenNewTicksLabels(t,\
                                                rank_label_lst,\
                                                max_ticks_to_show)
    ax1.set_xticks(new_ticks)
    ax1.set_xticklabels(new_labels)
    ax1.bar(t, rank_size_lst)
   # ax1.plot(t, rank_size_lst, color=color)
   # ax1.tick_params(axis='y', labelcolor='black')
   # ax1.set_xticks(range(0, len(t), 64))
   # fig.tight_layout()  # otherwise the right y-label is slightly clipped
   # plt.setp(ax1.get_xticklabels(), rotation=90)
    plt.savefig(subtitle+'_ds_distri.pdf', format = 'pdf', bbox_inches='tight')
  
    return rank_stat

# calculate the number of files, number of IO requests, and the IO size of each rank
def PlotReqCntDistr(ds_rank_stat_dict, subtitle):

    max_rank = -1
    for cur_rank, file_info in ds_rank_stat_dict.items():
        if cur_rank > max_rank:
            max_rank = cur_rank

    rank_cnt_lst = []
    rank_label_lst = []

    rank_stat = [dict() for x in range(0, max_rank + 1)]
    rank_stat.append(dict())
    
    for cur_rank, file_info in ds_rank_stat_dict.items():
        # print ("cur_rank is %d, size is %d\n"%(cur_rank, len(ds_rank_stat_dict.items())))
        rank_stat[cur_rank]["file_count"] = len(ds_rank_stat_dict[cur_rank].items())
        
        rank_io_size = 0
        rank_io_cnt = 0
        for file_name, counters in ds_rank_stat_dict[cur_rank].items():
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_BYTES_WRITTEN", -1) == -1:
                pos_rank_write_size = 0
            else:
                pos_rank_write_size = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_BYTES_WRITTEN"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_BYTES_READ", -1) == -1:
                pos_rank_read_size = 0
            else:
                pos_rank_read_size = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_BYTES_READ"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_BYTES_WRITTEN", -1) == -1:
                std_rank_write_size = 0
            else:
                std_rank_write_size = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_BYTES_WRITTEN"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_BYTES_READ", -1) == -1:
                std_rank_read_size = 0
            else:
                std_rank_read_size = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_BYTES_READ"])
                
            tmp_io_size = int(pos_rank_write_size + pos_rank_read_size + std_rank_write_size + std_rank_read_size)
            rank_io_size += tmp_io_size
            
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_READS", -1) == -1:
                pos_rank_read_cnt = 0
            else:
                pos_rank_read_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_READS"])
            
            if ds_rank_stat_dict[cur_rank][file_name].get("POSIX_WRITES", -1) == -1:
                pos_rank_write_cnt = 0
            else:
                pos_rank_write_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["POSIX_WRITES"])
                
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_READS", -1) == -1:
                std_rank_read_cnt = 0
            else:
                std_rank_read_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_READS"])
            
            if ds_rank_stat_dict[cur_rank][file_name].get("STDIO_WRITES", -1) == -1:
                std_rank_write_cnt = 0
            else:
                std_rank_write_cnt = float(ds_rank_stat_dict[cur_rank][file_name]["STDIO_WRITES"])    
            
            tmp_io_cnt = int(pos_rank_read_cnt + pos_rank_write_cnt + std_rank_read_cnt + std_rank_write_cnt)
            rank_io_cnt += tmp_io_cnt
#            print("#####rank:%d, io_cnt:%ld, io_size:%ld, filename:%s\n"%(cur_rank,\
#                        rank_io_cnt, rank_io_size, file_name))
            rank_stat[cur_rank + 1]["req_count"] = rank_io_cnt
            rank_stat[cur_rank + 1]["rank_io_size"] = rank_io_size
         
    plt.gcf().clear()    

    rank_cursor = 0
    for record in rank_stat:
        if record.get("req_count", -1) != -1:
            rank_cnt_lst.append(int(record["req_count"])/1000)
            rank_label_lst.append(str(rank_cursor-1))
            rank_cursor += 1
            
   # print(rank_label_lst)   
    t = range(0, len(rank_cnt_lst))
    fig, ax1 = plt.subplots()
    
    ax1.set_xlabel('Rank')
    ax1.set_ylabel('Request Count (K)')
    ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
    
    (new_ticks, new_labels) = GenNewTicksLabels(t,\
                                                rank_label_lst,\
                                                max_ticks_to_show)
    ax1.set_xticks(new_ticks)
    ax1.set_xticklabels(new_labels)
    ax1.bar(t, rank_cnt_lst)
   # ax1.plot(t, rank_size_lst, color=color)
   # ax1.tick_params(axis='y', labelcolor='black')
   # ax1.set_xticks(range(0, len(t), 64))
   # fig.tight_layout()  # otherwise the right y-label is slightly clipped
   # plt.setp(ax1.get_xticklabels(), rotation=90)
    plt.savefig(subtitle+'_req_cnt.pdf', format = 'pdf', bbox_inches='tight')
  
    return rank_stat

# calculate the number of files, number of IO requests, and the IO size of each rank
def PlotFileCntDistr(ds_rank_stat_dict, subtitle):
   
    max_rank = -1
    for cur_rank, file_info in ds_rank_stat_dict.items():
        if cur_rank > max_rank:
            max_rank = cur_rank
   
    rank_stat = [dict() for x in range(0, max_rank + 1)]
    rank_filecnt_lst = []
    rank_label_lst = []
    
    for cur_rank, file_info in ds_rank_stat_dict.items():
        rank_stat[cur_rank]["file_count"] = len(ds_rank_stat_dict[cur_rank].items())
         
    plt.gcf().clear()    

    rank_cursor = 0
    for record in rank_stat:
        if record.get("file_count", -1) != -1:
            rank_filecnt_lst.append(int(record["file_count"]))
            rank_label_lst.append(str(rank_cursor-1))
            rank_cursor += 1
            
    # print(rank_label_lst)   
    t = range(0, len(rank_filecnt_lst))
    fig, ax1 = plt.subplots()
    
    ax1.set_xlabel('Rank')
    ax1.set_ylabel('File Count')
    ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
    
    (new_ticks, new_labels) = GenNewTicksLabels(t,\
                                                rank_label_lst,\
                                                max_ticks_to_show)
    ax1.set_xticks(new_ticks)
    ax1.set_xticklabels(new_labels)
    ax1.bar(t, rank_filecnt_lst)
   # ax1.plot(t, rank_size_lst, color=color)
   # ax1.tick_params(axis='y', labelcolor='black')
   # ax1.set_xticks(range(0, len(t), 64))
   # fig.tight_layout()  # otherwise the right y-label is slightly clipped
   # plt.setp(ax1.get_xticklabels(), rotation=90)
    plt.savefig(subtitle+'_file_cnt.pdf', format = 'pdf', bbox_inches='tight')
  
    return rank_stat

#plot job's I/O activity and the sweep line
def PltEvents(plt_points,
              sweep_line_set,
              line_tuples,
              nprocs,
              direction,
              sub_title):
    plt.gcf().clear()
    
    title = "%s_%s"%(direction, sub_title)

    plot_y = []
    plot_x = []
    plot_x_delta = []
    
    plot_sweep_x = []
    plot_sweep_x_delta = []
    plot_sweep_y = []
        
    (norm_line_tuples, norm_line_size, line_size) = NormalizeLineTuples(plt_points,
                                                                        sweep_line_set,
                                                                        line_tuples)

    norm_pointer = 0
    for i in range(0, len(plt_points)):
        if norm_pointer + 1 < len(norm_line_tuples):
            if line_tuples[norm_pointer + 1][0] <= plt_points[i].start:
                norm_pointer += 1
        x_start = norm_line_tuples[norm_pointer][0] +\
                    float(plt_points[i].start - line_tuples[norm_pointer][0]) * norm_line_size / line_size
        x_end = norm_line_tuples[norm_pointer][0] + \
                float(plt_points[i].end - line_tuples[norm_pointer][0]) * norm_line_size / line_size

        plot_x.append(x_start)
        plot_x_delta.append(x_end)
        plot_y.append(plt_points[i].rank)
 
    norm_pointer = 0
    
    pre_end = -1
    for i in range(0, len(sweep_line_set)):
        if norm_pointer + 1 < len(norm_line_tuples):
            if line_tuples[norm_pointer + 1][0] <= sweep_line_set[i].start:
                norm_pointer += 1
        x_start = norm_line_tuples[norm_pointer][0] +\
                    float(sweep_line_set[i].start -\
                          line_tuples[norm_pointer][0]) * norm_line_size / line_size
        if pre_end > x_start:
            x_start = pre_end
    
        x_end = norm_line_tuples[norm_pointer][0] +\
                float(sweep_line_set[i].end -\
                      line_tuples[norm_pointer][0]) * norm_line_size / line_size
        pre_end = x_end
    
        
        plot_sweep_x.append(x_start)
        plot_sweep_x_delta.append(x_end)
        plot_sweep_y.append(sweep_line_set[i].rank)
    
    if direction == "w":
        name = "write activities"
    else:
        if direction == "r":
            name = "read activities"
        else:
            name = "IO activities"
    
    if min(plot_y) == max(plot_y) or max(plot_y) == min(plot_y) + 1:
        plt.yticks([min(plot_y), min(plot_y) + 1])
    else:
        plt.yticks([min(plot_y), math.ceil(max(plot_y))+1])

    plt.hlines(plot_y, plot_x, plot_x_delta, lw = 4, color = 'lightskyblue', label = name)
    plt.hlines(plot_sweep_y, plot_sweep_x, plot_sweep_x_delta, lw = 3, color = 'blue', label = "IO covering set")

    plt.xticks((min(plot_x), max(plot_x_delta)), (str(round(min(plot_x), 1)), str(round(max(plot_x_delta), 1))))
    plt.legend(loc = "upper left")
    plt.xlabel("Time (s)", fontsize = '16')
    plt.ylabel("Rank Number", fontsize = '16')

    plt.tick_params(labelsize=16)
    
    plt.savefig(title+'.pdf', format = 'pdf', bbox_inches='tight')
    
    # calculate the IO size, number of I/O requests distributed to all the OSTs accessed by this job    
def CalOSTSizeDistri(ost_lst, per_file_dict, ds_rank_stat_dict):
    out_ost_lst = [dict() for x in range(max_ost_id + 1)]
    for i in range(0, max_ost_id + 1):
        if ost_lst.get(i, -1) == -1:
            ost_lst[i] = {}
        if len(ost_lst[i].items()) != 0:
            for key,value in ost_lst[i].items():
                file_name = key
                # print("filename:%s\n"%file_name)
                ost_idx = i
                ost_id_in_file = int(value)
                # for this file, calculate the read size/write size/total_size on this OST, and number of ranks
                stripe_size = int(per_file_dict[file_name]["LUSTRE_STRIPE_SIZE"])
                stripe_width = int(per_file_dict[file_name]["LUSTRE_STRIPE_WIDTH"])
                if per_file_dict[file_name].get("POSIX_BYTES_WRITTEN", -1) != -1:
                    pos_write_size = int(float(per_file_dict[file_name]["POSIX_BYTES_WRITTEN"]))
                    pos_read_size = int(float(per_file_dict[file_name]["POSIX_BYTES_READ"]))
                    pos_max_write = int(float(per_file_dict[file_name]["POSIX_MAX_BYTE_WRITTEN"]))
                else:
                    pos_write_size = 0
                    pos_read_size = 0
                    pos_max_write = 0
                    
                if per_file_dict[file_name].get("STDIO_BYTES_WRITTEN", -1) != -1:
                    std_write_size = int(float(per_file_dict[file_name]["STDIO_BYTES_WRITTEN"]))
                    std_read_size = int(float(per_file_dict[file_name]["STDIO_BYTES_READ"]))
                    std_max_write = int(float(per_file_dict[file_name]["STDIO_MAX_BYTE_WRITTEN"]))
                else:
                    std_write_size = 0
                    std_read_size = 0
                    std_max_write = 0

                if pos_max_write > std_max_write:
                    max_write = pos_max_write
                else:
                    max_write = std_max_write
                    
                print("#####filename:%s#####\n"%file_name)
                      
                if per_file_dict[file_name].get("nprocs", -1) != -1:
                    nprocs = int(float(per_file_dict[file_name]["nprocs"]))
                else:
                    nprocs = GetProcCnt(ds_rank_stat_dict, file_name)
                
                print("######filename:%s, nprocs:%d, ost:%d######\n"%(file_name, nprocs, ost_idx))
            
                if float(max_write)/stripe_size < stripe_width:
                    w_stripe_width = max_write/stripe_size + 1
                else:
                    w_stripe_width = stripe_width
                
                if ost_id_in_file < w_stripe_width:
                    r_data_size = (pos_read_size + std_read_size)/w_stripe_width
                else:
                    r_data_size = 0
                    
                if ost_id_in_file < w_stripe_width:
                    w_data_size = (pos_write_size + std_write_size)/w_stripe_width
                else:
                    w_data_size = 0
                                   
                # if ost_idx == 101:
                #    print("filename:%s, write data size:%d, read data size:%d, max_write:%d, write_size:%d, read_size:%d, stripe width:%d, w_stripe_width:%d, stripe_size:%d"%(file_name, w_data_size, r_data_size, max_write, write_size, read_size, stripe_width, w_stripe_width, stripe_size))
                if (out_ost_lst[ost_idx].get("nprocs", -1) == -1):
                    out_ost_lst[ost_idx]["nprocs"] = 0
                out_ost_lst[ost_idx]["nprocs"] += nprocs
                if out_ost_lst[ost_idx].get("r_data_size", -1) == -1:
                    out_ost_lst[ost_idx]["r_data_size"] = 0
                out_ost_lst[ost_idx]["r_data_size"] += r_data_size #
                if out_ost_lst[ost_idx].get("w_data_size", -1) == -1:
                    out_ost_lst[ost_idx]["w_data_size"] = 0
                out_ost_lst[ost_idx]["w_data_size"] += w_data_size
                if out_ost_lst[ost_idx].get("io_size", -1) == -1:
                    out_ost_lst[ost_idx]["io_size"] = 0
                
                out_ost_lst[ost_idx]["io_size"] += (w_data_size + r_data_size)
                # if (ost_idx == 101):
                #    print("#####%dth io size is %d, file:%s#####\n"%(ost_idx, out_ost_lst[ost_idx]["io_size"], file_name))
    return out_ost_lst   

def GetProcCnt(ds_rank_stat_dict, file_name):
    count = 0
    for key, value in ds_rank_stat_dict.items():
        if key == -1:
            continue
        file_dict = value
        if file_dict.get(file_name, -1) != -1:
            count += 1
    return count
            

# get the ost list accessed by the slowest N files (N = max_files) on the covering set
def GetMarkedOST(ost_lst, rw_sweep_line_set, max_files):
    rw_sweep_line_set.sort(key=lambda x: (-(x.end - x.start)))
    sweep_line_set = rw_sweep_line_set
    mark_ost_lst = []
    
    counter = 0
    file_set = set()
    for point in sweep_line_set:
        # print("point's filename is %s\n"%point.filename)
        file_set.add(point.filename) 
        counter += 1
        if max_files != -1 and counter == max_files:
            break;
    
    for key, value in ost_lst.items(): # for each ost, match its files with file_set
        if len(value.items()) != 0:
            for sub_key,sub_value in value.items():
                file_name = sub_key
                ost_id = int(key)
                if file_name in file_set:
                    mark_ost_lst.append(ost_id)
                    break;
                            
    return mark_ost_lst

# plot the IO size on each OST. The OSTs returned by get_marked_ost are highlighted    
def PlotOSTDs(ost_lst, mark_ost_lst, subtitle):
    size_lst = [0] * (max_ost_id + 1)
    mark_size_lst = [0] * len(mark_ost_lst)
    
    plt.gcf().clear()

    for i in range(0, len(ost_lst)):
        if ost_lst[i].get("io_size", -1) != -1:
            size_lst[i]=(float(ost_lst[i]["io_size"])/1048576)
    
    t = range(0, max_ost_id + 1)
    fig, ax1 = plt.subplots()

#   mark_ost_lst.sort()
    
#    for ost_id in mark_ost_lst:
#        print ("###mark_ost_lst's ost_id is %d\n"%ost_id)
    color = 'blue'
    ax1.set_xlabel('OST ID')
    ax1.set_ylabel('Data Size (MB)', color='black')
#    ax1.plot(t, size_lst, color=color, markevery = mark_ost_lst, markersize = 6, marker = '*')
    ax1.bar(t, size_lst, color=color)
    
    for i in range(0, len(mark_ost_lst)):
        mark_size_lst[i] = size_lst[mark_ost_lst[i]]
    ax1.scatter(mark_ost_lst, mark_size_lst, marker = '*')
    ax1.tick_params(axis='y', labelcolor='black')
    
    if max_ost_id + 1 < 8:
        ax1.set_xticks(range(0, max_ost_id, 1))
    else:
        ax1.set_xticks(range(0, max_ost_id, int(max_ost_id/8)))
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.setp(ax1.get_xticklabels(), rotation=90)
    plt.savefig(subtitle+'_ost_ds.pdf', format = 'pdf', bbox_inches='tight')
    
# plot the process count on each OST 
def PlotOSTNProcs(ost_lst, mark_ost_lst, subtitle):
    nproc_lst = []
    mark_nproc_lst = [0] * len(mark_ost_lst)
    
    plt.gcf().clear()

    for record in ost_lst:
        if record.get("nprocs", -1) != -1:
            nproc_lst.append((int(record["nprocs"])))
        else:
            nproc_lst.append(0)
    
    t = range(0, max_ost_id + 1)
    fig, ax1 = plt.subplots()

    color = 'blue'
    ax1.set_xlabel('OST ID')
    ax1.set_ylabel('Process Count', color='black')
#    ax1.plot(t, size_lst, color=color, markevery = mark_ost_lst, markersize = 6, marker = '*')
    ax1.bar(t, nproc_lst, color=color)
    
    for i in range(0, len(mark_ost_lst)):
        mark_nproc_lst[i] = nproc_lst[mark_ost_lst[i]]
    ax1.scatter(mark_ost_lst, mark_nproc_lst, marker = '*')
    ax1.tick_params(axis='y', labelcolor='black')
    
    if max_ost_id + 1 < 8:
        ax1.set_xticks(range(0, max_ost_id, 1))
    else:
        ax1.set_xticks(range(0, max_ost_id, int(max_ost_id/8)))
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.setp(ax1.get_xticklabels(), rotation=90)
    plt.savefig(subtitle+'_ost_nproc.pdf', format = 'pdf', bbox_inches='tight')

def ParseDarshanUserName(s):
    DARSHAN_FILENAME_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
    darshan_filename_pattern = re.compile(DARSHAN_FILENAME_PATTERN)
    match = darshan_filename_pattern.match(s)
    if match is not None:
        return match.group(1)
    else:
        return "UNKNOWN"

def ParseDarshanJobID(s):
    DARSHAN_FILENAME_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
    darshan_filename_pattern = re.compile(DARSHAN_FILENAME_PATTERN)
    match = darshan_filename_pattern.match(s)
    if match is not None:
        return match.group(2)
    else:
        return "0"  

def IsOutputSaved(
        darshan_log, 
        parsed_darshan_dir):
    
    filename = parsed_darshan_dir+darshan_log.rpartition('/')[2]+'.all'
    return (os.path.exists(filename) and os.path.getsize(filename) > 0)

def SaveParserOutput(
        darshan_log, 
        filename):
   
    with open(filename, 'wb') as target:
        ret_val = subprocess.call(['darshan-parser', 
                                   '--all', 
                                   darshan_log], 
                                   stdout = target)
    return ret_val
        

cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument("--darshan", help = "path of darshan compressed file")
args = cmd_parser.parse_args()

darshan_log = args.darshan
sub_title = ntpath.basename(darshan_log)
sub_title, extension = os.path.splitext(sub_title)
log_file = "./"+sub_title+"_stat.log"

try:
    os.remove(log_file)
except OSError:
    pass

try:
    log_fd = open(log_file,"a")
except:
    print("Fail to open log file %s\n"%log_file)
    exit(1)

if extension == ".darshan":
    parsed_darshan_log = "./"+darshan_log.rpartition('/')[2]+'.all'
    ret_val = SaveParserOutput(darshan_log, parsed_darshan_log)
    if ret_val < 0:
        print("Invalid darshan format\n")
else:
    parsed_darshan_log = darshan_log
    

#if (ret_val != 0):
 #   print("[ERROR] darshan-parser failed to parse the specified darshan path!\n")
 
#parsed_darshan_log = "./chenb_vasp_neb_gamma_knl_id7267682_10-1-27018-14021789285964563928_1.darshan.all"

stat_dict = ExtractProcInfo(parsed_darshan_log)


# generate the read covering set by gen_sweep_line(r_rank_arr), and plot the read activities by plot_events
r_sweep_line_set = []
r_line_tuples = []
if len(stat_dict["rank_file_rstat_lst"]) != 0:
    (plt_points, r_sweep_line_set, r_line_tuples) = GenSweepLine(stat_dict["rank_file_rstat_lst"])
    NormalizeLineTuples(plt_points, r_sweep_line_set, r_line_tuples)
    PltEvents(plt_points, r_sweep_line_set, r_line_tuples, int(stat_dict["global_dict"]["nprocs"]), 'r', sub_title)
    
# generate the write covering set by gen_sweep_line(w_rank_arr), and plot the write activities by plot_events
w_sweep_line_set = []
w_line_tuples = []
if len(stat_dict["rank_file_wstat_lst"]) != 0:
    (plt_points, w_sweep_line_set, w_line_tuples) = GenSweepLine(stat_dict["rank_file_wstat_lst"])
    NormalizeLineTuples(plt_points, w_sweep_line_set, w_line_tuples)
    PltEvents(plt_points,w_sweep_line_set, w_line_tuples, int(stat_dict["global_dict"]["nprocs"]), 'w', sub_title)
    
# generate the readwrite covering set by gen_sweep_line(wr_rank_arr), and plot the readwite activities by plot_events
wr_sweep_line_set = []
wr_line_tuples = []
if len(stat_dict["rank_file_wrstat_lst"]) != 0:
    (plt_points, wr_sweep_line_set, wr_line_tuples) = GenSweepLine(stat_dict["rank_file_wrstat_lst"])
    NormalizeLineTuples(plt_points, wr_sweep_line_set, wr_line_tuples)
    PltEvents(plt_points,wr_sweep_line_set, wr_line_tuples, int(stat_dict["global_dict"]["nprocs"]), 'wr', sub_title) 
    
ExtractContriFactors(stat_dict["ds_rank_stat_dict"], stat_dict["global_dict"],\
                           stat_dict["ds_file_stat_dict"], wr_sweep_line_set,\
                           r_line_tuples, w_line_tuples, \
                           wr_line_tuples, stat_dict["ds_rank_io"], \
                           line_point = None)

PlotRankDataDistr(stat_dict["ds_rank_stat_dict"], sub_title)

PlotReqCntDistr(stat_dict["ds_rank_stat_dict"], sub_title)

PlotFileCntDistr(stat_dict["ds_rank_stat_dict"], sub_title)

if max_ost_id != -1:
    out_ost_lst = CalOSTSizeDistri(stat_dict["ds_ost_dict"],\
                                   stat_dict["ds_file_stat_dict"],\
                                   stat_dict["ds_rank_stat_dict"]
                                   )
    marked_ost_lst = GetMarkedOST(stat_dict["ds_ost_dict"],\
                                  wr_sweep_line_set,\
                                  print_file_cnt)
    PlotOSTDs(out_ost_lst, marked_ost_lst, sub_title)
    PlotOSTNProcs(out_ost_lst, marked_ost_lst, sub_title)
    
log_fd.close()

