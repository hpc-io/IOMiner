import string
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
import json
from bitmap import *
import pandas as pd
import numpy as np
import pickle

root_dir = "/global/cscratch1/sd/tengwang/miner1217/" 
def get_perf_matrix(stat_table):
	str_read_time = "total_POSIX_F_READ_TIME"		
	str_write_time = "total_POSIX_F_WRITE_TIME"
	str_meta_time = "total_POSIX_F_META_TIME"
	str_nprocs = "nprocs"
	str_read_size = "total_POSIX_BYTES_READ"
	str_write_size = "total_POSIX_BYTES_WRITTEN"
	str_bw = "POSIX_agg_perf_by_slowest"
	str_nnodes = "nnodes"
	str_nosts = "ost_cnt"
	str_small_r_pct1 = "small_read (10KB)"
	str_small_w_pct1 = "small_write (10KB)"
	str_small_r_pct2 = "small_read (1KB)"
	str_small_w_pct2 = "small_write (1KB)"
	str_seq_r = "total_POSIX_CONSEC_READS"
	str_seq_w = "total_POSIX_CONSEC_WRITES"
	str_col_read_ratio = "col_read"
	str_col_write_ratio = "col_write"
	str_col_io_ratio = "col_io"
	str_tot_io = "total_ost_IO"
	str_read_bw = "read_bw"
	str_write_bw = "write_bw"
	str_ossCPUAve = "ossCPUAve"
	str_ossCPUMax = "ossCPUMax"
	str_ostIOAve = "ostIOAve"
	str_ostIOMax = "ostIOMax"
	str_mdsCPUAve = "mdsCPUAve"
	str_tot_posix_size_0_100_r = "total_POSIX_SIZE_READ_0_100"
	str_tot_posix_size_0_100_w = "total_POSIX_SIZE_WRITE_0_100"
	str_tot_posix_size_100_1k_r = "total_POSIX_SIZE_READ_100_1K"
	str_tot_posix_size_100_1k_w = "total_POSIX_SIZE_WRITE_100_1K"
	str_tot_reads = "total_POSIX_READS"
	str_tot_writes = "total_POSIX_WRITES"

	resultDict = {}
#	resultDict["tot_io_time"] = []
#	resultDict["tot_size"] = []
#	resultDict["nprocs"] = []
#	resultDict["nnodes"] = []
#	resultDict["ost_cnt"] = []
#	resultDict["seq_io"] = []
#	resultDict["small_io"] = []
#	resultDict["ossave"] = []
#	resultDict["ostioave"] = []
#	resultDict["readbw"] = [] 
#	resultDict["writebw"] = [] 
#	resultDict["coll"] = [] 
#	resultDict["meta_ratio"] = [] 
#	resultDict["bw"] = []
#	resultDict["t"] = []
	resultDict["s"] = []
	resultDict["p"] = []
	resultDict["n"] = []
	resultDict["o"] = []
	resultDict["se"] = []
	resultDict["sm"] = []
	resultDict["oss"] = []
	resultDict["ost"] = []
	resultDict["mds"] = []
#	resultDict["rb"] = [] 
#	resultDict["wb"] = [] 
	resultDict["col"] = [] 
	resultDict["m"] = [] 
	resultDict["b"] = []
	appList = [] 
	for record in stat_table:
		read_time = float(record[str_read_time])
		write_time = float(record[str_write_time])
		meta_time = float(record[str_meta_time])
		read_size = long(record[str_read_size])
		write_size = long(record[str_write_size])
		tot_size = read_size + write_size
		nnodes = long(record["nnodes"])
		nprocs = int(record["nprocs"])
		ost_cnt = -1
		if record.get("ost_map", -1) != -1:
			tmp_bitmap = record["ost_map"]
			ost_cnt = bitmap_counter(tmp_bitmap)
		else:
                        print "here 1\n"
			continue	
		nosts = ost_cnt
                if record.get(str_bw, -1) == -1:
                    print "here 2\n"
                    continue
		bw = float(record[str_bw])
		small_reads = long(record[str_tot_posix_size_0_100_r]) + \
			long(record[str_tot_posix_size_100_1k_r])
		small_writes = long(record[str_tot_posix_size_0_100_w]) + \
			long(record[str_tot_posix_size_100_1k_w])
		seq_reads = long(record[str_seq_r])
		tot_reads = long(record[str_tot_reads]) 
		seq_writes = long(record[str_seq_w])
		tot_writes = long(record[str_tot_writes])
		ossave = float(record[str_ossCPUAve])
		ostioave = float(record[str_ostIOAve])
		ossmax = float(record[str_ossCPUMax])
		ostiomax = float(record[str_ostIOMax])
                print "ostioave is %s\n"%str(ostioave)
	        mdscpuave = float(record[str_mdsCPUAve])
#                print "cpuave is %lf\n"%mdscpuave
		if record.get("sum_read_tuple", -1) != -1:
		    read_bw = tot_size/float(record["sum_read_tuple"][1])/1048576
		else:
                    read_bw = 0
		if record.get("sum_write_tuple", -1) != -1:
		    write_bw = tot_size/float(record["sum_write_tuple"][1])/1048576
		else:
                    write_bw = 0
		col_read_size = 0
		if record.get("total_MPIIO_COLL_READS", -1) != -1:
                    col_read_size = long(record["total_MPIIO_BYTES_READ"])
		col_write_size = 0
		if record.get("total_MPIIO_COLL_WRITES", -1) != -1:
                    col_write_size = long(record["total_MPIIO_BYTES_WRITTEN"])
                if nnodes > 12076:
                    continue
                if read_time < 0 or write_time < 0 or meta_time < 0:
                    continue
                if read_time + write_time + meta_time == 0:
                    continue
#		resultDict["t"].append(read_time+write_time+meta_time)
		resultDict["s"].append(tot_size)
		resultDict["p"].append(nprocs)
		resultDict["n"].append(nnodes)
		resultDict["o"].append(ost_cnt)
		resultDict["se"].append(float(small_reads + small_writes)/(tot_reads + tot_writes))
		resultDict["sm"].append(float(seq_writes + seq_reads)/(tot_reads + tot_writes))
		resultDict["oss"].append(ossave)
		resultDict["ost"].append(ostioave)
                resultDict["mds"].append(mdscpuave);
#		resultDict["rb"].append(read_bw)
#		resultDict["wb"].append(write_bw)
		resultDict["col"].append(float((col_read_size + col_write_size)/(read_size + write_size)))
		resultDict["m"].append(float(meta_time)/(read_time + write_time + meta_time))
		resultDict["b"].append(bw)
                print "app:%s, read_bw:%lf, write_bw:%lf, ossave:%lf, ostioave:%lf, col_read_size:%lf, col_write_size:%lf, nnodes:%d, bw:%lf, mratio:%lf\n"%(record["FileName"],\
                        read_bw, write_bw, ossave, ostioave, col_read_size, col_write_size, nnodes, bw, float(meta_time)/(read_time+write_time+meta_time))
		appList.append(record["FileName"])
	return (appList, resultDict)

#stat_table = []
#load the table generated by combine_log.py
tmp_str = root_dir+"meaningful_job_mds.log"
save_fd = open(tmp_str, 'rb')
stat_table = pickle.load(save_fd)
save_fd.close()

# generate the correlation matrix and store into resultDict.log
(appList, resultDict) = get_perf_matrix(stat_table) 


tmp_str = root_dir+"resultDict.log"
save_fd = open(tmp_str, 'wb')
pickle.dump(resultDict, save_fd, -1)
save_fd.close()

