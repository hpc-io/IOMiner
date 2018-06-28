import pickle
import string
import re
import time
from bitmap import *
plot_dir = "/global/cscratch1/sd/tengwang/miner0612/plots/allpkl/"

fname = plot_dir + "mpi_prb0_1.pkl"
file_pos = 0

def getJobBW(fname):
    pickle_file = open(fname, 'rb') 
    bw_file_pair = pickle.load(pickle_file)
    return bw_file_pair

# unique: 0 0 0
# shared: 1 6442450944 6442450944

# performance
# -----------
# total_bytes: 6442450944
#
# I/O timing for unique files (seconds):
# ...........................
# unique files: slowest_rank_io_time: 0.000000
# unique files: slowest_rank_meta_only_time: 0.000000
# unique files: slowest rank: 0

IO_MODE_PATTERN = '(#\s+):\s+(\S+)\s+(\S+)\s+(\S+)'
IND_SHARED_PATTERN = '(#\s+):\s+(\S+):\s+(\S+)'
FS_PATTERN = '(#\s+)(mount entry):\s+(\S+)\s+(\S+)' 
fs_pattern = re.compile(FS_PATTERN)
# mount entry:  /var/opt/cray/imps-distribution/squash/mounts/global    squashfs
# darshan log version: 3.00
def parseFSList(fname):
    index = 0
    fs_map = []
    with open(fname) as infile:
        for line in infile:
            fs_match = fs_pattern.match(line)
#            print "line is %s"%line
            if fs_match is not None:
                fs_map.append((fs_match.group(3), fs_match.group(4)))
    return fs_map


#<module>       <rank>  <record id>     <counter>       <value> <file name>     <mount pt>      <fs type>

LOG_PATTERN = '(\S+)\t([+-]?\d+(?:\.\d+)?)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t(\S+)\t(\S+)'
log_pattern = re.compile(LOG_PATTERN)
def getOnePath(fname):
    with open(fname) as infile:
        for line in infile:
            log_match = log_pattern.match(line)
            if log_match is not None:
                return (log_match.group(6), log_match.group(8))
    return ()


def replace_ext(fname,ext):
    return fname.rsplit('.', 1)[0]+ext

NPROCS_PATTERN = '(#\s+nprocs:\s+)(\d+)'
nprocs_pattern = re.compile(NPROCS_PATTERN)

KEY_VALUE_PATTERN = '(\S+):(\s+)([+-]?\d+(?:\.\d+)?)'
kv_pattern = re.compile(KEY_VALUE_PATTERN)
# nprocs: 1600

HEADER_PATTERN = '(#\s+)(\S+):(\s+)(\d+)'
header_pattern = re.compile(HEADER_PATTERN)
# uid: 71113
# jobid: 5226291
# <YEAR>/<MONTH>/<DAY>/<USERNAME>_<BINARY_NAME>_<JOB_ID>_<DATE>_<UNIQUE_ID>_<TIMING>.darshan.gz

DARSHAN_FILE_PATTERN = '(\S+)_id(\d+)_\d+-\d+-(\S+).darshan'
darshan_file_pattern = re.compile(DARSHAN_FILE_PATTERN)

VERSION_PATTERN = '(#\s+)(darshan log version):\s+(\d+(?:\.\d+)?)'
darshan_version_pattern = re.compile(VERSION_PATTERN)
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

def bitmap_counter(tmp_bitmap):
#    print "bitmap size:%d"%tmp_bitmap.size
    cnt = 0
    for i in range(0, 248):
        if tmp_bitmap.test(i):
            cnt = cnt + 1
#            print "###cnt%d\n"%(cnt)
    return cnt


def summarize_file_stat(tuple_list, to_print):
    sweep_tuples = []
    sweep_dict = {}
    cursor = 0

    for record in tuple_list:
        # time, index, is_start
        sweep_tuples.append((record[5], cursor, 1))
        sweep_tuples.append((record[6], cursor, 0))
        cursor += 1

    sweep_tuples = sorted(sweep_tuples, key=lambda x:(x[0], x[2]))

    cursor = 0
    for record in sweep_tuples:
        cursor += 1

    counter = 0
    last_idx = -1
    last_time = -1
    small_avg = 0
    nconsec_avg = 0
    proc_ost_ratio_avg = 0
    col_avg = 0
    interval_sum = 0
    for record in sweep_tuples:
        if record[2] == 1: #is start
            sweep_dict[record[1]] = record[1]
            counter += 1
            if counter == 1:
                last_time = record[0]
                last_idx = record[1]
        if record[2] == 0: # is end
            if record[1] == last_idx:
                interval = record[0] - last_time
#                if to_print == 1:
#                    print "file:%s, start time:%lf, end time:%lf, last_time:%lf, interval1:%lf, interval2:%lf, ratio:%lf, nconsec:%lf, small:%lf, counter:%d\n"%(tuple_list[last_idx][0], tuple_list[last_idx][5], tuple_list[last_idx][6], last_time, tuple_list[last_idx][6] - last_time, interval, tuple_list[last_idx][3], tuple_list[last_idx][2], tuple_list[last_idx][1], counter)
                small_avg += interval * tuple_list[last_idx][1]
                nconsec_avg += interval * tuple_list[last_idx][2]
                proc_ost_ratio_avg += interval * tuple_list[last_idx][3]
                col_avg += interval * tuple_list[last_idx][4]
                interval_sum += interval
                last_time = record[0]
                max_idx = -1
                max_time = -1
                del sweep_dict[record[1]]
                counter -= 1
#                print "length is %ld\n"%len(sweep_dict)
                for key,value in sweep_dict.iteritems():
                    if tuple_list[key][6] > max_time:
                        max_idx = key
                        max_time = tuple_list[key][6]
                last_time = record[0]
                last_idx = max_idx
#                if to_print == 1:
#                    print "new idx file:%s, start time:%lf, end time:%lf, last_time:%lf, ratio:%lf, nconsec:%lf, small:%lf, counter:%d\n"%(tuple_list[last_idx][0], tuple_list[last_idx][5], tuple_list[last_idx][6], last_time, tuple_list[last_idx][3], tuple_list[last_idx][2], tuple_list[last_idx][1], counter)
            else:
                del sweep_dict[record[1]]
                counter -= 1
    return ((float(small_avg)/interval_sum, float(nconsec_avg)/interval_sum, float(col_avg)/interval_sum, float(proc_ost_ratio_avg)/interval_sum), interval_sum)

def print_record_info(record, per_file_handle):
#    print "appname:%s\n"%record["AppName"]
    ost_cnt = 0
    offset = long(record["EXTERNAL_FILE_OFFSET"])
    length = long(record["EXTERNAL_FILE_LENGTH"])
    tot_read_start = float(record["total_POSIX_F_READ_START_TIMESTAMP"])
    tot_read_end = float(record["total_POSIX_F_READ_END_TIMESTAMP"])
    tot_read_time = tot_read_end - tot_read_start


    tot_write_start = float(record["total_POSIX_F_WRITE_START_TIMESTAMP"])
    tot_write_end = float(record["total_POSIX_F_WRITE_END_TIMESTAMP"])
    tot_write_time = tot_write_end - tot_write_start

    per_file_handle.seek(offset)
    serialized_obj = per_file_handle.read(length)
    tmp_dict = pickle.loads(serialized_obj)
    posix_cnt = 0
    mpiio_cnt = 0
    stdio_cnt = 0
    tot_ost_cnt = 0
    stripe_sz_list = []
    stripe_cnt_list = []
    bm = Bitmap(279) #####
    tmp_bm = Bitmap(279) #####
    max_read_time = -1
    max_write_time = -1

    write_tuple_list = []
    read_tuple_list = []
    for k in tmp_dict:
        tmp_tmp_dict = tmp_dict[k]
        if tmp_tmp_dict.get("POSIX_F_READ_START_TIMESTAMP", -1) != -1:
            tmp_read_start = float(tmp_tmp_dict["POSIX_F_READ_START_TIMESTAMP"])
            tmp_read_end = float(tmp_tmp_dict["POSIX_F_READ_END_TIMESTAMP"])
            tmp_read_time = tmp_read_end - tmp_read_start
            if tmp_read_time > max_read_time:
                max_read_time = tmp_read_time
                max_read_fname = k

        if tmp_tmp_dict.get("POSIX_F_WRITE_START_TIMESTAMP", -1) != -1:
            tmp_write_start = float(tmp_tmp_dict["POSIX_F_WRITE_START_TIMESTAMP"])
            tmp_write_end = float(tmp_tmp_dict["POSIX_F_WRITE_END_TIMESTAMP"])
            tmp_write_time = tmp_write_end - tmp_write_start
            if tmp_write_time > max_write_time:
                max_write_time = tmp_write_time
                max_write_fname = k
#                print "max_write_start:%lf, max_write_end:%lf\n"%(tmp_write_start, tmp_write_end)


        if tmp_tmp_dict.get("POSIX_BYTES_READ", -1) != -1 and long(tmp_tmp_dict["POSIX_BYTES_READ"]) != 0 and float(tmp_tmp_dict["POSIX_READS"]) != 0:
            small_read_cnt1 = float(tmp_tmp_dict["POSIX_SIZE_READ_0_100"])
            small_read_cnt2 = float(tmp_tmp_dict["POSIX_SIZE_READ_100_1K"])
            small_read_cnt3 = float(tmp_tmp_dict["POSIX_SIZE_READ_1K_10K"])
            small_read_cnt4 = float(tmp_tmp_dict["POSIX_SIZE_READ_10K_100K"])
            tot_small_reads = small_read_cnt1 + small_read_cnt2 + small_read_cnt3 + small_read_cnt4
            tot_reads = float(tmp_tmp_dict["POSIX_READS"])
            small_read_ratio = tot_small_reads/tot_reads

            consec_reads = float(tmp_tmp_dict["POSIX_CONSEC_READS"])
            nconsec_read_ratio = 1 - consec_reads/tot_reads

            read_start = float(tmp_tmp_dict["POSIX_F_READ_START_TIMESTAMP"])
#            read_end = float(tmp_tmp_dict["POSIX_F_READ_END_TIMESTAMP"])
            if tmp_tmp_dict.get("nprocs", -1) == -1:
                print "wrong log path 1:%s, fname:%s\n"%(record["FileName"], k)
                continue
            proc_cnt = float(tmp_tmp_dict["nprocs"])
            if float(tmp_tmp_dict["nprocs"]) == 1:
                read_time = float(tmp_tmp_dict["POSIX_F_READ_TIME"])
            else:
                read_time = float(tmp_tmp_dict["POSIX_F_READ_END_TIMESTAMP"]) - \
                        float(tmp_tmp_dict["POSIX_F_READ_START_TIMESTAMP"])

            read_end = read_start + read_time

            if tmp_tmp_dict.get("LUSTRE_STRIPE_WIDTH", -1) != -1:
                stripe_width = int(tmp_tmp_dict["LUSTRE_STRIPE_WIDTH"])
            else:
                stripe_width = -1
            proc_ost_ratio = float(tmp_tmp_dict["nprocs"])/stripe_width

            if tmp_tmp_dict.get("MPIIO_COLL_READS", -1) != -1:
                if long(tmp_tmp_dict["MPIIO_COLL_READS"]) != 0:
                    col_reads = 1
                else:
                    col_reads = 0
            else:
                col_reads = 0
#            if long(tmp_tmp_dict["nprocs"]) > 1:
#                print "darshan:%s, file:%s, tot_reads:%lf, tot_small_reads:%lf, consec_reads:%lf, read_start:%lf, read_end:%lf, nprocs:%d, stripe_width:%d, coll_reads:%ld, proc_ost_ratio:%lf\n"%(record["FileName"], k, tot_reads, tot_small_reads, consec_reads, read_start, read_end, float(tmp_tmp_dict["nprocs"]), stripe_width, col_reads, proc_ost_ratio)
            if read_end - read_start != 0:
                read_tuple_list.append((k, small_read_ratio, nconsec_read_ratio, proc_ost_ratio, col_reads, read_start, read_end))

        if tmp_tmp_dict.get("POSIX_BYTES_WRITTEN", -1) != -1 and long(tmp_tmp_dict["POSIX_BYTES_WRITTEN"]) != 0 and float(tmp_tmp_dict["POSIX_WRITES"]) != 0:
            small_write_cnt1 = float(tmp_tmp_dict["POSIX_SIZE_WRITE_0_100"])
            small_write_cnt2 = float(tmp_tmp_dict["POSIX_SIZE_WRITE_100_1K"])
            small_write_cnt3 = float(tmp_tmp_dict["POSIX_SIZE_WRITE_1K_10K"])
            small_write_cnt4 = float(tmp_tmp_dict["POSIX_SIZE_WRITE_10K_100K"])
            tot_small_writes = small_write_cnt1 + small_write_cnt2 + small_write_cnt3 + small_write_cnt4
            tot_writes = float(tmp_tmp_dict["POSIX_WRITES"])
            small_write_ratio = tot_small_writes/tot_writes

            consec_writes = float(tmp_tmp_dict["POSIX_CONSEC_WRITES"])
            nconsec_write_ratio = 1 - consec_writes/tot_writes

            write_start = float(tmp_tmp_dict["POSIX_F_WRITE_START_TIMESTAMP"])
            if tmp_tmp_dict.get("nprocs", -1) == -1:
                print "wrong log path 2:%s, fname:%s\n"%(record["FileName"], k)
                continue
            if float(tmp_tmp_dict["nprocs"]) == 1:
                write_time = float(tmp_tmp_dict["POSIX_F_WRITE_TIME"])
            else:
                write_time = float(tmp_tmp_dict["POSIX_F_WRITE_END_TIMESTAMP"]) - \
                        float(tmp_tmp_dict["POSIX_F_WRITE_START_TIMESTAMP"])

            write_end = write_start + write_time
#            write_end = float(tmp_tmp_dict["POSIX_F_WRITE_END_TIMESTAMP"])
            if tmp_tmp_dict.get("LUSTRE_STRIPE_WIDTH", -1) != -1:
                stripe_width = int(tmp_tmp_dict["LUSTRE_STRIPE_WIDTH"])
#                print "---stripe_width here:%d\n"%stripe_width
            else:
                stripe_width = -1
            proc_ost_ratio = float(tmp_tmp_dict["nprocs"])/stripe_width

            if tmp_tmp_dict.get("MPIIO_COLL_WRITES", -1) != -1:
                if long(tmp_tmp_dict["MPIIO_COLL_WRITES"]) != 0:
                    col_writes = 1
                else:
                    col_writes = 0
            else:
                col_writes = 0
#            if long(tmp_tmp_dict["nprocs"]) > 1:
#                print "darshan:%s, file:%s, tot_writes:%lf, tot_small_writes:%lf, consec_writes:%lf, write_start:%lf, write_end:%lf, nprocs:%d, stripe_width:%d, coll_writes:%ld, proc_ost_ratio:%lf\n"%(record["FileName"], k, tot_writes, tot_small_writes, consec_writes, write_start, write_end, float(tmp_tmp_dict["nprocs"]), stripe_width, col_writes, proc_ost_ratio)
#
            if write_end - write_start != 0:
                write_tuple_list.append((k, small_write_ratio, nconsec_write_ratio, proc_ost_ratio, col_writes, write_start, write_end))
#            print "path:%s, appending:%s, write_start:%lf, write_end:%lf, proc_ost_ratio:%lf\n"%(record["FileName"], k, write_start, write_end, proc_ost_ratio)


        for tmp_k in tmp_tmp_dict:
            if tmp_k in "OST_MAP":
                bm = tmp_tmp_dict[tmp_k]
                tmp_bm.orme(bm)
#                bitmap_counter(record["AppName"], k, bm, "bm")
#                bitmap_counter(record["AppName"], k, tmp_bm, "tmp_bm")
            if tmp_k == "POSIX_OPENS":
                posix_cnt = posix_cnt + 1
            if tmp_k == "MPIIO_INDEP_OPENS":
                mpiio_cnt = mpiio_cnt + 1
            if tmp_k == "STDIO_OPENS" and k !="<STDOUT>" and k != "<STDIN>":
                stdio_cnt = stdio_cnt + 1

#                bitmap_counter(bitmap)
            if tmp_k in "LUSTRE_STRIPE_SIZE":
                str_stripe_sz = tmp_tmp_dict[tmp_k]
                if str_stripe_sz not in stripe_sz_list:
                    stripe_sz_list.append(str_stripe_sz)

            if tmp_k in "LUSTRE_STRIPE_WIDTH":
                str_stripe_cnt = tmp_tmp_dict[tmp_k]
                if str_stripe_cnt not in stripe_cnt_list:
                    stripe_cnt_list.append(str_stripe_cnt)
                tot_ost_cnt = tot_ost_cnt + int(tmp_tmp_dict[tmp_k])
                if int(tmp_tmp_dict[tmp_k]) > 1:
                    ost_cnt = ost_cnt + 1
#                    print "appname:%s, filename:%s,stripe_width:%s\n"%(record["AppName"], k, tmp_tmp_dict[tmp_k])
#                    bitmap = tmp_tmp_dict[tmp_k]
#                    bitmap_counter(bm)
    record["stdio_cnt"] = str(stdio_cnt)
    record["mpiio_cnt"] = str(mpiio_cnt)
    record["posix_cnt"] = str(posix_cnt)
    record["tot_ost_cnt"] = str(tot_ost_cnt)
    record["tot_stripe_sz"] = stripe_sz_list
    record["tot_stripe_cnt"] = stripe_cnt_list

    if len(write_tuple_list) != 0:
        print "parsing write job:%s\n"%record["FileName"]
        sum_write_tuple = summarize_file_stat(write_tuple_list, 0)
        print "write path:%s, small_write:%lf, nconsec_write:%lf, col:%lf, proc_ost_ratio:%lf, time:%lf\n"%(record["FileName"], sum_write_tuple[0][0], sum_write_tuple[0][1], sum_write_tuple[0][2], sum_write_tuple[0][3], sum_write_tuple[1])
    if len(read_tuple_list) != 0:
        print "parsing read job:%s\n"%record["FileName"]
        sum_read_tuple = summarize_file_stat(read_tuple_list, 0)
        print "read path:%s, small_read:%lf, nconsec_read:%lf, col:%lf, proc_ost_ratio:%lf, time:%lf\n"%(record["FileName"], sum_read_tuple[0][0], sum_read_tuple[0][1], sum_read_tuple[0][2], sum_read_tuple[0][3], sum_read_tuple[1])

    if len(write_tuple_list) != 0:
        record["sum_write_tuple"] = sum_write_tuple
    else:
        record["sum_write_tuple"] = -1

    if len(read_tuple_list) != 0:
        record["sum_read_tuple"] = sum_read_tuple
    else:
        record["sum_read_tuple"] = -1
    record["ost_map"] = tmp_bm

###to add here
    record["max_read_time"] = max_read_time
    record["max_read_fname"] = max_read_fname
    record["max_read_stat"] = tmp_dict[max_read_fname]
    record["max_write_time"] = max_write_time
    record["max_write_fname"] = max_write_fname
    record["max_write_stat"] = tmp_dict[max_write_fname]
    record["tot_write_time"] = tot_write_time
    record["tot_read_time"] = tot_read_time

    return ost_cnt

def getStatTable(fname, tmpFname, path_tuple, f_handle):
    out_dict = {}
    per_file_dict = {}
    suffix = fname.rsplit('/')[-1]
    out_dict["FileName"] = fname

    jobID = parse_darshan_jobid(suffix)
    out_dict["JobID"] = jobID

    userAppName = parse_darshan_user_appname(suffix)
    appName = userAppName.split("_")[1]
    out_dict["AppName"] = appName
#    print "job id:%s, app name:%s, fname:%s\n"%(jobID, appName, fname)

    version_time = 0
    with open(fname) as infile:
        for line in infile:
#            nprocs_match = nprocs_pattern.match(line) 
#            if nprocs_match is not None:
#                out_nprocs = int(nprocs_match.group(2))
            version_start = time.time() 
            version_match = darshan_version_pattern.match(line)
            if version_match is not None:
                counter_val = version_match.group(3)
                out_dict["darshan_version"] = counter_val
            version_end = time.time() 
            version_time += version_end - version_start
            header_match = header_pattern.match(line)
            if header_match is not None:
                counter_key = header_match.group(2)
                counter_val = header_match.group(4)
                out_dict[counter_key] = counter_val
    #            print "key:%s, val:%s\n"%(counter_key, counter_val)
            kv_match = kv_pattern.match(line)
            if kv_match is not None:
                counter_key = kv_match.group(1)
#                counter_val = float(kv_match.group(3))
                counter_val = kv_match.group(3)
#                if cmp(counter_key.strip(), "total_POSIX_OPENS"):
#                    out_open_count = counter_val
#                if cmp(counter_key.strip(), "total_POSIX_READS"):
#                    out_read_count = counter_val
#                if cmp(counter_key.strip(), "total_POSIX_WRITES"):
#                    out_write_count = counter_val
#                if cmp(counter_key.strip(), "total_POSIX_WRITES"):
#                    out_write_count = counter_val
                out_dict[counter_key] = counter_val

# unique: 0 0 0
# shared: 1 6442450944 6442450944
 # I/O timing for unique files (seconds):
 # ...........................
 # unique files: slowest_rank_io_time: 0.000000
 # unique files: slowest_rank_meta_only_time: 0.000000
 # unique files: slowest rank: 0

    IO_MODE_PATTERN = '(#\s+)(\S+):\s+(\d+)\s+(\d+)\s+(\d+)'
    IND_SHARED_PATTERN = '(#\s+)(\S+\s+\S+):\s+(\S+):\s+(\d+\.\d+)'
    PER_FILE_PATTERN = '([^#]+)(\s+)(\S+)(\s+)(\d+)(\s+(\S+)){12,}'

#    IO_MODE_PATTERN = '(#\s+)(\S+):(\s+)(\d+)'
  
    darshan_io_mode_pattern = re.compile(IO_MODE_PATTERN) 
    darshan_ind_shared_pattern = re.compile(IND_SHARED_PATTERN)
    darshan_log_pattern = re.compile(LOG_PATTERN)
    darshan_per_file_pattern = re.compile(PER_FILE_PATTERN)

    file_pattern_match_time = 0
    log_pattern_time = 0
    mode_pattern_time = 0
    shared_pattern_time = 0
    with open(tmpFname) as infile:
        for line in infile:
            matched = 0
            if line.find("total_MPIIO") != -1:
                key_prefix="MPIIO_"
            if line.find("total_STDIO") != -1:
                key_prefix="STDIO_"
            if line.find("total_POSIX") != -1:
                key_prefix="POSIX_"

            log_pattern_start = time.time()
            log_pattern_match = darshan_log_pattern.match(line)
            log_pattern_end = time.time()
            log_pattern_time += log_pattern_end - log_pattern_start
            if log_pattern_match is not None:
                matched = 1
                file_name = log_pattern_match.group(6).strip()
                per_file_key = log_pattern_match.group(4).strip()
                per_file_val = log_pattern_match.group(5).strip()
#                print "file key:%s, file val:%s\n"%(per_file_key, per_file_val)
                if per_file_dict.get(file_name, -1) == -1:
                    per_file_dict[file_name] = {}
                    per_file_dict[file_name][per_file_key] = per_file_val
                    per_file_dict[file_name]["fs_type"] = log_pattern_match.group(8).strip()
                else:
                    if per_file_dict[file_name].get(per_file_key, -1) != -1:
                        flag = 0
                        if "POSIX_F_READ_START_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) < float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_READ_END_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) > float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_WRITE_START_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) < float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_WRITE_END_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) > float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_OPEN_START_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) < float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_OPEN_END_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) > float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val

                        if "POSIX_F_CLOSE_START_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) < float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_CLOSE_END_TIMESTAMP" in per_file_key:
                            flag = 1
                            if float(per_file_val) > float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_READ_TIME" in per_file_key:
                            flag = 1
                            if float(per_file_val) > float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val
                        if "POSIX_F_WRITE_TIME" in per_file_key:
                            flag = 1
                            if float(per_file_val) > float(per_file_dict[file_name][per_file_key]):
                                per_file_dict[file_name][per_file_key] = per_file_val

                        if "LUSTRE_STRIPE_WIDTH" in per_file_key:
                            flag = 1
                        if "LUSTRE_STRIPE_SIZE" in per_file_key:
                            flag = 1
                        if "LUSTRE_OST_ID" in per_file_key:
                            flag = 1
                        if flag == 0:
                            cur_val = float(per_file_dict[file_name][per_file_key])
                            cur_val = cur_val + float(per_file_val)
                            per_file_dict[file_name][per_file_key] = cur_val
                    else:
                        per_file_dict[file_name][per_file_key] = per_file_val
                        per_file_dict[file_name]["fs_type"] = log_pattern_match.group(8).strip()

                if "LUSTRE_OST_ID" in per_file_key:
                    ost_id = per_file_key.rsplit('_', 1)[1]
                    real_ost_id = int(per_file_dict[file_name][per_file_key])
             #       print "ostid is %s\n"%ost_id
                    if per_file_dict[file_name].get("OST_MAP", -1) == -1:
                        bitmap = Bitmap(279)
                        bitmap.set(int(real_ost_id))
                        per_file_dict[file_name]["OST_MAP"] = bitmap
                    else:
                        bitmap = per_file_dict[file_name]["OST_MAP"]
                        bitmap.set(int(real_ost_id))
                        per_file_dict[file_name]["OST_MAP"] = bitmap
                    del per_file_dict[file_name][per_file_key]

#                print "fname:%s, key:%s, value:%s, fs_type:%s\n"%(file_name, per_file_key, per_file_dict[file_name][per_file_key], per_file_dict[file_name]["fs_type"])
            if matched == 1:
                continue
            mode_pattern_start = time.time()
            io_mode_match = darshan_io_mode_pattern.match(line)
            mode_pattern_end = time.time()
            mode_pattern_time += mode_pattern_end - mode_pattern_start
            if io_mode_match is not None:
                matched = 1
                tmp_key = key_prefix+io_mode_match.group(2)
                tmp_val = io_mode_match.group(3)
                out_dict[tmp_key] = tmp_val
             #   print "key,value is %s,%s\n"%(tmp_key, tmp_val)
            if matched == 1:
                continue
            shared_pattern_start = time.time()
            ind_shared_match = darshan_ind_shared_pattern.match(line)
            shared_pattern_end = time.time()
            shared_pattern_time += shared_pattern_end - shared_pattern_start
            if ind_shared_match is not None:
                matched = 1
                tmp_key = key_prefix + ind_shared_match.group(2)
                tmp_val = ind_shared_match.group(4) 

                if tmp_key.find("unique files") != -1:
                    middle_key = ind_shared_match.group(3)
                    if middle_key.find("slowest_rank_io_time") != -1:
                        out_dict[tmp_key] = tmp_val
             #           print "key,value %s,%s\n"%(tmp_key, tmp_val)
                if tmp_key.find("shared files") != -1:
                    middle_key = ind_shared_match.group(3)
                    if middle_key.find("time_by_slowest") != -1:
                        out_dict[tmp_key] = tmp_val
             #           print "key,value %s,%s\n"%(tmp_key, tmp_val)
            if matched == 1:
                continue
            file_pattern_match_start = time.time()
            per_file_pattern_match = darshan_per_file_pattern.match(line)
            file_pattern_match_end = time.time()
            file_pattern_match_time += file_pattern_match_end - file_pattern_match_start
#            print "here, file_pattern_match is :%lf\n"%file_pattern_match_time
            if per_file_pattern_match is not None:
                matched = 1
                file_name = line.split("\t")[1]
                nprocs = line.split("\t")[2]
                if per_file_dict.get(file_name, -1) == -1:
                    per_file_dict[file_name] = {}
                per_file_dict[file_name]["nprocs"] = nprocs
                print "###path:%s, matched file is %s, nprocs is %s\n"%(out_dict["FileName"], file_name, nprocs)

    out_dict["PATH"] = path_tuple[0]
    out_dict["FS"] = path_tuple[1]

    serialize_obj = pickle.dumps(per_file_dict);
    global file_pos
    f_handle.seek(file_pos)
    f_handle.write(serialize_obj)
    out_dict["EXTERNAL_FILE_OFFSET"] = str(file_pos);
    out_dict["EXTERNAL_FILE_LENGTH"] = str(len(serialize_obj))
    file_pos += len(serialize_obj)
    print "file:%s, file_pattern_match_time:%lf, log_pattern_time:%lf, mode_pattern_time:%lf, shared_pattern_time:%lf\n"%(out_dict["FileName"], file_pattern_match_time, log_pattern_time, mode_pattern_time, shared_pattern_time)
    if file_pattern_match_time > 600 or  log_pattern_time > 600 or mode_pattern_time > 600 or shared_pattern_time > 600:
        print "problematic file is %s\n"%out_dict["FileName"]

#    '(\S+)\t([+-]?\d+(?:\.\d+)?)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t([+-]?\d+(?:\.\d+)?)\t(\S+)\t(\S+)\t(\S+)'
    return out_dict
#bw_file_pair = getJobBW(fname)
#index = 0
#stat_table = []
#for record in bw_file_pair:
#    if index == 0:
#        tmp_record = replace_ext(record[1], '.base')
#        fs_map = parseFSList(tmp_record)
#        sorted(fs_map, key=lambda x:x[0])
#    tmp_record = replace_ext(record[1], '.base')
#    path_tuple = getOnePath(tmp_record)
#    out_dict = getStatTable(record[1], path_tuple) 
#    stat_table.append(out_dict)
#    index = index + 1
#
#print "length is %d, index is %d, stat_table length is %d\n"%(len(bw_file_pair),index, len(stat_table))
#fname = plot_dir + "read_stat_table.pkl"
#pickle_file = open(fname, 'wb') 
#pickle.dump(stat_table, pickle_file)
#
