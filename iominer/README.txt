===============================================================================
               IOMiner: A Comprehensive IO Analytics Framework - 0.1.1
===============================================================================



Background:
===============================================================================

IOMiner is a comprehensive analytics framework for HPC users to characterize platform-wide,
application-wide and individual job wide IO profiles. It synthesizes various types of IO traces on 
the supercomputers, and provides insights such as how HPC users could improve their individual application
IO, and how the system administrators could architect their storage system for better file system 
bandwidth utilization. 

The current release includes its core sweepline analysis component that allows HPC users to 
quickly identify the IO bottleneck of their applications (iominer_sweepline.py),
a batch copy tool (batch_copy_darsh_lmt_slurm.py) that copies the darshan, lmt, slurm logs in a
specified period from the global directory or database to the user specified directory,
a batch darshan parser (batch_darshan_parser.py) that parses all the darshan logs under a specified directory
into a darshan human readable format, and stores them in a target directory, and 
a batch darshan formater (construct_darshan_map.py) that extracts all the counters from
the parsed darshan logs and stores them into pandas dataframe, then persists the dataframe on the disk,
a pandas dataframe constructor (gen_pandas_for_darsh.py) that concatenates all the results from construct_darshan_map.py for a specified periods 
into a single dataframe, and persists it into the disk, (extract_fields_for_darshan.py) that further
extracts the specified fields from the output of gen_pandas_for_darsh.py within a given period, and
store them into a pandas dataframe, a slurm dataframe constructor (gen_pandas_for_slurm.py) that
takes in the result from batch_copy_darsh_lmt_slurm.py, and format slurm counters into
pandas format indexed by jobid, gen_pandas_for_lmt.py that generates the lmt dataframe for
LMT statistics, as well as parallel_coordinate_plot.py that contains functions 
for clustering io metrics based on parallel coordiante plot. 

iominer_sweepline.py takes in a job's Darshan log, and delivers multiple useful 
IO analysis and visualization results that guide users to find out this job's key IO bottlenecks, including:

1. Visualization of the IO timing on all the files written/read by all the processes in a job. 
Each HPC job typically contains multiple processes (a.k.a, MPI ranks), and each rank can write/read
one or multiple individual/shared files. 

2. Identifying the bottleneck files whose IO timing fall on this job's IO covering set, 
refer to Section II(B) of the following paper detailed instruction of this feature.
 "A Zoom-in Analysis of I/O Logs to Detect Root Causes of I/O Performance Bottlenecks"
 
3. Important IO statistics about the top N (default 5) files in the IO covering set. For example, 
the percent of small (<1KB) reads/writes, total read/write count, total read/write size, 
metadata time percent, the number of metadata operations, stripe count, the number of 
competing processes on this file, etc.

4. Same IO statistics in 3 on the job level based on Darshan summary on all the files.

5. Distribution of the IO size, IO request count, and file count among all the processes in the job.

6. Distribution of the IO size, number of competing processes on each OST used by the job.


Usage of iominer_sweepline.py:
===============================================================================
python iominer_sweepline.py --darshan <Darshan log path>

Output:
<executable>_stat.log: contains all the statistics information mentioned in 3 and 4.
For example, in the associated sample file sample_stat.log, prefix "std" refers to
the standard IO statistics, e.g., fwrite, fread, etc. "posix" referes to POSIX IO
statistics. The IO statistics on stdout/stdin/stderr are excluded.


global statistics: the job-level statistics

  (the total seconds during which there are read/write/metadata operations in the job)
  pure IO time (s): 237

  (the total seconds during which there are read operations in the job)
  pure read time (s):2.212216

  (pure write time: the total seconds during which there are write operations in the job)
  pure write time (s):222.114888 

  (job_run_time: the total elapse time of the job)
  key:job_run_time, value:20962 

  (darshan_bw: the job???s IO bandwidth extracted from Darshan)
  key:darshan_bw, value:37.065056MB/s

  (miner_r_bw: the job???s read bandwidth, calculated by read_size/pure_read_time)
  key:miner_r_bw, value:471.9393358177348MB/s

  (miner_w_bw: the job???s write bandwidth, calculated by write_size/pure_write_time)
  key:miner_w_bw, value:34.91159982715931MB/s

  (username: the name of user running this job)
  key:username, value:UNKNOWN

  (nprocs: the total number of processes in the job)
  key:nprocs, value:136.0

  (ost_cnt: the total number of osts used in the job, available since Darshan 3.10)
  key:ost_cnt, value:156.0

  (read_size: the total read size summed up by all the processes' reads)
  key:read_size, value:1094746636.0 

  (write_size: the total write size that summed up by all the processes' writes)
  key:write_size, value:8131063144.0 

  (read_count: the total read count summed up by all the processes' reads)
  key:read_count, value:140076.0 

  (write_count: the total write count summed up by all the processes' writes)
  key:write_count, value:4117124.0 

  (open_count: the total open count summed up by all the processes' opens)
  key:open_count, value:5941 

  (read_time: the total read time summed up by all the processes' read time)
  key:read_time, value:117.943975

  (write_time: the total write time summed up by all the processes' write time)
  key:write_time, value:262.539185 

 (meta_time: the metadata operations time summed up by all the processes' metadata operation time, 
  metadata operations such as open/close/fsync/seek, etc)
  key:meta_time, value:36.147733 

 (stat_count: the total stat count summed up by all the processes' stat  calls)
  key:stat_count, value:7220 

 (fsync_count: the total async count summed up by all the processes' fsync  calls)
  key:fsync_count, value:0 (the total fsync count summed up by all the processes' fsync calls)

 (seek_count: the total seek count summed up by all the processes' seek calls)
  key:seek_count, value:194857  

 (seq_w_ratio: the sequential write count/the total write count)
  key:seq_w_ratio, value:0.9993437166332615

 (seq_r_ratio: the sequential read count/the total read count)
  key:seq_r_ratio, value:0.9464005254290528

 (seq_io_ratio: the sequential read+write count/the total read+write count)
  key:seq_io_ratio, value:0.9976017100441604

  (small_r_ratio: the small read count (<1KB)/the total read count)
  key:small_r_ratio, value:0.03418858334047231

  (small_w_ratio: the small write count (<1KB)/the total write count)
  key:small_w_ratio, value:0.9915946179906168

  (small_io_ratio: the small read+write count (<1KB)/the total read+write count)
  key:small_io_ratio, value:0.9600927839894766

  (unaligned_ratio: the read+write count unaligned with stripe size/the total read+write count)
  key:unaligned_ratio, value:0.9975382880766701

  (meta_ratio: the total metadata time summed up from all 
   processes/total read+write+metadata time summed up from all processes)
  key:meta_ratio, value:0.08676200830839494

  (max_rank_pct_r: the read size by the rank spent most of the time/total read size)
  key:max_rank_pct_r, value:1.0

  (max_rank_pct_w: the write size by the rank spent most of the time/total write size)
  key:max_rank_pct_w, value:0.5914245085587051

  (max_rank_pct_wr: the write+read size by the rank spent most of the time/total write+read size)
  key:max_rank_pct_wr, value:0.6017550744972514

(top IO consumer file statistics: these represents the file level statistics similar to globgal statistics)
top IO consumer file statistics:
	filename:/global/cscratch1/sd/gebhard/PEA2PBI4/bulk/daniel_subst/2-f/tight/pwscf.bfgs:
		key:read_size, value:206810703.0
		key:write_size, value:208899700.0
		key:read_count, value:25443.0
		key:write_count, value:2862100.0
		key:open_count, value:200
		key:read_time, value:1.157481
		key:write_time, value:130.911569
		key:meta_time, value:0.928978
		key:stat_count, value:300
		key:fsync_count, value:0
		key:seek_count, value:100
		key:seq_w_ratio, value:0.9999650606198246
		key:seq_r_ratio, value:0.9961089494163424
		key:seq_io_ratio, value:0.9999310832773746
		key:small_r_ratio, value:0.0038910505836575876
		key:small_w_ratio, value:1.0
		key:small_io_ratio, value:0.9912229878481463
		key:unaligned_ratio, value:0.9999310832773746
		key:meta_ratio, value:0.006984900558074441
		(nprocs: number of processes accessing this file)
		key:nprocs, value:1
		key:stripe_width, value:1
		key:stripe_size, value:1048576
	filename:/global/cscratch1/sd/gebhard/PEA2PBI4/bulk/daniel_subst/2-f/tight/pwscf.save/data-file.xml:
		key:read_size, value:0.0
		key:write_size, value:4150717.0
		key:read_count, value:0.0
		key:write_count, value:95568.0
		key:open_count, value:101
		key:read_time, value:0.0
		key:write_time, value:14.324703
		key:meta_time, value:0.168251
		key:stat_count, value:101
		key:fsync_count, value:0
		key:seek_count, value:101
		key:seq_w_ratio, value:0.9989431608906747
		key:seq_r_ratio, value:0
		key:seq_io_ratio, value:0.9989431608906747
		key:small_r_ratio, value:0
		key:small_w_ratio, value:1.0
		key:small_io_ratio, value:1.0
		key:unaligned_ratio, value:0.9989431608906747
		key:meta_ratio, value:0.011609158491774694
		key:nprocs, value:1
		key:stripe_width, value:1
		key:stripe_size, value:1048576

<wr_executable.pdf>: the IO timing of all the processes' IO activities in a job. 
<w_executable.pdf>: the IO timing of all the processes' write activities in a job. 
<r_executable.pdf>: the IO timing of all the processes' read activities in a job. 

<executable_ost_ds.pdf>: the distribution of total read/write sizes on each OST. The stars in the 
figure are the OSTs that host those files in the IO covering set.

<executable_ost_nproc.pdf>: the distribution of concurrent accessing process count on each OST.
The stars in the figure are the OSTs that host those files in the IO covering set.

<executable_req_cnt.pdf>: the distribution of IO requests among processes. -1 in X axis means the request involving all processes.

<executable_ds_distr.pdf>: the distribution of IO size among processes

<executable_file_cnt.pdf>: the distribution of accessed file count among processes

Usage of batch_copy_darsh_lmt_slurm.py:
===============================================================================
python ./batch_copy_darsh_lmt_slurm.py <start date> <end date> <the directory that contains lmt/darshan logs> <the directory to copy the logs to> --thread_count=<thread count> --repl_type=<lmt|darshan>
python ./batch_copy_darsh_lmt_slurm.py 2020-01-01 2020-02-01 /sample/src_lmt_dir /sample/dst_lmt_dir --thread_count=1 --repl_type=lmt


Input directory structure:
  -- /sample/src_lmt_dir/2019-1-1/a
	...
	-- /sample/src_lmt_dir/2010-2-1/b

output:
...
  -- /sample/dst_lmt_dir/2019-1-1/a
	...
	-- /sample/dst_lmt_dir/2010-2-1/b
  

Usage of batch_darshan_parser.py:
===============================================================================
python ./batch_darshan_parser.py <start date> <end date> <the directory that contains accumulated original Darshan logs> <the directory that contains parsed darshan logs> --thread_count=<number of threads used for parsing, default 1> [--all specifies whether or not we wat to generate .all file as well, which contain per-file darshan statistics]

Example:
python ./batch_darshan_parser.py 2019-01-01 2019-01-31 /sample/compressed_darshan /sample/decompressed_darshan --thread_count=2 --all
/sample/compressed_darshan/

Input directory structure:
  -- /sample/compressed_darshan/2019/1/1/a.darshan
	...
	-- /sample/compressed_darshan/2019/1/31/b.darshan

output:
/sample/decompressed_darshan/2019/1/1/a.total (the parsed file by darshan-parser --total)
/sample/decompressed_darshan/2019/1/1/a.all (the parsed file by darshan-parser --all)
...
/sample/decompressed_darshan/2019/1/31/b.all
/sample/decompressed_darshan/2019/1/31/b.total


Usage of construct_darshan_map.py 
=============================================================================
python ./construct_darshan_map.py <start_date> <end_date> <the directory containing all the parsed darshans>  <the output of formatted darshan> --thread_count <thread count> [--reset: whether we want to continue from the leftover since last run or we want to reset] <--all: whether we only parsed the .total darshan files or both .total and .all darshan files, specifiying --all will parse both the .total and .all files>

Example:
python ./construct_darshan_map.py 2019-1-1 2019-1-1 /sample/decompressed_darshan /sample/format_darshan --thread_count 2 --reset --all

reset: specifies we want to clear the previous logs. Without using reset, we will skip the already parsed jobs when we run this script multiple times.

Input directory structure:
    /sample/decompressed_darshan/2019/1/1/a.all (the parsed file by darshan-parser --all)
    ...
    /sample/decompressed_darshan/2019/1/1/b.all (the pased file by darshan-parser --all)

output:
    /sample/decompressed_darshan/2019/1/1/perjob_stat.log (the file containing the job-level counters for all jobs under this directory, each job is a pandas dataframe serialized as an object by pickle)
    /sample/decompressed_darshan/2019/1/1/perfile_stat.log (the file containing the file-level counters for all the files of the jobs under this directory, the files of each job are stored in one pandas dataframe, serailized as an object by pickle)
    /sample/decompressed_darshan/2019/1/1/meta_stat.log (the metadata file that contains the offset and length of each serialized object in perjob_stat.log and perfile_stat.log)
		...
    /sample/decompressed_darshan/2019/1/1/perjob_stat.log
    /sample/decompressed_darshan/2019/1/1/perfile_stat.log
    /sample/decompressed_darshan/2019/1/1/meta_stat.log

		meta_stat.log format:
			<darshanfilename>:<offset for job1's pickle object in perjob_stat.log>:<length for job1's pickle object in per_job_stat.log>,<offset for job1's pickle object in perfile_stat.log>:<length for job1's pickle object in perfile_stat.log>

Usage of gen_pandas_for_darsh.py 
=============================================================================
Example:
python ./gen_pandas_for_darsh.py 2019-01-01 2019-01-01 /sample/format_darshan  --thread_count 8 


Input directory structure:
    /sample/decompressed_darshan/2019/1/1/perjob_stat.log
    /sample/decompressed_darshan/2019/1/1/perfile_stat.log
    ...
    /sample/decompressed_darshan/2019/1/1/meta_stat.log 

output:
    /sample/decompressed_darshan/darshan_state_<starttime>_<endtime>: serialized pandas dataframe, which can be deserialized using pickle

Usage of extract_fields_for_darshan.py 
============================================================================
python ./extract_fields_for_periods.py <start_time> <end_time> <result from gen_pandas_for_darsh.py> <output directory> <fields separated by comma> 

Example:
python ./extract_fields_for_periods.py 2017-08-01 2017-08-02 /sample/format_darshan /sample/fields total_POSIX_OPENS,total_POSIX_READS  

Input directory structure
/sample/format_darshan/darshan_state_<start_time>_<end_time>

output:
/sample/fields/darshan_state_total_POSIX_OPENS_total_POSIX_READS

Usage of gen_pandas_for_slurm.py 
=============================================================================
Example:
python ./gen_pandas_for_slurm.py 2019-01-01 2019-01-01 /sample/raw_slurm /sample/slurm_pandas 
Input directory structure:
/sample/raw_slurm/


Input directory structure:
/sample/raw_slurm/slurm_<start_timestamp>_<end_timestamp>.log

output:
/sample/slurm_pandas/slurm_pd_<start_timestamp>_<end_timestamp>


Usage of gen_pandas_for_lmt.py 
============================================================================
python ./gen_pandas_for_lmt.py <start_timestamp> <end_timestamp> <path of pytokio>  <lmt output path>  2>&1|tee info_2017.log

Example:
python ./gen_lmt_for_periods.py 2017-8-1 2017-10-31 /cscratch/pytokio  /global/parsed_lmt 

output:
/global/parsed_lmt/lmt_state_1501570800_1509519599

This file is a pickle dump of a python dictionary, you can extract the lmt logs by
loading the dictionary using pickle, and reference the metadata server workload,
oss workload, and ost read/write workload by dict["mdsCPU"], dict["ossCPU"],
dict["ostRead"] and dict["ostWrite"], respectively.



References
===============================================================================
Teng Wang, Suren Byna, Glenn Lockwood, Philip Carns, Shane Snyder, Sunggon Kim, and Nicholas Wright, 
"A Zoom-in Analysis of I/O Logs to Detect Root Causes of I/O Performance Bottlenecks", 
IEEE/ACM CCGrid 2019 

Teng Wang, Suren Byna, Glenn Lockwood, Nicholas Wright, Phil Carns, and Shane Snyder, 
"IOMiner: Large-scale Analytics Framework for Gaining Knowledge from I/O Logs", 
IEEE Cluster 2018

Contact
===============================================================================
Teng Wang (tzw0019@gmail.com)
Suren Byna (sbyna@lbl.gov)
