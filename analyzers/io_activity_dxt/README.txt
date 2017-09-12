#
# io_activity_dxt.py 
# Performance Log Miner (PerfLM), 
# Copyright (c) 2017, The Regents of the University of California, 
# through Lawrence Berkeley National Laboratory (subject to receipt
# of any required approvals from the U.S. Dept. of Energy).  
# All rights reserved.
#
# If you have questions about your rights to use or distribute this software, 
# please contact Berkeley Lab's Innovation & Partnerships Office 
# at IPO@lbl.gov.
#
# Email questions to SDMSUPPORT@LBL.GOV
# Scientific Data Management Research Group
# Lawrence Berkeley National Laboratory
#
# last update on Mon Aug  7 08:47:28 PDT 2017


To plot the read or write activity from Darshan Extended Trace (DXT) logs.

% ./io_activity_dxt.py --help
usage: io_activity_dxt.py [-h] -i DXT_LOGNAME [-o SAVEFIG] [--show] [--read]
                            [--filemode] [-f FNAME]

io activity plot from dxt log

optional arguments:
  -h, --help            show this help message and exit
  -i DXT_LOGNAME, --input DXT_LOGNAME
                        dxt log path
  -o SAVEFIG, --save SAVEFIG
                        output file name for the plot
  --show                Show the plot rather than saving to a PDF
  --read                READ I/O action to be plotted. 
                        Default is False for WRITE mode.
  --filemode            Single file mode (must be used with --fname). 
                        Default is False for all files
  -f FNAME, --fname FNAME
                        name of file to be plotted (must use with --filemode)

Example runs:
% python io_activity_dxt.py -i darshan_dxt-a.txt 

% python io_activity_dxt.py -i darshan_dxt-a.txt \
        --filemode -f /global/cscratch1/sd/asim/amrex/a24/plt00000.hdf5

% python io_activity_dxt.py -i darshan_dxt-d.txt \
	-o dxt-d.pdf

% python io_activity_dxt.py -i darshan_dxt-df.txt 

% python io_activity_dxt.py -i darshan_dxt-c.txt 

% python io_activity_dxt.py -i darshan_dxt-v.txt 


For more information on creating DXT logs, see:
http://www.mcs.anl.gov/research/projects/darshan/docs/darshan3-util.html#_darshan_dxt_parser 
