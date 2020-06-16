import datetime
import os
import re
import traceback
from pyspark.sql import Row
from os.path import expanduser
home = expanduser("~")

COMMENT_PATTERN = '(#...)'
DARSHAN_LOG_PATTERN = '(\d+)\t(\d+)\t(\S+)\t(\d+)\t(\S+)\t(\S+)\t(\S+)'

def displaymatch(match):
    if match is None:
        return None
    return '<Match: %r, groups=%r>' % (match.group(), match.groups())

comment = re.compile(COMMENT_PATTERN)
valid = re.compile(DARSHAN_LOG_PATTERN)

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
        return (logline,0)
    
    return (Row(
        rank = match.group(1),
        filehash = match.group(2),
        counter = match.group(3),
        value = match.group(4),
        suffix = match.group(5),
        mount = match.group(6),
        fs = match.group(7)        
    ),1)
    
def parse_darshan_log_line(logline):
    if len(logline) == 0:
        return (logline, 0)
    match = valid.match(logline)
    if match is None:
        print len(logline)
        print "Invalid logline: %s" % logline
        return (logline, 0)
    #print match
    return (Row(
        rank = match.group(1),
        filehash = match.group(2),
        counter = match.group(3),
        value = match.group(4),
        suffix = match.group(5),
        mount = match.group(6),
        fs = match.group(7)        
    ),1)

logname = "sbyna_iomi3d.Linux.64.CC.ftn.OPTHIGH.MPI.ex_id2100562_11-26-48462-15111169965657257860_1.darshan.base"
logname = home + "/darshan/" + logname
pname = logname + '.p'
if not os.path.exists(logname): 
    print logname
    raise Exception("logname exists")
if os.path.exists(pname): 
    print pname
    raise Exception("pname exists")
rdd = sc.textFile(logname, use_unicode=False).map(test_darshan_log_line)
    #rdd.saveAsPickleFile(pname)