import re,glob

VPIC_PATTERN = '(\s+)(\d+)(\s+)(\d+)(\s+)(0x[0-9a-fA-F]+)(\s+)(\d+)'
vpicp = re.compile(VPIC_PATTERN)

def parse_darshan_log_line(logline):
    if len(logline) == 0:
        return (logline, 0)
    match = vpicp.match(logline)

    if match is None:
        #print len(logline)
        #print "Invalid logline: %s" % logline
        return (logline,-2)
    
    return (match.group(2), 1)

def parse_vpic(file_name):
    ost_list = []
    with open(file_name) as infile:
        for line in infile:
            (content, flag) = parse_darshan_log_line(line)
            if flag == 1:
                ost_list.append(int(content))
    return sorted(ost_list)

path = '/project/projectdirs/m888/ssio/wyoo_data/cori/slurm/' 
file_list = glob.glob(path+'*/slurm-*.out')
#file_list = [slurm-2993268.out']
print sc.parallelize(file_list).map(lambda x:len(parse_vpic(x))).collect()
