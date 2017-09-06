import os,sys
import shutil,glob
import datetime
import subprocess

host_name = os.getenv('NERSC_HOST')
if host_name ==None or len(host_name) == 0:
    host_name = os.getenv('HOSTNAME')
if host_name != None:
    if host_name.startswith("cori"):
        cluster_name = "cori"
    elif host_name.startswith("edison"):
        cluster_name = "edison"
    elif host_name.startswith("b"):
        cluster_name = "babbage"
    elif host_name.startswith("n"):
        cluster_name = "lawrencium"

if len(sys.argv) <= 1: sys.exit(-1)
job_id = int(sys.argv[1])

if cluster_name == "local":
    dirname = "/Users/wyoo/spark-1.6.1/cori/"
    darshan_cp_root = dirname + "darshan/%s/"%(job_id)
    darshan_logname = darshan_cp_root + "wyoo_vpicio_uni_id%s.darshan"%(job_id)
    vpic_logname = dirname + "slurm/%s/slurm-%s.out"%(job_id,job_id)
    lmt_client_loglist = glob.glob(dirname+"LMT_client/%s/*.out"%job_id)
    lmt_server_path = dirname + "LMT_server/"
    slurm_dst = dirname + 'slurm/%s'%job_id
elif cluster_name == "cori":
    dirname = "/project/projectdirs/m888/ssio/wyoo_data/cori/"
    darshan_root = "/global/cscratch1/sd/darshanlogs/"
    darshan_cp_root = dirname + "darshan/%s/"%(job_id)
    darshan_logname = darshan_cp_root + "wyoo_vpicio_uni_id%s.darshan"%(job_id)
    app_logname = dirname + "slurm/%s/slurm-%s.out"%(job_id,job_id)
    lmt_client_src = '/global/homes/w/wyoo/vpicio_uni/'
    lmt_client_dst = dirname+'LMT_client/%s/'%job_id
    lmt_server_path = "/project/projectdirs/pma/www/daily/"
    slurm_dst = dirname + 'slurm/%s'%job_id
elif cluster_name == "edison":
    dirname = "/project/projectdirs/m888/ssio/wyoo_data/edison/"
    darshan_root = "/global/cscratch1/sd/darshanlogs/edison-temp/"
    darshan_cp_root = dirname + "darshan/%s/"%(job_id)
    #darshan_logname = darshan_cp_root + "wyoo_vpicio_uni_id%s.darshan"%(job_id)
    app_logname = dirname + "slurm/%s/slurm-%s.out"%(job_id,job_id)
    lmt_client_src = '/global/homes/w/wyoo/vpicio_edison/'
    lmt_client_dst = dirname+'LMT_client/%s/'%job_id
    lmt_server_path = "/project/projectdirs/pma/www/daily/"
    slurm_dst = dirname + 'slurm/%s'%job_id
else:
    print "error: cluster_name is not defined: ",cluster_name, " ", host_name

if not os.path.exists(slurm_dst):
    os.makedirs(slurm_dst)
if not os.path.exists(lmt_client_dst):
    os.makedirs(lmt_client_dst)

slurm_file = lmt_client_src+'slurm-%s.out'%job_id
if os.path.exists(slurm_file):
    shutil.move(slurm_file,slurm_dst)
    print lmt_client_src+'slurm-%s.out'%job_id
for outfile in glob.glob(lmt_client_src+'*.out'):
    shutil.move(outfile,lmt_client_dst)
    print outfile 
dt = datetime.date.today()
if not os.path.exists(darshan_cp_root):
    os.makedirs(darshan_cp_root)
#print darshan_root+'%s/%s/%s/wyoo_vpicio_uni_id%s*'%(dt.year,dt.month,dt.day,job_id)
#dt = datetime.date(2016,11,2)
for darshan_file in glob.glob(darshan_root+'%s/%s/%s/wyoo_vpicio_uni_id%s*'%(dt.year,dt.month,dt.day,job_id)):
    print darshan_file
    darshan_logname = darshan_cp_root + darshan_file.rpartition('/')[2]
    print darshan_logname
    shutil.copy(darshan_file, darshan_logname)
    darshan_total = darshan_logname+'.total'
    with file(darshan_total, 'wb') as target:
        subprocess.call(['darshan-parser','--total',darshan_logname],stdout=target)


