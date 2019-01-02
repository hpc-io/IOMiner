
# coding: utf-8

# In[ ]:


import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import pickle
from miner_plot import *
from distri_plot import *
#get_ipython().run_line_magic('load_ext', 'autoreload')
#get_ipython().run_line_magic('autoreload', '2')

appname = "cesm.exe"
plot_path = "/global/cscratch1/sd/tengwang/miner0810/%s_%d_plot_factors.log"%(appname, 3)
save_fd = open(plot_path, 'rb')
output_arr = pickle.load(save_fd)
output_arr = sorted(output_arr, key = lambda x:x[1])
#output_arr = sorted(output_arr, key = lambda x:x[1])
x_labels = ["UserNo", "JobNo", "ossAvgCPU(%)", "ostAvgIO(MB)", "Datasize (GB)", "nprocs", "OST #", "Seq (%)", "Small (%)","mdsAvgCPU(%)", "MTime (%)", "MaxRankIO (%)", "BW (GB/s)" ]
print "count is %d\n"%len(output_arr)
#for record in output_arr:
#    print "jobid:%d, appid:%d, nnodes:%d, col:%d, small:%d, size:%d,nonconsec:%d, ost_cnt:%d, proc_ost:%d\n"%(record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8])
plotDistri(output_arr, "rw", "%s_u%d"%(appname, 3), x_labels)
save_fd.close()

