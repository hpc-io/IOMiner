import datetime
import matplotlib
import matplotlib.pyplot as plt
import pickle 

#hardcoded parameter:
# lmt_dir: directory that stores the lmt logs constructed by extract_ost.py
# root_dir: directory that stores the output ("lustre_info.log")of this file 

# format the output of extract_ost.py into dictionary format, and store into lustre_info.log 
lmt_dir="/global/cscratch1/sd/tengwang/miner1217/lmt_dir/"

root_dir="/global/cscratch1/sd/tengwang/miner1217/"
#root_dir="/global/cscratch1/sd/tengwang/pytokio_set/pytokio/examples/lmt_dir1/"
months = [10, 11]
days = [31, 30]

out_dict = {}
for monthIdx in range(0, len(months)):
	for dayIdx in range(1, days[monthIdx] + 1):
		tmp_str = lmt_dir+"lmt_info_%d_%d.log"%(months[monthIdx], dayIdx)
		print "tmp_str is %s\n"%tmp_str
		save_fd = open(tmp_str, 'rb')
		glb_tab = pickle.load(save_fd)
		for record in glb_tab:
			if record.get("mdsCPUAve", -1) != -1:
				print "jobid:%s, userID:%s, path:%s, mdsCPU:%s\n"%(record["JobID"], record["UserID"], record["Path"], record["mdsCPUAve"])
			if out_dict.get(record["JobID"], -1) == -1:
				out_dict[record["JobID"]] = []

			out_dict[record["JobID"]].append(record)
			
		save_fd.close()

save_fd = open(root_dir+"lustre_info.log", 'wb')
pickle.dump(out_dict, save_fd, -1)
save_fd.close()
