#This file reuse most of the code from Sunggon
import sys 
sys.path.append('/global/cscratch1/sd/tengwang/pytokio_set/pytokio')

import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams.update({'font.size': 14})
import datetime
import tokio.tools
import pandas
import tokio
import tokio.tools
import tokio.config
import pickle

import os
import multiprocessing
from multiprocessing import Manager
import sqlite3
import re
import datetime
import subprocess
import scandir

#hardcoded parameters:
# /global/cscratch1/sd/tengwang/pytokio_set/pytokio path of pytokio directory
# root_dir: directory of iominer
# dir: the directory of parsed Darshan (e.g. .total and .all files)

#os.chdir("/global/u2/s/sgkim/codes/darshan_code_skim/")
root_dir="/global/cscratch1/sd/tengwang/miner1217/lmt_dir/"
# This file builds a table that contains the records of all the Darshan logs stored (lmt_info_<month>_<day>.log) under root_dir. Each log corresponds to a record in the table, this record contains the LMT information associated with the log. The table is stored under "root_dir". LMT information include: 
#ossCPUMin: Minimum CPU utilization of OSSs used by the job 5-second before it starts
#ossCPUMax: Maximum CPU utilization of OSSs used by the job 5-second before it starts
#ossCPUAve: Average CPU utilization of OSSs used by the job 5-second before it starts
#ostIOMin = Min IO on OSTs used by the job 5-second before it starts
#ostIOMax = Max IO on OSTs used by the job 5-second before it starts
#ostIOAve = Average IO on OSTs used by the job 5-second before it starts
#mdsCPUAve: Average CPU utilization of metadata servers used by the job 5-second before it starts

# the path of parsed Darshan files
dir = "/global/cscratch1/sd/tengwang/miner1217/parsed_darshan/2017/"


def process(glbDict, file, mdsCPU, ossCPU, ostIO):
	fullDir = file
	tmpPath = file
	file = os.path.basename(file)
	if file.endswith(".all"):
		#Check filename with application name
		tempID = re.findall(r"id+\d+", os.path.basename(file))[0]
		#if os.path.basename(file)[file.find("_") + 1 : file.find(tempID) - 1] == app:
		if 1:
			progName = userID = jobID = startTime = runTime = numProc = numOST = stripeSize  = -1
			ioStartTime = -1
			totalFilePOSIX = totalFileMPIIO = totalFileSTDIO = 0
			writeRatePOSIX = readRatePOSIX = 0
			writeRateSTDIO = readRateSTDIO = 0
			writeRateMPIIO = readRateMPIIO = 0
			writeBytesPOSIX = writeTimePOSIX = readBytesPOSIX = readTimePOSIX =  0
			writeBytesMPIIO = writeTimeMPIIO = readBytesMPIIO = readTimeMPIIO =  0
			writeBytesSTDIO = writeTimeSTDIO = readBytesSTDIO = readTimeSTDIO =  0
			writeBytesTotal = writeRateTotal = 0
			metaTimePOSIX = 0
			readBytesTotal = readRateTotal = 0
			totalWriteReq = totalReadReq = 0	
			seqWriteReq = seqReadReq = 0	
			seqWritePct = seqReadPct = 0
			
			readLess1k = readMore1k = writeLess1k = writeMore1k = 0

			usedOST = list()					

			usedOSTDict = {}	
			progName = os.path.basename(file)[file.find("_") + 1 : file.find(tempID) - 1] 
			userID = os.path.basename(file)[:file.find("_")]
			jobID = re.findall(r"id+\d+", os.path.basename(file))[0][2:]
			glbDict["JobID"] = jobID 
			glbDict["UserID"] = userID
			glbDict["Path"] = tmpPath
                        print "##path:%s\n"%glbDict["Path"]
			glbDict["AppName"] = progName 
			try:
				f = open(fullDir , "rb", 32768 )
			except:
				print "ERROR"

			for line in f:
				if "start_time_asci: " in line:
					startTime = line.split(": ",1)[1][:-1] 
					temp = re.findall(r'\S+', startTime)
					startTime = temp[1] + " " + temp[2] + " " +  temp[3] + " " + temp[4]
					startTime = datetime.datetime.strptime(startTime, '%b %d %H:%M:%S %Y')
				if "run time: " in line:
					runTime = line.split(": ",1)[1][:-1]
				if "start_time: " in line:
					intStartTime = line.split(": ",1)[1][:-1]
					glbDict["start_time"] = str(intStartTime) 
				if "end_time: " in line:
					intEndTime = line.split(": ",1)[1][:-1]
					glbDict["end_time"] = str(intEndTime) 
				if "nprocs: " in line:
					numProc = line.split(": ",1)[1][:-1]
					glbDict["proc_cnt"] = str(numProc) 
				if ("total_POSIX_F_OPEN_START_TIMESTAMP: " in line) or ("total_POSIX_F_READ_START_TIMESTAMP: " in line) or ("total_POSIX_F_WRITE_START_TIMESTAMP: " in line):
					if ioStartTime == -1 or ioStartTime > line.split(" ")[1]:
						ioStartTime = float(line.split(" ")[1])
				if "total: " in line:
					if int(line.split(" ")[3]) == int((writeBytesPOSIX + readBytesPOSIX)*1000000):
						totalFilePOSIX = line.split(" ")[2]
					elif int(line.split(" ")[3]) == int((writeBytesMPIIO + readBytesMPIIO)*1000000):
						totalFileMPIIO = line.split(" ")[2]
					elif int(line.split(" ")[3]) == int((writeBytesSTDIO + readBytesSTDIO)*1000000):
						totalFileSTDIO = line.split(" ")[2]	
				if "LUSTRE_OST_ID_" in line and not line.startswith("#"):
					temp = int(line.split("\t")[3][14:]) + 1
					#set numOST as the highest OST
					#numOST may vary accroding to file or directory
					if int(line.split("\t")[4]) not in usedOST:
						usedOST.append(int(line.split("\t")[4]))

					if temp > numOST:
						numOST = temp
				if "LUSTRE_STRIPE_SIZE" in line and not line.startswith("#"):
					temp = int(line.split("\t")[4])
					#set stripeSize as the max stripe sizeT
					#stripesize may vary accroding to file or directory
					if temp > stripeSize:
							stripeSize = temp
				#POSIX request Size
				if "total_POSIX_SIZE_READ_0_100: " in line:
					readLess1k = readLess1k + int(line.split(" ")[1])
				elif "total_POSIX_SIZE_READ_100_1K: " in line:
					readLess1k = readLess1k + int(line.split(" ")[1])
				elif "total_POSIX_SIZE_READ" in line:
					readMore1k = readMore1k + int(line.split(" ")[1])

				if "total_POSIX_SIZE_WRITE_0_100: " in line:
					writeLess1k = writeLess1k + int(line.split(" ")[1])
				elif "total_POSIX_SIZE_WRITE_100_1K: " in line:
					writeLess1k = writeLess1k + int(line.split(" ")[1])
				elif "total_POSIX_SIZE_WRITE" in line:
					writeMore1k = writeMore1k + int(line.split(" ")[1])

		 
					
				#ADD METATIME to write/read time	
				if "total_POSIX_F_META_TIME: " in line and not line.startswith("#"):
					metaTimePOSIX = float(line.split(" ")[1])

				#POSIX I/O Rate
				#writeRatePOSIX
				if "total_POSIX_BYTES_WRITTEN:" in line and not line.startswith("#"):
					writeBytesPOSIX = float(line.split(" ")[1])/1000000
				if "total_POSIX_F_MAX_WRITE_TIME:" in line and not line.startswith("#"):
					writeTimePOSIX = float(line.split(" ")[1])
				#readRatePOSIX
				if "total_POSIX_BYTES_READ:" in line and not line.startswith("#"):
					readBytesPOSIX = float(line.split(" ")[1])/1000000
				if "total_POSIX_F_MAX_READ_TIME:" in line and not line.startswith("#"):
					readTimePOSIX = float(line.split(" ")[1])
				#MPIIO I/O Rate
				#writeRateMPIIO
				if "total_MPIIO_BYTES_WRITTEN:" in line and not line.startswith("#"):
					writeBytesMPIIO = float(line.split(" ")[1])/1000000
				if "total_MPIIO_F_WRITE_TIME:" in line and not line.startswith("#"):
					writeTimeMPIIO = float(line.split(" ")[1])
				#readRateMPIIO
				if "total_MPIIO_BYTES_READ:" in line and not line.startswith("#"):
					readBytesMPIIO = float(line.split(" ")[1])/1000000
				if "total_MPIIO_F_READ_TIME:" in line and not line.startswith("#"):
					readTimeMPIIO = float(line.split(" ")[1])
				#STDIO I/O Rate
				#writeRateSTDIO
				if "total_STDIO_BYTES_WRITTEN:" in line and not line.startswith("#"):
					writeBytesSTDIO = float(line.split(" ")[1])/1000000
				if "total_STDIO_F_WRITE_TIME:" in line and not line.startswith("#"):
					writeTimeSTDIO = float(line.split(" ")[1])
				#readRateSTDIO
				if "total_STDIO_BYTES_READ:" in line and not line.startswith("#"):
					readBytesSTDIO = float(line.split(" ")[1])/1000000
				if "total_STDIO_F_READ_TIME:" in line and not line.startswith("#"):
					readTimeSTDIO = float(line.split(" ")[1])

				#POSIX seq request
				if "total_POSIX_WRITES:" in line and not line.startswith("#"):
					totalWriteReq = float(line.split(" ")[1])
				if "total_POSIX_SEQ_WRITES:" in line and not line.startswith("#"):
					seqWriteReq = float(line.split(" ")[1])

				if "total_POSIX_READS" in line and not line.startswith("#"):
					totalReadReq = float(line.split(" ")[1])
				if "total_POSIX_SEQ_READS:" in line and not line.startswith("#"):
					seqReadReq = float(line.split(" ")[1])
			#END LINE loop
		
			writeTimePOSIX = metaTimePOSIX + writeTimePOSIX
			readTimePOSIX = metaTimePOSIX + readTimePOSIX
			#calculate rates after file iteration
			#MB/s	
			if writeBytesPOSIX != 0 and writeTimePOSIX != 0:
				writeRatePOSIX = float(writeBytesPOSIX/writeTimePOSIX)
			if readBytesPOSIX != 0:
				readRatePOSIX = readBytesPOSIX/readTimePOSIX
			if writeBytesMPIIO != 0 and writeTimeMPIIO != 0:		
				writeRateMPIIO = writeBytesMPIIO/writeTimeMPIIO
			if readBytesMPIIO != 0 and readTimeMPIIO != 0:
				readRateMPIIO = readBytesMPIIO/readTimeMPIIO
			if writeBytesSTDIO != 0 and writeTimeSTDIO != 0:	
				writeRateSTDIO = writeBytesSTDIO/writeTimeSTDIO
			if readBytesSTDIO != 0 and readTimeSTDIO != 0:
				readRateSTDIO = readBytesSTDIO/readTimeSTDIO
			#calculate total read/write btyes
			#MB/s
			if (writeBytesPOSIX or writeBytesMPIIO or writeBytesSTDIO) != 0:
				writeBytesTotal = float(writeBytesPOSIX + writeBytesMPIIO + writeBytesSTDIO)

			if (readBytesPOSIX or readBytesMPIIO or readBytesSTDIO) != 0:
				readBytesTotal = float(readBytesPOSIX + readBytesMPIIO + readBytesSTDIO)

			writeRateTotal = (writeRatePOSIX + writeRateMPIIO + writeRateSTDIO)
			readRateTotal = (readRatePOSIX + readRateMPIIO + readRateSTDIO)
			if totalWriteReq != 0:
				seqWritePct = (seqWriteReq/totalWriteReq) * 100
				writeLess1k = (writeLess1k/totalWriteReq) * 100
				writeMore1k = (writeMore1k/totalWriteReq) * 100
			if totalReadReq != 0:
				seqReadPct = (seqReadReq/totalReadReq) * 100		
				readLess1k = (readLess1k/totalReadReq) * 100
				readMore1k = (readMore1k/totalReadReq) * 100
			
			#In Case of no POSIX, just set start IO time to 0
			if ioStartTime == -1:
				ioStartTime = 0
			
			#get numCPU from SLURM
			#figure out which th darshanfile with same jobID (to match SLURM)
#			cmd = "sacct -j " + str(jobID) + " --format=ncpus,nnodes"
#			tempOut =  subprocess.check_output(cmd, shell=True)
#			tempOut = tempOut.strip()
#			tempOut = tempOut.split(" ")
#			temp = []
#			for i in tempOut:
#				if i.isdigit():
#					temp.append(i)
#			numCPU = int(temp[0])
#			numNode = int(temp[1])

			#get MDS CPU/OSS CPU/OST I/O from LMT data
			startLMT = str(startTime)
			#LMT is only collect data in 5 sec interval. so modify start time to the closest time
			if int(startLMT[-1:]) > 5:
				startLMT = startLMT[0:-1] + "5"
			if int(startLMT[-1:]) < 5:
				startLMT = startLMT[0:-1] + "0"
			try:
				mdsCPUAve = mdsCPU.loc[startLMT, "unknown_mds"]
			except:
				mdsCPUAve = 0
			ossCPUMin = 0
			ossCPUMax = 0
			ossCPUAve = 0
			ostIOMin = 0
			ostIOMax = 0
			ostIOAve = 0 
			if usedOST != []:
				for ost in usedOST:
#                                        print "path:%s, app:%s, ost:%d\n"%(glbDict["Path"], progName, ost)
					#oss CPU format: dex starts from 000 
                                        try:
                                            currOssCPU = ossCPU.loc[startLMT, "snx11168n" + str(format(ost + 4, '03'))]
                                            if ossCPUMin == 0 or ossCPUMin > currOssCPU:
                                                ossCPUMin = currOssCPU
                                            if ossCPUMax == 0 or  ossCPUMax < currOssCPU:
                                                ossCPUMax = currOssCPU
                                            ossCPUAve = ossCPUAve + currOssCPU

					#ost IO format: hex. starts from 0000
                                            currOstIO = ostIO.loc[startLMT, "snx11168-OST" + str(format(ost, '04x'))]
					    if ostIOMin == 0 or ostIOMin > currOstIO:
                                                ostIOMin = currOstIO
                                            if ostIOMax == 0 or ostIOMax < currOstIO:
                                                ostIOMax = currOstIO
					    ostIOAve = ostIOAve + currOstIO
                                        except:
                                            print "wrong path:%s, ost:%d, startLMT:%s, count:%d\n"%(glbDict["Path"], ost, startLMT, len(usedOST))
                                            
				
				ossCPUAve = ossCPUAve / len(usedOST)
				ostIOAve = ostIOAve / len(usedOST)
				ostIOAve = ostIOAve / 1000000
				ostIOMin = ostIOMin / 1000000
				ostIOMax = ostIOMax / 1000000
				glbDict["ossCPUAve"] = str(ossCPUAve) 
				glbDict["ossCPUMax"] = str(ossCPUMax) 
				glbDict["ossCPUMin"] = str(ossCPUMin) 
				glbDict["mdsCPUAve"] = str(mdsCPUAve) 
				glbDict["ostIOAve"] = str(ostIOAve) 
				glbDict["ostIOMin"] = str(ostIOMin) 
				glbDict["ostIOMax"] = str(ostIOMax) 

			#print file
			#print insertString
			#print writeBytesPOSIX
			#writeList.append(insertString)
			f.close()

main_start = datetime.datetime.now()

p = multiprocessing.Pool(16)

print "At main for loop"
#scandir has MUCH better performance than os.walk
for dirs in scandir.scandir(dir):
	if dirs.is_dir(follow_symlinks=False):
		month = int(os.path.basename(dirs.path))
		for subdirs in scandir.scandir(dirs.path):
			print "=================="
			day = int(os.path.basename(subdirs.path))
			H5LMT_BASE = '/project/projectdirs/pma/www/daily'
			h5lmt_file = 'cori_snx11168.h5lmt'
			start_time = datetime.datetime(2017, month, day, 0,0,0 )
			end_time = datetime.datetime(2017, month, day, 23,59,59 )
#			if month != 10 or day >= 15:
#				continue
			print "month:%d, day:%d\n"%(month, day)

			mdsCPU = ossCPU = ostIO = 0	

			if month == 11 and day == 5:
				mdsCPU = ossCPU = ostIO = 0
			else:
                            try:
                                print "start:%s,end:%s\n"%(start_time, end_time)
                                mdsCPU = tokio.tools.hdf5.get_dataframe_from_time_range(
                                        file_name=h5lmt_file,
                                        dataset_name='MDSCPUGroup/MDSCPUDataSet',
				        datetime_start=start_time,
			                datetime_end=end_time)
			        ossCPU = tokio.tools.hdf5.get_dataframe_from_time_range(
                                       file_name=h5lmt_file,
                                       dataset_name='OSSCPUGroup/OSSCPUDataSet',
                                       datetime_start=start_time,
                                       datetime_end=end_time)
			        ostRead = tokio.tools.hdf5.get_dataframe_from_time_range(
                                        file_name=h5lmt_file,
                                        dataset_name='OSTReadGroup/OSTBulkReadDataSet',
                                        datetime_start=start_time,datetime_end=end_time)
                                ostWrite = tokio.tools.hdf5.get_dataframe_from_time_range(
				        file_name=h5lmt_file,
				        dataset_name='OSTWriteGroup/OSTBulkWriteDataSet',
				        datetime_start=start_time,
			                datetime_end=end_time)
                        #    print ostWrite
			        ostIO = (ostRead + ostWrite)
                            except:
                                mdsCPU = ossCPU = ostIO = 0
                                print "import error for month:%s, day:%s\n"%(month, day)
                        print "Finish import for :" + str(month) + "\t" + str(day)
			print (datetime.datetime.now() - main_start)
			main_start = datetime.datetime.now()
		

                        tmp_str = root_dir+"/lmt_info_%d_%d.log"%(month, day)
                        save_fd = open(tmp_str, 'wb')
                        glb_tab = []
			#a = 0
			for file in scandir.scandir(subdirs.path):
				if file.path.endswith(".all"):
                                    glbDict={}
                                    print file
#                                    p.apply_async(process, [glbDict, file.path, mdsCPU, ossCPU, ostIO])
                                    process(glbDict, file.path, mdsCPU, ossCPU, ostIO)
                                    glb_tab.append(glbDict)
						#process(file.path, mdsCPU, ossCPU, ostIO)
					#a = a+ 1
					#if a == 10:
					#	break
                        pickle.dump(glb_tab, save_fd, -1)
                        save_fd.close()

			print "Finish processing for :" + str(month) + "\t" + str(day)
			print (datetime.datetime.now() - main_start)
			main_start = datetime.datetime.now()
#p.close()
#p.join()

print "END processing"
print (datetime.datetime.now() - main_start)
#SQLite apply
