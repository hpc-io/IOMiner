from pyspark import SparkContext
sc = SparkContext.getOrCreate()

def bin_op(tag, x, field):
    if x[field] > 200:
        x[tag] = 1
    else:
        x[tag] = 0
    return x

def group_op(x):
    print "grouping by field"
    ost_cnt = 0
    for record in x[1]:
        ost_cnt += record["ost_cnt"]
    return {x[0]:ost_cnt}


def reshape(x):
    my_dict = {}
    for tmp_dict in x:
        for (k,v) in tmp_dict.iteritems():
            my_dict[k] = v
    return my_dict


class darshan_slurm_tuples(object):
    def __init__(self, cur_rdd):
        self.rdd = cur_rdd

    def filter(self, field, op, value):
        if op == "=":
            new_rdd = self.rdd.filter(lambda x: x[field] == value) 
        if op == ">":
            new_rdd = self.rdd.filter(lambda x: x[field] > value) 
        if op == "<":
            new_rdd = self.rdd.filter(lambda x: x[field] < value) 
        if op == ">=":
            new_rdd = self.rdd.filter(lambda x: x[field] >= value) 
        if op == "<=":
            new_rdd = self.rdd.filter(lambda x: x[field] <= value)
        print "field is %s\n"%field
        for record in new_rdd.collect():
            print "job id is %s\n"%record[field]
        return darshan_slurm_tuples(new_rdd)

    def join(self, other_tuples, field):
        print "joining by field:%s\n"%field
        my_rdd = self.rdd.map(lambda x: (x[field], x))
        other_rdd = other_tuples.rdd.map(lambda x: (x[field], x))
        new_rdd = my_rdd.join(other_rdd)
#        for record in new_rdd.collect():
#            print record
        ret_rdd = new_rdd.map(lambda x: reshape(x[1]))
#        ret_rdd = new_rdd.map(lambda x: reshape(x[1]))
        for record in ret_rdd.collect():
            print record
        return darshan_slurm_tuples(ret_rdd) 

        
    def project(self, *kw):
        # add more element 
        print "operating on project1, size:%d\n"%len(kw)
        if len(kw) == 1:
            new_rdd = self.rdd.map(lambda x: {kw[0]:x[kw[0]]})
        if len(kw) == 2:
            new_rdd = self.rdd.map(lambda x: {kw[0]:x[kw[0]], kw[1]:x[kw[1]]})

        for record in new_rdd.collect():
            print "field:%s, value:%s\n"%(kw[1], record[kw[1]])
        #add more spark operator according to the number of project parameters
        return darshan_slurm_tuples(new_rdd)


    def bin(self, tag, bin_op, field):
        print "binning by field:%s\n"%field
        new_rdd = self.rdd.map(lambda x:bin_op(tag, x, field))
        return darshan_slurm_tuples(new_rdd)
#        for record in new_rdd.collect():
#            print "%s is %d\n"%(tag, record[tag])
        


    def percentile(self, field):
        print "percentile based on fields:%s\n"%field
        new_rdd = self.rdd.map(lambda x:x[field])
        tot_tuples = new_rdd.collect()
        return tot_tuples
        # calculate percent list
        
    def group(self, field, group_op = None):
        print "grouping based on %s\n"%field
        if group_op != None:
            new_rdd = self.rdd.groupBy(lambda x:x[field])
            grouped_rdd = new_rdd.map(lambda x: group_op((x[0], x[1])))
#            for record in grouped_rdd.collect():
#                for key,value in record.iteritems():
#                    print "key:%s, value:%d\n"%(key, value)
            return darshan_slurm_tuples(grouped_rdd)
        else:
            mapped_rdd = self.rdd.map(lambda x:(x[field], x))
            new_rdd = mapped_rdd.groupByKey()
            ret_rdd = new_rdd.map(lambda x:{field:x[0], "payload":x[1]})
#            for record in ret_rdd.collect():
#                print list(record["payload"])

#            grouped_rdd = new_rdd.map(lambda x:{field:x[0], "files":x[1]}) 
#            for record in new_rdd.collect():
#                print record
            return darshan_slurm_tuples(ret_rdd)

    def union(self, other_tuples):
        new_rdd = self.rdd.union(other_tuples.rdd)
        for record in new_rdd.collect():
            print record

    def collect(self):
        print "generate RDD\n"

class IOMiner(object):
    def __init__(self, json_file):
        self.json_file = json_file

    def init_stores(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        darshan_job_converted = 0
        darshan_file_converted = 0
        darshan_lmt_converted = 0
        if darshan_job_converted == 0:
            print "coverting darshan job\n"
            darshan_job_converted = 1
        if darshan_file_converted == 0:
            print "converting darshan file\n"
            darshan_file_converted = 1
        if darshan_lmt_converted == 0:
            print "converting lmt\n"
            darshan_lmt_converted = 1


    def load(self, job_file):
        if job_file == "Darshan_Job":
            job_tuples = self.load_darshan_job()
            return job_tuples
        if job_file == "Darshan_File":
            file_tuples = self.load_darshan_file()
            return file_tuples
        if job_file == "SLURM":
            slurm_tuples = self.load_slurm()
            return slurm_tuples
        if job_file == "LMT":
            lmt_tuples = self.load_lmt()
            return lmt_tuples

    def load_darshan_job(self):
        print "loading darshan job\n"
        my_tuple = []
        my_tuple.append({"job_id":"1", "data_size":100, "write_bw":200})
        my_tuple.append({"job_id":"2", "data_size":200, "write_bw":500})
        my_tuple.append({"job_id":"3", "data_size":300, "write_bw":700})
        my_tuple.append({"job_id":"4", "data_size":400, "write_bw":150})
        my_tuple.append({"job_id":"5", "data_size":500, "write_bw":180})
        tmp_rdd = sc.parallelize(my_tuple)
        return darshan_slurm_tuples(tmp_rdd)

    def load_darshan_job1(self):
        print "loading darshan job\n"
        my_tuple = []
        my_tuple.append({"job_id":"6", "data_size":100, "write_bw":252})
        my_tuple.append({"job_id":"7", "data_size":200, "write_bw":310})
        my_tuple.append({"job_id":"8", "data_size":300, "write_bw":70})
        my_tuple.append({"job_id":"9", "data_size":400, "write_bw":110})
        my_tuple.append({"job_id":"10", "data_size":500, "write_bw":280})
        tmp_rdd = sc.parallelize(my_tuple)
        return darshan_slurm_tuples(tmp_rdd)

    def load_darshan_file(self):
        my_tuple = []
        my_tuple.append({"job_id":"1", "file_name":"abc1.1", "ost_cnt":1})
        my_tuple.append({"job_id":"2", "file_name":"abc2.1", "ost_cnt":5})
        my_tuple.append({"job_id":"2", "file_name":"abc2.2", "ost_cnt":1})
        my_tuple.append({"job_id":"1", "file_name":"abc1.2", "ost_cnt":2})
        tmp_rdd = sc.parallelize(my_tuple)
        return darshan_slurm_tuples(tmp_rdd) 

    def load_slurm(self):
        print "loading slurm\n"
        my_tuple = []
        my_tuple.append({"job_id":"1", "nnodes":5})
        my_tuple.append({"job_id":"2", "nnodes":6})
        my_tuple.append({"job_id":"3", "nnodes":7})
        tmp_rdd = sc.parallelize(my_tuple)
        return darshan_slurm_tuples(tmp_rdd) 

    def load_lmt(self):
        print "loading lmt\n"
        
miner = IOMiner("abc")
miner.init_stores(100, 200)
darshan_job_tuples = miner.load("Darshan_Job")
darshan_job_tuples1 = miner.load_darshan_job1()
darshan_job_tuples.bin("abc", bin_op, "data_size")
darshan_job_tuples.percentile("data_size")

darshan_file_tuples = miner.load("Darshan_File")
darshan_file_tuples.group("job_id", group_op)

slurm_tuples = miner.load("SLURM")
darshan_file_tuples.group("job_id").join(darshan_job_tuples, "job_id").filter("write_bw", "<", 500)



#darshan_job_tuples.join(slurm_tuples, "job_id").project("job_id", "nnodes")
#darshan_job_tuples.union(darshan_job_tuples1)
#darshan_job_tuples.filter("data_size", ">", 100)
#darshan_job_tuples.project("data_size").filter("data_size", ">", 100)
#darshan_job_tuples.filter("data_size", "<=", 300).join(darshan_file_tuples, "job_id")






