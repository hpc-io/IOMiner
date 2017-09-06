import sys
import math
import cPickle
import datetime
import matplotlib.pyplot as plt
from operator import mul
from pyspark.mllib.stat import Statistics
from matplotlib.backends.backend_pdf import PdfPages
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
import os

METRIC_SUFFIXES = {'K': 1e3, 'M': 1e6, 'G': 1e9}

#----------------------------------------------------------------------------------
# Beginning of helper functions

def convertToDateTime(slurmDateTime):
    """
        Converts a string in slurm date time format (YYYY-MM-DDTHH:MM:SS)
        into a python datetime object
    """
    [date, time] = slurmDateTime.split("T")
    dateFields = [int(x) for x in date.split("-")]
    timeFields = [int(x) for x in time.split(":")]
    
    dateObject = datetime.datetime(dateFields[0], dateFields[1], dateFields[2],
                                   timeFields[0], timeFields[1], timeFields[2])
    return dateObject

def timeDifference(t1, t2):
    """
        Takes two strings in slurm time format and returns the time
        difference in seconds. Assumes t1 < t2.
    """
    firstDate = convertToDateTime(t1)
    secondDate = convertToDateTime(t2)
    return (secondDate - firstDate).total_seconds()

# A temporary solution to truncated data
# You don't need to understand this...
def amendTime(slurmTime):
    """
        In case there is a '+' at the end of the string, we fix it
        by adding the appropriate number of zeros.
    """
    fixedTime = slurmTime
    if fixedTime[-1] == '+':
        fixedTime = fixedTime[:-1]
        if (fixedTime[-1] == ':'):
            fixedTime += "00"
        elif (fixedTime[-2] == ':'):
            if fixedTime.count(':') == 1:
                fixedTime += "0:00"
            else:
                fixedTime += "0"
        else:
            fixedTime += ":00"
                
    return fixedTime

def convertTime(slurmTime):
    """
        Takes in a string in slurm time format ([DD-[HH:]]MM:SS) and
        returns the number of seconds
    """
    if slurmTime.find("-") != -1:
        [days, rest] = slurmTime.split("-")
        # Icky magic constants...
        return int(days) * 24 * 60 * 60 + convertTime(rest)
    else:
        # amendedTime = amendTime(slurmTime) # temporary solution to '+'
        splitTime = slurmTime.split(":")
    
        # In the case we have (MM:SS)
        if len(splitTime) == 2: 
            return int(splitTime[0]) * 60 + float(splitTime[1])
        
        # In the case we have (HH:MM:SS)
        return int(splitTime[0]) * 60 * 60 + int(splitTime[1]) * 60 \
                                           + float(splitTime[2])

def convertMetricSuffix(num):
    """
        Takes in a string that could have a metric suffix and
        returns the value of the string as a float.
        
        Example: convertMetricSuffix("1.46K") => 1460.0
    """
    # A very temporary fix to the plus issue
    
    if ((num[-1]) == '+') or ((num[-1]) == '?'):
        num = num[:-1]
    if (num[-1].isalpha()):
        base = float(num[:-1])
        return base * METRIC_SUFFIXES[num[-1]]
    
    return float(num)

def reduceJobID(jobID):
    """
        Modifies jobID to just be the actual jobID instead of a subjob
    """
#     if jobID[-1] == '+':
#         jobID = jobID[:-1]
#     print jobID
    position = jobID.find('.')
    if position == -1:
        return jobID
    else:
        return jobID[:position]
    
def reduceState(state1, state2):
    """
        Takes in two states from log and returns COMPLETED only if
        both states are COMPLETED
    """
    if (state1 == "COMPLETED") and (state2 == "COMPLETED"):
        return "COMPLETED"
    return "NOT ALL COMPLETED"

def convertToReductionTuple(entry, headers, maxEntrySize):
    """
        For a given entry in the slurm job log, converts the tuples
        to help perform reductions. headers is a dictionary that contains
        the indices of the fields.
        
        Current output format:
        (jobID, elapsedTime, AllocNodes, NTasks, AllocCPUS, SystemCPU, UserCPU
         AveCPU*, MinCPU, AveVMSize*, MaxVMSize, AveRSS*, MaxRSS,
         ConsumedEnergy, NodeList)
         
        Where starred entries are multiplied by the number of tasks in the entry
        in order to find the average for the overall job.
        
        WARNING: Headers do not work for all log entries. Indices sometimes
                 need to be manually calculated
    """
    JobIDIndex          = headers["JobID"]
    StartTimeIndex      = headers["Start"]
    EndTimeIndex        = headers["End"]
    ElapsedTimeIndex    = headers["Elapsed"]
    AllocNodesIndex     = headers["AllocNodes"]
    NTasksIndex         = headers["NTasks"]
    AllocCPUSIndex      = headers["AllocCPUS"]
    SystemCPUIndex      = headers["SystemCPU"]
    UserCPUIndex        = headers["UserCPU"]
    AveCPUIndex         = headers["AveCPU"]
    MinCPUIndex         = headers["MinCPU"]
    AveVMSizeIndex      = headers["AveVMSize"]
    MaxVMSizeIndex      = headers["MaxVMSize"]
    AveRSSIndex         = headers["AveRSS"]
    MaxRSSIndex         = headers["MaxRSS"]
    AvePagesIndex       = headers["AvePages"]
    MaxPagesIndex       = headers["MaxPages"]
    ConsumedEnergyIndex = headers["ConsumedEnergy"]
    NodeListIndex       = headers["NodeList"]
    StateIndex          = headers["State"]
    
    # In the case our job log entry is short
    if (len(entry) < maxEntrySize):
        return (reduceJobID(entry[JobIDIndex]),(
                convertTime(entry[ElapsedTimeIndex]),
                int(entry[AllocNodesIndex]),
                0,               # NTasks
                int(entry[AllocCPUSIndex - 1]),
                convertTime(entry[SystemCPUIndex - 1]), # Need to shift because of missing entries
                convertTime(entry[UserCPUIndex - 1]),
                0,               # TotalCPU
                entry[StateIndex], # State
                0,               # AveCPU
                float("inf"),    # MinCPU; inf b/c it shouldn't contribute to minp
                0,               # AveVMSize
                0,               # MaxVMSize
                0,               # AveRSS
                0,               # MaxRSS
                0,               # AvePages
                0,               # MaxPages
                0,               # Empty spot to sum # of nodes
                0,               # ConsumedEnergy
                entry[-1]))      # Nodelist
    else:
        # Painful case where State is "Cancelled by [userid]"; This makes
        # the log entries longer, and we have to compensate for that.
        offset = 0
        if entry[StateIndex + 1] == "by":
            offset = 2
        
        # We need to add offsets to entries after the "State" entry
        ntasks = float(entry[NTasksIndex])
        return (reduceJobID(entry[JobIDIndex]),(
                0,               # ElapsedTime
                0,               # AllocNodes
                ntasks,
                0,               # AllocCPUS
                0,               # SystemCPU
                0,               # UserCPU
                0,               # TotalCPU
                entry[StateIndex], # State
                convertTime(entry[AveCPUIndex + offset]) * ntasks,
                convertTime(entry[MinCPUIndex + offset]),                  
                convertMetricSuffix(entry[AveVMSizeIndex + offset]) * ntasks,
                convertMetricSuffix(entry[MaxVMSizeIndex + offset]),
                convertMetricSuffix(entry[AveRSSIndex + offset]) * ntasks,
                convertMetricSuffix(entry[MaxRSSIndex + offset]),
                convertMetricSuffix(entry[AvePagesIndex + offset]) * ntasks,
                convertMetricSuffix(entry[MaxPagesIndex + offset]),
                int(entry[AllocNodesIndex + offset]) * convertTime(entry[ElapsedTimeIndex]),  # To sum later
                convertMetricSuffix(entry[ConsumedEnergyIndex + offset]),
                ""))             # NodeList

def combineTuple(t1, t2):
    """
        Aggregates job characteristics together
    """
    return (t1[0] + t2[0],       # Elapsed Time
            t1[1] + t2[1],       # AllocNodes
            t1[2] + t2[2],       # NTasks
            t1[3] + t2[3],       # AllocCPUS
            t1[4] + t2[4],       # SystemCPU
            t1[5] + t2[5],       # UserCPU
            t1[6] + t2[6],       # TotalCPU
            reduceState(t1[7],t2[7]), # State
            t1[8] + t2[8],       # AveCPU
            min(t1[9], t2[9]),   # MinCPU
            t1[10] + t2[10],     # AveVMSize
            max(t1[11], t2[11]), # MaxVMSize
            t1[12] + t2[12],     # AveRSS
            max(t1[13], t2[13]), # MaxRSS
            t1[14] + t2[14],     # Number of pages faults
            max(t1[15], t2[15]), # Largest number of pages faults of all tasks
            t1[16] + t2[16],     # Sum of node time products
            t1[17] + t2[17],     # ConsumedEnergy
            max(t1[18], t2[18])) # NodeList

def normalize(t):
    """
        Normalizes aggregated job characteristics from combineTuple
    """
    ntasks = t[2]
    listVersion = list(t)
    listVersion[6] = t[4] + t[5]
    t = tuple(listVersion)
    if ntasks == 0:
        return t
    
    firstHalf = t[:8]
    secondHalf = t[8:]
    newSecondHalf = (secondHalf[0]/ntasks, secondHalf[1],
                     secondHalf[2]/ntasks, secondHalf[3],
                     secondHalf[4]/ntasks, secondHalf[5],
                     secondHalf[6], secondHalf[7],
                     secondHalf[9]/secondHalf[8], # Watts per node
                     secondHalf[9], secondHalf[10])
    return firstHalf + newSecondHalf

def outputFile(data, prefix):
    lenData = range(len(data))
    listOfVectors = []
    for m in range(len(data[0])):
        newList = [data[i][m] for i in lenData]
        listOfVectors += [newList]

    secondListOfVectors = [list(x) for x in data]
    cPickle.dump(listOfVectors, open(prefix + 'normalizedLists.p', 'wb'))
    cPickle.dump(secondListOfVectors, open(prefix + 'correlationLists.p', 'wb'))

def transpose(tupleValues):
    """
        Takes in a list of tuples and returns a list of lists
        
        Example: [(1,2),(3,4),(5,6)] => [[1,3,5],[2,4,6]]
    """
    return zip(*tupleValues)
    
#     collectedValues = rddOfValues.collect()
#     listsOfData = [[] for x in collectedValues[0]]
#     for i in range(len(collectedValues)): # For each tuple
#         for j in range(len(collectedValues[0])): # For each element in the tuple
#             listsOfData[j].append(collectedValues[i][j])
#     return listsOfData

def calcCPURatio(values):
    """
        Returns the cpu ratio from a tuple of values
    """
    return values[6]/values[0]/values[3]

def aggregateXValues(model, p):
    """
        Creates new x values for linear regression.
        
        Note: Not currently used; planned for linear regressions with
              multiple input features.
    """
    total = model.intercept
    for i in range(len(p.features)):
        total += model.weights[0]*p.features[0]
    return total

# End of helper functions
#----------------------------------------------------------------------------------
# Beginning of interactive plotting class

# Modified from http://scipy-cookbook.readthedocs.io/items/Matplotlib_Interactive_Plotting.html
class AnnoteFinder(object):
    """
        Callback for matplotlib to display an annotation when points are
        clicked on.  The point which is closest to the click and within
        xtol and ytol is identified.

        Register this function like this:

        scatter(xdata, ydata)
        af = AnnoteFinder(xdata, ydata, annotes)
        connect('button_press_event', af)
    """

    def __init__(self, xdata, ydata, annotes, ax=None, xtol=None, ytol=None):
        self.data = zip(xdata, ydata, annotes)
        if xtol is None:
            xtol = ((max(xdata) - min(xdata))/float(len(xdata)))/2
        if ytol is None:
            ytol = ((max(ydata) - min(ydata))/float(len(ydata)))/2
        self.xtol = xtol
        self.ytol = ytol
        if ax is None:
            self.ax = plt.gca()
        else:
            self.ax = ax
        self.drawnAnnotations = {}
        self.links = []

    def distance(self, x1, x2, y1, y2):
        """
            Return the distance between two points
        """
        return(math.sqrt((x1 - x2)**2 + (y1 - y2)**2))

    def __call__(self, event):
        ind = event.ind
        xVal, yVal, annotation = self.data[ind[0]]
        self.drawAnnote(xVal, yVal, annotation)
        for l in self.links:
            l.drawSpecificAnnote(annotation)
        print(xVal, yVal, annotation)

    def drawAnnote(self, x, y, annote):
        """
            Draw the annotation on the plot
        """
        if (x, y) in self.drawnAnnotations:
            markers = self.drawnAnnotations[(x, y)]
            for m in markers:
                m.set_visible(not m.get_visible())
            self.ax.figure.canvas.draw_idle()
        else:
            xyCoordinate = tuple(["%.5f" % v for v in [x,y]])
            t = self.ax.text(x, y, " %s \n %s" % (xyCoordinate, annote))
            m = self.ax.scatter([x], [y], marker='d', c='r', zorder=100)
            self.drawnAnnotations[(x, y)] = (t, m)
            self.ax.figure.canvas.draw_idle()

    def drawSpecificAnnote(self, annote):
        annotesToDraw = [(x, y, a) for x, y, a in self.data if a == annote]
        for x, y, a in annotesToDraw:
            self.drawAnnote(self.ax, x, y, a)

# End of interactive plotting class
#----------------------------------------------------------------------------------
# Beginning of plotting functions

def plotHistograms(rddOfValues, labels, dimensions, fileName = "",
                   xLim=[], yLim=[], xLogBool=False, yLogBool=False):
    """
        Plots the histograms of the rdd with labels and dimensions.
        The rdd can be a list of tuples, and the # of labels and
        dimensions must be equal to the sizes of the tuples.
    """
    transposedValues = transpose(rddOfValues.collect())
    if fileName == "":
        fileName = "slurm_histograms.pdf"
    pp = PdfPages(fileName)
    for i in range(len(labels)):
        plt.figure(figsize=(14,10))
        plt.hist(transposedValues[i], bins = 100)
        plt.title(labels[i])
        plt.xlabel(dimensions[i])
        plt.ylabel("Number of jobs")
        if xLim:
            plt.xlim(*xLim)
        if yLim:
            plt.ylim(*yLim)
        if xLogBool:
            plt.xscale('symlog', nonposy='clip')
        if yLogBool:
            plt.yscale('symlog', nonposy='clip')
        plt.savefig(pp, format='pdf')
        plt.clf()
    pp.close()
    plt.close('all')

def plotScatterPlot(rddOfValues, labels, dimensions, fileName = "",
                    xLim=[], yLim=[], xLogBool=False, yLogBool=False):
    """
        Returns a scatter plot of rddValues. The rdd's tuples must be pairs.
        Likewise, labels and dimensions must contain two elements.
    """
    transposedValues = transpose(rddOfValues.collect())
    if fileName == "":
        fileName = labels[1] + " vs. " + labels[0] + ".pdf"
    pp = PdfPages(fileName)
    plt.figure(figsize=(14,10))
#     lowess = sm.nonparametric.lowess(transposedValues[1], transposedValues[0], frac=0.75)
    plt.plot(transposedValues[0], transposedValues[1], 'ro')
#     plt.plot(lowess[:, 0], lowess[:, 1], linewidth=2.4)
    plt.title(labels[1] + " vs. " + labels[0])
    plt.xlabel(dimensions[0])
    plt.ylabel(dimensions[1])
    if xLim:
        plt.xlim(*xLim)
    if yLim:
        plt.ylim(*yLim)
    if xLogBool:
        plt.xscale('symlog', nonposy='clip')
    if yLogBool:
        plt.yscale('symlog', nonposy='clip')
    plt.savefig(pp, format="pdf")
    plt.clf()
    pp.close()
    plt.close('all')
    
def plotMultipleScatterPlots(yRdd, xRddOfValues, labels, dimensions, fileName = "",
                             xRanges=[], xLogBool=False, yLogBool=False):
    """
        Returns multiple scatter plots.
    """
    yValue  = yRdd.collect()
    xValues = xRddOfValues.collect()
    if fileName == "":
        fileName = "scatterPlotsOf" + labels[0] + ".pdf"
    pp = PdfPages(fileName)
    for i in range(len(labels) - 1):
        currentXList = [x[i] for x in xValues]
        plt.figure(figsize=(14,10))
        plt.plot(currentXList, yValue, 'ro')
        plt.title(labels[0] + " vs. " + labels[i + 1])
        plt.xlabel(dimensions[i + 1])
        plt.ylabel(dimensions[0])
        if xRanges:
            plt.xlim(*xRanges[i])
        if xLogBool:
            plt.xscale('symlog', nonposy='clip')
        if yLogBool:
            plt.yscale('symlog', nonposy='clip')
        plt.savefig(pp, format="pdf")
        plt.clf()
    pp.close()
    plt.close('all')

def interactivePlot(rddOfValues, labels, dimensions, annotations,
                    xLim=[], yLim=[], xLogBool=False, yLogBool=False):
    """
        Draws an interactive plot of what plotScatterPlot draws. Uses annotations
        for the annotations of the points where annotations is an rdd
    """
    transposedValues = transpose(rddOfValues.collect())
    x = transposedValues[0]
    y = transposedValues[1]
    annotes = annotations.collect()

    fig, ax = plt.subplots()
    
    # For now we plot both ways and test
    l, = plt.plot(x, y, 'bo', picker=True)
    ax.scatter(x,y, marker = 'o', s=35, c='b', picker=True)
    
    af = AnnoteFinder(x,y, annotes, ax=ax)
    fig.canvas.mpl_connect('pick_event', af)
    plt.title(labels[1] + " vs. " + labels[0])
    plt.xlabel(dimensions[0])
    plt.ylabel(dimensions[1])
    if xLim:
        plt.xlim(*xLim)
    if yLim:
        plt.ylim(*yLim)
    if xLogBool:
        plt.xscale('symlog', nonposy='clip')
    if yLogBool:
        plt.yscale('symlog', nonposy='clip')
    plt.show()
    plt.close('all')

def plotLinearRegression(featureTuplesRDD, iterations, stepSize, labels, dimensions,
                         fileName = "", xLim=[], yLim=[], xLogBool=False, yLogBool=False):
    """
        Takes in a rdd of feature tuples where the last element of each tuple is
        the feature we want to predict, and the other elements are what we use
        to predict the output.
        
        Warning: Currently only works with one input feature. It only does the usual
                 linear regressions.
    """
    labeledPoints = featureTuplesRDD.map(lambda x: LabeledPoint(x[-1], list(x[:-1])))
    model = LinearRegressionWithSGD.train(labeledPoints,
                                          iterations=iterations,
                                          step=stepSize,
                                          intercept=True)
    print model.intercept
    print model.weights
    regressionPoints = labeledPoints.map(lambda p: (map(mul,p.features,model.weights),
                                                    model.predict(p.features)))
    
#     originalScatterValues = transpose(featureTuplesRDD.collect())
    transposedValues = transpose(regressionPoints.collect())
    print transposedValues
    originalScatterValues = featureTuplesRDD.map(lambda x: x[:-1])\
                                            .map(lambda x: map(mul,x,model.weights))\
                                            .map(lambda x: x[0] + x[1])\
                                            .collect()
    originalYValues = featureTuplesRDD.map(lambda x: x[-1]).collect()
#     print originalYValues
#     print originalScatterValues
    if fileName == "":
        fileName = labels[1] + " vs. " + labels[0] + ".pdf"
    pp = PdfPages(fileName)
    plt.figure(figsize=(14,10))
    plt.plot(transposedValues[0], transposedValues[1], 'b')
    plt.plot(originalScatterValues, originalYValues, 'ro')
    plt.title(labels[-1] + " vs. " + str(labels[:-1]))
    plt.xlabel(str(dimensions[:-1]))
    plt.ylabel(dimensions[-1])
    if xLim:
        plt.xlim(*xLim)
    if yLim:
        plt.ylim(*yLim)
    if xLogBool:
        plt.xscale('symlog', nonposy='clip')
    if yLogBool:
        plt.yscale('symlog', nonposy='clip')
    plt.savefig(pp, format="pdf")
    plt.clf()
    pp.close()
    plt.close('all')

host_name = os.getenv('HOSTNAME')
cluster_name = "local"
if host_name != None:
    if host_name.startswith("cori"):
        cluster_name = "cori"
    elif host_name.startswith("edison"):
        cluster_name = "edison"
    elif host_name.startswith("b"):
        cluster_name = "babbage"
    elif host_name.startswith("n"):
        cluster_name = "lawrencium"

from datetime import datetime, timedelta
def get_slurm_rdd(sc, file_path, start_date, end_date):
    d = datetime(start_date.year,start_date.month,start_date.day)
    delta = timedelta(days=1)
    file_list = []
    while d <= end_date:
        sdate = d.strftime("%y_%m_%d")
        file_list.append(file_path + 'corislurm_' + sdate +'.log')
        d += delta
    
    #for slurm_log in file_list:
    #file_rdd = sc.parallelize(file_list)
    raw = sc.textFile(','.join(file_list))
    #job_data = newRaw.zipWithIndex().filter(lambda pair: pair[1] > 1).keys() \
    #             .map(lambda x: x.split()).map(lambda x: (x[0], x[1], x[2], x[-1]))
    #job_data = job_data.filter(lambda x: (x[0].find(".") == -1) and (x[0].find("_") == -1))
    #return job_data
    return raw

# dates for getting slurm job logs
def get_jobData(sc, start_date,end_date):

    #start_date = datetime(2016,3,1)
    #end_date = datetime(2016,5,31)
    start_date = datetime(2017,5,20,0,0,0)
    end_date = datetime(2017,6,20,0,0,0)
    if cluster_name == "local":
        raw = get_slurm_rdd(sc, "/Users/wyoo/spark-1.6.1/cori/slurm/",start_date,end_date)
    elif cluster_name == "lawrencium":
        raw = get_slurm_rdd(sc, "/global/home/users/wyoo/slurm/cori/",start_date,end_date)
    else:
        #raw = get_slurm_rdd(sc, "/project/projectdirs/m1248/slurm/cori/",start_date,end_date)
        filename = '/project/projectdirs/m888/ssio/wyoo_data/slurm_copy/' + cluster_name +'/corislurm_%s_%s.log'%(start_date.strftime("%y_%m_%d-%H-%M-%S"),end_date.strftime("%y_%m_%d-%H-%M-%S"))
        print filename
        raw = sc.textFile(filename)
    # Create a dictionary to map headers to indices
    headers = {}
    headerLines = raw.first().split()
    for i in range(len(headerLines)):
        headers[headerLines[i]] = i
    
    # print headerLines
    maxEntrySize = len(headerLines) - 2
    
    
    # Remove headers and prepare for reduction
                  #.filter(lambda x:x[1] == 'ptfproc')\
    jobData = raw.zipWithIndex()\
                 .filter(lambda pair: pair[1] > 1).keys()\
                 .map(lambda x: tuple(x.split()))\
                 .filter(lambda x: len(x) >= maxEntrySize)\
                 .filter(lambda x: x[0][-6:] != ".batch")\
                 .filter(lambda x: x[5] == "COMPLETED")\
    #              .filter(lambda x: x[6] != "1")
    return jobData

# jobData.take(1)
# print jobData.collect()
wattsLabels     = ["Watts per Node", "CPU Ratio", "Elapsed time",
                   "# of Nodes", "# of tasks", "# of cores",
                   "Total CPU time / # of Nodes", 
                   "Ave CPU time per task / # of Nodes",
                   "Ave VM Size per task / # of Nodes",
                   "Max VM Size / # of Nodes",
                   "Ave RSS per task / # of Nodes",
                   "Max RSS / # of Nodes",
                   "# of Page faults / # of Nodes",
                   "Max Pages faults of all tasks / # of Nodes",
                   "Average CPU Frequency"]

wattsDimensions = ["Watts per Node", "CPU Ratio", "Elapsed time",
                   "# of Nodes", "# of tasks", "# of cores",
                   "Total CPU time / # of Nodes", 
                   "Ave CPU time per task / # of Nodes",
                   "Ave VM Size per task / # of Nodes",
                   "Max VM Size / # of Nodes",
                   "Ave RSS per task / # of Nodes",
                   "Max RSS / # of Nodes",
                   "# of Page faults / # of Nodes",
                   "Max Pages faults of all tasks / # of Nodes",
                   "Average CPU Frequency"]

def convertToFloat(s):
    lst = s.split('K')
    if len(lst) <= 1:
        return float(s)
    else:
        return float(lst[0])*1000

# filter only jobs using 1000 more cores on Cori Haswell (core number per node <= 64)
def filter_large_jobs(sc, jobData):
    return jobData.map(lambda x:(x[0],convertToFloat(x[6]),convertToFloat(x[8]))).filter(lambda x: (x[2]/x[1])<=64)\
    .filter(lambda x:int(x[2]) > 1000).map(lambda x:(int(x[0].split('.')[0]))).distinct().collect()


