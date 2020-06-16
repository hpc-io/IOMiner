import pickle
import string
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
import json
from distri_plot import *

def plotDistri(data, io_type, low, x_labels):
    print "dimension:%d\n"%len(data)
    title = "%s_%s.jpg"%(low, io_type)
    style = []
    color_dict = {}
    color_count = 0
    tot_ost_cnt_r = 0
    tot_ost_cnt_w = 0
    for i in range(0, len(data)):
        if color_dict.get(data[i][0], -1) == -1:
            color_dict[data[i][0]] = 1
            color_count += 1
        style.append(int(data[i][0]))

    parallel_coordinates(data, color_count, x_labels, style).savefig(title, bbox_inches = 'tight', format = 'jpg')
    print "after plotting, color count:%d\n"%color_count
