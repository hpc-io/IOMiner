# -*- coding: utf-8 -*-
"""
@author: tzw00
"""

import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import parallel_coordinates
import random
from collections import OrderedDict
import matplotlib.pylab as pl
import math
import numpy as np


def GenTicksLabelsForCategorical(col_val_lst, col_label_lst):
    maxVal = max(col_val_lst)
    minVal = min(col_val_lst)
    
   
    if maxVal == minVal:
        norm_ticks = [0, 1]
        show_labels = [0, col_label_lst[0]]
        return
        
    
    od = OrderedDict()
    for idx in range(0, len(col_val_lst)):
        od[col_val_lst[idx]] = col_label_lst[idx]
        
    norm_values = [ float((x - minVal))/(maxVal - minVal) for x in col_val_lst]
        
    show_labels = []
    norm_ticks = []
    
    for key,value in od.items():        
        norm_ticks.append(float(key - minVal) / (maxVal - minVal))
        show_labels.append(value)
    return (norm_values, norm_ticks, show_labels)

    

                
def GenTicksLabelsForNum(col_val_lst, is_log_scale, max_ticks):
    minVal = min(col_val_lst)
    maxVal = max(col_val_lst)
    
    
  #  print(col_val_lst)
  #  print("max ticks is %d, minVal:%d, maxVal:%d\n"%(max_ticks, minVal, maxVal))
    if minVal == maxVal:
        if is_log_scale:
            showTicks = [float(minVal/10), minVal]
            normTicks = [0.1, 1]
        else:
            showTicks = [minVal, maxVal]
            normTicks = [0, 1]
        return (normTicks, showTicks)
        
    lowerBound = 1
    
    if minVal < 1:
        tmpVal = 1
        while(tmpVal > minVal):
            tmpVal /= 10
        lowerBound = tmpVal
    else:
        if minVal == 1:
            lowerBound = 1
        else:
            tmpVal = 1
            while tmpVal * 10 <= minVal:
                tmpVal *= 10
            lowerBound = tmpVal
            
    if maxVal < 1:
        tmpVal = 1
        while(tmpVal / 10 >= maxVal):
            tmpVal /= 10
        upperBound = tmpVal
    else:
        if maxVal == 1:
            upperBound = 1
        else:
            tmpVal = 1
            while tmpVal < maxVal:
                tmpVal *= 10
            upperBound = tmpVal
            
    if is_log_scale:
        normUpperBound = 1
        normLowerBound = float(1)/(upperBound/lowerBound)
    else:
        normUpperBound = 1
        normLowerBound = 0
        
    if max_ticks < 2:
        return None

    show_ticks = []
    norm_ticks = []
    
    if is_log_scale:
        norm_values = [ float(x/(float(upperBound/lowerBound))) for x in col_val_lst ]
    else:
        norm_values = [ float((x - minVal))/(maxVal - minVal) for x in col_val_lst]
        
    if is_log_scale:
        show_ticks.append(upperBound)
        norm_ticks.append(normUpperBound)
    else:
        show_ticks.append(minVal)
        norm_ticks.append(0)
        
    if is_log_scale:
        tensCnt = getTens(upperBound/lowerBound)
        
        if tensCnt % (max_ticks - 1) == 0:
            mulTens = int(tensCnt / (max_ticks - 1))
        else:
            mulTens = int(tensCnt / (max_ticks - 1))
     #   mulTens = tensCnt % (max_ticks - 1) == 0 ? tensCnt/(max_ticks - 1) : tensCnt/(max_ticks - 1) + 1
     
        tmpNormHigh = normUpperBound
        tmpHigh = upperBound
        
        while tmpNormHigh >= normLowerBound:
         #   print("tmpNormHigh is %f, mulTens is %lf\n"%(tmpNormHigh, 10 ** int(mulTens)))
            tmpNormHigh /= (10 ** mulTens)
            tmpHigh /= (10 ** mulTens)   
            norm_ticks.append(tmpNormHigh)
            show_ticks.append(tmpHigh)
        norm_ticks.reverse()
        show_ticks.reverse()
        
        logRangeLow = math.log(norm_ticks[0], 10 ** mulTens)
        logRangeHigh = math.log(norm_ticks[len(norm_ticks) - 1], 10 ** mulTens)
        
        tmp_ticks = [float(math.log(norm_ticks[i], 10 ** mulTens) - logRangeLow)/(logRangeHigh - logRangeLow) for i in range(0, len(norm_ticks))]
        norm_ticks = tmp_ticks.copy()
        
        tmp_values = [float(math.log(norm_values[i], 10 ** mulTens) - logRangeLow)/(logRangeHigh - logRangeLow) for i in range(0, len(norm_values))]
        norm_values = tmp_values.copy()
        
    else:
        if (maxVal - minVal) % (max_ticks - 1) == 0:
            rangeVal = (maxVal - minVal) / (max_ticks - 1)
        else:
            rangeVal = (maxVal - minVal) / (max_ticks - 1)

        tmpLow = minVal
        while tmpLow < maxVal:
            tmpLow += rangeVal

            show_ticks.append(tmpLow)
            norm_ticks.append(float(tmpLow - minVal)/(maxVal - minVal))

    return (norm_values, norm_ticks, show_ticks)

        
def getTens(val):
    count = 0
    val = int(val)
    while int(val) != 0:
        val /= 10
        count += 1
    return count
        
def GetColorLst(class_field_vals):
    labelDict = {}
    count = 0
    for i in range(0, len(class_field_vals)):
        if labelDict.get(class_field_vals[i], -1) == -1:
            labelDict[class_field_vals[i]] = count
          #  print("count is %d\n"%count)
            count += 1
    return labelDict
    
# Generates the parallel coordinate plot for the passed-in data frame
# class_field: the field in the dataframe for clustering. The rows
#              that have the same value on this filed, 
#              are colored the same in the plot
# field_lst: a set of tuples that specify how to draw each field in
#            the plot. Each tuple is formated as 
#            (field_name, is_numeric (0/1), is_log_scale (0/1))
# max_tick_cnt: the maximum ticks to be shown on all the y-axis, if
#               the field is log scale, we will not enforce this restriction
# file_name: the name of the file which the plot will be saved

def drawParCol(df, field_lst, class_field, max_tick_cnt, file_name):
    idx = 0
    drawDf = pd.DataFrame()
    tickDict = dict()
    labelDict = dict()
    
    for field in field_lst:
        fieldName = field[0]
        fieldType = field[1]
        is_log_scale = field[2]

        if fieldName == class_field:
            drawDf[fieldName] = df[fieldName]
            continue
        
        row = df[fieldName].tolist()
        
        if fieldType == 1:
            row = [int (i) for i in df[fieldName].tolist()]
            (x_norm_values, x_ticks, x_labels) = GenTicksLabelsForNum(row, is_log_scale, max_tick_cnt)

            drawDf[fieldName] = x_norm_values
            tickDict[fieldName] = x_ticks
            labelDict[fieldName] = x_labels
             
        if fieldType == 0:
            df[fieldName] = df[fieldName].astype('category')
            (x_norm_values, x_ticks, x_labels) = GenTicksLabelsForCategorical(df[fieldName].cat.codes, df[fieldName].tolist())
            drawDf[fieldName] = x_norm_values
            tickDict[fieldName] = x_ticks
            labelDict[fieldName] = x_labels

    class_field_vals = drawDf[class_field].astype('category').cat.codes
    color_dict = GetColorLst(class_field_vals)
    colors = pl.get_cmap('RdYlGn')(np.linspace(0.15, 0.85, len(color_dict)))

    parallel_coordinates(drawDf, class_field, color = colors)
    
    ax = plt.gca()
    
    index = 0
    for (columnName, columnData) in tickDict.items():
        if columnName == class_field:
            continue
        
        rowValues = columnData
        labelValues = labelDict[columnName]
        for i in range(0, len(rowValues)):
            ax.annotate(int(labelValues[i]), xy = (index, rowValues[i]), ha = 'right', va = 'center')
        index += 1
    
    ax.set_yticks([])
    
    plt.savefig(file_name)
    


# Here is an example of how to use the parallel coordinate plot for clustering.
df = pd.DataFrame()

max_tick_cnt = 5


df = pd.DataFrame([["dog", 10, 4000, 1000],\
                   ["dog", 30, 500, 10],\
                   ["cat", 20, 100, 570000],\
                   ["dog", 50, 2000, 300],\
                   ["dog", 70, 700, 477],\
                   ["cat", 40, 300, 9150],\
                   ["dog", 50, 1500, 376],\
                   ["dog", 70, 800, 88965],\
                   ["cat", 10, 50, 3000000],\
                   ["cat", 5, 30, 27]],
                   columns = ["pet", "weight", "price", "valuation"])
    
field_lst = [("pet", 0, 0), ("weight", 1, 0), ("price", 1, 0), ("valuation", 1, 1)]
drawParCol(df, field_lst, "pet", 4, "pet.pdf")

    
    
                
    
    
        
                
        
        
    
