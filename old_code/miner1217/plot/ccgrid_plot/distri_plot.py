#!/usr/bin/python
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.pylab as pl
import numpy as np
import matplotlib.colors as colors
import matplotlib.cm as cmx

def parallel_coordinates(data_sets, color_count, x_labels, style=None):
    dims = len(data_sets[0])
    x    = range(dims)
    fig, axes = plt.subplots(1, dims-1, sharey=False, figsize=(8, 10))

    if style is None:
        style = ['b-']*len(data_sets)

    jet = cm = plt.get_cmap('hsv')   
    cNorm = colors.Normalize(vmin = 0, vmax = color_count)
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=jet)
    # Calculate the limits on the data
    cursor  = 0
    min_max_range = list()
    for m in zip(*data_sets):
        mn = min(m)
        mx = max(m)
        if mn == mx:
            r = float(1)
        else:
            r  = float(mx - mn)
        cursor = cursor + 1
        min_max_range.append((mn, mx, r))
#        print "mn:%d, mx:%d, r:%d\n"%(mn, mx, r)

    # Normalize the data sets
    norm_data_sets = list()
    for ds in data_sets:
        nds = []
        for dimension,value in enumerate(ds):
                nval = (value - min_max_range[dimension][0]) / min_max_range[dimension][2]
                nds.append(nval)
        norm_data_sets.append(nds)
    data_sets = norm_data_sets

    # Plot the datasets on all the subplots
#    x_labels = ["Job No.", "App No.", "Node#", "Coll(%)", "BW (GB/s)", "Small(%)", "Size (GB)", "Non-consec(%)", "OST#", "Proc/OST Level"]
#    x_labels = ["Job No.", "App No.", "Node#", "Coll", "Small(%)", "Size (GB)", "Non-consec(%)", "OST#", "Proc/OST Level", "BW (MB/s)"]
    cursor = 0
    for i, ax in enumerate(axes):
        for dsi, d in enumerate(data_sets):
#            print "dsi is %d, style is %d\n"%(dsi, style[dsi])
            colorVal = scalarMap.to_rgba(style[dsi])
            ax.plot(x, d, color = colorVal, linewidth='1')

        ax.set_xticklabels([x_labels[i]], rotation = 90, fontsize="18")
        if cursor == len(axes) - 1:
            ax.set_xticklabels([x_labels[i], x_labels[i + 1]], rotation=90, fontsize="18")
        ax.set_xlim([x[i], x[i+1]])

        cursor = cursor + 1

   # Set the x axis ticks
#    y_full_labels = ["0 (0,1]", "1 [1, 10)", "2 [10, 100)", "3 (>=100)"]

    cursor = 0
    for dimension, (axx,xx) in enumerate(zip(axes, x[:-1])):
        if dimension == 12:
            break
        axx.xaxis.set_major_locator(ticker.FixedLocator([xx]))
        ticks = len(axx.get_yticklabels())
        labels = list()
        step = min_max_range[dimension][2] / (ticks - 1)
        mn   = min_max_range[dimension][0]
        if min_max_range[dimension][2] <= ticks - 1:
            min_tick = int(mn)
            max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
            step = 1
            ticks = max_tick - min_tick + 1
        else:
            min_tick = int(mn)
            max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
            step = min_max_range[dimension][2] / (ticks - 1)
            ticks = len(axx.get_yticklabels())
        cursor = cursor + 1

        y_labels = []
        y_values = []
        mn = min_tick
        v = mn
#        print "ticks here is %d\n"%ticks
        for i in xrange(ticks):
            if dimension != 8: 
                y_labels.append(int(v))
                y_values.append(float(v - mn)/(max_tick - min_tick))
            else:
                y_labels.append(int(v))
                y_values.append(float(v - mn)/(max_tick - min_tick))
#                y_labels.append(y_full_labels[int(v)])
#                y_values.append(float(v - mn)/(max_tick - min_tick))
            v = mn + (i + 1)*step
        axx.set_yticks(y_values)
        axx.set_yticklabels(y_labels, fontsize='15', fontweight='bold', rotation=90, color='black')
#        print "y_values:\n"
#        print y_values
#        print "y_labels:\n"
#        print y_labels

        start = mn

    # Move the final axis' ticks to the right-hand side
    axx = plt.twinx(axes[-1])
    dimension += 1
    axx.xaxis.set_major_locator(ticker.FixedLocator([x[-2], x[-1]]))
    ticks = len(axx.get_yticklabels())
    step = min_max_range[dimension][2] / (ticks - 1)
    mn   = min_max_range[dimension][0]
    if min_max_range[dimension][2] <= ticks - 1:
        min_tick = int(mn)
        max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
        step = 1
        ticks = max_tick - min_tick + 1
#        print "min_tick:%d, max_tick:%d, step:%d\n"%(min_tick, max_tick, step)
#        print "ticks is %d\n"%ticks
    else:
        min_tick = int(mn)
        max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
        ticks = len(axx.get_yticklabels())
        step = min_max_range[dimension][2] / (ticks - 1)
#        ticks = max_tick - min_tick + 1

#    print "dimension is %d\n"%dimension
    ticks = len(axx.get_yticklabels())
    print ticks
    step = min_max_range[dimension][2] / (ticks - 1)
    mn   = min_max_range[dimension][0]
    if min_max_range[dimension][2] <= ticks - 1:
        min_tick = int(mn)
        max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
        step = 1
        ticks = max_tick - min_tick + 1
#        print "min_tick:%d, max_tick:%d, step:%d\n"%(min_tick, max_tick, step)
#        print "ticks is %d\n"%ticks
    else:
        min_tick = int(mn)
        max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
        ticks = len(axx.get_yticklabels())
        step = min_max_range[dimension][2] / (ticks - 1)
#        ticks = max_tick - min_tick + 1

    y_full_labels = ["(0,1)", "[1,10)", "[10,100)", ">100"]
    y_labels = []
    y_values = []
    mn = min_tick
    v = mn
#    print "ticks is %d\n"%ticks
    for i in xrange(ticks):
#        print "v is %d, value is %lf, ticks:%d, tick_range:%ld, mn:%d\n"%(int(v - mn),float(v - mn)/(max_tick - min_tick), ticks, max_tick - min_tick, mn)
        if v > 3:
            v = 3
            print "mn:%d, step:%d, i:%d, v:%d\n"%(mn, step, i, v)
        y_labels.append(y_full_labels[int(v)])
        y_values.append(float(v - mn)/(max_tick - min_tick))
#        print "v is %d, max_tick:%d, min_tick:%d, v - mn:%d\n"%(int(v), max_tick, min_tick, v - mn)
#        y_labels.append(y_full_labels[int(v)])
#        y_values.append(float(v - mn)/(max_tick - min_tick))
        v = mn + (i + 1)*step
#    axx.set_yticklabels(y_values)
#    y_values.append(int(v/(max_tick - min_tick + 1)))
#    y_labels.append(int(v))
#    print "y_values:\n"
#    print y_values
#    print "y_labels:\n"
#    print y_labels
    axx.set_yticks(y_values)
    axx.set_yticklabels(y_labels, fontsize='15', fontweight='bold', color='black', rotation = 90)

    # Stack the subplots 
    plt.subplots_adjust(wspace=0)

    return plt


