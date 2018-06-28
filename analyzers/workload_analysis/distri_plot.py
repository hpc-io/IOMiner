#!/usr/bin/python
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

def parallel_coordinates(data_sets, is_log_scale, is_per_file = 0, is_per_factor = 0, is_large = 0, style=None):

    dims = len(data_sets[0])
    x    = range(dims)
    fig, axes = plt.subplots(1, dims-1, sharey=False)

    if style is None:
        style = ['b-']*len(data_sets)

    # Calculate the limits on the data
    cursor  = 0
    min_max_range = list()
    for m in zip(*data_sets):
        mn = min(m)
        mx = max(m)
        print "cursor:%d\n"%cursor
        if is_log_scale[cursor] == 1:
            r = float(mx)/float(mn)
        if is_log_scale[cursor] == 0: 
            if mn == mx:
                r = float(1)
            else:
                r  = float(mx - mn)
#            mn -= 0.5
#            mx = mn + 1.
        cursor = cursor + 1
        min_max_range.append((mn, mx, r))

    # Normalize the data sets
#    for ds in data_sets:
#        print "\n"
#        for i in ds:
#            print "%lf,"%i

    norm_data_sets = list()
    for ds in data_sets:
        nds = []
        for dimension,value in enumerate(ds):
#                print "min range:%lf, max_range:%lf\n"%(min_max_range[dimension][0], min_max_range[dimension][1])
                if is_log_scale[dimension] == 1:
                    nval = float(value) / min_max_range[dimension][1] 
#                    print "nval:%lf, max:%lf, value:%lf\n"%(nval, min_max_range[dimension][1], value)
                if is_log_scale[dimension] == 0:
                    nval = (value - min_max_range[dimension][0]) / min_max_range[dimension][2]
                nds.append(nval)
        norm_data_sets.append(nds)
    data_sets = norm_data_sets

#    for ds in data_sets:
#        print "\n"
#        for i in ds:
#            print "%lf,"%i
    # Plot the datasets on all the subplots
    if is_per_factor == 1:
        if is_large == 1:
            x_labels = ["Job No.", "App No.", "JobID", "Col(%)", "JobID", "BW (GB/s)", "JobNo.", "Small(%)", "JobID", "Size (GB)", "JobID", "Nonconsec(%)","JobID", "OST#", "JobID", "Proc/OST Level"]
        else:
            x_labels = ["Job No.", "App No.", "JobID", "Col(%)", "JobID", "BW (MB/s)", "JobNo.", "Small(%)", "JobID", "Size (GB)", "JobID", "Nonconsec(%)","JobID", "OST#", "JobID", "Proc/OST Level"]

    else:
        if is_large == 1:
            x_labels = ["Job No.", "App No.", "Col(%)", "BW (GB/s)", "Small(%)", "Size (GB)", "Nonconsec(%)", "OST#", "Node#", "Proc/OST Level"]
        else:
            x_labels = ["Job No.", "App No.", "Col(%)", "BW (MB/s)", "Small(%)", "Size (GB)", "Nonconsec(%)", "OST#", "Node#", "Proc/OST Level"]
    cursor = 0
    for i, ax in enumerate(axes):
        for dsi, d in enumerate(data_sets):
            ax.plot(x, d, style[dsi], linewidth='0.2')
#        if is_log_scale[i] == 1:
#            ax.set_yscale("log")
        ax.set_xticklabels([x_labels[i]], rotation = 90)
        if cursor == len(axes) - 1:
            ax.set_xticklabels([x_labels[i], x_labels[i + 1]], rotation=90)
        ax.set_xlim([x[i], x[i+1]])

        cursor = cursor + 1

   # Set the x axis ticks
    if is_per_file:
       y_full_labels = ["(0,1)GB", "[1,2)GB", "[2,4)GB", "[4,8)GB", "[8,16)GB", "[16,100)GB",">=100GB"]
    else:
       y_full_labels = ["(1,2)GB", "[2,4)GB", "[4,8)GB", "[8,16)GB", "[16,100)GB", ">=100GB"]

    cursor = 0
    for dimension, (axx,xx) in enumerate(zip(axes, x[:-1])):
        axx.xaxis.set_major_locator(ticker.FixedLocator([xx]))
        ticks = len(axx.get_yticklabels())
        labels = list()
        step = min_max_range[dimension][2] / (ticks - 1)
        mn   = min_max_range[dimension][0]
        if min_max_range[dimension][2] <= ticks - 1:
            min_tick = int(mn)
            max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
#            print "min_tick:%d, max_tick:%d, step:%d\n"%(min_tick, max_tick, step)
#            print "min_tick:%d, max_tick:%d\n"%(min_tick, max_tick)
            step = 1
            ticks = max_tick - min_tick + 1
        else:
            min_tick = int(mn)
            max_tick = int(mn + min_max_range[dimension][2] - 0.001) + 1
            step = min_max_range[dimension][2] / (ticks - 1)
            ticks = len(axx.get_yticklabels())
        if is_log_scale[dimension] == 1:
#            print "log cursor is %d\n"%cursor
            axx.set_yscale('log')
        cursor = cursor + 1

        y_labels = []
        y_values = []
        mn = min_tick
        v = mn
#        print "min_tick:%d\n"%min_tick
        if is_log_scale[dimension] == 0:
            for i in xrange(ticks):
#                y_labels.append(int(v))
#                y_values.append(float(v - mn)/(max_tick - min_tick))
                if dimension != -1: 
                    y_labels.append(int(v))
                    y_values.append(float(v - mn)/(max_tick - min_tick))
                else:
                    y_labels.append(y_full_labels[int(v)])
                    y_values.append(float(v - mn)/(max_tick - min_tick))
                v = mn + (i + 1)*step
            axx.set_yticks(y_values)
            axx.set_yticklabels(y_labels, fontsize='10', fontweight='bold', rotation=90, color='red')

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

    y_full_labels = ["0 (0,1]", "1 [1, 10)", "2 [10, 100)", "3 (>=100)"]
    y_labels = []
    y_values = []
    mn = min_tick
    v = mn
    for i in xrange(ticks):
#        print "v is %d, value is %lf, ticks:%d, tick_range:%ld, mn:%d\n"%(int(v - mn),float(v - mn)/(max_tick - min_tick), ticks, max_tick - min_tick, mn)
        y_labels.append(y_full_labels[int(v)])
        y_values.append(float(v - mn)/(max_tick - min_tick))
        v = mn + (i + 1)*step
#    axx.set_yticklabels(y_values)
#    y_values.append(int(v/(max_tick - min_tick + 1)))
#    y_labels.append(int(v))
    axx.set_yticks(y_values)
    axx.set_yticklabels(y_labels, fontsize='10', fontweight='bold', rotation=90, color='red')
#    if is_log_scale[dimension] == 1:
#        print "log cursor is %d\n"%cursor
#        axx.set_yscale('log')
#

    # Stack the subplots 
    plt.subplots_adjust(wspace=0)

    return plt


