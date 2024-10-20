from math import inf

import matplotlib.pyplot as plt

from conf.conf import CFG
from lib.utils.utils import clear_plt, print_plot

plt.rcParams.update({'font.size': 20})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

colors = ['red', 'blue', 'k', 'green', 'y', 'c']
markers = ["x", "o", ".", "^", "d"]


def plot_latency_comparison(metrics=[], output_dir=''):
    title = 'Latency Comparison'
    axis_x = metrics[0].Metrics.Throughput_Latency['time_stamp'].to_numpy()

    clear_plt()
    fig, ax = plt.subplots()

    for m in range(len(metrics)):
        ax.plot(axis_x, metrics[m].Metrics.Throughput_Latency['latency'].to_numpy(), color=colors[m], marker=markers[m],
                label=str(metrics[m].Metrics.Name).replace('.log', ''), alpha=0.8)
        plt.fill_between(x=axis_x, y1=metrics[m].Metrics.Throughput_Latency['min_latency'].to_numpy(),
                         y2=metrics[m].Metrics.Throughput_Latency['max_latency'].to_numpy(), facecolor=colors[m],
                         alpha=0.5)

    # plot_reconfiguration(ax=ax, reconfig=metrics[0].Metrics.ReconfigFiles,
    #                      reconfig_begin=metrics[0].Metrics.ReconfigBegin,
    #                      reconfig_end=metrics[0].Metrics.ReconfigEnd,
    #                      upper_bound=axis_y1_max)

    ax.set_xlabel('Execution Time(s)')
    ax.set_ylabel('Milliseconds')
    print_plot(output_dir=output_dir, name=title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_throughput_comparison(metrics=[], output_dir=''):
    title = 'Throughput Comparison'
    axis_x = metrics[0].Metrics.Throughput_Latency['time_stamp'].to_numpy()

    clear_plt()
    fig, ax = plt.subplots()
    for m in range(len(metrics)):
        # plot_reconfiguration(ax=ax, reconfig=metrics[0].Metrics.ReconfigFiles,
        #                      reconfig_begin=metrics[0].Metrics.ReconfigBegin,
        #                      reconfig_end=metrics[0].Metrics.ReconfigEnd,
        #                      upper_bound=axis_y2_th)

        ax.plot(axis_x, metrics[m].Metrics.Throughput_Latency['output_rate'].to_numpy(), color=colors[m],
                marker=markers[m], label=str(metrics[m].Metrics.Name).replace('.log', ''), alpha=0.8)

    ax.set_xlabel('Execution Time(s)')
    ax.set_ylabel('Tuples/s')
    print_plot(output_dir=output_dir, name=title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_cdf(metrics=[], output_dir='', min=0, max=0):
    title = 'CDF Latency'
    clear_plt()

    fig, ax = plt.subplots()
    for m in range(len(metrics)):
        df_dev = metrics[m].Metrics.Throughput_Latency[(metrics[m].Metrics.Throughput_Latency['time_stamp'] > min) & (
                metrics[m].Metrics.Throughput_Latency['time_stamp'] < max)]

        ax.hist(df_dev['latency'].to_numpy(), 25, density=True, histtype='step', cumulative=True, linestyle='dashed',
                color=colors[m], label=str(metrics[m].Metrics.Name).replace('.log', ''), linewidth=2, alpha=0.8)
        # break
    ax.set_xlabel('Latency(ms)')
    ax.set_ylabel('CDF')
    print_plot(output_dir=output_dir, name=title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title, grouping=True, legend_column=1)


def plot_overlapping(metrics=[], output_dir=''):
    min_value = inf
    for m in range(len(metrics)):
        if len(metrics[m].Metrics.Throughput_Latency.index) < min_value:
            min_value = len(metrics[m].Metrics.Throughput_Latency.index)

    for m in range(len(metrics)):
        lines_to_drop = len(metrics[m].Metrics.Throughput_Latency.index) - min_value
        if lines_to_drop > 0:
            metrics[m].Metrics.Throughput_Latency = metrics[m].Metrics.Throughput_Latency.iloc[:-lines_to_drop]

    # plot_latency_comparison(metrics=metrics, output_dir=output_dir)
    # plot_throughput_comparison(metrics=metrics, output_dir=output_dir)
    plot_cdf(metrics=metrics, output_dir=output_dir, min=430, max=600)
