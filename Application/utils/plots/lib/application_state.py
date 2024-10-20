import matplotlib.pyplot as plt
import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.utils import clear_plt, print_plot, create_sub_folders, plot_reconfiguration

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def get_data_center_operator(dt=pd.DataFrame()):
    return numpy.unique(dt['node_id']), numpy.unique(dt['operator_id'])


def plot_window_time_items(dt=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), output_dir=''):
    title = "Window Time vs Number of Items - Overview"

    clear_plt()
    fig, ax = plt.subplots()

    ax.plot(dt['time_stamp'], dt['windowing_time'], marker="o", color="blue", alpha=.6)
    ax.set_xlabel("Execution Time (s)")
    ax.set_ylabel("Window Time (ms)", color="blue")

    ax2 = ax.twinx()
    ax2.plot(dt['time_stamp'], dt["items"], marker="X", color="red", alpha=.6)
    ax2.set_ylabel("Number of items", color="red")

    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_window_items(dt=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), output_dir=''):
    title = "Window Number of Items"
    nodes, operators = get_data_center_operator(dt=dt)

    clear_plt()
    fig, ax = plt.subplots()
    ax.set_xlabel("Execution Time (s)")
    ax.set_ylabel("Number of items")
    count = 0
    for node in range(len(nodes)):
        for operator in range(len(operators)):
            df_dev = dt[(dt['node_id'] == nodes[node]) & (dt['operator_id'] == operators[operator])]
            if df_dev.size > 0:
                ax.plot(df_dev['time_stamp'], df_dev["items"], color=CFG.color_palette[count], marker="o",
                        label=nodes[node] + " " + operators[operator], alpha=.6)
                count += 1

    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_window_time(dt=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), output_dir=''):
    title = "Window Time"
    nodes, operators = get_data_center_operator(dt=dt)

    clear_plt()
    fig, ax = plt.subplots()
    ax.set_xlabel("Execution Time (s)")
    ax.set_ylabel("Window Time (ms)")
    count = 0
    for node in range(len(nodes)):
        for operator in range(len(operators)):
            df_dev = dt[(dt['node_id'] == nodes[node]) & (dt['operator_id'] == operators[operator])]
            if df_dev.size > 0:
                ax.plot(df_dev['time_stamp'], df_dev['windowing_time'], color=CFG.color_palette[count], marker="o",
                        label=nodes[node] + " " + operators[operator], alpha=.6)
                count += 1

    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_window_size_items(dt=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), output_dir=''):
    title = "Size Bytes vs Number of Items"

    clear_plt()
    fig, ax = plt.subplots()
    # print("Min: " + str(dt['windowing_time'].min()) + " Max: " + str(dt['windowing_time'].max()))
    ax.plot(dt['time_stamp'], dt['size_bytes'], color="red", marker="X", alpha=.6)
    ax.set_xlabel("Execution Time (s)")
    ax.set_ylabel("Window size (bytes)", color="red")

    ax2 = ax.twinx()
    ax2.plot(dt['time_stamp'], dt["items"], color="blue", marker="o", alpha=.6)
    ax2.set_ylabel("Number of items", color="blue")

    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_application_state_statistics(metrics=object):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='application_state',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)

    plot_window_time_items(dt=metrics.ApplicationState, reconfiguration_metrics=metrics.Reconfiguration, output_dir=output_dir)

    plot_window_size_items(dt=metrics.ApplicationState, reconfiguration_metrics=metrics.Reconfiguration, output_dir=output_dir)

    plot_window_items(dt=metrics.ApplicationState, reconfiguration_metrics=metrics.Reconfiguration, output_dir=output_dir)

    plot_window_time(dt=metrics.ApplicationState, reconfiguration_metrics=metrics.Reconfiguration, output_dir=output_dir)
