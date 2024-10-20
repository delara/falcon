import matplotlib.pyplot as plt
import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.utils import clear_plt, print_plot, create_sub_folders, plot_reconfiguration

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def prepare_df(containers_metrics=pd.DataFrame()):
    container_list = numpy.unique(containers_metrics['container_id'])
    return containers_metrics, container_list


def plot_cpu_utilization(df=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), containers=[], output_dir='',
                         per_cpu=False):
    title = 'CPU Utilization (Containers) Per_Core: ' + str(per_cpu)

    clear_plt()
    # create figure and axis objects with subplots()
    fig, ax = plt.subplots()

    for i in range(len(containers)):
        df_dev = df[(df['container_id'] == containers[i])]
        # print('Container = ' + container_list[i] + ' Initialize: = ' + str(df_dev['time_stamp'].min()))
        # make a plot
        if per_cpu:
            ax.plot(df_dev['time_stamp'], df_dev['cpu_utilization_per_core'], color=CFG.color_palette[i], marker="o",
                    label=containers[i])
        else:
            ax.plot(df_dev['time_stamp'], df_dev['container_cpu_utilization'], color=CFG.color_palette[i], marker="o",
                    label=containers[i])
        ax.set_xlabel("Execution Time(s)")
        ax.set_ylabel("%")

    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_mem_utilization(df=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), containers=[], output_dir='',
                         perc=False):
    title = 'Memory Utilization (Containers) - Perc: ' + str(perc)

    clear_plt()
    # create figure and axis objects with subplots()
    fig, ax = plt.subplots()

    for i in range(len(containers)):
        df_dev = df[(df['container_id'] == containers[i])]
        # print('Container = ' + container_list[i] + ' Initialize: = ' + str(df_dev['time_stamp'].min()))
        # make a plot
        if perc:
            ax.plot(df_dev['time_stamp'], df_dev['mem_percentage_utilization'], color=CFG.color_palette[i], marker="o",
                    label=containers[i])
            ax.set_ylabel("%")
        else:
            ax.plot(df_dev['time_stamp'], df_dev['container_memory_usage'], color=CFG.color_palette[i], marker="o",
                    label=containers[i])
            ax.set_ylabel("Bytes")
        ax.set_xlabel("Execution Time(s)")

    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_container_statistics(metrics=object):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='containers',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)

    df, containers = prepare_df(containers_metrics=metrics.Container)
    plot_cpu_utilization(df=df, reconfiguration_metrics=metrics.Reconfiguration, containers=containers,
                         output_dir=output_dir, per_cpu=False)
    plot_cpu_utilization(df=df, reconfiguration_metrics=metrics.Reconfiguration, containers=containers,
                         output_dir=output_dir, per_cpu=True)

    plot_mem_utilization(df=df, reconfiguration_metrics=metrics.Reconfiguration, containers=containers,
                         output_dir=output_dir, perc=False)
    plot_mem_utilization(df=df, reconfiguration_metrics=metrics.Reconfiguration, containers=containers,
                         output_dir=output_dir, perc=True)
