import os
import shutil
from copy import deepcopy

import matplotlib.pyplot as plt
import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.utils import create_sub_folders, clear_plt, print_plot, plot_reconfiguration

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def plot_app_comparison(application_metrics=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), field1='',
                        field2='', title_base='', ylabel='',
                        operators=[], output_dir=''):
    max = application_metrics[field1].max()
    max += max * .1
    if field2 != '':
        max2 = application_metrics[field2].max()
        max2 += max2 * .1
        if max2 > max:
            max = max2

    # create figure and axis objects with subplots()
    for j in range(len(operators)):
        c = 0
        title = title_base + " Operator: " + operators[j]
        clear_plt()
        fig, ax = plt.subplots()

        df_dep = application_metrics[(application_metrics['operator_id'] == operators[j])]

        if not df_dep.empty:
            ax.plot(df_dep['time_stamp'], df_dep[field1], color=CFG.color_palette[c], marker="o", alpha=0.8,
                    label=operators[j] + " Input")

            ax.set_ylim(0, max)

            if field2 != '':
                ax.plot(df_dep['time_stamp'], df_dep[field2], linestyle='dashed', alpha=0.8,
                        color=CFG.color_palette[c + 1],
                        marker="*", label=operators[j] + " Output")
            ax.set_xlabel('Execution Time(s)')
            ax.set_ylabel(ylabel)
            c += 1

        plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
        print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
                   plot_title=CFG.plot_title)


def plt_app_comparison_box_plot(application_metrics=pd.DataFrame(), processing_time=False, title='', field1='',
                                field2='', label1='', label2='', output_dir=''):
    clear_plt()
    fig, ax = plt.subplots()
    if not processing_time:
        df = application_metrics[['operator_id', field1, field2]]
        df.columns = ['Operator', label1, label2]
        df.boxplot(by='Operator', column=[label1, label2], grid=False, rot=45, fontsize=5)

    else:
        df = application_metrics[['operator_id', 'processing_time']]
        df.columns = ['Operator', 'Processing Time (ms)']
        df = df.set_index('Operator')

        df.boxplot(by='Operator', column=['Processing Time (ms)'],
                   grid=False, rot=45, fontsize=7, ax=ax)
        ax.set_yscale("log")
        plt.ylabel('Milliseconds')

    plt.suptitle('')
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title, grouping=True)


def load_bandwidth(metrics=object):
    df = metrics.Application[
        ['time_stamp', 'node_id', 'operator_id', 'sequence_id', 'input_rate', 'output_rate', 'input_msg_size',
         'output_msg_size']]

    df['bdw_usage_input'] = (df['input_rate'] * df['input_msg_size'] * 2) / 1048576
    df['bdw_usage_output'] = (df['output_rate'] * df['output_msg_size'] * 2) / 1048576
    df['operator_id'] = df['operator_id'].str.slice(0, 4)


def plot_throughput(dt=pd.DataFrame(), output_dir='', reconfiguration_metrics=pd.DataFrame()):
    title = "Throughput"

    clear_plt()
    # create figure and axis objects with subplots()
    lines = dt.plot.line(x='time_stamp', y='output_rate')
    lines.get_legend().remove()

    plot_reconfiguration(ax=lines, reconfiguration_metrics=reconfiguration_metrics)

    plt.ylabel('Tuples/s')
    plt.xlabel('Execution Time (s)')
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_latency(dt=pd.DataFrame(), output_dir='', reconfiguration_metrics=pd.DataFrame()):
    title = "Latency"
    clear_plt()
    # create figure and axis objects with subplots()
    lines = dt.plot.line(x='time_stamp', y='latency')
    plt.fill_between(x='time_stamp', y1='min_latency', y2='max_latency', alpha=0.5, data=dt)

    plot_reconfiguration(ax=lines, reconfiguration_metrics=reconfiguration_metrics)

    # lines.set_ylim(y2_min, y2_max)
    # lines.set_yscale('log')
    lines.get_legend().remove()
    plt.ylabel('Latency (ms)')
    plt.xlabel('Execution Time (s)')
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_throughput_latency(dt=pd.DataFrame(), output_dir='', reconfiguration_metrics=pd.DataFrame()):
    title = "Latency vs Throughput"

    clear_plt()
    fig, ax = plt.subplots()
    ax.plot(dt['time_stamp'], dt['output_rate'], color="red", marker="o")
    ax.set_xlabel("Execution Time (s)")
    ax.set_ylabel("Tuples/s", color="red")
    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)

    ax2 = ax.twinx()
    ax2.plot(dt['time_stamp'], dt["latency"], color="blue", marker="o")
    plt.fill_between(x='time_stamp', y1='min_latency', y2='max_latency', facecolor="blue", alpha=0.5, data=dt)
    ax2.set_ylabel("Latency (ms)", color="blue")

    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_latency_throughput_bandwidth(metrics=object, output_dir=''):
    df = deepcopy(metrics.Throughput_Latency)

    plot_throughput(dt=df, output_dir=output_dir, reconfiguration_metrics=metrics.Reconfiguration)

    plot_latency(dt=df, output_dir=output_dir, reconfiguration_metrics=metrics.Reconfiguration)

    plot_throughput_latency(dt=df, output_dir=output_dir, reconfiguration_metrics=metrics.Reconfiguration)


def prepare_df(application_metrics=pd.DataFrame()):
    operator_list = numpy.unique(application_metrics['operator_id'])
    return operator_list


def plot_application_statistics(metrics=object):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='application',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)

    operators = prepare_df(application_metrics=metrics.Application)

    output_dir_base = output_dir
    if CFG.create_sub_folders:
        output_dir_base = output_dir + 'operators/'
        if os.path.exists(output_dir_base):
            shutil.rmtree(output_dir_base)
        os.mkdir(output_dir_base)

    plot_app_comparison(metrics.Application, reconfiguration_metrics=metrics.Reconfiguration, field1='input_msg_size',
                        field2='output_msg_size',
                        ylabel='Mean Msg Size (Bytes)',
                        title_base='Operator Input vs Output Msg Size_', operators=operators,
                        output_dir=output_dir_base)

    plot_app_comparison(metrics.Application, reconfiguration_metrics=metrics.Reconfiguration, field1='input_rate',
                        field2='output_rate',
                        ylabel='Msgs/s',
                        title_base='Operator Input vs Output Rate_', operators=operators, output_dir=output_dir_base)

    plot_app_comparison(metrics.Application, reconfiguration_metrics=metrics.Reconfiguration, field1='processing_time',
                        ylabel='Milliseconds',
                        title_base='Operator Processing Time', operators=operators, output_dir=output_dir_base)

    plt_app_comparison_box_plot(metrics.Application, processing_time=True, title='Processing Time',
                                output_dir=output_dir_base)

    plot_latency_throughput_bandwidth(metrics=metrics, output_dir=output_dir)

    # plot_bandwidth(latxth=dt, output_dir=output_dir)
