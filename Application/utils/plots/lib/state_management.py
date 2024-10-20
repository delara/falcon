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


def plot_checkpointing(dt=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), output_dir=''):
    nodes, operators = get_data_center_operator(dt=dt)
    title = "Checkpointing Duration"

    clear_plt()
    fig, ax = plt.subplots()
    count = 0
    for node in range(len(nodes)):
        df_dev = dt[(dt['operation_type'] == 'Checkpointing') & (dt['node_id'] == nodes[node])]
        for operator in range(len(operators)):
            df_op = df_dev[(df_dev['operator_id'] == operators[operator])]
            if df_op.size > 0:
                ax.plot(df_op['time_stamp'], df_op['duration'], color=CFG.color_palette[count],
                        label=nodes[node] + "-" + operators[operator])
                count += 1

    ax.set_xlabel("Execution Time (s)")
    ax.set_ylabel("Duration (ms)")
    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_checkpointing_byte_size(dt=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), output_dir=''):
    nodes, operators = get_data_center_operator(dt=dt)
    title = "Checkpointing Size"

    clear_plt()
    fig, ax = plt.subplots()
    count = 0
    for node in range(len(nodes)):
        df_dev = dt[(dt['operation_type'] == 'Checkpointing') & (dt['node_id'] == nodes[node])]
        for operator in range(len(operators)):
            df_op = df_dev[(df_dev['operator_id'] == operators[operator])]
            if df_op.size > 0:
                ax.plot(df_op['time_stamp'], df_op['size_bytes'], color=CFG.color_palette[count],
                        label=nodes[node] + "-" + operators[operator])
                count += 1

    ax.set_xlabel("Execution Time (s)")
    ax.set_ylabel("Bytes")
    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plt_operations_box_plot(dt=pd.DataFrame(), output_dir=''):
    nodes, operators = get_data_center_operator(dt=dt)

    title = "State Management Operations Times"

    for node in range(len(nodes)):
        for operator in range(len(operators)):
            df_dev = dt[(dt['node_id'] == nodes[node]) & (dt['operator_id'] == operators[operator])]
            if df_dev.size > 0:
                clear_plt()
                fig, ax = plt.subplots()
                df_dev = df_dev[['operation_type', 'duration']]
                df_dev.columns = ['Operation', 'Duration (ms)']
                df_dev.boxplot(by='Operation', column=['Duration (ms)'], grid=False, rot=45)
                # ax.set_yscale("log")
                plt.ylabel('Milliseconds')

                plt.suptitle('')
                print_plot(output_dir=output_dir,
                           name=CFG.deployment + ' - ' + title + ' Node ' + nodes[node] + ' Operator ' + operators[
                               operator], pdf_file=CFG.plot_pdf,
                           plot_title=CFG.plot_title, grouping=True)


def plt_operations_box_plot_byte_size(dt=pd.DataFrame(), output_dir=''):
    nodes, operators = get_data_center_operator(dt=dt)

    title = "State Management Operations Byte Size"

    for node in range(len(nodes)):
        for operator in range(len(operators)):
            df_dev = dt[(dt['node_id'] == nodes[node]) & (dt['operator_id'] == operators[operator])]
            if df_dev.size > 0:
                clear_plt()
                fig, ax = plt.subplots()
                df_dev = df_dev[['operation_type', 'size_bytes']]
                df_dev.columns = ['Operation', 'Bytes']
                df_dev.boxplot(by='Operation', column=['Bytes'], grid=False, rot=45)
                # ax.set_yscale("log")
                plt.ylabel('Bytes')

                plt.suptitle('')
                print_plot(output_dir=output_dir,
                           name=CFG.deployment + ' - ' + title + ' Node ' + nodes[node] + ' Operator ' + operators[
                               operator], pdf_file=CFG.plot_pdf,
                           plot_title=CFG.plot_title, grouping=True)


def plot_state_management_statistics(metrics=object):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='state_management',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)

    plot_checkpointing(dt=metrics.StateManagement, reconfiguration_metrics=metrics.Reconfiguration,
                       output_dir=output_dir)

    plot_checkpointing_byte_size(dt=metrics.StateManagement, reconfiguration_metrics=metrics.Reconfiguration,
                                 output_dir=output_dir)

    plt_operations_box_plot(dt=metrics.StateManagement, output_dir=output_dir)

    plt_operations_box_plot_byte_size(dt=metrics.StateManagement, output_dir=output_dir)
