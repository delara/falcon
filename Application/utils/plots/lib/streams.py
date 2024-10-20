import matplotlib.pyplot as plt
import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.utils import clear_plt, print_plot, create_sub_folders, plot_reconfiguration

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (7, 4)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def prepare_df(streams_metrics=pd.DataFrame()):
    df = streams_metrics
    df = df[['time_stamp', 'connection', 'transferring_time', 'min_transferring_time', 'max_transferring_time',
             'std_transferring_time', 'median_transferring_time']]
    df.columns = ['time_stamp', 'connection', 'transferring_time', 'min_transferring_time', 'max_transferring_time',
                  'std_transferring_time', 'median_transferring_time']
    connections = numpy.unique(df['connection'])
    return df, connections


def transferring_times(df=pd.DataFrame(), reconfiguration_metrics=pd.DataFrame(), connections=[], output_dir=''):
    title = "Transferring Time Between Operators"

    clear_plt()
    fig, ax = plt.subplots()
    for i in range((len(connections))):
        # if connections[i] != 'nan->OP_P_002' and connections[i] != 'nan->OP_P_001' and connections[i] !='OP_R_001->OP_B_001' and connections[i] != 'OP_P_001->OP_R_001' and connections[i] != 'OP_J_001->OP_A_001'  and connections[i] != 'OP_I_001->OP_J_001'  and connections[i] != 'OP_B_001->OP_I_001'  and connections[i] != 'OP_A_001->OP_C_001' and connections[i] != 'OP_B_002->OP_I_001' and connections[i] != 'OP_I_002->OP_J_001':
        # if connections[i] == 'OP_I_002->OP_J_001':
        df_con = df[(df['connection'] == connections[i])]
        ax.plot(df_con['time_stamp'], df_con['transferring_time'], color=CFG.color_palette[i], marker="o", alpha=0.8,
                label=connections[i])
        plt.fill_between(x=df_con['time_stamp'], y1=df_con['min_transferring_time'], y2=df_con['max_transferring_time'],
                         facecolor=CFG.color_palette[i], alpha=0.2, data=df_con)

        ax.set_xlabel('Execution Time(s)')
        ax.set_ylabel('Milliseconds')

    plot_reconfiguration(ax=ax, reconfiguration_metrics=reconfiguration_metrics)
    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_operator_connections_statistics(metrics=object):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='streams',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)

    df, connections = prepare_df(streams_metrics=metrics.OperatorConnections)
    transferring_times(df=df, reconfiguration_metrics=metrics.Reconfiguration, connections=connections,
                       output_dir=output_dir)
