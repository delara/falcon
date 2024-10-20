import os
import shutil
from argparse import ArgumentParser

import matplotlib.pyplot as plt
import pandas as pd

from conf.conf import CFG


def clear_plt():
    plt.cla()
    plt.clf()


def print_plot(output_dir='', name='', pdf_file=False, plot_title=True, grouping=False, legend_column=1):
    plt.legend(loc=0)
    if not grouping:
        plt.xlim(0, CFG.max_timestamp)

    if plot_title:
        plt.title(name)
    else:
        plt.suptitle("")
        plt.title("")

    plt.tight_layout()

    if not pdf_file:
        plt.show()
    else:
        plt.savefig(output_dir + name + '.' + CFG.output_file_extension)

    plt.close()


def parse_options():
    """Parse the command line options for plots creation"""
    parser = ArgumentParser(description='Prepare plotting parameters.')

    parser.add_argument('-i', '--input', type=str, required=True,
                        help='absolute directory/absolute file directory')

    args = parser.parse_args()
    return args


def create_sub_folders(output_dir='', folder='', create=False):
    output_dir = output_dir
    if create:
        if CFG.input_directory != (output_dir + folder):
            if os.path.exists(output_dir + folder):
                shutil.rmtree(output_dir + folder)

        os.mkdir(output_dir + folder)
        output_dir = output_dir + folder + '/'
    return output_dir


def plot_reconfiguration(ax=object, reconfiguration_metrics=pd.DataFrame()):
    if CFG.annotate_reconfiguration:
        ymin, ymax = ax.get_ylim()
        postext = ymax * .9
        count = 0
        for index, row in reconfiguration_metrics.iterrows():
            v = postext / ymax
            if row['operation'] == 'StartupTime':
                if CFG.annotate_startup:
                    txt = row['operation'] + ' ' + row['node_id'] + ' ' + row['operator_id']
            else:
                txt = row['operation'] + ' Creation'

            if CFG.annotate_startup or row['operation'] != 'StartupTime':
                ax.annotate(txt,
                            xy=(row['source_creation_timestamp'], postext),
                            xytext=(row['source_creation_timestamp'] + 1, postext),
                            xycoords='data', textcoords='data', fontsize=5, color=CFG.color_palette[count])

                ax.axvline(x=row['source_creation_timestamp'], ymin=0, ymax=v, color=CFG.color_palette[count], linewidth=2,
                           linestyle="--", alpha=.8)

                if row['operation'] != 'StartupTime':
                    ax.annotate(row['operation'] + ' ' + row['node_id'] + ' ' + row['operator_id'],
                                xy=(row['time_stamp'], postext),
                                xytext=(row['time_stamp'] + 1, postext),
                                xycoords='data', textcoords='data', fontsize=5, color=CFG.color_palette[count])

                    ax.axvline(row['time_stamp'], ymin=0, ymax=v, color=CFG.color_palette[count], linewidth=2,
                               linestyle='dotted', alpha=.8)

                postext = postext * .9
                count += 1
