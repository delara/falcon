import matplotlib.pyplot as plt
import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.utils import clear_plt, print_plot, create_sub_folders

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def prepare_df(hardware_metrics=pd.DataFrame()):
    device_list = numpy.unique(hardware_metrics['worker_name'])
    return hardware_metrics, device_list


def memory_consumption(df=pd.DataFrame(), devices=[], output_dir=''):
    title = "Hardware (Memory)"
    clear_plt()
    # create figure and axis objects with subplots()
    fig, ax = plt.subplots()

    for i in range(len(devices)):
        df_dev = df[(df['worker_name'] == devices[i])]
        df_dev['mem_available'] = df_dev['mem_available'] / pow(1024, 3)
        # make a plot
        ax.plot(df_dev['time_stamp'], df_dev['mem_available'], color=CFG.color_palette[i], marker="o",
                label=devices[i] + "_Available_Mem")
        # ax.plot(df_dev['time_stamp'], df_dev['mem_free'], linestyle='dashed', color=color2[len(color2) - (i + 1)], marker="*",
        #         label=device_list[i] + "_Free_Mem")
        ax.set_xlabel("Execution Time(s)")
        ax.set_ylabel("Gigabytes")

    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_hardware_statistics(hardware_metrics=pd.DataFrame()):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='hardware',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)

    df, devices = prepare_df(hardware_metrics=hardware_metrics)
    memory_consumption(df=df, devices=devices, output_dir=output_dir)
