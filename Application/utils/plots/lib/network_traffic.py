import matplotlib.pyplot as plt
import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.utils import clear_plt, print_plot, create_sub_folders

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def traffic(traffic_metrics=pd.DataFrame(), output_dir=''):
    links = numpy.unique(traffic_metrics['link'])
    title = "Traffic"

    df = traffic_metrics[['time_stamp', 'link',  'transferred_data']]

    df = df.groupby(
        ['time_stamp', 'link']
    ).agg(
        {
            'transferred_data': 'max'
        }
    )
    df = df.reset_index()
    df = df.sort_values(by='time_stamp', ascending=True)

    clear_plt()
    fig, ax = plt.subplots()
    for i in range((len(links))):
        # if connections[i] != 'nan->OP_P_002' and connections[i] != 'nan->OP_P_001' and connections[i] !='OP_R_001->OP_B_001' and connections[i] != 'OP_P_001->OP_R_001' and connections[i] != 'OP_J_001->OP_A_001'  and connections[i] != 'OP_I_001->OP_J_001'  and connections[i] != 'OP_B_001->OP_I_001'  and connections[i] != 'OP_A_001->OP_C_001' and connections[i] != 'OP_B_002->OP_I_001' and connections[i] != 'OP_I_002->OP_J_001':
        # if connections[i] == 'OP_I_002->OP_J_001':
        df_con = df[(df['link'] == links[i])]
        ax.plot(df['time_stamp'], df['transferred_data'], color=CFG.color_palette[i], marker="o", alpha=0.8,
                label=links[i])

        ax.set_xlabel('Execution Time(s)')
        ax.set_ylabel('MB')

    print_plot(output_dir=output_dir, name=CFG.deployment + ' - ' + title, pdf_file=CFG.plot_pdf,
               plot_title=CFG.plot_title)


def plot_network_traffic_statistics(traffic_metrics=pd.DataFrame()):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='traffic',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)
    traffic(traffic_metrics=traffic_metrics, output_dir=output_dir)
