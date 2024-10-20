import matplotlib.pyplot as plt
import numpy
import pandas as pd

from conf.conf import CFG
from lib.utils.utils import clear_plt, print_plot, create_sub_folders

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

def plot_reconfiguration_statistics(reconfiguration_metrics=pd.DataFrame()):
    output_dir = create_sub_folders(output_dir=CFG.output_directory, folder='reconfiguration',
                                    create=CFG.create_sub_folders and CFG.plot_pdf)