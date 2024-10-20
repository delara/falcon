from argparse import ArgumentParser

import matplotlib.pyplot as plt
import pandas as pd

from lib.utils.utils import clear_plt

plt.rcParams.update({'font.size': 12})
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def parse_options():
    """Parse the command line options for plots creation"""
    parser = ArgumentParser(description='Prepare plotting parameters.')

    parser.add_argument('-i', '--input', type=str, required=True,
                        help='absolute file directory')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    opts = parse_options()

    df = pd.read_csv(opts.input, sep=':', header=None)
    df.columns = ['hit', 'key']
    # df = df[(df['hit'] > 30)]
    # print(df)

    clear_plt()
    fig, ax = plt.subplots()
    ax = df.plot.barh(x='key', y='hit')
    ax.set_xscale("log")
    plt.show()
