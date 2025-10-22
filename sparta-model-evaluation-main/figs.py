import argparse
import os
import json
import matplotlib.pyplot as plt
import numpy as np

import src.stats

FONT_SIZE = 24
LINE_WIDTH = 4
MSG_SIZE = 128
MSG_TO_MB = MSG_SIZE / 1000000

ESTIMATE = 0
SYS_BANDWIDTH = 2
USR_BANDWIDTH = 3


def read_data(in_path, dataset, interval, name):
    path = os.path.join(in_path, dataset, interval, f"{name}.csv")
    data = np.genfromtxt(path, delimiter='\t', skip_header=1)
    print(data.shape)
    if len(data.shape) == 1:
        data = data.reshape(1, -1)
    return data[:, [ESTIMATE, SYS_BANDWIDTH, USR_BANDWIDTH]]


def user_bandwidth_fig(x, y, outmax, inmax, interval):
    plt.plot(x, MSG_SIZE / interval * y,
             label="$k^*_{out}$", linewidth=LINE_WIDTH)
    plt.plot(x, MSG_SIZE / interval * outmax,
             label="$k_{out}$", linewidth=LINE_WIDTH)
    plt.plot(x, MSG_SIZE / interval * inmax,
             label="$k_{in}$", linewidth=LINE_WIDTH)

    ax = plt.gca()

    # Set the line width for the axes
    for spine in ax.spines.values():
        spine.set_linewidth(LINE_WIDTH)

    plt.yscale("log")
    plt.xscale("log")
    plt.yticks(fontsize=18)
    plt.xticks(x, labels=[f"${i}$" for i in x], fontsize=18)
    plt.xlabel("Estimation Factor (log)", fontweight="bold", fontsize=24)
    plt.ylabel("User Bandwidth (B/s)",
               fontweight="bold", fontsize=24)
    plt.tight_layout()
    plt.legend(fontsize="xx-large")
    plt.savefig("enron.pdf", format="pdf")
    plt.show()


def main(in_path, out_path, dataset, interval):
    k_out = "k-out*"
    k_out_100 = "k-out-100"
    k_in_100 = "k-in-100"

    k_out_data = read_data(in_path, dataset, interval, k_out)
    k_out_100_data = read_data(in_path, dataset, interval, k_out_100)
    k_in_100_data = read_data(in_path, dataset, interval, k_in_100)

    # x = k_out_data[:, 0].flatten()
    x = [2**i for i in range(0, 10)]
    sys_out_y = k_out_data[:, 1].flatten()
    sys_out_100_y = np.full(len(x), k_out_100_data[:, 1])
    sys_in_100_y = np.full(len(x), k_in_100_data[:, 1])

    usr_out_y = k_out_data[:, 2].flatten()
    usr_out_100_y = np.full(usr_out_y.shape, k_out_100_data[:, 2].flatten())
    usr_in_100_y = np.full(usr_out_y.shape, k_in_100_data[:, 2].flatten())

    # system_bandwidth_fig(x, sys_out_y, sys_out_100_y, sys_in_100_y)
    user_bandwidth_fig(x, usr_out_y, usr_out_100_y,
                       usr_in_100_y, float(interval))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Simulator",
        description="""
                    This builds the figures.
                    """
    )

    parser.add_argument("inpath", type=str, help="Path to data.")
    parser.add_argument("outpath", type=str, help="Path to figs.")
    parser.add_argument("dataset", type=str, help="Dataset to use.")
    parser.add_argument("interval", type=str)

    args = parser.parse_args()
    in_path = os.path.abspath(args.inpath)
    out_path = os.path.abspath(args.outpath)

    main(in_path, out_path, args.dataset, args.interval)
