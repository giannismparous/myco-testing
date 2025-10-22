import argparse
import collections
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

import src.utils
import src.k_fns

LINE_WIDTH = 4


def get_pdf(df):
    users = src.utils.senders(df)
    num_users = len(src.utils.users(df))
    messages_per_receiver = src.k_fns.user_num_messages(users, num_users)

    count = collections.Counter(messages_per_receiver)
    min_k, max_k = np.min(list(count.keys())), np.max(list(count.keys()))
    keys = np.arange(min_k, max_k + 1)
    pdf = np.array([count[k] for k in keys]) / num_users

    return keys, pdf


def main(data_path):
    enron_path = os.path.join(data_path, "enron", "clean.csv")
    seattle_path = os.path.join(data_path, "seattle", "clean.csv")
    enron = pd.read_csv(enron_path).to_numpy()
    seattle = pd.read_csv(seattle_path).to_numpy()

    enron_k, enron_pdf = get_pdf(enron)
    seattle_k, seattle_pdf = get_pdf(seattle)

    # Extend the keys
    if enron_k[-1] > seattle_k[-1]:
        keys = enron_k
        seattle_pdf = np.append(
            seattle_pdf, np.zeros(int(enron_k[-1] - seattle_k[-1])))
    else:
        keys = seattle_k
        enron_pdf = np.append(
            enron_pdf, np.zeros(int(seattle_k[-1] - enron_k[-1])))

    plt.plot(keys, enron_pdf, label="Enron", linewidth=LINE_WIDTH)
    plt.plot(keys, seattle_pdf, label="Seattle", linewidth=LINE_WIDTH)
    print(len(keys))

    ax = plt.gca()

    # Set the line width for the axes
    for spine in ax.spines.values():
        spine.set_linewidth(LINE_WIDTH)

    plt.ylim(-0.01, 0.25)
    plt.xscale("log")
    plt.yticks(fontsize=18)
    plt.xlabel(f"Messages per sender", fontweight="bold", fontsize=24)
    plt.ylabel("Probability Density",
               fontweight="bold", fontsize=24)
    plt.tight_layout()
    plt.legend(fontsize="xx-large")
    plt.savefig(f"sender-pdf.pdf", format="pdf")
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Sender figure", description="This code produces a figure of messages by sender.")
    parser.add_argument(
        "path", type=str, help="Data path to look for data files.")
    args = parser.parse_args()

    data_path = os.path.abspath(args.path)

    main(data_path)
