import argparse
import json
import math
import numpy as np
import os
import pandas as pd
from tqdm import tqdm

import src.k_fns
import src.systems
import src.utils


def write_results(out_path, stats):
    """
    If outpath DNE then create it and add a record, or if it does exist then
    remove any tuple matching (interval, k, varies) and update with the new
    data.
    """

    directory = os.path.dirname(out_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(out_path, "w") as f:
        s = json.dumps(stats)
        f.write(s)


def experiment(df, interval, num_estimates, prep_fn, k_fn):
    data = np.empty((0, 6))
    for estimate in tqdm([2 ** i for i in range(0, num_estimates)]):
        (users, submits, start) = prep_fn(df)
        num_users = src.utils.num_users(df)
        k = k_fn(users, num_users, submits, interval, estimate)
        stats = src.systems.deferred(interval, users, start, submits, k)
        print(f"{estimate}: {stats}")
        data = np.vstack([data, list((estimate, *stats))])
    return data


def main(inpath, outpath, interval, system, args):
    df = pd.read_csv(os.path.join(inpath, "clean.csv")).to_numpy()

    # prep function
    filename = system
    if system == "k-in":
        prep_fn = src.systems.k_in_prep
    else:  # k-out
        prep_fn = src.systems.k_out_prep

    # k-fn
    if args.varies != None:
        filename += "*"
        varies = args.varies
        k_fn = src.k_fns.k_peak_user
    else:  # fixed
        filename += f"-{args.fixed}"
        varies = 1

        def k_fn(users, num_users, submits, interval, estimate):
            return src.k_fns.k_peak(users, num_users, submits, interval, args.fixed, estimate)

    data = experiment(df, interval, varies, prep_fn, k_fn)
    outpath = os.path.join(outpath, str(interval), f"{filename}.csv")
    header = "estimate\tlatency\tsys bw\tuser bw\toverhead\toverhead/u/r"
    np.savetxt(outpath, data, delimiter="\t", header=header)

    # print(data.shape)
    # for target in tqdm(targets):
    #     stats = stats_fn(target)
    #     l = list((target, *stats))
    #     data = np.vstack([data, l])

    # sort_idx = src.utils.sort_receiver(df)
    # df = df[sort_idx]

    # receivers = src.utils.receivers(df)
    # num_users = src.utils.num_users(df)
    # submits = src.utils.submits(df)
    # peak_rates = src.k_fns.k_peak(
    #     receivers,
    #     num_users,
    #     submits,
    #     INTERVAL,
    #     50,
    #     round_factor=1,
    # )
    # print(peak_rates)
    # print(src.systems.k_out(INTERVAL, df, peak_rates))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Simulator",
        description="This runs empirical simulations of different anonymous \
                         communication system types."
    )

    parser.add_argument("inpath", type=str, help="Path to data dir.")
    parser.add_argument("outpath", type=str, help="Path to results dir.")
    parser.add_argument("interval", type=int, help="Interval")
    parser.add_argument("system", choices=["k-in", "k-out"])

    parser.add_argument("--varies", type=int)
    parser.add_argument("--fixed", type=int)

    args = parser.parse_args()
    print(args)

    main(
        args.inpath,
        args.outpath,
        args.interval,
        args.system,
        args,
    )

# INTERVALS = [60, 300, 900, 1800, 3600, 10800, 21600, 43200, 84600]

# def get_k_fn(interval, min_time, max_time, args):
#     if args.k != None:
#         def fn(x, y): return src.k_fns.k(x, y, args.k)
#         fn_name = f"-k-{args.k}"
#     elif args.avg != None:
#         def fn(x, y): return src.k_fns.avg(
#             x,
#             y,
#             interval,
#             min_time,
#             max_time,
#             args.avg
#         )
#         fn_name = f"-avg-{args.avg}"
#     elif args.user_avg != None:
#         def fn(x, y): return src.k_fns.user_avg(
#             x,
#             y,
#             interval,
#             min_time,
#             max_time,
#             args.user_avg
#         )
#         fn_name = f"-user_avg-{args.user_avg}"
#     else:
#         raise ValueError(f"Unrecognized k function.")

#     return fn, fn_name

    # if args.latency:
    #     targets = LATENCY_TARGETS
    # else:
    #     targets = OVERHEAD_TARGETS

    # def stats_fn(target):
    #     if args.system == "k-in":
    #         print('kin')
    #         system_fn = src.systems.k_in
    #     elif args.system == "k-out":
    #         print('kout')
    #         system_fn = src.systems.k_out
    #     else:
    #         raise ValueError(f"Unrecognized system type: {args.system}")

    #     return system_fn(INTERVAL, df, target, args.varies, args.latency)

    # data = np.empty((0, 4))
    # print(data.shape)
    # for target in tqdm(targets):
    #     stats = stats_fn(target)
    #     l = list((target, *stats))
    #     data = np.vstack([data, l])
