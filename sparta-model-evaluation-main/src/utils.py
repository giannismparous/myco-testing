from itertools import repeat
import math
import numpy as np
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

SENDER_COL = 0
RECEIVER_COL = 1
SUBMIT_COL = 2


def minmax_time(df):
    return np.min(df[:, SUBMIT_COL]), np.max(df[:, SUBMIT_COL])


def senders(df):
    return df[:, SENDER_COL]


def receivers(df):
    return df[:, RECEIVER_COL]


def users(df):
    return np.unique(df[:, [SENDER_COL, RECEIVER_COL]].flatten())


def num_users(df):
    return len(users(df))


def submits(df):
    return df[:, SUBMIT_COL]


def interarrival_times(l):
    """
    Computes the interarrival times of l.
    """

    return np.array(l[1:]) - np.array(l[:-1])


def max_user_latency(latencies, users, num_users):
    max_latencies = np.zeros(num_users)
    for (latency, user) in zip(latencies, users):
        if max_latencies[user] < latency:
            max_latencies[user] = latency
    return max_latencies


def avg_user_latency(latencies, users, num_users):
    sum_latencies = np.zeros(num_users)
    num_latencies = np.zeros(num_users)

    for (latency, user) in zip(latencies, users):
        sum_latencies[user] += latency
        num_latencies[user] += 1

    for i in range(len(num_latencies)):
        if num_latencies[i] == 0:
            num_latencies[i] = 1

    return sum_latencies / num_latencies


def fn_helper(args):
    fn, *fn_args = args
    return fn(*fn_args)


def parallel_stats(fn, intervals, df, *args):
    """
    Computes stats given by fn in parallel.
    """

    input = zip(
        repeat(fn),
        intervals,
        repeat(df),
        *(repeat(arg) for arg in args)
    )
    output = process_map(fn_helper, input, tqdm_class=tqdm,
                         total=len(intervals))

    return tuple(np.array(x) for x in zip(*output))


def sort_rounds(df, rounds):
    """
    Sorts df by round then receiver.
    """

    idx = np.lexsort((receivers(df), rounds))
    return idx


def sort_receiver(df):
    """
    Sorts df by receiver then submit time.
    """

    idx = np.lexsort((submits(df), receivers(df)))
    return idx


def sort_sender(df):
    """
    Sorts df by sender then submit time.
    """

    idx = np.lexsort((submits(df), senders(df)))
    return idx


def sorted_sender_submits(df):
    df = df[sort_sender(df)]
    return df[:, [SENDER_COL, SUBMIT_COL]]


def sorted_receiver_submits(df):
    df = df[sort_receiver(df)]
    return df[:, [RECEIVER_COL, SUBMIT_COL]]


def round_to_power(num, base):
    exp = math.ceil(math.log(num, base))
    return int(base ** exp)


def round_to_mult(num, base):
    return base * math.ceil(num / base)


def round_sigfigs(num, sig_figs):
    if num == 0:
        return 0
    return round(num, -int(math.floor(math.log10(abs(num)))) + (sig_figs - 1))
