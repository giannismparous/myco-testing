"""
Module responsible for simulating interesting system types. We support four
systems
- direct delivery with full participation,
- broadcast,
- k-broadcast with dropped messages, and
- k-broadcast with deferred messages.

For each, given a dataframe, interval, and other parameters, it computes
- overhead, which is the fraction of dummy messages to delivered messages,
- latency, which is the difference in rounds, between when a message is
submitted and when it is sent, and
- dropped, which is the fraction of dropped messages to submitted messages.
"""

import numpy as np

import src.rounds
import src.utils
import src.stats
import src.k_fns

LATENCY = 0
OVERHEAD = 1

SIG_FIGS = 2


def broadcast(interval, df):
    """
    Computes statistics for broadcast.

    Returns overhead, latency, median traffic, maximum traffic.
    """

    sort_idx = src.utils.sort_receiver(df)
    df = df[sort_idx]
    num_users = src.utils.num_users(df)

    # Latencies
    start = np.min(src.utils.submits(df))
    message_rounds = src.rounds.round_submitted(
        src.utils.submits(df),
        start,
        interval
    )
    message_delivers = src.rounds.round_end(message_rounds, start, interval)
    latencies = message_delivers - src.utils.submits(df)

    # Overheads
    round_messages = src.rounds.messages_per_round(
        message_rounds,
        np.unique(message_rounds)
    )

    return {
        "latencies": latencies.tolist(),
        "round_messages": round_messages.tolist(),
        "users": src.utils.receivers(df).tolist(),
        "num_users": num_users,
    }


def deferred(interval, users, start, submits, k):

    start = np.min(submits)

    send_rounds = src.rounds.round_deferred(users, submits, start, interval, k)
    sends = src.rounds.round_end(send_rounds, start, interval)
    latencies = sends - submits

    num_rounds = np.max(send_rounds) - np.min(send_rounds) + 1

    avg_latency = src.stats.avg_latency(latencies)

    system_bandwidth = np.sum(k)
    user_bandwidth = np.average(k)
    total_overhead = src.stats.system_avg_overhead(k, num_rounds, len(sends))
    overhead_per_user_round = total_overhead / (len(k) * num_rounds)
    return (avg_latency, system_bandwidth, user_bandwidth, total_overhead, overhead_per_user_round)


def k_in_prep(df):
    sort_idx = src.utils.sort_sender(df)
    df = df[sort_idx]
    users = src.utils.senders(df)
    submits = src.utils.submits(df)
    start = np.min(submits)

    return (users, submits, start)


def k_out_prep(df):
    sort_idx = src.utils.sort_receiver(df)
    df = df[sort_idx]
    users = src.utils.receivers(df)
    submits = src.utils.submits(df)
    start = np.min(submits)

    return (users, submits, start)


def one_per_round(interval, df, k):

    submits = np.sort(src.utils.submits(df))
    users = np.zeros(df.shape[0], dtype=int)

    return deferred(interval, users, submits, 1, int(k), False)
