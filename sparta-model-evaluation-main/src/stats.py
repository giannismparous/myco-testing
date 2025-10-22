import numpy as np

import src.utils


# Aggregate latency
def avg_latency(latencies):
    return np.mean(latencies)


def med_latency(latencies):
    return np.median(latencies)


# Aggregate overhead
def broadcast_avg_overhead(num_users):
    return num_users - 1


def system_avg_overhead(k, num_rounds, num_real):
    total_traffic = np.sum(k) * num_rounds
    return total_traffic / num_real - 1


# Per round bandwidth
def broadcast_round_bandwidth(num_users, round_messages):
    return num_users * round_messages


def system_round_bandwidth(num_rounds, k):
    return np.full(num_rounds, np.sum(k))


# Per user peak bandwidth usage.
def broadcast_peak_user_bandwidth(round_messages, num_users):
    return np.full(num_users, np.max(round_messages))


def system_peak_user_bandwidth(k):
    return k


# Per user peak latency.
def peak_user_latency(latencies, users, num_users):
    return src.utils.max_user_latency(latencies, users, num_users)


def avg_user_latency(latencies, users, num_users):
    return src.utils.avg_user_latency(latencies, users, num_users)
