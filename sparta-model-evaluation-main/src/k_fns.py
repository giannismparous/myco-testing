import collections
import math
import numpy as np

import src.rounds
import src.utils


def user_num_messages(users, num_users):
    user_messages = np.zeros(num_users)
    count = collections.Counter(users)
    for i in range(num_users):
        user_messages[i] = count[i]

    return user_messages


# I cannot use user_avg here because I'd be double rounding, so it wouldn't be
# the same function
def avg(users, num_users, interval, min_time, max_time, base=1):
    user_messages = user_num_messages(users, num_users)
    if base != 1:
        def f(x): return src.utils.round_to_power(x, base)
        user_messages = np.vectorize(f)(user_messages)

    return np.ceil(np.full(num_users, np.sum(user_messages)) * interval /
                   (num_users * (max_time - min_time)))


def user_avg(users, num_users, interval, min_time, max_time, base=1):

    user_messages = user_num_messages(users, num_users)
    if base != 1:
        def f(x): return src.utils.round_to_power(x, base)
        user_messages = np.vectorize(f)(user_messages)

    return np.ceil(user_messages * interval / (max_time - min_time))


def k(k, num_users):
    return np.full(num_users, k)


def round_up(num, factor):
    return int(np.ceil(num / factor) * factor)


def k_peak_user(users, num_users, submits, interval, round_factor=1):
    start = np.min(submits)
    round_submitted = src.rounds.round_submitted(submits, start, interval)

    indices = [0]
    prev_user = users[0]
    for idx, user in enumerate(users[1:], 1):
        if user != prev_user:
            indices.append(idx)
            prev_user = user
    indices.append(len(users))

    peak_rates = np.zeros(num_users)
    for i in range(len(indices) - 1):
        start, end = indices[i], indices[i + 1]
        user = users[start]
        counts = collections.Counter(round_submitted[start:end])
        peak_rates[user] = np.max(list(counts.values()))

        if round_factor != 1:
            peak_rates[user] = src.utils.round_to_power(
                peak_rates[user],
                round_factor
            )

    return peak_rates


def k_peak(users, num_users, submits, interval, percentile, round_factor=1):
    peak_rates = k_peak_user(users, num_users, submits, interval, round_factor)
    value = int(np.ceil(np.percentile(peak_rates, percentile)))
    return np.full(
        num_users,
        max(1, value)
    )
