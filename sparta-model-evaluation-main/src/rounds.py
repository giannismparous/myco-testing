import collections
import math
import numpy as np


# def k_average(users, submits, )


def round_deferred(users, submits, start, interval, k):
    """
    Given a round schedule, determine what round messages are sent at the end
    of.
    """

    deferred = round_submitted(submits, start, interval)

    count = 0
    prev_round = deferred[0]
    prev_user = users[0]

    for i in range(0, len(users)):
        if prev_user == users[i]:  # same user
            if deferred[i] <= prev_round:  # same round
                deferred[i] = prev_round

                if count >= k[users[i]]:  # different round
                    deferred[i] += 1
                    count = 0
            else:  # different round
                count = 0
        else:  # different sender
            prev_user = users[i]
            count = 0

        count += 1
        prev_round = deferred[i]

    return deferred


def round_submitted(submits, start, interval):
    """
    This computes the round that a particular message was submitted in. If we
    don't care about how many messages a user submits in a single round, then
    use this.
    """

    return np.array([math.floor((submit - start) / interval) for submit in submits])


def round_end(rounds, start, interval):
    """
    This computes when a round ends in absolute time.
    """
    return interval * (rounds + 1) + start


def messages_per_round(rounds, unique_rounds):
    """
    Computes the number of messages per round over a list of round names.

    Note: use precomputed unique rounds rather than computing in function
    because sometimes we can reuse unique_rounds, rather than having to
    recompute with each function call.
    """
    min_round = min(rounds)
    max_round = max(rounds)
    messages = np.zeros(max_round - min_round + 1)

    count = collections.Counter(rounds)
    for round in unique_rounds:
        messages[round - min_round] = count[round]

    return messages
