import numpy as np
import unittest

import src.systems


class TestStats(unittest.TestCase):
    def setUp(self):
        self.df1 = np.array([[0, 0, 2],
                             [0, 0, 3],
                             [2, 1, 6],
                             [1, 0, 2],
                             [1, 3, 5],
                             [0, 1, 6],
                             [2, 1, 4],
                             [2, 3, 10]])

        self.df2 = np.array([[0, 0, 7],
                             [0, 1, 11],
                             [1, 3, 10],
                             [2, 1, 11],
                             [0, 0, 8],
                             [2, 1, 9],
                             [1, 0, 7],
                             [2, 3, 15],
                             [4, 0, 16]])

    def test_direct_delivery(self):
        """
        Tests direct delivery for correctness under
        - skipped rounds
        - queued sender messages
        - different intervals
        - different numbers of users
        - receivers who don't send
        - different start times
        """

        actual_overhead, actual_latency, actual_dropped = 1.5, 1/8, 0
        overhead, latency = src.systems.k_in(
            2, self.df1, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 16/9, 1/9, 0
        overhead, latency = src.systems.k_in(
            2, self.df2, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 1/2, 3/4, 0
        overhead, latency = src.systems.k_in(
            5, self.df1, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 15/9 - 1, 2/3, 0
        overhead, latency = src.systems.k_in(
            5, self.df2, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

    # def test_broadcast(self):
    #     """
    #     Tests broadcast for correctness under
    #     - skipped rounds
    #     - different numbers of users
    #     - receivers who don't send
    #     - senders who don't receive
    #     """

    #     actual_overhead, actual_latency, actual_dropped = 3, 0, 0
    #     overhead, latency = src.systems.broadcast(None, self.df1)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)

    #     actual_overhead, actual_latency, actual_dropped = 4, 0, 0
    #     overhead, latency = src.systems.broadcast(None, self.df2)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)

    # def test_kbroadcast_dropped(self):
    #     """
    #     Tests kbroadcast_dropped for correctness under
    #     - skipped rounds
    #     - stack messages on the receiver side
    #     - different intervals
    #     - different numbers of users
    #     - receivers who don't send
    #     - senders who don't receive
    #     - different start times
    #     """

    #     # df1
    #     actual_overhead, actual_latency, actual_dropped = 11/5, 0, 3/8
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         2, self.df1, 1)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 3, 0, 1/8
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         2, self.df1, 2)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 3, 0, 0
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         2, self.df1, 1000)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 1, 0, 1/2
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         5, self.df1, 1)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 1, 0, 1/4
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         5, self.df1, 2)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 3, 0, 0
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         5, self.df1, 1000)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     # df2
    #     actual_overhead, actual_latency, actual_dropped = 14/6, 0, 1/3
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         2, self.df2, 1)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 4, 0, 1/9
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         2, self.df2, 2)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 4, 0, 0
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         2, self.df2, 1000)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 5/5, 0, 4/9
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         5, self.df2, 1)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 13/7, 0, 2/9
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         5, self.df2, 2)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    #     actual_overhead, actual_latency, actual_dropped = 4, 0, 0
    #     overhead, latency, dropped = src.systems.kbroadcast_dropped(
    #         5, self.df2, 1000)
    #     self.assertEqual(actual_overhead, overhead)
    #     self.assertEqual(actual_latency, latency)
    #     self.assertEqual(actual_dropped, dropped)

    def test_kbroadcast_deferred(self):
        """
        Tests kbroadcast_deferred for correctness under
        - skipped rounds
        - stack messages on the receiver side
        - different intervals
        - different numbers of users
        - receivers who don't send
        - senders who don't receive
        - different start times
        """

        # df1
        actual_overhead, actual_latency, actual_dropped = 12/8, 1/2, 0
        overhead, latency = src.systems.k_out(
            2, self.df1, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 32/8, 1/8, 0
        overhead, latency = src.systems.k_out(
            2, self.df1, 2, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 4/8, 6/8, 0
        overhead, latency = src.systems.k_out(
            5, self.df1, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 8/8, 2/8, 0
        overhead, latency = src.systems.k_out(
            5, self.df1, 2, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        # df2
        actual_overhead, actual_latency, actual_dropped = 16/9, 4/9, 0
        overhead, latency = src.systems.k_out(
            2, self.df2, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 41/9, 1/9, 0
        overhead, latency = src.systems.k_out(
            2, self.df2, 2, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 11/9, 8/9, 0
        overhead, latency = src.systems.k_out(
            5, self.df2, 1, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)

        actual_overhead, actual_latency, actual_dropped = 11/9, 2/9, 0
        overhead, latency = src.systems.k_out(
            5, self.df2, 2, False)
        self.assertEqual(actual_overhead, overhead)
        self.assertEqual(actual_latency, latency)
