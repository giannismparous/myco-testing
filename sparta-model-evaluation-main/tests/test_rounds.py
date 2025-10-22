import numpy as np
import unittest

import src.rounds
import src.utils


class TestRounds(unittest.TestCase):
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

    def test_round_deferred_sender(self):
        """
        Tests round_deferred_sender.
        """
        df1 = self.df1[src.utils.sort_sender(self.df1)]
        df1_senders = src.utils.senders(df1)
        df1_submits = src.utils.submits(df1)

        df2 = self.df2[src.utils.sort_sender(self.df2)]
        df2_senders = src.utils.senders(df2)
        df2_submits = src.utils.submits(df2)

        # df1, 2
        actual_rounds = np.array([0, 1, 2, 0, 1, 1, 2, 4])
        k = np.full(fill_value=1, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_senders, df1_submits, 2, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 2, 0, 1, 1, 2, 4])
        k = np.full(fill_value=2, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_senders, df1_submits, 2, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 2, 0, 1, 1, 2, 4])
        k = np.full(fill_value=3, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_senders, df1_submits, 2, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        # df1, 5
        actual_rounds = np.array([0, 1, 2, 0, 1, 0, 1, 2])
        k = np.full(fill_value=1, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_senders, df1_submits, 2, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 1, 0, 0, 0, 0, 1])
        k = np.full(fill_value=2, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_senders, df1_submits, 2, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 0, 0, 0, 0, 1])
        k = np.full(fill_value=3, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_senders, df1_submits, 2, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        # df2, 2
        actual_rounds = np.array([0, 1, 2, 0, 1, 1, 2, 4, 4])
        k = np.full(fill_value=1, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_senders, df2_submits, 7, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 2, 0, 1, 1, 2, 4, 4])
        k = np.full(fill_value=2, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_senders, df2_submits, 7, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 2, 0, 1, 1, 2, 4, 4])
        k = np.full(fill_value=3, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_senders, df2_submits, 7, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        # df2, 5
        actual_rounds = np.array([0, 1, 2, 0, 1, 0, 1, 2, 1])
        k = np.full(fill_value=1, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_senders, df2_submits, 7, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 1, 0, 0, 0, 0, 1, 1])
        k = np.full(fill_value=2, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_senders, df2_submits, 7, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 0, 0, 0, 0, 1, 1])
        k = np.full(fill_value=3, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_senders, df2_submits, 7, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

    def test_round_deferred_receiver(self):
        df1 = self.df1[src.utils.sort_receiver(self.df1)]
        df1_receivers = src.utils.receivers(df1)
        df1_submits = src.utils.submits(df1)

        df2 = self.df2[src.utils.sort_receiver(self.df2)]
        df2_receivers = src.utils.receivers(df2)
        df2_submits = src.utils.submits(df2)

        # df1, 2
        actual_rounds = np.array([0, 1, 2, 1, 2, 3, 1, 4])
        k = np.full(fill_value=1, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_receivers, df1_submits, 2, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 1, 1, 2, 2, 1, 4])
        k = np.full(fill_value=2, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_receivers, df1_submits, 2, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 1, 2, 2, 1, 4])
        k = np.full(fill_value=3, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_receivers, df1_submits, 2, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        # df1, 5
        actual_rounds = np.array([0, 1, 2, 0, 1, 2, 0, 1])
        k = np.full(fill_value=1, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_receivers, df1_submits, 2, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 1, 0, 0, 1, 0, 1])
        k = np.full(fill_value=2, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_receivers, df1_submits, 2, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 0, 0, 0, 0, 1])
        k = np.full(fill_value=3, shape=src.utils.num_users(df1))
        computed_rounds = src.rounds.round_deferred(
            df1_receivers, df1_submits, 2, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        # df2, 2
        actual_rounds = np.array([0, 1, 2, 4, 1, 2, 3, 1, 4])
        k = np.full(fill_value=1, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_receivers, df2_submits, 7, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 1, 4, 1, 2, 2, 1, 4])
        k = np.full(fill_value=2, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_receivers, df2_submits, 7, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 4, 1, 2, 2, 1, 4])
        k = np.full(fill_value=3, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_receivers, df2_submits, 7, 2, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        # df2, 5
        actual_rounds = np.array([0, 1, 2, 3, 0, 1, 2, 0, 1])
        k = np.full(fill_value=1, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_receivers, df2_submits, 7, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 1, 1, 0, 0, 1, 0, 1])
        k = np.full(fill_value=2, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_receivers, df2_submits, 7, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 1, 0, 0, 0, 0, 1])
        k = np.full(fill_value=3, shape=src.utils.num_users(df2))
        computed_rounds = src.rounds.round_deferred(
            df2_receivers, df2_submits, 7, 5, k)
        np.testing.assert_equal(actual_rounds, computed_rounds)

    def test_round_submitted(self):
        """
        Tests rounds_submitted.
        """

        actual_rounds = np.array([0, 0, 2, 0, 1, 2, 1, 4])
        computed_rounds = src.rounds.round_submitted(
            self.df1[:, src.utils.SUBMIT_COL], 2, 2)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 2, 1, 2, 0, 1, 0, 4, 4])
        computed_rounds = src.rounds.round_submitted(
            self.df2[:, src.utils.SUBMIT_COL], 7, 2)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 0, 0, 0, 0, 1])
        computed_rounds = src.rounds.round_submitted(
            self.df1[:, src.utils.SUBMIT_COL], 2, 5)
        np.testing.assert_equal(actual_rounds, computed_rounds)

        actual_rounds = np.array([0, 0, 0, 0, 0, 0, 0, 1, 1])
        computed_rounds = src.rounds.round_submitted(
            self.df2[:, src.utils.SUBMIT_COL], 7, 5)
        np.testing.assert_equal(actual_rounds, computed_rounds)

    def test_messages_per_round(self):
        """
        Tests messages_per_round.
        """

        actual_counts = np.array([3, 2, 2, 0, 1])
        rounds = src.rounds.round_submitted(src.utils.submits(self.df1), 2, 2)
        computed_counts = src.rounds.messages_per_round(
            rounds, np.unique(rounds))
        np.testing.assert_equal(computed_counts, actual_counts)

        actual_counts = np.array([3, 2, 2, 0, 2])
        rounds = src.rounds.round_submitted(src.utils.submits(self.df2), 7, 2)
        computed_counts = src.rounds.messages_per_round(
            rounds, np.unique(rounds))
        np.testing.assert_equal(computed_counts, actual_counts)
