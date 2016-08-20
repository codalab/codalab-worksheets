import unittest

from codalab.lib.server_util import *

import time


class ServerUtilTest(unittest.TestCase):
    def test_rate_limit_not_exceeded(self):
        @rate_limited(10, 1)
        def limited_function(arg):
            return arg

        for _ in xrange(10):
            self.assertEqual(limited_function("same"), "same")

        time.sleep(1)

        for _ in xrange(10):
            self.assertEqual(limited_function("same"), "same")

    def test_rate_limit_exceeded(self):
        @rate_limited(10, 3600)
        def limited_function():
            pass

        def make_n_calls(n):
            for _ in xrange(n):
                limited_function()

        self.assertRaises(RateLimitExceededError, lambda: make_n_calls(11))
