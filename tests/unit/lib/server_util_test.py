import unittest

from codalab.lib import server_util

import time


class ServerUtilTest(unittest.TestCase):
    def test_rate_limit_not_exceeded(self):
        sentinel = 23948

        @server_util.rate_limited(3600)
        def limited_function(arg):
            return arg

        for _ in range(3600):
            self.assertEqual(limited_function(sentinel), sentinel)

        # After one second, should recover credit for at least another call
        time.sleep(1)
        self.assertEqual(limited_function(sentinel), sentinel)

    def test_rate_limit_exceeded(self):
        @server_util.rate_limited(10)
        def limited_function():
            pass

        self.assertRaises(
            server_util.RateLimitExceededError, lambda: [limited_function() for _ in range(11)]
        )

    def test_exc_frame_locals(self):
        def baz():
            a = 1
            b = 2
            assert a == 1
            assert b == 2
            raise NotImplementedError

        def bar():
            c = 3
            d = 4
            assert c == 3
            assert d == 4
            baz()

        def foo():
            e = 5
            f = 6
            assert e == 5
            assert f == 6
            bar()

        try:
            baz()
        except NotImplementedError:
            self.assertEqual(server_util.exc_frame_locals(), {'a': 1, 'b': 2})

        try:
            foo()
        except NotImplementedError:
            self.assertEqual(server_util.exc_frame_locals(), {'a': 1, 'b': 2})
