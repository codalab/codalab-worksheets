import unittest
from codalabworker.fsm import *


class NoWait(object):
    @property
    def update_period(self):
        return 0


class B(NoWait, State):
    def __init__(self, value):
        self.value = value

    def update(self):
        return State.DONE


class A(NoWait, State):
    def __init__(self, value):
        self.value = value

    def update(self):
        return B([self.value, self.value])


class Endless(State):
    def update(self):
        return self


class FiniteStateMachineTest(unittest.TestCase):
    def test_simple(self):
        fsm = FiniteStateMachine(initial_state=A(1))
        fsm.run()
        self.assertIsNone(fsm._state, "should have reached done state")

    def test_custom_done_check(self):
        fsm = FiniteStateMachine(initial_state=A(1), done_check=lambda s: isinstance(s, B))
        fsm.run()
        self.assertIsNotNone(fsm._state, "should not have reached State.DONE due to custom done check")
        self.assertIsInstance(fsm._state, B, "should have stopped at state B")
        self.assertEqual([1, 1], fsm._state.value, "should have expected value")


class ThreadedFiniteStateMachineTest(unittest.TestCase):
    def test_simple(self):
        fsm = ThreadedFiniteStateMachine(A(1))
        fsm.start()
        fsm._thread.join()
        self.assertIsNone(fsm._state, "should have reached done state")

    def test_stop_endless(self):
        fsm = ThreadedFiniteStateMachine(Endless())
        fsm.start()
        self.assertTrue(fsm._thread.isAlive(), "thread should be running")
        fsm.stop()
        fsm._thread.join(1)
        self.assertFalse(fsm._thread.isAlive(), "thread should not be running after stop")

