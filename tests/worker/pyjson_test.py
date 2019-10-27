import unittest
from collections import namedtuple

from codalab.worker import pyjson


class PyJSONTest(unittest.TestCase):
    def test_one(self):
        t1 = namedtuple("t1", "w1 w2 w3")
        t2 = namedtuple("t2", "r1 t2 y3")

        cases = {
            'a_string': 'blah',
            'a_set': set([1, 3, 4]),
            'a_list': [1, 3, 4],
            'list_of_sets': [set(), set([0])],
            'nested_dict': {
                'dict1': {'a_string': 'blah', 'a_set': set([1, 3, 4])},
                'dict2': {'a_int': 7, 'a_list': [1, 2, 3], 'a_t1': t1(10, 20, 30)},
            },
            'a_namedtuple': t2(3, 4, 6),
            'set_of_namedtuples': {t1(1, 2, 3), t1(2, 4, 6), t2(20, 40, 60)},
            ('key', 'tuple'): 'blah',
            ('unicode', 'key'): 'unicode_val',
            'unicode': 'val',
            '(\'key\', \'tuple\')': 'malicious_key',
            t1('w1', 'w2', 'w3'): t2('r1', 't2', 'y3'),
            t1('', '', ''): t2('', '', ''),
        }

        reloaded = pyjson.loads(pyjson.dumps(cases))
        self.assertEqual(reloaded, cases)

    def test_tuples(self):
        tuple_dict = {('0x123799xasd', ''): {'dict': 'vals'}}

        reloaded = pyjson.loads(pyjson.dumps(tuple_dict))
        self.assertEqual(reloaded, tuple_dict)
