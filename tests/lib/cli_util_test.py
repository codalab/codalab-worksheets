import unittest

from codalab.lib import cli_util
from codalab.common import UsageError

class CLIUtilTest(unittest.TestCase):
    def test_parse_key_target(self):
        cases = [
                ('a:b', ('a', 'b')),
                (':b', ('', 'b')),
                ('b', (None, 'b')),
                ('dash-key:https://worksheets.codalab.org::some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    ('dash-key', 'https://worksheets.codalab.org::some-worksheet//some-bundle-2.dirname/this/is/a/path.txt')),
                (':https://worksheets.codalab.org::some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    ('', 'https://worksheets.codalab.org::some-worksheet//some-bundle-2.dirname/this/is/a/path.txt')),
                ('prod::some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    (None, 'prod::some-worksheet//some-bundle-2.dirname/this/is/a/path.txt')),
                ('dash-key:some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    ('dash-key', 'some-worksheet//some-bundle-2.dirname/this/is/a/path.txt')),
                (':some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    ('', 'some-worksheet//some-bundle-2.dirname/this/is/a/path.txt')),
                ('some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    (None, 'some-worksheet//some-bundle-2.dirname/this/is/a/path.txt')),
                ('dash-key:some-bundle-2.dirname/this/is/a/path.txt',
                    ('dash-key', 'some-bundle-2.dirname/this/is/a/path.txt')),
                (':some-bundle-2.dirname/this/is/a/path.txt',
                    ('', 'some-bundle-2.dirname/this/is/a/path.txt')),
                ('some-bundle-2.dirname/this/is/a/path.txt',
                    (None, 'some-bundle-2.dirname/this/is/a/path.txt')),
                ('dash-key:some-bundle-2.dirname',
                    ('dash-key', 'some-bundle-2.dirname')),
                (':some-bundle-2.dirname',
                    ('', 'some-bundle-2.dirname')),
                ('some-bundle-2.dirname',
                    (None, 'some-bundle-2.dirname'))
                ]
        for spec, expected_parse in cases:
            self.assertEqual(cli_util.parse_key_target(spec), expected_parse)

    def test_parse_target_spec(self):
        cases = [
                ('https://worksheets.codalab.org::some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    ('https://worksheets.codalab.org', 'some-worksheet', 'some-bundle-2.dirname', 'this/is/a/path.txt')),
                ('some-worksheet//some-bundle-2.dirname/this/is/a/path.txt',
                    (None, 'some-worksheet', 'some-bundle-2.dirname', 'this/is/a/path.txt')),
                ('some-bundle-2.dirname/this/is/a/path.txt',
                    (None, None, 'some-bundle-2.dirname', 'this/is/a/path.txt')),
                ('some-bundle-2.dirname',
                    (None, None, 'some-bundle-2.dirname', None)),
                ('prod::bundle',
                    ('prod', None, 'bundle', None)),
                ('worksheet//bundle',
                    (None, 'worksheet', 'bundle', None)),
                ('bundle/path',
                    (None, None, 'bundle', 'path'))
                ]
        for spec, expected_parse in cases:
            self.assertEqual(cli_util.parse_target_spec(spec), expected_parse)

    def test_desugar(self):
        self.assertEqual(cli_util.desugar_command([], 'echo hello'), ([], 'echo hello'))
        self.assertEqual(cli_util.desugar_command([':a-bundle'], 'run a-bundle'), (["a-bundle:a-bundle"], 'run a-bundle'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %b:c%'), (['a:b', 'b:c'], 'echo b'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %c%'), (['a:b', 'b2:c'], 'echo b2'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %:c%'), (['a:b', 'c:c'], 'echo c'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %a:b% %a:b%'), (['a:b'], 'echo a a'))
        self.assertEqual(cli_util.desugar_command([], 'echo %a% %a% %a%'), (['b1:a'], 'echo b1 b1 b1'))
        self.assertRaises(UsageError, lambda : cli_util.desugar_command([], 'echo %a:b% %a:c%'))
        self.assertRaises(UsageError, lambda : cli_util.desugar_command([':b'], 'echo %b:c%'))
