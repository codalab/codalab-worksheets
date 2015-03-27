import unittest

from codalab.lib import cli_util
from codalab.common import UsageError

class CLIUtilTest(unittest.TestCase):
    def test_desugar(self):
        self.assertEqual(cli_util.desugar_command([], 'echo hello'), ([], 'echo hello'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %b:c%'), (['a:b', 'b:c'], 'echo b'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %c%'), (['a:b', 'b2:c'], 'echo b2'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %:c%'), (['a:b', ':c'], 'echo c'))
        self.assertEqual(cli_util.desugar_command(['a:b'], 'echo %a:b% %a:b%'), (['a:b'], 'echo a a'))
        self.assertEqual(cli_util.desugar_command([], 'echo %a% %a% %a%'), (['b1:a'], 'echo b1 b1 b1'))
        self.assertRaises(UsageError, lambda : cli_util.desugar_command([], 'echo %a:b% %a:c%'))
        self.assertRaises(UsageError, lambda : cli_util.desugar_command([':b'], 'echo %b:c%'))
