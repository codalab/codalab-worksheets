import unittest
from codalab.lib.bundle_cli import BundleCLI


class BundleCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle_cli = BundleCLI

    def tearDown(self) -> None:
        del self.bundle_cli

    def test_collapse_bare_command_empty_args(self):
        argv = ['cl', 'run', '---', 'echo', '']
        expected_result = ['cl', 'run', "echo ''"]
        actual_result = self.bundle_cli.collapse_bare_command(argv)
        self.assertEqual(actual_result, expected_result)

    def test_collapse_bare_command_non_empty_str_args(self):
        argv = ['cl', 'run', '---', 'echo', 'hello']
        expected_result = ['cl', 'run', "echo hello"]
        actual_result = self.bundle_cli.collapse_bare_command(argv)
        self.assertEqual(actual_result, expected_result)

    def test_collapse_bare_command_non_empty_str_args_with_escaped_char(self):
        argv = ['cl', 'run', '---', 'echo', 'hello world!']
        expected_result = ['cl', 'run', "echo 'hello world!'"]
        actual_result = self.bundle_cli.collapse_bare_command(argv)
        self.assertEqual(actual_result, expected_result)
