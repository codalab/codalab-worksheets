"""
Script to run both frontend and backend tests for CodaLab. Depending on the test modules, this script
will also create a second instance of CodaLab to test against.
"""

from tests.cli.test_cli import TestModule  # type: ignore

import argparse
import random
import socket
import string
import subprocess
import sys
import time


class TestRunner(object):
    _CODALAB_SERVICE_SCRIPT = 'codalab_service.py'
    _TEMP_INSTANCE_NEEDED_TESTS = ['all', 'default', 'copy']
    _FRONTEND_MODULE = 'frontend'

    @staticmethod
    def _docker_exec(command):
        return 'docker exec codalab_rest-server_1 /bin/bash -c "{}"'.format(command)

    @staticmethod
    def _create_temp_instance(name, version):
        def get_free_ports(num_ports):
            socks = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(num_ports)]
            for s in socks:
                # When binding a socket to port 0, the kernel will assign it a free port
                s.bind(('', 0))
            ports = [str(s.getsockname()[1]) for s in socks]
            for s in socks:
                # Queue up to 10 requests
                s.listen(10)
            return ports

        rest_port, http_port = get_free_ports(2)
        instance = 'http://rest-server:%s' % rest_port
        print('Creating another CodaLab instance {} at {} for testing...'.format(name, instance))

        try:
            start_time = time.time()
            subprocess.check_call(
                ' '.join(
                    [
                        'python3',
                        TestRunner._CODALAB_SERVICE_SCRIPT,
                        'start',
                        '--instance-name %s' % name,
                        '--rest-port %s' % rest_port,
                        '--version %s' % version,
                        '--services init ws-server rest-server',
                    ]
                ),
                shell=True,
            )
            print(
                'It took {} seconds to create the temp instance.'.format(time.time() - start_time)
            )
        except subprocess.CalledProcessError as ex:
            print('There was an error while creating the temp instance: %s' % ex.output)
            raise

        return instance

    def __init__(self, args):
        self.instance = args.instance
        self.tests = args.tests

        # Check if a second, temporary instance of CodaLab is needed for testing
        self.temp_instance_required = any(
            test in self.tests for test in TestRunner._TEMP_INSTANCE_NEEDED_TESTS
        )
        if self.temp_instance_required:
            self.temp_instance_name = 'temp-instance%s' % ''.join(
                random.choice(string.digits) for _ in range(8)
            )
            self.temp_instance = TestRunner._create_temp_instance(
                self.temp_instance_name, args.version
            )

    def run(self):
        success = True
        try:

            non_frontend_tests = list(
                filter(lambda test: test != TestRunner._FRONTEND_MODULE, self.tests)
            )

            if len(non_frontend_tests):
                # Run backend tests using test_cli
                test_command = 'python3 tests/cli/test_cli.py --instance %s ' % self.instance
                if self.temp_instance_required:
                    test_command += '--second-instance %s ' % self.temp_instance

                test_command += ' '.join(non_frontend_tests)
                print('Running backend tests with command: %s' % test_command)
                subprocess.check_call(TestRunner._docker_exec(test_command), shell=True)

            # Run frontend tests
            self._run_frontend_tests()

        except subprocess.CalledProcessError as ex:
            print('Exception while executing tests: %s' % ex.output)
            success = False

        self._cleanup()
        return success

    def _run_frontend_tests(self):
        if TestRunner._FRONTEND_MODULE not in self.tests:
            return

        # Execute front end tests here
        print('Running frontend tests...')
        # Run Selenium UI tests
        subprocess.check_call('python3 tests/ui/ui_tester.py --headless', shell=True)

    def _cleanup(self):
        if not self.temp_instance_required:
            return

        print('Shutting down the temp instance {}...'.format(self.temp_instance_name))
        subprocess.check_call(
            ' '.join(
                [
                    'python3',
                    TestRunner._CODALAB_SERVICE_SCRIPT,
                    'stop',
                    '--instance-name %s' % self.temp_instance_name,
                ]
            ),
            shell=True,
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs the specified tests against the specified CodaLab instance (defaults to localhost)'
    )
    parser.add_argument(
        '--version',
        type=str,
        help='CodaLab version to use for multi-instance tests, defaults to "latest"',
        default='latest',
    )
    parser.add_argument(
        '--instance',
        type=str,
        help='CodaLab instance to run tests against, defaults to "http://rest-server:2900"',
        default='http://rest-server:2900',
    )
    parser.add_argument(
        'tests',
        metavar='TEST',
        nargs='+',
        type=str,
        choices=list(TestModule.modules.keys()) + ['all', 'default', 'frontend'],
        help='Tests to run from: {%(choices)s}',
    )

    args = parser.parse_args()
    test_runner = TestRunner(args)
    if not test_runner.run():
        sys.exit(1)
