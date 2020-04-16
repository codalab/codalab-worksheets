from test_cli import TestModule

import argparse
import random
import string
import subprocess
import sys


class TestRunner(object):
    _CODALAB_SERVICE_SCRIPT = 'codalab_service.py'

    @staticmethod
    def _docker_exec(command):
        return 'docker exec -it codalab_rest-server_1 /bin/bash -c "{}"'.format(command)

    @staticmethod
    def _create_temp_instance(name):
        print('Creating another CodaLab instance for testing...')

        def get_free_ports(num_ports):
            import socket

            socks = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(num_ports)]
            for s in socks:
                s.bind(("", 0))
            ports = [str(s.getsockname()[1]) for s in socks]
            for s in socks:
                s.close()
            return ports

        rest_port, http_port, mysql_port = get_free_ports(3)
        instance = 'http://rest-server:%s' % rest_port
        try:
            subprocess.check_call(
                ' '.join(
                    [
                        'python3',
                        TestRunner._CODALAB_SERVICE_SCRIPT,
                        'start',
                        '--instance-name %s' % name,
                        '--rest-port %s' % rest_port,
                        '--http-port %s' % http_port,
                        '--mysql-port %s' % mysql_port,
                        '--version %s' % version,
                        '--services default',
                    ]
                ),
                shell=True,
            )
        except subprocess.CalledProcessError as ex:
            print('Temp instance exception: %s' % ex.output)
            raise

        return instance

    def __init__(self, instance, tests):
        self.instance = instance
        self.temp_instance_name = 'temp-instance%s' % ''.join(
            random.choice(string.digits) for _ in range(8)
        )
        self.temp_instance = TestRunner._create_temp_instance(self.temp_instance_name)
        self.tests = tests

    def run(self):
        try:
            subprocess.check_call(
                TestRunner._docker_exec(
                    'python3 test_cli.py --instance {} --second-instance {} {}"'.format(
                        self.instance, self.temp_instance, ' '.join(self.tests)
                    )
                ),
                shell=True,
            )
        except subprocess.CalledProcessError as ex:
            print('Exception while executing tests: %s' % ex.output)
            raise

        self._cleanup()

    def _cleanup(self):
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
        '--cl-executable',
        type=str,
        help='Path to CodaLab CLI executable, defaults to "cl"',
        default='cl',
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
        choices=list(TestModule.modules.keys()) + ['all', 'default'],
        help='Tests to run from: {%(choices)s}',
    )

    args = parser.parse_args()
    cl = args.cl_executable
    version = args.version
    test_runner = TestRunner(args.instance, args.tests)
    if not test_runner.run():
        sys.exit(1)
