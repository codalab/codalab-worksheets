#! /usr/bin/python2.7

import argparse
import errno
import os
import subprocess
import test_cli


def main():
    args = CodalabArgs.get_args()
    service_manager = CodalabServiceManager(args)
    service_manager.execute()


class CodalabArgs(argparse.Namespace):
    DEFAULT_ARGS = {
        'version': 'latest',
        'dev': False,
        'push': False,
        'docker_user': None,
        'docker_pwd': None,
        'build_locally': False,
        'test_build': False,
        'user_compose_file': None,
        'start_worker': False,
        'initial_config': False,
        'root_dir': None,
        'mysql_root_pwd': 'mysql_root_pwd',
        'mysql_user': 'codalab',
        'mysql_pwd': 'mysql_pwd',
        'root_user': 'codalab',
        'root_pwd': 'testpassword',
        'uid': None,
        'service_home': None,
        'mysql_mount': None,
        'worker_dir': None,
        'bundle_stores': [],
        'worker_docker_network_name': 'codalab_worker_network',
        'http_port': '80',
        'rest_port': None,
        'frontend_port': None,
        'mysql_port': None,
        'use_ssl': False,
        'ssl_cert_file': None,
        'ssl_key_file': None,
        'follow': False,
        'tail': None,
        'tests': ['default'],
    }

    ARG_TO_ENV_VAR = {
        'version': 'CODALAB_VERSION',
        'dev': 'CODALAB_DEV',
        'push': 'CODALAB_PUSH',
        'docker_user': 'CODALAB_DOCKER_USER',
        'docker_pwd': 'CODALAB_DOCKER_PWD',
        'user_compose_file': 'CODALAB_USER_COMPOSE_FILE',
        'start_worker': 'CODALAB_START_WORKER',
        'initial_config': 'CODALAB_INITIAL_CONFIG',
        'mysql_root_pwd': 'CODALAB_MYSQL_ROOT_PWD',
        'mysql_user': 'CODALAB_MYSQL_USER',
        'mysql_pwd': 'CODALAB_MYSQL_PWD',
        'root_user': 'CODALAB_ROOT_USER',
        'root_pwd': 'CODALAB_ROOT_PWD',
        'uid': 'CODALAB_UID',
        'service_home': 'CODALAB_SERVICE_HOME',
        'mysql_mount': 'CODALAB_MYSQL_MOUNT',
        'worker_dir': 'CODALAB_WORKER_DIR',
        'worker_docker_network_name': 'CODALAB_WORKER_DOCKER_NETWORK_NAME',
        'http_port': 'CODALAB_HTTP_PORT',
        'rest_port': 'CODALAB_REST_PORT',
        'frontend_port': 'CODALAB_FRONTEND_PORT',
        'mysql_port': 'CODALAB_MYSQL_PORT',
        'ssl_cert_file': 'CODALAB_SSL_CERT_FILE',
        'ssl_key_file': 'CODALAB_SSL_KEY_FILE',
    }

    @staticmethod
    def _get_parser():
        parser = argparse.ArgumentParser(description="Manages your local CodaLab worksheets back-end deployment using docker-compose")
        subparsers = parser.add_subparsers(dest='command', description='Command to run')

        # SUBCOMMANDS

        start_cmd = subparsers.add_parser('start', help='Start a CodaLab backend instance')
        logs_cmd = subparsers.add_parser('logs', help='View logs for existing CodaLab instance')
        test_cmd = subparsers.add_parser('test', help='Run tests against an existing CodaLab instance')
        build_cmd = subparsers.add_parser('build', help='Build CodaLab docker images using the local codebase')

        subparsers.add_parser('stop', help='Stop any existing CodaLab backend instance')
        subparsers.add_parser('down', help='Bring down any existing CodaLab backend instance')
        subparsers.add_parser('restart', help='Restart any existing CodaLab backend instance')

        #  BUILD SETTINGS

        for cmd in [build_cmd, start_cmd]:
            cmd.add_argument('--version', '-v', type=str, help='CodaLab version to use for building and deployment', default=argparse.SUPPRESS)
            cmd.add_argument('--dev', action='store_true', help='If specified use dev versions of images', default=argparse.SUPPRESS)
            cmd.add_argument('--push', action='store_true', help='If specified push the images to Dockerhub', default=argparse.SUPPRESS)
            cmd.add_argument('--docker-user', type=str, help='DockerHub username to push images from', default=argparse.SUPPRESS)
            cmd.add_argument('--docker-pwd', type=str, help='DockerHub password to push images from', default=argparse.SUPPRESS)

        #  DEPLOYMENT SETTINGS

        start_cmd.add_argument('--build-locally', '-b', action='store_true', help='If specified build VERSION using local code.', default=argparse.SUPPRESS)
        start_cmd.add_argument('--test-build', '-t', action='store_true', help='If specified run tests on the build.', default=argparse.SUPPRESS)
        start_cmd.add_argument('--user-compose-file', type=str, help='If specified path to a user-defined Docker compose file that overwrites the defaults', default=argparse.SUPPRESS)
        start_cmd.add_argument('--start-worker', '-w', action='store_true', help='If specified start a CodaLab worker on this machine.', default=argparse.SUPPRESS)
        start_cmd.add_argument('--initial-config', '-i', action='store_true', help='If specified, save the initial configuration of the instance (defaults to true if the service home or the database mounts are ephemeral)', default=argparse.SUPPRESS)

        #  USER CREDENTIALS

        start_cmd.add_argument('--mysql-root-pwd', type=str, help='Root password for the database', default=argparse.SUPPRESS)
        start_cmd.add_argument('--mysql-user', type=str, help='MYSQL username for the Codalab MYSQL client', default=argparse.SUPPRESS)
        start_cmd.add_argument('--mysql-pwd', type=str, help='MYSQL password for the Codalab MYSQL client', default=argparse.SUPPRESS)
        start_cmd.add_argument('--root-user', type=str, help='Codalab username for the Codalab admin user', default=argparse.SUPPRESS)
        start_cmd.add_argument('--root-pwd', type=str, help='Codalab password for the Codalab admin user', default=argparse.SUPPRESS)

        #  HOST FILESYSTEM MOUNTS

        start_cmd.add_argument('--uid', type=str, help='Linux UID that owns the files created by Codalab. default=(ID of the user running this script)', default=argparse.SUPPRESS)
        start_cmd.add_argument('--service-home', type=str, help='Path on the host machine to store home directory of the Codalab server (by default nothing is stored', default=argparse.SUPPRESS)
        start_cmd.add_argument('--mysql-mount', type=str, help='Path on the host machine to store mysql data files, by default the database is ephemeral', default=argparse.SUPPRESS)
        start_cmd.add_argument('--worker-dir', type=str, help='Path on the host machine to store worker data files, by default these are ephemeral', default=argparse.SUPPRESS)
        start_cmd.add_argument('--bundle-store', type=str, help='Path on the host machine to store bundle data files, by default these are ephemeral', default=[], dest='bundle_stores', action='append')

        start_cmd.add_argument('--worker-docker-network-name', type=str, help='Name of the docker network that includes the worker and runs', default=argparse.SUPPRESS)

        #  HOST PORT MOUNTS

        start_cmd.add_argument('--http-port', type=str, help='HTTP port for the server to listen on', default=argparse.SUPPRESS)
        start_cmd.add_argument('--rest-port', type=str, help='Port for the REST server to listen on (by default it is not exposed to the host machine)', default=argparse.SUPPRESS)
        start_cmd.add_argument('--frontend-port', type=str, help='Port for the React server to listen on (by default it is not exposed to the host machine)', default=argparse.SUPPRESS)
        start_cmd.add_argument('--mysql-port', type=str, help='Port for the MYSQL database to listen on (by default it is not exposed to the host machine)', default=argparse.SUPPRESS)

        #  SSL CONFIGURATION

        start_cmd.add_argument('--use-ssl', action='store_true', help='If specified set the server up with SSL', default=argparse.SUPPRESS)
        start_cmd.add_argument('--ssl-cert-file', type=str, help='Path to the cert file for SSL', default=argparse.SUPPRESS)
        start_cmd.add_argument('--ssl-key-file', type=str, help='Path to the key file for SSL', default=argparse.SUPPRESS)

        #  LOGS SETTINGS

        logs_cmd.add_argument('services', nargs='*', default='all', help='Services to print logs for', choices=['mysql', 'rest-server', 'bundle-manager', 'frontend', 'nginx', 'worker', 'all'])
        logs_cmd.add_argument('--follow', '-f', action='store_true', help='If specified follow the logs', default=argparse.SUPPRESS)
        logs_cmd.add_argument('--tail', '-t', type=int, help='If specified tail TAIL lines from the ends of each log', default=argparse.SUPPRESS)

        #  TESTS SETTINGS

        test_cmd.add_argument('tests', metavar='TEST', nargs='+', type=str, choices=test_cli.TestModule.modules.keys() + ['all', 'default'], default=['default'], help='Tests to run')
        return parser

    @classmethod
    def get_args(cls):
        parser = cls._get_parser()
        args = cls()
        args.apply_environment(os.environ)
        parser.parse_args(namespace=args)
        return args

    def __init__(self):
        for arg in self.DEFAULT_ARGS.keys():
            setattr(self, arg, None)
        self.root_dir = os.path.dirname(os.path.realpath(__file__))

    def _apply_defaults(self):
        for arg, default in self.DEFAULT_ARGS.items():
            if getattr(self, arg) is None:
                setattr(self, arg, default)

        if self.worker_dir is None:
            self.worker_dir = os.path.join(self.root_dir, 'codalab-worker-scratch')

    def apply_environment(self, env):
        for arg, var in self.ARG_TO_ENV_VAR.items():
            if var in env:
                setattr(self, arg, env[var])
        self._apply_defaults()


class CodalabServiceManager(object):

    @staticmethod
    def resolve_env_vars(args):
        environment = {
            'CODALAB_MYSQL_ROOT_PWD': args.mysql_root_pwd,
            'CODALAB_MYSQL_USER': args.mysql_user,
            'CODALAB_MYSQL_PWD': args.mysql_pwd,
            'CODALAB_ROOT_USER': args.root_user,
            'CODALAB_ROOT_PWD': args.root_pwd,
            'CODALAB_HTTP_PORT': args.http_port,
            'CODALAB_VERSION': args.version,
            'CODALAB_WORKER_NETWORK_NAME': args.worker_docker_network_name,
        }
        if args.uid:
            environment['CODALAB_UID'] = args.uid
        else:
            environment['CODALAB_UID'] = '%s:%s' % (os.getuid(), os.getgid())
        if args.service_home:
            environment['CODALAB_SERVICE_HOME'] = args.service_home
        else:
            environment['CODALAB_SERVICE_HOME'] = '/home/codalab'
        if args.mysql_mount:
            environment['CODALAB_MYSQL_MOUNT'] = args.mysql_mount
        if args.start_worker:
            environment['CODALAB_WORKER_DIR'] = args.worker_dir
        if args.rest_port:
            environment['CODALAB_REST_PORT'] = args.rest_port
        if args.frontend_port:
            environment['CODALAB_FRONTEND_PORT'] = args.frontend_port
        if args.mysql_port:
            environment['CODALAB_MYSQL_PORT'] = args.mysql_port
        if args.use_ssl:
            environment['CODALAB_SSL_CERT_FILE'] = args.ssl_cert_file
            environment['CODALAB_SSL_KEY_FILE'] = args.ssl_key_file
        return environment

    @staticmethod
    def resolve_compose_files(args):
        compose_files = ['docker-compose.yml']
        if args.dev:
            compose_files.append('docker-compose.dev.yml')
        if args.service_home:
            compose_files.append('docker-compose.home_mount.yml')
        else:
            compose_files.append('docker-compose.no_home_mount.yml')
        if args.mysql_mount:
            compose_files.append('docker-compose.mysql_mount.yml')
        if args.bundle_stores:
            compose_files.append('docker-compose.bundle_mounts.yml')
        if args.start_worker:
            compose_files.append('docker-compose.worker.yml')
        if args.rest_port:
            compose_files.append('docker-compose.rest_port.yml')
        if args.frontend_port:
            compose_files.append('docker-compose.frontend_port.yml')
        if args.mysql_port:
            compose_files.append('docker-compose.mysql_port.yml')
        if args.use_ssl:
            compose_files.append('docker-compose.ssl.yml')
        if args.user_compose_file:
            compose_files.append(args.user_compose_file)
        return compose_files

    def __init__(self, args):
        self.args = args
        self.root_dir = args.root_dir
        self.command = args.command
        self.compose_cwd = os.path.join(self.root_dir, 'docker', 'compose_files')
        self.compose_files = self.resolve_compose_files(args)
        self.compose_env = self.resolve_env_vars(args)
        if self.args.service_home:
            try:
                os.makedirs(self.args.service_home)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        if self.args.start_worker:
            try:
                os.makedirs(self.args.worker_dir)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        if self.args.mysql_mount:
            try:
                os.makedirs(self.args.mysql_mount)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        for bundle_store in self.args.bundle_stores:
            try:
                os.makedirs(self.args.bundle_store)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    def execute(self):
        if self.command == 'build' or (self.command == 'start' and self.args.build_locally):
            self.build()
        if self.command == 'start':
            self.start_service()
            if self.args.test_build:
                self.test()
        elif self.command == 'logs':
            self._run_compose_cmd('logs')
        elif self.command == 'stop':
            self._run_compose_cmd('stop')
        elif self.command == 'down':
            self._run_compose_cmd('down --remove-orphans')
        elif self.command == 'test':
            self.test()

    def _run_docker_cmd(self, cmd):
        subprocess.check_call('docker ' + cmd, shell=True, cwd=self.root_dir)

    def build_image(self, image, dockerfile):
        print("[CODALAB] ==> Building %s image " % image)
        self._run_docker_cmd('build -t codalab/%s:%s -f docker/dockerfiles/Dockerfile.%s .' % (image, self.args.version, dockerfile))

    def push_image(self, image):
        self._run_docker_cmd('push codalab/%s:%s' % (image, self.args.version))

    def _run_compose_cmd(self, cmd):
        files_string = ' -f '.join(self.compose_files)
        command_string = 'docker-compose -f %s %s' % (files_string, cmd)
        subprocess.check_call(command_string, cwd=self.compose_cwd, env=self.compose_env, shell=True)

    def bring_up_service(self, service):
        self._run_compose_cmd('up -d --no-deps --no-recreate %s' % service)

    def run_service_cmd(self, cmd, root=False, service='rest-server'):
        if root:
            uid = '0:0'
        else:
            uid = self.compose_env['CODALAB_UID']
        self._run_compose_cmd('run --no-deps --rm --entrypoint="" --user=%s ' % uid + service + ' bash -c "%s"' % cmd)

    def start_service(self):
        print("[CODALAB] ==> Starting MySQL")
        self.bring_up_service('mysql')

        print("[CODALAB] ==> Configuring the service")
        self.run_service_cmd("data/bin/wait-for-it.sh mysql:3306 -- /opt/codalab-worksheets/codalab/bin/cl config server/engine_url mysql://%s:%s@mysql:3306/codalab_bundles && /opt/codalab-worksheets/codalab/bin/cl config cli/default_address http://rest-server:2900 && /opt/codalab-worksheets/codalab/bin/cl config server/rest_host 0.0.0.0" % (self.compose_env['CODALAB_MYSQL_USER'], self.compose_env['CODALAB_MYSQL_PWD']), root=(not self.args.service_home))

        if self.args.initial_config:
            print("[CODALAB] ==> Creating root user")
            self.run_service_cmd("/opt/codalab-worksheets/venv/bin/pip install /opt/codalab-worksheets && data/bin/wait-for-it.sh mysql:3306 -- opt/codalab-worksheets/venv/bin/python /opt/codalab-worksheets/scripts/create-root-user.py %s" % self.compose_env['CODALAB_ROOT_PWD'], root=True)

        print("[CODALAB] ==> Starting rest server")
        self.bring_up_service('rest-server')

        if self.args.initial_config:
            print("[CODALAB] ==> Creating initial worksheets")
            self.run_service_cmd("data/bin/wait-for-it.sh rest-server:2900 -- opt/codalab-worksheets/codalab/bin/cl logout && /opt/codalab-worksheets/codalab/bin/cl new home && /opt/codalab-worksheets/codalab/bin/cl new dashboard", root=(not self.args.service_home))

        print("[CODALAB] ==> Starting bundle manager")
        self.bring_up_service('bundle-manager')
        print("[CODALAB] ==> Starting frontend")
        self.bring_up_service('frontend')
        print("[CODALAB] ==> Starting nginx")
        self.bring_up_service('nginx')
        if self.args.start_worker:
            print("[CODALAB] ==> Starting worker")
            self.bring_up_service('worker')

    def build(self):
        print("[CODALAB] => Building Docker images")
        self.build_image('bundleserver', 'server')
        self.build_image('frontend', 'frontend')
        self.build_image('worker', 'worker')
        self.build_image('default-cpu', 'cpu')
        self.build_image('default-gpu', 'gpu')
        if self.args.push:
            self._run_docker_cmd('login -u %s -p %s' % (self.args.docker_user, self.args.docker_pwd))
            self.push_image('bundleserver')
            self.push_image('frontend')
            self.push_image('worker')
            self.push_image('default-cpu')
            self.push_image('default-gpu')

    def test(self):
        test_cli.cl = '/u/nlp/bin/cl'
        test_cli.TestModule.run(self.args.tests, 'localhost')


if __name__ == '__main__':
    main()
