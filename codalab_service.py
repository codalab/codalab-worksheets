#! /usr/bin/python2.7

import argparse
import errno
import os
import subprocess
import sys
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
        'docker_password': None,
        'build_locally': False,
        'image': 'service',
        'external_db_url': None,
        'test_build': False,
        'user_compose_file': None,
        'start_worker': False,
        'initial_config': False,
        'root_dir': None,
        'instance_name': 'codalab',
        'mysql_root_password': 'codalab',
        'mysql_user': 'codalab',
        'mysql_password': 'codalab',
        'codalab_user': 'codalab',
        'codalab_password': 'codalab',
        'uid': None,
        'codalab_home': None,
        'mysql_mount': None,
        'worker_dir': None,
        'bundle_stores': [],
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
        'external_db_url': 'CODALAB_EXTERNAL_DB_URL',
        'docker_user': 'DOCKER_USER',
        'docker_password': 'DOCKER_PWD',
        'user_compose_file': 'CODALAB_USER_COMPOSE_FILE',
        'start_worker': 'CODALAB_START_WORKER',
        'initial_config': 'CODALAB_INITIAL_CONFIG',
        'mysql_root_password': 'CODALAB_MYSQL_ROOT_PWD',
        'mysql_user': 'CODALAB_MYSQL_USER',
        'mysql_password': 'CODALAB_MYSQL_PWD',
        'codalab_user': 'CODALAB_ROOT_USER',
        'codalab_password': 'CODALAB_ROOT_PWD',
        'uid': 'CODALAB_UID',
        'codalab_home': 'CODALAB_SERVICE_HOME',
        'mysql_mount': 'CODALAB_MYSQL_MOUNT',
        'worker_dir': 'CODALAB_WORKER_DIR',
        'http_port': 'CODALAB_HTTP_PORT',
        'rest_port': 'CODALAB_REST_PORT',
        'frontend_port': 'CODALAB_FRONTEND_PORT',
        'mysql_port': 'CODALAB_MYSQL_PORT',
        'ssl_cert_file': 'CODALAB_SSL_CERT_FILE',
        'ssl_key_file': 'CODALAB_SSL_KEY_FILE',
    }

    @staticmethod
    def _get_parser():
        parser = argparse.ArgumentParser(
            description="Manages your local CodaLab worksheets service deployment using docker-compose"
        )
        subparsers = parser.add_subparsers(dest='command', description='Command to run')

        # SUBCOMMANDS

        start_cmd = subparsers.add_parser('start', help='Start a CodaLab service instance')
        logs_cmd = subparsers.add_parser('logs', help='View logs for existing CodaLab instance')
        test_cmd = subparsers.add_parser(
            'test', help='Run tests against an existing CodaLab instance'
        )
        build_cmd = subparsers.add_parser(
            'build', help='Build CodaLab docker images using the local codebase'
        )
        run_cmd = subparsers.add_parser('run', help='Run a command inside a service container')

        stop_cmd = subparsers.add_parser('stop', help='Stop any existing CodaLab service instances')
        down_cmd = subparsers.add_parser(
            'down', help='Bring down any existing CodaLab service instances'
        )
        restart_cmd = subparsers.add_parser(
            'restart', help='Restart any existing CodaLab service instances'
        )

        #  CLIENT SETTINGS

        for cmd in [start_cmd, run_cmd, test_cmd]:
            cmd.add_argument(
                '--codalab-user',
                type=str,
                help='Codalab username for the Codalab admin user',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--codalab-password',
                type=str,
                help='Codalab password for the Codalab admin user',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--rest-port',
                type=str,
                help='Port for the REST server to listen on (by default it is not exposed to the host machine)',
                default=argparse.SUPPRESS,
            )

            # MYSQL SETTINGS

            cmd.add_argument(
                '--external-db-url',
                help='if specified, use this database uri instead of starting a local mysql container',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--mysql-port',
                type=str,
                help='Port for the MYSQL database to listen on (by default it is not exposed to the host machine)',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--mysql-user',
                type=str,
                help='MYSQL username for the Codalab MYSQL client',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--mysql-password',
                type=str,
                help='MYSQL password for the Codalab MYSQL client',
                default=argparse.SUPPRESS,
            )

        #  BUILD SETTINGS

        for cmd in [build_cmd, start_cmd, run_cmd]:
            cmd.add_argument(
                '--version',
                '-v',
                type=str,
                help='CodaLab version to use for building and deployment',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--dev',
                '-d',
                action='store_true',
                help='If specified, use dev versions of images',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--push',
                action='store_true',
                help='If specified, push the images to Dockerhub',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--docker-user',
                type=str,
                help='DockerHub username to push images from',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--docker-password',
                type=str,
                help='DockerHub password to push images from',
                default=argparse.SUPPRESS,
            )

        build_cmd.add_argument(
            'image',
            default='service',
            help='Images to build. \'service\' for server-side images (server, frontend, worker) \
                    \'all\' for those and the default execution images',
            choices=CodalabServiceManager.ALL_IMAGES + ['all', 'service'],
            nargs='?',
        )

        #  DEPLOYMENT SETTINGS
        for cmd in [start_cmd, stop_cmd, restart_cmd, down_cmd, logs_cmd]:
            cmd.add_argument(
                '--instance-name',
                type=str,
                help='Docker compose name to use for instance',
                default=argparse.SUPPRESS,
            )

        start_cmd.add_argument(
            '--build-locally',
            '-b',
            action='store_true',
            help='If specified, build VERSION using local code.',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--test-build',
            '-t',
            action='store_true',
            help='If specified, run tests on the build.',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--user-compose-file',
            type=str,
            help='If specified, path to a user-defined Docker compose file that overwrites the defaults',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--start-worker',
            '-w',
            action='store_true',
            help='If specified, start a CodaLab worker on this machine.',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--initial-config',
            '-i',
            action='store_true',
            help='If specified, save the initial configuration of the instance (defaults to true if the service home or the database mounts are ephemeral)',
            default=argparse.SUPPRESS,
        )

        #  USER CREDENTIALS

        start_cmd.add_argument(
            '--mysql-root-password',
            type=str,
            help='Root password for the database',
            default=argparse.SUPPRESS,
        )

        #  HOST FILESYSTEM MOUNTS

        start_cmd.add_argument(
            '--uid',
            type=str,
            help='Linux UID that owns the files created by Codalab. default=(ID of the user running this script)',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--codalab-home',
            type=str,
            help='Path on the host machine to store home directory of the Codalab server (by default nothing is stored)',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--mysql-mount',
            type=str,
            help='Path on the host machine to store mysql data files (by default the database is ephemeral)',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--worker-dir',
            type=str,
            help='Path on the host machine to store worker data files (defaults to <repo root>/codalab-worker-scratch if worker started)',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--bundle-store',
            type=str,
            help='Path on the host machine to store bundle data files (by default these are ephemeral)',
            default=[],
            dest='bundle_stores',
            action='append',
        )

        #  HOST PORT MOUNTS

        start_cmd.add_argument(
            '--http-port',
            type=str,
            help='HTTP port for the server to listen on',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--frontend-port',
            type=str,
            help='Port for the React server to listen on (by default it is not exposed to the host machine)',
            default=argparse.SUPPRESS,
        )

        #  SSL CONFIGURATION

        start_cmd.add_argument(
            '--use-ssl',
            action='store_true',
            help='If specified, set the server up with SSL',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--ssl-cert-file',
            type=str,
            help='Path to the cert file for SSL',
            default=argparse.SUPPRESS,
        )
        start_cmd.add_argument(
            '--ssl-key-file',
            type=str,
            help='Path to the key file for SSL',
            default=argparse.SUPPRESS,
        )

        #  LOGS SETTINGS

        logs_cmd.add_argument(
            'services',
            nargs='*',
            default='all',
            help='Services to print logs for',
            choices=[
                'mysql',
                'rest-server',
                'bundle-manager',
                'frontend',
                'nginx',
                'worker',
                'all',
            ],
        )
        logs_cmd.add_argument(
            '--follow',
            '-f',
            action='store_true',
            help='If specified, follow the logs',
            default=argparse.SUPPRESS,
        )
        logs_cmd.add_argument(
            '--tail',
            '-t',
            type=int,
            help='If specified, tail TAIL lines from the ends of each log',
            default=argparse.SUPPRESS,
        )

        #  TESTS SETTINGS

        test_cmd.add_argument(
            'tests',
            metavar='TEST',
            nargs='+',
            type=str,
            choices=test_cli.TestModule.modules.keys() + ['all', 'default'],
            default=['default'],
            help='Tests to run',
        )

        #  RUN SETTINGS

        run_cmd.add_argument(
            'service',
            metavar='SERVICE',
            type=str,
            choices=[
                'mysql',
                'rest-server',
                'bundle-manager',
                'frontend',
                'nginx',
                'worker',
                'all',
            ],
            help='Service container to run command on',
        )
        run_cmd.add_argument('cmd', metavar='CMD', type=str, help='Command to run')
        run_cmd.add_argument(
            '--no-root', action='store_true', help='If specified, run as current user'
        )
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

    SERVICE_IMAGES = ['server', 'frontend', 'worker']
    ALL_IMAGES = SERVICE_IMAGES + ['default-cpu', 'default-gpu']

    @staticmethod
    def resolve_env_vars(args):
        environment = {
            'CODALAB_MYSQL_ROOT_PWD': args.mysql_root_password,
            'CODALAB_MYSQL_USER': args.mysql_user,
            'CODALAB_MYSQL_PWD': args.mysql_password,
            'CODALAB_ROOT_USER': args.codalab_user,
            'CODALAB_ROOT_PWD': args.codalab_password,
            'CODALAB_HTTP_PORT': args.http_port,
            'CODALAB_VERSION': args.version,
            'CODALAB_WORKER_NETWORK_NAME': '%s-worker-network' % args.instance_name,
        }
        if args.uid:
            environment['CODALAB_UID'] = args.uid
        else:
            environment['CODALAB_UID'] = '%s:%s' % (os.getuid(), os.getgid())
        if args.codalab_home:
            environment['CODALAB_SERVICE_HOME'] = args.codalab_home
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
        if 'DOCKER_HOST' in os.environ:
            environment['DOCKER_HOST'] = os.environ['DOCKER_HOST']
        return environment

    @staticmethod
    def resolve_compose_files(args):
        compose_files = ['docker-compose.yml']
        if args.dev:
            compose_files.append('docker-compose.dev.yml')
        if args.codalab_home:
            compose_files.append('docker-compose.home_mount.yml')
        else:
            compose_files.append('docker-compose.no_home_mount.yml')
        if args.external_db_url is None:
            compose_files.append('docker-compose.mysql.yml')
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
        if self.args.codalab_home:
            try:
                os.makedirs(self.args.codalab_home)
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
        elif self.command == 'run':
            self.run_service_cmd(
                self.args.cmd, root=not self.args.no_root, service=self.args.service
            )
        elif self.command == 'logs':
            cmd_str = 'logs'
            if self.args.tail is not None:
                cmd_str += ' --tail %s' % self.args.tail
            if self.args.follow:
                cmd_str += ' -f'
            self._run_compose_cmd('logs')
        elif self.command == 'stop':
            self._run_compose_cmd('stop')
        elif self.command == 'down':
            self._run_compose_cmd('down --remove-orphans')
        elif self.command == 'test':
            self.test()

    def _run_docker_cmd(self, cmd):
        subprocess.check_call('docker ' + cmd, shell=True, cwd=self.root_dir)

    def build_image(self, image):
        print("[CODALAB] ==> Building %s image " % image)
        self._run_docker_cmd(
            'build -t codalab/%s:%s -f docker/dockerfiles/Dockerfile.%s .'
            % (image, self.args.version, image)
        )

    def push_image(self, image):
        self._run_docker_cmd('push codalab/%s:%s' % (image, self.args.version))

    def _run_compose_cmd(self, cmd):
        files_string = ' -f '.join(self.compose_files)
        command_string = 'docker-compose -p %s -f %s %s' % (
            self.args.instance_name,
            files_string,
            cmd,
        )
        subprocess.check_call(
            command_string, cwd=self.compose_cwd, env=self.compose_env, shell=True
        )

    def bring_up_service(self, service):
        self._run_compose_cmd('up -d --no-deps %s' % service)

    def run_service_cmd(self, cmd, root=False, service='rest-server'):
        if root:
            uid = '0:0'
        else:
            uid = self.compose_env['CODALAB_UID']
        self._run_compose_cmd(
            'run --no-deps --rm --entrypoint="" --user=%s ' % uid + service + ' bash -c "%s"' % cmd
        )

    def start_service(self):
        if self.args.external_db_url is None:
            print("[CODALAB] ==> Starting MySQL")
            self.bring_up_service('mysql')
            cmd_prefix = '/opt/wait-for-it.sh mysql:3306 -- '
            mysql_url = 'mysql://%s:%s@mysql:3306/codalab_bundles' % (
                self.compose_env['CODALAB_MYSQL_USER'],
                self.compose_env['CODALAB_MYSQL_PWD'],
            )
        else:
            cmd_prefix = ''
            mysql_url = 'mysql://%s:%s@%s/codalab_bundles' % (
                self.compose_env['CODALAB_MYSQL_USER'],
                self.compose_env['CODALAB_MYSQL_PWD'],
                self.args.external_db_url,
            )
        print("[CODALAB] ==> Configuring the service")
        self.run_service_cmd(
            "%s/opt/codalab-worksheets/codalab/bin/cl config server/engine_url %s && /opt/codalab-worksheets/codalab/bin/cl config cli/default_address http://rest-server:2900 && /opt/codalab-worksheets/codalab/bin/cl config server/rest_host 0.0.0.0"
            % (cmd_prefix, mysql_url),
            root=(not self.args.codalab_home),
        )

        if self.args.initial_config:
            print("[CODALAB] ==> Creating root user")
            self.run_service_cmd(
                "/opt/codalab-worksheets/venv/bin/pip install /opt/codalab-worksheets && %s/opt/codalab-worksheets/venv/bin/python /opt/codalab-worksheets/scripts/create-root-user.py %s"
                % (cmd_prefix, self.compose_env['CODALAB_ROOT_PWD']),
                root=True,
            )

        print("[CODALAB] ==> Starting rest server")
        self.bring_up_service('rest-server')

        if self.args.initial_config:
            print("[CODALAB] ==> Creating initial worksheets")
            self.run_service_cmd(
                "/opt/wait-for-it.sh rest-server:2900 -- opt/codalab-worksheets/codalab/bin/cl logout && /opt/codalab-worksheets/codalab/bin/cl new home && /opt/codalab-worksheets/codalab/bin/cl new dashboard",
                root=(not self.args.codalab_home),
            )

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
        if self.args.image == 'all':
            images_to_build = self.ALL_IMAGES
        elif self.args.image == 'service':
            images_to_build = self.SERVICE_IMAGES
        else:
            images_to_build = [self.args.image]
        for image in images_to_build:
            if image == 'frontend' and self.args.dev:
                image = 'frontend-dev'
            self.build_image(image)
        if self.args.push:
            self._run_docker_cmd(
                'login -u %s -p %s' % (self.args.docker_user, self.args.docker_password)
            )
            for image in images_to_build:
                self.push_image(image)

    def test(self):
        instance = 'http://localhost:%s' % self.args.rest_port
        test_cli.cl = 'codalab/bin/cl'
        test_cli.cl_version = self.args.version
        codalab_client_env = {
            'CODALAB_USERNAME': self.args.codalab_user,
            'CODALAB_PASSWORD': self.args.codalab_password,
        }
        if self.args.external_db_url:
            mysql_url = 'mysql://%s:%s@%s/codalab_bundles' % (
                self.args.mysql_user,
                self.args.mysql_password,
                self.args.external_db_url,
            )
        else:
            if not self.args.mysql_port:
                raise (
                    'ERROR: Tests fired without an external DB URL or MYSQL port exposed to host, "events" tests will fail.'
                )
            mysql_url = 'mysql://%s:%s@127.0.0.1:%s/codalab_bundles' % (
                self.args.mysql_user,
                self.args.mysql_password,
                self.args.mysql_port,
            )
        subprocess.check_call(
            '%s config server/engine_url %s' % (test_cli.cl, mysql_url),
            env=codalab_client_env,
            shell=True,
        )
        subprocess.check_call(
            '%s work %s::' % (test_cli.cl, instance), env=codalab_client_env, shell=True
        )
        success = test_cli.TestModule.run(self.args.tests, instance)
        if not success:
            sys.exit(1)


if __name__ == '__main__':
    main()
