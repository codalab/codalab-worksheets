#! /usr/bin/python2.7

"""
The main entry point for bringing up CodaLab services.  This is used for both
local development and actual deployment.
"""
from __future__ import print_function

import argparse
import errno
import os
import subprocess
import sys

SERVICES = ['mysql', 'nginx', 'frontend', 'rest-server', 'bundle-manager', 'worker']

SERVICE_TO_IMAGE = {
    'frontend': 'frontend',
    'rest-server': 'server',
    'bundle-manager': 'server',
    'worker': 'worker',
}


def print_header(description):
    print('[CodaLab] {}'.format(description))


def should_run_service(args, service):
    # `default` is generally used to bring up everything for local dev or quick testing.
    # `default-no-worker` is generally used for real deployment since we don't
    # want a worker running on the same machine.
    return (
        service in args.services
        or (service != 'test' and 'default' in args.services)
        or (service != 'test' and service != 'worker' and 'default-no-worker' in args.services)
    )


def need_image_for_service(args, image):
    """Does `image` support a service we want to run."""
    for service, service_image in SERVICE_TO_IMAGE.items():
        if should_run_service(args, service) and image == service_image:
            return True
    return False


def should_build_image(args, image):
    if image in args.images:
        return True
    if 'all' in args.images:
        return True
    if 'services' in args.images:
        return need_image_for_service(args, image)
    return False


def main():
    args = CodalabArgs.get_args()
    service_manager = CodalabServiceManager(args)
    service_manager.execute()


def get_default_version():
    """Get the current git branch."""
    return subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip()


class CodalabArgs(argparse.Namespace):
    DEFAULT_ARGS = {
        'version': get_default_version(),
        'dev': False,
        'pull': False,
        'push': False,
        'docker_user': None,
        'docker_password': None,
        'build_images': False,
        'images': ['services'],
        'external_db_url': None,
        'user_compose_file': None,
        'services': ['default'],
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
        'rest_port': '2900',
        'frontend_port': None,
        'mysql_port': None,
        'use_ssl': False,
        'ssl_cert_file': None,
        'ssl_key_file': None,
        'follow': False,
        'tail': None,
    }

    ARG_TO_ENV_VAR = {
        'version': 'CODALAB_VERSION',
        'dev': 'CODALAB_DEV',
        'push': 'CODALAB_PUSH',
        'external_db_url': 'CODALAB_EXTERNAL_DB_URL',
        'docker_user': 'DOCKER_USER',
        'docker_password': 'DOCKER_PWD',
        'user_compose_file': 'CODALAB_USER_COMPOSE_FILE',
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
        pull_cmd = subparsers.add_parser('pull', help='Pull images from Docker Hub')
        build_cmd = subparsers.add_parser(
            'build', help='Build CodaLab docker images using the local codebase'
        )
        run_cmd = subparsers.add_parser('run', help='Run a command inside a service container')
        stop_cmd = subparsers.add_parser('stop', help='Stop any existing CodaLab service instances')
        delete_cmd = subparsers.add_parser(
            'delete',
            help='Bring down any existing CodaLab service instances (and delete all non-external data!)',
        )
        restart_cmd = subparsers.add_parser(
            'restart', help='Restart any existing CodaLab service instances'
        )

        #  CLIENT SETTINGS
        for cmd in [
            start_cmd,
            logs_cmd,
            pull_cmd,
            build_cmd,
            run_cmd,
            stop_cmd,
            delete_cmd,
            restart_cmd,
        ]:
            cmd.add_argument(
                '--dry-run',
                action='store_true',
                help='Just print out the commands that will be run and not execute anything',
            )

        for cmd in [start_cmd, run_cmd]:

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
                help='Port for the MySQL database to listen on (by default it is not exposed to the host machine)',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--mysql-user',
                type=str,
                help='MySQL username for the CodaLab MySQL client',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--mysql-password',
                type=str,
                help='MySQL password for the CodaLab MySQL client',
                default=argparse.SUPPRESS,
            )

        #  BUILD SETTINGS

        for cmd in [build_cmd, start_cmd, run_cmd]:
            cmd.add_argument(
                '--version',
                '-v',
                type=str,
                help='CodaLab version to use for building and deployment (defaults to branch name, set only for Travis CI)',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--dev',
                '-d',
                action='store_true',
                help='If specified, mount local code for frontend so that changes are reflected right away',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--pull',
                action='store_true',
                help='If specified, pull images from Docker Hub (for caching)',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--push',
                action='store_true',
                help='If specified, push the images to Docker Hub',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--docker-user',
                type=str,
                help='Docker Hub username to push images from',
                default=argparse.SUPPRESS,
            )
            cmd.add_argument(
                '--docker-password',
                type=str,
                help='Docker Hub password to push images from',
                default=argparse.SUPPRESS,
            )

        build_cmd.add_argument(
            'images',
            default='services',
            help='Images to build. \'services\' for server-side images (frontend, server, worker) \
                    \'all\' for those and the default execution images',
            choices=CodalabServiceManager.ALL_IMAGES + ['all', 'services'],
            nargs='*',
        )

        #  DEPLOYMENT SETTINGS
        for cmd in [start_cmd, stop_cmd, restart_cmd, delete_cmd, logs_cmd]:
            cmd.add_argument(
                '--instance-name',
                type=str,
                help='Docker compose name to use for instance',
                default=argparse.SUPPRESS,
            )

        start_cmd.add_argument(
            '--build-images',
            '-b',
            action='store_true',
            help='If specified, build Docker images using local code.',
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
            '--services',
            '-s',
            nargs='*',
            help='List of services to run',
            choices=SERVICES + ['default', 'default-no-worker', 'init', 'update', 'test'],
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
            default='default',
            help='Services to print logs for',
            choices=SERVICES + ['default'],
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

        #  RUN SETTINGS

        run_cmd.add_argument(
            'service',
            metavar='SERVICE',
            type=str,
            choices=SERVICES,
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
            #'PATH': os.environ['PATH'],
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
        if should_run_service(args, 'worker'):
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
        if should_run_service(args, 'worker'):
            compose_files.append('docker-compose.worker.yml')
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
        if should_run_service(self.args, 'worker'):
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
                os.makedirs(bundle_store)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    def execute(self):
        if self.command == 'build':
            self.build_images()
        elif self.command == 'pull':
            self.pull_images()
        elif self.command == 'start':
            if self.args.build_images:
                self.build_images()
            self.start_services()
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
        elif self.command == 'delete':
            self._run_compose_cmd('down --remove-orphans -v')
        else:
            raise Exception('Bad command: ' + self.command)

    def build_image(self, image):
        print_header('Building {} image'.format(image))
        master_docker_image = 'codalab/{}:{}'.format(image, 'master')
        docker_image = 'codalab/{}:{}'.format(image, self.args.version)

        # Pull the previous image on this version (branch) if we have it.  Otherwise, use master.
        if self.args.pull:
            if self._run_docker_cmd('pull {}'.format(docker_image), allow_fail=True):
                cache_image = docker_image
            else:
                self._run_docker_cmd('pull {}'.format(master_docker_image))
                cache_image = master_docker_image
            cache_args = ' --cache-from {}'.format(cache_image)
        else:
            cache_args = ''

        # Build the image using the cache
        self._run_docker_cmd(
            'build%s -t %s -f docker/dockerfiles/Dockerfile.%s .'
            % (cache_args, docker_image, image)
        )

    def push_image(self, image):
        self._run_docker_cmd('push codalab/%s:%s' % (image, self.args.version))

    def pull_image(self, image):
        self._run_docker_cmd('pull codalab/%s:%s' % (image, self.args.version))

    def _run_docker_cmd(self, cmd, allow_fail=False):
        """Return whether the command succeeded."""
        command_string = 'docker ' + cmd
        print('(cd {}; {})'.format(self.root_dir, command_string))
        if self.args.dry_run:
            success = True
        else:
            try:
                subprocess.check_call(command_string, shell=True, cwd=self.root_dir)
                success = True
            except subprocess.CalledProcessError as e:
                success = False
                if not allow_fail:
                    raise e
        print('')
        return success

    def _run_compose_cmd(self, cmd):
        files_string = ' -f '.join(self.compose_files)
        command_string = 'docker-compose -p %s -f %s %s' % (
            self.args.instance_name,
            files_string,
            cmd,
        )
        compose_env_string = ' '.join('{}={}'.format(k, v) for k, v in self.compose_env.items())
        print('(cd {}; {} {})'.format(self.compose_cwd, compose_env_string, command_string))
        if self.args.dry_run:
            success = True
        else:
            try:
                popen = subprocess.Popen(
                    command_string,
                    cwd=self.compose_cwd,
                    env=self.compose_env,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                # Note: don't do `for stdout_line in popen.stdout` because that buffers.
                while True:
                    stdout_line = popen.stdout.readline()
                    if not stdout_line:
                        break
                    print(
                        "process: " + stdout_line.decode('utf-8').encode('ascii', errors='replace'),
                        end="",
                    )
                popen.wait()
                success = popen.returncode == 0
                if not success:
                    raise Exception('Command exited with code {}'.format(popen.returncode))
            except subprocess.CalledProcessError as e:
                print(
                    "CalledProcessError: {}, {}".format(
                        str(e), e.output.decode('utf-8').encode('ascii', errors='replace')
                    )
                )
                raise e
        print('')
        return success

    def bring_up_service(self, service):
        if should_run_service(self.args, service):
            print_header('Bringing up {}'.format(service))
            self._run_compose_cmd('up -d --no-deps %s' % service)

    def run_service_cmd(self, cmd, root=False, service='rest-server'):
        if root:
            uid = '0:0'
        else:
            uid = self.compose_env['CODALAB_UID']
        self._run_compose_cmd(
            ('run --no-deps --rm --entrypoint="" --user=%s ' % uid)
            + service
            + (
                ' bash -c \'%s\'' % cmd
            )  # TODO: replace with shlex.quote(cmd) once we're on Python 3
        )

    def start_services(self):
        if self.args.external_db_url is None:
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

        if should_run_service(self.args, 'init'):
            print_header('Configuring the service')
            self.run_service_cmd(
                "%scl config server/engine_url %s && cl config cli/default_address http://rest-server:2900 && cl config server/rest_host 0.0.0.0"
                % (cmd_prefix, mysql_url),
                root=(not self.args.codalab_home),
            )

            print_header('Creating root user')
            self.run_service_cmd(
                "%spython scripts/create-root-user.py %s"
                % (cmd_prefix, self.compose_env['CODALAB_ROOT_PWD']),
                root=True,
            )

            print_header('Initializing/migrating the database with alembic')
            self.run_service_cmd("%secho mysql ready" % cmd_prefix, root=True)
            # The first time, we need to stamp; after that upgrade.
            self.run_service_cmd(
                "if [ $(alembic current | wc -l) -gt 0 ]; then echo upgrade; alembic upgrade head; else echo stamp; alembic stamp head; fi",
                root=True,
            )

        self.bring_up_service('rest-server')

        if should_run_service(self.args, 'init'):
            print_header('Creating home and dashboard worksheets')
            self.run_service_cmd(
                "/opt/wait-for-it.sh rest-server:2900 -- cl logout && cl status && ((cl new home && cl new dashboard) || exit 0)",
                root=(not self.args.codalab_home),
            )

        self.bring_up_service('bundle-manager')
        self.bring_up_service('frontend')
        self.bring_up_service('nginx')
        self.bring_up_service('worker')

        if should_run_service(self.args, 'test'):
            print_header('Running tests')
            self.run_service_cmd(
                "/opt/wait-for-it.sh rest-server:2900 -- python test_cli.py --instance http://rest-server:2900 default",
                root=(not self.args.codalab_home),
            )

    def pull_images(self):
        for image in self.SERVICE_IMAGES:
            self.pull_image(image)

    def build_images(self):
        images_to_build = [
            image for image in self.ALL_IMAGES if should_build_image(self.args, image)
        ]

        for image in images_to_build:
            self.build_image(image)

        if self.args.push:
            self._run_docker_cmd(
                'login -u %s -p %s' % (self.args.docker_user, self.args.docker_password)
            )
            for image in images_to_build:
                self.push_image(image)


if __name__ == '__main__':
    main()
