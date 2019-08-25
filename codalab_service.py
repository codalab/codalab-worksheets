#! /usr/bin/python2.7

"""
The main entry point for bringing up CodaLab services.  This is used for both
local development and actual deployment.

A full deployment is governed by a set of *arguments*, which can either be
specified via (later overriding the former):
- (i) defaults defined in this file,
- (ii) environment variables (e.g., CODALAB_HOME),
- (iii) command-line arguments

We then launch a set of services (e.g., `rest-server`), where for each one:
- We create an environment from a subset of the above arguments.
- We call `docker-compose` or `docker run`, which might read these environment
  variables and pass them through to the actual service (see Docker files for
  exact logic).
"""
from __future__ import print_function

import argparse
import errno
import os
import subprocess
from collections import namedtuple

DEFAULT_SERVICES = ['mysql', 'nginx', 'frontend', 'rest-server', 'bundle-manager', 'worker', 'init']

ALL_SERVICES = DEFAULT_SERVICES + ['test', 'monitor']

ALL_NO_SERVICES = [
    'no-' + service for service in ALL_SERVICES
]  # Identifiers that stand for exclusion of certain services

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

# Which docker image is used to run each service?
SERVICE_TO_IMAGE = {
    'frontend': 'frontend',
    'rest-server': 'server',
    'bundle-manager': 'server',
    'monitor': 'server',
    'worker': 'worker',
}


def ensure_directory_exists(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def print_header(description):
    print('[CodaLab] {}'.format(description))


def should_run_service(args, service):
    # `default` is generally used to bring up everything for local dev or quick testing.
    # `default no-worker` is generally used for real deployment since we don't want a worker running on the same machine.
    services = [] if args.services is None else args.services
    if 'default' in args.services:
        services.extend(DEFAULT_SERVICES)

    return (service in services) and ('no-' + service not in services)


def need_image_for_service(args, image):
    """Does `image` support a service we want to run?"""
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


def var_path(name):
    return lambda args: os.path.join(BASE_DIR, 'var', args.instance_name, name)


# An configuration argument.
class CodalabArg(object):
    def __init__(self, name, help, type=str, env_var=None, flag=None, default=None):
        self.name = name
        self.help = help
        self.type = type
        self.env_var = env_var or 'CODALAB_' + name.upper()
        self.flag = flag  # Command-line argument
        self.default = default

    def has_constant_default(self):
        return self.default is not None and not callable(self.default)

    def has_callable_default(self):
        return self.default is not None and callable(self.default)


CODALAB_ARGUMENTS = [
    CodalabArg(
        name='version',
        help='Version of CodaLab (usually the branch name)',
        default=get_default_version(),
        flag='-v',
    ),
    CodalabArg(
        name='instance_name',
        help='Instance name (prefixed to Docker containers)',
        default='codalab',
    ),
    CodalabArg(
        name='worker_network_name',
        help='Network name for the worker',
        default=lambda args: args.instance_name + '-worker-network',
    ),
    ### Docker
    CodalabArg(name='docker_username', help='Docker Hub username to push built images'),
    CodalabArg(name='docker_password', help='Docker Hub password to push built images'),
    ### CodaLab
    CodalabArg(
        name='codalab_username',
        env_var='CODALAB_USERNAME',
        help='CodaLab (root) username',
        default='codalab',
    ),
    CodalabArg(
        name='codalab_password',
        env_var='CODALAB_PASSWORD',
        help='CodaLab (root) password',
        default='codalab',
    ),
    ### MySQL
    CodalabArg(name='mysql_host', help='MySQL hostname', default='mysql'),  # Inside Docker
    CodalabArg(name='mysql_port', help='MySQL hostname', default=3306, type=int),
    CodalabArg(name='mysql_database', help='MySQL database name', default='codalab_bundles'),
    CodalabArg(name='mysql_username', help='MySQL username', default='codalab'),
    CodalabArg(name='mysql_password', help='MySQL password', default='codalab'),
    CodalabArg(name='mysql_root_password', help='MySQL root password', default='codalab'),
    CodalabArg(
        'uid',
        help='Linux UID that owns the files created by Codalab. default=(ID of the user running this script)',
        default='%s:%s' % (os.getuid(), os.getgid()),
    ),
    CodalabArg(
        'codalab_home',
        env_var='CODALAB_HOME',
        help='Path on the host machine to store home directory of the Codalab server (e.g., config.json file)',
        default=var_path('home'),
    ),
    CodalabArg(
        'bundle_store',
        help='Path on the host machine to store bundle data files',
        default=var_path('bundles'),
    ),
    CodalabArg(
        'mysql_mount',
        help='Path on the host machine to store MySQL data files (by default the database is ephemeral)',
        default=var_path('mysql'),
    ),
    CodalabArg(
        'monitor_dir',
        help='Path on the host machine to store monitor script output',
        default=var_path('monitor'),
    ),
    CodalabArg(
        'worker_dir',
        help='Path on the host machine to store worker data files (defaults to <repo root>/codalab-worker-scratch if worker started)',
        default=var_path('worker'),
    ),
    CodalabArg('http_port', help='HTTP port for the server to listen on', type=int, default=80),
    CodalabArg(
        'https_port',
        help='HTTP port for the server to listen on (when using SSL)',
        type=int,
        default=443,
    ),
    CodalabArg(
        'frontend_port',
        help='Port for the React server to listen on (by default it is not exposed to the host machine)',
        type=int,
        default=2700,
    ),
    CodalabArg(
        name='rest_port',
        help='Port for the REST server to listen on (by default it is not exposed to the host machine)',
        type=int,
        default=2900,
    ),
    ### Email
    CodalabArg(name='admin_email', help='Email to send admin notifications to (e.g., monitoring)'),
    CodalabArg(name='email_host', help='Send email by logging into this SMTP server'),
    CodalabArg(name='email_username', help='Username of email account for sending email'),
    CodalabArg(name='email_password', help='Password of email account for sending email'),
    ### SSL
    CodalabArg(
        name='use_ssl', help='If specified, set the server up with SSL', type=bool, default=False
    ),
    CodalabArg(name='ssl_cert_file', help='Path to the cert file for SSL'),
    CodalabArg(name='ssl_key_file', help='Path to the key file for SSL'),
]


class CodalabArgs(object):
    @staticmethod
    def _get_parser():
        parser = argparse.ArgumentParser(
            description='Manages your local CodaLab Worksheets service deployment'
        )
        subparsers = parser.add_subparsers(dest='command', description='Command to run')

        # Subcommands
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

        # Arguments for every subcommand
        for cmd in [start_cmd, logs_cmd, pull_cmd, build_cmd, run_cmd, stop_cmd, delete_cmd]:
            cmd.add_argument(
                '--dry-run',
                action='store_true',
                help='Just print out the commands that will be run and not execute anything',
            )

            ## Arguments for `start`
            # for cmd in [start_cmd]:
            for arg in CODALAB_ARGUMENTS:
                unnamed = ['--' + arg.name.replace('_', '-')]
                if arg.flag:
                    unnamed.append(arg.flag)
                named = {'help': arg.help}
                if arg.has_constant_default():
                    named['default'] = arg.default
                if arg.type == bool:
                    named['action'] = 'store_true'
                else:
                    named['type'] = arg.type
                cmd.add_argument(*unnamed, **named)

            # Arguments for `start`
            cmd.add_argument(
                '--build-images',
                '-b',
                action='store_true',
                help='If specified, build Docker images using local code.',
            )

            # Arguments for `build` and `start`
            cmd.add_argument(
                '--pull',
                action='store_true',
                help='If specified, pull images from Docker Hub (for caching)',
                default=False,
            )
            cmd.add_argument(
                '--push',
                action='store_true',
                help='If specified, push the images to Docker Hub',
                default=False,
            )
            cmd.add_argument(
                'images',
                default='services',
                help='Images to build. \'services\' for server-side images (frontend, server, worker) \
                        \'all\' for those and the default execution images',
                choices=CodalabServiceManager.ALL_IMAGES + ['all', 'services'],
                nargs='*',
            )
            cmd.add_argument(
                '--dev',
                '-d',
                action='store_true',
                help='If specified, mount local code for frontend so that changes are reflected right away',
            )
            cmd.add_argument(
                '--services',
                '-s',
                nargs='*',
                help='List of services to run',
                choices=ALL_SERVICES + ALL_NO_SERVICES + ['default'],
                default=['default'],
            )

        # Arguments for `logs`
        logs_cmd.add_argument(
            'services',
            nargs='*',
            default='default',
            help='Services to print logs for',
            choices=ALL_SERVICES + ['default'],
        )
        logs_cmd.add_argument(
            '--follow',
            '-f',
            action='store_true',
            help='If specified, follow the logs',
            default=True,
        )
        logs_cmd.add_argument(
            '--tail',
            '-t',
            type=int,
            help='If specified, tail TAIL lines from the ends of each log',
            default=100,
        )

        # Arguments for `run`
        run_cmd.add_argument(
            'service',
            metavar='SERVICE',
            choices=ALL_SERVICES,
            help='Service container to run command on',
        )
        run_cmd.add_argument('command', metavar='CMD', type=str, help='Command to run')

        return parser

    @classmethod
    def get_args(cls):
        parser = cls._get_parser()
        args = argparse.Namespace()

        # Set from command-line arguments
        parser.parse_args(namespace=args)

        # Set from environment variables
        for arg in CODALAB_ARGUMENTS:
            if getattr(args, arg.name, None):  # Skip if set
                continue
            if arg.env_var in os.environ:
                setattr(args, arg.name, os.environ[arg.env_var])

        # Set constant default values
        for arg in CODALAB_ARGUMENTS:
            if getattr(args, arg.name, None):  # Skip if set
                continue
            if arg.has_constant_default():
                setattr(args, arg.name, arg.default)

        # Set functional default values (needs to be after everything)
        for arg in CODALAB_ARGUMENTS:
            if getattr(args, arg.name, None):  # Skip if set
                continue
            if arg.has_callable_default():
                setattr(args, arg.name, arg.default(args))

        return args


class CodalabServiceManager(object):
    SERVICE_IMAGES = ['server', 'frontend', 'worker']
    ALL_IMAGES = SERVICE_IMAGES + ['default-cpu', 'default-gpu']

    @staticmethod
    def resolve_env_vars(args):
        """Return environment with all the arguments (which might have originally come from the environment too)."""
        environment = {}
        for arg in CODALAB_ARGUMENTS:
            value = getattr(args, arg.name, None)
            if value:
                environment[arg.env_var] = str(value)
        # Additional environment variables to pass through
        for env_var in ['PATH', 'DOCKER_HOST']:
            if env_var in os.environ:
                environment[env_var] = os.environ[env_var]
        return environment

    def __init__(self, args):
        self.args = args
        self.compose_cwd = os.path.join(BASE_DIR, 'docker', 'compose_files')

        self.compose_files = ['docker-compose.yml']
        if self.args.dev:
            self.compose_files.append('docker-compose.dev.yml')
        if self.args.use_ssl:
            self.compose_files.append('docker-compose.ssl.yml')

        self.compose_env = self.resolve_env_vars(args)
        ensure_directory_exists(self.args.codalab_home)
        ensure_directory_exists(self.args.monitor_dir)
        ensure_directory_exists(self.args.worker_dir)
        ensure_directory_exists(self.args.mysql_mount)
        ensure_directory_exists(self.args.bundle_store)

    def execute(self):
        command = self.args.command
        if command == 'build':
            self.build_images()
        elif command == 'pull':
            self.pull_images()
        elif command == 'start':
            if self.args.build_images:
                self.build_images()
            self.start_services()
        elif command == 'run':
            self.run_service_cmd(self.args.command, service=self.args.service)
        elif command == 'logs':
            cmd_str = 'logs'
            if self.args.tail is not None:
                cmd_str += ' --tail %s' % self.args.tail
            if self.args.follow:
                cmd_str += ' -f'
            self._run_compose_cmd('logs')
        elif command == 'stop':
            self._run_compose_cmd('stop')
        elif command == 'delete':
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
        print(command_string)
        if self.args.dry_run:
            success = True
        else:
            try:
                subprocess.check_call(command_string, shell=True, cwd=BASE_DIR)
                success = True
            except subprocess.CalledProcessError as e:
                success = False
                if not allow_fail:
                    raise e
        print('')
        return success

    def _run_compose_cmd(self, cmd):
        compose_files_str = ' '.join('-f ' + f for f in self.compose_files)
        command_string = 'docker-compose -p %s %s %s' % (
            self.args.instance_name,
            compose_files_str,
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
            uid = self.args.uid
        self._run_compose_cmd(
            ('run --no-deps --rm --user=%s ' % uid)
            + service
            + (
                ' bash -c \'%s\'' % cmd
            )  # TODO: replace with shlex.quote(cmd) once we're on Python 3
        )

    def start_services(self):
        def wait(host, port, cmd):
            return '/opt/wait-for-it.sh {}:{} -- {}'.format(host, port, cmd)

        def wait_mysql(cmd):
            return wait(self.args.mysql_host, self.args.mysql_port, cmd)

        def wait_rest_server(cmd):
            return wait('rest-server', self.args.rest_port, cmd)

        mysql_url = 'mysql://{}:{}@{}:{}/{}'.format(
            self.args.mysql_username,
            self.args.mysql_password,
            self.args.mysql_host,
            self.args.mysql_port,
            self.args.mysql_database,
        )
        rest_url = 'http://rest-server:{}'.format(self.args.rest_port)

        if self.args.mysql_host == 'mysql':
            self.bring_up_service('mysql')

        if should_run_service(self.args, 'init'):
            print_header('Populating config.json')
            commands = []
            for config_prop, value in [
                ('cli/default_address', rest_url),
                ('server/engine_url', mysql_url),
                ('server/rest_host', '0.0.0.0'),
                ('server/admin_email', self.args.admin_email),
                ('email/host', self.args.email_host),
                ('email/username', self.args.email_username),
                ('email/password', self.args.email_password),
            ]:
                if value:
                    commands.append('cl config {} {}'.format(config_prop, value))
            self.run_service_cmd(' && '.join(commands))

            print_header('Creating root user')
            self.run_service_cmd(
                wait_mysql(
                    'python scripts/create-root-user.py {}'.format(self.args.codalab_password)
                )
            )

            print_header('Initializing/migrating the database with alembic')
            # The first time, we need to stamp; after that upgrade.
            self.run_service_cmd(
                'if [ $(alembic current | wc -l) -gt 0 ]; then echo upgrade; alembic upgrade head; else echo stamp; alembic stamp head; fi'
            )

        self.bring_up_service('rest-server')

        if should_run_service(self.args, 'init'):
            print_header('Creating home and dashboard worksheets')
            self.run_service_cmd(
                wait_rest_server(
                    'cl logout && cl status && ((cl new home && cl new dashboard) || exit 0)'
                )
            )

        self.bring_up_service('bundle-manager')
        self.bring_up_service('frontend')
        self.bring_up_service('nginx')
        self.bring_up_service('worker')

        if should_run_service(self.args, 'test'):
            print_header('Running tests')
            self.run_service_cmd(
                wait_rest_server('python test_cli.py --instance {} default'.format(rest_url))
            )

        self.bring_up_service('monitor')

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
                'login -u %s -p %s' % (self.args.docker_username, self.args.docker_password)
            )
            for image in images_to_build:
                self.push_image(image)


if __name__ == '__main__':
    main()
