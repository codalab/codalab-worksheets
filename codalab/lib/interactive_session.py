from codalab.lib.cli_util import parse_key_target
from codalab.lib.spec_util import generate_uuid
from codalab.lib.codalab_manager import CodaLabManager

import argparse
import os
import docker
import shutil
import sys


"""
TODO: delete later

DependencyManager logic

- connect_to_codalab_server
    - args.server
    - args.password_file
    - returns bundle_service
- DependencyManager
    - os.path.join(args.work_dir, 'dependencies-state.json')
    - bundle_service
    - args.work_dir
    - args.max_work_dir_size
"""


class InteractiveSession:
    _BASH_HISTORY_CONTAINER_PATH = '/root/.bash_history'
    _CHOOSE_COMMAND_INSTRUCTIONS = (
        "\n\n@@\n"
        "@@ Choose the commands to use for cl run:\n"
        "@@\n"
        "@@ 1. Delete the lines of any unwanted commands.\n"
        "@@ 2. Add any additional commands on a new line above these instructions.\n"
        "@@\n"
    )
    _INSTRUCTIONS_DELIMITER = '@@'
    _MAX_SESSION_TIMEOUT = 60 * 60  # 1 hour in seconds
    _NULL_BYTE = '\x00'

    def __init__(
        self, docker_image, manager=None, targets=[], stdout=sys.stdout, stderr=sys.stderr
    ):
        # Instantiate a CodaLabManager if one is not passed in
        self._manager = manager if manager else CodaLabManager()
        self._docker_image = docker_image
        self._targets = targets

        self._docker_client = docker.from_env(timeout=InteractiveSession._MAX_SESSION_TIMEOUT)
        self._session_uuid = generate_uuid()
        self._stdout = stdout
        self._stderr = stderr

    def start(self):
        self._bundle_path = os.path.join(
            self._manager.codalab_home, 'local_bundles', self._session_uuid
        )
        os.makedirs(self._bundle_path)

        # Mount dependencies on to the session container
        target_specs = [parse_key_target(spec) for spec in self._targets]
        dependencies = [
            ('{}'.format(key), '/{}_dependencies/{}'.format(self._session_uuid, key))
            for key, _ in target_specs
        ]
        # TODO: remove later -tony
        print(
            'self._targets: {}\ntarget_specs:{}\ndependencies:{}\n'.format(
                self._targets, target_specs, dependencies
            ),
            file=self._stdout,
        )
        # TODO: handle dependencies here -tony
        # e.g. -v local_path/some_folder:/0x707e903500e54bcf9b072ac7e3f5ed36_dependencies/foo

        print('\nStarting an interactive session...\n', file=self._stdout)
        print('=' * 150, file=self._stdout)
        print('Session UUID: ', self._session_uuid, file=self._stdout)
        print('CodaLab instance: ', self._manager.current_client().address, file=self._stdout)
        print('Container name: ', self._get_container_name(), file=self._stdout)
        print('Container Docker image: ', self._docker_image, file=self._stdout)
        print('You can find local bundle contents at: ', self._bundle_path, file=self._stdout)
        print('=' * 150 + '\n', file=self._stdout)

        self._container = self._start_session()
        return self._construct_final_command()

    def cleanup(self):
        print('\nCleaning up the session...', file=self._stdout)
        self._container.stop()
        self._container.remove()
        shutil.rmtree(self._bundle_path, ignore_errors=True)
        print('Done.\n\n', file=self._stdout)

    def _start_session(self):
        # Create a Docker container for this interactive session
        name = self._get_container_name()
        command = (
            'docker run '
            '-it '
            f'--name {name} '
            f'-w /{self._session_uuid} '
            f'{self._docker_image} '
            'bash'
        )
        os.system(command)
        containers = self._docker_client.containers.list(all=True, filters={'name': name})
        if len(containers) == 0:
            raise RuntimeError('Could not find interactive session container with name: %s' % name)
        return containers[0]

    def _get_container_name(self):
        return 'interactive-session-%s' % self._session_uuid

    def _construct_final_command(self):
        try:
            candidate_commands = self._get_bash_history()
        except:
            print(
                'The history of bash commands could not be retrieved at path: %s'
                % InteractiveSession._BASH_HISTORY_CONTAINER_PATH,
                file=self._stderr,
            )
            return ''

        # Write out commands to choose from plus the instructions out to a file
        path = os.path.join(self._bundle_path, 'edit_commands.txt')
        with open(path, 'w') as f:
            for command in candidate_commands:
                f.write(command)
            f.write(InteractiveSession._CHOOSE_COMMAND_INSTRUCTIONS)

        # Use vi to allow users to choose commands
        os.system('vi %s' % path)

        # Extract out final commands minus the instructions
        commands = []
        for line in open(path).read().splitlines():
            command = line.lstrip().rstrip()
            if command and not command.startswith(InteractiveSession._INSTRUCTIONS_DELIMITER):
                commands.append(command)

        final_command = '&&'.join(commands)
        print('\nFinal constructed command:\n%s' % final_command, file=self._stdout)
        return final_command

    def _get_bash_history(self):
        # Copies .bash_history from the container to a new file .bash_history in bundle_path
        path = os.path.join(self._bundle_path, '.bash_history')
        f = open(path, 'wb')
        stream, _ = self._container.get_archive(InteractiveSession._BASH_HISTORY_CONTAINER_PATH)
        for chunk in stream:
            f.write(chunk)
        f.close()

        # Extract a list of commands from the .bash_history file that was copied out of the container
        commands = []
        with open(path) as f:
            for i, line in enumerate(f):
                command = (
                    line.rstrip(InteractiveSession._NULL_BYTE)
                    if i > 0
                    else line.split(InteractiveSession._NULL_BYTE)[-1]
                )
                if command:
                    commands.append(command)
        return commands


def main():
    # Example usage of InteractiveSession:
    session = InteractiveSession(args.docker_image)
    session.start()
    session.cleanup()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs the specified CodaLab stress tests against the specified CodaLab instance (defaults to localhost).'
    )
    parser.add_argument(
        '--docker-image',
        type=str,
        help='Docker image used to create a container for the interactive session.',
        default='codalab/default-cpu:latest',
    )

    # Parse args and run this script
    args = parser.parse_args()
    main()
