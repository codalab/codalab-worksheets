import argparse
import os
import random
import string
import sys
import time

from abc import ABC, abstractmethod
from multiprocessing import Process
from threading import Thread

from test_cli import run_command

'''
TODO:
[] ssh into dev environment and run python stress_test.py
[X] Upload a huge bundle (20 GB?)
[X] Upload many (10000) small bundles
[X] Many (1000) runs which are trivial commands (e.g., date) in parallel
[X] Run that writes infinitely to the disk
[X] Test many worksheet copies.
[X] Many runs that all use up plenty of disk (1GB each) - test that the local dependency cleanup of workers is working
[X] Run that writes to use infinite memory
[X] Run that tries to use up all the GPUs (+ memory)
[X] Many runs that use all sorts of docker images (choose 100 big docker images)
[X] Call CodaLab repeatedly to get information / load worksheets to make sure it's still responsive
'''

# TODO: need to find more large docker images to populate this list
# List of large docker images
_LARGE_DOCKER_IMAGES = ['iwane/numpy-matplotlib', 'adreeve/python-numpy', 'openjdk:11.0.5-jre']


class TestFile:
    """
    Wrapper around temporary file. Creates the file at initialization.

    Args:
        file_name (str): Name of the file to create and manage
        size_mb (int): Size of the test file in megabytes
        content (str): Content to write out to the file. Default is None.
    """

    def __init__(self, file_name, size_mb=1, content=None):
        self._file_name = file_name
        if content == None:
            self._size_mb = size_mb
            self._make_random_file()
        else:
            self._write_content(content)

    def _make_random_file(self):
        with open(self._file_name, 'wb') as file:
            file.seek(self._size_mb * 1024)  # seek takes in file size in terms of bytes
            file.write(b'0')
        print('Created file {} of size {} MB.'.format(self._file_name, self._size_mb))

    def _write_content(self, content):
        with open(self._file_name, 'w') as file:
            file.write(content)

    def name(self):
        return self._file_name

    def delete(self):
        '''
        Removes the file.
        '''
        if os.path.exists(self._file_name):
            os.remove(self._file_name)
            print('Deleted file {}.'.format(self._file_name))
        else:
            print('File {} has already been deleted.'.format(self._file_name))


class StressTestRunner(ABC):
    """
    Abstract class that holds common logic for stress test runners.

    Args:
        cl: cl instance used to run CodaLab commands
    """

    def __init__(self, cl):
        self._cl = cl

    def run(self):
        print('Running stress tests...')
        self._start_heartbeat()
        self._test_large_bundle()
        self._test_many_bundle_uploads()
        self._test_many_worksheet_copies()
        self._test_parallel_runs()
        self._test_many_docker_runs()
        self._test_infinite_memory()
        self._test_infinite_gpu()
        self._test_infinite_disk()
        self._test_many_disk_writes()
        print('Done.')

    def _start_heartbeat(self):
        # Start heartbeats in the background. Each heartbeat creates a worksheet and prints its content.
        t = Thread(target=self._heartbeat)
        t.daemon = True
        t.start()

    def _heartbeat(self):
        while True:
            # Create a worksheet in a separate thread and check if it times out or not.
            worksheet_name = 'heartbeat_worksheet' + self._generate_random_id()
            p = Process(
                target=StressTestRunner._create_and_output_worksheet,
                args=(self._cl, worksheet_name),
            )
            p.start()
            p.join(timeout=10)
            if p.is_alive():
                print('Heartbeat failed. Exiting...')
                sys.exit(1)
            # Have heartbeat run every second
            time.sleep(1)

    def _test_large_bundle(self):
        self._set_worksheet('test_large_bundles')
        large_file = TestFile('large_file', self.get_large_file_size_gb() * 1000)
        run_command([self._cl, 'upload', large_file.name()])
        large_file.delete()

    def _test_many_bundle_uploads(self):
        self._set_worksheet('test_many_bundle_uploads')
        file = TestFile('small_file', 1)
        for _ in range(self.get_bundle_uploads_count()):
            run_command([self._cl, 'upload', file.name()])
        file.delete()

    def _test_many_worksheet_copies(self):
        # Initialize a worksheet with 10 bundles to be replicated
        worksheet_uuid = self._set_worksheet('test_many_worksheet_copies')
        file = TestFile('copy_file', 1)
        for _ in range(10):
            run_command([self._cl, 'upload', file.name()])
        file.delete()

        # Create many worksheets with current worksheet's content copied over
        for _ in range(self.get_create_worksheets_count()):
            other_worksheet_uuid = run_command([self._cl, 'new', self._generate_random_id()])
            run_command([self._cl, 'wadd', worksheet_uuid, other_worksheet_uuid])

    def _test_parallel_runs(self):
        self._set_worksheet('test_parallel_runs')
        processes = []
        for _ in range(self.get_parallel_runs_count()):
            p = Process(target=StressTestRunner._simple_run, args=(self._cl,))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

    def _test_many_docker_runs(self):
        self._set_worksheet('test_many_docker_runs')
        for _ in range(self.get_num_of_docker_rounds()):
            for image in _LARGE_DOCKER_IMAGES:
                run_command(
                    [
                        self._cl,
                        'run',
                        'echo building {}...'.format(image),
                        '--request-docker-image',
                        image,
                    ]
                )

    def _test_infinite_memory(self):
        if not self.should_test_infinite_memory():
            return
        self._set_worksheet('test_infinite_memory')
        file = self._create_infinite_memory_script()
        run_command([self._cl, 'upload', file.name()])
        run_command([self._cl, 'run', ':' + file.name(), 'python ' + file.name()])
        file.delete()

    def _test_infinite_gpu(self):
        if not self.should_test_infinite_gpu():
            return
        self._set_worksheet('test_infinite_gpu')
        file = self._create_infinite_memory_script()
        run_command([self._cl, 'upload', file.name()])
        for _ in range(self.get_infinite_gpu_run_count()):
            run_command(
                [self._cl, 'run', ':' + file.name(), 'python ' + file.name(), '--request-gpus=1']
            )
        file.delete()

    def _create_infinite_memory_script(self):
        code = 'a=["codalab stress test memory"]\nwhile True: a.extend(a); print(a)'
        return TestFile('stress_memory.py', content=code)

    def _test_infinite_disk(self):
        if not self.should_test_infinite_disk():
            return
        self._set_worksheet('test_infinite_disk')
        # Infinitely write out random characters to disk
        run_command([self._cl, 'run', 'dd if=/dev/zero of=1g.bin bs=1G;'])
        run_command([self._cl, 'run', 'dd if=/dev/urandom of=/dev/sda;'])

    def _test_many_disk_writes(self):
        self._set_worksheet('test_many_disk_writes')
        for _ in range(self.get_disk_write_count()):
            # Write out 1 GB worth of bytes out to disk
            run_command([self._cl, 'run', 'dd if=/dev/zero of=1g.bin bs=1G count=1;'])

    def _set_worksheet(self, run_name):
        worksheet_name = '{}_worksheet{}'.format(run_name, self._generate_random_id())
        uuid = run_command([self._cl, 'new', worksheet_name])
        run_command([self._cl, 'work', worksheet_name])
        return uuid

    def _generate_random_id(self):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(24)
        )

    @abstractmethod
    def get_large_file_size_gb(self):
        pass

    @abstractmethod
    def get_bundle_uploads_count(self):
        pass

    @abstractmethod
    def get_create_worksheets_count(self):
        pass

    @abstractmethod
    def get_parallel_runs_count(self):
        pass

    @abstractmethod
    def get_num_of_docker_rounds(self):
        pass

    @abstractmethod
    def should_test_infinite_memory(self):
        pass

    @abstractmethod
    def should_test_infinite_disk(self):
        pass

    @abstractmethod
    def should_test_infinite_gpu(self):
        pass

    @abstractmethod
    def get_infinite_gpu_run_count(self):
        pass

    @abstractmethod
    def get_disk_write_count(self):
        pass

    @staticmethod
    def _simple_run(cl):
        run_command([cl, 'run', 'echo stress testing...'])

    @staticmethod
    def _create_and_output_worksheet(cl, name):
        run_command([cl, 'new', name])
        run_command([cl, 'print', name])


class LightStressTestRunner(StressTestRunner):
    def __init__(self, cl):
        self._cl = cl
        super().__init__(cl)

    def get_large_file_size_gb(self):
        return 1

    def get_bundle_uploads_count(self):
        return 1

    def get_create_worksheets_count(self):
        return 2

    def get_parallel_runs_count(self):
        return 1

    def get_num_of_docker_rounds(self):
        return 1

    def should_test_infinite_memory(self):
        return False

    def should_test_infinite_disk(self):
        return False

    def should_test_infinite_gpu(self):
        return False

    def get_infinite_gpu_run_count(self):
        return 1

    def get_disk_write_count(self):
        return 1


class HeavyStressTestRunner(StressTestRunner):
    def __init__(self, cl):
        self._cl = cl
        super().__init__(cl)

    def get_large_file_size_gb(self):
        return 20

    def get_bundle_uploads_count(self):
        return 10000

    def get_create_worksheets_count(self):
        return 10000

    def get_parallel_runs_count(self):
        return 1000

    def get_num_of_docker_rounds(self):
        return 1000

    def should_test_infinite_memory(self):
        return True

    def should_test_infinite_disk(self):
        return True

    def should_test_infinite_gpu(self):
        return True

    def get_infinite_gpu_run_count(self):
        return 1000

    def get_disk_write_count(self):
        return 1000


class StressTestRunnerFactory:
    @staticmethod
    def create(args, cl):
        """
        Creates an instance of StressTestRunner based on command line arguments passed in.
        :param args: Dictionary of command line arguments and their values
        :param cl: cl instance used to run CodaLab commands
        :return: An instance of StressTestRunner.
        """
        if args.light:
            print('Created a LightStressTestRunner...')
            return LightStressTestRunner(cl)
        else:
            print('Created a HeavyStressTestRunner...')
            return HeavyStressTestRunner(cl)


def main():
    runner = StressTestRunnerFactory.create(args, cl)
    runner.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs the specified CodaLab stress tests against the specified CodaLab instance (defaults to localhost)'
    )
    parser.add_argument(
        '--cl-executable',
        type=str,
        help='Path to codalab CLI executable, defaults to "cl"',
        default='cl',
    )
    parser.add_argument('--light', action='store_true')

    args = parser.parse_args()
    cl = args.cl_executable
    main()
