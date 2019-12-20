import argparse
import os
import random
import string
import sys
import time

from abc import ABC, abstractmethod
from enum import Enum
from multiprocessing import cpu_count, Process, Pool
from threading import Thread

from test_cli import run_command

"""
Script to stress test CodaLab's backend. The following is a list of what's being tested:
- Large bundle upload
- Many small bundle uploads
- Many worksheet creations and copies
- Many small runs (trivial commands) in parallel
- Run that writes infinitely to disk
- Many runs that all use up plenty of disk (1GB each)
- Run that uses infinite memory
- Run that tries to use up all the GPUs (+ memory)
- Many runs that use all sorts of large Docker images
- Call CodaLab repeatedly to get information to check if the system is still responsive
"""

# List of random, large docker images
_LARGE_DOCKER_IMAGES = [
    'adreeve/python-numpy',
    'larger/rdp:dev',
    'openjdk:11.0.5-jre',
    'tensorflow/tensorflow:devel-gpu',
    'iwane/numpy-matplotlib',
    'aaronyalai/openaibaselines:gym3',
    'mysql:latest',
    'couchbase:latest',
    'large64/docker-test:ruby',
    'pytorch/pytorch:1.3-cuda10.1-cudnn7-devel',
]


class StressTestArg(Enum):
    CL_EXECUTABLE = 'cl_executable'
    HEAVY = 'heavy'
    BYPASS_CLEAN_UP = 'bypass_clean_up'
    LARGE_FILE_SIZE_GB = 'large_file_size_gb'
    BUNDLE_UPLOAD_COUNT = 'bundle_upload_count'
    CREATE_WORKSHEET_COUNT = 'create_worksheet_count'
    PARALLEL_RUNS_COUNT = 'parallel_runs_count'
    LARGE_DOCKER_RUNS_COUNT = 'large_docker_runs_count'
    TEST_INFINITE_MEMORY = 'test_infinite_memory'
    TEST_INFINITE_DISK = 'test_infinite_disk'
    TEST_INFINITE_GPU = 'test_infinite_gpu'
    INFINITE_GPU_RUNS_COUNT = 'infinite_gpu_runs_count'
    LARGE_DISK_WRITE_COUNT = 'large_disk_write_count'

    def get_cla_form(self):
        return '--' + self.value.replace('_', '-')


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
            file.seek(self._size_mb * 1024 * 1024)  # seek takes in file size in terms of bytes
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

    def __init__(self, cl, args):
        self._cl = cl
        self._args = vars(args)
        self._worksheets = []
        self._bundles = []

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
        self._cleanup()
        print('Done.')

    def get_arg_val(self, argname, default):
        if argname in self._args and self._args.get(argname) is not None:
            return self._args.get(argname)
        else:
            return default

    def _start_heartbeat(self):
        # Start heartbeats in the background. Each heartbeat creates a worksheet and prints its content.
        t = Thread(target=self._heartbeat)
        t.daemon = True
        t.start()

    def _heartbeat(self):
        while True:
            # Run a search in a separate thread and check if it times out or not.
            p = Process(target=StressTestRunner._search_failed_runs, args=(self._cl,))
            p.start()
            p.join(timeout=10)
            if p.is_alive():
                print('Heartbeat failed. Exiting...')
                sys.exit(1)
            # Have heartbeat run every 5 seconds
            time.sleep(5)

    def _test_large_bundle(self):
        self._set_worksheet('large_bundles')
        large_file = TestFile('large_file', self.get_large_file_size_gb() * 1000)
        self._run_bundle([self._cl, 'upload', large_file.name()])
        large_file.delete()

    def _test_many_bundle_uploads(self):
        self._set_worksheet('many_bundle_uploads')
        file = TestFile('small_file', 1)
        for _ in range(self.get_bundle_uploads_count()):
            self._run_bundle([self._cl, 'upload', file.name()])
        file.delete()

    def _test_many_worksheet_copies(self):
        # Initialize a worksheet with 10 bundles to be replicated
        worksheet_uuid = self._set_worksheet('many_worksheet_copies')
        file = TestFile('copy_file', 1)
        for _ in range(10):
            self._run_bundle([self._cl, 'upload', file.name()])
        file.delete()

        # Create many worksheets with current worksheet's content copied over
        for _ in range(self.get_create_worksheets_count()):
            worksheet_name = self._create_worksheet_name('other_worksheet_copy')
            other_worksheet_uuid = run_command([self._cl, 'new', worksheet_name])
            self._worksheets.append(other_worksheet_uuid)
            run_command([self._cl, 'wadd', worksheet_uuid, other_worksheet_uuid])

    def _test_parallel_runs(self):
        self._set_worksheet('parallel_runs')
        pool = Pool(cpu_count())
        for _ in range(self.get_parallel_runs_count()):
            pool.apply(StressTestRunner._simple_run, (self._cl, self._bundles))
        pool.close()

    def _test_many_docker_runs(self):
        self._set_worksheet('many_docker_runs')
        for _ in range(self.get_num_of_docker_rounds()):
            for image in _LARGE_DOCKER_IMAGES:
                self._run_bundle(
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
        self._set_worksheet('infinite_memory')
        file = self._create_infinite_memory_script()
        self._run_bundle([self._cl, 'upload', file.name()])
        self._run_bundle([self._cl, 'run', ':' + file.name(), 'python ' + file.name()])
        file.delete()

    def _test_infinite_gpu(self):
        if not self.should_test_infinite_gpu():
            return
        self._set_worksheet('infinite_gpu')
        file = self._create_infinite_memory_script()
        self._run_bundle([self._cl, 'upload', file.name()])
        for _ in range(self.get_infinite_gpu_run_count()):
            self._run_bundle(
                [self._cl, 'run', ':' + file.name(), 'python ' + file.name(), '--request-gpus=1']
            )
        file.delete()

    def _create_infinite_memory_script(self):
        code = 'a=["codalab stress test memory"]\nwhile True: a.extend(a); print(a)'
        return TestFile('stress_memory.py', content=code)

    def _test_infinite_disk(self):
        if not self.should_test_infinite_disk():
            return
        self._set_worksheet('infinite_disk')
        # Infinitely write out random characters to disk
        self._run_bundle([self._cl, 'run', 'dd if=/dev/zero of=1g.bin bs=1G;'])
        self._run_bundle([self._cl, 'run', 'dd if=/dev/urandom of=/dev/sda;'])

    def _test_many_disk_writes(self):
        self._set_worksheet('many_disk_writes')
        for _ in range(self.get_disk_write_count()):
            # Write out 1 GB worth of bytes out to disk
            self._run_bundle([self._cl, 'run', 'dd if=/dev/zero of=1g.bin bs=1G count=1;'])

    def _set_worksheet(self, run_name):
        worksheet_name = self._create_worksheet_name(run_name)
        uuid = run_command([self._cl, 'new', worksheet_name])
        run_command([self._cl, 'work', worksheet_name])
        self._worksheets.append(uuid)
        return uuid

    def _create_worksheet_name(self, base_name):
        return 'stress_test_{}_ws_{}'.format(base_name, self._generate_random_id())

    def _generate_random_id(self):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(24)
        )

    def _run_bundle(self, args):
        uuid = run_command(args)
        self._bundles.append(uuid)
        return uuid

    def _cleanup(self):
        if self.get_arg_val(StressTestArg.BYPASS_CLEAN_UP.value, False):
            return
        print('Cleaning up...')
        for uuid in self._bundles:
            # wait until the bundle finishes and then delete it
            print(run_command([self._cl, 'wait', uuid]))
            print(run_command([self._cl, 'rm', uuid, '--force']))
        for uuid in self._worksheets:
            print(run_command([self._cl, 'wrm', uuid, '--force']))
        print(
            'Removed {} bundles and {} worksheets.'.format(
                len(self._bundles), len(self._worksheets)
            )
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
    def _simple_run(cl, bundles):
        bundles.append(run_command([cl, 'run', 'echo stress testing...']))

    @staticmethod
    def _search_failed_runs(cl):
        run_command([cl, 'search', 'state=failed', 'created=.sort-'])


class LightStressTestRunner(StressTestRunner):
    def __init__(self, cl, args):
        super().__init__(cl, args)

    def get_large_file_size_gb(self):
        return self.get_arg_val(StressTestArg.LARGE_FILE_SIZE_GB.value, 1)

    def get_bundle_uploads_count(self):
        return self.get_arg_val(StressTestArg.BUNDLE_UPLOAD_COUNT.value, 1)

    def get_create_worksheets_count(self):
        return self.get_arg_val(StressTestArg.CREATE_WORKSHEET_COUNT.value, 1)

    def get_parallel_runs_count(self):
        return self.get_arg_val(StressTestArg.PARALLEL_RUNS_COUNT.value, 4)

    def get_num_of_docker_rounds(self):
        return self.get_arg_val(StressTestArg.LARGE_DOCKER_RUNS_COUNT.value, 1)

    def should_test_infinite_memory(self):
        return self.get_arg_val(StressTestArg.TEST_INFINITE_MEMORY.value, False)

    def should_test_infinite_disk(self):
        return self.get_arg_val(StressTestArg.TEST_INFINITE_DISK.value, False)

    def should_test_infinite_gpu(self):
        return self.get_arg_val(StressTestArg.TEST_INFINITE_GPU.value, False)

    def get_infinite_gpu_run_count(self):
        return self.get_arg_val(StressTestArg.INFINITE_GPU_RUNS_COUNT.value, 0)

    def get_disk_write_count(self):
        return self.get_arg_val(StressTestArg.LARGE_DISK_WRITE_COUNT.value, 1)


class HeavyStressTestRunner(StressTestRunner):
    def __init__(self, cl, args):
        super().__init__(cl, args)

    def get_large_file_size_gb(self):
        return self.get_arg_val(StressTestArg.LARGE_FILE_SIZE_GB.value, 20)

    def get_bundle_uploads_count(self):
        return self.get_arg_val(StressTestArg.BUNDLE_UPLOAD_COUNT.value, 2000)

    def get_create_worksheets_count(self):
        return self.get_arg_val(StressTestArg.CREATE_WORKSHEET_COUNT.value, 2000)

    def get_parallel_runs_count(self):
        return self.get_arg_val(StressTestArg.PARALLEL_RUNS_COUNT.value, 1000)

    def get_num_of_docker_rounds(self):
        return self.get_arg_val(StressTestArg.LARGE_DOCKER_RUNS_COUNT.value, 1000)

    def should_test_infinite_memory(self):
        return self.get_arg_val(StressTestArg.TEST_INFINITE_MEMORY.value, True)

    def should_test_infinite_disk(self):
        return self.get_arg_val(StressTestArg.TEST_INFINITE_DISK.value, True)

    def should_test_infinite_gpu(self):
        return self.get_arg_val(StressTestArg.TEST_INFINITE_GPU.value, True)

    def get_infinite_gpu_run_count(self):
        return self.get_arg_val(StressTestArg.INFINITE_GPU_RUNS_COUNT.value, 1000)

    def get_disk_write_count(self):
        return self.get_arg_val(StressTestArg.LARGE_DISK_WRITE_COUNT.value, 1000)


def main():
    if args.heavy:
        print('Created a HeavyStressTestRunner...')
        runner = HeavyStressTestRunner(cl, args)
    else:
        print('Created a LightStressTestRunner...')
        runner = LightStressTestRunner(cl, args)

    # Run stress tests and time how long it takes to complete
    start_time = time.time()
    runner.run()
    duration_seconds = time.time() - start_time
    print("--- Completion Time: {} minutes---".format(duration_seconds / 60))


if __name__ == '__main__':

    def str_to_bool(val):
        if val.lower() == 'true':
            return True
        elif val.lower() == 'false':
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value of "true" or "false" expected.')

    parser = argparse.ArgumentParser(
        description='Runs the specified CodaLab stress tests against the specified CodaLab instance (defaults to localhost).'
    )
    parser.add_argument(
        StressTestArg.CL_EXECUTABLE.get_cla_form(),
        type=str,
        help='Path to codalab CLI executable. The default is "cl".',
        default='cl',
    )
    parser.add_argument(
        StressTestArg.HEAVY.get_cla_form(),
        action='store_true',
        help='Runs a heavy version of the stress tests. The default is false.',
    )

    parser.add_argument(
        StressTestArg.BYPASS_CLEAN_UP.get_cla_form(),
        action='store_true',
        help='Bypasses clean up of all the worksheets and bundles post-stress testing. The default is false.',
    )

    # Custom stress test runner arguments
    parser.add_argument(
        StressTestArg.LARGE_FILE_SIZE_GB.get_cla_form(),
        type=int,
        help='Override size of large file in GB for single upload.',
    )
    parser.add_argument(
        StressTestArg.BUNDLE_UPLOAD_COUNT.get_cla_form(),
        type=int,
        help='Override number of small bundles to upload.',
    )
    parser.add_argument(
        StressTestArg.CREATE_WORKSHEET_COUNT.get_cla_form(),
        type=int,
        help='Override number of worksheets to create.',
    )
    parser.add_argument(
        StressTestArg.PARALLEL_RUNS_COUNT.get_cla_form(),
        type=int,
        help='Override number of small, parallel runs.',
    )
    parser.add_argument(
        StressTestArg.LARGE_DOCKER_RUNS_COUNT.get_cla_form(),
        type=int,
        help='Override number of runs with large docker images.',
    )
    parser.add_argument(
        StressTestArg.TEST_INFINITE_MEMORY.get_cla_form(),
        type=str_to_bool,
        help='Override whether infinite memory stress test is run by passing in "true" or "false".',
    )
    parser.add_argument(
        StressTestArg.TEST_INFINITE_DISK.get_cla_form(),
        type=str_to_bool,
        help='Override whether infinite disk write test is run by passing in "true" or "false".',
    )
    parser.add_argument(
        StressTestArg.TEST_INFINITE_GPU.get_cla_form(),
        type=str_to_bool,
        help='Override whether infinite gpu usage test is run by passing in "true" or "false".',
    )
    parser.add_argument(
        StressTestArg.INFINITE_GPU_RUNS_COUNT.get_cla_form(),
        type=int,
        help='Override number of infinite gpu runs.',
    )
    parser.add_argument(
        StressTestArg.LARGE_DISK_WRITE_COUNT.get_cla_form(),
        type=int,
        help='Override number of runs with 1 GB disk writes.',
    )

    # Parse args and run this script
    args = parser.parse_args()
    cl = args.cl_executable
    main()
