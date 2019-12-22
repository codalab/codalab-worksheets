import argparse
import os
import random
import string
import sys
import time

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


class StressTestArg(Enum):
    CL_EXECUTABLE = 'cl_executable'
    HEAVY = 'heavy'
    CLEANUP_ONLY = 'cleanup_only'
    BYPASS_CLEANUP = 'bypass_cleanup'
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
            file.seek(self._size_mb * 1024 * 1024)  # Seek takes in file size in terms of bytes
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


class StressTestRunner:
    """
    Abstract class that holds common logic for stress test runners.

    Args:
        cl: cl instance used to run CodaLab commands
        args: command line arguments
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
    _TAG = 'codalab-stress-test'

    def __init__(self, cl, args):
        self._cl = cl
        self._args = args

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
        self.cleanup()
        print('Done.')

    def _start_heartbeat(self):
        # Start heartbeats in the background. Each heartbeat creates a worksheet and prints its content.
        t = Thread(target=self._heartbeat)
        t.daemon = True
        t.start()

    def _heartbeat(self):
        while True:
            # Run a search in a separate thread and check if it times out or not.
            t = Thread(target=StressTestRunner._search_failed_runs, args=(self._cl,))
            t.start()
            t.join(timeout=10)
            if t.is_alive():
                print('Heartbeat failed. Exiting...')
                sys.exit(1)
            # Have heartbeat run every 5 seconds
            time.sleep(5)

    def _test_large_bundle(self):
        self._set_worksheet('large_bundles')
        large_file = TestFile('large_file', self._get_large_file_size_gb() * 1000)
        self._run_bundle([self._cl, 'upload', large_file.name()])
        large_file.delete()

    def _test_many_bundle_uploads(self):
        self._set_worksheet('many_bundle_uploads')
        file = TestFile('small_file', 1)
        for _ in range(self._get_bundle_uploads_count()):
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
        for _ in range(self._get_create_worksheets_count()):
            other_worksheet_uuid = self._set_worksheet('other_worksheet_copy')
            run_command(
                [self._cl, 'wadd', worksheet_uuid, other_worksheet_uuid], force_subprocess=True
            )

    def _test_parallel_runs(self):
        self._set_worksheet('parallel_runs')
        pool = Pool(cpu_count())
        for _ in range(self._get_parallel_runs_count()):
            pool.apply(StressTestRunner._simple_run, (self._cl,))
        pool.close()

    def _test_many_docker_runs(self):
        self._set_worksheet('many_docker_runs')
        for _ in range(self._get_num_of_docker_rounds()):
            for image in StressTestRunner._LARGE_DOCKER_IMAGES:
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
        if not self._should_test_infinite_memory():
            return
        self._set_worksheet('infinite_memory')
        file = self._create_infinite_memory_script()
        self._run_bundle([self._cl, 'upload', file.name()])
        self._run_bundle([self._cl, 'run', ':' + file.name(), 'python ' + file.name()])
        file.delete()

    def _test_infinite_gpu(self):
        if not self._should_test_infinite_gpu():
            return
        self._set_worksheet('infinite_gpu')
        file = self._create_infinite_memory_script()
        self._run_bundle([self._cl, 'upload', file.name()])
        for _ in range(self._get_infinite_gpu_run_count()):
            self._run_bundle(
                [self._cl, 'run', ':' + file.name(), 'python ' + file.name(), '--request-gpus=1']
            )
        file.delete()

    def _create_infinite_memory_script(self):
        code = 'a=["codalab stress test memory"]\nwhile True: a.extend(a); print(a)'
        return TestFile('stress_memory.py', content=code)

    def _test_infinite_disk(self):
        if not self._should_test_infinite_disk():
            return
        self._set_worksheet('infinite_disk')
        # Infinitely write out random characters to disk
        self._run_bundle([self._cl, 'run', 'dd if=/dev/zero of=1g.bin bs=1G;'])
        self._run_bundle([self._cl, 'run', 'dd if=/dev/urandom of=/dev/sda;'])

    def _test_many_disk_writes(self):
        self._set_worksheet('many_disk_writes')
        for _ in range(self._get_disk_write_count()):
            # Write out 1 GB worth of bytes out to disk
            self._run_bundle([self._cl, 'run', 'dd if=/dev/zero of=1g.bin bs=1G count=1;'])

    def _set_worksheet(self, run_name):
        worksheet_name = self._create_worksheet_name(run_name)
        uuid = run_command([self._cl, 'new', worksheet_name], force_subprocess=True)
        run_command([self._cl, 'work', worksheet_name], force_subprocess=True)
        run_command([self._cl, 'wedit', '--tag=%s' % StressTestRunner._TAG], force_subprocess=True)
        return uuid

    def _create_worksheet_name(self, base_name):
        return 'stress_test_{}_ws_{}'.format(base_name, self._generate_random_id())

    def _generate_random_id(self):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(24)
        )

    def _run_bundle(self, args):
        args.append('--tags=%s' % StressTestRunner._TAG)
        return run_command(args, force_subprocess=True)

    def cleanup(self):
        if self._should_bypass_cleanup():
            return
        print('Cleaning up...')
        bundles = run_command(
            [self._cl, 'search', 'tags=%s' % StressTestRunner._TAG, '.limit=10000', '--uuid-only'],
            force_subprocess=True,
        ).split('\n')
        worksheets = run_command(
            [self._cl, 'wsearch', 'tag=%s' % StressTestRunner._TAG, '.limit=1000', '--uuid-only'],
            force_subprocess=True,
        ).split('\n')
        for uuid in bundles:
            if len(uuid) == 0:
                continue
            # Wait until the bundle finishes and then delete it
            run_command([self._cl, 'wait', uuid], force_subprocess=True)
            run_command([self._cl, 'rm', uuid, '--force'], force_subprocess=True)
        for uuid in worksheets:
            if len(uuid) == 0:
                continue
            run_command([self._cl, 'wrm', uuid, '--force'], force_subprocess=True)
        print('Removed {} bundles and {} worksheets.'.format(len(bundles), len(worksheets)))

    def _should_bypass_cleanup(self):
        return self._args[StressTestArg.BYPASS_CLEANUP.value]

    def _get_large_file_size_gb(self):
        return self._args[StressTestArg.LARGE_FILE_SIZE_GB.value]

    def _get_bundle_uploads_count(self):
        return self._args[StressTestArg.BUNDLE_UPLOAD_COUNT.value]

    def _get_create_worksheets_count(self):
        return self._args[StressTestArg.CREATE_WORKSHEET_COUNT.value]

    def _get_parallel_runs_count(self):
        return self._args[StressTestArg.PARALLEL_RUNS_COUNT.value]

    def _get_num_of_docker_rounds(self):
        return self._args[StressTestArg.LARGE_DOCKER_RUNS_COUNT.value]

    def _should_test_infinite_memory(self):
        return self._args[StressTestArg.TEST_INFINITE_MEMORY.value]

    def _should_test_infinite_disk(self):
        return self._args[StressTestArg.TEST_INFINITE_DISK.value]

    def _should_test_infinite_gpu(self):
        return self._args[StressTestArg.TEST_INFINITE_GPU.value]

    def _get_infinite_gpu_run_count(self):
        return self._args[StressTestArg.INFINITE_GPU_RUNS_COUNT.value]

    def _get_disk_write_count(self):
        return self._args[StressTestArg.LARGE_DISK_WRITE_COUNT.value]

    @staticmethod
    def _simple_run(cl):
        run_command(
            [cl, 'run', 'echo stress testing...', '--tags=%s' % StressTestRunner._TAG],
            force_subprocess=True,
        )

    @staticmethod
    def _search_failed_runs(cl):
        run_command([cl, 'search', 'state=failed', 'created=.sort-'], force_subprocess=True)


def main():
    if args[StressTestArg.CLEANUP_ONLY.value]:
        runner = StressTestRunner(cl, args)
        runner.cleanup()
        return

    if args[StressTestArg.HEAVY.value]:
        print('Setting the heavy configuration...')
        args[StressTestArg.LARGE_FILE_SIZE_GB.value] = 20
        args[StressTestArg.BUNDLE_UPLOAD_COUNT.value] = 2000
        args[StressTestArg.CREATE_WORKSHEET_COUNT.value] = 2000
        args[StressTestArg.PARALLEL_RUNS_COUNT.value] = 1000
        args[StressTestArg.LARGE_DOCKER_RUNS_COUNT.value] = 1000
        args[StressTestArg.TEST_INFINITE_MEMORY.value] = True
        args[StressTestArg.TEST_INFINITE_DISK.value] = True
        args[StressTestArg.TEST_INFINITE_GPU.value] = True
        args[StressTestArg.INFINITE_GPU_RUNS_COUNT.value] = 1000
        args[StressTestArg.LARGE_DISK_WRITE_COUNT.value] = 1000
    print(args)

    # Run stress tests and time how long it takes to complete
    runner = StressTestRunner(cl, args)
    start_time = time.time()
    runner.run()
    duration_seconds = time.time() - start_time
    print("--- Completion Time: {} minutes---".format(duration_seconds / 60))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs the specified CodaLab stress tests against the specified CodaLab instance (defaults to localhost).'
    )
    parser.add_argument(
        StressTestArg.CL_EXECUTABLE.get_cla_form(),
        type=str,
        help='Path to Codalab CLI executable (defaults to "cl")',
        default='cl',
    )
    parser.add_argument(
        StressTestArg.HEAVY.get_cla_form(),
        action='store_true',
        help='Whether to run the heavy version of the stress tests (defaults to false)',
    )
    parser.add_argument(
        StressTestArg.CLEANUP_ONLY.get_cla_form(),
        action='store_true',
        help='Whether to just clean up bundles and worksheets from previous stress test runs (defaults to false)',
    )
    parser.add_argument(
        StressTestArg.BYPASS_CLEANUP.get_cla_form(),
        action='store_true',
        help='Whether to bypass clean up of all the worksheets and bundles post-stress testing (defaults to false)',
    )

    # Custom stress test runner arguments
    parser.add_argument(
        StressTestArg.LARGE_FILE_SIZE_GB.get_cla_form(),
        type=int,
        help='Size of large file in GB for single upload (defaults to 1)',
        default=0,
    )
    parser.add_argument(
        StressTestArg.BUNDLE_UPLOAD_COUNT.get_cla_form(),
        type=int,
        help='Number of small bundles to upload (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        StressTestArg.CREATE_WORKSHEET_COUNT.get_cla_form(),
        type=int,
        help='Number of worksheets to create (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        StressTestArg.PARALLEL_RUNS_COUNT.get_cla_form(),
        type=int,
        help='Number of small, parallel runs (defaults to 4)',
        default=4,
    )
    parser.add_argument(
        StressTestArg.LARGE_DOCKER_RUNS_COUNT.get_cla_form(),
        type=int,
        help='Number of runs with large docker images (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        StressTestArg.TEST_INFINITE_MEMORY.get_cla_form(),
        action='store_true',
        help='Whether infinite memory stress test is run (defaults to false)',
    )
    parser.add_argument(
        StressTestArg.TEST_INFINITE_DISK.get_cla_form(),
        action='store_true',
        help='Whether infinite disk write test is run (defaults to false)',
    )
    parser.add_argument(
        StressTestArg.TEST_INFINITE_GPU.get_cla_form(),
        action='store_true',
        help='Whether infinite gpu usage test is run (defaults to false)',
    )
    parser.add_argument(
        StressTestArg.INFINITE_GPU_RUNS_COUNT.get_cla_form(),
        type=int,
        help='Number of infinite gpu runs (defaults to 0)',
        default=0,
    )
    parser.add_argument(
        StressTestArg.LARGE_DISK_WRITE_COUNT.get_cla_form(),
        type=int,
        help='Number of runs with 1 GB disk writes (defaults to 1)',
        default=1,
    )

    # Parse args and run this script
    args = vars(parser.parse_args())
    cl = args[StressTestArg.CL_EXECUTABLE.value]
    main()
