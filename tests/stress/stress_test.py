import argparse
import os
import random
import string
import subprocess
import sys
import time

from multiprocessing import cpu_count, Pool
from threading import Thread

from scripts.test_util import cleanup, run_command

import os

def temp_path(file_name):
    root = '/tmp'
    return os.path.join(root, file_name)

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
        self._file_path = temp_path(file_name)
        if content is None:
            self._size_mb = size_mb
            self._make_random_file()
        else:
            self._write_content(content)

    def _make_random_file(self):
        with open(self._file_path, 'wb') as file:
            file.seek(int(self._size_mb * 1024 * 1024))  # Seek takes in file size in terms of bytes
            file.write(b'0')
        print('Created file {} of size {} MB.'.format(self._file_path, self._size_mb))

    def _write_content(self, content):
        with open(self._file_path, 'w') as file:
            file.write(content)

    def name(self):
        return self._file_name
    
    def path(self):
        return self._file_path

    def delete(self):
        '''
        Removes the file.
        '''
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
            print('Deleted file {}.'.format(self._file_path))
        else:
            print('File {} has already been deleted.'.format(self._file_path))


class StressTestRunner:
    """
    This runner object holds the logic on how the stress tests are run based on the command line
    arguments passed in.

    Args:
        cl: cl instance used to run CodaLab commands
        args: command line arguments
    """

    # List of random, large Docker images in alphabetical order
    _LARGE_DOCKER_IMAGES = [
        'aaronyalai/openaibaselines:gym3',
        'adreeve/python-numpy',
        'couchbase:latest',
        'golang:latest',
        'iwane/numpy-matplotlib',
        'jenkins/jenkins',
        'larger/rdp:dev',
        'maven:latest',
        'mysql:latest',
        'mongo:latest',
        'neo4j:latest',
        'node:latest',
        'openjdk:latest',
        'perl:latest',
        'postgres:latest',
        'pytorch/pytorch:latest',
        'rails:latest',
        'solr:slim',
        'sonarqube:latest',
        'tensorflow/tensorflow:latest',
    ]

    def __init__(self, cl, args, tag='codalab-stress-test'):
        self._cl = cl
        self._args = args
        self._TAG = tag

        # Connect to the instance the stress tests will run on
        print('Connecting to instance %s...' % args.instance)
        subprocess.call([self._cl, 'work', '%s::' % args.instance])

    def run(self):
        print('Cleaning up stress test files from other runs...')
        cleanup(self._cl, self._TAG, should_wait=False)

        print('Running stress tests...')
        self._start_heartbeat()

        self._test_large_bundle_result()
        print('_test_large_bundle_result finished')
        self.cleanup()

        self._test_large_bundle_upload()
        print('_test_large_bundle_upload finished')
        self.cleanup()

        self._test_many_gpu_runs()
        print('_test_many_gpu_runs finished')
        self.cleanup()

        self._test_multiple_cpus_runs_count()
        print('_test_multiple_cpus_runs_count finished')
        self.cleanup()

        self._test_many_bundle_uploads()
        print('_test_many_bundle_uploads finished')
        self.cleanup()

        self._test_many_worksheet_copies()
        print('_test_many_worksheet_copies finished')
        self.cleanup()

        self._test_parallel_runs()
        print('_test_parallel_runs finished')
        self.cleanup()

        self._test_many_docker_runs()
        print('_test_many_docker_runs finished')
        self.cleanup()

        self._test_infinite_memory()
        print('_test_infinite_memory finished')
        self.cleanup()

        self._test_infinite_gpu()
        print('_test_infinite_gpu finished')
        self.cleanup()

        self._test_infinite_disk()
        print('_test_infinite_disk finished')
        self.cleanup()

        self._test_many_disk_writes()
        print('_test_many_disk_writes finished')
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
            t = Thread(target=StressTestRunner._heartbeat_cl_commands, args=(self._cl,))
            t.start()
            t.join(timeout=10)
            if t.is_alive():
                print('Heartbeat failed. Exiting...')
                sys.exit(1)
            # Have heartbeat run every 30 seconds
            time.sleep(30)

    def _test_large_bundle_result(self) -> None:
        def create_large_file_in_bundle(large_file_size_gb: int) -> TestFile:
            code: str = 'with open("largefile", "wb") as out:\n\tout.truncate({} * 1024 * 1024 * 1024)'.format(
                large_file_size_gb
            )
            return TestFile('large_dependency.py', content=code)

        self._set_worksheet('large_bundle_result')
        file: TestFile = create_large_file_in_bundle(self._args.large_dependency_size_gb)
        self._run_bundle([self._cl, 'upload', file.path()])
        file.delete()

        dependency_uuid: str = self._run_bundle(
            [self._cl, 'run', ':' + file.name(), 'python ' + file.name()]
        )
        uuid: str = self._run_bundle(
            [
                self._cl,
                'run',
                'large_bundle:{}'.format(dependency_uuid),
                'wc -c large_bundle/largefile',
            ]
        )
        # Wait for the run to finish before cleaning up the dependency
        run_command([cl, 'wait', uuid])

    def _test_large_bundle_upload(self) -> None:
        self._set_worksheet('large_bundle_upload')
        large_file: TestFile = TestFile('large_file', self._args.large_file_size_gb * 1000)
        dependency_uuid: str = self._run_bundle([self._cl, 'upload', large_file.path()])
        large_file.delete()
        uuid: str = self._run_bundle(
            [
                self._cl,
                'run',
                'large_dependency:{}'.format(dependency_uuid),
                'wc -c large_dependency',
            ]
        )
        # Wait for the run to finish before cleaning up the dependency
        run_command([cl, 'wait', uuid])

    def _test_many_gpu_runs(self):
        self._set_worksheet('many_gpu_runs')
        for _ in range(self._args.gpu_runs_count):
            self._run_bundle([self._cl, 'run', 'echo running with a gpu...', '--request-gpus=1'])

    def _test_multiple_cpus_runs_count(self):
        self._set_worksheet('multiple_cpus_requested_runs')
        for _ in range(args.multiple_cpus_runs_count):
            self._run_bundle([self._cl, 'run', 'sleep 30', '--request-cpus=4'])

    def _test_many_bundle_uploads(self):
        self._set_worksheet('many_bundle_uploads')
        file = TestFile('small_file', 1)
        for _ in range(self._args.bundle_upload_count):
            self._run_bundle([self._cl, 'upload', file.path()])
        file.delete()

    def _test_many_worksheet_copies(self):
        # Initialize a worksheet with 10 bundles to be replicated
        worksheet_uuid = self._set_worksheet('many_worksheet_copies')
        file = TestFile('copy_file', 1)
        for _ in range(10):
            self._run_bundle([self._cl, 'upload', file.path()])
        file.delete()

        # Create many worksheets with current worksheet's content copied over
        for _ in range(self._args.create_worksheet_count):
            other_worksheet_uuid = self._set_worksheet('other_worksheet_copy')
            run_command([self._cl, 'wadd', worksheet_uuid, other_worksheet_uuid])

    def _test_parallel_runs(self):
        self._set_worksheet('parallel_runs')
        pool = Pool(cpu_count())
        for _ in range(self._args.parallel_runs_count):
            pool.apply(StressTestRunner._simple_run, (self._cl,))
        pool.close()

    def _test_many_docker_runs(self):
        self._set_worksheet('many_docker_runs')
        for _ in range(self._args.large_docker_runs_count):
            # Pick a random image from the list of large Docker images to use for the run
            image = random.choice(StressTestRunner._LARGE_DOCKER_IMAGES)
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
        if not self._args.test_infinite_memory:
            return
        self._set_worksheet('infinite_memory')
        file = self._create_infinite_memory_script()
        self._run_bundle([self._cl, 'upload', file.path()])
        self._run_bundle(
            [self._cl, 'run', ':' + file.name(), 'python ' + file.name()], expected_exit_code=1
        )
        file.delete()

    def _create_infinite_memory_script(self):
        code = 'a=["codalab stress test memory"]\nwhile True: a.extend(a); print(a)'
        return TestFile('stress_memory.py', content=code)

    def _test_infinite_gpu(self):
        if not self._args.test_infinite_gpu:
            return
        self._set_worksheet('infinite_gpu')
        file = self._create_infinite_memory_script()
        self._run_bundle([self._cl, 'upload', file.path()])
        for _ in range(self._args.infinite_gpu_runs_count):
            self._run_bundle(
                [self._cl, 'run', ':' + file.name(), 'python ' + file.name(), '--request-gpus=1'],
                expected_exit_code=1,
            )
        file.delete()

    def _test_infinite_disk(self):
        if not self._args.test_infinite_disk:
            return
        self._set_worksheet('infinite_disk')
        # Infinitely write out random characters to disk
        self._run_bundle([self._cl, 'run', 'dd if=/dev/zero of=infinite.bin bs=1G;'])
        self._run_bundle([self._cl, 'run', 'dd if=/dev/urandom of=/dev/sda;'])

    def _test_many_disk_writes(self):
        self._set_worksheet('many_disk_writes')
        for i in range(self._args.large_disk_write_count):
            # Write out disk_write_bytes worth of bytes out to disk every iteration
            command = 'dd if=/dev/zero of=output{}.bin bs={} count=1;'.format(
                i, self._args.disk_write_size_bytes
            )
            self._run_bundle([self._cl, 'run', command])

    def _set_worksheet(self, run_name):
        worksheet_name = self._create_worksheet_name(run_name)
        uuid = run_command([self._cl, 'new', worksheet_name])
        run_command([self._cl, 'work', worksheet_name])
        run_command([self._cl, 'wedit', '--tag=%s' % self._TAG])
        return uuid

    def _create_worksheet_name(self, base_name):
        return 'stress_test_{}_ws_{}'.format(base_name, self._generate_random_id())

    def _generate_random_id(self):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(24)
        )

    def _run_bundle(self, args, expected_exit_code=0):
        args.append('--tags=%s' % self._TAG)
        return run_command(args, expected_exit_code)

    def cleanup(self):
        if self._args.bypass_cleanup:
            return
        cleanup(self._cl, self._TAG, not self._args.bypass_wait)

    @staticmethod
    def _simple_run(cl, tag='codalab-stress-test'):
        run_command([cl, 'run', 'echo stress testing...', '--tags=%s' % tag])

    @staticmethod
    def _heartbeat_cl_commands(cl):
        run_command([cl, 'search', 'state=failed', 'created=.sort-'])
        run_command([cl, 'workers'])


def main():
    if args.cleanup_only:
        runner = StressTestRunner(cl, args)
        runner.cleanup()
        return

    if args.heavy:
        print('Setting the heavy configuration...')
        # Set the sizes of the large files to be bigger than the max memory on the system to test that data
        # is being streamed when the large bundles are used as a dependencies.
        args.large_dependency_size_gb = 16
        args.large_file_size_gb = 16
        args.gpu_runs_count = 50
        args.multiple_cpus_runs_count = 50
        args.bundle_upload_count = 500
        args.create_worksheet_count = 500
        args.parallel_runs_count = 500
        args.large_docker_runs_count = 100
        args.test_infinite_memory = False
        args.test_infinite_gpu = False
        args.infinite_gpu_runs_count = 0
        # TODO: It is a known issue that writing to disk with dd will cause the worker to go down.
        # Disable these tests until we can fix this problem.
        # Issue: https://github.com/codalab/codalab-worksheets/issues/1919
        print('Skipping infinite disk write and large disk writes tests...')
        args.test_infinite_disk = False
        args.large_disk_write_count = 0  # TODO: set to 500
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
        '--cl-executable',
        type=str,
        help='Path to Codalab CLI executable (defaults to "cl")',
        default='cl',
    )
    parser.add_argument(
        '--instance',
        type=str,
        help='CodaLab instance to run stress tests against (defaults to "localhost")',
        default='localhost',
    )
    parser.add_argument(
        '--heavy',
        action='store_true',
        help='Whether to run the heavy version of the stress tests (defaults to false)',
    )
    parser.add_argument(
        '--bypass-wait',
        action='store_true',
        help='Whether to bypass waiting for bundles to finish before cleaning them up (defaults to false)',
    )
    parser.add_argument(
        '--cleanup-only',
        action='store_true',
        help='Whether to just clean up bundles and worksheets from previous stress test runs (defaults to false)',
    )
    parser.add_argument(
        '--bypass-cleanup',
        action='store_true',
        help='Whether to bypass clean up of all the worksheets and bundles post-stress testing (defaults to false)',
    )

    # Custom stress test runner arguments
    parser.add_argument(
        '--large-dependency-size-gb',
        type=int,
        help='Size of large dependency in GB (defaults to 1). Set this to larger than the max memory on the system to test that data is being streamed',
        default=1,
    )
    parser.add_argument(
        '--large-file-size-gb',
        type=int,
        help='Size of large file in GB for single upload (defaults to 1). Set this to larger than the max memory on the system to test that data is being streamed',
        default=1,
    )
    parser.add_argument(
        '--gpu-runs-count',
        type=int,
        help='Number of runs that request a GPU (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        '--multiple-cpus-runs-count',
        type=int,
        help='Number of runs that requests more than one CPU (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        '--bundle-upload-count',
        type=int,
        help='Number of small bundles to upload (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        '--create-worksheet-count',
        type=int,
        help='Number of worksheets to create (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        '--parallel-runs-count',
        type=int,
        help='Number of small, parallel runs (defaults to 4)',
        default=4,
    )
    parser.add_argument(
        '--large-docker-runs-count',
        type=int,
        help='Number of runs with large Docker images (defaults to 20)',
        default=20,
    )
    parser.add_argument(
        '--test-infinite-memory',
        action='store_true',
        help='Whether infinite memory stress test is run (defaults to false)',
    )
    parser.add_argument(
        '--test-infinite-disk',
        action='store_true',
        help='Whether infinite disk write test is run (defaults to false)',
    )
    parser.add_argument(
        '--test-infinite-gpu',
        action='store_true',
        help='Whether infinite gpu usage test is run (defaults to false)',
    )
    parser.add_argument(
        '--infinite-gpu-runs-count',
        type=int,
        help='Number of infinite gpu runs (defaults to 0)',
        default=0,
    )
    parser.add_argument(
        '--large-disk-write-count',
        type=int,
        help='Number of runs with disk writes (defaults to 1)',
        default=1,
    )
    parser.add_argument(
        '--disk-write-size-bytes',
        type=int,
        help='Size of each disk write in bytes (defaults to 1GB)',
        default=1024 * 1024 * 1024,
    )

    # Parse args and run this script
    args = parser.parse_args()
    cl = args.cl_executable
    main()
