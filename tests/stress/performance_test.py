import argparse
from collections import defaultdict
import random
import string
import tempfile
import time
import json
from scripts.test_util import cleanup, run_command
from test_runner import TestRunner, TestFile


class PerformanceTestRunner(TestRunner):
    """
    This runner tests the performance of upload / download of files of varying sizes.

    Args:
        cl: cl instance used to run CodaLab commands
        args: command line arguments
    """

    _TAG = 'codalab-performance-test'

    def cleanup(self):
        cleanup(self._cl, PerformanceTestRunner._TAG, should_wait=False)

    def _create_worksheet_name(self, base_name):
        return 'performance_test_{}_ws_{}'.format(base_name, self._generate_random_id())

    def _generate_random_id(self):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(24)
        )

    def _set_worksheet(self, run_name):
        worksheet_name = self._create_worksheet_name(run_name)
        uuid = run_command([self._cl, 'new', worksheet_name])
        run_command([self._cl, 'work', worksheet_name])
        run_command([self._cl, 'wedit', '--tag=%s' % PerformanceTestRunner._TAG])
        return uuid

    def _run_bundle(self, args, expected_exit_code=0):
        args.append('--tags=%s' % PerformanceTestRunner._TAG)
        return run_command(args, expected_exit_code)

    def upload_download_file(self, size_mb, storage_type="disk"):
        stats = {}
        large_file: TestFile = TestFile('large_file', size_mb)
        start = time.time()
        if storage_type == "blob":
            uuid: str = self._run_bundle([self._cl, 'upload', '-a', large_file.name()])
        else:
            uuid: str = self._run_bundle([self._cl, 'upload', large_file.name()])
        stats["upload"] = time.time() - start
        start = time.time()
        with tempfile.NamedTemporaryFile() as f:
            self._run_bundle([self._cl, 'download', uuid, '-o', f.name])
            stats["download"] = time.time() - start
        return stats

    def run(self):
        print('Cleaning up performance test files from other runs...')
        self.cleanup()

        self._set_worksheet('bundle_upload')

        print('Running performance tests...')

        file_sizes_mb = [100, 1000, 10000, 100000, 200000]
        stats = defaultdict(dict)
        for file_size_mb in file_sizes_mb:
            for storage_type in ("disk", "blob"):
                stats[storage_type][file_size_mb] = self.upload_download_file(file_sizes_mb)
                print(storage_type, file_size_mb, stats[storage_type][file_size_mb])
        print('test finished')
        print(stats)
        with open("perf-output.json", "w+") as f:
            json.dump(stats, f, indent=4)
        self.cleanup()


def main():
    runner = PerformanceTestRunner(cl, args)
    start_time = time.time()
    runner.run()
    duration_seconds = time.time() - start_time
    print("--- Completion Time: {} minutes---".format(duration_seconds / 60))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs the specified CodaLab performance tests against the specified CodaLab instance (defaults to localhost).'
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
        help='CodaLab instance to run tests against (defaults to "localhost")',
        default='localhost',
    )

    # Parse args and run this script
    args = parser.parse_args()
    cl = args.cl_executable
    main()
