import argparse
from collections import defaultdict
import random
import os
import string
import tempfile
import time
import json
from scripts.test_util import cleanup, run_command
from test_runner import TestRunner
import tarfile


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

    def upload_download_file(self, file_name, storage_type="disk", is_archive=False):
        stats = {}
        start = time.time()
        if storage_type == "blob":
            uuid: str = self._run_bundle([self._cl, 'upload', '-a', file_name])
        else:
            uuid: str = self._run_bundle([self._cl, 'upload', file_name])
        stats["upload"] = time.time() - start
        start = time.time()
        with tempfile.TemporaryDirectory() as dir:
            run_command([self._cl, 'download', uuid, '-o', os.path.join(dir, "output")])
            stats["download"] = time.time() - start
        if is_archive:
            start = time.time()
            with tempfile.TemporaryDirectory() as dir:
                run_command(
                    [self._cl, 'download', f'{uuid}/README.md', '-o', os.path.join(dir, "output")]
                )
                stats["download_small_file"] = time.time() - start
            start = time.time()
            with tempfile.TemporaryDirectory() as dir:
                run_command(
                    [self._cl, 'download', f'{uuid}/blob', '-o', os.path.join(dir, "output")]
                )
                stats["download_large_file"] = time.time() - start
        run_command([self._cl, 'rm', uuid])
        stats["rm"] = time.time() - start
        return stats

    def write_stats(self, stats):
        with open("perf-output.json", "w+") as f:
            json.dump(stats, f, indent=4)

    def run(self):
        print('Cleaning up performance test files from other runs...')
        self.cleanup()

        self._set_worksheet('bundle_upload')

        print('Running performance tests...')

        file_sizes_mb = [10, 100, 1000, 10000, 100000, 200000]
        stats = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for file_size_mb in file_sizes_mb:
            for is_archive in (True, False):
                archive_label = "archive" if is_archive else "single_file"
                with tempfile.TemporaryDirectory() as tempdir:
                    file_name = os.path.join(tempdir, "blob")
                    with open(file_name, 'wb') as file:
                        file.seek(
                            file_size_mb * 1024 * 1024
                        )  # Seek takes in file size in terms of bytes
                        file.write(b'0')
                    if is_archive:
                        small_file_name = os.path.join(tempdir, "README.md")
                        with open(small_file_name, "w") as f:
                            f.write("Hello world\n" * 1000)
                        tar_file_name = os.path.join(tempdir, "archive.tar.gz")
                        with tarfile.open(tar_file_name, "w:gz") as tar:
                            tar.add(file_name, arcname="./blob")
                            tar.add(small_file_name, arcname="./README.md")
                        os.remove(small_file_name)
                        os.remove(file_name)
                        file_name = tar_file_name
                        archive_size_mb = os.path.getsize(file_name) / 1024 / 1024
                        stats["archive_sizes_mb"][file_size_mb] = archive_size_mb
                    for storage_type in ("disk", "blob"):
                        for i in (1, 2, 3):
                            result = self.upload_download_file(file_name, storage_type, is_archive)
                            stats[archive_label][storage_type][file_size_mb].append(result)
                            print(archive_label, storage_type, file_size_mb, i, result)
                            self.write_stats(stats)
        print('test finished')
        print(stats)
        self.write_stats(stats)
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
