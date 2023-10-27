"""
These tests will try a basic suite of CodaLab functionality and print out timing results for each of them.
Note that timing results can vary widely depending on the environment they are run in.
Therefore, things that are fast in dev may not be fast in prod and vice versa.
"""
from tests.stress.stress_test import *
from scripts.test_util import timer
from enum import Enum
import random
import json
import os


def temp_path(file_name):
    root = '/tmp'
    return os.path.join(root, file_name)


class TimingTest:
    def __init__(self, num_repeats, fn, *args, **kwargs):
        """
        Parameters:
            fn (function): function we want to test for timing.
            *args, **kwargs: args to function
            num_repeats (int): number of times to repeat test
        """
        self.fn = fn
        self.fn_args = args
        self.fn_kwargs = kwargs
        self.num_repeats = num_repeats

        args_list = [str(a) for a in args] + [f'{k}={v}' for k, v in kwargs.items()]
        args_str = ','.join(args_list)
        self.name = f"{fn.__name__}({args_str})"

        # For storing results
        self.timing_runs = list()
        self.timing_mean = list()
        self.runs_exceptions = list()

    def _run_test(self):
        t = timer(handle_timeouts=False)
        with t:
            try:
                self.fn(*self.fn_args, **self.fn_kwargs)
            except Exception as e:
                self.runs_exceptions.append(str(e))
        self.timing_runs.append(t.time_elapsed)

    def run_test(self):
        for _ in range(self.num_repeats):
            self._run_test()
        self.timing_mean = sum(self.timing_runs) / len(self.timing_runs)
        return self.results()

    def results(self):
        timing_run_results = {
            'runs': self.timing_runs,
            'mean': self.timing_mean,
            'exceptions': self.runs_exceptions,
        }
        return {self.name: timing_run_results}

    def __str__(self):

        return json.dumps(self.results())


class TimingTestRunner(StressTestRunner):
    def __init__(self, cl, args):
        super().__init__(cl, args, 'codalab-timing-test')

    def upload_bundle(self, size_mb):
        """
        TODO
        """
        file = TestFile(temp_path('upload_bundle_file'), size_mb)
        uuid = self._run_bundle([self._cl, 'upload', file.name()])
        run_command([cl, 'wait', uuid])
        file.delete()

    def get_info(self, uuid):
        """
        TODO
        """
        run_command([cl, 'info', uuid, '-f', 'name'])

    def rm(self, uuid):
        """
        TODO
        """
        run_command([cl, 'rm', uuid])

    def run_basic_bundle(self):
        uuid = self._run_bundle([self._cl, 'run', 'echo hello'])
        run_command([cl, 'wait', uuid])

    def kill_bundle(self):
        uuid = self._run_bundle([self._cl, 'run', 'sleep 1000000000;'])
        self._run_bundle()([self._cl, 'kill', uuid])
        run_command([cl, 'wait', uuid])

    def run_bundle_with_wide_dependencies(self, dependency_uuids):
        """
        TODO!
        """
        dependency_strs = [f'dep{i}:{dep}' for i, dep in enumerate(deps)]
        uuid = self._run_bundle([self._cl, 'run'] + dependency_strs + ['echo hello'])
        run_command([cl, 'wait', uuid])

    def run_bundle_with_narrow_dependencies(self, num_dependencies):
        """
        TODO!
        """
        pass


def main(args):
    """
    Runs the timing tests.
    Note that there is no timing code inserted here; we use the Sentry profiler to
    upload timing results to Sentry.
    """
    test_runner = TimingTestRunner(args.cl_executable, args)
    num_repeats = args.num_repeats
    results = list()

    # Try basic uploads. Sweep file sizes.
    for file_size in test_runner._args.upload_test_file_sizes:
        result = TimingTest(num_repeats, test_runner.upload_bundle, file_size).run_test()
        results.append(result)

    # Bundle info
    recent_uuids = run_command([cl, 'search', '.mine', '--uuid-only']).split('\n')
    sample_uuids = random.sample(recent_uuids, args.num_random_samples)
    for uuid in sample_uuids:
        result = TimingTest(num_repeats, test_runner.get_info, uuid).run_test()
        results.append(result)

    # Removing bundle
    timing_test_uuids = run_command(
        [cl, 'search', '.mine', 'tags=%s' % test_runner._TAG, '--uuid-only']
    ).split('\n')
    for uuid in timing_test_uuids:
        result = TimingTest(1, test_runner.rm, uuid).run_test()  # only delete once.
        results.append(result)

    # Running a basic bundle
    # result = TimingTest(num_repeats, test_runner.run_basic_bundle).run_test()
    # results.append(result)

    # Kill bundle
    result = TimingTest(num_repeats, test_runner.kill_bundle).run_test()
    results.append(result)

    # Worksheet stuff goes down here.

    results = json.dumps(results, indent=4)
    print(results)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run some basic timing tests and output how long things take.'
        'Please note the environment these tests are run in and that timing may be different'
        'in production and dev environments given the difference in the size and content of the databases used.'
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
        help='CodaLab instance to run timing tests against (defaults to "localhost")',
        default='localhost',
    )
    parser.add_argument(
        '--upload-test-file-sizes',
        type=float,
        nargs='*',
        help='File size in MB to sweep when doing upload timing test.',
        default=map(int, []),
    )
    parser.add_argument(
        '--num-random-samples',
        type=int,
        help='Number of uuids to randomly sample when performing tests on already existing uuids',
        default=1,
    )
    parser.add_argument(
        '--num-repeats', type=int, help='Number of times to repeat timing test', default=1
    )
    parser.add_argument(
        '--results-dir', type=str, help='Directory to write results file to', default='tests/timing'
    )
    # Parse args and run this script
    args = parser.parse_args()
    cl = args.cl_executable
    main(args)
