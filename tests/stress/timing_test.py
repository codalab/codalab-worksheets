from stress_test import *
from enum import Enum
import random
class FileSizeType(Enum):
    SMALL = 1
    MEDIUM = 2
    LARGE = 3
    RANDOM = 4

class TimingTestRunner(StressTestRunner):
    def __init__(self, cl, args):
        super().__init__(cl, args, 'codalab-timing-test')
        self.upload_bundle_uuids = None

    def create_test_file(self, size_type):
        """
        Creates a TestFile of a certain size.

        size_type(FileSizeType): indicates how big the file should be.
        """
        file_size = 10
        if size_type == FileSizeType.SMALL:
            file_size = 1
        elif size_type == FileSizeType.MEDIUM:
            file_size = 10
        elif size_type == FileSizeType.LARGE:
            file_size = 100
        elif size_type == FileSizeType.RANDOM:
            file_size = random.randint(1, 100)
        return TestFile('file', file_size)
    
    def upload_bundle(self, size_mb):
        """
        TODO
        """
        file = TestFile('upload bundle file', size_mb)
        uuid = self._run_bundle([self._cl, 'upload', file.name()])
        run_command([cl, 'wait', uuid])
        file.delete()
        return uuid
    
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

        

    def setup_database(self, num_bundles, size_type):
        """
        Setup the database to mimic prod to some extent.

        Parameters:
            num_bundles (int): The number of bundles to create.
            size_type (FileSizeType): The sizes of the bundles to be uploaded.
        
        Returns:
            A list of the uuids of the uploaded files.
        """
        file = self.create_test_file(size_type)
        uuids = list()
        for _ in range(self._args.num_bundles):
            uuid = self._run_bundle([self._cl, 'upload', file.name()])
            uuids.append(uuid)
            run_command([cl, 'wait', uuid])
        file.delete()
        self.upload_bundle_uuids = uuids
        
    def run_bundle_with_wide_dependencies(self, num_dependencies):
        """
        TODO!
        """
        dependency_uuids = random.sample(self.upload_bundle_uuids, num_dependencies)
        dependency_strs = [f'dep{i}:{dep}' for i, dep in enumerate(deps)]
        uuid = self._run_bundle([self.cl, 'run'] + dependency_strs + ['echo hello'])
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

    # Populate the database with all the bundles.
    test_runner.setup_database(args.num_bundles, args.file_size_type)

    # Try basic uploads. Sweep file sizes.
    for file_size in test_runner.upload_test_file_sizes:
        upload_bundle(file_size)
    
    # Try getting bundle info
    # Do this a bunch of times so we can take the mean of the times.
    sample_uuids = random.sample(test_runner.upload_bundle_uuids, args.num_random_samples)
    for uuid in sample_uuids: test_runner.get_info(uuid)

    # Try removing bundle.
    for uuid in sample_uuids: test_runner.remove(uuid)

    # Worksheet stuff goes down here.

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Sets up database for timing tests.'
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
        '--num-bundles',
        type=int,
        help='Number of bundles to add to the database.',
        default=2e5
    )
    parser.add_argument(
        '--file-size-type',
        type=str,
        help='Size of files to upload to the database.',
        default='small'
    )
    parser.add_argument(
        '--upload-test-file-sizes',
        type=int,
        nargs='*',
        help='File size in MB to sweep when doing upload timing test.',
        default=map(int, [1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1, 1e2, 1e3, 1e4])
    )
    parser.add_argument(
        '--num-random-samples',
        type=int,
        help='Number of random samples to take for timing test',
        default=1
    )
    # Parse args and run this script
    args = parser.parse_args()
    cl = args.cl_executable
    main(args)