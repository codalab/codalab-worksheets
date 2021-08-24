import os
import subprocess


class TestRunner:
    """Base class for test runners."""

    def __init__(self, cl, args):
        self._cl = cl
        self._args = args

        # Connect to the instance the tests will run on
        print('Connecting to instance %s...' % args.instance)
        subprocess.call([self._cl, 'work', '%s::' % args.instance])


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
        if content is None:
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
