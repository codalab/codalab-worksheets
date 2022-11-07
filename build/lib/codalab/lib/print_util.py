import sys
import time

from codalab.lib.formatting import pretty_json, ratio_str


def open_line(s, f=sys.stderr):
    print('\r\033[K%s' % s, end=' ', file=f)


def clear_line(f=sys.stderr):
    print('\r\033[K', end=' ', file=f)


def pretty_print_json(obj, f=sys.stdout):
    f.write(pretty_json(obj))
    f.write('\n')
    f.flush()


class FileTransferProgress(object):
    """
    Formats and displays progress on operations involving transferring bytes.

    Should be used as a context manager:

        with FileTransferProgress('Uploading ', total_bytes) as progress:
            while 1:
                ...
                progress.update(num_bytes)
                ...
    """

    def __init__(self, prefix, bytes_total=None, f=sys.stderr):
        """
        :param prefix: Message to prepend the progress text.
        :param bytes_total: Number of bytes total to transfer, or None if unknown
        :param f: Destination file for progress messages.
        """
        self.prefix = prefix
        self.bytes_total = bytes_total
        self.f = f

    @staticmethod
    def format_size(num_bytes):
        # Simply formats number of mebibytes
        return "%.2fMiB" % (num_bytes / 1024.0 / 1024.0)

    def __enter__(self):
        self.start_time = time.time()
        return self

    def update(self, bytes_done):
        """
        Update progress display.

        :param bytes_done: Number of bytes transferred
        :returns True: To resume connections for breakable
            operations like uploads
        """
        self.f.write('\r')
        self.f.write(self.prefix)
        if self.bytes_total is None:
            self.f.write(self.format_size(bytes_done))
        else:
            self.f.write(ratio_str(self.format_size, bytes_done, self.bytes_total))
        speed = float(bytes_done) / (time.time() - self.start_time)
        self.f.write(' [%s/sec]' % self.format_size(speed))
        self.f.write('    \t\t\t')
        self.f.flush()
        return True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f.write('\n')
