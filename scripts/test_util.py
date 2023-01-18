import io
import subprocess
import sys
import traceback

global cl


class Colorizer(object):
    RED = "\033[31;1m"
    GREEN = "\033[32;1m"
    YELLOW = "\033[33;1m"
    CYAN = "\033[36;1m"
    RESET = "\033[0m"
    NEWLINE = "\n"

    @classmethod
    def _colorize(cls, string, color):
        return getattr(cls, color) + string + cls.RESET + cls.NEWLINE

    @classmethod
    def red(cls, string):
        return cls._colorize(string, "RED")

    @classmethod
    def green(cls, string):
        return cls._colorize(string, "GREEN")

    @classmethod
    def yellow(cls, string):
        return cls._colorize(string, "YELLOW")

    @classmethod
    def cyan(cls, string):
        return cls._colorize(string, "CYAN")


class FakeStdout(io.StringIO):
    """Fake class to mimic stdout. We can't just use io.StringIO because we need
    to fake the ability to write binary files to sys.stdout.buffer (thus this
    class has a "buffer" attribute that behaves the same way).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = io.BytesIO()

    def getvalue(self):
        """
        If self.buffer has a non-unicode value, return that value.
        Otherwise, decode the self.buffer value and append it
        to self.getvalue().

        This is because this function is mimicking the behavior of `sys.stdout`.
        `sys.stdout` can be read as either a string or bytes.

        When a string is written to `sys.stdout`, it returns a string when doing `getvalue()`.

        When bytes are written to `sys.stdout` (by writing to `sys.stdout.buffer`),
        it returns bytes when doing `getvalue()`. The reason we need to account for this
        case is that there are tests in which a binary file is uploaded, then it is
        printed out (by writing to `sys.stdout.buffer`), and then the test reads what's
        printed out and makes sure it matches the original file.
        """
        try:
            buffer_value = self.buffer.getvalue().decode()
        except UnicodeDecodeError:
            return self.buffer.getvalue()
        return super().getvalue() + buffer_value


def run_command(
    args,
    expected_exit_code=0,
    max_output_chars=1024,
    env=None,
    include_stderr=False,
    binary=False,
    force_subprocess=False,
    cwd=None,
):
    # We import the following imports here because codalab_service.py imports TestModule from
    # this file. If we kept the imports at the top, then anyone who ran codalab_service.py
    # would also have to install all the dependencies that BundleCLI and CodaLabManager use.
    from codalab.lib.bundle_cli import BundleCLI
    from codalab.lib.codalab_manager import CodaLabManager

    def sanitize(string, max_chars=256):
        # Sanitize and truncate output so it can be printed on the command line.
        # Don't print out binary.
        if isinstance(string, bytes):
            string = '<binary>'
        if len(string) > max_chars:
            string = string[:max_chars] + ' (...more...)'
        return string

    # If we don't care about the exit code, set `expected_exit_code` to None.
    print(">>", *map(str, args), sep=" ")
    sys.stdout.flush()

    try:
        kwargs = dict(env=env)
        if not binary:
            kwargs = dict(kwargs, encoding="utf-8")
        if include_stderr:
            kwargs = dict(kwargs, stderr=subprocess.STDOUT)
        if cwd:
            kwargs = dict(kwargs, cwd=cwd)
        if not force_subprocess:
            # In this case, run the Codalab CLI directly, which is much faster
            # than opening a new subprocess to do so.
            stderr = io.StringIO()  # Not used; we just don't want to redirect cli.stderr to stdout.
            stdout = FakeStdout()
            cli = BundleCLI(CodaLabManager(), stdout=stdout, stderr=stderr)
            try:
                cli.do_command(args[1:])
                exitcode = 0
            except SystemExit as e:
                exitcode = e.code
            output = stdout.getvalue()
        else:
            output = subprocess.check_output([a.encode() for a in args], **kwargs)
            exitcode = 0
    except subprocess.CalledProcessError as e:
        output = e.output
        exitcode = e.returncode
    except Exception:
        output = traceback.format_exc()
        exitcode = 1

    if expected_exit_code is not None and exitcode != expected_exit_code:
        colorize = Colorizer.red
        extra = ' BAD'
    else:
        colorize = Colorizer.cyan
        extra = ''
    print(colorize(" (exit code %s, expected %s%s)" % (exitcode, expected_exit_code, extra)))
    sys.stdout.flush()
    print(sanitize(output, max_output_chars))
    sys.stdout.flush()
    assert (
        expected_exit_code == exitcode
    ), f'Exit codes don\'t match: got {exitcode}, expected {expected_exit_code}'
    return output.rstrip()


def cleanup(cl, tag, should_wait=True):
    '''
    Removes all bundles and worksheets with the specified tag.
    :param cl: str
        Path to CodaLab command line.
    :param tag: str
        Specific tag use to search for bundles and worksheets to delete.
    :param should_wait: boolean
        Whether to wait for a bundle to finish running before deleting (default is true).
    :return:
    '''
    print('Cleaning up bundles and worksheets tagged with {}...'.format(tag))
    # Clean up tagged bundles
    bundles_removed = 0
    while True:
        # Query 1000 bundles at a time for removal
        query_result = run_command([cl, 'search', 'tags=%s' % tag, '.limit=1000', '--uuid-only'])
        if len(query_result) == 0:
            break
        for uuid in query_result.split('\n'):
            if should_wait:
                # Wait until the bundle finishes and then delete it
                run_command([cl, 'wait', uuid])
            run_command([cl, 'rm', uuid, '--force'])
            bundles_removed += 1
    # Clean up tagged worksheets
    worksheets_removed = 0
    while True:
        query_result = run_command([cl, 'wsearch', 'tag=%s' % tag, '.limit=1000', '--uuid-only'])
        if len(query_result) == 0:
            break
        for uuid in query_result.split('\n'):
            run_command([cl, 'wrm', uuid, '--force'])
            worksheets_removed += 1
    print('Removed {} bundles and {} worksheets.'.format(bundles_removed, worksheets_removed))
