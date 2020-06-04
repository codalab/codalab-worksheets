# -*- coding: utf-8 -*-
"""
Tests all the CLI functionality end-to-end.

Tests will operate on temporary worksheets created during testing.  In theory,
it should not mutate preexisting data on your instance, but this is not
guaranteed, and you should run this command in an unimportant CodaLab account.

For full coverage of testing, be sure to run this over a remote connection (i.e.
while connected to localhost::) in addition to local testing, in order to test
the full RPC pipeline, and also as a non-root user, to hammer out unanticipated
permission issues.

Things not tested:
- Interactive modes (cl edit, cl wedit)
- Permissions
"""

from collections import namedtuple, OrderedDict
from contextlib import contextmanager
from codalab.worker.download_util import BundleTarget
from codalab.worker.bundle_state import State
from scripts.create_sample_worksheet import SampleWorksheet
from scripts.test_util import Colorizer, run_command

import argparse
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
import traceback
from datetime import datetime


global cl
# Directory where this script lives.
base_path = os.path.dirname(os.path.abspath(__file__))
crazy_name = 'crazy (ain\'t it)'
CodaLabInstance = namedtuple('CodaLabInstance', 'host home username password')


def test_path(name):
    """Return the path to the test file ``name``."""
    return os.path.join(base_path, 'tests', 'files', name)


# Note: when we talk about contents, we always apply rstrip() even if it's a
# binary file.  This is fine as long as we're consistent about doing rstrip()
# everywhere to test for equality.


def test_path_contents(name, binary=False):
    return path_contents(test_path(name), binary=binary)


def path_contents(path, binary=False):
    with open(path, "rb") as file:
        if binary:
            return file.read().rstrip()
        return file.read().decode().rstrip()


def temp_path(suffix, tmp=True):
    root = '/tmp' if tmp else base_path
    return os.path.join(root, random_name() + suffix)


def random_name():
    return 'temp-test-cli-' + str(random.randint(0, 1000000))


def current_worksheet():
    """
    Returns the full worksheet spec of the current worksheet.

    Does so by parsing the output of `cl work`:
        Switched to worksheet http://localhost:2900/worksheets/0x87a7a7ffe29d4d72be9b23c745adc120 (home-codalab).
    """
    m = re.search('(http.*?)/worksheets/(.*?) \((.*?)\)', _run_command([cl, 'work']))
    assert m is not None
    worksheet_host, worksheet_uuid, worksheet_name = m.group(1), m.group(2), m.group(3)
    return worksheet_host + "::" + worksheet_name


def current_user():
    """
    Return the uuid and username of the current user in a tuple
    Does so by parsing the output of `cl uinfo` which by default returns the info
    of the current user
    """
    user_id = _run_command([cl, 'uinfo', '-f', 'id'])
    user_name = _run_command([cl, 'uinfo', '-f', 'user_name'])
    return user_id, user_name


def get_uuid(line):
    """
    Returns the uuid from a line where the uuid is between parentheses
    """
    m = re.search(".*\((0x[a-z0-9]+)\)", line)
    assert m is not None
    return m.group(1)


def get_info(uuid, key):
    return _run_command([cl, 'info', '-f', key, uuid])


def wait_until_state(uuid, expected_state, timeout_seconds=100):
    """
    Waits until a bundle in in the expected state or one of the final states. If a bundle is
    in one of the final states that is not the expected_state, fail earlier than the timeout.

    Parameters:
        uuid: UUID of bundle to check state for
        expected_state: Expected state of bundle
        timeout_seconds: Maximum timeout to wait for the bundle. Default is 100 seconds.
    """
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise AssertionError('timeout while waiting for %s to run' % uuid)
        current_state = get_info(uuid, 'state')

        # Stop waiting when the bundle is in the expected state or one of the final states
        if current_state == expected_state:
            return
        elif current_state in State.FINAL_STATES:
            raise AssertionError(
                "For bundle with uuid {}, waited for '{}' state, but got '{}'.".format(
                    uuid, expected_state, current_state
                )
            )
        time.sleep(0.5)


def wait_for_contents(uuid, substring, timeout_seconds=100):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise AssertionError('timeout while waiting for %s to run' % uuid)
        try:
            out = _run_command([cl, 'cat', uuid])
        except AssertionError:
            time.sleep(0.5)
            continue
        if substring in out:
            return True
        time.sleep(0.5)


def wait(uuid, expected_exit_code=0):
    _run_command([cl, 'wait', uuid], expected_exit_code)


def check_equals(true_value, pred_value):
    assert true_value == pred_value, "expected '%s', but got '%s'" % (true_value, pred_value)
    return pred_value


def check_not_equals(true_value, pred_value):
    assert (
        true_value != pred_value
    ), "expected something that doesn't equal to '%s', but got '%s'" % (true_value, pred_value)
    return pred_value


def check_contains(true_value, pred_value):
    if isinstance(true_value, list):
        for v in true_value:
            check_contains(v, pred_value)
    else:
        assert true_value in pred_value or re.search(
            true_value, pred_value
        ), "expected something that contains '%s', but got '%s'" % (true_value, pred_value)
    return pred_value


def check_num_lines(true_value, pred_value):
    num_lines = len(pred_value.split('\n'))
    assert num_lines == true_value, "expected %d lines, but got %s" % (true_value, num_lines)
    return pred_value


def wait_until_substring(fp, substr):
    """
    Block until we see substr appear in the given file fp.
    """
    while True:
        line = fp.readline()
        if substr in line:
            return


def _run_command(
    args,
    expected_exit_code=0,
    max_output_chars=4096,
    env=None,
    include_stderr=False,
    binary=False,
    force_subprocess=False,
):
    # We skip using the cli directly if force_subprocess is set to true (which forces
    # us to use subprocess even for cl commands).
    force_subprocess = not force_subprocess and args[0] == cl
    return run_command(
        args, expected_exit_code, max_output_chars, env, include_stderr, binary, force_subprocess
    )


@contextmanager
def remote_instance(remote_host):
    """
    Usage:
        with remote_instance(host) as remote:
            run_command([cl, 'work', remote.home])
            ... do more stuff with new temp instance ...
    """
    # Dockerized instance
    original_worksheet = current_worksheet()

    # Switch to new host and log in to cache auth token
    remote_worksheet = '%s::' % remote_host
    _run_command([cl, 'logout', remote_worksheet[:-2]])

    env = {'CODALAB_USERNAME': 'codalab', 'CODALAB_PASSWORD': 'codalab'}
    _run_command([cl, 'work', remote_worksheet], env=env)

    yield CodaLabInstance(
        remote_host, remote_worksheet, env['CODALAB_USERNAME'], env['CODALAB_PASSWORD']
    )

    _run_command([cl, 'work', original_worksheet])


class ModuleContext(object):
    """ModuleContext objects manage the context of a test module.

    Instances of ModuleContext are meant to be used with the Python
    'with' statement (PEP 343).

    For documentation on with statement context managers:
    https://docs.python.org/2/reference/datamodel.html#with-statement-context-managers
    """

    def __init__(self, instance, second_instance):
        # These are the temporary worksheets and bundles that need to be
        # cleaned up at the end of the test.
        self.instance = instance
        self.second_instance = second_instance
        self.worksheets = []
        self.bundles = []
        self.groups = []
        self.error = None

        # Allow for making REST calls
        from codalab.lib.codalab_manager import CodaLabManager

        manager = CodaLabManager()
        self.client = manager.current_client()

    def __enter__(self):
        """Prepares clean environment for test module."""
        print("[*][*] SWITCHING TO TEMPORARY WORKSHEET")

        self.original_environ = os.environ.copy()
        self.original_worksheet = _run_command([cl, 'work', '-u'])
        temp_worksheet = _run_command([cl, 'new', random_name()])
        self.worksheets.append(temp_worksheet)
        _run_command([cl, 'work', temp_worksheet])

        print("[*][*] BEGIN TEST")

        return self

    def __exit__(self, exc_type, exc_value, tb):
        """Tears down temporary environment for test module."""
        # Check for and handle exceptions if any
        if exc_type is not None:
            self.error = (exc_type, exc_value, tb)
            if exc_type is AssertionError:
                print(Colorizer.red("[!] ERROR: %s" % str(exc_value)))
            elif exc_type is KeyboardInterrupt:
                print(Colorizer.red("[!] Caught interrupt! Quitting after cleanup."))
            else:
                print(Colorizer.red("[!] ERROR: Test raised an exception!"))
                traceback.print_exception(exc_type, exc_value, tb)
        else:
            print(Colorizer.green("[*] TEST PASSED"))

        # Clean up and restore original worksheet
        print("[*][*] CLEANING UP")
        os.environ.clear()
        os.environ.update(self.original_environ)

        _run_command([cl, 'work', self.original_worksheet])
        for worksheet in self.worksheets:
            self.bundles.extend(_run_command([cl, 'ls', '-w', worksheet, '-u']).split())
            _run_command([cl, 'wrm', '--force', worksheet])

        # Delete all bundles (kill and dedup first)
        if len(self.bundles) > 0:
            for bundle in set(self.bundles):
                try:
                    if _run_command([cl, 'info', '-f', 'state', bundle]) not in State.FINAL_STATES:
                        _run_command([cl, 'kill', bundle])
                        _run_command([cl, 'wait', bundle], expected_exit_code=1)
                except AssertionError:
                    print('CAUGHT')
                    pass
                _run_command([cl, 'rm', '--force', bundle])

        # Delete all groups (dedup first)
        if len(self.groups) > 0:
            _run_command([cl, 'grm'] + list(set(self.groups)))

        # Reraise only KeyboardInterrupt
        if exc_type is KeyboardInterrupt:
            return False
        else:
            return True

    def collect_worksheet(self, uuid):
        """Mark a worksheet for cleanup on exit."""
        self.worksheets.append(uuid)

    def collect_bundle(self, uuid):
        """Mark a bundle for cleanup on exit."""
        self.bundles.append(uuid)

    def collect_group(self, uuid):
        """Mark a group for cleanup on exit."""
        self.groups.append(uuid)


class TestModule(object):
    """Instances of TestModule each encapsulate a test module and its metadata.

    The class itself also maintains a registry of the existing modules, providing
    a decorator to register new modules and a class method to run modules by name.
    """

    modules = OrderedDict()

    def __init__(self, name, func, description, default):
        self.name = name
        self.func = func
        self.description = description
        self.default = default

    @classmethod
    def register(cls, name, default=True):
        """Returns a decorator to register new test modules.

        The decorator will add a given function as test modules to the registry
        under the name provided here. The function's docstring (PEP 257) will
        be used as the prose description of the test module.

        :param name: name of the test module
        :param default: True to include in the 'default' module set
        """

        def add_module(func):
            cls.modules[name] = TestModule(name, func, func.__doc__, default)

        return add_module

    @classmethod
    def all_modules(cls):
        return list(cls.modules.values())

    @classmethod
    def default_modules(cls):
        return [m for m in cls.modules.values() if m.default]

    @classmethod
    def run(cls, tests, instance, second_instance):
        """Run the modules named in tests against instances.

        tests should be a list of strings, each of which is either 'all',
        'default', or the name of an existing test module.

        instance should be a CodaLab instance to connect. The following are some examples:
            - main
            - localhost
            - http://server-domain:2900
        """
        # Might prompt user for password
        subprocess.call([cl, 'work', '%s::' % instance])

        # Build list of modules to run based on tests
        modules_to_run = []
        for name in tests:
            if name == 'all':
                modules_to_run.extend(cls.all_modules())
            elif name == 'default':
                modules_to_run.extend(cls.default_modules())
            elif name in cls.modules:
                modules_to_run.append(cls.modules[name])
            else:
                print(Colorizer.red("[!] Could not find module %s" % name))
                print(Colorizer.red("[*] Modules: all %s" % " ".join(list(cls.modules.keys()))))
                sys.exit(1)

        print(
            (
                Colorizer.yellow(
                    "[*][*] Running modules %s" % " ".join([m.name for m in modules_to_run])
                )
            )
        )

        # Run modules, continuing onto the next test module regardless of
        # failure
        failed = []
        for module in modules_to_run:
            print(Colorizer.yellow("[*][*] BEGIN MODULE: %s" % module.name))
            if module.description is not None:
                print(Colorizer.yellow("[*][*] DESCRIPTION: %s" % module.description))

            with ModuleContext(instance, second_instance) as ctx:
                module.func(ctx)

            if ctx.error:
                failed.append(module.name)

        # Provide a (currently very rudimentary) summary
        print(Colorizer.yellow("[*][*][*] SUMMARY"))
        if failed:
            print(Colorizer.red("[!][!] Tests failed: %s" % ", ".join(failed)))
            return False
        else:
            print(Colorizer.green("[*][*] All tests passed!"))
            return True


############################################################


@TestModule.register('unittest')
def test(ctx):
    """Run nose unit tests (exclude this file)."""
    _run_command(['nosetests', '-e', 'test_cli.py'])


@TestModule.register('gen-rest-docs')
def test(ctx):
    """Generate REST API docs."""
    _run_command(['python3', os.path.join(base_path, 'scripts/gen-rest-docs.py'), '--docs', '/tmp'])


@TestModule.register('gen-cli-docs')
def test(ctx):
    """Generate CLI docs."""
    _run_command(['python3', os.path.join(base_path, 'scripts/gen-cli-docs.py'), '--docs', '/tmp'])


@TestModule.register('gen-readthedocs')
def test(ctx):
    """Generate the readthedocs site."""
    # Make sure there are no extraneous things.
    # mkdocs doesn't return exit code 1 for some warnings.
    check_num_lines(3, _run_command(['mkdocs', 'build', '-d', '/tmp/site'], include_stderr=True))


@TestModule.register('basic')
def test(ctx):
    # upload
    uuid = _run_command(
        [cl, 'upload', test_path('a.txt'), '--description', 'hello', '--tags', 'a', 'b']
    )
    check_equals('a.txt', get_info(uuid, 'name'))
    check_equals('hello', get_info(uuid, 'description'))
    check_contains(['a', 'b'], get_info(uuid, 'tags'))
    check_equals(State.READY, get_info(uuid, 'state'))
    check_equals('ready\thello', get_info(uuid, 'state,description'))

    # edit
    _run_command([cl, 'edit', uuid, '--name', 'a2.txt', '--tags', 'c', 'd', 'e'])
    check_equals('a2.txt', get_info(uuid, 'name'))
    check_contains(['c', 'd', 'e'], get_info(uuid, 'tags'))

    # cat, info
    check_equals(test_path_contents('a.txt'), _run_command([cl, 'cat', uuid]))
    check_contains(['bundle_type', 'uuid', 'owner', 'created'], _run_command([cl, 'info', uuid]))
    check_contains('license', _run_command([cl, 'info', '--raw', uuid]))
    check_contains(['host_worksheets', 'contents'], _run_command([cl, 'info', '--verbose', uuid]))
    # test interpret_file_genpath
    check_equals(' '.join(test_path_contents('a.txt').splitlines(False)), get_info(uuid, '/'))

    # rm
    _run_command([cl, 'rm', '--dry-run', uuid])
    check_contains('0x', get_info(uuid, 'data_hash'))
    _run_command([cl, 'rm', '--data-only', uuid])
    check_equals('None', get_info(uuid, 'data_hash'))
    _run_command([cl, 'rm', uuid])

    # run and check the data_hash
    uuid = _run_command([cl, 'run', 'echo hello'])
    print('Waiting echo hello with uuid %s' % uuid)
    wait(uuid)
    check_contains('0x', get_info(uuid, 'data_hash'))


@TestModule.register('upload1')
def test(ctx):
    # Upload contents
    uuid = _run_command([cl, 'upload', '-c', 'hello'])
    check_equals('hello', _run_command([cl, 'cat', uuid]))

    # Upload binary file
    uuid = _run_command([cl, 'upload', test_path('echo')])
    check_equals(
        test_path_contents('echo', binary=True), _run_command([cl, 'cat', uuid], binary=True)
    )

    # Upload file with crazy name
    uuid = _run_command([cl, 'upload', test_path(crazy_name)])
    check_equals(test_path_contents(crazy_name), _run_command([cl, 'cat', uuid]))

    # Upload directory with a symlink
    uuid = _run_command([cl, 'upload', test_path('')])
    check_equals(' -> /etc/passwd', _run_command([cl, 'cat', uuid + '/passwd']))

    # Upload symlink without following it.
    uuid = _run_command([cl, 'upload', test_path('a-symlink.txt')], 1)

    # Upload symlink, follow link
    uuid = _run_command([cl, 'upload', test_path('a-symlink.txt'), '--follow-symlinks'])
    check_equals(test_path_contents('a-symlink.txt'), _run_command([cl, 'cat', uuid]))
    _run_command([cl, 'cat', uuid])  # Should have the full contents

    # Upload broken symlink (should not be possible)
    uuid = _run_command([cl, 'upload', test_path('broken-symlink'), '--follow-symlinks'], 1)

    # Upload directory with excluded files
    uuid = _run_command([cl, 'upload', test_path('dir1'), '--exclude-patterns', 'f*'])
    check_num_lines(
        2 + 2, _run_command([cl, 'cat', uuid])
    )  # 2 header lines, Only two files left after excluding and extracting.

    # Upload multiple files with excluded files
    uuid = _run_command(
        [
            cl,
            'upload',
            test_path('dir1'),
            test_path('echo'),
            test_path(crazy_name),
            '--exclude-patterns',
            'f*',
        ]
    )
    check_num_lines(
        2 + 3, _run_command([cl, 'cat', uuid])
    )  # 2 header lines, 3 items at bundle target root
    check_num_lines(
        2 + 2, _run_command([cl, 'cat', uuid + '/dir1'])
    )  # 2 header lines, Only two files left after excluding and extracting.

    # Upload directory with only one file, should not simplify directory structure
    uuid = _run_command([cl, 'upload', test_path('dir2')])
    check_num_lines(
        2 + 1, _run_command([cl, 'cat', uuid])
    )  # Directory listing with 2 headers lines and one file


@TestModule.register('upload2')
def test(ctx):
    # Upload tar.gz and zip.
    for suffix in ['.tar.gz', '.zip']:
        # Pack it up
        archive_path = temp_path(suffix)
        contents_path = test_path('dir1')
        if suffix == '.tar.gz':
            _run_command(
                [
                    'tar',
                    'cfz',
                    archive_path,
                    '-C',
                    os.path.dirname(contents_path),
                    os.path.basename(contents_path),
                ]
            )
        else:
            _run_command(
                [
                    'bash',
                    '-c',
                    'cd %s && zip -r %s %s'
                    % (
                        os.path.dirname(contents_path),
                        archive_path,
                        os.path.basename(contents_path),
                    ),
                ]
            )

        # Upload it and unpack
        uuid = _run_command([cl, 'upload', archive_path])
        check_equals(os.path.basename(archive_path).replace(suffix, ''), get_info(uuid, 'name'))
        check_equals(test_path_contents('dir1/f1'), _run_command([cl, 'cat', uuid + '/f1']))

        # Upload it but don't unpack
        uuid = _run_command([cl, 'upload', archive_path, '--pack'])
        check_equals(os.path.basename(archive_path), get_info(uuid, 'name'))
        check_equals(
            test_path_contents(archive_path, binary=True),
            _run_command([cl, 'cat', uuid], binary=True),
        )

        # Force compression
        uuid = _run_command([cl, 'upload', test_path('echo'), '--force-compression'])
        check_equals('echo', get_info(uuid, 'name'))
        check_equals(
            test_path_contents('echo', binary=True), _run_command([cl, 'cat', uuid], binary=True)
        )

        os.unlink(archive_path)


@TestModule.register('upload3')
def test(ctx):
    # Upload URL
    uuid = _run_command([cl, 'upload', 'https://www.wikipedia.org'])
    check_contains('<title>Wikipedia</title>', _run_command([cl, 'cat', uuid]))

    # Upload URL that's an archive
    uuid = _run_command([cl, 'upload', 'http://alpha.gnu.org/gnu/bc/bc-1.06.95.tar.bz2'])
    check_contains(['README', 'INSTALL', 'FAQ'], _run_command([cl, 'cat', uuid]))

    # Upload URL from Git
    uuid = _run_command([cl, 'upload', 'https://github.com/codalab/codalab-worksheets', '--git'])
    check_contains(['README.md', 'codalab', 'scripts'], _run_command([cl, 'cat', uuid]))


@TestModule.register('upload4')
def test(ctx):
    # Uploads a pair of archives at the same time. Makes sure they're named correctly when unpacked.
    archive_paths = [temp_path(''), temp_path('')]
    archive_exts = [p + '.tar.gz' for p in archive_paths]
    contents_paths = [test_path('dir1'), test_path('a.txt')]
    for (archive, content) in zip(archive_exts, contents_paths):
        _run_command(
            ['tar', 'cfz', archive, '-C', os.path.dirname(content), os.path.basename(content)]
        )
    uuid = _run_command([cl, 'upload'] + archive_exts)

    # Make sure the names do not end with '.tar.gz' after being unpacked.
    check_contains(
        [os.path.basename(archive_paths[0]) + r'\s', os.path.basename(archive_paths[1]) + r'\s'],
        _run_command([cl, 'cat', uuid]),
    )

    # Cleanup
    for archive in archive_exts:
        os.unlink(archive)


@TestModule.register('download')
def test(ctx):
    # Upload test files directory as archive to preserve everything invariant of the upload implementation
    archive_path = temp_path('.tar.gz')
    contents_path = test_path('')
    _run_command(
        ['tar', 'cfz', archive_path, '-C', os.path.dirname(contents_path), '--']
        + os.listdir(contents_path)
    )
    uuid = _run_command([cl, 'upload', archive_path])

    # Download whole bundle
    path = temp_path('')
    _run_command([cl, 'download', uuid, '-o', path])
    check_contains(['a.txt', 'b.txt', 'echo', crazy_name], _run_command(['ls', '-R', path]))
    shutil.rmtree(path)

    # Download a target inside (binary)
    _run_command([cl, 'download', uuid + '/echo', '-o', path])
    check_equals(test_path_contents('echo', binary=True), path_contents(path, binary=True))
    os.unlink(path)

    # Download a target inside (crazy name)
    _run_command([cl, 'download', uuid + '/' + crazy_name, '-o', path])
    check_equals(test_path_contents(crazy_name), path_contents(path))
    os.unlink(path)

    # Download a target inside (name starting with hyphen)
    _run_command([cl, 'download', uuid + '/' + '-AmMDnVl4s8', '-o', path])
    check_equals(test_path_contents('-AmMDnVl4s8'), path_contents(path))
    os.unlink(path)

    # Download a target inside (symlink)
    _run_command([cl, 'download', uuid + '/a-symlink.txt', '-o', path], 1)  # Disallow symlinks

    # Download a target inside (directory)
    _run_command([cl, 'download', uuid + '/dir1', '-o', path])
    check_equals(test_path_contents('dir1/f1'), path_contents(path + '/f1'))
    shutil.rmtree(path)

    # Download something that doesn't exist
    _run_command([cl, 'download', 'not-exists'], 1)
    _run_command([cl, 'download', uuid + '/not-exists'], 1)


@TestModule.register('refs')
def test(ctx):
    # Test references
    uuid = _run_command([cl, 'upload', test_path('a.txt')])
    wuuid = _run_command([cl, 'work', '-u'])
    # Compound bundle references
    _run_command([cl, 'info', wuuid + '/' + uuid])
    # . is current worksheet
    check_contains(wuuid, _run_command([cl, 'ls', '-w', '.']))
    # / is home worksheet
    check_contains('::home-', _run_command([cl, 'ls', '-w', '/']))


@TestModule.register('binary')
def test(ctx):
    # Upload a binary file and test it
    path = '/bin/ls'
    uuid = _run_command([cl, 'upload', path])
    check_equals(open(path, 'rb').read(), _run_command([cl, 'cat', uuid], binary=True))
    _run_command([cl, 'info', '--verbose', uuid])


@TestModule.register('rm')
def test(ctx):
    uuid = _run_command([cl, 'upload', test_path('a.txt')])
    _run_command([cl, 'add', 'bundle', uuid])  # Duplicate
    _run_command([cl, 'rm', uuid])  # Can delete even though it exists twice on the same worksheet


@TestModule.register('make')
def test(ctx):
    uuid1 = _run_command([cl, 'upload', test_path('a.txt')])
    uuid2 = _run_command([cl, 'upload', test_path('b.txt')])
    # make
    uuid3 = _run_command([cl, 'make', 'dep1:' + uuid1, 'dep2:' + uuid2])
    wait(uuid3)
    check_contains(['dep1', uuid1, 'dep2', uuid2], _run_command([cl, 'info', uuid3]))
    # anonymous make
    uuid4 = _run_command([cl, 'make', uuid3, '--name', 'foo'])
    wait(uuid4)
    check_contains([uuid3], _run_command([cl, 'info', uuid3]))
    # Cleanup
    _run_command([cl, 'rm', uuid1], 1)  # should fail
    _run_command([cl, 'rm', '--force', uuid2])  # force the deletion
    _run_command([cl, 'rm', '-r', uuid1])  # delete things downstream


@TestModule.register('worksheet')
def test(ctx):
    wname = random_name()
    # Create new worksheet
    wuuid = _run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    check_contains(['Switched', wname, wuuid], _run_command([cl, 'work', wuuid]))
    # ls
    check_equals('', _run_command([cl, 'ls', '-u']))
    uuid = _run_command([cl, 'upload', test_path('a.txt')])
    check_equals(uuid, _run_command([cl, 'ls', '-u']))
    # create worksheet
    check_contains(uuid[0:5], _run_command([cl, 'ls']))
    _run_command([cl, 'add', 'text', 'testing'])
    _run_command([cl, 'add', 'text', '你好世界😊'])
    _run_command([cl, 'add', 'text', '% display contents / maxlines=10'])
    _run_command([cl, 'add', 'bundle', uuid])
    _run_command([cl, 'add', 'text', '// comment'])
    _run_command([cl, 'add', 'text', '% schema foo'])
    _run_command([cl, 'add', 'text', '% add uuid'])
    _run_command([cl, 'add', 'text', '% add data_hash data_hash s/0x/HEAD'])
    _run_command([cl, 'add', 'text', '% add CREATE created "date | [0:5]"'])
    _run_command([cl, 'add', 'text', '% display table foo'])

    _run_command([cl, 'add', 'bundle', uuid])
    _run_command(
        [cl, 'add', 'bundle', uuid, '--dest-worksheet', wuuid]
    )  # not testing real copying ability
    _run_command([cl, 'add', 'worksheet', wuuid])
    check_contains(
        ['Worksheet', 'testing', '你好世界😊', test_path_contents('a.txt'), uuid, 'HEAD', 'CREATE'],
        _run_command([cl, 'print']),
    )
    _run_command([cl, 'wadd', wuuid, wuuid])
    check_num_lines(8, _run_command([cl, 'ls', '-u']))
    _run_command([cl, 'wedit', wuuid, '--name', wname + '2'])

    _run_command(
        [cl, 'wedit', wuuid, '--file', test_path('unicode-worksheet')]
    )  # try unicode in worksheet contents
    check_contains([test_path_contents('unicode-worksheet')], _run_command([cl, 'print', '-r']))

    _run_command([cl, 'wedit', wuuid, '--file', '/dev/null'])  # wipe out worksheet


@TestModule.register('worksheet_search')
def test(ctx):
    wname = random_name()
    # Create new worksheet
    wuuid = _run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    check_contains(['Switched', wname, wuuid], _run_command([cl, 'work', wuuid]))
    uuid = _run_command([cl, 'upload', test_path('a.txt')])
    _run_command([cl, 'add', 'text', '% search ' + uuid])
    _run_command([cl, 'add', 'text', '% wsearch ' + wuuid])
    check_contains([uuid[0:8], wuuid[0:8]], _run_command([cl, 'print']))
    # Check search by group
    group_wname = random_name()
    group_wuuid = _run_command([cl, 'new', group_wname])
    ctx.collect_worksheet(group_wuuid)
    check_contains(['Switched', group_wname, group_wuuid], _run_command([cl, 'work', group_wuuid]))
    user_id, user_name = current_user()
    # Create new group
    group_name = random_name()
    group_uuid_line = _run_command([cl, 'gnew', group_name])
    group_uuid = get_uuid(group_uuid_line)
    ctx.collect_group(group_uuid)
    # Make worksheet unavailable to public but available to the group
    _run_command([cl, 'wperm', group_wuuid, 'public', 'n'])
    _run_command([cl, 'wperm', group_wuuid, group_name, 'r'])
    check_contains(group_wuuid[:8], _run_command([cl, 'wls', '.shared']))
    check_contains(group_wuuid[:8], _run_command([cl, 'wls', 'group={}'.format(group_uuid)]))
    check_contains(group_wuuid[:8], _run_command([cl, 'wls', 'group={}'.format(group_name)]))


@TestModule.register('worksheet_tags')
def test(ctx):
    wname = random_name()
    wuuid = _run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    # Add tags
    tags = ['foo', 'bar', 'baz']
    _run_command([cl, 'wedit', wname, '--tags'] + tags)
    check_contains(['Tags: %s' % ' '.join(tags)], _run_command([cl, 'ls', '-w', wuuid]))
    # Modify tags
    fewer_tags = ['bar', 'foo']
    _run_command([cl, 'wedit', wname, '--tags'] + fewer_tags)
    check_contains(['Tags: %s' % ' '.join(fewer_tags)], _run_command([cl, 'ls', '-w', wuuid]))
    # Modify to non-ascii tags
    # TODO: enable with Unicode support.
    non_ascii_tags = ['你好世界😊', 'fáncy ünicode']
    _run_command(
        [cl, 'wedit', wname, '--tags'] + non_ascii_tags, 1, force_subprocess=True
    )  # TODO: find a way to make this work without force_subprocess
    # check_contains(non_ascii_tags, _run_command([cl, 'ls', '-w', wuuid]))
    # Delete tags
    _run_command([cl, 'wedit', wname, '--tags'])
    check_contains(r'Tags:\s+###', _run_command([cl, 'ls', '-w', wuuid]))


@TestModule.register('freeze')
def test(ctx):
    _run_command([cl, 'work', '-u'])
    wname = random_name()
    wuuid = _run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    check_contains(['Switched', wname, wuuid], _run_command([cl, 'work', wuuid]))
    # Before freezing: can modify everything
    uuid1 = _run_command([cl, 'upload', '-c', 'hello'])
    _run_command([cl, 'add', 'text', 'message'])
    _run_command([cl, 'wedit', '-t', 'new_title'])
    _run_command([cl, 'wperm', wuuid, 'public', 'n'])
    _run_command([cl, 'wedit', '--freeze'])
    # After freezing: can only modify contents
    _run_command([cl, 'detach', uuid1], 1)  # would remove an item
    _run_command([cl, 'rm', uuid1], 1)  # would remove an item
    _run_command([cl, 'add', 'text', 'message'], 1)  # would add an item
    _run_command([cl, 'wedit', '-t', 'new_title'])  # can edit
    _run_command([cl, 'wperm', wuuid, 'public', 'a'])  # can edit


@TestModule.register('detach')
def test(ctx):
    uuid1 = _run_command([cl, 'upload', test_path('a.txt')])
    uuid2 = _run_command([cl, 'upload', test_path('b.txt')])
    _run_command([cl, 'add', 'bundle', uuid1])
    ctx.collect_bundle(uuid1)
    _run_command([cl, 'add', 'bundle', uuid2])
    ctx.collect_bundle(uuid2)
    # State after the above: 1 2 1 2
    _run_command([cl, 'detach', uuid1], 1)  # multiple indices
    _run_command([cl, 'detach', uuid1, '-n', '3'], 1)  # index out of range
    _run_command([cl, 'detach', uuid2, '-n', '2'])  # State: 1 1 2
    check_equals(get_info('^', 'uuid'), uuid2)
    _run_command([cl, 'detach', uuid2])  # State: 1 1
    check_equals(get_info('^', 'uuid'), uuid1)
    _run_command([cl, 'detach', uuid1, '-n', '2'])  # State: 1
    _run_command([cl, 'detach', uuid1])  # Worksheet becomes empty
    check_equals(
        '', _run_command([cl, 'ls', '-u'])
    )  # Return string from `cl ls -u` should be empty


@TestModule.register('perm')
def test(ctx):
    uuid = _run_command([cl, 'upload', test_path('a.txt')])
    check_equals('all', _run_command([cl, 'info', '-v', '-f', 'permission', uuid]))
    check_contains('none', _run_command([cl, 'perm', uuid, 'public', 'n']))
    check_contains('read', _run_command([cl, 'perm', uuid, 'public', 'r']))
    check_contains('all', _run_command([cl, 'perm', uuid, 'public', 'a']))


@TestModule.register('search')
def test(ctx):
    name = random_name()
    uuid1 = _run_command([cl, 'upload', test_path('a.txt'), '-n', name])
    uuid2 = _run_command([cl, 'upload', test_path('b.txt'), '-n', name])
    check_equals(uuid1, _run_command([cl, 'search', uuid1, '-u']))
    check_equals(uuid1, _run_command([cl, 'search', 'uuid=' + uuid1, '-u']))
    check_equals('', _run_command([cl, 'search', 'uuid=' + uuid1[0:8], '-u']))
    check_equals(uuid1, _run_command([cl, 'search', 'uuid=' + uuid1[0:8] + '.*', '-u']))
    check_equals(uuid1, _run_command([cl, 'search', 'uuid=' + uuid1[0:8] + '%', '-u']))
    check_equals(uuid1, _run_command([cl, 'search', 'uuid=' + uuid1, 'name=' + name, '-u']))
    check_equals(
        uuid1 + '\n' + uuid2, _run_command([cl, 'search', 'name=' + name, 'id=.sort', '-u'])
    )
    check_equals(
        uuid1 + '\n' + uuid2,
        _run_command([cl, 'search', 'uuid=' + uuid1 + ',' + uuid2, 'id=.sort', '-u']),
    )
    check_equals(
        uuid2 + '\n' + uuid1, _run_command([cl, 'search', 'name=' + name, 'id=.sort-', '-u'])
    )
    check_equals('2', _run_command([cl, 'search', 'name=' + name, '.count']))
    size1 = float(_run_command([cl, 'info', '-f', 'data_size', uuid1]))
    size2 = float(_run_command([cl, 'info', '-f', 'data_size', uuid2]))
    check_equals(
        size1 + size2, float(_run_command([cl, 'search', 'name=' + name, 'data_size=.sum']))
    )
    # Check search by group
    group_bname = random_name()
    group_buuid = _run_command([cl, 'run', 'echo hello', '-n', group_bname])
    wait(group_buuid)
    ctx.collect_bundle(group_buuid)
    user_id, user_name = current_user()
    # Create new group
    group_name = random_name()
    group_uuid_line = _run_command([cl, 'gnew', group_name])
    group_uuid = get_uuid(group_uuid_line)
    ctx.collect_group(group_uuid)
    # Make bundle unavailable to public but available to the group
    _run_command([cl, 'perm', group_buuid, 'public', 'n'])
    _run_command([cl, 'perm', group_buuid, group_name, 'r'])
    check_contains(group_buuid[:8], _run_command([cl, 'search', '.shared']))
    check_contains(group_buuid[:8], _run_command([cl, 'search', 'group={}'.format(group_uuid)]))
    check_contains(group_buuid[:8], _run_command([cl, 'search', 'group={}'.format(group_name)]))


@TestModule.register('search_time')
def test(ctx):
    name = random_name()
    time1 = datetime.now().isoformat()
    # These sleeps are required to ensure that there is sufficient time that passes between tests
    # If there is not enough time, all bundles might appear to have the same time
    time.sleep(1)
    uuid1 = run_command([cl, 'run', 'date', '-n', name])
    time.sleep(1)
    time2 = datetime.now().isoformat()
    time.sleep(1)
    uuid2 = run_command([cl, 'run', 'date', '-n', name])
    uuid3 = run_command([cl, 'run', 'date', '-n', name])
    time.sleep(1)
    time3 = datetime.now().isoformat()

    # No results
    check_equals('', run_command([cl, 'search', 'name=' + name, '.before=' + time1, '-u']))
    check_equals('', run_command([cl, 'search', 'name=' + name, '.after=' + time3, '-u']))

    # Before
    check_equals(
        uuid1, run_command([cl, 'search', 'name=' + name, '.before=' + time2, 'id=.sort', '-u'])
    )
    check_equals(
        uuid1 + '\n' + uuid2 + '\n' + uuid3,
        run_command([cl, 'search', 'name=' + name, '.before=' + time3, 'id=.sort', '-u']),
    )

    # After
    check_equals(
        uuid1 + '\n' + uuid2 + '\n' + uuid3,
        run_command([cl, 'search', 'name=' + name, '.after=' + time1, 'id=.sort', '-u']),
    )
    check_equals(
        uuid2 + '\n' + uuid3,
        run_command([cl, 'search', 'name=' + name, '.after=' + time2, 'id=.sort', '-u']),
    )

    # Before And After
    check_equals(
        uuid1,
        run_command(
            [cl, 'search', 'name=' + name, '.after=' + time1, '.before=' + time2, 'id=.sort', '-u']
        ),
    )
    check_equals(
        uuid2 + '\n' + uuid3,
        run_command(
            [cl, 'search', 'name=' + name, '.after=' + time2, '.before=' + time3, 'id=.sort', '-u']
        ),
    )


@TestModule.register('run')
def test(ctx):
    name = random_name()
    uuid = _run_command([cl, 'run', 'echo hello', '-n', name])
    wait(uuid)
    # test search
    check_contains(name, _run_command([cl, 'search', name]))
    check_equals(uuid, _run_command([cl, 'search', name, '-u']))
    _run_command([cl, 'search', name, '--append'])
    # test download stdout
    path = temp_path('')
    _run_command([cl, 'download', uuid + '/stdout', '-o', path])
    check_equals('hello', path_contents(path))
    # get info
    check_equals(State.READY, _run_command([cl, 'info', '-f', 'state', uuid]))
    check_contains(['run "echo hello"'], _run_command([cl, 'info', '-f', 'args', uuid]))
    check_equals('hello', _run_command([cl, 'cat', uuid + '/stdout']))
    # block
    # TODO: Uncomment this when the tail bug is figured out
    # check_contains('hello', _run_command([cl, 'run', 'echo hello', '--tail']))
    # invalid child path
    _run_command([cl, 'run', 'not/allowed:' + uuid, 'date'], expected_exit_code=1)
    # make sure special characters in the name of a bundle don't break
    special_name = random_name() + '-dashed.dotted'
    _run_command([cl, 'run', 'echo hello', '-n', special_name])
    dependent = _run_command([cl, 'run', ':%s' % special_name, 'cat %s/stdout' % special_name])
    wait(dependent)
    check_equals('hello', _run_command([cl, 'cat', dependent + '/stdout']))

    # test running with a reference to this worksheet
    source_worksheet_full = current_worksheet()
    source_worksheet_name = source_worksheet_full.split("::")[1]

    # Create new worksheet
    new_wname = random_name()
    new_wuuid = _run_command([cl, 'new', new_wname])
    ctx.collect_worksheet(new_wuuid)
    check_contains(['Switched', new_wname, new_wuuid], _run_command([cl, 'work', new_wuuid]))

    remote_name = random_name()
    remote_uuid = _run_command(
        [
            cl,
            'run',
            'source:{}//{}'.format(source_worksheet_name, name),
            "cat source/stdout",
            '-n',
            remote_name,
        ]
    )
    wait(remote_uuid)
    check_contains(remote_name, _run_command([cl, 'search', remote_name]))
    check_equals(remote_uuid, _run_command([cl, 'search', remote_name, '-u']))
    check_equals('hello', _run_command([cl, 'cat', remote_uuid + '/stdout']))

    sugared_remote_name = random_name()
    sugared_remote_uuid = _run_command(
        [
            cl,
            'run',
            'cat %{}//{}%/stdout'.format(source_worksheet_name, name),
            '-n',
            sugared_remote_name,
        ]
    )
    wait(sugared_remote_uuid)
    check_contains(sugared_remote_name, _run_command([cl, 'search', sugared_remote_name]))
    check_equals(sugared_remote_uuid, _run_command([cl, 'search', sugared_remote_name, '-u']))
    check_equals('hello', _run_command([cl, 'cat', sugared_remote_uuid + '/stdout']))

    # Explicitly fail when a remote instance name with : in it is supplied
    _run_command(
        [cl, 'run', 'cat %%%s//%s%%/stdout' % (source_worksheet_full, name)], expected_exit_code=1
    )

    # Test multiple keys pointing to the same bundle
    multi_alias_uuid = _run_command(
        [
            cl,
            'run',
            'foo:{}'.format(uuid),
            'foo1:{}'.format(uuid),
            'foo2:{}'.format(uuid),
            'echo "three aliases"',
        ]
    )
    wait(multi_alias_uuid)
    check_equals('three aliases', _run_command([cl, 'cat', multi_alias_uuid + '/stdout']))
    check_equals('hello', _run_command([cl, 'cat', multi_alias_uuid + '/foo/stdout']))
    check_equals('hello', _run_command([cl, 'cat', multi_alias_uuid + '/foo1/stdout']))
    check_equals('hello', _run_command([cl, 'cat', multi_alias_uuid + '/foo2/stdout']))


@TestModule.register('read')
def test(ctx):
    dep_uuid = _run_command([cl, 'upload', test_path('')])
    uuid = _run_command(
        [
            cl,
            'run',
            'dir:' + dep_uuid,
            'file:' + dep_uuid + '/a.txt',
            'ls dir; cat file; seq 1 10; touch done; while true; do sleep 60; done',
        ]
    )
    wait_until_state(uuid, State.RUNNING)

    # Tests reading first while the bundle is running and then after it is
    # killed.
    for running in [True, False]:
        # Wait for the output to appear. Also, tests cat on a directory.
        wait_for_contents(uuid, substring='done', timeout_seconds=60)

        # Info has only the first 10 lines
        info_output = _run_command([cl, 'info', uuid, '--verbose'])
        print(info_output)
        check_contains('a.txt', info_output)
        assert '5\n6\n7' not in info_output, 'info output should contain only first 10 lines'

        # Cat has everything.
        cat_output = _run_command([cl, 'cat', uuid + '/stdout'])
        check_contains('5\n6\n7', cat_output)
        check_contains('This is a simple text file for CodaLab.', cat_output)

        # Read a non-existant file.
        _run_command([cl, 'cat', uuid + '/unknown'], 1)

        # Dependencies should not be visible.
        dir_cat = _run_command([cl, 'cat', uuid])
        assert 'dir' not in dir_cat, '"dir" should not be in bundle'
        assert 'file' not in dir_cat, '"file" should not be in bundle'

        # You should be able to cat dependencies if specified directly
        dep_cat_output = _run_command([cl, 'cat', uuid + '/dir'])
        check_contains('-AmMDnVl4s8', dep_cat_output)
        dep_cat_output = _run_command([cl, 'cat', uuid + '/file'])
        check_contains('This is a simple text file for CodaLab.', dep_cat_output)

        # Download the whole bundle.
        path = temp_path('')
        _run_command([cl, 'download', uuid, '-o', path])
        assert not os.path.exists(
            os.path.join(path, 'dir')
        ), '"dir" should not be in downloaded bundle'
        assert not os.path.exists(
            os.path.join(path, 'file')
        ), '"file" should not be in downloaded bundle'
        with open(os.path.join(path, 'stdout')) as fileobj:
            check_contains('5\n6\n7', fileobj.read())
        shutil.rmtree(path)

        if running:
            _run_command([cl, 'kill', uuid])
            wait(uuid, 1)


@TestModule.register('kill')
def test(ctx):
    uuid = _run_command([cl, 'run', 'while true; do sleep 100; done'])
    wait_until_state(uuid, State.RUNNING)
    check_equals(uuid, _run_command([cl, 'kill', uuid]))
    _run_command([cl, 'wait', uuid], 1)
    _run_command([cl, 'wait', uuid], 1)
    check_equals(str(['kill']), get_info(uuid, 'actions'))


@TestModule.register('write')
def test(ctx):
    uuid = _run_command([cl, 'run', 'sleep 5'])
    wait_until_state(uuid, State.RUNNING)
    target = uuid + '/message'
    _run_command([cl, 'write', 'file with space', 'hello world'], 1)  # Not allowed
    check_equals(uuid, _run_command([cl, 'write', target, 'hello world']))
    _run_command([cl, 'wait', uuid])
    check_equals('hello world', _run_command([cl, 'cat', target]))
    check_equals(str(['write\tmessage\thello world']), get_info(uuid, 'actions'))


@TestModule.register('mimic')
def test(ctx):
    def data_hash(uuid):
        _run_command([cl, 'wait', uuid])
        return get_info(uuid, 'data_hash')

    simple_name = random_name()

    input_uuid = _run_command([cl, 'upload', test_path('a.txt'), '-n', simple_name + '-in1'])
    simple_out_uuid = _run_command([cl, 'make', input_uuid, '-n', simple_name + '-out'])

    new_input_uuid = _run_command([cl, 'upload', test_path('a.txt')])

    # Try three ways of mimicing, should all produce the same answer
    input_mimic_uuid = _run_command([cl, 'mimic', input_uuid, new_input_uuid, '-n', 'new'])
    check_equals(data_hash(simple_out_uuid), data_hash(input_mimic_uuid))

    full_mimic_uuid = _run_command(
        [cl, 'mimic', input_uuid, simple_out_uuid, new_input_uuid, '-n', 'new']
    )
    check_equals(data_hash(simple_out_uuid), data_hash(full_mimic_uuid))

    simple_macro_uuid = _run_command([cl, 'macro', simple_name, new_input_uuid, '-n', 'new'])
    check_equals(data_hash(simple_out_uuid), data_hash(simple_macro_uuid))

    complex_name = random_name()

    numbered_input_uuid = _run_command(
        [cl, 'upload', test_path('a.txt'), '-n', complex_name + '-in1']
    )
    named_input_uuid = _run_command(
        [cl, 'upload', test_path('b.txt'), '-n', complex_name + '-in-named']
    )
    out_uuid = _run_command(
        [
            cl,
            'make',
            'numbered:' + numbered_input_uuid,
            'named:' + named_input_uuid,
            '-n',
            complex_name + '-out',
        ]
    )

    new_numbered_input_uuid = _run_command([cl, 'upload', test_path('a.txt')])
    new_named_input_uuid = _run_command([cl, 'upload', test_path('b.txt')])

    # Try running macro with numbered and named inputs
    macro_out_uuid = _run_command(
        [
            cl,
            'macro',
            complex_name,
            new_numbered_input_uuid,
            'named:' + new_named_input_uuid,
            '-n',
            'new',
        ]
    )
    check_equals(data_hash(out_uuid), data_hash(macro_out_uuid))

    # Another basic test
    uuidA = _run_command([cl, 'upload', test_path('a.txt')])
    uuidB = _run_command([cl, 'upload', test_path('b.txt')])
    uuidCountA = _run_command([cl, 'run', 'input:' + uuidA, 'wc -l input'])
    uuidCountB = _run_command([cl, 'mimic', uuidA, uuidB])
    wait(uuidCountA)
    wait(uuidCountB)
    # Check that the line counts for a.txt and b.txt are correct
    check_contains('2', _run_command([cl, 'cat', uuidCountA + '/stdout']).split())
    check_contains('1', _run_command([cl, 'cat', uuidCountB + '/stdout']).split())


@TestModule.register('status')
def test(ctx):
    _run_command([cl, 'status'])
    _run_command([cl, 'alias'])
    help_output = _run_command([cl, 'help'])
    cl_output = _run_command([cl])
    check_contains("Commands for bundles", help_output)
    check_contains("Commands for bundles", cl_output)
    check_equals(cl_output, help_output)


@TestModule.register('batch')
def test(ctx):
    """Test batch resolution of bundle uuids"""
    wother = random_name()
    bnames = [random_name() for _ in range(2)]

    # Create worksheet and bundles
    wuuid = _run_command([cl, 'new', wother])
    ctx.collect_worksheet(wuuid)
    buuids = [
        _run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[0]]),
        _run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[1]]),
        _run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[0], '-w', wother]),
        _run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[1], '-w', wother]),
    ]

    # Test batch info call
    output = _run_command(
        [
            cl,
            'info',
            '-f',
            'uuid',
            bnames[0],
            bnames[1],
            '%s/%s' % (wother, bnames[0]),
            '%s/%s' % (wother, bnames[1]),
        ]
    )
    check_equals('\n'.join(buuids), output)

    # Test batch info call with combination of uuids and names
    output = _run_command([cl, 'info', '-f', 'uuid', buuids[0], bnames[0], bnames[0], buuids[0]])
    check_equals('\n'.join([buuids[0]] * 4), output)


@TestModule.register('resources')
def test(ctx):
    """Test whether resource constraints are respected"""
    uuid = _run_command([cl, 'upload', 'scripts/stress-test.pl'])

    def stress(
        use_time,
        request_time,
        use_memory,
        request_memory,
        use_disk,
        request_disk,
        expected_exit_code,
        expected_failure_message,
    ):
        run_uuid = _run_command(
            [
                cl,
                'run',
                'main.pl:' + uuid,
                'perl main.pl %s %s %s' % (use_time, use_memory, use_disk),
                '--request-time',
                str(request_time),
                '--request-memory',
                str(request_memory) + 'm',
                '--request-disk',
                str(request_disk) + 'm',
            ]
        )
        wait(run_uuid, expected_exit_code)
        if expected_failure_message:
            check_contains(expected_failure_message, get_info(run_uuid, 'failure_message'))

    # Good
    stress(
        use_time=1,
        request_time=10,
        use_memory=50,
        request_memory=1000,
        use_disk=10,
        request_disk=100,
        expected_exit_code=0,
        expected_failure_message=None,
    )

    # Too much time
    stress(
        use_time=10,
        request_time=1,
        use_memory=50,
        request_memory=1000,
        use_disk=10,
        request_disk=100,
        expected_exit_code=1,
        expected_failure_message='Time limit exceeded.',
    )

    # Too much memory
    # TODO(klopyrev): CircleCI doesn't seem to support cgroups, so we can't get
    # the memory usage of a Docker container.
    # stress(use_time=2, request_time=10, use_memory=1000, request_memory=50, use_disk=10, request_disk=100, expected_exit_code=1, expected_failure_message='Memory limit 50mb exceeded.')

    # Too much disk
    stress(
        use_time=2,
        request_time=10,
        use_memory=50,
        request_memory=1000,
        use_disk=10,
        request_disk=2,
        expected_exit_code=1,
        expected_failure_message='Disk limit 2mb exceeded.',
    )

    # Test network access
    wait(_run_command([cl, 'run', 'ping -c 1 google.com']), 1)
    wait(_run_command([cl, 'run', 'ping -c 1 google.com', '--request-network']), 0)


@TestModule.register('copy')
def test(ctx):
    def assert_bundles_ready(worksheet):
        _run_command([cl, 'work', worksheet])
        bundles = _run_command([cl, 'ls', '--uuid-only'])
        for uuid in bundles.split('\n'):
            wait_until_state(uuid, State.READY)

    """Test copying between instances."""
    source_worksheet = current_worksheet()

    with remote_instance(ctx.second_instance) as remote:

        def compare_output_across_instances(command):
            check_equals(
                _run_command(command + ['-w', source_worksheet]),
                _run_command(command + ['-w', remote_worksheet]),
            )

        remote_worksheet = remote.home
        print('Source worksheet: %s' % source_worksheet)
        print('Remote_worksheet: %s' % remote_worksheet)

        # Upload to original worksheet, transfer to remote
        _run_command([cl, 'work', source_worksheet])
        uuid = _run_command([cl, 'upload', test_path('')])
        _run_command([cl, 'add', 'bundle', uuid, '--dest-worksheet', remote_worksheet])
        compare_output_across_instances([cl, 'info', '-f', 'data_hash,name', uuid])
        # TODO: `cl cat` is not working even with the bundle available
        # compare_output_across_instances([cl, 'cat', uuid])

        # Upload to remote, transfer to local
        _run_command([cl, 'work', remote_worksheet])
        uuid = _run_command([cl, 'upload', test_path('')])
        _run_command([cl, 'add', 'bundle', uuid, '--dest-worksheet', source_worksheet])
        compare_output_across_instances([cl, 'info', '-f', 'data_hash,name', uuid])
        # compare_output_across_instances([cl, 'cat', uuid])

        # Upload to remote, transfer to local (metadata only)
        _run_command([cl, 'work', remote_worksheet])
        uuid = _run_command([cl, 'upload', '-c', 'hello'])
        _run_command([cl, 'rm', '-d', uuid])  # Keep only metadata
        _run_command([cl, 'add', 'bundle', uuid, '--dest-worksheet', source_worksheet])

        # Upload to local, transfer to remote (metadata only)
        _run_command([cl, 'work', source_worksheet])
        uuid = _run_command([cl, 'upload', '-c', 'hello'])
        _run_command([cl, 'rm', '-d', uuid])  # Keep only metadata
        _run_command([cl, 'add', 'bundle', uuid, '--dest-worksheet', remote_worksheet])

        # Create at local, transfer to remote (non-terminal state bundle)
        uuid = _run_command([cl, 'run', 'date', '--request-gpus', '100'])
        wait_until_state(uuid, State.STAGED)

        # Test adding worksheet items
        _run_command([cl, 'wadd', source_worksheet, remote_worksheet])
        _run_command([cl, 'wadd', remote_worksheet, source_worksheet])
        assert_bundles_ready(source_worksheet)
        assert_bundles_ready(remote_worksheet)


@TestModule.register('groups')
def test(ctx):
    # Should not crash
    _run_command([cl, 'ginfo', 'public'])

    user_id, user_name = current_user()
    # Create new group
    group_name = random_name()
    group_uuid_line = _run_command([cl, 'gnew', group_name])
    group_uuid = get_uuid(group_uuid_line)
    ctx.collect_group(group_uuid)

    # Check that you are added to your own group
    group_info = _run_command([cl, 'ginfo', group_name])
    check_contains(user_name, group_info)
    my_groups = _run_command([cl, 'gls'])
    check_contains(group_name, my_groups)

    # Try to relegate yourself to non-admin status
    _run_command([cl, 'uadd', user_name, group_name], expected_exit_code=1)

    # TODO: Test other group membership semantics:
    # - removing a group
    # - adding new members
    # - adding an admin
    # - converting member to admin
    # - converting admin to member
    # - permissioning


@TestModule.register('netcat')
def test(ctx):
    script_uuid = _run_command([cl, 'upload', test_path('netcat-test.py')])
    uuid = _run_command([cl, 'run', 'netcat-test.py:' + script_uuid, 'python netcat-test.py'])
    wait_until_state(uuid, State.RUNNING)
    time.sleep(5)
    output = _run_command([cl, 'netcat', uuid, '5005', '---', 'hi patrick'])
    check_equals('No, this is dawg', output)

    uuid = _run_command([cl, 'run', 'netcat-test.py:' + script_uuid, 'python netcat-test.py'])
    wait_until_state(uuid, State.RUNNING)
    time.sleep(5)
    output = _run_command([cl, 'netcat', uuid, '5005', '---', 'yo dawg!'])
    check_equals('Hi this is dawg', output)


@TestModule.register('netcurl')
def test(ctx):
    uuid = _run_command([cl, 'run', 'echo hello > hello.txt; python -m SimpleHTTPServer'])
    wait_until_state(uuid, State.RUNNING)
    address = ctx.client.address
    check_equals(
        'hello',
        _run_command(['curl', '{}/rest/bundles/{}/netcurl/8000/hello.txt'.format(address, uuid)]),
    )


@TestModule.register('anonymous')
def test(ctx):
    # Should not crash
    # TODO: multi-user tests that check that owner is hidden for anonymous objects
    _run_command([cl, 'wedit', '--anonymous'])
    _run_command([cl, 'wedit', '--not-anonymous'])
    uuid = _run_command([cl, 'upload', test_path('a.txt')])
    _run_command([cl, 'edit', '--anonymous', uuid])
    _run_command([cl, 'edit', '--not-anonymous', uuid])


@TestModule.register('docker', default=False)
def test(ctx):
    """
    Placeholder for tests for default Codalab docker images
    """
    uuid = _run_command(
        [cl, 'run', '--request-docker-image=codalab/default-cpu:latest', 'python --version']
    )
    wait(uuid)
    check_contains('2.7', _run_command([cl, 'cat', uuid + '/stderr']))
    uuid = _run_command(
        [cl, 'run', '--request-docker-image=codalab/default-cpu:latest', 'python3 --version']
    )
    wait(uuid)
    check_contains('3.6', _run_command([cl, 'cat', uuid + '/stdout']))
    uuid = _run_command(
        [
            cl,
            'run',
            '--request-docker-image=codalab/default-cpu:latest',
            'python -c "import tensorflow"',
        ]
    )
    wait(uuid)
    uuid = _run_command(
        [cl, 'run', '--request-docker-image=codalab/default-cpu:latest', 'python -c "import torch"']
    )
    wait(uuid)
    uuid = _run_command(
        [cl, 'run', '--request-docker-image=codalab/default-cpu:latest', 'python -c "import numpy"']
    )
    wait(uuid)
    uuid = _run_command(
        [cl, 'run', '--request-docker-image=codalab/default-cpu:latest', 'python -c "import nltk"']
    )
    wait(uuid)
    uuid = _run_command(
        [cl, 'run', '--request-docker-image=codalab/default-cpu:latest', 'python -c "import spacy"']
    )
    wait(uuid)
    uuid = _run_command(
        [
            cl,
            'run',
            '--request-docker-image=codalab/default-cpu:latest',
            'python -c "import matplotlib"',
        ]
    )
    wait(uuid)
    uuid = _run_command(
        [
            cl,
            'run',
            '--request-docker-image=codalab/default-cpu:latest',
            'python3 -c "import tensorflow"',
        ]
    )
    wait(uuid)
    uuid = _run_command(
        [
            cl,
            'run',
            '--request-docker-image=codalab/default-cpu:latest',
            'python3 -c "import torch"',
        ]
    )
    wait(uuid)
    uuid = _run_command(
        [
            cl,
            'run',
            '--request-docker-image=codalab/default-cpu:latest',
            'python3 -c "import numpy"',
        ]
    )
    wait(uuid)
    uuid = _run_command(
        [cl, 'run', '--request-docker-image=codalab/default-cpu:latest', 'python3 -c "import nltk"']
    )
    wait(uuid)
    uuid = _run_command(
        [
            cl,
            'run',
            '--request-docker-image=codalab/default-cpu:latest',
            'python3 -c "import spacy"',
        ]
    )
    wait(uuid)
    uuid = _run_command(
        [
            cl,
            'run',
            '--request-docker-image=codalab/default-cpu:latest',
            'python3 -c "import matplotlib"',
        ]
    )
    wait(uuid)
    pass


@TestModule.register('competition')
def test(ctx):
    """Sanity-check the competition script."""
    submit_tag = 'submit'
    eval_tag = 'eval'
    log_worksheet_uuid = _run_command([cl, 'work', '-u'])
    devset_uuid = _run_command([cl, 'upload', test_path('a.txt')])
    testset_uuid = _run_command([cl, 'upload', test_path('b.txt')])
    script_uuid = _run_command([cl, 'upload', test_path('evaluate.sh')])
    _run_command(
        [
            cl,
            'run',
            'dataset.txt:' + devset_uuid,
            'echo dataset.txt > predictions.txt',
            '--tags',
            submit_tag,
        ]
    )

    config_file = temp_path('-competition-config.json')
    with open(config_file, 'w') as fp:
        json.dump(
            {
                "host": ctx.instance,
                "username": 'codalab',
                "password": 'codalab',
                "log_worksheet_uuid": log_worksheet_uuid,
                "submission_tag": submit_tag,
                "predict": {"mimic": [{"old": devset_uuid, "new": testset_uuid}], "tag": "predict"},
                "evaluate": {
                    "dependencies": [
                        {"parent_uuid": script_uuid, "child_path": "evaluate.sh"},
                        {
                            "parent_uuid": "{predict}",
                            "parent_path": "predictions.txt",
                            "child_path": "predictions.txt",
                        },
                    ],
                    "command": "cat predictions.txt | bash evaluate.sh",
                    "tag": eval_tag,
                },
                "score_specs": [{"name": "goodness", "key": "/stdout:goodness"}],
                "metadata": {"name": "Cool Competition Leaderboard"},
            },
            fp,
        )

    out_file = temp_path('-competition-out.json')
    try:
        _run_command(['cl-competitiond', config_file, out_file, '--verbose'])

        # Check that eval bundle gets created
        results = _run_command([cl, 'search', 'tags=' + eval_tag, '-u'])
        check_equals(1, len(results.splitlines()))
    finally:
        os.remove(config_file)
        os.remove(out_file)


@TestModule.register('unicode')
def test(ctx):
    # Non-unicode in worksheet title
    wuuid = _run_command([cl, 'new', random_name()])

    _run_command([cl, 'wedit', wuuid, '--title', 'nonunicode'])
    check_contains('nonunicode', _run_command([cl, 'print', wuuid]))

    # unicode in worksheet title
    _run_command([cl, 'wedit', wuuid, '--title', 'fáncy ünicode 你好世界😊'], 0)
    check_contains('fáncy ünicode 你好世界😊', _run_command([cl, 'print', wuuid]))

    # Non-unicode in file contents
    uuid = _run_command([cl, 'upload', '--contents', 'nounicode'])
    check_equals('nounicode', _run_command([cl, 'cat', uuid]))

    # Unicode in file contents
    uuid = _run_command([cl, 'upload', '--contents', '你好世界😊'])
    check_equals('_', get_info(uuid, 'name'))
    check_equals('你好世界😊', _run_command([cl, 'cat', uuid]))

    # Unicode in bundle description, tags and command
    # TODO: enable with Unicode support.
    uuid = _run_command([cl, 'upload', test_path('a.txt'), '--description', '你好'], 1)
    # check_equals('你好', get_info(uuid, 'description'))
    uuid = _run_command([cl, 'upload', test_path('a.txt'), '--tags', 'test', '😁'], 1)
    # check_contains(['test', '😁'], get_info(uuid, 'tags'))
    uuid = _run_command([cl, 'run', 'echo "fáncy ünicode"'], 1)

    # edit description with unicode
    uuid = _run_command([cl, 'upload', test_path('a.txt')])
    _run_command([cl, 'edit', uuid, '-d', '你好世界😊'], 1)
    # check_equals('你好世界😊', get_info(uuid, 'description'))


@TestModule.register('workers')
def test(ctx):
    # Run workers command
    result = _run_command([cl, 'workers'])
    lines = result.split("\n")

    # Output should contain at least 3 lines as following:
    # worker_id        cpus  gpus  memory  free_disk  last_checkin  tag  runs
    # -----------------------------------------------------------------------
    # 7a343e1015c7(1)  0/2   0/0   2.0g    32.9g      2.0s ago
    check_equals(True, len(lines) >= 3)

    # Check header which includes 8 columns in total from output.
    header = lines[0]
    check_contains(
        [
            'worker_id',
            'cpus',
            'gpus',
            'memory',
            'free_disk',
            'last_checkin',
            'tag',
            'runs',
            'shared_file_system',
            'tag_exclusive',
        ],
        header,
    )

    # Check number of not null values. First 7 columns should be not null. Column "tag" and "runs" could be empty.
    worker_info = lines[2].split()
    check_equals(True, len(worker_info) >= 8)


@TestModule.register('rest1')
def test(ctx):
    """
    Call REST APIs.  Most things should be captured by CLI commands, but add things here that aren't.
    """
    # Basic getting info and blob contents of a bundle
    path = test_path('a.txt')
    uuid = _run_command([cl, 'upload', path])
    response = ctx.client.fetch_contents_info(BundleTarget(uuid, ''))
    check_equals(response['name'], uuid)
    check_equals(
        open(path, 'rb').read(), ctx.client.fetch_contents_blob(BundleTarget(uuid, '')).read()
    )

    # Display image - should not crash
    wuuid = _run_command([cl, 'work', '-u'])
    uuid = _run_command([cl, 'upload', test_path('codalab.png')])
    _run_command([cl, 'add', 'text', '% display image / width=800'])
    _run_command([cl, 'add', 'bundle', uuid])
    response = ctx.client.fetch_interpreted_worksheet(wuuid)
    check_equals(response['uuid'], wuuid)


@TestModule.register('worksheets')
def test(ctx):
    # Create a comprehensive worksheet and test the output of cl print
    test_worksheet = SampleWorksheet(cl)
    test_worksheet.create()
    test_worksheet.test_print()


@TestModule.register('memoize')
def test(ctx):
    # Case 1: no dependency
    uuid = _run_command([cl, 'run', 'echo hello'])
    wait(uuid)
    check_equals('hello', _run_command([cl, 'cat', uuid + '/stdout']))
    uuid1 = _run_command([cl, 'run', 'echo hello2'])
    wait(uuid1)
    check_equals('hello2', _run_command([cl, 'cat', uuid1 + '/stdout']))
    # memo tests
    check_equals(uuid, _run_command([cl, 'run', 'echo hello', '--memoize']))

    # Case 2: single dependency
    # target_spec: ':<uuid>'
    uuid_dep = _run_command([cl, 'run', ':{}'.format(uuid), 'echo hello'])
    wait(uuid_dep)
    check_equals('hello', _run_command([cl, 'cat', uuid_dep + '/stdout']))
    # memo tests
    check_equals(uuid_dep, _run_command([cl, 'run', ':{}'.format(uuid), 'echo hello', '--memoize']))

    # Case 3: multiple dependencies without key
    # target_spec: ':<uuid_1> :<uuid_2>'
    uuid_deps = _run_command(
        [cl, 'run', ':{}'.format(uuid), ':{}'.format(uuid1), 'echo multi_deps']
    )
    wait(uuid_deps)
    check_equals('multi_deps', _run_command([cl, 'cat', uuid_deps + '/stdout']))
    # memo tests
    check_equals(
        uuid_deps,
        _run_command(
            [cl, 'run', ':{}'.format(uuid), ':{}'.format(uuid1), 'echo multi_deps', '--memoize']
        ),
    )

    # Case 4: multiple key points to the same bundle
    # target_spec: 'foo:<uuid> foo1:<uuid>'
    uuid_multi_alias = _run_command(
        [cl, 'run', 'foo:{}'.format(uuid), 'foo1:{}'.format(uuid), 'echo hello']
    )
    wait(uuid_multi_alias)
    check_equals('hello', _run_command([cl, 'cat', uuid_multi_alias + '/stdout']))
    # memo tests
    check_equals(
        uuid_multi_alias,
        _run_command(
            [cl, 'run', 'foo:{}'.format(uuid), 'foo1:{}'.format(uuid), 'echo hello', '--memoize']
        ),
    )

    # Case 5: duplicate dependencies
    # target_spec: ':<uuid> :<uuid>'
    check_equals(
        uuid_dep,
        _run_command(
            [cl, 'run', ':{}'.format(uuid), ':{}'.format(uuid), 'echo hello', '--memoize']
        ),
    )

    # Case 6: multiple dependencies
    # target_spec: 'a:<uuid_1> b:<uuid_2>'
    uuid_a_b = _run_command([cl, 'run', 'a:{}'.format(uuid), 'b:{}'.format(uuid1), 'echo a_b'])
    wait(uuid_a_b)
    check_equals('a_b', _run_command([cl, 'cat', uuid_a_b + '/stdout']))

    # target_spec: 'a:<uuid_1> b:<uuid_2>'
    uuid_a_bb = _run_command([cl, 'run', 'a:{}'.format(uuid), 'b:{}'.format(uuid1), 'echo a_bb'])
    wait(uuid_a_bb)
    check_equals('a_bb', _run_command([cl, 'cat', uuid_a_bb + '/stdout']))

    # target_spec: 'b:<uuid_1> a:<uuid_2>'
    uuid_b_a = _run_command([cl, 'run', 'b:{}'.format(uuid), 'a:{}'.format(uuid1), 'echo b_a'])
    wait(uuid_b_a)
    check_equals('b_a', _run_command([cl, 'cat', uuid_b_a + '/stdout']))

    # target_spec: 'a:<uuid_1> b:<uuid_2> c:<uuid_3>'
    uuid_a_b_c = _run_command(
        [
            cl,
            'run',
            'a:{}'.format(uuid),
            'b:{}'.format(uuid1),
            'c:{}'.format(uuid_dep),
            'echo a_b_c',
        ]
    )
    wait(uuid_a_b_c)
    check_equals('a_b_c', _run_command([cl, 'cat', uuid_a_b_c + '/stdout']))

    # memo tests
    check_equals(
        uuid_a_b,
        _run_command(
            [cl, 'run', 'a:{}'.format(uuid), 'b:{}'.format(uuid1), 'echo a_b', '--memoize']
        ),
    )

    check_equals(
        uuid_a_bb,
        _run_command(
            [cl, 'run', 'a:{}'.format(uuid), 'b:{}'.format(uuid1), 'echo a_bb', '--memoize']
        ),
    )

    check_equals(
        uuid_b_a,
        _run_command(
            [cl, 'run', 'b:{}'.format(uuid), 'a:{}'.format(uuid1), 'echo b_a', '--memoize']
        ),
    )

    check_equals(
        uuid_a_b_c,
        _run_command(
            [
                cl,
                'run',
                'a:{}'.format(uuid),
                'b:{}'.format(uuid1),
                'c:{}'.format(uuid_dep),
                'echo a_b_c',
                '--memoize',
            ]
        ),
    )

    check_not_equals(
        uuid_a_b_c,
        _run_command(
            [
                cl,
                'run',
                'b:{}'.format(uuid),
                'a:{}'.format(uuid1),
                'd:{}'.format(uuid_dep),
                'echo a_b_d',
                '--memoize',
            ]
        ),
    )

    # test different dependency order in target_spec: 'a:<uuid_2> b:<uuid_1>'
    check_equals(
        uuid_b_a,
        _run_command(
            [cl, 'run', 'a:{}'.format(uuid1), 'b:{}'.format(uuid), 'echo b_a', '--memoize']
        ),
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs the specified CodaLab worksheets unit and integration tests against the specified CodaLab instance (defaults to localhost)'
    )
    parser.add_argument(
        '--cl-executable',
        type=str,
        help='Path to codalab CLI executable, defaults to "cl"',
        default='cl',
    )
    parser.add_argument(
        '--instance',
        type=str,
        help='CodaLab instance to run tests against, defaults to "localhost"',
        default='localhost',
    )
    parser.add_argument(
        '--second-instance',
        type=str,
        help='Another CodaLab instance used for tests that require a second instance, defaults to "localhost"',
        default='localhost',
    )
    parser.add_argument(
        '--cl-version',
        type=str,
        help='CodaLab version to use for multi-instance tests, defaults to "latest"',
        default='latest',
    )
    parser.add_argument(
        'tests',
        metavar='TEST',
        nargs='+',
        type=str,
        choices=list(TestModule.modules.keys()) + ['all', 'default'],
        help='Tests to run from: {%(choices)s}',
    )

    args = parser.parse_args()
    cl = args.cl_executable
    cl_version = args.cl_version
    success = TestModule.run(args.tests, args.instance, args.second_instance)
    if not success:
        sys.exit(1)
