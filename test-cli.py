#!/usr/bin/env python
'''
Tests all the CLI functionality end-to-end.

Tests will operate on temporary worksheets created during testing.  In theory, it
shouldn't mutate preexisting data on your instance, but this is not guaranteed,
and you should run this command in an unimportant CodaLab account.

For full coverage of testing, be sure to run this over a remote connection (i.e. while
connected to localhost::) in addition to local testing, in order to test the full RPC
pipeline, and also as a non-root user, to hammer out unanticipated permission issues.

Things not tested:
- Interactive modes (cl edit, cl wedit)
- Permissions
- Worker system
'''

import subprocess
import sys
import re
import os
import shutil
import random
import time
import traceback
from collections import OrderedDict

cl = 'cl'
base_path = os.path.dirname(os.path.abspath(__file__))  # Directory where this script lives.

crazy_name = 'crazy ("ain\'t it?")'

def test_path(name):
    """
    Return the path to the test file |name|.
    """
    return os.path.join(base_path, 'tests', 'files', name)

# Note: when we talk about contents, we always apply rstrip() even if it's a
# binary file.  This is fine as long as we're consistent about doing rstrip()
# everywhere to test for equality.
def test_path_contents(name):
    return path_contents(test_path(name))

def path_contents(path):
    return open(path).read().rstrip()

def temp_path(suffix):
    return os.path.join(base_path, random_name() + suffix)

def random_name():
    return 'temp-test-cli-' + str(random.randint(0, 1000000))

def sanitize(string):
    try:
        string = string.decode('utf-8')
        n = 256
        if len(string) > n:
            string = string[:n] + ' (...more...)'
        return string
    except UnicodeDecodeError:
        return '<binary>\n'

def run_command(args, expected_exit_code=0):
    try:
        output = subprocess.check_output(args)
        exitcode = 0
    except subprocess.CalledProcessError, e:
        output = e.output
        exitcode = e.returncode
    print '>> %s (exit code %s, expected %s)\n%s' % (args, exitcode, expected_exit_code, sanitize(output))
    assert expected_exit_code == exitcode, 'Exit codes don\'t match'
    return output.rstrip()

def get_info(uuid, key):
    return run_command([cl, 'info', '-f', key, uuid])

def wait(uuid):
    run_command([cl, 'wait', uuid])

def check_equals(true_value, pred_value):
    assert true_value == pred_value, "expected '%s', but got '%s'" % (true_value, pred_value)
    return pred_value

def check_contains(true_value, pred_value):
    if isinstance(true_value, list):
        for v in true_value:
            check_contains(v, pred_value)
    else:
        assert (true_value in pred_value or re.search(true_value, pred_value)), \
            "expected something that contains '%s', but got '%s'" % (true_value, pred_value)
    return pred_value

def check_num_lines(true_value, pred_value):
    num_lines = len(pred_value.split('\n'))
    assert num_lines == true_value, "expected %d lines, but got %s" % (true_value, num_lines)
    return pred_value

class ModuleContext(object):
    '''ModuleContext objects manage the context of a test module.

    Instances of ModuleContext are meant to be used with the Python
    'with' statement (PEP 343).

    For documentation on with statement context managers:
    https://docs.python.org/2/reference/datamodel.html#with-statement-context-managers
    '''
    def __init__(self):
        # These are the temporary worksheets and bundles that need to be
        # cleaned up at the end of the test.
        self.worksheets = []
        self.bundles = []
        self.error = None

    def __enter__(self):
        '''Prepares clean environment for test module.'''
        print 'SWITCHING TO TEMPORARY WORKSHEET'
        print

        self.original_worksheet = run_command([cl, 'work', '-u'])
        temp_worksheet = run_command([cl, 'new', random_name()])
        self.worksheets.append(temp_worksheet)
        run_command([cl, 'work', temp_worksheet])

        print 'BEGIN TEST'
        print

        return self

    def __exit__(self, exc_type, exc_value, tb):
        '''Tears down temporary environment for test module.'''
        # Check for and handle exceptions if any
        if exc_type is not None:
            self.error = (exc_type, exc_value, tb)
            if exc_type is AssertionError:
                print 'ERROR: %s' % exc_value.message
            else:
                print 'ERROR: Test raised an exception!'
                traceback.print_exception(exc_type, exc_value, tb)
        else:
            print 'TEST PASSED'
        print

        # Clean up and restore original worksheet
        print 'CLEANING UP'
        print
        run_command([cl, 'work', self.original_worksheet])
        for worksheet in self.worksheets:
            self.bundles.extend(run_command([cl, 'ls', '-w', worksheet, '-u']).split())
            run_command([cl, 'wrm', '--force', worksheet])

        # Delete all bundles (dedup first)
        if len(self.bundles) > 0:
            run_command([cl, 'rm', '--force'] + list(set(self.bundles)))

        # Do not reraise exception
        return True

    def collect_worksheet(self, uuid):
        '''Mark a worksheet for cleanup on exit.'''
        self.worksheets.append(uuid)

    def collect_bundle(self, uuid):
        '''Mark a bundle for cleanup on exit.'''
        self.bundles.append(uuid)

class TestModule(object):
    '''Instances of TestModule each encapsulate a test module and its metadata.

    The class itself also maintains a registry of the existing modules, providing
    a decorator to register new modules and a class method to run modules by name.
    '''
    modules = OrderedDict()

    def __init__(self, name, func, description):
        self.name = name
        self.func = func
        self.description = description

    @classmethod
    def register(cls, name):
        '''Returns a decorator to register new test modules.

        The decorator will add a given function as test modules to the registry
        under the name provided here. The function's docstring (PEP 257) will
        be used as the prose description of the test module.
        '''
        def add_module(func):
            cls.modules[name] = TestModule(name, func, func.__doc__)
        return add_module

    @classmethod
    def run(cls, query):
        '''Run the modules named in query.

        query should be a list of strings, each of which is either 'all'
        or the name of an existing test module.
        '''
        # Build list of modules to run based on query
        modules_to_run = []
        for name in query:
            if name == 'all':
                modules_to_run.extend(cls.modules.values())
            elif name in cls.modules:
                modules_to_run.append(cls.modules[name])
            else:
                print 'Could not find module %s' % name
                print 'Modules: all ' + ' '.join(cls.modules.keys())
                sys.exit(1)

        print 'Running modules ' + ' '.join([m.name for m in modules_to_run])
        print

        # Run modules, continuing onto the next test module regardless of failure
        failed = []
        for module in modules_to_run:
            print '============= BEGIN MODULE: ' + module.name
            if module.description is not None:
                print 'DESCRIPTION: ' + module.description
            print

            with ModuleContext() as ctx:
                module.func(ctx)

            if ctx.error:
                failed.append(module.name)

        # Provide a (currently very rudimentary) summary
        print '============= SUMMARY'
        if failed:
            print 'Tests failed: %s' % ', '.join(failed)
        else:
            print 'All tests passed.'

############################################################

@TestModule.register('unittest')
def test(ctx):
    '''Run nose unit tests'''
    run_command(['venv/bin/nosetests'])

@TestModule.register('basic')
def test(ctx):
    # upload
    uuid = run_command([cl, 'upload', test_path('a.txt'), '--description', 'hello', '--tags', 'a', 'b'])
    check_equals('a.txt', get_info(uuid, 'name'))
    check_equals('hello', get_info(uuid, 'description'))
    check_contains(['a', 'b'], get_info(uuid, 'tags'))
    check_equals('ready', get_info(uuid, 'state'))
    check_equals('ready\thello', get_info(uuid, 'state,description'))

    # edit
    run_command([cl, 'edit', uuid, '--name', 'a2.txt'])
    check_equals('a2.txt', get_info(uuid, 'name'))

    # cat, info
    check_equals(test_path_contents('a.txt'), run_command([cl, 'cat', uuid]))
    check_contains(['bundle_type', 'uuid', 'owner', 'created'], run_command([cl, 'info', uuid]))
    check_contains('license', run_command([cl, 'info', '--raw', uuid]))
    check_contains(['host_worksheets', 'contents'], run_command([cl, 'info', '--verbose', uuid]))

    # rm
    run_command([cl, 'rm', '--dry_run', uuid])
    check_contains('0x', get_info(uuid, 'data_hash'))
    run_command([cl, 'rm', '--data_only', uuid])
    check_equals('None', get_info(uuid, 'data_hash'))
    run_command([cl, 'rm', uuid])

@TestModule.register('upload1')
def test(ctx):
    # Upload contents
    uuid = run_command([cl, 'upload', '-c', 'hello'])
    check_equals('hello', run_command([cl, 'cat', uuid]))

    # Upload binary file
    uuid = run_command([cl, 'upload', test_path('echo')])
    check_equals(test_path_contents('echo'), run_command([cl, 'cat', uuid]))

    # Upload file with crazy name
    uuid = run_command([cl, 'upload', test_path(crazy_name)])
    check_equals(test_path_contents(crazy_name), run_command([cl, 'cat', uuid]))

    # Upload symlink
    uuid = run_command([cl, 'upload', test_path('passwd')])
    run_command([cl, 'cat', uuid], 1)  # Should not resolve this - otherwise it's dangerous!

    # Upload symlink, follow link
    uuid = run_command([cl, 'upload', test_path('a-symlink.txt'), '--follow_symlinks'])
    check_equals(test_path_contents('a-symlink.txt'), run_command([cl, 'cat', uuid]))
    run_command([cl, 'cat', uuid])  # Should have the full contents

    # Upload broken symlink (should be possible)
    uuid = run_command([cl, 'upload', test_path('broken-symlink')])
    run_command([cl, 'cat', uuid], 1)

    # Upload directory with excluded files
    uuid = run_command([cl, 'upload', test_path('dir1'), '--exclude_patterns', 'f*'])
    check_num_lines(2 + 1, run_command([cl, 'cat', uuid]))  # 2 lines header,oOnly one file left after excluding

@TestModule.register('upload2')
def test(ctx):
    # Upload tar.gz and zip.
    for suffix in ['.tar.gz', '.zip']:
        # Pack it up
        archive_path = temp_path(suffix)
        contents_path = test_path('dir1')
        if suffix == '.tar.gz':
            run_command(['tar', 'cfz', archive_path, '-C', os.path.dirname(contents_path), os.path.basename(contents_path)])
        else:
            run_command(['bash', '-c', 'cd %s && zip -r %s %s' % (os.path.dirname(contents_path), archive_path, os.path.basename(contents_path))])

        # Upload it and unpack
        uuid = run_command([cl, 'upload', archive_path])
        check_equals(os.path.basename(archive_path).replace(suffix, ''), get_info(uuid, 'name'))
        check_equals(test_path_contents('dir1/f1'), run_command([cl, 'cat', uuid + '/f1']))

        # Upload it but don't unpack
        uuid = run_command([cl, 'upload', archive_path, '--pack'])
        check_equals(os.path.basename(archive_path), get_info(uuid, 'name'))
        check_equals(test_path_contents(archive_path), run_command([cl, 'cat', uuid]))

        os.unlink(archive_path)

@TestModule.register('upload3')
def test(ctx):
    # Upload URL
    uuid = run_command([cl, 'upload', 'https://www.wikipedia.org'])
    check_contains('<title>Wikipedia</title>', run_command([cl, 'cat', uuid]))

    # Upload URL that's an archive
    uuid = run_command([cl, 'upload', 'http://alpha.gnu.org/gnu/bc/bc-1.06.95.tar.bz2'])
    check_contains(['README', 'INSTALL', 'FAQ'], run_command([cl, 'cat', uuid]))

    # Upload URL from Git
    uuid = run_command([cl, 'upload', 'https://github.com/codalab/codalab-cli', '--git'])
    check_contains(['README.md', 'codalab', 'scripts'], run_command([cl, 'cat', uuid]))


@TestModule.register('download')
def test(ctx):
    uuid = run_command([cl, 'upload', test_path('')])

    # Download whole bundle
    path = temp_path('')
    run_command([cl, 'download', uuid, '-o', path])
    check_contains(['a.txt', 'b.txt', 'echo', crazy_name], run_command(['ls', '-R', path]))
    shutil.rmtree(path)

    # Download a target inside (binary)
    run_command([cl, 'download', uuid + '/echo', '-o', path])
    check_equals(test_path_contents('echo'), path_contents(path))
    os.unlink(path)

    # Download a target inside (crazy name)
    run_command([cl, 'download', uuid + '/' + crazy_name, '-o', path])
    check_equals(test_path_contents(crazy_name), path_contents(path))
    os.unlink(path)

    # Download a target inside (symlink)
    run_command([cl, 'download', uuid + '/a-symlink.txt', '-o', path], 1)  # Disallow symlinks

    # Download a target inside (directory)
    run_command([cl, 'download', uuid + '/dir1', '-o', path])
    check_equals(test_path_contents('dir1/f1'), path_contents(path + '/f1'))
    shutil.rmtree(path)

    # Download something that doesn't exist
    run_command([cl, 'download', 'not-exists'], 1)
    run_command([cl, 'download', uuid + '/not-exists'], 1)

@TestModule.register('refs')
def test(ctx):
    # Test references
    uuid = run_command([cl, 'upload', test_path('a.txt')])
    wuuid = run_command([cl, 'work', '-u'])
    # Compound bundle references
    run_command([cl, 'info', wuuid + '/' + uuid])
    # . is current worksheet
    check_contains(wuuid, run_command([cl, 'ls', '-w', '.']))
    # / is home worksheet
    check_contains('::home-', run_command([cl, 'ls', '-w', '/']))

@TestModule.register('rm')
def test(ctx):
    uuid = run_command([cl, 'upload', test_path('a.txt')])
    run_command([cl, 'add', 'bundle', uuid, '.'])  # Duplicate
    run_command([cl, 'rm', uuid])  # Can delete even though it exists twice on the same worksheet

@TestModule.register('make')
def test(ctx):
    uuid1 = run_command([cl, 'upload', test_path('a.txt')])
    uuid2 = run_command([cl, 'upload', test_path('b.txt')])
    # make
    uuid3 = run_command([cl, 'make', 'dep1:'+uuid1, 'dep2:'+uuid2])
    wait(uuid3)
    check_equals('ready', run_command([cl, 'info', '-f', 'state', uuid3]))
    check_contains(['dep1', uuid1, 'dep2', uuid2], run_command([cl, 'info', uuid3]))
    # anonymous make
    uuid4 = run_command([cl, 'make', uuid3, '--name', 'foo'])
    wait(uuid4)
    check_equals('ready', run_command([cl, 'info', '-f', 'state', uuid4]))
    check_contains([uuid3], run_command([cl, 'info', uuid3]))
    # Cleanup
    run_command([cl, 'rm', uuid1], 1)  # should fail
    run_command([cl, 'rm', '--force', uuid2])  # force the deletion
    run_command([cl, 'rm', '-r', uuid1])  # delete things downstream

@TestModule.register('run')
def test(ctx):
    name = random_name()
    uuid = run_command([cl, 'run', 'echo hello', '-n', name])
    wait(uuid)
    # test search
    check_contains(name, run_command([cl, 'search', name]))
    check_equals(uuid, run_command([cl, 'search', name, '-u']))
    run_command([cl, 'search', name, '--append'])
    # get info
    check_equals('ready', run_command([cl, 'info', '-f', 'state', uuid]))
    check_contains(['run "echo hello"'], run_command([cl, 'info', '-f', 'args', uuid]))
    check_equals('hello', run_command([cl, 'cat', uuid+'/stdout']))
    # block
    uuid2 = check_contains('hello', run_command([cl, 'run', 'echo hello', '--tail'])).split('\n')[0]
    # cleanup
    run_command([cl, 'rm', uuid, uuid2])

@TestModule.register('worksheet')
def test(ctx):
    wname = random_name()
    # Create new worksheet
    wuuid = run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    check_contains(['Switched', wname, wuuid], run_command([cl, 'work', wuuid]))
    # ls
    check_equals('', run_command([cl, 'ls', '-u']))
    uuid = run_command([cl, 'upload', test_path('a.txt')])
    check_equals(uuid, run_command([cl, 'ls', '-u']))
    # create worksheet
    check_contains(uuid[0:5], run_command([cl, 'ls']))
    run_command([cl, 'add', 'text', 'testing', '.'])
    run_command([cl, 'add', 'text', '% display contents / maxlines=10', '.'])
    run_command([cl, 'add', 'bundle', uuid, '.'])
    run_command([cl, 'add', 'text', '// comment', '.'])
    run_command([cl, 'add', 'text', '% schema foo', '.'])
    run_command([cl, 'add', 'text', '% add uuid', '.'])
    run_command([cl, 'add', 'text', '% add data_hash data_hash s/0x/HEAD', '.'])
    run_command([cl, 'add', 'text', '% add CREATE created "date | [0:5]"', '.'])
    run_command([cl, 'add', 'text', '% display table foo', '.'])
    run_command([cl, 'add', 'bundle', uuid, '.'])
    run_command([cl, 'add', 'bundle', uuid, wuuid])  # not testing real copying ability
    run_command([cl, 'add', 'worksheet', wuuid, '.'])
    check_contains(['Worksheet', 'testing', test_path_contents('a.txt'), uuid, 'HEAD', 'CREATE'], run_command([cl, 'print']))
    run_command([cl, 'wadd', wuuid, wuuid])
    check_num_lines(8, run_command([cl, 'ls', '-u']))
    run_command([cl, 'wedit', wuuid, '--name', wname + '2'])
    run_command([cl, 'wedit', wuuid, '--file', '/dev/null'])  # wipe out worksheet
    # cleanup
    run_command([cl, 'rm', uuid])

@TestModule.register('worksheet_search')
def test(ctx):
    wname = random_name()
    # Create new worksheet
    wuuid = run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    check_contains(['Switched', wname, wuuid], run_command([cl, 'work', wuuid]))
    uuid = run_command([cl, 'upload', test_path('a.txt')])
    run_command([cl, 'add', 'text', '% search ' + uuid, '.'])
    run_command([cl, 'add', 'text', '% wsearch ' + wuuid, '.'])
    check_contains([uuid[0:8], wuuid[0:8]], run_command([cl, 'print']))
    run_command([cl, 'rm', uuid])

@TestModule.register('worksheet_tags')
def test(ctx):
    wname = random_name()
    wuuid = run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    # Add tags
    run_command([cl, 'wedit', wname, '--tags', 'foo', 'bar', 'baz'])
    check_contains(['Tags: \\[\'foo\', \'bar\', \'baz\'\\]'], run_command([cl, 'ls', '-w', wuuid]))
    # Modify tags
    run_command([cl, 'wedit', wname, '--tags', 'bar', 'foo'])
    check_contains(['Tags: \\[\'bar\', \'foo\'\\]'], run_command([cl, 'ls', '-w', wuuid]))
    # Delete tags
    run_command([cl, 'wedit', wname, '--tags'])
    check_contains(['Tags: \\[\\]'], run_command([cl, 'ls', '-w', wuuid]))

@TestModule.register('freeze')
def test(ctx):
    orig_wuuid = run_command([cl, 'work', '-u'])
    wname = random_name()
    wuuid = run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    check_contains(['Switched', wname, wuuid], run_command([cl, 'work', wuuid]))
    # Before freezing: can modify everything
    uuid1 = run_command([cl, 'upload', '-c', 'hello'])
    run_command([cl, 'add', 'text', 'message', '.'])
    run_command([cl, 'wedit', '-t', 'new_title'])
    run_command([cl, 'wperm', wuuid, 'public', 'n'])
    run_command([cl, 'wedit', '--freeze'])
    # After freezing: can only modify contents
    run_command([cl, 'detach', uuid1], 1)  # would remove an item
    run_command([cl, 'rm', uuid1], 1)  # would remove an item
    run_command([cl, 'add', 'text', 'message', '.'], 1)  # would add an item
    run_command([cl, 'wedit', '-t', 'new_title']) # can edit
    run_command([cl, 'wperm', wuuid, 'public', 'a']) # can edit

@TestModule.register('detach')
def test(ctx):
    uuid1 = run_command([cl, 'upload', test_path('a.txt')])
    uuid2 = run_command([cl, 'upload', test_path('b.txt')])
    run_command([cl, 'add', 'bundle', uuid1, '.'])
    ctx.collect_bundle(uuid1)
    run_command([cl, 'add', 'bundle', uuid2, '.'])
    ctx.collect_bundle(uuid2)
    # State after the above: 1 2 1 2
    run_command([cl, 'detach', uuid1], 1) # multiple indices
    run_command([cl, 'detach', uuid1, '-n', '3'], 1) # indes out of range
    run_command([cl, 'detach', uuid2, '-n', '2']) # State: 1 1 2
    check_equals(get_info('^', 'uuid'), uuid2)
    run_command([cl, 'detach', uuid2]) # State: 1 1
    check_equals(get_info('^', 'uuid'), uuid1)

@TestModule.register('perm')
def test(ctx):
    uuid = run_command([cl, 'upload', test_path('a.txt')])
    check_equals('all', run_command([cl, 'info', '-v', '-f', 'permission', uuid]))
    check_contains('none', run_command([cl, 'perm', uuid, 'public', 'n']))
    check_contains('read', run_command([cl, 'perm', uuid, 'public', 'r']))
    check_contains('all', run_command([cl, 'perm', uuid, 'public', 'a']))
    run_command([cl, 'rm', uuid])

@TestModule.register('search')
def test(ctx):
    name = random_name()
    uuid1 = run_command([cl, 'upload', test_path('a.txt'), '-n', name])
    uuid2 = run_command([cl, 'upload', test_path('b.txt'), '-n', name])
    check_equals(uuid1, run_command([cl, 'search', uuid1, '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1, '-u']))
    check_equals('', run_command([cl, 'search', 'uuid='+uuid1[0:8], '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1[0:8]+'.*', '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1[0:8]+'%', '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1, 'name='+name, '-u']))
    check_equals(uuid1 + '\n' + uuid2, run_command([cl, 'search', 'name='+name, 'id=.sort', '-u']))
    check_equals(uuid2 + '\n' + uuid1, run_command([cl, 'search', 'name='+name, 'id=.sort-', '-u']))
    check_equals('2', run_command([cl, 'search', 'name='+name, '.count']))
    size1 = float(run_command([cl, 'info', '-f', 'data_size', uuid1]))
    size2 = float(run_command([cl, 'info', '-f', 'data_size', uuid2]))
    check_equals(size1 + size2, float(run_command([cl, 'search', 'name='+name, 'data_size=.sum'])))
    run_command([cl, 'rm', uuid1, uuid2])

@TestModule.register('kill')
def test(ctx):
    uuid = run_command([cl, 'run', 'sleep 1000'])
    time.sleep(2)
    check_equals(uuid, run_command([cl, 'kill', uuid]))
    run_command([cl, 'wait', uuid], 1)
    run_command([cl, 'rm', uuid])

@TestModule.register('mimic')
def test(ctx):
    name = random_name()
    def data_hash(uuid):
        run_command([cl, 'wait', uuid])
        return get_info(uuid, 'data_hash')
    uuid1 = run_command([cl, 'upload', test_path('a.txt'), '-n', name + '-in1'])
    uuid2 = run_command([cl, 'make', uuid1, '-n', name + '-out'])
    uuid3 = run_command([cl, 'upload', test_path('a.txt')])
    # Try three ways of mimicing, should all produce the same answer
    uuid4 = run_command([cl, 'mimic', uuid1, uuid3, '-n', 'new'])
    check_equals(data_hash(uuid2), data_hash(uuid4))
    uuid5 = run_command([cl, 'mimic', uuid1, uuid2, uuid3, '-n', 'new'])
    check_equals(data_hash(uuid2), data_hash(uuid5))
    uuid6 = run_command([cl, 'macro', name, uuid3, '-n', 'new'])
    check_equals(data_hash(uuid2), data_hash(uuid6))
    run_command([cl, 'rm', uuid1, uuid2, uuid3, uuid4, uuid5, uuid6])

@TestModule.register('status')
def test(ctx):
    run_command([cl, 'status'])
    run_command([cl, 'alias'])
    run_command([cl, 'help'])

@TestModule.register('events')
def test(ctx):
    local = 'local::' in run_command([cl, 'work'])
    if local:
        run_command([cl, 'events'])
        run_command([cl, 'events', '-n'])
        run_command([cl, 'events', '-g', 'user'])
        run_command([cl, 'events', '-g', 'user', '-n'])
        run_command([cl, 'events', '-g', 'command'])
        run_command([cl, 'events', '-o', '1', '-l', '2'])
        run_command([cl, 'events', '-a', '%true%', '-n'])
    else:
        # Shouldn't be allowed to run unless in local mode.
        run_command([cl, 'events'], 1)

@TestModule.register('batch')
def test(ctx):
    '''Test batch resolution of bundle uuids'''
    wother = random_name()
    bnames = [random_name() for _ in range(2)]

    # Create worksheet and bundles
    wuuid = run_command([cl, 'new', wother])
    ctx.collect_worksheet(wuuid)
    buuids = [
        run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[0]]),
        run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[1]]),
        run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[0], '-w', wother]),
        run_command([cl, 'upload', test_path('a.txt'), '-n', bnames[1], '-w', wother])
    ]

    # Test batch info call
    output = run_command([cl, 'info', '-f', 'uuid', bnames[0], bnames[1],
        '%s/%s' % (wother, bnames[0]), '%s/%s' % (wother, bnames[1])])
    check_equals('\n'.join(buuids), output)

    # Test batch info call with combination of uuids and names
    output = run_command([cl, 'info', '-f', 'uuid', buuids[0], bnames[0], bnames[0], buuids[0]])
    check_equals('\n'.join([buuids[0]] * 4), output)

@TestModule.register('copy')
def test(ctx):
    '''Test copying between instances.'''
    # Figure out the current instance
    # Switched to worksheet http://localhost:2800::home-pliang(0x87a7a7ffe29d4d72be9b23c745adc120).
    m = re.search('(http[^\(]+)', run_command([cl, 'work']))
    if not m:
        print 'Not a remote instance, skipping test.'
        return
    remote_worksheet = m.group(1)

    # Create another local CodaLab instance.
    home = temp_path('-home')
    #home = 'temp-home'  # For consistency
    os.environ['CODALAB_HOME'] = home
    local_worksheet = 'local::'
    # Initialize: press n, n and type in username/password for current worksheet.
    subprocess.call([cl, 'work', remote_worksheet])

    def check_agree(command):
        check_equals(run_command(command + ['-w', local_worksheet]), run_command(command + ['-w', remote_worksheet]))

    # Upload to local, transfer to remote
    run_command([cl, 'work', local_worksheet])
    uuid = run_command([cl, 'upload', test_path('')])
    run_command([cl, 'add', 'bundle', uuid, remote_worksheet])
    check_agree([cl, 'info', '-f', 'data_hash,data_size,name', uuid])
    check_agree([cl, 'cat', uuid])

    # Upload to remote, transfer to local
    run_command([cl, 'work', remote_worksheet])
    uuid = run_command([cl, 'upload', test_path('')])
    run_command([cl, 'add', 'bundle', uuid, local_worksheet])
    check_agree([cl, 'info', '-f', 'data_hash,data_size,name', uuid])
    check_agree([cl, 'cat', uuid])

    # Upload to remote, transfer to local (metadata only)
    run_command([cl, 'work', remote_worksheet])
    uuid = run_command([cl, 'upload', '-c', 'hello'])
    run_command([cl, 'rm', '-d', uuid])  # Keep only metadata
    run_command([cl, 'add', 'bundle', uuid, local_worksheet])

    # Test adding worksheet items
    run_command([cl, 'wadd', local_worksheet, remote_worksheet])
    run_command([cl, 'wadd', remote_worksheet, local_worksheet])

    # Cleanup
    del os.environ['CODALAB_HOME']
    run_command([cl, 'work', remote_worksheet])
    shutil.rmtree(home)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print 'Usage: python %s <module> ... <module>' % sys.argv[0]
        print 'This test will modify your current instance by creating temporary worksheets and bundles, but these should be deleted.'
        print 'Remember to run this both in local and remote modes.',
        print 'Modules: all ' + ' '.join(TestModule.modules.keys())
    else:
        TestModule.run(sys.argv[1:])
