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

def random_name():
    return 'test-cli-' + str(random.randint(0, 1000000))

def run_command(args, expected_exit_code=0):
    try:
        output = subprocess.check_output(args)
        exitcode = 0
    except subprocess.CalledProcessError, e:
        output = e.output
        exitcode = e.returncode
    print '>> %s (exit code %s, expected %s)\n%s' % (args, exitcode, expected_exit_code, output)
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
        assert re.search(true_value, pred_value), "expected something that contains '%s', but got '%s'" % (true_value, pred_value)
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
            self.bundles.extend(run_command([cl, 'ls', worksheet, '-u']).split())
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

@TestModule.register('upload1')
def test(ctx):
    # upload
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts', '--description', 'hello', '--tags', 'a', 'b'])
    check_equals('hosts', get_info(uuid, 'name'))
    check_equals('hello', get_info(uuid, 'description'))
    check_contains(['a', 'b'], get_info(uuid, 'tags'))
    check_equals('ready', get_info(uuid, 'state'))
    check_equals('ready\thello', get_info(uuid, 'state,description'))

    # edit
    run_command([cl, 'edit', uuid, '--name', 'hosts2'])
    check_equals('hosts2', get_info(uuid, 'name'))

    # cat, info
    check_contains('127.0.0.1', run_command([cl, 'cat', uuid]))
    check_contains(['bundle_type', 'uuid', 'owner', 'created'], run_command([cl, 'info', uuid]))
    check_contains('license', run_command([cl, 'info', '--raw', uuid]))
    check_contains(['host_worksheets', 'contents'], run_command([cl, 'info', '--verbose', uuid]))

    # rm
    run_command([cl, 'rm', '--dry-run', uuid])
    check_contains('0x', get_info(uuid, 'data_hash'))
    run_command([cl, 'rm', '--data-only', uuid])
    check_equals('None', get_info(uuid, 'data_hash'))
    run_command([cl, 'rm', uuid])

@TestModule.register('upload2')
def test(ctx):
    # Upload two files
    uuid = run_command([cl, 'upload', 'program', '/etc/hosts', '/etc/group', '--description', 'hello'])
    check_contains('127.0.0.1', run_command([cl, 'cat', uuid + '/hosts']))
    # Upload with base
    uuid2 = run_command([cl, 'upload', 'program', '/etc/hosts', '/etc/group', '--base', uuid])
    check_equals('hello', get_info(uuid2, 'description'))
    # Cleanup
    run_command([cl, 'rm', uuid, uuid2])

@TestModule.register('upload3')
def test(ctx):
    uuid = run_command([cl, 'upload', 'dataset', '-c', 'hello'])
    check_equals('hello', run_command([cl, 'cat', uuid]))
    run_command([cl, 'rm', uuid])

@TestModule.register('rm')
def test(ctx):
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    run_command([cl, 'cp', uuid, '.'])  # Duplicate
    run_command([cl, 'rm', uuid])  # Can delete even though it exists twice on the same worksheet

@TestModule.register('make')
def test(ctx):
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    uuid2 = run_command([cl, 'upload', 'dataset', '/etc/group'])
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
    check_equals('run "echo hello"', run_command([cl, 'info', '-f', 'args', uuid]))
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
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    check_equals(uuid, run_command([cl, 'ls', '-u']))
    # create worksheet
    check_contains(uuid[0:5], run_command([cl, 'ls']))
    run_command([cl, 'add', '-m', 'testing'])
    run_command([cl, 'add', '-m', '% display contents / maxlines=10'])
    run_command([cl, 'cp', uuid, '.'])
    run_command([cl, 'add', '-m', '%% comment'])
    run_command([cl, 'add', '-m', '% schema foo'])
    run_command([cl, 'add', '-m', '% add uuid'])
    run_command([cl, 'add', '-m', '% add data_hash data_hash s/0x/HEAD'])
    run_command([cl, 'add', '-m', '% add CREATE created "date | [0:5]"'])
    run_command([cl, 'add', '-m', '% display table foo'])
    run_command([cl, 'cp', uuid, '.'])
    run_command([cl, 'cp', uuid, wuuid])  # not testing real copying ability
    run_command([cl, 'add', wuuid])
    check_contains(['Worksheet', 'testing', 'hosts', '127.0.0.1', uuid, 'HEAD', 'CREATE'], run_command([cl, 'print']))
    run_command([cl, 'wcp', wuuid, wuuid])
    check_num_lines(8, run_command([cl, 'ls', '-u']))
    run_command([cl, 'wedit', wuuid, '--name', wname + '2'])
    run_command([cl, 'wedit', wuuid, '--file', '/dev/null'])  # wipe out worksheet
    # cleanup
    run_command([cl, 'rm', uuid])

@TestModule.register('worksheet_tags')
def test(ctx):
    wname = random_name()
    wuuid = run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    # Add tags
    run_command([cl, 'wedit', wname, '--tags', 'foo', 'bar', 'baz'])
    check_contains(['Tags: \\[\'foo\', \'bar\', \'baz\'\\]'], run_command([cl, 'ls', wuuid]))
    # Modify tags
    run_command([cl, 'wedit', wname, '--tags', 'bar', 'foo'])
    check_contains(['Tags: \\[\'bar\', \'foo\'\\]'], run_command([cl, 'ls', wuuid]))
    # Delete tags
    run_command([cl, 'wedit', wname, '--tags'])
    check_contains(['Tags: \\[\\]'], run_command([cl, 'ls', wuuid]))

@TestModule.register('freeze')
def test(ctx):
    orig_wuuid = run_command([cl, 'work', '-u'])
    wname = random_name()
    wuuid = run_command([cl, 'new', wname])
    ctx.collect_worksheet(wuuid)
    check_contains(['Switched', wname, wuuid], run_command([cl, 'work', wuuid]))
    # Before freezing: can modify everything
    uuid1 = run_command([cl, 'upload', 'dataset', '-c', 'hello'])
    run_command([cl, 'add', '-m' 'message'])
    run_command([cl, 'wedit', '-t', 'new_title'])
    run_command([cl, 'wperm', wuuid, 'public', 'n'])
    run_command([cl, 'wedit', '--freeze'])
    # After freezing: can only modify contents
    run_command([cl, 'detach', uuid1], 1)  # would remove an item
    run_command([cl, 'rm', uuid1], 1)  # would remove an item
    run_command([cl, 'add', '-m', 'message'], 1)  # would add an item
    run_command([cl, 'wedit', '-t', 'new_title']) # can edit
    run_command([cl, 'wperm', wuuid, 'public', 'a']) # can edit

@TestModule.register('copy')
def test(ctx):
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts', '/etc/group'])
    # download
    run_command([cl, 'download', uuid, '-o', uuid])
    run_command(['ls', '-R', uuid])
    shutil.rmtree(uuid)
    # cleanup
    run_command([cl, 'rm', uuid])

@TestModule.register('detach')
def test(ctx):
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    uuid2 = run_command([cl, 'upload', 'dataset', '/etc/group'])
    run_command([cl, 'cp', uuid1, '.'])
    ctx.collect_bundle(uuid1)
    run_command([cl, 'cp', uuid2, '.'])
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
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    check_equals('all', run_command([cl, 'info', '-v', '-f', 'permission', uuid]))
    check_contains('none', run_command([cl, 'perm', uuid, 'public', 'n']))
    check_contains('read', run_command([cl, 'perm', uuid, 'public', 'r']))
    check_contains('all', run_command([cl, 'perm', uuid, 'public', 'a']))
    run_command([cl, 'rm', uuid])

@TestModule.register('search')
def test(ctx):
    name = random_name()
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', name])
    uuid2 = run_command([cl, 'upload', 'dataset', '/etc/group', '-n', name])
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
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', name + '-in1'])
    uuid2 = run_command([cl, 'make', uuid1, '-n', name + '-out'])
    uuid3 = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
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
    run_command([cl, 'events'])
    run_command([cl, 'events', '-n'])
    run_command([cl, 'events', '-g', 'user'])
    run_command([cl, 'events', '-g', 'user', '-n'])
    run_command([cl, 'events', '-g', 'command'])
    run_command([cl, 'events', '-o', '1', '-l', '2'])
    run_command([cl, 'events', '-a', '%true%', '-n'])

@TestModule.register('batch')
def test(ctx):
    '''Test batch resolution of bundle uuids'''
    wother = random_name()
    bnames = [random_name() for _ in range(2)]

    # Create worksheet and bundles
    wuuid = run_command([cl, 'new', wother])
    ctx.collect_worksheet(wuuid)
    buuids = [
        run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[0]]),
        run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[1]]),
        run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[0], '-w', wother]),
        run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[1], '-w', wother])
    ]

    # Test batch info call
    output = run_command([cl, 'info', '-f', 'uuid', bnames[0], bnames[1],
        '%s/%s' % (wother, bnames[0]), '%s/%s' % (wother, bnames[1])])
    check_equals('\n'.join(buuids), output)

    # Test batch info call with combination of uuids and names
    output = run_command([cl, 'info', '-f', 'uuid', buuids[0], bnames[0], bnames[0], buuids[0]])
    check_equals('\n'.join([buuids[0]] * 4), output)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print 'Usage: python %s <module> ... <module>' % sys.argv[0]
        print 'Note that this will modify your current worksheet, but should restore it.'
        print 'Modules: all ' + ' '.join(TestModule.modules.keys())
    else:
        TestModule.run(sys.argv[1:])
