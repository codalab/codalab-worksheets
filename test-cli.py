#!/usr/bin/env python

import subprocess
import sys
import re
import os
import shutil

'''
Tests all the CLI functionality end-to-end.

Currently, the tests will operate on your current worksheet.  In theory, it
shouldn't mutate anything, but this is not guaranteed, and you should run this
command in an unimportant CodaLab account.

Things not tested:
- Interactive modes (cl edit, cl wedit)
- Permissions
- Worker system
'''

cl = 'cl'

def run_command(args, expected_exit_code=0):
    try:
        output = subprocess.check_output(args)
        exitcode = 0
    except subprocess.CalledProcessError, e:
        output = e.output
        exitcode = e.returncode
    print '>> %s (exit code %s, expected %s)\n%s' % (args, exitcode, expected_exit_code, output)
    if expected_exit_code != exitcode:
        error('Exit codes don\'t match')
    return output.rstrip()

def get_info(uuid, key):
    return run_command([cl, 'info', '-f', key, uuid])

def wait(uuid):
    run_command([cl, 'wait', uuid])

def error(message):
    print 'ERROR:', message
    sys.exit(1)

def check_equals(true_value, pred_value):
    if true_value != pred_value:
        error("expected '%s', but got '%s'" % (true_value, pred_value))
    return pred_value

def check_contains(true_value, pred_value):
    if isinstance(true_value, list):
        for v in true_value:
            check_contains(v, pred_value)
    else:
        if not re.search(true_value, pred_value):
            error("expected something that contains '%s', but got '%s'" % (true_value, pred_value))
    return pred_value

def check_num_lines(true_value, pred_value):
    num_lines = len(pred_value.split('\n'))
    if num_lines != true_value:
        error("expected %d lines, but got %s" % (true_value, num_lines))
    return pred_value

tests = []
def add_test(name, func):
    tests.append((name, func))
def run_test(query_name):
    for name, func in tests:
        if query_name == 'all' or query_name == name:
            print '============= ' + name
            func()

############################################################

def test():
    run_command(['venv/bin/nosetests'])
add_test('unittest', test)

def test():
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
add_test('upload1', test)

def test():
    # Upload two files
    uuid = run_command([cl, 'upload', 'program', '/etc/hosts', '/etc/issue', '--description', 'hello'])
    check_contains('127.0.0.1', run_command([cl, 'cat', uuid + '/hosts']))
    # Upload with base
    uuid2 = run_command([cl, 'upload', 'program', '/etc/hosts', '/etc/issue', '--base', uuid])
    check_equals('hello', get_info(uuid2, 'description'))
    # Cleanup
    run_command([cl, 'rm', uuid, uuid2])
add_test('upload2', test)

def test():
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    run_command([cl, 'cp', uuid, '.'])  # Duplicate
    run_command([cl, 'rm', uuid])  # Can delete even though it exists twice on the same worksheet
add_test('rm', test)

def test():
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    uuid2 = run_command([cl, 'upload', 'dataset', '/etc/issue'])
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
    run_command([cl, 'rm', '-f', uuid2])  # force the deletion
    run_command([cl, 'rm', '-r', uuid1])  # delete things downstream
add_test('make', test)

def test():
    name = 'test-hello-run'
    uuid = run_command([cl, 'run', 'echo hello', '-n', name])
    wait(uuid)
    # test search
    check_contains('hello', run_command([cl, 'search', name]))
    check_equals(uuid, run_command([cl, 'search', name, '-u']))
    run_command([cl, 'search', 'test-hello-run', '--append'])
    # get info
    check_equals('ready', run_command([cl, 'info', '-f', 'state', uuid]))
    check_equals('run "echo hello"', run_command([cl, 'info', '-f', 'args', uuid]))
    check_equals('hello', run_command([cl, 'cat', uuid+'/stdout']))
    # block
    uuid2 = check_contains('hello', run_command([cl, 'run', 'echo hello', '--tail'])).split('\n')[0]
    # cleanup
    run_command([cl, 'rm', '-f', uuid, uuid2])  # force because bundle shows up twice
add_test('run', test)

def test():
    wname = 'test-worksheet'
    # Create new worksheet
    orig_wuuid = run_command([cl, 'work', '--raw'])
    wuuid = run_command([cl, 'new', wname, '--raw'])
    check_contains(['Switched', wname, wuuid], run_command([cl, 'work', wuuid]))
    # ls
    check_equals('', run_command([cl, 'ls', '-u']))
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    check_equals(uuid, run_command([cl, 'ls', '-u']))
    # create worksheet
    check_contains(uuid[0:5], run_command([cl, 'ls']))
    run_command([cl, 'add', '-m', 'testing'])
    run_command([cl, 'add', '-m', '% display contents / maxlines=10'])
    run_command([cl, 'add', uuid])
    run_command([cl, 'add', '-m', '%% comment'])
    run_command([cl, 'add', '-m', '% schema foo'])
    run_command([cl, 'add', '-m', '% add uuid'])
    run_command([cl, 'add', '-m', '% add data_hash data_hash s/0x/HEAD'])
    run_command([cl, 'add', '-m', '% add CREATE created "date | [0:5]"'])
    run_command([cl, 'add', '-m', '% display table foo'])
    run_command([cl, 'add', uuid])
    run_command([cl, 'cp', uuid, wuuid])  # not testing real copying ability
    run_command([cl, 'wadd', wuuid])
    check_contains(['Worksheet', 'testing', 'hosts', '127.0.0.1', uuid, 'HEAD', 'CREATE'], run_command([cl, 'print']))
    run_command([cl, 'wcp', wuuid, wuuid])
    check_num_lines(8, run_command([cl, 'ls', '-u']))
    run_command([cl, 'wedit', wuuid, '--name', wname + '2'])
    run_command([cl, 'wedit', wuuid, '--file', '/dev/null'])  # wipe out worksheet
    # cleanup
    run_command([cl, 'rm', uuid])
    run_command([cl, 'wrm', wuuid])
    run_command([cl, 'work', orig_wuuid])
add_test('worksheet', test)

def test():
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts', '/etc/issue'])
    # download
    run_command([cl, 'download', uuid, '-o', uuid])
    run_command(['ls', '-R', uuid])
    shutil.rmtree(uuid)
    # cleanup
    run_command([cl, 'rm', uuid])
add_test('copy', test)

def test():
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    uuid2 = run_command([cl, 'upload', 'dataset', '/etc/issue'])
    run_command([cl, 'add', uuid1])
    run_command([cl, 'add', uuid2])
    # State after the above: 1 2 1 2
    run_command([cl, 'hide', uuid1], 1) # multiple indices
    run_command([cl, 'hide', uuid1, '-n', '3'], 1) # indes out of range
    run_command([cl, 'hide', uuid2, '-n', '2']) # State: 1 1 2
    check_equals(get_info('^', 'uuid'), uuid2)
    run_command([cl, 'hide', uuid2]) # State: 1 1
    check_equals(get_info('^', 'uuid'), uuid1)
    # Cleanup
    run_command([cl, 'rm', uuid1, uuid2])
add_test('hide', test)

def test():
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    check_equals('all', run_command([cl, 'info', '-v', '-f', 'permission', uuid]))
    check_contains('none', run_command([cl, 'perm', uuid, 'public', 'n']))
    check_contains('read', run_command([cl, 'perm', uuid, 'public', 'r']))
    check_contains('all', run_command([cl, 'perm', uuid, 'public', 'a']))
    run_command([cl, 'rm', uuid])
add_test('perm', test)

def test():
    name = 'test-123'
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', name])
    uuid2 = run_command([cl, 'upload', 'dataset', '/etc/issue', '-n', name])
    check_equals(uuid1, run_command([cl, 'search', uuid1, '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1, '-u']))
    check_equals('', run_command([cl, 'search', 'uuid='+uuid1[0:8], '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1[0:8]+'.*', '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1[0:8]+'%', '-u']))
    check_equals(uuid1, run_command([cl, 'search', 'uuid='+uuid1, 'name='+name, '-u']))
    check_equals(uuid1 + '\n' + uuid2, run_command([cl, 'search', 'name='+name, 'id=.sort', '-u']))
    check_equals(uuid2 + '\n' + uuid1, run_command([cl, 'search', 'name='+name, 'id=.sort-', '-u']))
    check_equals('2', run_command([cl, 'search', 'name='+name, '.count']))
    size1 = int(run_command([cl, 'info', '-f', 'data_size', uuid1]))
    size2 = int(run_command([cl, 'info', '-f', 'data_size', uuid2]))
    check_equals(str(size1 + size2), run_command([cl, 'search', 'name='+name, 'data_size=.sum']))
    run_command([cl, 'rm', uuid1, uuid2])
add_test('search', test)

def test():
    run_command([cl, 'status'])
    run_command([cl, 'alias'])
    run_command([cl, 'help'])
add_test('status', test)

if len(sys.argv) == 1:
    print 'Usage: python %s <module> ... <module>' % sys.argv[0]
    print 'Note that this will modify your current worksheet, but should restore it.'
    print 'Modules: all ' + ' '.join(name for name, func in tests)
else:
    for name in sys.argv[1:]:
        run_test(name)
