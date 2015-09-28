#!/usr/bin/env python

import subprocess
import sys
import re
import os
import shutil
import random
import time

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

tests = []
def add_test(name, func):
    tests.append((name, func))
def run_test(query_name):
    failed = []
    for name, func in tests:
        if query_name == 'all' or query_name == name:
            print '============= ' + name
            try:
                func()
            except AssertionError as e:
                print "ERROR: %s" % e.message
                failed.append(name)

    if failed:
        print "Tests failed: %s" % ', '.join(failed)
    else:
        print "All tests passed."

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
    uuid = run_command([cl, 'upload', 'program', '/etc/hosts', '/etc/group', '--description', 'hello'])
    check_contains('127.0.0.1', run_command([cl, 'cat', uuid + '/hosts']))
    # Upload with base
    uuid2 = run_command([cl, 'upload', 'program', '/etc/hosts', '/etc/group', '--base', uuid])
    check_equals('hello', get_info(uuid2, 'description'))
    # Cleanup
    run_command([cl, 'rm', uuid, uuid2])
add_test('upload2', test)

def test():
    uuid = run_command([cl, 'upload', 'dataset', '-c', 'hello'])
    check_equals('hello', run_command([cl, 'cat', uuid]))
    run_command([cl, 'rm', uuid])
add_test('upload3', test)

def test():
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    run_command([cl, 'cp', uuid, '.'])  # Duplicate
    run_command([cl, 'rm', uuid])  # Can delete even though it exists twice on the same worksheet
add_test('rm', test)

def test():
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
add_test('make', test)

def test():
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
add_test('run', test)

def test():
    wname = random_name()
    # Create new worksheet
    orig_wuuid = run_command([cl, 'work', '-u'])
    wuuid = run_command([cl, 'new', wname])
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
    orig_wuuid = run_command([cl, 'work', '-u'])
    wname = random_name()
    wuuid = run_command([cl, 'new', wname])
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
    # cleanup
    run_command([cl, 'wrm', '--force', wuuid])
    run_command([cl, 'rm', uuid1])
    run_command([cl, 'work', orig_wuuid])
add_test('freeze', test)

def test():
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts', '/etc/group'])
    # download
    run_command([cl, 'download', uuid, '-o', uuid])
    run_command(['ls', '-R', uuid])
    shutil.rmtree(uuid)
    # cleanup
    run_command([cl, 'rm', uuid])
add_test('copy', test)

def test():
    uuid1 = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    uuid2 = run_command([cl, 'upload', 'dataset', '/etc/group'])
    run_command([cl, 'add', uuid1])
    run_command([cl, 'add', uuid2])
    # State after the above: 1 2 1 2
    run_command([cl, 'detach', uuid1], 1) # multiple indices
    run_command([cl, 'detach', uuid1, '-n', '3'], 1) # indes out of range
    run_command([cl, 'detach', uuid2, '-n', '2']) # State: 1 1 2
    check_equals(get_info('^', 'uuid'), uuid2)
    run_command([cl, 'detach', uuid2]) # State: 1 1
    check_equals(get_info('^', 'uuid'), uuid1)
    # Cleanup
    run_command([cl, 'rm', uuid1, uuid2])
add_test('detach', test)

def test():
    uuid = run_command([cl, 'upload', 'dataset', '/etc/hosts'])
    check_equals('all', run_command([cl, 'info', '-v', '-f', 'permission', uuid]))
    check_contains('none', run_command([cl, 'perm', uuid, 'public', 'n']))
    check_contains('read', run_command([cl, 'perm', uuid, 'public', 'r']))
    check_contains('all', run_command([cl, 'perm', uuid, 'public', 'a']))
    run_command([cl, 'rm', uuid])
add_test('perm', test)

def test():
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
add_test('search', test)

def test():
    uuid = run_command([cl, 'run', 'sleep 1000'])
    time.sleep(2)
    check_equals(uuid, run_command([cl, 'kill', uuid]))
    run_command([cl, 'wait', uuid], 1)
    run_command([cl, 'rm', uuid])
add_test('kill', test)

def test():
    # Do everything on a new worksheet
    wname = random_name()
    old = run_command([cl, 'work', '-u'])
    new = run_command([cl, 'new', wname])

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

    # Cleanup
    run_command([cl, 'wedit', new, '-f', '/dev/null'])
    run_command([cl, 'wrm', new])
    run_command([cl, 'work', old])

add_test('mimic', test)

def test():
    run_command([cl, 'status'])
    run_command([cl, 'alias'])
    run_command([cl, 'help'])
add_test('status', test)

def test():
    run_command([cl, 'events'])
    run_command([cl, 'events', '-n'])
    run_command([cl, 'events', '-g', 'user'])
    run_command([cl, 'events', '-g', 'user', '-n'])
    run_command([cl, 'events', '-g', 'command'])
    run_command([cl, 'events', '-o', '1', '-l', '2'])
    run_command([cl, 'events', '-a', '%true%', '-n'])
add_test('events', test)

def test():
    wother = random_name()
    bnames = [random_name() for _ in range(2)]
    buuids = []

    # Create worksheets and bundles
    run_command([cl, 'new', wother])
    buuids.append(run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[0]]))
    buuids.append(run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[1]]))
    buuids.append(run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[0], '-w', wother]))
    buuids.append(run_command([cl, 'upload', 'dataset', '/etc/hosts', '-n', bnames[1], '-w', wother]))

    # Test batch info call
    output = run_command([cl, 'info', '-f', 'uuid', bnames[0], bnames[1],
        '%s/%s' % (wother, bnames[0]), '%s/%s' % (wother, bnames[1])])
    check_equals('\n'.join(buuids), output)

    # Test batch info call with combination of uuids and names
    output = run_command([cl, 'info', '-f', 'uuid', buuids[0], bnames[0], bnames[0], buuids[0]])
    check_equals('\n'.join([buuids[0]] * 4), output)

    # Cleanup
    run_command([cl, 'rm'] + buuids)
    run_command([cl, 'wrm', '--force', wother])
add_test('batch', test)

if len(sys.argv) == 1:
    print 'Usage: python %s <module> ... <module>' % sys.argv[0]
    print 'Note that this will modify your current worksheet, but should restore it.'
    print 'Modules: all ' + ' '.join(name for name, func in tests)
else:
    for name in sys.argv[1:]:
        run_test(name)
