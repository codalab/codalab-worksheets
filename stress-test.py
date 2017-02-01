#import altf4

import logging
import os, sys
import uuid
import subprocess
import re
import time
from cStringIO import StringIO

from codalab.lib.bundle_cli import BundleCLI, Commands
from codalab.lib.codalab_manager import CodaLabManager

from multiprocessing.dummy import Pool as ThreadPool

logger = logging.getLogger(__file__)

class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def run_cl_command(args, timeout=30):
    '''
    Run command with timeout

    Returns: exit_code, time_elapsed, output

    '''

    start_time = time.time()
    try:
        args = ['timeout', '{}s'.format(timeout)] + args
        output = subprocess.check_output(args)
        exit_code = 0
    except subprocess.CalledProcessError, e:
        output = e.output
        exit_code = e.returncode
    end_time = time.time()
    time_elapsed = end_time - start_time
    print('Code {}, Time {}s, Ran: {}'.format(exit_code, int(time_elapsed), args))

    return exit_code, time_elapsed, output.rstrip()

def run_stuff(i):
    exit_code, time_elapsed, output = run_command(['cl', 'search', '.count'])
    if exit_code != 0:
        raise Exception(output)
    #run_command(['cl', 'search', 'size=.sum'])
    #run_command(['cl', 'search', 'size=.sort-', '.limit=5'])
    #run_command(['cl', 'search', 'size=.sort-', '.limit=50'])
    #run_command(['cl', 'search', 'size=.sort-', '.limit=500'])
    #run_command(['cl', 'search', '.last', '.limit=5'])
    #run_command(['cl', 'search', '.last', '.limit=500'])

def create_worksheet(i):
    ws = 'gregwasher91-test-worksheet-{}'.format(i)
    try:
        cli.do_command(['new', ws])
    except:
        return None
    return ws

def delete_worksheet(ws):
    return cli.do_command(['wrm', ws])

def test_number_of_worksheets(cli, n, multithread=False):

    if multithread:
        array = [ i for i in range(n) ]
        pool = ThreadPool(50)
        ws_names = pool.map(create_worksheet, array)
    else:
        ws_names = [ create_worksheet(i) for i in range(n) ]

    with Timer() as t:
        output = cli.do_command(['wsearch', '.mine'])
    print 'Searching through ~{} worksheets took {}s'.format(n, t.interval)

    ws_names = [ ws for ws in ws_names if ws ]
    if multithread:
        pool = ThreadPool(50)
        ws_names = pool.map(delete_worksheet, ws_names)
    else:
        ws_names = [ delete_worksheet(w) for w in ws_names ]

def upload_big_bundle(i):
    return cli.do_command([
        'upload',
        'http://cs246h.stanford.edu/map_only_data.zip'
    ])

def test_upload_bundles(cli, n, multithread=False):
    if multithread:
        array = [ i for i in range(n) ]
        pool = ThreadPool(50)
        pool.map(upload_big_bundle, array)
    else:
        [ upload_big_bundle(i) for i in range(n) ]

    with Timer() as t:
        output = cli.do_command(['ls'])
    print 'Listing ~{} bundles took {}s'.format(n, t.interval)

    while 1:
        bundles = [ r['uuid'] for r in cli.do_command(['search', '.mine', 'map_only_data'])['refs'].values() ]
        if not bundles:
            return
        cli.do_command(['rm'] + bundles)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    logger.info('hi')

    cli = BundleCLI(CodaLabManager(), stdout=StringIO(), stderr=StringIO())
    # Make sure we can connect (might prompt for username/password)
    try:
        cli.do_command(['work', 'https://worksheets-test.codalab.org/::gregwasher91-worksheetname'])
    except Exception, e:
        logger.error(e)
        sys.exit(1)

    #test_number_of_worksheets(cli, 300, True)
    test_upload_bundles(cli, 10, True)
