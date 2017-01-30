#import altf4

#import gevent
#from gevent import monkey
#monkey.patch_all(thread=False)
#
#from gevent import GreenletExit
#from gevent.pool import Group

import logging
import os, sys
import uuid
import subprocess
import re
import time

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

def start_greenlet(func, *args):
    try:
        func(*args)
    except GreenletExit:
        pass

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

def test_number_of_worksheets(cli, n):
    def create_worksheet(i):
        ws = 'gregwasher91-test-worksheet-{}'.format(i)
        try:
            cli.do_command(['new', ws])
        except:
            return None
        return ws

    def delete_worksheet(ws):
        return cli.do_command(['wrm', ws])

    array = [ i for i in range(n) ]
    pool = ThreadPool(n)
    ws_names = pool.map(create_worksheet, array)

    with Timer() as t:
        output = cli.do_command(['wsearch', '.mine'])
    print 'Searching through ~{} worksheets took {}s'.format(n, t.interval)

    ws_names = [ ws for ws in ws_names if ws ]
    pool = ThreadPool(len(ws_names))
    ws_names = pool.map(delete_worksheet, ws_names)

    output = cli.do_command(['wsearch', '.mine'])
    #print output


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    logger.info('hi')

    cli = BundleCLI(CodaLabManager())
    # Make sure we can connect (might prompt for username/password)
    try:
        cli.do_command(['work', 'https://worksheets-test.codalab.org/::'])
    except Exception, e:
        logger.error(e)
        sys.exit(1)

    #N = 10
    #array = [None] * N
    #pool = ThreadPool(N)
    #results = pool.map(run_stuff, array)
    test_number_of_worksheets(cli, 10)

    #cli.do_command(sys.argv[1:])
