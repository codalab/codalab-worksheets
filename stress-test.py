#!./venv/bin/python

"""
To Do List
Things to push the limits of:

    (Higher priority)
    - Amount of data read by a run
    - Amount of data written by a run
    - num of workers running jobs
    - num of times that a bundle is used by downstream bundles

    (Lower priority)
    - Size of bundle
    - num of bundle dependencies for a run/make bundle
    - num users in a group
    - num of permissions for a bundle

"""

import logging
import os, sys
import uuid
import re
import time
from cStringIO import StringIO

from codalab.lib.bundle_cli import BundleCLI, Commands
from codalab.lib.codalab_manager import CodaLabManager
from codalab.common import UsageError

from multiprocessing.dummy import Pool as ThreadPool

logger = logging.getLogger(__file__)

class Timer(object):
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, eType, eValue, eTrace):
        self.end = time.clock()
        self.interval = self.end - self.start

        if eType:
            return False

class CreateWorksheets(object):
    def __init__(self, cli, n, multithread=True, batch_size=50):
        self.cli = cli
        self.n = n
        self.multithread = multithread
        self.batch_size = batch_size

    def __enter__(self):
        def create_worksheet(i):
            ws = 'gregwasher91-test-worksheet-{}'.format(i)
            try:
                cli.do_command(['new', ws], cli=False)
            except:
                return None
            return ws

        if self.multithread:
            array = [i for i in range(self.n)]
            pool = ThreadPool(self.batch_size)
            self.ws_names = pool.map(create_worksheet, array)
        else:
            self.ws_names = [ create_worksheet(i) for i in range(self.n) ]

    def __exit__(self, eType, eValue, eTrace):
        def delete_worksheet(ws):
            return cli.do_command(['wrm', ws], cli=False)

        ws_names = [ws for ws in self.ws_names if ws]
        if self.multithread:
            pool = ThreadPool(10)
            ws_names = pool.map(delete_worksheet, ws_names)
        else:
            ws_names = [delete_worksheet(w) for w in ws_names]

        if eType:
            return False

class CreateWorksheet(object):
    def __init__(self, cli, suffix, prefix='gregwasher91-stress-'):
        self.cli = cli
        self.ws_name = prefix + suffix

    def __enter__(self):
        try:
            cli.do_command(['new', self.ws_name], cli=False)
            cli.do_command(['work', 'https://worksheets-test.codalab.org/::{}'.format(self.ws_name)], cli=False)
        except:
            return None
        return self.ws_name

    def __exit__(self, eType, eValue, eTrace):
        cli.do_command(['wrm', '--force', self.ws_name], cli=False)
        if eType:
            return False

class UploadBalloons(object):

    def upload_balloon(self):
        ''' tell codalab to download a balloon, aka a zip file that becomes big when unzipped '''

        self.cli.do_command([
            'upload',
            'stress-test-data/balloon.zip' # 250MB per
        ], cli=False)

    def __init__(self, cli):
        self.cli = cli

    def __enter__(self):
        try:
            c = 0
            while True:
                self.upload_balloon()
                c += 1
        except UsageError:
            print "Reached disk quota. Uploaded {} balloons".format(c)

    def __exit__(self, eType, eValue, eTrace):
        while 1: # remove uploaded bundles. Loop because only 10 results are returned at a time
            bundles = [r['uuid'] for r in self.cli.do_command(['search', '.mine', 'balloon'], cli=False)['refs'].values()]
            if not bundles:
                return
            self.cli.do_command(['rm'] + bundles, cli=False)
        if eType:
            return False

class UploadTiny(object):

    def upload_tiny(self):
        ''' upload lots tiny files '''

        try:
            bundle = self.cli.do_command([
                'upload',
                'stress-test-data/tiny.txt'
            ], cli=False)
            self.bundles.append(bundle)
            return True
        except UsageError, e:
            print e
            return False

    def __init__(self, cli, n=3000, cleanup=True):
        self.cli = cli
        self.n = n
        self.batch_size = 100
        self.cleanup = cleanup
        self.bundles = []

    def __enter__(self):
        stop = False
        c = 0
        while not stop and c < self.n:
            array = [i for i in range(self.batch_size)]
            pool = ThreadPool(self.batch_size)
            results = pool.map(lambda x: self.upload_tiny(), array)
            stop = not all(r for r in results)
            c += self.batch_size
            print "uploaded c={}".format(c)

    def __exit__(self, eType, eValue, eTrace):
        while not self.cleanup: # remove uploaded bundles. Loop because only 10 results are returned at a time
            bundles = [r['uuid'] for r in self.cli.do_command(['search', '.mine', 'tiny'], cli=False)['refs'].values()]
            if not bundles:
                return
            self.cli.do_command(['rm'] + bundles, cli=False)
            #self.cli.do_command(['rm'] + self.bundles, cli=False)
        if eType:
            return False

class UploadMemoryHog(object):
    def __init__(self, cli):
        self.cli = cli

    def __enter__(self):
        self.bundle = self.cli.do_command([
            'upload',
            'stress-test-data/memory_hog.py',
        ], cli=False)

    def __exit__(self, eType, eValue, eTrace):
        self.cli.do_command([
            'rm',
            self.bundle['uuid']
        ], cli=False)

        if eType:
            return False

if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    print sys.argv
    test_id = sys.argv[1]

    cli = BundleCLI(CodaLabManager(), stdout=StringIO(), stderr=StringIO())

    # Make sure we can connect (might prompt for username/password)
    try:
        print cli.do_command(['work', 'https://worksheets-test.codalab.org/::'], cli=False)
    except Exception, e:
        logger.error(e)
        sys.exit(1)

    '''
    n = 300
    with CreateWorksheets(cli, n):
        with Timer() as t:
            output = cli.do_command(['wsearch', '.mine'], cli=False)
        print 'Searching through ~{} worksheets took {}s'.format(n, t.interval)

    with UploadBalloons(cli):
        with Timer() as t:
            output = cli.do_command(['search', '.mine'], cli=False)
        print 'Listing ~{} bundles took {}s'.format(len(output['refs']), t.interval)
    '''

    with CreateWorksheet(cli, test_id) as ws_name:
        with UploadMemoryHog(cli):
            def run(cli):
                bundle = cli.do_command([
                    'run',
                    ':memory_hog.py',
                    '---',
                    'python memory_hog.py 1000 180'
                ], cli=False)
                return bundle

            bundles = []
            for i in range(8):
                bundles.append(run(cli))

            t = 0
            while t < 3000:
                bundle_stats = [cli.do_command(['info', bundle['uuid']], cli=False) for bundle in bundles]
                if all(b['state'] in ('ready', 'failed') for b in bundle_stats): # TODO: report failed bundles
                    for bundle in bundles:
                        cli.do_command(['rm', bundle['uuid']], cli=False)
                    break;
                else:
                    time.sleep(1)
                    t += 1

    '''
    with UploadTiny(cli, n=3000, cleanup=False):
        with Timer() as t:
            output = cli.do_command(['search', '.mine'], cli=False)
        print 'Listing ~{} bundles took {}s'.format(len(output['refs']), t.interval)
    '''

