import logging
import os, sys
import uuid
import subprocess
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
    def __init__(self, cli, n, multithread=True):
        self.cli = cli
        self.n = n

    def __enter__(self):
        def create_worksheet(i):
            ws = 'gregwasher91-test-worksheet-{}'.format(i)
            try:
                cli.do_command(['new', ws], cli=False)
            except:
                return None
            return ws

        if self.multithread:
            array = [ i for i in range(n) ]
            pool = ThreadPool(10)
            self.ws_names = pool.map(create_worksheet, array)
        else:
            self.ws_names = [ create_worksheet(i) for i in range(n) ]

    def __exit__(self, eType, eValue, eTrace):
        def delete_worksheet(ws):
            return cli.do_command(['wrm', ws], cli=False)

        ws_names = [ ws for ws in self.ws_names if ws ]
        if self.multithread:
            pool = ThreadPool(10)
            ws_names = pool.map(delete_worksheet, ws_names)
        else:
            ws_names = [ delete_worksheet(w) for w in ws_names ]

        if eType:
            return False

class UploadBalloons(object):

    def upload_balloon(self):
        ''' tell codalab to download a balloon, aka a zip file that becomes big when unzipped '''

        self.cli.do_command([
            'upload',
            'http://cs246h.stanford.edu/map_only_data.zip' # 250MB per
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
            bundles = [ r['uuid'] for r in self.cli.do_command(['search', '.mine', 'map_only_data'], cli=False)['refs'].values() ]
            if not bundles:
                return
            self.cli.do_command(['rm'] + bundles, cli=False)
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

    cli = BundleCLI(CodaLabManager(), stdout=StringIO(), stderr=StringIO())
    # Make sure we can connect (might prompt for username/password)
    try:
        cli.do_command(['work', 'https://worksheets-test.codalab.org/::gregwasher91-worksheetname'], cli=False)
    except Exception, e:
        logger.error(e)
        sys.exit(1)

    with CreateWorksheets(cli, n):
        with Timer() as t:
            output = cli.do_command(['wsearch', '.mine'], cli=False)
        print 'Searching through ~{} worksheets took {}s'.format(n, t.interval)

    with UploadBalloons(cli):
        with Timer() as t:
            output = cli.do_command(['ls'], cli=False)
        print 'Listing ~{} bundles took {}s'.format(len(output['refs']), t.interval)

    with UploadMemoryHog(cli):
        bundle = cli.do_command([
            'run',
            ':memory_hog.py',
            '---',
            'timeout 300s python memory_hog.py 500 30'
        ], cli=False)

        cli.do_command(['wait', bundle['uuid']], cli=False)
        cli.do_command(['rm', bundle['uuid']], cli=False)
