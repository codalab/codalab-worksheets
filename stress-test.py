#!./venv/bin/python

import logging
import os, sys
import uuid
import re
import time
import tempfile
from cStringIO import StringIO
import sys, random, string

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

def CreateRandomFile(size_mb=50, chunk_size_mb=50):
    with open('tmp_file', 'w') as f:
        while size_mb > 0:
            s = string.lowercase + string.digits + string.uppercase
            contents = ''.join(random.choice(s) for i in xrange(int(chunk_size_mb * 1024 * 1024)))
            f.write(contents)
            size_mb -= chunk_size_mb
    return 'tmp_file'

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

class UploadFile(object):
    def __init__(self, cli, filename):
        self.cli = cli
        self.filename = filename

    def __enter__(self):
        self.bundle = self.cli.do_command([
            'upload',
            self.filename,
        ], cli=False)
        return self.bundle['uuid']

    def __exit__(self, eType, eValue, eTrace):
        self.cli.do_command([
            'rm',
            self.bundle['uuid']
        ], cli=False)

        if eType:
            return False

def wait_for_bundles(bundles, max_seconds):
    ''' return 0 if all bundles succeed, X > 0 if X bundles failed, or -1 if timeout '''

    t = 0
    while t < max_seconds:
        bundle_stats = [cli.do_command(['info', bundle['uuid']], cli=False) for bundle in bundles]
        if all(b['state'] in ('ready', 'failed') for b in bundle_stats): # TODO: report failed bundles
            num_failed = sum([1 for b in bundle_stats if b['state'] == 'failed'])
            for bundle in bundles:
                cli.do_command(['rm', bundle['uuid']], cli=False)
            return num_failed
        else:
            time.sleep(1)
            t += 1
    return -1

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

    def test_create_worksheets():
        n = 300
        with CreateWorksheets(cli, n):
            with Timer() as t:
                output = cli.do_command(['wsearch', '.mine'], cli=False)
            print 'Searching through ~{} worksheets took {}s'.format(n, t.interval)

    def test_upload_balloons():
        with UploadBalloons(cli):
            with Timer() as t:
                output = cli.do_command(['search', '.mine'], cli=False)
            print 'Listing ~{} bundles took {}s'.format(len(output['refs']), t.interval)

    def test_count_lines():
        with UploadFile(cli, 'stress-test-data/count_lines.py') as count_lines:
            with CreateWorksheet(cli, test_id) as ws_name:
                with UploadFile(cli, 'stress-test-data/memory_hog.py'):
                    #def run(cli):
                    #    bundle = cli.do_command([
                    #        'run',
                    #        ':memory_hog.py',
                    #        '---',
                    #        'python memory_hog.py 1000 180'
                    #    ], cli=False)
                    #    return bundle

                    def run(cli):
                        bundle = cli.do_command([
                            'run',
                            'balloon:0x5a49ab',
                            'count_lines.py:{}'.format(count_lines),
                            '---',
                            'python count_lines.py balloon 10'
                        ], cli=False)
                        return bundle

                    bundles = []
                    for i in range(4):
                        bundles.append(run(cli))

                    wait_for_bundles(bundles, 1200)

    def test_print_lines():
        with UploadFile(cli, 'stress-test-data/print_lines.py') as print_lines:
            with CreateWorksheet(cli, test_id) as ws_name:
                    def run(cli):
                        bundle = cli.do_command([
                            'run',
                            'print_lines.py:{}'.format(print_lines),
                            '---',
                            'python print_lines.py 0.5 180'
                        ], cli=False)
                        return bundle

                    bundles = []
                    for i in range(1):
                        bundles.append(run(cli))

                    code = wait_for_bundles(bundles, 1200)
                    if code != 0:
                        raise Exception('Failed run: wait_for_bundles returns {}'.format(code))

    def test_upload_big():
        with CreateWorksheet(cli, test_id) as ws_name:
            for i in range(1):
                filename = CreateRandomFile(size_mb=1024*2)
                with UploadFile(cli, filename) as bundle:
                    print cli.do_command(['info', bundle], cli=False)

    def test_upload_big():
        with UploadTiny(cli, n=3000, cleanup=False):
            with Timer() as t:
                output = cli.do_command(['search', '.mine'], cli=False)
            print 'Listing ~{} bundles took {}s'.format(len(output['refs']), t.interval)

