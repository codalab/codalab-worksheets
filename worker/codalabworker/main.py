#!/usr/bin/env python2.7
# For information about the design of the worker, see design.pdf in the same
# directory as this file. For information about running a worker, see the
# tutorial on the CodaLab Wiki.

import argparse
import getpass
import os
import logging
import signal
import socket
import stat
import sys

from bundle_service_client import BundleServiceClient
from docker_client import DockerClient
from formatting import parse_size
from worker import Worker

def main():
    parser = argparse.ArgumentParser(description='CodaLab worker.')
    parser.add_argument('--tag',
                        help='Tag that allows for scheduling runs on specific '
                             'workers.')
    parser.add_argument('--server', required=True,
                        help='URL of the CodaLab server, in the format '
                             '<http|https>://<hostname>[:<port>] (e.g., https://worksheets.codalab.org)')
    parser.add_argument('--work-dir', default='codalab-worker-scratch',
                        help='Directory where to store temporary bundle data, '
                             'including dependencies and the data from run '
                             'bundles.')
    parser.add_argument('--max-work-dir-size', type=str, metavar='SIZE', default='10g',
                        help='Maximum size of the temporary bundle data '
                             '(e.g., 3, 3k, 3m, 3g, 3t).')
    parser.add_argument('--max-image-cache-size', type=str, metavar='SIZE',
                        help='Limit the disk space used to cache Docker images '
                             'for worker jobs to the specified amount (e.g. '
                             '3, 3k, 3m, 3g, 3t). If the limit is exceeded, '
                             'the least recently used images are removed first. '
                             'Worker will not remove any images if this option '
                             'is not specified.')
    parser.add_argument('--slots', type=int, default=1,
                        help='Number of slots to use for running bundles. '
                             'A single bundle takes up a single slot.')
    parser.add_argument('--password-file',
                        help='Path to the file containing the username and '
                             'password for logging into the bundle service, '
                             'each on a separate line. If not specified, the '
                             'password is read from standard input.')
    parser.add_argument('--verbose', action='store_true',
                        help='Whether to output verbose log messages.')
    parser.add_argument('--id', default='%s(%d)' % (socket.gethostname(), os.getpid()),
                        help='Internal use: ID to use for the worker.')
    parser.add_argument('--shared-file-system', action='store_true',
                        help='Internal use: Whether the file system containing '
                             'bundle data is shared between the bundle service '
                             'and the worker.')
    args = parser.parse_args()

    # Get the username and password.
    print 'Connecting to %s' % args.server
    if args.password_file:
        if os.stat(args.password_file).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            print >> sys.stderr, """
Permissions on password file are too lax.
Only the user should be allowed to access the file.
On Linux, run:
chmod 600 %s""" % args.password_file
            exit(1)
        with open(args.password_file) as f:
            username = f.readline().strip()
            password = f.readline().strip()
    else:
        username = raw_input('Username: ')
        password = getpass.getpass()

    # Set up logging.
    if args.verbose:
        logging.basicConfig(format='%(asctime)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s',
                            level=logging.INFO)

    max_work_dir_size_bytes = parse_size(args.max_work_dir_size)
    if args.max_image_cache_size is None:
        max_images_bytes = None
    else:
        max_images_bytes = parse_size(args.max_image_cache_size)
    worker = Worker(args.id, args.tag, args.work_dir, max_work_dir_size_bytes,
                    max_images_bytes, args.shared_file_system, args.slots,
                    BundleServiceClient(args.server, username, password),
                    DockerClient())

    # Register a signal handler to ensure safe shutdown.
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(sig, lambda signup, frame: worker.signal())

    print 'Worker started.'
    worker.run()

if __name__ == '__main__':
    main()
