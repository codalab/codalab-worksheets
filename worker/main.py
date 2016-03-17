#!/usr/bin/env python2.7
# TODO(klopyrev): This worker in general needs client level documentation. I
#                 need to figure out where it is most appropriate and put it
#                 there. Thus, I'm omitting any documentation here for now.

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
from worker import Worker

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CodaLab worker.')
    parser.add_argument('--id', default='%s(%d)' % (socket.gethostname(), os.getpid()),
                        help='ID to use for the worker. If not specified, one '
                             'will be assigned.')
    parser.add_argument('--bundle-service-url', required=True,
                        help='URL of the bundle service, in the format '
                             '<http|https>://<hostname>[:<port>]')
    parser.add_argument('--work-dir', default='scratch',
                        help='Directory where to store temporary bundle data.')
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
    parser.add_argument('--shared-file-system', action='store_true',
                        help='Internal use: Whether the file system containing '
                             'bundle data is shared between the bundle service '
                             'and the worker. ')
    args = parser.parse_args()

    # Get the username and password.
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

    worker = Worker(args.id, args.work_dir, args.shared_file_system, args.slots,
                    BundleServiceClient(args.bundle_service_url,
                                        username, password),
                    DockerClient())

    # Register a signal handler to ensure safe shutdown.
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(sig, lambda signup, frame: worker.signal())

    worker.run()
