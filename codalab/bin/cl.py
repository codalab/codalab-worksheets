#!/usr/bin/env python
# Main entry point for CodaLab.
# Run 'cl' rather than this script.
import os
import signal
import sys
import time
import subprocess

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from codalab.lib.bundle_cli import BundleCLI, Commands
from codalab.lib.codalab_manager import CodaLabManager

class ClFileWatcherEventHandler(FileSystemEventHandler):
    SERVER_PROCESS = None

    def __init__(self, argv):
        super(ClFileWatcherEventHandler, self).__init__()
        self.argv = argv
        self.restart()

    def restart(self):
        if self.SERVER_PROCESS:
            self.SERVER_PROCESS.kill()

        self.SERVER_PROCESS = subprocess.Popen(self.argv)

    def on_any_event(self, event):
        extensions_to_watch = ('.js', '.py', '.html', '.css', '.tpl')
        file_extension = os.path.splitext(event.src_path)[1]

        if file_extension in extensions_to_watch:
            print "Saw file change: %s -- restarting!" % (os.path.basename(event.src_path))
            self.restart()


def run_server_with_watch():
    modified_argv = list(sys.argv)
    modified_argv[0] = 'cl'
    modified_argv.remove('--watch')
    event_handler = ClFileWatcherEventHandler(modified_argv)
    # Listen to root dir (/codalab/bin/../../)
    path = os.path.join(os.path.dirname(__file__), '../../')
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


@Commands.command(
    'server',
    help='Start an instance of a CodaLab bundle service with a REST API.',
    arguments=(
        Commands.Argument(
            '--watch', help='Restart the server on code changes.',
            action='store_true'),
        Commands.Argument(
            '-p', '--processes',
            help='Number of processes to use. A production deployment should '
                 'use more than 1 process to make the best use of multiple '
                 'CPUs.',
            type=int, default=1),
        Commands.Argument(
            '-t', '--threads',
            help='Number of threads to use. The server will be able to handle '
                 '(--processes) x (--threads) requests at the same time.',
            type=int, default=50),
        Commands.Argument(
            '-d', '--debug', help='Run the development server for debugging.',
            action='store_true')
    ),
)
def do_rest_server_command(bundle_cli, args):
    bundle_cli._fail_if_headless(args)
    # Force initialization of the bundle store, so that the misc_temp directory is created
    bundle_cli.manager.bundle_store()
    if args.watch:
        run_server_with_watch()
    else:
        from codalab.server.rest_server import run_rest_server
        run_rest_server(bundle_cli.manager, args.debug, args.processes, args.threads)


@Commands.command(
    'bundle-manager',
    help = 'Start the bundle manager that executes run and make bundles.',
    arguments=(
        Commands.Argument(
            '--sleep-time',
            help='Number of seconds to wait between successive actions.',
            type=int, default=0.5),
    ),
)
def do_bundle_manager_command(bundle_cli, args):
    bundle_cli._fail_if_headless(args)
    from codalab.worker.bundle_manager import BundleManager
    manager = BundleManager.create(bundle_cli.manager)

    # Register a signal handler to ensure safe shutdown.
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(sig, lambda signup, frame: manager.signal())

    manager.run(args.sleep_time)

def main():
    cli = BundleCLI(CodaLabManager())
    try:
        cli.do_command(sys.argv[1:])
    except KeyboardInterrupt:
        print 'Terminated by Ctrl-C'
        sys.exit(130)

if __name__ == '__main__':
    main()
