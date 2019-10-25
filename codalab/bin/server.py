# Main entry point for CodaLab cl-server.
import os
import sys
import time
import subprocess
import argparse

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
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
            print("Saw file change: %s -- restarting!" % (os.path.basename(event.src_path)))
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--watch', help='Restart the server on code changes.', action='store_true')
    parser.add_argument(
        '-p',
        '--processes',
        help='Number of processes to use. A production deployment should '
        'use more than 1 process to make the best use of multiple '
        'CPUs.',
        type=int,
        default=1,
    )
    parser.add_argument(
        '-t',
        '--threads',
        help='Number of threads to use. The server will be able to handle '
        '(--processes) x (--threads) requests at the same time.',
        type=int,
        default=50,
    )
    parser.add_argument(
        '-d', '--debug', help='Run the development server for debugging.', action='store_true'
    )
    args = parser.parse_args()

    if args.watch:
        run_server_with_watch()
    else:
        from codalab.server.rest_server import run_rest_server

        run_rest_server(CodaLabManager(), args.debug, args.processes, args.threads)


if __name__ == '__main__':
    main()
