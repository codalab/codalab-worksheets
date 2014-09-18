#!/usr/bin/env python
# Main entry point for CodaLab.
# Run 'cl' rather than this script.
import os
import sys
import time
import subprocess

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from codalab.lib.codalab_manager import CodaLabManager


class ClFileWatcherEventHandler(FileSystemEventHandler):
    SERVER_PROCESS = None
    manager = None

    def __init__(self, manager):
        super(ClFileWatcherEventHandler, self).__init__()
        self.manager = manager
        self.restart()

    def restart(self):
        if self.SERVER_PROCESS:
            self.SERVER_PROCESS.kill()

        self.SERVER_PROCESS = subprocess.Popen(['cl', 'server'])

    def on_any_event(self, event):
        extensions_to_watch = ('.js', '.py', '.html', '.css')
        file_extension = os.path.splitext(event.src_path)[1]

        if file_extension in extensions_to_watch:
            print "Saw file change: %s -- restarting!" % (os.path.basename(event.src_path))
            self.restart()


if __name__ == '__main__':
    manager = CodaLabManager()
    # Either start the server or the client.
    if len(sys.argv) > 1 and sys.argv[1] == 'server':
        if '--watch' in sys.argv:
            # Listen to root dir (/codalab/bin/../../)
            path = os.path.join(os.path.dirname(__file__), '../../')
            event_handler = ClFileWatcherEventHandler(manager)
            observer = Observer()
            observer.schedule(event_handler, path, recursive=True)
            observer.start()
            try:
                while True:
                    time.sleep(100)
            except KeyboardInterrupt:
                observer.stop()
            observer.join()
        else:
            from codalab.server.bundle_rpc_server import BundleRPCServer
            rpc_server = BundleRPCServer(manager)
            rpc_server.serve_forever()
    else:
        from codalab.lib.bundle_cli import BundleCLI
        cli = BundleCLI(manager)
        cli.do_command(sys.argv[1:])
