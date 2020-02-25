# Main entry point for CodaLab cl-bundle-manager.
import signal
import argparse
from codalab.lib.codalab_manager import CodaLabManager
from codalab.server.bundle_manager import BundleManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--sleep-time',
        help='Number of seconds to wait between successive actions.',
        type=int,
        default=0.5,
    )
    args = parser.parse_args()

    manager = BundleManager(CodaLabManager())
    # Register a signal handler to ensure safe shutdown.
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(sig, lambda signup, frame: manager.signal())

    manager.run(args.sleep_time)


if __name__ == '__main__':
    main()
