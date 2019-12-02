# Main entry point for CodaLab.
import sys
from codalab.lib.bundle_cli import BundleCLI
from codalab.lib.codalab_manager import CodaLabManager


def main():
    cli = BundleCLI(CodaLabManager())
    try:
        cli.do_command(sys.argv[1:])
    except KeyboardInterrupt:
        print('Terminated by Ctrl-C')
        sys.exit(130)


if __name__ == '__main__':
    main()
