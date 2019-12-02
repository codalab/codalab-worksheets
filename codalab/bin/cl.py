# Main entry point for CodaLab.
import sys
from codalab.lib.bundle_cli import BundleCLI
from codalab.lib.codalab_manager import CodaLabManager


def run_cli_command(argv):
    cli = BundleCLI(CodaLabManager())
    try:
        cli.do_command(argv)
    except KeyboardInterrupt:
        print('Terminated by Ctrl-C')
        sys.exit(130)

def main():
    run_cli_command(sys.argv[1:])

if __name__ == '__main__':
    main()
