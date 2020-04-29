"""
Generate CLI docs.
"""
import os
import argparse
from codalab.lib.bundle_cli import Commands

INTRODUCTION = """# CLI Reference

This file is auto-generated from the output of `cl help -v -m` and provides the list of all CLI commands.
"""


def indent(s):
    return '\n'.join(line if line else '' for line in s.split('\n'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--docs', default='docs')
    args = parser.parse_args()

    with open(os.path.join(args.docs, 'CLI-Reference.md'), 'w') as f:
        print(INTRODUCTION, file=f)
        print(indent(Commands.help_text(verbose=True, markdown=True)), file=f)


if __name__ == '__main__':
    main()
