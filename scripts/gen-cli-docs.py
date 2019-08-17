"""
Generate CLI docs.
"""
import os
import sys
from codalab.bin import cl  # Important to put here to register the commands.
from codalab.lib.bundle_cli import Commands

INTRODUCTION = """# CLI Reference

This file is auto-generated from the output of `cl help -v` and provides the list of all CLI commands.
"""


def indent(s, padding='    '):
    # Add `padding` in front of each non-empty line.
    return '\n'.join(padding + line if line else '' for line in s.split('\n'))


def main():
    with open(os.path.join('docs', 'CLI-Reference.md'), 'w') as f:
        print(INTRODUCTION, file=f)
        print(indent(Commands.help_text(True)), file=f)


if __name__ == '__main__':
    main()
