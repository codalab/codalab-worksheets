import contextlib
import os
import sys

from codalab.common import (
  precondition,
  UsageError,
)


@contextlib.contextmanager
def chdir(new_dir):
  cur_dir = os.getcwd()
  try:
    os.chdir(new_dir)
    yield
  finally:
    os.chdir(cur_dir)


def ls(path):
  precondition(os.path.isabs(path), 'ls got relative path: %s' % (path,))
  if not os.path.exists(path):
    raise UsageError('ls got non-existent path: %s' % (path,))
  if not os.path.isdir(path):
    raise UsageError('ls got non-directory: %s' % (path,))
  (directories, files) = ([], [])
  for file_name in os.listdir(path):
    if os.path.isfile(os.path.join(path, file_name)):
      files.append(file_name)
    else:
      directories.append(file_name)
  return (directories, files)


def cat(path):
  precondition(os.path.isabs(path), 'cat got relative path: %s' % (path,))
  if not os.path.exists(path):
    raise UsageError('cat got non-existent path: %s' % (path,))
  if os.path.isdir(path):
    raise UsageError('cat got directory: %s' % (path,))
  BUFFER_SIZE = 0x40000
  with open(path, 'rb') as f:
    while True:
      buffer = f.read(BUFFER_SIZE)
      if not buffer:
        break
      sys.stdout.write(buffer)
      sys.stdout.flush()
