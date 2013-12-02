import os

from codalab.common import (
  precondition,
  UsageError,
)


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
