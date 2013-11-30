import os


def ls(path):
  if not os.path.isabs(path):
    raise ValueError('Tried to ls relative path: %s' % (path,))
  (directories, files) = ([], [])
  for file_name in os.listdir(path):
    if os.path.isfile(os.path.join(path, file_name)):
      files.append(file_name)
    else:
      directories.append(file_name)
  return (directories, files)
