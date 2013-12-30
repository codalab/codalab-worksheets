BUFFER_SIZE = 0x40000


def copy(source, dest):
  '''
  Read from the source file handle and write the data to the dest file handle.
  '''
  while True:
    buffer = source.read(BUFFER_SIZE)
    if not buffer:
      break
    dest.write(buffer)
    dest.flush()
