from codalab.common import precondition


class DatabaseObject(object):
  # To use this class, subclass it and set TABLE to a SQLAlchemy table object.
  TABLE = None

  def __init__(self, row):
    self.columns = tuple(
      column.name for column in self.TABLE.c if column.name != 'id'
    )
    self.update_in_memory(dict(row), strict=True)

  def update_in_memory(self, row, strict=False):
    '''
    Initialize the attributes on this object from the data in the row.
    The attributes of the row are inferred from the table columns.

    If strict is True, checks that all columns are included in the row.
    '''
    if strict:
      for column in self.columns:
        precondition(column in row, 'Row %s missing column: %s' % (row, column))
    for (key, value) in row.iteritems():
      message = 'Row %s has extra column: %s' % (row, key)
      precondition(hasattr(self.TABLE.c, key), message)
      setattr(self, key, value)

  def to_dict(self):
    '''
    Return a JSON-serializable and database-uploadable dictionary that
    represents this object.
    '''
    return {column: getattr(self, column) for column in self.columns}
