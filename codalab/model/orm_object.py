'''
ORMObject is an abstract base class for an object can be serialized to and
deserialized from a database row. Subclasses of this class can be initialized
with a database row, and their to_dict method serializes them back to a row.

To use this class, subclass it and set its COLUMNS class attribute to be the
non-id columns of a SQLAlchemy table.
'''
from typing import Tuple

from codalab.common import precondition


class ORMObject(object):
    COLUMNS: Tuple[str, ...]

    def __init__(self, row, strict=True):
        self.update_in_memory(dict(row), strict=strict)

    def update_in_memory(self, row, strict=False):
        '''
        Initialize the attributes on this object from the data in the row.
        The attributes of the row are inferred from the table columns.

        If strict is True, checks that all columns are included in the row.
        '''
        if strict:
            for column in self.COLUMNS:
                precondition(column in row, 'Row %s missing column: %s' % (row, column))
        for (key, value) in row.items():
            message = 'Row %s has extra column: %s' % (row, key)
            precondition(key in self.COLUMNS or key == 'id', message)
            setattr(self, key, value)

    def to_dict(self, strict=True):
        """
        Return a JSON-serializable and database-uploadable dictionary that
        represents this object.

        If strict is True, checks that all columns are set in this object.
        """
        result = {}
        for column in self.COLUMNS:
            if not strict and not hasattr(self, column):
                continue
            value = getattr(self, column)
            result[column] = value
        return result
