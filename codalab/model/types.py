import array
import sqlalchemy.types as types

class UnicodeCoerceBase(types.TypeDecorator):
    """Base class for Unicode Coerce types. This allows one to store
    Unicode values in a latin1-encoded column, by encoding the data
    on its way in and decoding it on its way out.
    """

    # Subclasses must override impl, which is the SQLAlchemy base type whose
    # functionality the subclass extends.
    impl = None

    def process_bind_param(self, value, dialect):
        # Sometimes non-strings (or None) could be stored in this
        # column, so we check to make sure it's a string first.
        if type(value) is str:
            return value.encode()
        return value

    def process_result_value(self, value, dialect):
        if type(value) is str:
            return array.array('B', [ord(char) for char in value]).tostring().decode()
        return value

class UnicodeCoerceString(UnicodeCoerceBase):
    impl = types.String

class UnicodeCoerceText(UnicodeCoerceBase):
    impl = types.Text