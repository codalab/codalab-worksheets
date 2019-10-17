import json
from collections import namedtuple


class PyJSONEncoder(json.JSONEncoder):
    """
    Use with json.dumps to allow Python sets and (named)tuples to be encoded to JSON
    Also allows dicts with namedtuple and tuple keys to be JSON-encoded (as long as the keys don't include
    the special token strings used for encoding)
    """

    NAMEDTUPLE_NAME_STR = ' _namedtuple_name_'
    NAMEDTUPLE_FIELD_STR = ' _namedtuple_field_'
    NAMEDTUPLE_VAL_STR = ' _namedtuple_val_'
    TUPLE_KEY_STR = '_tuple_key_'
    TUPLE_ELEM_STR = '_tuple_sep_'

    def encode_key(self, key):
        """
        Encodes a dict key. Currently only supports encoding tuples and strings as keys
        """
        if not (isinstance(key, tuple) or isinstance(key, str) or isinstance(key, str)):
            raise Exception('PyJSON can only encode dicts with str, unicode or tuple keys')
        if hasattr(key, '_asdict'):  # detect namedtuple
            nt_name = type(key).__name__
            nt_fields = key._asdict().keys()
            nt_vals = key._asdict().values()
            if PyJSONEncoder.NAMEDTUPLE_FIELD_STR in nt_name:
                raise Exception(
                    '%s is reserved for pyjson encoder but found in namedtuple name. Can\'t encode this dict'
                    % PyJSONEncoder.NAMEDTUPLE_FIELD_STR
                )
            if PyJSONEncoder.NAMEDTUPLE_VAL_STR in nt_name:
                raise Exception(
                    '%s is reserved for pyjson encoder but found in namedtuple name. Can\'t encode this dict'
                    % PyJSONEncoder.NAMEDTUPLE_VAL_STR
                )
            if PyJSONEncoder.NAMEDTUPLE_FIELD_STR in nt_fields:
                raise Exception(
                    '%s is reserved for pyjson encoder but found in namedtuple field name. Can\'t encode this dict'
                    % PyJSONEncoder.NAMEDTUPLE_FIELD_STR
                )
            if PyJSONEncoder.NAMEDTUPLE_VAL_STR in nt_fields:
                raise Exception(
                    '%s is reserved for pyjson encoder but found in namedtuple field name. Can\'t encode this dict'
                    % PyJSONEncoder.NAMEDTUPLE_VAL_STR
                )
            if PyJSONEncoder.NAMEDTUPLE_FIELD_STR in nt_vals:
                raise Exception(
                    '%s is reserved for pyjson encoder but found in namedtuple value. Can\'t encode this dict'
                    % PyJSONEncoder.NAMEDTUPLE_FIELD_STR
                )
            if PyJSONEncoder.NAMEDTUPLE_VAL_STR in nt_vals:
                raise Exception(
                    '%s is reserved for pyjson encoder but found in namedtuple value. Can\'t encode this dict'
                    % PyJSONEncoder.NAMEDTUPLE_VAL_STR
                )

            key = '%s%s%s%s%s%s' % (
                PyJSONEncoder.NAMEDTUPLE_NAME_STR,
                nt_name,
                PyJSONEncoder.NAMEDTUPLE_FIELD_STR,
                PyJSONEncoder.NAMEDTUPLE_FIELD_STR.join(nt_fields),
                PyJSONEncoder.NAMEDTUPLE_VAL_STR,
                PyJSONEncoder.NAMEDTUPLE_VAL_STR.join(nt_vals),
            )
        if isinstance(key, tuple):
            if not all(isinstance(tuple_el, str) for tuple_el in key):
                raise Exception(
                    'Tuple elements need to be all strings (or unicode) for PyJSON to work'
                )

            if any(PyJSONEncoder.TUPLE_KEY_STR in tuple_el for tuple_el in key):
                raise Exception(
                    '%s is reserved for pyjson encoder but found in keys. Can\'t encode this dict'
                    % PyJSONEncoder.TUPLE_KEY_STR
                )

            if any(PyJSONEncoder.TUPLE_ELEM_STR in tuple_el for tuple_el in key):
                raise Exception(
                    '%s is reserved for pyjson encoder but found in keys. Can\'t encode this dict'
                    % PyJSONEncoder.TUPLE_ELEM_STR
                )

            key = '%s%s' % (PyJSONEncoder.TUPLE_KEY_STR, PyJSONEncoder.TUPLE_ELEM_STR.join(key))

        return key

    def default(self, obj):
        if hasattr(obj, '_asdict'):  # detect namedtuple
            odct = obj._asdict()
            return dict(
                _namedtuple_name=type(obj).__name__,
                _namedtuple_fields=list(odct.keys()),
                _namedtuple_values=list(self.default(o) for o in odct.values()),
            )
        elif isinstance(obj, set):
            return dict(_set_object=list(self.default(o) for o in obj))
        elif isinstance(obj, dict):
            return {self.encode_key(k): self.default(v) for k, v in obj.items()}
        elif isinstance(obj, tuple):
            return dict(_tuple_object=list(self.default(o) for o in obj))
        else:
            return obj

    def encode(self, obj):
        return super(PyJSONEncoder, self).encode(self.default(obj))


class PyJSONDecoder(json.JSONDecoder):
    """ Decoder """

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, *args, object_hook=self.json_as_python, **kwargs)

    def decode_key(self, key):
        """
        Should do the opposite of what encode_key does
        """
        if isinstance(key, str) and key.startswith(PyJSONEncoder.TUPLE_KEY_STR):
            key_str = key[len(PyJSONEncoder.TUPLE_KEY_STR) :]
            return tuple(key_str.split(PyJSONEncoder.TUPLE_ELEM_STR))
        elif isinstance(key, str) and key.startswith(PyJSONEncoder.NAMEDTUPLE_NAME_STR):
            namedtuple_name = key[
                len(PyJSONEncoder.NAMEDTUPLE_NAME_STR) : key.find(
                    PyJSONEncoder.NAMEDTUPLE_FIELD_STR
                )
            ]
            namedtuple_fields = (
                key[
                    key.find(PyJSONEncoder.NAMEDTUPLE_FIELD_STR) : key.find(
                        PyJSONEncoder.NAMEDTUPLE_VAL_STR
                    )
                ]
                .strip(PyJSONEncoder.NAMEDTUPLE_FIELD_STR)
                .split(PyJSONEncoder.NAMEDTUPLE_FIELD_STR)
            )
            namedtuple_vals = (
                key[key.find(PyJSONEncoder.NAMEDTUPLE_VAL_STR) :]
                .strip(PyJSONEncoder.NAMEDTUPLE_VAL_STR)
                .split(PyJSONEncoder.NAMEDTUPLE_VAL_STR)
            )
            ntc = namedtuple(namedtuple_name, namedtuple_fields)
            return ntc(*namedtuple_vals)
        return key

    def json_as_python(self, dct):
        if isinstance(dct, dict):
            if '_set_object' in dct:
                return set(self.json_as_python(item) for item in dct['_set_object'])
            elif '_namedtuple_name' in dct:
                ntc = namedtuple(dct['_namedtuple_name'], dct['_namedtuple_fields'])
                t = list(self.json_as_python(item) for item in dct['_namedtuple_values'])
                return ntc(*t)
            elif '_tuple_object' in dct:
                return tuple(self.json_as_python(item) for item in dct['_tuple_object'])
            else:
                return {self.decode_key(k): self.json_as_python(v) for k, v in dct.items()}
        return dct


def load(*args, **kwargs):
    return json.load(*args, cls=PyJSONDecoder, **kwargs)


def loads(*args, **kwargs):
    return json.loads(*args, cls=PyJSONDecoder, **kwargs)


def dump(*args, **kwargs):
    return json.dump(*args, cls=PyJSONEncoder, **kwargs)


def dumps(*args, **kwargs):
    return json.dumps(*args, cls=PyJSONEncoder, **kwargs)
