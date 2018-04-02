import json
import sys
from collections import namedtuple

class PyJSONEncoder(json.JSONEncoder):
    """
    Use with json.dumps to allow Python sets and (named)tuples to be encoded to JSON
    """

    def default(self, obj):
        if hasattr(obj, "_asdict"): # detect namedtuple
            odct = obj._asdict()
            return dict(
                    _namedtuple_name=type(obj).__name__,
                    _namedtuple_fields=odct.keys(),
                    _namedtuple_values=list(self.default(o) for o in odct.values()),
            )
        elif isinstance(obj, set):
            return dict(_set_object=list(self.default(o) for o in obj))
        elif isinstance(obj, dict):
            return {k: self.default(v) for k, v in obj.items()}
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
                return {k: self.json_as_python(v) for k, v in dct.items()}
        return dct

def load(*args, **kwargs):
    return json.load(*args, cls=PyJSONDecoder, **kwargs)

def loads(*args, **kwargs):
    return json.loads(*args, cls=PyJSONDecoder, **kwargs)

def dump(*args, **kwargs):
    return json.dump(*args, cls=PyJSONEncoder, **kwargs)

def dumps(*args, **kwargs):
    return json.dumps(*args, cls=PyJSONEncoder, **kwargs)
