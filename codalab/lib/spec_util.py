'''
spec_util contains some simple methods to generate and check names and uuids.
'''
import re
import uuid

from codalab.common import (
  precondition,
  UsageError,
)

UUID_REGEX = re.compile('^0x[0-9a-f]{32}$')
UUID_PREFIX_REGEX = re.compile('^0x[0-9a-f]{1,31}$')

NAME_REGEX = re.compile('^[a-zA-Z_][a-zA-Z0-9_\.\-]*$')  # Names must match this
NAME_PATTERN_REGEX = re.compile('^([a-zA-Z_][a-zA-Z0-9_\.\-]*\\$?)$')  # Name pattern
NAME_HISTORY_REGEX = re.compile('^([a-zA-Z_][a-zA-Z0-9_\.\-]*\$?)\^([0-9]*)$')
HISTORY_REGEX = re.compile('^\^([0-9]*)$')
ID_REGEX = re.compile('^[0-9]+\Z')
NOT_NAME_CHAR_REGEX = re.compile('[^a-zA-Z0-9_\.\-]')

def generate_uuid():
    return '0x%s' % (uuid.uuid4().hex,)


def check_uuid(uuid):
    '''
    Raise a PreconditionViolation if the uuid does not conform to its regex.
    '''
    message = 'uuids must match %s, was %s' % (UUID_REGEX.pattern, uuid)
    precondition(UUID_REGEX.match(uuid), message)


def check_name(name):
    if not NAME_REGEX.match(name):
        raise UsageError('Names must match %s, was %s' % (NAME_REGEX.pattern, name))

def check_id(owner_id):
    if owner_id != None and type(owner_id) != int:
        raise UsageError('ID must be an integer.')

def shorten_name(name, n=32):
    if len(name) <= 32: return name
    return name[0:n/2-1] + '..' + name[len(name)-n/2+1:]

def create_default_name(bundle_type, raw_material):
    name = bundle_type + '-' + NOT_NAME_CHAR_REGEX.sub('-', raw_material)
    name = re.compile('\-+').sub('-', name)
    name = shorten_name(name)  # Shorten
    return name
