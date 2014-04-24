'''
spec_util contains some simple methods to generate and check names and uuids.
'''
import re
import uuid

from codalab.common import (
  precondition,
  UsageError,
)


UUID_REGEX = re.compile('^0x[0-9a-f]{32}\Z')
UUID_PREFIX_REGEX = re.compile('^0x[0-9a-f]{1,31}\Z')

NAME_REGEX = re.compile('^[a-zA-Z_][a-zA-Z0-9_\.\-]*\Z')
ID_REGEX = re.compile('^[0-9]+\Z')

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
    if owner_id != None and not ID_REGEX.match(owner_id):
        raise UsageError('IDs must match %s, was %s' % (ID_REGEX.pattern, name))

