'''
RunBundle is a bundle type that is produced by running a program on an input.

Its constructor takes a program target (which must be in a ProgramBundle),
an input target (which can be in any bundle), and a command to run.

When the bundle is executed, it symlinks the program target in to ./program,
symlinks the input target in to ./input, and then streams output to ./stdout
and ./stderr. The ./output directory may also be used to store output files.
'''
import os
import subprocess

from codalab.bundles.named_bundle import NamedBundle
from codalab.bundles.program_bundle import ProgramBundle
from codalab.common import (
  State,
  UsageError,
)
import codalab.lib.path_util as path_util


class RunBundle(NamedBundle):
  BUNDLE_TYPE = 'run'
  NAME_LENGTH = 8

  @classmethod
  def construct(cls, program_target, input_target, command, metadata):
    (program, program_path) = program_target
    (input, input_path) = input_target
    if not isinstance(program, ProgramBundle):
      raise UsageError('%s is not a program!' % (program,))
    if not isinstance(input, NamedBundle):
      raise UsageError('%s is not a named input!' % (input,))
    if not isinstance(command, basestring):
      raise UsageError('%r is not a valid command!' % (command,))
    uuid = cls.generate_uuid()
    # Support anonymous run bundles with names based on their uuid.
    if not metadata['name']:
      metadata['name'] = 'run-%s' % (uuid[:cls.NAME_LENGTH],)
    # List the dependencies of this bundle on its targets.
    dependencies = []
    targets = {'program': program_target, 'input': input_target}
    for (child_path, (parent, parent_path)) in targets.iteritems():
      dependencies.append({
        'child_uuid': uuid,
        'child_path': child_path,
        'parent_uuid': parent.uuid,
        'parent_path': parent_path,
      })
    return cls({
      'uuid': uuid,
      'bundle_type': cls.BUNDLE_TYPE,
      'command': command,
      'data_hash': None,
      'state': State.CREATED,
      'metadata': metadata,
      'dependencies': dependencies,
    })

  def run(self, bundle_store, parent_dict, temp_dir):
    command = self.command
    self.install_dependencies(bundle_store, parent_dict, temp_dir, rel=False)
    with path_util.chdir(temp_dir):
      print 'Executing command: %s' % (command,)
      print 'In temp directory: %s' % (temp_dir,)
      os.mkdir('output')
      with open('stdout', 'wb') as stdout, open('stderr', 'wb') as stderr:
        subprocess.check_call(command, stdout=stdout, stderr=stderr, shell=True)
      os.unlink('program')
      os.unlink('input')
    return bundle_store.upload(temp_dir, allow_symlinks=True)
