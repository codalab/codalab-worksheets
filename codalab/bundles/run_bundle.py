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
  def construct(cls, program_target, input_target, command):
    (program, program_path) = program_target
    (input, input_path) = input_target
    if not isinstance(program, ProgramBundle):
      raise UsageError('%s is not a program!' % (program,))
    if not isinstance(input, NamedBundle):
      raise UsageError('%s is not a named input!' % (input,))
    if not isinstance(command, basestring):
      raise UsageError('%r is not a valid command!' % (command,))
    uuid = cls.generate_uuid()
    # Compute metadata with default values for name and description.
    description = 'Run %s/%s on %s/%s: %r' % (
      program.metadata.name,
      program_path,
      input.metadata.name,
      input_path,
      command,
    )
    metadata = {
      'name': 'run-%s' % (uuid[:cls.NAME_LENGTH],),
      'description': description,
      'tags': [],
    }
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
    for macro in ('program', 'input', 'output'):
      command = command.replace('$' + macro, macro)
    stdout_path = os.path.join('output', 'stdout')
    stderr_path = os.path.join('output', 'stderr')
    self.install_dependencies(bundle_store, parent_dict, temp_dir, rel=False)
    with path_util.chdir(temp_dir):
      print 'Executing command: %s' % (command,)
      print 'In temp directory: %s' % (temp_dir,)
      os.mkdir('output')
      with open(stdout_path, 'wb') as stdout, open(stderr_path, 'wb') as stderr:
        subprocess.check_call(command, stdout=stdout, stderr=stderr, shell=True)
      os.unlink('program')
      os.unlink('input')
    self.install_dependencies(bundle_store, parent_dict, temp_dir, rel=True)
    return bundle_store.upload(temp_dir, allow_symlinks=True)
