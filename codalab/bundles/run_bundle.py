from codalab.bundles.named_bundle import NamedBundle
from codalab.bundles.program_bundle import ProgramBundle
from codalab.common import (
  State,
  UsageError,
)


class RunBundle(NamedBundle):
  BUNDLE_TYPE = 'run'
  NAME_LENGTH = 8

  @classmethod
  def construct(cls, program, input, command):
    if not isinstance(program, ProgramBundle):
      raise UsageError('%s is not a program!' % (program,))
    if not isinstance(input, NamedBundle):
      raise UsageError('%s is not a named input!' % (input,))
    if not isinstance(command, basestring):
      raise UsageError('%r is not a valid command!' % (command,))
    uuid = cls.generate_uuid()
    # Compute metadata with default values for name and description.
    description = 'Run %s on %s: %r' % (
      program.metadata.name,
      input.metadata.name,
      command,
    )
    metadata = {
      'name': 'run-%s' % (uuid[:cls.NAME_LENGTH],),
      'description': description,
      'tags': [],
    }
    # List the dependencies of this bundle on its targets.
    dependencies = []
    for (parent, child_path) in ((program, 'program'), (input, 'input')):
      dependencies.append({
        'child_uuid': uuid,
        'child_path': child_path,
        'parent_uuid': parent.uuid,
        'parent_path': '',
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

  def run(self, bundle_store, parent_dict):
    temp_dir = self.symlink_dependencies(bundle_store, parent_dict)
    raise NotImplementedError(temp_dir)
