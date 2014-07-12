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
import re

from codalab.bundles.named_bundle import NamedBundle
from codalab.bundles.program_bundle import ProgramBundle
from codalab.common import (
  State,
  UsageError,
)
from codalab.lib import (
  path_util,
  spec_util,
)


class RunBundle(NamedBundle):
    BUNDLE_TYPE = 'run'

    @classmethod
    def construct(cls, targets, command, metadata, uuid=None, data_hash=None):
        if not uuid: uuid = spec_util.generate_uuid()
        # Check that targets does not include both keyed and anonymous targets.
        if len(targets) > 1 and '' in targets:
            raise UsageError('Must specify keys when packaging multiple targets!')
        if not isinstance(command, basestring):
            raise UsageError('%r is not a valid command!' % (command,))
        # Support anonymous run bundles with names based on their commands
        if not metadata['name']:
            metadata['name'] = spec_util.create_default_name(cls.BUNDLE_TYPE, command)
        # List the dependencies of this bundle on its targets.
        dependencies = []
        for (child_path, (parent_uuid, parent_path)) in targets.iteritems():
            dependencies.append({
              'child_uuid': uuid,
              'child_path': child_path,
              'parent_uuid': parent_uuid,
              'parent_path': parent_path,
            })
        return super(RunBundle, cls).construct({
          'uuid': uuid,
          'bundle_type': cls.BUNDLE_TYPE,
          'command': command,
          'data_hash': data_hash,
          'state': State.CREATED,
          'metadata': metadata,
          'dependencies': dependencies,
        })

    def get_hard_dependencies(self):
        # The program and input are symlinked into a run bundle while it is being
        # executed, but they are deleted once the run is complete.
        return []

    def complete(self, bundle_store, parent_dict, temp_dir):
        command = self.command
        path_util.make_directory(temp_dir)
        self.install_dependencies(bundle_store, parent_dict, temp_dir, relative_symlinks=False)
        # TODO: have a mode where we ssh into another machine to do this
        # In that case, need to copy files around.
        with path_util.chdir(temp_dir):
            print 'Executing command: %s' % (command,)
            print 'In temp directory: %s' % (temp_dir,)
            os.mkdir('output')  # Only stuff written to the output directory is copied back.
            with open('stdout', 'wb') as stdout, open('stderr', 'wb') as stderr:
                subprocess.check_call(command, stdout=stdout, stderr=stderr, shell=True)
            #os.unlink('program')
            #os.unlink('input')
        return bundle_store.upload(temp_dir, allow_symlinks=True)
