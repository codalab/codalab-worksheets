'''
RunBundle is a bundle type that is produced by running a program on an input.

Its constructor takes a program target (which must be in a ProgramBundle),
an input target (which can be in any bundle), and a command to run.

When the bundle is executed, it symlinks the program target in to ./program,
symlinks the input target in to ./input, and then streams output to ./stdout
and ./stderr. The ./output directory may also be used to store output files.
'''
from typing import List

from codalab.bundles.derived_bundle import DerivedBundle
from codalab.common import UsageError

from codalab.lib.completers import DockerImagesCompleter
from codalab.objects.metadata_spec import MetadataSpec
from codalab.worker.bundle_state import State


class RunBundle(DerivedBundle):
    BUNDLE_TYPE = 'run'
    METADATA_SPECS = list(DerivedBundle.METADATA_SPECS)  # type: List
    # Note that these are strings, which need to be parsed
    # Request a machine with this much resources and don't let run exceed these resources
    # Don't format metadata specs
    # fmt: off
    METADATA_SPECS.append(MetadataSpec('request_docker_image', str, 'Which docker image (either tag or digest, e.g., '
                                                                    'codalab/default-cpu:latest) we wish to use.', completer=DockerImagesCompleter, hide_when_anonymous=True, default=None))
    METADATA_SPECS.append(MetadataSpec('request_time', str, 'Amount of time (e.g., 3, 3m, 3h, 3d) allowed for this run. Defaults to user time quota left.', formatting='duration', default=None))
    METADATA_SPECS.append(MetadataSpec('request_memory', str, 'Amount of memory (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run.', formatting='size', default='2g'))
    METADATA_SPECS.append(MetadataSpec('request_disk', str, 'Amount of disk space (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run. Defaults to user disk quota left.', formatting='size', default=None))
    METADATA_SPECS.append(MetadataSpec('request_cpus', int, 'Number of CPUs allowed for this run.', default=1))
    METADATA_SPECS.append(MetadataSpec('request_gpus', int, 'Number of GPUs allowed for this run.', default=0))
    METADATA_SPECS.append(MetadataSpec('request_queue', str, 'Submit run to this job queue.', hide_when_anonymous=True, default=None))
    METADATA_SPECS.append(MetadataSpec('request_priority', int, 'Job priority (higher is more important). Negative '
                                                                'priority bundles are queued behind bundles with no specified priority.', default=None))
    METADATA_SPECS.append(MetadataSpec('request_network', bool, 'Whether to allow network access.', default=False))
    METADATA_SPECS.append(MetadataSpec('exclude_patterns', list, 'Exclude these file patterns from being saved into the bundle contents.', default=[]))

    METADATA_SPECS.append(MetadataSpec('actions', list, 'Actions (e.g., kill) that were performed on this run.', generated=True))

    METADATA_SPECS.append(MetadataSpec('time', float, 'Amount of wall clock time (seconds) used by this run in total. '
                                                      '[Runtime of the Docker container excluding CodaLab related '
                                                      'steps such as preparing/uploading results]',
                                       generated=True,
                                       formatting='duration'))
    METADATA_SPECS.append(MetadataSpec('time_user', float, 'Amount of user time (seconds) used by this run.', generated=True, formatting='duration'))
    METADATA_SPECS.append(MetadataSpec('time_system', float, 'Amount of system time (seconds) used by this run.', generated=True, formatting='duration'))
    METADATA_SPECS.append(MetadataSpec('memory', float, 'Amount of memory (bytes) used by this run.', generated=True, formatting='size'))
    METADATA_SPECS.append(MetadataSpec('memory_max', float, 'Maximum amount of memory (bytes) used by this run at any time during execution.', generated=True, formatting='size'))

    METADATA_SPECS.append(MetadataSpec('started', int, 'Time when this bundle started executing.', generated=True, formatting='date'))
    METADATA_SPECS.append(MetadataSpec('last_updated', int, 'Time when information about this bundle was last updated.', generated=True, formatting='date'))
    METADATA_SPECS.append(MetadataSpec('run_status', str, 'Execution status of the bundle.', generated=True))
    METADATA_SPECS.append(MetadataSpec('staged_status', str, 'Information about the status of the staged bundle.', generated=True))

    # Information about running
    METADATA_SPECS.append(MetadataSpec('docker_image', str, 'Which docker image was used to run the process.', generated=True, hide_when_anonymous=True))
    METADATA_SPECS.append(MetadataSpec('exitcode', int, 'Exitcode of the process.', generated=True))
    METADATA_SPECS.append(MetadataSpec('job_handle', str, 'Identifies the job handle (internal).', generated=True, hide_when_anonymous=True))
    METADATA_SPECS.append(MetadataSpec('remote', str, 'Where this job is/was run (internal).', generated=True, hide_when_anonymous=True))
    # fmt: on

    @classmethod
    def construct(
        cls, targets, command, metadata, owner_id, uuid=None, data_hash=None, state=State.CREATED
    ):
        if not isinstance(command, str):
            raise UsageError('%r is not a valid command!' % (command,))
        return super(RunBundle, cls).construct(
            targets, command, metadata, owner_id, uuid, data_hash, state
        )

    def validate(self):
        super(RunBundle, self).validate()
        for dep in self.dependencies:
            dep.validate(require_child_path=True)
