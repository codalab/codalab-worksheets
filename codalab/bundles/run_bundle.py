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
    METADATA_SPECS.append(
        MetadataSpec(
            'request_docker_image',
            str,
            'Which docker image (either tag or digest, e.g., codalab/default-cpu:latest) we wish to use (request_docker_image).',
            completer=DockerImagesCompleter,
            hide_when_anonymous=True,
            default=None,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_time',
            str,
            'Amount of time (e.g., 3, 3m, 3h, 3d) allowed for this run (request_time). Defaults to user time quota left.',
            formatting='duration',
            default=None,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_memory',
            str,
            'Amount of memory (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_memory).',
            formatting='size',
            default='2g',
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_disk',
            str,
            'Amount of disk space (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_disk). Defaults to user disk quota left.',
            formatting='size',
            default=None,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_cpus',
            int,
            'Number of CPUs allowed for this run (request_cpus).',
            default=1,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_gpus',
            int,
            'Number of GPUs allowed for this run (request_gpus).',
            default=0,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_queue',
            str,
            'Submit run to this job queue (request_queue).',
            hide_when_anonymous=True,
            default=None,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_priority',
            int,
            'Job priority (request_priority). Higher is more important. Negative priority bundles are queued behind bundles with no specified priority.',
            default=None,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'request_network',
            bool,
            'Whether to allow network access (request_network).',
            default=True,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'cpu_usage',
            float,
            'Proportion of current CPU usage for a running bundle (cpu_usage). This field is only relevant for running bundles. (e.g., 0.24)',
            default=0.0,
            generated=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'memory_usage',
            float,
            'Proportion of current memory usage based on the memory limit (memory_usage).',
            default=0.0,
            generated=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'exclude_patterns',
            list,
            'Exclude these file patterns from being saved into the bundle contents (exclude_patterns).',
            default=[],
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'store',
            str,
            'The name of the bundle store where bundle results should be initially uploaded (store). If unspecified, an optimal available bundle store will be chosen.',
            default=None,
            hidden=True,
            optional=True,
            lock_after_start=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'actions',
            list,
            'Actions (e.g., kill) that were performed on this run (actions).',
            generated=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'time',
            float,
            'Amount of wall clock time (seconds) used by this run in total (time). [Runtime of the Docker container excluding CodaLab related steps such as preparing/uploading results]',
            generated=True,
            formatting='duration',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'time_user',
            float,
            'Amount of user time (seconds) used by this run (time_user).',
            generated=True,
            formatting='duration',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'time_system',
            float,
            'Amount of system time (seconds) used by this run (time_system).',
            generated=True,
            formatting='duration',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'memory',
            float,
            'Amount of memory (bytes) used by this run (memory).',
            generated=True,
            formatting='size',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'memory_max',
            float,
            'Maximum amount of memory (bytes) used by this run at any time during execution (memory_max).',
            generated=True,
            formatting='size',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'started',
            int,
            'Time when this bundle started executing (started).',
            generated=True,
            formatting='date',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'last_updated',
            int,
            'Time when information about this bundle was last updated (last_updated).',
            generated=True,
            formatting='date',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'run_status', str, 'Execution status of the bundle (run_status).', generated=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'staged_status',
            str,
            'Information about the status of the staged bundle (staged_status).',
            generated=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'time_preparing',
            float,
            'Amount of system time in the PREPARING stage (time_preparing).',
            generated=True,
            formatting='duration',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'time_running',
            float,
            'Amount of system time in the RUNNING stage (time_running).',
            generated=True,
            formatting='duration',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'time_cleaning_up',
            float,
            'Amount of system time in the CLEANING_UP stage (time_cleaning_up).',
            generated=True,
            formatting='duration',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'time_uploading_results',
            float,
            'Amount of system time in the UPLOADING_RESULTS stage (time_uploading_results).',
            generated=True,
            formatting='duration',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'docker_image',
            str,
            'Which docker image was used to run the process (docker_image).',
            generated=True,
            hide_when_anonymous=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec('exitcode', int, 'Exitcode of the process (exitcode).', generated=True,)
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'job_handle',
            str,
            'Identifies the job handle (job_handle) [internal].',
            generated=True,
            hide_when_anonymous=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'remote',
            str,
            'Where this job is/was run (remote) [internal].',
            generated=True,
            hide_when_anonymous=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'remote_history',
            list,
            'All workers where this job has been run (remote_history) [internal]. Multiple values indicate that the bundle was preempted and moved to a different worker.',
            generated=True,
            hide_when_anonymous=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'on_preemptible_worker',
            bool,
            'Whether the bundle is currently running / finished on a preemptible worker (on_preemptible_worker).',
            generated=True,
            hide_when_anonymous=True,
            default=False,
        )
    )

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
