import unittest
from types import SimpleNamespace
from typing import List

from codalab.worker_manager.slurm_batch_worker_manager import SlurmBatchWorkerManager
from codalab.worker_manager.worker_manager import BundlesPayload


class SlurmBatchWorkerManagerTest(unittest.TestCase):
    def test_base_command(self):
        args: SimpleNamespace = SimpleNamespace(
            server='some_server',
            temp_session=True,
            user='some_user',
            partition='some_partition',
            worker_executable='cl-worker',
            worker_idle_seconds='888',
            worker_download_dependencies_max_retries=5,
            worker_tag='some_tag',
            worker_group='some_group',
            worker_exit_after_num_runs=8,
            worker_max_work_dir_size='88g',
            worker_work_dir_prefix='/some/path',
            worker_delete_work_dir_on_exit=False,
            worker_exit_on_exception=False,
            worker_tag_exclusive=False,
            worker_pass_down_termination=False,
            worker_checkin_frequency_seconds=30,
            password_file=None,
            slurm_work_dir=None,
            exit_after_num_failed=None,
            worker_shared_memory_size_gb=10,
            worker_use_shared_cache=True,
        )

        worker_manager: SlurmBatchWorkerManager = SlurmBatchWorkerManager(args)
        command: List[str] = worker_manager.setup_codalab_worker('some_worker_id')

        # --pass-down-termination should always be set for Slurm worker managers
        self.assertTrue('--pass-down-termination' in command)

        expected_command_str = (
            "cl-worker --server some_server --verbose --exit-when-idle --idle-seconds 888 "
            "--work-dir /some/path/some_user-codalab-SlurmBatchWorkerManager-scratch/"
            "some_user-codalab-slurm-worker-shared "
            "--id $(hostname -s)-some_worker_id --network-prefix cl_worker_some_worker_id_network --tag some_tag "
            "--group some_group --exit-after-num-runs 8 --download-dependencies-max-retries 5 "
            "--max-work-dir-size 88g --checkin-frequency-seconds 30 --shared-memory-size-gb 10 "
            "--use-shared-cache --pass-down-termination"
        )
        self.assertEqual(' '.join(command), expected_command_str)

    def test_filter_bundles(self):
        args: SimpleNamespace = SimpleNamespace(
            server='some_server',
            temp_session=True,
            user='some_user',
            partition='some_partition',
            worker_executable='cl-worker',
            worker_idle_seconds='888',
            worker_download_dependencies_max_retries=5,
            worker_tag='some_tag',
            worker_group='some_group',
            worker_exit_after_num_runs=8,
            worker_max_work_dir_size='88g',
            worker_work_dir_prefix='/some/path',
            worker_delete_work_dir_on_exit=False,
            worker_exit_on_exception=False,
            worker_tag_exclusive=False,
            worker_pass_down_termination=False,
            password_file=None,
            exit_after_num_failed=None,
            memory_mb=1024,
            cpus=3,
            gpus=1,
            worker_shared_memory_size_gb=None,
            worker_use_shared_cache=True,
        )

        worker_manager: SlurmBatchWorkerManager = SlurmBatchWorkerManager(args)
        filtered_bundles: BundlesPayload = worker_manager.filter_bundles(
            [
                {
                    'uuid': 0x01,
                    'metadata': {'request_cpus': 5, 'request_gpus': 0, 'request_memory': '1m'},
                },
                {
                    'uuid': 0x02,
                    'metadata': {'request_cpus': 3, 'request_gpus': 1, 'request_memory': '1g'},
                },
                {
                    'uuid': 0x03,
                    'metadata': {'request_cpus': 1, 'request_gpus': 0, 'request_memory': '2g'},
                },
            ]
        )

        self.assertEqual(len(filtered_bundles), 1)
        self.assertEqual(
            filtered_bundles[0],
            {
                'uuid': 0x02,
                'metadata': {'request_cpus': 3, 'request_gpus': 1, 'request_memory': '1g'},
            },
        )
