import unittest
from types import SimpleNamespace

from codalab.worker_manager.slurm_batch_worker_manager import SlurmBatchWorkerManager


class SlurmBatchWorkerManagerTest(unittest.TestCase):
    def test_base_command(self):
        args = SimpleNamespace(
            server="some_server",
            user="some_user",
            partition="some_partition",
            worker_executable="cl-worker",
            worker_idle_seconds="888",
            worker_tag="some_tag",
            worker_group="some_group",
            worker_exit_after_num_runs=8,
            worker_max_work_dir_size="88g",
            worker_work_dir_prefix="/some/path",
            worker_delete_work_dir_on_exit=False,
            worker_exit_on_exception=False,
            worker_tag_exclusive=False,
            worker_pass_down_termination=False,
            password_file=None,
        )

        worker_manager = SlurmBatchWorkerManager(args)
        command = worker_manager.setup_codalab_worker("some_worker_id")

        # --pass-down-termination should always be set for Slurm worker managers
        self.assertTrue('--pass-down-termination' in command)

        expected_command_str = (
            "cl-worker --server some_server --verbose --exit-when-idle --idle-seconds 888 "
            "--work-dir /some/path/some_user-codalab-SlurmBatchWorkerManager-scratch/some_worker_id "
            "--id some_worker_id --network-prefix cl_worker_some_worker_id_network --tag some_tag "
            "--group some_group --exit-after-num-runs 8 --max-work-dir-size 88g --pass-down-termination"
        )
        self.assertEqual(' '.join(command), expected_command_str)
