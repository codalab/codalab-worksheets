from codalab.worker.bundle_state import Dependency, State
from codalab.objects.dependency import Dependency
from codalab.bundles.run_bundle import RunBundle
from codalab.lib.spec_util import generate_uuid
from tests.unit.server.bundle_manager import BASE_METADATA, BaseBundleManagerTest


class BundleManagerStageBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        self.bundle_manager._stage_bundles()

    def test_single_bundle(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id='id1',
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_with_dependency(self):
        parent = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.READY,
        )
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": parent.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]

        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_do_not_stage_with_failed_dependency(self):
        for state in (State.FAILED, State.KILLED):
            with self.subTest(state=state):
                parent = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=BASE_METADATA,
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=state,
                )
                bundle = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=BASE_METADATA,
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=State.CREATED,
                )
                bundle.dependencies = [
                    Dependency(
                        {
                            "parent_uuid": parent.uuid,
                            "parent_path": "",
                            "child_uuid": bundle.uuid,
                            "child_path": "src",
                        }
                    )
                ]

                self.bundle_manager._model.save_bundle(parent)
                self.bundle_manager._model.save_bundle(bundle)

                self.bundle_manager._stage_bundles()

                bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
                self.assertEqual(bundle.state, State.FAILED)
                self.assertIn(
                    "Please use the --allow-failed-dependencies flag",
                    bundle.metadata.failure_message,
                )

    def test_allow_failed_dependencies(self):
        for state in (State.FAILED, State.KILLED):
            with self.subTest(state=state):
                parent = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=BASE_METADATA,
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=state,
                )
                bundle = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=dict(BASE_METADATA, allow_failed_dependencies=True),
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=State.CREATED,
                )
                bundle.dependencies = [
                    Dependency(
                        {
                            "parent_uuid": parent.uuid,
                            "parent_path": "",
                            "child_uuid": bundle.uuid,
                            "child_path": "src",
                        }
                    )
                ]

                self.bundle_manager._model.save_bundle(parent)
                self.bundle_manager._model.save_bundle(bundle)

                self.bundle_manager._stage_bundles()

                bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
                self.assertEqual(bundle.state, State.STAGED)

    def test_missing_parent(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": generate_uuid(),
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]

        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("Missing parent bundles", bundle.metadata.failure_message)

    def test_no_permission_parents(self):
        parent = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=generate_uuid(),
            uuid=generate_uuid(),
            state=State.READY,
        )
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": parent.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]

        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("does not have sufficient permissions", bundle.metadata.failure_message)
