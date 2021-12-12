from codalab.common import StorageType, StorageFormat, UsageError
from tests.unit.server.bundle_manager import BaseBundleManagerTest


class BundleStoreTest(BaseBundleManagerTest):
    def test_bundle_store_workflow(self):
        """
        Tests the workflow for creating bundles
        """
        # first, make sure there are already no bundles in the bundle store
        bundle_stores = self.bundle_manager._model.get_bundle_stores(self.user_id)
        self.assertEqual(len(bundle_stores), 0)
        # add a bundle store
        bundle_store_uuid = self.bundle_manager._model.create_bundle_store(
            user_id=self.user_id,
            name="store1",
            storage_type=StorageType.DISK_STORAGE.value,
            storage_format=StorageFormat.UNCOMPRESSED.value,
            url="http://url",
            authentication="authentication",
        )
        # it should now exist
        bundle_stores = self.bundle_manager._model.get_bundle_stores(self.user_id)
        self.assertEqual(len(bundle_stores), 1)
        self.assertEqual(bundle_stores[0].get("uuid"), bundle_store_uuid)
        self.assertEqual(bundle_stores[0].get("name"), "store1")
        # update one of the bundle store fields
        self.bundle_manager._model.update_bundle_store(
            self.user_id, bundle_store_uuid, {"name": "im-not-a-store"}
        )
        # check if the field has been updated
        bundle_stores = self.bundle_manager._model.get_bundle_stores(self.user_id)
        self.assertEqual(len(bundle_stores), 1)
        self.assertEqual(bundle_stores[0].get("name"), "im-not-a-store")
        # Deletion should succeed since there are bundle locations associated with the bundle store
        self.bundle_manager._model.delete_bundle_store(self.user_id, bundle_store_uuid)
        bundle_stores = self.bundle_manager._model.get_bundle_stores(self.user_id)
        self.assertEqual(len(bundle_stores), 0)

    def test_add_bundle_location(self):
        """
        Creates a new bundle and multiple associated bundle stores and bundle locations to test the get_bundle_locations, get_bundle_location, and add_bundle_location functions
        """
        # Create bundle
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)

        # Create first bundle store
        bundle_store_uuid = self.bundle_manager._model.create_bundle_store(
            user_id=self.user_id,
            name="store1",
            storage_type=StorageType.DISK_STORAGE.value,
            storage_format=StorageFormat.UNCOMPRESSED.value,
            url="http://url",
            authentication="authentication",
        )

        # Calls add_bundle_location to add first bundle location to the bundle
        self.bundle_manager._model.add_bundle_location(bundle.uuid, bundle_store_uuid)

        # Call get_bundle_locations
        bundle_locations = self.bundle_manager._model.get_bundle_locations(bundle.uuid)
        self.assertEqual(
            bundle_locations,
            [
                {
                    'name': 'store1',
                    'storage_type': 'disk',
                    'storage_format': 'uncompressed',
                    'url': 'http://url',
                }
            ],
        )

        # Call get_bundle_location
        bundle_location = self.bundle_manager._model.get_bundle_location(
            bundle.uuid, bundle_store_uuid
        )
        self.assertEqual(
            bundle_location,
            {
                'name': 'store1',
                'storage_type': 'disk',
                'storage_format': 'uncompressed',
                'url': 'http://url',
            },
        )

        # Create second bundle store
        bundle_store_uuid_2 = self.bundle_manager._model.create_bundle_store(
            user_id=self.user_id,
            name="store2",
            storage_type=StorageType.DISK_STORAGE.value,
            storage_format=StorageFormat.UNCOMPRESSED.value,
            url="http://url2",
            authentication="authentication2",
        )

        # Add second bundle store to the bundle
        self.bundle_manager._model.add_bundle_location(bundle.uuid, bundle_store_uuid_2)

        # Call get_bundle_locations
        bundle_locations_2 = self.bundle_manager._model.get_bundle_locations(bundle.uuid)
        self.assertEqual(
            bundle_locations_2,
            [
                {
                    'name': 'store1',
                    'storage_type': 'disk',
                    'storage_format': 'uncompressed',
                    'url': 'http://url',
                },
                {
                    'name': 'store2',
                    'storage_type': 'disk',
                    'storage_format': 'uncompressed',
                    'url': 'http://url2',
                },
            ],
        )

        # Call get_bundle_location
        bundle_location_2 = self.bundle_manager._model.get_bundle_location(
            bundle.uuid, bundle_store_uuid_2
        )
        self.assertEqual(
            bundle_location_2,
            {
                'name': 'store2',
                'storage_type': 'disk',
                'storage_format': 'uncompressed',
                'url': 'http://url2',
            },
        )

        # Deletion of bundle store should fail because there are still BundleLocations associated with the BundleStore.
        with self.assertRaises(UsageError):
            self.bundle_manager._model.delete_bundle_store(self.user_id, bundle_store_uuid_2)
