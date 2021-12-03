from codalab.common import StorageType, StorageFormat
from tests.unit.server.bundle_manager import BaseBundleManagerTest


class BundleStoreTest(BaseBundleManagerTest):
    def test_add_bundle_location(self):
        """Create a bundle location for a bundle store."""
        # Create bundle
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)

        # Create first bundle store
        bundle_store_uuid = self.bundle_manager._model.create_bundle_store(
            user=self.user_id,
            name="store1",
            storage_type=StorageType.DISK_STORAGE.value,
            storage_format=StorageFormat.UNCOMPRESSED.value,
            url="http://url",
            authentication="authentication",
        )

        # Calls add_bundle_location to add first bundle location to the bundle
        bundle_location_uuid = self.bundle_manager._model.add_bundle_location(
            bundle.uuid, bundle_store_uuid
        )

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
            user=self.user_id,
            name="store2",
            storage_type=StorageType.DISK_STORAGE.value,
            storage_format=StorageFormat.UNCOMPRESSED.value,
            url="http://url2",
            authentication="authentication2",
        )

        # Add second bundle store to the bundle
        bundle_location_uuid_2 = self.bundle_manager._model.add_bundle_location(
            bundle.uuid, bundle_store_uuid_2
        )

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
