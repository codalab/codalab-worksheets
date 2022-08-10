import uuid
from .base import BaseTestCase
import datetime
from freezegun import freeze_time


TEST_BUNDLE_BODY = {
    'data': [
        {
            'type': 'bundles',
            'attributes': {
                'bundle_type': 'run',
                'command': 'echo TEST',
                'metadata': {
                    'name': 'run-echo',
                    'description': '',
                    'tags': [''],
                    'allow_failed_dependencies': False,
                    'request_docker_image': 'codalab/default-cpu:latest',
                    'request_time': '',
                    'request_memory': '4g',
                    'request_disk': '',
                    'request_cpus': 1,
                    'request_gpus': 0,
                    'request_queue': '',
                    'request_priority': 0,
                    'request_network': False,
                    'exclude_patterns': [],
                    'store': '',
                },
                'dependencies': [],
            },
        },
        {
            'type': 'bundles',
            'attributes': {
                'bundle_type': 'run',
                'command': 'echo TEST',
                'metadata': {
                    'name': 'run-echo',
                    'description': '',
                    'tags': [''],
                    'allow_failed_dependencies': False,
                    'request_docker_image': 'codalab/default-cpu:latest',
                    'request_time': '',
                    'request_memory': '4g',
                    'request_disk': '',
                    'request_cpus': 1,
                    'request_gpus': 0,
                    'request_queue': '',
                    'request_priority': 0,
                    'request_network': False,
                    'exclude_patterns': [],
                    'store': '',
                },
                'dependencies': [],
            },
        },
        {
            'type': 'bundles',
            'attributes': {
                'bundle_type': 'run',
                'command': 'echo TEST',
                'metadata': {
                    'name': 'run-echo',
                    'description': '',
                    'tags': [''],
                    'allow_failed_dependencies': False,
                    'request_docker_image': 'codalab/default-cpu:latest',
                    'request_time': '',
                    'request_memory': '4g',
                    'request_disk': '',
                    'request_cpus': 1,
                    'request_gpus': 0,
                    'request_queue': '',
                    'request_priority': 0,
                    'request_network': False,
                    'exclude_patterns': [],
                    'store': '',
                },
                'dependencies': [],
            },
        },
    ]
}


class WorksheetsTest(BaseTestCase):
    @freeze_time("2012-01-14", as_kwarg="frozen_time")
    def test_create(self, frozen_time):
        """
        Create a new worksheet, then ensure that the proper fields are returned.
        """
        worksheet_name = f"codalab-{uuid.uuid4()}"
        response = self.app.post_json(
            "/rest/worksheets",
            {"data": [{"type": "worksheets", "attributes": {"name": worksheet_name}}]},
        )
        worksheet_id = response.json["data"][0]["id"]
        data = response.json["data"]
        self.assertEqual(
            data,
            [
                {
                    "type": "worksheets",
                    "id": worksheet_id,
                    "attributes": {"name": worksheet_name, "uuid": worksheet_id},
                    "relationships": {
                        "items": {
                            "data": [
                                {"type": "worksheet-items", "id": f"('name', '{worksheet_name}')"},
                                {"type": "worksheet-items", "id": f"('uuid', '{worksheet_id}')"},
                            ]
                        }
                    },
                }
            ],
        )

        # Verify that the date_created and date_last_modified fields are set to the current time for a newly created worksheet.
        worksheet_info = self.app.get("/rest/interpret/worksheet/" + worksheet_id).json
        current_time = datetime.datetime.isoformat(frozen_time.time_to_freeze)
        self.assertEqual(current_time, worksheet_info["date_created"])
        self.assertEqual(current_time, worksheet_info["date_last_modified"])

    def test_refresh(self):
        """
        Upload 3 bundles to a worksheet, then specify a bundle_uuid to refresh, only the queried bundle info should be returned
        """
        worksheet_id = self.create_worksheet()
        response = self.app.post_json(f'/rest/bundles?worksheet={worksheet_id}', TEST_BUNDLE_BODY)
        self.assertEqual(response.status_int, 200)
        data = response.json["data"]
        bundle_id = data[0]["id"]

        # Verify that only the queried bundle info should be returned, so there will only be 1 non-null element in the block_infos
        refreshed_bundles = self.app.get(
            "/rest/interpret/worksheet/" + worksheet_id + "?bundle_uuid=" + bundle_id
        ).json
        blocks_info = refreshed_bundles["blocks"][0]["bundles_spec"]["bundle_infos"]
        bundles_count = sum(1 for bundle in blocks_info if bundle)
        self.assertEqual(1, bundles_count)

    def test_sort_key_normalization(self):
        """
        Confirm that a worksheet contains valid sort keys when worksheet info
        is fetched via get_worksheet_info.
        """
        worksheet_id = self.create_worksheet()

        # Add items with null sort_key to a worksheet.
        self.app.post_json(f'/rest/bundles?worksheet={worksheet_id}', TEST_BUNDLE_BODY)

        # Get worksheet info, which should normalize sort keys.
        worksheet_info = self.app.get('/rest/interpret/worksheet/' + worksheet_id).json
        sort_keys = worksheet_info['blocks'][0]['sort_keys']
        last_sort_key = None

        for sort_key in sort_keys:

            # Assert that there are no null sort keys.
            self.assertIsNotNone(sort_key)

            # Assert that sort keys monotonically increase.
            if last_sort_key is not None:
                self.assertEqual(sort_key, last_sort_key + 1)
            last_sort_key = sort_key
