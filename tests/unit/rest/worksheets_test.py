import uuid
from .base import BaseTestCase
import datetime


class WorksheetsTest(BaseTestCase):
    def test_create(self):
        worksheet_name = f"codalab-{uuid.uuid4()}"
        response = self.app.post_json(
            '/rest/worksheets',
            {'data': [{'type': 'worksheets', 'attributes': {'name': worksheet_name}}]},
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
        # Verify the date_created and date_last_modified field has been set to the current time for a new created worksheet
        worksheet_info = self.app.get('/rest/interpret/worksheet/' + worksheet_id).json
        date_created = datetime.datetime.strptime(worksheet_info['date_created'], '%Y-%m-%dT%H:%M:%S')
        date_last_modified = datetime.datetime.strptime(worksheet_info['date_last_modified'], '%Y-%m-%dT%H:%M:%S')
        current_time = datetime.datetime.utcnow()
        self.assertAlmostEqual(current_time, date_created, delta=datetime.timedelta(seconds=1))
        self.assertAlmostEqual(current_time, date_last_modified, delta=datetime.timedelta(seconds=1))
