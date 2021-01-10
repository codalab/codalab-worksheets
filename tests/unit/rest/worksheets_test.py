import uuid
from .base import BaseTestCase
import datetime
from freezegun import freeze_time


class WorksheetsTest(BaseTestCase):
    @freeze_time("2012-01-14", as_kwarg="frozen_time")
    def test_create(self, frozen_time):
        """Create a new worksheet, then ensure that the proper fields are returned.
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
                                {"type": "worksheet-items", "id": f"('name', '{worksheet_name}')",},
                                {"type": "worksheet-items", "id": f"('uuid', '{worksheet_id}')",},
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
