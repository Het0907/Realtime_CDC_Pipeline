import unittest

import pandas as pd

import silver_processor


class SilverContractTests(unittest.TestCase):
    def test_update_event_preserves_after_row_and_metadata(self):
        event = {
            "operation": "u",
            "timestamp_ms": 123,
            "ingested_at": "2026-10-25T00:00:00+00:00",
            "after": {"id": 7, "email": "updated@example.com"},
            "before": {"id": 7, "email": "old@example.com"},
        }

        row = silver_processor.build_silver_row(event)

        self.assertEqual(row["email"], "updated@example.com")
        self.assertEqual(row["_cdc_op"], "u")
        self.assertFalse(row["_is_deleted"])
        self.assertEqual(row["_event_ts"], 123)

    def test_delete_event_uses_before_row_and_marks_deleted(self):
        event = {
            "operation": "d",
            "timestamp_ms": 456,
            "before": {"id": 8, "email": "deleted@example.com"},
            "after": None,
        }

        row = silver_processor.build_silver_row(event)

        self.assertEqual(row["id"], 8)
        self.assertEqual(row["_cdc_op"], "d")
        self.assertTrue(row["_is_deleted"])

    def test_latest_event_wins_for_each_primary_key(self):
        events = [
            {"operation": "c", "timestamp_ms": 100, "after": {"id": 9, "email": "old@example.com"}},
            {"operation": "u", "timestamp_ms": 200, "after": {"id": 9, "email": "new@example.com"}},
        ]

        frame = silver_processor.events_to_dataframe(events, "customers")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["email"], "new@example.com")

    def test_spark_bronze_reader_has_expected_contract(self):
        if not silver_processor.SPARK_BRONZE_DIR.exists():
            self.skipTest("Spark Bronze table is not present")

        events = silver_processor.load_spark_bronze_events("customers")

        self.assertTrue(events)
        self.assertTrue({"operation", "timestamp_ms", "before", "after"}.issubset(events[0]))


if __name__ == "__main__":
    unittest.main()
