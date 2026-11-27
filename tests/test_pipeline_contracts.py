import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd
from deltalake import DeltaTable, write_deltalake

import silver_processor
from data_quality.run_checks import validate_with_gx


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
        frame = pd.DataFrame([{
            "table": "customers", "topic": "dbserver1.inventory.customers",
            "operation": "u", "event_ts": 100, "partition": 0, "offset": 42,
            "ingested_at": "2026-10-25T00:00:00+00:00",
            "before_json": '{"id": 9}', "after_json": '{"id": 9, "first_name": "Updated"}',
        }])
        with TemporaryDirectory() as tmp, patch.object(silver_processor, "SPARK_BRONZE_DIR", Path(tmp)):
            write_deltalake(tmp, frame, partition_by=["table"])
            events = silver_processor.load_spark_bronze_events("customers")
            self.assertEqual(silver_processor.load_spark_bronze_events("orders"), [])
        self.assertEqual(events[0]["offset"], 42)
        self.assertEqual(events[0]["before"], {"id": 9})
        self.assertEqual(events[0]["after"]["first_name"], "Updated")

    def test_merge_preserves_delete_against_stale_event_and_replay(self):
        update = {"operation": "u", "timestamp_ms": 200, "partition": 0, "offset": 5,
                  "after": {"id": 9, "email": "customer@example.com"}}
        delete = {"operation": "d", "timestamp_ms": 200, "partition": 0, "offset": 6,
                  "before": {"id": 9}}
        both = silver_processor.events_to_dataframe([delete, update], "customers")
        self.assertTrue(both.iloc[0]["_is_deleted"])

        with TemporaryDirectory() as tmp, patch.object(silver_processor, "SILVER_DIR", Path(tmp)):
            for event in (update, delete, update, delete):
                frame = silver_processor.events_to_dataframe([event], "customers")
                silver_processor.write_to_silver(frame, "customers")
            table = DeltaTable(str(Path(tmp) / "customers"))
            row = table.to_pyarrow_table().to_pylist()[0]
            self.assertTrue(row["_is_deleted"])
            self.assertEqual(row["_kafka_offset"], 6)
            self.assertEqual(table.version(), 1)

    def test_merge_adds_column_to_existing_legacy_delta_table(self):
        with TemporaryDirectory() as tmp, patch.object(silver_processor, "SILVER_DIR", Path(tmp)):
            path = str(Path(tmp) / "customers")
            write_deltalake(path, pd.DataFrame([{"id": 9, "_event_ts": 100}]))
            event = {"operation": "u", "timestamp_ms": 200, "partition": 0, "offset": 5,
                     "after": {"id": 9, "loyalty_tier": "gold"}}
            frame = silver_processor.events_to_dataframe([event], "customers")
            silver_processor.write_to_silver(frame, "customers")
            row = DeltaTable(path).to_pyarrow_table().to_pylist()[0]
            self.assertEqual(row["loyalty_tier"], "gold")
            self.assertEqual(row["_kafka_offset"], 5)
            self.assertNotIn("loyalty_tier", DeltaTable(path, version=0).to_pyarrow_table().column_names)

    def test_quality_gate_ignores_deleted_business_values_and_checks_active_rows(self):
        frame = pd.DataFrame([
            {"id": 9, "email": "customer@example.com", "first_name": "Alice", "_is_deleted": False, "_cdc_op": "u"},
            {"id": 10, "email": None, "first_name": None, "_is_deleted": True, "_cdc_op": "d"},
        ])
        checks = validate_with_gx("customers", frame)
        self.assertTrue(all(passed for _, passed, _ in checks), checks)
        frame.loc[0, "email"] = "invalid"
        checks = validate_with_gx("customers", frame)
        self.assertIn("email contains @", [name for name, passed, _ in checks if not passed])


if __name__ == "__main__":
    unittest.main()
