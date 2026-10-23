"""Tests for the generated code and the descriptor set.

    python schema/generate.py
    python -m pytest schema/tests
"""

import sys
from pathlib import Path

from google.protobuf import descriptor_pb2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from telemetry_schema import message_class  # noqa: E402
from telemetry_schema.registry import DESCRIPTOR_SET  # noqa: E402

VehicleTelemetry = message_class("vehicle.v2.VehicleTelemetry")


def sample() -> VehicleTelemetry:
    message = VehicleTelemetry(
        message_id="m-1",
        vehicle_id="vehicle-001",
        Vehicle={
            "VehicleIdentification": {"BodyType": "truck"},
            "Speed": 42.5,
            "CurrentLocation": {"Latitude": 59.33, "Longitude": 18.07, "Heading": 90},
        },
        events=["HARSH_BRAKING", "SPEEDING"],
    )
    message.timestamp.FromJsonString("2026-09-29T08:00:00.123Z")
    return message


def test_record_batch_round_trips_message_bytes():
    KafkaRecordBatch = message_class("kafka.v1.KafkaRecordBatch")
    batch = KafkaRecordBatch()
    batch.records.add(offset=7, message_type="vehicle.v2.VehicleTelemetry", value=sample().SerializeToString())
    decoded = KafkaRecordBatch.FromString(batch.SerializeToString())
    assert VehicleTelemetry.FromString(decoded.records[0].value) == sample()


def test_descriptor_set_contains_all_messages_and_imports():
    descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(DESCRIPTOR_SET.read_bytes())
    files = {f.name for f in descriptor_set.file}
    assert {"vehicle/v2/vehicle_telemetry.proto", "vss/v6/vss.proto", "kafka/v1/kafka_record.proto"} <= files
    # Imports are included, so Spark can resolve google.protobuf.Timestamp.
    assert "google/protobuf/timestamp.proto" in files
