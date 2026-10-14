"""Tests for the schema-driven converters.

    python schema/generate.py
    python -m pytest schema/tests
"""

import sys
from pathlib import Path

from google.protobuf import descriptor_pb2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from telemetry_schema import elk, message_class  # noqa: E402
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


def test_elk_document_uses_field_names_enum_names_and_defaults():
    document = elk.to_document(sample())
    assert document["Vehicle"]["VehicleIdentification"]["BodyType"] == "truck"
    assert document["Vehicle"]["Speed"] == 42.5
    assert document["events"] == ["HARSH_BRAKING", "SPEEDING"]
    assert document["timestamp"] == "2026-09-29T08:00:00.123Z"
    # Fields with default values are included, so all documents look alike.
    assert document["Vehicle"]["TraveledDistance"] == 0
    # Unset nested messages (VSS branches) are left out.
    assert "Powertrain" not in document["Vehicle"]


def test_elk_document_adds_geo_point_from_option():
    location = elk.to_document(sample())["Vehicle"]["CurrentLocation"]
    assert location["GeoPoint"] == {"lat": 59.33, "lon": 18.07}
    # The VSS signals are still there.
    assert location["Heading"] == 90


def test_elk_mapping_follows_types_and_custom_options():
    properties = elk.index_properties(VehicleTelemetry.DESCRIPTOR)
    vehicle = properties["Vehicle"]["properties"]
    location = vehicle["CurrentLocation"]["properties"]
    assert location["GeoPoint"] == {"type": "geo_point"}  # (elk.geo_point) option
    assert location["Latitude"] == {"type": "double"}
    assert properties["timestamp"] == {"type": "date"}  # google.protobuf.Timestamp
    assert properties["events"] == {"type": "keyword"}  # enum
    assert vehicle["Speed"] == {"type": "float"}
    assert vehicle["TraveledDistance"] == {"type": "long"}  # uint32
    tire = vehicle["Chassis"]["properties"]["Axle"]["properties"]["Row2"]["properties"]["Wheel"]["properties"]["Left"]["properties"]["Tire"]
    assert tire["properties"]["Pressure"] == {"type": "long"}
    # Every field in the message is mapped.
    assert set(properties) == {f.name for f in VehicleTelemetry.DESCRIPTOR.fields}


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
