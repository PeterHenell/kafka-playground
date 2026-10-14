"""Tests for the dbt source test generator.

    python schema/generate.py
    python -m pytest schema/tests
"""

import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from telemetry_schema import dbt_tests, message_class  # noqa: E402

VehicleTelemetry = message_class("vehicle.v2.VehicleTelemetry")
KafkaRecord = message_class("kafka.v1.KafkaRecord")
SOURCES_FILE = Path(__file__).resolve().parents[2] / "dbt" / "src" / "sources" / "raw_vehicle_telemetry.yml"


def generated() -> str:
    return dbt_tests.source_yaml(VehicleTelemetry.DESCRIPTOR, KafkaRecord.DESCRIPTOR, "vehicle_telemetry", "vehicle/v2/vehicle_telemetry.proto")


def value_tests() -> dict:
    source = yaml.safe_load(generated())["sources"][0]
    records = next(t for t in source["tables"] if t["name"] == "vehicle_telemetry_records")
    value = next(c for c in records["columns"] if c["name"] == "value")
    return {next(iter(t.values()))["name"]: next(iter(t.values())) for t in value["data_tests"]}


def test_every_message_is_checked_to_decode():
    assert value_tests()["protobuf_decodes__vehicle_telemetry"]["arguments"]["message_type"] == "vehicle.v2.VehicleTelemetry"


def test_protovalidate_rules_become_sql_conditions():
    tests = value_tests()
    speed = tests["protobuf_valid__vehicle_telemetry__Vehicle_Speed"]["arguments"]["condition"]
    assert "coalesce(m.`Vehicle`.`Speed`, 0) >= 0.0" in speed
    assert "coalesce(m.`Vehicle`.`Speed`, 0) <= 250.0" in speed
    # repeated items
    dtc = tests["protobuf_valid__vehicle_telemetry__Vehicle_Diagnostics_DTCList"]["arguments"]["condition"]
    assert "forall(m.`Vehicle`.`Diagnostics`.`DTCList`, x2 -> x2 rlike '^[PCBU][0-9]{4}$')" in dtc
    # string.in
    body_type = tests["protobuf_valid__vehicle_telemetry__Vehicle_VehicleIdentification_BodyType"]["arguments"]["condition"]
    assert "in ('car', 'van', 'truck', 'bus')" in body_type


def test_nested_messages_get_a_test_per_field():
    tests = value_tests()
    # required on a nested message
    location = tests["protobuf_valid__vehicle_telemetry__Vehicle_CurrentLocation"]
    assert location["arguments"]["field"] == "Vehicle.CurrentLocation"
    assert "m.`Vehicle`.`CurrentLocation` is not null" in location["arguments"]["condition"]
    # Rules of a nested field only apply when its parent message is set.
    latitude = tests["protobuf_valid__vehicle_telemetry__Vehicle_CurrentLocation_Latitude"]["arguments"]["condition"]
    assert latitude.startswith("((m.`Vehicle`.`CurrentLocation` is null or coalesce(m.`Vehicle`.`CurrentLocation`.`Latitude`, 0) >= -90.0))")


def test_enum_values_must_be_defined():
    condition = value_tests()["protobuf_valid__vehicle_telemetry__events"]["arguments"]["condition"]
    assert "'UNKNOWN_ENUM_VALUE_'" in condition


def test_fields_without_rules_get_no_test():
    tests = value_tests()
    assert "protobuf_valid__vehicle_telemetry__Vehicle_Powertrain_FuelSystem_IsFuelLevelLow" not in tests
    # A branch without a `required` rule has no test of its own.
    assert "protobuf_valid__vehicle_telemetry__Vehicle_Powertrain" not in tests


def test_sql_string_escapes_backslashes_and_quotes():
    assert dbt_tests.sql_string("a\\d'b") == "'a\\\\d\\'b'"


def test_committed_dbt_sources_are_up_to_date():
    """The generated file is committed; regenerate it after changing a .proto file."""
    if os.getenv("ARCHIVE_NAME", "vehicle_telemetry") != "vehicle_telemetry" or not SOURCES_FILE.exists():
        return
    assert SOURCES_FILE.read_text() == generated(), "Run: python schema/generate.py"
