-- One row per telemetry reading, with the VSS signals flattened into columns.
--
-- telemetry_source() decodes the binary protobuf files written by the
-- protobuf consumer (see dbt/src/protobuf_macros/telemetry_source.sql).
--
-- The readings arrive in the `Vehicle` struct, a tree that follows the COVESA
-- Vehicle Signal Specification (VSS): Vehicle.Speed,
-- Vehicle.Powertrain.FuelSystem.RelativeLevel, and so on. This model maps
-- each signal we use to a column, with the unit in its name, and converts
-- the units we want differently (the odometer is in meters in VSS).
--
-- The consumers deliver messages at-least-once, so the same message can end
-- up in the files more than once. We keep one row per message_id.
--
-- Protobuf enums arrive as their names (HARSH_BRAKING); they are lower-cased
-- here. New signals added to the .proto files are available in
-- telemetry_source() automatically; add them below to use them in the models.

with source as (

    select * from {{ telemetry_source() }}

),

deduplicated as (

    select
        message_id,
        vehicle_id,
        {{ adapter.quote('timestamp') }} as recorded_at,
        to_date({{ adapter.quote('timestamp') }}) as event_date,

        Vehicle.VehicleIdentification.VIN as vin,
        Vehicle.VehicleIdentification.BodyType as vehicle_type,

        Vehicle.CurrentLocation.Latitude as latitude,
        Vehicle.CurrentLocation.Longitude as longitude,
        Vehicle.CurrentLocation.Heading as heading_deg,
        Vehicle.Speed as speed_kmh,
        Vehicle.Acceleration.Longitudinal as acceleration_mps2,
        Vehicle.Powertrain.CombustionEngine.Speed as rpm,
        Vehicle.Powertrain.Transmission.CurrentGear as gear,
        Vehicle.Powertrain.CombustionEngine.EngineCoolant.Temperature as engine_temp_c,
        Vehicle.Powertrain.FuelSystem.RelativeLevel as fuel_level_pct,
        Vehicle.LowVoltageBattery.CurrentVoltage as battery_voltage,
        Vehicle.TraveledDistance / 1000 as odometer_km,

        -- VSS numbers the axles from the front: Row1 is the front axle.
        Vehicle.Chassis.Axle.Row1.Wheel.Left.Tire.Pressure as tire_front_left_kpa,
        Vehicle.Chassis.Axle.Row1.Wheel.Right.Tire.Pressure as tire_front_right_kpa,
        Vehicle.Chassis.Axle.Row2.Wheel.Left.Tire.Pressure as tire_rear_left_kpa,
        Vehicle.Chassis.Axle.Row2.Wheel.Right.Tire.Pressure as tire_rear_right_kpa,

        -- An active trouble code lights the check engine light.
        Vehicle.Diagnostics.DTCCount > 0 as check_engine_light,
        Vehicle.Diagnostics.DTCList as dtc_codes,
        transform(events, e -> lower(e)) as events,

        kafka_partition,
        kafka_offset,
        ingested_at

    from source

    qualify row_number() over (partition by message_id order by ingested_at) = 1

),

with_previous as (

    select
        *,
        lag(odometer_km) over (partition by vehicle_id order by recorded_at) as previous_odometer_km

    from deduplicated

)

select
    *,
    -- Distance driven since the vehicle's previous reading. The odometer jumps
    -- when the producer restarts, so implausible jumps count as 0.
    case
        when odometer_km - previous_odometer_km between 0 and 1
            then odometer_km - previous_odometer_km
        else 0
    end as distance_km

from with_previous
