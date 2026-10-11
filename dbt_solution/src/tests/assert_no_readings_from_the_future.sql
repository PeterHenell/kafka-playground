-- A singular test: a vehicle can't record a reading in the future. A row
-- here points at a vehicle (or producer) with a wrong clock. No rows means
-- the test passes.

select
    message_id,
    vehicle_id,
    recorded_at

from {{ ref('stg_vehicle_telemetry') }}

where recorded_at > current_timestamp()
