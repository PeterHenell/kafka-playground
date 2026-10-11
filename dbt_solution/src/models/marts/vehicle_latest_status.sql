-- The most recent reading of every vehicle: where is it and is it healthy?

select
    vehicle_id,
    vehicle_type,
    recorded_at,
    latitude,
    longitude,
    speed_kmh,
    engine_temp_c,
    fuel_level_pct,
    fuel_level_pct < {{ var('low_fuel_pct') }} as is_low_on_fuel,
    battery_voltage,
    {{ min_tire_pressure() }} as min_tire_pressure_kpa,
    check_engine_light,
    dtc_codes

from {{ ref('stg_vehicle_telemetry') }}

qualify row_number() over (partition by vehicle_id order by recorded_at desc) = 1
