-- One row per event (harsh_braking, engine_overheating, ...), with where and
-- when it happened. A reading can carry several events.
--
-- Incremental: events never change once they happened, so each run only
-- processes the readings archived since the last run (by ingested_at, when
-- the protobuf consumer received them from Kafka).
--
-- Why not Kafka offsets? They only grow within a partition, until the topic
-- is recreated: then they start at 0 again, and new records would look older
-- than the ones already loaded.
--
-- The run looks back a few minutes, and the merge on unique_key makes that
-- safe: an event that is processed again updates its row instead of adding
-- a second one. Rebuild the whole table with `dbt build --full-refresh`.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}

with readings as (

    select *

    from {{ ref('stg_vehicle_telemetry') }}

    {% if is_incremental() %}
    -- {{ this }} is this model's own table, as it was after the last run. The
    -- consumer writes a file every minute, so a reading can reach the files a
    -- minute after it was ingested: look back further than that.
    where ingested_at > (select max(ingested_at) - interval 5 minutes from {{ this }})
    {% endif %}

),

exploded as (

    select
        message_id,
        vehicle_id,
        recorded_at,
        event_date,
        latitude,
        longitude,
        speed_kmh,
        engine_temp_c,
        kafka_partition,
        kafka_offset,
        ingested_at,
        explode(events) as event_type

    from readings

)

select
    {{ dbt.concat(['message_id', "'-'", 'event_type']) }} as event_id,
    *

from exploded
