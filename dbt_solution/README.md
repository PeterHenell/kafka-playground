# vehicle_telemetry dbt project: the solution

The finished version of the dbt lab: what [../dbt](../dbt) looks like after
all the steps in [LEARN_DBT.md](../dbt/LEARN_DBT.md). Use it to check your
work, or copy a file from it to catch up.

Build it from the repository root:

```bash
make solution                                   # dbt build
make solution ARGS="show --select fct_vehicle_daily"
```

Its tables go to the schema `vehicle_telemetry_solution`, so they don't mix
with the ones you build in the lab (`vehicle_telemetry`). The project uses
the lab's profile, generated sources and protobuf macros (`../dbt/src`), so
those exist only once; see [dbt/README.md](../dbt/README.md) for them.

## Models

```
source raw.vehicle_telemetry_records ──> stg_vehicle_telemetry ──┬──> dim_vehicles           one row per vehicle ──> dim_vehicles_snapshot (its history)
(view over the protobuf files;           (view: decoded,        ├──> fct_vehicle_daily      distance and health per vehicle and day
 generated schema tests)                  flattened, de-dup.)   ├──> fct_vehicle_events     one row per event (harsh_braking, …), incremental
                                                                 └──> vehicle_latest_status  latest reading per vehicle
```

- `stg_vehicle_telemetry` removes duplicates. Both consumers deliver
  *at-least-once*, so a message can show up in more than one file. It also
  computes the distance driven since each vehicle's previous reading, and
  lower-cases the protobuf enum names (`HARSH_BRAKING` → `harsh_braking`).
  It maps the VSS signals in the nested `Vehicle` struct to flat columns
  (`Vehicle.Speed` → `speed_kmh`, `Vehicle.TraveledDistance` in meters →
  `odometer_km`), so the marts don't depend on the VSS paths. It lists the
  columns it uses: add new `.proto` fields here to use them.
- The marts are Delta tables (`+file_format: delta` in `dbt_project.yml`), as
  on Databricks.
- `fct_vehicle_events` is *incremental*: each run only processes the readings
  archived since the last run (by `ingested_at`, with a few minutes of
  lookback), and merges on `event_id`, so an event seen twice doesn't add a
  second row. It doesn't use Kafka offsets as the watermark: they start at 0
  again when the topic is recreated. `dbt build --full-refresh` rebuilds it.
- `dim_vehicles_snapshot` (`src/snapshots/`) is a *snapshot* of
  `dim_vehicles`: every run adds a new version of the vehicles that sent new
  readings, with `dbt_valid_from`/`dbt_valid_to`. It holds the only copy of
  that history, so `make clean` loses it.
- `min_tire_pressure()` (`src/macros/`) is used by two marts, and the
  `low_fuel_pct` variable by `vehicle_latest_status`.
- Tests: the built-in generic tests in the `_*.yml` files next to the models,
  a custom generic test (`unique_combination_of_columns`, in `src/macros/`)
  and a singular test (`src/tests/`).
