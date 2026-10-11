# dbt hands-on lab: vehicle telemetry

In this lab you build a dbt project from scratch, step by step: from raw
Kafka data to tested, documented analytics tables, an incremental model and a
snapshot. It takes about two hours. It's written for data engineers who know
Databricks but haven't used dbt.

The data: simulated vehicles send telemetry (position, speed, engine
temperature, tire pressure, events like `harsh_braking`) through Kafka. A
consumer archives the raw Kafka records as binary protobuf files. Your dbt
project turns those files into tables.

**How the lab works.** Each step introduces one dbt concept, then you use it.
Every step has the same parts:

- a short explanation of the concept,
- **Do**: what to create or change,
- **Check**: a command to run, and what you should see,
- **Solution**: the finished files, in [`../dbt_solution`](../dbt_solution).

You work in this folder (`dbt/`). All paths below are relative to it.

**Stuck?** Compare your file with the solution, e.g.
`diff dbt/src/models/marts/dim_vehicles.sql dbt_solution/src/models/marts/dim_vehicles.sql`
(from the repository root), or copy the solution file and move on. You can
also ask the host.

## Session plan

| Step | Topic                                                           | Minutes |
|------|-----------------------------------------------------------------|---------|
| 0    | [Check your setup](#step-0-check-your-setup)                    | 5       |
| 1    | [What dbt is](#step-1-what-dbt-is)                              | 5       |
| 2    | [The project and the profile](#step-2-the-project-and-the-profile) | 5    |
| 3    | [Sources](#step-3-sources)                                      | 5       |
| 4    | [Your first model](#step-4-your-first-model)                    | 15      |
| 5    | [`ref()` and the DAG](#step-5-ref-and-the-dag)                  | 10      |
| 6    | [Materializations](#step-6-materializations)                    | 5       |
| 7    | [More marts](#step-7-more-marts)                                | 10      |
| 8    | [Jinja and compiled SQL](#step-8-jinja-and-compiled-sql)        | 8       |
| 9    | [Macros](#step-9-macros)                                        | 7       |
| 10   | [Variables and hooks](#step-10-variables-and-hooks)             | 5       |
| 11   | [Tests](#step-11-tests)                                         | 13      |
| 12   | [Documentation](#step-12-documentation)                         | 4       |
| 13   | [Incremental models](#step-13-incremental-models)               | 10      |
| 14   | [Snapshots](#step-14-snapshots)                                 | 7       |
| 15   | [Wrap-up](#step-15-wrap-up)                                     | 6       |
|      | [Optional: run it on Databricks](#optional-run-it-on-databricks) | —      |

What you'll have built:

```
                                                                 ┌──> dim_vehicles ──> dim_vehicles_snapshot
source raw.vehicle_telemetry_records ──> stg_vehicle_telemetry ──┼──> fct_vehicle_daily
(provided)                               (step 4)               ├──> fct_vehicle_events (incremental)
                                                                 └──> vehicle_latest_status
```

## Before the session

Do this the day before: the first build downloads a few GB.

1. You need Docker with Docker Compose (or Podman with `podman compose`),
   `make`, and about 5 GB of free memory. See
   [Getting started](../README.md#getting-started) for Podman details.
2. Clone the repository and start the playground from its root:
   ```bash
   make up
   ```
3. Open the control panel at http://localhost:8090. After a minute or two the
   producer and both consumers should show messages. Leave the playground
   running: the protobuf consumer writes a file every minute, and the lab
   uses that data.

---

## Step 0: Check your setup

You run dbt through `make`, from the repository root. It runs dbt in a
container, in this folder:

```bash
make dbt                          # dbt build
make dbt ARGS="<dbt command>"     # any dbt command, e.g. make dbt ARGS="run"
```

What's in this folder:

| Path                    | What it is                                                   |
|-------------------------|--------------------------------------------------------------|
| `dbt_project.yml`       | The project's configuration (step 2)                         |
| `profiles.yml`          | Where dbt runs its SQL (step 2)                              |
| `src/models/`           | **Your models. Empty: you write them in this lab.**          |
| `src/macros/`           | **Your macros. Empty too.**                                  |
| `src/sources/`          | The source declaration, generated from the protobuf schema (step 3) |
| `src/protobuf_macros/`  | Provided macros that decode the protobuf files               |
| `databricks.yml`, `resources/` | The Databricks Asset Bundle (optional step)           |

**Do:** run dbt on the empty project:

```bash
make dbt
```

**Check:** the first run builds the container image, which takes a while.
The end of the output looks like this:

```
Finished running 3 project hooks, 20 data tests in 0 hours 0 minutes and 9.0 seconds (9.0s).
Completed successfully
Done. PASS=23 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=23
```

There are no models yet, but dbt already ran some *hooks* and *tests*. You'll
see what they are in steps 3 and 10.

Spark prints a few `WARN` lines and progress bars (`[Stage 0:> …]`) in
between. You can ignore them: dbt's own lines start with the time.

## Step 1: What dbt is

dbt is the **T** in ELT. It doesn't load or move data. You write `select`
statements, and dbt turns them into tables and views in your warehouse, in the
right order. The SQL runs on the warehouse (Databricks, or Spark in this lab);
dbt only generates it and sends it there.

On top of plain SQL you get dependencies between tables, tests,
documentation, and the same code running in dev and prod.

| On Databricks you might use…                        | In dbt                   | Step |
|-----------------------------------------------------|--------------------------|------|
| `CREATE OR REPLACE TABLE … AS SELECT` in a notebook | A model (a `.sql` file)  | 4    |
| Ordering job tasks by hand                          | `ref()` and the DAG      | 5    |
| Widgets / job parameters                            | Variables                | 10   |
| Expectations, ad-hoc check queries                  | Tests                    | 11   |
| Table and column comments in Unity Catalog          | Descriptions in YAML     | 12   |
| `MERGE INTO` only the new rows, with a watermark    | Incremental models       | 13   |
| An SCD type 2 `MERGE` written by hand               | Snapshots                | 14   |

Lakeflow Declarative Pipelines (DLT) is the closest Databricks feature: it also
lets you declare tables as queries and works out the order for you.

**Do:** nothing to build yet. Read on.

## Step 2: The project and the profile

A dbt **project** is a folder with a `dbt_project.yml`. It names the project,
says where the models are and sets defaults for them.

The **profile** says *where* the SQL runs: which warehouse, which catalog and
schema, which credentials. It lives in `profiles.yml`, apart from the
project code, because it differs per person and per environment. A profile
has one or more **targets** (e.g. `dev` and `prod`); you pick one with
`--target`.

**Do:** open [dbt_project.yml](dbt_project.yml) and [profiles.yml](profiles.yml).

- `profile: vehicle_telemetry` in `dbt_project.yml` matches the name at the
  top of `profiles.yml`.
- `model-paths` says dbt looks for models in `src/models` and `src/sources`.
- The `dev` target runs on Apache Spark, inside the dbt process
  (`method: session`), and writes to the schema `vehicle_telemetry`.

**Check:**

```bash
make dbt ARGS="debug"
```

ends with `All checks passed!`.

## Step 3: Sources

Data that dbt didn't create, loaded by another tool, is a **source**. You
declare sources in YAML and read them with `{{ source('<source>', '<table>') }}`.
Sources are the starting points of your project.

This project's source was generated for you from the protobuf schema:
[src/sources/raw_vehicle_telemetry.yml](src/sources/raw_vehicle_telemetry.yml).
It declares a source `raw` with two tables, both views over the protobuf files:

| Table                       | One row per…                                               |
|-----------------------------|------------------------------------------------------------|
| `vehicle_telemetry_files`   | file: its path and its bytes                               |
| `vehicle_telemetry_records` | Kafka record: topic, partition, offset, timestamp, and the message, still serialized, in `value` |

Most of the file is **tests** on the source: every message must decode, and
follow the rules in the `.proto` file. Those are the 20 tests you saw in step 0.
Step 11 is about tests.

**Do:** look at the raw Kafka records:

```bash
make dbt ARGS="show --inline \"select topic, partition, offset, timestamp, message_type from {{ source('raw', 'vehicle_telemetry_records') }}\""
```

**Check:** you see five Kafka records, from partitions 0, 1 and 2.

`dbt show` runs a query and prints the first rows. `--inline` takes the query
from the command line; usually you'll pass a model with `--select`.

## Step 4: Your first model

A **model** is one `.sql` file with one `select` statement. The file name is
the name of the view or table dbt creates. You never write `CREATE`: dbt
wraps your `select` in the right DDL.

A common convention is to sort models into layers. **Staging** models
(`stg_`) clean up one source each: rename, cast, flatten, remove duplicates.
No business logic. The **marts** (steps 5 to 8) are built on top of them.

### 4.1 Decode the messages

**Do:** create `src/models/staging/stg_vehicle_telemetry.sql`:

```sql
select * from {{ telemetry_source() }}
```

`telemetry_source()` is a provided *macro* (step 9) that decodes the protobuf
messages in the source. Run your model:

```bash
make dbt ARGS="run --select stg_vehicle_telemetry"
```

**Check:** `OK created sql view model vehicle_telemetry.stg_vehicle_telemetry`.
Look at the data:

```bash
make dbt ARGS="show --select stg_vehicle_telemetry"
```

The columns come from the protobuf message. Some are nested: `position` is
a struct of `lat` and `lon` (printed as `[59.33…, 18.02…]`), and
`tire_pressure_kpa` has one value per tire. Enums are upper case (`BUS`).
`dbt show` only prints the columns that fit; `…` marks the rest.

### 4.2 Clean it up

**Do:** replace the content of `stg_vehicle_telemetry.sql` with this. Read it
before you run it:

```sql
-- One row per telemetry reading, with nested fields flattened into columns.
--
-- telemetry_source() decodes the binary protobuf files written by the
-- protobuf consumer (see dbt/src/protobuf_macros/telemetry_source.sql).
--
-- The consumers deliver messages at-least-once, so the same message can end
-- up in the files more than once. We keep one row per message_id.
--
-- Protobuf enums arrive as their names (CAR, HARSH_BRAKING); they are
-- lower-cased here. New fields added to the .proto file are available in
-- telemetry_source() automatically; add them below to use them in the models.

with source as (

    select * from {{ telemetry_source() }}

),

deduplicated as (

    select
        message_id,
        vehicle_id,
        vin,
        lower(vehicle_type) as vehicle_type,
        {{ adapter.quote('timestamp') }} as recorded_at,
        to_date({{ adapter.quote('timestamp') }}) as event_date,

        source.position.lat as latitude,
        source.position.lon as longitude,
        heading_deg,
        speed_kmh,
        rpm,
        gear,
        engine_temp_c,
        fuel_level_pct,
        battery_voltage,
        odometer_km,

        source.tire_pressure_kpa.front_left as tire_front_left_kpa,
        source.tire_pressure_kpa.front_right as tire_front_right_kpa,
        source.tire_pressure_kpa.rear_left as tire_rear_left_kpa,
        source.tire_pressure_kpa.rear_right as tire_rear_right_kpa,

        check_engine_light,
        dtc_codes,
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
```

Note the style: one CTE per step (`source` → `deduplicated` →
`with_previous` → final `select`). This is the usual dbt style, and it keeps
long models readable. `{{ adapter.quote('timestamp') }}` quotes a column
name that's also a SQL keyword.

**Check:** run it and look at the result again, as in 4.1. The timestamp is
called `recorded_at` now, there's an `event_date`, and the vehicle types are
lower case (`car`).

**Solution:** [stg_vehicle_telemetry.sql](../dbt_solution/src/models/staging/stg_vehicle_telemetry.sql)

## Step 5: `ref()` and the DAG

A model reads another model with `{{ ref('<model name>') }}`. This does two things:

1. dbt replaces it with the real table name, e.g.
   `vehicle_telemetry.stg_vehicle_telemetry`. The schema comes from the
   target, so the same code writes to your dev schema in dev and to the prod
   schema in prod.
2. dbt learns that your model depends on the other one. From all the
   `ref()`s it builds a graph of dependencies (the **DAG**) and runs the
   models in that order.

Never write a table name by hand; always use `ref()` (or `source()`).

**Do:** create the first mart, `src/models/marts/dim_vehicles.sql`: one row
per vehicle, read from `stg_vehicle_telemetry`, with these columns:

| Column          | Value                                    |
|-----------------|------------------------------------------|
| `vehicle_id`    |                                          |
| `vin`           | from the vehicle's latest reading        |
| `vehicle_type`  | from the vehicle's latest reading        |
| `first_seen_at` | the first `recorded_at`                  |
| `last_seen_at`  | the last `recorded_at`                   |
| `reading_count` | the number of readings                   |
| `odometer_km`   | the highest odometer value               |

Hint: `max_by(vin, recorded_at)` returns the `vin` of the row with the
latest `recorded_at`.

```bash
make dbt ARGS="run --select dim_vehicles"
make dbt ARGS="show --select dim_vehicles"
```

**Check:** about five rows, one per vehicle. Then look at the DAG:

```bash
make dbt ARGS="ls --select +dim_vehicles --resource-type model"    # dim_vehicles and everything upstream
make dbt ARGS="run --select stg_vehicle_telemetry+"                 # stg_vehicle_telemetry and everything downstream
```

The first command lists both models. The second runs both, staging first.

Did you notice? The run said `sql view model vehicle_telemetry.dim_vehicles`:
your mart is a view. That's the default. Step 6 changes it.

**Solution:** [dim_vehicles.sql](../dbt_solution/src/models/marts/dim_vehicles.sql)

## Step 6: Materializations

A **materialization** decides what dbt creates from a model:

| Materialization | What it creates                                  |
|-----------------|--------------------------------------------------|
| `view`          | A view; the query runs when you read it (default) |
| `table`         | A table, rebuilt completely on every run         |
| `incremental`   | A table that only processes new rows (step 13)   |
| `ephemeral`     | Nothing; it's inlined as a CTE where it's used   |

You set it, like most configuration, either for a whole folder in
`dbt_project.yml`, or in the model's file with
`{{ config(materialized='table') }}` at the top. The setting closest to the
model wins.

**Do:** add this at the end of [dbt_project.yml](dbt_project.yml):

```yaml
models:
  vehicle_telemetry:
    # Delta Lake tables, as on Databricks (where it's the default). Locally,
    # the incremental model (step 13) needs it.
    +file_format: delta
    staging:
      +materialized: view
    marts:
      +materialized: table
```

`vehicle_telemetry` is the project name; `staging` and `marts` are folders
under `src/models`. The `+` marks a configuration value (rather than a
folder name). Settings apply to the folder and everything in it.

**Check:**

```bash
make dbt ARGS="run"
```

shows `sql view model vehicle_telemetry.stg_vehicle_telemetry` and
`sql table model vehicle_telemetry.dim_vehicles`.

**Solution:** [dbt_project.yml](../dbt_solution/dbt_project.yml)

## Step 7: More marts

Marts are what people query: **facts** (`fct_`, events and measurements)
and **dimensions** (`dim_`, things). They only read staging models (or other
marts), never sources.

**Do:** create two more marts.

1. `src/models/marts/vehicle_latest_status.sql`: the latest reading of every
   vehicle, with the columns `vehicle_id`, `vehicle_type`, `recorded_at`,
   `latitude`, `longitude`, `speed_kmh`, `engine_temp_c`, `fuel_level_pct`,
   `battery_voltage`, `check_engine_light`, `dtc_codes`, and
   `min_tire_pressure_kpa`, the lowest of the four tire pressures:

   ```sql
   least(tire_front_left_kpa, tire_front_right_kpa, tire_rear_left_kpa, tire_rear_right_kpa) as min_tire_pressure_kpa
   ```

   Hint: `qualify row_number() over (partition by vehicle_id order by recorded_at desc) = 1`.

2. `src/models/marts/fct_vehicle_daily.sql`: one row per vehicle and day.
   This one is mostly SQL, so copy it:

   ```sql
   -- One row per vehicle per day: how far it drove and how it was doing.

   select
       vehicle_id,
       event_date,
       count(*) as reading_count,
       min(recorded_at) as first_reading_at,
       max(recorded_at) as last_reading_at,

       sum(distance_km) as distance_km,
       avg(speed_kmh) as avg_speed_kmh,
       max(speed_kmh) as max_speed_kmh,
       avg(case when speed_kmh > 1 then speed_kmh end) as avg_moving_speed_kmh,

       avg(engine_temp_c) as avg_engine_temp_c,
       max(engine_temp_c) as max_engine_temp_c,
       min(fuel_level_pct) as min_fuel_level_pct,
       min(battery_voltage) as min_battery_voltage,
       min(least(tire_front_left_kpa, tire_front_right_kpa, tire_rear_left_kpa, tire_rear_right_kpa)) as min_tire_pressure_kpa,

       sum(case when array_contains(events, 'harsh_braking') then 1 else 0 end) as harsh_braking_count,
       sum(case when array_contains(events, 'harsh_acceleration') then 1 else 0 end) as harsh_acceleration_count,
       sum(case when array_contains(events, 'speeding') then 1 else 0 end) as speeding_readings,
       sum(case when array_contains(events, 'engine_overheating') then 1 else 0 end) as overheating_readings,
       max(case when check_engine_light then 1 else 0 end) = 1 as had_check_engine_light

   from {{ ref('stg_vehicle_telemetry') }}

   group by vehicle_id, event_date
   ```

**Check:** select a whole folder by its name:

```bash
make dbt ARGS="run --select marts"
make dbt ARGS="show --select fct_vehicle_daily"
```

Three table models are created. `fct_vehicle_daily` shows the distance per
vehicle and day.

**Solution:** [vehicle_latest_status.sql](../dbt_solution/src/models/marts/vehicle_latest_status.sql),
[fct_vehicle_daily.sql](../dbt_solution/src/models/marts/fct_vehicle_daily.sql)
(these already have the changes from steps 9 and 10).

## Step 8: Jinja and compiled SQL

Everything in `{{ … }}` and `{% … %}` is **Jinja**, a template language.
dbt first renders the template into plain SQL (**compiles** it), then sends
that SQL to the warehouse. `ref()` and `source()` are Jinja functions.
`{{ … }}` outputs a value, `{% … %}` is logic (`if`, `for`, `set`), and
`{# … #}` is a comment.

dbt also ships with macros that generate the right SQL for each warehouse,
such as `dbt.concat()`.

**Do:** create `src/models/marts/fct_vehicle_events.sql`, one row per event
(a reading can have several events):

```sql
-- One row per event (harsh_braking, engine_overheating, ...), with where and
-- when it happened. A reading can carry several events.

with exploded as (

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

    from {{ ref('stg_vehicle_telemetry') }}

)

select
    {{ dbt.concat(['message_id', "'-'", 'event_type']) }} as event_id,
    *

from exploded
```

Compile it, without running it:

```bash
make dbt ARGS="compile --select fct_vehicle_events"
```

**Check:** the output is plain SQL: `ref('stg_vehicle_telemetry')` became
`vehicle_telemetry.stg_vehicle_telemetry`, and `dbt.concat(…)` became
`concat(message_id, '-', event_type)`. The compiled SQL is also saved in
`target/compiled/vehicle_telemetry/src/models/marts/fct_vehicle_events.sql`.
When a model fails, look there first: it's the SQL that actually ran.

Then build it: `make dbt ARGS="run --select fct_vehicle_events"`.

**Solution:** the solution's [fct_vehicle_events.sql](../dbt_solution/src/models/marts/fct_vehicle_events.sql)
is the incremental version from step 13.

## Step 9: Macros

A **macro** is a reusable piece of Jinja and SQL, like a function. Macros
live in `src/macros/`; dbt finds them by name.

In step 7 you wrote the list of four tires twice. That's what macros are for.

**Do:**

1. Create `src/macros/min_tire_pressure.sql`:

   ```sql
   {#
     The lowest of the four tire pressures of a reading. Used by
     fct_vehicle_daily and vehicle_latest_status, so the list of tires is
     written only once.
   #}
   {% macro min_tire_pressure() %}
       least(tire_front_left_kpa, tire_front_right_kpa, tire_rear_left_kpa, tire_rear_right_kpa)
   {% endmacro %}
   ```

2. Use it in `fct_vehicle_daily.sql`:
   `min({{ min_tire_pressure() }}) as min_tire_pressure_kpa`,
   and in `vehicle_latest_status.sql`:
   `{{ min_tire_pressure() }} as min_tire_pressure_kpa`.

**Check:** `make dbt ARGS="compile --select vehicle_latest_status"` shows the
`least(…)` back in the compiled SQL. (dbt only prints the compiled SQL when
you compile one model; for more, look in `target/compiled/`.) Then run them:
`make dbt ARGS="run --select marts"`.

Macros can take arguments, like functions. Two provided macros worth a look,
in [src/protobuf_macros/telemetry_source.sql](src/protobuf_macros/telemetry_source.sql):

- `telemetry_source()`, which your staging model calls, returns a subquery
  that decodes the messages with Spark's `from_protobuf`.
- `raw_protobuf_files()` is **dispatched**: it calls
  `spark__raw_protobuf_files()` locally and `databricks__raw_protobuf_files()`
  on Databricks, because reading files works a little differently there.
  This is how one project runs on both.

**Solution:** [min_tire_pressure.sql](../dbt_solution/src/macros/min_tire_pressure.sql)

## Step 10: Variables and hooks

**Variables** are project settings that you can override for one run.
They're defined under `vars:` in `dbt_project.yml` and used with
`{{ var('name') }}`. `env_var()` reads environment variables, e.g. for
secrets.

Your project already has some: `raw_path`, where the protobuf files are, is
used by the provided macros. The Databricks job overrides it to read the
files from a Unity Catalog volume.

**Hooks** run SQL at the start or end of a run (`on-run-start`,
`on-run-end`), or before or after each model (`pre-hook`, `post-hook`).
The `on-run-start` hooks in your `dbt_project.yml` create the source views
over the protobuf files before anything else runs. Those were the
"3 project hooks" in step 0.

**Do:**

1. In [dbt_project.yml](dbt_project.yml), add a variable under `vars:`:

   ```yaml
     # Below this fuel level, vehicle_latest_status flags a vehicle.
     low_fuel_pct: 15
   ```

2. In `vehicle_latest_status.sql`, add a column after `fuel_level_pct`:

   ```sql
       fuel_level_pct < {{ var('low_fuel_pct') }} as is_low_on_fuel,
   ```

**Check:** compile it with a different value for the variable:

```bash
make dbt ARGS="compile --select vehicle_latest_status --vars '{low_fuel_pct: 100}'"
```

The compiled SQL says `fuel_level_pct < 100 as is_low_on_fuel`. Without
`--vars` it's 15. Then build it: `make dbt ARGS="run --select vehicle_latest_status"`.

**Solution:** [dbt_project.yml](../dbt_solution/dbt_project.yml),
[vehicle_latest_status.sql](../dbt_solution/src/models/marts/vehicle_latest_status.sql)

## Step 11: Tests

A dbt **test** is a `select` that returns the rows that are wrong. No rows
means it passes.

**Generic tests** are declared in YAML, next to the models, and take
arguments. Four come with dbt:

| Test              | Fails when                                            |
|-------------------|-------------------------------------------------------|
| `unique`          | a value appears more than once                        |
| `not_null`        | a value is null                                       |
| `accepted_values` | a value isn't in the list                             |
| `relationships`   | a value has no match in another model (a foreign key) |

The same YAML files hold `description`s of models and columns (step 12). The
file name doesn't matter; `_<folder>.yml` is a common convention.

### 11.1 Tests on the staging model

**Do:** create `src/models/staging/_staging.yml`:

```yaml
version: 2

models:
  - name: stg_vehicle_telemetry
    description: >
      Vehicle telemetry decoded from the binary protobuf files written by the
      protobuf consumer, flattened and de-duplicated on message_id.
    columns:
      - name: message_id
        description: Unique id the producer gives every message.
        data_tests:
          - unique
          - not_null
      - name: vehicle_id
        data_tests:
          - not_null
      - name: vehicle_type
        data_tests:
          - accepted_values:
              arguments:
                values: ["car", "van", "truck", "bus"]
      - name: recorded_at
        description: When the vehicle took the reading.
        data_tests:
          - not_null
      - name: latitude
        data_tests:
          - not_null
      - name: longitude
        data_tests:
          - not_null
```

Then run the tests of that model: `make dbt ARGS="test --select stg_vehicle_telemetry"`.

### 11.2 Tests on the marts

**Do:** create `src/models/marts/_marts.yml`. Start with this, then add the
tests for `fct_vehicle_events` yourself:

- `event_id` is unique and not null,
- every `vehicle_id` exists in `dim_vehicles` (`relationships`, with
  `arguments:` `to: ref('dim_vehicles')` and `field: vehicle_id`),
- `event_type` is one of `harsh_braking`, `harsh_acceleration`, `speeding`,
  `engine_overheating`, `low_fuel`, `refueled`, `low_tire_pressure`.

```yaml
version: 2

models:
  - name: dim_vehicles
    description: One row per vehicle.
    columns:
      - name: vehicle_id
        data_tests:
          - unique
          - not_null

  - name: vehicle_latest_status
    description: The latest reading of every vehicle.
    columns:
      - name: vehicle_id
        data_tests:
          - unique
          - not_null

  - name: fct_vehicle_daily
    description: Distance driven and health indicators per vehicle and day.
    columns:
      - name: distance_km
        description: Sum of the distance driven between consecutive readings.
```

### 11.3 Your own generic test

Is `fct_vehicle_daily` really one row per vehicle and day? No built-in test
checks a combination of columns, but you can write your own generic test:
a macro in a `{% test %}` block.

**Do:** create `src/macros/test_unique_combination_of_columns.sql`:

```sql
{#
  A custom generic test: fails if the combination of the given columns is
  not unique. (dbt_utils has the same test; this keeps the project free of
  package dependencies.)
#}
{% test unique_combination_of_columns(model, columns) %}

select {{ columns | join(', ') }}, count(*) as row_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
```

and use it on the whole model (not on a column) in `_marts.yml`:

```yaml
  - name: fct_vehicle_daily
    description: Distance driven and health indicators per vehicle and day.
    data_tests:
      - unique_combination_of_columns:
          arguments:
            columns: [vehicle_id, event_date]
    columns:
      ...
```

### 11.4 A singular test

A **singular test** is a one-off: a `.sql` file in `src/tests/` that returns
the bad rows.

**Do:** create `src/tests/assert_no_readings_from_the_future.sql`:

```sql
-- A singular test: a vehicle can't record a reading in the future. A row
-- here points at a vehicle (or producer) with a wrong clock. No rows means
-- the test passes.

select
    message_id,
    vehicle_id,
    recorded_at

from {{ ref('stg_vehicle_telemetry') }}

where recorded_at > current_timestamp()
```

### 11.5 Build

`dbt test` only runs tests. **`dbt build`** runs models and tests together,
in DAG order: each model's tests run right after it, and when a test fails,
the models downstream are skipped. Bad data doesn't reach the marts.

**Check:**

```bash
make dbt
```

ends with `Completed successfully`: no `ERROR` and no `FAIL`. Your own tests
are listed among the 20 source tests.

Now make a test fail. In `_staging.yml`, remove `"bus"` from the accepted
values of `vehicle_type`, and run `make dbt` again. The test fails
(`Got 1 result`) and the four marts are skipped (`SKIP`). The failing test's
SQL is in `target/compiled/`; run it to see the bad rows. **Put `"bus"`
back afterwards.**

**Solution:** [_staging.yml](../dbt_solution/src/models/staging/_staging.yml),
[_marts.yml](../dbt_solution/src/models/marts/_marts.yml),
[test_unique_combination_of_columns.sql](../dbt_solution/src/macros/test_unique_combination_of_columns.sql),
[assert_no_readings_from_the_future.sql](../dbt_solution/src/tests/assert_no_readings_from_the_future.sql)

## Step 12: Documentation

The `description:` fields in your YAML files are documentation. dbt generates
a website from them, with every model, column, test and the lineage graph.

**Do:** add a `description:` to `fct_vehicle_events` in `_marts.yml`, e.g.
`One row per event (harsh braking, overheating, ...) reported by a vehicle.`
Then generate the site:

```bash
make dbt ARGS="docs generate --static"
```

**Check:** open `dbt/target/static_index.html` in your browser. Find
`fct_vehicle_events` and its description, then click the lineage button
(bottom right) to see your DAG.

On Databricks, the `persist_docs` setting can also write the descriptions to
Unity Catalog, as table and column comments.

## Step 13: Incremental models

A `table` model is rebuilt from scratch on every run. Kafka data only grows,
so that gets slower every day, while most of the data hasn't changed since
the last run. An **incremental** model only processes the new rows and adds
them to the existing table.

Events never change once they happened, so `fct_vehicle_events` is a good
fit. But which rows are new? You need a **watermark**: a column that grows,
so "new" means "higher than the highest value already in the table".

Kafka offsets look perfect for this: within a partition they only grow. But
they start at 0 again when a topic is recreated, and then new records look
older than the ones you already loaded. (That happened while this lab was
written: the model silently skipped every new record.) So this model uses
`ingested_at`, the time the protobuf consumer received the record from Kafka.

**Do:** replace `src/models/marts/fct_vehicle_events.sql` with:

```sql
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
```

- `{{ config(…) }}` sets the materialization for this one model (step 6).
- `is_incremental()` is true when the table already exists (and you didn't
  pass `--full-refresh`). The first run builds the whole table; later runs
  only add.
- `{{ this }}` is the model's own table.
- The consumer writes a file once a minute, so a reading reaches the files up
  to a minute after its `ingested_at`. The model looks back 5 minutes to be
  safe, so it sees some events again. And Kafka delivers **at-least-once**,
  so the same event can come twice anyway. With `unique_key` and the `merge`
  strategy, dbt updates the existing row instead of adding a second one.
  Running the model twice gives the same result.

**Check:**

1. The model has a new column (`ingested_at`), so rebuild it once from
   scratch. `--full-refresh` ignores the existing table:
   ```bash
   make dbt ARGS="build --select fct_vehicle_events --full-refresh"
   ```
2. Count the events:
   ```bash
   make dbt ARGS="show --inline \"select count(*) as events, max(ingested_at) as last_ingested from {{ ref('fct_vehicle_events') }}\""
   ```
3. Wait a minute, so the protobuf consumer writes a new file, then build
   (incrementally this time):
   ```bash
   make dbt ARGS="build --select fct_vehicle_events"
   ```
   The log says `OK created sql incremental model`.
4. Count again: there are more events, and a later `last_ingested`.
5. Look at the SQL dbt ran, in
   `target/run/vehicle_telemetry/src/models/marts/fct_vehicle_events.sql`.
   It's a `merge into … using fct_vehicle_events__dbt_tmp … on … event_id`.
   `make dbt ARGS="compile --select fct_vehicle_events"` shows the `where`
   filter on `ingested_at`.

`--full-refresh` is what you use whenever you change an incremental model's
columns or logic.

Other strategies: `append` (just insert), `insert_overwrite` (replace whole
partitions) and, on Databricks, `replace_where` and `microbatch` (process the
data in time batches).

A limitation here: the staging view still decodes all the files on every run,
so this saves work in the model, not in reading the files. On Databricks you
would also read only the new files, e.g. with Auto Loader or a streaming table.

**Solution:** [fct_vehicle_events.sql](../dbt_solution/src/models/marts/fct_vehicle_events.sql)

## Step 14: Snapshots

dbt rebuilds tables, so they only show the current state. `dim_vehicles` says
how far each vehicle has driven *now*, but not how far it had driven last
week. A **snapshot** keeps the history: every time it runs, it compares the
model with its previous run, closes the rows that changed and adds their new
version. This is a slowly changing dimension of type 2 (SCD2).

**Do:** create `src/snapshots/dim_vehicles_snapshot.yml`:

```yaml
# Keeps the history of dim_vehicles: `dbt snapshot` (and `dbt build`) adds a
# new version of a vehicle's row whenever it changed, and closes the old one.
# Each version has dbt_valid_from and dbt_valid_to; the current one has
# dbt_valid_to = null. This is a slowly changing dimension of type 2.
snapshots:
  - name: dim_vehicles_snapshot
    relation: ref('dim_vehicles')
    description: Every version of each vehicle's row in dim_vehicles.
    config:
      unique_key: vehicle_id
      # A row has changed when last_seen_at is newer, i.e. the vehicle sent
      # new readings. (strategy: check compares chosen columns instead.)
      strategy: timestamp
      updated_at: last_seen_at
      # Snapshots need Delta Lake locally (it's the default on Databricks).
      file_format: delta
```

- `strategy: timestamp`: a row has changed when its `updated_at` column is
  newer. `strategy: check` compares a list of columns (`check_cols`) instead,
  for tables without a reliable "updated at" column.
- dbt adds the columns `dbt_valid_from` and `dbt_valid_to`. The current
  version has `dbt_valid_to = null`.
- `dbt snapshot` runs the snapshots. `dbt build` runs them too, in DAG order.
- A snapshot can't be rebuilt: the history only exists in that table. That's
  why it isn't a model, and `--full-refresh` doesn't touch it.

**Check:**

1. Take the first snapshot: `make dbt ARGS="snapshot"`.
2. Wait a minute, then rebuild `dim_vehicles` and everything after it,
   including the snapshot: `make dbt ARGS="build --select dim_vehicles+"`.
3. Look at one vehicle's history:
   ```bash
   make dbt ARGS="show --inline \"select vehicle_id, last_seen_at, reading_count, dbt_valid_from, dbt_valid_to from {{ ref('dim_vehicles_snapshot') }} where vehicle_id = 'vehicle-001' order by dbt_valid_from\""
   ```
   There are two versions: the old one has a `dbt_valid_to`, the current one
   doesn't.

**Solution:** [dim_vehicles_snapshot.yml](../dbt_solution/src/snapshots/dim_vehicles_snapshot.yml)

## Step 15: Wrap-up

**Do:** run everything once more, from scratch:

```bash
make dbt ARGS="build --full-refresh"
```

**Check:** `Completed successfully`. Then compare your project with the
solution, from the repository root:

```bash
diff -r dbt/src/models dbt_solution/src/models
```

Differences in the models you wrote yourself (steps 5 and 7) are fine, as
long as the tests pass. To see the solution's tables, build it into its own
schema, `vehicle_telemetry_solution`:

```bash
make solution
```

You can query both schemas in Superset's SQL Lab (http://localhost:8088,
admin / admin).

### What we didn't cover

- **Seeds**: small CSV files loaded as tables, e.g. lookup lists.
- **Packages**: reuse other people's macros and tests, like `dbt_utils`
  (`packages.yml`, `dbt deps`).
- **Model contracts and versions**: enforce a model's columns and types.
- **Exposures**: declare the dashboards that use your models.
- **Source freshness**: warn when a source hasn't received new data.

### If you finish early

1. Add a model `fct_vehicle_hourly` with the average speed per vehicle and
   hour, with tests and a description.
2. Change the snapshot to `strategy: check` with `check_cols: [odometer_km]`.
   When does it add a new version now?
3. Add a `relationships` test from `vehicle_latest_status` to `dim_vehicles`.
4. Do the optional Databricks step below.

## Optional: run it on Databricks

The same project runs on Databricks, deployed as a Databricks Asset Bundle.
You need your own workspace (the free edition works) and the Databricks CLI.
See [README.md](README.md#deploying-to-databricks-as-an-asset-bundle) to log
in and configure it.

- [resources/vehicle_telemetry.job.yml](resources/vehicle_telemetry.job.yml)
  is a job with a **dbt task** that runs `dbt build` on a SQL warehouse. It
  passes the volume path with `--vars` (step 10).
- Databricks creates the dbt profile for the job itself, from the warehouse,
  catalog and schema in the task. You don't commit credentials.
- `raw_protobuf_files()` picks the Databricks implementation (step 9).
  Nothing else changes: tables are Delta there too, so the incremental model
  and the snapshot work the same way.
- In the `dev` target, the bundle deploys to the schema
  `dev_<your_name>_vehicle_telemetry`, so everyone has their own copy.

```bash
make dab-deploy     # create the schema, volume and job
make dab-upload     # copy the protobuf files to the volume
make dab-run        # run dbt build on Databricks
```

---

## Notes for the host

- Ask everyone to do [Before the session](#before-the-session) a day ahead;
  building the images on conference Wi-Fi takes too long.
- Common problems:
  - **YAML indentation** in `_staging.yml`, `_marts.yml` and
    `dbt_project.yml`. dbt's error names the file.
  - **A typo in `ref()`**: `depends on a node named '…' which was not found`.
  - **An error about an existing table** after changing an incremental model,
    e.g. `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY` when `ingested_at` is
    missing (step 13 skipped its `--full-refresh`): run with `--full-refresh`.
  - **A failing test someone forgot to restore** (`"bus"` in step 11).
- Someone who falls behind can copy the solution files for the steps they
  skip. Each step links them. The solution's marts already include the macro
  (step 9) and the variable (step 10), so copy `src/macros/min_tire_pressure.sql`
  and the `low_fuel_pct` variable along with them.
- `make solution` builds the finished project into `vehicle_telemetry_solution`
  if you want to show the end result first.
