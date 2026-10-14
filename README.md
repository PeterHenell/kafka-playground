# Kafka playground

A small, self-contained setup for learning Apache Kafka. A Python producer
simulates a fleet of vehicles that send telemetry (position, speed, engine
temperature, fuel level, tire pressure...) to Kafka as
[Protocol Buffers](https://protobuf.dev/) messages. The readings follow the
[COVESA Vehicle Signal Specification](https://covesa.github.io/vehicle_signal_specification/)
(VSS), an open standard for naming vehicle data: the speed is
`Vehicle.Speed`, the fuel level `Vehicle.Powertrain.FuelSystem.RelativeLevel`.
Two consumers read the same stream, each in its own consumer group:

- **consumer** stores the messages in an ELK stack (Elasticsearch, Logstash,
  Kibana), where you can search, chart and map them in near real time.
- **protobuf-consumer** archives the messages, still in binary protobuf, to
  files. A dbt project decodes them with Apache Spark and turns them into
  analytics tables, which you can browse, query and chart in
  [Apache Superset](https://superset.apache.org/). You build that project
  yourself in a [hands-on dbt lab](dbt/LEARN_DBT.md); the finished version is
  in [dbt_solution/](dbt_solution/README.md). The same dbt project can be
  deployed to Databricks as an Asset Bundle.

The message format is defined once, in `.proto` files. Everything else
(the Python classes, the conversion to Elasticsearch documents, the
Elasticsearch mapping, the schema Spark decodes with) follows from it; see
[The message schema](#the-message-schema).

```
                                                 group elk-writer
┌──────────┐    ┌─────────────────────────┐    ┌──────────┐    ┌──────────┐    ┌───────────────┐    ┌────────┐
│ producer │ -> │ Kafka                   │ -> │ consumer │ -> │ Logstash │ -> │ Elasticsearch │ <- │ Kibana │
│ (python) │    │ topic vehicle-telemetry │    │ (python) │    │ (HTTP)   │    │               │    │        │
└──────────┘    │ 3 partitions            │    └──────────┘    └──────────┘    └───────────────┘    └────────┘
                └─────────────────────────┘
                             │                   group protobuf-writer
                             │                 ┌───────────────────┐    ┌────────────────┐    ┌─────────────────┐
                             └───────────────> │ protobuf-consumer │ -> │ data/protobuf/ │ -> │ dbt + Spark     │
                                               │ (python)          │    │ *.pb           │    │ (or Databricks) │
                                               └───────────────────┘    └────────────────┘    └─────────────────┘
```

Everything runs in a single Docker Compose project.

## Getting started

Requirements: Docker with Docker Compose, or Podman with `podman compose`,
and about 5 GB of free memory for the containers.

```bash
make up          # or: docker compose up -d --build
```

The Makefile uses Podman if it's installed, and Docker otherwise; choose with
e.g. `make up ENGINE=docker`. With Podman, read `podman` wherever these docs
say `docker` (`podman compose run --rm dbt build`, `podman exec kafka ...`).
`podman compose` needs Podman's API socket: on macOS and Windows,
`podman machine start` provides it; on Linux, run
`systemctl --user enable --now podman.socket`.

The first start takes a minute or two while Elasticsearch and Kibana boot.
Check progress with `make ps` and `make logs`. Then open the control panel at
http://localhost:8090, which links to all the other tools.

The Makefile has shortcuts for the common tasks (run `make` to list them):

| Command         | What it does                                                              |
|-----------------|---------------------------------------------------------------------------|
| `make up`       | Build and start everything                                                |
| `make down`     | Stop everything; the data is kept                                         |
| `make ps`       | Show the status of the services                                           |
| `make logs`     | Follow the logs of the producer and the consumers                         |
| `make dbt`      | Run `dbt build` on your lab project (`dbt/`); other commands with `make dbt ARGS="test"` |
| `make solution` | Run `dbt build` on the finished project (`dbt_solution/`)                 |
| `make spark`    | Open a Spark SQL shell on the dbt tables                                  |
| `make superset` | Show where Superset runs and how to log in                                |
| `make invalid`  | Publish a few invalid messages, to see the dbt source tests fail          |
| `make clean`    | Stop everything and delete **all** data (Kafka, Elasticsearch, Superset, `./data`) |

| Service       | URL                        | What it is                                      |
|---------------|----------------------------|-------------------------------------------------|
| Control panel | http://localhost:8090      | Pause/resume components, produced/consumed/lag  |
| Superset      | http://localhost:8088      | Table catalog, SQL on Spark, charts (admin / admin) |
| Spark UI      | http://localhost:4040      | Jobs and queries of the Spark Thrift Server     |
| Spark Thrift Server | `localhost:10000`    | JDBC/ODBC (HiveServer2) access to the Spark tables |
| Kibana        | http://localhost:5601      | Explore the stored telemetry                    |
| Kafka UI      | http://localhost:8080      | Topics, decoded messages, consumer groups       |
| Elasticsearch | http://localhost:9200      | REST API for the stored data                    |
| Logstash      | http://localhost:8081      | HTTP input the consumer posts to                |
| Kafka         | `localhost:9094`           | Bootstrap server for clients on your machine    |

Stop everything with `docker compose down`. Add `-v` to also delete the stored
Kafka and Elasticsearch data and Superset's own database (charts, dashboards,
saved queries). The protobuf files, the Spark tables and their
catalog (the `metastore-db` Postgres data) are in `./data`; delete that folder
to start over.

### The control panel

http://localhost:8090 is the starting point. Its **Tools** section links to
the other web UIs (Superset, Kibana, Kafka UI, the Spark UI and the
Elasticsearch API), says what each one is for, and shows whether it's up.

Below that, it shows per component whether it's running, and for the topic
and each consumer group:

- **Produced**: messages written to the topic (the sum of its end offsets).
- **Consumed**: messages a consumer group has committed (the sum of its
  committed offsets).
- **Not consumed**: the difference, the group's *lag*, also as a chart over
  the last 10 minutes and per partition in a table.

Each component has a **Pause** button. The panel writes the desired state to
the compacted `playground-control` topic, and the components follow it
(`common/kafka_control.py`):

- The producer stops sending.
- A consumer calls Kafka's `consumer.pause()` on its partitions. It keeps
  polling, so it stays in its consumer group (no rebalance), but gets no
  messages until it's resumed. Its lag grows meanwhile, and it catches up
  after resuming.

Because the topic is compacted, the state survives restarts: a paused
component that restarts stays paused.

### Looking at the data in Kibana

A data view called **Vehicle telemetry** is created automatically.

Fields are named by their VSS path, e.g. `Vehicle.Speed`.

- **Discover** (menu → Analytics → Discover): see the raw messages. Try the
  queries `events : "HARSH_BRAKING"`,
  `Vehicle.Powertrain.CombustionEngine.EngineCoolant.Temperature > 100` or
  `Vehicle.Diagnostics.DTCCount > 0`.
- **Maps** (menu → Analytics → Maps): add a *Documents* layer on the
  *Vehicle telemetry* data view, with the geo field
  `Vehicle.CurrentLocation.GeoPoint`, to watch the vehicles move around
  Stockholm. Style it by `vehicle_id` and turn on auto-refresh.
- **Dashboards**: build charts of e.g. average `Vehicle.Speed` or maximum
  `Vehicle.Powertrain.CombustionEngine.EngineCoolant.Temperature` per
  `vehicle_id` over time.

### Processing the protobuf files with dbt

The protobuf consumer writes a file to `data/protobuf/vehicle_telemetry/date=YYYY-MM-DD/`
every 60 seconds (or every 5000 messages). Turning them into tables is a
hands-on lab: [dbt/LEARN_DBT.md](dbt/LEARN_DBT.md) walks you through building
the dbt project in `dbt/`, step by step, in about two hours.

To get the finished tables right away, build the solution, once the first
file is there:

```bash
make solution                                        # docker compose run --rm dbt-solution build
make solution ARGS="show --select fct_vehicle_daily"
```

Its tables go to the schema `vehicle_telemetry_solution`; the lab builds into
`vehicle_telemetry`. See [dbt/README.md](dbt/README.md) for how the project
reads the protobuf files and how to deploy it to Databricks as a Databricks
Asset Bundle, and [dbt_solution/README.md](dbt_solution/README.md) for the
models.

### Exploring the tables in Superset

[Apache Superset](https://superset.apache.org/) runs at http://localhost:8088
(log in with **admin** / **admin**; the port is only published on
127.0.0.1). It comes with a database connection called
**Spark (vehicle telemetry)**.

- **SQL Lab** (menu → SQL → SQL Lab, http://localhost:8088/sqllab/): the
  left side is a catalog browser. Pick the `vehicle_telemetry_solution`
  schema (or `vehicle_telemetry`, for the tables you built in the lab) to see
  its tables and views and their columns, and write queries on the right,
  e.g. `select * from vehicle_telemetry_solution.fct_vehicle_daily`. The raw
  sources are there too: `stg_vehicle_telemetry` is a view that decodes the
  protobuf archive with `from_protobuf` on every query.
- **Charts and dashboards**: turn a table (menu → Datasets → + Dataset) or a
  SQL Lab query (*Save dataset*) into a dataset, then chart it, e.g. distance
  per vehicle and day from `fct_vehicle_daily`.

Superset doesn't run queries itself. It sends them to the **Spark Thrift
Server** (`spark-thrift-server`), a long-running Spark application that
answers SQL over JDBC/ODBC (the HiveServer2 protocol). Its Spark UI, at
http://localhost:4040, shows the jobs behind each query. Other SQL clients,
such as DBeaver or beeline, can connect to it too:
`jdbc:hive2://localhost:10000/vehicle_telemetry` (no password).

Superset, dbt and the `spark-sql` shell can all run at the same time. They
share one table catalog, a Hive metastore kept in the `metastore-db` Postgres
container (`spark/spark-defaults.conf`). Spark's default, an embedded Derby
database, can only be opened by one process at a time. Superset keeps its
own settings, charts and dashboards in the `superset-home` volume.

## What's in the repo

| Path                                   | Description                                                       |
|----------------------------------------|-------------------------------------------------------------------|
| `docker-compose.yml`                   | All services                                                      |
| `schema/proto/`                        | The protobuf message definitions                                  |
| `schema/generate.py`                   | Generates code, the Elasticsearch mapping and dbt tests from the .proto files |
| `schema/telemetry_schema/`             | Shared code: finding message classes, protobuf to Elasticsearch   |
| `producer/producer.py`                 | Vehicle simulator, publishes to Kafka                             |
| `producer/send_invalid.py`             | Publishes a few invalid messages, to see the dbt source tests fail |
| `control-panel/`                       | The control panel: a Flask app (`app.py`) and one HTML page       |
| `superset/`                            | The Superset image: config, and the start script that adds the Spark connection |
| `spark/spark-defaults.conf`            | Spark settings shared by dbt, spark-sql and the Thrift Server     |
| `metastore/init/`                      | Hive metastore schema for the `metastore-db` Postgres              |
| `common/kafka_control.py`              | Pause/resume through the `playground-control` topic              |
| `consumer/consumer.py`                 | Reads from Kafka, posts batches to Logstash                       |
| `protobuf-consumer/protobuf_consumer.py` | Reads from Kafka, writes binary protobuf files to `./data/protobuf` |
| `dbt/`                                 | The dbt lab (`LEARN_DBT.md`): starter project and Databricks Asset Bundle |
| `dbt_solution/`                        | The finished dbt project                                          |
| `elk/logstash/pipeline/*.conf`         | Logstash pipeline: HTTP in, Elasticsearch out                     |
| `elk/setup/setup.py`                   | Installs the generated index template and the Kibana data view    |
| `kafka-ui/config.yml`                  | Lets Kafka UI decode the protobuf messages                        |

### A telemetry message

The producer uses `vehicle_id` as the message **key**. Kafka hashes the key
to choose a partition, so all messages from one vehicle go to the same
partition and keep their order.

The value is a `vehicle.v2.VehicleTelemetry` protobuf message, and a
`message-type` header names that type. It's an envelope: who sent the
reading and when, the events the vehicle detected, and the readings
themselves in `Vehicle`, a tree of
[VSS](https://covesa.github.io/vehicle_signal_specification/) signals. As a
document in Elasticsearch, a message looks like this:

```json
{
  "message_id": "6b2e25f1-46bd-4469-9da1-176885f5c0c3",
  "vehicle_id": "vehicle-001",
  "timestamp": "2026-10-14T12:33:54.524220Z",
  "Vehicle": {
    "VehicleIdentification": { "VIN": "E42JTRY3DA4R2AR0H", "BodyType": "car" },
    "Speed": 47.0,
    "TraveledDistance": 225879305,
    "Acceleration": { "Longitudinal": 0.14 },
    "CurrentLocation": {
      "Latitude": 59.351746, "Longitude": 18.062717, "Heading": 326.9,
      "GeoPoint": { "lat": 59.351746, "lon": 18.062717 }
    },
    "Powertrain": {
      "CombustionEngine": { "Speed": 1792.0, "EngineCoolant": { "Temperature": 117.8 } },
      "Transmission": { "CurrentGear": 3 },
      "FuelSystem": { "RelativeLevel": 54, "IsFuelLevelLow": false }
    },
    "LowVoltageBattery": { "CurrentVoltage": 14.07 },
    "Chassis": { "Axle": {
      "Row1": { "Wheel": { "Left":  { "Tire": { "Pressure": 238, "IsPressureLow": false } },
                           "Right": { "Tire": { "Pressure": 212, "IsPressureLow": false } } } },
      "Row2": { "Wheel": { "Left":  { "Tire": { "Pressure": 228, "IsPressureLow": false } },
                           "Right": { "Tire": { "Pressure": 214, "IsPressureLow": false } } } }
    } },
    "Diagnostics": { "DTCCount": 1, "DTCList": ["P0217"] }
  },
  "events": ["ENGINE_OVERHEATING"]
}
```

VSS also fixes each signal's data type and unit: `Speed` is a float in km/h,
`TraveledDistance` (the odometer) an integer in meters, `RelativeLevel` a
whole percent, tire `Pressure` whole kPa. `Row1` is the front axle.
`GeoPoint` isn't a VSS signal: it's added for the Kibana map (see below).

Some simulated vehicles have a faulty cooling system or a leaking tire, so
over time you will see events like `ENGINE_OVERHEATING`, `LOW_TIRE_PRESSURE`,
`HARSH_BRAKING`, `SPEEDING` and `LOW_FUEL`, along with diagnostic trouble codes.

## The message schema

Two files define the message:

- [`schema/proto/vehicle/v2/vehicle_telemetry.proto`](schema/proto/vehicle/v2/vehicle_telemetry.proto):
  the envelope (`message_id`, `vehicle_id`, `timestamp`, `events`) and the
  `Vehicle` field.
- [`schema/proto/vss/v6/vss.proto`](schema/proto/vss/v6/vss.proto): the
  part of VSS 6.1 our vehicles send. It's laid out the way COVESA's
  [vss-tools](https://github.com/COVESA/vss-tools) exports VSS to protobuf:
  one message per branch (`Vehicle.CurrentLocation` → `VehicleCurrentLocation`),
  one field per signal, named as in VSS. The comments give each signal's
  unit.

`schema/generate.py` turns them into everything the services need, in
`schema/generated/`:

| Output                                  | Made by                       | Used by                              |
|-----------------------------------------|-------------------------------|--------------------------------------|
| `vehicle/v2/vehicle_telemetry_pb2.py`, `vss/v6/vss_pb2.py` | `protoc` (via `grpcio-tools`) | all services, to (de)serialize |
| `kafka/v1/kafka_record_pb2.py`          | `protoc`                      | protobuf-consumer, file format       |
| `descriptors/descriptor_set.desc`       | `protoc`                      | Spark's `from_protobuf`, to decode   |
| `elasticsearch/index_template.json`     | `telemetry_schema/elk.py`     | `elk-setup`, to create the mapping   |
| `dbt/src/sources/raw_vehicle_telemetry.yml` | `telemetry_schema/dbt_tests.py` | dbt, to validate the archived messages (see [Validating the messages](#validating-the-messages)) |

The conversions at runtime don't use any hand-written, per-field code. They
walk the protobuf *descriptors*, the schema information that `protoc` embeds
in the generated code:

- **Elasticsearch**: protobuf's own [`json_format`](https://googleapis.dev/python/protobuf/latest/google/protobuf/json_format.html)
  turns a message into a JSON document. The index mapping comes from
  `telemetry_schema/elk.py`. It maps each protobuf type to an Elasticsearch
  type and honors custom field options for special cases
  ([`elk/options.proto`](schema/proto/elk/options.proto)):
  `(elk.field_type)` picks another Elasticsearch type, and `(elk.geo_point)`
  adds the `GeoPoint` field that Kibana's map needs, built from VSS's
  `Latitude` and `Longitude`.
- **Spark**: the protobuf consumer doesn't decode anything. It stores the
  Kafka records as they are, and Spark decodes them with its built-in
  [`from_protobuf`](https://spark.apache.org/docs/latest/sql-data-sources-protobuf.html)
  function, using the *descriptor set* (the compiled schema) that the consumer
  writes next to the files. Nested messages become structs, repeated fields
  become arrays, enums become their names, and Timestamps become timestamps.
- **Kafka UI** decodes the messages with its built-in `ProtobufFile` serde,
  reading the same `.proto` files.

The ELK consumer picks the message class by the `message-type` header, and
the protobuf consumer stores the header with each record. Neither contains
any field names.

### The protobuf files

Each file is one `kafka.v1.KafkaRecordBatch` message
([`kafka_record.proto`](schema/proto/kafka/v1/kafka_record.proto)): a list
of Kafka records, each with the original message bytes, the message type,
and the topic, partition, offset and timestamp.

```
data/protobuf/vehicle_telemetry/
├── _schema/descriptor_set.desc              the compiled schema, rewritten on every start
└── date=2026-09-29/                         the date of the Kafka timestamp
    ├── part-20260929T100733-2ad5872b.pb     one KafkaRecordBatch per file
    └── part-20260929T100833-4c021d36.pb
```

Decoding takes two `from_protobuf` calls: one for the file, one for each
record's bytes. See `dbt/src/protobuf_macros/telemetry_source.sql`, or try it in the
Spark SQL shell (`docker compose run --rm spark-sql`):

```sql
select from_protobuf(record.value, 'vehicle.v2.VehicleTelemetry', '/data/protobuf/vehicle_telemetry/_schema/descriptor_set.desc',
                     map('emit.default.values', 'true')) as message
from (
  select explode(from_protobuf(content, 'kafka.v1.KafkaRecordBatch', '/data/protobuf/vehicle_telemetry/_schema/descriptor_set.desc').records) as record
  from binaryFile.`/data/protobuf/vehicle_telemetry`
)
limit 5;
```

### Validating the messages

Types alone don't say much: any `float` is a valid speed. The `.proto` files
therefore also contain validation rules, written as
[protovalidate](https://protovalidate.com) annotations, the open-source
standard for this. They check the ranges VSS defines (a latitude is -90 to
90, a uint8 is at most 255) and some stricter "fleet rules" for implausible
values:

```proto
// Vehicle speed. Unit: km/h. VSS type: float. Fleet rule: at most 250.
float Speed = 2 [(buf.validate.field).float = {gte: 0, lte: 250}];
repeated string DTCList = 2 [(buf.validate.field).repeated.items.string.pattern = "^[PCBU][0-9]{4}$"];
```

(`schema/proto/buf/validate/validate.proto` is protovalidate's definition of
these options, copied from its repository.)

`schema/generate.py` turns the schema into **dbt source tests**
(`telemetry_schema/dbt_tests.py` writes
`dbt/src/sources/raw_vehicle_telemetry.yml`). They run against the
archived Kafka records, before any model is built, and check that every
message:

- **decodes** as a `vehicle.v2.VehicleTelemetry`;
- only uses **enum values defined in the schema** (protobuf accepts unknown
  enum numbers on the wire);
- follows the **protovalidate rules**: required fields, number ranges,
  string lengths and patterns, repeated items, timestamps not in the future.

There is one test per field that has rules, including every VSS signal in
`Vehicle`, named after its path, e.g.
`protobuf_valid__vehicle_telemetry__Vehicle_Speed`. When one fails, `dbt build`
skips the models that depend on the source, so invalid data never reaches
them. Run only the source tests with
`docker compose run --rm dbt test --select source:raw`.

What the tests can't see: a message cut off exactly between two fields is
still a valid (shorter) message, and fields that aren't in the schema are
ignored by Spark. Rules that can't be translated to SQL (e.g. CEL
expressions) are listed under `meta.unchecked_rules` in the generated file.

The rules found real bugs in the simulator when they were introduced: a
leaking tire's pressure went below 0 after about an hour, and a heading of
359.96° was rounded to 360.0°.

### Changing the schema

1. Edit the `.proto` file. To send another VSS signal, look it up in the
   [VSS documentation](https://covesa.github.io/vehicle_signal_specification/)
   and add it with its VSS name, type and the branch messages it needs. For
   `Vehicle.Exterior.AirTemperature` (a float, in Celsius), add to `vss.proto`:
   ```proto
   // Vehicle.Exterior: information about the vehicle's surroundings.
   message VehicleExterior {
     // Air temperature outside the vehicle. Unit: Celsius. VSS type: float.
     float AirTemperature = 1;
   }
   ```
   and `VehicleExterior Exterior = 10;` to `message Vehicle`.
2. Set the new field in `producer/producer.py`.
3. Rebuild and restart: `docker compose up -d --build`. Each image runs
   `schema/generate.py` while it is built.

The result:

- New Elasticsearch documents contain the field. `elk-setup` updates the
  index template and adds the field to the mapping of existing indices.
- The protobuf consumer writes the new descriptor set, so Spark can decode
  the field. It decodes the older files with the new schema too; in messages
  from before the change, the field has its default value (0, "", false).
- The same goes for messages still in Kafka that were produced with the old
  schema.
- The dbt source tests are regenerated (by the dbt image, or by
  `python schema/generate.py`), so new rules are checked too. The generated
  file is committed, so a schema change shows up in the diff of the tests.

To keep old and new messages compatible, only add fields with new numbers.
Never change the number or type of an existing field, and `reserve` the
numbers of removed fields. The dbt models list the columns they use, so add
new fields to the staging model (`stg_vehicle_telemetry.sql`, in `dbt/` and
`dbt_solution/`) to use them
there.

### Delivery guarantees

- The **producer** uses `acks=all` and `enable.idempotence=true`, so a retried
  send can't create duplicates in Kafka.
- The **consumer** turns off auto-commit and commits offsets only after
  Logstash has accepted the batch (*at-least-once*). If it crashes before
  committing, it reads the same messages again after a restart.
- **Logstash** uses `message_id` as the Elasticsearch document id, so
  messages that are delivered twice overwrite the same document instead of
  creating a duplicate.
- The **protobuf consumer** also commits only after a file is written. A file
  can't be "overwritten" like a document, so duplicates are removed later, in
  the dbt staging model.

## Running the scripts from your machine

The producer and consumer also run in Compose, but you can run extra copies
on your machine (they connect to `localhost:9094` by default):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python schema/generate.py          # again after every .proto change

python producer/producer.py
python consumer/consumer.py
python protobuf-consumer/protobuf_consumer.py
TOOLS_CHECK_HOST=localhost python control-panel/app.py   # http://localhost:8090
```

The scripts are configured with environment variables. The docstring at the
top of each script lists them, for example `NUM_VEHICLES=20 INTERVAL_SECONDS=0.2 python producer/producer.py`.

The converters have unit tests: `pip install pytest && python -m pytest schema/tests`.

## Things to try

1. **Scale out the consumer group.** Start a second consumer with the same group
   id: `docker compose up -d --scale consumer=2` (first remove the
   `container_name: consumer` line), or run `python consumer/consumer.py`
   on your machine. Watch the `Assigned partitions` lines in the logs as Kafka
   rebalances the 3 partitions. What happens with 4 consumers?
2. **Consumer groups.** `elk-writer` and `protobuf-writer` both read every
   message, independently. Add a third group by running
   `KAFKA_GROUP_ID=my-group python consumer/consumer.py`. The new group gets
   its own copy of every message, starting from the beginning of the topic.
3. **Consumer lag.** Pause the ELK consumer in the [control panel](http://localhost:8090),
   wait a minute, and watch the lag of the `elk-writer` group grow. It's also
   in Kafka UI, or on the command line:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --describe --group elk-writer
   ```
   Resume it and watch it catch up. Then try stopping the container instead
   (`docker compose stop consumer`): the group becomes *Empty*, and when it
   starts again Kafka rebalances. Compare with `protobuf-writer`: its lag
   grows for up to a minute and then drops to 0, because it only commits after
   writing a file.
4. **Kafka CLI tools.** The broker image comes with the standard tools:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vehicle-telemetry
   docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic vehicle-telemetry --property print.key=true --property print.headers=true
   ```
   The values are binary protobuf, so they look garbled. That's the price of
   a compact format: you need the schema to read them (as Kafka UI does).
5. **Replay.** Reset the group's offsets to the beginning (stop the consumer first) and
   notice that Elasticsearch doesn't get duplicates, because documents are keyed by
   `message_id`:
   ```bash
   docker compose stop consumer
   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
     --group elk-writer --topic vehicle-telemetry --reset-offsets --to-earliest --execute
   docker compose start consumer
   ```
6. **Evolve the schema.** Follow [Changing the schema](#changing-the-schema) to add a
   field, then find it in Kibana, Kafka UI and Spark
   (`docker compose run --rm dbt show --inline "select Vehicle.Exterior.AirTemperature, count(*) from {{ telemetry_source() }} group by 1"`).
   What happens if you change the type of an existing field instead?
7. **Send invalid messages.** `python producer/send_invalid.py` publishes a few
   messages that break the schema in different ways (garbage bytes, an unknown
   enum value, a speed of 999, bad patterns, a body type that isn't allowed,
   missing required fields). Wait a
   minute for the protobuf consumer to write them to a file, then run
   `make solution`: the source tests fail, one per kind
   of problem, and the models are skipped. To start clean again, run
   `docker compose down -v` and delete `./data`.
8. **Break things.** Stop Logstash (`docker compose stop logstash`) and see the
   consumer retry without committing. Stop Kafka and see what the producer does.

## Notes

- This is a learning setup: Kafka runs as a single node in KRaft mode (no
  ZooKeeper), and Elasticsearch security is turned off. Don't use this
  configuration in production.
- If Elasticsearch fails to start on Linux with a `vm.max_map_count` error, run
  `sudo sysctl -w vm.max_map_count=262144`.
- Up to version 2.0 the messages were `vehicle.v1.VehicleTelemetry`, with
  flat fields like `speed_kmh`. The VSS-based `vehicle.v2` can't decode them,
  so after upgrading run `make clean` before `make up` to start without the
  old data.
