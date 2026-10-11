#!/bin/sh
# Installs the dbt source tests generated from the .proto files (while the
# image was built) into the mounted dbt project, then runs dbt.
set -e
mkdir -p /dbt/src/sources
cp /opt/generated-dbt-sources/*.yml /dbt/src/sources/
exec dbt "$@"
