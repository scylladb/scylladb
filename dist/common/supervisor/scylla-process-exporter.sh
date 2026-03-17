#!/bin/bash

. "$(dirname -- "$0")/scylla_util.sh"

# scylladir is defined by scylla_util.sh
# shellcheck disable=SC2154
exec "$scylladir"/dependencies/process-exporter -config.path "$scylladir"/dependencies/process-exporter.yml
