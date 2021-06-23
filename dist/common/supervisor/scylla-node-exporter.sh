#!/bin/bash

. $(dirname "$0")/scylla_util.sh

. "$sysconfdir"/scylla-node-exporter

exec "$scylladir"/node_exporter/node_exporter $SCYLLA_NODE_EXPORTER_ARGS
