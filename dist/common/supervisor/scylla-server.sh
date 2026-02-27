#!/bin/bash

. $(dirname "$0")/scylla_util.sh

. "$sysconfdir"/scylla-server

for f in "$etcdir"/scylla.d/*.conf; do
    . "$f"
done

if is_privileged; then
    "$scriptsdir"/scylla_prepare
fi
execsudo /usr/bin/env SCYLLA_HOME=$SCYLLA_HOME SCYLLA_CONF=$SCYLLA_CONF "$bindir"/scylla $SCYLLA_ARGS $SEASTAR_IO $DEV_MODE $CPUSET $SCYLLA_DOCKER_ARGS
