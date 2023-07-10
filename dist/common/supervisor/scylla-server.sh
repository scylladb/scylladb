#!/bin/bash

. $(dirname "$0")/scylla_util.sh

. "$sysconfdir"/scylla-server

for f in "$etcdir"/scylla.d/*.conf; do
    . "$f"
done

if is_privileged; then
    "$scriptsdir"/scylla_prepare
fi
execsudo /usr/bin/env SCYLLA_HOME=/var/lib/scylla SCYLLA_CONF=/etc/scylla "$bindir"/scylla $SCYLLA_ARGS $SEASTAR_IO $DEV_MODE $CPUSET $SCYLLA_DOCKER_ARGS
