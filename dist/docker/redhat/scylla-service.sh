#!/bin/bash

/usr/lib/scylla/scylla_prepare

. /etc/sysconfig/scylla-server

export SCYLLA_HOME SCYLLA_CONF

for f in /etc/scylla.d/*.conf; do
    . "$f"
done

exec /usr/bin/scylla $SCYLLA_ARGS $SEASTAR_IO $DEV_MODE $CPUSET $SCYLLA_DOCKER_ARGS
