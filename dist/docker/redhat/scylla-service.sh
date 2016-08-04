#!/bin/bash

. /usr/lib/scylla/scylla_prepare

export SCYLLA_HOME SCYLLA_CONF

/usr/bin/scylla $SCYLLA_ARGS $SEASTAR_IO $DEV_MODE $CPUSET
