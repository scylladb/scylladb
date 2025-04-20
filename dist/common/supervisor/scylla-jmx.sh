#!/bin/bash

. $(dirname "$0")/scylla_util.sh

. "$sysconfdir"/scylla-jmx

exec "$scylladir"/jmx/scylla-jmx $SCYLLA_JMX_PORT $SCYLLA_API_PORT $SCYLLA_API_ADDR $SCYLLA_JMX_ADDR $SCYLLA_JMX_FILE $SCYLLA_JMX_LOCAL $SCYLLA_JMX_REMOTE $SCYLLA_JMX_DEBUG
