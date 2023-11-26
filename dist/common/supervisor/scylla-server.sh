#!/bin/bash

. $(dirname "$0")/scylla_util.sh

. "$sysconfdir"/scylla-server


if is_privileged; then
    "$scriptsdir"/scylla_prepare
fi
execsudo /usr/bin/env SCYLLA_CONF=/etc/scylla SEASTAR_CONF_FILE=/etc/scylla.d/seastar.conf "$bindir"/scylla $SCYLLA_ARGS
