#!/bin/bash
#
# Copyright (C) 2019-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

version_ge() {
    [  "$2" = "`echo -e "$1\n$2" | sort -V | head -n1`" ]
}

# Install capabilities.conf when AmbientCapabilities supported
. /etc/os-release

KERNEL_VER=$(uname -r)

# CAP_PERFMON is only available on linux-5.8+
if version_ge $KERNEL_VER 5.8; then
    AMB_CAPABILITIES="$AMB_CAPABILITIES CAP_PERFMON"
    mkdir -p /etc/systemd/system/scylla-server.service.d/
    cat << EOS > /etc/systemd/system/scylla-server.service.d/capabilities.conf
[Service]
AmbientCapabilities=CAP_PERFMON
EOS
fi

# For systems with not a lot of memory, override default reservations for the slices
# seastar has a minimum reservation of 1.5GB that kicks in, and 21GB * 0.07 = 1.5GB.
# So for anything smaller than that we will not use percentages in the helper slice
MEMTOTAL=$(cat /proc/meminfo |grep -e "^MemTotal:"|sed -s 's/^MemTotal:\s*\([0-9]*\) kB$/\1/')
MEMTOTAL_BYTES=$(($MEMTOTAL * 1024))
if [ $MEMTOTAL_BYTES -lt 23008753371 ]; then
    mkdir -p /etc/systemd/system/scylla-helper.slice.d/
    cat << EOS > /etc/systemd/system/scylla-helper.slice.d/memory.conf
[Slice]
MemoryHigh=1200M
MemoryMax=1400M
EOS
fi

if [ -e /etc/systemd/system/systemd-coredump@.service.d/timeout.conf ]; then
    COREDUMP_RUNTIME_MAX=$(grep RuntimeMaxSec /etc/systemd/system/systemd-coredump@.service.d/timeout.conf)
    if [ -z $COREDUMP_RUNTIME_MAX ]; then
    cat << EOS > /etc/systemd/system/systemd-coredump@.service.d/timeout.conf
[Service]
RuntimeMaxSec=infinity
TimeoutSec=infinity
EOS
    fi
fi

systemctl --system daemon-reload >/dev/null || true
