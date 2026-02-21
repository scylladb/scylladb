#!/bin/bash -e
#
# Copyright (C) 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

trap 'echo "error $? in $0 line $LINENO"' ERR

version_ge() {
    [  "$2" = "`echo -e "$1\n$2" | sort -V | head -n1`" ]
}

KERNEL_VER=$(uname -r)

if ! version_ge $KERNEL_VER 5.8; then
    # On older kernel environment, we have to relax perf_event_paranoid setting
    # since there is no CAP_PERFMON.
    PERF_EVENT_PARANOID=1
elif [ $(cat /proc/sys/kernel/perf_event_paranoid) -ge 3  ]; then
    # On Debian/Ubuntu, it deny access from non-root even with CAP_PERFMON
    # It requires to set perf_event_paranoid=2 to use CAP_PERFMON with non-root
    PERF_EVENT_PARANOID=2
fi
if [ -n "$PERF_EVENT_PARANOID" ]; then
    cat << EOS > /etc/sysctl.d/99-scylla-perfevent.conf
kernel.perf_event_paranoid = $PERF_EVENT_PARANOID
EOS
    sysctl -p /etc/sysctl.d/99-scylla-perfevent.conf
fi

# Tune tcp_mem to max out at 3% of total system memory.
# Seastar defaults to allocating 93% of physical memory. The kernel's default
# allocation for TCP is ~9%. This adds up to 102%. Reduce the TCP allocation
# to 3% to avoid OOM.
PAGE_SIZE=$(getconf PAGE_SIZE)
TOTAL_MEM_KB=$(sed -n 's/^MemTotal:[[:space:]]*\([0-9]*\).*/\1/p' /proc/meminfo)
TOTAL_MEM_BYTES=$((TOTAL_MEM_KB * 1024))
TCP_MEM_MAX=$((TOTAL_MEM_BYTES * 3 / 100))
TCP_MEM_MAX_PAGES=$((TCP_MEM_MAX / PAGE_SIZE))
TCP_MEM_MID_PAGES=$((TCP_MEM_MAX * 2 / 3 / PAGE_SIZE))
TCP_MEM_MIN_PAGES=$((TCP_MEM_MAX / 2 / PAGE_SIZE))
cat << EOS > /etc/sysctl.d/99-scylla-tcp.conf
# Scylla: limit TCP memory to 3% of total system memory
net.ipv4.tcp_mem = $TCP_MEM_MIN_PAGES $TCP_MEM_MID_PAGES $TCP_MEM_MAX_PAGES
EOS
sysctl -p /etc/sysctl.d/99-scylla-tcp.conf || :

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

systemctl --system daemon-reload >/dev/null || true
systemctl --system enable --now scylla-tune-sched.service || true
