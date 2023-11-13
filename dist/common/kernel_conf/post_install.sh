#!/bin/bash
#
# Copyright (C) 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

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

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

systemctl --system daemon-reload >/dev/null || true
systemctl --system enable --now scylla-tune-sched.service || true
