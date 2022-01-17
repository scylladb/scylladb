#!/bin/bash
#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

set -e

if [ -z "$BASH_VERSION" ]; then
    echo "Unsupported shell, please run this script on bash."
    exit 1
fi

# change directory to the package's root directory
cd "$(dirname "$0")"

# just used for keep same code style with install.sh
root=/
prefix=`realpath -m "$(dirname "$0")"`

if [ ! -f "$prefix/SCYLLA-OFFLINE-FILE" ]; then
    echo "Does not found installed Scylla image at $prefix."
    exit 1
fi

if [ -f "$prefix/SCYLLA-NONROOT-FILE" ]; then
    nonroot=true
else
    nonroot=false
fi

rprefix=$(realpath -m "$root/$prefix")
if ! $nonroot; then
    # detect sysconfdir
    if [ -f /etc/default/scylla-server ]; then
        sysconfdir=/etc/default
    else
        sysconfdir=/etc/sysconfig
    fi
    retc="$root/etc"
    rsysconfdir="$root/$sysconfdir"
    rusr="$root/usr"
    rsystemd="$rusr/lib/systemd/system"
    rdoc="$rprefix/share/doc"
    rdata="$root/var/lib/scylla"
    rhkdata="$root/var/lib/scylla-housekeeping"
else
    sysconfdir=/etc/sysconfig
    retc="$rprefix/etc"
    rsysconfdir="$rprefix/$sysconfdir"
    rsystemd="$HOME/.config/systemd/user"
    rdoc="$rprefix/share/doc"
    rdata="$rprefix"
fi

rm -fv "$rsystemd"/{scylla-*.service,scylla-*.timer,scylla-*.slice}
if ! $nonroot; then
    rm -rfv "$retc"/systemd/system/scylla-*.service.d
    rm -fv "$retc"/bash_completion.d/nodetool-completion
    rm -fv "$retc"/supervisord.d/scylla-*.ini
    rm -fv "$rusr"/lib/sysctl.d/99-scylla-*.conf
    rm -fv "$rusr"/bin/{scylla,iotune,scyllatop}
    rm -fv "$rusr"/sbin/{scylla_*setup,node_exporter_install,node_health_check,scylla_ec2_check,scylla_kernel_check}
    find "$rusr"/lib/scylla -type l -exec rm -fv {} \;
else
    rm -rfv "$rsystemd"/scylla-*.service.d
fi

rm -rfv "$rprefix"

if ! $nonroot; then
    systemctl daemon-reload
else
    systemctl --user daemon-reload
fi

echo "Scylla uninstall completed."
