#!/bin/bash
#
# Copyright (C) 2020 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

set -e

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

# scylla-kernel-conf
rm -fv "$rusr"/lib/sysctl.d/99-scylla-*.conf

# scylla-server
rm -fv "$rsystemd"/{scylla-server.*,scylla-fstrim.*,scylla-helper.service,scylla-housekeeping-*.*,node-exporter.service}
rm -rfv "$rprefix"/libreloc
rm -rfv "$rprefix"/libexec
rm -rfv "$rdoc"
rm -rfv "$rprefix"/swagger-ui
rm -rfv "$rprefix"/api
rm -rfv "$rprefix"/scyllatop
rm -rfv "$rprefix"/scripts
rm -rfv "$rprefix"/bin
if ! $nonroot; then
    rm -rfv "$retc"/systemd/system/scylla-server.service.d
    rm -rfv "$retc"/systemd/system/scylla-housekeeping-*.service.d
    rm -fv "$retc"/security/limits.d/scylla.conf
    rm -fv "$rusr"/bin/scylla
    rm -fv "$rusr"/bin/iotune
    rm -fv "$rusr"/bin/scyllatop
    rm -fv "$rusr"/sbin/{scylla_*setup,node_exporter_install,node_health_check,scylla_ec2_check,scylla_kernel_check}
    find "$rusr"/lib/scylla -type l -exec rm -fv {} \;
    rm -rfv "$rusr"/lib/scylla/scyllatop
else
    rm -rfv "$rprefix"/sbin
    rm -rfv "$rsystemd"/scylla-server.service.d
    rm -rfv "$rsystemd"/node-exporter.service.d
fi

# scylla-python3
rm -rfv "$rprefix"/python3

# scylla-jmx
rm -fv "$rsystemd"/scylla-jmx.service
if ! $nonroot; then
    rm -rfv "$retc"/systemd/system/scylla-jmx.service.d
else
    rm -rfv "$rsystemd"/scylla-jmx.service.d
fi
rm -rfv "$rprefix"/jmx
if ! $nonroot; then
    rm -rfv "$rusr"/lib/scylla/jmx
fi

# scylla-tools-core
rm -rfv "$retc"/scylla/cassandra
rm -rfv "$rprefix"/share/cassandra/lib
rm -rfv "$rprefix"/share/cassandra/doc
rm -rfv "$rprefix"/share/cassandra/bin/cassandra.in.sh

# scylla-tools
rm -rfv "$rprefix"/share/cassandra/pylib
rm -fv "$retc"/bash_completion.d/nodetool-completion
rm -fv "$rprefix"/share/cassandra/bin/{nodetool,sstableloader,cqlsh,cqlsh.py,scylla-sstableloader,cassandra-stress,cassandra-stressd,sstabledump,sstablelevelreset,sstablemetadata,sstablerepairedset}
if ! $nonroot; then
    rm -fv "$rusr"/bin/{nodetool,sstableloader,cqlsh,cqlsh.py,scylla-sstableloader,cassandra-stress,cassandra-stressd,sstabledump,sstablelevelreset,sstablemetadata,sstablerepairedset}
fi
rm -fdv "$rprefix"/share/cassandra/bin
rm -fdv "$rprefix"/share/cassandra

rm -fdv "$rprefix"/share
rm -fv SCYLLA-*-FILE
rm -fv "$0"
if ! $nonroot; then
    systemctl daemon-reload
else
    systemctl --user daemon-reload
fi

echo "Scylla uninstall completed."
