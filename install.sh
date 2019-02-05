#!/bin/bash
#
# Copyright (C) 2018 ScyllaDB
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

print_usage() {
    cat <<EOF
Usage: install.sh [options]

Options:
  --root /path/to/root     alternative install root (default /)
  --prefix /prefix         directory prefix (default /usr)
  --python3 /opt/python3   path of the python3 interpreter relative to install root (default /opt/scylladb/python3/bin/python3)
  --housekeeping           enable housekeeping service
  --target centos          specify target distribution
  --help                   this helpful message
EOF
    exit 1
}

root=/
prefix=/usr
housekeeping=false
target=centos
python3=/opt/scylladb/python3/bin/python3

while [ $# -gt 0 ]; do
    case "$1" in
        "--root")
            root="$2"
            shift 2
            ;;
        "--prefix")
            prefix="$2"
            shift 2
            ;;
        "--housekeeping")
            housekeeping=true
            shift 1
            ;;
        "--target")
            target="$2"
            shift 2
            ;;
        "--python3")
            python3="$2"
            shift 2
            ;;
        "--help")
            shift 1
	    print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done

rprefix="$root/$prefix"
retc="$root/etc"
rdoc="$rprefix/share/doc"

MUSTACHE_DIST="\"redhat\": true, \"$target\": true, \"target\": \"$target\""
mkdir -p build
pystache dist/common/systemd/scylla-server.service.mustache "{ $MUSTACHE_DIST }" > build/scylla-server.service
pystache dist/common/systemd/scylla-housekeeping-daily.service.mustache "{ $MUSTACHE_DIST }" > build/scylla-housekeeping-daily.service
pystache dist/common/systemd/scylla-housekeeping-restart.service.mustache "{ $MUSTACHE_DIST }" > build/scylla-housekeeping-restart.service


install -m755 -d "$retc/sysconfig" "$retc/security/limits.d"
install -m755 -d "$retc/scylla.d" "$rprefix/lib/sysctl.d"
install -m644 dist/common/sysconfig/scylla-server -Dt "$retc"/sysconfig
install -m644 dist/common/limits.d/scylla.conf -Dt "$retc"/security/limits.d
install -m644 dist/common/scylla.d/*.conf -Dt "$retc"/scylla.d
install -m644 dist/common/sysctl.d/*.conf -Dt "$rprefix"/lib/sysctl.d

SYSCONFDIR="/etc/sysconfig"
REPOFILES="'/etc/yum.repos.d/scylla*.repo'"


install -d -m755 "$retc"/scylla "$rprefix/lib/systemd/system" "$rprefix/lib/scylla" "$rprefix/bin" "$root/opt/scylladb/bin" "$root/opt/scylladb/libexec" "$root/opt/scylladb/libreloc"
install -m644 conf/scylla.yaml -Dt "$retc"/scylla
install -m644 conf/cassandra-rackdc.properties -Dt "$retc"/scylla
install -m644 build/*.service -Dt "$rprefix"/lib/systemd/system
install -m644 dist/common/systemd/*.service -Dt "$rprefix"/lib/systemd/system
install -m644 dist/common/systemd/*.timer -Dt "$rprefix"/lib/systemd/system
install -m755 seastar/scripts/seastar-cpu-map.sh -Dt "$rprefix"/lib/scylla/
install -m755 seastar/dpdk/usertools/dpdk-devbind.py -Dt "$rprefix"/lib/scylla/
install -m755 bin/* -Dt "$root/opt/scylladb/bin"
# some files in libexec are symlinks, which "install" dereferences
# use cp -P for the symlinks instead.
install -m755 libexec/*.bin -Dt "$root/opt/scylladb/libexec"
for f in libexec/*; do
    if [[ "$f" != *.bin ]]; then
        cp -P "$f" "$root/opt/scylladb/libexec"
    fi
done
install -m755 libreloc/* -Dt "$root/opt/scylladb/libreloc"
ln -srf "$root/opt/scylladb/bin/scylla" "$rprefix/bin/scylla"
ln -srf "$root/opt/scylladb/bin/iotune" "$rprefix/bin/iotune"
ln -srf "$rprefix/lib/scylla/scyllatop/scyllatop.py" "$rprefix/bin/scyllatop"

if $housekeeping; then
    install -m644 conf/housekeeping.cfg -Dt "$retc"/scylla.d
fi
install -d -m755 "$rdoc"/scylla
install -m644 README.md -Dt "$rdoc"/scylla/
install -m644 README-DPDK.md -Dt "$rdoc"/scylla
install -m644 NOTICE.txt -Dt "$rdoc"/scylla/
install -m644 ORIGIN -Dt "$rdoc"/scylla/
install -d -m755 -d "$rdoc"/scylla/licenses/
install -m644 licenses/* -Dt "$rdoc"/scylla/licenses/
install -m755 -d "$root"/var/lib/scylla/
install -m755 -d "$root"/var/lib/scylla/data
install -m755 -d "$root"/var/lib/scylla/commitlog
install -m755 -d "$root"/var/lib/scylla/hints
install -m755 -d "$root"/var/lib/scylla/view_hints
install -m755 -d "$root"/var/lib/scylla/coredump
install -m755 -d "$root"/var/lib/scylla-housekeeping
install -m755 -d "$rprefix"/lib/scylla/swagger-ui
cp -r swagger-ui/dist "$rprefix"/lib/scylla/swagger-ui
install -d -m755 -d "$rprefix"/lib/scylla/api
cp -r api/api-doc "$rprefix"/lib/scylla/api
cp -r tools/scyllatop "$rprefix"/lib/scylla/scyllatop
install -d "$rprefix"/sbin
cp -P dist/common/sbin/* "$rprefix"/sbin
install -m755 scylla-gdb.py -Dt "$rprefix"/lib/scylla/

find ./dist/common/scripts -type f -exec ./relocate_python_scripts.py \
            --installroot $rprefix/lib/scylla/ --with-python3 "$root/$python3" {} +

./relocate_python_scripts.py \
            --installroot $rprefix/lib/scylla/ --with-python3 "$root/$python3" \
            seastar/scripts/perftune.py seastar/scripts/seastar-addr2line seastar/scripts/perftune.py

./relocate_python_scripts.py \
            --installroot $rprefix/lib/scylla/scyllatop/ --with-python3 "$root/$python3" \
            tools/scyllatop/scyllatop.py
