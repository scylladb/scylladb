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

print_usage() {
    cat <<EOF
Usage: install.sh [options]

Options:
  --root /path/to/root     alternative install root (default /)
  --prefix /prefix         directory prefix (default /usr)
  --python3 /opt/python3   path of the python3 interpreter relative to install root (default /opt/scylladb/python3/bin/python3)
  --housekeeping           enable housekeeping service
  --nonroot                install Scylla without required root priviledge
  --sysconfdir /etc/sysconfig   specify sysconfig directory name
  --help                   this helpful message
EOF
    exit 1
}

root=/
housekeeping=false
python3=/opt/scylladb/python3/bin/python3
sysconfdir=/etc/sysconfig
nonroot=false

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
        "--python3")
            python3="$2"
            shift 2
            ;;
        "--nonroot")
            nonroot=true
            shift 1
            ;;
        "--sysconfdir")
            sysconfdir="$2"
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

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi
rprefix=$(realpath -m "$root/$prefix")

scylla_args=()
args=()

if $housekeeping; then
    scylla_args+=(--housekeeping)
fi
if $nonroot; then
    scylla_args+=(--nonroot)
    args+=(--nonroot)
fi

(cd $(readlink -f scylla); ./install.sh --root "$root" --prefix "$prefix" --python3 "$python3" --sysconfdir "$sysconfdir" ${scylla_args[@]})

(cd $(readlink -f scylla-python3); ./install.sh --root "$root" --prefix "$prefix" ${args[@]})

(cd $(readlink -f scylla-jmx); ./install.sh --root "$root" --prefix "$prefix"  --sysconfdir "$sysconfdir" ${args[@]})

(cd $(readlink -f scylla-tools); ./install.sh --root "$root" --prefix "$prefix" ${args[@]})

install -m755 uninstall.sh -Dt "$rprefix"
