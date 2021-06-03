#!/bin/bash
#
# Copyright (C) 2020-present ScyllaDB
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

if [ -z "$BASH_VERSION" ]; then
    echo "Unsupported shell, please run this script on bash."
    exit 1
fi

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

check_usermode_support() {
    user=$(systemctl --help|grep -e '--user')
    [ -n "$user" ]
}

root=/
housekeeping=false
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

if [ ! -d /run/systemd/system/ ]; then
    echo "systemd is not detected, unsupported distribution."
    exit 1
fi

has_java=false
if [ -x /usr/bin/java ]; then
    javaver=$(/usr/bin/java -version 2>&1|head -n1|cut -f 3 -d " ")
    if [[ "$javaver" =~ ^\"1.8.0 || "$javaver" =~ ^\"11.0. ]]; then
        has_java=true
    fi
fi
if ! $has_java; then
    echo "Please install openjdk-8 or openjdk-11 before running install.sh."
    exit 1
fi

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi
rprefix=$(realpath -m "$root/$prefix")

if [ -f "/etc/os-release" ]; then
    . /etc/os-release
fi

if [ -z "$sysconfdir" ]; then
    sysconfdir=/etc/sysconfig
    if ! $nonroot; then
        if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
            sysconfdir=/etc/default
        fi
    fi
fi

if [ -z "$python3" ]; then
    python3=$prefix/python3/bin/python3
fi

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

if $nonroot && ! check_usermode_support; then
    echo "WARNING: This distribution does not support systemd user mode, please configure and launch Scylla manually."
fi
