#!/bin/bash
#
# Copyright (C) 2022-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
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
  --nonroot                install Scylla without required root privilege
  --help                   this helpful message
EOF
    exit 1
}

# Some words about pathnames in this script.
#
# A pathname has three components: "$root/$prefix/$rest".
#
# $root is used to point at the entire installed hierarchy, so you can perform
# an install to a temporary directory without modifying your system, with the intent
# that the files are copied later to the real /. So, if "$root"="/tmp/xyz", you'd get
# a standard hierarchy under /tmp/xyz: /tmp/xyz/etc, /tmp/xyz/var, and 
# /tmp/xyz/opt/scylladb/bin/scylla. This is used by rpmbuild --root to create a filesystem
# image to package.
#
# When this script creates a file, it must use "$root" to refer to the file. When this
# script inserts a file name into a file, it must not use "$root", because in the installed
# system "$root" is stripped out. Example:
#
#    echo "This file's name is /a/b/c. > "$root/a/b/c"
#
# The second component is "$prefix". It is used by non-root install to place files into
# a directory of the user's choice (typically somewhere under their home directory). In theory
# all files should be always under "$prefix", but in practice /etc files are not under "$prefix"
# for standard installs (we use /etc not /usr/etc) and are under "$prefix" for non-root installs.
# Another exception is files that go under /opt/scylladb in a standard install go under "$prefix"
# for a non-root install.
#
# The last component is the rest of the file name, which doesn't matter for this script and
# isn't changed by it.

root=/
nonroot=false

while [ $# -gt 0 ]; do
    case "$1" in
        "--root")
            root="$(realpath "$2")"
            shift 2
            ;;
        "--prefix")
            prefix="$2"
            shift 2
            ;;
        "--nonroot")
            nonroot=true
            shift 1
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

# change directory to the package's root directory
cd "$(dirname "$0")"

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi

rprefix=$(realpath -m "$root/$prefix")

install -d -m755 "$rprefix"/libexec/.debug
cp -r ./libexec/.debug/* "$rprefix"/libexec/.debug
install -d -m755 "$rprefix"/node_exporter/.debug
cp -r ./node_exporter/.debug/* "$rprefix"/node_exporter/.debug
