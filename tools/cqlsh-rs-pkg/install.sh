#!/bin/bash

set -e

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

root=/
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

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi

rprefix=$(realpath -m "$root/$prefix")
if ! $nonroot; then
    rusr="$root/usr"
fi

install -d -m755 "$rprefix"/share/cassandra/bin
if ! $nonroot; then
    install -d -m755 "$rusr"/bin
fi

# Install the cqlsh-rs binary
install -m755 bin/cqlsh-rs "$rprefix"/share/cassandra/bin/cqlsh-rs

# Create cqlsh symlink pointing to cqlsh-rs
ln -sf cqlsh-rs "$rprefix"/share/cassandra/bin/cqlsh

if ! $nonroot; then
    ln -srf "$rprefix"/share/cassandra/bin/cqlsh-rs "$rusr"/bin/cqlsh-rs
    ln -srf "$rprefix"/share/cassandra/bin/cqlsh "$rusr"/bin/cqlsh
fi
