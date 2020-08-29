#!/bin/bash -e
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

print_usage() {
    echo "build_unified.sh --mode <mode>"
    echo "  --mode specify mode (default: release)"
    echo "  --unified-pkg specify package path (default: build/release/scylla-unified-package.tar.gz)"
    exit 1
}

MODE="release"
UNIFIED_PKG="build/release/scylla-unified-package.tar.gz"
while [ $# -gt 0 ]; do
    case "$1" in
        "--mode")
            MODE="$2"
            shift 2
            ;;
        "--unified-pkg")
            UNIFIED_PKG="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

UNIFIED_PKG="$(realpath -s $UNIFIED_PKG)"
PKGS="build/$MODE/scylla-package.tar.gz build/$MODE/scylla-python3-package.tar.gz tools/jmx/build/scylla-jmx-package.tar.gz tools/java/build/scylla-tools-package.tar.gz"

rm -rf build/"$MODE"/unified/
mkdir -p build/"$MODE"/unified/
for pkg in $PKGS; do
    if [ ! -e "$pkg" ]; then
        echo "$pkg not found."
        echo "please build relocatable package before building unified package."
        exit 1
    fi
    pkg="$(readlink -f $pkg)"
    tar -C build/"$MODE"/unified/ -xpf "$pkg"
    dirname=$(basename "$pkg"| sed -e "s/-package.tar.gz//")
    if [ ! -d build/"$MODE"/unified/"$dirname" ]; then
        echo "Directory $dirname not found in $pkg, the pacakge may corrupted."
        exit 1
    fi
done
ln -f unified/install.sh build/"$MODE"/unified/
ln -f unified/uninstall.sh build/"$MODE"/unified/
cd build/"$MODE"/unified
tar czvpf "$UNIFIED_PKG" * .relocatable_package_version
