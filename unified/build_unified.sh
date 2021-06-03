#!/bin/bash -e
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

print_usage() {
    echo "build_unified.sh --mode <mode>"
    echo "  --mode specify mode (default: release)"
    echo "  --unified-pkg specify package path (default: build/release/scylla-unified-package.tar.gz)"
    exit 1
}

# configure.py will run SCYLLA-VERSION-GEN prior to this case
# but just in case...
if [ ! -f build/SCYLLA-PRODUCT-FILE ]; then
    ./SCYLLA-VERSION-GEN
fi
PRODUCT=`cat build/SCYLLA-PRODUCT-FILE`

MODE="release"
UNIFIED_PKG="build/release/$PRODUCT-unified-package.tar.gz"
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
PKGS="build/$MODE/dist/tar/$PRODUCT-package.tar.gz build/$MODE/dist/tar/$PRODUCT-python3-package.tar.gz build/$MODE/dist/tar/$PRODUCT-jmx-package.tar.gz build/$MODE/dist/tar/$PRODUCT-tools-package.tar.gz"

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
    dirname=${dirname/#$PRODUCT/scylla}
    if [ ! -d build/"$MODE"/unified/"$dirname" ]; then
        echo "Directory $dirname not found in $pkg, the pacakge may corrupted."
        exit 1
    fi
done
ln -f unified/install.sh build/"$MODE"/unified/
ln -f unified/uninstall.sh build/"$MODE"/unified/
cd build/"$MODE"/unified
tar cpf "$UNIFIED_PKG" --use-compress-program=pigz * .relocatable_package_version
