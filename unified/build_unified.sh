#!/bin/bash -e
#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

print_usage() {
    echo "build_unified.sh --mode <mode>"
    echo "  --mode specify mode (default: release)"
    echo "  --pkgs specify source packages"
    echo "  --unified-pkg specify package path (default: build/release/scylla-unified-package.tar.gz)"
    exit 1
}

# configure.py will run SCYLLA-VERSION-GEN prior to this case
# but just in case...
if [ ! -f build/SCYLLA-PRODUCT-FILE ]; then
    ./SCYLLA-VERSION-GEN
fi
PRODUCT=`cat build/SCYLLA-PRODUCT-FILE`
VERSION=`cat build/SCYLLA-VERSION-FILE`
VERSION_ESC=${VERSION//./\.}
RELEASE=`cat build/SCYLLA-RELEASE-FILE`
RELEASE_ESC=${RELEASE//./\.}

PKGS=
MODE="release"
UNIFIED_PKG="build/release/$PRODUCT-unified-$VERSION-$RELEASE.$(arch).tar.gz"
while [ $# -gt 0 ]; do
    case "$1" in
        "--mode")
            MODE="$2"
            shift 2
            ;;
        "--pkgs")
            PKGS="$2"
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
PKGS="build/$MODE/dist/tar/$PRODUCT-$VERSION-$RELEASE.$(arch).tar.gz build/$MODE/dist/tar/$PRODUCT-python3-$VERSION-$RELEASE.$(arch).tar.gz build/$MODE/dist/tar/$PRODUCT-jmx-$VERSION-$RELEASE.noarch.tar.gz build/$MODE/dist/tar/$PRODUCT-tools-$VERSION-$RELEASE.noarch.tar.gz"

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
    dirname=$(basename "$pkg"| sed -e "s/-$VERSION_ESC-$RELEASE_ESC\.[^.]*\.tar\.gz//")
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
