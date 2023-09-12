#!/bin/bash -e
#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

print_usage() {
    echo "build_unified.sh --build-dir <build_dir>"
    echo "  --build-dir specify build directory (default: build/release)"
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
VERSION=`sed 's/-/~/' build/SCYLLA-VERSION-FILE`
VERSION_ESC=${VERSION//./\.}
RELEASE=`cat build/SCYLLA-RELEASE-FILE`
RELEASE_ESC=${RELEASE//./\.}

PKGS=
BUILD_DIR="build/release"
UNIFIED_PKG=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--build-dir")
            BUILD_DIR="$2"
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

if [ -z "$UNIFIED_PKG" ]; then
    UNIFIED_PKG="$BUILD_DIR/$PRODUCT-unified-$VERSION-$RELEASE.$(arch).tar.gz"
fi
UNIFIED_PKG="$(realpath -s $UNIFIED_PKG)"
PKGS="$BUILD_DIR/dist/tar/$PRODUCT-$VERSION-$RELEASE.$(arch).tar.gz $BUILD_DIR/dist/tar/$PRODUCT-python3-$VERSION-$RELEASE.$(arch).tar.gz $BUILD_DIR/dist/tar/$PRODUCT-jmx-$VERSION-$RELEASE.noarch.tar.gz $BUILD_DIR/dist/tar/$PRODUCT-tools-$VERSION-$RELEASE.noarch.tar.gz $BUILD_DIR/dist/tar/$PRODUCT-cqlsh-$VERSION-$RELEASE.noarch.tar.gz"
BASEDIR="$BUILD_DIR/unified/$PRODUCT-$VERSION"

rm -rf $BUILD_DIR/unified/
mkdir -p "$BASEDIR"
for pkg in $PKGS; do
    if [ ! -e "$pkg" ]; then
        echo "$pkg not found."
        echo "please build relocatable package before building unified package."
        exit 1
    fi
    pkg="$(readlink -f $pkg)"
    tar -C "$BASEDIR" -xpf "$pkg"
    dirname=$(basename "$pkg"| sed -e "s/-$VERSION_ESC-$RELEASE_ESC\.[^.]*\.tar\.gz//")
    dirname=${dirname/#$PRODUCT/scylla}
    if [ ! -d "$BASEDIR/$dirname" ]; then
        echo "Directory $dirname not found in $pkg, the pacakge may corrupted."
        exit 1
    fi
done
ln -f unified/install.sh "$BASEDIR"
ln -f unified/uninstall.sh "$BASEDIR"
# relocatable package format version = 3.0
echo "3.0" > "$BASEDIR"/.relocatable_package_version
cd $BUILD_DIR/unified
tar cpf "$UNIFIED_PKG" --use-compress-program=pigz "$PRODUCT-$VERSION"
