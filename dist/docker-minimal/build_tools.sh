#!/bin/bash -e

print_usage() {
    echo "build_docker.sh --mode release --unified-pkg build/release/scylla-unified.tar.gz"
    echo "  --mode scylla build mode"
    echo "  --unified-pkg specify package path"
    exit 1
}

PRODUCT=`cat build/SCYLLA-PRODUCT-FILE`
VERSION=`sed 's/-/~/' build/SCYLLA-VERSION-FILE`
RELEASE=`cat build/SCYLLA-RELEASE-FILE`

BASE_IMAGE="gcr.io/distroless/java-debian11:debug-nonroot"
MODE="release"

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

if [ -z "$UNIFIED_PKG" ]; then
    UNIFIED_PKG="build/$MODE/dist/tar/$PRODUCT-unified-$VERSION-$RELEASE.$(arch).tar.gz"
fi
BUILDDIR=build/"$MODE"/dist/docker-minimal/scylla-tools

if [ ! -e $UNIFIED_PKG ]; then
    echo "unified package is not specified."
    exit 1
fi

rm -rf "$BUILDDIR"
mkdir -p "$BUILDDIR"
tar xpf "$UNIFIED_PKG" -C "$BUILDDIR"
TOPDIR=`pwd`
cd "$BUILDDIR"
cd scylla-*/scylla-tools
./install.sh --root ../../scylla-root
cd -

# XXX: these fixup should be move to install.sh
for i in scylla-root/usr/bin/*;do
    if [ -f $i ]; then
        sed -i -e '1 s#\#!/usr/bin/env bash#\#!/busybox/sh#' $i
    fi
done
for i in scylla-root/opt/scylladb/share/cassandra/bin/*;do
    if [ -f $i ]; then
        sed -i -e '1 s#\#!/bin/sh#\#!/busybox/sh#' $i
        sed -i -e '1 s#\#!/usr/bin/env bash#\#!/busybox/sh#' $i
    fi
done

CONTAINER="$(buildah from $BASE_IMAGE)"
IMAGE="$PRODUCT-minimal-tools-$VERSION-$RELEASE"
buildah copy "$CONTAINER" scylla-root/etc/ /etc/
buildah copy "$CONTAINER" scylla-root/usr/ /usr/
buildah copy "$CONTAINER" scylla-root/opt/ /opt/

buildah config --entrypoint '["/busybox/sh"]' "$CONTAINER"
buildah config --cmd "" "$CONTAINER"
buildah commit "$CONTAINER" "oci-archive:$IMAGE"
echo "Image is now available in $BUILDDIR/$IMAGE."
