#!/bin/bash -e

print_usage() {
    echo "build_docker.sh --mode release --unified-pkg build/release/scylla-unified.tar.gz"
    echo "  --mode scylla build mode"
    echo "  --unified-pkg specify package path"
    echo "  --with-busybox build container with busybox"
    exit 1
}

PRODUCT=`cat build/SCYLLA-PRODUCT-FILE`
VERSION=`sed 's/-/~/' build/SCYLLA-VERSION-FILE`
RELEASE=`cat build/SCYLLA-RELEASE-FILE`

BASE_IMAGE="gcr.io/distroless/java-debian11:nonroot"
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
        "--with-busybox")
            BASE_IMAGE="gcr.io/distroless/java-debian11:debug-nonroot"
            shift 1
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ -z "$UNIFIED_PKG" ]; then
    UNIFIED_PKG="build/$MODE/dist/tar/$PRODUCT-unified-$VERSION-$RELEASE.$(arch).tar.gz"
fi
BUILDDIR=build/"$MODE"/dist/docker-minimal/scylla-jmx

if [ ! -e $UNIFIED_PKG ]; then
    echo "unified package is not specified."
    exit 1
fi

rm -rf "$BUILDDIR"
mkdir -p "$BUILDDIR"
tar xpf "$UNIFIED_PKG" -C "$BUILDDIR"
TOPDIR=`pwd`
cd "$BUILDDIR"
cd scylla-*/scylla-jmx
./install.sh --packaging --root ../../scylla-root --sysconfdir /etc/default
cd -

# XXX: these fixup should be move to install.sh
rm -rf scylla-root/{etc,usr} scylla-root/opt/scylladb/jmx/{scylla-jmx,symlinks} scylla-root/opt/scylladb/scripts

CONTAINER="$(buildah from $BASE_IMAGE)"
IMAGE="$PRODUCT-minimal-jmx-$VERSION-$RELEASE"
buildah copy "$CONTAINER" scylla-root/opt/ /opt/

buildah config --entrypoint '["/usr/bin/java", "-Xmx256m", "-XX:+UseSerialGC", "-XX:+HeapDumpOnOutOfMemoryError", "-Dcom.sun.management.jmxremote.authenticate=false", "-Dcom.sun.management.jmxremote.ssl=false", "-Dcom.sun.management.jmxremote.host=localhost", "-Dcom.sun.management.jmxremote", "-Dcom.sun.management.jmxremote.port=7199", "-Djava.rmi.server.hostname=localhost", "-Dcom.sun.management.jmxremote.rmi.port=7199", "-Djavax.management.builder.initial=com.scylladb.jmx.utils.APIBuilder", "-jar", "/opt/scylladb/jmx/scylla-jmx-1.0.jar"]' "$CONTAINER"
buildah config --cmd "" "$CONTAINER"
buildah config --port 7199 "$CONTAINER"
buildah commit "$CONTAINER" "oci-archive:$IMAGE"
echo "Image is now available in $BUILDDIR/$IMAGE."
