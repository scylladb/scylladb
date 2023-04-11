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

BASE_IMAGE="gcr.io/distroless/static-debian11"
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
            BASE_IMAGE="gcr.io/distroless/static-debian11:debug"
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
BUILDDIR=build/"$MODE"/dist/docker-minimal/scylla-server

if [ ! -e $UNIFIED_PKG ]; then
    echo "unified package is not specified."
    exit 1
fi

rm -rf "$BUILDDIR"
mkdir -p "$BUILDDIR"
tar xpf "$UNIFIED_PKG" -C "$BUILDDIR"
TOPDIR=`pwd`
cd "$BUILDDIR"
cd scylla-*/scylla
./install.sh --packaging --root ../../scylla-root --sysconfdir /etc/default
cd -
cd scylla-*/scylla-python3
./install.sh --root ../../scylla-root
cd -

# XXX: these fixup should be move to install.sh
rm -rf scylla-root/etc/systemd scylla-root/usr scylla-root/opt/scylladb/{api,bin,node_exporter,scyllatop,share,swagger-ui} scylla-root/opt/scylladb/libexec/{ethtool,gawk,gzip,hwloc-*,ifconfig,lsblk,lscpu,netstat,patchelf}
mkdir scylla-root/opt/scylladb/docker-minimal
install -m755 "$TOPDIR"/dist/docker/docker-entrypoint.py scylla-root/opt/scylladb/docker-minimal
install -m644 "$TOPDIR"/dist/docker/commandlineparser.py "$TOPDIR"/dist/docker/scyllasetup.py "$TOPDIR"/dist/docker/minimalcontainer.py scylla-root/opt/scylladb/docker-minimal
touch scylla-root/opt/scylladb/SCYLLA-MINIMAL-CONTAINER-FILE
sed -i -e 's/^SCYLLA_ARGS=".*"$/SCYLLA_ARGS="--log-to-syslog 0 --log-to-stdout 1 --default-log-level info --network-stack posix"/' scylla-root/etc/default/scylla-server

CONTAINER="$(buildah from $BASE_IMAGE)"
IMAGE="$PRODUCT-minimal-server-$VERSION-$RELEASE"
buildah copy "$CONTAINER" scylla-root/etc/ /etc/
buildah copy "$CONTAINER" scylla-root/opt/ /opt/
buildah copy --chown 65532:65532 "$CONTAINER" scylla-root/var/ /var/

PYTHON_BIN=$(basename scylla-root/opt/scylladb/python3/libexec/python*.bin)
buildah config --env LANG=C.UTF-8 "$CONTAINER"
buildah config --env LANGUAGE= "$CONTAINER"
buildah config --env LC_ALL=C.UTF-8 "$CONTAINER"
buildah config --env PYTHONPATH=/opt/scylladb/docker-minimal/ "$CONTAINER"
buildah config --entrypoint "[\"/opt/scylladb/python3/libexec/ld.so\", \"/opt/scylladb/python3/libexec/$PYTHON_BIN\", \"-s\", \"/opt/scylladb/docker-minimal/docker-entrypoint.py\"]" "$CONTAINER"
buildah config --cmd "" "$CONTAINER"
buildah config --port 10000 --port 9042 --port 9160 --port 9180 --port 7000 --port 7001 "$CONTAINER"
buildah config --volume "/var/lib/scylla" "$CONTAINER"
buildah commit "$CONTAINER" "oci-archive:$IMAGE"
echo "Image is now available in $BUILDDIR/$IMAGE."
