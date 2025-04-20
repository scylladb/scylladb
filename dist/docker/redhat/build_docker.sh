#!/bin/bash -ex

#
# Copyright (C) 2021-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

trap 'echo "error $? in $0 line $LINENO"' ERR

product="$(<build/SCYLLA-PRODUCT-FILE)"
version="$(sed 's/-/~/' <build/SCYLLA-VERSION-FILE)"
release="$(<build/SCYLLA-RELEASE-FILE)"

mode="release"

arch="$(uname -m)"


print_usage() {
    echo "usage: $0 [--mode mode]"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)
            mode="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

# CMake puts deb packages in build/$<CONFIG>, so translate $mode
# to $<CONFIG> if build.ninja is located under build/.
config=$mode
if [ -f build/build.ninja ]; then
   case $mode in
       release)
           config=RelWithDebInfo
           ;;
       dev)
           config=Dev
           ;;
       debug)
           config=Debug
           ;;
       *)
           echo "unsupported mode: ${mode}"
           exit 1
           ;;
   esac
fi

bcp() { buildah copy "$container" "$@"; }
run() { buildah run "$container" "$@"; }
bconfig() { buildah config "$@" "$container"; }

container="$(buildah from docker.io/redhat/ubi9-minimal:latest)"

packages=(
    "build/dist/$config/redhat/RPMS/$arch/$product-$version-$release.$arch.rpm"
    "build/dist/$config/redhat/RPMS/$arch/$product-server-$version-$release.$arch.rpm"
    "build/dist/$config/redhat/RPMS/$arch/$product-conf-$version-$release.$arch.rpm"
    "build/dist/$config/redhat/RPMS/$arch/$product-kernel-conf-$version-$release.$arch.rpm"
    "build/dist/$config/redhat/RPMS/$arch/$product-node-exporter-$version-$release.$arch.rpm"
    "tools/cqlsh/build/redhat/RPMS/$arch/$product-cqlsh-$version-$release.$arch.rpm"
    "tools/python3/build/redhat/RPMS/$arch/$product-python3-$version-$release.$arch.rpm"
)

bcp "${packages[@]}" packages/

bcp dist/docker/etc etc/
bcp dist/docker/scylla-housekeeping-service.sh /scylla-housekeeping-service.sh

bcp dist/docker/scyllasetup.py /scyllasetup.py
bcp dist/docker/commandlineparser.py /commandlineparser.py
bcp dist/docker/docker-entrypoint.py /docker-entrypoint.py

bcp dist/docker/scylla_bashrc /scylla_bashrc

bcp LICENSE-ScyllaDB-Source-Available.md /licenses/

run microdnf clean all
run microdnf --setopt=tsflags=nodocs -y update
run microdnf --setopt=tsflags=nodocs -y install hostname python3 python3-pip kmod
run microdnf clean all
run pip3 install --no-cache-dir --prefix /usr supervisor
run bash -ec "echo LANG=C.UTF-8 > /etc/locale.conf"
run bash -ec "rpm -ivh packages/*.rpm"
run bash -ec "cat /scylla_bashrc >> /etc/bash.bashrc"
run mkdir -p /var/log/scylla
run chown -R scylla:scylla /var/lib/scylla
run sed -i -e 's/^SCYLLA_ARGS=".*"$/SCYLLA_ARGS="--log-to-syslog 0 --log-to-stdout 1 --network-stack posix"/' /etc/sysconfig/scylla-server

run mkdir -p /opt/scylladb/supervisor
run touch /opt/scylladb/SCYLLA-CONTAINER-FILE
bcp dist/common/supervisor/scylla-server.sh /opt/scylladb/supervisor/scylla-server.sh
bcp dist/common/supervisor/scylla-node-exporter.sh /opt/scylladb/supervisor/scylla-node-exporter.sh
bcp dist/common/supervisor/scylla_util.sh /opt/scylladb/supervisor/scylla_util.sh

# XXX: This is required to run setup scripts in root-mode with non-root user
run chown -R scylla:scylla /etc/scylla.d

bconfig --user scylla:scylla
bconfig --env PATH=/opt/scylladb/python3/bin:/usr/bin:/usr/sbin
bconfig --env LANG=C.UTF-8
bconfig --env LANGUAGE=
bconfig --env LC_ALL=C.UTF-8
bconfig --entrypoint  '["/docker-entrypoint.py"]'
bconfig --cmd  ''
bconfig --port 10000 --port 9042 --port 9160 --port 9180 --port 7000 --port 7001 --port 22
bconfig --volume "/var/lib/scylla"
bconfig --label org.opencontainers.image.ref.name="ScyllaDB"
bconfig --label org.opencontainers.image.version="$version-$release"
bconfig --label description="ScyllaDB"
bconfig --label summary="NoSQL data store using the Seastar framework, compatible with Apache Cassandra And Amazon DynamoDB protocols"
# These are required for Red Hat OpenShift Certification
bconfig --label name="ScyllaDB"
bconfig --label maintainer="ScyllaDB, Inc."
bconfig --label vendor="ScyllaDB, Inc."
bconfig --label version="$version"
bconfig --label release="$release"

mkdir -p build/$mode/dist/docker/
image="oci-archive:build/$mode/dist/docker/$product-$version-$release"
buildah commit --rm "$container" "$image"

echo "Image is now available in $image."
