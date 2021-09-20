#!/bin/bash -ex

#
# Copyright (C) 2021-present ScyllaDB
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

MODE="release"
BASE_IMAGE='docker.io/ubuntu:20.04'
PRODUCT="$(<build/SCYLLA-PRODUCT-FILE)"
VERSION="$(<build/SCYLLA-VERSION-FILE)"
RELEASE="$(<build/SCYLLA-RELEASE-FILE)"
MACHINE_ARCH=$(uname -m)
    case "$MACHINE_ARCH" in
      "x86_64")
        arch="amd64"
        ;;
      "aarch64")
        arch="arm64"
        ;;
      *)
        echo "Unsupported architecture: $MACHINE_ARCH"
        exit 1
    esac


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

container="$(buildah from $BASE_IMAGE)"

packages=(
    "build/dist/$MODE/debian/${product}_$VERSION-$RELEASE-1_$ARCH.deb"
    "build/dist/$MODE/debian/$PRODUCT-server_$VERSION-$RELEASE-1_$ARCH.deb"
    "build/dist/$MODE/debian/$PRODUCT-conf_$VERSION-$RELEASE-1_$ARCH.deb"
    "build/dist/$MODE/debian/$PRODUCT-kernel-conf_$VERSION-$RELEASE-1_$ARCH.deb"
    "build/dist/$MODE/debian/$PRODUCT-node-exporter_$VERSION-$RELEASE-1_$ARCH.deb"
    "tools/java/build/debian/$PRODUCT-tools_$VERSION-$RELEASE-1_all.deb"
    "tools/java/build/debian/$PRODUCT-tools-core_$VERSION-$RELEASE-1_all.deb"
    "tools/jmx/build/debian/$PRODUCT-jmx_$VERSION-$RELEASE-1_all.deb"
    "tools/python3/build/debian/$PRODUCT-python3_$VERSION-$RELEASE-1_$ARCH.deb"
)

bcp() { buildah copy "$container" "$@"; }
run() { buildah run "$container" "$@"; }
bconfig() { buildah config "$@" "$container"; }


bcp "${packages[@]}" packages/

bcp dist/docker/etc etc/
bcp dist/docker/scylla-housekeeping-service.sh /scylla-housekeeping-service.sh
bcp dist/docker/sshd-service.sh /sshd-service.sh

bcp dist/docker/scyllasetup.py /scyllasetup.py
bcp dist/docker/commandlineparser.py /commandlineparser.py
bcp dist/docker/docker-entrypoint.py /docker-entrypoint.py

bcp dist/docker/scylla_bashrc /scylla_bashrc

run apt-get -y clean expire-cache
run apt-get -y update
run apt-get -y install dialog apt-utils
run bash -ec "echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections"
run bash -ec "rm -rf /etc/rsyslog.conf"
run apt-get -y install hostname supervisor openssh-server openssh-client openjdk-11-jre-headless python python-yaml curl rsyslog locales sudo
run locale-gen en_US.UTF-8
run bash -ec "dpkg -i packages/*.deb"
run apt-get -y clean all
run bash -ec "cat /scylla_bashrc >> /etc/bashrc"
run mkdir -p /etc/supervisor.conf.d
run mkdir -p /var/log/scylla
run chown -R scylla:scylla /var/lib/scylla

run mkdir -p /opt/scylladb/supervisor
bcp dist/common/supervisor/scylla-server.sh /opt/scylladb/supervisor/scylla-server.sh
bcp dist/common/supervisor/scylla-jmx.sh /opt/scylladb/supervisor/scylla-jmx.sh
bcp dist/common/supervisor/scylla-node-exporter.sh /opt/scylladb/supervisor/scylla-node-exporter.sh
bcp dist/common/supervisor/scylla_util.sh /opt/scylladb/supervisor/scylla_util.sh

bconfig --env PATH=/opt/scylladb/python3/bin:/usr/bin:/usr/sbin
bconfig --entrypoint  '["/docker-entrypoint.py"]'
bconfig --port 10000 --port 9042 --port 9160 --port 9180 --port 7000 --port 7001 --port 22
bconfig --volume "/var/lib/scylla"

mkdir -p build/$MODE/dist/docker/
image="oci-archive:build/$MODE/dist/docker/$PRODUCT-$VERSION-$RELEASE"
buildah commit "$container" "$image"

echo "Image is now available in $image"
