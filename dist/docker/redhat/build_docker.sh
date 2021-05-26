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

product="$(<build/SCYLLA-PRODUCT-FILE)"
version="$(<build/SCYLLA-VERSION-FILE)"
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

container="$(buildah from docker.io/centos:7)"

packages=(
    "build/dist/$mode/redhat/RPMS/$arch/$product-$version-$release.$arch.rpm"
    "build/dist/$mode/redhat/RPMS/$arch/$product-server-$version-$release.$arch.rpm"
    "build/dist/$mode/redhat/RPMS/$arch/$product-conf-$version-$release.$arch.rpm"
    "build/dist/$mode/redhat/RPMS/$arch/$product-kernel-conf-$version-$release.$arch.rpm"
    "build/dist/$mode/redhat/RPMS/$arch/$product-node-exporter-$version-$release.$arch.rpm"
    "tools/java/build/redhat/RPMS/noarch/$product-tools-$version-$release.noarch.rpm"
    "tools/java/build/redhat/RPMS/noarch/$product-tools-core-$version-$release.noarch.rpm"
    "tools/jmx/build/redhat/RPMS/noarch/$product-jmx-$version-$release.noarch.rpm"
    "tools/python3/build/redhat/RPMS/$arch/$product-python3-$version-$release.$arch.rpm"
)

bcp() { buildah copy "$container" "$@"; }
run() { buildah run "$container" "$@"; }
bconfig() { buildah config "$@" "$container"; }


bcp "${packages[@]}" packages/

bcp dist/docker/redhat/etc etc/
bcp dist/docker/redhat/scylla-service.sh /scylla-service.sh
bcp dist/docker/redhat/node-exporter-service.sh /node-exporter-service.sh
bcp dist/docker/redhat/scylla-housekeeping-service.sh /scylla-housekeeping-service.sh
bcp dist/docker/redhat/scylla-jmx-service.sh /scylla-jmx-service.sh
bcp dist/docker/redhat/sshd-service.sh /sshd-service.sh

bcp dist/docker/redhat/scyllasetup.py /scyllasetup.py
bcp dist/docker/redhat/commandlineparser.py /commandlineparser.py
bcp dist/docker/redhat/docker-entrypoint.py /docker-entrypoint.py
bcp dist/docker/redhat/node_exporter_install /node_exporter_install

bcp dist/docker/redhat/scylla_bashrc /scylla_bashrc

run yum -y install epel-release
run yum -y clean expire-cache
run yum -y update
run bash -ec "yum -y localinstall packages/*.rpm"
run yum -y install hostname supervisor openssh-server openssh-clients rsyslog
run yum -y clean all
run bash -ec "cat /scylla_bashrc >> /etc/bashrc"
run mkdir -p /etc/supervisor.conf.d
run mkdir -p /var/log/scylla
run /node_exporter_install
run chown -R scylla:scylla /var/lib/scylla

bconfig --env PATH=/opt/scylladb/python3/bin:/usr/bin:/usr/sbin
bconfig --entrypoint  "/docker-entrypoint.py"
bconfig --port 10000 --port 9042 --port 9160 --port 9180 --port 7000 --port 7001 --port 22
bconfig --volume "/var/lib/scylla"

mkdir -p build/$mode/dist/docker/
image="oci-archive:build/$mode/dist/docker/$product-$version-$release"
buildah commit "$container" "$image"

echo "Image is now available in $image."
