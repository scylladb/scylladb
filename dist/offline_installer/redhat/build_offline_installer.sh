#!/bin/bash -e
#
#  Copyright (C) 2017-present ScyllaDB

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

if [ ! -e dist/offline_installer/redhat/build_offline_installer.sh ]; then
    echo "run build_offline_installer.sh in top of scylla dir"
    exit 1
fi

print_usage() {
    echo "build_offline_installer.sh --repo [URL]"
    echo "  --repo  repository for fetching scylla rpm, specify .repo file URL"
    echo "  --image [IMAGE]  Use the specified docker IMAGE"
    echo "  --no-docker  Build offline installer without using docker"
    exit 1
}

here="$(realpath $(dirname "$0"))"
releasever=`rpm -q --provides $(rpm -q --whatprovides "system-release(releasever)") | grep "system-release(releasever)"| uniq |  cut -d ' ' -f 3`

REPO=
IMAGE=docker.io/centos:7
NO_DOCKER=false
while [ $# -gt 0 ]; do
    case "$1" in
        "--repo")
            REPO=$2
            shift 2
            ;;
        "--image")
            IMAGE=$2
            shift 2
            ;;
        "--no-docker")
            NO_DOCKER=true
            shift 1
            ;;
        *)
            print_usage
            ;;
    esac
done

. /etc/os-release

if [ -z $REPO ]; then
    print_usage
    exit 1
fi

if ! $NO_DOCKER; then
    if [[ -f ~/.config/scylladb/dbuild ]]; then
        . ~/.config/scylladb/dbuild
    fi
    if which docker >/dev/null 2>&1 ; then
      tool=${DBUILD_TOOL-docker}
    elif which podman >/dev/null 2>&1 ; then
      tool=${DBUILD_TOOL-podman}
    else
      echo "Please make sure you install either podman or docker on this machine to run dbuild" && exit 1
    fi
fi

if [ ! -f /usr/bin/wget ]; then
    sudo yum -y install wget
fi

if [ ! -f /usr/bin/makeself ]; then
    if $NO_DOCKER; then
        # makeself on EPEL7 is too old, borrow it from EPEL8
        # since there is no dependency on the package, it just work
        if [ $release_major = '7' ]; then
            sudo rpm --import https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-8
            sudo cp "$here"/lib/epel8.repo /etc/yum.repos.d/
            YUM_OPTS="--enablerepo=epel8"
        elif [ $release_major = '8' ]; then
            yum -y install epel-release || yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
        fi
    fi
    sudo yum -y install "$YUM_OPTS" makeself
fi

if [ ! -f /usr/bin/createrepo ]; then
    sudo yum -y install createrepo
fi

makeself_ver=$(makeself --version|cut -d ' ' -f 3|sed -e 's/\.//g')
if [ $makeself_ver -lt 240 ]; then
    echo "$(makeself --version) is too old, please install 2.4.0 or later"
    exit 1
fi

sudo rm -rf build/installroot build/offline_docker build/offline_installer build/scylla_offline_installer.sh
mkdir -p build/installroot
mkdir -p build/installroot/etc/yum/vars

mkdir -p build/offline_docker
wget "$REPO" -O build/offline_docker/scylla.repo
cp "$here"/lib/install_deps.sh build/offline_docker
cp "$here"/lib/Dockerfile.in build/offline_docker/Dockerfile
sed -i -e "s#@@IMAGE@@#$IMAGE#" build/offline_docker/Dockerfile

cd build/offline_docker
if $NO_DOCKER; then
    sudo cp scylla.repo /etc/yum.repos.d/scylla.repo
    sudo ./install_deps.sh
else
    image_id=$($tool build -q .)
fi
cd -

mkdir -p build/offline_installer
cp "$here"/lib/header build/offline_installer
if $NO_DOCKER; then
    "$here"/lib/construct_offline_repo.sh
else
    ./tools/toolchain/dbuild --image "$image_id" -- "$here"/lib/construct_offline_repo.sh
fi
(cd build/offline_installer; createrepo -v .)
(cd build; makeself --keep-umask offline_installer scylla_offline_installer.sh "Scylla offline package" ./header)
