#!/bin/bash -e
#
#  Copyright (C) 2017 ScyllaDB

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
    echo "  --releasever  use specific minor version of the distribution repo (ex: 7.4)"
    exit 1
}

is_rhel7_variant() {
    [ "$ID" = "rhel" -o "$ID" = "ol" -o "$ID" = "centos" ] && [[ "$VERSION_ID" =~ ^7 ]]
}

REPO=
RELEASEVER=
while [ $# -gt 0 ]; do
    case "$1" in
        "--repo")
            REPO=$2
            shift 2
            ;;
        "--releasever")
            RELEASEVER=$2
            shift 2
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

if ! is_rhel7_variant; then
    echo "Unsupported distribution"
    exit 1
fi

if [ "$ID" = "centos" ]; then
    if [ ! -f /etc/yum.repos.d/epel.repo ]; then
        sudo yum install -y epel-release
    fi
    RELEASE=7
else
    if [ ! -f /etc/yum.repos.d/epel.repo ]; then
        sudo rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
    fi
    RELEASE=7Server
fi

if [ ! -f /usr/bin/yumdownloader ]; then
    sudo yum -y install yum-utils
fi

if [ ! -f /usr/bin/wget ]; then
    sudo yum -y install wget
fi

if [ ! -f /usr/bin/makeself ]; then
    sudo yum -y install makeself
fi

if [ ! -f /usr/bin/createrepo ]; then
    sudo yum -y install createrepo
fi

sudo yum -y install yum-plugin-downloadonly

cd /etc/yum.repos.d/
sudo wget -N $REPO
cd -

sudo rm -rf build/installroot build/offline_installer build/scylla_offline_installer.sh
mkdir -p build/installroot
mkdir -p build/installroot/etc/yum/vars
sudo sh -c "echo $RELEASE >> build/installroot/etc/yum/vars/releasever"

mkdir -p build/offline_installer
cp dist/offline_installer/redhat/header build/offline_installer
if [ -n "$RELEASEVER" ]; then
    YUMOPTS="--releasever=$RELEASEVER"
fi
sudo yum -y install $YUMOPTS --downloadonly --installroot=`pwd`/build/installroot --downloaddir=build/offline_installer scylla sudo ntp ntpdate net-tools kernel-tools
(cd build/offline_installer; createrepo -v .)
(cd build; makeself offline_installer scylla_offline_installer.sh "Scylla offline package" ./header)
