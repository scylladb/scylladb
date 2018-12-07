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
    exit 1
}

is_rhel7_variant() {
    [ "$ID" = "rhel" -o "$ID" = "ol" -o "$ID" = "centos" ] && [[ "$VERSION_ID" =~ ^7 ]]
}

REPO=
while [ $# -gt 0 ]; do
    case "$1" in
        "--repo")
            REPO=$2
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
else
    if [ ! -f /etc/yum.repos.d/epel.repo ]; then
        sudo rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
    fi
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

sudo rm -rf build/installroot build/offline_installer build/scylla_offline_installer.sh
mkdir -p build/installroot
sudo rpm --root build/installroot --initdb
sudo yum --nogpgcheck --releasever=7 --installroot=`pwd`/build/installroot install -y @base 
sudo yum --nogpgcheck --releasever=7 --installroot=`pwd`/build/installroot install -y epel-release
sudo wget -P build/installroot/etc/yum.repos.d $REPO

mkdir -p build/offline_installer
cp dist/offline_installer/redhat/header build/offline_installer
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve scylla
# XXX: resolve option doesn't fetch some dependencies, need to manually fetch them
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve sudo.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve ntp.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libedit.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve ntpdate.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve net-tools.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve kernel
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve grubby.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve linux-firmware
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve initscripts.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve iproute.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve iptables.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libnfnetlink.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libnetfilter_conntrack.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libmnl.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve sysvinit-tools.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve yajl.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve mdadm.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libreport-filesystem.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve xfsprogs.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve PyYAML.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libyaml.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libjpeg-turbo.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libaio.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve snappy.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve pciutils.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve hwdata.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve libpciaccess.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve pciutils-libs.x86_64
sudo yumdownloader --installroot=`pwd`/build/installroot --archlist=x86_64 --destdir=build/offline_installer --resolve python-six.noarch
(cd build; makeself offline_installer scylla_offline_installer.sh "Scylla offline package" ./header)
