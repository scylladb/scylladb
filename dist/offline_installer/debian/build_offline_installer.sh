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

if [ ! -e dist/offline_installer/debian/build_offline_installer.sh ]; then
    echo "run build_offline_installer.sh in top of scylla dir"
    exit 1
fi

print_usage() {
    echo "build_offline_installer.sh --repo [URL] --suite [SUITE]"
    echo "  --repo  repository for fetching scylla .deb, specify .list file URL"
    echo "  --suite specify Ubuntu/Debian release code name"
    exit 1
}

is_debian_variant() {
    [ -f /etc/debian_version ]
}

REPO=
SUITE=
while [ $# -gt 0 ]; do
    case "$1" in
        "--repo")
            REPO=$2
            shift 2
            ;;
        "--suite")
            SUITE=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

. /etc/os-release

if [ -z $REPO ] || [ -z $SUITE ]; then
    print_usage
    exit 1
fi

if ! is_debian_variant; then
    echo "Unsupported distribution"
    exit 1
fi

sudo apt-get update

if [ ! -f /usr/bin/wget ]; then
    sudo apt-get -y install wget
fi

if [ ! -f /usr/sbin/debootstrap ]; then
    sudo apt-get -y install debootstrap
fi

if [ ! -f /usr/bin/makeself ]; then
    sudo apt-get -y install makeself
fi

sudo rm -rf build/chroot build/offline_installer build/scylla_offline_installer.sh
mkdir -p build/chroot
sudo debootstrap $SUITE build/chroot
if [ "$SUITE" = "trusty" ] || [ "$SUITE" = "xenial" ]; then
    sudo tee build/chroot/etc/apt/sources.list << EOS
deb mirror://mirrors.ubuntu.com/mirrors.txt $SUITE main restricted universe multiverse
deb mirror://mirrors.ubuntu.com/mirrors.txt $SUITE-updates main restricted universe multiverse
deb mirror://mirrors.ubuntu.com/mirrors.txt $SUITE-backports main restricted universe multiverse
deb mirror://mirrors.ubuntu.com/mirrors.txt $SUITE-security main restricted universe multiverse
EOS
elif [ "$SUITE" = "jessie" ]; then
    sudo tee build/chroot/etc/apt/sources.list << EOS
deb http://httpredir.debian.org/debian $SUITE main contrib non-free
deb-src http://httpredir.debian.org/debian $SUITE main contrib non-free

deb http://httpredir.debian.org/debian $SUITE-updates main contrib non-free
deb-src http://httpredir.debian.org/debian $SUITE-updates main contrib non-free

deb http://security.debian.org/ $SUITE/updates main contrib non-free
deb-src http://security.debian.org/ $SUITE/updates main contrib non-free
EOS
fi
sudo wget -P build/chroot/etc/apt/sources.list.d $REPO
sudo chroot build/chroot apt-get update --allow-unauthenticated -y
sudo chroot build/chroot apt-get purge -y python-minimal python3-minimal python2.7-minimal libpython2.7-minimal libpython3.*-minimal libssl1.0.0 libexpat1
sudo chroot build/chroot apt-get -y install python-minimal python3-minimal
if [ "$SUITE" = "trusty" ]; then
    sudo chroot build/chroot apt-get -y install software-properties-common
    sudo chroot build/chroot add-apt-repository -y ppa:openjdk-r/ppa
    sudo chroot build/chroot apt-get update --allow-unauthenticated -y
    sudo chroot build/chroot apt-mark hold initramfs-tools
    sudo chroot build/chroot apt-mark hold udev
fi

sudo chroot build/chroot env DEBIAN_FRONTEND=noninteractive apt-get upgrade --allow-unauthenticated -y
sudo chroot build/chroot env DEBIAN_FRONTEND=noninteractive apt-get install -d --allow-unauthenticated -y scylla

mkdir -p build/offline_installer/debs
cp dist/offline_installer/debian/header build/offline_installer
cp build/chroot/var/cache/apt/archives/*.deb build/offline_installer/debs
(cd build; makeself offline_installer scylla_offline_installer.sh "Scylla offline package" ./header)
