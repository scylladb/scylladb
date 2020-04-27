#!/bin/bash
#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# os-release may be missing in container environment by default.
if [ -f "/etc/os-release" ]; then
    . /etc/os-release
elif [ -f "/etc/arch-release" ]; then
    export ID=arch
else
    echo "/etc/os-release missing."
    exit 1
fi

bash seastar/install-dependencies.sh

debian_base_packages=(
    liblua5.3-dev
    python3-pyparsing
    python3-colorama
    libsnappy-dev
    libjsoncpp-dev
    rapidjson-dev
    scylla-libthrift010-dev
    scylla-antlr35-c++-dev
    thrift-compiler
    git
    pigz
    libunistring-dev
)

fedora_packages=(
    lua-devel
    yaml-cpp-devel
    thrift-devel
    antlr3-tool
    antlr3-C++-devel
    jsoncpp-devel
    rapidjson-devel
    snappy-devel
    systemd-devel
    git
    python
    sudo
    java-1.8.0-openjdk-headless
    ant
    ant-junit
    maven
    pystache
    patchelf
    python3
    python3-PyYAML
    python3-pyudev
    python3-setuptools
    python3-urwid
    python3-pyparsing
    python3-requests
    python3-pyudev
    python3-setuptools
    python3-magic
    python3-psutil
    python3-cassandra-driver
    python3-colorama
    python3-boto3
    python3-pytest
    dnf-utils
    pigz
    net-tools
    tar
    gzip
    gawk
    util-linux
    ethtool
    hwloc
    glibc-langpack-en
    lld
    xxhash-devel
)

centos_packages=(
    yaml-cpp-devel
    thrift-devel
    scylla-antlr35-tool
    scylla-antlr35-C++-devel
    jsoncpp-devel snappy-devel
    rapidjson-devel
    scylla-boost163-static
    scylla-python34-pyparsing20
    systemd-devel
    pigz
)

# 1) glibc 2.30-3 has sys/sdt.h (systemtap include)
#    some old containers may contain glibc older,
#    so enforce update on that one.
# 2) if problems with signatures, ensure having fresh
#    archlinux-keyring: pacman -Sy archlinux-keyring && pacman -Syyu
# 3) aur installations require having sudo and being
#    a sudoer. makepkg does not work otherwise.
#
# aur: antlr3, antlr3-cpp-headers-git
arch_packages=(
    base-devel
    filesystem
    git
    glibc
    jsoncpp
    lua
    python-pyparsing
    python3
    rapidjson
    snappy
    thrift
)

if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
    apt-get -y install "${debian_base_packages[@]}"
    if [ "$VERSION_ID" = "8" ]; then
        apt-get -y install libsystemd-dev scylla-antlr35 libyaml-cpp-dev
    elif [ "$VERSION_ID" = "14.04" ]; then
        apt-get -y install scylla-antlr35 libyaml-cpp-dev
    elif [ "$VERSION_ID" = "9" ]; then
        apt-get -y install libsystemd-dev antlr3 scylla-libyaml-cpp05-dev
    else
        apt-get -y install libsystemd-dev antlr3 libyaml-cpp-dev
    fi
    echo -e "Configure example:\n\t./configure.py --enable-dpdk --mode=release --static-thrift --static-boost --static-yaml-cpp --compiler=/opt/scylladb/bin/g++-7 --cflags=\"-I/opt/scylladb/include -L/opt/scylladb/lib/x86-linux-gnu/\" --ldflags=\"-Wl,-rpath=/opt/scylladb/lib\""
elif [ "$ID" = "fedora" ]; then
    if rpm -q --quiet yum-utils; then
        echo
        echo "This script will install dnf-utils package, witch will conflict with currently installed package: yum-utils"
        echo "Please remove the package and try to run this script again."
        exit 1
    fi
    yum install -y "${fedora_packages[@]}"
elif [ "$ID" = "centos" ]; then
    yum install -y "${centos_packages[@]}"
    echo -e "Configure example:\n\tpython3.4 ./configure.py --enable-dpdk --mode=release --static-boost --compiler=/opt/scylladb/bin/g++-7.3 --python python3.4 --ldflag=-Wl,-rpath=/opt/scylladb/lib64 --cflags=-I/opt/scylladb/include --with-antlr3=/opt/scylladb/bin/antlr3"
elif [ "$ID" == "arch" ]; then
    # main
    if [ "$EUID" -eq "0" ]; then
        pacman -Sy --needed --noconfirm "${arch_packages[@]}"
    else
        echo "scylla: You now ran $0 as non-root. Run it again as root to execute the pacman part of the installation." 1>&2
    fi

    # aur
    if [ ! -x /usr/bin/antlr3 ]; then
        echo "Installing aur/antlr3..."
        if (( EUID == 0 )); then
            echo "You now ran $0 as root. This can only update dependencies with pacman. Please run again it as non-root to complete the AUR part of the installation." 1>&2
            exit 1
        fi
        TEMP=$(mktemp -d)
        pushd "$TEMP" > /dev/null || exit 1
        git clone --depth 1 https://aur.archlinux.org/antlr3.git
        cd antlr3 || exit 1
        makepkg -si
        popd > /dev/null || exit 1
    fi
    if [ ! -f /usr/include/antlr3.hpp ]; then
        echo "Installing aur/antlr3-cpp-headers-git..."
        if (( EUID == 0 )); then
            echo "You now ran $0 as root. This can only update dependencies with pacman. Please run again it as non-root to complete the AUR part of the installation." 1>&2
            exit 1
        fi
        TEMP=$(mktemp -d)
        pushd "$TEMP" > /dev/null || exit 1
        git clone --depth 1 https://aur.archlinux.org/antlr3-cpp-headers-git.git
        cd antlr3-cpp-headers-git || exit 1
        makepkg -si
        popd > /dev/null || exit 1
    fi
    echo -e "Configure example:\n\t./configure.py\n\tninja release"
fi
