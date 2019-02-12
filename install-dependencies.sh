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

. /etc/os-release

bash seastar/install-dependencies.sh

debian_base_packages=(
    python3-pyparsing
    libsnappy-dev
    libjsoncpp-dev
    scylla-libthrift010-dev
    scylla-antlr35-c++-dev
    thrift-compiler
    git
    pigz
)

fedora_packages=(
    yaml-cpp-devel
    thrift-devel
    antlr3-tool
    antlr3-C++-devel
    jsoncpp-devel
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
    dnf-utils
    pigz
)

centos_packages=(
    yaml-cpp-devel
    thrift-devel
    scylla-antlr35-tool
    scylla-antlr35-C++-devel
    jsoncpp-devel snappy-devel
    scylla-boost163-static
    scylla-python34-pyparsing20
    systemd-devel
    pigz
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
    yum install -y "${fedora_packages[@]}"
elif [ "$ID" = "centos" ]; then
    yum install -y "${centos_packages[@]}"
    echo -e "Configure example:\n\tpython3.4 ./configure.py --enable-dpdk --mode=release --static-boost --compiler=/opt/scylladb/bin/g++-7.3 --python python3.4 --ldflag=-Wl,-rpath=/opt/scylladb/lib64 --cflags=-I/opt/scylladb/include --with-antlr3=/opt/scylladb/bin/antlr3"
fi
