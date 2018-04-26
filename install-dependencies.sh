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

if [ "$ID" = "ubuntu" ]; then
    echo "Adding /etc/apt/sources.list.d/scylla.list"

    if wget "http://downloads.scylladb.com/deb/3rdparty/${VERSION_CODENAME}/scylla-3rdparty.list" -q -O /dev/null; then
        echo "deb  [trusted=yes arch=amd64] http://downloads.scylladb.com/deb/3rdparty/${VERSION_CODENAME} ${VERSION_CODENAME} scylladb/multiverse" > /etc/apt/sources.list.d/scylla-3rdparty.list
    else
        echo "Packages are not available for your Ubuntu release yet, using the Xenial (16.04) list instead"

        echo "deb  [trusted=yes arch=amd64] http://downloads.scylladb.com/deb/3rdparty/xenial xenial scylladb/multiverse" > /etc/apt/sources.list.d/scylla-3rdparty.list
    fi

    apt -y update

    apt -y install libsystemd-dev python3-pyparsing libsnappy-dev libjsoncpp-dev libyaml-cpp-dev libthrift-dev antlr3-c++-dev antlr3 thrift-compiler
elif [ "$ID" = "debian" ]; then
    apt -y install libyaml-cpp-dev libjsoncpp-dev libsnappy-dev
    echo antlr3 and thrift still missing - waiting for ppa
elif [ "$ID" = "fedora" ]; then
    yum install -y yaml-cpp-devel thrift-devel antlr3-tool antlr3-C++-devel jsoncpp-devel snappy-devel
elif [ "$ID" = "centos" ]; then
    yum install -y yaml-cpp-devel thrift-devel scylla-antlr35-tool scylla-antlr35-C++-devel jsoncpp-devel snappy-devel scylla-boost163-static scylla-python34-pyparsing20 systemd-devel
    echo -e "Configure example:\n\tpython3.4 ./configure.py --enable-dpdk --mode=release --static-boost --compiler=/opt/scylladb/bin/g++-7.3 --python python3.4 --ldflag=-Wl,-rpath=/opt/scylladb/lib64 --cflags=-I/opt/scylladb/include --with-antlr3=/opt/scylladb/bin/antlr3"
fi
